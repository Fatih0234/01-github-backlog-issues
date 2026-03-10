"""
Bronze extraction: fetch GitHub issues page-by-page and persist raw issue JSON plus
run metadata into run-scoped Parquet files.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from gitpulse.runtime import (
    RuntimeConfig,
    WatermarkReadError,
    bronze_manifest_relative_path,
    bronze_page_relative_path,
    configure_logging,
    get_latest_successful_manifest,
    silver_dataset_relative_path,
)

log = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"
RETRYABLE_STATUSES = {403, 429, 500, 502, 503, 504}


def _is_retryable(exc: BaseException) -> bool:
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code in RETRYABLE_STATUSES
    )


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"Unsupported datetime value {value!r}")


def make_session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "gitpulse",
        }
    )
    return session


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    before_sleep=before_sleep_log(log, logging.WARNING),
)
def fetch_page(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    response = session.get(url, params=params, timeout=30)
    log.info(
        "GET %s status=%s rate_limit_remaining=%s",
        response.url,
        response.status_code,
        response.headers.get("X-RateLimit-Remaining", "?"),
    )
    response.raise_for_status()
    return response


def parse_next_url(link_header: str | None) -> str | None:
    """Parse the `rel=\"next\"` URL from the GitHub Link header."""
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, *rel_parts = part.strip().split(";")
        url_part = url_part.strip().strip("<>")
        for rel in rel_parts:
            if rel.strip() == 'rel="next"':
                return url_part
    return None


def read_watermark(
    repo_owner: str,
    repo_name: str,
    config: RuntimeConfig | None = None,
) -> str | None:
    """
    Read the watermark from repo-scoped silver sync_state.

    Returns ``None`` only when the repo has never completed a successful silver run.
    Any other read failure raises ``WatermarkReadError`` so the pipeline fails closed.
    """

    runtime = config or RuntimeConfig.from_env()
    path = silver_dataset_relative_path("sync_state", repo_owner, repo_name)
    if not runtime.exists(path):
        return None

    try:
        rows = runtime.read_small_parquet(path)
    except FileNotFoundError:
        return None
    except Exception as exc:  # pragma: no cover - defensive storage error boundary
        raise WatermarkReadError(
            f"Failed to read sync_state for {repo_owner}/{repo_name}"
        ) from exc

    if not rows:
        return None

    watermark = rows[0].get("last_successful_watermark_updated_at")
    if watermark is None:
        return None

    since_dt = _coerce_datetime(watermark) - timedelta(days=30)
    since = since_dt.isoformat()
    log.info("Watermark found: %s -> using since=%s", watermark, since)
    return since


def _build_bronze_rows(
    records: list[dict],
    *,
    run_id: str,
    repo_owner: str,
    repo_name: str,
    page_number: int,
    fetched_at: str,
) -> list[dict]:
    rows: list[dict] = []
    for record in records:
        rows.append(
            {
                "run_id": run_id,
                "repo_owner": repo_owner,
                "repo_name": repo_name,
                "page_number": page_number,
                "fetched_at": fetched_at,
                "issue_id": record.get("id"),
                "issue_updated_at": record.get("updated_at"),
                "payload_json": json.dumps(record, sort_keys=True, separators=(",", ":")),
            }
        )
    return rows


def run_extraction(
    token: str,
    repo_owner: str,
    repo_name: str,
    *,
    config: RuntimeConfig | None = None,
    max_pages: int | None = None,
    since: str | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> dict:
    runtime = config or RuntimeConfig.from_env()
    started = now or datetime.now(timezone.utc)
    effective_run_id = run_id or f"manual__{started.strftime('%Y-%m-%dT%H-%M-%SZ')}"

    session = make_session(token)
    initial_url = f"{GITHUB_API_BASE}/repos/{repo_owner}/{repo_name}/issues"

    if since:
        log.info("Incremental mode: since=%s", since)
    else:
        log.info("Full fetch mode (no watermark/since)")

    initial_params = {
        "state": "all",
        "sort": "updated",
        "direction": "asc",
        "per_page": 100,
    }
    if since:
        initial_params["since"] = since

    pages_fetched = 0
    records_fetched = 0
    max_issue_updated_at: str | None = None
    next_url: str | None = initial_url
    next_params: dict | None = initial_params
    final_url = initial_url

    while next_url:
        if max_pages and pages_fetched >= max_pages:
            log.info("Reached MAX_PAGES=%d, stopping.", max_pages)
            break

        response = fetch_page(session, next_url, params=next_params)
        final_url = str(response.url)
        payload = response.json()
        next_url = parse_next_url(response.headers.get("Link"))
        next_params = None

        if not payload:
            log.info("Empty page returned, finishing run without writing an empty page file.")
            break

        pages_fetched += 1
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = _build_bronze_rows(
            payload,
            run_id=effective_run_id,
            repo_owner=repo_owner,
            repo_name=repo_name,
            page_number=pages_fetched,
            fetched_at=fetched_at,
        )
        runtime.write_small_parquet(
            bronze_page_relative_path(repo_owner, repo_name, effective_run_id, pages_fetched),
            rows,
        )
        records_fetched += len(rows)

        for record in payload:
            updated_at = record.get("updated_at")
            if updated_at and (max_issue_updated_at is None or updated_at > max_issue_updated_at):
                max_issue_updated_at = updated_at

    finished_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "run_id": effective_run_id,
        "repo_owner": repo_owner,
        "repo_name": repo_name,
        "status": "success",
        "started_at": started.isoformat(),
        "finished_at": finished_at,
        "since": since,
        "pages_fetched": pages_fetched,
        "records_fetched": records_fetched,
        "max_issue_updated_at": max_issue_updated_at,
        "final_url": final_url,
    }
    runtime.append_small_parquet(bronze_manifest_relative_path(repo_owner, repo_name), [manifest])
    log.info(
        "Bronze extraction complete for %s/%s run_id=%s pages=%d records=%d",
        repo_owner,
        repo_name,
        effective_run_id,
        pages_fetched,
        records_fetched,
    )
    return manifest


def latest_successful_run(
    repo_owner: str,
    repo_name: str,
    config: RuntimeConfig | None = None,
) -> dict | None:
    runtime = config or RuntimeConfig.from_env()
    return get_latest_successful_manifest(runtime, repo_owner, repo_name)


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Bronze: extract GitHub issues")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages for test runs")
    parser.add_argument("--repo-owner", default=None)
    parser.add_argument("--repo-name", default=None)
    parser.add_argument(
        "--since",
        default=None,
        help="ISO 8601 timestamp; overrides watermark lookup (e.g. for backfills)",
    )
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        log.error("GITHUB_TOKEN not set")
        sys.exit(1)

    repo_owner = args.repo_owner or os.environ.get("REPO_OWNER", "apache")
    repo_name = args.repo_name or os.environ.get("REPO_NAME", "airflow")
    max_pages = args.max_pages or (
        int(os.environ["MAX_PAGES"]) if "MAX_PAGES" in os.environ else None
    )

    since = args.since or read_watermark(repo_owner, repo_name)
    run_extraction(
        token=token,
        repo_owner=repo_owner,
        repo_name=repo_name,
        max_pages=max_pages,
        since=since,
    )


if __name__ == "__main__":
    main()
