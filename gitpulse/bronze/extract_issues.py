"""
Bronze extraction: fetch GitHub issues page-by-page, save as Parquet to MinIO.
"""

import argparse
import io
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import json

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.client import Config
from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
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


def make_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "gitpulse",
        }
    )
    return s


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    before_sleep=before_sleep_log(log, logging.WARNING),
)
def fetch_page(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    resp = session.get(url, params=params, timeout=30)
    log.info(
        "GET %s status=%s rate_limit_remaining=%s",
        resp.url,
        resp.status_code,
        resp.headers.get("X-RateLimit-Remaining", "?"),
    )
    resp.raise_for_status()
    return resp


def parse_next_url(link_header: str | None) -> str | None:
    """Parse the `rel="next"` URL from the GitHub Link header."""
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, *rel_parts = part.strip().split(";")
        url_part = url_part.strip().strip("<>")
        for rel in rel_parts:
            if rel.strip() == 'rel="next"':
                return url_part
    return None


def read_watermark(repo_owner: str, repo_name: str) -> str | None:
    """
    Read last_successful_watermark_updated_at from silver sync_state parquet.
    Returns (watermark - 30 days).isoformat() or None if file doesn't exist (full fetch).
    """
    import duckdb

    bucket = os.environ.get("MINIO_BUCKET", "gitpulse")
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "gitpulse")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "gitpulse")
    sync_state_path = f"s3://{bucket}/silver/sync_state/data.parquet"
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"""
            CREATE OR REPLACE SECRET minio (
                TYPE S3,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                ENDPOINT '{endpoint}',
                USE_SSL false,
                URL_STYLE 'path'
            );
        """)
        row = conn.execute(
            f"SELECT last_successful_watermark_updated_at FROM read_parquet('{sync_state_path}') "
            f"WHERE repo_owner = '{repo_owner}' AND repo_name = '{repo_name}' LIMIT 1"
        ).fetchone()
        conn.close()
        if row is None or row[0] is None:
            return None
        watermark_dt = datetime.fromisoformat(str(row[0]))
        since_dt = watermark_dt - timedelta(days=30)
        since_str = since_dt.isoformat()
        log.info("Watermark found: %s → using since=%s", row[0], since_str)
        return since_str
    except Exception as exc:
        log.info("No watermark found (%s) — full fetch mode.", exc)
        return None


def make_s3_client():
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "gitpulse")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "gitpulse")
    return boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _normalize(v):
    """Scalars → str, dicts/lists → JSON string, None → None."""
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return str(v)


def upload_parquet(s3, bucket: str, key: str, records: list[dict]) -> None:
    """Convert list of dicts to Parquet bytes and upload to MinIO."""
    normalized = [{k: _normalize(v) for k, v in rec.items()} for rec in records]
    table = pa.Table.from_pylist(normalized)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log.info("Uploaded s3://%s/%s (%d rows)", bucket, key, len(records))


def run_extraction(
    token: str,
    repo_owner: str,
    repo_name: str,
    max_pages: int | None = None,
    since: str | None = None,
) -> None:
    run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')}"
    extraction_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bucket = os.environ.get("MINIO_BUCKET", "gitpulse")

    prefix = (
        f"bronze/github/issues"
        f"/repo_owner={repo_owner}"
        f"/repo_name={repo_name}"
        f"/extraction_date={extraction_date}"
    )

    s3 = make_s3_client()
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

    started_at = datetime.now(timezone.utc).isoformat()
    page_num = 0
    next_url: str | None = initial_url
    next_params: dict | None = initial_params
    final_url = initial_url

    while next_url:
        if max_pages and page_num >= max_pages:
            log.info("Reached MAX_PAGES=%d, stopping.", max_pages)
            break
        page_num += 1

        resp = fetch_page(session, next_url, params=next_params if page_num == 1 else None)
        final_url = resp.url
        payload = resp.json()

        key = f"{prefix}/page_{page_num:03d}.parquet"
        upload_parquet(s3, bucket, key, payload)

        next_url = parse_next_url(resp.headers.get("Link"))
        next_params = None  # params are encoded in next_url after page 1

        if not payload:
            log.info("Empty page — stopping.")
            break

    finished_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "run_id": run_id,
        "repo": f"{repo_owner}/{repo_name}",
        "pages_fetched": str(page_num),
        "started_at": started_at,
        "finished_at": finished_at,
        "final_url": final_url,
    }
    manifest_key = f"{prefix}/manifest.parquet"
    upload_parquet(s3, bucket, manifest_key, [manifest])
    log.info("Done. pages_fetched=%d", page_num)


def main() -> None:
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
