from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from gitpulse.runtime import (
    RuntimeConfig,
    bronze_manifest_relative_path,
    bronze_page_relative_path,
)


def _iso(value: str) -> str:
    return value if value.endswith("Z") else f"{value}Z"


@pytest.fixture
def local_runtime(tmp_path) -> RuntimeConfig:
    return RuntimeConfig(
        storage_backend="local",
        data_root=str(tmp_path / "data"),
        duckdb_file=str(tmp_path / "gitpulse.duckdb"),
    )


@pytest.fixture
def issue_factory():
    def _factory(
        *,
        issue_id: int,
        title: str | None = None,
        state: str = "open",
        created_at: str = "2024-01-01T00:00:00Z",
        updated_at: str = "2024-01-01T00:00:00Z",
        closed_at: str | None = None,
        labels: list[dict] | None = None,
        assignees: list[dict] | None = None,
        comments: int = 0,
        is_pull_request: bool = False,
    ) -> dict:
        issue = {
            "id": issue_id,
            "node_id": f"ISSUE_{issue_id}",
            "number": issue_id,
            "title": title or f"Issue {issue_id}",
            "state": state,
            "state_reason": "completed" if state == "closed" else None,
            "locked": False,
            "active_lock_reason": None,
            "created_at": _iso(created_at),
            "updated_at": _iso(updated_at),
            "closed_at": _iso(closed_at) if closed_at else None,
            "html_url": f"https://github.com/apache/airflow/issues/{issue_id}",
            "url": f"https://api.github.com/repos/apache/airflow/issues/{issue_id}",
            "comments_url": f"https://api.github.com/repos/apache/airflow/issues/{issue_id}/comments",
            "events_url": f"https://api.github.com/repos/apache/airflow/issues/{issue_id}/events",
            "author_association": "CONTRIBUTOR",
            "comments": comments,
            "user": {
                "login": f"user-{issue_id}",
                "id": issue_id + 1000,
                "type": "User",
            },
            "labels": labels or [],
            "assignees": assignees or [],
            "milestone": None,
            "closed_by": None,
            "performed_via_github_app": None,
            "body": f"body-{issue_id}",
        }
        if is_pull_request:
            issue["pull_request"] = {
                "url": f"https://api.github.com/repos/apache/airflow/pulls/{issue_id}",
                "html_url": f"https://github.com/apache/airflow/pull/{issue_id}",
            }
        return issue

    return _factory


@pytest.fixture
def write_bronze_run(local_runtime):
    def _writer(
        *,
        repo_owner: str,
        repo_name: str,
        run_id: str,
        pages: list[list[dict]],
        started_at: str = "2024-01-01T00:00:00Z",
        finished_at: str = "2024-01-01T00:10:00Z",
        since: str | None = None,
        append_manifest: bool = True,
    ) -> dict:
        pages_fetched = 0
        records_fetched = 0
        max_issue_updated_at: str | None = None

        for page_number, issues in enumerate(pages, start=1):
            if not issues:
                continue
            rows = []
            pages_fetched += 1
            for issue in issues:
                rows.append(
                    {
                        "run_id": run_id,
                        "repo_owner": repo_owner,
                        "repo_name": repo_name,
                        "page_number": page_number,
                        "fetched_at": datetime(2024, 1, page_number, 0, 0, tzinfo=timezone.utc).isoformat(),
                        "issue_id": issue["id"],
                        "issue_updated_at": issue.get("updated_at"),
                        "payload_json": json.dumps(issue, sort_keys=True, separators=(",", ":")),
                    }
                )
                updated_at = issue.get("updated_at")
                if updated_at and (max_issue_updated_at is None or updated_at > max_issue_updated_at):
                    max_issue_updated_at = updated_at
            local_runtime.write_small_parquet(
                bronze_page_relative_path(repo_owner, repo_name, run_id, page_number),
                rows,
            )
            records_fetched += len(rows)

        manifest = {
            "run_id": run_id,
            "repo_owner": repo_owner,
            "repo_name": repo_name,
            "status": "success",
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "since": since,
            "pages_fetched": pages_fetched,
            "records_fetched": records_fetched,
            "max_issue_updated_at": max_issue_updated_at,
            "final_url": f"https://api.github.com/repos/{repo_owner}/{repo_name}/issues?page={pages_fetched or 1}",
        }
        if append_manifest:
            local_runtime.append_small_parquet(
                bronze_manifest_relative_path(repo_owner, repo_name),
                [manifest],
            )
        return manifest

    return _writer
