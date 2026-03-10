from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from gitpulse.bronze.extract_issues import (
    WatermarkReadError,
    parse_next_url,
    read_watermark,
    run_extraction,
)
from gitpulse.runtime import bronze_manifest_relative_path, get_latest_successful_manifest, silver_dataset_relative_path


class _FakeResponse:
    def __init__(self, *, url: str, payload: list[dict], link: str | None = None):
        self.url = url
        self._payload = payload
        self.status_code = 200
        self.headers = {"X-RateLimit-Remaining": "4999"}
        if link:
            self.headers["Link"] = link

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def test_parse_next_url_returns_next_link():
    header = (
        '<https://api.github.com/repositories/1/issues?page=2>; rel="next", '
        '<https://api.github.com/repositories/1/issues?page=5>; rel="last"'
    )
    assert parse_next_url(header) == "https://api.github.com/repositories/1/issues?page=2"


def test_read_watermark_returns_none_when_sync_state_missing(local_runtime):
    assert read_watermark("apache", "airflow", config=local_runtime) is None


def test_read_watermark_fails_closed_on_invalid_sync_state(local_runtime):
    path = Path(local_runtime.resolve(silver_dataset_relative_path("sync_state", "apache", "airflow")))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not parquet", encoding="utf-8")

    with pytest.raises(WatermarkReadError):
        read_watermark("apache", "airflow", config=local_runtime)


def test_get_latest_successful_manifest_selects_latest_run(local_runtime, write_bronze_run, issue_factory):
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-01T00-00-00Z",
        finished_at="2024-01-01T00:05:00Z",
        pages=[[issue_factory(issue_id=1)]],
    )
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-02T00-00-00Z",
        finished_at="2024-01-02T00:05:00Z",
        pages=[[issue_factory(issue_id=1, updated_at="2024-01-02T00:00:00Z")]],
    )

    latest = get_latest_successful_manifest(local_runtime, "apache", "airflow")

    assert latest is not None
    assert latest["run_id"] == "manual__2024-01-02T00-00-00Z"


def test_run_extraction_writes_run_scoped_pages_and_manifest(local_runtime, monkeypatch, issue_factory):
    responses = iter(
        [
            _FakeResponse(
                url="https://api.github.com/repos/apache/airflow/issues?page=1",
                payload=[issue_factory(issue_id=1)],
                link='<https://api.github.com/repos/apache/airflow/issues?page=2>; rel="next"',
            ),
            _FakeResponse(
                url="https://api.github.com/repos/apache/airflow/issues?page=2",
                payload=[issue_factory(issue_id=2, updated_at="2024-01-02T00:00:00Z")],
            ),
        ]
    )
    monkeypatch.setattr(
        "gitpulse.bronze.extract_issues.fetch_page",
        lambda session, url, params=None: next(responses),
    )

    manifest = run_extraction(
        token="token",
        repo_owner="apache",
        repo_name="airflow",
        config=local_runtime,
        run_id="manual__2024-01-02T00-00-00Z",
        now=datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
    )

    manifest_rows = local_runtime.read_small_parquet(
        bronze_manifest_relative_path("apache", "airflow")
    )
    assert manifest["run_id"] == "manual__2024-01-02T00-00-00Z"
    assert manifest["records_fetched"] == 2
    assert manifest["pages_fetched"] == 2
    assert manifest_rows[-1]["run_id"] == manifest["run_id"]


def test_run_extraction_does_not_append_manifest_on_failure(local_runtime, monkeypatch, issue_factory):
    calls = 0

    def _raise_on_second_page(session, url, params=None):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _FakeResponse(
                url="https://api.github.com/repos/apache/airflow/issues?page=1",
                payload=[issue_factory(issue_id=1)],
                link='<https://api.github.com/repos/apache/airflow/issues?page=2>; rel="next"',
            )
        raise RuntimeError("boom")

    monkeypatch.setattr("gitpulse.bronze.extract_issues.fetch_page", _raise_on_second_page)

    with pytest.raises(RuntimeError, match="boom"):
        run_extraction(
            token="token",
            repo_owner="apache",
            repo_name="airflow",
            config=local_runtime,
            run_id="manual__2024-01-03T00-00-00Z",
            now=datetime(2024, 1, 3, 0, 0, tzinfo=timezone.utc),
        )

    assert not local_runtime.exists(bronze_manifest_relative_path("apache", "airflow"))
