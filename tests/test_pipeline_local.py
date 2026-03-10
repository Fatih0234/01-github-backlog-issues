from __future__ import annotations

import pytest

from gitpulse.dq.run_checks import run_checks
from gitpulse.gold.build_gold_marts import run_gold
from gitpulse.runtime import (
    bronze_manifest_relative_path,
    gold_dataset_relative_path,
    silver_dataset_relative_path,
)
from gitpulse.silver.process_bronze_to_silver import run_silver


def test_silver_merges_latest_delta_with_existing_snapshot(
    local_runtime,
    write_bronze_run,
    issue_factory,
):
    issue_one_v1 = issue_factory(
        issue_id=1,
        title="Original title",
        labels=[{"id": 10, "name": "bug", "description": "bug", "color": "ff0000", "default": False}],
        assignees=[{"login": "alice", "id": 100, "type": "User"}],
    )
    issue_two = issue_factory(
        issue_id=2,
        title="Carry forward",
        created_at="2024-01-02T00:00:00Z",
        updated_at="2024-01-02T00:00:00Z",
        state="closed",
        closed_at="2024-01-03T00:00:00Z",
    )
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-01T00-00-00Z",
        finished_at="2024-01-01T00:05:00Z",
        pages=[[issue_one_v1, issue_two]],
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    issue_one_v2 = issue_factory(
        issue_id=1,
        title="Updated title",
        updated_at="2024-01-10T00:00:00Z",
        labels=[{"id": 11, "name": "feature", "description": "feature", "color": "00ff00", "default": False}],
        assignees=[],
    )
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-10T00-00-00Z",
        finished_at="2024-01-10T00:05:00Z",
        since="2023-12-11T00:00:00Z",
        pages=[[issue_one_v2]],
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    current_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow")
    )
    label_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_labels", "apache", "airflow")
    )
    assignee_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_assignees", "apache", "airflow")
    )

    rows_by_issue = {row["issue_id"]: row for row in current_rows}
    assert sorted(rows_by_issue) == [1, 2]
    assert rows_by_issue[1]["title"] == "Updated title"
    assert rows_by_issue[2]["title"] == "Carry forward"
    assert {(row["issue_id"], row["label_name"]) for row in label_rows} == {(1, "feature")}
    assert assignee_rows == []


def test_silver_ignores_orphan_failed_run(local_runtime, write_bronze_run, issue_factory):
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-01T00-00-00Z",
        finished_at="2024-01-01T00:05:00Z",
        pages=[[issue_factory(issue_id=1, title="Stable title")]],
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-02T00-00-00Z",
        finished_at="2024-01-02T00:05:00Z",
        pages=[[issue_factory(issue_id=1, title="Broken title", updated_at="2024-01-02T00:00:00Z")]],
        append_manifest=False,
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    current_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow")
    )
    assert current_rows[0]["title"] == "Stable title"


def test_silver_prefers_later_record_when_updated_at_ties(
    local_runtime,
    write_bronze_run,
    issue_factory,
):
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-05T00-00-00Z",
        finished_at="2024-01-05T00:05:00Z",
        pages=[
            [issue_factory(issue_id=1, title="old", updated_at="2024-01-05T00:00:00Z")],
            [issue_factory(issue_id=1, title="new", updated_at="2024-01-05T00:00:00Z")],
        ],
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    current_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow")
    )
    assert current_rows[0]["title"] == "new"


def test_silver_merges_existing_snapshot_when_schema_evolves(
    local_runtime,
    write_bronze_run,
    issue_factory,
):
    local_runtime.write_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow"),
        [
            {
                "issue_id": 1,
                "issue_node_id": "ISSUE_1",
                "repo_owner": "apache",
                "repo_name": "airflow",
                "issue_number": 1,
                "title": "Legacy snapshot row",
                "state": "open",
                "state_reason": None,
                "is_locked": False,
                "active_lock_reason": None,
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": "2024-01-01T00:00:00+00:00",
                "closed_at": None,
                "html_url": "https://github.com/apache/airflow/issues/1",
                "api_url": "https://api.github.com/repos/apache/airflow/issues/1",
                "comments_url": "https://api.github.com/repos/apache/airflow/issues/1/comments",
                "events_url": "https://api.github.com/repos/apache/airflow/issues/1/events",
                "author_association": "CONTRIBUTOR",
                "comment_count": 0,
                "is_pull_request": False,
                "author_login": "user-1",
                "author_id": 1001,
                "author_type": "User",
                "assignee_count": 0,
                "milestone_title": None,
                "closed_by_login": None,
                "performed_via_github_app_name": None,
                "pull_request_url": None,
                "pull_request_html_url": None,
                "body": "legacy",
                "record_loaded_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-10T00-00-00Z",
        finished_at="2024-01-10T00:05:00Z",
        since="2024-01-01T00:00:00Z",
        pages=[[issue_factory(issue_id=2, title="New delta row", updated_at="2024-01-10T00:00:00Z")]],
    )

    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    current_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow")
    )
    rows_by_issue = {row["issue_id"]: row for row in current_rows}

    assert sorted(rows_by_issue) == [1, 2]
    assert rows_by_issue[1]["title"] == "Legacy snapshot row"
    assert rows_by_issue[1]["source_run_id"] is None
    assert rows_by_issue[2]["title"] == "New delta row"
    assert rows_by_issue[2]["source_run_id"] == "manual__2024-01-10T00-00-00Z"


def test_silver_registers_global_views_without_reading_legacy_flat_files(
    local_runtime,
    write_bronze_run,
    issue_factory,
):
    local_runtime.write_small_parquet(
        "silver/sync_state/data.parquet",
        [
            {
                "endpoint_name": "issues",
                "last_successful_watermark_updated_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )

    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-10T00-00-00Z",
        finished_at="2024-01-10T00:05:00Z",
        pages=[[issue_factory(issue_id=1, title="Fresh row", updated_at="2024-01-10T00:00:00Z")]],
    )

    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    current_rows = local_runtime.read_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow")
    )
    assert current_rows[0]["title"] == "Fresh row"


def test_gold_builds_dense_daily_spine_and_true_rolling_windows(
    local_runtime,
    write_bronze_run,
    issue_factory,
):
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-01T00-00-00Z",
        finished_at="2024-01-01T00:05:00Z",
        pages=[[issue_factory(issue_id=1, created_at="2024-01-01T00:00:00Z")]],
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)

    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-10T00-00-00Z",
        finished_at="2024-01-10T00:05:00Z",
        since="2023-12-11T00:00:00Z",
        pages=[[issue_factory(issue_id=2, created_at="2024-01-10T00:00:00Z", updated_at="2024-01-10T00:00:00Z")]],
    )
    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)
    run_gold(repo_owner="apache", repo_name="airflow", config=local_runtime)

    daily_rows = local_runtime.read_small_parquet(
        gold_dataset_relative_path("mart_issue_daily_flow", "apache", "airflow")
    )
    rows_by_date = {str(row["calendar_date"]): row for row in daily_rows}

    assert len(daily_rows) == 10
    assert rows_by_date["2024-01-05"]["opened_count"] == 0
    assert rows_by_date["2024-01-10"]["opened_count"] == 1
    assert rows_by_date["2024-01-10"]["rolling_7d_opened"] == 1


def test_local_end_to_end_pipeline_passes_dq(
    local_runtime,
    write_bronze_run,
    issue_factory,
):
    write_bronze_run(
        repo_owner="apache",
        repo_name="airflow",
        run_id="manual__2024-01-01T00-00-00Z",
        finished_at="2024-01-01T00:05:00Z",
        pages=[[
            issue_factory(issue_id=1),
            issue_factory(
                issue_id=2,
                state="closed",
                created_at="2024-01-02T00:00:00Z",
                updated_at="2024-01-03T00:00:00Z",
                closed_at="2024-01-03T00:00:00Z",
            ),
            issue_factory(issue_id=3, is_pull_request=True, updated_at="2024-01-04T00:00:00Z"),
        ]],
    )

    run_silver(repo_owner="apache", repo_name="airflow", config=local_runtime)
    run_gold(repo_owner="apache", repo_name="airflow", config=local_runtime)

    run_checks(repo_owner="apache", repo_name="airflow", mode="silver", config=local_runtime)
    run_checks(repo_owner="apache", repo_name="airflow", mode="gold", config=local_runtime)


def test_gold_dq_fails_when_daily_spine_has_gap(local_runtime):
    local_runtime.write_small_parquet(
        silver_dataset_relative_path("issue_current", "apache", "airflow"),
        [
            {
                "issue_id": 1,
                "repo_owner": "apache",
                "repo_name": "airflow",
                "is_pull_request": False,
                "updated_at": "2024-01-01T00:00:00+00:00",
            }
        ],
    )
    local_runtime.write_small_parquet(
        silver_dataset_relative_path("issue_labels", "apache", "airflow"),
        [{"issue_id": 1, "repo_owner": "apache", "repo_name": "airflow", "label_name": "bug"}],
    )
    local_runtime.write_small_parquet(
        silver_dataset_relative_path("issue_assignees", "apache", "airflow"),
        [{"issue_id": 1, "repo_owner": "apache", "repo_name": "airflow", "assignee_login": "alice"}],
    )
    local_runtime.write_small_parquet(
        silver_dataset_relative_path("sync_state", "apache", "airflow"),
        [
            {
                "repo_owner": "apache",
                "repo_name": "airflow",
                "endpoint_name": "issues",
                "last_successful_watermark_updated_at": "2024-01-01T00:00:00+00:00",
                "last_successful_run_finished_at": "2024-01-01T00:05:00+00:00",
                "last_successful_bronze_run_id": "manual__2024-01-01T00-00-00Z",
            }
        ],
    )
    local_runtime.write_small_parquet(
        bronze_manifest_relative_path("apache", "airflow"),
        [
            {
                "run_id": "manual__2024-01-01T00-00-00Z",
                "repo_owner": "apache",
                "repo_name": "airflow",
                "status": "success",
                "started_at": "2024-01-01T00:00:00+00:00",
                "finished_at": "2024-01-01T00:05:00+00:00",
                "since": None,
                "pages_fetched": 1,
                "records_fetched": 1,
                "max_issue_updated_at": "2024-01-01T00:00:00+00:00",
                "final_url": "https://api.github.com/repos/apache/airflow/issues?page=1",
            }
        ],
    )
    local_runtime.write_small_parquet(
        gold_dataset_relative_path("mart_issue_lifecycle", "apache", "airflow"),
        [
            {
                "issue_id": 1,
                "repo_owner": "apache",
                "repo_name": "airflow",
                "is_pull_request": False,
            }
        ],
    )
    local_runtime.write_small_parquet(
        gold_dataset_relative_path("mart_issue_daily_flow", "apache", "airflow"),
        [
            {
                "repo_owner": "apache",
                "repo_name": "airflow",
                "calendar_date": "2024-01-01",
                "opened_count": 1,
                "closed_count": 0,
                "net_change": 1,
                "cumulative_net_change": 1,
                "rolling_7d_opened": 1,
                "rolling_7d_closed": 0,
                "rolling_7d_net_change": 1,
            },
            {
                "repo_owner": "apache",
                "repo_name": "airflow",
                "calendar_date": "2024-01-03",
                "opened_count": 0,
                "closed_count": 0,
                "net_change": 0,
                "cumulative_net_change": 1,
                "rolling_7d_opened": 1,
                "rolling_7d_closed": 0,
                "rolling_7d_net_change": 1,
            },
        ],
    )

    with pytest.raises(RuntimeError, match="date spine"):
        run_checks(repo_owner="apache", repo_name="airflow", mode="gold", config=local_runtime)
