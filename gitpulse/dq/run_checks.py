"""
Data quality checks for repo-scoped silver and gold datasets.
"""

from __future__ import annotations

import argparse
import logging
import os

import duckdb

from gitpulse.runtime import (
    RuntimeConfig,
    bronze_manifest_relative_path,
    configure_duckdb,
    configure_logging,
    gold_dataset_relative_path,
    silver_dataset_relative_path,
    sql_literal,
)

log = logging.getLogger(__name__)


def assert_sql(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    expected: int,
    msg: str,
) -> None:
    result = conn.execute(query).fetchone()
    actual = result[0] if result else 0
    if actual != expected:
        raise RuntimeError(f"DQ FAIL [{msg}]: expected {expected}, got {actual}")
    log.info("DQ PASS [%s]", msg)


def _register_silver_views(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
) -> None:
    silver_paths = {
        "silver": silver_dataset_relative_path("issue_current", repo_owner, repo_name),
        "silver_labels": silver_dataset_relative_path("issue_labels", repo_owner, repo_name),
        "silver_assignees": silver_dataset_relative_path("issue_assignees", repo_owner, repo_name),
        "sync_state": silver_dataset_relative_path("sync_state", repo_owner, repo_name),
        "manifest_history": bronze_manifest_relative_path(repo_owner, repo_name),
    }
    for view_name, relative_path in silver_paths.items():
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{sql_literal(config.resolve(relative_path))}', union_by_name=true)
            """
        )


def _register_gold_views(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
) -> None:
    gold_paths = {
        "gold_lifecycle": gold_dataset_relative_path("mart_issue_lifecycle", repo_owner, repo_name),
        "gold_daily_flow": gold_dataset_relative_path("mart_issue_daily_flow", repo_owner, repo_name),
    }
    for view_name, relative_path in gold_paths.items():
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{sql_literal(config.resolve(relative_path))}', union_by_name=true)
            """
        )


def run_checks(
    *,
    repo_owner: str,
    repo_name: str,
    mode: str = "silver",
    config: RuntimeConfig | None = None,
) -> None:
    runtime = config or RuntimeConfig.from_env()
    conn = duckdb.connect(":memory:")
    try:
        configure_duckdb(conn, runtime)
        _register_silver_views(conn, config=runtime, repo_owner=repo_owner, repo_name=repo_name)

        if mode == "silver":
            assert_sql(
                conn,
                "SELECT COUNT(*) FROM silver WHERE issue_id IS NULL",
                expected=0,
                msg="Null issue_id in silver",
            )
            assert_sql(
                conn,
                """
                SELECT COUNT(*) FROM (
                    SELECT issue_id
                    FROM silver
                    GROUP BY issue_id
                    HAVING COUNT(*) > 1
                )
                """,
                expected=0,
                msg="Duplicate issue_id rows in silver current snapshot",
            )
            assert_sql(
                conn,
                """
                SELECT COUNT(*)
                FROM silver
                WHERE closed_at IS NOT NULL
                  AND closed_at < created_at - INTERVAL '1 hour'
                """,
                expected=0,
                msg="closed_at before created_at (>1h tolerance)",
            )
            assert_sql(
                conn,
                """
                SELECT COUNT(*)
                FROM silver_labels l
                LEFT JOIN silver s
                  ON s.issue_id = l.issue_id
                 AND s.repo_owner = l.repo_owner
                 AND s.repo_name = l.repo_name
                WHERE s.issue_id IS NULL
                """,
                expected=0,
                msg="Orphan label rows in silver",
            )
            assert_sql(
                conn,
                """
                SELECT COUNT(*)
                FROM silver_assignees a
                LEFT JOIN silver s
                  ON s.issue_id = a.issue_id
                 AND s.repo_owner = a.repo_owner
                 AND s.repo_name = a.repo_name
                WHERE s.issue_id IS NULL
                """,
                expected=0,
                msg="Orphan assignee rows in silver",
            )
            assert_sql(
                conn,
                """
                WITH latest_manifest AS (
                    SELECT *
                    FROM manifest_history
                    WHERE status = 'success'
                    ORDER BY finished_at DESC, run_id DESC
                    LIMIT 1
                )
                SELECT COUNT(*)
                FROM sync_state s
                CROSS JOIN latest_manifest m
                WHERE s.last_successful_bronze_run_id IS DISTINCT FROM m.run_id
                   OR s.last_successful_run_finished_at IS DISTINCT FROM m.finished_at
                   OR s.last_successful_watermark_updated_at IS DISTINCT FROM (
                        SELECT MAX(updated_at) FROM silver
                   )
                """,
                expected=0,
                msg="Sync state mismatch with latest successful manifest",
            )
        elif mode == "gold":
            _register_gold_views(conn, config=runtime, repo_owner=repo_owner, repo_name=repo_name)
            assert_sql(
                conn,
                """
                SELECT COUNT(*)
                FROM gold_lifecycle g
                JOIN silver s
                  ON s.issue_id = g.issue_id
                 AND s.repo_owner = g.repo_owner
                 AND s.repo_name = g.repo_name
                WHERE s.is_pull_request = TRUE
                """,
                expected=0,
                msg="PR contamination in gold mart_issue_lifecycle",
            )
            assert_sql(
                conn,
                """
                SELECT ABS(
                    (SELECT COUNT(*) FROM gold_lifecycle)
                    -
                    (SELECT COUNT(*) FROM silver WHERE is_pull_request = FALSE)
                )
                """,
                expected=0,
                msg="Gold lifecycle count mismatch versus silver non-PR issues",
            )
            assert_sql(
                conn,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT
                        repo_owner,
                        repo_name,
                        COUNT(*) AS actual_rows,
                        DATEDIFF(
                            'day',
                            MIN(CAST(calendar_date AS DATE)),
                            MAX(CAST(calendar_date AS DATE))
                        ) + 1 AS expected_rows
                    FROM gold_daily_flow
                    GROUP BY repo_owner, repo_name
                    HAVING COUNT(*) != DATEDIFF(
                        'day',
                        MIN(CAST(calendar_date AS DATE)),
                        MAX(CAST(calendar_date AS DATE))
                    ) + 1
                )
                """,
                expected=0,
                msg="Gaps detected in gold mart_issue_daily_flow date spine",
            )
        else:
            raise ValueError(f"Unknown mode: {mode!r}. Use 'silver' or 'gold'.")
    finally:
        conn.close()

    log.info("All DQ checks passed for %s/%s (%s mode).", repo_owner, repo_name, mode)


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="DQ: run data quality checks")
    parser.add_argument("--repo-owner", default=None)
    parser.add_argument("--repo-name", default=None)
    parser.add_argument("--mode", default="silver", choices=["silver", "gold"])
    args = parser.parse_args()

    run_checks(
        repo_owner=args.repo_owner or os.environ.get("REPO_OWNER", "apache"),
        repo_name=args.repo_name or os.environ.get("REPO_NAME", "airflow"),
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
