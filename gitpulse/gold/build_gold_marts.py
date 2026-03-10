"""
Gold layer: transform repo-scoped silver snapshots into analytics marts.
"""

from __future__ import annotations

import argparse
import logging
import os

import duckdb

from gitpulse.runtime import (
    RuntimeConfig,
    configure_duckdb,
    configure_logging,
    gold_dataset_relative_path,
    silver_dataset_relative_path,
    sql_literal,
)

log = logging.getLogger(__name__)


def _register_repo_silver_views(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
) -> None:
    paths = {
        "silver_github_issue_current": silver_dataset_relative_path("issue_current", repo_owner, repo_name),
        "silver_github_issue_labels": silver_dataset_relative_path("issue_labels", repo_owner, repo_name),
        "silver_github_issue_assignees": silver_dataset_relative_path("issue_assignees", repo_owner, repo_name),
    }
    for name, relative_path in paths.items():
        if not config.exists(relative_path):
            conn.execute(
                f"""
                CREATE OR REPLACE TABLE {name} AS
                SELECT * FROM (SELECT 1) WHERE FALSE
                """
            )
            continue

        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM read_parquet('{sql_literal(config.resolve(relative_path))}', union_by_name=true)
            """
        )

    log.info("Registered silver views for %s/%s", repo_owner, repo_name)


def _write_repo_parquet(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    table_name: str,
    relative_path: str,
) -> None:
    config.ensure_parent_dir(relative_path)
    conn.execute(
        f"COPY {table_name} TO '{sql_literal(config.resolve(relative_path))}' (FORMAT PARQUET)"
    )
    log.info("Wrote %s", config.resolve(relative_path))


def _build_mart_issue_lifecycle(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _mart_issue_lifecycle AS
        WITH lbl_agg AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                list_sort(list(label_name)) AS label_names,
                list_sort(list(label_name))[1] AS first_label
            FROM silver_github_issue_labels
            GROUP BY issue_id, repo_owner, repo_name
        ),
        asgn_agg AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                list_sort(list(assignee_login)) AS assignee_logins
            FROM silver_github_issue_assignees
            GROUP BY issue_id, repo_owner, repo_name
        )
        SELECT
            i.issue_id,
            i.issue_node_id,
            i.repo_owner,
            i.repo_name,
            i.issue_number,
            i.title,
            i.html_url,
            i.state,
            i.state_reason,
            i.created_at,
            CAST(i.created_at AS DATE)                                  AS created_date,
            i.closed_at,
            CAST(i.closed_at AS DATE)                                   AS closed_date,
            i.updated_at,
            (i.state = 'open')                                          AS is_open,
            (i.state = 'closed')                                        AS is_closed,
            CASE
                WHEN i.closed_at IS NOT NULL
                THEN DATEDIFF('day', i.created_at, i.closed_at)
            END                                                         AS closure_age_days,
            i.author_association,
            i.comment_count,
            COALESCE(l.label_names, [])                                 AS label_names,
            COALESCE(l.first_label, 'other')                            AS label_group,
            COALESCE(a.assignee_logins, [])                             AS assignee_logins,
            CURRENT_TIMESTAMP                                           AS loaded_at
        FROM silver_github_issue_current i
        LEFT JOIN lbl_agg  l ON l.issue_id = i.issue_id
                             AND l.repo_owner = i.repo_owner
                             AND l.repo_name  = i.repo_name
        LEFT JOIN asgn_agg a ON a.issue_id = i.issue_id
                             AND a.repo_owner = i.repo_owner
                             AND a.repo_name  = i.repo_name
        WHERE i.is_pull_request = false
        """
    )


def _build_mart_issue_daily_flow(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _mart_issue_daily_flow AS
        WITH opened AS (
            SELECT repo_owner, repo_name, created_date AS calendar_date, COUNT(*) AS opened_count
            FROM _mart_issue_lifecycle
            WHERE created_date IS NOT NULL
            GROUP BY repo_owner, repo_name, created_date
        ),
        closed AS (
            SELECT repo_owner, repo_name, closed_date AS calendar_date, COUNT(*) AS closed_count
            FROM _mart_issue_lifecycle
            WHERE is_closed = true AND closed_date IS NOT NULL
            GROUP BY repo_owner, repo_name, closed_date
        ),
        date_bounds AS (
            SELECT repo_owner, repo_name, MIN(calendar_date) AS min_date, MAX(calendar_date) AS max_date
            FROM (
                SELECT repo_owner, repo_name, calendar_date FROM opened
                UNION ALL
                SELECT repo_owner, repo_name, calendar_date FROM closed
            ) all_dates
            GROUP BY repo_owner, repo_name
        ),
        date_spine AS (
            SELECT
                b.repo_owner,
                b.repo_name,
                CAST(g.calendar_date AS DATE) AS calendar_date
            FROM date_bounds b,
            LATERAL generate_series(b.min_date, b.max_date, INTERVAL 1 DAY) AS g(calendar_date)
        ),
        daily AS (
            SELECT
                d.repo_owner,
                d.repo_name,
                d.calendar_date,
                COALESCE(o.opened_count, 0) AS opened_count,
                COALESCE(c.closed_count, 0) AS closed_count,
                COALESCE(o.opened_count, 0) - COALESCE(c.closed_count, 0) AS net_change
            FROM date_spine d
            LEFT JOIN opened o
                ON o.repo_owner = d.repo_owner
               AND o.repo_name = d.repo_name
               AND o.calendar_date = d.calendar_date
            LEFT JOIN closed c
                ON c.repo_owner = d.repo_owner
               AND c.repo_name = d.repo_name
               AND c.calendar_date = d.calendar_date
        )
        SELECT
            repo_owner,
            repo_name,
            calendar_date,
            opened_count,
            closed_count,
            net_change,
            SUM(net_change) OVER (
                PARTITION BY repo_owner, repo_name
                ORDER BY calendar_date
                ROWS UNBOUNDED PRECEDING
            ) AS cumulative_net_change,
            SUM(opened_count) OVER (
                PARTITION BY repo_owner, repo_name
                ORDER BY calendar_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS rolling_7d_opened,
            SUM(closed_count) OVER (
                PARTITION BY repo_owner, repo_name
                ORDER BY calendar_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS rolling_7d_closed,
            SUM(net_change) OVER (
                PARTITION BY repo_owner, repo_name
                ORDER BY calendar_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS rolling_7d_net_change
        FROM daily
        ORDER BY repo_owner, repo_name, calendar_date
        """
    )


def _build_mart_issue_closure_age_monthly(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _mart_issue_closure_age_monthly AS
        SELECT
            repo_owner,
            repo_name,
            DATE_TRUNC('month', closed_date)::DATE                          AS year_month,
            COUNT(*)                                                        AS closed_issue_count,
            approx_quantile(closure_age_days, 0.5)                          AS median_closure_age_days,
            approx_quantile(closure_age_days, 0.9)                          AS p90_closure_age_days,
            SUM(CASE WHEN closure_age_days <= 7 THEN 1 ELSE 0 END)::DOUBLE
                / COUNT(*)                                                  AS share_closed_within_7d,
            SUM(CASE WHEN closure_age_days > 90 THEN 1 ELSE 0 END)::DOUBLE
                / COUNT(*)                                                  AS share_closed_after_90d
        FROM _mart_issue_lifecycle
        WHERE is_closed = true
          AND closed_date IS NOT NULL
          AND closure_age_days IS NOT NULL
        GROUP BY repo_owner, repo_name, DATE_TRUNC('month', closed_date)::DATE
        ORDER BY repo_owner, repo_name, year_month
        """
    )


def _build_mart_issue_weekday_rhythm(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _mart_issue_weekday_rhythm AS
        WITH opened AS (
            SELECT
                repo_owner,
                repo_name,
                ((DAYOFWEEK(created_date) + 6) % 7) AS weekday_num,
                COUNT(*) AS issue_count,
                'opened' AS event_type
            FROM _mart_issue_lifecycle
            WHERE created_date IS NOT NULL
            GROUP BY repo_owner, repo_name, weekday_num, event_type
        ),
        closed AS (
            SELECT
                repo_owner,
                repo_name,
                ((DAYOFWEEK(closed_date) + 6) % 7) AS weekday_num,
                COUNT(*) AS issue_count,
                'closed' AS event_type
            FROM _mart_issue_lifecycle
            WHERE is_closed = true AND closed_date IS NOT NULL
            GROUP BY repo_owner, repo_name, weekday_num, event_type
        ),
        combined AS (
            SELECT * FROM opened
            UNION ALL
            SELECT * FROM closed
        )
        SELECT
            repo_owner,
            repo_name,
            weekday_num,
            CASE weekday_num
                WHEN 0 THEN 'Monday'
                WHEN 1 THEN 'Tuesday'
                WHEN 2 THEN 'Wednesday'
                WHEN 3 THEN 'Thursday'
                WHEN 4 THEN 'Friday'
                WHEN 5 THEN 'Saturday'
                WHEN 6 THEN 'Sunday'
            END AS weekday_name,
            event_type,
            issue_count
        FROM combined
        ORDER BY repo_owner, repo_name, event_type, weekday_num
        """
    )


def _build_mart_issue_swing_days(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _mart_issue_swing_days AS
        SELECT
            repo_owner,
            repo_name,
            calendar_date,
            opened_count,
            closed_count,
            net_change,
            RANK() OVER (PARTITION BY repo_owner, repo_name ORDER BY net_change DESC) AS swing_rank_positive,
            RANK() OVER (PARTITION BY repo_owner, repo_name ORDER BY net_change ASC) AS swing_rank_negative
        FROM _mart_issue_daily_flow
        """
    )


def _register_gold_views(conn: duckdb.DuckDBPyConnection, config: RuntimeConfig) -> None:
    views = {
        "gold_mart_issue_lifecycle": config.resolve(
            "gold/mart_issue_lifecycle/repo_owner=*/repo_name=*/*.parquet"
        ),
        "gold_mart_issue_daily_flow": config.resolve(
            "gold/mart_issue_daily_flow/repo_owner=*/repo_name=*/*.parquet"
        ),
        "gold_mart_issue_closure_age_monthly": config.resolve(
            "gold/mart_issue_closure_age_monthly/repo_owner=*/repo_name=*/*.parquet"
        ),
        "gold_mart_issue_weekday_rhythm": config.resolve(
            "gold/mart_issue_weekday_rhythm/repo_owner=*/repo_name=*/*.parquet"
        ),
        "gold_mart_issue_swing_days": config.resolve(
            "gold/mart_issue_swing_days/repo_owner=*/repo_name=*/*.parquet"
        ),
    }
    for view_name, path in views.items():
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{sql_literal(path)}', hive_partitioning=true, union_by_name=true)
            """
        )


def run_gold(
    *,
    repo_owner: str,
    repo_name: str,
    config: RuntimeConfig | None = None,
) -> None:
    runtime = config or RuntimeConfig.from_env()
    conn = duckdb.connect(runtime.duckdb_file)
    try:
        configure_duckdb(conn, runtime)
        _register_repo_silver_views(
            conn,
            config=runtime,
            repo_owner=repo_owner,
            repo_name=repo_name,
        )

        _build_mart_issue_lifecycle(conn)
        _build_mart_issue_daily_flow(conn)
        _build_mart_issue_closure_age_monthly(conn)
        _build_mart_issue_weekday_rhythm(conn)
        _build_mart_issue_swing_days(conn)

        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_mart_issue_lifecycle",
            relative_path=gold_dataset_relative_path("mart_issue_lifecycle", repo_owner, repo_name),
        )
        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_mart_issue_daily_flow",
            relative_path=gold_dataset_relative_path("mart_issue_daily_flow", repo_owner, repo_name),
        )
        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_mart_issue_closure_age_monthly",
            relative_path=gold_dataset_relative_path(
                "mart_issue_closure_age_monthly",
                repo_owner,
                repo_name,
            ),
        )
        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_mart_issue_weekday_rhythm",
            relative_path=gold_dataset_relative_path("mart_issue_weekday_rhythm", repo_owner, repo_name),
        )
        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_mart_issue_swing_days",
            relative_path=gold_dataset_relative_path("mart_issue_swing_days", repo_owner, repo_name),
        )
        _register_gold_views(conn, runtime)
        log.info("Gold processing complete for %s/%s.", repo_owner, repo_name)
    finally:
        conn.close()


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Gold: build dashboard marts")
    parser.add_argument("--repo-owner", default=None)
    parser.add_argument("--repo-name", default=None)
    args = parser.parse_args()

    run_gold(
        repo_owner=args.repo_owner or os.environ.get("REPO_OWNER", "apache"),
        repo_name=args.repo_name or os.environ.get("REPO_NAME", "airflow"),
    )


if __name__ == "__main__":
    main()
