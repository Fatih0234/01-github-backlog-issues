"""
Gold layer: transform Silver Parquet (on MinIO) → Gold mart Parquet (on MinIO).

DuckDB reads silver via httpfs/S3, computes marts, writes gold Parquet back to MinIO,
then registers persistent views in the .duckdb file for local development.
"""

import logging
import os
import sys

import duckdb
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def _setup_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "gitpulse")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "gitpulse")
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
    log.info("DuckDB httpfs configured for MinIO at %s", endpoint)


def _register_silver_views(conn: duckdb.DuckDBPyConnection, bucket: str) -> None:
    views = {
        "silver_github_issue_current":
            f"s3://{bucket}/silver/issue_current/**/*.parquet",
        "silver_github_issue_labels":
            f"s3://{bucket}/silver/issue_labels/**/*.parquet",
        "silver_github_issue_assignees":
            f"s3://{bucket}/silver/issue_assignees/**/*.parquet",
    }
    for name, path in views.items():
        conn.execute(f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM read_parquet('{path}', hive_partitioning=true, union_by_name=true);
        """)
    log.info("Silver views registered.")


def _build_mart_issue_lifecycle(conn: duckdb.DuckDBPyConnection, bucket: str) -> list[tuple]:
    log.info("Building mart_issue_lifecycle ...")
    conn.execute("""
        CREATE OR REPLACE TABLE _mart_issue_lifecycle AS
        WITH lbl_agg AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                list(label_name ORDER BY label_name)    AS label_names,
                list(label_name ORDER BY label_name)[1] AS first_label
            FROM silver_github_issue_labels
            GROUP BY issue_id, repo_owner, repo_name
        ),
        asgn_agg AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                list(assignee_login ORDER BY assignee_login) AS assignee_logins
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
            CAST(i.created_at AS DATE)                  AS created_date,
            i.closed_at,
            CAST(i.closed_at AS DATE)                   AS closed_date,
            i.updated_at,
            (i.state = 'open')                          AS is_open,
            (i.state = 'closed')                        AS is_closed,
            CASE
                WHEN i.closed_at IS NOT NULL
                THEN DATEDIFF('day', i.created_at, i.closed_at)
            END                                         AS closure_age_days,
            i.author_association,
            i.comment_count,
            COALESCE(l.label_names,  [])                AS label_names,
            COALESCE(l.first_label, 'other')            AS label_group,
            COALESCE(a.assignee_logins, [])             AS assignee_logins,
            CURRENT_TIMESTAMP                           AS loaded_at
        FROM silver_github_issue_current i
        LEFT JOIN lbl_agg  l ON l.issue_id = i.issue_id
                             AND l.repo_owner = i.repo_owner
                             AND l.repo_name  = i.repo_name
        LEFT JOIN asgn_agg a ON a.issue_id = i.issue_id
                             AND a.repo_owner = i.repo_owner
                             AND a.repo_name  = i.repo_name
        WHERE i.is_pull_request = false
    """)

    repos = conn.execute(
        "SELECT DISTINCT repo_owner, repo_name FROM _mart_issue_lifecycle ORDER BY 1, 2"
    ).fetchall()
    log.info("Repos in lifecycle mart: %s", repos)

    for owner, name in repos:
        path = (
            f"s3://{bucket}/gold/mart_issue_lifecycle"
            f"/repo_owner={owner}/repo_name={name}/data.parquet"
        )
        conn.execute(f"""
            COPY (
                SELECT * FROM _mart_issue_lifecycle
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)

    return repos


def _build_mart_issue_daily_flow(
    conn: duckdb.DuckDBPyConnection, bucket: str, repos: list[tuple]
) -> None:
    log.info("Building mart_issue_daily_flow ...")
    conn.execute("""
        CREATE OR REPLACE TABLE _mart_issue_daily_flow AS
        WITH opened AS (
            SELECT repo_owner, repo_name, created_date AS calendar_date,
                   COUNT(*) AS opened_count
            FROM _mart_issue_lifecycle
            WHERE created_date IS NOT NULL
            GROUP BY repo_owner, repo_name, created_date
        ),
        closed AS (
            SELECT repo_owner, repo_name, closed_date AS calendar_date,
                   COUNT(*) AS closed_count
            FROM _mart_issue_lifecycle
            WHERE is_closed = true AND closed_date IS NOT NULL
            GROUP BY repo_owner, repo_name, closed_date
        ),
        all_dates AS (
            SELECT repo_owner, repo_name, calendar_date FROM opened
            UNION
            SELECT repo_owner, repo_name, calendar_date FROM closed
        ),
        daily AS (
            SELECT
                d.repo_owner,
                d.repo_name,
                d.calendar_date,
                COALESCE(o.opened_count, 0) AS opened_count,
                COALESCE(c.closed_count, 0) AS closed_count,
                COALESCE(o.opened_count, 0) - COALESCE(c.closed_count, 0) AS net_change
            FROM all_dates d
            LEFT JOIN opened o
                ON o.repo_owner = d.repo_owner
               AND o.repo_name  = d.repo_name
               AND o.calendar_date = d.calendar_date
            LEFT JOIN closed c
                ON c.repo_owner = d.repo_owner
               AND c.repo_name  = d.repo_name
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
    """)

    for owner, name in repos:
        path = (
            f"s3://{bucket}/gold/mart_issue_daily_flow"
            f"/repo_owner={owner}/repo_name={name}/data.parquet"
        )
        conn.execute(f"""
            COPY (
                SELECT * FROM _mart_issue_daily_flow
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)


def _build_mart_issue_closure_age_monthly(
    conn: duckdb.DuckDBPyConnection, bucket: str, repos: list[tuple]
) -> None:
    log.info("Building mart_issue_closure_age_monthly ...")
    conn.execute("""
        CREATE OR REPLACE TABLE _mart_issue_closure_age_monthly AS
        SELECT
            repo_owner,
            repo_name,
            DATE_TRUNC('month', closed_date)::DATE                          AS year_month,
            COUNT(*)                                                         AS closed_issue_count,
            approx_quantile(closure_age_days, 0.5)                          AS median_closure_age_days,
            approx_quantile(closure_age_days, 0.9)                          AS p90_closure_age_days,
            SUM(CASE WHEN closure_age_days <= 7  THEN 1 ELSE 0 END)::DOUBLE
                / COUNT(*)                                                   AS share_closed_within_7d,
            SUM(CASE WHEN closure_age_days > 90 THEN 1 ELSE 0 END)::DOUBLE
                / COUNT(*)                                                   AS share_closed_after_90d
        FROM _mart_issue_lifecycle
        WHERE is_closed = true
          AND closed_date IS NOT NULL
          AND closure_age_days IS NOT NULL
        GROUP BY repo_owner, repo_name, DATE_TRUNC('month', closed_date)::DATE
        ORDER BY repo_owner, repo_name, year_month
    """)

    for owner, name in repos:
        path = (
            f"s3://{bucket}/gold/mart_issue_closure_age_monthly"
            f"/repo_owner={owner}/repo_name={name}/data.parquet"
        )
        conn.execute(f"""
            COPY (
                SELECT * FROM _mart_issue_closure_age_monthly
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)


def _build_mart_issue_weekday_rhythm(
    conn: duckdb.DuckDBPyConnection, bucket: str, repos: list[tuple]
) -> None:
    log.info("Building mart_issue_weekday_rhythm ...")
    conn.execute("""
        CREATE OR REPLACE TABLE _mart_issue_weekday_rhythm AS
        WITH opened AS (
            SELECT
                repo_owner,
                repo_name,
                ((DAYOFWEEK(created_date) + 6) % 7)     AS weekday_num,
                COUNT(*)                                  AS issue_count,
                'opened'                                  AS event_type
            FROM _mart_issue_lifecycle
            WHERE created_date IS NOT NULL
            GROUP BY repo_owner, repo_name, weekday_num, event_type
        ),
        closed AS (
            SELECT
                repo_owner,
                repo_name,
                ((DAYOFWEEK(closed_date) + 6) % 7)      AS weekday_num,
                COUNT(*)                                  AS issue_count,
                'closed'                                  AS event_type
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
            END                                           AS weekday_name,
            event_type,
            issue_count
        FROM combined
        ORDER BY repo_owner, repo_name, event_type, weekday_num
    """)

    for owner, name in repos:
        path = (
            f"s3://{bucket}/gold/mart_issue_weekday_rhythm"
            f"/repo_owner={owner}/repo_name={name}/data.parquet"
        )
        conn.execute(f"""
            COPY (
                SELECT * FROM _mart_issue_weekday_rhythm
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)


def _build_mart_issue_swing_days(
    conn: duckdb.DuckDBPyConnection, bucket: str, repos: list[tuple]
) -> None:
    log.info("Building mart_issue_swing_days ...")
    conn.execute("""
        CREATE OR REPLACE TABLE _mart_issue_swing_days AS
        SELECT
            repo_owner,
            repo_name,
            calendar_date,
            opened_count,
            closed_count,
            net_change,
            RANK() OVER (
                PARTITION BY repo_owner, repo_name
                ORDER BY net_change DESC
            ) AS swing_rank_positive,
            RANK() OVER (
                PARTITION BY repo_owner, repo_name
                ORDER BY net_change ASC
            ) AS swing_rank_negative
        FROM _mart_issue_daily_flow
    """)

    for owner, name in repos:
        path = (
            f"s3://{bucket}/gold/mart_issue_swing_days"
            f"/repo_owner={owner}/repo_name={name}/data.parquet"
        )
        conn.execute(f"""
            COPY (
                SELECT * FROM _mart_issue_swing_days
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)


def _register_gold_views(conn: duckdb.DuckDBPyConnection, bucket: str) -> None:
    views = {
        "gold_mart_issue_lifecycle":
            f"s3://{bucket}/gold/mart_issue_lifecycle/**/*.parquet",
        "gold_mart_issue_daily_flow":
            f"s3://{bucket}/gold/mart_issue_daily_flow/**/*.parquet",
        "gold_mart_issue_closure_age_monthly":
            f"s3://{bucket}/gold/mart_issue_closure_age_monthly/**/*.parquet",
        "gold_mart_issue_weekday_rhythm":
            f"s3://{bucket}/gold/mart_issue_weekday_rhythm/**/*.parquet",
        "gold_mart_issue_swing_days":
            f"s3://{bucket}/gold/mart_issue_swing_days/**/*.parquet",
    }
    for view_name, path in views.items():
        conn.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{path}', hive_partitioning=true, union_by_name=true);
        """)
        log.info("View registered: %s", view_name)


def _cleanup_temp_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for tbl in [
        "_mart_issue_lifecycle",
        "_mart_issue_daily_flow",
        "_mart_issue_closure_age_monthly",
        "_mart_issue_weekday_rhythm",
        "_mart_issue_swing_days",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")


def run_gold() -> None:
    bucket = os.environ.get("MINIO_BUCKET", "gitpulse")
    duckdb_file = os.environ.get("DUCKDB_FILE", "/tmp/gitpulse.duckdb")
    log.info("Opening DuckDB at %s", duckdb_file)
    conn = duckdb.connect(duckdb_file)
    try:
        _setup_duckdb(conn)
        _register_silver_views(conn, bucket)

        repos = _build_mart_issue_lifecycle(conn, bucket)
        _build_mart_issue_daily_flow(conn, bucket, repos)
        _build_mart_issue_closure_age_monthly(conn, bucket, repos)
        _build_mart_issue_weekday_rhythm(conn, bucket, repos)
        _build_mart_issue_swing_days(conn, bucket, repos)

        _cleanup_temp_tables(conn)
        _register_gold_views(conn, bucket)
        log.info("Gold processing complete.")
    finally:
        conn.close()


def main() -> None:
    run_gold()


if __name__ == "__main__":
    main()
