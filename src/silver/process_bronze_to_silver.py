"""
Silver layer: transform Bronze Parquet (on MinIO) → Silver Parquet (on MinIO).

DuckDB reads bronze via httpfs/S3, writes silver Parquet back to MinIO,
then registers persistent views in the .duckdb file for Metabase.
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


def _transform_and_write_silver(conn: duckdb.DuckDBPyConnection, bucket: str) -> None:
    bronze_glob = f"s3://{bucket}/bronze/github/issues/**/*.parquet"
    log.info("Reading bronze Parquet from %s", bronze_glob)

    conn.execute(f"""
        CREATE OR REPLACE TABLE _bronze_raw AS
        SELECT * EXCLUDE (filename)
        FROM read_parquet('{bronze_glob}', hive_partitioning=true, union_by_name=true, filename=true)
        WHERE "id" IS NOT NULL
          AND "id" != 'run_id'
    """)

    row_count = conn.execute("SELECT COUNT(*) FROM _bronze_raw").fetchone()[0]
    log.info("Bronze rows loaded: %d", row_count)

    repos = conn.execute(
        "SELECT DISTINCT repo_owner, repo_name FROM _bronze_raw ORDER BY 1, 2"
    ).fetchall()
    log.info("Repos found: %s", repos)

    # ── issue_current ─────────────────────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE TABLE _silver_issue_current AS
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST("id" AS BIGINT)
                    ORDER BY COALESCE("updated_at", '') DESC
                ) AS rn
            FROM _bronze_raw
        )
        SELECT
            CAST("id"         AS BIGINT)              AS issue_id,
            "node_id"                                  AS issue_node_id,
            repo_owner,
            repo_name,
            TRY_CAST("number"    AS BIGINT)            AS issue_number,
            "title",
            "state",
            "state_reason",
            TRY_CAST("locked"    AS BOOLEAN)           AS is_locked,
            "active_lock_reason",
            TRY_CAST("created_at" AS TIMESTAMPTZ)      AS created_at,
            TRY_CAST("updated_at" AS TIMESTAMPTZ)      AS updated_at,
            TRY_CAST("closed_at"  AS TIMESTAMPTZ)      AS closed_at,
            "html_url",
            "url"                                      AS api_url,
            "comments_url",
            "events_url",
            "author_association",
            TRY_CAST("comments"  AS BIGINT)            AS comment_count,
            ("pull_request" IS NOT NULL
             AND "pull_request" NOT IN ('None', 'null')) AS is_pull_request,
            -- author fields (from user JSON column)
            json_extract_string("user", '$.login')     AS author_login,
            TRY_CAST(json_extract_string("user", '$.id') AS BIGINT) AS author_id,
            json_extract_string("user", '$.type')      AS author_type,
            -- assignee count
            TRY_CAST(json_array_length(
                CASE WHEN "assignees" IN ('None', 'null', '') THEN '[]'
                     ELSE "assignees" END
            ) AS BIGINT)                               AS assignee_count,
            -- milestone
            json_extract_string("milestone", '$.title') AS milestone_title,
            -- closed_by
            json_extract_string("closed_by", '$.login') AS closed_by_login,
            -- performed_via_github_app
            json_extract_string("performed_via_github_app", '$.name') AS performed_via_github_app_name,
            -- pull_request URLs
            json_extract_string("pull_request", '$.url')       AS pull_request_url,
            json_extract_string("pull_request", '$.html_url')  AS pull_request_html_url,
            -- body
            "body",
            CURRENT_TIMESTAMP                          AS record_loaded_at
        FROM ranked
        WHERE rn = 1
    """)
    for owner, name in repos:
        path = f"s3://{bucket}/silver/issue_current/repo_owner={owner}/repo_name={name}/data.parquet"
        conn.execute(f"""
            COPY (
                SELECT * FROM _silver_issue_current
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)

    # ── issue_labels ──────────────────────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE TABLE _silver_issue_labels AS
        WITH base AS (
            SELECT
                CAST("id" AS BIGINT) AS issue_id,
                repo_owner,
                repo_name,
                "labels"
            FROM _bronze_raw
            WHERE "labels" IS NOT NULL
              AND "labels" NOT IN ('[]', 'None', 'null', '')
        ),
        exploded AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                unnest("labels"::JSON[]) AS lbl
            FROM base
        )
        SELECT DISTINCT
            issue_id,
            repo_owner,
            repo_name,
            TRY_CAST(json_extract_string(lbl, '$.id')          AS BIGINT)  AS label_id,
            json_extract_string(lbl, '$.name')                              AS label_name,
            json_extract_string(lbl, '$.description')                       AS label_description,
            json_extract_string(lbl, '$.color')                             AS label_color,
            TRY_CAST(json_extract_string(lbl, '$.default')     AS BOOLEAN) AS label_is_default,
            CURRENT_TIMESTAMP                                               AS record_loaded_at
        FROM exploded
        WHERE json_extract_string(lbl, '$.id') IS NOT NULL
    """)
    for owner, name in repos:
        path = f"s3://{bucket}/silver/issue_labels/repo_owner={owner}/repo_name={name}/data.parquet"
        conn.execute(f"""
            COPY (
                SELECT * FROM _silver_issue_labels
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)

    # ── issue_assignees ───────────────────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE TABLE _silver_issue_assignees AS
        WITH base AS (
            SELECT
                CAST("id" AS BIGINT) AS issue_id,
                repo_owner,
                repo_name,
                "assignees"
            FROM _bronze_raw
            WHERE "assignees" IS NOT NULL
              AND "assignees" NOT IN ('[]', 'None', 'null', '')
        ),
        exploded AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                unnest("assignees"::JSON[]) AS a
            FROM base
        )
        SELECT DISTINCT
            issue_id,
            repo_owner,
            repo_name,
            json_extract_string(a, '$.login')                          AS assignee_login,
            TRY_CAST(json_extract_string(a, '$.id')       AS BIGINT)  AS assignee_id,
            json_extract_string(a, '$.type')                           AS assignee_type,
            CURRENT_TIMESTAMP                                          AS record_loaded_at
        FROM exploded
        WHERE json_extract_string(a, '$.login') IS NOT NULL
    """)
    for owner, name in repos:
        path = f"s3://{bucket}/silver/issue_assignees/repo_owner={owner}/repo_name={name}/data.parquet"
        conn.execute(f"""
            COPY (
                SELECT * FROM _silver_issue_assignees
                WHERE repo_owner = '{owner}' AND repo_name = '{name}'
            ) TO '{path}' (FORMAT PARQUET)
        """)
        log.info("Wrote %s", path)

    # ── sync_state ────────────────────────────────────────────────────────────
    sync_path = f"s3://{bucket}/silver/sync_state/data.parquet"
    conn.execute(f"""
        COPY (
            SELECT
                repo_owner,
                repo_name,
                'issues'                                            AS endpoint_name,
                MAX(TRY_CAST("updated_at" AS TIMESTAMPTZ))         AS last_successful_watermark_updated_at,
                CURRENT_TIMESTAMP                                   AS last_successful_run_finished_at
            FROM _bronze_raw
            WHERE "updated_at" IS NOT NULL
            GROUP BY repo_owner, repo_name
        ) TO '{sync_path}' (FORMAT PARQUET)
    """)
    log.info("Wrote %s", sync_path)

    conn.execute("DROP TABLE IF EXISTS _bronze_raw")
    conn.execute("DROP TABLE IF EXISTS _silver_issue_current")
    conn.execute("DROP TABLE IF EXISTS _silver_issue_labels")
    conn.execute("DROP TABLE IF EXISTS _silver_issue_assignees")


def _register_views(conn: duckdb.DuckDBPyConnection, bucket: str) -> None:
    views = {
        "silver_github_issue_current":
            f"s3://{bucket}/silver/issue_current/**/*.parquet",
        "silver_github_issue_labels":
            f"s3://{bucket}/silver/issue_labels/**/*.parquet",
        "silver_github_issue_assignees":
            f"s3://{bucket}/silver/issue_assignees/**/*.parquet",
        "silver_github_issue_sync_state":
            f"s3://{bucket}/silver/sync_state/**/*.parquet",
    }
    for view_name, path in views.items():
        conn.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{path}', hive_partitioning=true, union_by_name=true);
        """)
        log.info("View registered: %s", view_name)


def main() -> None:
    bucket = os.environ.get("MINIO_BUCKET", "gitpulse")
    duckdb_file = os.environ.get("DUCKDB_FILE", "/tmp/gitpulse.duckdb")

    log.info("Opening DuckDB at %s", duckdb_file)
    conn = duckdb.connect(duckdb_file)
    try:
        _setup_duckdb(conn)
        _transform_and_write_silver(conn, bucket)
        _register_views(conn, bucket)
        log.info("Silver processing complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
