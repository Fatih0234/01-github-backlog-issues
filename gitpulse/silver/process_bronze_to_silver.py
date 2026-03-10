"""
Silver layer: merge the latest successful bronze delta into a repo-scoped current-state
snapshot and derive normalized issue child tables.
"""

from __future__ import annotations

import argparse
import logging
import os

import duckdb

from gitpulse.runtime import (
    RuntimeConfig,
    bronze_page_relative_path,
    configure_duckdb,
    configure_logging,
    get_latest_successful_manifest,
    silver_dataset_relative_path,
    sql_literal,
)

log = logging.getLogger(__name__)


def _create_empty_delta_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _bronze_delta_raw AS
        SELECT
            CAST(NULL AS VARCHAR) AS run_id,
            CAST(NULL AS VARCHAR) AS repo_owner,
            CAST(NULL AS VARCHAR) AS repo_name,
            CAST(NULL AS INTEGER) AS page_number,
            CAST(NULL AS TIMESTAMPTZ) AS fetched_at,
            CAST(NULL AS BIGINT) AS issue_id,
            CAST(NULL AS TIMESTAMPTZ) AS issue_updated_at,
            CAST(NULL AS VARCHAR) AS payload_json
        WHERE FALSE
        """
    )


def _load_latest_bronze_delta(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
    manifest: dict,
) -> None:
    page_prefix = bronze_page_relative_path(repo_owner, repo_name, manifest["run_id"], 1).rsplit("/", 1)[0]
    page_paths = [
        path
        for path in config.list_relative_paths(page_prefix)
        if path.endswith(".parquet") and "/page_" in path
    ]
    if not page_paths:
        _create_empty_delta_table(conn)
        return

    bronze_glob = config.resolve(f"{page_prefix}/page_*.parquet")
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE _bronze_delta_raw AS
        SELECT
            run_id,
            repo_owner,
            repo_name,
            TRY_CAST(page_number AS INTEGER) AS page_number,
            TRY_CAST(fetched_at AS TIMESTAMPTZ) AS fetched_at,
            TRY_CAST(issue_id AS BIGINT) AS issue_id,
            TRY_CAST(issue_updated_at AS TIMESTAMPTZ) AS issue_updated_at,
            payload_json
        FROM read_parquet('{sql_literal(bronze_glob)}', union_by_name=true)
        """
    )


def _build_delta_current_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _bronze_delta_current AS
        SELECT
            issue_id,
            json_extract_string(payload_json, '$.node_id')                             AS issue_node_id,
            repo_owner,
            repo_name,
            TRY_CAST(json_extract_string(payload_json, '$.number') AS BIGINT)          AS issue_number,
            json_extract_string(payload_json, '$.title')                               AS title,
            json_extract_string(payload_json, '$.state')                               AS state,
            json_extract_string(payload_json, '$.state_reason')                        AS state_reason,
            TRY_CAST(json_extract_string(payload_json, '$.locked') AS BOOLEAN)         AS is_locked,
            json_extract_string(payload_json, '$.active_lock_reason')                  AS active_lock_reason,
            TRY_CAST(json_extract_string(payload_json, '$.created_at') AS TIMESTAMPTZ) AS created_at,
            TRY_CAST(json_extract_string(payload_json, '$.updated_at') AS TIMESTAMPTZ) AS updated_at,
            TRY_CAST(json_extract_string(payload_json, '$.closed_at') AS TIMESTAMPTZ)  AS closed_at,
            json_extract_string(payload_json, '$.html_url')                            AS html_url,
            json_extract_string(payload_json, '$.url')                                 AS api_url,
            json_extract_string(payload_json, '$.comments_url')                        AS comments_url,
            json_extract_string(payload_json, '$.events_url')                          AS events_url,
            json_extract_string(payload_json, '$.author_association')                  AS author_association,
            TRY_CAST(json_extract_string(payload_json, '$.comments') AS BIGINT)        AS comment_count,
            COALESCE(json_exists(payload_json, '$.pull_request'), FALSE)               AS is_pull_request,
            json_extract_string(payload_json, '$.user.login')                          AS author_login,
            TRY_CAST(json_extract_string(payload_json, '$.user.id') AS BIGINT)         AS author_id,
            json_extract_string(payload_json, '$.user.type')                           AS author_type,
            TRY_CAST(
                json_array_length(
                    COALESCE(json_extract(payload_json, '$.assignees'), '[]'::JSON)
                ) AS BIGINT
            )                                                                          AS assignee_count,
            json_extract_string(payload_json, '$.milestone.title')                     AS milestone_title,
            json_extract_string(payload_json, '$.closed_by.login')                     AS closed_by_login,
            json_extract_string(payload_json, '$.performed_via_github_app.name')       AS performed_via_github_app_name,
            json_extract_string(payload_json, '$.pull_request.url')                    AS pull_request_url,
            json_extract_string(payload_json, '$.pull_request.html_url')               AS pull_request_html_url,
            json_extract_string(payload_json, '$.body')                                AS body,
            CURRENT_TIMESTAMP                                                          AS record_loaded_at,
            run_id                                                                     AS source_run_id,
            fetched_at                                                                 AS source_recorded_at,
            page_number                                                                AS source_page_number,
            payload_json                                                               AS source_payload_json
        FROM _bronze_delta_raw
        WHERE issue_id IS NOT NULL
        """
    )


def _load_existing_issue_current(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
) -> None:
    path = silver_dataset_relative_path("issue_current", repo_owner, repo_name)
    if not config.exists(path):
        conn.execute(
            """
            CREATE OR REPLACE TABLE _silver_existing_current AS
            SELECT * FROM _bronze_delta_current WHERE FALSE
            """
        )
        return

    current_uri = config.resolve(path)
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE _silver_existing_current AS
        SELECT * FROM read_parquet('{sql_literal(current_uri)}', union_by_name=true)
        """
    )


def _build_issue_current_snapshot(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _current_candidates AS
        SELECT * FROM _silver_existing_current
        UNION ALL BY NAME
        SELECT * FROM _bronze_delta_current
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE _silver_issue_current AS
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY repo_owner, repo_name, issue_id
                    ORDER BY
                        updated_at DESC NULLS LAST,
                        source_recorded_at DESC NULLS LAST,
                        source_run_id DESC,
                        source_page_number DESC NULLS LAST,
                        source_payload_json DESC
                ) AS rn
            FROM _current_candidates
        )
        SELECT
            issue_id,
            issue_node_id,
            repo_owner,
            repo_name,
            issue_number,
            title,
            state,
            state_reason,
            is_locked,
            active_lock_reason,
            created_at,
            updated_at,
            closed_at,
            html_url,
            api_url,
            comments_url,
            events_url,
            author_association,
            comment_count,
            is_pull_request,
            author_login,
            author_id,
            author_type,
            assignee_count,
            milestone_title,
            closed_by_login,
            performed_via_github_app_name,
            pull_request_url,
            pull_request_html_url,
            body,
            CURRENT_TIMESTAMP AS record_loaded_at,
            source_run_id,
            source_recorded_at,
            source_page_number,
            source_payload_json
        FROM ranked
        WHERE rn = 1
        """
    )


def _build_issue_labels(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _silver_issue_labels AS
        WITH base AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                source_payload_json
            FROM _silver_issue_current
            WHERE COALESCE(json_array_length(COALESCE(json_extract(source_payload_json, '$.labels'), '[]'::JSON)), 0) > 0
        ),
        exploded AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                unnest(COALESCE(json_extract(source_payload_json, '$.labels'), '[]'::JSON)::JSON[]) AS lbl
            FROM base
        )
        SELECT DISTINCT
            issue_id,
            repo_owner,
            repo_name,
            TRY_CAST(json_extract_string(lbl, '$.id') AS BIGINT)          AS label_id,
            json_extract_string(lbl, '$.name')                            AS label_name,
            json_extract_string(lbl, '$.description')                     AS label_description,
            json_extract_string(lbl, '$.color')                           AS label_color,
            TRY_CAST(json_extract_string(lbl, '$.default') AS BOOLEAN)    AS label_is_default,
            CURRENT_TIMESTAMP                                             AS record_loaded_at
        FROM exploded
        WHERE json_extract_string(lbl, '$.id') IS NOT NULL
        """
    )


def _build_issue_assignees(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _silver_issue_assignees AS
        WITH base AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                source_payload_json
            FROM _silver_issue_current
            WHERE COALESCE(json_array_length(COALESCE(json_extract(source_payload_json, '$.assignees'), '[]'::JSON)), 0) > 0
        ),
        exploded AS (
            SELECT
                issue_id,
                repo_owner,
                repo_name,
                unnest(COALESCE(json_extract(source_payload_json, '$.assignees'), '[]'::JSON)::JSON[]) AS a
            FROM base
        )
        SELECT DISTINCT
            issue_id,
            repo_owner,
            repo_name,
            json_extract_string(a, '$.login')                         AS assignee_login,
            TRY_CAST(json_extract_string(a, '$.id') AS BIGINT)       AS assignee_id,
            json_extract_string(a, '$.type')                          AS assignee_type,
            CURRENT_TIMESTAMP                                         AS record_loaded_at
        FROM exploded
        WHERE json_extract_string(a, '$.login') IS NOT NULL
        """
    )


def _write_repo_parquet(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    table_name: str,
    relative_path: str,
) -> None:
    config.ensure_parent_dir(relative_path)
    target = config.resolve(relative_path)
    conn.execute(f"COPY {table_name} TO '{sql_literal(target)}' (FORMAT PARQUET)")
    log.info("Wrote %s", target)


def _write_sync_state(
    conn: duckdb.DuckDBPyConnection,
    *,
    config: RuntimeConfig,
    repo_owner: str,
    repo_name: str,
    manifest: dict,
) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE _silver_sync_state AS
        SELECT
            repo_owner,
            repo_name,
            'issues'                                                   AS endpoint_name,
            MAX(updated_at)                                            AS last_successful_watermark_updated_at,
            ?::TIMESTAMPTZ                                             AS last_successful_run_finished_at,
            ?                                                          AS last_successful_bronze_run_id
        FROM _silver_issue_current
        GROUP BY repo_owner, repo_name
        """,
        [str(manifest["finished_at"]), str(manifest["run_id"])],
    )
    if conn.execute("SELECT COUNT(*) FROM _silver_sync_state").fetchone()[0] == 0:
        conn.execute(
            """
            INSERT INTO _silver_sync_state
            VALUES (?, ?, 'issues', NULL, ?::TIMESTAMPTZ, ?)
            """,
            [repo_owner, repo_name, str(manifest["finished_at"]), str(manifest["run_id"])],
        )

    _write_repo_parquet(
        conn,
        config=config,
        table_name="_silver_sync_state",
        relative_path=silver_dataset_relative_path("sync_state", repo_owner, repo_name),
    )


def _register_views(conn: duckdb.DuckDBPyConnection, config: RuntimeConfig) -> None:
    views = {
        "silver_github_issue_current": config.resolve(
            "silver/issue_current/repo_owner=*/repo_name=*/*.parquet"
        ),
        "silver_github_issue_labels": config.resolve(
            "silver/issue_labels/repo_owner=*/repo_name=*/*.parquet"
        ),
        "silver_github_issue_assignees": config.resolve(
            "silver/issue_assignees/repo_owner=*/repo_name=*/*.parquet"
        ),
        "silver_github_issue_sync_state": config.resolve(
            "silver/sync_state/repo_owner=*/repo_name=*/*.parquet"
        ),
    }
    for view_name, path in views.items():
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{sql_literal(path)}', hive_partitioning=true, union_by_name=true)
            """
        )


def run_silver(
    *,
    repo_owner: str,
    repo_name: str,
    config: RuntimeConfig | None = None,
) -> dict:
    runtime = config or RuntimeConfig.from_env()
    manifest = get_latest_successful_manifest(runtime, repo_owner, repo_name)
    if manifest is None:
        raise RuntimeError(f"No successful bronze manifests found for {repo_owner}/{repo_name}")

    log.info(
        "Processing silver for %s/%s from bronze run_id=%s",
        repo_owner,
        repo_name,
        manifest["run_id"],
    )

    conn = duckdb.connect(runtime.duckdb_file)
    try:
        configure_duckdb(conn, runtime)
        _load_latest_bronze_delta(
            conn,
            config=runtime,
            repo_owner=repo_owner,
            repo_name=repo_name,
            manifest=manifest,
        )
        _build_delta_current_table(conn)
        _load_existing_issue_current(conn, config=runtime, repo_owner=repo_owner, repo_name=repo_name)
        _build_issue_current_snapshot(conn)
        _build_issue_labels(conn)
        _build_issue_assignees(conn)

        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_silver_issue_current",
            relative_path=silver_dataset_relative_path("issue_current", repo_owner, repo_name),
        )
        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_silver_issue_labels",
            relative_path=silver_dataset_relative_path("issue_labels", repo_owner, repo_name),
        )
        _write_repo_parquet(
            conn,
            config=runtime,
            table_name="_silver_issue_assignees",
            relative_path=silver_dataset_relative_path("issue_assignees", repo_owner, repo_name),
        )
        _write_sync_state(
            conn,
            config=runtime,
            repo_owner=repo_owner,
            repo_name=repo_name,
            manifest=manifest,
        )
        _register_views(conn, runtime)
        log.info("Silver processing complete for %s/%s.", repo_owner, repo_name)
    finally:
        conn.close()

    return manifest


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Silver: process bronze into current-state silver")
    parser.add_argument("--repo-owner", default=None)
    parser.add_argument("--repo-name", default=None)
    args = parser.parse_args()

    run_silver(
        repo_owner=args.repo_owner or os.environ.get("REPO_OWNER", "apache"),
        repo_name=args.repo_name or os.environ.get("REPO_NAME", "airflow"),
    )


if __name__ == "__main__":
    main()
