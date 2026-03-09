"""
Data quality checks: read silver/gold Parquet from MinIO via DuckDB httpfs.
Raises RuntimeError on any assertion failure — stops the pipeline.
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


def run_checks(repo_owner: str, repo_name: str) -> None:
    bucket = os.environ.get("MINIO_BUCKET", "gitpulse")
    silver_glob = (
        f"s3://{bucket}/silver/issue_current"
        f"/repo_owner={repo_owner}/repo_name={repo_name}/**/*.parquet"
    )
    gold_glob = (
        f"s3://{bucket}/gold/mart_issue_lifecycle"
        f"/repo_owner={repo_owner}/repo_name={repo_name}/**/*.parquet"
    )
    conn = duckdb.connect(":memory:")
    _setup_duckdb(conn)

    conn.execute(f"""
        CREATE OR REPLACE VIEW silver AS
        SELECT * FROM read_parquet('{silver_glob}', hive_partitioning=true, union_by_name=true)
    """)
    conn.execute(f"""
        CREATE OR REPLACE VIEW gold AS
        SELECT * FROM read_parquet('{gold_glob}', hive_partitioning=true, union_by_name=true)
    """)

    assert_sql(
        conn,
        "SELECT COUNT(*) FROM silver WHERE issue_id IS NULL",
        expected=0,
        msg="Null issue_id in silver",
    )
    assert_sql(
        conn,
        "SELECT COUNT(*) FROM silver WHERE closed_at IS NOT NULL AND closed_at < created_at - INTERVAL '1 hour'",
        expected=0,
        msg="closed_at before created_at (>1h tolerance)",
    )
    assert_sql(
        conn,
        """SELECT COUNT(*)
           FROM gold g
           JOIN silver s ON s.issue_id   = g.issue_id
                        AND s.repo_owner = g.repo_owner
                        AND s.repo_name  = g.repo_name
           WHERE s.is_pull_request = TRUE""",
        expected=0,
        msg="PR contamination in gold mart_issue_lifecycle",
    )
    conn.close()
    log.info("All DQ checks passed.")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="DQ: run data quality checks")
    parser.add_argument("--repo-owner", default=None)
    parser.add_argument("--repo-name", default=None)
    args = parser.parse_args()

    repo_owner = args.repo_owner or os.environ.get("REPO_OWNER", "apache")
    repo_name = args.repo_name or os.environ.get("REPO_NAME", "airflow")

    run_checks(repo_owner=repo_owner, repo_name=repo_name)


if __name__ == "__main__":
    main()
