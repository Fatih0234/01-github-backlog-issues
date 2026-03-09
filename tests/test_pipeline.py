"""
Pipeline unit tests — run with: uv run --group dev pytest

DQ assertions use in-memory DuckDB (no infrastructure needed).
The watermark test requires a live MinIO — mark with -m integration to include it:
    uv run --group dev pytest -m integration
"""

import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.dq.run_checks import assert_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sql_val(v, cast: str) -> str:
    if v is None:
        return f"NULL::{cast}"
    return f"{v!r}::{cast}"


def _make_conn(silver_rows: list[dict], gold_rows: list[dict]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")

    silver_values = ", ".join(
        f"({_sql_val(r['issue_id'], 'VARCHAR')}, "
        f"{_sql_val(r['created_at'], 'TIMESTAMP')}, "
        f"{_sql_val(r['closed_at'], 'TIMESTAMP')})"
        for r in silver_rows
    )
    conn.execute(f"""
        CREATE OR REPLACE VIEW silver AS
        SELECT * FROM (VALUES {silver_values}) t(issue_id, created_at, closed_at)
    """)

    gold_values = ", ".join(
        f"({_sql_val(r['is_pull_request'], 'BOOLEAN')},)"
        for r in gold_rows
    )
    conn.execute(f"""
        CREATE OR REPLACE VIEW gold_lifecycle AS
        SELECT * FROM (VALUES {gold_values}) t(is_pull_request)
    """)

    return conn


_CHECKS = [
    ("SELECT COUNT(*) FROM silver WHERE issue_id IS NULL", 0, "Null issue_id in silver"),
    ("SELECT COUNT(*) FROM silver WHERE closed_at IS NOT NULL AND closed_at < created_at", 0, "closed_at before created_at"),
    ("SELECT COUNT(*) FROM gold_lifecycle WHERE is_pull_request = true", 0, "PR rows in gold mart"),
]


# ---------------------------------------------------------------------------
# DQ tests (in-memory, no infrastructure)
# ---------------------------------------------------------------------------

def test_dq_clean_data_passes():
    conn = _make_conn(
        silver_rows=[
            {"issue_id": "1", "created_at": "2024-01-01 00:00:00", "closed_at": "2024-01-02 00:00:00"},
            {"issue_id": "2", "created_at": "2024-01-01 00:00:00", "closed_at": None},
        ],
        gold_rows=[{"is_pull_request": False}],
    )
    for query, expected, msg in _CHECKS:
        assert_sql(conn, query, expected, msg)
    conn.close()


def test_dq_null_issue_id_fails():
    conn = _make_conn(
        silver_rows=[{"issue_id": None, "created_at": "2024-01-01 00:00:00", "closed_at": None}],
        gold_rows=[{"is_pull_request": False}],
    )
    with pytest.raises(RuntimeError, match="Null issue_id"):
        assert_sql(conn, _CHECKS[0][0], _CHECKS[0][1], _CHECKS[0][2])
    conn.close()


def test_dq_closed_before_created_fails():
    conn = _make_conn(
        silver_rows=[{"issue_id": "1", "created_at": "2024-06-01 00:00:00", "closed_at": "2024-01-01 00:00:00"}],
        gold_rows=[{"is_pull_request": False}],
    )
    with pytest.raises(RuntimeError, match="closed_at before"):
        assert_sql(conn, _CHECKS[1][0], _CHECKS[1][1], _CHECKS[1][2])
    conn.close()


def test_dq_pr_in_gold_fails():
    conn = _make_conn(
        silver_rows=[{"issue_id": "1", "created_at": "2024-01-01 00:00:00", "closed_at": None}],
        gold_rows=[{"is_pull_request": True}],
    )
    with pytest.raises(RuntimeError, match="PR rows"):
        assert_sql(conn, _CHECKS[2][0], _CHECKS[2][1], _CHECKS[2][2])
    conn.close()


# ---------------------------------------------------------------------------
# Watermark integration test (requires live MinIO + silver layer written)
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_watermark_reads_from_silver():
    from src.bronze.extract_issues import read_watermark

    repo_owner = os.environ.get("REPO_OWNER", "apache")
    repo_name = os.environ.get("REPO_NAME", "airflow")
    since = read_watermark(repo_owner, repo_name)
    # Either a timestamp string (incremental) or None (first run) — both valid
    assert since is None or isinstance(since, str)
