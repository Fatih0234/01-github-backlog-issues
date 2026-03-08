"""
Smoke tests for Phase 3+4 without running full pipelines.

Test 1 — Watermark:
    Calls read_watermark() directly. Prints what `since=` the next bronze run would use.
    Proves incremental sync is wired up without fetching 500 pages.

Test 2 — DQ assertions (in-memory):
    Creates synthetic DuckDB tables (no S3), runs assert_sql() on:
      a) Clean data  → should PASS
      b) Null issue_id → should FAIL
      c) closed_at < created_at → should FAIL
      d) PR in gold mart → should FAIL
"""

import os
import sys
import traceback

import duckdb
from dotenv import load_dotenv

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

from src.bronze.extract_issues import read_watermark
from src.dq.run_checks import assert_sql

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"


# ---------------------------------------------------------------------------
# Test 1: Watermark
# ---------------------------------------------------------------------------

def test_watermark():
    print("\n" + "=" * 60)
    print("TEST 1: Watermark read")
    print("=" * 60)

    repo_owner = os.environ.get("REPO_OWNER", "apache")
    repo_name = os.environ.get("REPO_NAME", "airflow")

    since = read_watermark(repo_owner, repo_name)

    if since:
        print(f"  [{PASS}] Watermark found.")
        print(f"         Next bronze run will use: --since {since}")
        print(f"         (= last successful sync minus 30-day safety buffer)")
    else:
        print(f"  [INFO] No watermark found — silver sync_state not yet written.")
        print(f"         Next bronze run will do a FULL fetch (expected on first run).")
        print(f"         Run silver layer first, then re-run this script.")


# ---------------------------------------------------------------------------
# Test 2: DQ assertions against synthetic in-memory data
# ---------------------------------------------------------------------------

def _sql_val(v, cast: str) -> str:
    """Render a Python value as a SQL literal with the given cast."""
    if v is None:
        return f"NULL::{cast}"
    return f"{v!r}::{cast}"


def make_conn_with_data(silver_rows: list[dict], gold_rows: list[dict]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")

    # Build silver table
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

    # Build gold_lifecycle table
    gold_values = ", ".join(
        f"({_sql_val(r['is_pull_request'], 'BOOLEAN')},)"
        for r in gold_rows
    )
    conn.execute(f"""
        CREATE OR REPLACE VIEW gold_lifecycle AS
        SELECT * FROM (VALUES {gold_values}) t(is_pull_request)
    """)

    return conn


def run_dq_case(label: str, silver_rows: list[dict], gold_rows: list[dict], expect_fail: bool) -> bool:
    conn = make_conn_with_data(silver_rows, gold_rows)
    checks = [
        ("SELECT COUNT(*) FROM silver WHERE issue_id IS NULL", 0, "Null issue_id in silver"),
        ("SELECT COUNT(*) FROM silver WHERE closed_at IS NOT NULL AND closed_at < created_at", 0, "closed_at before created_at"),
        ("SELECT COUNT(*) FROM gold_lifecycle WHERE is_pull_request = true", 0, "PR rows in gold mart"),
    ]
    raised = False
    for query, expected, msg in checks:
        try:
            assert_sql(conn, query, expected, msg)
        except RuntimeError as e:
            raised = True
            if expect_fail:
                print(f"  [{PASS}] {label}: correctly raised → {e}")
            else:
                print(f"  [{FAIL}] {label}: unexpectedly raised → {e}")
            break

    if not raised:
        if not expect_fail:
            print(f"  [{PASS}] {label}: all checks passed as expected.")
        else:
            print(f"  [{FAIL}] {label}: expected a failure but none raised.")

    conn.close()
    return raised == expect_fail


def test_dq():
    print("\n" + "=" * 60)
    print("TEST 2: DQ assertions (in-memory synthetic data)")
    print("=" * 60)

    results = []

    # 2a: clean data — all checks should pass
    results.append(run_dq_case(
        label="2a clean data",
        silver_rows=[
            {"issue_id": "1", "created_at": "2024-01-01 00:00:00", "closed_at": "2024-01-02 00:00:00"},
            {"issue_id": "2", "created_at": "2024-01-01 00:00:00", "closed_at": None},
        ],
        gold_rows=[{"is_pull_request": False}],
        expect_fail=False,
    ))

    # 2b: null issue_id — should fail
    results.append(run_dq_case(
        label="2b null issue_id",
        silver_rows=[
            {"issue_id": None, "created_at": "2024-01-01 00:00:00", "closed_at": None},
        ],
        gold_rows=[{"is_pull_request": False}],
        expect_fail=True,
    ))

    # 2c: closed_at before created_at — should fail
    results.append(run_dq_case(
        label="2c closed_at < created_at",
        silver_rows=[
            {"issue_id": "1", "created_at": "2024-06-01 00:00:00", "closed_at": "2024-01-01 00:00:00"},
        ],
        gold_rows=[{"is_pull_request": False}],
        expect_fail=True,
    ))

    # 2d: PR in gold mart — should fail
    results.append(run_dq_case(
        label="2d PR in gold mart",
        silver_rows=[
            {"issue_id": "1", "created_at": "2024-01-01 00:00:00", "closed_at": None},
        ],
        gold_rows=[{"is_pull_request": True}],
        expect_fail=True,
    ))

    return all(results)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ok = True
    try:
        test_watermark()
    except Exception:
        print(f"  [{FAIL}] Watermark test crashed:")
        traceback.print_exc()
        ok = False

    try:
        dq_ok = test_dq()
        if not dq_ok:
            ok = False
    except Exception:
        print(f"  [{FAIL}] DQ test crashed:")
        traceback.print_exc()
        ok = False

    print()
    if ok:
        print("All smoke tests passed.")
        sys.exit(0)
    else:
        print("One or more smoke tests FAILED.")
        sys.exit(1)
