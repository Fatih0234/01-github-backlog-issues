from __future__ import annotations

import duckdb
import pytest

from gitpulse.dq.run_checks import assert_sql


def test_assert_sql_passes_when_expected_value_matches():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t AS SELECT 0 AS failures")
    assert_sql(conn, "SELECT failures FROM t", 0, "no failures")
    conn.close()


def test_assert_sql_raises_when_expected_value_differs():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE t AS SELECT 2 AS failures")
    with pytest.raises(RuntimeError, match="expected 0, got 2"):
        assert_sql(conn, "SELECT failures FROM t", 0, "bad failures")
    conn.close()
