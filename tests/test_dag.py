from __future__ import annotations

from pathlib import Path


def test_dag_declares_runtime_policies_and_repo_scoped_stage_calls():
    source = Path("dags/github_issues_ingestion_v1.py").read_text(encoding="utf-8")

    assert '"retries": 2' in source
    assert "execution_timeout" in source
    assert "run_silver(" in source
    assert 'repo_owner=p["repo_owner"]' in source
    assert 'repo_name=p["repo_name"]' in source
    assert "run_gold(" in source
