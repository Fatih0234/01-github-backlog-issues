"""
Airflow DAG: github_issues_ingestion_v1
Weekly incremental sync of GitHub issues → Bronze → Silver → DQ → Gold → summary.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

with DAG(
    dag_id="github_issues_ingestion_v1",
    schedule="0 4 * * 0",  # Sunday 04:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "repo_owner": "apache",
        "repo_name": "airflow",
    },
    tags=["github", "issues", "gitpulse"],
) as dag:

    fetch_bronze = BashOperator(
        task_id="fetch_bronze",
        bash_command=(
            "cd " + PROJECT_DIR + " && "
            "python src/bronze/extract_issues.py "
            "--repo-owner {{ params.repo_owner }} "
            "--repo-name {{ params.repo_name }}"
        ),
    )

    process_silver = BashOperator(
        task_id="process_silver",
        bash_command="cd " + PROJECT_DIR + " && python src/silver/process_bronze_to_silver.py",
    )

    run_dq = BashOperator(
        task_id="run_dq",
        bash_command=(
            "cd " + PROJECT_DIR + " && "
            "python src/dq/run_checks.py "
            "--repo-owner {{ params.repo_owner }} "
            "--repo-name {{ params.repo_name }}"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="cd " + PROJECT_DIR + " && python src/gold/build_gold_marts.py",
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=(
            "cd " + PROJECT_DIR + " && "
            "python src/dq/run_checks.py "
            "--repo-owner {{ params.repo_owner }} "
            "--repo-name {{ params.repo_name }} "
            "--mode gold"
        ),
    )

    emit_summary = BashOperator(
        task_id="emit_summary",
        bash_command=(
            "echo 'Pipeline complete for {{ params.repo_owner }}/{{ params.repo_name }}'"
        ),
    )

    fetch_bronze >> process_silver >> run_dq >> build_gold >> validate_gold >> emit_summary
