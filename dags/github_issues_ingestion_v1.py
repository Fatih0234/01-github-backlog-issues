"""
Airflow DAG: github_issues_ingestion_v1
Weekly incremental sync of GitHub issues → Bronze → Silver → DQ → Gold → summary.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="github_issues_ingestion_v1",
    schedule="0 4 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"repo_owner": "apache", "repo_name": "airflow"},
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["github", "issues", "gitpulse"],
)
def github_issues_ingestion_v1():
    @task
    def fetch_bronze(**context):
        import os

        from gitpulse.bronze.extract_issues import read_watermark, run_extraction
        from gitpulse.runtime import RuntimeConfig

        p = context["params"]
        token = os.environ["GITHUB_TOKEN"]
        config = RuntimeConfig.from_env()
        since = read_watermark(p["repo_owner"], p["repo_name"], config=config)
        run_extraction(
            token=token,
            repo_owner=p["repo_owner"],
            repo_name=p["repo_name"],
            since=since,
            config=config,
        )

    @task
    def process_silver(**context):
        from gitpulse.silver.process_bronze_to_silver import run_silver
        from gitpulse.runtime import RuntimeConfig

        p = context["params"]
        run_silver(
            repo_owner=p["repo_owner"],
            repo_name=p["repo_name"],
            config=RuntimeConfig.from_env(),
        )

    @task
    def run_dq(**context):
        from gitpulse.dq.run_checks import run_checks
        from gitpulse.runtime import RuntimeConfig

        p = context["params"]
        run_checks(
            repo_owner=p["repo_owner"],
            repo_name=p["repo_name"],
            mode="silver",
            config=RuntimeConfig.from_env(),
        )

    @task
    def build_gold(**context):
        from gitpulse.gold.build_gold_marts import run_gold
        from gitpulse.runtime import RuntimeConfig

        p = context["params"]
        run_gold(
            repo_owner=p["repo_owner"],
            repo_name=p["repo_name"],
            config=RuntimeConfig.from_env(),
        )

    @task
    def validate_gold(**context):
        from gitpulse.dq.run_checks import run_checks
        from gitpulse.runtime import RuntimeConfig

        p = context["params"]
        run_checks(
            repo_owner=p["repo_owner"],
            repo_name=p["repo_name"],
            mode="gold",
            config=RuntimeConfig.from_env(),
        )

    @task
    def emit_summary(**context):
        p = context["params"]
        print(f"Pipeline complete for {p['repo_owner']}/{p['repo_name']}")

    fetch_bronze() >> process_silver() >> run_dq() >> build_gold() >> validate_gold() >> emit_summary()


github_issues_ingestion_v1()
