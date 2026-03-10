from __future__ import annotations

"""
Minimal teaching script for the GitPulse project walkthrough.

What this does:
1) Calls the GitHub REST API for ONE page of repository issues.
2) Prints the pagination and rate-limit headers that matter.
3) Saves the raw API response to JSON.
4) Saves a smaller, project-relevant JSON snapshot.
 
Why this is useful in the video:
- It shows what working with a REST API feels like before the full project.
- It shows the habit of saving raw data first.
- It shows how to choose only the fields that answer your question.
- It sets up the next step: loading JSON into DuckDB and writing Parquet.

Important:
- This is intentionally simple.
- No retries, no backoff, no looping through all pages, no orchestration.
- The goal is to teach first principles, not to rebuild the whole project live.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


# -----------------------------------------------------------------------------
# Demo settings
# -----------------------------------------------------------------------------
OWNER = "apache"
REPO = "airflow"
PAGE = 1
PER_PAGE = 30  # keep it small for a demo; enough to show pagination exists
STATE = "all"  # open + closed gives a better pipeline snapshot
SORT = "updated"
DIRECTION = "desc"

BASE_URL = "https://api.github.com"
OUTPUT_DIR = Path("demo_output")
RAW_JSON_PATH = OUTPUT_DIR / "issues_page_1_raw.json"
RELEVANT_JSON_PATH = OUTPUT_DIR / "issues_page_1_relevant.json"
META_JSON_PATH = OUTPUT_DIR / "issues_page_1_meta.json"


# -----------------------------------------------------------------------------
# Small helper: read GITHUB_TOKEN from environment first, then fallback to .env
# using python-dotenv for parsing.
# -----------------------------------------------------------------------------

def load_env_value(key: str, env_file: str = ".env") -> str | None:
    value = os.getenv(key)
    if value:
        return value

    load_dotenv(dotenv_path=env_file)
    value = os.getenv(key)
    return value if value else None


# -----------------------------------------------------------------------------
# Keep only fields that matter for the backlog / medallion pipeline story.
# This is the key teaching idea: ask a question, keep the fields that help answer
# that question, and leave the rest for later.
# -----------------------------------------------------------------------------

def shape_issue(issue: dict[str, Any], extracted_at_utc: str) -> dict[str, Any]:
    labels = [label for label in issue.get("labels", []) if isinstance(label, dict)]
    assignees = [a for a in issue.get("assignees", []) if isinstance(a, dict)]
    user = issue.get("user") or {}
    milestone = issue.get("milestone") or {}
    pull_request = issue.get("pull_request") or {}

    return {
        # pipeline / lineage fields
        "repo_owner": OWNER,
        "repo_name": REPO,
        "extracted_at_utc": extracted_at_utc,
        # issue identity
        "issue_id": issue.get("id"),
        "node_id": issue.get("node_id"),
        "issue_number": issue.get("number"),
        # descriptive fields
        "title": issue.get("title"),
        "state": issue.get("state"),
        "state_reason": issue.get("state_reason"),
        # timing fields
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        # status / workflow fields
        "is_locked": issue.get("locked"),
        "active_lock_reason": issue.get("active_lock_reason"),
        "author_association": issue.get("author_association"),
        "comment_count": issue.get("comments"),
        # author fields
        "author_login": user.get("login"),
        "author_id": user.get("id"),
        "author_type": user.get("type"),
        # useful nested business context
        "milestone_title": milestone.get("title"),
        "milestone_state": milestone.get("state"),
        # links
        "html_url": issue.get("html_url"),
        "api_url": issue.get("url"),
        "comments_url": issue.get("comments_url"),
        "events_url": issue.get("events_url"),
        # important GitHub REST API caveat:
        # the issues endpoint can also return pull requests
        "is_pull_request": bool(pull_request),
        "pull_request_url": pull_request.get("url"),
        # keep some nested structures because they become great teaching examples
        # for flattening in the silver layer later on
        "labels": [
            {
                "id": label.get("id"),
                "name": label.get("name"),
                "color": label.get("color"),
                "description": label.get("description"),
                "default": label.get("default"),
            }
            for label in labels
        ],
        "label_names": [label.get("name") for label in labels if label.get("name")],
        "assignees": [
            {
                "login": assignee.get("login"),
                "id": assignee.get("id"),
                "type": assignee.get("type"),
            }
            for assignee in assignees
        ],
        "assignee_logins": [a.get("login") for a in assignees if a.get("login")],
    }


# -----------------------------------------------------------------------------
# Main flow
# -----------------------------------------------------------------------------

def main() -> None:
    token = load_env_value("GITHUB_TOKEN")
    if not token:
        raise SystemExit(
            "GITHUB_TOKEN was not found. Put it in your shell environment or .env file."
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    url = f"{BASE_URL}/repos/{OWNER}/{REPO}/issues"
    params = {
        "state": STATE,
        "sort": SORT,
        "direction": DIRECTION,
        "per_page": PER_PAGE,
        "page": PAGE,
    }
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    print("Fetching one page from the GitHub REST API...")
    print(f"Repository: {OWNER}/{REPO}")
    print(f"Endpoint:   {url}")
    print(f"Params:     {params}")

    response = requests.get(url, headers=headers, params=params, timeout=30)

    print("\nHTTP / API info")
    print("-" * 80)
    print(f"Status code:              {response.status_code}")
    print(f"Requested URL:            {response.url}")
    print(f"Rate limit limit:         {response.headers.get('X-RateLimit-Limit')}")
    print(f"Rate limit remaining:     {response.headers.get('X-RateLimit-Remaining')}")
    print(f"Rate limit reset (epoch): {response.headers.get('X-RateLimit-Reset')}")
    print(f"Link header present:      {'Link' in response.headers}")

    if response.status_code >= 400:
        print("\nGitHub returned an error response:")
        print(response.text)
        raise SystemExit("Request failed. Fix the token, permissions, or request params.")

    payload = response.json()
    if not isinstance(payload, list):
        raise SystemExit("Expected a list of issues, but got a different response shape.")

    extracted_at_utc = datetime.now(timezone.utc).isoformat()
    relevant_rows = [shape_issue(issue, extracted_at_utc) for issue in payload]

    next_page_url = response.links.get("next", {}).get("url")
    last_page_url = response.links.get("last", {}).get("url")
    pull_request_rows = sum(1 for row in relevant_rows if row["is_pull_request"])

    meta = {
        "repo_owner": OWNER,
        "repo_name": REPO,
        "extracted_at_utc": extracted_at_utc,
        "endpoint": url,
        "request_params": params,
        "requested_url": response.url,
        "status_code": response.status_code,
        "rows_returned": len(payload),
        "pull_request_rows_in_issues_endpoint": pull_request_rows,
        "has_next_page": bool(next_page_url),
        "next_page_url": next_page_url,
        "last_page_url": last_page_url,
        "rate_limit_limit": response.headers.get("X-RateLimit-Limit"),
        "rate_limit_remaining": response.headers.get("X-RateLimit-Remaining"),
        "rate_limit_reset_epoch": response.headers.get("X-RateLimit-Reset"),
        "link_header": response.headers.get("Link"),
    }

    RAW_JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    RELEVANT_JSON_PATH.write_text(json.dumps(relevant_rows, indent=2), encoding="utf-8")
    META_JSON_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("\nSaved files")
    print("-" * 80)
    print(f"Raw API page:             {RAW_JSON_PATH}")
    print(f"Relevant snapshot:        {RELEVANT_JSON_PATH}")
    print(f"Request / pagination meta:{META_JSON_PATH}")

    print("\nQuick sanity check")
    print("-" * 80)
    print(f"Rows fetched:             {len(relevant_rows)}")
    print(f"Rows that are PRs:        {pull_request_rows}")
    print(f"Has next page:            {bool(next_page_url)}")

    if relevant_rows:
        first = relevant_rows[0]
        print("\nFirst record preview")
        print("-" * 80)
        print(json.dumps(first, indent=2)[:1500])

    print(
        "\nDone. Next step: open DuckDB CLI and load the curated JSON snapshot."
    )


if __name__ == "__main__":
    main()


# =============================================================================
# DUCKDB CLI NOTES
# =============================================================================
# Open DuckDB in your terminal:
#
#   duckdb demo.duckdb
#
# Then paste the commands below one by one.
#
# Why use the CURATED JSON file first?
# - It is smaller and easier to explain than the full raw payload.
# - It already shows the field-selection mindset of a pipeline.
# - It still keeps arrays like labels and assignees so you can talk about
#   flattening later in the project.
#
# -----------------------------------------------------------------------------
# 1) Load the curated JSON into a table
# -----------------------------------------------------------------------------
# CREATE OR REPLACE TABLE github_issues_page_1 AS
# SELECT *
# FROM read_json_auto('demo_output/issues_page_1_relevant.json');
#
# -----------------------------------------------------------------------------
# 2) Look at the schema
# -----------------------------------------------------------------------------
# DESCRIBE github_issues_page_1;
#
# -----------------------------------------------------------------------------
# 3) Preview the data
# -----------------------------------------------------------------------------
# SELECT issue_number, title, state, created_at, updated_at, is_pull_request
# FROM github_issues_page_1
# ORDER BY updated_at DESC
# LIMIT 10;
#
# -----------------------------------------------------------------------------
# 4) Filter out pull requests because the Issues endpoint can return both
# -----------------------------------------------------------------------------
# SELECT COUNT(*) AS true_issue_rows
# FROM github_issues_page_1
# WHERE is_pull_request = false;
#
# -----------------------------------------------------------------------------
# 5) Create one simple view that feels like a silver/gold stepping stone
# -----------------------------------------------------------------------------
# CREATE OR REPLACE VIEW v_open_backlog_snapshot AS
# SELECT
#     issue_id,
#     issue_number,
#     title,
#     state,
#     created_at,
#     updated_at,
#     comment_count,
#     label_names,
#     assignee_logins,
#     author_login,
#     html_url
# FROM github_issues_page_1
# WHERE is_pull_request = false
#   AND state = 'open';
#
# -----------------------------------------------------------------------------
# 6) Query the view
# -----------------------------------------------------------------------------
# SELECT *
# FROM v_open_backlog_snapshot
# ORDER BY updated_at DESC
# LIMIT 10;
#
# -----------------------------------------------------------------------------
# 7) Write the table to Parquet
# -----------------------------------------------------------------------------
# COPY github_issues_page_1
# TO 'demo_output/issues_page_1_relevant.parquet'
# (FORMAT PARQUET);
#
# -----------------------------------------------------------------------------
# 8) Read the Parquet back
# -----------------------------------------------------------------------------
# SELECT COUNT(*)
# FROM read_parquet('demo_output/issues_page_1_relevant.parquet');
#
# -----------------------------------------------------------------------------
# 9) Optional: show a tiny aggregate that feels like "analytics"
# -----------------------------------------------------------------------------
# SELECT state, COUNT(*) AS issue_count
# FROM github_issues_page_1
# WHERE is_pull_request = false
# GROUP BY 1
# ORDER BY 2 DESC;
#
# Teaching message for the audience:
# - Ask a question.
# - Find the endpoint that can answer it.
# - Keep the fields you actually need.
# - Save the raw extract.
# - Shape it into a cleaner target dataset.
# - Load it into an analytical engine.
# - Turn it into a reusable table/view/parquet asset.
