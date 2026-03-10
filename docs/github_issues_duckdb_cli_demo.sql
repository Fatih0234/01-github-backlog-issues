-- Minimal DuckDB CLI demo for the JSON snapshot produced by
-- github_issues_api_to_json_demo.py

CREATE OR REPLACE TABLE github_issues_page_1 AS
SELECT *
FROM read_json_auto('demo_output/issues_page_1_relevant.json');

DESCRIBE github_issues_page_1;

SELECT issue_number, title, state, created_at, updated_at, is_pull_request
FROM github_issues_page_1
ORDER BY updated_at DESC
LIMIT 10;

SELECT COUNT(*) AS true_issue_rows
FROM github_issues_page_1
WHERE is_pull_request = false;

CREATE OR REPLACE VIEW v_open_backlog_snapshot AS
SELECT
    issue_id,
    issue_number,
    title,
    state,
    created_at,
    updated_at,
    comment_count,
    label_names,
    assignee_logins,
    author_login,
    html_url
FROM github_issues_page_1
WHERE is_pull_request = false
  AND state = 'open';

SELECT *
FROM v_open_backlog_snapshot
ORDER BY updated_at DESC
LIMIT 10;

COPY github_issues_page_1
TO 'demo_output/issues_page_1_relevant.parquet'
(FORMAT PARQUET);

SELECT COUNT(*)
FROM read_parquet('demo_output/issues_page_1_relevant.parquet');

SELECT state, COUNT(*) AS issue_count
FROM github_issues_page_1
WHERE is_pull_request = false
GROUP BY 1
ORDER BY 2 DESC;
