````markdown id="g7p4nx"
# GitPulse Ingestion Spec
## GitHub Issues → Bronze / Silver / Gold pipeline with Airflow

## 1. Goal

Build a reliable ingestion pipeline for the GitPulse V1 dashboard that answers one operational question:

> Is the open issue backlog for a GitHub repository growing or shrinking over the last 24 months?

The ingestion design must support:
- one historical backfill,
- safe incremental refreshes,
- Airflow retries and backfills,
- Metabase-ready marts,
- protection against GitHub API pitfalls like pagination, rate limits, and PR contamination.

GitHub’s issues endpoint is appropriate for this because it exposes `created_at`, `closed_at`, `updated_at`, `state`, labels, assignees, and the `pull_request` marker, but GitHub also explicitly notes that issues endpoints may return pull requests and that PR rows must be identified via the `pull_request` key. :contentReference[oaicite:0]{index=0}

---

## 2. Scope and time horizon

## Analytics scope
- Default dashboard lens: **last 730 days**
- Optional filters later: 90d, 180d, 365d, 730d, all-time

## Ingestion scope
- Initial backfill: **730 days of issue history**
- Incremental sync: **updated issues**, not just newly created issues
- Reconciliation: periodic wider-window re-pull

This split is important because the product is about current backlog pressure, not the repo’s entire lifetime history.

---

## 3. Core ingestion principles

## Principle 1: Use `updated_at` as the extraction watermark
For this project, `created_at` and `closed_at` are analytic dates. They are **not** the right extraction watermark.

Why:
- an old issue can close today,
- labels can change later,
- assignees can change later,
- an issue can be reopened,
- edits can update the issue without changing `created_at`.

GitHub’s issues API supports a `since` parameter based on update time, which makes it suitable for change-based extraction. :contentReference[oaicite:1]{index=1}

**Rule**
- Extraction watermark = `updated_at`
- Analytical lifecycle dates = `created_at`, `closed_at`

---

## Principle 2: Make all tasks idempotent
Airflow retries, reruns, and backfills are normal, and Airflow’s docs emphasize backfill support and idempotent retry behavior. Airflow also notes that XComs are cleared on retry to help make task runs idempotent. :contentReference[oaicite:2]{index=2}

**Rule**
Every task must be safe to run multiple times without producing duplicate final state.

That means:
- raw files are append-only but uniquely named,
- silver tables are merged/upserted by stable keys,
- gold marts are rebuilt deterministically from silver,
- watermark state only advances after a successful load.

---

## Principle 3: Follow pagination correctly
GitHub paginates REST responses and recommends following the `Link` header rather than assuming one page is enough. :contentReference[oaicite:3]{index=3}

**Rule**
- Request with `per_page=100`
- Traverse until `rel="next"` disappears from the `Link` header
- Never assume page 1 is representative

---

## Principle 4: Always filter out pull requests
GitHub documents that issues endpoints may return both issues and pull requests, and PR rows are identified by `pull_request`. :contentReference[oaicite:4]{index=4}

**Rule**
- Keep the raw rows
- In silver/gold, define `is_pull_request`
- Exclude PR rows from all GitPulse issue-backlog marts

---

## Principle 5: Use authenticated requests
GitHub’s REST API rate limit is **60 requests/hour** for unauthenticated requests and much higher for authenticated requests, typically **5,000 requests/hour** for authenticated users. :contentReference[oaicite:5]{index=5}

**Rule**
- Use `GITHUB_TOKEN`
- Pass token in the Authorization header
- Log rate-limit headers for observability

---

## Principle 6: Avoid aggressive concurrency
GitHub recommends avoiding concurrent requests and handling rate limits carefully. :contentReference[oaicite:6]{index=6}

**Rule**
- Fetch pages serially
- Add backoff on 403/429/5xx
- Keep the DAG boring and predictable

---

## 4. Recommended pipeline architecture

```text
GitHub REST API
    ↓
Bronze: raw API page payloads + extraction metadata
    ↓
Silver: canonical issue-current model + normalized child tables
    ↓
Gold: dashboard marts for Metabase
    ↓
Metabase dashboard cards
````

---

## 5. Bronze layer design

## 5.1 Purpose

Bronze is your audit trail. It should preserve what GitHub returned, page by page, with enough metadata to debug any future mismatch.

## 5.2 Storage pattern

Store one raw file per page per run, for example:

```text
bronze/github/issues/
  repo_owner=apache/
  repo_name=airflow/
  extraction_date=2026-03-08/
  run_id=manual__2026-03-08T10-00-00Z/
  page_001.json
  page_002.json
  ...
```

If you also store a manifest file, that is even better:

```text
manifest.json
```

with request metadata and page counts.

## 5.3 Bronze table: `bronze_github_issue_pages`

**Grain:** one row per API page response

### Columns

* `ingestion_run_id`
* `repo_owner`
* `repo_name`
* `request_url`
* `request_params_json`
* `response_status_code`
* `response_headers_json`
* `page_number`
* `page_item_count`
* `fetched_at`
* `raw_payload_json`
* `etag` nullable
* `link_header` nullable

## Why this matters

When a dashboard number looks wrong, bronze lets you answer:

* what exactly did GitHub return?
* how many pages did we fetch?
* did rate limiting happen?
* did pagination stop early?

---

## 6. Silver layer design

Silver is where the raw page payloads become reusable relational assets.

## 6.1 Main table: `silver_github_issue_current`

**Grain:** one row per GitHub issue resource id

### Columns

* `issue_id`
* `issue_node_id`
* `repo_owner`
* `repo_name`
* `issue_number`
* `title`
* `state`
* `state_reason`
* `is_locked`
* `active_lock_reason`
* `created_at`
* `updated_at`
* `closed_at`
* `html_url`
* `api_url`
* `comments_url`
* `events_url`
* `author_login`
* `author_id`
* `author_type`
* `author_association`
* `assignee_count`
* `comment_count`
* `milestone_title`
* `closed_by_login`
* `performed_via_github_app_name`
* `is_pull_request`
* `pull_request_url`
* `pull_request_html_url`
* `body`
* `raw_issue_json`
* `last_seen_ingestion_run_id`
* `record_loaded_at`

## Rules

* merge/upsert on `issue_id`
* keep the latest version by source `updated_at`
* if duplicate versions exist in one load, keep the latest fetched record

## 6.2 Child table: `silver_github_issue_labels`

**Grain:** one row per issue-label pair

### Columns

* `issue_id`
* `label_id`
* `label_name`
* `label_description`
* `label_color`
* `label_is_default`
* `record_loaded_at`

## 6.3 Child table: `silver_github_issue_assignees`

**Grain:** one row per issue-assignee pair

### Columns

* `issue_id`
* `assignee_login`
* `assignee_id`
* `assignee_type`
* `record_loaded_at`

## 6.4 Optional state table: `silver_github_issue_sync_state`

**Grain:** one row per repo + endpoint

### Columns

* `repo_owner`
* `repo_name`
* `endpoint_name`
* `last_successful_watermark_updated_at`
* `last_successful_run_id`
* `last_successful_run_finished_at`

This table is critical because it keeps extraction state separate from the actual issue rows.

---

## 7. Gold layer design

Gold is built for the Metabase dashboard. These tables should already reflect GitPulse’s semantics.

## 7.1 `mart_issue_lifecycle`

**Grain:** one row per true issue

### Source

`silver_github_issue_current`

### Filters

* `is_pull_request = false`

### Columns

* `repo_owner`
* `repo_name`
* `issue_id`
* `issue_number`
* `title`
* `html_url`
* `author_login`
* `author_association`
* `state`
* `state_reason`
* `created_at`
* `created_date`
* `closed_at`
* `closed_date`
* `updated_at`
* `is_open`
* `is_closed`
* `closure_age_days`
* `comment_count`
* `milestone_title`
* `closed_by_login`
* `label_names`
* `label_group`
* `assignee_logins`
* `loaded_at`

This is the main issue detail table for Metabase.

---

## 7.2 `mart_issue_daily_flow`

**Grain:** one row per repo per calendar date

### Columns

* `repo_owner`
* `repo_name`
* `calendar_date`
* `opened_count`
* `closed_count`
* `net_change`
* `cumulative_net_change`
* `rolling_7d_opened`
* `rolling_7d_closed`
* `rolling_7d_net_change`

### Logic

* `opened_count` from `created_date`
* `closed_count` from `closed_date`
* `net_change = opened_count - closed_count`
* cumulative and rolling windows precomputed here

This powers the hero chart and intake vs resolution cards.

---

## 7.3 `mart_issue_closure_age_monthly`

**Grain:** one row per repo per month

### Columns

* `repo_owner`
* `repo_name`
* `year_month`
* `closed_issue_count`
* `median_closure_age_days`
* `p90_closure_age_days`
* `share_closed_within_7d`
* `share_closed_after_90d`

This powers the closure-age section of the dashboard.

---

## 7.4 `mart_issue_weekday_rhythm`

**Grain:** one row per repo per weekday and event type

### Columns

* `repo_owner`
* `repo_name`
* `weekday_num`
* `weekday_name`
* `event_type`
* `issue_count`

Where:

* `event_type in ('opened', 'closed')`

---

## 7.5 `mart_issue_swing_days`

**Grain:** one row per repo per date

### Columns

* `repo_owner`
* `repo_name`
* `calendar_date`
* `opened_count`
* `closed_count`
* `net_change`
* `positive_swing_rank`
* `negative_swing_rank`

This makes the “worst intake-pressure days” and “largest cleanup bursts” easy to surface in Metabase.

---

## 8. Watermark strategy

## 8.1 Watermark choice

Use:

* `updated_at`

Do not use:

* `created_at`
* `closed_at`

because they are not reliable change-detection fields for recurring syncs.

## 8.2 Incremental extraction rule

For every recurring sync:

```text
extract issues where updated_at >= (last_successful_watermark - lookback_window)
```

## 8.3 Recommended lookback window

For a weekly DAG:

* use **30 days** as the default overlap window

For a daily DAG:

* use **14 days**

This overlap absorbs:

* retries,
* delayed source updates,
* timing edges,
* state changes to older issues.

## 8.4 Watermark advancement rule

Advance `last_successful_watermark_updated_at` only after:

1. extraction completed,
2. bronze write completed,
3. silver merge completed,
4. data quality checks passed.

Never advance the watermark immediately after the API call.

---

## 9. Airflow DAG design

## DAG name

`github_issues_ingestion_v1`

## Recommended schedule

* Weekly, for example every Sunday morning
* Separate ad hoc DAG run or parameterized backfill mode for initial history

Airflow supports recurring schedules and explicit backfill runs for past dates. ([Apache Airflow][1])

## DAG parameters

* `repo_owner`
* `repo_name`
* `mode` = `backfill` or `incremental`
* `start_date_override` nullable
* `end_date_override` nullable
* `lookback_days`
* `force_full_resync` boolean

## Task flow

### 1. `resolve_run_window`

Determine the extraction window:

* if `mode=backfill`, use the supplied historical range
* if `mode=incremental`, read watermark and subtract overlap window

**Output**

* `window_start_updated_at`
* `window_end_updated_at`

### 2. `fetch_issue_pages`

Call:

```http
GET /repos/{owner}/{repo}/issues?state=all&sort=updated&direction=asc&per_page=100&since=...
```

Process:

* authenticated request with `GITHUB_TOKEN`
* follow `Link` header
* serial pagination
* capture response headers including rate-limit headers
* write each page to bronze

GitHub documents the issues endpoint parameters and Link-header pagination behavior. ([GitHub Docs][2])

### 3. `load_bronze_manifest`

Insert page-level metadata into `bronze_github_issue_pages`.

### 4. `flatten_issue_rows`

Explode raw page JSON into row-level staging data.

### 5. `merge_silver_issue_current`

Upsert into `silver_github_issue_current` keyed by `issue_id`.

### 6. `merge_silver_issue_labels`

Upsert issue-label pairs.

### 7. `merge_silver_issue_assignees`

Upsert issue-assignee pairs.

### 8. `run_data_quality_checks`

Run validations such as:

* no duplicate `issue_id` in silver current
* `updated_at >= created_at`
* `closed_at >= created_at` when present
* bronze page count > 0 when results expected
* no null `issue_id`
* issue rows contain valid repo owner/name

### 9. `build_gold_marts`

Rebuild or incrementally refresh:

* `mart_issue_lifecycle`
* `mart_issue_daily_flow`
* `mart_issue_closure_age_monthly`
* `mart_issue_weekday_rhythm`
* `mart_issue_swing_days`

### 10. `advance_watermark`

Write the new successful watermark only after all previous tasks pass.

### 11. `emit_run_summary`

Store metrics:

* total pages fetched
* total issue rows processed
* total PR rows seen
* final watermark
* rate-limit remaining
* duration

---

## 10. Backfill strategy

## Initial backfill

For the first historical load:

* run a backfill covering the last 730 days
* sort ascending by updated time for easier checkpoint reasoning
* allow reruns without duplication

Airflow explicitly supports backfill creation for past dates. ([Apache Airflow][1])

## Monthly reconciliation

Even after incremental syncs are running, do one of these:

* monthly re-pull last 180 days, or
* quarterly re-pull full 730-day supported window

This helps correct drift from:

* older issues updated outside expected ranges,
* bugs in incremental logic,
* missed pages or temporary failures.

---

## 11. Failure modes and how the DAG should handle them

## 11.1 Rate-limit pressure

GitHub exposes REST API rate limits and recommends careful use. ([GitHub Docs][3])

### Mitigation

* use token auth
* log `X-RateLimit-Remaining`
* sleep/backoff if low
* keep page fetches serial
* avoid parallel issue-page extraction

## 11.2 Partial pagination failure

Example:

* pages 1–7 fetched successfully
* page 8 fails

### Mitigation

* bronze writes page by page
* do not advance watermark
* rerun safely from the same window
* silver merge remains idempotent

## 11.3 Schema surprises

GitHub may add fields or change nullable patterns.

### Mitigation

* keep `raw_issue_json`
* select only supported fields into silver columns
* monitor for parsing failures
* avoid making the DAG brittle on unknown keys

## 11.4 Duplicate rows due to overlap windows

Overlap is intentional, so duplicate source rows across runs are expected.

### Mitigation

* merge/upsert by `issue_id`
* keep latest source version by `updated_at`
* do not append blindly to silver current

## 11.5 PR contamination

GitHub issue responses include pull requests in issue endpoints. ([GitHub Docs][2])

### Mitigation

* create `is_pull_request`
* keep them in raw/silver for QA
* exclude from all GitPulse backlog marts

---

## 12. Data quality checks

The pipeline should fail fast when the contract is violated.

## Bronze checks

* response status is 200
* page payload is valid JSON array
* page item count matches parsed items
* pagination chain is complete

## Silver checks

* `issue_id` unique
* `issue_number` not null
* `created_at` not null
* `updated_at` not null
* `closed_at >= created_at` when present
* `repo_owner` and `repo_name` not null

## Gold checks

* `opened_count >= 0`
* `closed_count >= 0`
* `net_change = opened_count - closed_count`
* cumulative values behave deterministically
* no PR rows in `mart_issue_lifecycle`

---

## 13. Secrets and config handling in Airflow

Use:

* `GITHUB_TOKEN` from environment variables or Airflow connections/secrets backend
* repo parameters as DAG params or environment-backed config

Airflow recommends using environment variables rather than top-level Airflow Variable lookups in DAG code to avoid unnecessary metadata DB load during parsing. ([Apache Airflow][4])

**Rule**

* do not hardcode tokens
* do not fetch Airflow Variables at import time
* keep DAG parsing lightweight

---

## 14. Recommended request pattern

## Historical backfill request shape

```http
GET /repos/{owner}/{repo}/issues?state=all&sort=updated&direction=asc&per_page=100&since={window_start}
Accept: application/vnd.github+json
Authorization: Bearer ${GITHUB_TOKEN}
X-GitHub-Api-Version: 2022-11-28
User-Agent: gitpulse
```

## Why sort by `updated`

Because the extraction window is update-based, sorting by `updated` makes the paging and checkpoint logic easier to reason about.

---

## 15. Why this design fits the Metabase dashboard

Metabase works best when the warehouse already exposes question-friendly tables. This ingestion spec supports that by separating concerns:

* Bronze preserves source truth
* Silver creates a stable issue-level contract
* Gold precomputes dashboard metrics
* Metabase only needs simple SQL per card

That lets the dashboard remain simple:

* hero chart from `mart_issue_daily_flow`
* closure-age card from `mart_issue_closure_age_monthly`
* weekly rhythm from `mart_issue_weekday_rhythm`
* issue drilldown from `mart_issue_lifecycle`

---

## 16. Recommended V1 implementation order

## Phase 1

Build:

* bronze raw page landing
* silver current issue table
* PR filter logic
* manual backfill run for one repo

## Phase 2

Add:

* watermark state table
* incremental weekly sync
* label and assignee child tables
* quality checks

## Phase 3

Build gold marts:

* daily flow
* closure age
* weekday rhythm
* swing days
* issue detail table

## Phase 4

Hook into Metabase:

* save questions/cards
* wire date filter
* validate dashboard numbers against GitHub UI samples

---

## 17. Final engineering message

We are not building a “new issues loader.”

We are building a **GitHub issue state ingestion pipeline** for backlog analytics.

That means:

* extraction must track changed issue state via `updated_at`,
* PR rows must be detected and excluded from backlog marts,
* pagination and rate limits must be handled correctly,
* Airflow retries and backfills must be safe,
* the warehouse contract must support a narrow Metabase dashboard focused on backlog pressure.

If the data team builds this contract, the dashboard product becomes straightforward.

---

## 18. Bottom line

For GitPulse V1, the right ingestion approach is:

* **historical backfill** for the last 730 days,
* **weekly authenticated incremental sync** using `updated_at`,
* **30-day overlap window** for safety,
* **idempotent Airflow tasks**,
* **raw bronze preservation**,
* **silver upserted issue-current model**,
* **gold Metabase marts** built from true issues only.

That gives you a pipeline that is small enough for a strong YouTube project, but principled enough to feel like real data engineering. ([GitHub Docs][2])

```
::contentReference[oaicite:14]{index=14}
```

[1]: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html?utm_source=chatgpt.com "Backfill — Airflow 3.1.7 Documentation"
[2]: https://docs.github.com/rest/issues/issues?utm_source=chatgpt.com "REST API endpoints for issues"
[3]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?utm_source=chatgpt.com "Rate limits for the REST API"
[4]: https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html?utm_source=chatgpt.com "Dynamic Dag Generation — Airflow 3.1.7 Documentation"
