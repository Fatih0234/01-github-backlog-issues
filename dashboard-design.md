# GitPulse V1 Dashboard Design Memo
## Single-use-case dashboard design and gold/mart contract for Metabase

## 1. Product decision: narrow the story

The PRD is broad by design, but for the actual project and YouTube build story, the product should be narrowed to **one concrete maintainer decision**:

> **Use case:** “As a repo maintainer or triage lead, I want to know in under 2 minutes whether my issue backlog is getting worse or better in the recent period, and whether that change comes from intake pressure, cleanup bursts, or slow resolution of older work.”

That keeps the dashboard focused on the PRD’s main promise: **operational clarity about backlog pressure**, not generic repository health. The PRD already emphasizes that the dashboard should answer five investigation questions in order: backlog direction, intake vs resolution, swing periods, fresh vs old-debt closure, and weekly timing patterns. :contentReference[oaicite:0]{index=0}

So the dashboard should be framed as:

# **Dashboard title**
**GitPulse — Open Issue Backlog Monitor**

# **Primary audience**
- maintainer
- triage lead
- active contributor responsible for issue flow

# **Primary job**
- decide whether the repo needs intervention now
- decide whether the problem is intake pressure, stale debt, or bursty cleanup behavior

# **What this dashboard is not**
- not a global repo score
- not a contributor leaderboard
- not a full workflow event audit
- not a multi-repo comparison surface :contentReference[oaicite:1]{index=1}

---

## 2. Why this design works well in Metabase

Metabase dashboards are composed of **questions/cards** that can be arranged on a dashboard, and dashboard filters can be connected across multiple questions. This fits your use case very well because each section of the PRD can become a saved question with a clear SQL definition, then placed as a card in a top-to-bottom narrative layout. Metabase also supports text/header cards for narrative guidance, date and category filters, tables, line charts, bar charts, and dashboard interactivity like clicking a chart to update a filter. :contentReference[oaicite:2]{index=2}

That means the data engineering team should build the mart so that:
- each dashboard card maps cleanly to one SQL query,
- date and label filters can be applied consistently,
- most cards can read from narrow, dashboard-ready mart tables instead of doing heavy logic in Metabase,
- drilldowns use issue-level detail tables.

---

## 3. Final V1 dashboard concept

## Dashboard name
**GitPulse — Is the issue backlog growing or shrinking?**

## One-sentence product promise
A single-repo Metabase dashboard that lets a maintainer understand backlog pressure in under two minutes by showing whether issue intake is outrunning issue resolution, when the backlog changed most, and whether closures reflect fresh work or old-debt cleanup. :contentReference[oaicite:3]{index=3}

## Date scope
Default: **last 180 days**  
Why:
- long enough to show drift and cleanup periods
- short enough to remain legible in Metabase line and bar charts
- good for video storytelling

## V1 filters
Placed in the dashboard header row:
1. **Date range** — required
2. **Optional label group** — only if normalized labels are ready
3. **Repo selector placeholder** — hidden or fixed in V1 since PRD is single-repo, but the schema should be repo-ready for later 

---

## 4. Dashboard layout blueprint

The PRD says the dashboard should read like an investigation from top to bottom. The layout below follows that exactly. :contentReference[oaicite:5]{index=5}

---

# Section A. Orientation strip

## A1. Text/Header card
**Placement:** very top, full width  
**Card type:** text/header card

**Purpose**  
Tell the user what this dashboard is for and how to read it.

**Suggested text**  
“This dashboard shows whether issue backlog pressure is rising or falling in the selected period. Read from left to right and top to bottom: backlog direction, intake vs resolution, major swing periods, closure age, and weekly rhythm.”

**Why it matters**  
Metabase supports text and header cards, so this is the easiest way to keep the dashboard narrative explicit instead of leaving the charts to speak alone. :contentReference[oaicite:6]{index=6}

---

## A2. Filter row
**Placement:** directly below the text card  
**Components:**
- Date range filter
- Optional label group filter

**Purpose**  
Give the user one shared context for the whole dashboard.

**Behavior**
- All cards except the repo title card should be wired to the date filter
- Label filter should only apply if label-group marts exist
- Issue detail tables should inherit both filters

Metabase supports dashboard filter widgets, including date and category/text filters, connected across multiple questions. :contentReference[oaicite:7]{index=7}

---

# Section B. Header summary row

## Layout
One row of **six KPI cards**.

### B1. Repo name
**Placement:** first card, leftmost  
**Shape:** small number/text card  
**Purpose:** orient the user to the single repo in scope

### B2. Date window
**Shape:** text card  
**Purpose:** show the selected date range explicitly

### B3. Opened in window
**Shape:** scalar KPI card  
**Definition:** count of true issues with `created_at` in filter window

### B4. Closed in window
**Shape:** scalar KPI card  
**Definition:** count of true issues with `closed_at` in filter window

### B5. Net change in window
**Shape:** scalar KPI card  
**Definition:** `opened - closed`

### B6. Current backlog direction signal
**Shape:** scalar KPI card with semantic coloring in Metabase  
**Definition:** one of:
- “Worsening” if net change > threshold
- “Improving” if net change < negative threshold
- “Flat” otherwise

**Why this row exists**  
The PRD’s first component is a header summary that immediately answers whether pressure increased or decreased in the selected window. :contentReference[oaicite:8]{index=8}

**Metabase note**  
These should be individual saved questions because Metabase dashboards work naturally with question cards. :contentReference[oaicite:9]{index=9}

---

# Section C. Backlog direction row

## C1. Backlog direction chart
**Placement:** full-width primary visual, directly under the KPI row  
**Shape:** line chart  
**Metric:** cumulative net backlog change over time  
**Formula:** cumulative sum of daily `(opened_count - closed_count)`

**Title**
**Backlog direction over time**

**Subtitle**
“Cumulative net change of true issues in selected window”

**Why it is the hero chart**
This is the fastest answer to the main product question. The PRD explicitly defines it as the primary chart because cumulative net change makes drift visible even when daily issue counts are noisy. :contentReference[oaicite:10]{index=10}

**Metabase visualization choice**
A line chart is the right fit because Metabase recommends line charts for trends over time. :contentReference[oaicite:11]{index=11}

**Interpretation rule shown in description**
- upward slope = backlog pressure increasing
- downward slope = backlog pressure easing
- flat line = intake roughly matches resolution

---

# Section D. Intake vs resolution row

## Layout
Two cards side by side.

## D1. 7-day opened vs 7-day closed
**Placement:** left half  
**Shape:** two-series line chart  
**Series:**
- rolling 7-day opened
- rolling 7-day closed

**Purpose**
Show whether current operating rhythm is balanced or not.

**Why this belongs**
The PRD calls for a “7-day opened vs 7-day closed” comparison to smooth daily noise and make recent imbalance obvious. :contentReference[oaicite:12]{index=12}

**Metabase note**
Metabase supports multi-series charts and line charts for time series. :contentReference[oaicite:13]{index=13}

## D2. 7-day backlog velocity
**Placement:** right half  
**Shape:** bar chart or line chart  
**Metric:** rolling 7-day `(opened - closed)`

**Purpose**
Give one compact “catching up vs falling behind” view.

**Recommended display**
Bar chart if you want positive vs negative periods to pop visually  
Line chart if you want continuity with the rest of the dashboard

**Decision**
Use **bar chart** in Metabase for V1 because positive and negative values are visually easier to distinguish for a maintainer.

---

# Section E. Backlog swing detector row

## Layout
Two cards side by side.

## E1. Top positive net-change days
**Placement:** left half  
**Shape:** table  
**Columns:**
- date
- opened_count
- closed_count
- net_change
- optional example issue titles count link

**Purpose**
Find the worst intake-pressure days or weeks.

## E2. Top negative net-change days
**Placement:** right half  
**Shape:** table  
**Columns:**
- date
- opened_count
- closed_count
- net_change
- optional example issue titles count link

**Purpose**
Find cleanup bursts or closure sweeps.

**Why tables instead of charts**
The PRD frames this panel as an investigation device. For that, ranking is more useful than another visual trend. A table gives immediate specificity and works well in Metabase, especially when row indices and sortable columns are enabled. 

**Optional Metabase interaction**
If label filtering is added later, clicking the date or label can update dashboard filters using Metabase cross-filtering behavior. :contentReference[oaicite:15]{index=15}

---

# Section F. Closure age row

## Layout
One wide chart plus two KPI cards or one wide chart plus one companion table.

## F1. Median and P90 closure age by month
**Placement:** wide left area  
**Shape:** two-series line chart  
**Series:**
- monthly median age at close
- monthly p90 age at close

**Purpose**
Tell whether closures are mostly fresh work or old debt.

**Why it matters**
The PRD explicitly says this prevents misleading interpretation of high closure volume. A team can close many issues while still ignoring old backlog. :contentReference[oaicite:16]{index=16}

## F2. Share closed within 7 days
**Placement:** right-side KPI card  
**Shape:** scalar

## F3. Share closed after 90+ days
**Placement:** right-side KPI card  
**Shape:** scalar

**Interpretation**
- high share within 7 days = good fresh-turnaround behavior
- high share after 90+ days = debt cleanup or stale-resolution behavior
- both can rise simultaneously during special cleanup periods

---

# Section G. Weekly rhythm row

## Layout
Two cards side by side.

## G1. Issues opened by weekday
**Placement:** left half  
**Shape:** bar chart  
**X-axis:** weekday  
**Y-axis:** opened issue count

## G2. Issues closed by weekday
**Placement:** right half  
**Shape:** bar chart  
**X-axis:** weekday  
**Y-axis:** closed issue count

**Purpose**
Reveal timing asymmetry and operational cadence.

**Why this is useful**
The PRD calls out staffing patterns, triage habits, delayed response around weekends, and burst processing as relevant insights. :contentReference[oaicite:17]{index=17}

**Metabase visualization choice**
Bar charts are the clearest way to compare weekday distributions. Metabase supports them directly. :contentReference[oaicite:18]{index=18}

---

# Section H. Drilldown row

## H1. Issue detail table
**Placement:** bottom, full width  
**Shape:** table  
**Columns:**
- issue_number
- title
- created_at
- closed_at
- state
- closure_age_days
- author_login
- label_group
- html_url

**Purpose**
Let the maintainer inspect concrete issues behind the patterns.

**Behavior**
- inherits dashboard date filter
- optionally inherits label group filter
- sorted by newest created date by default
- can be duplicated for “open issues only” if desired

**Why it belongs**
The dashboard needs one concrete bridge from summary to action. A detail table is the most practical Metabase artifact for that. Tables in Metabase also allow column formatting and click behavior, which is helpful for issue links. :contentReference[oaicite:19]{index=19}

---

## 5. Final recommended card order

1. Header text card  
2. Filters  
3. KPI summary row  
4. Hero backlog direction line chart  
5. 7-day opened vs closed  
6. 7-day backlog velocity  
7. Top positive swing days table  
8. Top negative swing days table  
9. Median and p90 closure age line chart  
10. Share closed within 7 days KPI  
11. Share closed after 90+ days KPI  
12. Opened by weekday  
13. Closed by weekday  
14. Issue detail table

This gives a clean narrative:
- **What is happening?**
- **Is it happening now?**
- **When did it change?**
- **What kind of closure behavior explains it?**
- **Is there an operational rhythm?**
- **Which issues are underneath?**

That sequence matches the PRD’s intended investigative reading order. :contentReference[oaicite:20]{index=20}

---

## 6. Data contract: what the gold/mart layer must provide

The data engineers should not be asked to let Metabase compute all logic directly from raw issue rows. The mart layer should expose **dashboard-ready facts and dimensions** so the Metabase cards stay simple, fast, and explainable.

The gold layer should support two types of use:
1. **aggregated dashboard cards**
2. **issue-level drilldown**

So the mart should be designed around a small set of canonical assets.

---

## 7. Recommended mart architecture

## 7.1 Core design principle

The mart layer should expose:
- one **issue-level canonical fact table**
- one **daily backlog fact table**
- one **monthly closure-age fact table**
- one **weekday rhythm fact table**
- optional dimensions for labels and repos

This keeps the dashboard stable and SQL-simple.

---

## 7.2 Required mart artifacts

# A. `mart_issue_lifecycle`
## Grain
**one row per true issue**

## Purpose
Canonical issue-level table for drilldown and reusable aggregations.

## Required columns
- `repo_id`
- `repo_owner`
- `repo_name`
- `issue_id`
- `issue_number`
- `title`
- `html_url`
- `author_login`
- `author_association`
- `state`
- `state_reason`
- `created_at`
- `created_date`
- `closed_at`
- `closed_date`
- `updated_at`
- `is_closed`
- `is_open`
- `closure_age_days`
- `is_pull_request`
- `label_names`
- `label_group`
- `milestone_title`
- `assignee_logins`
- `comment_count`
- `closed_by_login`
- `fetched_at`

## Rules
- **must exclude pull requests** from downstream dashboard tables
- keep `is_pull_request` anyway for QA
- `closure_age_days` only populated when `closed_at` exists
- `label_group` should be a normalized optional mapping, not raw GitHub labels only

## Why this table matters
This is the root truth table for all issue-based analytics in V1. The PRD’s required lifecycle fields are explicitly issue-level fields like `id`, `number`, `created_at`, `closed_at`, `state`, labels, and author. :contentReference[oaicite:21]{index=21}

---

# B. `mart_issue_daily_flow`
## Grain
**one row per repo per calendar date**

## Purpose
Power the hero trend card, intake-vs-resolution, and swing detector.

## Required columns
- `repo_id`
- `repo_owner`
- `repo_name`
- `calendar_date`
- `opened_count`
- `closed_count`
- `net_change`
- `cumulative_net_change`
- `rolling_7d_opened`
- `rolling_7d_closed`
- `rolling_7d_net_change`

## Logic
- `opened_count` = count of true issues where `created_date = calendar_date`
- `closed_count` = count of true issues where `closed_date = calendar_date`
- `net_change = opened_count - closed_count`
- `cumulative_net_change` computed over ordered dates within repo
- rolling 7-day fields computed in mart, not in Metabase, to keep card SQL simple

## Why it matters
This single table supports the most important V1 visuals with clean and lightweight Metabase queries.

---

# C. `mart_issue_closure_age_monthly`
## Grain
**one row per repo per month**

## Purpose
Power the closure age panel.

## Required columns
- `repo_id`
- `repo_owner`
- `repo_name`
- `year_month`
- `closed_issue_count`
- `median_closure_age_days`
- `p90_closure_age_days`
- `share_closed_within_7d`
- `share_closed_after_90d`

## Logic notes
- only closed true issues included
- use `closed_at - created_at`
- month should be grouped by `closed_date` month, not created month

## Why it matters
This makes the “fresh work vs old debt cleanup” story explicit and easy to query.

---

# D. `mart_issue_weekday_rhythm`
## Grain
**one row per repo per weekday and event type**

## Purpose
Power the weekly rhythm panel.

## Required columns
- `repo_id`
- `repo_owner`
- `repo_name`
- `weekday_num`
- `weekday_name`
- `event_type` (`opened` or `closed`)
- `issue_count`

## Why this table helps
This avoids repeating weekday extraction logic in multiple Metabase questions.

---

# E. `mart_issue_swing_days`
## Grain
**one row per repo per date**

## Purpose
Power the ranked swing tables.

## Required columns
- `repo_id`
- `repo_owner`
- `repo_name`
- `calendar_date`
- `opened_count`
- `closed_count`
- `net_change`
- `swing_rank_positive`
- `swing_rank_negative`

## Optional extra columns
- `sample_open_issue_numbers`
- `sample_closed_issue_numbers`

## Why separate this from daily flow?
It can be derived from daily flow, but having a saved mart or model for “top swing periods” keeps Metabase table logic trivial.

---

# F. Optional `dim_issue_label_group`
## Grain
**one row per raw label name**

## Purpose
Map noisy raw GitHub labels into stable analysis groups.

## Example columns
- `raw_label_name`
- `label_group`
- `label_group_sort_order`
- `is_active`

## Example groups
- bug
- feature
- docs
- maintenance
- triage
- infra
- other

## Why this matters
The PRD only says label group is optional if normalized data is available. This dimension is how that becomes manageable. :contentReference[oaicite:22]{index=22}

---

## 8. What the SQL in Metabase should and should not do

## Metabase should do:
- simple filtering
- lightweight grouping when necessary
- final card shaping
- drilldown rendering
- dashboard-level filter wiring

## Metabase should not do:
- raw deduplication
- pull request filtering logic repeated in every query
- rolling windows computed independently in each card
- percentile logic repeated in the dashboard
- label normalization logic

This separation is important because Metabase is question/card oriented. It works best when the underlying tables are already modeled around the questions the dashboard is trying to answer. :contentReference[oaicite:23]{index=23}

---

## 9. Suggested saved questions/cards for Metabase

Each of these should be a saved question in a collection, then assembled into the final dashboard.

1. `kpi_repo_name`
2. `kpi_date_window`
3. `kpi_opened_in_window`
4. `kpi_closed_in_window`
5. `kpi_net_change_in_window`
6. `kpi_backlog_direction_signal`
7. `ts_cumulative_backlog_direction`
8. `ts_7d_opened_vs_closed`
9. `ts_7d_backlog_velocity`
10. `tbl_top_positive_swing_days`
11. `tbl_top_negative_swing_days`
12. `ts_monthly_closure_age`
13. `kpi_share_closed_within_7d`
14. `kpi_share_closed_after_90d`
15. `bar_opened_by_weekday`
16. `bar_closed_by_weekday`
17. `tbl_issue_detail`

This naming convention makes the dashboard easy to maintain and easy to explain in a YouTube project walkthrough.

---

## 10. Recommended dashboard-to-mart mapping

| Dashboard component | Mart source |
|---|---|
| Repo name KPI | `mart_issue_lifecycle` or repo dimension |
| Opened / Closed / Net KPIs | `mart_issue_daily_flow` |
| Backlog direction chart | `mart_issue_daily_flow` |
| 7-day opened vs closed | `mart_issue_daily_flow` |
| 7-day backlog velocity | `mart_issue_daily_flow` |
| Positive/negative swing tables | `mart_issue_swing_days` |
| Closure age monthly chart | `mart_issue_closure_age_monthly` |
| Within 7d / After 90d KPIs | `mart_issue_closure_age_monthly` |
| Weekly rhythm charts | `mart_issue_weekday_rhythm` |
| Detail issue table | `mart_issue_lifecycle` |

---

## 11. Recommended engineering message to the data team

## Product request to engineering

We are building **one Metabase dashboard for one operational use case**: helping a maintainer quickly judge whether issue backlog pressure is rising or falling in a single GitHub repository, and why.

To support that dashboard reliably, the gold/mart layer should provide:

1. a canonical issue lifecycle fact table at issue grain,
2. a daily issue-flow fact table,
3. a monthly closure-age fact table,
4. a weekday rhythm fact table,
5. an optional label-group dimension.

All downstream dashboard metrics must:
- exclude pull requests,
- treat issue creation and issue closure as separate event dates,
- support dashboard-wide date filtering,
- preserve issue-level drilldown.

The marts should be optimized for **Metabase question/card construction**, meaning rolling windows, cumulative backlog direction, and age-at-close distributions should be precomputed in the gold layer wherever practical.

---

## 12. Recommended V1 scope boundary

To keep the project tight and YouTube-friendly, V1 should **not** include:
- reopened issue event modeling,
- PR analytics,
- contributor productivity views,
- repo comparison,
- advanced forecasting,
- causal “why exactly did this happen” event stitching.

That is still aligned with the PRD, which explicitly excludes perfect lifecycle explanation and event-heavy modeling for V1. :contentReference[oaicite:24]{index=24}

---

## 13. Final product statement

The final dashboard should feel like this:

> “In one screen, the maintainer can see whether issue pressure is rising, whether the team is keeping up recently, which periods changed the story, whether closures are fresh work or stale debt cleanup, and which issues deserve inspection next.”

That is the right narrow product for the PRD, the right shape for Metabase, and the right level of ambition for a clean data-engineering video project. 