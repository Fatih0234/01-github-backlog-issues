# V1 Dashboard PRD: Open Issue Backlog

## Product Summary
Build a single-repo dashboard that helps a maintainer understand whether issue backlog pressure is rising or falling, what is driving the change, and where attention is needed first.

This dashboard is not a generic repo scorecard. Its first job is operational clarity: intake versus resolution, backlog direction, and signals that suggest triage debt, cleanup bursts, or instability.

## Primary User
Repo maintainer, triage lead, or active contributor responsible for keeping issue flow under control.

## Secondary User
Developer evaluating whether a project appears responsive and manageable before adopting or contributing.

## User Problem
GitHub shows current state well enough, but it is weak at explaining change over time. A maintainer can see open issues today, but not quickly answer:

- Are we keeping up with new issues?
- Is backlog growth concentrated in a few bad periods?
- Are we closing fresh issues quickly, or only cleaning up old debt?
- Are closures steady, or do they happen in bursts?

## Job To Be Done
When I check repo health, I want to see whether incoming issue load is outpacing resolution and why, so I can decide where to intervene.

## V1 Product Goal
Let a maintainer understand backlog pressure in under 2 minutes.

## Core User Questions
- Is the open issue backlog growing or shrinking over time?
- Are issues being closed as fast as they are opened?
- Which days or weeks caused the biggest backlog swings?
- Are closed issues mostly recent work or old backlog cleanup?
- Does issue flow follow a visible weekly rhythm?

## Dashboard Narrative
The dashboard should read from top to bottom like an investigation:

1. What is the backlog direction?
2. Is intake outrunning resolution right now?
3. Which spikes changed the story?
4. Is the team processing new work or old debt?
5. Are there timing patterns worth acting on?

## V1 Components

### 1. Header Summary
Purpose: orient the user immediately.

Show:
- Repo name
- Date window
- Current derived backlog change in window
- Total issues opened in window
- Total issues closed in window
- Net change in window

### 2. Backlog Direction Chart
Purpose: show whether backlog pressure is accumulating or easing.

Primary chart:
- Cumulative net backlog change over time
- Formula: cumulative sum of `opened - closed`

Why it matters:
- Gives the fastest answer to the main question.
- Makes sustained drift obvious even when daily counts are noisy.

### 3. Intake vs Resolution Panel
Purpose: compare incoming work to completed work in the same frame.

Primary chart:
- 7-day opened vs 7-day closed

Companion chart:
- 7-day backlog velocity

Why it matters:
- Separates temporary noise from real operational imbalance.
- Shows whether the team is catching up or falling behind in recent periods.

### 4. Backlog Swing Detector
Purpose: identify the days that most changed the backlog story.

Show:
- Top positive net-change days
- Top negative net-change days

Why it matters:
- Helps the maintainer investigate what happened on unusual days.
- Creates curiosity and encourages drilldown into release periods, incidents, or cleanup sweeps.

### 5. Closure Age Panel
Purpose: distinguish fresh resolution from old-backlog cleanup.

Show:
- Median age at close by month
- P90 age at close by month
- Share closed within 7 days
- Share closed after 90+ days

Why it matters:
- Prevents misleading interpretation of “high closure volume.”
- A repo can close many issues while still avoiding older debt.

### 6. Weekly Rhythm Panel
Purpose: reveal timing asymmetry between issue creation and issue closure.

Show:
- Opened by weekday
- Closed by weekday

Why it matters:
- Shows whether maintainers process issues in predictable bursts.
- Can hint at staffing patterns, triage habits, or delayed responses around weekends and release cycles.

## V1 Filters
- Repo: single repo only in V1, but keep the layout filter-ready
- Date range
- Optional issue label group if already available in normalized data

## Key Metrics Definitions
- Opened: true issues created on a date
- Closed: true issues closed on a date
- Net change: `opened - closed`
- Backlog direction: cumulative `net change`
- Closure age: `closed_at - created_at`

All metrics should explicitly exclude pull requests.

## Data Sources
- Primary source: issue lifecycle fields from issue records
- Required fields: `id`, `number`, `created_at`, `closed_at`, `state`, labels, author
- Supporting checks: search endpoints for quick validation only

Do not use `open_issues_count` as the source of truth for historical trend.

## V1 Success Criteria
- A maintainer can identify whether backlog is worsening or improving within 2 minutes.
- A maintainer can point to at least one concrete period that explains the trend.
- A maintainer can tell whether closures reflect fast turnaround or old-debt cleanup.

## Non-Goals
- Global repo health score
- Multi-repo comparisons
- Timeline-event-heavy modeling for every issue
- Perfect causal explanation of every backlog movement
- Public-facing executive dashboard

## Risks And Caveats
- Reopened issues can distort first-pass backlog logic if modeled only with `created_at` and `closed_at`.
- Search APIs are rate-limited and useful for validation, not durable analytics.
- Closure counts alone can be misleading without age-at-close context.

## Recommended V1 Design Principle
Every section should answer a maintainer decision question. If a chart is interesting but does not help explain pressure, velocity, or cleanup behavior, it should not be in V1.