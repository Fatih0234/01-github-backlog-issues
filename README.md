# GitPulse

GitHub issue backlog analytics pipeline. Answers one question: **is the open issue backlog growing or shrinking?**

Bronze → Silver → Gold → Metabase, all on your laptop.

![Architecture](docs/assets/architecture.png)

---

## Stack

| Layer | Tech |
|-------|------|
| Ingestion | Python 3.12 + GitHub REST API |
| Storage | MinIO (S3-compatible, local Docker) |
| Format | Parquet (hive-partitioned) |
| Transforms | DuckDB + httpfs |
| Orchestration | Apache Airflow 3.0 (LocalExecutor) |
| Dashboard | Metabase v0.57.6 + MotherDuck DuckDB driver v1.4.3.1 |
| Runtime | uv |
| Infra | Docker Compose |

---

## Quick start

### 1. Prerequisites

- Docker Desktop
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- A GitHub personal access token

### 2. Install dependencies

```bash
uv sync
```

### 3. Environment

```bash
cp .env.example .env
# edit .env and fill in GITHUB_TOKEN
```

Key variables:

```
GITHUB_TOKEN=ghp_...           # required
REPO_OWNER=apache              # default
REPO_NAME=airflow              # default
GITPULSE_STORAGE_BACKEND=s3    # s3 for MinIO/docker, local for temp-dir tests
GITPULSE_LOCAL_DATA_ROOT=...   # required only when storage backend is local
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=gitpulse
MINIO_SECRET_KEY=gitpulse
MINIO_BUCKET=gitpulse
DUCKDB_FILE=/tmp/gitpulse.duckdb
METABASE_URL=http://localhost:3000
METABASE_EMAIL=admin@gitpulse.com
METABASE_PASSWORD=password
```

### 4. Start infrastructure

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO console | http://localhost:9001 | `gitpulse` / `gitpulse` |
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| Metabase | http://localhost:3000 | set on first login |

Airflow takes ~30s to initialize. Wait until `airflow-init` exits (status `Exited (0)`) before triggering runs.

### 5. Run the pipeline manually (optional)

You can run each layer directly without Airflow:

```bash
# Bronze: fetch raw issues from GitHub → MinIO Parquet
uv run python -m gitpulse.bronze.extract_issues

# Silver: deduplicate + flatten → MinIO Parquet
uv run python -m gitpulse.silver.process_bronze_to_silver

# Data quality checks
uv run python -m gitpulse.dq.run_checks

# Gold: build dashboard marts → MinIO Parquet
uv run python -m gitpulse.gold.build_gold_marts
```

### 6. Trigger via Airflow

The DAG `github_issues_ingestion_v1` runs automatically every Sunday at 04:00 UTC. To trigger manually:

1. Open http://localhost:8080 and log in with `admin` / `admin`
2. Find `github_issues_ingestion_v1` and click the ▶ (Trigger DAG) button
3. Watch task states: `fetch_bronze → process_silver → run_dq → build_gold → validate_gold → emit_summary`

---

## Tests

Default tests are local-first and do not require MinIO, Airflow, or Metabase:

```bash
uv run --group dev pytest
```

The suite exercises Bronze → Silver → Gold end-to-end against temp local storage and
captures the regression cases that previously slipped through:

- run-scoped bronze manifests and watermark fail-closed behavior
- repo-scoped incremental silver merges
- stale label / assignee removal from current-state snapshots
- dense daily spines and true 7-day rolling windows
- DAG retry / timeout policy wiring

If you want live-storage checks against MinIO after the local suite passes:

```bash
uv run --group dev pytest -m integration
```

---

## MinIO bucket layout

```
s3://gitpulse/
  bronze/github/issues/
    repo_owner=apache/repo_name=airflow/
      run_id=manual__2026-03-08T04-00-00Z/
        page_001.parquet
        page_002.parquet
    manifests/
      repo_owner=apache/repo_name=airflow/data.parquet

  silver/
    issue_current/repo_owner=apache/repo_name=airflow/data.parquet
    issue_labels/repo_owner=apache/repo_name=airflow/data.parquet
    issue_assignees/repo_owner=apache/repo_name=airflow/data.parquet
    sync_state/repo_owner=apache/repo_name=airflow/data.parquet

  gold/
    mart_issue_lifecycle/repo_owner=apache/repo_name=airflow/data.parquet
    mart_issue_daily_flow/repo_owner=apache/repo_name=airflow/data.parquet
    mart_issue_closure_age_monthly/repo_owner=apache/repo_name=airflow/data.parquet
    mart_issue_weekday_rhythm/repo_owner=apache/repo_name=airflow/data.parquet
    mart_issue_swing_days/repo_owner=apache/repo_name=airflow/data.parquet
```

---

## Metabase setup

### Start

```bash
docker compose up -d metabase
```

UI → http://localhost:3000 (takes ~30s to start)

Complete the Metabase onboarding wizard. When asked to add a database, skip it — add it manually afterward from **Settings → Databases → Add database**.

### Connect DuckDB

Select **DuckDB** as the database type and fill in the fields as shown:

![Metabase DuckDB connection setup](docs/assets/metabase-duckdb-setup.png)

| Field | Value |
|-------|-------|
| Display name | `Github Issues` (or anything) |
| Database file | `:memory:` |
| Establish a read-only connection | off |
| Allow loading unsigned extensions | **on** |
| Init SQL | *(see below)* |

**Init SQL** — paste this in full:

```sql
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET minio (
    TYPE S3,
    KEY_ID 'gitpulse',
    SECRET 'gitpulse',
    ENDPOINT 'minio:9000',
    USE_SSL false,
    URL_STYLE 'path'
);

-- Silver views
CREATE OR REPLACE VIEW silver_github_issue_current AS
    SELECT * FROM read_parquet('s3://gitpulse/silver/issue_current/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW silver_github_issue_labels AS
    SELECT * FROM read_parquet('s3://gitpulse/silver/issue_labels/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW silver_github_issue_assignees AS
    SELECT * FROM read_parquet('s3://gitpulse/silver/issue_assignees/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW silver_github_issue_sync_state AS
    SELECT * FROM read_parquet('s3://gitpulse/silver/sync_state/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);

-- Gold views
CREATE OR REPLACE VIEW gold_mart_issue_lifecycle AS
    SELECT * FROM read_parquet('s3://gitpulse/gold/mart_issue_lifecycle/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW gold_mart_issue_daily_flow AS
    SELECT * FROM read_parquet('s3://gitpulse/gold/mart_issue_daily_flow/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW gold_mart_issue_closure_age_monthly AS
    SELECT * FROM read_parquet('s3://gitpulse/gold/mart_issue_closure_age_monthly/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW gold_mart_issue_weekday_rhythm AS
    SELECT * FROM read_parquet('s3://gitpulse/gold/mart_issue_weekday_rhythm/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
CREATE OR REPLACE VIEW gold_mart_issue_swing_days AS
    SELECT * FROM read_parquet('s3://gitpulse/gold/mart_issue_swing_days/repo_owner=*/repo_name=*/*.parquet', hive_partitioning=true, union_by_name=true);
```

> **Note:** `ENDPOINT` uses `minio:9000` (Docker service name), not `localhost:9000`. Both Metabase and MinIO run in the same Docker network.

> **Note:** `Allow loading unsigned extensions` must be **on** — `httpfs` is an unsigned extension.

> **Important:** Do not use `**/*.parquet` against `silver/` or `gold/` anymore. V1 now writes hive-partitioned datasets under `repo_owner=.../repo_name=...`, and broad globs can accidentally pull in legacy flat files such as `silver/sync_state/data.parquet`, which causes DuckDB hive partition binder errors in Metabase.

---

## Architecture notes

- Airflow 3.x uses a split architecture: `api-server`, `dag-processor`, `scheduler` are separate containers. All must share the same `AIRFLOW__API_AUTH__JWT_SECRET` — if they don't, the scheduler gets `403 Forbidden` from the execution API and tasks never start.
- Airflow 3.x auth: `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin"` sets `username:role`, not the password. The password lives in `config/simple_auth_manager_passwords.json` as `{"admin": "admin"}`.
- Fresh Airflow DB migration requires **two passes**: `airflow db migrate && airflow db migrate`. One pass stamps the DB at an intermediate revision and stops.
- `MINIO_ENDPOINT` must be `minio:9000` (Docker service name) inside containers, not `localhost:9000`. Set it in the shared env block in `docker-compose.yml`, not only in `.env`, so it applies to Airflow tasks.
- Bronze writes run-scoped delta pages plus a manifest history dataset. Silver only consumes the latest successful bronze run for the requested repo and merges that delta into the prior repo snapshot.
- The default pytest suite runs the pipeline against local filesystem storage by setting `GITPULSE_STORAGE_BACKEND=local`; the Docker path continues to use MinIO/S3.
- The GitHub API occasionally returns issues where `closed_at` is a few minutes before `created_at` (e.g. issue #57708 in apache/airflow, ~55-min gap). The DQ check allows a 1-hour tolerance for this known API artifact.
- The 7-day charts now use a dense per-day date spine, so inactive days are zero-filled before rolling windows are computed.
- This V1 remains snapshot-based. Reopened issues can still distort backlog direction because the pipeline does not yet ingest GitHub issue events/timeline history.
- Metabase uses `:memory:` DuckDB + Init SQL that reads Parquet directly from MinIO via httpfs on every connection.
- The `Dockerfile.metabase` uses `eclipse-temurin:21-jre-jammy` (Ubuntu/glibc) instead of the official Metabase Alpine image because DuckDB's native library requires glibc and is incompatible with Alpine's musl libc.

---

## Gold marts reference

| Mart | Grain | Powers |
|------|-------|--------|
| `gold_mart_issue_lifecycle` | one row per issue | issue drilldown, label/assignee breakdowns |
| `gold_mart_issue_daily_flow` | one row per repo per date | hero chart (opened vs closed over time) |
| `gold_mart_issue_closure_age_monthly` | one row per repo per month | median/p90 closure age, speed trends |
| `gold_mart_issue_weekday_rhythm` | one row per repo per weekday per event | day-of-week opening/closing patterns |
| `gold_mart_issue_swing_days` | one row per repo per date | biggest intake-pressure and cleanup-burst days |

---

## Product requirements

See [`docs/github-issues-prd.md`](docs/github-issues-prd.md) for the full product requirements document.
