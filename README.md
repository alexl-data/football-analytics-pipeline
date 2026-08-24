# Football Analytics Pipeline

A batch data pipeline that combines live football data, historical results, and static reference
data into an analytics-ready warehouse — orchestrated with Airflow, transformed with dbt, running
on Databricks.

This is a portfolio project built to demonstrate a classic, widely-used batch orchestration stack
end to end: multi-source extraction with real task dependencies (not a single linear pipeline),
layered SQL transformations, data testing, slowly changing dimensions, and CI.

---

## Tech stack

| Layer               | Tool                                                        |
|----------------------|--------------------------------------------------------------|
| Orchestration        | Apache Airflow (Docker Compose, LocalExecutor)                |
| Transformation       | dbt (dbt-databricks adapter)                                  |
| Compute / warehouse  | Databricks (SQL warehouse)                                    |
| CI                   | GitHub Actions (dbt build/test, SQL linting, Docker image build) |

## Data sources

| Source | Type | Role |
|---|---|---|
| [football-data.org](https://www.football-data.org/) | REST API | Current season matches & standings for the Bundesliga (BL1) |
| [openfootball](https://github.com/openfootball) (via the [footballcsv](https://github.com/footballcsv/deutschland) CSV mirror) | Static file(s) (CSV) | Historical match results — one or more season files dropped into `data/landing/` |
| Team & stadium reference data | dbt seed (CSV) | Static dimension enrichment (capacity, city, founded year) |

---

## Architecture

```
  ┌────────────────────────────────┐     ┌───────────────────────────────────┐
  │      extract_live_matches      │     │      wait_for_historical_file     │
  │ (@task, football-data.org API) │     │      (FileSensor, soft_fail)      │
  └────────────────────────────────┘     └───────────────────────────────────┘
                   │                                       ▼
                   │                     ┌───────────────────────────────────┐
                   │                     │       upload_historical_file      │
                   │                     │ (local file -> Databricks Volume) │
                   │                     └───────────────────────────────────┘
                   │                                       ▼
                   │                     ┌───────────────────────────────────┐
                   │                     │       load_historical_to_raw      │
                   │                     │   (DatabricksSubmitRunOperator,   │
                   │                     │      sql_task via git_source)     │
                   │                     └───────────────────────────────────┘
                   │                                       │
                   └───────────────────┴───────────────────┘
                                       ▼
                              ┌────────────────┐
                              │ raw_data_ready │
                              └────────────────┘
                                       ▼
                              ┌────────────────┐
                              │    dbt_build   │
                              └────────────────┘
```

`extract_live_matches` pulls the current data interval's matches from football-data.org and MERGEs
them straight into a raw table. In parallel, `wait_for_historical_file` watches `data/landing/` for
a dropped CSV; if one appears, `upload_historical_file` stages it to a Unity Catalog Volume, and
`load_historical_to_raw` — a git-sourced Databricks SQL job, pulled directly from this repo with no
manual deployment into the workspace — merges the whole Volume directory into its own raw table.
Both paths converge at `raw_data_ready`, which only then triggers a single `dbt_build` task running
staging, intermediate, marts, snapshot, and tests together, in dbt's own dependency order.

The reasoning behind these choices — why two sources, why a snapshot instead of CDC, why one dbt
task instead of several — is in [Key Design Decisions](#key-design-decisions) below. A deeper dive
into the Airflow side specifically (scheduler / webserver / executor / metadata DB, the isolated
dbt virtualenv, what CI does and doesn't cover) lives in [`docs/architecture.md`](docs/architecture.md).

---

## Key Design Decisions

1. **Two sources, two time ranges.** football-data.org's free tier only exposes the current
   season — there's no way to pull prior seasons from it. Multi-season history comes from the
   static openfootball archive instead, picked up via an Airflow `FileSensor`. The two sources are
   independent of each other, so both extraction paths run in parallel; `dbt_build` starts once
   they converge at `raw_data_ready`.

2. **A dbt snapshot for standings history.** `snapshot_standings` uses dbt's `strategy=check` over
   `fct_standings_current` to get SCD Type 2 history: each run, dbt diffs the row against what's
   already stored and inserts a new version if any tracked column (points, goal difference, etc.)
   changed. This fits because `fct_standings_current` is a query result recomputed from scratch
   every run, not a source table with its own change log to read from — a snapshot is the tool
   built for exactly that case.

3. **Incremental materialization only on `fct_matches`.** It's the one table whose row count grows
   meaningfully over time (new matches daily), so it's `materialized='incremental'`, watermarked on
   `ingested_at` and merging only rows touched since the last run. Staging models sit directly on
   thin raw tables and stay full-refresh — the incremental bookkeeping wouldn't pay for itself
   there.

4. **Idempotent, backfill-capable extraction from the start.** The DAG runs with `catchup=True`,
   and each raw table is MERGEd on whatever key is actually available at that layer:
   `raw_football_api_matches` on the API's own `match_id`, `raw_openfootball_matches` on `date` +
   both team names (the historical source has no native match ID). Re-running any past date is a
   no-op beyond the first successful load — required for backfills to be safe. dbt later builds its
   own unified `match_id` per row (a hash of source + natural key) once both sources are combined
   in `int_matches__unioned` — a separate, hash-based ID from these raw-layer keys.

5. **`git_source`, not a manually deployed job.** `load_historical_to_raw` runs as a Databricks SQL
   job pulled straight from this repo via `git_source`, not a script copy-pasted into the workspace
   UI. One source of truth — the deployed job can't silently drift from what's in version control.

6. **An isolated dbt virtualenv inside the Airflow image.** `dbt-databricks` requires `click>=8.2`,
   but Airflow's own published constraints pin `click==8.1.7` — a genuine, unresolvable conflict
   between what the two tools need from the same package, not something a flag or version bump
   papers over. `airflow/Dockerfile` installs dbt into a second virtualenv at `/opt/dbt_venv`,
   untouched by Airflow's own dependency constraints. Full story in
   [`docs/architecture.md`](docs/architecture.md#running-dbt-from-inside-the-airflow-container).

---

## dbt layering convention

- **staging (`stg_`)** — 1:1 with the source: renaming, casting, light cleaning. No cross-source
  joins, no business logic.
- **intermediate (`int_`)** — reusable business-logic building blocks (e.g. matches enriched with
  a computed result type). Not meant to be queried directly by BI tools.
- **marts (`fct_`/`dim_`)** — final, consumption-ready fact and dimension tables (e.g.
  `fct_matches`, `dim_teams`). A mart can depend on another mart (e.g. `fct_standings_current`
  is built from `fct_matches`) — that's still marts, not intermediate. An intermediate model
  should never depend on a marts table; that inverts the intended staging → intermediate →
  marts direction.

## Project structure

```
airflow/                  Airflow Docker Compose setup + DAGs
data/landing/             Drop zone the FileSensor watches for the historical data file
databricks_jobs/          Databricks SQL job(s) run via git_source (e.g. load_historical_to_raw.sql)
dbt/football_analytics/   dbt project (staging / intermediate / marts / snapshots / seeds / macros)
docs/                     Architecture notes
.github/workflows/        CI pipelines (dbt build/test, SQL linting, Docker image build)
```

---

## Setup

### Airflow (Docker Compose)

```bash
cp .env.example .env   # fill in AIRFLOW_FERNET_KEY, AIRFLOW_WEBSERVER_SECRET_KEY, admin credentials
cd airflow
docker compose up -d
```

Open `http://localhost:8080` and log in with the admin credentials from `.env`. See
[`docs/architecture.md`](docs/architecture.md) for what each service does and why
LocalExecutor was chosen.

### dbt (Databricks)

```bash
python -m venv .venv
.venv/Scripts/pip install -r dbt/requirements.txt   # dbt-databricks

# fill in DATABRICKS_HOST/HTTP_PATH/TOKEN/CATALOG in .env first, then:
dbt debug --project-dir dbt/football_analytics --profiles-dir dbt/football_analytics
```

`profiles.yml` is committed on purpose — it only contains `env_var(...)` references
resolved from `.env` at runtime, no literal secrets.

**Windows note:** run dbt from PowerShell, not Git Bash. Git Bash auto-converts any
value starting with `/` (like `DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/...`) into a
Windows path when it's passed to a native `.exe` — silently corrupting it — even when
the value only lives in an environment variable, not a command-line argument.

To browse the generated documentation site (model/column descriptions, lineage graph):

```bash
dbt docs generate --project-dir dbt/football_analytics --profiles-dir dbt/football_analytics
dbt docs serve --project-dir dbt/football_analytics --profiles-dir dbt/football_analytics
```

### Extraction layer prerequisites

- A free [football-data.org](https://www.football-data.org/client/register) API key in `.env` (`FOOTBALL_DATA_API_KEY`).
- A Unity Catalog Volume for staging the historical file before it's merged:
  ```sql
  CREATE VOLUME football_analytics_dev.football_analytics.landing;
  ```
- The `databricks_default` and `fs_default` Airflow connections are created/refreshed
  automatically by `airflow-init` on every `docker compose up` — no manual setup.

### CI (GitHub Actions)

Three workflows run on pull requests, scoped by path so each only fires when relevant:
`ci-dbt-test.yml` and `ci-lint.yml` (`dbt/**`) build/test the dbt project and lint its SQL;
`ci-docker-build.yml` (`airflow/**`) confirms the Airflow image still builds. The first two need
the same Databricks credentials as local development, added as **repository secrets** (Settings →
Secrets and variables → Actions), not committed anywhere: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`,
`DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`. All three validate against the `dev` catalog — see
[`docs/architecture.md`](docs/architecture.md#what-cicd-does-and-doesnt-cover-here)
for why this project's CI intentionally stops there instead of also deploying to a
production environment that doesn't exist yet.

---

## Verify it worked

Quick sanity checks against the `dev` catalog after a pipeline run — run these in a Databricks SQL
editor:

```sql
-- Row counts across each layer
SELECT 'raw_football_api_matches' AS table_name, count(*) AS rows FROM football_analytics_dev.football_analytics.raw_football_api_matches
UNION ALL
SELECT 'raw_openfootball_matches', count(*) FROM football_analytics_dev.football_analytics.raw_openfootball_matches
UNION ALL
SELECT 'fct_matches', count(*) FROM football_analytics_dev.football_analytics.fct_matches
UNION ALL
SELECT 'fct_standings_current', count(*) FROM football_analytics_dev.football_analytics.fct_standings_current
UNION ALL
SELECT 'snapshot_standings (history)', count(*) FROM football_analytics_dev.football_analytics.snapshot_standings;
```

```sql
-- Current Bundesliga table, top 5
SELECT
    standings.table_position,
    teams.team_name,
    standings.played,
    standings.points
FROM football_analytics_dev.football_analytics.fct_standings_current AS standings
JOIN football_analytics_dev.football_analytics.dim_teams AS teams
    ON teams.team_id = standings.team_id
ORDER BY standings.table_position
LIMIT 5;
```

Or generate the dbt docs site (see [Setup](#setup) above) for column-level descriptions and the
full model lineage graph.
