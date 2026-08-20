# Football Analytics Pipeline

A batch data pipeline that combines live football data, historical results, and static reference
data into an analytics-ready warehouse — orchestrated with Airflow, transformed with dbt, running
on Databricks.

This is a portfolio project built to demonstrate a classic, widely-used batch orchestration stack
end to end: multi-source extraction with real task dependencies (not a single linear pipeline),
layered SQL transformations, data testing, slowly changing dimensions, and CI.

## Tech stack

| Layer            | Tool                                                   |
|-------------------|--------------------------------------------------------|
| Orchestration      | Apache Airflow (Docker Compose, LocalExecutor)         |
| Transformation      | dbt (dbt-databricks adapter)                          |
| Compute / warehouse | Databricks (SQL warehouse)                            |
| CI                | GitHub Actions (dbt tests, SQL linting)                |

## Data sources

| Source | Type | Role |
|---|---|---|
| [football-data.org](https://www.football-data.org/) | REST API | Current season matches & standings for the Bundesliga (BL1) |
| [openfootball](https://github.com/openfootball) (via the [footballcsv](https://github.com/footballcsv/deutschland) CSV mirror) | Static file(s) (CSV) | Historical match results — one or more season files dropped into `data/landing/` |
| Team & stadium reference data | dbt seed (CSV) | Static dimension enrichment (capacity, city, founded year) |

Combining a live API with a historical file source (picked up via an Airflow `FileSensor`) gives
the DAG a genuine fan-in dependency: transformation only starts once both independent extraction
paths have completed.

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

`extract_live_matches` and `load_historical_to_raw` both MERGE on natural keys, so
re-running a past date or re-uploading the same historical file(s) is a no-op —
required for `catchup=True` backfills to be safe. The `upload_historical_file` step
exists because Databricks compute can't read the local filesystem directly; every
CSV dropped in `data/landing/` (one season or several) is staged to a Unity Catalog
Volume under its own filename first, then a git-sourced SQL job (pulled straight
from this repo, no manual deployment into the Databricks workspace) reads the whole
Volume directory and merges it into the raw table — `mode => FAILFAST` makes that job
fail loudly rather than silently misparse if a future season file's column shape
doesn't match. `raw_data_ready` uses `trigger_rule=none_failed_min_one_success` so a
skipped (not failed) historical branch — the file sensor soft-fails when no new file
is dropped — doesn't block the daily live-data path.

`dbt_build` runs `dbt build` (seeds, models, snapshot, and tests together) as a
single Airflow task rather than separate staging/snapshot/marts/test tasks. That's
deliberate: `fct_standings_current` (the current-season league table, in marts) is
itself a dependency of `snapshot_standings`, since a snapshot needs something to
snapshot — so a fixed staging-then-snapshot-then-marts task order would actually be
wrong. dbt resolves this correctly on its own via its internal dependency graph;
Airflow doesn't need to (and shouldn't try to) micromanage dbt's internal ordering.
dbt itself runs from an isolated virtualenv baked into the Airflow image
(`airflow/Dockerfile`), because `dbt-databricks` requires a newer `click` version than
Airflow's own pinned constraints allow — installing both into the same environment is
a real, unresolvable version conflict, not just a preference.

A detailed explanation of the Airflow architecture (scheduler / webserver / executor / metadata
DB) lives in [`docs/architecture.md`](docs/architecture.md) (added alongside the Airflow setup).

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
airflow/            Airflow Docker Compose setup + DAGs
dbt/football_analytics/  dbt project (staging / intermediate / marts / snapshots / seeds / macros)
data/landing/        Drop zone the FileSensor watches for the historical data file
docs/                Architecture notes
.github/workflows/    CI pipelines (dbt build/test, SQL linting, Docker image build)
```

## Status

Project is being built incrementally, in commit-sized steps:

- [x] Step 1 — Project skeleton
- [x] Step 2 — Airflow setup (Docker Compose, LocalExecutor)
- [x] Step 3 — dbt project skeleton
- [x] Step 4 — Extraction layer (API + historical file + Databricks load)
- [x] Step 5 — Staging models & generic tests
- [x] Step 6 — Intermediate & marts models
- [x] Step 7 — Snapshot for SCD Type 2 standings history
- [x] Step 8 — Macros, custom tests, dbt docs
- [x] Step 9 — CI (GitHub Actions)

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

`ci-dbt-test.yml` and `ci-lint.yml` need the same Databricks credentials as local
development, added as **repository secrets** (Settings → Secrets and variables →
Actions), not committed anywhere: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`,
`DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`. Both run against the `dev` catalog — see
[`docs/architecture.md`](docs/architecture.md#what-cicd-does-and-doesnt-cover-here)
for why this project's CI intentionally stops there instead of also deploying to a
production environment that doesn't exist yet.
