# Airflow Architecture

This project runs Airflow locally via Docker Compose with the **LocalExecutor**. This
document explains the moving parts and why they're configured this way.

## Components

| Component | Role |
|---|---|
| **Scheduler** | Continuously parses DAG files, determines which tasks are due (based on schedule and upstream dependencies), and hands them to the executor. Runs as a long-lived container; if it's down, nothing gets scheduled — the webserver alone can't run tasks. |
| **Webserver** | Serves the UI (`localhost:8080`): DAG graph view, manual triggers, task logs, connections/variables management. Purely a presentation layer over the metadata DB — it does not execute tasks itself. |
| **Metadata DB (Postgres)** | Stores DAG run history, task instance state, connections, variables, and XComs. Scheduler and webserver both read/write here, which decouples them — either can restart independently without losing state. |
| **Executor (LocalExecutor)** | Not a separate container — it's how the scheduler runs tasks: as parallel subprocesses on the same host, backed by Postgres (which supports concurrent connections, unlike SQLite). |

## Why LocalExecutor

Three executor options were considered:

- **SequentialExecutor** (Airflow's default): runs on SQLite, executes one task at a
  time. The DAG's fan-in design (two independent extraction branches running in
  parallel, see [README.md](../README.md#architecture)) would collapse into a
  sequential path — defeats the point of demonstrating real dependency structure.
- **CeleryExecutor / KubernetesExecutor**: distribute tasks across multiple worker
  nodes/pods via a message broker (Redis/RabbitMQ) or the Kubernetes API. Built for
  horizontal scaling across hosts — unnecessary infrastructure for a single-host
  portfolio project, and it would obscure the orchestration logic behind
  infra-scaling concerns that aren't the point being demonstrated here.
- **LocalExecutor** (chosen): runs tasks as parallel subprocesses on one host, backed
  by Postgres. Enough real parallelism to demonstrate the fan-in DAG design, without
  extra services.

This is why the Compose file only defines `postgres`, `airflow-webserver`, and
`airflow-scheduler` — no separate worker, broker, or Flower service.

Note this is a hand-written, trimmed-down file, not the official
[Airflow docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
downloaded as-is. The official file targets CeleryExecutor by default (Redis, worker,
triggerer, Flower included) — using it here would mean stripping those services out
again to match the LocalExecutor choice above, so it was built from that same
end-state directly instead.

## The `airflow-init` service

Docker Compose also runs a one-off `airflow-init` service before the webserver and
scheduler start. It runs `airflow db migrate` (creates/updates the metadata schema)
and creates the admin user, then exits. This is the standard pattern for Airflow +
Compose: it keeps first-time setup out of the long-running webserver/scheduler
entrypoints, so they don't race each other to migrate the DB on startup.

## Pinning extra Python packages against the base image's Airflow version

`airflow/Dockerfile` installs `requirements.txt` with `pip install --constraint
<Airflow's published constraints file>`. Without it, pip's own dependency resolver is
free to upgrade `apache-airflow` itself while satisfying a provider package's
requirements — which is exactly what happened during development here:
`apache-airflow-providers-databricks`, installed unpinned, pulled in Airflow 3.x as a
dependency and silently upgraded the image away from the base's Airflow 2.10.4,
breaking the `airflow` CLI entirely (`airflow: command not found`, scheduler/webserver
crash-looping). The constraints file pins apache-airflow and every provider/dependency
to versions known-compatible with the base image's Airflow + Python version, so extra
packages can only be added, not silently upgrade the core.

## Running dbt from inside the Airflow container

`dbt_build` (the task that runs `dbt build`) can't just `pip install
dbt-databricks` alongside Airflow's own dependencies in `requirements.txt`:
`dbt-databricks` 1.12.4 requires `click>=8.2.0`, but the constraints file
above (needed for the reason described there) pins `click==8.1.7` for
Airflow's own CLI. That's a genuine, unresolvable conflict between what the
two tools need from the same package — not something a different flag or
version bump papers over. The fix is isolation, not compromise:
`airflow/Dockerfile` creates a second virtualenv at `/opt/dbt_venv` (as
`root`, then `chown`'d back to the `airflow` user) and installs
`dbt-databricks` only there, untouched by Airflow's constraints. The DAG
calls `/opt/dbt_venv/bin/dbt` directly rather than a bare `dbt` on `PATH`.

## Running it locally

```bash
cd airflow
cp ../.env.example ../.env   # if not already done; fill in real values
docker compose up -d
```

Then open `http://localhost:8080` and log in with `AIRFLOW_ADMIN_USERNAME` /
`AIRFLOW_ADMIN_PASSWORD` from `.env` (defaults to `admin` / `admin` if unset — change
this for anything beyond local use). The `setup_healthcheck` DAG should appear and
can be triggered manually to confirm the scheduler and executor work end to end; it's
temporary and will be removed once the real pipeline DAG exists.

## What CI/CD does and doesn't cover here

`.github/workflows/` (`ci-dbt-test.yml`, `ci-lint.yml`, `ci-docker-build.yml`) test
and validate code on every pull request — build the dbt project against the `dev`
catalog, lint the SQL, and confirm the Airflow image still builds. That's it: GitHub
Actions runs on a temporary, GitHub-hosted machine that exists only for the few
minutes a workflow takes, then disappears. It is not a place to host a long-running
service, so merging to `main` does not start, restart, or otherwise touch Airflow
anywhere.

Airflow itself currently only runs where `docker compose up` is run manually — this
machine. The `@daily` schedule only fires while that's up; there's no environment
where it runs continuously and unattended. That's a deliberate, reasonable scope
boundary for a local portfolio project, not an oversight, but it's worth being
explicit about: **running `docker compose up` locally is not the same as having a
production deployment**, even though the same Airflow/dbt/Databricks pieces would
carry over largely unchanged.

To actually run this unattended in production, Airflow would need a persistent host
— a managed service (AWS MWAA, Google Cloud Composer, Astronomer) or a
self-managed always-on VM/Kubernetes cluster running this same Docker Compose (or
Helm-equivalent) setup. Only once something is continuously feeding a separate
`football_analytics_prod` catalog with fresh raw data would a merge-triggered "`dbt
build` against prod" step in CD actually mean something — before that, it would just
rebuild marts on top of catalog that never receives new data, which is why this
project's CI stops at validating against `dev` and doesn't attempt a prod deploy step.
