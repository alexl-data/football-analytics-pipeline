"""Extraction layer: live Bundesliga matches (football-data.org) and the
static openfootball historical file, both landing in Databricks raw tables.

Idempotent and backfill-capable by design (see docs/architecture.md):
- extract_live_matches pulls the date range of the DAG run's own data
  interval, not "everything", and MERGEs on match_id so re-running a past
  day never duplicates rows.
- load_historical_to_raw MERGEs on match_id too, so re-uploading the same
  historical file is a no-op beyond the first successful load.
"""
from __future__ import annotations

import os

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sensors.filesystem import FileSensor

RAW_CATALOG = "football_analytics_dev"
RAW_SCHEMA = "football_analytics"
VOLUME_PATH = f"/Volumes/{RAW_CATALOG}/{RAW_SCHEMA}/landing"
GIT_REPO_URL = "https://github.com/alexl-data/football-analytics-pipeline"
GIT_BRANCH = "main"
HISTORICAL_LANDING_GLOB = "/opt/airflow/data/landing/*.csv"
# DATABRICKS_HTTP_PATH looks like /sql/1.0/warehouses/<warehouse_id>
SQL_WAREHOUSE_ID = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1]

default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="football_pipeline",
    description="Extracts live and historical Bundesliga match data into Databricks raw tables.",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 8, 15, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["football-analytics", "extraction"],
)
def football_pipeline():

    @task(sla=pendulum.duration(minutes=30))
    def extract_live_matches(data_interval_start=None, data_interval_end=None) -> int:
        """Pulls matches for this run's date range and MERGEs them into the raw table."""
        from databricks import sql as databricks_sql

        api_key = os.environ["FOOTBALL_DATA_API_KEY"]
        competition = os.environ.get("FOOTBALL_DATA_COMPETITION", "BL1")
        date_from = data_interval_start.to_date_string()
        date_to = data_interval_end.to_date_string()

        response = requests.get(
            f"https://api.football-data.org/v4/competitions/{competition}/matches",
            headers={"X-Auth-Token": api_key},
            params={"dateFrom": date_from, "dateTo": date_to},
            timeout=30,
        )
        response.raise_for_status()
        matches = response.json().get("matches", [])

        with databricks_sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {RAW_CATALOG}.{RAW_SCHEMA}.raw_football_api_matches (
                        match_id BIGINT,
                        competition STRING,
                        matchday INT,
                        utc_date TIMESTAMP,
                        status STRING,
                        home_team STRING,
                        away_team STRING,
                        home_score INT,
                        away_score INT,
                        ingested_at TIMESTAMP
                    ) USING DELTA
                """)

                for m in matches:
                    full_time = (m.get("score") or {}).get("fullTime") or {}
                    cursor.execute(
                        f"""
                        MERGE INTO {RAW_CATALOG}.{RAW_SCHEMA}.raw_football_api_matches AS target
                        USING (SELECT
                            ? AS match_id, ? AS competition, ? AS matchday,
                            to_timestamp(?) AS utc_date, ? AS status,
                            ? AS home_team, ? AS away_team,
                            ? AS home_score, ? AS away_score,
                            current_timestamp() AS ingested_at
                        ) AS source
                        ON target.match_id = source.match_id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                        """,
                        (
                            m["id"], competition, m.get("matchday"),
                            m.get("utcDate"), m.get("status"),
                            m["homeTeam"].get("name"), m["awayTeam"].get("name"),
                            full_time.get("home"), full_time.get("away"),
                        ),
                    )
        return len(matches)

    wait_for_historical_file = FileSensor(
        task_id="wait_for_historical_file",
        filepath=HISTORICAL_LANDING_GLOB,
        poke_interval=15,
        timeout=60,
        mode="reschedule",
        soft_fail=True,  # historical file is a one-time drop, not expected daily
    )

    @task
    def upload_historical_file() -> int:
        """Uploads every local historical file to the Volume, each under its own
        name (not a fixed filename) — supports dropping multiple season files.
        load_historical_to_raw then reads the whole landing directory."""
        import glob
        from pathlib import Path

        from databricks.sdk import WorkspaceClient

        local_paths = glob.glob(HISTORICAL_LANDING_GLOB)
        if not local_paths:
            return 0

        w = WorkspaceClient(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
        )
        for local_path in local_paths:
            with open(local_path, "rb") as f:
                w.files.upload(f"{VOLUME_PATH}/{Path(local_path).name}", f, overwrite=True)
        return len(local_paths)

    upload_task = upload_historical_file()
    wait_for_historical_file >> upload_task

    load_historical_to_raw = DatabricksSubmitRunOperator(
        task_id="load_historical_to_raw",
        databricks_conn_id="databricks_default",
        git_source={
            "git_url": GIT_REPO_URL,
            "git_provider": "gitHub",
            "git_branch": GIT_BRANCH,
        },
        tasks=[
            {
                "task_key": "merge_historical_matches",
                "sql_task": {
                    "warehouse_id": SQL_WAREHOUSE_ID,
                    "file": {
                        "path": "databricks_jobs/load_historical_to_raw.sql",
                        "source": "GIT",
                    },
                },
            }
        ],
        trigger_rule="none_failed_min_one_success",
    )

    raw_data_ready = EmptyOperator(
        task_id="raw_data_ready",
        trigger_rule="none_failed_min_one_success",
    )

    live_matches = extract_live_matches()
    upload_task >> load_historical_to_raw
    [live_matches, load_historical_to_raw] >> raw_data_ready


football_pipeline()
