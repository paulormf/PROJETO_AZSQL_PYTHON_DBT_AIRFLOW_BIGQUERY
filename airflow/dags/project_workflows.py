"""Airflow orchestration for Azure SQL, GCS, BigQuery, and dbt."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Sequence

import pendulum
from airflow.sdk import dag, task
from dotenv import dotenv_values


PROJECT_DIR = Path("/opt/airflow/project")
INGESTION_PYTHON = Path("/opt/airflow/venvs/ingestion/bin/python")
DBT_EXECUTABLE = Path("/opt/airflow/venvs/dbt/bin/dbt")
LOCAL_TIMEZONE = "America/Sao_Paulo"


def project_environment() -> dict[str, str]:
    """Load local project configuration for task subprocesses without logging it."""
    local_values = {
        key: value
        for key, value in dotenv_values(PROJECT_DIR / ".env").items()
        if key and value is not None
    }
    environment = {**os.environ, **local_values}
    # As bibliotecas clientes do Google procuram GOOGLE_CLOUD_PROJECT. A configuração do projeto
    # usa o nome genérico GCP_PROJECT_ID; por isso, mapeamos a variável para os contêineres.
    if not environment.get("GOOGLE_CLOUD_PROJECT") and environment.get("GCP_PROJECT_ID"):
        environment["GOOGLE_CLOUD_PROJECT"] = environment["GCP_PROJECT_ID"]
    return environment


def run_command(command: Sequence[str]) -> str:
    """Run one project command and stream its output into the Airflow task log."""
    process = subprocess.Popen(
        list(command),
        cwd=PROJECT_DIR,
        env=project_environment(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )
    output_lines: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        output_lines.append(line)
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command)
    return "".join(output_lines)


def extract_run_id(output: str, variable_name: str) -> str:
    match = re.search(rf"^{re.escape(variable_name)}=(\S+)$", output, re.MULTILINE)
    if not match:
        raise RuntimeError(f"The pipeline did not print {variable_name}")
    return match.group(1)


def run_dbt_build() -> None:
    run_command(
        [
            str(DBT_EXECUTABLE),
            "build",
            "--project-dir",
            str(PROJECT_DIR / "dbt"),
            "--profiles-dir",
            str(PROJECT_DIR / "dbt"),
        ]
    )


@dag(
    dag_id="azure_sql_cdc_to_bigquery_dbt",
    description="Extract and apply Azure SQL CDC changes, then build dbt models.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=LOCAL_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["azure-sql", "cdc", "bigquery", "dbt"],
)
def azure_sql_cdc_to_bigquery_dbt():
    @task
    def extract_cdc_to_gcs() -> str:
        output = run_command(
            [str(INGESTION_PYTHON), "-m", "pipelines.azure_sql_cdc_to_gcs"]
        )
        return extract_run_id(output, "CDC_RUN_ID")

    @task(pool="pipeline_mutations")
    def load_cdc_to_bigquery(batch_id: str) -> None:
        run_command(
            [
                str(INGESTION_PYTHON),
                "-m",
                "pipelines.gcs_cdc_to_bigquery",
                "--run-id",
                batch_id,
            ]
        )

    @task(pool="pipeline_mutations")
    def build_dbt() -> None:
        run_dbt_build()

    batch_id = extract_cdc_to_gcs()
    load_cdc_to_bigquery(batch_id) >> build_dbt()


@dag(
    dag_id="azure_sql_full_refresh_to_bigquery_dbt",
    description="Refresh full_refresh source tables, then build dbt models.",
    schedule="0 4 * * 0",
    start_date=pendulum.datetime(2026, 10, 1, tz=LOCAL_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["azure-sql", "full-refresh", "bigquery", "dbt"],
)
def azure_sql_full_refresh_to_bigquery_dbt():
    @task
    def extract_full_refresh_to_gcs() -> str:
        output = run_command(
            [
                str(INGESTION_PYTHON),
                "-m",
                "pipelines.azure_sql_to_gcs",
                "--strategy",
                "full_refresh",
            ]
        )
        return extract_run_id(output, "RUN_ID")

    @task(pool="pipeline_mutations")
    def load_full_refresh_to_bigquery(batch_id: str) -> None:
        run_command(
            [
                str(INGESTION_PYTHON),
                "-m",
                "pipelines.gcs_parquet_to_bigquery",
                "--strategy",
                "full_refresh",
                "--run-id",
                batch_id,
            ]
        )

    @task(pool="pipeline_mutations")
    def build_dbt() -> None:
        run_dbt_build()

    batch_id = extract_full_refresh_to_gcs()
    load_full_refresh_to_bigquery(batch_id) >> build_dbt()


azure_sql_cdc_to_bigquery_dbt()
azure_sql_full_refresh_to_bigquery_dbt()
