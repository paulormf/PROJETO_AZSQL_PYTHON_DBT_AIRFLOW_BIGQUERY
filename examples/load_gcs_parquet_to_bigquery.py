"""Load one completed GCS extraction run into native BigQuery tables."""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from src.export_config import load_enabled_exports


RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
BIGQUERY_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Parquet files from one GCS run into BigQuery."
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Run ID printed by export_order_items_to_gcs.py, e.g. 20261009T172516Z-aeb16ae6",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    if not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise ValueError("Invalid run ID format")

    config_path = Path(os.getenv("EXPORT_CONFIG_PATH", "config/gcs_exports.yaml"))
    exports = load_enabled_exports(config_path)
    if not exports:
        print("Nenhuma tabela habilitada para carga no BigQuery.")
        return

    project_id = required_setting("GCP_PROJECT_ID")
    dataset_name = os.getenv("BQ_DATASET", "raw")
    expected_location = os.getenv("BQ_LOCATION", "us-central1")
    bucket_name = required_setting("GCS_BUCKET")
    base_prefix = os.getenv("GCS_PREFIX", "raw").strip("/")
    extract_date = datetime.strptime(args.run_id[:8], "%Y%m%d").strftime("%Y-%m-%d")

    if not BIGQUERY_IDENTIFIER_PATTERN.fullmatch(dataset_name):
        raise ValueError("BQ_DATASET must be a valid BigQuery dataset ID")

    bigquery_client = bigquery.Client(project=project_id, location=expected_location)
    storage_client = None
    try:
        storage_client = storage.Client(project=project_id)
        dataset_id = f"{project_id}.{dataset_name}"
        try:
            dataset = bigquery_client.get_dataset(dataset_id)
        except NotFound as error:
            raise ValueError(
                f"Dataset {dataset_id} does not exist. Create it in {expected_location} first."
            ) from error

        if dataset.location.lower() != expected_location.lower():
            raise ValueError(
                f"Configured BQ_LOCATION={expected_location}, but dataset location is "
                f"{dataset.location}"
            )

        bucket = storage_client.get_bucket(bucket_name)
        if bucket.location.lower() != dataset.location.lower():
            raise ValueError(
                f"GCS bucket location ({bucket.location}) must match BigQuery dataset "
                f"location ({dataset.location}) for this load"
            )

        for export in exports:
            table_name = export["table"]
            if not BIGQUERY_IDENTIFIER_PATTERN.fullmatch(table_name):
                raise ValueError(f"Invalid BigQuery table name in config: {table_name!r}")

            object_prefix = (
                f"{base_prefix}/{export['name']}/extract_date={extract_date}"
                f"/run_id={args.run_id}"
            )
            parquet_objects = [
                blob.name
                for blob in storage_client.list_blobs(bucket_name, prefix=f"{object_prefix}/")
                if blob.name.endswith(".parquet")
            ]
            if not parquet_objects:
                raise FileNotFoundError(
                    f"No Parquet objects found for {export['name']} at "
                    f"gs://{bucket_name}/{object_prefix}/"
                )

            source_uri = f"gs://{bucket_name}/{object_prefix}/*.parquet"
            destination_table = f"{project_id}.{dataset_name}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            load_job = bigquery_client.load_table_from_uri(
                source_uri,
                destination_table,
                job_config=job_config,
                location=dataset.location,
            )
            load_job.result()

            print(
                f"Carga full concluída: {destination_table}, "
                f"{load_job.output_rows} linhas de {len(parquet_objects)} arquivos."
            )
    finally:
        bigquery_client.close()
        if storage_client is not None:
            storage_client.close()


if __name__ == "__main__":
    main()
