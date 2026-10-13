"""Validate a completed GCS snapshot in BigQuery staging, then promote to bronze."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPICallError, NotFound, PreconditionFailed
from google.cloud import bigquery, storage

from src.export_config import load_enabled_exports
from src.sql_schema import (
    SCHEMA_CONTRACT_VERSION,
    bigquery_schema_fields,
    compare_contract_types,
)


RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
BIGQUERY_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LSN_PATTERN = re.compile(r"^0x[0-9A-F]{20}$")


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def write_json_marker(bucket: storage.Bucket, object_name: str, payload: dict[str, Any]) -> None:
    bucket.blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )


def set_staging_expiration(
    client: bigquery.Client, table_id: str, ttl_days: int
) -> bool:
    """Set TTL using a fresh table etag; expiration is cleanup, not load success."""
    expires = datetime.now(timezone.utc) + timedelta(days=ttl_days)
    for attempt in range(3):
        table = client.get_table(table_id)
        table.expires = expires
        try:
            client.update_table(table, ["expires"])
            return True
        except PreconditionFailed:
            if attempt == 2:
                print(
                    f"Aviso: não foi possível configurar a expiração de {table_id} "
                    "após três tentativas; a promoção foi concluída, mas a tabela "
                    "de staging precisará de limpeza manual."
                )
        except GoogleAPICallError:
            print(
                f"Aviso: não foi possível configurar a expiração de {table_id}; "
                "a promoção foi concluída, mas a tabela de staging precisará de limpeza manual."
            )
            return False
    return False


def validate_cdc_baseline(value: Any, table_name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"Manifesto não contém baseline CDC para {table_name}")
    for key in ("capture_start_lsn", "min_lsn_at_baseline", "baseline_lsn"):
        if not isinstance(value.get(key), str) or not LSN_PATTERN.fullmatch(value[key]):
            raise ValueError(f"LSN inválido no baseline CDC de {table_name}: {key}")
    if not isinstance(value.get("capture_instance"), str) or not value["capture_instance"]:
        raise ValueError(f"Capture instance ausente no baseline CDC de {table_name}")
    baseline_bytes = bytes.fromhex(value["baseline_lsn"][2:])
    if baseline_bytes < bytes.fromhex(value["capture_start_lsn"][2:]):
        raise ValueError(f"Baseline CDC anterior ao início da captura de {table_name}")
    if baseline_bytes < bytes.fromhex(value["min_lsn_at_baseline"][2:]):
        raise ValueError(f"Baseline CDC fora da retenção para {table_name}")
    return value


def initialize_cdc_checkpoints(
    client: bigquery.Client,
    dataset_id: str,
    exports: list[dict[str, Any]],
    baselines: dict[str, dict[str, str]],
    run_id: str,
    location: str,
    reset_existing: bool,
) -> None:
    """Create one durable checkpoint per CDC table after the full is promoted."""
    table_id = f"{dataset_id}._cdc_checkpoints"
    try:
        client.get_table(table_id)
    except NotFound:
        checkpoint_table = bigquery.Table(
            table_id,
            schema=[
                bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_schema", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("capture_instance", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_lsn", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("baseline_run_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_run_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            ],
        )
        client.create_table(checkpoint_table)

    for export in exports:
        if export.get("incremental_strategy") != "cdc":
            continue
        baseline = baselines[export["name"]]
        sql = f"""
        ASSERT @reset_existing OR NOT EXISTS (
          SELECT 1 FROM `{table_id}`
          WHERE table_name = @table_name
            AND (source_schema != @source_schema OR source_table != @source_table
              OR capture_instance != @capture_instance OR last_lsn != @baseline_lsn)
        ) AS 'A CDC checkpoint already exists at a different baseline. Rebuild it explicitly before reinitializing.';
        MERGE `{table_id}` AS target
        USING (SELECT
          @table_name AS table_name,
          @source_schema AS source_schema,
          @source_table AS source_table,
          @capture_instance AS capture_instance,
          @baseline_lsn AS last_lsn,
          @run_id AS baseline_run_id,
          @run_id AS last_run_id
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED AND @reset_existing THEN
          UPDATE SET source_schema = source.source_schema,
            source_table = source.source_table,
            capture_instance = source.capture_instance,
            last_lsn = source.last_lsn,
            baseline_run_id = source.baseline_run_id,
            last_run_id = source.last_run_id,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (table_name, source_schema, source_table, capture_instance, last_lsn, baseline_run_id, last_run_id, updated_at)
          VALUES (source.table_name, source.source_schema, source.source_table, source.capture_instance, source.last_lsn, source.baseline_run_id, source.last_run_id, CURRENT_TIMESTAMP());
        """
        job = client.query(
            sql,
            location=location,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("table_name", "STRING", export["name"]),
                    bigquery.ScalarQueryParameter("source_schema", "STRING", export["schema"]),
                    bigquery.ScalarQueryParameter("source_table", "STRING", export["table"]),
                    bigquery.ScalarQueryParameter(
                        "capture_instance", "STRING", baseline["capture_instance"]
                    ),
                    bigquery.ScalarQueryParameter(
                        "baseline_lsn", "STRING", baseline["baseline_lsn"]
                    ),
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                    bigquery.ScalarQueryParameter("reset_existing", "BOOL", reset_existing),
                ]
            ),
        )
        job.result()


def validate_baseline_checkpoint_state(
    client: bigquery.Client,
    dataset_id: str,
    exports: list[dict[str, Any]],
    baselines: dict[str, dict[str, str]],
    location: str,
    reset_existing: bool,
) -> None:
    table_id = f"{dataset_id}._cdc_checkpoints"
    try:
        client.get_table(table_id)
    except NotFound:
        return
    table_names = [
        export["name"] for export in exports if export.get("incremental_strategy") == "cdc"
    ]
    rows = list(client.query(
        f"SELECT table_name, source_schema, source_table, capture_instance, last_lsn "
        f"FROM `{table_id}` WHERE table_name IN UNNEST(@table_names)",
        location=location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("table_names", "STRING", table_names)]
        ),
    ).result())
    row_names = [row.table_name for row in rows]
    if len(set(row_names)) != len(row_names):
        raise RuntimeError("Duplicate rows found in the CDC checkpoint table")
    exports_by_name = {export["name"]: export for export in exports}
    for row in rows:
        export = exports_by_name[row.table_name]
        baseline = baselines[row.table_name]
        matches = (
            row.source_schema == export["schema"]
            and row.source_table == export["table"]
            and row.capture_instance == baseline["capture_instance"]
            and row.last_lsn == baseline["baseline_lsn"]
        )
        if not matches and not reset_existing:
            raise RuntimeError(
                f"Existing CDC checkpoint for {row.table_name} differs from this full baseline. "
                "Use --reset-cdc-checkpoints only with an intentional fresh full rebaseline."
            )


def load_manifest(
    blob: storage.Blob,
    expected_run_id: str,
    expected_table: str,
    incremental_strategy: str,
    expected_cdc_baseline: dict[str, Any] | None,
) -> dict[str, Any]:
    if not blob.exists():
        raise FileNotFoundError(f"Manifesto ausente: gs://{blob.bucket.name}/{blob.name}")
    manifest = json.loads(blob.download_as_text())
    if (
        manifest.get("manifest_version") != 2
        or manifest.get("incremental_strategy", "full_refresh") != incremental_strategy
        or manifest.get("status") != "SUCCEEDED"
        or manifest.get("run_id") != expected_run_id
        or manifest.get("table_name") != expected_table
        or manifest.get("load_mode") != "full"
        or manifest.get("source_count") != manifest.get("exported_count")
        or manifest.get("reconciliation", {}).get("status") != "MATCHED"
    ):
        raise ValueError(f"Manifesto inválido ou incompatível: gs://{blob.bucket.name}/{blob.name}")
    baseline = manifest.get("cdc_baseline")
    schema_contract = manifest.get("schema_contract")
    if (
        not isinstance(schema_contract, dict)
        or schema_contract.get("version") != SCHEMA_CONTRACT_VERSION
        or not isinstance(schema_contract.get("fields"), list)
        or not schema_contract["fields"]
    ):
        raise ValueError(f"Schema contract missing or unsupported for {expected_table}")
    if incremental_strategy == "cdc":
        baseline = validate_cdc_baseline(baseline, expected_table)
        if baseline != expected_cdc_baseline:
            raise ValueError(f"Baseline CDC não corresponde ao manifesto geral para {expected_table}")
        if not manifest.get("cdc_baseline_captured_at"):
            raise ValueError(f"Manifesto sem horário do baseline CDC para {expected_table}")
    elif baseline is not None:
        raise ValueError(f"Tabela full_refresh {expected_table} não deve ter baseline CDC")
    return manifest


def verify_parquet_objects(
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str,
    manifest: dict[str, Any],
) -> list[str]:
    blobs = [
        blob
        for blob in storage_client.list_blobs(bucket_name, prefix=prefix)
        if blob.name.endswith(".parquet")
    ]
    manifest_files = manifest.get("files")
    if not isinstance(manifest_files, list):
        raise ValueError("Manifesto não contém uma lista de arquivos válida")
    expected_files = {
        item.get("name"): item
        for item in manifest_files
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    if (
        len(expected_files) != len(manifest_files)
        or {blob.name for blob in blobs} != set(expected_files)
        or len(manifest_files) != int(manifest.get("file_count", -1))
        or sum(int(item.get("rows", -1)) for item in manifest_files)
        != int(manifest.get("exported_count", -2))
        or any(
            blob.size != int(expected_files[blob.name].get("size_bytes", -1))
            for blob in blobs
        )
    ):
        raise ValueError("Objetos Parquet não correspondem ao manifesto da extração")
    if not blobs:
        raise ValueError(
            "A tabela não gerou arquivos Parquet. Tabelas vazias exigem tratamento de schema explícito."
        )
    return [blob.name for blob in blobs]


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Load a GCS snapshot to BigQuery staging, validate it, and promote to bronze."
    )
    parser.add_argument("--run-id", required=True, help="Run ID printed by the extraction pipeline")
    parser.add_argument(
        "--reset-cdc-checkpoints",
        action="store_true",
        help="Reset existing CDC checkpoints to this full snapshot for an intentional rebaseline",
    )
    args = parser.parse_args()
    if not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise ValueError("Invalid run ID format")

    config_path = Path(os.getenv("EXPORT_CONFIG_PATH", "config/gcs_exports.yaml"))
    exports = load_enabled_exports(config_path)
    if not exports:
        print("Nenhuma tabela habilitada para carga no BigQuery.")
        return

    project_id = required_setting("GCP_PROJECT_ID")
    staging_dataset_name = required_setting("BQ_STAGING_DATASET")
    bronze_dataset_name = required_setting("BQ_BRONZE_DATASET")
    expected_location = os.getenv("BQ_LOCATION", "us-central1")
    bucket_name = required_setting("GCS_BUCKET")
    base_prefix = os.getenv("GCS_PREFIX", "raw").strip("/")
    staging_ttl_days = int(os.getenv("BQ_STAGING_TABLE_TTL_DAYS", "7"))
    if staging_ttl_days <= 0:
        raise ValueError("BQ_STAGING_TABLE_TTL_DAYS must be greater than zero")

    for dataset_name in (staging_dataset_name, bronze_dataset_name):
        if not BIGQUERY_IDENTIFIER_PATTERN.fullmatch(dataset_name):
            raise ValueError(f"Invalid BigQuery dataset ID: {dataset_name!r}")
    if staging_dataset_name == bronze_dataset_name:
        raise ValueError("BQ_STAGING_DATASET and BQ_BRONZE_DATASET must be different")

    bigquery_client = bigquery.Client(project=project_id, location=expected_location)
    storage_client = storage.Client(project=project_id)
    try:
        staging_dataset_id = f"{project_id}.{staging_dataset_name}"
        bronze_dataset_id = f"{project_id}.{bronze_dataset_name}"
        try:
            staging_dataset = bigquery_client.get_dataset(staging_dataset_id)
            bronze_dataset = bigquery_client.get_dataset(bronze_dataset_id)
        except NotFound as error:
            raise ValueError(
                "Os datasets de staging e bronze precisam existir antes da carga. "
                f"Esperados: {staging_dataset_id} e {bronze_dataset_id}."
            ) from error

        for dataset in (staging_dataset, bronze_dataset):
            if dataset.location.lower() != expected_location.lower():
                raise ValueError(
                    f"Dataset {dataset.dataset_id} está em {dataset.location}; "
                    f"esperado {expected_location}."
                )

        bucket = storage_client.get_bucket(bucket_name)
        if bucket.location.lower() != staging_dataset.location.lower():
            raise ValueError(
                f"GCS bucket location ({bucket.location}) must match BigQuery location "
                f"({staging_dataset.location})"
            )

        run_manifest_path = f"{base_prefix}/_runs/run_id={args.run_id}/_RUN_SUCCESS.json"
        run_manifest_blob = bucket.blob(run_manifest_path)
        if not run_manifest_blob.exists():
            raise FileNotFoundError(
                f"Manifesto do run ausente: gs://{bucket_name}/{run_manifest_path}"
            )
        run_manifest = json.loads(run_manifest_blob.download_as_text())
        configured_names = {export["name"] for export in exports}
        cdc_export_names = {
            export["name"]
            for export in exports
            if export.get("incremental_strategy") == "cdc"
        }
        cdc_baselines = run_manifest.get("cdc_baselines", {})
        if (
            run_manifest.get("manifest_version") != 2
            or run_manifest.get("status") != "SUCCEEDED"
            or run_manifest.get("run_id") != args.run_id
            or set(run_manifest.get("table_names", [])) != configured_names
            or run_manifest.get("snapshot_isolation") != "SNAPSHOT"
            or not isinstance(cdc_baselines, dict)
            or set(cdc_baselines) != cdc_export_names
        ):
            raise ValueError("Manifesto geral do run não corresponde à configuração habilitada")
        for table_name, baseline in cdc_baselines.items():
            validate_cdc_baseline(baseline, table_name)
        if cdc_export_names and not run_manifest.get("cdc_baseline_captured_at"):
            raise ValueError("Manifesto geral sem horário de captura do baseline CDC")
        if cdc_export_names:
            validate_baseline_checkpoint_state(
                bigquery_client,
                bronze_dataset_id,
                exports,
                cdc_baselines,
                bronze_dataset.location,
                args.reset_cdc_checkpoints,
            )

        extract_date = datetime.strptime(args.run_id[:8], "%Y%m%d").strftime("%Y-%m-%d")
        run_table_results = []
        for export in exports:
            table_name = export["table"]
            if not BIGQUERY_IDENTIFIER_PATTERN.fullmatch(table_name):
                raise ValueError(f"Invalid BigQuery table name in config: {table_name!r}")

            object_prefix = (
                f"{base_prefix}/{export['name']}/extract_date={extract_date}"
                f"/run_id={args.run_id}"
            )
            prefix = f"{object_prefix}/"
            manifest = load_manifest(
                bucket.blob(f"{prefix}_SUCCESS.json"),
                args.run_id,
                export["name"],
                export.get("incremental_strategy", "full_refresh"),
                cdc_baselines.get(export["name"]),
            )
            parquet_objects = verify_parquet_objects(
                storage_client, bucket_name, prefix, manifest
            )
            source_uri = f"gs://{bucket_name}/{object_prefix}/*.parquet"
            run_suffix = args.run_id.replace("-", "_")
            staging_table_name = f"_stg_{table_name}_{run_suffix}"
            staging_table_id = f"{staging_dataset_id}.{staging_table_name}"
            bronze_table_id = f"{bronze_dataset_id}.{table_name}"

            load_job = bigquery_client.load_table_from_uri(
                source_uri,
                staging_table_id,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    schema=[
                        bigquery.SchemaField(**field)
                        for field in bigquery_schema_fields(manifest["schema_contract"])
                    ],
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ),
                location=staging_dataset.location,
            )
            load_job.result()
            expected_rows = int(manifest["exported_count"])
            staged_table = bigquery_client.get_table(staging_table_id)
            missing, unexpected, mismatches = compare_contract_types(
                manifest["schema_contract"], staged_table.schema
            )
            if missing or unexpected or mismatches:
                raise ValueError(
                    f"BigQuery staging schema differs from the extraction contract for "
                    f"{export['name']}: missing={missing}, unexpected={unexpected}, "
                    f"type_mismatches={mismatches}"
                )
            if int(staged_table.num_rows) != expected_rows:
                raise RuntimeError(
                    f"Reconciliação do staging falhou para {staging_table_id}: "
                    f"manifesto={expected_rows}, BigQuery={staged_table.num_rows}."
                )
            run_table_results.append(
                {
                    "export": export,
                    "prefix": prefix,
                    "manifest": manifest,
                    "parquet_objects": parquet_objects,
                    "staging_table_id": staging_table_id,
                    "bronze_table_id": bronze_table_id,
                    "load_job": load_job,
                    "expected_rows": expected_rows,
                }
            )

        # Do not touch any Bronze table until every source table is staged and validated.
        promoted_tables = []
        for result in run_table_results:
            staging_table_id = result["staging_table_id"]
            bronze_table_id = result["bronze_table_id"]
            expected_rows = result["expected_rows"]
            copy_job = bigquery_client.copy_table(
                staging_table_id,
                bronze_table_id,
                job_config=bigquery.CopyJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                ),
                location=bronze_dataset.location,
            )
            copy_job.result()
            bronze_table = bigquery_client.get_table(bronze_table_id)
            if int(bronze_table.num_rows) != expected_rows:
                raise RuntimeError(
                    f"Reconciliação bronze falhou para {bronze_table_id}: "
                    f"manifesto={expected_rows}, BigQuery={bronze_table.num_rows}."
                )

            staged_table = bigquery_client.get_table(staging_table_id)
            if int(staged_table.num_rows) != expected_rows:
                raise RuntimeError(
                    f"Reconciliação final do staging falhou para {staging_table_id}: "
                    f"esperado={expected_rows}, atual={staged_table.num_rows}."
                )
            set_staging_expiration(
                bigquery_client, staging_table_id, staging_ttl_days
            )
            table_success = {
                "manifest_version": 2,
                "status": "SUCCEEDED",
                "run_id": args.run_id,
                "table_name": result["export"]["name"],
                "destination_table": bronze_table_id,
                "staging_table": staging_table_id,
                "bigquery_load_job_id": result["load_job"].job_id,
                "bigquery_copy_job_id": copy_job.job_id,
                "loaded_rows": expected_rows,
                "cdc_baseline": result["manifest"].get("cdc_baseline"),
                "file_count": len(result["parquet_objects"]),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "reconciliation": {
                    "status": "MATCHED",
                    "manifest_rows": expected_rows,
                    "staging_rows": int(staged_table.num_rows),
                    "bronze_rows": int(bronze_table.num_rows),
                },
            }
            write_json_marker(
                bucket, f"{result['prefix']}_BIGQUERY_SUCCESS.json", table_success
            )
            promoted_tables.append(table_success)
            print(
                f"Carga promovida para {bronze_table_id}: {expected_rows} linhas; "
                f"staging {staging_table_id}."
            )

        if cdc_export_names:
            initialize_cdc_checkpoints(
                bigquery_client,
                bronze_dataset_id,
                exports,
                cdc_baselines,
                args.run_id,
                bronze_dataset.location,
                args.reset_cdc_checkpoints,
            )

        write_json_marker(
            bucket,
            f"{base_prefix}/_runs/run_id={args.run_id}/_BIGQUERY_RUN_SUCCESS.json",
            {
                "manifest_version": 2,
                "status": "SUCCEEDED",
                "run_id": args.run_id,
                "bronze_dataset": bronze_dataset_id,
                "staging_dataset": staging_dataset_id,
                "table_names": [result["table_name"] for result in promoted_tables],
                "cdc_baselines": {
                    result["table_name"]: result["cdc_baseline"]
                    for result in promoted_tables
                    if result["cdc_baseline"] is not None
                },
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    finally:
        bigquery_client.close()
        storage_client.close()


if __name__ == "__main__":
    main()
