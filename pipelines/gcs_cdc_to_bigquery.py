"""Apply immutable Azure SQL CDC batches to BigQuery history and current Bronze."""

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
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LSN_PATTERN = re.compile(r"^0x[0-9A-F]{20}$")
CDC_METADATA_COLUMNS = {
    "_cdc_start_lsn",
    "_cdc_seqval",
    "_cdc_operation",
    "_cdc_update_mask",
    "_cdc_ingested_at",
}


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def validate_lsn(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not LSN_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {field_name} in CDC manifest: {value!r}")
    return value


def bq_table(table_id: str) -> str:
    return f"`{table_id}`"


def write_json_marker(bucket: storage.Bucket, object_name: str, payload: dict[str, Any]) -> None:
    bucket.blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
    )


def get_current_checkpoints(
    client: bigquery.Client,
    checkpoint_table_id: str,
    table_names: list[str],
    location: str,
) -> dict[str, dict[str, str]]:
    try:
        client.get_table(checkpoint_table_id)
    except NotFound as error:
        raise RuntimeError(
            "CDC checkpoint table does not exist. Re-run the BigQuery full loader "
            "for the successful baseline RUN_ID first."
        ) from error
    rows = list(client.query(
        f"SELECT table_name, capture_instance, last_lsn, last_run_id "
        f"FROM `{checkpoint_table_id}` WHERE table_name IN UNNEST(@table_names)",
        location=location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("table_names", "STRING", table_names)]
        ),
    ).result())
    checkpoints = {row.table_name: dict(row.items()) for row in rows}
    if len(checkpoints) != len(rows):
        raise RuntimeError("Duplicate rows found in the CDC checkpoint table")
    missing = set(table_names) - set(checkpoints)
    if missing:
        raise RuntimeError(f"Missing CDC checkpoints for: {', '.join(sorted(missing))}")
    return checkpoints


def load_batch_manifest(
    bucket: storage.Bucket, object_prefix: str, run_id: str, export: dict[str, Any]
) -> dict[str, Any]:
    blob = bucket.blob(f"{object_prefix}/_SUCCESS.json")
    if not blob.exists():
        raise FileNotFoundError(f"CDC manifest missing: gs://{bucket.name}/{blob.name}")
    manifest = json.loads(blob.download_as_text())
    if (
        manifest.get("manifest_version") != 1
        or manifest.get("run_id") != run_id
        or manifest.get("table_name") != export["name"]
        or manifest.get("source")
        != {"schema": export["schema"], "table": export["table"]}
        or manifest.get("status") not in {"SUCCEEDED", "NO_CHANGES"}
        or manifest.get("reconciliation", {}).get("change_count")
        != manifest.get("change_count")
    ):
        raise ValueError(f"Invalid CDC manifest: gs://{bucket.name}/{blob.name}")
    contract = manifest.get("schema_contract")
    if (
        not isinstance(contract, dict)
        or contract.get("version") != SCHEMA_CONTRACT_VERSION
        or not isinstance(contract.get("fields"), list)
        or not contract["fields"]
    ):
        raise ValueError(f"Missing or unsupported schema contract for {export['name']}")
    validate_lsn(manifest.get("previous_lsn"), "previous_lsn")
    validate_lsn(manifest.get("to_lsn"), "to_lsn")
    if manifest["status"] == "SUCCEEDED":
        validate_lsn(manifest.get("from_lsn"), "from_lsn")
        previous_lsn = bytes.fromhex(manifest["previous_lsn"][2:])
        from_lsn = bytes.fromhex(manifest["from_lsn"][2:])
        to_lsn = bytes.fromhex(manifest["to_lsn"][2:])
        if not previous_lsn < from_lsn <= to_lsn:
            raise ValueError(f"CDC batch has a non-advancing interval for {export['name']}")
    elif manifest.get("change_count") != 0 or manifest.get("file_count") != 0:
        raise ValueError(f"NO_CHANGES manifest has data for {export['name']}")
    return manifest


def verify_batch_objects(
    storage_client: storage.Client,
    bucket_name: str,
    object_prefix: str,
    manifest: dict[str, Any],
) -> list[str]:
    blobs = [
        blob
        for blob in storage_client.list_blobs(bucket_name, prefix=f"{object_prefix}/")
        if blob.name.endswith(".parquet")
    ]
    files = manifest.get("files")
    if not isinstance(files, list):
        raise ValueError("CDC manifest files must be a list")
    expected = {item.get("name"): item for item in files if isinstance(item, dict)}
    if (
        len(expected) != len(files)
        or len(files) != int(manifest.get("file_count", -1))
        or {blob.name for blob in blobs} != set(expected)
        or sum(int(item.get("rows", -1)) for item in files)
        != int(manifest.get("change_count", -2))
        or any(
            blob.size != int(expected[blob.name].get("size_bytes", -1))
            for blob in blobs
        )
    ):
        raise ValueError("CDC Parquet objects do not match the manifest")
    if manifest["change_count"] > 0 and not blobs:
        raise ValueError("CDC manifest reports changes but has no Parquet objects")
    return [blob.name for blob in blobs]


def quote_column(name: str) -> str:
    if not isinstance(name, str) or not IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"Unsupported BigQuery column identifier: {name!r}")
    return f"`{name}`"


def compare_schema_fields(
    expected: dict[str, str], actual: dict[str, str]
) -> tuple[list[str], list[str], list[tuple[str, str, str]]]:
    """Return missing, unexpected, and type-mismatched fields, independent of order."""
    expected_names = set(expected)
    actual_names = set(actual)
    missing = sorted(expected_names - actual_names)
    unexpected = sorted(actual_names - expected_names)
    type_mismatches = sorted(
        (name, expected[name], actual[name])
        for name in expected_names & actual_names
        if expected[name] != actual[name]
    )
    return missing, unexpected, type_mismatches


def validate_stage_data(
    client: bigquery.Client,
    staging_table_id: str,
    export: dict[str, Any],
    location: str,
) -> list[str]:
    table = client.get_table(staging_table_id)
    columns = [field.name for field in table.schema]
    if not CDC_METADATA_COLUMNS.issubset(columns):
        raise ValueError(f"CDC stage table is missing metadata columns: {staging_table_id}")
    source_columns = [column for column in columns if column not in CDC_METADATA_COLUMNS]
    for column in source_columns:
        quote_column(column)
    if not set(export["primary_key"]).issubset(source_columns):
        raise ValueError(f"CDC stage table is missing key columns for {export['name']}")

    invalid_operation = next(iter(client.query(
        f"SELECT COUNTIF(_cdc_operation IS NULL OR _cdc_operation NOT IN (1, 2, 4)) AS invalid_count, "
        f"COUNTIF(_cdc_start_lsn IS NULL OR _cdc_seqval IS NULL) AS invalid_metadata "
        f"FROM `{staging_table_id}`",
        location=location,
    ).result()))
    if int(invalid_operation["invalid_count"]) or int(invalid_operation["invalid_metadata"]):
        raise ValueError(f"CDC stage contains invalid operation metadata for {export['name']}")

    key_columns = ", ".join(quote_column(column) for column in export["primary_key"])
    event_key_columns = ", ".join(
        ["_cdc_start_lsn", "_cdc_seqval", "_cdc_operation", key_columns]
    )
    duplicate = client.query(
        f"SELECT 1 FROM `{staging_table_id}` GROUP BY {event_key_columns} "
        "HAVING COUNT(*) > 1 LIMIT 1",
        location=location,
    ).result().total_rows
    if duplicate:
        raise ValueError(f"CDC batch has duplicate event identities for {export['name']}")
    null_key = " OR ".join(f"{quote_column(column)} IS NULL" for column in export["primary_key"])
    if client.query(
        f"SELECT 1 FROM `{staging_table_id}` WHERE {null_key} LIMIT 1", location=location
    ).result().total_rows:
        raise ValueError(f"CDC batch has null primary keys for {export['name']}")
    return source_columns


def compare_bronze_and_stage_schemas(
    client: bigquery.Client,
    bronze_table_id: str,
    staging_table_id: str,
    location: str,
) -> tuple[list[str], list[str], list[tuple[str, str, str]]]:
    """Require the CDC batch schema to match the existing Bronze schema exactly."""
    bronze_schema = {
        field.name: field.field_type for field in client.get_table(bronze_table_id).schema
    }
    staging_schema = {
        field.name: field.field_type
        for field in client.get_table(staging_table_id).schema
        if field.name not in CDC_METADATA_COLUMNS
    }

    return compare_schema_fields(bronze_schema, staging_schema)


def ensure_history_table(
    client: bigquery.Client,
    history_table_id: str,
    staging_table_id: str,
    primary_key: list[str],
) -> None:
    staged = client.get_table(staging_table_id)
    try:
        history = client.get_table(history_table_id)
    except NotFound:
        history = bigquery.Table(history_table_id, schema=staged.schema)
        history.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_cdc_ingested_at",
        )
        history.clustering_fields = primary_key[:4]
        client.create_table(history)
        return
    staged_schema = {field.name: field.field_type for field in staged.schema}
    history_schema = {field.name: field.field_type for field in history.schema}
    missing_columns, unexpected_columns, type_mismatches = compare_schema_fields(
        history_schema, staged_schema
    )
    if missing_columns or unexpected_columns or type_mismatches:
        raise ValueError(
            f"CDC history schema changed for {history_table_id}; "
            f"missing_in_batch={missing_columns}, unexpected_in_batch={unexpected_columns}, "
            f"type_mismatches={type_mismatches}. Migrate it explicitly."
        )


def build_apply_sql(
    checkpoint_table_id: str,
    target_table_id: str,
    history_table_id: str,
    staging_table_id: str | None,
    export: dict[str, Any],
    source_columns: list[str],
    has_changes: bool,
) -> str:
    if CDC_METADATA_COLUMNS.intersection(source_columns):
        raise ValueError("source_columns must not include CDC metadata fields")
    checkpoint = bq_table(checkpoint_table_id)
    target = bq_table(target_table_id)
    history = bq_table(history_table_id)
    primary_key = export["primary_key"]
    match_target = " AND ".join(
        f"T.{quote_column(column)} = S.{quote_column(column)}" for column in primary_key
    )
    match_history = " AND ".join(
        [
            "T._cdc_start_lsn = S._cdc_start_lsn",
            "T._cdc_seqval = S._cdc_seqval",
            "T._cdc_operation = S._cdc_operation",
            *(
                f"T.{quote_column(column)} = S.{quote_column(column)}"
                for column in primary_key
            ),
        ]
    )
    all_stage_columns = [
        "_cdc_start_lsn",
        "_cdc_seqval",
        "_cdc_operation",
        "_cdc_update_mask",
        "_cdc_ingested_at",
        *source_columns,
    ]
    quoted_all_stage_columns = ", ".join(quote_column(column) for column in all_stage_columns)
    source_values = ", ".join(f"S.{quote_column(column)}" for column in all_stage_columns)
    updates = ", ".join(
        f"{quote_column(column)} = S.{quote_column(column)}" for column in source_columns
    )
    insert_columns = ", ".join(quote_column(column) for column in source_columns)
    insert_values = ", ".join(f"S.{quote_column(column)}" for column in source_columns)

    statements = [
        "BEGIN TRANSACTION;",
        f"ASSERT (SELECT COUNT(*) FROM {checkpoint} "
        "WHERE table_name = @table_name AND capture_instance = @capture_instance "
        "AND last_lsn = @previous_lsn) = 1 "
        "AS 'CDC checkpoint changed after extraction; re-extract from the current checkpoint.';",
    ]
    if has_changes:
        assert staging_table_id is not None
        stage = bq_table(staging_table_id)
        statements.extend(
            [
                f"MERGE {history} AS T USING {stage} AS S ON {match_history} "
                f"WHEN NOT MATCHED THEN INSERT ({quoted_all_stage_columns}) VALUES ({source_values});",
                f"MERGE {target} AS T USING ("
                f"SELECT * FROM {stage} QUALIFY ROW_NUMBER() OVER "
                f"(PARTITION BY {', '.join(quote_column(column) for column in primary_key)} "
                "ORDER BY _cdc_start_lsn DESC, _cdc_seqval DESC, _cdc_operation DESC) = 1"
                f") AS S ON {match_target} "
                "WHEN MATCHED AND S._cdc_operation = 1 THEN DELETE "
                f"WHEN MATCHED AND S._cdc_operation IN (2, 4) THEN UPDATE SET {updates} "
                f"WHEN NOT MATCHED AND S._cdc_operation IN (2, 4) THEN "
                f"INSERT ({insert_columns}) VALUES ({insert_values});",
            ]
        )
    statements.extend(
        [
            f"UPDATE {checkpoint} SET last_lsn = @to_lsn, last_run_id = @run_id, "
            "updated_at = CURRENT_TIMESTAMP() "
            "WHERE table_name = @table_name AND capture_instance = @capture_instance "
            "AND last_lsn = @previous_lsn;",
            f"ASSERT (SELECT COUNT(*) FROM {checkpoint} WHERE table_name = @table_name "
            "AND last_lsn = @to_lsn AND last_run_id = @run_id) = 1 "
            "AS 'Failed to advance CDC checkpoint.';",
            "COMMIT TRANSACTION;",
        ]
    )
    return "\n".join(statements)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Apply a completed Azure SQL CDC batch to BigQuery Bronze."
    )
    parser.add_argument("--run-id", required=True, help="CDC_RUN_ID printed by extraction")
    args = parser.parse_args()
    if not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise ValueError("Invalid CDC run ID format")

    config_path = Path(os.getenv("EXPORT_CONFIG_PATH", "config/gcs_exports.yaml"))
    exports = [
        export
        for export in load_enabled_exports(config_path)
        if export.get("incremental_strategy") == "cdc"
    ]
    if not exports:
        raise RuntimeError("No enabled exports use incremental_strategy: cdc")

    project_id = required_setting("GCP_PROJECT_ID")
    bronze_dataset = required_setting("BQ_BRONZE_DATASET")
    staging_dataset = required_setting("BQ_STAGING_DATASET")
    bucket_name = required_setting("GCS_BUCKET")
    base_prefix = os.getenv("GCS_PREFIX", "raw").strip("/")
    location = os.getenv("BQ_LOCATION", "us-central1")
    ttl_days = int(os.getenv("BQ_STAGING_TABLE_TTL_DAYS", "7"))
    if ttl_days <= 0:
        raise ValueError("BQ_STAGING_TABLE_TTL_DAYS must be greater than zero")
    for name in (bronze_dataset, staging_dataset):
        if not IDENTIFIER_PATTERN.fullmatch(name):
            raise ValueError(f"Invalid BigQuery dataset ID: {name!r}")
    if bronze_dataset == staging_dataset:
        raise ValueError("BQ_BRONZE_DATASET and BQ_STAGING_DATASET must be different")

    bq_client = bigquery.Client(project=project_id, location=location)
    storage_client = storage.Client(project=project_id)
    try:
        bronze_dataset_id = f"{project_id}.{bronze_dataset}"
        staging_dataset_id = f"{project_id}.{staging_dataset}"
        checkpoint_table_id = f"{bronze_dataset_id}._cdc_checkpoints"
        bronze_dataset_metadata = bq_client.get_dataset(bronze_dataset_id)
        staging_dataset_metadata = bq_client.get_dataset(staging_dataset_id)
        for dataset in (bronze_dataset_metadata, staging_dataset_metadata):
            if dataset.location.lower() != location.lower():
                raise ValueError(
                    f"Dataset {dataset.dataset_id} is in {dataset.location}; expected {location}"
                )
        bucket = storage_client.get_bucket(bucket_name)
        if bucket.location.lower() != location.lower():
            raise ValueError(f"GCS bucket must be in {location}; found {bucket.location}")

        run_path = f"{base_prefix}/_cdc_runs/run_id={args.run_id}/_CDC_RUN_SUCCESS.json"
        run_blob = bucket.blob(run_path)
        if not run_blob.exists():
            raise FileNotFoundError(f"CDC run marker not found: gs://{bucket_name}/{run_path}")
        run_manifest = json.loads(run_blob.download_as_text())
        table_names = {export["name"] for export in exports}
        if (
            run_manifest.get("manifest_version") != 1
            or run_manifest.get("status") != "SUCCEEDED"
            or run_manifest.get("mode") != "cdc"
            or run_manifest.get("run_id") != args.run_id
            or set(run_manifest.get("table_names", [])) != table_names
        ):
            raise ValueError("CDC run manifest does not match current configuration")

        checkpoints = get_current_checkpoints(
            bq_client,
            checkpoint_table_id,
            [export["name"] for export in exports],
            location,
        )
        batch_results: list[dict[str, Any]] = []
        for export in exports:
            table = export["table"]
            for identifier in (table, export["name"]):
                if not IDENTIFIER_PATTERN.fullmatch(identifier):
                    raise ValueError(f"Invalid table identifier: {identifier!r}")
            extract_date = datetime.strptime(args.run_id[:8], "%Y%m%d").strftime(
                "%Y-%m-%d"
            )
            object_prefix = (
                f"{base_prefix}/{export['name']}/cdc/extract_date={extract_date}"
                f"/run_id={args.run_id}"
            )
            manifest = load_batch_manifest(bucket, object_prefix, args.run_id, export)
            if run_manifest.get("batches", {}).get(export["name"]) != {
                "status": manifest["status"],
                "previous_lsn": manifest["previous_lsn"],
                "from_lsn": manifest["from_lsn"],
                "to_lsn": manifest["to_lsn"],
            }:
                raise ValueError(f"CDC batch manifest does not match run manifest for {export['name']}")
            verify_batch_objects(
                storage_client, bucket_name, object_prefix, manifest
            )
            bronze_table_id = f"{bronze_dataset_id}.{table}"
            bronze_table = bq_client.get_table(bronze_table_id)
            missing, unexpected, mismatches = compare_contract_types(
                manifest["schema_contract"], bronze_table.schema
            )
            if missing or unexpected or mismatches:
                raise ValueError(
                    f"Azure schema contract differs from Bronze table {bronze_table_id}: "
                    f"missing={missing}, unexpected={unexpected}, type_mismatches={mismatches}. "
                    "Run an intentional full rebaseline/schema migration first."
                )
            if manifest["status"] == "NO_CHANGES":
                checkpoint = checkpoints[export["name"]]
                if (
                    checkpoint["capture_instance"] != manifest["capture_instance"]
                    or checkpoint["last_lsn"] != manifest["previous_lsn"]
                    or manifest["to_lsn"] != manifest["previous_lsn"]
                ):
                    raise RuntimeError(f"NO_CHANGES batch checkpoint mismatch for {export['name']}")
                batch_results.append(
                    {
                        "export": export,
                        "manifest": manifest,
                        "object_prefix": object_prefix,
                        "status": "NO_CHANGES",
                        "staging_table_id": None,
                        "history_table_id": None,
                        "source_columns": [],
                        "merge_job_id": None,
                    }
                )
                continue

            previous_lsn = manifest["previous_lsn"]
            to_lsn = manifest["to_lsn"]
            checkpoint = checkpoints[export["name"]]
            if checkpoint["capture_instance"] != manifest["capture_instance"]:
                raise RuntimeError(f"Capture instance checkpoint mismatch for {export['name']}")
            if checkpoint["last_lsn"] == to_lsn and checkpoint["last_run_id"] == args.run_id:
                batch_results.append(
                    {
                        "export": export,
                        "manifest": manifest,
                        "object_prefix": object_prefix,
                        "status": "ALREADY_APPLIED",
                        "staging_table_id": None,
                        "history_table_id": (
                            f"{bronze_dataset_id}._cdc_history_{table}"
                            if manifest["change_count"] > 0
                            else None
                        ),
                        "source_columns": [],
                        "merge_job_id": None,
                    }
                )
                continue
            if checkpoint["last_lsn"] != previous_lsn:
                raise RuntimeError(
                    f"CDC checkpoint mismatch for {export['name']}: "
                    f"current={checkpoint['last_lsn']}, batch starts after={previous_lsn}"
                )
            staging_table_id = None
            history_table_id = f"{bronze_dataset_id}._cdc_history_{table}"
            source_columns: list[str] = []
            if manifest["change_count"] > 0:
                source_uri = (
                    f"gs://{bucket_name}/{object_prefix}/*.parquet"
                )
                staging_table_id = (
                    f"{staging_dataset_id}._cdc_stg_{table}_{args.run_id.replace('-', '_')}"
                )
                load_job = bq_client.load_table_from_uri(
                    source_uri,
                    staging_table_id,
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        schema=[
                            bigquery.SchemaField("_cdc_start_lsn", "BYTES", mode="NULLABLE"),
                            bigquery.SchemaField("_cdc_seqval", "BYTES", mode="NULLABLE"),
                            bigquery.SchemaField("_cdc_operation", "INTEGER", mode="REQUIRED"),
                            bigquery.SchemaField("_cdc_update_mask", "BYTES", mode="NULLABLE"),
                            *[
                                bigquery.SchemaField(**field)
                                for field in bigquery_schema_fields(manifest["schema_contract"])
                            ],
                            bigquery.SchemaField(
                                "_cdc_ingested_at", "TIMESTAMP", mode="REQUIRED"
                            ),
                        ],
                        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ),
                    location=location,
                )
                load_job.result()
                stage_table = bq_client.get_table(staging_table_id)
                if int(stage_table.num_rows) != int(manifest["change_count"]):
                    raise RuntimeError(
                        f"CDC staging row reconciliation failed for {staging_table_id}: "
                        f"manifest={manifest['change_count']}, BigQuery={stage_table.num_rows}"
                    )
                source_columns = validate_stage_data(
                    bq_client, staging_table_id, export, location
                )
                missing_columns, unexpected_columns, type_mismatches = (
                    compare_bronze_and_stage_schemas(
                        bq_client, bronze_table_id, staging_table_id, location
                    )
                )
                if missing_columns or unexpected_columns or type_mismatches:
                    raise ValueError(
                        f"Source schema changed for {bronze_table_id}; "
                        f"missing_in_cdc={missing_columns}, "
                        f"unexpected_in_cdc={unexpected_columns}, "
                        f"type_mismatches={type_mismatches}. "
                        "Run a full refresh/schema migration before applying this batch."
                    )
                ensure_history_table(
                    bq_client,
                    history_table_id,
                    staging_table_id,
                    export["primary_key"],
                )

            sql = build_apply_sql(
                checkpoint_table_id,
                f"{bronze_dataset_id}.{table}",
                history_table_id,
                staging_table_id,
                export,
                source_columns,
                bool(manifest["change_count"]),
            )
            job = bq_client.query(
                sql,
                location=location,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("table_name", "STRING", export["name"]),
                        bigquery.ScalarQueryParameter("capture_instance", "STRING", manifest["capture_instance"]),
                        bigquery.ScalarQueryParameter("previous_lsn", "STRING", previous_lsn),
                        bigquery.ScalarQueryParameter("to_lsn", "STRING", to_lsn),
                        bigquery.ScalarQueryParameter("run_id", "STRING", args.run_id),
                    ]
                ),
            )
            job.result()
            if staging_table_id:
                stage_table = bq_client.get_table(staging_table_id)
                stage_table.expires = datetime.now(timezone.utc) + timedelta(days=ttl_days)
                try:
                    bq_client.update_table(stage_table, ["expires"])
                except (PreconditionFailed, GoogleAPICallError) as error:
                    print(f"Warning: could not set CDC staging expiration for {staging_table_id}: {error}")
            batch_results.append(
                {
                    "export": export,
                    "manifest": manifest,
                    "object_prefix": object_prefix,
                    "status": "SUCCEEDED",
                    "staging_table_id": staging_table_id,
                    "history_table_id": (
                        history_table_id if manifest["change_count"] > 0 else None
                    ),
                    "source_columns": source_columns,
                    "merge_job_id": job.job_id,
                }
            )

        # O marcador de sucesso da execução só é gravado após confirmar o checkpoint de cada tabela.
        for result in batch_results:
            manifest = result["manifest"]
            write_json_marker(
                bucket,
                f"{result['object_prefix']}_BIGQUERY_SUCCESS.json",
                {
                    "manifest_version": 1,
                    "status": result["status"],
                    "run_id": args.run_id,
                    "table_name": result["export"]["name"],
                    "previous_lsn": manifest["previous_lsn"],
                    "to_lsn": manifest["to_lsn"],
                    "change_count": manifest["change_count"],
                    "history_table": result["history_table_id"],
                    "staging_table": result["staging_table_id"],
                    "bigquery_apply_job_id": result["merge_job_id"],
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        write_json_marker(
            bucket,
            f"{base_prefix}/_cdc_runs/run_id={args.run_id}/_BIGQUERY_CDC_RUN_SUCCESS.json",
            {
                "manifest_version": 1,
                "status": "SUCCEEDED",
                "run_id": args.run_id,
                "table_names": [result["export"]["name"] for result in batch_results],
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        print(f"CDC load completed: {args.run_id}")
        for result in batch_results:
            print(
                f"{result['export']['name']}: {result['status']}; "
                f"{result['manifest']['change_count']} changes; "
                f"checkpoint={result['manifest']['to_lsn']}"
            )
    finally:
        bq_client.close()
        storage_client.close()


if __name__ == "__main__":
    main()
