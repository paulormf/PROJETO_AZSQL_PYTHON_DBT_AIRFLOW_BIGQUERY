"""Extract Azure SQL CDC changes from durable BigQuery checkpoints to GCS."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from sqlalchemy import Connection, Engine, text

from src.azure_sql import get_engine
from src.export_config import load_enabled_exports, quote_identifier


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LSN_PATTERN = re.compile(r"^0x[0-9A-F]{20}$")


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def format_lsn(value: Any) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if not isinstance(value, bytes) or len(value) != 10:
        raise RuntimeError(f"Invalid LSN returned by Azure SQL: {value!r}")
    return f"0x{value.hex().upper()}"


def parse_lsn(value: str) -> bytes:
    if not isinstance(value, str) or not LSN_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid LSN format: {value!r}")
    return bytes.fromhex(value[2:])


def write_json_marker(bucket: storage.Bucket, object_name: str, payload: dict[str, Any]) -> None:
    bucket.blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
        if_generation_match=0,
    )


def get_checkpoints(
    client: bigquery.Client,
    checkpoint_table_id: str,
    table_names: list[str],
    location: str,
) -> dict[str, dict[str, str]]:
    try:
        client.get_table(checkpoint_table_id)
    except NotFound as error:
        raise RuntimeError(
            "CDC checkpoint table is not initialized. Load the successful full baseline in BigQuery first."
        ) from error
    sql = f"""
    SELECT table_name, source_schema, source_table, capture_instance, last_lsn
    FROM `{checkpoint_table_id}`
    WHERE table_name IN UNNEST(@table_names)
    """
    job = client.query(
        sql,
        location=location,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("table_names", "STRING", table_names)]
        ),
    )
    rows = list(job.result())
    checkpoints = {row.table_name: dict(row.items()) for row in rows}
    if len(checkpoints) != len(rows):
        raise RuntimeError("Duplicate rows found in the CDC checkpoint table")
    missing = set(table_names) - set(checkpoints)
    if missing:
        raise RuntimeError(
            "CDC baseline checkpoint is missing for "
            f"{', '.join(sorted(missing))}. Run and load the full baseline first."
        )
    return checkpoints


def get_source_columns(connection: Connection, export: dict[str, Any]) -> list[str]:
    configured_columns = export["columns"]
    if configured_columns != "*":
        columns = configured_columns
    else:
        rows = connection.execute(
            text(
                """
                SELECT c.name
                FROM sys.columns AS c
                INNER JOIN sys.tables AS t ON t.object_id = c.object_id
                INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                WHERE s.name = :schema_name AND t.name = :table_name
                ORDER BY c.column_id
                """
            ),
            {"schema_name": export["schema"], "table_name": export["table"]},
        ).scalars().all()
        columns = list(rows)
    if not columns:
        raise RuntimeError(f"No source columns found for {export['schema']}.{export['table']}")
    for column in columns:
        quote_identifier(column)
    if not set(export["primary_key"]).issubset(columns):
        raise RuntimeError(f"CDC primary key is not present in captured columns for {export['name']}")
    reserved = {
        "_cdc_start_lsn",
        "_cdc_seqval",
        "_cdc_operation",
        "_cdc_update_mask",
        "_cdc_ingested_at",
    }
    if reserved.intersection(columns):
        raise RuntimeError(f"Source table {export['name']} uses reserved CDC metadata column names")
    return columns


def get_table_capture_metadata(
    connection: Connection, export: dict[str, Any]
) -> dict[str, Any]:
    row = connection.execute(
        text(
            """
            SELECT
                ct.capture_instance,
                ct.start_lsn AS capture_start_lsn,
                sys.fn_cdc_get_min_lsn(ct.capture_instance) AS min_lsn
            FROM cdc.change_tables AS ct
            INNER JOIN sys.tables AS t ON t.object_id = ct.source_object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name AND t.name = :table_name
            """
        ),
        {"schema_name": export["schema"], "table_name": export["table"]},
    ).mappings().all()
    if len(row) != 1:
        raise RuntimeError(f"Expected one CDC capture instance for {export['schema']}.{export['table']}")
    metadata = row[0]
    capture_instance = metadata["capture_instance"]
    if not isinstance(capture_instance, str) or not IDENTIFIER_PATTERN.fullmatch(capture_instance):
        raise RuntimeError(f"Unexpected CDC capture instance name: {capture_instance!r}")
    return {
        "capture_instance": capture_instance,
        "capture_start_lsn": format_lsn(metadata["capture_start_lsn"]),
        "min_lsn": format_lsn(metadata["min_lsn"]),
    }


def build_changes_query(capture_instance: str, columns: list[str]) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(capture_instance):
        raise ValueError(f"Invalid CDC capture instance: {capture_instance!r}")
    selected_columns = [
        "[__$start_lsn] AS [_cdc_start_lsn]",
        "[__$seqval] AS [_cdc_seqval]",
        "[__$operation] AS [_cdc_operation]",
        "[__$update_mask] AS [_cdc_update_mask]",
        *(quote_identifier(column) for column in columns),
    ]
    function_name = f"[cdc].[fn_cdc_get_all_changes_{capture_instance}]"
    return (
        f"SELECT {', '.join(selected_columns)} "
        f"FROM {function_name}(:from_lsn, :to_lsn, 'all')"
    )


def export_changes(
    connection: Connection,
    bucket: storage.Bucket,
    export: dict[str, Any],
    capture_instance: str,
    from_lsn: bytes,
    to_lsn: bytes,
    object_prefix: str,
    chunk_rows: int,
) -> dict[str, Any]:
    columns = get_source_columns(connection, export)
    query = text(build_changes_query(capture_instance, columns))
    chunks = pd.read_sql_query(
        query,
        connection,
        params={"from_lsn": from_lsn, "to_lsn": to_lsn},
        chunksize=chunk_rows,
        dtype_backend="pyarrow",
    )
    files: list[dict[str, Any]] = []
    total_rows = 0
    for part_number, dataframe in enumerate(chunks, start=1):
        for column in ("_cdc_start_lsn", "_cdc_seqval", "_cdc_update_mask"):
            dataframe[column] = dataframe[column].map(
                lambda value: (
                    value.tobytes()
                    if isinstance(value, memoryview)
                    else bytes(value)
                    if isinstance(value, bytearray)
                    else value
                )
            )
        dataframe["_cdc_ingested_at"] = datetime.now(timezone.utc)
        object_name = f"{object_prefix}/part-{part_number:05d}.parquet"
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            parquet_path = Path(tmp.name)
        try:
            dataframe.to_parquet(
                parquet_path,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            blob = bucket.blob(object_name)
            blob.upload_from_filename(
                str(parquet_path),
                content_type="application/vnd.apache.parquet",
                if_generation_match=0,
            )
            blob.reload()
        finally:
            parquet_path.unlink(missing_ok=True)
        row_count = len(dataframe)
        total_rows += row_count
        files.append({"name": object_name, "rows": row_count, "size_bytes": blob.size})
        print(f"{export['name']}: CDC enviou {row_count} registros para gs://{bucket.name}/{object_name}")
        del dataframe
    return {"change_count": total_rows, "file_count": len(files), "files": files}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract Azure SQL CDC changes from BigQuery checkpoints to GCS."
    )
    parser.parse_args()
    load_dotenv()
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
    bucket_name = required_setting("GCS_BUCKET")
    base_prefix = os.getenv("GCS_PREFIX", "raw").strip("/")
    location = os.getenv("BQ_LOCATION", "us-central1")
    default_chunk_rows = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))
    checkpoint_table_id = f"{project_id}.{bronze_dataset}._cdc_checkpoints"
    if default_chunk_rows <= 0:
        raise ValueError("EXPORT_CHUNK_ROWS must be greater than zero")

    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id, location=location)
    engine: Engine = get_engine()
    try:
        checkpoints = get_checkpoints(
            bq_client, checkpoint_table_id, [item["name"] for item in exports], location
        )
        run_time = datetime.now(timezone.utc)
        run_id = f"{run_time:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
        bucket = storage_client.bucket(bucket_name)
        table_results: list[dict[str, Any]] = []

        with engine.connect() as connection:
            connection = connection.execution_options(isolation_level="SNAPSHOT")
            with connection.begin():
                snapshot_state = connection.exec_driver_sql(
                    "SELECT snapshot_isolation_state_desc "
                    "FROM sys.databases WHERE database_id = DB_ID()"
                ).scalar_one()
                if snapshot_state != "ON":
                    raise RuntimeError("SNAPSHOT isolation must remain enabled for CDC extraction")
                source_to_lsn = format_lsn(
                    connection.execute(text("SELECT sys.fn_cdc_get_max_lsn()")).scalar_one()
                )
                for export in exports:
                    checkpoint = checkpoints[export["name"]]
                    if (
                        checkpoint["source_schema"] != export["schema"]
                        or checkpoint["source_table"] != export["table"]
                    ):
                        raise RuntimeError(
                            f"CDC checkpoint source does not match configuration for {export['name']}"
                        )
                    previous_lsn = checkpoint["last_lsn"]
                    previous_lsn_bytes = parse_lsn(previous_lsn)
                    metadata = get_table_capture_metadata(connection, export)
                    if metadata["capture_instance"] != checkpoint["capture_instance"]:
                        raise RuntimeError(
                            f"CDC capture instance changed for {export['name']}; a new full baseline is required"
                        )
                    min_lsn_bytes = parse_lsn(metadata["min_lsn"])
                    to_lsn_bytes = parse_lsn(source_to_lsn)
                    if to_lsn_bytes < previous_lsn_bytes:
                        raise RuntimeError(
                            f"Source CDC max LSN moved behind the checkpoint for {export['name']}; "
                            "verify whether the source database was restored."
                        )

                    if to_lsn_bytes == previous_lsn_bytes:
                        table_results.append(
                            {
                                "export": export,
                                "status": "NO_CHANGES",
                                "capture_instance": metadata["capture_instance"],
                                "previous_lsn": previous_lsn,
                                "from_lsn": None,
                                "to_lsn": previous_lsn,
                                "change_count": 0,
                                "file_count": 0,
                                "files": [],
                            }
                        )
                        continue

                    from_lsn_bytes = connection.execute(
                        text("SELECT sys.fn_cdc_increment_lsn(:last_lsn)"),
                        {"last_lsn": previous_lsn_bytes},
                    ).scalar_one()
                    from_lsn = format_lsn(from_lsn_bytes)
                    from_lsn_bytes = parse_lsn(from_lsn)
                    if parse_lsn(from_lsn) < min_lsn_bytes:
                        raise RuntimeError(
                            f"CDC retention gap for {export['name']}: next LSN {from_lsn} "
                            f"is older than retained minimum {metadata['min_lsn']}. "
                            "A new full baseline is required."
                        )

                    object_prefix = (
                        f"{base_prefix}/{export['name']}/cdc/extract_date={run_time:%Y-%m-%d}"
                        f"/run_id={run_id}"
                    )
                    if parse_lsn(from_lsn) > to_lsn_bytes:
                        changes = {"change_count": 0, "file_count": 0, "files": []}
                    else:
                        chunk_rows = int(export.get("chunk_rows", default_chunk_rows))
                        if chunk_rows <= 0:
                            raise ValueError(f"chunk_rows for '{export['name']}' must be positive")
                        changes = export_changes(
                            connection,
                            bucket,
                            export,
                            metadata["capture_instance"],
                            from_lsn_bytes,
                            to_lsn_bytes,
                            object_prefix,
                            chunk_rows,
                        )
                    table_results.append(
                        {
                            "export": export,
                            "status": "SUCCEEDED",
                            "capture_instance": metadata["capture_instance"],
                            "previous_lsn": previous_lsn,
                            "from_lsn": from_lsn,
                            "to_lsn": source_to_lsn,
                            "object_prefix": object_prefix,
                            **changes,
                        }
                    )

        # Publish success markers only after every configured table is extracted.
        for result in table_results:
            export = result["export"]
            object_prefix = result.get("object_prefix") or (
                f"{base_prefix}/{export['name']}/cdc/extract_date={run_time:%Y-%m-%d}"
                f"/run_id={run_id}"
            )
            manifest = {
                "manifest_version": 1,
                "status": result["status"],
                "run_id": run_id,
                "table_name": export["name"],
                "source": {"schema": export["schema"], "table": export["table"]},
                "capture_instance": result.get("capture_instance", checkpoints[export["name"]]["capture_instance"]),
                "previous_lsn": result["previous_lsn"],
                "from_lsn": result["from_lsn"],
                "to_lsn": result["to_lsn"],
                "started_at": run_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "change_count": result["change_count"],
                "file_count": result["file_count"],
                "files": result["files"],
                "reconciliation": {"status": "MATCHED", "change_count": result["change_count"]},
            }
            write_json_marker(bucket, f"{object_prefix}/_SUCCESS.json", manifest)
            result["object_prefix"] = object_prefix

        run_manifest = {
            "manifest_version": 1,
            "status": "SUCCEEDED",
            "mode": "cdc",
            "run_id": run_id,
            "started_at": run_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "table_names": [result["export"]["name"] for result in table_results],
            "batches": {
                result["export"]["name"]: {
                    "status": result["status"],
                    "previous_lsn": result["previous_lsn"],
                    "from_lsn": result["from_lsn"],
                    "to_lsn": result["to_lsn"],
                }
                for result in table_results
            },
        }
        write_json_marker(
            bucket,
            f"{base_prefix}/_cdc_runs/run_id={run_id}/_CDC_RUN_SUCCESS.json",
            run_manifest,
        )
        print(f"CDC_RUN_ID={run_id}")
        for result in table_results:
            print(
                f"{result['export']['name']}: {result['status']}; "
                f"{result['change_count']} alterações; checkpoint até {result['to_lsn']}"
            )
    finally:
        engine.dispose()
        bq_client.close()
        storage_client.close()


if __name__ == "__main__":
    main()
