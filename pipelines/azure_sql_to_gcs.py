"""Export enabled Azure SQL tables as one consistent Parquet snapshot in GCS."""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import Connection, Engine, text

from src.azure_sql import get_engine
from src.export_config import build_count_query, build_select_query, load_enabled_exports


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def export_table(
    connection: Connection,
    bucket: storage.Bucket,
    export: dict[str, Any],
    object_prefix: str,
    chunk_rows: int,
) -> dict[str, Any]:
    """Export one table using the caller's open SNAPSHOT transaction."""
    source_count = int(
        connection.exec_driver_sql(build_count_query(export)).scalar_one()
    )
    total_rows = 0
    files: list[dict[str, Any]] = []
    query = build_select_query(export)
    chunks = pd.read_sql_query(
        query,
        connection,
        chunksize=chunk_rows,
        dtype_backend="pyarrow",
    )

    for part_number, dataframe in enumerate(chunks, start=1):
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
        finally:
            parquet_path.unlink(missing_ok=True)

        row_count = len(dataframe)
        total_rows += row_count
        blob.reload()
        files.append({"name": object_name, "rows": row_count, "size_bytes": blob.size})
        print(f"{export['name']}: enviado gs://{bucket.name}/{object_name} ({row_count} linhas)")
        del dataframe

    return {
        "source_count": source_count,
        "exported_count": total_rows,
        "file_count": len(files),
        "files": files,
    }


def write_json_marker(
    bucket: storage.Bucket, object_name: str, payload: dict[str, Any]
) -> None:
    """Create an immutable JSON marker in GCS."""
    bucket.blob(object_name).upload_from_string(
        json.dumps(payload, ensure_ascii=False, indent=2),
        content_type="application/json",
        if_generation_match=0,
    )


def lsn_hex(value: Any) -> str:
    """Format SQL Server binary(10) LSN values consistently for manifests."""
    if value is None:
        raise RuntimeError("Azure SQL returned an empty CDC LSN")
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if not isinstance(value, bytes) or len(value) != 10:
        raise RuntimeError(f"Unexpected CDC LSN value returned by Azure SQL: {value!r}")
    return f"0x{value.hex().upper()}"


def capture_cdc_baselines(
    engine: Engine, exports: list[dict[str, Any]]
) -> dict[str, dict[str, str]]:
    """Read the CDC high watermark before starting the full SNAPSHOT transaction."""
    cdc_exports = [item for item in exports if item.get("incremental_strategy") == "cdc"]
    if not cdc_exports:
        return {}

    statement = text(
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
    )
    capture_metadata: dict[str, dict[str, Any]] = {}
    with engine.connect() as connection:
        for export in cdc_exports:
            rows = connection.execute(
                statement,
                {"schema_name": export["schema"], "table_name": export["table"]},
            ).mappings().all()
            if len(rows) != 1:
                raise RuntimeError(
                    f"Expected exactly one CDC capture instance for "
                    f"{export['schema']}.{export['table']}; found {len(rows)}"
                )
            row = rows[0]
            start_lsn = lsn_hex(row["capture_start_lsn"])
            min_lsn = lsn_hex(row["min_lsn"])
            capture_metadata[export["name"]] = {
                "capture_instance": row["capture_instance"],
                "capture_start_lsn": start_lsn,
                "min_lsn_at_baseline": min_lsn,
            }

        baseline_lsn = lsn_hex(
            connection.execute(text("SELECT sys.fn_cdc_get_max_lsn()")).scalar_one()
        )

    baselines: dict[str, dict[str, str]] = {}
    for export in cdc_exports:
        metadata = capture_metadata[export["name"]]
        baseline_bytes = bytes.fromhex(baseline_lsn[2:])
        if baseline_bytes < bytes.fromhex(metadata["capture_start_lsn"][2:]):
            raise RuntimeError(
                f"CDC capture for {export['schema']}.{export['table']} has not "
                "processed changes through its start LSN yet. Retry after CDC capture advances."
            )
        if baseline_bytes < bytes.fromhex(metadata["min_lsn_at_baseline"][2:]):
            raise RuntimeError(
                f"CDC baseline LSN is older than the retained minimum for "
                f"{export['schema']}.{export['table']}; inspect CDC retention."
            )
        baselines[export["name"]] = {**metadata, "baseline_lsn": baseline_lsn}
    return baselines


def main() -> None:
    load_dotenv()
    config_path = Path(os.getenv("EXPORT_CONFIG_PATH", "config/gcs_exports.yaml"))
    exports = load_enabled_exports(config_path)
    if not exports:
        print("Nenhuma tabela habilitada para exportação.")
        return

    bucket_name = required_setting("GCS_BUCKET")
    base_prefix = os.getenv("GCS_PREFIX", "raw").strip("/")
    default_chunk_rows = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))
    if default_chunk_rows <= 0:
        raise ValueError("EXPORT_CHUNK_ROWS must be greater than zero")

    run_time = datetime.now(timezone.utc)
    run_id = f"{run_time:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    storage_client = storage.Client()
    engine = get_engine()
    table_results: list[dict[str, Any]] = []

    try:
        bucket = storage_client.bucket(bucket_name)
        # The watermark must precede the SNAPSHOT. Changes after this point can
        # be replayed over the full baseline; taking it later could skip changes.
        cdc_baselines = capture_cdc_baselines(engine, exports)
        cdc_baseline_captured_at = datetime.now(timezone.utc).isoformat()
        # One transaction gives every table and its count the same database view.
        with engine.connect() as connection:
            connection = connection.execution_options(isolation_level="SNAPSHOT")
            with connection.begin():
                snapshot_state = connection.exec_driver_sql(
                    "SELECT snapshot_isolation_state_desc "
                    "FROM sys.databases WHERE database_id = DB_ID()"
                ).scalar_one()
                if snapshot_state != "ON":
                    raise RuntimeError(
                        "SNAPSHOT isolation is unavailable for the source database. "
                        "Ask the database administrator to enable ALLOW_SNAPSHOT_ISOLATION."
                    )

                for export in exports:
                    chunk_rows = int(export.get("chunk_rows", default_chunk_rows))
                    if chunk_rows <= 0:
                        raise ValueError(
                            f"chunk_rows for '{export['name']}' must be greater than zero"
                        )
                    object_prefix = (
                        f"{base_prefix}/{export['name']}/extract_date={run_time:%Y-%m-%d}"
                        f"/run_id={run_id}"
                    )
                    result = export_table(
                        connection, bucket, export, object_prefix, chunk_rows
                    )
                    if result["source_count"] != result["exported_count"]:
                        raise RuntimeError(
                            f"Reconciliation failed for {export['name']}: "
                            f"Azure SQL={result['source_count']}, "
                            f"Parquet={result['exported_count']}"
                        )
                    table_results.append(
                        {"export": export, "object_prefix": object_prefix, **result}
                    )

        # Markers appear only after the whole database snapshot transaction commits.
        for result in table_results:
            export = result["export"]
            manifest = {
                "manifest_version": 2,
                "status": "SUCCEEDED",
                "run_id": run_id,
                "table_name": export["name"],
                "source": {"schema": export["schema"], "table": export["table"]},
                "load_mode": export["load_mode"],
                "incremental_strategy": export.get("incremental_strategy", "full_refresh"),
                "snapshot_isolation": "SNAPSHOT",
                "cdc_baseline": cdc_baselines.get(export["name"]),
                "cdc_baseline_captured_at": (
                    cdc_baseline_captured_at
                    if export["name"] in cdc_baselines
                    else None
                ),
                "started_at": run_time.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "source_count": result["source_count"],
                "exported_count": result["exported_count"],
                "file_count": result["file_count"],
                "files": result["files"],
                "reconciliation": {
                    "status": "MATCHED",
                    "source_count": result["source_count"],
                    "exported_count": result["exported_count"],
                },
            }
            write_json_marker(
                bucket, f"{result['object_prefix']}/_SUCCESS.json", manifest
            )

        run_manifest = {
            "manifest_version": 2,
            "status": "SUCCEEDED",
            "run_id": run_id,
            "snapshot_isolation": "SNAPSHOT",
            "cdc_baselines": cdc_baselines,
            "cdc_baseline_captured_at": (
                cdc_baseline_captured_at if cdc_baselines else None
            ),
            "started_at": run_time.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "table_names": [result["export"]["name"] for result in table_results],
        }
        write_json_marker(
            bucket,
            f"{base_prefix}/_runs/run_id={run_id}/_RUN_SUCCESS.json",
            run_manifest,
        )

        for result in table_results:
            print(
                f"Exportação concluída para {result['export']['name']}: "
                f"{result['exported_count']} linhas, {result['file_count']} arquivos."
            )
        print(f"RUN_ID={run_id}")
    finally:
        engine.dispose()
        storage_client.close()


if __name__ == "__main__":
    main()
