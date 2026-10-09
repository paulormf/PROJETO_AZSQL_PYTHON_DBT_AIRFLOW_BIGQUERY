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
from sqlalchemy import Connection

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
    chunks = pd.read_sql_query(query, connection, chunksize=chunk_rows)

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
                "manifest_version": 1,
                "status": "SUCCEEDED",
                "run_id": run_id,
                "table_name": export["name"],
                "source": {"schema": export["schema"], "table": export["table"]},
                "load_mode": export["load_mode"],
                "snapshot_isolation": "SNAPSHOT",
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
            "manifest_version": 1,
            "status": "SUCCEEDED",
            "run_id": run_id,
            "snapshot_isolation": "SNAPSHOT",
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
