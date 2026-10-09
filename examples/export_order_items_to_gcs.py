"""Export enabled Azure SQL tables as chunked Parquet objects in GCS."""

from __future__ import annotations

import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import Engine, text

from src.azure_sql import get_engine


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def quote_identifier(identifier: str) -> str:
    """Allow simple SQL identifiers and quote them for SQL Server."""
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid SQL identifier in configuration: {identifier!r}")
    return f"[{identifier}]"


def build_select_query(export: dict[str, Any]) -> str:
    schema = quote_identifier(export["schema"])
    table = quote_identifier(export["table"])
    columns = export["columns"]

    if columns == "*":
        selected_columns = "*"
    elif isinstance(columns, list) and columns:
        selected_columns = ", ".join(quote_identifier(column) for column in columns)
    else:
        raise ValueError(
            f"Export '{export['name']}' must set columns to '*' or a non-empty list"
        )

    return f"SELECT {selected_columns} FROM {schema}.{table}"


def load_enabled_exports(config_path: Path) -> list[dict[str, Any]]:
    with config_path.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    exports = config.get("exports") if isinstance(config, dict) else None
    if not isinstance(exports, list):
        raise ValueError(f"Configuration file must contain an 'exports' list: {config_path}")

    enabled_exports = []
    seen_names = set()
    for export in exports:
        if not isinstance(export, dict):
            raise ValueError("Every item in 'exports' must be a mapping")
        if not isinstance(export.get("enabled"), bool):
            raise ValueError(f"Export '{export.get('name', '<unnamed>')}' needs enabled: true/false")
        if not export["enabled"]:
            continue

        for key in ("name", "schema", "table", "columns"):
            if key not in export:
                raise ValueError(f"Enabled export is missing required setting: {key}")

        name = export["name"]
        quote_identifier(name)
        if name in seen_names:
            raise ValueError(f"Duplicate export name in configuration: {name}")
        seen_names.add(name)

        # Validate SQL identifiers and the column configuration before opening connections.
        quote_identifier(export["schema"])
        quote_identifier(export["table"])
        build_select_query(export)
        enabled_exports.append(export)

    return enabled_exports


def export_table(
    engine: Engine,
    bucket: storage.Bucket,
    export: dict[str, Any],
    object_prefix: str,
    chunk_rows: int,
) -> tuple[int, int]:
    query = build_select_query(export)
    total_rows = 0
    part_number = 0

    with engine.connect() as connection:
        chunks = pd.read_sql_query(text(query), connection, chunksize=chunk_rows)

        for dataframe in chunks:
            part_number += 1
            object_name = f"{object_prefix}/part-{part_number:05d}.parquet"

            # Keep only one chunk on local disk at a time.
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                parquet_path = Path(tmp.name)

            try:
                dataframe.to_parquet(
                    parquet_path,
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                )
                bucket.blob(object_name).upload_from_filename(
                    str(parquet_path),
                    content_type="application/vnd.apache.parquet",
                    if_generation_match=0,
                )
            finally:
                parquet_path.unlink(missing_ok=True)

            chunk_count = len(dataframe)
            total_rows += chunk_count
            print(
                f"{export['name']}: enviado gs://{bucket.name}/{object_name} "
                f"({chunk_count} linhas)"
            )
            del dataframe

    return total_rows, part_number


def main() -> None:
    load_dotenv()

    config_path = Path(os.getenv("EXPORT_CONFIG_PATH", "config/gcs_exports.yaml"))
    exports = load_enabled_exports(config_path)
    if not exports:
        print("Nenhuma tabela habilitada para exportação.")
        return

    bucket_name = required_setting("GCS_BUCKET")
    base_prefix = os.getenv("GCS_PREFIX", "raw/azure_sql").strip("/")
    default_chunk_rows = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))
    if default_chunk_rows <= 0:
        raise ValueError("EXPORT_CHUNK_ROWS must be greater than zero")

    run_time = datetime.now(timezone.utc)
    run_id = f"{run_time:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    storage_client = storage.Client()
    engine = get_engine()

    try:
        bucket = storage_client.bucket(bucket_name)
        for export in exports:
            chunk_rows = int(export.get("chunk_rows", default_chunk_rows))
            if chunk_rows <= 0:
                raise ValueError(f"chunk_rows for '{export['name']}' must be greater than zero")

            object_prefix = (
                f"{base_prefix}/{export['name']}/extract_date={run_time:%Y-%m-%d}"
                f"/run_id={run_id}"
            )
            total_rows, part_number = export_table(
                engine, bucket, export, object_prefix, chunk_rows
            )
            print(
                f"Exportação concluída para {export['name']}: {total_rows} linhas, "
                f"{part_number} arquivos em gs://{bucket_name}/{object_prefix}/"
            )
    finally:
        engine.dispose()
        storage_client.close()


if __name__ == "__main__":
    main()
