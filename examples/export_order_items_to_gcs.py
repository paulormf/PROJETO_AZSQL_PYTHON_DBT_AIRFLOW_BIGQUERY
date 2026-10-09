"""Export Azure SQL rows as chunked Parquet objects in Google Cloud Storage."""

from __future__ import annotations

import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from sqlalchemy import text

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


def build_select_query(schema: str, table: str, columns: str) -> str:
    table_name = f"{quote_identifier(schema)}.{quote_identifier(table)}"
    if columns.strip() == "*":
        selected_columns = "*"
    else:
        parsed_columns = [column.strip() for column in columns.split(",")]
        if not parsed_columns or any(not column for column in parsed_columns):
            raise ValueError("AZURE_SQL_COLUMNS must be '*' or a comma-separated list")
        selected_columns = ", ".join(quote_identifier(column) for column in parsed_columns)
    return f"SELECT {selected_columns} FROM {table_name}"


def main() -> None:
    load_dotenv()

    schema = os.getenv("AZURE_SQL_SCHEMA", "dbo")
    table = required_setting("AZURE_SQL_TABLE")
    columns = os.getenv("AZURE_SQL_COLUMNS", "*")
    bucket_name = required_setting("GCS_BUCKET")
    prefix = os.getenv("GCS_PREFIX", f"raw/azure_sql/{table}").strip("/")
    chunk_rows = int(os.getenv("EXPORT_CHUNK_ROWS", "50000"))
    if chunk_rows <= 0:
        raise ValueError("EXPORT_CHUNK_ROWS must be greater than zero")

    query = build_select_query(schema, table, columns)
    run_time = datetime.now(timezone.utc)
    run_id = f"{run_time:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    object_prefix = f"{prefix}/extract_date={run_time:%Y-%m-%d}/run_id={run_id}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    engine = get_engine()
    total_rows = 0
    part_number = 0

    try:
        with engine.connect() as connection:
            chunks = pd.read_sql_query(
                text(query),
                connection,
                chunksize=chunk_rows,
            )

            for dataframe in chunks:
                part_number += 1
                object_name = f"{object_prefix}/part-{part_number:05d}.parquet"

                # Disk usage is bounded to one dataframe at a time.
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
                print(f"Enviado gs://{bucket_name}/{object_name} ({chunk_count} linhas)")

                del dataframe

    finally:
        engine.dispose()
        storage_client.close()

    print(
        f"Exportação concluída: {total_rows} linhas em {part_number} arquivos. "
        f"Destino: gs://{bucket_name}/{object_prefix}/"
    )


if __name__ == "__main__":
    main()
