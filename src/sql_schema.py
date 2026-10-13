"""Shared SQL Server to Arrow schema contract for full and CDC extracts."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import pyarrow as pa
from sqlalchemy import Connection, text


SCHEMA_CONTRACT_VERSION = 1

_ARROW_TYPES: dict[str, pa.DataType] = {
    "tinyint": pa.int64(),
    "smallint": pa.int64(),
    "int": pa.int64(),
    "bigint": pa.int64(),
    "bit": pa.bool_(),
    "float": pa.float64(),
    "real": pa.float64(),
    "char": pa.string(),
    "varchar": pa.string(),
    "text": pa.string(),
    "nchar": pa.string(),
    "nvarchar": pa.string(),
    "ntext": pa.string(),
    "sysname": pa.string(),
    "xml": pa.string(),
    "binary": pa.binary(),
    "varbinary": pa.binary(),
    "image": pa.binary(),
    "timestamp": pa.binary(),
    "rowversion": pa.binary(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "smalldatetime": pa.timestamp("us"),
    "datetime2": pa.timestamp("us"),
    "datetimeoffset": pa.timestamp("us", tz="UTC"),
    "time": pa.time64("us"),
    "uniqueidentifier": pa.string(),
}


def _sql_type_to_arrow(type_name: str, precision: int, scale: int) -> pa.DataType:
    normalized = type_name.lower()
    if normalized in {"decimal", "numeric"}:
        return pa.decimal128(precision, scale)
    if normalized == "money":
        return pa.decimal128(19, 4)
    if normalized == "smallmoney":
        return pa.decimal128(10, 4)
    try:
        return _ARROW_TYPES[normalized]
    except KeyError as error:
        raise ValueError(
            f"SQL Server type {type_name!r} has no schema-contract mapping; "
            "exclude the column or add an explicit mapping before extracting."
        ) from error


def get_source_schema(
    connection: Connection,
    schema_name: str,
    table_name: str,
    selected_columns: Sequence[str] | str,
) -> pa.Schema:
    """Read source catalog types and return the canonical Arrow schema."""
    rows = connection.execute(
        text(
            """
            SELECT c.name, base_type.name AS type_name, c.precision, c.scale, c.is_nullable
            FROM sys.columns AS c
            INNER JOIN sys.tables AS t ON t.object_id = c.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            INNER JOIN sys.types AS declared_type
              ON declared_type.user_type_id = c.user_type_id
            INNER JOIN sys.types AS base_type
              ON base_type.user_type_id = c.system_type_id
             AND base_type.user_type_id = base_type.system_type_id
            WHERE s.name = :schema_name AND t.name = :table_name
            ORDER BY c.column_id
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    ).mappings().all()
    if not rows:
        raise ValueError(f"Source table not found: {schema_name}.{table_name}")

    by_name = {row["name"]: row for row in rows}
    if selected_columns == "*":
        names = [row["name"] for row in rows]
    else:
        names = list(selected_columns)
        missing = set(names) - by_name.keys()
        if missing:
            raise ValueError(
                f"Configured columns do not exist in {schema_name}.{table_name}: "
                f"{', '.join(sorted(missing))}"
            )

    fields = []
    for name in names:
        row = by_name[name]
        sql_type = str(row["type_name"])
        arrow_type = _sql_type_to_arrow(
            sql_type, int(row["precision"]), int(row["scale"])
        )
        metadata = {
            b"sql_type": sql_type.encode("utf-8"),
            b"sql_precision": str(row["precision"]).encode("ascii"),
            b"sql_scale": str(row["scale"]).encode("ascii"),
        }
        fields.append(
            pa.field(name, arrow_type, nullable=bool(row["is_nullable"]), metadata=metadata)
        )
    return pa.schema(fields)


def _normalize_value(value: Any, field: pa.Field) -> Any:
    if value is None:
        return None
    if pa.types.is_binary(field.type):
        if isinstance(value, memoryview):
            return value.tobytes()
        if isinstance(value, bytearray):
            return bytes(value)
    if pa.types.is_string(field.type):
        if isinstance(value, UUID):
            return str(value)
        if not isinstance(value, str):
            return str(value)
    if pa.types.is_timestamp(field.type) and field.type.tz == "UTC":
        if not isinstance(value, datetime):
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if pa.types.is_decimal(field.type) and not isinstance(value, Decimal):
        return Decimal(value)
    return value


def iter_arrow_tables(
    connection: Connection,
    query: str,
    params: Mapping[str, Any] | None,
    chunk_rows: int,
    schema: pa.Schema,
) -> Iterator[pa.Table]:
    """Stream SQL result batches and materialize each with the canonical schema."""
    if chunk_rows <= 0:
        raise ValueError("chunk_rows must be greater than zero")
    result = connection.execution_options(stream_results=True).execute(text(query), params or {})
    column_names = list(result.keys())
    if column_names != schema.names:
        result.close()
        raise ValueError(
            f"Query columns do not match the schema contract: "
            f"query={column_names}, contract={schema.names}"
        )
    try:
        while rows := result.fetchmany(chunk_rows):
            arrays = []
            for position, field in enumerate(schema):
                values = [_normalize_value(row[position], field) for row in rows]
                try:
                    arrays.append(pa.array(values, type=field.type, from_pandas=True))
                except (pa.ArrowException, TypeError, ValueError) as error:
                    raise ValueError(
                        f"Values in column {field.name!r} do not match schema type "
                        f"{field.type}: {error}"
                    ) from error
            yield pa.Table.from_arrays(arrays, schema=schema)
    finally:
        result.close()


def contract_document(schema: pa.Schema) -> dict[str, Any]:
    """Return an auditable, JSON-serializable representation of an Arrow schema."""
    fields = []
    for field in schema:
        sql_metadata = field.metadata or {}
        fields.append(
            {
                "name": field.name,
                "sql_type": sql_metadata.get(b"sql_type", b"unknown").decode("utf-8"),
                "arrow_type": str(field.type),
                "nullable": field.nullable,
                "decimal_precision": (
                    field.type.precision if pa.types.is_decimal(field.type) else None
                ),
                "decimal_scale": (
                    field.type.scale if pa.types.is_decimal(field.type) else None
                ),
            }
        )
    return {"version": SCHEMA_CONTRACT_VERSION, "fields": fields}


def bigquery_schema_types(contract: Mapping[str, Any]) -> dict[str, str]:
    """Map contract Arrow types to the BigQuery types used by Parquet loading."""
    result: dict[str, str] = {}
    for item in contract.get("fields", []):
        arrow_type = item["arrow_type"]
        if arrow_type == "bool":
            bq_type = "BOOLEAN"
        elif arrow_type.startswith("int") or arrow_type.startswith("uint"):
            bq_type = "INTEGER"
        elif arrow_type.startswith("float"):
            bq_type = "FLOAT"
        elif arrow_type.startswith(("decimal128(", "decimal256(")):
            precision = int(item["decimal_precision"])
            scale = int(item["decimal_scale"])
            bq_type = "NUMERIC" if precision <= 38 and scale <= 9 else "BIGNUMERIC"
        elif arrow_type in {"string", "large_string"}:
            bq_type = "STRING"
        elif arrow_type in {"binary", "large_binary"}:
            bq_type = "BYTES"
        elif arrow_type == "date32[day]":
            bq_type = "DATE"
        elif arrow_type.startswith("timestamp["):
            bq_type = "TIMESTAMP"
        elif arrow_type.startswith("time64["):
            bq_type = "TIME"
        else:
            raise ValueError(f"Arrow type {arrow_type!r} has no BigQuery mapping")
        result[item["name"]] = bq_type
    return result


def bigquery_schema_fields(contract: Mapping[str, Any]) -> list[dict[str, str]]:
    """Build explicit BigQuery load fields from the versioned schema contract."""
    types = bigquery_schema_types(contract)
    return [
        {
            "name": item["name"],
            "field_type": types[item["name"]],
            "mode": "NULLABLE" if item["nullable"] else "REQUIRED",
        }
        for item in contract.get("fields", [])
    ]


def compare_contract_types(
    contract: Mapping[str, Any], actual: Sequence[Any]
) -> tuple[list[str], list[str], list[tuple[str, str, str]]]:
    """Compare contract field names/types to BigQuery SchemaField objects."""
    expected = bigquery_schema_types(contract)
    actual_types = {field.name: field.field_type.upper() for field in actual}
    missing = sorted(expected.keys() - actual_types.keys())
    unexpected = sorted(actual_types.keys() - expected.keys())
    mismatches = sorted(
        (name, expected[name], actual_types[name])
        for name in expected.keys() & actual_types.keys()
        if expected[name] != actual_types[name]
    )
    return missing, unexpected, mismatches
