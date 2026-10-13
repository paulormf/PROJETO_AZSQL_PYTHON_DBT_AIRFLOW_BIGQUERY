"""Report Azure SQL CDC, retention, and candidate-key metadata for configured tables."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text

from src.azure_sql import get_engine
from src.export_config import load_enabled_exports


DATABASE_STATUS_QUERY = """
SELECT
    DB_NAME() AS database_name,
    is_cdc_enabled,
    snapshot_isolation_state_desc
FROM sys.databases
WHERE database_id = DB_ID()
"""

CAPTURE_INSTANCES_QUERY = """
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    ct.capture_instance,
    CONVERT(varchar(22), ct.start_lsn, 1) AS capture_start_lsn,
    ct.supports_net_changes,
    ct.role_name,
    ct.index_name,
    ct.create_date,
    CONVERT(varchar(22), sys.fn_cdc_get_min_lsn(ct.capture_instance), 1) AS min_lsn,
    CONVERT(varchar(22), sys.fn_cdc_get_max_lsn(), 1) AS max_lsn
FROM cdc.change_tables AS ct
JOIN sys.tables AS t
    ON t.object_id = ct.source_object_id
JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
ORDER BY s.name, t.name, ct.capture_instance
"""

CANDIDATE_KEYS_QUERY = """
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.is_primary_key,
    i.is_unique,
    c.name AS column_name,
    ic.key_ordinal
FROM sys.tables AS t
JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
JOIN sys.indexes AS i
    ON i.object_id = t.object_id
JOIN sys.index_columns AS ic
    ON ic.object_id = i.object_id
    AND ic.index_id = i.index_id
JOIN sys.columns AS c
    ON c.object_id = ic.object_id
    AND c.column_id = ic.column_id
WHERE (i.is_primary_key = 1 OR i.is_unique = 1)
  AND i.is_disabled = 0
  AND ic.is_included_column = 0
  AND ic.key_ordinal > 0
ORDER BY s.name, t.name, i.is_primary_key DESC, i.name, ic.key_ordinal
"""


def query_rows(connection: Any, query: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(text(query)).mappings()]


def main() -> None:
    load_dotenv()
    config_path = Path(os.getenv("EXPORT_CONFIG_PATH", "config/gcs_exports.yaml"))
    exports = load_enabled_exports(config_path)
    if not exports:
        print("Nenhuma tabela habilitada em config/gcs_exports.yaml.")
        return

    engine = get_engine()
    try:
        with engine.connect() as connection:
            database_status = query_rows(connection, DATABASE_STATUS_QUERY)
            if not database_status:
                raise RuntimeError("Não foi possível consultar os metadados da base atual.")
            status = database_status[0]

            capture_rows: list[dict[str, Any]] = []
            retention_rows: list[dict[str, Any]] = []
            key_rows: list[dict[str, Any]] = []
            if status["is_cdc_enabled"]:
                capture_rows = query_rows(connection, CAPTURE_INSTANCES_QUERY)
                retention_rows = query_rows(
                    connection,
                    "SELECT job_type, retention, threshold FROM cdc.cdc_jobs ORDER BY job_type",
                )
            key_rows = query_rows(connection, CANDIDATE_KEYS_QUERY)

        report = {
            "database": {
                "name": status["database_name"],
                "cdc_enabled": bool(status["is_cdc_enabled"]),
                "snapshot_isolation": status["snapshot_isolation_state_desc"],
            },
            "cdc_jobs": retention_rows,
            "configured_tables": [],
        }

        for export in exports:
            table_id = (export["schema"].lower(), export["table"].lower())
            table_captures = [
                row
                for row in capture_rows
                if (row["schema_name"].lower(), row["table_name"].lower()) == table_id
            ]
            table_keys = [
                row
                for row in key_rows
                if (row["schema_name"].lower(), row["table_name"].lower()) == table_id
            ]

            grouped_keys: dict[str, dict[str, Any]] = {}
            for row in table_keys:
                key = grouped_keys.setdefault(
                    row["index_name"],
                    {
                        "index_name": row["index_name"],
                        "primary_key": bool(row["is_primary_key"]),
                        "unique": bool(row["is_unique"]),
                        "columns": [],
                    },
                )
                key["columns"].append(row["column_name"])

            report["configured_tables"].append(
                {
                    "config_name": export["name"],
                    "source_table": f"{export['schema']}.{export['table']}",
                    "incremental_strategy": export["incremental_strategy"],
                    "cdc_status": "TRACKED" if table_captures else "NOT_TRACKED",
                    "capture_instances": table_captures,
                    "candidate_keys": list(grouped_keys.values()),
                }
            )

        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
