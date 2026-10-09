"""Load and validate the table export allowlist from YAML."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(identifier: str) -> str:
    """Allow simple SQL identifiers and quote them for SQL Server."""
    if not isinstance(identifier, str) or not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid SQL identifier in export configuration: {identifier!r}")
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

        for key in ("name", "schema", "table", "columns", "load_mode"):
            if key not in export:
                raise ValueError(f"Enabled export is missing required setting: {key}")
        if export["load_mode"] != "full":
            raise ValueError(
                f"Export '{export.get('name')}' uses unsupported load_mode "
                f"{export['load_mode']!r}; only 'full' is implemented currently"
            )

        name = export["name"]
        quote_identifier(name)
        if name in seen_names:
            raise ValueError(f"Duplicate export name in configuration: {name}")
        seen_names.add(name)

        # Validate SQL identifiers and columns before opening cloud or SQL connections.
        quote_identifier(export["schema"])
        quote_identifier(export["table"])
        build_select_query(export)
        enabled_exports.append(export)

    return enabled_exports
