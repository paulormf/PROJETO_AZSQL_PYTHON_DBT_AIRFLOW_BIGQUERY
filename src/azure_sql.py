"""Factory for the project's Azure SQL SQLAlchemy engine."""

import os

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL


def _required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_engine() -> Engine:
    """Create an Azure SQL engine using settings from the environment or .env."""
    load_dotenv()

    url = URL.create(
        "mssql+pyodbc",
        username=_required_setting("AZURE_SQL_USERNAME"),
        password=_required_setting("AZURE_SQL_PASSWORD"),
        host=_required_setting("AZURE_SQL_SERVER"),
        database=_required_setting("AZURE_SQL_DATABASE"),
        query={
            "driver": os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            "Encrypt": os.getenv("AZURE_SQL_ENCRYPT", "yes"),
            "TrustServerCertificate": os.getenv(
                "AZURE_SQL_TRUST_SERVER_CERTIFICATE", "no"
            ),
        },
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "timeout": int(os.getenv("AZURE_SQL_CONNECTION_TIMEOUT", "30"))
        },
    )
