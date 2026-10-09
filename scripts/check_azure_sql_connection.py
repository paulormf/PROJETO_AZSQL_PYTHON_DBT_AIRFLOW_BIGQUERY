"""Check Azure SQL connectivity without displaying source data."""

from sqlalchemy import text

from src.azure_sql import get_engine

def main() -> None:
    engine = get_engine()
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1")).scalar_one()
        print("Azure SQL connection successful.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
