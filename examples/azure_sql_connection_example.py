from sqlalchemy import text

from src.azure_sql import get_engine

engine = get_engine()

try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT TOP (10) * FROM dbo.order_items"))

        for row in result:
            print(row)
finally:
    engine.dispose()