import unittest

import pyarrow as pa
from sqlalchemy import create_engine

from src.sql_schema import (
    bigquery_schema_types,
    compare_contract_types,
    contract_document,
    iter_arrow_tables,
)


class SqlSchemaContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("payload", pa.binary(), nullable=True),
                pa.field("amount", pa.decimal128(12, 2), nullable=True),
            ]
        )

    def test_explicit_schema_preserves_bytes_nullable_values_and_bigints(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE sample (id INTEGER, payload BLOB, amount TEXT)")
            connection.exec_driver_sql(
                "INSERT INTO sample VALUES (?, ?, ?), (?, ?, ?)",
                (9007199254740993, bytes([0, 168, 2]), "12.34", 2, None, None),
            )
            tables = list(
                iter_arrow_tables(
                    connection,
                    "SELECT id, payload, amount FROM sample ORDER BY rowid",
                    None,
                    10,
                    self.schema,
                )
            )

        self.assertEqual(len(tables), 1)
        table = tables[0]
        self.assertEqual(table.schema, self.schema)
        self.assertEqual(table.column("id")[0].as_py(), 9007199254740993)
        self.assertEqual(table.column("payload")[0].as_py(), bytes([0, 168, 2]))
        self.assertIsNone(table.column("payload")[1].as_py())
        self.assertEqual(str(table.column("amount")[0].as_py()), "12.34")

    def test_empty_query_produces_no_parquet_batches(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as connection:
            tables = list(
                iter_arrow_tables(
                    connection,
                    "SELECT 1 AS id, NULL AS payload, NULL AS amount WHERE 1 = 0",
                    None,
                    10,
                    self.schema,
                )
            )
        self.assertEqual(tables, [])

    def test_contract_maps_types_to_bigquery_and_detects_drift(self) -> None:
        contract = contract_document(self.schema)
        self.assertEqual(
            bigquery_schema_types(contract),
            {"id": "INTEGER", "payload": "BYTES", "amount": "NUMERIC"},
        )
        actual = [
            type("Field", (), {"name": "id", "field_type": "INTEGER"})(),
            type("Field", (), {"name": "payload", "field_type": "BYTES"})(),
            type("Field", (), {"name": "amount", "field_type": "NUMERIC"})(),
        ]
        self.assertEqual(compare_contract_types(contract, actual), ([], [], []))


if __name__ == "__main__":
    unittest.main()
