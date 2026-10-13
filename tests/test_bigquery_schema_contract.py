import unittest

from pipelines.gcs_cdc_to_bigquery import compare_schema_fields


class CompareSchemaFieldsTests(unittest.TestCase):
    def test_same_fields_in_different_order_match(self) -> None:
        expected = {"order_id": "INTEGER", "amount": "NUMERIC"}
        actual = {"amount": "NUMERIC", "order_id": "INTEGER"}

        self.assertEqual(compare_schema_fields(expected, actual), ([], [], []))

    def test_reports_missing_and_unexpected_fields(self) -> None:
        expected = {"order_id": "INTEGER", "amount": "NUMERIC"}
        actual = {"order_id": "INTEGER", "discount": "NUMERIC"}

        self.assertEqual(
            compare_schema_fields(expected, actual),
            (["amount"], ["discount"], []),
        )

    def test_reports_type_mismatch(self) -> None:
        expected = {"shipping_address_id": "FLOAT"}
        actual = {"shipping_address_id": "INTEGER"}

        self.assertEqual(
            compare_schema_fields(expected, actual),
            ([], [], [("shipping_address_id", "FLOAT", "INTEGER")]),
        )


if __name__ == "__main__":
    unittest.main()
