"""Boundary cases that can otherwise silently change a financial maximum."""

import csv
import tempfile
import unittest
from pathlib import Path

from dataset import CSV_COLUMNS, reference_result, transactions

FIXTURE = Path(__file__).parent / "edge_cases.csv"
RUN_ID = "00000000-0000-0000-0000-000000000001"


class DatasetTests(unittest.TestCase):
    def test_ties_cents_and_amounts_above_float_precision(self):
        count, result = reference_result(FIXTURE)
        self.assertEqual(count, 7)
        self.assertEqual(result, [
            {"us_state": "CA", "cat_id": "food_dining", "max_amount": "9.99", "source_row": "4"},
            {"us_state": "NY", "cat_id": "misc_pos", "max_amount": "900719925474099.92", "source_row": "7"},
            {"us_state": "TX", "cat_id": "grocery_net", "max_amount": "10.10", "source_row": "1"},
        ])

    def test_leading_zero_postcode_and_json_numeric_fields(self):
        row = next(transactions(FIXTURE, RUN_ID))
        self.assertEqual(row["post_code"], "00123")
        self.assertEqual(row["amount"], "10.10")
        self.assertEqual(row["source_row"], 1)
        self.assertEqual(row["population_city"], 1000)
        self.assertIsInstance(row["lat"], float)

    def test_limit_changes_reference_population(self):
        count, result = reference_result(FIXTURE, 2)
        self.assertEqual(count, 2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_row"], "1")

    def test_invalid_values_and_malformed_rows_are_rejected(self):
        with FIXTURE.open(newline="") as stream:
            template = next(csv.DictReader(stream))
        cases = [
            ("amount", "10.001"), ("amount", "NaN"), ("amount", "10000000000000000"),
            ("lat", "inf"), ("population_city", "1.5"), ("population_city", "-1"),
            ("population_city", "4294967296"), ("us_state", ""), ("cat_id", ""),
            ("transaction_time", "2019-09-14 02:46:00+03:00"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.csv"
            for column, value in cases:
                with self.subTest(column=column, value=value):
                    row = {**template, column: value}
                    with path.open("w", newline="") as stream:
                        writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS)
                        writer.writeheader()
                        writer.writerow(row)
                    with self.assertRaisesRegex(ValueError, "CSV row 1"):
                        reference_result(path)
            path.write_text(",".join(CSV_COLUMNS) + "\n2019-09-14 02:46,only_two_fields\n")
            with self.assertRaisesRegex(ValueError, "field count"):
                reference_result(path)

    def test_empty_and_missing_headers_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "empty.csv"
            path.write_text(",".join(CSV_COLUMNS) + "\n")
            with self.assertRaisesRegex(ValueError, "no transactions"):
                reference_result(path)
            path.write_text("us_state,amount\nTX,1.00\n")
            with self.assertRaisesRegex(ValueError, "Missing CSV columns"):
                reference_result(path)


if __name__ == "__main__":
    unittest.main()
