"""CSV validation and an independent Decimal-based reference calculation."""

import csv
import math
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

CSV_COLUMNS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon",
)
FLOAT_COLUMNS = ("lat", "lon", "merchant_lat", "merchant_lon")
RESULT_COLUMNS = ("us_state", "cat_id", "max_amount", "source_row")


def transactions(path: Path, run_id: str, limit: int | None = None):
    """Yield flat JSON-ready records; optional train.csv target is ignored."""
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        missing = set(CSV_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError("Missing CSV columns: " + ", ".join(sorted(missing)))
        for source_row, row in enumerate(reader, start=1):
            if limit is not None and source_row > limit:
                break
            try:
                if None in row or any(row.get(key) is None for key in CSV_COLUMNS):
                    raise ValueError("incorrect CSV field count")
                record = {key: row[key] for key in CSV_COLUMNS}
                if not record["us_state"] or not record["cat_id"]:
                    raise ValueError("empty state or category")
                timestamp = datetime.fromisoformat(record["transaction_time"])
                # Source data are timezone-free and have minute precision.
                if timestamp.tzinfo is not None or timestamp.microsecond:
                    raise ValueError("timestamp must have no timezone or fractional seconds")
                if not 1970 <= timestamp.year < 2106:
                    raise ValueError("timestamp outside DateTime range")
                record["transaction_time"] = timestamp.isoformat(sep=" ")
                amount = Decimal(record["amount"])
                if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
                    raise ValueError("amount must be finite and have at most two decimal places")
                if abs(amount) >= Decimal("10000000000000000"):
                    raise ValueError("amount outside Decimal(18, 2) range")
                record["amount"] = format(amount, ".2f")
                for key in FLOAT_COLUMNS:
                    record[key] = float(record[key])
                    if not math.isfinite(record[key]):
                        raise ValueError(f"non-finite {key}")
                population = Decimal(record["population_city"])
                if population != population.to_integral_value() or not 0 <= population < 2**32:
                    raise ValueError("population_city must fit UInt32")
                record["population_city"] = int(population)
                record.update(run_id=run_id, source_row=source_row)
            except (ValueError, TypeError, InvalidOperation) as error:
                raise ValueError(f"CSV row {source_row}: {error}") from error
            yield record


def reference_result(path: Path, limit: int | None = None):
    """First row wins a tie, independent of Kafka and ClickHouse aggregation."""
    winners = {}
    count = 0
    for record in transactions(path, "00000000-0000-0000-0000-000000000000", limit):
        count += 1
        state = record["us_state"]
        amount = Decimal(record["amount"])
        previous = winners.get(state)
        if previous is None or amount > previous[0]:
            winners[state] = (amount, record["cat_id"], record["source_row"])
    if not count:
        raise ValueError("CSV contains no transactions")
    result = [
        {"us_state": state, "cat_id": category, "max_amount": format(amount, ".2f"),
         "source_row": str(source_row)}
        for state, (amount, category, source_row) in sorted(winners.items())
    ]
    return count, result


def write_result(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=RESULT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
