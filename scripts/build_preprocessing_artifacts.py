"""Build small inference statistics from the supplied training CSV once.

Usage: python3 scripts/build_preprocessing_artifacts.py /path/to/train.csv
"""

import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

CATEGORICAL_COLUMNS = ("gender", "merch", "cat_id", "one_city", "us_state", "jobs")
TIME_COLUMNS = ("hour", "year", "month", "day_of_month", "day_of_week")


def rows(path):
    with path.open(newline="", encoding="utf-8") as file:
        yield from csv.DictReader(file)


def distance_km(row):
    lat1, lon1, lat2, lon2 = map(
        lambda name: math.radians(float(row[name])),
        ("lat", "lon", "merchant_lat", "merchant_lon"),
    )
    cosine = (math.sin(lat1) * math.sin(lat2)
              + math.cos(lat1) * math.cos(lat2) * math.cos(lon1 - lon2))
    return 6371.009 * math.acos(max(-1.0, min(1.0, cosine)))


def build(path):
    counts = {column: Counter() for column in CATEGORICAL_COLUMNS}
    for row in rows(path):
        for column in CATEGORICAL_COLUMNS:
            counts[column][row[column]] += 1

    categories = {}
    for column, frequency in counts.items():
        ranked = sorted(frequency, key=lambda value: (-frequency[value], value))
        categories[column] = {
            value: ("cat_NAN" if not value else
                    f"cat_{index}" if index < 50 else "cat_50+")
            for index, value in enumerate(ranked)
        }

    target_sums = defaultdict(lambda: defaultdict(float))
    target_counts = defaultdict(Counter)
    numeric_sums = Counter()
    numeric_counts = Counter()
    for row in rows(path):
        target = float(row["target"])
        timestamp = datetime.fromisoformat(row["transaction_time"])
        time_values = {
            "hour": timestamp.hour,
            "year": timestamp.year,
            "month": timestamp.month,
            "day_of_month": timestamp.day,
            "day_of_week": timestamp.weekday(),
        }
        for column in CATEGORICAL_COLUMNS:
            feature = f"{column}_cat"
            category = categories[column][row[column]]
            target_sums[feature][category] += target
            target_counts[feature][category] += 1
        for column, value in time_values.items():
            key = str(value)
            target_sums[column][key] += target
            target_counts[column][key] += 1
        numeric = {
            "amount": row["amount"],
            "population_city": row["population_city"],
            "distance": distance_km(row),
        }
        for column, value in numeric.items():
            if value not in (None, ""):
                numeric_sums[column] += float(value)
                numeric_counts[column] += 1

    means = {
        feature: {
            category: total / target_counts[feature][category]
            for category, total in sums.items()
        }
        for feature, sums in target_sums.items()
    }
    numeric_means = {
        column: numeric_sums[column] / numeric_counts[column]
        for column in ("amount", "population_city", "distance")
    }
    return {"categories": categories, "means": means, "numeric_means": numeric_means}


if __name__ == "__main__":
    source = Path(sys.argv[1])
    destination = Path(__file__).resolve().parents[1] / "fraud_detector/models/preprocessing.json"
    destination.write_text(json.dumps(build(source), ensure_ascii=False), encoding="utf-8")
    print(f"Saved {destination} ({destination.stat().st_size} bytes)")
