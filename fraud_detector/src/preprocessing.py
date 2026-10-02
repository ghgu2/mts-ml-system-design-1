"""Prepare a transaction with statistics saved before deployment."""

import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd

CATEGORICAL_COLUMNS = ("gender", "merch", "cat_id", "one_city", "us_state", "jobs")
TIME_COLUMNS = ("hour", "year", "month", "day_of_month", "day_of_week")
NUMERIC_COLUMNS = ("amount", "population_city", "distance")
FEATURE_COLUMNS = (
    list(TIME_COLUMNS)
    + [f"{column}_cat" for column in CATEGORICAL_COLUMNS]
    + [f"{column}_mean_enc" for column in
       [f"{name}_cat" for name in CATEGORICAL_COLUMNS] + list(TIME_COLUMNS)]
    + [f"{column}_log" for column in NUMERIC_COLUMNS]
)


def load_artifacts(path="models/preprocessing.json"):
    with Path(path).open(encoding="utf-8") as file:
        return json.load(file)


def great_circle_km(lat1, lon1, lat2, lon2):
    """Same spherical distance convention as geopy.great_circle."""
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    cosine = (math.sin(lat1) * math.sin(lat2)
              + math.cos(lat1) * math.cos(lat2) * math.cos(lon1 - lon2))
    return 6371.009 * math.acos(max(-1.0, min(1.0, cosine)))


def prepare_transaction(data, artifacts):
    timestamp = datetime.fromisoformat(data["transaction_time"])
    time_values = {
        "hour": timestamp.hour,
        "year": timestamp.year,
        "month": timestamp.month,
        "day_of_month": timestamp.day,
        "day_of_week": timestamp.weekday(),
    }
    values = {}
    for column in CATEGORICAL_COLUMNS:
        raw = data.get(column)
        category = artifacts["categories"][column].get(str(raw), "cat_NAN")
        values[f"{column}_cat"] = category

    for column, value in time_values.items():
        values[column] = str(value)

    for column in [f"{name}_cat" for name in CATEGORICAL_COLUMNS]:
        values[f"{column}_mean_enc"] = artifacts["means"][column].get(
            values[column], math.nan
        )
    for column, value in time_values.items():
        values[f"{column}_mean_enc"] = artifacts["means"][column].get(
            str(value), math.nan
        )

    numeric = {
        "amount": data.get("amount"),
        "population_city": data.get("population_city"),
        "distance": great_circle_km(
            float(data["lat"]), float(data["lon"]),
            float(data["merchant_lat"]), float(data["merchant_lon"]),
        ),
    }
    for column, value in numeric.items():
        number = float(value) if value not in (None, "") else math.nan
        if math.isnan(number):
            number = artifacts["numeric_means"][column]
        values[f"{column}_log"] = math.log1p(number)

    return pd.DataFrame([values], columns=FEATURE_COLUMNS)
