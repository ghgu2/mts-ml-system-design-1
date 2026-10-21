"""Train-only frequency encoding and geographic/calendar features for HW3.

This module is shared by Airflow and inference. It never uses target values.
"""

import numpy as np
import pandas as pd

CATEGORIES = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
NUMERIC = ["amount", "population_city", "lat", "lon", "merchant_lat", "merchant_lon"]
REQUIRED = ["transaction_time", *CATEGORIES, *NUMERIC]
FEATURES = [*CATEGORIES, *[f"{c}_frequency" for c in CATEGORIES],
            "amount_log", "population_log", "distance_log", "hour_sin", "hour_cos",
            "weekday_sin", "weekday_cos", "is_weekend", "amount_per_capita"]


def fit_preprocessor(train):
    """Read only training rows; return JSON-serializable medians and frequencies."""
    return {
        "schema": "frequency_geo_v1",
        "medians": {c: float(pd.to_numeric(train[c], errors="coerce").median())
                    for c in NUMERIC},
        "frequencies": {
            c: train[c].fillna("__missing__").astype(str).value_counts(normalize=True).to_dict()
            for c in CATEGORIES
        },
    }


def transform(frame, artifacts):
    """Read raw rows and frozen train statistics; return the same features online/offline."""
    if artifacts["schema"] != "frequency_geo_v1":
        raise ValueError("Unknown preprocessing schema")
    missing = set(REQUIRED) - set(frame.columns)
    if missing:
        raise ValueError(f"Missing features: {sorted(missing)}")
    output = pd.DataFrame(index=frame.index)
    for column in CATEGORIES:
        output[column] = frame[column].fillna("__missing__").astype(str)
        output[f"{column}_frequency"] = output[column].map(
            artifacts["frequencies"][column]).fillna(0.0)
    numeric = {c: pd.to_numeric(frame[c], errors="coerce").replace(
        [np.inf, -np.inf], np.nan).fillna(artifacts["medians"][c]) for c in NUMERIC}
    lat1, lon1, lat2, lon2 = [np.radians(numeric[c]) for c in
                            ["lat", "lon", "merchant_lat", "merchant_lon"]]
    haversine = (np.sin((lat2 - lat1) / 2) ** 2
                 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2)
    distance = 6371.009 * 2 * np.arcsin(np.sqrt(haversine.clip(0, 1)))
    output["amount_log"] = np.log1p(numeric["amount"].clip(lower=0))
    output["population_log"] = np.log1p(numeric["population_city"].clip(lower=0))
    output["distance_log"] = np.log1p(distance)
    time = pd.to_datetime(frame["transaction_time"], errors="raise")
    hour = time.dt.hour + time.dt.minute / 60
    output["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    output["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    output["weekday_sin"] = np.sin(2 * np.pi * time.dt.dayofweek / 7)
    output["weekday_cos"] = np.cos(2 * np.pi * time.dt.dayofweek / 7)
    output["is_weekend"] = (time.dt.dayofweek >= 5).astype(int)
    output["amount_per_capita"] = numeric["amount"] / (numeric["population_city"].clip(lower=0) + 1)
    return output[FEATURES]
