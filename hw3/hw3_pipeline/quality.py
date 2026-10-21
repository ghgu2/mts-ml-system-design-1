"""Executable data quality rules and an escaped HTML report, with no external service."""

import html
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from hw3_pipeline import settings as s
from hw3_pipeline.store import connection, read_frame
from model_registry import atomic_json
from retraining_features import CATEGORIES, NUMERIC, REQUIRED


def check_frame(frame):
    """Inspect raw labeled rows without modifying them; return explicit rule results."""
    results = []

    def rule(name, passed, detail):
        results.append({"rule": name, "passed": bool(passed), "detail": str(detail)})

    missing = sorted(set([*REQUIRED, "target"]) - set(frame.columns))
    rule("Required columns", not missing, f"Missing columns: {missing}")
    rule("At least 100 rows", len(frame) >= 100, f"Rows: {len(frame)}")
    if missing or frame.empty:
        return results
    required = frame[[*REQUIRED, "target"]]
    nulls = int(required.isna().sum().sum())
    rule("No missing required values", nulls == 0, f"Null cells: {nulls}")
    times = pd.to_datetime(frame["transaction_time"], errors="coerce", utc=True).dt.tz_localize(None)
    rule("Valid timestamps", times.notna().all(), f"Invalid timestamps: {times.isna().sum()}")
    target = pd.to_numeric(frame["target"], errors="coerce")
    rule("Binary target with both classes", target.isin([0, 1]).all() and target.nunique() == 2,
         f"Label counts: {target.value_counts(dropna=False).to_dict()}")
    numeric = frame[NUMERIC].apply(pd.to_numeric, errors="coerce")
    rule("Finite numeric values", np.isfinite(numeric.to_numpy()).all(), "No NaN or infinity in numeric fields")
    rule("Nonnegative amount and population", (numeric[["amount", "population_city"]] >= 0).all().all(),
         "amount >= 0; population_city >= 0")
    coordinates_ok = all(numeric[c].between(-90, 90).all() for c in ["lat", "merchant_lat"])
    coordinates_ok &= all(numeric[c].between(-180, 180).all() for c in ["lon", "merchant_lon"])
    rule("Geographic bounds", coordinates_ok, "Latitude in [-90,90], longitude in [-180,180]")
    rule("Nonempty categories", all(frame[c].fillna("").astype(str).str.strip().ne("").all() for c in CATEGORIES),
         "All six categorical features contain nonempty strings")
    monthly_classes = target.groupby(times.dt.to_period("M")).nunique()
    rule("Both classes in each month", len(monthly_classes) > 0 and monthly_classes.eq(2).all(),
         f"Classes per month: {monthly_classes.to_dict()}")
    return results


def snapshot_quality(workflow_run_id):
    """Read a consistent showcase snapshot; retries reuse the same saved rule results."""
    import hashlib
    report_id = hashlib.sha256(workflow_run_id.encode()).hexdigest()[:20]
    path = s.REPORTS_DIR / f"dq_{report_id}.json"
    if not path.exists():
        with connection() as conn:
            frame = read_frame(conn)
        atomic_json(path, {"run_id": workflow_run_id, "rows": len(frame),
                           "checked_at": datetime.now(timezone.utc).isoformat(),
                           "rules": check_frame(frame)})
    return str(path)


def render_report(json_path):
    """Publish an HTML report before any failure is raised; the same input is repeatable."""
    from pathlib import Path
    path = Path(json_path)
    result = json.loads(path.read_text())
    rows = "".join(f"<tr><td>{html.escape(r['rule'])}</td><td>{'PASS' if r['passed'] else 'FAIL'}</td>"
                   f"<td>{html.escape(r['detail'])}</td></tr>" for r in result["rules"])
    passed = all(r["passed"] for r in result["rules"])
    document = ("<!doctype html><html lang='en'><meta charset='utf-8'><title>Fraud data quality</title>"
                "<style>body{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 24px}"
                "table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:12px;text-align:left}"
                "th{background:#edf2f7}p{line-height:1.6}</style><h1>Fraud data quality</h1>"
                f"<p>Result: <strong>{'PASS' if passed else 'FAIL'}</strong> · Rows: {result['rows']}<br>"
                f"Run: {html.escape(result['run_id'])}<br>UTC: {html.escape(result['checked_at'])}</p>"
                f"<table><tr><th>Rule</th><th>Result</th><th>Details</th></tr>{rows}</table></html>")
    output = path.with_suffix(".html")
    temporary = output.with_suffix(".html.tmp")
    temporary.write_text(document, encoding="utf-8")
    os.replace(temporary, output)
    return {"html": str(output), "passed": passed}


def enforce_quality(result):
    """Fail the DAG on bad data, retaining its already published diagnostic report."""
    if not result["passed"]:
        raise ValueError(f"Data quality failed; inspect {result['html']}")
    return result
