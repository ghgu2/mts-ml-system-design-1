"""Validate CSV, publish to Kafka, await ClickHouse, export and benchmark."""

import argparse
import hashlib
import json
import platform
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from benchmark import QUERIES, SQL_DIR, measure, save_report
from clickhouse_client import ClickHouse
from dataset import reference_result, write_result
from load_csv import publish_csv


def normalize(rows):
    return [{"us_state": row["us_state"], "cat_id": row["cat_id"],
             "max_amount": format(Decimal(str(row["max_amount"])), ".2f"),
             "source_row": str(row["source_row"])} for row in rows]


def wait_for_ingestion(client, run_id: str, expected: int, timeout: int):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        counts = client.rows(
            "SELECT 'baseline' AS variant, count() AS rows, uniqExact(source_row) AS unique_rows "
            "FROM hw2.transactions_baseline WHERE run_id = {run_id:UUID} UNION ALL "
            "SELECT 'optimized', count(), uniqExact(source_row) FROM hw2.transactions "
            "WHERE run_id = {run_id:UUID}", params={"run_id": run_id},
        )
        current = tuple((row["variant"], int(row["rows"]), int(row["unique_rows"])) for row in counts)
        if current != last:
            print(f"ClickHouse ingestion: {current}; expected={expected:,}", flush=True)
            last = current
        if any(rows > expected for _, rows, _ in current):
            raise RuntimeError("Duplicate delivery detected: more ClickHouse rows than CSV rows")
        if all(rows == unique_rows == expected for _, rows, unique_rows in current):
            return
        time.sleep(1)
    raise TimeoutError(f"Ingestion did not finish within {timeout}s: {last}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--repeats", type=int, default=11)
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if args.repeats < 3 or args.timeout < 1:
        parser.error("--repeats must be >=3 and --timeout must be positive")
    run_id = str(uuid.uuid4())
    print(f"Validating {args.csv}; run_id={run_id}", flush=True)
    expected, reference = reference_result(args.csv, args.limit)
    with args.csv.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    client = ClickHouse()
    version = client.rows("SELECT version() AS version")[0]["version"]
    sent = publish_csv(args.csv, run_id, args.limit)
    if sent != expected:
        raise RuntimeError("CSV changed between validation and publication")
    with args.csv.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
            raise RuntimeError("CSV changed during publication")
    wait_for_ingestion(client, run_id, expected, args.timeout)
    results = {}
    for name, file in QUERIES.items():
        result = normalize(client.rows((SQL_DIR / file).read_text(), params={"run_id": run_id}))
        if result != reference:
            raise RuntimeError(f"{name} result differs from independent CSV calculation")
        results[name] = result
    print(f"All 3 queries match CSV reference for {len(reference)} states", flush=True)
    metrics, samples, storage = measure(client, run_id, args.repeats)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_result(args.output_dir / "max_transaction_by_state.csv", results["preaggregated"])
    write_result(args.output_dir / "max_transaction_baseline.csv", results["baseline"])
    write_result(args.output_dir / "max_transaction_storage.csv", results["optimized_storage"])
    write_result(args.output_dir / "reference.csv", reference)
    metadata = {
        "run_id": run_id, "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_file": args.csv.name, "dataset_sha256": digest, "limit": args.limit,
        "rows": expected, "states": len(reference), "clickhouse_version": version,
        "architecture": platform.machine(), "repetitions": args.repeats,
        "validation": "passed: counts, unique source rows, three SQL results vs CSV reference",
    }
    save_report(args.output_dir, metadata, metrics, samples, storage)
    print(json.dumps(metrics, indent=2), flush=True)
    print(f"PASS. CSV results and benchmark report saved to {args.output_dir}", flush=True)


if __name__ == "__main__":
    main()
