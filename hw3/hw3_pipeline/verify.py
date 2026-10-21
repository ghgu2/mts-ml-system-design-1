"""Trigger real Airflow runs, check receipts and probe Kafka before/after publication."""

import base64
import json
import os
import shutil
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from confluent_kafka import Consumer, Producer

from hw3_pipeline import settings as s
from hw3_pipeline.store import connection
from model_registry import atomic_json


def api(path, payload=None):
    credentials = base64.b64encode(b"admin:admin").decode()
    request = Request("http://airflow:8080/api/v1/" + path,
                      headers={"Authorization": "Basic " + credentials, "Content-Type": "application/json"},
                      data=json.dumps(payload).encode() if payload is not None else None)
    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen(request, timeout=10) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code not in [502, 503, 504] or time.monotonic() >= deadline:
                raise
        except (URLError, OSError):
            if time.monotonic() >= deadline:
                raise
        time.sleep(2)


def trigger_and_wait(dag_id, timeout=300):
    run_id = "manual__verify_" + uuid.uuid4().hex
    deadline = time.monotonic() + timeout
    while True:
        try:
            api(f"dags/{dag_id}/dagRuns", {"dag_run_id": run_id})
            break
        except HTTPError as error:
            if error.code == 409:
                break  # The same POST may have succeeded before a connection reset.
            if error.code != 404 or time.monotonic() > deadline:
                raise
            time.sleep(2)
    while time.monotonic() < deadline:
        result = api(f"dags/{dag_id}/dagRuns/{run_id}")
        if result["state"] == "success":
            return run_id
        if result["state"] == "failed":
            tasks = api(f"dags/{dag_id}/dagRuns/{run_id}/taskInstances")
            raise RuntimeError(f"DAG {dag_id} failed: {tasks}")
        time.sleep(2)
    raise TimeoutError(f"DAG {dag_id} did not finish in {timeout}s")


def probe():
    with connection() as conn:
        row = json.loads(conn.execute("SELECT payload FROM labeled_transactions LIMIT 1").fetchone()[0])
    row.pop("target", None)
    consumer = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "verify-" + uuid.uuid4().hex,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(["scoring"])
    producer = Producer({"bootstrap.servers": "kafka:9092"})
    transaction_id = "probe-" + uuid.uuid4().hex
    errors = []
    producer.produce("transactions", key=transaction_id.encode(),
                     value=json.dumps({"transaction_id": transaction_id, "data": row}).encode(),
                     on_delivery=lambda error, _: errors.append(str(error)) if error else None)
    if producer.flush(15) or errors:
        raise RuntimeError(f"Could not send probe: {errors}")
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            result = json.loads(message.value())
            if result.get("transaction_id") == transaction_id:
                assert 0 <= result["score"] <= 1 and result["fraud_flag"] in [0, 1]
                assert result["model_version"]
                return result
        raise TimeoutError("No scoring response for the probe")
    finally:
        consumer.close()


def main():
    before = probe()
    supply_id = trigger_and_wait("supply_labeled_month")
    with connection() as conn:
        receipt = conn.execute("""SELECT j.result FROM jobs j JOIN receipts r
            ON j.kind=r.kind AND j.month=r.month WHERE r.kind='supply' AND r.run_id=?""", (supply_id,)).fetchone()
        supply = json.loads(receipt[0])
        count = conn.execute("SELECT count(*) FROM labeled_transactions WHERE batch=?", (supply["month"],)).fetchone()[0]
        assert count == supply["rows"] == s.BATCH_SIZE
    decisions = []
    for _ in range(6):
        run_id = trigger_and_wait("retrain_fraud_model")
        with connection() as conn:
            result = conn.execute("""SELECT j.result FROM jobs j JOIN receipts r
                ON j.kind=r.kind AND j.month=r.month WHERE r.kind='train' AND r.run_id=?""", (run_id,)).fetchone()
        if result is None:
            raise RuntimeError("No new labeled month; run supply_labeled_month")
        decisions.append(json.loads(result[0]))
        after = probe()
        if after["model_version"] != before["model_version"]:
            break
        trigger_and_wait("supply_labeled_month")
    else:
        raise RuntimeError("Gate rejected all six candidates; inspect evaluation reports")
    dq_run = trigger_and_wait("check_data_quality")
    import hashlib
    dq_path = s.REPORTS_DIR / ("dq_" + hashlib.sha256(dq_run.encode()).hexdigest()[:20] + ".html")
    output = Path(os.getenv("VERIFY_OUTPUT_DIR", "/reports/verification"))
    output.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(dq_path, output / "dq_latest.html")
    summary = {"before": before, "after": after, "decisions": decisions,
               "supply": supply, "quality_run_id": dq_run, "quality_report": dq_path.name,
               "hot_reload_verified": True}
    atomic_json(output / "verification.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
