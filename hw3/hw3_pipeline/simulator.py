"""Continuously send raw labeled-showcase samples to Kafka, without their target."""

import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from hw3_pipeline.store import connection


def main():
    with connection() as conn:
        examples = [json.loads(r[0]) for r in conn.execute("SELECT payload FROM labeled_transactions LIMIT 100")]
    producer = Producer({"bootstrap.servers": "kafka:9092", "enable.idempotence": True})
    index = 0
    while True:
        row = dict(examples[index % len(examples)])
        row.pop("target", None)
        row["transaction_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        transaction_id = str(uuid.uuid4())
        producer.produce("transactions", key=transaction_id.encode(),
                         value=json.dumps({"transaction_id": transaction_id, "data": row}).encode())
        if producer.flush(10):
            raise RuntimeError("Kafka delivery timed out")
        index += 1
        time.sleep(1)


if __name__ == "__main__":
    main()
