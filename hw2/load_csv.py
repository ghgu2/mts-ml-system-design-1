"""Publish one flat JSONEachRow message per CSV transaction to Kafka."""

import argparse
import json
import os
import uuid
from pathlib import Path

from dataset import reference_result, transactions


def publish_csv(path: Path, run_id: str, limit: int | None = None,
                brokers: str | None = None, topic: str | None = None) -> int:
    from confluent_kafka import Producer

    uuid.UUID(run_id)
    topic = topic or os.getenv("KAFKA_TOPIC", "hw2-transactions")
    producer = Producer({
        "bootstrap.servers": brokers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.size": 131072,
        "message.timeout.ms": 120000,
    })
    failures = []
    delivered = 0
    count = 0

    def delivery_report(error, message):
        nonlocal delivered
        if error is not None:
            if len(failures) < 5:
                failures.append(str(error))
        else:
            delivered += 1

    for record in transactions(path, run_id, limit):
        value = json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=record["us_state"], value=value,
                                 on_delivery=delivery_report)
                break
            except BufferError:
                producer.poll(0.1)
                if failures:
                    raise RuntimeError("Kafka delivery failed: " + "; ".join(failures))
        producer.poll(0)
        count += 1
        if count % 50000 == 0:
            print(f"Queued {count:,} transactions", flush=True)
    pending = producer.flush(130)
    if failures or pending or delivered != count:
        raise RuntimeError(
            f"Kafka delivery incomplete: sent={count}, acknowledged={delivered}, "
            f"pending={pending}, errors={failures}"
        )
    print(f"Kafka acknowledged {count:,} messages; topic={topic}; run_id={run_id}", flush=True)
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    parser.add_argument("--brokers")
    parser.add_argument("--topic")
    args = parser.parse_args()
    # Reject malformed files before sending any messages.
    reference_result(args.csv, args.limit)
    publish_csv(args.csv, args.run_id, args.limit, args.brokers, args.topic)


if __name__ == "__main__":
    main()
