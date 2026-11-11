"""Persist Kafka scoring results in PostgreSQL and export write metrics."""

import json
import logging
import os
from datetime import datetime, timezone
from time import perf_counter

import psycopg
from confluent_kafka import Consumer, KafkaException
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WRITTEN = Counter("fraud_writer_scores_written_total", "New scores committed to PostgreSQL")
FLAGGED = Counter("fraud_writer_flagged_written_total", "New fraud flags committed to PostgreSQL")
WRITE_TIME = Histogram(
    "fraud_writer_write_seconds", "Time for an INSERT and implicit commit in PostgreSQL",
    buckets=(0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5),
)
SCORES = Histogram(
    "fraud_writer_score", "Score distribution for newly committed rows",
    buckets=tuple(i / 20 for i in range(21)),
)

INSERT_SCORE = """
INSERT INTO scoring.scores
    (transaction_id, scored_at, score, fraud_flag, model_version, us_state, merch, cat_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
"""


def parse_score(message):
    result = json.loads(message.value())
    transaction_id = result["transaction_id"]
    score = float(result["score"])
    flag = result["fraud_flag"]
    version = result.get("model_version") or "unknown"
    if not isinstance(transaction_id, str) or not transaction_id:
        raise ValueError("Missing transaction ID")
    if not 0 <= score <= 1 or flag not in (0, 1) or not isinstance(version, str):
        raise ValueError("Invalid score, fraud flag or model version")
    _, timestamp_ms = message.timestamp()
    scored_at = (
        datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc)
        if timestamp_ms >= 0 else datetime.now(timezone.utc)
    )
    headers = dict(message.headers() or ())
    dimensions = [headers.get(field) for field in ("us_state", "merch", "cat_id")]
    return (
        transaction_id, scored_at, score, int(flag), version,
        *(value.decode("utf-8") if value else None for value in dimensions),
    )


def main():
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": "hw11-scoring-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.environ["KAFKA_SCORING_TOPIC"]])
    start_http_server(8001)
    try:
        with psycopg.connect(autocommit=True) as database:
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    logger.error("Kafka poll error: %s", message.error())
                    continue
                try:
                    row = parse_score(message)
                except (ValueError, KeyError, TypeError, AttributeError, OverflowError, OSError):
                    logger.exception("Skipping invalid score at offset %s", message.offset())
                else:
                    started_at = perf_counter()
                    inserted = database.execute(INSERT_SCORE, row).rowcount
                    elapsed = perf_counter() - started_at
                    if inserted:
                        WRITTEN.inc()
                        FLAGGED.inc(row[3])
                        WRITE_TIME.observe(elapsed)
                        SCORES.observe(row[2])
                for partition in consumer.commit(message=message, asynchronous=False):
                    if partition.error:
                        raise KafkaException(partition.error)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
