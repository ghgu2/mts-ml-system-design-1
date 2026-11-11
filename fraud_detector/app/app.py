"""Consume transactions, calculate scores and publish the results."""

import json
import logging
import os
import sys
from pathlib import Path
from time import perf_counter

from confluent_kafka import Consumer, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
OUTPUT_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


def main():
    models_dir = os.getenv("MODELS_DIR")
    versioned = None
    if models_dir:
        from model_registry import VersionedScorer
        versioned = VersionedScorer(models_dir)
    else:
        from preprocessing import load_artifacts, prepare_transaction
        from scorer import make_prediction
        artifacts = load_artifacts(Path(__file__).resolve().parents[1] / "models/preprocessing.json")
    metrics = None
    if os.getenv("SCORING_METRICS_PORT"):
        from metrics import ScoringMetrics
        metrics = ScoringMetrics(int(os.environ["SCORING_METRICS_PORT"]))
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": "ml-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": BROKERS})
    consumer.subscribe([INPUT_TOPIC])
    logger.info("Scoring service is listening to %s", INPUT_TOPIC)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue
            started_at = perf_counter()
            try:
                payload = json.loads(message.value())
                transaction_id = str(payload["transaction_id"])
                transaction = payload["data"]
                if versioned:
                    previous_version = versioned.version
                    try:
                        score, fraud_flag, model_version = versioned.predict(transaction)
                    finally:
                        if metrics and versioned.version != previous_version:
                            metrics.record_model_switch(versioned.version)
                else:
                    features = prepare_transaction(transaction, artifacts)
                    score, fraud_flag = make_prediction(features)
            except Exception:
                logger.exception("Failed to score message at offset %s", message.offset())
                continue

            result = {
                "transaction_id": transaction_id,
                "score": score,
                "fraud_flag": fraud_flag,
            }
            if versioned:
                result["model_version"] = model_version
            headers = []
            if metrics:
                for field in ("us_state", "merch", "cat_id"):
                    value = transaction.get(field)
                    if isinstance(value, str) and value.strip():
                        headers.append((field, value.strip()))
            publish_started_at = perf_counter()
            delivery_errors = []
            producer.produce(
                OUTPUT_TOPIC,
                key=transaction_id.encode("utf-8"),
                value=json.dumps(result).encode("utf-8"),
                headers=headers,
                on_delivery=lambda error, _: delivery_errors.append(error) if error else None,
            )
            if producer.flush(10) or delivery_errors:
                raise RuntimeError(f"Kafka delivery failed: {delivery_errors}")
            publish_seconds = perf_counter() - publish_started_at
            if metrics:
                metrics.record_score(score, fraud_flag, perf_counter() - started_at, publish_seconds)
            consumer.commit(message=message, asynchronous=False)
            logger.info("Scored %s: %.5f, fraud_flag=%d", transaction_id, score, fraud_flag)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
