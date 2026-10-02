"""Consume transactions, calculate scores and publish the results."""

import json
import logging
import os
import sys
from pathlib import Path

from confluent_kafka import Consumer, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from preprocessing import load_artifacts, prepare_transaction
from scorer import make_prediction

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
OUTPUT_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


def main():
    artifacts = load_artifacts(Path(__file__).resolve().parents[1] / "models/preprocessing.json")
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
            try:
                payload = json.loads(message.value())
                transaction_id = str(payload["transaction_id"])
                features = prepare_transaction(payload["data"], artifacts)
                score, fraud_flag = make_prediction(features)
                result = {
                    "transaction_id": transaction_id,
                    "score": score,
                    "fraud_flag": fraud_flag,
                }
                delivery_errors = []
                producer.produce(
                    OUTPUT_TOPIC,
                    key=transaction_id.encode("utf-8"),
                    value=json.dumps(result).encode("utf-8"),
                    on_delivery=lambda error, _: delivery_errors.append(error) if error else None,
                )
                if producer.flush(10) or delivery_errors:
                    raise RuntimeError(f"Kafka delivery failed: {delivery_errors}")
                consumer.commit(message=message, asynchronous=False)
                logger.info("Scored %s: %.5f, fraud_flag=%d", transaction_id, score, fraud_flag)
            except Exception:
                logger.exception("Failed to score message at offset %s", message.offset())
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
