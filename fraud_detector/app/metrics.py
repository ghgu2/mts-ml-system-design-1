"""Prometheus instrumentation for the online scoring service."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server


class ScoringMetrics:
    def __init__(self, port):
        self.total = 0
        self.total_flagged = 0
        self.processed = Counter("fraud_scoring_processed_total", "Results delivered to Kafka")
        self.flagged = Counter("fraud_scoring_flagged_total", "Delivered results flagged as fraud")
        self.fraud_ratio = Gauge("fraud_scoring_fraud_ratio", "Fraction of results flagged since startup")
        self.processing = Histogram(
            "fraud_scoring_processing_seconds", "Time from receipt through Kafka delivery",
            buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
        )
        self.scores = Histogram(
            "fraud_scoring_score", "Score distribution for results delivered to Kafka",
            buckets=tuple(i / 20 for i in range(21)),
        )
        self.kafka_publish = Histogram(
            "fraud_scoring_kafka_publish_seconds", "Time to publish a result and receive Kafka acknowledgement",
            buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 5, 10),
        )
        self.model_switches = Counter(
            "fraud_scoring_model_switches_total", "Successful in-process model reloads", ["model_version"],
        )
        self.model_switch_time = Gauge(
            "fraud_scoring_model_switch_timestamp_seconds", "Unix time of last reload of this model",
            ["model_version"],
        )
        start_http_server(port)

    def record_model_switch(self, version):
        self.model_switches.labels(model_version=version).inc()
        self.model_switch_time.labels(model_version=version).set_to_current_time()

    def record_score(self, score, fraud_flag, processing_seconds, publish_seconds):
        self.total += 1
        self.total_flagged += fraud_flag
        self.processed.inc()
        self.flagged.inc(fraud_flag)
        self.fraud_ratio.set(self.total_flagged / self.total)
        self.processing.observe(processing_seconds)
        self.scores.observe(score)
        self.kafka_publish.observe(publish_seconds)
