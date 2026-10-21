"""Monthly labeled data simulation with transactional append and retention."""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from hw3_pipeline.supply import append_month, prune_history, sample_month


@dag(dag_id="supply_labeled_month", schedule="2-59/5 * * * *", catchup=False,
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(seconds=15)}, tags=["hw3", "supply"])
def supply():
    sampled = task(sample_month)("{{ run_id }}")
    appended = task(append_month)(sampled)
    task(prune_history)(appended)


supply()
