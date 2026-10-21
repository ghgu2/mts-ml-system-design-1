"""The HTML report is saved before the task enforcing DQ can fail."""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from hw3_pipeline.quality import enforce_quality, render_report, snapshot_quality


@dag(dag_id="check_data_quality", schedule="4-59/5 * * * *", catchup=False,
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), max_active_runs=1,
     default_args={"retries": 1, "retry_delay": timedelta(seconds=15)}, tags=["hw3", "quality"])
def quality():
    snapshot = task(snapshot_quality)("{{ run_id }}")
    report = task(render_report)(snapshot)
    task(enforce_quality)(report)


quality()
