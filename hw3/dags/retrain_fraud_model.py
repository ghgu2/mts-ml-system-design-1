"""Only DAG wiring; every task implementation is a separate importable function."""

from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from hw3_pipeline.retraining import (
    NoNewMonth, evaluate_candidate, prepare_window, preprocess_training,
    publish_and_commit, split_by_time, train_candidate,
)


@task(task_id="prepare_window")
def prepare(workflow_run_id):
    """Adapt the domain's no-new-data result into Airflow's skipped state."""
    try:
        return prepare_window(workflow_run_id)
    except NoNewMonth as error:
        raise AirflowSkipException(str(error)) from error


@dag(dag_id="retrain_fraud_model", schedule="*/5 * * * *", catchup=False,
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
     tags=["hw3", "training"], dagrun_timeout=timedelta(minutes=15))
def retraining():
    window = prepare("{{ run_id }}")
    split = task(split_by_time)(window)
    preprocessed = task(preprocess_training)(split)
    candidate = task(train_candidate)(preprocessed)
    evaluated = task(evaluate_candidate)(candidate)
    task(publish_and_commit)(evaluated)


retraining()
