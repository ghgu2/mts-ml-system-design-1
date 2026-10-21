"""Monthly sampling, transactional append and bounded history retention."""

import calendar
import json
import pandas as pd

from hw3_pipeline import settings as s
from hw3_pipeline.retraining import write_parquet
from hw3_pipeline.store import connection, insert_frame, job_for_run, month_add, register_job, state


def shift_into_month(frame, month):
    """Copy rows into a calendar month; clip the 29th–31st to its last day."""
    result = frame.copy()
    source = pd.to_datetime(result["transaction_time"])
    target = pd.Timestamp(month)
    last = calendar.monthrange(target.year, target.month)[1]
    result["transaction_time"] = pd.to_datetime({
        "year": target.year, "month": target.month, "day": source.dt.day.clip(upper=last),
        "hour": source.dt.hour, "minute": source.dt.minute, "second": source.dt.second,
    }).dt.strftime("%Y-%m-%d %H:%M:%S")
    return result


def sample_month(workflow_run_id):
    """Freeze the next month and sample on disk; retries reuse the same month and rows."""
    with connection(write=True) as conn:
        previous = job_for_run(conn, "supply", workflow_run_id)
        if previous:
            return previous
        newest = conn.execute("SELECT max(transaction_time) FROM labeled_transactions").fetchone()[0]
        if not newest:
            raise ValueError("The labeled showcase is empty")
        month = month_add(newest, 1)
        required_start = month_add(state(conn)["next_month"], -(s.WINDOW_MONTHS - 1))
        cutoff = month_add(month, -(s.HISTORY_MONTHS - 1))
        if cutoff > required_start:
            raise RuntimeError("Supply would delete an unprocessed training window; retrain must catch up first")
        existing = conn.execute("SELECT context FROM jobs WHERE kind='supply' AND month=?", (month,)).fetchone()
        if existing:
            context = json.loads(existing[0])
        else:
            rows = conn.execute("SELECT payload FROM labeled_transactions ORDER BY random() LIMIT ?",
                                (s.BATCH_SIZE,)).fetchall()
            frame = pd.DataFrame([json.loads(r[0]) for r in rows])
            if len(frame) < s.BATCH_SIZE:
                frame = frame.sample(n=s.BATCH_SIZE, replace=True, random_state=42)
            frame = shift_into_month(frame.reset_index(drop=True), month)
            path = s.DATA_DIR / "supply" / f"{month}.parquet"
            write_parquet(frame, path)
            context = {"month": month, "path": str(path), "rows": len(frame)}
        return register_job(conn, "supply", workflow_run_id, month, context)


def append_month(context):
    """Append rows and their completion receipt in one transaction; a retry cannot duplicate data."""
    with connection(write=True) as conn:
        row = conn.execute("SELECT status FROM jobs WHERE kind='supply' AND month=?",
                           (context["month"],)).fetchone()
        if row is None:
            raise ValueError("Unregistered supply job")
        if row[0] == "complete":
            return context
        frame = pd.read_parquet(context["path"])
        insert_frame(conn, frame, context["month"])
        conn.execute("UPDATE jobs SET status='complete', result=? WHERE kind='supply' AND month=?",
                     (json.dumps(context), context["month"]))
    return context


def prune_history(context):
    """Delete outside the retained calendar months; repetition and old task clears are safe."""
    with connection(write=True) as conn:
        newest = conn.execute("SELECT max(transaction_time) FROM labeled_transactions").fetchone()[0]
        cutoff = month_add(newest, -(s.HISTORY_MONTHS - 1))
        required_start = month_add(state(conn)["next_month"], -(s.WINDOW_MONTHS - 1))
        if cutoff > required_start:
            raise RuntimeError("Refusing to prune an unprocessed training window")
        deleted = conn.execute("DELETE FROM labeled_transactions WHERE transaction_time < ?", (cutoff,)).rowcount
    return {**context, "cutoff": cutoff, "deleted": deleted}


def run_supply(run_id):
    return prune_history(append_month(sample_month(run_id)))
