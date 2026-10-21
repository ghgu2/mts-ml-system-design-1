"""First-start labeled data import and a baseline trained without future-month leakage."""

import json

import pandas as pd

from hw3_pipeline import settings as s
from hw3_pipeline.quality import check_frame
from hw3_pipeline.retraining import run_retraining
from hw3_pipeline.store import connection, ensure_schema, insert_frame, month_add, state


def initialize():
    """Atomically import CSV once, then resume the same bootstrap job after any failure."""
    ensure_schema()
    with connection(write=True) as conn:
        if conn.execute("SELECT count(*) FROM state").fetchone()[0] == 0:
            frame = pd.read_csv(s.CSV_PATH, dtype={"post_code": str})
            failures = [r for r in check_frame(frame) if not r["passed"]]
            if failures:
                raise ValueError(f"Input CSV failed DQ: {failures}")
            frame["transaction_time"] = pd.to_datetime(frame["transaction_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            first = frame["transaction_time"].min()
            last = frame["transaction_time"].max()
            if month_add(first, s.WINDOW_MONTHS - 1)[:7] > last[:7]:
                raise ValueError("Input needs at least WINDOW_MONTHS calendar months")
            conn.execute("""CREATE TABLE IF NOT EXISTS labeled_transactions (
                id INTEGER PRIMARY KEY, transaction_time TEXT NOT NULL,
                batch TEXT NOT NULL, payload TEXT NOT NULL)""")
            conn.execute("CREATE INDEX IF NOT EXISTS transactions_time ON labeled_transactions(transaction_time)")
            insert_frame(conn, frame, "initial_csv")
            conn.execute("INSERT INTO state VALUES (1, ?, ?, ?)",
                         (month_add(first, s.WINDOW_MONTHS - 1), s.WINDOW_MONTHS, s.HISTORY_MONTHS))
        else:
            state(conn)
    result = run_retraining("bootstrap")
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    initialize()
