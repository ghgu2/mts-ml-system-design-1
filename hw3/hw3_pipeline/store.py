"""Transactional labeled showcase and durable run receipts in SQLite.

Airflow metadata is in PostgreSQL. This separate embedded database belongs only
to the data pipeline and is stored on a local Docker named volume, never NFS.
"""

import json
import sqlite3
from contextlib import contextmanager

import pandas as pd

from hw3_pipeline import settings as s


def month_add(month, count):
    return (pd.Period(month, freq="M") + count).strftime("%Y-%m-01")


@contextmanager
def connection(write=False):
    """Use one transaction for a mutation; rollback completely on any exception."""
    s.DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(s.DATA_DIR / "showcase.sqlite", timeout=60)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        if write:
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        if write:
            conn.commit()
    finally:
        conn.close()


def ensure_schema():
    """Create state and receipt tables once; existing data and cursors stay intact."""
    with connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY CHECK(id=1), next_month TEXT NOT NULL,
                window_months INTEGER NOT NULL, history_months INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS jobs (
                kind TEXT NOT NULL, month TEXT NOT NULL, context TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'prepared', result TEXT,
                PRIMARY KEY(kind, month));
            CREATE TABLE IF NOT EXISTS receipts (
                kind TEXT NOT NULL, run_id TEXT NOT NULL, month TEXT NOT NULL,
                PRIMARY KEY(kind, run_id));
        """)


def read_frame(conn, start=None, end=None):
    query = "SELECT payload FROM labeled_transactions"
    params = []
    if start is not None:
        query += " WHERE transaction_time >= ? AND transaction_time < ?"
        params = [start, end]
    query += " ORDER BY transaction_time, id"
    return pd.DataFrame([json.loads(row[0]) for row in conn.execute(query, params)])


def insert_frame(conn, frame, batch):
    """Append rows in the caller's transaction, together with its durable receipt."""
    frame = frame.copy()
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    records = json.loads(frame.to_json(orient="records"))
    conn.executemany(
        "INSERT INTO labeled_transactions(transaction_time, batch, payload) VALUES (?, ?, ?)",
        [(r["transaction_time"], batch, json.dumps(r, allow_nan=False)) for r in records])


def state(conn):
    row = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
    if row is None:
        raise RuntimeError("Initialize the showcase first")
    if row["window_months"] != s.WINDOW_MONTHS or row["history_months"] != s.HISTORY_MONTHS:
        raise ValueError("Window/history changed for an existing volume; use original settings or a new volume")
    return dict(row)


def job_for_run(conn, kind, run_id):
    row = conn.execute("""SELECT j.context FROM jobs j JOIN receipts r
        ON r.kind=j.kind AND r.month=j.month WHERE r.kind=? AND r.run_id=?""",
        (kind, run_id)).fetchone()
    return json.loads(row[0]) if row else None


def register_job(conn, kind, run_id, month, context):
    """Attach retries and replacement DAG runs to the same immutable monthly job."""
    conn.execute("INSERT OR IGNORE INTO jobs(kind, month, context) VALUES (?, ?, ?)",
                 (kind, month, json.dumps(context)))
    conn.execute("INSERT INTO receipts(kind, run_id, month) VALUES (?, ?, ?)",
                 (kind, run_id, month))
    return json.loads(conn.execute("SELECT context FROM jobs WHERE kind=? AND month=?",
                                  (kind, month)).fetchone()[0])
