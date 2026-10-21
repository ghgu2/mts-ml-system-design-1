"""Separate task functions for immutable windows, CPU training, evaluation and commit."""

import json
import os
import shutil
import uuid
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

from hw3_pipeline import settings as s
from hw3_pipeline.quality import check_frame
from hw3_pipeline.store import connection, job_for_run, month_add, read_frame, register_job, state
from model_registry import atomic_json, checksum, read_bundle
from retraining_features import CATEGORIES, fit_preprocessor, transform


class NoNewMonth(Exception):
    """The next complete window is not available yet."""


def write_parquet(frame, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        frame.to_parquet(temp, index=False)
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def prepare_window(run_id):
    """Freeze the next fixed-length window and incumbent; retries never choose a new month."""
    with connection(write=True) as conn:
        previous = job_for_run(conn, "train", run_id)
        if previous:
            return previous
        cursor = state(conn)["next_month"]
        start = month_add(cursor, -(s.WINDOW_MONTHS - 1))
        frame = read_frame(conn, start, month_add(cursor, 1))
        if frame.empty or frame["transaction_time"].str[:7].max() < cursor[:7]:
            raise NoNewMonth(f"No labeled data for {cursor[:7]}; run supply_labeled_month first")
        actual = sorted(pd.to_datetime(frame["transaction_time"]).dt.strftime("%Y-%m-01").unique())
        expected = [month_add(start, n) for n in range(s.WINDOW_MONTHS)]
        if actual != expected:
            raise ValueError(f"Window has missing months: expected {expected}, found {actual}")
        version = "m" + cursor[:7].replace("-", "")
        directory = s.DATA_DIR / "runs" / version
        path = directory / "window.parquet"
        existing = conn.execute("SELECT context FROM jobs WHERE kind='train' AND month=?", (cursor,)).fetchone()
        if existing:
            context = json.loads(existing[0])
        else:
            write_parquet(frame, path)
            current = s.MODELS_DIR / "current.json"
            incumbent = json.loads(current.read_text())["version"] if current.exists() else None
            context = {"version": version, "month": cursor, "start": start,
                       "window_path": str(path), "directory": str(directory), "incumbent": incumbent}
        return register_job(conn, "train", run_id, cursor, context)


def split_by_time(context):
    """Read the frozen window; write earlier months to train and newest month to test."""
    frame = pd.read_parquet(context["window_path"])
    failures = [r for r in check_frame(frame) if not r["passed"]]
    if failures:
        raise ValueError(f"Training window failed DQ: {failures}")
    times = pd.to_datetime(frame["transaction_time"])
    test_mask = times >= pd.Timestamp(context["month"])
    train, test = frame.loc[~test_mask], frame.loc[test_mask]
    if train.empty or test.empty or train["target"].nunique() != 2 or test["target"].nunique() != 2:
        raise ValueError("Train and test must be nonempty and contain both classes")
    for name, subset in [("train", train), ("test", test)]:
        write_parquet(subset, Path(context["directory"]) / f"{name}.parquet")
    return context


def preprocess_training(context):
    """Fit statistics only on earlier train months; write frozen artifacts for both consumers."""
    train = pd.read_parquet(Path(context["directory"]) / "train.parquet")
    atomic_json(Path(context["directory"]) / "preprocessing.json", fit_preprocessor(train))
    return context


def train_candidate(context):
    """Train CPU CatBoost with fixed iterations; test labels never tune the candidate."""
    bundle = s.MODELS_DIR / "candidates" / context["version"]
    if (bundle / "manifest.json").exists():
        read_bundle(bundle)
        return context
    directory = Path(context["directory"])
    artifacts = json.loads((directory / "preprocessing.json").read_text())
    train = pd.read_parquet(directory / "train.parquet")
    model = CatBoostClassifier(iterations=s.ITERATIONS, depth=5, learning_rate=0.1,
                               loss_function="Logloss", auto_class_weights="Balanced",
                               random_seed=42, thread_count=2, task_type="CPU",
                               allow_writing_files=False, verbose=False)
    model.fit(transform(train, artifacts), train["target"], cat_features=CATEGORIES)
    staging = bundle.with_name(f".{bundle.name}.{uuid.uuid4().hex}.tmp")
    staging.mkdir(parents=True)
    try:
        model.save_model(str(staging / "model.cbm"))
        atomic_json(staging / "preprocessing.json", artifacts)
        atomic_json(staging / "manifest.json", {
            "version": context["version"], "schema": artifacts["schema"],
            "train_start": context["start"], "train_end_exclusive": context["month"],
            "test_month": context["month"], "train_rows": len(train),
            "sha256": {name: checksum(staging / name) for name in ["model.cbm", "preprocessing.json"]},
        })
        os.replace(staging, bundle)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return context


def metrics_for(bundle, test):
    model, artifacts, _ = read_bundle(bundle)
    score = model.predict_proba(transform(test, artifacts), thread_count=2)[:, 1]
    return {"average_precision": float(average_precision_score(test["target"], score)),
            "roc_auc": float(roc_auc_score(test["target"], score)),
            "f1_at_0_98": float(f1_score(test["target"], score > 0.98, zero_division=0))}


def passes_gate(candidate, incumbent):
    """Accept only if neither Average Precision nor ROC-AUC decreases on the same holdout."""
    import math
    keys = ["average_precision", "roc_auc"]
    return all(math.isfinite(candidate[k]) and math.isfinite(incumbent[k])
               and candidate[k] + 1e-12 >= incumbent[k] for k in keys)


def evaluate_candidate(context):
    """Score candidate and frozen incumbent on the same untouched latest month; persist decision."""
    path = Path(context["directory"]) / "evaluation.json"
    if path.exists():
        return context
    test = pd.read_parquet(Path(context["directory"]) / "test.parquet")
    candidate = metrics_for(s.MODELS_DIR / "candidates" / context["version"], test)
    incumbent_version = context["incumbent"]
    incumbent = metrics_for(s.MODELS_DIR / "releases" / incumbent_version, test) if incumbent_version else None
    accepted = incumbent is None or passes_gate(candidate, incumbent)
    atomic_json(path, {"version": context["version"], "incumbent_version": incumbent_version,
                       "test_month": context["month"], "test_rows": len(test),
                       "candidate": candidate, "incumbent": incumbent, "accepted": accepted,
                       "reason": "bootstrap" if incumbent is None else
                       "both ranking metrics did not decrease" if accepted else "ranking metric decreased"})
    return context


def publish_and_commit(context):
    """Publish a validated pair, then advance the cursor once; recover crashes after pointer swap.

Rejected candidates stay in candidates/. They were successfully saved and evaluated,
so the training cursor advances, but current.json and inference stay unchanged.
An old cleared DAG task returns its receipt and cannot republish an obsolete model.
"""
    with connection(write=True) as conn:
        row = conn.execute("SELECT status, result FROM jobs WHERE kind='train' AND month=?",
                           (context["month"],)).fetchone()
        if row is None:
            raise ValueError("Unregistered training job")
        if row["status"] == "complete":
            return json.loads(row["result"])
        if state(conn)["next_month"] != context["month"]:
            raise RuntimeError("Training cursor changed; refusing a stale publication")
        candidate = s.MODELS_DIR / "candidates" / context["version"]
        read_bundle(candidate)
        evaluation = json.loads((Path(context["directory"]) / "evaluation.json").read_text())
        if evaluation["version"] != context["version"] or evaluation["test_month"] != context["month"]:
            raise ValueError("Evaluation does not belong to this candidate")
        atomic_json(candidate / "evaluation.json", evaluation)
        if evaluation["accepted"]:
            pointer_path = s.MODELS_DIR / "current.json"
            current = json.loads(pointer_path.read_text())["version"] if pointer_path.exists() else None
            if current not in [context["incumbent"], context["version"]]:
                raise RuntimeError("Incumbent changed during training")
            release = s.MODELS_DIR / "releases" / context["version"]
            release.parent.mkdir(parents=True, exist_ok=True)
            if not release.exists():
                staging = release.with_name(f".{release.name}.{uuid.uuid4().hex}.tmp")
                try:
                    shutil.copytree(candidate, staging)
                    read_bundle(staging)
                    os.replace(staging, release)
                finally:
                    shutil.rmtree(staging, ignore_errors=True)
            read_bundle(release)
            atomic_json(pointer_path, {"version": context["version"]})
        conn.execute("UPDATE state SET next_month=? WHERE id=1", (month_add(context["month"], 1),))
        conn.execute("UPDATE jobs SET status='complete', result=? WHERE kind='train' AND month=?",
                     (json.dumps(evaluation), context["month"]))
        return evaluation


def run_retraining(run_id):
    """Execute the identical task functions outside Airflow, for bootstrap and integration checks."""
    context = prepare_window(run_id)
    for step in [split_by_time, preprocess_training, train_candidate, evaluate_candidate]:
        context = step(context)
    return publish_and_commit(context)
