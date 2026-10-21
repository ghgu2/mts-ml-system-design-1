"""Regression checks for publication crashes, retry receipts, leakage and hot reload."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from hw3_pipeline import settings as s
from hw3_pipeline.initialize import initialize
from hw3_pipeline.quality import check_frame, enforce_quality, render_report, snapshot_quality
from hw3_pipeline.retraining import (
    evaluate_candidate, passes_gate, prepare_window, preprocess_training,
    publish_and_commit, run_retraining, split_by_time, train_candidate,
)
from hw3_pipeline.store import connection, state
from hw3_pipeline.supply import append_month, prune_history, sample_month, shift_into_month
from model_registry import VersionedScorer, atomic_json
from retraining_features import fit_preprocessor, transform


def fixture_frame():
    rows = []
    for month in range(1, 7):
        for index in range(120):
            label = int(index % 10 == 0)
            rows.append({"transaction_time": f"2019-{month:02d}-{index % 28 + 1:02d} 12:00:00",
                         "merch": "shop" + str(index % 3), "cat_id": "food", "gender": "F",
                         "one_city": "City", "us_state": "CA", "jobs": "Engineer",
                         "amount": 500.0 if label else 5.0, "population_city": 1000,
                         "lat": 40.0, "lon": -90.0, "merchant_lat": 40.1, "merchant_lon": -90.1,
                         "target": label})
    return pd.DataFrame(rows)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.saved = {key: getattr(s, key) for key in
                      ["DATA_DIR", "MODELS_DIR", "REPORTS_DIR", "CSV_PATH", "ITERATIONS", "BATCH_SIZE", "HISTORY_MONTHS"]}
        s.DATA_DIR, s.MODELS_DIR, s.REPORTS_DIR = root / "data", root / "models", root / "reports"
        s.CSV_PATH, s.ITERATIONS, s.BATCH_SIZE = root / "train.csv", 6, 200
        s.HISTORY_MONTHS = 36
        self.frame = fixture_frame()
        self.frame.to_csv(s.CSV_PATH, index=False)
        initialize()

    def tearDown(self):
        for key, value in self.saved.items():
            setattr(s, key, value)
        self.temp.cleanup()

    def cursor(self):
        with connection() as conn:
            return state(conn)["next_month"]

    def current(self):
        return json.loads((s.MODELS_DIR / "current.json").read_text())["version"]

    def prepared_candidate(self, run="manual-test"):
        context = prepare_window(run)
        split_by_time(context)
        preprocess_training(context)
        train_candidate(context)
        return context

    def count(self):
        with connection() as conn:
            return conn.execute("SELECT count(*) FROM labeled_transactions").fetchone()[0]

    def test_bootstrap_is_idempotent(self):
        before = self.cursor(), self.current(), self.count()
        initialize()
        self.assertEqual(before, (self.cursor(), self.current(), self.count()))

    def test_train_only_artifacts_and_unseen_categories(self):
        train = self.frame.iloc[:240]
        original = fit_preprocessor(train)
        self.assertEqual(original, fit_preprocessor(train.assign(target=1 - train.target)))
        unseen = self.frame.iloc[240:241].copy()
        unseen["merch"] = "brand_new"
        unseen["amount"] = np.nan
        features = transform(unseen, original)
        self.assertEqual(0.0, features.iloc[0]["merch_frequency"])
        self.assertTrue(np.isfinite(features.select_dtypes("number")).all().all())
        pd.testing.assert_frame_equal(transform(unseen, original), transform(unseen.drop(columns="target"), original))

    def test_fixed_window_time_split_and_same_month_after_error(self):
        context = self.prepared_candidate()
        self.assertEqual(context, prepare_window("replacement-run-after-failure"))
        self.assertEqual(context, prepare_window("manual-test"))
        train = pd.read_parquet(Path(context["directory"]) / "train.parquet")
        test = pd.read_parquet(Path(context["directory"]) / "test.parquet")
        self.assertLess(train.transaction_time.max(), test.transaction_time.min())
        self.assertEqual({"2019-02", "2019-03"}, set(train.transaction_time.str[:7]))
        self.assertEqual({"2019-04"}, set(test.transaction_time.str[:7]))
        self.assertEqual("2019-04-01", self.cursor())

    def test_supply_retry_does_not_duplicate_or_skip_month(self):
        context = sample_month("supply-test")
        before = self.count()
        append_month(context)
        append_month(context)
        self.assertEqual(before + s.BATCH_SIZE, self.count())
        self.assertEqual(context, sample_month("supply-test"))
        self.assertEqual("2019-08-01", sample_month("new-run")["month"])

    def test_append_failure_rolls_back_rows_and_receipt(self):
        from hw3_pipeline.supply import insert_frame
        context = sample_month("supply-test")
        before = self.count()

        def insert_and_fail(*args):
            insert_frame(*args)
            raise RuntimeError("Simulated crash before commit")

        with patch("hw3_pipeline.supply.insert_frame", side_effect=insert_and_fail):
            with self.assertRaises(RuntimeError):
                append_month(context)
        self.assertEqual(before, self.count())
        append_month(context)
        self.assertEqual(before + s.BATCH_SIZE, self.count())

    def test_rejected_candidate_saved_but_never_published(self):
        before = self.current()
        context = self.prepared_candidate()
        with patch("hw3_pipeline.retraining.passes_gate", return_value=False):
            evaluate_candidate(context)
        result = publish_and_commit(context)
        self.assertFalse(result["accepted"])
        self.assertEqual(before, self.current())
        self.assertEqual("2019-05-01", self.cursor())
        self.assertTrue((s.MODELS_DIR / "candidates" / context["version"] / "model.cbm").exists())
        self.assertFalse((s.MODELS_DIR / "releases" / context["version"]).exists())

    def test_pointer_swap_crash_recovers_without_skipping_month(self):
        context = self.prepared_candidate()
        with patch("hw3_pipeline.retraining.passes_gate", return_value=True):
            evaluate_candidate(context)

        def publish_then_fail(path, value):
            atomic_json(path, value)
            if Path(path).name == "current.json":
                raise RuntimeError("Simulated crash between filesystem and DB commit")

        with patch("hw3_pipeline.retraining.atomic_json", side_effect=publish_then_fail):
            with self.assertRaises(RuntimeError):
                publish_and_commit(context)
        self.assertEqual(context["version"], self.current())
        self.assertEqual("2019-04-01", self.cursor())
        publish_and_commit(context)
        publish_and_commit(context)
        self.assertEqual("2019-05-01", self.cursor())

    def test_old_task_clear_cannot_roll_back_deployed_model(self):
        first = self.prepared_candidate("first")
        with patch("hw3_pipeline.retraining.passes_gate", return_value=True):
            evaluate_candidate(first)
            publish_and_commit(first)
            run_retraining("second")
        latest = self.current()
        publish_and_commit(first)
        self.assertEqual(latest, self.current())
        self.assertEqual(first, prepare_window("first"))

    def test_hot_reload_changes_version_without_new_scorer(self):
        scorer = VersionedScorer(s.MODELS_DIR)
        transaction = self.frame.iloc[0].to_dict()
        before = scorer.predict(transaction)
        with patch("hw3_pipeline.retraining.passes_gate", return_value=True):
            result = run_retraining("new-model")
        after = scorer.predict(transaction)
        self.assertNotEqual(before[2], after[2])
        self.assertEqual(result["version"], after[2])

    def test_broken_bundle_or_pointer_keeps_previous_pair(self):
        scorer = VersionedScorer(s.MODELS_DIR)
        before = scorer.predict(self.frame.iloc[0].to_dict())
        atomic_json(s.MODELS_DIR / "current.json", {"version": "missing-version"})
        with self.assertLogs(level="ERROR"):
            after = scorer.predict(self.frame.iloc[0].to_dict())
        self.assertEqual(before, after)
        (s.MODELS_DIR / "current.json").write_text("broken JSON")
        with self.assertLogs(level="ERROR"):
            self.assertEqual(before, scorer.predict(self.frame.iloc[0].to_dict()))

    def test_quality_report_survives_validation_failure(self):
        broken = self.frame.copy()
        broken.loc[0, "lat"] = 200
        with patch("hw3_pipeline.quality.read_frame", return_value=broken):
            path = snapshot_quality("bad-data")
        report = render_report(path)
        self.assertFalse(report["passed"])
        self.assertIn("FAIL", Path(report["html"]).read_text())
        with self.assertRaises(ValueError):
            enforce_quality(report)
        self.assertEqual(10, len(check_frame(self.frame)))

    def test_retention_preserves_unprocessed_window_and_then_catches_up(self):
        s.HISTORY_MONTHS = 6
        with connection(write=True) as conn:
            conn.execute("UPDATE state SET history_months=6")
        context = append_month(sample_month("first-month"))
        self.assertGreater(prune_history(context)["deleted"], 0)
        with self.assertRaises(RuntimeError):
            sample_month("too-far-ahead")
        run_retraining("catch-up")
        self.assertEqual("2019-08-01", sample_month("allowed-now")["month"])


class PureRulesTests(unittest.TestCase):
    def test_gate_rejects_any_decrease_and_nonfinite_metrics(self):
        incumbent = {"average_precision": 0.7, "roc_auc": 0.8}
        self.assertTrue(passes_gate(incumbent, incumbent))
        self.assertFalse(passes_gate({"average_precision": 0.69, "roc_auc": 0.9}, incumbent))
        self.assertFalse(passes_gate({"average_precision": 0.9, "roc_auc": 0.79}, incumbent))
        self.assertFalse(passes_gate({"average_precision": float("nan"), "roc_auc": 0.9}, incumbent))

    def test_month_end_and_leap_year(self):
        frame = fixture_frame().iloc[:1].copy()
        frame["transaction_time"] = "2019-01-31 23:59:58"
        self.assertEqual("2019-02-28 23:59:58", shift_into_month(frame, "2019-02-01").iloc[0].transaction_time)
        self.assertEqual("2020-02-29 23:59:58", shift_into_month(frame, "2020-02-01").iloc[0].transaction_time)


if __name__ == "__main__":
    unittest.main()
