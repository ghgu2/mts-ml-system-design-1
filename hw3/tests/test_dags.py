"""Parse the real DAGs in the Airflow image; no scheduler or database is needed."""

import importlib.util
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Runs inside the Airflow image")
class DagTests(unittest.TestCase):
    def test_all_graphs_parse_and_have_explicit_dependencies(self):
        from airflow.models import DagBag
        from airflow.utils.context import KNOWN_CONTEXT_KEYS
        from airflow.utils.operator_helpers import KeywordParameters
        directory = Path(__file__).resolve().parents[1] / "dags"
        bag = DagBag(str(directory), include_examples=False)
        self.assertEqual({}, bag.import_errors)
        expected = {"retrain_fraud_model": 6, "supply_labeled_month": 3, "check_data_quality": 3}
        self.assertEqual(set(expected), set(bag.dags))
        for name, count in expected.items():
            dag = bag.dags[name]
            self.assertEqual(count, len(dag.tasks))
            self.assertFalse(dag.catchup)
            self.assertEqual(1, dag.max_active_runs)
            self.assertIsNotNone(dag.schedule_interval)
            self.assertEqual(count - 1, sum(len(t.downstream_task_ids) for t in dag.tasks))
            for task in dag.tasks:
                KeywordParameters.determine(task.python_callable, task.op_args,
                                            {key: None for key in KNOWN_CONTEXT_KEYS})
