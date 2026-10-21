"""Immutable model bundles and atomic publication of one version pointer."""

import hashlib
import json
import os
import uuid
from pathlib import Path

from catboost import CatBoostClassifier

from retraining_features import FEATURES, transform


def atomic_json(path, value):
    """Write JSON beside the destination and replace it atomically; retries are safe."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(value, file, ensure_ascii=False, indent=2, allow_nan=False)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def checksum(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_bundle(directory):
    """Validate a complete immutable bundle before a reader can switch models."""
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    for name in ["model.cbm", "preprocessing.json"]:
        if checksum(directory / name) != manifest["sha256"][name]:
            raise ValueError(f"Invalid bundle checksum: {directory / name}")
    artifacts = json.loads((directory / "preprocessing.json").read_text())
    model = CatBoostClassifier()
    model.load_model(str(directory / "model.cbm"))
    if model.feature_names_ != FEATURES:
        raise ValueError("Model feature schema mismatch")
    return model, artifacts, manifest


class VersionedScorer:
    """Hot-reload a validated model and its statistics as one pair, between messages."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.version = None
        self.model = None
        self.artifacts = None
        self.reload()

    def reload(self):
        import logging
        try:
            pointer = json.loads((self.directory / "current.json").read_text())
            version = pointer["version"]
            if version == self.version:
                return
            if Path(version).name != version:
                raise ValueError("Invalid model version")
            model, artifacts, manifest = read_bundle(self.directory / "releases" / version)
            if manifest["version"] != version:
                raise ValueError("Bundle version mismatch")
        except Exception:
            if self.model is None:
                raise
            logging.exception("New model bundle is invalid; keeping %s", self.version)
            return
        self.model, self.artifacts, self.version = model, artifacts, version
        logging.info("Loaded model version %s", version)

    def predict(self, transaction):
        import pandas as pd
        self.reload()
        features = transform(pd.DataFrame([transaction]), self.artifacts)
        score = float(self.model.predict_proba(features, thread_count=2)[0, 1])
        return score, int(score > 0.98), self.version
