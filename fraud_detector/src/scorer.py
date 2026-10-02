"""CPU inference with the supplied trained CatBoost model."""

from pathlib import Path

from catboost import CatBoostClassifier
from preprocessing import FEATURE_COLUMNS

MODEL_PATH = Path(__file__).resolve().parents[1] / "models/my_catboost.cbm"
FRAUD_THRESHOLD = 0.98
model = CatBoostClassifier()
model.load_model(str(MODEL_PATH))
if model.feature_names_ != FEATURE_COLUMNS:
    raise RuntimeError("Model and preprocessing feature columns do not match")


def make_prediction(features):
    score = float(model.predict_proba(features)[0, 1])
    return score, int(score > FRAUD_THRESHOLD)
