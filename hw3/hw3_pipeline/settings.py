"""Configuration shared by tasks and the command-line verification."""

import os
from pathlib import Path

DATA_DIR = Path(os.getenv("HW3_DATA_DIR", "/data"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/reports"))
CSV_PATH = Path(os.getenv("LABELED_CSV_PATH", "/input/hw3_train.csv"))
WINDOW_MONTHS = int(os.getenv("WINDOW_MONTHS", "3"))
HISTORY_MONTHS = int(os.getenv("HISTORY_MONTHS", "36"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))
ITERATIONS = int(os.getenv("CATBOOST_ITERATIONS", "100"))

if WINDOW_MONTHS < 2 or HISTORY_MONTHS < WINDOW_MONTHS or BATCH_SIZE < 1:
    raise ValueError("Need window >= 2, history >= window, batch size > 0")
