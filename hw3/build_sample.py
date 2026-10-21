"""Reproduce the included six-month, proportionally stratified sample of train.csv."""

import argparse
from pathlib import Path

import pandas as pd


def build(source, output):
    frame = pd.read_csv(source, dtype={"post_code": str})
    periods = pd.to_datetime(frame["transaction_time"]).dt.to_period("M")
    parts = []
    for month in sorted(periods.unique())[:6]:
        subset = frame.loc[periods == month]
        count = min(2000, len(subset))
        positive = max(1, round(count * subset["target"].mean()))
        for label, size in [(1, positive), (0, count - positive)]:
            parts.append(subset.loc[subset["target"] == label].sample(n=size, random_state=42))
    result = pd.concat(parts).sort_values("transaction_time", kind="stable")
    result.to_csv(output, index=False)
    print(f"Saved {len(result)} rows, labels {result['target'].value_counts().to_dict()} to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, default=Path("examples/hw3_train.csv"))
    args = parser.parse_args()
    build(args.source, args.output)
