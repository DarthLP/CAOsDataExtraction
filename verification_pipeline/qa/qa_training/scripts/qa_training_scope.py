"""qa_training_scope.py — Stage 1 scope filter for training topic.

Filters the full extracted_data_non_salary.csv to the curated 95-CAO scope,
keeps the most-recent ingangsdatum per CAO with non-empty training source.
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
QA = HERE.parent
PROJECT_ROOT = QA.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from qa.shared import scope_filter, logging_util  # noqa: E402

OUT = QA / "inputs" / "scoped_records.csv"


def main():
    print("=== Stage 1 (training) ===")
    full = scope_filter.load_full_csv()
    print(f"  total CSV records: {len(full)}")
    scoped = scope_filter.most_recent_doc_per_cao(full, "training")
    print(f"  scoped records: {len(scoped)} ({scoped['cao_number'].nunique()} unique CAOs)")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    scoped.to_csv(OUT, sep=";", index=False)
    print(f"Wrote {OUT}")
    logging_util.log_event("training", "stage_1", "scope filter complete",
                           total_csv=len(full),
                           scoped_records=len(scoped),
                           unique_caos=int(scoped['cao_number'].nunique()))


if __name__ == "__main__":
    main()
