"""qa_bonus_scope.py — Stage 1 scope filter for bonus topic (source: wage_information.md)."""

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
    print("=== Stage 1 (bonus) ===")
    full = scope_filter.load_full_csv()
    print(f"  total CSV records: {len(full)}")
    scoped = scope_filter.most_recent_doc_per_cao(full, "bonus")
    print(f"  scoped: {len(scoped)} ({scoped['cao_number'].nunique()} unique CAOs)")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    scoped.to_csv(OUT, sep=";", index=False)
    print(f"Wrote {OUT}")
    logging_util.log_event("bonus", "stage_1", "scope filter complete",
                           total_csv=len(full), scoped_records=len(scoped),
                           unique_caos=int(scoped['cao_number'].nunique()))


if __name__ == "__main__":
    main()
