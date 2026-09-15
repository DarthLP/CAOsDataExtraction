"""qa_homeoffice_scope.py — Stage 1 scope filter for homeoffice topic.

Added 2026-05-27 for template consistency (homeoffice originally did scope ad-hoc).
Standard 5-script layout; safe to re-run.
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
    print("=== Stage 1 (homeoffice) ===")
    full = scope_filter.load_full_csv()
    scoped = scope_filter.most_recent_doc_per_cao(full, "homeoffice")
    print(f"  scoped: {len(scoped)} ({scoped['cao_number'].nunique()} unique CAOs)")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    scoped.to_csv(OUT, sep=";", index=False)
    logging_util.log_event("homeoffice", "stage_1", "scope filter complete",
                           total_csv=len(full), scoped_records=len(scoped),
                           unique_caos=int(scoped['cao_number'].nunique()))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
