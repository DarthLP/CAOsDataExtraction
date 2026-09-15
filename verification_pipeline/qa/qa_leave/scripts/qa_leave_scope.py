"""qa_leave_scope.py — Stage 1 scope filter for leave (STANDARDIZED re-run).

NOTE: the reference implementation is the top-level qa_leave/ dir (bespoke, do not
modify). This qa/qa_leave/ is a fresh run through the standardized pipeline, to
compare against that reference. Source: leave_information.md.
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
    print("=== Stage 1 (leave, standardized re-run) ===")
    full = scope_filter.load_full_csv()
    scoped = scope_filter.most_recent_doc_per_cao(full, "leave")
    print(f"  scoped: {len(scoped)} ({scoped['cao_number'].nunique()} unique CAOs)")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    scoped.to_csv(OUT, sep=";", index=False)
    logging_util.log_event("leave", "stage_1", "scope filter complete (standardized re-run)",
                           total_csv=len(full), scoped_records=len(scoped),
                           unique_caos=int(scoped['cao_number'].nunique()))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
