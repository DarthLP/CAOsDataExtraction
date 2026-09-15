"""qa_fringe_aggregate.py — Stage 4 aggregator + audit for fringe.

Full pipeline with all data-quality guards:
  csv_recovery -> aggregate -> suppress noop-vs-CSV -> suppress boolean-on-numeric
  -> audit (incl. A18) -> flag NHR -> suppress unit-without-value.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
QA = HERE.parent
PROJECT_ROOT = QA.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from qa.shared import (aggregator_lib, audit_lib, logging_util, csv_recovery)  # noqa: E402


def main():
    print("=== Stage 4 (fringe) — aggregate + audit ===")
    rec = csv_recovery.recover_glob(QA / "outputs" / "subagent_worksheets" / "chunks")
    print(f"  csv recovery: {rec['total']}")
    paths = aggregator_lib.AggregationPaths(
        deterministic_csv=QA / "outputs" / "corrections_deterministic.csv",
        subagent_csv_glob=str(QA / "outputs" / "subagent_worksheets" / "chunks" / "chunk_*_corrections.csv"),
        corrections_out=QA / "outputs" / "corrections.csv",
        audit_out=QA / "outputs" / "corrections_audit.csv",
        topic="fringe",
    )
    res = aggregator_lib.run_aggregation(paths)
    print(f"  total rows: {res.total_rows}  is_noop: {res.is_noop_count}  real: {res.real_corrections}")

    supn = aggregator_lib.suppress_noop_vs_actual_csv(
        paths.corrections_out, QA / "inputs" / "scoped_records.csv")
    print(f"  suppressed noop-vs-CSV rows: {supn}")
    supb = aggregator_lib.suppress_boolean_on_numeric_field(paths.corrections_out)
    print(f"  suppressed boolean-on-numeric rows: {supb}")

    print("\n--- Audit ---")
    audit = audit_lib.run_audit(paths.corrections_out)
    for aid, n in sorted(audit.summary().items()):
        if n:
            print(f"  {aid}: {n}")
    nhr_path = QA / "outputs" / "needs_human_review.csv"
    nhr_count = audit_lib.flag_outliers_as_needs_human(paths.corrections_out, audit, nhr_path)
    print(f"  NHR rows: {nhr_count}")

    sup = aggregator_lib.suppress_unit_without_value(
        paths.corrections_out, QA / "inputs" / "scoped_records.csv")
    print(f"  suppressed unit-without-value rows: {sup}")

    corr_df = pd.read_csv(paths.corrections_out, sep=";", dtype=str, keep_default_na=False)
    real_df = corr_df[corr_df["is_noop"].str.strip().str.lower() != "true"]
    nhr_keys = set()
    if nhr_path.exists():
        nhr_df = pd.read_csv(nhr_path, sep=";", dtype=str, keep_default_na=False)
        nhr_keys = {(r["record_id"], r["original_field"]) for _, r in nhr_df.iterrows()}
    clean = sum(1 for _, r in real_df.iterrows()
                if (r["record_id"], r["original_field"]) not in nhr_keys)
    print(f"  clean wins: {clean}")
    logging_util.log_event("fringe", "stage_4", "aggregate+audit",
                           total=res.total_rows, real=res.real_corrections,
                           clean=clean, nhr=nhr_count)


if __name__ == "__main__":
    main()
