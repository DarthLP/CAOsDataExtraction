"""qa_training_aggregate.py — Stage 4 aggregator + audit for training."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
QA = HERE.parent
PROJECT_ROOT = QA.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from qa.shared import (aggregator_lib, audit_lib, logging_util,
                       csv_recovery)  # noqa: E402


def main():
    print("=== Stage 4 (training) — aggregate + audit ===")
    # 0. Repair any malformed subagent CSV rows (unquoted ';') before aggregating
    rec = csv_recovery.recover_glob(
        QA / "outputs" / "subagent_worksheets" / "chunks")
    print(f"  csv recovery: {rec['total']}")
    paths = aggregator_lib.AggregationPaths(
        deterministic_csv=QA / "outputs" / "corrections_deterministic.csv",
        subagent_csv_glob=str(QA / "outputs" / "subagent_worksheets" / "chunks" / "chunk_*_corrections.csv"),
        corrections_out=QA / "outputs" / "corrections.csv",
        audit_out=QA / "outputs" / "corrections_audit.csv",
        topic="training",
    )
    res = aggregator_lib.run_aggregation(paths)
    print(f"  total rows: {res.total_rows}")
    print(f"  is_noop: {res.is_noop_count}")
    print(f"  real corrections: {res.real_corrections}")
    print(f"  by_fix_method: {res.by_fix_method}")
    print(f"  by_changed: {res.by_changed}")

    # Suppress confirmations where new value == existing CSV value (L2 false-changes).
    supn = aggregator_lib.suppress_noop_vs_actual_csv(
        paths.corrections_out, QA / "inputs" / "scoped_records.csv")
    print(f"  suppressed noop-vs-CSV rows: {supn}")
    # Suppress type-invalid boolean values in numeric fields BEFORE audit.
    supb = aggregator_lib.suppress_boolean_on_numeric_field(paths.corrections_out)
    print(f"  suppressed boolean-on-numeric rows: {supb}")

    # Audit
    print("\n--- Audit ---")
    audit = audit_lib.run_audit(paths.corrections_out)
    print(f"  audit-flagged keys: {len(audit.flagged_keys)}")
    smry = audit.summary()
    if smry:
        for aid, n in sorted(smry.items()):
            print(f"  {aid}: {n}")

    # NHR
    nhr_path = QA / "outputs" / "needs_human_review.csv"
    nhr_count = audit_lib.flag_outliers_as_needs_human(
        paths.corrections_out, audit, nhr_path)
    print(f"\n  NHR rows: {nhr_count}")

    # Suppress unit-only corrections whose paired value isn't a clean win
    # (runs after NHR flagging so it sees needs_human_review siblings).
    sup = aggregator_lib.suppress_unit_without_value(
        paths.corrections_out, QA / "inputs" / "scoped_records.csv")
    print(f"  suppressed unit-without-value rows: {sup}")

    # Summary: clean wins
    corr_df = pd.read_csv(paths.corrections_out, sep=";", dtype=str, keep_default_na=False)
    real_df = corr_df[corr_df["is_noop"].str.strip().str.lower() != "true"]
    nhr_keys = set()
    if nhr_path.exists():
        nhr_df = pd.read_csv(nhr_path, sep=";", dtype=str, keep_default_na=False)
        nhr_keys = {(r["record_id"], r["original_field"]) for _, r in nhr_df.iterrows()}
    clean_count = sum(1 for _, r in real_df.iterrows()
                      if (r["record_id"], r["original_field"]) not in nhr_keys)
    print(f"  clean wins: {clean_count}")

    logging_util.log_event("training", "stage_4",
                           "aggregator+audit complete",
                           total=res.total_rows,
                           real=res.real_corrections,
                           clean=clean_count,
                           nhr=nhr_count)


if __name__ == "__main__":
    main()
