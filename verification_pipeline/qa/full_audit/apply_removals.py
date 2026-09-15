"""Apply the RECONCILE + EMPTY absence-removals to a COPY of qa/corrected_dataset.csv.

Per Hanna's 2026-07-01 decision (docs/DECISIONS.md "Backlog apply plan"): these ~3,808
removals were sample-verified against source first (see scratchpad removals_verify /
full_audit/removals_sample_verification.csv). Cells whose sample verdict was SUPPORTED
or UNSURE are EXCLUDED here and routed to removals_excluded_by_verification.csv.

Rules (mirrors apply_perfile_fixes.py):
- new value = fix_target_value from ALL_actions (RECONCILE booleans -> 'False';
  numerics/text -> '' when blank).
- When a value cell is cleared to '', its paired *_unit cell is cleared too (an
  orphan unit is noise, per the aggregator guard chain).
- Guard: the current cell must equal the recorded dataset_value, else skip+log.
- Output: qa/corrected_dataset.removals_applied.csv + removals_apply_changelog.csv.
  The base file is NOT touched; promotion is a separate, explicit step.

Run: python3.13 -m qa.full_audit.apply_removals [--exclude-csv PATH] [--limit N]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
BASE = common.PROJECT_ROOT / "qa" / "corrected_dataset.csv"
OUT = common.PROJECT_ROOT / "qa" / "corrected_dataset.removals_applied.csv"
ACTIONS = HERE / "perfile_work" / "ALL_actions.csv"
CHANGELOG = HERE / "removals_apply_changelog.csv"
EXCLUDED_OUT = HERE / "removals_excluded_by_verification.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exclude-csv", default="", help="CSV with record_id;field rows to exclude (verified SUPPORTED/UNSURE)")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    info = common.classify_columns()
    df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    base = df.copy()
    colset = set(df.columns)
    idx = {rid: i for i, rid in enumerate(df["id"])}

    aa = pd.read_csv(ACTIONS, sep=";", dtype=str, keep_default_na=False,
                     engine="python", on_bad_lines="skip")
    rem = aa[aa["proposed_action"].isin(["RECONCILE", "EMPTY"])].copy()

    excl_keys: set[tuple[str, str]] = set()
    if a.exclude_csv:
        ex = pd.read_csv(a.exclude_csv, sep=";", dtype=str, keep_default_na=False)
        excl_keys = {(r.record_id, r.field) for r in ex.itertuples()}
    excluded = rem[[(r.record_id, r.field) in excl_keys for r in rem.itertuples()]]
    rem = rem[[(r.record_id, r.field) not in excl_keys for r in rem.itertuples()]]
    if a.limit:
        rem = rem.head(a.limit)

    changes, skipped = [], []
    for _, r in rem.iterrows():
        rid, col = r.record_id, r.field
        if rid not in idx or col not in colset:
            skipped.append((rid, col, "missing")); continue
        i = idx[rid]
        cur, exp = str(df.at[i, col]).strip(), str(r.dataset_value).strip()
        if cur != exp:
            skipped.append((rid, col, f"current={cur!r}!=expected={exp!r}")); continue
        new = str(r.fix_target_value).strip()
        df.at[i, col] = new
        ci = info.get(col)
        ucol = ci.unit_partner if ci else None
        old_unit = new_unit = ""
        if new == "" and ucol and ucol in colset:
            old_unit = str(df.at[i, ucol]).strip()
            if old_unit:
                df.at[i, ucol] = ""
                new_unit = ""
        changes.append({"record_id": rid, "topic": r.topic, "field": col,
                        "action": r.proposed_action, "old_value": exp, "new_value": new,
                        "unit_field": (ucol if old_unit else ""), "old_unit": old_unit,
                        "new_unit": new_unit, "explanation": str(r.explanation)[:200]})

    df.to_csv(OUT, sep=";", index=False)
    pd.DataFrame(changes).to_csv(CHANGELOG, sep=";", index=False)
    if len(excluded):
        excluded.to_csv(EXCLUDED_OUT, sep=";", index=False)

    ne = (df.values != base.values)
    total_cell_diff = int(ne.sum())
    logged_cells = len(changes) + sum(1 for c in changes if c["unit_field"])
    print(f"applied {len(changes)} removals -> {OUT.name}")
    print(f"  paired units cleared: {sum(1 for c in changes if c['unit_field'])}")
    print(f"  excluded by verification: {len(excluded)} -> {EXCLUDED_OUT.name if len(excluded) else '-'}")
    print(f"  cells differing from original = {total_cell_diff} (expected = {logged_cells}) MATCH={total_cell_diff==logged_cells}")
    print(f"  rows = {len(df)} (== base: {len(df)==len(base)}) | skipped = {len(skipped)}")
    if skipped:
        print(f"  first skips: {skipped[:5]}")


if __name__ == "__main__":
    main()
