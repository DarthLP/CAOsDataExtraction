"""Apply the ADJUDICATED FIX_audit + surcharge_normalize + statutory_deferred corrections
(Layer 5) to a COPY of qa/corrected_dataset.csv.

Input: an adjudicated_fixes.csv produced by the consolidation step — every row was judged
under Hanna's conventions (docs/DECISIONS.md 2026-07-01: CAO base figure excluding statutory
add-ons; surcharge = increment not total; enums only when the quote is unambiguous) and passed
deterministic validation. Residue/undecided rows are NOT in this file.

Guards mirror apply_perfile_fixes.py / apply_removals.py:
- current cell must equal the recorded dataset_value, else skip+log;
- paired *_unit updated when a final_unit is supplied (or cleared when value is cleared);
- output copy + reversible changelog; base file untouched; promotion is a separate step.

Run: python3.13 -m qa.full_audit.apply_fix_audit --fixes PATH
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
BASE = common.PROJECT_ROOT / "qa" / "corrected_dataset.csv"
OUT = common.PROJECT_ROOT / "qa" / "corrected_dataset.fixaudit_applied.csv"
CHANGELOG = HERE / "fixaudit_apply_changelog.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixes", required=True)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    info = common.classify_columns()
    df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    base = df.copy()
    colset = set(df.columns)
    idx = {rid: i for i, rid in enumerate(df["id"])}

    fx = pd.read_csv(a.fixes, sep=";", dtype=str, keep_default_na=False)
    if a.limit:
        fx = fx.head(a.limit)

    changes, skipped = [], []
    for _, r in fx.iterrows():
        rid, col = r.record_id, r.field
        if rid not in idx or col not in colset:
            skipped.append((rid, col, "missing")); continue
        i = idx[rid]
        cur, exp = str(df.at[i, col]).strip(), str(r.dataset_value).strip()
        if cur != exp:
            skipped.append((rid, col, f"current={cur!r}!=expected={exp!r}")); continue
        new = str(r.final_value).strip()
        df.at[i, col] = new
        ci = info.get(col)
        ucol = ci.unit_partner if ci else None
        old_unit = new_unit = ""
        if ucol and ucol in colset:
            old_unit = str(df.at[i, ucol]).strip()
            tgt_unit = str(r.final_unit).strip()
            if new == "" and old_unit:
                new_unit = ""; df.at[i, ucol] = ""
            elif tgt_unit and tgt_unit != old_unit:
                new_unit = tgt_unit; df.at[i, ucol] = tgt_unit
            else:
                old_unit = ""  # no unit change to log
        changes.append({"record_id": rid, "topic": r.topic, "field": col, "bucket": r.bucket,
                        "verdict": r.verdict, "old_value": exp, "new_value": new,
                        "unit_field": (ucol if (old_unit or new_unit) else ""),
                        "old_unit": old_unit, "new_unit": new_unit,
                        "quote": str(r.source_quote)[:200]})

    df.to_csv(OUT, sep=";", index=False)
    pd.DataFrame(changes).to_csv(CHANGELOG, sep=";", index=False)

    ne = (df.values != base.values)
    total_cell_diff = int(ne.sum())
    unit_changes = sum(1 for c in changes if c["unit_field"])
    logged_cells = len(changes) + unit_changes
    print(f"applied {len(changes)} adjudicated fixes -> {OUT.name}")
    print(f"  paired unit cells updated: {unit_changes}")
    print(f"  cells differing from original = {total_cell_diff} (expected = {logged_cells}) MATCH={total_cell_diff==logged_cells}")
    print(f"  rows = {len(df)} (== base: {len(df)==len(base)}) | skipped = {len(skipped)}")
    if skipped:
        print(f"  first skips: {skipped[:5]}")


if __name__ == "__main__":
    main()
