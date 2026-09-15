"""Apply ONLY the FIX_clear changes (record's-own-source, high-confidence) to a COPY
of qa/corrected_dataset.csv. The original is never touched. Every change is verified
(current cell must equal the recorded 'before') and logged. See APPLY_POLICY.md.

Run: python3.13 -m qa.full_audit.apply_perfile_fixes [--limit N]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
BASE = common.PROJECT_ROOT / "qa" / "corrected_dataset.csv"          # read-only source of truth
OUT = common.PROJECT_ROOT / "qa" / "corrected_dataset.perfile_applied.csv"  # the copy we write
CLEAR = HERE / "perfile_work" / "FIX_clear.csv"
CHANGELOG = HERE / "perfile_apply_changelog.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    info = common.classify_columns()
    df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    base = df.copy()
    colset = set(df.columns)
    idx = {rid: i for i, rid in enumerate(df["id"])}

    fx = pd.read_csv(CLEAR, sep=";", dtype=str, keep_default_na=False)
    if a.limit:
        fx = fx.head(a.limit)

    changes, skipped = [], []
    for _, r in fx.iterrows():
        rid, col = r.record_id, r.field
        if rid not in idx or col not in colset:
            skipped.append((rid, col, "missing")); continue
        i = idx[rid]
        cur, exp = str(df.at[i, col]).strip(), str(r.dataset_value).strip()
        if cur != exp:                                   # safety: only change the exact 'before' cell
            skipped.append((rid, col, f"current={cur!r}!=expected={exp!r}")); continue
        new = str(r.fix_target_value).strip()
        df.at[i, col] = new
        ci = info.get(col)
        ucol = ci.unit_partner if ci else None
        old_unit = new_unit = ""
        if ucol and ucol in colset and not common.is_blank(r.fix_target_unit):
            old_unit, new_unit = str(df.at[i, ucol]).strip(), str(r.fix_target_unit).strip()
            df.at[i, ucol] = new_unit
        changes.append({"record_id": rid, "topic": r.topic, "field": col,
                        "old_value": exp, "new_value": new,
                        "unit_field": ucol or "", "old_unit": old_unit, "new_unit": new_unit,
                        "quote": str(r.quote)[:200]})

    df.to_csv(OUT, sep=";", index=False)
    pd.DataFrame(changes).to_csv(CHANGELOG, sep=";", index=False)

    # verify: the copy differs from BASE ONLY in the cells we logged
    ne = (df.values != base.values)
    total_cell_diff = int(ne.sum())
    logged_cells = len(changes) + sum(1 for c in changes if c["new_unit"] != c["old_unit"] and c["unit_field"])
    print(f"applied {len(changes)} FIX_clear value-changes -> {OUT.name}")
    print(f"  unit cells also updated: {sum(1 for c in changes if c['unit_field'] and c['new_unit']!=c['old_unit'])}")
    print(f"  cells differing from original = {total_cell_diff}  (expected = {logged_cells})  MATCH={total_cell_diff==logged_cells}")
    print(f"  rows in copy = {len(df)} (== base {len(base)}: {len(df)==len(base)}) | skipped = {len(skipped)}")
    if skipped:
        print(f"  first skips: {skipped[:5]}")


if __name__ == "__main__":
    main()
