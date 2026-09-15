"""Apply a focused re-verification's corrections to the proposed dataset:
REVERT our wrong changes, RE-FIX where source says a different value, flag UNSURE.
Robust to enum/boolean fields (where the 'stem' IS the field) and numeric fields
(stem + _value/_unit). Updates proposed_corrected_dataset.csv + appends
apply_changelog.csv; UNSURE/no-replacement -> <results>_review.csv.

Run: python3.13 -m qa.full_audit.apply_reverify_fixes [results_csv]
     (default results = reverify_applied_results.csv)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
PROPOSED = HERE / "proposed_corrected_dataset.csv"
CHANGELOG = HERE / "apply_changelog.csv"


def _blank(s):
    return str(s).strip().lower() in ("", "nan", "none", "null")


def _cols_for(stem, info):
    """Return (value_col, unit_col) for a stem, handling enum/boolean (stem is
    the field) and numeric (stem+_value/_unit)."""
    if stem + "_value" in info:
        return stem + "_value", (stem + "_unit" if stem + "_unit" in info else None)
    if stem in info:                      # enum/boolean/date/freetext field
        return stem, None
    # range fields: stem+_range_min/_max share stem+_range_unit — fall back to first existing
    for c in (stem + "_range_min", stem + "_range_max"):
        if c in info:
            return c, (stem + "_range_unit" if stem + "_range_unit" in info else None)
    return None, None


def main():
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "reverify_applied_results.csv"
    review_path = HERE / (results.stem.replace("_results", "") + "_review.csv")

    info = common.classify_columns()
    base = {r["id"]: r for r in pd.read_csv(HERE.parent / "corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    pf = pd.read_csv(PROPOSED, sep=";", dtype=str, keep_default_na=False)
    order, cols = list(pf["id"]), list(pf.columns)
    byid = {r["id"]: dict(r) for _, r in pf.iterrows()}
    res = pd.read_csv(results, sep=";", dtype=str, keep_default_na=False)

    changes, review = [], []
    reverted = refixed = 0

    def setcell(rid, col, newval, why):
        rec = byid.get(rid)
        if rec is None or col is None or col not in rec:
            return
        old = str(rec.get(col, "")).strip()
        if old == str(newval).strip():
            return
        rec[col] = str(newval).strip()
        changes.append((rid, col, "reverify_" + why, old, str(newval).strip(), "applied"))

    for _, r in res.iterrows():
        rid, stem, v = r["record_id"], r["stem"], r["verdict"]
        vcol, ucol = _cols_for(stem, info)
        if v == "REVERT_TO_ORIGINAL":
            setcell(rid, vcol, base.get(rid, {}).get(vcol, ""), "revert")
            if ucol:
                setcell(rid, ucol, base.get(rid, {}).get(ucol, ""), "revert")
            reverted += 1
        elif v == "NEEDS_DIFFERENT":
            cv, cu = r.get("correct_value", ""), r.get("correct_unit", "")
            if not _blank(cv) and vcol:
                setcell(rid, vcol, cv, "refix")
                if not _blank(cu) and ucol:
                    setcell(rid, ucol, cu, "refix")
                refixed += 1
            else:
                setcell(rid, vcol, base.get(rid, {}).get(vcol, ""), "revert_nofix")
                if ucol:
                    setcell(rid, ucol, base.get(rid, {}).get(ucol, ""), "revert_nofix")
                review.append({**r.to_dict(), "disposition": "reverted_no_replacement"})
        elif v == "UNSURE":
            review.append({**r.to_dict(), "disposition": "left_as_applied_needs_human"})

    pd.DataFrame([byid[i] for i in order], columns=cols).to_csv(PROPOSED, sep=";", index=False)
    add = pd.DataFrame(changes, columns=["record_id", "target_field", "route",
                                         "flagged_current", "new_value", "status"])
    cl = pd.read_csv(CHANGELOG, sep=";", dtype=str, keep_default_na=False)
    for c in cl.columns:
        if c not in add.columns:
            add[c] = ""
    pd.concat([cl, add[cl.columns]], ignore_index=True).to_csv(CHANGELOG, sep=";", index=False)
    pd.DataFrame(review).to_csv(review_path, sep=";", index=False)

    print(f"[apply reverify fixes] {results.name} -> {PROPOSED.name}")
    print(f"  REVERTED : {reverted}   RE-FIXED : {refixed}   cells changed: {len(changes)}")
    print(f"  to review (UNSURE / no-replacement): {len(review)} -> {review_path.name}")


if __name__ == "__main__":
    main()
