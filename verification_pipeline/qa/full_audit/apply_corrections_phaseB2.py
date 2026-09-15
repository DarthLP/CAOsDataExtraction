"""Phase B2 — apply the deferred UNIT changes whose paired VALUE field was
already corrected (Phase A/B), so the value+unit pair is left consistent.
A unit dimension/timebase change is normally risky, but when the partner value
was corrected by the SAME verification, applying the unit completes the pair
(e.g. value 3.5 + unit '% of annual income'; value 1659 + unit 'hours per year').
Unit changes with NO paired value correction stay manual (value unverified).

Run: python3.13 -m qa.full_audit.apply_corrections_phaseB2
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
PROPOSED = HERE / "proposed_corrected_dataset.csv"
CHANGELOG = HERE / "apply_changelog.csv"
DEFER = HERE / "needs_manual_placement.csv"


def main():
    info = common.classify_columns()
    base = pd.read_csv(PROPOSED, sep=";", dtype=str, keep_default_na=False)
    order, cols = list(base["id"]), list(base.columns)
    byid = {r["id"]: dict(r) for _, r in base.iterrows()}

    cl = pd.read_csv(CHANGELOG, sep=";", dtype=str, keep_default_na=False)
    applied_val = {(r["record_id"], r["target_field"]) for _, r in cl.iterrows()
                   if r["status"] == "applied"}
    # unit_field -> its value partner(s)
    partners = {}
    for c, ci in info.items():
        if ci.unit_partner:
            partners.setdefault(ci.unit_partner, []).append(c)

    dm = pd.read_csv(DEFER, sep=";", dtype=str, keep_default_na=False)
    new_changes, remain = [], []
    applied = conflict = 0
    for _, r in dm.iterrows():
        uf = r["field"]
        is_unit = r["defer_reason"] == "unit_field_defer"
        paired_fixed = is_unit and any((r["record_id"], v) in applied_val
                                       for v in partners.get(uf, []))
        if not paired_fixed:
            remain.append(dict(r)); continue
        rec = byid.get(r["record_id"])
        ba = str(rec.get(uf, "")).strip() if rec else None
        exp, nv = str(r["current_value"]).strip(), str(r["suggested_value"]).strip()
        if rec is None:
            remain.append(dict(r)); continue
        if ba == nv:
            new_changes.append((r["record_id"], uf, "unit_pair_complete", exp, ba, nv, "already_correct"))
        elif ba == exp:
            rec[uf] = nv
            new_changes.append((r["record_id"], uf, "unit_pair_complete", exp, ba, nv, "applied")); applied += 1
        else:
            new_changes.append((r["record_id"], uf, "unit_pair_complete", exp, ba, nv, "conflict_skip"))
            conflict += 1
            remain.append(dict(r))

    pd.DataFrame([byid[i] for i in order], columns=cols).to_csv(PROPOSED, sep=";", index=False)
    add = pd.DataFrame(new_changes, columns=["record_id", "target_field", "route",
                                             "flagged_current", "base_actual", "new_value", "status"])
    pd.concat([pd.read_csv(CHANGELOG, sep=";", dtype=str, keep_default_na=False), add],
              ignore_index=True).to_csv(CHANGELOG, sep=";", index=False)
    pd.DataFrame(remain).to_csv(DEFER, sep=";", index=False)

    print(f"[apply Phase B2] paired-unit completion")
    print(f"  unit cells APPLIED : {applied}")
    print(f"  conflict_skip      : {conflict}")
    print(f"  still manual       : {len(remain)} -> {DEFER.name}")


if __name__ == "__main__":
    main()
