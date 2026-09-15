"""Phase B — clean up the messy corrections deferred by Phase A, and apply the
ones that can be safely routed. Operates on the Phase-A output
(proposed_corrected_dataset.csv), appends to apply_changelog.csv, and rewrites
needs_manual_placement.csv with only the genuinely un-cleanable remainder.

messy_numeric : take the PRIMARY value+unit, dropping a trailing "(...)" caveat —
                but ONLY if unambiguous (no ';' tier, no " or " alternative).
unit_field    : apply only when the unit's dimension+timebase is UNCHANGED (pure
                formatting, e.g. 'hours_per_week'->'hours per week'). A dimension
                or time-base change (%/EUR, per week/per year) means the VALUE is
                also wrong, so it stays manual (flagged 'value_also_needs_fix').

Run: python3.13 -m qa.full_audit.apply_corrections_phaseB
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
PROPOSED = HERE / "proposed_corrected_dataset.csv"
CHANGELOG = HERE / "apply_changelog.csv"
DEFER = HERE / "needs_manual_placement.csv"

_NUM_UNIT = re.compile(r"^(-?\d+(?:\.\d+)?)\s+(.+)$")
_NUM_ONLY = re.compile(r"^-?\d+(?:\.\d+)?$")


def clean_messy_numeric(s: str):
    """-> (value, unit_or_None) primary parse, or None if too ambiguous."""
    s = str(s).strip()
    if ";" in s:                      # tiered (e.g. '100 wk1; 70 wk2-3')
        return None
    core, paren = s, ""
    m = re.match(r"^(.*?)\s*\((.*)\)\s*$", s)
    if m:
        core, paren = m.group(1).strip(), m.group(2)
    pl = paren.lower()
    if "unit:" in pl:                 # '6.0 (unit: times weekly working hours)'
        if _NUM_ONLY.match(core):
            return core, re.sub(r"(?i)^unit:\s*", "", paren).strip()
        return None
    if " or " in pl or re.search(r"\d", paren):   # parenthetical offers an alternative
        return None
    m = _NUM_UNIT.match(core)
    if m:
        return m.group(1), m.group(2).strip()
    if _NUM_ONLY.match(core):
        return core, None
    return None


def main():
    base = pd.read_csv(PROPOSED, sep=";", dtype=str, keep_default_na=False)
    order, cols = list(base["id"]), list(base.columns)
    byid = {r["id"]: dict(r) for _, r in base.iterrows()}
    info = common.classify_columns()
    deferred = pd.read_csv(DEFER, sep=";", dtype=str, keep_default_na=False)

    new_changes, still_manual = [], []
    applied = conflict = 0

    def try_apply(rid, tf, exp, nv, route):
        nonlocal applied, conflict
        rec = byid.get(rid)
        if rec is None:
            return False
        ba = str(rec.get(tf, "")).strip()
        nv_s, exp_s = str(nv).strip(), str(exp).strip()
        if ba == nv_s:
            new_changes.append((rid, tf, route, exp_s, ba, nv_s, "already_correct")); return True
        if ba == exp_s:
            rec[tf] = nv_s
            new_changes.append((rid, tf, route, exp_s, ba, nv_s, "applied")); applied += 1; return True
        new_changes.append((rid, tf, route, exp_s, ba, nv_s, "conflict_skip")); conflict += 1
        return True

    for _, r in deferred.iterrows():
        rid, field, reason = r["record_id"], r["field"], r["defer_reason"]
        cur, cur_u, sug = r["current_value"], r["current_unit"], r["suggested_value"]
        ci = info.get(field)
        if reason == "messy_numeric":
            parsed = clean_messy_numeric(sug)
            if parsed is None:
                still_manual.append({**r, "phaseB": "kept_manual:ambiguous_multivalue"}); continue
            val, unit = parsed
            try_apply(rid, field, cur, val, "messyB_value")
            if unit is not None and ci and ci.unit_partner:
                try_apply(rid, ci.unit_partner, cur_u, unit, "messyB_unit")
        elif reason == "unit_field_defer":
            # apply only pure-formatting unit changes (same dimension+timebase)
            if common.unit_signature(cur) == common.unit_signature(sug) and not common.is_blank(cur):
                try_apply(rid, field, cur, sug, "unit_formatting")
            else:
                still_manual.append({**r, "phaseB": "kept_manual:value_also_needs_fix(dimension/timebase change)"})
        else:
            still_manual.append({**r, "phaseB": "kept_manual"})

    # write updated proposed dataset
    pd.DataFrame([byid[i] for i in order], columns=cols).to_csv(PROPOSED, sep=";", index=False)
    # append to changelog
    add = pd.DataFrame(new_changes, columns=["record_id", "target_field", "route",
                                             "flagged_current", "base_actual", "new_value", "status"])
    old = pd.read_csv(CHANGELOG, sep=";", dtype=str, keep_default_na=False)
    pd.concat([old, add], ignore_index=True).to_csv(CHANGELOG, sep=";", index=False)
    pd.DataFrame(still_manual).to_csv(DEFER, sep=";", index=False)

    print(f"[apply Phase B] cleaned the {len(deferred)} deferred")
    print(f"  cells APPLIED (Phase B): {applied}")
    print(f"  conflict_skip          : {conflict}")
    print(f"  still manual           : {len(still_manual)} -> {DEFER.name}")
    from collections import Counter
    print("  still-manual reasons   :", dict(Counter(m["phaseB"] for m in still_manual)))


if __name__ == "__main__":
    main()
