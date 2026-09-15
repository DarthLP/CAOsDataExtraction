"""Apply full-audit corrections onto a COPY of corrected_dataset.csv, routing
each new value to the correct field. SURFACE-SAFE: writes only NEW files under
qa/full_audit/ (proposed_corrected_dataset.csv + changelog), never the base.

Phase A (clean): only unambiguous routings —
  - numeric clean number            -> value field
  - numeric "<n> <unit>" / "(unit:)" -> SPLIT to value field + paired unit field
  - enum (suggestion is a valid token)
  - boolean (True/False)
  - date / freetext
Unit-field changes, messy multi-part numerics, invalid-enum suggestions, and
relocations are DEFERRED to needs_manual_placement.csv (Phase B handles those).

Apply rule per target cell (base = corrected_dataset.csv):
  base == new            -> already_correct (skip)
  base == flagged_current -> APPLY (replace)
  base == anything else   -> conflict_skip (don't clobber a per-topic edit)

Run: python3.13 -m qa.full_audit.apply_corrections
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / "corrected_dataset.csv"
APPLY_SRC = HERE / "handoff" / "01_corrections_to_apply.csv"
OUT = HERE / "proposed_corrected_dataset.csv"
CHANGELOG = HERE / "apply_changelog.csv"
DEFER = HERE / "needs_manual_placement.csv"

_NUM_ONLY = re.compile(r"^-?\d+(?:\.\d+)?$")
_UNIT_PAREN = re.compile(r"^(-?\d+(?:\.\d+)?)\s*\(unit:\s*(.+?)\)\s*$", re.I)
_NUM_UNIT = re.compile(r"^(-?\d+(?:\.\d+)?)\s+([A-Za-z%][\w %/.\-]*)$")


def _blank(s) -> bool:
    return str(s).strip().lower() in ("", "nan", "none", "null")


def route(field, cur, cur_unit, sug, info, enum):
    """-> (routes, clean, reason). routes = [(target_field, expected_current, new_value)]."""
    ci = info.get(field)
    kind = ci.kind if ci else "?"
    sug = str(sug).strip()
    if kind == "numeric":
        if _NUM_ONLY.match(sug):
            return [(field, cur, sug)], True, "clean_number"
        m = _UNIT_PAREN.match(sug) or (_NUM_UNIT.match(sug) if ";" not in sug else None)
        if m and ci and ci.unit_partner:
            return ([(field, cur, m.group(1)),
                     (ci.unit_partner, cur_unit, m.group(2).strip())], True, "split_value_unit")
        return [], False, "messy_numeric"
    if kind == "enum":
        if sug in (enum.get(field) or set()):
            return [(field, cur, sug)], True, "enum_valid"
        return [], False, f"enum_invalid_suggestion:{sug}"
    if kind == "boolean":
        if sug.lower() in ("true", "false"):
            return [(field, cur, sug.capitalize())], True, "boolean"
        return [], False, "boolean_unclear"
    if kind in ("date", "freetext"):
        return [(field, cur, sug)], True, kind
    if kind == "unit":
        return [], False, "unit_field_defer"
    return [], False, f"kind_{kind}"


def main():
    info = common.classify_columns()
    enum = common.ENUM_ALLOWED
    base = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    order = list(base["id"])
    cols = list(base.columns)
    byid = {r["id"]: dict(r) for _, r in base.iterrows()}
    src = pd.read_csv(APPLY_SRC, sep=";", dtype=str, keep_default_na=False)

    changelog, deferred = [], []
    applied = already = conflict = 0
    for _, r in src.iterrows():
        rid = r["record_id"]
        routes, clean, reason = route(r["field"], r["current_value"],
                                      r["current_unit"], r["suggested_value"], info, enum)
        if not clean:
            deferred.append({"record_id": rid, "field": r["field"],
                             "current_value": r["current_value"], "current_unit": r["current_unit"],
                             "suggested_value": r["suggested_value"], "defer_reason": reason,
                             "source_quote": r.get("source_quote", "")})
            continue
        rec = byid.get(rid)
        if rec is None:
            deferred.append({"record_id": rid, "field": r["field"], "current_value": r["current_value"],
                             "current_unit": r["current_unit"], "suggested_value": r["suggested_value"],
                             "defer_reason": "no_record_in_base", "source_quote": ""})
            continue
        for tf, exp, nv in routes:
            base_actual = str(rec.get(tf, "")).strip()
            nv_s, exp_s = str(nv).strip(), str(exp).strip()
            if base_actual == nv_s:
                status = "already_correct"; already += 1
            elif base_actual == exp_s:
                rec[tf] = nv_s; status = "applied"; applied += 1
            else:
                status = "conflict_skip"; conflict += 1
            changelog.append({"record_id": rid, "target_field": tf, "route": reason,
                              "flagged_current": exp_s, "base_actual": base_actual,
                              "new_value": nv_s, "status": status,
                              "source_quote": r.get("source_quote", "")})

    # write proposed dataset in original order
    out_rows = [byid[i] for i in order]
    pd.DataFrame(out_rows, columns=cols).to_csv(OUT, sep=";", index=False)
    pd.DataFrame(changelog).to_csv(CHANGELOG, sep=";", index=False)
    pd.DataFrame(deferred).to_csv(DEFER, sep=";", index=False)

    print(f"[apply Phase A] base={BASE.name} -> {OUT.name} (base NOT modified)")
    print(f"  cells APPLIED         : {applied}")
    print(f"  already_correct       : {already}")
    print(f"  conflict_skip         : {conflict}  (base differs from flagged value — left as-is)")
    print(f"  deferred to manual    : {len(deferred)}  -> {DEFER.name}")
    from collections import Counter
    print("  deferral reasons:", dict(Counter(d["defer_reason"] for d in deferred)))
    print(f"  changelog ({len(changelog)} cell-actions) -> {CHANGELOG.name}")


if __name__ == "__main__":
    main()
