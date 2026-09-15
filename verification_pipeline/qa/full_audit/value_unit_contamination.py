"""Check 4 — value↔unit contamination & swaps.

A *value* cell should hold a clean number; its paired *unit* cell should hold a
unit string. Contamination is when those leak into each other:

  - a bare number sits in the UNIT slot (the value leaked across), or
  - unit text sits in the VALUE slot, or
  - the two are swapped, or
  - a "<number> <unit>" pair got mashed into the unit cell with the value left
    blank.

CALIBRATION (the important part — see full_audit_summary.md):
This dataset is, empirically, almost perfectly clean on this axis:
  - VALUE cells: 0 / ~all populated numeric cells fail to parse as a float — i.e.
    no value cell holds unit text. So `value_unit_swap` / unit-in-value ≈ 0.
  - UNIT cells: ~2,100 contain a digit somewhere, but ONLY ~40 begin with a
    digit and EVERY one of those is a legitimate descriptive unit that merely
    starts with a number — "36, 37, or 38 hours per week", "100 percent for
    first 52 weeks, then 70 percent…", "10/7 times the AOW…", "2nd class public
    transport", "12-hour shifts". NONE is a bare leaked value.
A naive "number appears in the unit cell" rule would emit ~2,109 false
positives. So we detect only the *unambiguous* contamination signatures below;
on the current dataset they correctly fire ~0, which is itself the finding (the
value/unit columns are clean). The detectors remain valuable: they will catch a
genuine slip if one is ever introduced, with near-zero false-positive risk.

Subchecks:
  - value_unit_swap : VALUE cell holds non-numeric (unit-like) text AND its UNIT
                      partner is a pure number — the two slots are swapped. high.
  - unit_is_number  : the UNIT cell is ENTIRELY a single number (no letters, no
                      "/" formula) — a value leaked into the unit slot. high.
  - value_in_unit   : the UNIT cell is exactly "<number> <single-unit-word>"
                      (e.g. "2 weeks") AND every value/min/max partner is blank —
                      a value+unit pair mashed into the unit cell. medium.

Surfacing only. Run:  python3.13 -m qa.full_audit.value_unit_contamination
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

from qa.full_audit import common

OUT = Path(__file__).resolve().parent / "value_unit_contamination.csv"

# A unit cell that is ENTIRELY a single number: digits + one decimal separator,
# optional sign, and NOTHING else. Excludes "10/7" (slash formula), "1-3"
# (range), "2nd …" (trailing letters) — those are legitimate unit expressions.
_PURE_NUMBER_RE = re.compile(r"^[-+]?\d+(?:[.,]\d+)?$")

# "<number> <one simple unit word>" and nothing else — the mashed value+unit
# signature. Tightly anchored so multi-tier descriptive units
# ("36, 37, or 38 hours per week") never match.
_SIMPLE_UNIT = (
    r"weeks?|weken|maand(?:en)?|months?|hours?|uur|uren|days?|dag(?:en)?|"
    r"years?|jaar|jaren|fte|eur|euro|km|%|percent|procent"
)
_VALUE_UNIT_RE = re.compile(
    rf"^[-+]?\d+(?:[.,]\d+)?\s*(?:{_SIMPLE_UNIT})$", re.IGNORECASE)


def run() -> list[dict]:
    df = common.load_dataset()
    recs = df.to_dict("records")
    info = common.classify_columns()

    # unit col -> its value/min/max partner cols
    unit_partners: dict[str, list[str]] = {
        c: list(ci.value_partners)
        for c, ci in info.items() if ci.kind == "unit"
    }
    # value/min/max col -> its unit partner col
    value_unit = common.value_unit_pairs()

    flags: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    def emit(rec, field, subcheck, severity, reason, ctx=None, value=None,
             unit=None):
        key = (common.norm(rec.get("id", "")), field, subcheck)
        if key in seen:
            return
        seen.add(key)
        flags.append(common.make_flag(
            rec, field, "value_unit_contamination", severity, reason,
            json.dumps(ctx or {}, ensure_ascii=False), subcheck=subcheck,
            value=value, unit=unit))

    for rec in recs:
        # ---- value↔unit swap: unit-like text in VALUE, number in UNIT --------
        for vcol, ucol in value_unit:
            v = common.norm(rec.get(vcol, ""))
            u = common.norm(rec.get(ucol, ""))
            if common.is_blank(v) or common.is_blank(u):
                continue
            # value cell is non-numeric (holds text) while the unit cell is a
            # pure number → the two are swapped.
            if (common.to_float(v) is None
                    and _PURE_NUMBER_RE.match(u)
                    and common.unit_category(v) != ""):
                emit(rec, vcol, "value_unit_swap", "high",
                     f"value cell holds unit-like text {v!r} while unit cell "
                     f"holds a bare number {u!r} — value/unit appear swapped",
                     {"value_cell": v, "unit_cell": u, "unit_field": ucol},
                     value=v, unit=u)

        # ---- unit_is_number: the UNIT cell is entirely a number --------------
        for ucol in unit_partners:
            u = common.norm(rec.get(ucol, ""))
            if common.is_blank(u) or not _PURE_NUMBER_RE.match(u):
                continue
            partners = unit_partners[ucol]
            pop = [p for p in partners if not common.is_blank(rec.get(p, ""))]
            emit(rec, ucol, "unit_is_number", "high",
                 f"unit cell is a bare number {u!r} (no unit word) — a value "
                 f"leaked into the unit slot"
                 + ("" if pop else "; value field(s) are blank"),
                 {"unit_cell": u, "populated_value_fields": pop,
                  "value_fields": partners})

        # ---- value_in_unit: "<number> <unit>" mashed into the unit cell ------
        for ucol, partners in unit_partners.items():
            u = common.norm(rec.get(ucol, ""))
            if common.is_blank(u) or not _VALUE_UNIT_RE.match(u):
                continue
            if not partners:
                continue
            if all(common.is_blank(rec.get(p, "")) for p in partners):
                num = common.extract_leading_number(u)
                emit(rec, ucol, "value_in_unit", "medium",
                     f"unit cell {u!r} is a value+unit pair but every value "
                     f"field {partners} is blank — the value was not split out",
                     {"unit_cell": u, "leading_number": num,
                      "value_fields": partners})
    return flags


def main():
    flags = run()
    from qa.shared import resilient_csv
    resilient_csv.write_csv(flags, OUT, fieldnames=common.FLAG_COLUMNS)
    from collections import Counter
    by_sub = Counter(f["subcheck"] for f in flags)
    print(f"[value_unit_contamination] {len(flags)} flags -> {OUT}")
    if not flags:
        print("  (no contamination detected — value/unit columns are clean)")
    for k, v in by_sub.most_common():
        print(f"  {k:20s} {v}")


if __name__ == "__main__":
    main()
