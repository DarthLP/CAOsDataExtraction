"""Check 3 — enum / format / date / cross-field validity.

Per-cell and per-record structural checks that need no statistics — they test a
value against the schema's own rules:

  - enum_invalid     : a value outside the field's controlled vocabulary
                       (ENUM_ALLOWED, faithful to the schema's e.g.-lists which
                       always include 'other'/'unspecified' catch-alls). medium.
  - malformed_number : a non-blank numeric field that does not parse as a number
                       (text / range / junk in a number slot). medium.
  - range_inverted   : range_min > range_max (same group). high.
  - date_malformed   : a non-blank date field that does not parse. medium.
  - date_implausible : a parsed date with year <1990 or >2035. low.
  - date_order       : ingangsdatum > expiratiedatum (start after end), and the
                       general_start_date > general_expiry_date analogue. high.
  - date_mismatch    : ingangsdatum ≠ general_start_date (they match in 98% of
                       records, so a disagreement is a genuine inconsistency).
                       medium.
  - detail_no_present: a *_present flag is explicitly 'False' yet a numeric
                       detail value for that feature is populated — a "says
                       absent but has a number" contradiction. medium.
  - unit_no_value    : a unit cell populated with no value/min/max in its group
                       (orphan unit) — a weak data-quality signal. low.

Surfacing only. Run:  python3.13 -m qa.full_audit.enum_format
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from qa.full_audit import common

OUT = Path(__file__).resolve().parent / "enum_format.csv"

YEAR_MIN, YEAR_MAX = 1990, 2035
DATE_ORDER_PAIRS = [               # (start_col, end_col)
    ("ingangsdatum", "expiratiedatum"),
    ("general_start_date", "general_expiry_date"),
]


def _present_detail_map() -> dict[str, dict]:
    """Map each *_present flag to its numeric and note/rule sibling columns."""
    cols = list(common.dataset_columns())
    info = common.classify_columns()
    out: dict[str, dict] = {}
    for pcol in common.boolean_columns():
        if not pcol.endswith("_present"):
            continue
        prefix = pcol[: -len("present")]          # keeps trailing '_'
        nums, notes = [], []
        for c in cols:
            if c == pcol or not c.startswith(prefix) or c.endswith("_present"):
                continue
            ci = info.get(c)
            if ci and ci.kind == "numeric":
                nums.append(c)
            elif ci and ci.kind in ("freetext",):
                notes.append(c)
        if nums or notes:
            out[pcol] = {"num": nums, "note": notes}
    return out


def run() -> list[dict]:
    df = common.load_dataset()
    recs = df.to_dict("records")
    info = common.classify_columns()
    cols = common.dataset_columns()

    # group_key -> {role: col}
    bygk: dict[str, dict] = defaultdict(dict)
    for c, ci in info.items():
        bygk[ci.group_key][ci.role] = c

    present_map = _present_detail_map()

    flags: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    def emit(rec, field, subcheck, severity, reason, ctx=None, value=None):
        key = (common.norm(rec.get("id", "")), field, subcheck)
        if key in seen:
            return
        seen.add(key)
        flags.append(common.make_flag(
            rec, field, "enum_format", severity, reason,
            json.dumps(ctx or {}, ensure_ascii=False), subcheck=subcheck,
            value=value))

    enum_allowed = {c: {a.lower() for a in (info[c].enum_values or [])}
                    for c in common.enum_columns()}

    for rec in recs:
        # ---- enum vocabulary ------------------------------------------------
        for col, allowed in enum_allowed.items():
            v = common.norm(rec.get(col, ""))
            if not common.is_blank(v) and v.lower() not in allowed:
                emit(rec, col, "enum_invalid", "medium",
                     f"value {v!r} not in allowed enum "
                     f"{sorted(info[col].enum_values or [])}",
                     {"allowed": sorted(info[col].enum_values or [])})

        # ---- malformed numbers ----------------------------------------------
        for col in common.numeric_columns():
            v = common.norm(rec.get(col, ""))
            if not common.is_blank(v) and common.to_float(v) is None:
                emit(rec, col, "malformed_number", "medium",
                     f"non-numeric value {v!r} in a numeric field")

        # ---- range inversion ------------------------------------------------
        for gk, roles in bygk.items():
            if "min" in roles and "max" in roles:
                lo = common.to_float(common.norm(rec.get(roles["min"], "")))
                hi = common.to_float(common.norm(rec.get(roles["max"], "")))
                if lo is not None and hi is not None and lo > hi:
                    emit(rec, roles["min"], "range_inverted", "high",
                         f"range_min {lo:g} > range_max {hi:g} ({gk})",
                         {"min": lo, "max": hi, "max_field": roles["max"]})

        # ---- dates: malformed / implausible ---------------------------------
        for col in common.DATE_COLS:
            if col not in cols:
                continue
            v = common.norm(rec.get(col, ""))
            if common.is_blank(v):
                continue
            d = common.parse_date(v)
            if d is None:
                emit(rec, col, "date_malformed", "medium",
                     f"unparseable date {v!r}")
            elif d.year < YEAR_MIN or d.year > YEAR_MAX:
                emit(rec, col, "date_implausible", "low",
                     f"date {v!r} has implausible year {d.year}")

        # ---- date order + ingangsdatum/general_start_date mismatch ----------
        for scol, ecol in DATE_ORDER_PAIRS:
            sd = common.parse_date(rec.get(scol, ""))
            ed = common.parse_date(rec.get(ecol, ""))
            if sd and ed and sd > ed:
                emit(rec, scol, "date_order", "high",
                     f"{scol} {common.norm(rec.get(scol,''))} is after "
                     f"{ecol} {common.norm(rec.get(ecol,''))}",
                     {"start": str(sd), "end": str(ed), "end_field": ecol})
        ing = common.parse_date(rec.get("ingangsdatum", ""))
        gsd = common.parse_date(rec.get("general_start_date", ""))
        if ing and gsd and ing != gsd:
            emit(rec, "general_start_date", "date_mismatch", "medium",
                 f"general_start_date {common.norm(rec.get('general_start_date',''))} "
                 f"≠ ingangsdatum {common.norm(rec.get('ingangsdatum',''))}",
                 {"ingangsdatum": str(ing), "general_start_date": str(gsd)})

        # ---- present-flag explicitly False but a numeric value populated ----
        for pcol, sibs in present_map.items():
            if not sibs["num"]:
                continue
            if common.norm(rec.get(pcol, "")).lower() != "false":
                continue
            hit = [c for c in sibs["num"] if not common.is_blank(rec.get(c, ""))]
            if hit:
                emit(rec, pcol, "detail_no_present", "medium",
                     f"{pcol}=False but value field(s) "
                     f"{hit} are populated — contradiction",
                     {"populated_details": hit}, value="False")

        # ---- orphan unit (unit populated, no value/min/max) -----------------
        for gk, roles in bygk.items():
            ucol = roles.get("unit")
            if not ucol:
                continue
            valcols = [roles[x] for x in ("value", "min", "max") if x in roles]
            if not valcols:
                continue
            if (not common.is_blank(rec.get(ucol, ""))
                    and all(common.is_blank(rec.get(vc, "")) for vc in valcols)):
                emit(rec, ucol, "unit_no_value", "low",
                     f"unit {common.norm(rec.get(ucol,''))!r} present but no "
                     f"value in group {gk}",
                     {"value_fields": valcols})
    return flags


def main():
    flags = run()
    from qa.shared import resilient_csv
    resilient_csv.write_csv(flags, OUT, fieldnames=common.FLAG_COLUMNS)
    from collections import Counter
    by_sub = Counter(f["subcheck"] for f in flags)
    print(f"[enum_format] {len(flags)} flags -> {OUT}")
    for k, v in by_sub.most_common():
        print(f"  {k:20s} {v}")


if __name__ == "__main__":
    main()
