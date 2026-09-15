"""Reason about each of the 76 deferred manual corrections and emit a concrete
suggestion per case -> qa/full_audit/manual_suggestions.csv.

Categories of suggestion (action):
  APPLY_RELABEL   - unit -> canonical Dutch '(times) weekly working hours'; VALUE
                    PRESERVED (26 weeks == 26x weekly hours). Safe.
  APPLY_CLEANUP   - strip pay-rate / caveat baked into the unit string (e.g.
                    'weeks at 100% pay' -> 'weeks'); VALUE PRESERVED. Safe.
  SET_STATUTORY   - value+unit fixed to a statutory constant (sick pay 104 weeks).
  SUGGEST_FIRSTTIER - tiered pay -> first-period/headline rate (dataset convention
                    = 100 dominant); standardize across a CAO's versions.
  KEEP_CURRENT    - current value is already right (field-name disambiguates).
  NEEDS_SOURCE    - value AND unit interdependent / ambiguous; needs the original
                    document. A best-guess is still given in `suggestion`.
Each row: rationale + confidence. Surface-only; applies nothing.

Run: python3.13 -m qa.full_audit.build_manual_suggestions
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
OUT = HERE / "manual_suggestions.csv"


def _firstnum(s):
    m = re.search(r"-?\d+(?:\.\d+)?", str(s))
    return m.group(0) if m else ""


def suggest(field, dr, cur, cur_u, sug):
    """-> (action, sugg_value, sugg_unit, confidence, rationale)."""
    s = str(sug)
    sl = s.lower()
    # ---- unit-field changes -------------------------------------------------
    if dr == "unit_field_defer":
        if "weekly working hours" in sl:
            return ("APPLY_RELABEL", "KEEP", s, "high",
                    "Canonical Dutch statutory unit (N x weekly working hours); numerically "
                    "equal to the stored 'weeks' value, so VALUE is preserved — only the unit label refines.")
        # strip pay/caveat baked into a duration unit -> base unit
        base = re.sub(r"\s*\(.*?\)\s*", "", str(cur)).strip()
        base = re.split(r"\s+at\s+", base, flags=re.I)[0].strip()
        if not common.is_blank(s) and s.strip().lower() == base.lower() and base.lower() != str(cur).strip().lower():
            return ("APPLY_CLEANUP", "KEEP", s, "high",
                    f"Unit string had pay/caveat baked in ({cur!r}); cleaned to base unit. VALUE preserved.")
        if "sickpay_duration" in field and ("104" in s or "weeks" in sl):
            return ("SET_STATUTORY", "104.0", "weeks", "medium",
                    "Statutory continued-pay duration is 104 weeks (loondoorbetaling). Set value=104, unit=weeks.")
        if "ketenregeling" in field:
            return ("NEEDS_SOURCE", "", s, "low",
                    f"Both value ({cur!r}) and unit look off vs the statutory chain limit (36 months / 3 years). Verify against source.")
        if "percent" in sl and str(cur_u or cur).strip().lower() in ("weeks", "week", "days", "day"):
            return ("NEEDS_SOURCE", "", s, "low",
                    "Unit -> percent means the stored number is a PAY RATE, not a duration. Relocate to the *_pay field "
                    "or treat as unit-kind contamination (see unit_semantics_reconciliation.csv); do not just swap the unit.")
        if "eur" in sl and "percent" in str(cur).lower() or ("percent" in sl and "eur" in str(cur).lower()):
            return ("NEEDS_SOURCE", "", s, "low",
                    "EUR<->percent dimension change: the value is also wrong (and may be dual: % surcharge + EUR allowance). Verify.")
        if ("per year" in str(cur).lower() and "per month" in sl) or ("per week" in str(cur).lower() and "per year" in sl) or ("weeks" == str(cur).strip().lower() and "months" in sl):
            return ("NEEDS_SOURCE", "", s, "low",
                    f"Time-base change ({cur!r} -> {s!r}) rescales the value; needs source to set the value too.")
        return ("NEEDS_SOURCE", "", s, "low",
                f"Unit change {cur!r} -> {s!r} likely implies a value change; verify against source.")
    # ---- messy tiered numerics ---------------------------------------------
    if dr == "messy_numeric":
        if field == "leave_parental_unpaid_value":
            return ("KEEP_CURRENT", str(cur), cur_u, "high",
                    f"Field is the UNPAID portion; current {cur!r} = total minus paid UWV weeks. The suggestion's larger "
                    "number is the TOTAL. Keep current.")
        if any(k in field for k in ("short_term_care_pay", "sickpay_continuation", "long_term_care_pay")):
            ft = _firstnum(s)
            return ("SUGGEST_FIRSTTIER", ft, cur_u, "medium",
                    f"Tiered scheme ({s!r}). Dataset convention stores the FIRST-period/headline rate (100 dominant). "
                    f"Suggest {ft} and standardize across this CAO's versions (they are currently inconsistent).")
        if field == "leave_paid_paternity_value":
            return ("NEEDS_SOURCE", "", s, "low",
                    f"Source ambiguous ({s!r}); current {cur!r} looks wrong. Likely 1 week (= 1x weekly hours). Confirm.")
        if field == "training_time_yearly_value":
            return ("NEEDS_SOURCE", _firstnum(s), "", "low",
                    f"Paid vs combined paid+unpaid ({s!r}); pick per field intent (paid-only -> first number).")
    return ("NEEDS_SOURCE", "", s, "low", "Review against source.")


def main():
    dm = pd.read_csv(HERE / "needs_manual_placement.csv", sep=";", dtype=str, keep_default_na=False)
    rows = []
    for _, r in dm.iterrows():
        action, sv, su, conf, rat = suggest(r["field"], r["defer_reason"],
                                             r["current_value"], r.get("current_unit", ""),
                                             r["suggested_value"])
        rows.append({"record_id": r["record_id"], "field": r["field"],
                     "defer_reason": r["defer_reason"], "current_value": r["current_value"],
                     "current_unit": r.get("current_unit", ""), "audit_suggestion": r["suggested_value"],
                     "action": action, "suggested_value": sv, "suggested_unit": su,
                     "confidence": conf, "rationale": rat})
    df = pd.DataFrame(rows).sort_values(["action", "field", "record_id"])
    df.to_csv(OUT, sep=";", index=False)
    print(f"[manual_suggestions] {len(df)} rows -> {OUT}")
    print(df["action"].value_counts().to_string())


if __name__ == "__main__":
    main()
