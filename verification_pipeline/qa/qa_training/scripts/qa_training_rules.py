"""qa_training_rules.py — Layer 1 deterministic QA for training_* fields.

Schema-internal checks only. Reads `qa/qa_training/inputs/scoped_records.csv`,
writes `qa/qa_training/outputs/training_rule_violations.csv`.

The training topic has 9 base fields (no enums):
  has_training_rights (bool)
  time_yearly (Amount; schema example "hours per year"; real CAOs use days OR hours)
  budget (Amount; schema example "EUR per year")
  career_scan_freq (Amount; schema example "times per year")
  cost_reimbursement (Amount; schema example "% of costs")
  fund_present (bool)
  reclaim_clause_present (bool)
  mandatory_training_paid (bool)
  note (str)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "training_rule_violations.csv"


def _truthy(v) -> bool:
    if v is None or pd.isna(v): return False
    return str(v).strip().lower() in {"true", "1", "yes", "y", "ja"}


def _falsy(v) -> bool:
    if v is None or pd.isna(v): return False
    return str(v).strip().lower() in {"false", "0", "no", "n", "nee"}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)): return True
    s = str(v).strip().lower()
    return s in {"", "nan", "none", "null", "n/a", "unknown"}


def _to_float(v) -> Optional[float]:
    if _blank(v): return None
    try: return float(str(v).replace(",", "."))
    except (ValueError, TypeError): return None


def _new(record_id, rule_id, field, severity, worksheet_mode,
         csv_value_old="", csv_unit_old="",
         csv_value_new="", csv_unit_new="",
         verdict="", target_field="", notes=""):
    return {
        "record_id": str(record_id),
        "rule_id": rule_id,
        "field": field,
        "severity": severity,
        "worksheet_mode": worksheet_mode,
        "csv_value_old": str(csv_value_old) if not _blank(csv_value_old) else "",
        "csv_unit_old":  str(csv_unit_old)  if not _blank(csv_unit_old)  else "",
        "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new":  str(csv_unit_new)  if not _blank(csv_unit_new)  else "",
        "verdict": verdict,
        "target_field": target_field,
        "notes": notes,
        "topic_group": "training",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


# === Rules ===

def rule_R1_time_yearly_value_no_unit(row, rid):
    """time_yearly_value present but unit empty."""
    v = row.get("training_time_yearly_value", "")
    u = row.get("training_time_yearly_unit", "")
    if not _blank(v) and _blank(u):
        return _new(rid, "R1", "training_time_yearly_unit",
                    "medium", "extract",
                    csv_value_old=u,
                    notes="time_yearly value present, unit empty; extract from source")
    return None


def rule_R2_time_yearly_unit_inconsistent(row, rid):
    """time_yearly_unit like 'hours per week' or 'days per shift' suggests
    extraction error — schema is per-year. Routes to subagent informed mode."""
    u = str(row.get("training_time_yearly_unit", "")).strip().lower()
    if _blank(u):
        return None
    # Canonical: "hours per year", "days per year". Anything else flags.
    canonical = ("hours per year", "days per year", "days/year", "hours/year")
    if any(c in u for c in canonical):
        return None
    # Known suspicious forms — non-yearly units
    if "week" in u or "shift" in u or "month" in u or "day" not in u and "hour" not in u:
        return _new(rid, "R2", "training_time_yearly_unit",
                    "medium", "informed",
                    csv_value_old=u,
                    notes=f"time_yearly unit {u!r} not yearly (schema is per year); verify or correct")
    return None


def rule_R3_budget_value_no_unit(row, rid):
    """budget_value present but unit empty."""
    v = row.get("training_budget_value", "")
    u = row.get("training_budget_unit", "")
    if not _blank(v) and _blank(u):
        return _new(rid, "R3", "training_budget_unit",
                    "medium", "extract",
                    csv_value_old=u,
                    notes="budget value present, unit empty; extract from source")
    return None


def rule_R4_budget_unit_noncanonical(row, rid):
    """budget_unit like 'EVC trajectories' or 'percent of actual wage' is
    a non-canonical / non-EUR unit. Surface for verification."""
    u = str(row.get("training_budget_unit", "")).strip().lower()
    if _blank(u):
        return None
    canonical_substrings = ("eur", "€", "euro", "percent of", "% of",
                            "hours", "days")
    if any(s in u for s in canonical_substrings):
        return None
    return _new(rid, "R4", "training_budget_unit",
                "medium", "informed",
                csv_value_old=u,
                notes=f"budget unit {u!r} non-canonical; verify against source")


def rule_R5_career_scan_freq_unit_inversion(row, rid):
    """career_scan_freq has unit 'years' (period) instead of 'times per year'
    (frequency). Schema example: value=2, unit='times per year'.
    'Every 5 years' source → extractor wrote value=5 unit='years' but
    schema-canonical is value=0.2 unit='times per year'."""
    u = str(row.get("training_career_scan_freq_unit", "")).strip().lower()
    if _blank(u):
        return None
    # Frequency-canonical forms
    if "times per year" in u or "per year" in u or "annually" in u:
        return None
    # Period forms suggesting inversion need
    if u in {"year", "years", "year(s)"} or "every" in u or "year" in u:
        fv = _to_float(row.get("training_career_scan_freq_value", ""))
        new_val = ""
        if fv and fv > 0:
            new_val = str(round(1.0 / fv, 3))
        return _new(rid, "R5", "training_career_scan_freq_unit",
                    "medium", "informed",
                    csv_value_old=u,
                    csv_unit_old=u,
                    csv_value_new=new_val,
                    csv_unit_new="times per year",
                    notes=f"career_scan_freq unit {u!r}: schema expects 'times per year'; "
                          f"if source says 'every {row.get('training_career_scan_freq_value', '')} years', "
                          f"convert to {new_val} times per year")
    return None


def rule_R6_cost_reimbursement_unit_noncanonical(row, rid):
    """cost_reimbursement unit variants. Schema: '% of costs'. Common
    extractor variants: 'percent', 'percent of study costs', 'percent of
    travel and study costs'. Surface for normalization."""
    u = str(row.get("training_cost_reimbursement_unit", "")).strip().lower()
    if _blank(u):
        return None
    # Canonical accepts: "% of costs", "% of study costs", or any "% of …"
    if u in {"% of costs", "% of study costs", "percent of costs", "percent of study costs"}:
        return None
    # Bare "percent" or "%" is non-canonical (missing "of costs")
    if u in {"percent", "%"}:
        return _new(rid, "R6", "training_cost_reimbursement_unit",
                    "low", "informed",
                    csv_value_old=u, csv_unit_old=u,
                    csv_value_new="", csv_unit_new="% of costs",
                    verdict="correct_in_place",
                    notes="bare 'percent' → '% of costs' (schema canonical)")
    return None


def rule_R7_cost_reimb_value_above_100(row, rid):
    """cost_reimbursement value > 100 with % unit is impossible — extraction
    error (probably scale or decimal-strip)."""
    fv = _to_float(row.get("training_cost_reimbursement_value", ""))
    u = str(row.get("training_cost_reimbursement_unit", "")).strip().lower()
    if fv is None or "percent" not in u and "%" not in u:
        return None
    if fv > 100:
        return _new(rid, "R7", "training_cost_reimbursement_value",
                    "high", "blind",
                    csv_value_old=fv, csv_unit_old=u,
                    notes=f"cost_reimbursement {fv}% impossible; suspect scale/decimal-strip")
    return None


def rule_R8_year_in_duration_field(row, rid):
    """Year-like value (1990-2030) in time_yearly_value or budget_value —
    GENERAL_FM_03 article-number / year extraction error."""
    out = []
    for f in ("training_time_yearly_value", "training_budget_value"):
        fv = _to_float(row.get(f, ""))
        if fv is not None and 1990 <= fv <= 2030:
            out.append(_new(rid, "R8", f,
                            "high", "none",
                            csv_value_old=fv,
                            csv_value_new="", csv_unit_new="",
                            verdict="clear",
                            notes=f"year-like value {fv} in {f} (GENERAL_FM_03)"))
    return out[0] if out else None  # only return first for simplicity


def rule_R9_has_rights_false_with_data(row, rid):
    """has_training_rights=False but other training_* fields populated."""
    h = row.get("training_has_training_rights", "")
    if not _falsy(h): return None
    populated = []
    for f in ["training_time_yearly_value", "training_budget_value",
              "training_cost_reimbursement_value", "training_career_scan_freq_value",
              "training_note"]:
        if not _blank(row.get(f, "")):
            populated.append(f)
    if populated:
        return _new(rid, "R9", "training_has_training_rights",
                    "medium", "blind",
                    csv_value_old=h, csv_value_new="True",
                    verdict="set_boolean",
                    notes=f"has_training_rights=False but populated: {populated[:3]}")
    return None


def rule_R10_budget_value_implausibly_small(row, rid):
    """budget_value < 30 EUR/year is implausibly small for an annual budget
    (likely scale error from %  → EUR mix-up). Excludes when unit is %."""
    fv = _to_float(row.get("training_budget_value", ""))
    u = str(row.get("training_budget_unit", "")).strip().lower()
    if fv is None:
        return None
    if "%" in u or "percent" in u or "trajector" in u:
        return None  # not a EUR amount
    if "eur" in u or "€" in u:
        if fv < 30:
            return _new(rid, "R10", "training_budget_value",
                        "medium", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"budget {fv} EUR/year implausibly small; verify scale")
    return None


RULES = [
    rule_R1_time_yearly_value_no_unit,
    rule_R2_time_yearly_unit_inconsistent,
    rule_R3_budget_value_no_unit,
    rule_R4_budget_unit_noncanonical,
    rule_R5_career_scan_freq_unit_inversion,
    rule_R6_cost_reimbursement_unit_noncanonical,
    rule_R7_cost_reimb_value_above_100,
    rule_R8_year_in_duration_field,
    rule_R9_has_rights_false_with_data,
    rule_R10_budget_value_implausibly_small,
]


def main():
    df = pd.read_csv(CSV_PATH, sep=";", dtype=str, keep_default_na=False)
    print(f"Loaded {len(df)} scoped records")
    violations = []
    for _, row in df.iterrows():
        rid = str(row.get("id", "")).strip()
        for rule in RULES:
            v = rule(row, rid)
            if v is not None:
                v["cao_number"] = str(row.get("cao_number", ""))
                v["file_name"] = str(row.get("file_name", ""))
                v["ingangsdatum"] = str(row.get("ingangsdatum", ""))
                violations.append(v)
    out_df = pd.DataFrame(violations)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_PATH, sep=";", index=False)
    print(f"Wrote {len(violations)} violations")
    if violations:
        print("\nBy rule:")
        for rid, n in out_df.groupby("rule_id").size().sort_values(ascending=False).items():
            print(f"  {rid}: {n}")
    return out_df


if __name__ == "__main__":
    main()
