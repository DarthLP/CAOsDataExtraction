"""qa_homeoffice_rules.py — Layer 1 deterministic QA for homeoffice_* fields.

Schema-internal checks only. Reads `qa/qa_homeoffice/inputs/scoped_records.csv`,
writes `qa/qa_homeoffice/outputs/homeoffice_rule_violations.csv`.

The homeoffice topic has 10 base fields:
  has_homeoffice_rights (bool)
  entitlement (Amount)
  stipend_present (bool)
  stipend (Amount)
  discretion (str enum)
  costs_reimbursed (bool)
  agreement_required (bool)
  health_safety_guarantee (bool)
  travel_time_compensation (bool)
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
OUT_PATH = QA_ROOT / "outputs" / "homeoffice_rule_violations.csv"

# Canonical enum for discretion field (per schema)
DISCRETION_VALID = {"employer_only", "joint_with_OR", "employee_request",
                    "unspecified", "other"}


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
        "topic_group": "homeoffice",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


# === Rules ===
def rule_R1_stipend_present_but_no_value(row, rid):
    """stipend_present=True but stipend_value empty → either set value or correct flag."""
    p = row.get("homeoffice_stipend_present", "")
    v = row.get("homeoffice_stipend_value", "")
    if _truthy(p) and _blank(v):
        return _new(rid, "R1", "homeoffice_stipend_value",
                    "medium", "extract",
                    csv_value_old=v,
                    notes="stipend_present=True but stipend_value empty; subagent should extract or set flag false")
    return None


def rule_R2_stipend_value_without_unit(row, rid):
    """stipend_value present but unit empty → propose 'EUR per month' (most common)."""
    v = row.get("homeoffice_stipend_value", "")
    u = row.get("homeoffice_stipend_unit", "")
    if not _blank(v) and _blank(u):
        return _new(rid, "R2", "homeoffice_stipend_unit",
                    "medium", "informed",
                    csv_value_old=u,
                    csv_value_new="EUR per month",
                    verdict="correct_in_place",
                    notes="stipend value present, unit empty; default assumed")
    return None


def rule_R3_stipend_value_unrealistic(row, rid):
    """stipend > 500 EUR/month is implausibly large (likely scale/extraction error)."""
    fv = _to_float(row.get("homeoffice_stipend_value", ""))
    u = str(row.get("homeoffice_stipend_unit", "")).strip().lower()
    if fv is not None and fv > 500 and ("month" in u or "maand" in u):
        return _new(rid, "R3", "homeoffice_stipend_value",
                    "high", "blind",
                    csv_value_old=fv, csv_unit_old=u,
                    notes=f"stipend {fv} {u} unusually large; verify scale/decimal")
    return None


def rule_R4_entitlement_value_implausible(row, rid):
    """entitlement value should be 1-7 days/week or 1-40 hours/week."""
    fv = _to_float(row.get("homeoffice_entitlement_value", ""))
    u = str(row.get("homeoffice_entitlement_unit", "")).strip().lower()
    if fv is None:
        return None
    if "day" in u or "dag" in u:
        if fv < 0.5 or fv > 7:
            return _new(rid, "R4", "homeoffice_entitlement_value",
                        "medium", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"entitlement {fv} days/week outside plausible range")
    elif "hour" in u or "uur" in u:
        if fv < 4 or fv > 40:
            return _new(rid, "R4", "homeoffice_entitlement_value",
                        "medium", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"entitlement {fv} hours/week outside plausible range")
    return None


def rule_R5_discretion_noncanonical(row, rid):
    """discretion populated with non-canonical value → must use enum."""
    v = str(row.get("homeoffice_discretion", "")).strip()
    if _blank(v):
        return None
    if v not in DISCRETION_VALID:
        return _new(rid, "R5", "homeoffice_discretion",
                    "medium", "informed",
                    csv_value_old=v,
                    notes=f"non-canonical enum value {v!r}; valid: {sorted(DISCRETION_VALID)}")
    return None


def rule_R6_has_rights_false_with_data(row, rid):
    """has_homeoffice_rights=False but other homeoffice_* fields populated."""
    h = row.get("homeoffice_has_homeoffice_rights", "")
    if not _falsy(h): return None
    populated = []
    for f in ["homeoffice_entitlement_value", "homeoffice_stipend_value",
              "homeoffice_discretion", "homeoffice_note"]:
        if not _blank(row.get(f, "")):
            populated.append(f)
    if populated:
        return _new(rid, "R6", "homeoffice_has_homeoffice_rights",
                    "medium", "blind",
                    csv_value_old=h, csv_value_new="True",
                    verdict="set_boolean",
                    notes=f"has_rights=False but populated: {populated[:3]}")
    return None


def rule_R7_year_in_duration_field(row, rid):
    """Year-like value in entitlement field — GENERAL_FM_03."""
    fv = _to_float(row.get("homeoffice_entitlement_value", ""))
    if fv is not None and 1990 <= fv <= 2030:
        return _new(rid, "R7", "homeoffice_entitlement_value",
                    "high", "none",
                    csv_value_old=fv,
                    csv_value_new="", csv_unit_new="",
                    verdict="clear",
                    notes="year-like value in duration field (GENERAL_FM_03)")
    return None


RULES = [
    rule_R1_stipend_present_but_no_value,
    rule_R2_stipend_value_without_unit,
    rule_R3_stipend_value_unrealistic,
    rule_R4_entitlement_value_implausible,
    rule_R5_discretion_noncanonical,
    rule_R6_has_rights_false_with_data,
    rule_R7_year_in_duration_field,
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
