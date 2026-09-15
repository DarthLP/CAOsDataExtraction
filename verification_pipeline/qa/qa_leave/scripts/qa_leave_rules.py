"""qa_leave_rules.py — Layer 1 deterministic QA for leave_* fields (standardized re-run).

L1 = schema-internal consistency only (no statutory facts; statutory floors are
checked post-hoc by era_baselines):
  R1  master leave_has_leave_enhancements=False but detail fields populated
  R2  a *_statutory_ref=True with non-empty paired sub-values (restatement should be empty)
"""
from __future__ import annotations
import pandas as pd

_DETAIL_FIELDS = [
    "leave_paid_maternity_value", "leave_paid_paternity_value",
    "leave_partially_paid_paternity_value", "leave_parental_topup_present",
    "leave_parental_unpaid_value", "leave_sick_topup_present",
    "leave_short_term_care_value", "leave_long_term_care_value",
    "leave_vacation_time_value", "leave_extra_seniority_present",
    "leave_adoption_value", "leave_note",
]

_STATUTORY_PAIRS = [
    ("leave_parental_statutory_ref",
     ["leave_parental_min_tenure_value", "leave_parental_topup_pay_value",
      "leave_parental_unpaid_value"]),
    ("leave_care_statutory_ref",
     ["leave_short_term_care_value", "leave_long_term_care_value"]),
]


def _falsy(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    return str(v).strip().lower() in {"false", "0", "no", "n", "nee"}


def _truthy(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    return str(v).strip().lower() in {"true", "1", "yes", "y", "ja"}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    return str(v).strip().lower() in {"", "nan", "none", "null", "n/a", "unknown"}


def _new(record_id, rule_id, field, severity, worksheet_mode,
         csv_value_old="", csv_value_new="", verdict="", notes=""):
    return {
        "record_id": str(record_id), "rule_id": rule_id, "field": field,
        "severity": severity, "worksheet_mode": worksheet_mode,
        "csv_value_old": str(csv_value_old) if not _blank(csv_value_old) else "",
        "csv_unit_old": "", "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new": "", "verdict": verdict, "target_field": "", "notes": notes,
        "topic_group": "leave", "confidence": "", "evidence_quote": "",
    }


def rule_R1_master_flag_false_with_details(row, rid):
    master = row.get("leave_has_leave_enhancements", "")
    if not _falsy(master):
        return None
    present = [f for f in _DETAIL_FIELDS if not _blank(row.get(f, ""))]
    if present:
        return _new(rid, "R1", "leave_has_leave_enhancements", "medium", "blind",
                    csv_value_old=master, csv_value_new="True", verdict="set_boolean",
                    notes=f"has_leave_enhancements=False but detail fields populated: {present[:4]}")
    return None


def rule_R2_statutory_ref_with_values(row, rid):
    for ref_field, subs in _STATUTORY_PAIRS:
        if _truthy(row.get(ref_field, "")):
            filled = [s for s in subs if not _blank(row.get(s, ""))]
            if filled:
                return _new(rid, "R2", ref_field, "low", "blind",
                            csv_value_old=row.get(ref_field, ""),
                            notes=f"{ref_field}=True (statutory restatement) but sub-values populated: {filled}")
    return None


RULES = [rule_R1_master_flag_false_with_details,
         rule_R2_statutory_ref_with_values]
