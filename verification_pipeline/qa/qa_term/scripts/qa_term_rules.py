"""qa_term_rules.py — Layer 1 deterministic QA for term_* fields.

L1 = schema-internal consistency only (no statutory figures — those are checked
post-hoc by era_baselines, never here and never in the subagent prompt):
  R1  master-boolean term_has_termination_rules=False but detail fields populated
  R2  term_probation_allowed=False but a probation value is set
  R3  notice range_min > range_max (employer / employee) — likely swapped
"""
from __future__ import annotations
import pandas as pd

_DETAIL_FIELDS = [
    "term_employer_notice_value", "term_employee_notice_value",
    "term_employer_notice_range_min", "term_employee_notice_range_min",
    "term_probation_fixedterm_value", "term_probation_indef_value",
    "term_severance_extra_value", "term_severance_extra_formula",
    "term_notice_tenure_rule", "term_notice_min_floor_value",
]


def _falsy(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    return str(v).strip().lower() in {"false", "0", "no", "n", "nee"}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    return str(v).strip().lower() in {"", "nan", "none", "null", "n/a", "unknown"}


def _num(v):
    try:
        return float(str(v).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def _new(record_id, rule_id, field, severity, worksheet_mode,
         csv_value_old="", csv_value_new="", verdict="", notes=""):
    return {
        "record_id": str(record_id), "rule_id": rule_id, "field": field,
        "severity": severity, "worksheet_mode": worksheet_mode,
        "csv_value_old": str(csv_value_old) if not _blank(csv_value_old) else "",
        "csv_unit_old": "", "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new": "", "verdict": verdict, "target_field": "", "notes": notes,
        "topic_group": "term", "confidence": "", "evidence_quote": "",
    }


def rule_R1_master_flag_false_with_details(row, rid):
    master = row.get("term_has_termination_rules", "")
    if not _falsy(master):
        return None
    present = [f for f in _DETAIL_FIELDS if not _blank(row.get(f, ""))]
    if present:
        return _new(rid, "R1", "term_has_termination_rules", "medium", "blind",
                    csv_value_old=master, csv_value_new="True", verdict="set_boolean",
                    notes=f"has_termination_rules=False but detail fields populated: {present[:4]}")
    return None


def rule_R2_probation_value_without_allowed(row, rid):
    allowed = row.get("term_probation_allowed", "")
    has_val = (not _blank(row.get("term_probation_fixedterm_value", ""))) or \
              (not _blank(row.get("term_probation_indef_value", "")))
    if _falsy(allowed) and has_val:
        return _new(rid, "R2", "term_probation_allowed", "medium", "blind",
                    csv_value_old=allowed, csv_value_new="True", verdict="set_boolean",
                    notes="probation_allowed=False but a probation period value is set")
    return None


def _range_swap(row, rid, minf, maxf):
    lo, hi = _num(row.get(minf, "")), _num(row.get(maxf, ""))
    if lo is not None and hi is not None and lo > hi:
        return _new(rid, "R3", minf, "low", "blind",
                    csv_value_old=row.get(minf, ""),
                    notes=f"{minf}={lo} > {maxf}={hi} (range min>max; likely swapped)")
    return None


def rule_R3_employer_notice_range_order(row, rid):
    return _range_swap(row, rid, "term_employer_notice_range_min", "term_employer_notice_range_max")


def rule_R3b_employee_notice_range_order(row, rid):
    return _range_swap(row, rid, "term_employee_notice_range_min", "term_employee_notice_range_max")


RULES = [rule_R1_master_flag_false_with_details,
         rule_R2_probation_value_without_allowed,
         rule_R3_employer_notice_range_order,
         rule_R3b_employee_notice_range_order]
