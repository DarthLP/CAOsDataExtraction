"""qa_pension_rules.py — Layer 1 deterministic QA for pension_* fields.

L1 = schema-internal consistency only (no statutory figures — Witteveen accrual
cap / franchise floor are checked post-hoc by era_baselines, never here):
  R1  master pension_has_pension_scheme=False but detail fields populated
  R2  employee_contrib range_min > range_max (likely swapped)
  R3  premium_total range_min > range_max (likely swapped)
"""
from __future__ import annotations
import pandas as pd

_DETAIL_FIELDS = [
    "pension_pension_type", "pension_accrual_rate_value",
    "pension_franchise_value", "pension_employee_contrib_value",
    "pension_retire_age_normal_value", "pension_premium_total_range_min",
    "pension_employee_contrib_range_min", "pension_mandatory_participation",
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
        "topic_group": "pension", "confidence": "", "evidence_quote": "",
    }


def rule_R1_master_flag_false_with_details(row, rid):
    master = row.get("pension_has_pension_scheme", "")
    if not _falsy(master):
        return None
    present = [f for f in _DETAIL_FIELDS if not _blank(row.get(f, ""))]
    if present:
        return _new(rid, "R1", "pension_has_pension_scheme", "medium", "blind",
                    csv_value_old=master, csv_value_new="True", verdict="set_boolean",
                    notes=f"has_pension_scheme=False but detail fields populated: {present[:4]}")
    return None


def _range_swap(row, rid, minf, maxf):
    lo, hi = _num(row.get(minf, "")), _num(row.get(maxf, ""))
    if lo is not None and hi is not None and lo > hi:
        return _new(rid, "R2", minf, "low", "blind",
                    csv_value_old=row.get(minf, ""),
                    notes=f"{minf}={lo} > {maxf}={hi} (range min>max; likely swapped)")
    return None


def rule_R2_contrib_range_order(row, rid):
    return _range_swap(row, rid, "pension_employee_contrib_range_min", "pension_employee_contrib_range_max")


def rule_R3_premium_range_order(row, rid):
    return _range_swap(row, rid, "pension_premium_total_range_min", "pension_premium_total_range_max")


RULES = [rule_R1_master_flag_false_with_details,
         rule_R2_contrib_range_order,
         rule_R3_premium_range_order]
