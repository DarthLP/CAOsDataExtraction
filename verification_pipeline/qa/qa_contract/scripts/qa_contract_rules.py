"""qa_contract_rules.py — Layer 1 deterministic QA for contract_* fields.

Contract schema has 15 base fields (per NON_SALARY_PROMPTS_AND_SCHEMA.md):
  has_contract_type_rules (bool)
  full_time_hours (Amount)
  part_time_allowed (bool)
  part_time_range (AmountRange)
  minmax_hours_contract_allowed (bool)
  minmax_hours_range (AmountRange)
  zero_hour_oncall_allowed (bool)
  ketenregeling_deviation_present (bool)
  ketenregeling_max_contracts (Amount)
  ketenregeling_max_duration (Amount)
  conversion_rights_temp_to_perm_present (bool)
  conversion_rights_rule_text (str)
  workhours_adjustment_right_present (bool)
  workhours_adjustment_tenure_requirement (Amount)
  workhours_adjustment_min_firm_size (Amount)
  workhours_adjustment_note (str)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "contract_rule_violations.csv"


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
        "topic_group": "contract",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


# === Rules ===

def rule_R1_full_time_hours_implausible(row, rid):
    """full_time_hours should be 32-42 hours/week or 1500-2200 hours/year."""
    fv = _to_float(row.get("contract_full_time_hours_value", ""))
    u = str(row.get("contract_full_time_hours_unit", "")).strip().lower()
    if fv is None: return None
    if "week" in u:
        if fv < 30 or fv > 42:
            return _new(rid, "R1", "contract_full_time_hours_value",
                        "high", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"full_time {fv} hr/week implausible (expected 32-40)")
    elif "year" in u or "annual" in u:
        if fv < 1400 or fv > 2400:
            return _new(rid, "R1", "contract_full_time_hours_value",
                        "high", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"full_time {fv} hr/year implausible (expected 1500-2200)")
    return None


def rule_R2_unit_snake_case(row, rid):
    """Snake_case unit variants like 'hours_per_week' → 'hours per week'."""
    UNIT_FIELDS = [
        "contract_full_time_hours_unit",
        "contract_part_time_range_unit",
        "contract_minmax_hours_range_unit",
        "contract_ketenregeling_max_contracts_unit",
        "contract_ketenregeling_max_duration_unit",
        "contract_workhours_adjustment_tenure_requirement_unit",
        "contract_workhours_adjustment_min_firm_size_unit",
    ]
    for f in UNIT_FIELDS:
        u = str(row.get(f, "")).strip()
        if not _blank(u) and "_" in u and " " not in u:
            normalized = u.replace("_", " ")
            return _new(rid, "R2", f,
                        "low", "informed",
                        csv_value_old=u, csv_unit_old=u,
                        csv_value_new="", csv_unit_new=normalized,
                        verdict="correct_in_place",
                        notes=f"snake_case unit {u!r} → {normalized!r}")
    return None


def rule_R3_unit_singular(row, rid):
    """Singular unit forms (year → years, contract → contracts, employee → employees)."""
    SING_TO_PLURAL = {
        ("contract_ketenregeling_max_contracts_unit", "contract"): "contracts",
        ("contract_ketenregeling_max_duration_unit", "year"): "years",
        ("contract_workhours_adjustment_tenure_requirement_unit", "year"): "years",
        ("contract_workhours_adjustment_min_firm_size_unit", "employee"): "employees",
    }
    for (f, sing), plural in SING_TO_PLURAL.items():
        u = str(row.get(f, "")).strip().lower()
        if u == sing:
            return _new(rid, "R3", f,
                        "low", "informed",
                        csv_value_old=u, csv_unit_old=u,
                        csv_value_new="", csv_unit_new=plural,
                        verdict="correct_in_place",
                        notes=f"singular unit {u!r} → {plural!r}")
    return None


def rule_R4_ketenregeling_max_contracts_implausible(row, rid):
    """ketenregeling_max_contracts > 8 or < 2 is implausible."""
    fv = _to_float(row.get("contract_ketenregeling_max_contracts_value", ""))
    if fv is None: return None
    if fv > 8 or fv < 2:
        return _new(rid, "R4", "contract_ketenregeling_max_contracts_value",
                    "medium", "blind",
                    csv_value_old=fv,
                    notes=f"ketenregeling max_contracts={fv} implausible (expected 3-6)")
    return None


def rule_R5_ketenregeling_max_duration_unit_mismatch(row, rid):
    """ketenregeling_max_duration_value 24-72 with unit 'years' looks wrong
    (should be months); value 2-6 with 'months' looks wrong (should be years).
    Routes to subagent informed mode."""
    fv = _to_float(row.get("contract_ketenregeling_max_duration_value", ""))
    u = str(row.get("contract_ketenregeling_max_duration_unit", "")).strip().lower()
    if fv is None: return None
    if u in {"years", "year"} and fv > 10:
        return _new(rid, "R5", "contract_ketenregeling_max_duration_unit",
                    "medium", "informed",
                    csv_value_old=u, csv_unit_old=u,
                    csv_value_new="", csv_unit_new="months",
                    notes=f"value {fv} with unit {u!r} unlikely (likely months not years)")
    if u in {"months", "month"} and fv < 6:
        return _new(rid, "R5", "contract_ketenregeling_max_duration_unit",
                    "medium", "informed",
                    csv_value_old=u, csv_unit_old=u,
                    csv_value_new="", csv_unit_new="years",
                    notes=f"value {fv} with unit {u!r} unlikely (likely years not months)")
    return None


def rule_R6_part_time_range_with_disallow(row, rid):
    """part_time_range populated but part_time_allowed=False."""
    pa = row.get("contract_part_time_allowed", "")
    if not _falsy(pa): return None
    mn = row.get("contract_part_time_range_min", "")
    mx = row.get("contract_part_time_range_max", "")
    if not _blank(mn) or not _blank(mx):
        return _new(rid, "R6", "contract_part_time_allowed",
                    "medium", "blind",
                    csv_value_old=pa, csv_value_new="True",
                    verdict="set_boolean",
                    notes=f"part_time_allowed=False but range populated (min={mn}, max={mx})")
    return None


def rule_R7_minmax_range_with_disallow(row, rid):
    """minmax_hours_range populated but minmax_hours_contract_allowed=False."""
    ma = row.get("contract_minmax_hours_contract_allowed", "")
    if not _falsy(ma): return None
    mn = row.get("contract_minmax_hours_range_min", "")
    mx = row.get("contract_minmax_hours_range_max", "")
    if not _blank(mn) or not _blank(mx):
        return _new(rid, "R7", "contract_minmax_hours_contract_allowed",
                    "medium", "blind",
                    csv_value_old=ma, csv_value_new="True",
                    verdict="set_boolean",
                    notes=f"minmax_contract_allowed=False but range populated")
    return None


def rule_R8_year_in_numeric_field(row, rid):
    """Year-like value (1990-2030) in numeric fields — GENERAL_FM_03."""
    NUMERIC_FIELDS = [
        "contract_full_time_hours_value",
        "contract_ketenregeling_max_contracts_value",
        "contract_ketenregeling_max_duration_value",
        "contract_workhours_adjustment_tenure_requirement_value",
        "contract_workhours_adjustment_min_firm_size_value",
    ]
    for f in NUMERIC_FIELDS:
        fv = _to_float(row.get(f, ""))
        if fv is not None and 1990 <= fv <= 2030:
            return _new(rid, "R8", f,
                        "high", "none",
                        csv_value_old=fv,
                        csv_value_new="", csv_unit_new="",
                        verdict="clear",
                        notes=f"year-like value {fv} in {f} (GENERAL_FM_03)")
    return None


def rule_R9_workhours_adjust_with_disallow(row, rid):
    """workhours_adjustment_tenure_requirement or min_firm_size populated
    but workhours_adjustment_right_present=False."""
    p = row.get("contract_workhours_adjustment_right_present", "")
    if not _falsy(p): return None
    populated = []
    for f in ["contract_workhours_adjustment_tenure_requirement_value",
              "contract_workhours_adjustment_min_firm_size_value",
              "contract_workhours_adjustment_note"]:
        if not _blank(row.get(f, "")):
            populated.append(f)
    if populated:
        return _new(rid, "R9", "contract_workhours_adjustment_right_present",
                    "medium", "blind",
                    csv_value_old=p, csv_value_new="True",
                    verdict="set_boolean",
                    notes=f"adjustment_right=False but populated: {populated[:2]}")
    return None


def rule_R10_conversion_with_disallow(row, rid):
    """conversion_rights_rule_text populated but conversion_rights_present=False."""
    p = row.get("contract_conversion_rights_temp_to_perm_present", "")
    if not _falsy(p): return None
    t = row.get("contract_conversion_rights_rule_text", "")
    if not _blank(t):
        return _new(rid, "R10", "contract_conversion_rights_temp_to_perm_present",
                    "medium", "blind",
                    csv_value_old=p, csv_value_new="True",
                    verdict="set_boolean",
                    notes="conversion_rights=False but rule_text populated")
    return None


RULES = [
    rule_R1_full_time_hours_implausible,
    rule_R2_unit_snake_case,
    rule_R3_unit_singular,
    rule_R4_ketenregeling_max_contracts_implausible,
    rule_R5_ketenregeling_max_duration_unit_mismatch,
    rule_R6_part_time_range_with_disallow,
    rule_R7_minmax_range_with_disallow,
    rule_R8_year_in_numeric_field,
    rule_R9_workhours_adjust_with_disallow,
    rule_R10_conversion_with_disallow,
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
