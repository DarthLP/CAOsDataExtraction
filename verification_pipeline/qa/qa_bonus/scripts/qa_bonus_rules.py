"""qa_bonus_rules.py — Layer 1 deterministic QA for bonus_* fields.

Bonus schema (bonuses_info) — presence flags paired with amounts/notes, no enums:
  has_bonus_schemes (bool)
  sign_on_bonus_present (bool) + sign_on_bonus (Amount: _value/_unit)
  thirteenth_month (bool) + thirteenth_month_amt (Amount: _value/_unit)
  fixed_annual_lump (Amount: _value/_unit)
  profit_sharing_present (bool) + profit_sharing_note (str)
  performance_bonus_present (bool)
  job_allowances_present (bool) + job_allowances_note (str)
  qual_bonus_present (bool) + qualification_bonus_note (str)
  seniority_loyalty_bonus (bool)
  retire_gratuity_present (bool) + retirement_gratuity_note (str)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "bonus_rule_violations.csv"


def _truthy(v) -> bool:
    if v is None or pd.isna(v): return False
    return str(v).strip().lower() in {"true", "1", "yes", "y", "ja"}


def _falsy(v) -> bool:
    if v is None or pd.isna(v): return False
    return str(v).strip().lower() in {"false", "0", "no", "n", "nee"}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)): return True
    return str(v).strip().lower() in {"", "nan", "none", "null", "n/a", "unknown"}


def _to_float(v) -> Optional[float]:
    if _blank(v): return None
    try: return float(str(v).replace(",", "."))
    except (ValueError, TypeError): return None


def _new(record_id, rule_id, field, severity, worksheet_mode,
         csv_value_old="", csv_unit_old="", csv_value_new="", csv_unit_new="",
         verdict="", target_field="", notes=""):
    return {
        "record_id": str(record_id), "rule_id": rule_id, "field": field,
        "severity": severity, "worksheet_mode": worksheet_mode,
        "csv_value_old": str(csv_value_old) if not _blank(csv_value_old) else "",
        "csv_unit_old":  str(csv_unit_old)  if not _blank(csv_unit_old)  else "",
        "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new":  str(csv_unit_new)  if not _blank(csv_unit_new)  else "",
        "verdict": verdict, "target_field": target_field, "notes": notes,
        "topic_group": "bonus",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


# Presence-flag → paired data fields. If flag is False but paired data is
# populated, that's an inconsistency (R1).
_FLAG_TO_PAIR = {
    "bonus_sign_on_bonus_present": ["bonus_sign_on_bonus_value", "bonus_sign_on_bonus_unit"],
    "bonus_thirteenth_month": ["bonus_thirteenth_month_amt_value", "bonus_thirteenth_month_amt_unit"],
    "bonus_profit_sharing_present": ["bonus_profit_sharing_note"],
    "bonus_job_allowances_present": ["bonus_job_allowances_note"],
    "bonus_qual_bonus_present": ["bonus_qualification_bonus_note"],
    "bonus_retire_gratuity_present": ["bonus_retirement_gratuity_note"],
}

# Value→unit pairs (value present but unit empty → R2)
_VALUE_UNIT_PAIRS = [
    ("bonus_sign_on_bonus_value", "bonus_sign_on_bonus_unit"),
    ("bonus_thirteenth_month_amt_value", "bonus_thirteenth_month_amt_unit"),
    ("bonus_fixed_annual_lump_value", "bonus_fixed_annual_lump_unit"),
]


def rule_R1_flag_false_with_data(row, rid):
    """Presence flag False but paired amount/note populated → set flag True."""
    for flag, pairs in _FLAG_TO_PAIR.items():
        if not _falsy(row.get(flag, "")):
            continue
        populated = [p for p in pairs if not _blank(row.get(p, ""))]
        if populated:
            return _new(rid, "R1", flag, "medium", "blind",
                        csv_value_old=row.get(flag, ""), csv_value_new="True",
                        verdict="set_boolean",
                        notes=f"{flag}=False but paired data populated: {populated}")
    return None


def rule_R2_value_without_unit(row, rid):
    """Amount value present but unit empty → extract unit."""
    for vf, uf in _VALUE_UNIT_PAIRS:
        if not _blank(row.get(vf, "")) and _blank(row.get(uf, "")):
            return _new(rid, "R2", uf, "medium", "extract",
                        csv_value_old=row.get(uf, ""),
                        notes=f"{vf} present but {uf} empty; extract unit from source")
    return None


def rule_R3_thirteenth_month_pct_implausible(row, rid):
    """13th-month amount as a percentage should be ~8.33% (1/12); flag > 15%
    (likely a different bonus or scale error). Skip non-% units."""
    fv = _to_float(row.get("bonus_thirteenth_month_amt_value", ""))
    u = str(row.get("bonus_thirteenth_month_amt_unit", "")).strip().lower()
    if fv is None:
        return None
    if "%" in u or "percent" in u:
        if fv > 15:
            return _new(rid, "R3", "bonus_thirteenth_month_amt_value",
                        "medium", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"13th-month {fv}% implausibly high (≈8.33% expected); verify")
    return None


def rule_R4_year_in_numeric(row, rid):
    """Year-like value (1990-2030) in a numeric amount field (GENERAL_FM_03)."""
    for f in ("bonus_thirteenth_month_amt_value", "bonus_fixed_annual_lump_value",
              "bonus_sign_on_bonus_value"):
        fv = _to_float(row.get(f, ""))
        if fv is not None and 1990 <= fv <= 2030:
            return _new(rid, "R4", f, "high", "none",
                        csv_value_old=fv, csv_value_new="", csv_unit_new="",
                        verdict="clear",
                        notes=f"year-like value {fv} in {f} (GENERAL_FM_03)")
    return None


def rule_R5_has_schemes_false_with_data(row, rid):
    """has_bonus_schemes=False but a specific bonus flag/amount is populated."""
    if not _falsy(row.get("bonus_has_bonus_schemes", "")):
        return None
    signals = []
    for f in ["bonus_thirteenth_month", "bonus_profit_sharing_present",
              "bonus_performance_bonus_present", "bonus_sign_on_bonus_present",
              "bonus_qual_bonus_present", "bonus_seniority_loyalty_bonus",
              "bonus_retire_gratuity_present"]:
        if _truthy(row.get(f, "")):
            signals.append(f)
    # also amount fields
    for f in ["bonus_thirteenth_month_amt_value", "bonus_fixed_annual_lump_value"]:
        if not _blank(row.get(f, "")):
            signals.append(f)
    if signals:
        return _new(rid, "R5", "bonus_has_bonus_schemes", "medium", "blind",
                    csv_value_old=row.get("bonus_has_bonus_schemes", ""),
                    csv_value_new="True", verdict="set_boolean",
                    notes=f"has_bonus_schemes=False but bonuses present: {signals[:3]}")
    return None


RULES = [
    rule_R1_flag_false_with_data,
    rule_R2_value_without_unit,
    rule_R3_thirteenth_month_pct_implausible,
    rule_R4_year_in_numeric,
    rule_R5_has_schemes_false_with_data,
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
        for rid, n in out_df.groupby("rule_id").size().sort_values(ascending=False).items():
            print(f"  {rid}: {n}")
    return out_df


if __name__ == "__main__":
    main()
