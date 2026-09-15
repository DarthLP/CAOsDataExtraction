"""qa_fringe_rules.py — Layer 1 deterministic QA for fringe_* fields.

Presence flags paired with amounts/notes (no hard enums; meal_benefit_type is
a soft str enum handled by the subagent).
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional
import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "fringe_rule_violations.csv"


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
        "csv_unit_old": str(csv_unit_old) if not _blank(csv_unit_old) else "",
        "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new": str(csv_unit_new) if not _blank(csv_unit_new) else "",
        "verdict": verdict, "target_field": target_field, "notes": notes,
        "topic_group": "fringe",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


_FLAG_TO_PAIR = {
    "fringe_commuting_allowance_present": ["fringe_commuting_allowance_value", "fringe_commuting_allowance_unit"],
    "fringe_meal_benefit_present": ["fringe_meal_benefit_type", "fringe_meal_benefit_amt_value", "fringe_meal_benefit_amt_unit"],
    "fringe_health_insurance_support_present": ["fringe_health_insurance_support_note"],
    "fringe_insurance_or_savings_benefit_present": ["fringe_insurance_or_savings_benefit_note"],
    "fringe_relocation_allowance_present": ["fringe_relocation_allowance_value", "fringe_relocation_allowance_unit"],
}
_VALUE_UNIT_PAIRS = [
    ("fringe_commuting_allowance_value", "fringe_commuting_allowance_unit"),
    ("fringe_meal_benefit_amt_value", "fringe_meal_benefit_amt_unit"),
    ("fringe_relocation_allowance_value", "fringe_relocation_allowance_unit"),
]


def rule_R1_flag_false_with_data(row, rid):
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
    for vf, uf in _VALUE_UNIT_PAIRS:
        if not _blank(row.get(vf, "")) and _blank(row.get(uf, "")):
            return _new(rid, "R2", uf, "medium", "extract",
                        csv_value_old=row.get(uf, ""),
                        notes=f"{vf} present but {uf} empty; extract unit")
    return None


def rule_R3_commuting_km_implausible(row, rid):
    """Per-km commuting allowance should be ~0.10-0.40 EUR. Flag outliers."""
    fv = _to_float(row.get("fringe_commuting_allowance_value", ""))
    u = str(row.get("fringe_commuting_allowance_unit", "")).strip().lower()
    if fv is None: return None
    if "km" in u or "kilomet" in u:
        if fv > 1.0:
            return _new(rid, "R3", "fringe_commuting_allowance_value", "medium", "blind",
                        csv_value_old=fv, csv_unit_old=u,
                        notes=f"commuting {fv}/km implausibly high (≈0.19-0.23 expected); verify cents vs EUR")
    return None


def rule_R4_year_in_numeric(row, rid):
    for f in ("fringe_commuting_allowance_value", "fringe_meal_benefit_amt_value",
              "fringe_relocation_allowance_value"):
        fv = _to_float(row.get(f, ""))
        if fv is not None and 1990 <= fv <= 2030:
            return _new(rid, "R4", f, "high", "none",
                        csv_value_old=fv, csv_value_new="", csv_unit_new="",
                        verdict="clear", notes=f"year-like value {fv} in {f} (GENERAL_FM_03)")
    return None


def rule_R5_has_fringe_false_with_data(row, rid):
    if not _falsy(row.get("fringe_has_fringe_benefits", "")):
        return None
    signals = []
    for f in ["fringe_commuting_allowance_present", "fringe_bike_scheme_present",
              "fringe_internet_or_phone_reimbursement_present", "fringe_meal_benefit_present",
              "fringe_health_insurance_support_present", "fringe_insurance_or_savings_benefit_present",
              "fringe_relocation_allowance_present", "fringe_mandatory_certifications_paid"]:
        if _truthy(row.get(f, "")):
            signals.append(f)
    if signals:
        return _new(rid, "R5", "fringe_has_fringe_benefits", "medium", "blind",
                    csv_value_old=row.get("fringe_has_fringe_benefits", ""),
                    csv_value_new="True", verdict="set_boolean",
                    notes=f"has_fringe_benefits=False but benefits present: {signals[:3]}")
    return None


RULES = [
    rule_R1_flag_false_with_data,
    rule_R2_value_without_unit,
    rule_R3_commuting_km_implausible,
    rule_R4_year_in_numeric,
    rule_R5_has_fringe_false_with_data,
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
