"""qa_childcare_rules.py — Layer 1 deterministic QA for childcare_* fields.

Booleans (support_present, inhouse_present, discount_present, priority_access,
public_coord*, funding_sector_fund) + amounts (support, support_cap, age_min,
age_max, min_fte) + min_tenure_months (float) + str enums (provider_scope,
public_coord) + notes (eligibility_note).
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional
import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "childcare_rule_violations.csv"


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
        "topic_group": "childcare",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


_VALUE_UNIT_PAIRS = [
    ("childcare_support_value", "childcare_support_unit"),
    ("childcare_support_cap_value", "childcare_support_cap_unit"),
    ("childcare_age_min_value", "childcare_age_min_unit"),
    ("childcare_age_max_value", "childcare_age_max_unit"),
    ("childcare_min_fte_value", "childcare_min_fte_unit"),
]


def rule_R1_support_flag_false_with_data(row, rid):
    """childcare_support_present=False but support value/cap populated."""
    if not _falsy(row.get("childcare_childcare_support_present", "")):
        return None
    populated = [f for f in ("childcare_support_value", "childcare_support_cap_value")
                 if not _blank(row.get(f, ""))]
    if populated:
        return _new(rid, "R1", "childcare_childcare_support_present", "medium", "blind",
                    csv_value_old=row.get("childcare_childcare_support_present", ""),
                    csv_value_new="True", verdict="set_boolean",
                    notes=f"support_present=False but populated: {populated}")
    return None


def rule_R2_value_without_unit(row, rid):
    for vf, uf in _VALUE_UNIT_PAIRS:
        if not _blank(row.get(vf, "")) and _blank(row.get(uf, "")):
            return _new(rid, "R2", uf, "medium", "extract",
                        csv_value_old=row.get(uf, ""),
                        notes=f"{vf} present but {uf} empty; extract unit")
    return None


def rule_R3_age_implausible(row, rid):
    """age_min should be 0-6 years; age_max 2-18 years."""
    amin = _to_float(row.get("childcare_age_min_value", ""))
    amax = _to_float(row.get("childcare_age_max_value", ""))
    u_min = str(row.get("childcare_age_min_unit", "")).strip().lower()
    u_max = str(row.get("childcare_age_max_unit", "")).strip().lower()
    if amin is not None and ("year" in u_min or u_min == "") and (amin < 0 or amin > 6):
        return _new(rid, "R3", "childcare_age_min_value", "medium", "blind",
                    csv_value_old=amin, csv_unit_old=u_min,
                    notes=f"age_min {amin} outside plausible 0-6 years")
    if amax is not None and ("year" in u_max or u_max == "") and (amax < 2 or amax > 18):
        return _new(rid, "R3", "childcare_age_max_value", "medium", "blind",
                    csv_value_old=amax, csv_unit_old=u_max,
                    notes=f"age_max {amax} outside plausible 2-18 years")
    return None


def rule_R4_year_in_numeric(row, rid):
    for f in ("childcare_support_value", "childcare_support_cap_value",
              "childcare_age_min_value", "childcare_age_max_value", "childcare_min_fte_value"):
        fv = _to_float(row.get(f, ""))
        if fv is not None and 1990 <= fv <= 2030:
            return _new(rid, "R4", f, "high", "none",
                        csv_value_old=fv, csv_value_new="", csv_unit_new="",
                        verdict="clear", notes=f"year-like value {fv} in {f} (GENERAL_FM_03)")
    return None


RULES = [
    rule_R1_support_flag_false_with_data,
    rule_R2_value_without_unit,
    rule_R3_age_implausible,
    rule_R4_year_in_numeric,
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
