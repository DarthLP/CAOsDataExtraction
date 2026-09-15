"""qa_safety_rules.py — Layer 1 deterministic QA for safety_* fields.

Safety schema is 12 boolean presence flags + 2 notes (psa_measures_note,
safety_note); no numeric/amount fields, no umbrella has_* boolean. So L1 is
limited to present-flag/note consistency. The bulk of review is L2 (boolean
fields scanned for false-negatives) handled in the det driver.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "safety_rule_violations.csv"


def _falsy(v) -> bool:
    if v is None or pd.isna(v): return False
    return str(v).strip().lower() in {"false", "0", "no", "n", "nee"}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)): return True
    return str(v).strip().lower() in {"", "nan", "none", "null", "n/a", "unknown"}


def _new(record_id, rule_id, field, severity, worksheet_mode,
         csv_value_old="", csv_value_new="", verdict="", notes=""):
    return {
        "record_id": str(record_id), "rule_id": rule_id, "field": field,
        "severity": severity, "worksheet_mode": worksheet_mode,
        "csv_value_old": str(csv_value_old) if not _blank(csv_value_old) else "",
        "csv_unit_old": "", "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new": "", "verdict": verdict, "target_field": "", "notes": notes,
        "topic_group": "safety",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


def rule_R1_psa_flag_false_with_note(row, rid):
    """psa_prevention_measures_present=False but psa_measures_note populated."""
    if _falsy(row.get("safety_psa_prevention_measures_present", "")) and \
            not _blank(row.get("safety_psa_measures_note", "")):
        return _new(rid, "R1", "safety_psa_prevention_measures_present",
                    "medium", "blind",
                    csv_value_old=row.get("safety_psa_prevention_measures_present", ""),
                    csv_value_new="True", verdict="set_boolean",
                    notes="psa_prevention_measures_present=False but psa_measures_note populated")
    return None


RULES = [rule_R1_psa_flag_false_with_note]


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
    return out_df


if __name__ == "__main__":
    main()
