"""qa_overtime_rules.py — Layer 1 deterministic QA for overtime_* fields.

Reads:
  qa/qa_overtime/inputs/scoped_records.csv  (Stage 1 output)

Writes:
  qa/qa_overtime/outputs/overtime_rule_violations.csv  (one row per violation)

Schema-internal consistency only. Does NOT read source text.

Each rule emits a violation dict with:
  rule_id, record_id, field, severity, worksheet_mode, csv_value_old,
  csv_unit_old, csv_value_new, csv_unit_new, verdict, target_field, notes

`worksheet_mode` choice (PLAN.md §1.3 and §3.1 conv #12):
  - "none"     : silent auto-correct, no subagent verification
  - "extract"  : presence-mismatch L2 case (field empty, source mentions topic)
  - "blind"    : verdict produced; subagent verifies independently
  - "informed" : verdict + proposed_correction shown to subagent
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

HERE = Path(__file__).resolve().parent
QA_ROOT = HERE.parent  # qa/qa_overtime/
CSV_PATH = QA_ROOT / "inputs" / "scoped_records.csv"
OUT_PATH = QA_ROOT / "outputs" / "overtime_rule_violations.csv"


# === Helpers ===
def _truthy(v) -> bool:
    if v is None or pd.isna(v):
        return False
    return str(v).strip().lower() in {"true", "1", "yes", "y", "ja"}


def _falsy(v) -> bool:
    if v is None or pd.isna(v):
        return False
    return str(v).strip().lower() in {"false", "0", "no", "n", "nee"}


def _blank(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    s = str(v).strip().lower()
    return s in {"", "nan", "none", "null", "n/a", "unknown"}


def _to_float(v) -> Optional[float]:
    if _blank(v):
        return None
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


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
        "csv_unit_old": str(csv_unit_old) if not _blank(csv_unit_old) else "",
        "csv_value_new": str(csv_value_new) if not _blank(csv_value_new) else "",
        "csv_unit_new": str(csv_unit_new) if not _blank(csv_unit_new) else "",
        "verdict": verdict,
        "target_field": target_field,
        "notes": notes,
        "topic_group": "overtime",
        "confidence": "high" if worksheet_mode == "none" else "",
        "evidence_quote": "",
    }


# === Rules ===
def rule_R1_unit_missing(row, rid):
    """Pay-rate value present but unit empty -> propose `% of hourly rate`."""
    v = row.get("overtime_allowance_value", "")
    u = row.get("overtime_allowance_unit", "")
    if not _blank(v) and _blank(u):
        return _new(rid, "R1", "overtime_allowance_value",
                    "medium", "informed",
                    csv_value_old=v, csv_unit_old=u,
                    csv_value_new=v, csv_unit_new="% of hourly rate",
                    verdict="correct_in_place",
                    notes="unit missing on pay-rate value; default assumed")
    return None


def rule_R2_pay_with_duration_unit(row, rid):
    """Pay-rate field with a duration unit -> field/unit mismatch (GENERAL_FM_05)."""
    v = row.get("overtime_allowance_value", "")
    u = str(row.get("overtime_allowance_unit", "")).strip().lower()
    duration_units = {"weeks", "week", "weken", "days", "day", "dagen",
                      "hours", "hour", "uur", "uren", "months", "month",
                      "maanden", "years", "year", "jaar"}
    if not _blank(v) and u in duration_units:
        return _new(rid, "R2", "overtime_allowance_value",
                    "high", "blind",
                    csv_value_old=v, csv_unit_old=u,
                    csv_value_new="", csv_unit_new="",
                    verdict="clear",
                    notes=f"pay-rate field with duration unit '{u}'; likely extraction error")
    return None


def rule_R3_pay_value_over_1000(row, rid):
    """Pay-rate value > 1000 likely indicates scale/extraction error."""
    fv = _to_float(row.get("overtime_allowance_value", ""))
    if fv is not None and fv > 1000:
        return _new(rid, "R3", "overtime_allowance_value",
                    "high", "blind",
                    csv_value_old=row.get("overtime_allowance_value", ""),
                    csv_unit_old=row.get("overtime_allowance_unit", ""),
                    verdict="",  # subagent decides
                    notes=f"value={fv} implausibly large; check decimal-strip / scale")
    return None


def rule_R4_trigger_daily_implausible(row, rid):
    """trigger_daily should be roughly 6-12 hours."""
    fv = _to_float(row.get("overtime_trigger_daily_value", ""))
    if fv is not None and (fv < 6 or fv > 14):
        return _new(rid, "R4", "overtime_trigger_daily_value",
                    "medium", "blind",
                    csv_value_old=row.get("overtime_trigger_daily_value", ""),
                    csv_unit_old=row.get("overtime_trigger_daily_unit", ""),
                    verdict="",
                    notes=f"daily trigger {fv}h outside plausible range [6, 14]")
    return None


def rule_R5_trigger_weekly_implausible(row, rid):
    """trigger_weekly should be roughly 30-50 hours."""
    fv = _to_float(row.get("overtime_trigger_weekly_value", ""))
    if fv is not None and (fv < 30 or fv > 60):
        return _new(rid, "R5", "overtime_trigger_weekly_value",
                    "medium", "blind",
                    csv_value_old=row.get("overtime_trigger_weekly_value", ""),
                    csv_unit_old=row.get("overtime_trigger_weekly_unit", ""),
                    verdict="",
                    notes=f"weekly trigger {fv}h outside plausible range [30, 60]")
    return None


def rule_R6_range_without_hetero(row, rid):
    """allowance_range_min/max filled but hetero_present is False/empty."""
    rng_min = row.get("overtime_allowance_range_min", "")
    rng_max = row.get("overtime_allowance_range_max", "")
    hetero = row.get("overtime_hetero_present", "")
    if (not _blank(rng_min) or not _blank(rng_max)) and not _truthy(hetero):
        return _new(rid, "R6", "overtime_hetero_present",
                    "medium", "informed",
                    csv_value_old=hetero,
                    csv_value_new="True",
                    verdict="set_boolean",
                    notes="allowance_range populated but hetero_present is not True")
    return None


def rule_R7_year_in_duration_field(row, rid):
    """A trigger or max-hours value in 1990-2030 range = year reference."""
    for field in ["overtime_trigger_daily_value",
                  "overtime_trigger_weekly_value",
                  "overtime_max_hours_per_day_value",
                  "overtime_max_hours_per_week_value",
                  "overtime_compulsory_annual_value"]:
        fv = _to_float(row.get(field, ""))
        if fv is not None and 1990 <= fv <= 2030:
            return _new(rid, "R7", field,
                        "high", "none",  # generic FM, safe to auto-clear
                        csv_value_old=row.get(field, ""),
                        csv_unit_old=row.get(field.rsplit("_", 1)[0] + "_unit", ""),
                        csv_value_new="", csv_unit_new="",
                        verdict="clear",
                        notes=f"year-like value in duration field (GENERAL_FM_03)")
    return None


def rule_R8_max_lt_trigger(row, rid):
    """overtime_max_hours_per_week_value should be >= overtime_trigger_weekly_value."""
    mx = _to_float(row.get("overtime_max_hours_per_week_value", ""))
    tg = _to_float(row.get("overtime_trigger_weekly_value", ""))
    if mx is not None and tg is not None and mx < tg:
        return _new(rid, "R8", "overtime_max_hours_per_week_value",
                    "medium", "blind",
                    csv_value_old=row.get("overtime_max_hours_per_week_value", ""),
                    notes=f"max_weekly={mx} < trigger_weekly={tg}; values may be swapped")
    return None


def rule_R9_min_rest_unit_check(row, rid):
    """overtime_min_rest_between_shifts should have unit 'hours' if value is set."""
    v = row.get("overtime_min_rest_between_shifts_value", "")
    u = str(row.get("overtime_min_rest_between_shifts_unit", "")).strip().lower()
    if not _blank(v) and u not in ("hours", "hour", "uur", "uren", ""):
        return _new(rid, "R9", "overtime_min_rest_between_shifts_unit",
                    "low", "informed",
                    csv_value_old=u, csv_unit_old="",
                    csv_value_new="hours",
                    verdict="correct_in_place",
                    notes=f"min_rest expected in hours, got '{u}'")
    return None


def rule_R10_has_overtime_rules_false_with_data(row, rid):
    """has_overtime_rules=False but other overtime_* fields populated."""
    has = row.get("overtime_has_overtime_rules", "")
    if not _falsy(has):
        return None
    populated = []
    for f in ["overtime_trigger_daily_value", "overtime_trigger_weekly_value",
              "overtime_allowance_value", "overtime_compensation_mode"]:
        if not _blank(row.get(f, "")):
            populated.append(f)
    if populated:
        return _new(rid, "R10", "overtime_has_overtime_rules",
                    "medium", "blind",
                    csv_value_old=has, csv_value_new="True",
                    verdict="set_boolean",
                    notes=f"has_overtime_rules=False but other fields populated: {populated[:3]}")
    return None


RULES = [
    rule_R1_unit_missing,
    rule_R2_pay_with_duration_unit,
    rule_R3_pay_value_over_1000,
    rule_R4_trigger_daily_implausible,
    rule_R5_trigger_weekly_implausible,
    rule_R6_range_without_hetero,
    rule_R7_year_in_duration_field,
    rule_R8_max_lt_trigger,
    rule_R9_min_rest_unit_check,
    rule_R10_has_overtime_rules_false_with_data,
]


def main():
    df = pd.read_csv(CSV_PATH, sep=";", dtype=str, keep_default_na=False)
    print(f"Loaded {len(df)} scoped records from {CSV_PATH}")

    violations = []
    for _, row in df.iterrows():
        rid = str(row.get("id", "")).strip()
        for rule in RULES:
            v = rule(row, rid)
            if v is not None:
                # Pass through cao_number + file_name + ingangsdatum
                v["cao_number"] = str(row.get("cao_number", ""))
                v["file_name"] = str(row.get("file_name", ""))
                v["ingangsdatum"] = str(row.get("ingangsdatum", ""))
                violations.append(v)

    out_df = pd.DataFrame(violations)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT_PATH, sep=";", index=False)
    print(f"Wrote {len(violations)} violations to {OUT_PATH}")

    # Summary by rule
    if violations:
        summary = out_df.groupby("rule_id").size().sort_values(ascending=False)
        print("\nViolations by rule:")
        for rule_id, count in summary.items():
            print(f"  {rule_id}: {count}")
    return out_df


if __name__ == "__main__":
    main()
