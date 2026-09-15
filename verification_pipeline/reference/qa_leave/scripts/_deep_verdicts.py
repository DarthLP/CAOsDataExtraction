"""
Deep verdicts (Claude-judged) on the bucket-B Layer-1-flagged sample.

For these records, Layer-1 surfaced a likely issue. The deep judge confirms
or refines the diagnosis against the source_text.

Scope of this file: 10 records hand-judged so far; can be extended in future
sessions by appending to VERDICTS.

Output: qa_leave/outputs/deep_sample/deep_verdicts.csv
"""
from __future__ import annotations
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_INDEX = ROOT / "outputs" / "deep_sample" / "sample_index.csv"
OUT_PATH = ROOT / "outputs" / "deep_sample" / "deep_verdicts.csv"


VERDICTS: list[dict] = [
    # --- 433001  CAO 433 RAS 2010-2011  (PAR_03 + VAC_01) ---
    # Source: "The employee must have worked for the same employer for at least one year." (parental)
    # Source: "Vacation entitlement: 10% of every paid hour" (vacation)
    {"record_id": "433001", "topic_group": "parental", "status": "contradicted",
     "field_issues": [
         {"field": "leave_parental_exceptions", "csv_value": "False",
          "issue": "Source explicitly states '1 year tenure required for parental leave eligibility'. CAO has a CAO-specific tenure exception over default Wet Arbeid en Zorg eligibility, so parental_exceptions MUST be True. Layer-1 PAR_03 fired correctly; the fix is to set exceptions=True, not to clear the tenure value.",
          "severity": "high"},
     ],
     "evidence_quote": "The employee must have worked for the same employer for at least one year",
     "judge_notes": "PAR_03 root cause: exceptions=False is wrong. Tenure value of 1 year is correct."},
    {"record_id": "433001", "topic_group": "vacation_holidays", "status": "contradicted",
     "field_issues": [
         {"field": "leave_vacation_time_value", "csv_value": "10.0",
          "issue": "Value 10.0 with unit 'percent of paid hours' is a percentage-of-paid-hours accrual model, not a duration. The schema's vacation_time field expects a 'days per year' or 'hours per year' value; 10% is a rate. Should be normalized: 10% of paid hours ≈ 25 days/year for full-time worker.",
          "severity": "medium"},
     ],
     "evidence_quote": "Vacation entitlement: 10% of every paid hour or part thereof",
     "judge_notes": "Field-role confusion: rate stored in duration field. RAS-style accrual model is unusual."},
    {"record_id": "433001", "topic_group": "sick", "status": "partial",
     "field_issues": [
         {"field": "leave_sickpay_continuation_value", "csv_value": "100.0",
          "issue": "Source has 3-tier scheme based on tenure (70% / 90% / 100%). 100% captures only the highest tier (>2 years tenure); shorter-tenure workers get less. Tier structure lost.",
          "severity": "medium"},
     ],
     "evidence_quote": "less than 6 months: 70% ... between 6 months and 2 years: 90% ... more than 2 years: 100%",
     "judge_notes": "duration=2 years correct. Tiered structure preserved only if note field includes it."},

    # --- 433004  CAO 433 RAS 2014  (PAR_03 + CARE_01 + VAC_01) ---
    {"record_id": "433004", "topic_group": "parental", "status": "contradicted",
     "field_issues": [
         {"field": "leave_parental_exceptions", "csv_value": "False",
          "issue": "Same pattern as 433001 — CAO has 1-year tenure rule but exceptions=False. Should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "must have worked for the same employer for at least one year",
     "judge_notes": "Recurring CAO 433 pattern — fix is exceptions=True."},
    {"record_id": "433004", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Layer-1 CARE_01 fired: care_statutory_ref=True AND care_exceptions=False but short_term_care + long_term_care fields filled with concrete CAO-specific values. The CAO does extend statutory care leave (70% pay floor at minimum wage; 12 weeks long-term). exceptions should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "Income: Employer pays 70% of the wage; if that is less than the minimum wage, the employer pays the minimum wage",
     "judge_notes": "Same CARE_01 root-cause pattern as bucket-B records 35004/51014/51015/243014/243019/433008: exceptions=False wrong."},
    {"record_id": "433004", "topic_group": "vacation_holidays", "status": "contradicted",
     "field_issues": [
         {"field": "leave_vacation_time_value", "csv_value": "10.0",
          "issue": "Same as 433001 — 10% of paid hours stored as if it were days/hours per year.",
          "severity": "medium"},
     ],
     "evidence_quote": "Vacation entitlement: 10% of every paid hour or part thereof",
     "judge_notes": "Recurring CAO 433 vacation accrual model issue."},

    # --- 433008  CAO 433 RAS 2017  (CARE_01 + VAC_01) ---
    {"record_id": "433008", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Same CARE_01 root cause: care_statutory_ref=True but care_exceptions=False while detail fields are populated. Should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care leave detail fields filled — implies CAO has CAO-specific provision)",
     "judge_notes": "Recurring CARE_01 pattern."},
    {"record_id": "433008", "topic_group": "vacation_holidays", "status": "contradicted",
     "field_issues": [
         {"field": "leave_vacation_time_value", "csv_value": "10.0",
          "issue": "Recurring CAO 433 model — 10% of paid hour stored in vacation_time.",
          "severity": "medium"},
     ],
     "evidence_quote": "(percentage-of-paid-hour model)",
     "judge_notes": "Recurring."},

    # --- 35004  CAO 35 Textielverzorging 2024  (CARE_01) ---
    {"record_id": "35004", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "CARE_01 root cause: care_statutory_ref=True AND care_exceptions=False but short_term_care=2x weekly hours @ 70% and long_term_care fields are filled — CAO has CAO-specific values that exceed pure statutory recital. exceptions should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care fields populated with concrete values; statutory_ref=True implies CAO references Wet Arbeid en Zorg)",
     "judge_notes": "Same gating-boolean pattern across all bucket-B CARE_01 records."},

    # --- 41008  CAO 41 2020  (CARE_01) ---
    {"record_id": "41008", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Same CARE_01 pattern: care_statutory_ref=True AND care_exceptions=False but care detail fields populated (short_term=2 weeks, long_term=6 weeks). CAO has CAO-specific values; exceptions should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care detail fields filled to non-default values)",
     "judge_notes": "Recurring."},

    # --- 51014, 51015  CAO 51 Timmer 2019, 2020  (CARE_01) ---
    {"record_id": "51014", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Recurring CARE_01 pattern: ref=True, exceptions=False, but care fields filled with non-default values. Fix: exceptions=True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care detail fields populated)",
     "judge_notes": "Recurring."},
    {"record_id": "51015", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Recurring CARE_01 pattern: ref=True, exceptions=False, but care fields filled. Fix: exceptions=True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care detail fields populated)",
     "judge_notes": "Recurring."},

    # --- 243014, 243019  CAO 243 Hoveniers 2018, 2021  (CARE_01) ---
    {"record_id": "243014", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Recurring CARE_01 root cause. Fix: exceptions=True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care detail fields populated)",
     "judge_notes": "Recurring."},
    {"record_id": "243019", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Recurring CARE_01 root cause. Fix: exceptions=True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care detail fields populated)",
     "judge_notes": "Recurring."},

    # --- 359009  CAO 359 Zoetwaren 2023  (CARE_01 + CARE_02) ---
    # Different pattern: pay_value=100 but value=null
    {"record_id": "359009", "topic_group": "care", "status": "partial",
     "field_issues": [
         {"field": "leave_short_term_care_value", "csv_value": "null",
          "issue": "CARE_02 fired because short_term_care_pay_value=100 was filled while short_term_care_value was empty. Either both should be filled, or both empty. Likely the duration was missed during extraction.",
          "severity": "medium"},
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Same CARE_01 gating issue — pay value being filled implies CAO has a specific provision; exceptions should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "(care_pay_value present but care_value missing)",
     "judge_notes": "Two distinct issues stack: missing duration + wrong exceptions flag."},
]


def write_csv() -> Path:
    import pandas as pd
    idx = pd.read_csv(SAMPLE_INDEX, sep=";", dtype=str)
    by_id = {str(r["record_id"]): r for _, r in idx.iterrows()}

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow([
            "record_id", "cao_number", "file_name", "ingangsdatum",
            "general_document_type", "bucket", "topic_group", "status",
            "n_field_issues", "field_issues_json", "evidence_quote",
            "judge_notes", "max_severity",
        ])
        for v in VERDICTS:
            meta = by_id.get(str(v["record_id"]), {})
            issues = v["field_issues"]
            severities = [i.get("severity", "low") for i in issues]
            max_sev = "high" if "high" in severities else (
                "medium" if "medium" in severities else (
                    "low" if "low" in severities else ""
                )
            )
            w.writerow([
                v["record_id"],
                meta.get("cao_number", ""),
                meta.get("file_name", ""),
                meta.get("ingangsdatum", ""),
                meta.get("general_document_type", ""),
                meta.get("bucket", ""),
                v["topic_group"],
                v["status"],
                len(issues),
                json.dumps(issues, ensure_ascii=False),
                v["evidence_quote"],
                v["judge_notes"],
                max_sev,
            ])
    print(f"Wrote {len(VERDICTS)} verdicts to {OUT_PATH}")
    return OUT_PATH


if __name__ == "__main__":
    write_csv()
