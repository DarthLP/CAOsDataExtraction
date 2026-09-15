"""
Pilot verdicts produced by Claude (judge model) on 9 hand-picked records.
This file is the human-readable, reviewable record of the pilot.

To regenerate verdict_csv from this file:
    python _pilot_verdicts.py

Status taxonomy:
  supported       - all CSV fields in this topic group are grounded in the source text
  partial         - some fields supported, others wrong / missing / oversimplified
  contradicted    - one or more CSV values directly conflict with the source
  unsupported     - CSV values cannot be found anywhere in the source text
  not_applicable  - source is silent on this topic and CSV is correctly empty/false

Severity for individual field issues:
  high   - factual error or strong contradiction
  medium - oversimplification, lossy capture, or unit/format issue
  low    - debatable / stylistic
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# VERDICTS  (one entry per (record_id, topic_group))
# ---------------------------------------------------------------------------

VERDICTS: list[dict] = [

    # ============= 50010  CAO 50 Apotheken 2021-2024 (full_cao_original) =============
    {"record_id": "50010", "topic_group": "general", "status": "partial",
     "field_issues": [{"field": "leave_hetero_present", "csv_value": "True",
                       "issue": "Source does not describe distinct worker groups with different leave entitlements; only age-based seniority differences exist (which belong in extra_seniority).",
                       "severity": "medium"}],
     "evidence_quote": "(no group-level differentiation in source)",
     "judge_notes": "has_leave_enhancements=True is supported (Article 45 explicitly enhances statute). hetero_present=True is unsupported."},

    {"record_id": "50010", "topic_group": "maternity", "status": "supported",
     "field_issues": [], "evidence_quote": "(maternity not discussed)",
     "judge_notes": "Source is silent on maternity beyond statutory; all CSV maternity fields correctly empty/false."},

    {"record_id": "50010", "topic_group": "paternity", "status": "supported",
     "field_issues": [],
     "evidence_quote": "paid leave equal to once the average contractual working hours per week ... additional birth leave for five times the average contractual working hours per week ... supplements the statutory benefit to 100% of the gross monthly salary",
     "judge_notes": "1 week paid + 5 weeks partially paid at 100% topup all correctly captured. paternity_explicitly_above_statutory=True is supported by the explicit 100% top-up clause."},

    {"record_id": "50010", "topic_group": "adoption", "status": "supported",
     "field_issues": [],
     "evidence_quote": "ten times the average contractual working hours per week ... supplements the statutory benefit to 100% of the gross monthly salary",
     "judge_notes": "10 weeks at 100% supported."},

    {"record_id": "50010", "topic_group": "parental", "status": "supported",
     "field_issues": [], "evidence_quote": "(parental leave not in source)",
     "judge_notes": "Source has no parental leave block (Bouw-style p3 omission risk). All CSV fields empty/False is internally consistent."},

    {"record_id": "50010", "topic_group": "sick", "status": "contradicted",
     "field_issues": [
         {"field": "leave_sickpay_duration_value", "csv_value": "26.0",
          "issue": "Source describes a tiered sick-pay schedule running 100%/90%/80%/70% across 26+26+26+26 weeks until end of wage obligation (~104 weeks). 26 weeks captures only the first (100%) tier, not the topup duration.",
          "severity": "high"},
         {"field": "leave_sickpay_continuation_value", "csv_value": "100.0",
          "issue": "Tiered structure (100/90/80/70) lost; 100% applies only to weeks 1-26.",
          "severity": "medium"},
     ],
     "evidence_quote": "100% for weeks 1-26, 90% for weeks 27-52, 80% for weeks 53-78, and 70% from week 78 until the end of the wage payment obligation",
     "judge_notes": "Compare with same-CAO record 50011 which captured 104 weeks correctly — strong cross-record inconsistency from same source."},

    {"record_id": "50010", "topic_group": "care", "status": "supported",
     "field_issues": [],
     "evidence_quote": "twice the average contractual working hours per week within a one-year period",
     "judge_notes": "short_term_care=2 weeks at 100% pay supported. care_exceptions=True defensible (extends scope of relatives covered)."},

    {"record_id": "50010", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "180 vacation hours per year ... holiday allowance is 8% of the monthly salary",
     "judge_notes": "180 hr/yr and 8% supported. Liberation Day not in source → both flags False, defensible."},

    {"record_id": "50010", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [], "evidence_quote": "(no age/tenure-based extra leave in source)",
     "judge_notes": "extra_seniority_present=False supported."},

    # ============= 331004  CAO 331 GIL 2010-2012 (full_cao_update) =============
    {"record_id": "331004", "topic_group": "general", "status": "partial",
     "field_issues": [{"field": "leave_hetero_present", "csv_value": "True",
                       "issue": "Source describes age-based extra days (40/45/50/55/60) and one function-group bonus, but no major worker group differentiation; this is seniority, not heterogeneity.",
                       "severity": "low"}],
     "evidence_quote": "1 day at 40, 2 days at 45, 3 days at 50, 4 days at 55, and 5 days at 60",
     "judge_notes": "has_leave_enhancements=True supported (extra-statutory rules across multiple categories)."},

    {"record_id": "331004", "topic_group": "maternity", "status": "supported",
     "field_issues": [],
     "evidence_quote": "(source's 'maternity leave' line actually describes paternity)",
     "judge_notes": "Source mislabels paternity as 'maternity leave' but CSV correctly leaves maternity fields empty (the model didn't fall for the mislabel)."},

    {"record_id": "331004", "topic_group": "paternity", "status": "partial",
     "field_issues": [{"field": "leave_paid_paternity_unit", "csv_value": "days plus duration of delivery",
                       "issue": "Unit string is unusual / not in any expected format; better captured as 2.0 unit='days' with the 'duration of delivery' caveat moved to a note field.",
                       "severity": "low"}],
     "evidence_quote": "leave for the duration of the delivery plus two days thereafter",
     "judge_notes": "Value 2.0 days captures the durable component correctly; only the unit string is non-canonical."},

    {"record_id": "331004", "topic_group": "adoption", "status": "supported",
     "field_issues": [], "evidence_quote": "In case of adopting a child, 4 weeks of leave are granted.",
     "judge_notes": "4 weeks supported. adoption_pay null is defensible (source doesn't specify pay)."},

    {"record_id": "331004", "topic_group": "parental", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Employees with at least 1 year of service are entitled to unpaid parental leave for each child aged 0 to 8 years ... maximum of the weekly working hours over a period of 26 weeks",
     "judge_notes": "Tenure=1 year, unpaid=26 weeks, eligibility_present=True all supported. Note that this is a positive-tenure case (vs the 'less than X also entitled → 0' rule)."},

    {"record_id": "331004", "topic_group": "sick", "status": "partial",
     "field_issues": [{"field": "leave_sickpay_continuation_value", "csv_value": "100.0",
                       "issue": "Tiered (100% wks 1-26, 90% wks 27-52, 80%-90% year 2) collapsed to single 100% value; only first tier captured.",
                       "severity": "medium"}],
     "evidence_quote": "first 26 weeks ... 100% ... subsequent 26 weeks ... 90% ... second year ... 80% ... 90% if employee actively utilizes their remaining earning capacity",
     "judge_notes": "duration=2 years is the right total. extra_insurance=True correctly captures WGA-gap and 15-35% incapacity insurances."},

    {"record_id": "331004", "topic_group": "care", "status": "partial",
     "field_issues": [
         {"field": "leave_long_term_care_value", "csv_value": "10.0",
          "issue": "Source's '10 days for terminal phase care of first-degree relatives' is terminal/end-of-life care, distinct from statutory long-term care leave (langdurig zorgverlof, typically 6×weekly hours). Should arguably go in care_note rather than long_term_care.",
          "severity": "medium"},
         {"field": "leave_short_term_care_pay_value", "csv_value": "null",
          "issue": "Source explicitly grants short-term care 'with continued pay'; pay value should be 100%, not null.",
          "severity": "high"},
         {"field": "leave_long_term_care_pay_value", "csv_value": "null",
          "issue": "Same as above — terminal-phase care is paid in source but pay null in CSV.",
          "severity": "high"},
     ],
     "evidence_quote": "Short-term care leave for sick first-degree relatives ... is 10 days ... Care leave for first-degree relatives ... in their final life phase is 10 days",
     "judge_notes": "Pay rates missing despite explicit 'with continued pay' language."},

    {"record_id": "331004", "topic_group": "vacation_holidays", "status": "partial",
     "field_issues": [{"field": "leave_vacation_time_unit", "csv_value": "days of 7.2 hours",
                       "issue": "Unit string mixes vacation count and per-day hours; canonical 'days per year' with the 7.2hr-per-day specification moved to a note would match the schema's allowed pattern.",
                       "severity": "low"}],
     "evidence_quote": "Normal vacation entitlement is 25 days of an average of 7.2 hours per day",
     "judge_notes": "25 days, 8%, lustrum=True all supported. Only the unit token is non-canonical."},

    {"record_id": "331004", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [],
     "evidence_quote": "1 day at 40, 2 days at 45, 3 days at 50, 4 days at 55, and 5 days at 60",
     "judge_notes": "Schedule captures the age-based extras and the function-group bonus correctly."},

    # ============= 1060018  CAO 1060 NBBU Uitzendkrachten 2009-2013 (full_cao_original) =============
    {"record_id": "1060018", "topic_group": "general", "status": "supported",
     "field_issues": [],
     "evidence_quote": "(distinct rules for uitzendbeding / fixed-term / vacation workers)",
     "judge_notes": "hetero_present=True genuinely supported here (three distinct contract-type groups with different rules)."},

    {"record_id": "1060018", "topic_group": "maternity", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(maternity not discussed)",
     "judge_notes": "Source silent on maternity. All empty/false is correct."},

    {"record_id": "1060018", "topic_group": "paternity", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(paternity not discussed in this file)",
     "judge_notes": "Source has no paternity block; all empty/false correct."},

    {"record_id": "1060018", "topic_group": "adoption", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(adoption not discussed)", "judge_notes": ""},

    {"record_id": "1060018", "topic_group": "parental", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(parental not discussed)", "judge_notes": ""},

    {"record_id": "1060018", "topic_group": "sick", "status": "supported",
     "field_issues": [],
     "evidence_quote": "supplements this benefit to 90% of the daily wage for the first 52 weeks of incapacity for work",
     "judge_notes": "52 weeks at 90% supported. After that statutory rules take over → 52 is correct CAO-specific topup duration. extra_insurance=True supported (Sickness Benefits Act top-up insurance)."},

    {"record_id": "1060018", "topic_group": "care", "status": "supported",
     "field_issues": [],
     "evidence_quote": "according to the rules of the Work and Care Act ... twice the weekly working hours ... 70% of the wage but at least the applicable statutory minimum wage",
     "judge_notes": "All fields supported including statutory_ref=True and the 70% pay rate."},

    {"record_id": "1060018", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "16.00 hours of vacation for every fully worked month ... holiday allowance of 8% of the actual wage ... Liberation Day 2015",
     "judge_notes": "16 hours/month is unusual but correct for uitzendkrachten reservation model. Liberation Day 2015 mention triggers lustrum=True correctly."},

    {"record_id": "1060018", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [], "evidence_quote": "(no age/tenure-based extras)",
     "judge_notes": "extra_seniority_present=False correctly captured."},

    # ============= 50001  CAO 50 Apotheken 2014 (full_cao_original) =============
    {"record_id": "50001", "topic_group": "general", "status": "partial",
     "field_issues": [{"field": "leave_hetero_present", "csv_value": "True",
                       "issue": "No major worker groups in source; only age-based seniority. Same pattern as 50010/50011 (same CAO across years).",
                       "severity": "medium"}],
     "evidence_quote": "(no group-level differentiation)",
     "judge_notes": "Recurring hetero_present overcalling on this CAO."},

    {"record_id": "50001", "topic_group": "maternity", "status": "supported",
     "field_issues": [], "evidence_quote": "(maternity not discussed)",
     "judge_notes": "Source silent on maternity, CSV correctly empty/false."},

    {"record_id": "50001", "topic_group": "paternity", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Upon childbirth of the spouse, 10 days of special leave with salary continuation",
     "judge_notes": "10 paid days correctly captured. paternity_explicitly_above_statutory=True is supported (statute was 1 day in 2014; 10 days is materially above)."},

    {"record_id": "50001", "topic_group": "adoption", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(adoption not discussed)", "judge_notes": ""},

    {"record_id": "50001", "topic_group": "parental", "status": "supported",
     "field_issues": [], "evidence_quote": "(parental not discussed in source)",
     "judge_notes": "All empty/false. statutory_ref=False is technically correct relative to silent source — see global note about possible p3 omission risk."},

    {"record_id": "50001", "topic_group": "sick", "status": "partial",
     "field_issues": [{"field": "leave_sickpay_continuation_value", "csv_value": "100.0",
                       "issue": "Source has tiered pay (100/95/90/70 across 12+3+3+6 months); 100% captures only first tier. Tiered structure preserved in general.note but lost from structured field.",
                       "severity": "medium"}],
     "evidence_quote": "first 12 months 100% ... 13-15 month 95% ... 16-18 month 90% ... 19-24 month 70%",
     "judge_notes": "duration=24 months is correct total."},

    {"record_id": "50001", "topic_group": "care", "status": "supported",
     "field_issues": [],
     "evidence_quote": "10 days per year ... salary continuation",
     "judge_notes": "10 days/year at 100% supported."},

    {"record_id": "50001", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "172.8 vacation hours per year ... 8% of the monthly salary ... Liberation Day in every lustrum year (2010, 2015 etc.)",
     "judge_notes": "All supported including correct lustrum=True flag."},

    {"record_id": "50001", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [],
     "evidence_quote": "180 vacation hours per year if the employee is 45 years or older ... 187.2 ... 55 ... 194.4 ... 60",
     "judge_notes": "Schedule supported."},

    # ============= 50011  CAO 50 Apotheken 2021-2024 update (full_cao_update) =============
    {"record_id": "50011", "topic_group": "general", "status": "partial",
     "field_issues": [{"field": "leave_hetero_present", "csv_value": "True",
                       "issue": "Same overcalling as 50001/50010 — no major worker groups, only age cohorts (which belong in seniority).",
                       "severity": "medium"}],
     "evidence_quote": "(no group-level differentiation)", "judge_notes": ""},

    {"record_id": "50011", "topic_group": "maternity", "status": "supported",
     "field_issues": [],
     "evidence_quote": "If the employee does not perform her work due to maternity, childbirth, parental or birth leave, the average contractual working hours apply. No specific duration or pay levels beyond this are mentioned.",
     "judge_notes": "Empty fields correct; note captures the only maternity reference."},

    {"record_id": "50011", "topic_group": "paternity", "status": "supported",
     "field_issues": [],
     "evidence_quote": "once the average contractual working hours per week ... five times the average contractual working hours per week ... supplements the statutory benefit to 100% of the gross monthly salary",
     "judge_notes": "Same as 50010 — 1 week + 5 weeks @ 100% top-up. Internally consistent."},

    {"record_id": "50011", "topic_group": "adoption", "status": "supported",
     "field_issues": [],
     "evidence_quote": "ten times the average contractual working hours per week ... supplements the statutory benefit to 100%",
     "judge_notes": "10 weeks at 100% supported."},

    {"record_id": "50011", "topic_group": "parental", "status": "supported",
     "field_issues": [], "evidence_quote": "(no specific employer top-ups or duration)",
     "judge_notes": "Source explicitly says no specific employer top-ups or unpaid duration mentioned. All empty/false is consistent."},

    {"record_id": "50011", "topic_group": "sick", "status": "partial",
     "field_issues": [{"field": "leave_sickpay_continuation_value", "csv_value": "100.0",
                       "issue": "Tiered (100/90/80/70 across 26+26+26+26 weeks) — only first tier captured.",
                       "severity": "medium"}],
     "evidence_quote": "100% for weeks 1-26 ... 90% for weeks 27-52 ... 80% for weeks 53-78 ... 70% from week 78 until the end of the obligation",
     "judge_notes": "**Duration=104 weeks is correct here**, vs the 26 in 50010 which is the same CAO. Strong cross-record inconsistency flag for the QA layer."},

    {"record_id": "50011", "topic_group": "care", "status": "supported",
     "field_issues": [],
     "evidence_quote": "twice the average contractual working hours per week in a period of one year",
     "judge_notes": "2 weeks @ 100% supported."},

    {"record_id": "50011", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "180 vacation hours per year ... Holiday allowance is 8% ... Liberation Day (in each lustrum year: 2025, 2030, etc.)",
     "judge_notes": "All supported."},

    {"record_id": "50011", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Employees aged 55-59 on Dec 31, 2022, retain 187.2 hours/year. Employees aged 60 or older on Dec 31, 2022, retain 194.4 hours/year.",
     "judge_notes": "Schedule supported (transitional clauses captured)."},

    # ============= 1060002  CAO 1060 NBBU partial amendment 2023 (partial_amendment_of_latest) =============
    {"record_id": "1060002", "topic_group": "general", "status": "supported",
     "field_issues": [],
     "evidence_quote": "(uitzendbeding vs no uitzendbeding vs holiday workers - three distinct groups)",
     "judge_notes": "hetero_present=True correctly supported."},

    {"record_id": "1060002", "topic_group": "maternity", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(maternity not discussed)", "judge_notes": ""},

    {"record_id": "1060002", "topic_group": "paternity", "status": "partial",
     "field_issues": [{"field": "leave_partially_paid_paternity_value", "csv_value": "null",
                       "issue": "Source mentions birth leave 'for a period of four weeks' (window) and an Article 1:2 Wet Arbeid en Zorg additional leave. The structured CSV captures only the 1×weekly hours fully-paid portion; the WIEG additional 5×weekly hours is missing from partially_paid fields.",
                       "severity": "medium"}],
     "evidence_quote": "birth leave for a period of four weeks ... once the weekly working hours",
     "judge_notes": "1 week paid is right; statutory partner's leave (5 wks @ 70% via UWV) not captured."},

    {"record_id": "1060002", "topic_group": "adoption", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(adoption not discussed)", "judge_notes": ""},

    {"record_id": "1060002", "topic_group": "parental", "status": "not_applicable",
     "field_issues": [], "evidence_quote": "(parental not discussed)", "judge_notes": ""},

    {"record_id": "1060002", "topic_group": "sick", "status": "partial",
     "field_issues": [{"field": "leave_sickpay_continuation_unit", "csv_value": "% of daily wage for first 52 weeks",
                       "issue": "Unit string smuggles tier information into the unit, which won't normalize. Tiered (90% wks 1-52, 80% wks 53-104) better captured as continuation=90 + note describing the tier.",
                       "severity": "low"}],
     "evidence_quote": "90% of the daily wage for the first 52 weeks ... 80% for the 53rd to 104th week",
     "judge_notes": "duration=104 weeks correct; tiered structure information is in the unit field which is non-canonical."},

    {"record_id": "1060002", "topic_group": "care", "status": "partial",
     "field_issues": [{"field": "leave_short_term_care_value", "csv_value": "null",
                       "issue": "Source mentions 'short-term leave due to unforeseen circumstances' as a brief reference to calamity leave / kortverzuim. Statutory short-term care leave (Wet Arbeid en Zorg) is not mentioned in this partial amendment, so empty is defensible.",
                       "severity": "low"}],
     "evidence_quote": "short-term leave due to unforeseen circumstances requiring an immediate interruption of work, or due to very special personal circumstances",
     "judge_notes": "This partial amendment doesn't restate care leave; defensible to leave fields empty. care_exceptions=True is borderline since this amendment doesn't add care-specific exceptions."},

    {"record_id": "1060002", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "16 2/3 hours of vacation ... 8.33% holiday allowance ... Liberation Day in lustrum years",
     "judge_notes": "All supported. liberation_day_comp_note correctly captures the worked-day-of-week eligibility rule."},

    {"record_id": "1060002", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [], "evidence_quote": "(no age/tenure-based extras)", "judge_notes": ""},

    # ============= 10008  CAO 10 Bouw 2014 (full_cao_update) =============
    {"record_id": "10008", "topic_group": "general", "status": "supported",
     "field_issues": [],
     "evidence_quote": "(bouwplaats vs UTA - two distinct worker groups with different leave/seniority schedules)",
     "judge_notes": "hetero_present=True genuinely supported."},

    {"record_id": "10008", "topic_group": "maternity", "status": "supported",
     "field_issues": [], "evidence_quote": "maternity leave: Not explicitly mentioned",
     "judge_notes": "Source explicit 'not mentioned'; all empty/false correct. has_above_statutory_maternity=False is the right call (no enhancement)."},

    {"record_id": "10008", "topic_group": "paternity", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Employees have a right to 1 day of paid leave for the birth of their partner",
     "judge_notes": "1 day paid supported. paternity_explicitly_above_statutory=False defensible (in 2014 statutory was 2 days; this is BELOW statute, but the field's bar is 'explicitly above'). Consider whether 1 day is actually inferior to statute and worth flagging."},

    {"record_id": "10008", "topic_group": "adoption", "status": "supported",
     "field_issues": [], "evidence_quote": "adoption and foster leave: Not explicitly mentioned",
     "judge_notes": "All empty correctly."},

    {"record_id": "10008", "topic_group": "parental", "status": "partial",
     "field_issues": [{"field": "leave_parental_statutory_ref", "csv_value": "False",
                       "issue": "Source explicitly cites Wet Arbeid en Zorg for long-term care leave linked to terminal care. parental_statutory_ref should arguably be True (the reference is to long-term care under the same statute family).",
                       "severity": "low"}],
     "evidence_quote": "long-term care leave under the Work and Care Act (Wet Arbeid en Zorg)",
     "judge_notes": "The Wet Arbeid en Zorg reference is in the source but linked to care leave, not parental. Defensible interpretation either way."},

    {"record_id": "10008", "topic_group": "sick", "status": "contradicted",
     "field_issues": [
         {"field": "leave_sick_topup_present", "csv_value": "False",
          "issue": "Source says 100% in year 1 / 70% in year 2. Statutory baseline is 70%/70%, so 100% in year 1 IS an employer top-up. sick_topup_present=False is wrong.",
          "severity": "high"},
         {"field": "leave_sickpay_duration_value", "csv_value": "null",
          "issue": "Should be 104 weeks (or 2 years) given source covers years 1+2.",
          "severity": "high"},
         {"field": "leave_sickpay_continuation_value", "csv_value": "null",
          "issue": "Should be 100 (year 1) or capture tiered 100/70.",
          "severity": "high"},
         {"field": "leave_sickpay_extra_insurance_present", "csv_value": "False",
          "issue": "Source mentions WGA-insurance equivalent to bpfBOUW (Article 87) — extra_insurance_present should be True.",
          "severity": "high"},
     ],
     "evidence_quote": "100% wage payment during the first year of sickness ... 70% wage payment ... obliged to take out WGA-insurance",
     "judge_notes": "Multi-field contradiction. Highest-severity finding in the pilot."},

    {"record_id": "10008", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_short_term_care_value", "csv_value": "null",
          "issue": "Source's 10 days of paid terminal-care leave belongs in short_term_care (paid, ≤ 12 months horizon), not long_term_care.",
          "severity": "high"},
         {"field": "leave_long_term_care_value", "csv_value": "10.0",
          "issue": "Mapped 10 days to long_term_care_value. Statutory long-term care (langdurig zorgverlof) is typically 6×weekly hours unpaid; 10 days of paid terminal care is conceptually short-term care. Field misclassification.",
          "severity": "high"},
     ],
     "evidence_quote": "10 days of paid leave per twelve months ... for end-of-life care in the terminal phase",
     "judge_notes": "Care field misclassification. The 10 days is paid terminal-care leave, which the schema doesn't have a dedicated field for, but short_term_care is the closer match given pay=100% and ≤ 12-month horizon."},

    {"record_id": "10008", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "18 to 54 years | 20 | 5 | 0 | 25 ... 8% of the fixed agreed wage ... Liberation Day in lustrum years",
     "judge_notes": "25 days for typical worker (18-54), 8%, lustrum=True all supported."},

    {"record_id": "10008", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [],
     "evidence_quote": "55 to 59 years | 20 | 5 | 10 | 35 ... 60 years or older | 20 | 5 | 13 | 38",
     "judge_notes": "Schedule captures both bouwplaats and UTA tables correctly."},

    # ============= 1188002  CAO 1188 Education 2021 (full_cao_original) =============
    {"record_id": "1188002", "topic_group": "general", "status": "supported",
     "field_issues": [],
     "evidence_quote": "(management/teachers vs OOP - two distinct staff categories)",
     "judge_notes": "hetero_present=True genuinely supported (education sector splits)."},

    {"record_id": "1188002", "topic_group": "maternity", "status": "supported",
     "field_issues": [], "evidence_quote": "(only vacation-overlap rule mentioned)",
     "judge_notes": "Empty fields correct; only vacation-overlap rule captured in note."},

    {"record_id": "1188002", "topic_group": "paternity", "status": "partial",
     "field_issues": [
         {"field": "leave_unpaid_paternity_value", "csv_value": "5.0",
          "issue": "Same 5 weeks captured in BOTH leave_partially_paid_paternity_value AND leave_unpaid_paternity_value. Statutory partner's leave (WIEG) is paid 70% by UWV — should sit in partially_paid only, not duplicated under unpaid.",
          "severity": "high"},
         {"field": "leave_partially_paid_paternity_pay_value", "csv_value": "null",
          "issue": "WIEG partner's leave is UWV-paid at 70%; pay value should be 70 if partially_paid is being used.",
          "severity": "medium"},
     ],
     "evidence_quote": "supplementary birth leave for a maximum of 5 full weeks ... unpaid, but the employee is entitled to a benefit from the UWV",
     "judge_notes": "Common pattern: model duplicates the same period across partially_paid and unpaid fields when the source says 'unpaid but UWV pays'."},

    {"record_id": "1188002", "topic_group": "adoption", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Adoption of a child: 6 weeks. This also applies to employees taking in a foster child.",
     "judge_notes": "6 weeks supported. adoption_pay null is defensible (statutory UWV)."},

    {"record_id": "1188002", "topic_group": "parental", "status": "partial",
     "field_issues": [
         {"field": "leave_parental_unpaid_value", "csv_value": "830.0",
          "issue": "Source says total entitlement is 830 hours, of which up to 415 are paid at 55%. The unpaid portion is 415 hours, not 830. parental_unpaid conflates total entitlement with unpaid portion.",
          "severity": "high"},
     ],
     "evidence_quote": "maximum leave entitlement is 830 hours per child ... maximum of 415 hours per child ... can be paid ... retains 55% of their remuneration",
     "judge_notes": "Topup fields (present=True, pay=55%) correctly captured. Only parental_unpaid_value miscaptured."},

    {"record_id": "1188002", "topic_group": "sick", "status": "supported",
     "field_issues": [],
     "evidence_quote": "retains full remuneration for 12 months ... 70% of their remuneration ... IVA-uitkering ... up to 100% of their income",
     "judge_notes": "Duration 12 months captures the 100% top-up tier (defensible as 'topup duration'). extra_insurance=True correctly reflects the IVA supplement."},

    {"record_id": "1188002", "topic_group": "care", "status": "partial",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "care_exceptions=False AND statutory_ref=False, but the CSV has filled in care detail fields (2× weekly hours @ 100%). Source describes a CAO-specific paid short-term care provision — exceptions should be True.",
          "severity": "medium"},
     ],
     "evidence_quote": "Short-term care leave with pay is granted ... maximum of twice the weekly working hours in any 12-month period",
     "judge_notes": "Detail fields are correct (2 weeks @ 100%); only the gating booleans are inconsistent."},

    {"record_id": "1188002", "topic_group": "vacation_holidays", "status": "contradicted",
     "field_issues": [
         {"field": "leave_vacation_time_value", "csv_value": "160.0",
          "issue": "160 hours/year is the statutory minimum for OOP at 40hr/week ONLY. The actual entitlement (statutory + supplementary) for OOP is 160+266=426 hours at 40hr/week (or 152+170=322 at 38hr or 144+74=218 at 36hr). Captured value undercounts the worker's real entitlement.",
          "severity": "high"},
     ],
     "evidence_quote": "Statutory vacation leave for OOP is: 160 hours/year for a 40-hour work week ... Supplementary vacation leave for OOP is: 266 hours/year for a 40-hour work week",
     "judge_notes": "School-sector CAOs split statutory + supplementary lines; the schema's 'typical' value should be the total. lustrum=False is correct (Liberation Day not mentioned)."},

    {"record_id": "1188002", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Employees 57 years or older receive an additional leave budget of up to 120 hours per year",
     "judge_notes": "Schedule captures the 57+ provisions and saved-leave/sabbatsverlof correctly."},

    # ============= 41011  CAO 41 (full_cao_original 2021-2022) =============
    {"record_id": "41011", "topic_group": "general", "status": "supported",
     "field_issues": [],
     "evidence_quote": "(young employees + pre/post-2019 cohorts as worker categories)",
     "judge_notes": "hetero_present=True is borderline-defensible given young/older/transitional cohorts."},

    {"record_id": "41011", "topic_group": "maternity", "status": "contradicted",
     "field_issues": [
         {"field": "leave_paid_maternity_value", "csv_value": "100.0",
          "issue": "Value 100 with unit 'percent of daily wage' is a PAY RATE, not a duration. paid_maternity_value should be a duration (e.g. 16 weeks). The 100% should sit in partially_paid_maternity_pay_value (or a maternity_note) since maternity in NL is statutory 16 weeks at 100% UWV-paid.",
          "severity": "high"},
     ],
     "evidence_quote": "Employees receive 100% of their daily wage during maternity leave",
     "judge_notes": "Field/role confusion — pay rate stored in duration field. has_above_statutory_maternity=False is correct (100% is statutory, not enhancement)."},

    {"record_id": "41011", "topic_group": "paternity", "status": "partial",
     "field_issues": [
         {"field": "leave_unpaid_paternity_value", "csv_value": "5.0",
          "issue": "Same 5 weeks duplicated in partially_paid AND unpaid. Source says additional WIEG leave is unpaid by employer but UWV pays 70% — partially_paid is the right slot, not both.",
          "severity": "high"},
         {"field": "leave_paid_paternity_unit", "csv_value": "working week",
          "issue": "Should be normalized to 'weeks' or 'days'.",
          "severity": "low"},
     ],
     "evidence_quote": "1 working week of paternity leave ... five additional weeks of unpaid leave ... benefit from the UWV",
     "judge_notes": "Same duplication pattern as 1188002 — recurring extraction error."},

    {"record_id": "41011", "topic_group": "adoption", "status": "contradicted",
     "field_issues": [
         {"field": "leave_adoption_value", "csv_value": "26.0",
          "issue": "26 weeks is the WINDOW over which 6 weeks of adoption leave can be spread — not the duration of the leave itself. Should be 6 weeks.",
          "severity": "high"},
         {"field": "leave_adoption_pay_value", "csv_value": "0.0",
          "issue": "Source says 'Employer is not obliged to pay salary during this leave, as the employee can apply for a benefit'. UWV pays statutory benefit, so 0% from employer is technically correct but misleading; usually captured as null + note.",
          "severity": "low"},
     ],
     "evidence_quote": "full-time for a maximum of 6 consecutive weeks or spread over a maximum of 26 weeks",
     "judge_notes": "Window vs duration confusion."},

    {"record_id": "41011", "topic_group": "parental", "status": "supported",
     "field_issues": [],
     "evidence_quote": "maximum entitlement is 26 times the weekly working hours ... Unpaid",
     "judge_notes": "26×weekly hours unpaid supported. statutory_ref/exceptions/eligibility flags all True is reasonable since the source extends or restates statute."},

    {"record_id": "41011", "topic_group": "sick", "status": "supported",
     "field_issues": [],
     "evidence_quote": "100% of average gross salary ... for the first 12 months. Thereafter, 70% ... for a maximum of 12 months",
     "judge_notes": "duration=24 months total, continuation=100% in year 1. Tiered structure preserved in general.note. Defensible."},

    {"record_id": "41011", "topic_group": "care", "status": "contradicted",
     "field_issues": [
         {"field": "leave_care_exceptions", "csv_value": "False",
          "issue": "Source explicitly describes BOTH short-term care (2× weekly hours @ 70%) AND long-term care (6× weekly hours, unpaid) — care_exceptions should be True.",
          "severity": "high"},
         {"field": "leave_short_term_care_value", "csv_value": "null",
          "issue": "Source: 'Maximum duration is two times the weekly working hours per 12 months'. Should be 2× weekly hours.",
          "severity": "high"},
         {"field": "leave_short_term_care_pay_value", "csv_value": "null",
          "issue": "Source: 'Paid at 70% of salary'. Should be 70%.",
          "severity": "high"},
         {"field": "leave_long_term_care_value", "csv_value": "null",
          "issue": "Source: 'Maximum duration is six times the weekly working hours per 12 months'. Should be 6× weekly hours.",
          "severity": "high"},
     ],
     "evidence_quote": "short-term care leave: ... two times the weekly working hours per 12 months. Paid at 70% of salary ... long-term care leave: ... six times the weekly working hours per 12 months. Unpaid.",
     "judge_notes": "Whole care block missed despite explicit source. Highest-severity miss in this record."},

    {"record_id": "41011", "topic_group": "vacation_holidays", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Full-time employees accrue 25 vacation days per year ... 8% of the gross annual salary ... Liberation Day (5 May in 2020 and 2025)",
     "judge_notes": "All supported including correct lustrum=True call."},

    {"record_id": "41011", "topic_group": "seniority_special", "status": "supported",
     "field_issues": [],
     "evidence_quote": "Employees aged 55 or older ... 27 vacation days ... Employees aged 60 or older ... 30 vacation days",
     "judge_notes": "Schedule captures young/older/pre/post 2019 schedules."},
]


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------


def write_csv() -> Path:
    out = ROOT / "outputs" / "pilot" / "pilot_verdicts.csv"

    # Load index for record metadata
    import pandas as pd
    idx = pd.read_csv(ROOT / "outputs" / "leave_qa_payload_index.csv", sep=";", dtype=str)
    idx_by_id = {str(r["record_id"]): r for _, r in idx.iterrows()}

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            "record_id", "cao_number", "file_name", "ingangsdatum",
            "general_document_type", "topic_group", "status", "n_field_issues",
            "field_issues_json", "evidence_quote", "judge_notes",
            "max_severity",
        ])
        for v in VERDICTS:
            meta = idx_by_id.get(v["record_id"], {})
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
                v["topic_group"],
                v["status"],
                len(issues),
                json.dumps(issues, ensure_ascii=False),
                v["evidence_quote"],
                v["judge_notes"],
                max_sev,
            ])
    print(f"Wrote {len(VERDICTS)} verdicts to {out}")
    return out


if __name__ == "__main__":
    write_csv()
