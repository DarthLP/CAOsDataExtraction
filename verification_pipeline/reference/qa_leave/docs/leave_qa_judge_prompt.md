# Leave-information QA Judge Rubric

**Version:** v2 (2026-04-29) — tightened after smoke-test on chunk_000 produced 101 false-positive "unsupported" verdicts.

**Used by:** subagents running the full Layer-2 sweep on `qa_leave/outputs/leave_qa_payloads.jsonl`

This document is the rubric. It defines the verdict format, the topic groups, and the calibration rules learned from the 9-record pilot. Subagents must follow it exactly.

## CRITICAL — read the full source_text first

The `source_text` field uses **natural-language section labels in English**, not the topic_group tokens. Before judging, read the full source_text and map sections to topic groups using the table below. Do NOT do a literal string match on the topic_group token.

| topic_group | Source phrases that map to it (any one of these means the topic IS discussed) |
|---|---|
| `general` | `"general leave enhancements"`, `"minimum-cao"`, opening clauses about leave-rule scope or hierarchy |
| `maternity` | `"maternity leave"`, `"zwangerschapsverlof"`, `"bevallingsverlof"`, `"pregnancy"`, references to UWV maternity benefit. **NOTE:** Dutch sources sometimes mislabel paternity as "maternity"; read carefully. |
| `paternity` | `"paternity"`, `"partner leave"`, `"geboorteverlof"`, `"birth leave"`, `"WIEG"`, `"aanvullend geboorteverlof"` |
| `adoption` | `"adoption"`, `"foster"`, `"adoptieverlof"`, `"pleegzorgverlof"` |
| `parental` | `"parental leave"`, `"ouderschapsverlof"` |
| `sick` | `"sickness"`, `"sick pay"`, `"incapacity for work"`, `"loondoorbetaling bij ziekte"`, `"WGA"`, `"IVA"` |
| `care` | `"care leave"`, `"zorgverlof"`, `"short-term care"`, `"long-term care"`, `"kortdurend zorgverlof"`, `"langdurend zorgverlof"`, `"terminal care"`, `"end-of-life care"`, `"calamiteitenverlof"` |
| `vacation_holidays` | `"vacation"`, `"holiday allowance"`, `"vakantie"`, `"vakantiegeld"`, `"vakantietoeslag"`, `"Liberation Day"`, public-holiday tables |
| `seniority_special` | `"special leaves"`, `"seniorendagen"`, `"senior days"`, `"extra leave for older employees"`, `"functioneel leeftijdsontslag"`, age- or tenure-banded extra vacation rules |

If a source phrase from the table appears in source_text → the topic IS discussed → status cannot be `not_applicable`.

---

## Task

For every record in your assigned chunk you receive:

- `record_id`, `cao_number`, `file_name`, `ingangsdatum`, `general_document_type`
- `source_text`: the p3 narrative block extracted from `leave_information.md` for this CAO file
- `csv_leave_fields`: the structured fields p4 produced for this same record, grouped by topic

You must produce **one verdict per topic group, per record** — exactly 8 verdicts per record. The eight topic groups are fixed and listed below. Even if a topic is silent in both source and CSV, you still emit a verdict (status = `not_applicable`).

## Verdict format (CSV, one row per verdict)

```
record_id;cao_number;file_name;ingangsdatum;general_document_type;topic_group;status;n_field_issues;field_issues_json;evidence_quote;judge_notes;max_severity
```

Field meanings:

- `topic_group`: one of `general`, `maternity`, `paternity`, `adoption`, `parental`, `sick`, `care`, `vacation_holidays`, `seniority_special` — exactly these tokens.
- `status`: one of:
  - `supported` — every CSV field in this group is grounded in the source
  - `partial` — some fields supported, others wrong / missing / oversimplified
  - `contradicted` — at least one CSV value directly conflicts with the source
  - `unsupported` — CSV values appear but are not findable anywhere in the source (rare)
  - `not_applicable` — source is silent on this topic AND CSV is correctly empty/false
- `n_field_issues`: integer count of items in `field_issues_json`
- `field_issues_json`: a JSON array (possibly empty) of `{"field": str, "csv_value": str, "issue": str, "severity": "high"|"medium"|"low"}`
  - Use `field` names exactly as they appear in the CSV (e.g., `leave_paid_maternity_value`).
  - `severity = high` for factual errors and clear contradictions.
  - `severity = medium` for oversimplification, lossy capture, unit/format issues.
  - `severity = low` for stylistic / debatable.
- `evidence_quote`: a short verbatim snippet from the source that supports your verdict. If the source is silent, use `(topic not discussed)`.
- `judge_notes`: free-text explanation, max ~250 chars.
- `max_severity`: highest severity among `field_issues_json`, or empty string if no issues.

CSV must be **semicolon-separated**, UTF-8, with the exact header row above. JSON inside `field_issues_json` must be a single line.

## Topic-group → CSV field mapping

| topic_group | CSV fields covered |
|---|---|
| `general` | `leave_has_leave_enhancements`, `leave_hetero_present`, `leave_note` |
| `maternity` | `leave_has_above_statutory_maternity`, `leave_paid_maternity_*`, `leave_partially_paid_maternity_*`, `leave_partially_paid_maternity_pay_*`, `leave_unpaid_maternity_*`, `leave_maternity_note` |
| `paternity` | `leave_paternity_explicitly_above_statutory`, `leave_paid_paternity_*`, `leave_partially_paid_paternity_*`, `leave_partially_paid_paternity_pay_*`, `leave_unpaid_paternity_*` |
| `adoption` | `leave_adoption_*`, `leave_adoption_pay_*` |
| `parental` | `leave_parental_statutory_ref`, `leave_parental_exceptions`, `leave_parental_eligibility_present`, `leave_parental_min_contract_length_*`, `leave_parental_min_tenure_*`, `leave_parental_note`, `leave_parental_topup_present`, `leave_parental_topup_pay_*`, `leave_parental_unpaid_*`, `leave_abortion_present` |
| `sick` | `leave_sick_topup_present`, `leave_sickpay_duration_*`, `leave_sickpay_continuation_*`, `leave_sickpay_extra_insurance_present` |
| `care` | `leave_care_statutory_ref`, `leave_care_exceptions`, `leave_care_topup_present`, `leave_short_term_care_*`, `leave_short_term_care_pay_*`, `leave_long_term_care_*`, `leave_long_term_care_pay_*` |
| `vacation_holidays` | `leave_vacation_time_*`, `leave_vacation_bonus_*`, `leave_liberation_day_annual`, `leave_liberation_day_lustrum`, `leave_liberation_day_comp_note` |
| `seniority_special` | `leave_extra_seniority_present`, `leave_extra_seniority_schedule` |

---

## Calibration rules (learned from the pilot)

These are the rules where the LLM extractor is consistently weakest. Apply them strictly.

### R1 — `hetero_present` requires worker GROUPS, not age cohorts

Set verdict to `partial` and add a `medium`-severity field issue if `leave_hetero_present = True` but the source describes only:

- age-based seniority bands (e.g., 18–54 / 55–59 / 60+)
- pre/post-effective-date cohorts of the same worker type
- full-time vs part-time

True heterogeneity requires distinct worker groups: bouwplaats vs UTA, management vs OOP, uitzendbeding vs no-uitzendbeding, office vs field, etc. Age splits go in `seniority_special.extra_seniority_*`.

### R2 — Tiered sick-pay handling

When the source describes pay tiers across multiple periods (e.g. 100% wks 1–26, 90% wks 27–52, 80% wks 53–78, 70% wk 78+):

- `leave_sickpay_duration_value` should equal the **total topup horizon** — i.e., the last period in which pay is above statutory 70%. For NL CAOs this is typically 104 weeks or 24 months when at least one tier exceeds 70%.
- `leave_sickpay_continuation_value` may capture the first-tier rate (e.g., 100). This is acceptable — but flag `partial` with `medium` severity noting the tiered structure was lost.
- `leave_sick_topup_present` MUST be `True` if any tier exceeds 70%.

If `sick_topup_present = False` and any single tier > 70%, that's a **`high`-severity contradiction**, not a partial.

### R3 — Paternity / partner's leave (WIEG)

When the source says "additional birth leave for X weeks, unpaid by employer but with UWV benefit":

- The X weeks belong in `partially_paid_paternity_value` with `partially_paid_paternity_pay_value = 70` (UWV rate).
- The X weeks must NOT also appear in `unpaid_paternity_value`. Same period in both fields = `high`-severity duplicate.
- If the CAO supplements UWV to 100%, `partially_paid_paternity_pay_value = 100` and `paternity_explicitly_above_statutory = True`.

### R4 — Care-leave field discipline

Two-rule system:

- **R4a**: If source describes a CAO-specific care provision (a number, a pay rate, or extended scope of relatives), `leave_care_exceptions` MUST be `True`. If `False` while detail fields are filled = `medium`-severity contradiction.
- **R4b**: If `leave_care_statutory_ref = True` AND `leave_care_exceptions = False`, all of `short_term_care_*` and `long_term_care_*` MUST be null. Filled detail fields = `high`-severity contradiction.

Terminal-care leave (10 days for end-of-life care of relatives) maps to `short_term_care_*`, NOT `long_term_care_*` — because it's paid (typically 100%) and within a 12-month horizon. Long-term care (langdurig zorgverlof) is the unpaid 6×weekly-hours statutory leave.

### R5 — Window vs duration

Some CAOs say "X weeks of adoption leave, to be taken within Y weeks of the child's arrival" — Y is the **window**, X is the **duration**. The duration goes in the `_value` field; the window goes in a note or is dropped. Common error: `adoption_value = 26 weeks` when the source actually means "6 weeks spread over a 26-week window".

### R6 — Education-sector vacation

School-sector CAOs (CAO 1188 Voortgezet Onderwijs, similar) split vacation into "statutory" and "supplementary" (or "above-statutory") lines, often by work-week size (36 / 38 / 40 hours). The `vacation_time_value` should reflect the **total** entitlement (statutory + supplementary), not just the statutory line. Capturing only the statutory line is a `high`-severity undercount.

### R7 — Pay rate vs duration field-role

`leave_paid_maternity_value` is a **duration** (in weeks). `leave_partially_paid_maternity_pay_value` is a **rate** (in %). A pay rate landing in a duration field = `high`-severity field-role confusion.

### R8 — `adoption_pay_value` semantics

When source says "employer doesn't pay; employee can apply for UWV benefit": the conservative capture is `adoption_pay_value = null` with a note that UWV pays. Setting it to `0.0` is misleading because it implies the employee receives nothing.

### R9 — `parental_unpaid_value` semantics

When the CAO grants total parental leave of T hours, of which P hours are paid at some %: `parental_unpaid_value` is **T - P**, not T. Capturing the total entitlement misrepresents how much of the leave is actually unpaid.

### R10 — Below-statutory clauses

If the CAO grants something explicitly *below* statute (e.g., 1 day paternity in a year when statute was 2), do NOT mark `paternity_explicitly_above_statutory = True`. But DO add a `low`-severity field issue noting "appears below statutory minimum" so the human reviewer can check whether the CAO is actually superseded by statute (CAOs cannot deviate downward from mandatory statutory minimums).

---

## Status decision tree (READ THIS BEFORE JUDGING)

For each (record, topic_group):

1. Does the source_text discuss this topic? Use the source-phrase table above. If YES → go to step 2. If NO → go to step 3.

2. **Source DOES discuss the topic.** Compare every CSV field in the topic group to the source:
   - All CSV values supported and complete → `supported`.
   - Most CSV values supported, some missing / oversimplified / weakly-supported → `partial`.
   - At least one CSV value directly conflicts with source (e.g., wrong number, wrong field, factual error) → `contradicted`.

3. **Source is SILENT on the topic.** Now look at the CSV fields:
   - All boolean fields are `False` or `null`, all `_value` / `_unit` / `_note` fields are `null` or empty → `not_applicable`. Default booleans (`False`) and empty fields are NOT evidence of CSV claims; they're absence of data, which matches the silent source.
   - At least one `_value` / `_unit` / `_note` field is non-empty, OR a boolean is `True` → `unsupported`. This is the only path to `unsupported`. List the non-empty fields in `field_issues_json` with `severity = high`.

### Boolean defaults (very important)

These boolean fields default to `False` and that default is NOT a positive claim:
- `leave_has_above_statutory_maternity`, `leave_paternity_explicitly_above_statutory`
- `leave_parental_statutory_ref`, `leave_parental_exceptions`, `leave_parental_eligibility_present`, `leave_parental_topup_present`, `leave_abortion_present`
- `leave_sick_topup_present`, `leave_sickpay_extra_insurance_present`
- `leave_care_statutory_ref`, `leave_care_exceptions`, `leave_care_topup_present`
- `leave_liberation_day_annual`, `leave_liberation_day_lustrum`
- `leave_extra_seniority_present`, `leave_hetero_present`, `leave_has_leave_enhancements`

When source is silent and these are `False` → that's the DEFAULT, not an unsupported claim. → `not_applicable`.

When source is silent and any of these are `True` → that's an unsupported claim. → `unsupported` with `severity=high` for the offending field.

### Worked examples

Suppose the source_text contains `"vacation and holiday allowance: ... 25 days ... 8% holiday allowance"` and the CSV has `vacation_time_value=25, vacation_time_unit='days', vacation_bonus_value=8, vacation_bonus_unit='percent', liberation_day_lustrum=False, liberation_day_annual=False`.

→ Topic IS discussed (source phrase "vacation and holiday allowance" matches `vacation_holidays`). All values supported. The `False` Liberation Day flags are defaults — defensible since source doesn't mention Liberation Day. **Status: `supported`.**

Suppose the source_text contains nothing about adoption and the CSV has `adoption_value=null, adoption_unit=null, adoption_pay_value=null, adoption_pay_unit=null`.

→ Topic NOT discussed. All CSV fields empty. **Status: `not_applicable`.**

Suppose the source_text contains nothing about adoption but the CSV has `adoption_value=6, adoption_unit='weeks'`.

→ Topic NOT discussed. CSV has a non-empty value. **Status: `unsupported`** with field issue on `leave_adoption_value` (`severity=high`, csv_value=6, issue="Value present in CSV but adoption is not mentioned in the source text. p4 may have read this from the PDF outside the leave_information section, or hallucinated. Flag for human review.").

Suppose the source_text says `"sickness: 100% of fixed wage during the first year of sickness, 70% during the second year"` and the CSV has `sick_topup_present=False, sickpay_duration_value=null, sickpay_continuation_value=null`.

→ Topic IS discussed. Source clearly describes a topup (100% in year 1 vs statutory 70%). CSV missed it. **Status: `contradicted`** with field issues on `leave_sick_topup_present` and `leave_sickpay_duration_value` (severity=high).

## Other edge cases

- **CSV value is empty/null where source has clear content** → flag a field issue with `severity = high` and `csv_value = "null"`.
- **Conflicting tiered structures (e.g., sick pay)** → status = `partial`, not `contradicted`, when the structured field captures the first tier truthfully even though it loses the multi-tier nature.
- **`leave_note` notes**: when a number is missing from a structured field but present verbatim in `leave_note`, that's still a `partial` for the structured field — note normalization is part of the QA goal.

---

## Output discipline

- Emit verdicts in the input chunk's record order.
- Always emit exactly 8 verdicts per record (one per topic group). No skipping.
- Keep `evidence_quote` short (≤ 200 chars). Use `(topic not discussed)` placeholder if no quote.
- Keep `judge_notes` ≤ 250 chars.
- Do not include any commentary outside the CSV.
- The first row of your output is the exact header in the section above.
