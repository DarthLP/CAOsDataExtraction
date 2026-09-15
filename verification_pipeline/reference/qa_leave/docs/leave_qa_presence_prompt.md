# Topic-Presence Check Rubric (narrow scope)

**Version:** v1 (2026-04-29)
**Used by:** subagents running the topic-presence sweep on every chunk in `qa_leave/outputs/subagent_chunks/chunk_*.jsonl`

This rubric is intentionally narrow. You are NOT judging whether CSV values are correct against the source. You are answering two binary questions per (record × topic_group):

1. **`topic_in_source`** — does the source_text discuss this topic? (yes / no)
2. **`csv_has_data`** — are any CSV fields in this topic group populated above their defaults? (yes / no)

From these two booleans we derive `presence_status`:

| topic_in_source | csv_has_data | presence_status | meaning |
|---|---|---|---|
| yes | yes | `discussed`         | Topic is in source and CSV has data — defer deep judgment to the human-judged sample. |
| yes | no  | `discussed_csv_empty` | Topic is in source but CSV is at defaults. Likely a missed extraction; flag for review. |
| no  | yes | `unsupported_csv`   | Topic is NOT in source but CSV has data. Possible hallucination, or p4 read directly from PDF. |
| no  | no  | `not_applicable`    | Topic absent from both. No-op record. |

## How to detect `topic_in_source`

The source_text uses natural-language English section labels, NOT the topic_group token. Use this mapping. If ANY phrase in the row appears in source_text (case-insensitive), set `topic_in_source = yes`.

| topic_group | Source phrases (any one match → yes) |
|---|---|
| `general` | `"general leave enhancements"`, `"minimum-cao"`, `"minimum regulation"`, `"minimum provision"` |
| `maternity` | `"maternity leave"`, `"zwangerschap"`, `"bevallingsverlof"`, `"pregnancy leave"`, `"pregnancy and maternity"` |
| `paternity` | `"paternity"`, `"partner leave"`, `"geboorteverlof"`, `"birth leave"`, `"WIEG"`, `"aanvullend geboorteverlof"`, `"birth of"` (with childbirth context) |
| `adoption` | `"adoption"`, `"foster"`, `"adoptieverlof"`, `"pleegzorgverlof"` |
| `parental` | `"parental leave"`, `"ouderschapsverlof"` |
| `sick` | `"sickness"`, `"sick pay"`, `"incapacity for work"`, `"loondoorbetaling"`, `"WGA"`, `"IVA"`, `"first year of sickness"`, `"second year of"` (sickness context) |
| `care` | `"care leave"`, `"zorgverlof"`, `"short-term care"`, `"long-term care"`, `"kortdurend"`, `"langdurend"`, `"terminal care"`, `"end-of-life"`, `"calamiteit"`, `"kort verzuim"` |
| `vacation_holidays` | `"vacation"`, `"holiday allowance"`, `"vakantie"`, `"vakantiegeld"`, `"vakantietoeslag"`, `"Liberation Day"`, `"public holiday"`, `"feestdag"` |
| `seniority_special` | `"special leave"`, `"seniorendagen"`, `"senior days"`, `"extra leave for older"`, `"extra vacation for older"`, `"functioneel leeftijdsontslag"`, `"age-based"`, `"jubilee"` (in service-anniversary context) |

If multiple phrases match, that still counts as a single yes. If none match, `topic_in_source = no`.

## How to detect `csv_has_data`

For each topic group, look at every CSV field in `csv_leave_fields[topic_group]`. The field is "populated above default" if:

- For boolean flag fields (`*_present`, `*_explicitly_above_statutory`, `*_statutory_ref`, `*_exceptions`, `*_eligibility_present`, `*_topup_present`, `*_extra_insurance_present`, `*_annual`, `*_lustrum`, `liberation_day_*`, `extra_seniority_present`, `hetero_present`, `has_leave_enhancements`, `has_above_statutory_maternity`, `paternity_explicitly_above_statutory`, `abortion_present`): value is `True` (case-insensitive). `False`, `null`, empty, or missing = NOT populated.
- For numeric `_value` fields, percent fields, and unit/string fields (`*_value`, `*_unit`, `*_note`, `*_schedule`, `*_comp_note`): value is non-null AND not the empty string after stripping. `null`, `""`, or missing = NOT populated.

`csv_has_data = yes` if ANY field in the topic group is populated above default. Otherwise `no`.

## Output format

For each record, emit exactly 9 rows (one per topic_group). 30 records × 9 = 270 rows per chunk.

CSV header (semicolon-separated, UTF-8):

```
record_id;cao_number;file_name;ingangsdatum;general_document_type;topic_group;topic_in_source;csv_has_data;presence_status;matched_phrases;populated_fields
```

- `topic_in_source`, `csv_has_data`: literal `yes` or `no`
- `presence_status`: one of `discussed`, `discussed_csv_empty`, `unsupported_csv`, `not_applicable`
- `matched_phrases`: pipe-separated list of source phrases that matched (or empty)
- `populated_fields`: pipe-separated list of CSV field names that are populated above default (or empty)

Use Python's `csv` module to write — never manually concatenate strings. Quote any field containing semicolons or newlines.

## Discipline

- Always emit 9 rows per record. No skipping, no deduplication.
- Do NOT add extra columns.
- Do NOT make judgments about correctness. That's not your job here.
- Do NOT include reasoning in the CSV.

## Final check before writing

Before writing the CSV, verify on a single record that you got exactly 9 rows for it and that `presence_status` is consistent with the four-cell table at the top of this rubric.
