# corrections.csv — schema reference

Each row represents one correction proposal for one (record_id, field) pair.

## Columns

| Column | Type | Description |
|---|---|---|
| record_id | str | The CSV row this correction applies to. |
| cao_number | str | The CAO this record belongs to. |
| original_field | str | The field being corrected. |
| topic_group | str | Topic family (e.g. overtime, pension). |
| csv_value_old | str | Original value in the CSV. |
| csv_unit_old | str | Original unit in the CSV. |
| verdict | enum | confirm / clear / correct_in_place / move / set_boolean / unable_to_verify |
| target_field | str | Set only when verdict=move. |
| csv_value_new | str | Corrected value, or "" for clear, or UNKNOWN. |
| csv_unit_new | str | Corrected unit. |
| evidence_quote | str | Verbatim source text supporting the verdict. |
| confidence | enum | high / medium / low |
| fix_method | enum | det+sub_agree / det_only / sub_only / det_sub_conflict / needs_human_review |
| failure_modes_referenced | str | Comma-separated FM IDs the subagent applied. |
| topic_section_was_truncated | bool | True if whole low-relevance passages were dropped by the slicer. Passed through from the worksheet item. |
| dropped_passages_summary | json-str | Serialized list of `{section, first_100_chars, anchors_matched}` for each dropped passage. Empty when not truncated. Lets a reviewer see what was set aside without re-running the slicer. |
| value_not_in_source | bool | True if CSV had a value but no variant of it was found in the source slice. Signals possible unit-mismatch, decimal-strip, or paraphrase. |
| is_noop | bool | True if cosmetic-only (filter out for analysis). |
| changed | enum | none / value / unit / both |
| notes | str | Free text (tier schedules, multi-value notes, etc.). |

## How to use this file

- For analysis: filter `is_noop=False`. The remaining rows are the real corrections.
- For human review: filter `fix_method=needs_human_review`. These are the audit-flagged outliers.
- For high-confidence corrections: filter `confidence=high AND fix_method=det+sub_agree`.
- For truncation-related concerns: filter `topic_section_was_truncated=true` —
  inspect `dropped_passages_summary` to judge whether the slicer set aside
  relevant content. A14 (high-confidence-on-truncated) and A15 (negative-on-truncated)
  audit hits land here.
- For value-anchor misses: filter `value_not_in_source=true` — signals possible
  decimal-strip, unit mismatch, or paraphrase the variant generator missed.
- `csv_value_new=""` (clear) means the original value was garbage and should be blanked.
- `csv_value_new=UNKNOWN` with `confidence=low` means the source did not allow a verdict — different from "" (clear).

## CSV format

- Delimiter: `;` (semicolon) — same as `inputs/extracted_data_non_salary.csv`.
- All multi-line text in cells must be properly escaped per the `resilient_csv` module.
