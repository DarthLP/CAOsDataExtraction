"""qa/full_audit/ — broad, dataset-internal QA audit across ALL records/fields.

Unlike the standard pipeline (which source-verified only the latest version of
each of the 95 curated CAOs), this module surfaces likely mistakes and outliers
across all 2,739 records and all 317 columns of
`inputs/extracted_data_non_salary.csv`, WITHOUT requiring source text for the
flagging stage. Every output is a FLAG (candidate for human review) — nothing is
auto-fixed, nothing is written back to the dataset.

Four checks (one script each) + a combiner:
  1. outliers_numeric.py          — statistical outliers on numeric fields
  2. cross_version.py             — same-CAO cross-version consistency
  3. enum_format.py               — enum/format/date/cross-field validity
  4. value_unit_contamination.py  — value<->unit contamination & swaps
  combine.py                      — dedup + prioritize -> full_audit_flags.csv

See FULL_DATASET_AUDIT_PROMPT.md (task spec) and qa/PLAN.md.
"""
