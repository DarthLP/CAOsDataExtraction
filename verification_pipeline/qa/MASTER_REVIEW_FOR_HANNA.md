# Master Review — every outstanding item across all 12 topics (for Hanna)

Generated 2026-05-27 by `consolidate_review.py` (re-runnable). Companion to
`MASTER_REVIEW_FOR_HANNA.csv` (**126 rows**, UTF-8 with BOM).

> **SUPERSEDED as the master to-do (2026-07-03):** the live list of remaining work is
> [`docs/ROADMAP.md`](../docs/ROADMAP.md). This file remains as the record of the May-2026 review round.

## Status: fully resolved AND applied

You accepted all FIX + SUGGEST and ruled on every DECIDE item, so the review has no open
judgment calls — all 126 rows are concrete edits. They have been **written into a corrected
copy of the dataset** (`corrected_dataset.csv`); `inputs/` is untouched.

| action | meaning | rows |
|---|---|---:|
| **CHANGE_VALUE** | replace a wrong/normalized value | 57 |
| **ADD_UNIT** | fill a blank `_unit` column (value is correct) | 33 |
| **ADD_VALUE** | fill an empty value field (incl. value-halves paired to a unit) | 32 |
| **CLEAR_VALUE** | remove an unsupported/impossible value | 4 |

Buckets: **FIX 35** (P1 7 · P2 28) · **SUGGEST 91** (P3 normalizations + your 29 DECIDE rulings).

## The corrected dataset (`corrected_dataset.csv`)
- **462 cells changed across 94 records** = the 126 reviewed edits + 336 accepted clean wins.
  **0 skips.** Companion logs: `apply_changelog.csv` (every old→new), `apply_skipped.csv` (empty).
  *(Update: a later apply_list re-run added the 66 `leave_source_review` + 4 `source_review_nonleave`
  rows, so the final Layer-1 changelog is **514 cells / 111 records**. Two further layers landed after
  this memo — full-audit promotion + per-file `FIX_clear`; see `docs/DATA_LINEAGE.md` for the current state.)*
- **Applied surgically (your "same CAO, different row" warning):** each change matched on the
  **unique row `id`** (never `cao_number`, which has up to 39 versioned rows per CAO), required
  exactly one matching row, and verified the base cell equalled the expected current value
  before writing — only the single named cell of that one row was touched.
- **Clean wins the full-text re-check rejected were excluded** (e.g. `contract/20002`
  conversion-rights & `573014` ketenregeling stayed `False`, not flipped to `True`); genuine
  presence wins (`shift_allowance_present`, …) did flip to `True`.

## P1 — the 7 genuine errors corrected
- **overtime/557002** `max_hours_per_week` 40 → **45**.
- **overtime/1287018** `unfavourable_hours_allowance` → **100** + `% of hourly rate`.
- **overtime/496024** `unfavourable_hours_allowance` → **10** + `% of hourly rate`.
- **pension/157017** & **/1287018** `accrual_rate` **100% → cleared** (value + unit) — source-verified misread (salary-continuation %, not a DB accrual rate).

## Your DECIDE rulings (29 rows, encoded in `DECISIONS{}`)
- **overtime/50012** unfavourable-hours → **150**, unit = total-pay rate (not surcharge).
- **6 training cost-reimbursement** "fully borne by employer" → **100 % of costs**.
- **training/301025** → **1.5 % of salary** (Career Budget accrual, not full coverage).
- **training/924016** career scan → **1 times per year**.
- **fringe/214021** relocation → **12 % of annual salary**.
- **term notice/probation (233017, 163011, 725023, 727036, 609002, 1618009)** → base notice tier / probation cap + `months` (read from source).
- **homeoffice/243025** → `employee_request`; **homeoffice/730012** → `other`.
- **bonus/163011** thirteenth-month → **value 1.15**, unit `"% of functional wage and allowances + EUR 400 fixed gross"` (compound amount: % stored as the value, the €400 fixed component preserved in the unit so nothing is lost).
- **Declined (left as-is):** overtime/157017 & /83014 weekends-off text, training/609002 budget (sector voucher, not per-employee), fringe/487015 meal-benefit (kept False), safety/1471012 workload-monitoring (kept False).

## Conventions confirmed with you
- **Value ↔ unit pairing (asymmetric):** a value is never added without a unit; a unit *may*
  stand alone without a value (formula bases like `pension_franchise_unit = "gross minimum
  wage plus holiday allowance"`). The 39 standalone-unit fields are left untouched.
- **No missing units (unlike leave):** a full value↔unit scan found **0** "value present, unit
  blank" across all 12 topics — the non-leave extractor paired them. Leave's 923 came from a
  different, older extraction.
- **Era outliers source-verified:** the 3 franchise "below floor" flags were false alarms
  (real values stated verbatim); `era_baselines.py` now annualizes monthly figures + uses a
  conservative floor. Only the 2 accrual=100% misreads remained (cleared). 93 tests pass.

## Files (all in `qa/`)
- **corrected_dataset.csv** — the deliverable: full dataset with the 462 edits applied
- **apply_changelog.csv** — every changed cell · **apply_list.csv** — the change manifest
- **MASTER_REVIEW_FOR_HANNA.csv** — the 126 reviewed edits (this doc's companion)
- Per topic: `qa_<topic>/outputs/{corrections.csv, needs_human_review.csv}`; plus
  `verify_changes/`, `second_pass_nhr/`, `qa_*/recheck/`, `qa_pension/holistic/`, era_outliers.
