# Full-Audit Summary

Dataset-internal QA audit of `inputs/extracted_data_non_salary.csv`.
**Surface-only**: every output is a FLAG for human review. Nothing was written
into the dataset, into `inputs/`, into `qa_leave/`, or into `CAOsDataExtraction/`.
All artifacts live under `qa/full_audit/`.

Generated: 2026-05-31.

---

## 1. Scope

| | |
|---|---|
| Records audited | **2,739** (every row) |
| Columns audited | **317** (every column) |
| Unique CAOs in file | 242 |
| Checks run | 5 structural + 1 holistic verification (Check 5 `unit_semantics` added 2026-06-02) |
| Flag rows produced | **4,778** over **1,949** records / **245** distinct fields |

The audit examines the *whole* file, not just the 95 curated in-scope CAOs.
In-scope records were verified against the curated `inputs/by_topic/*_information.md`
blocks; out-of-scope records, plus `general`/`meta` fields, were verified against the
read-only `CAOsDataExtraction/.../new_flow/<cao>/*_extract.json` English extracts.

---

## 2. Flag volume per check

Raw counts (pre-dedup), then folded into the combined file (a flag raised by two
checks on the same cell is merged into one row, attributed to both):

| Check | Raw flags | Real-issue rate† | Notes |
|---|---:|---:|---|
| `cross_version` (lineage OOM / odd-one-out) | 3,597 | 35.3 % | Largest source; compares each cell against its CAO's other versions. |
| `enum_format` (invalid enum / bad date / format) | 618 | **52.6 %** | Highest precision — an invalid enum is usually a real problem. |
| `outliers_numeric` → `numeric_outlier` (robust fence / scale slip) | 587 | 17.9 % | Noisiest, as expected — many statistical outliers are legitimate. |
| `outliers_numeric` → `era_baseline` (statutory-floor) | 91 | 22.0 % | Floor violations vs WAZO / AOW / minimum-wage baselines. |
| `value_unit_contamination` (value/unit swaps) | **0** | — | Fired on **nothing**; see caveat §7. |

† Real-issue rate = (NEEDS_CHANGE + UNSUPPORTED) ÷ verified flags for that check.
No check is a false-positive flood: even the noisiest (`numeric_outlier`, 17.9 %)
surfaces a real issue roughly one time in six, and enum violations land >50 %.

**Severity split** (combined): high 387 · medium 2,883 · low 1,508.

---

## 3. Verification verdict mix

All **high + medium** flags (3,270) were sent to holistic per-(topic, record)
verification — a subagent read the *complete* topic source section and judged
*every* populated field of that topic at once. Low-severity flags (1,508) are
out-of-scope for verification (format-only noise).

| Verdict | Count | Share of verified |
|---|---:|---:|
| CONFIRM (value supported) | 2,120 | 67.2 % |
| NEEDS_CHANGE (wrong, correction suggested) | 765 | 24.2 % |
| UNSUPPORTED (source does not support it) | 272 | 8.6 % |
| — verified subtotal — | **3,157** | 100 % |
| unverified_no_source (empty source section) | 113 | — |

**~32.8 % of verified flags are real problems** (NEEDS_CHANGE + UNSUPPORTED = 1,037),
the rest correctly CONFIRM cross-version encoding differences the heuristics misfired on.

`no_source`: 41 (topic, record) pairs (113 flag rows) had a genuinely empty source
section, so they cannot be verified by source and are left as `unverified_no_source`.

---

## 4. Per-topic breakdown

Flag counts and verified verdict mix by topic (verify-scope only):

| Topic | Flags | CONFIRM | NEEDS_CHANGE | UNSUPPORTED | issue % |
|---|---:|---:|---:|---:|---:|
| leave | 1,086 | 530 | 192 | 45 | 30.9 |
| overtime | 935 | 447 | 296 | 54 | 43.9 |
| pension | 569 | 171 | 31 | 86 | 40.6 |
| term | 529 | 248 | 63 | 36 | 28.5 |
| contract | 325 | 160 | 38 | 19 | 26.3 |
| fringe | 299 | 163 | 40 | 3 | 20.9 |
| training | 294 | 178 | 37 | 9 | 20.5 |
| bonus | 208 | 77 | 17 | 3 | 20.6 |
| general | 188 | 114 | 22 | 9 | 21.4 |
| safety | 166 | — | — | — | (mostly low-sev) |
| homeoffice | 75 | 14 | 24 | 5 | 67.4 |
| childcare | 44 | 16 | 5 | 3 | 33.3 |
| wage | 41 | — | — | — | (cross-topic field bleed) |
| ai | 17 | 1 | 0 | 0 | 0.0 |
| meta | 2 | 1 | 0 | 0 | 0.0 |

Highest issue density: `overtime` (43.9 %), `pension` (40.6 % — driven by UNSUPPORTED
contribution-rate values), `homeoffice` (67.4 %, small N).

---

## 5. Top fields by flag count

| Field | Flags | | Field | Flags |
|---|---:|---|---|---:|
| overtime_stacking_rule | 118 | | general_document_type | 64 |
| pension_retire_age_normal_unit | 110 | | leave_paid_maternity_value | 63 |
| leave_vacation_time_value | 106 | | leave_sickpay_duration_value | 62 |
| overtime_trigger_weekly_value | 89 | | overtime_allowance_range_max | 61 |
| fringe_meal_benefit_type | 80 | | training_budget_value | 60 |
| overtime_shift_allowance_range_min | 73 | | general_start_date | 60 |
| overtime_unfavourable_hours_allowance_value | 73 | | | |
| overtime_compulsory_annual_value | 68 | | | |
| fringe_commuting_allowance_value | 66 | | | |

---

## 6. New findings (off-flag)

Because verification judged *all* populated fields of a flagged record's topic — not
only the flagged cell — it surfaced **405** problems on fields that no structural check
had flagged (`new_findings.csv`):

- UNSUPPORTED 214 · NEEDS_CHANGE 191
- Confidence: high 253 · medium 147 · low 5
- By topic: leave 155 · overtime 150 · fringe 49 · general 12 · bonus 11 · pension 10 · childcare 6 · term 4 · contract 4 · homeoffice 2

These are the audit's highest-value output: errors that the four heuristics structurally
could not see (an already-populated value that is simply wrong but not an outlier, not an
enum violation, and stable across versions).

---

## 7. Coverage & method notes

**Subagent dispatch.** 432 verification chunks + 2 top-up worksheets = **434** worksheet
units, each processed by a `general-purpose` subagent reading the complete source section.
Dispatched in batched waves at **≤ 12 parallel** (MAX_PARALLEL_HARD_CAP): this session
used 56 assignment-file batches for the high-volume topics plus opus waves; earlier waves
covered the remaining topics. Model: **opus** for `term` and `pension`, **sonnet** otherwise.
Every subagent wrote exactly one result JSONL; the pure-Python `consolidate` step folds
verdicts back by (record_id, field). Zero subagent recursion.

**Field-kind coverage** (which of the 317 columns each check can reach):

| Kind | Columns | ≥1 flag | Covered by |
|---|---:|---:|---|
| numeric | 81 | 70 | outliers_numeric, era_baseline, cross_version |
| unit | 72 | 62 | cross_version (value_unit_contamination eligible but fired 0) |
| enum | 14 | 14 | enum_format, cross_version |
| date | 11 | 6 | enum_format (order/format), cross_version |
| boolean | 97 | 93 | cross_version only — **no dedicated structural check** |
| freetext | 37 | **0** | **no structural check** — holistic verify only |
| list / id / meta | 5 | 0 | not value-checked by design |

**Fields with no structural check applied:**
- **37 freetext fields** (`*_rule`, `*_description`, free-form notes) receive *no* automated
  structural check — they have no enum, no numeric expectation, and vary legitimately across
  versions. They are examined *only* by the holistic verification stage, and only when they
  belong to a flagged record's topic.
- **97 boolean fields** have no dedicated structural check; they are caught only by
  `cross_version` (version flips) and by holistic review (which applies the boolean rule:
  a False/blank boolean on a silent source = CONFIRM, not UNSUPPORTED). A boolean that is
  wrong but consistent across all versions on a silent source would not be flagged.
- `general_updated_topics` (list), `id`, and the 3 `meta` housekeeping columns are not value-checked.

**`value_unit_contamination` → addressed by Check 5 (`unit_semantics`), added 2026-06-02.**
The original contamination check raised **0** flags because it only catches *structural* slips
(a number in the unit slot, unit text in the value slot). It is blind to the bigger problem: a
cell where both halves are individually clean but the unit's KIND disagrees with the field —
e.g. `leave_paid_maternity_value = 100` with unit `% of salary` (the 100 % pay-rate bleeding
into a field that is canonically a DURATION; 198 records, and summing the column as "weeks"
is corrupted by every one). Check 5 maps each unit to a family (time / pay / count / distance)
via earliest-dimension-token, fixes a field's canonical family, and flags cells whose family
disagrees — but only policies TIME-canonical fields (PAY fields legitimately carry €, %,
months-of-salary). Result: **351 flags (341 high)**, dominated by maternity (198), paternity,
vacation-time and adoption pay-in-duration contamination. Folded into `full_audit_flags.csv`
(310 new). Tests: `tests/test_unit_semantics.py` (9 passing).

**PRESERVATION-AWARE verification (2026-06-02) — and the data-loss guard it revealed.**
The 310 were verified by 10 subagents under strict rules: *never delete, never invent, only
suggest a value the source explicitly states, always preserve the displaced value in a note.*
Outcome: **KEEP_NONSTANDARD_UNIT 269 · NEEDS_CHANGE 21 · RELOCATE 17 · CONFIRM 1 · UNSUPPORTED 1**
(1 straggler, record 1618009, un-emitted). The critical finding: **87% are KEEP** — the value
(e.g. maternity "100% of salary") is genuinely source-supported and the source states *no*
duration, so it is a valid alternative encoding that must be **preserved, not deleted**. A naive
"flag → blank the mismatch" fix would have destroyed real data in ~286 cells. Only 21 have a
source-stated duration to recover (pay-rate kept in the note); 17 belong in an existing pay field
(RELOCATE). Disposition is therefore **never "delete"** — keep / recover-and-note / relocate.
Full per-cell detail (original value preserved verbatim + intended/detected unit kind + verdict +
source quote + disposition note + sibling note) is the Hanna deliverable
`unit_semantics_reconciliation.csv` (309 rows; instructions in `verify/SUBAGENT_INSTRUCTIONS_CHECK5.md`).

---

## 8. Spot-check (by eye, 14 cells)

14 flagged cells were inspected by hand against the raw dataset value **and** the source text,
stratified across all four checks, all three verdicts, and 7 topics. **All 14 were sound** —
both the flag and the verification verdict held up. No fabricated corrections, no
false-positive flood.

Verdict mix of the sample: 4 CONFIRM · 6 NEEDS_CHANGE · 4 UNSUPPORTED.

Representative cases:

- **CONFIRM (heuristic false-alarm correctly cleared):**
  - `819011 bonus_fixed_annual_lump_value = 2083.97` — source gives the exact € floor; the
    "OOM vs median 8" alarm fired because sibling versions store holiday allowance as **8 %**.
  - `1646002 term_severance_extra_value = 83000` — source literally caps at "€ 83,000"; the
    "100× scale slip" alarm misfired on a legitimately large cap.
  - `823011 ingangsdatum = 01/11/2018` — `date_order` flag fired, but the source confirms
    "from 1 November 2018"; the *sibling* `expiratiedatum` (31/10/2018) is the real error.
- **NEEDS_CHANGE (real error, correction grounded in source):**
  - `1618005 leave_vacation_time_value 176 → 172` — source: "144 statutory + 28 above-statutory".
  - `1494005 overtime_trigger_weekly_value 40 → 0.5` — source defines overtime as work
    exceeding contracted hours "by 30 minutes or more"; 40 was the absolute workweek.
  - `234014 overtime_selection_rule` free text → enum `other`.
- **UNSUPPORTED (source does not support the value):**
  - `1945009 leave_paid_maternity_value = 1 week` — source describes *kraamverlof* (1-week
    partner leave), not statutory maternity leave (16 wks); a field-semantics mislabel.
  - `533006 fringe_meal_benefit_type` — source block is empty `[]`; the comma-joined
    `free_meals, meal_allowance` is also an invalid enum.
  - `819004 pension_employee_contrib_value = 13.8962` — source gives a 54 %/46 % split, not
    that rate.

---

## 9. Top likely errors (highest-confidence corrections)

High-severity NEEDS_CHANGE with a source-grounded suggested value (58 such; sample):

| Record | Field | Current | → Suggested |
|---|---|---:|---:|
| 1188007 | contract_full_time_hours_value | 36.86 | 1659.0 (annual vs weekly) |
| 218006 | contract_part_time_range_max | 39.99 | 2080.0 |
| 1618007 | training_budget_value | 1.3 | 4988.0 |
| 157011 | bonus_thirteenth_month_amt_value | 564.85 | 3.5 (% not €) |
| 1494005 | overtime_trigger_weekly_value | 40.0 | 0.5 |
| 3356022 | overtime_allowance_value | 1.48 | 100.0 |
| 1618010 | leave_paid_maternity_value | 10.0 | 16.0 |
| 1264006 | leave_short_term_care_pay_value | 7.0 | 100.0 |
| 1869017 | leave_adoption_pay_value | 2.0 | 100.0 |

Recurring error patterns the audit exposes: weekly-vs-annual hours swaps,
percent-vs-EUR unit confusion, leave *duration* written into a *pay-rate* field,
enum violations (free-text or comma-joined values), pension contribution split-vs-rate
confusion, and *kraamverlof*/maternity-leave conflation.

---

## 10. Files

| File | Contents |
|---|---|
| `full_audit_flags.csv` | 4,778 flags + verification verdicts (the master output) |
| `verify/new_findings.csv` | 405 off-flag problems found during holistic verify |
| `outliers_numeric.csv` / `cross_version.csv` / `enum_format.csv` / `value_unit_contamination.csv` | raw per-check outputs |
| `verify/chunks/`, `verify/outputs/` | per-record worksheets and subagent verdict JSONL |
| `full_audit_summary.md` | this file |

**All review is advisory. Apply nothing automatically — a human curator decides each correction.**
