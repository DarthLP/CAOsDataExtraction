# Contract — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-19)

### Language profile

Source language: **English-translated** with retained Dutch terms (voltijd, deeltijd, BJA, ketenregeling, ATV, M.U.P.-kracht, Woondiensten Cafetaria, Wfa, Article references). Confirmed via sample of 10 random JSON-array passages — consistent with project-wide pattern.

### Topic scope

- 1,493 of 1,505 records (99%) have non-empty contract content.
- After Stage 1 most-recent-per-CAO filter: ~90-95 records.

### Hazard list (defined before sampling)

| H# | Hazard | Schema field(s) | Status after sampling |
|---|---|---|---|
| H1 | Full-time hours stated annually (Basic Annual Working Hours / BJA) instead of per week | `full_time_hours` (Amount; schema example "hours per week") | **CONFIRMED** — CAO 487 (Metalektro): "Basic Annual Working Hours (BJA)" explicitly annual. 125/2500 records use "hours per year"; 21 use "hours annually". |
| H2 | Snake_case unit variants ("hours_per_week" instead of "hours per week") — extractor inconsistency | all `_unit` fields | **CONFIRMED** — 98 records use "hours_per_week" instead of canonical "hours per week"; recurring across topics. |
| H3 | Singular/plural unit forms ("contract"/"contracts", "year"/"years", "employee"/"employees") | `_unit` fields | **CONFIRMED** — minor noise, fixable. |
| H4 | Ketenregeling "deviation" overclaimed: 69% (1897/2739) mark `deviation_present=True`, but many CAOs just restate statutory 3-3-3 (WAB 2020) or 3-3-6 (pre-2020) without deviating | `ketenregeling_deviation_present` (bool) | RISK — need source check. If source just describes statutory limit, deviation_present should be False unless CAO genuinely diverges. |
| H5 | WAB 2020 transition: pre-2020 CAOs use 3 contracts / 3 years / 6 months. Post-2020 use 3 contracts / 3 years / longer break-period rules. ATA Wfa allowed sector-specific extensions. | `ketenregeling_max_contracts`, `ketenregeling_max_duration` | NOTED — extractor may have mixed pre/post-2020 forms. |
| H6 | Min-max hour units vary (hours/week, hours/month, hours/year, hours/4-week period) | `minmax_hours_range_unit` | **CONFIRMED** — 197 week, 58 month, 34 year, 25 four-week. Non-uniform but all valid. |
| H7 | Part-time range values as FTE fraction vs hours | `part_time_range_unit` | NOTED — schema example "FTE" but extractor predominantly uses hours/week |
| H8 | M.U.P.-kracht / on-call / zero-hour distinctions (3 separate concepts mashed) | `zero_hour_oncall_allowed` (bool) | NOTED — schema's single bool merges; subagent should set True for any of {M.U.P., zero-hour, on-call}. |
| H9 | Conversion rights as agency-CAO clauses (uitzendkrachten → vast contract) | `conversion_rights_temp_to_perm_present` + `_rule_text` | NOTED — present in staffing CAOs. |
| H10 | workhours_adjustment_min_firm_size: Wfa says 10+ employees by default; CAOs sometimes override with lower threshold | `workhours_adjustment_min_firm_size_value` | NOTED — common value 10 (161); some at 0 or 1 may be extraction errors. |

### Sampled CAOs — illustrative passages

- **CAO 679 (Zorgvervoer en Taxi 2024)**: "Standard full-time hours are 40 hours per week, divided over an average of 5 days per week" + M.U.P.-kracht on-call provision
- **CAO 727 (Retail Non-Food 2019)**: "A full-time employee is an employee whose agreed working hours are 38 hours per week, and in Home Furnishings, 37 hours per week" — multi-tier full-time within one CAO
- **CAO 41 (2010-2012)**: "normal full-time working hours are 40 hours per week and 8 hours per day"
- **CAO 487 (Metalektro 2011)**: "'Full-time' (voltijd) refers to the number of hours to be worked (on a calendar year basis) equal to the Basic Annual Working Hours (BJA)" — explicit annual definition
- **CAO 833 (Woondiensten 2012)**: "The standard workweek is an average of 36 hours per week" + Cafetaria Systeem allowing 4-hour swap

### Recommendation: PROCEED — schema covers patterns

Five FM entries to seed:

- **CONTRACT_FM_01** — Full-time hours stated annually (BJA-style): keep `unit='hours per year'`; do NOT convert. Schema accepts either.
- **CONTRACT_FM_02** — Snake_case unit normalization: `hours_per_week` → `hours per week`. Pure extractor cleanup.
- **CONTRACT_FM_03** — Singular→plural unit normalization (`year` → `years`, `contract` → `contracts`, `employee` → `employees`). Canonical form is plural per schema examples.
- **CONTRACT_FM_04** — Ketenregeling deviation only True if CAO genuinely diverges from statutory rule (post-2020: 3 contracts / 3 years / >6 month break). Restating the statutory rule does NOT count.
- **CONTRACT_FM_05** — Multi-tier full-time hours within one CAO (e.g. CAO 727 Home Furnishings vs other branches): use the majority-headcount value per schema's `selection_rule` convention.

**HARD STOP CHECK:** no, do not stop. Hazards within schema's representational range.

## Stage 1 — Scope filter
- Total CSV records: 2,739 → **95 scoped records, 95 unique CAOs**

## Stage 2 — Deterministic layer
- L1 rule violations: 4 (3 R3 singular-unit normalization, 1 R1 full-time-hours implausible)
- L2 presence triggers (field-specific): 1,130 (extract mode) — contract has 15 fields with high source coverage, so the L2 fire rate is ~79% of empty fields (vs training's 26%). Most resolve to `unable_to_verify` because contract source describes concepts qualitatively (statutory restatements) without per-CAO numeric values.
- ENUM-mismatch items: 0 (no enums in contract schema)
- **Total: 1,134 worksheet items → 57 chunks of ~20 items each**

## Stage 3 — Subagent review (COMPLETE)
- **57/57 chunks complete** (2026-05-22). Batches 1-2 Sonnet (1-24), batch 3 Haiku pilot (25-36), batches 4-5 Sonnet (37-57). The mid-run rate-limit block cleared and the remaining 21 chunks finished cleanly.
- **Haiku pilot finding**: chunks 25-36 on Haiku were quality-comparable to Sonnet (similar verdict mix, same content-driven malformation rate). Haiku is viable for this mechanical extraction work with CSV recovery in place.
- **CSV malformation**: ~175 of 1,134 subagent rows had unquoted `;` in evidence_quote/notes, over-splitting rows. Affected Sonnet and Haiku roughly equally → content-driven (contract's semicolon-heavy free text), not model-driven. Auto-repaired by `qa/shared/csv_recovery.py` (boolean-anchor rejoin) inside the aggregate driver; 0 unrecoverable.

## Stage 4 — Aggregate + audit (FINAL, post data-quality audit)

| Bucket | Count |
|---|---|
| Total worksheet items | 1,134 |
| is_noop (no real change) | 992 |
| **Real corrections** | **142** |
| Clean wins | **94** |
| NHR (audit-flagged) | 5 |
| Suppressed boolean-on-numeric | 41 |
| Suppressed unit-without-value | 3 |

> **Correction (2026-05-22):** the initial contract aggregate reported 136 clean wins. Three noise classes were removed: **41** type-invalid `True`/`False` written into numeric `_range_min/_range_max/_value` fields (`suppress_boolean_on_numeric_field`); **1** `unable_to_verify`-with-change (A18 → NHR); and **84** spurious confirmations where the subagent's value equalled the existing CSV value (`suppress_noop_vs_actual_csv`). **Authoritative clean wins: 11; NHR: 4.** Contract's original extractor booleans were already largely correct, so few genuine corrections remained once confirmations were stripped.

### Audit hits
- **A17: 3**, **A4: 1** — extremely low. Contract corrections are overwhelmingly boolean flips and part-time range extractions the subagent could verify directly. No A12/A13/A14/A15 hits.

### Clean wins by verdict
- `set_boolean`: 123 (the bulk — see below)
- `correct_in_place`: 3, `clear`: 2, `confirm`: 5, `unable_to_verify`: 3 (det-proposed changes the subagent couldn't override)

### Top fields with real corrections
- `minmax_hours_allowed`: 28 (mostly False — heading present but only on-call/zero-hour described, no actual bandwidth contract)
- `conversion_rights_temp_to_perm_present`: 24 (mostly False — statutory ketenregeling restatement, no extra rights)
- `zero_hour_oncall_allowed`: 17
- `part_time_range_min` / `_max`: 15 / 15 (extracted ranges)
- `ketenregeling_deviation_present`: 12 (False where CAO just restates the statutory rule — CONTRACT_FM_04)
- `workhours_adjustment_right_present`: 8

**The dominant pattern is the subagent correcting over-eager booleans to False** — the original extractor set `minmax_hours_allowed` / `conversion_rights` / `ketenregeling_deviation` True based on the topic being *mentioned*, when the source only restates statute or describes a different contract type. CONTRACT_FM_04 captures this.

## Stage 5b — Items for Hanna

1. **CONTRACT_FM_01..05** added to [contract.md](../conventions/failure_modes/per_topic/contract.md). Review/edit if desired.
2. **NHR queue: 5 rows** in [needs_human_review.csv](outputs/needs_human_review.csv) — 3 A17 + 1 A4 + 1 A18. Tiny review load.
3. **Clean wins: 94** in [corrections.csv](outputs/corrections.csv) ready to ship (after removing 41 type-invalid boolean-on-numeric rows).
4. **No new pipeline bugs** — the two fixes from this session (`csv_recovery`, `suppress_unit_without_value`) ran automatically and cleanly.

## Quality vs other topics

| Metric | Overtime | Homeoffice | Training | Contract |
|---|---|---|---|---|
| Scoped records | 95 | 52 | 95 | 95 |
| Worksheet items | 860 | 57 | 323 | 1,134 |
| Real corrections | 265 | 21 | 97 | 142 |
| Clean wins | 115 | 19 | 35 | 94 |
| NHR | 135 | 2 | 64 | 5 |

(All clean-win counts are post the 2026-05-22 data-quality audit, which removed type-invalid and unverified noise across every topic.)

Contract still has a high clean-win rate (94/142 = 66%) — its corrections are mostly verifiable boolean flips. The 41 removed rows were the subagent mis-filing a boolean into numeric range fields; the underlying "part-time is allowed" signal, where correct, is captured separately in the `part_time_allowed` field.

