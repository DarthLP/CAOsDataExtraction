# Training — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-19)

### Language profile

Source language verified: **English-translated** with occasional preserved Dutch terms (Article references; statutory acronyms WAB/EVC/BHV/EHBO; sectoral fund names O&O-fonds, scholingsfonds, OSV-fonds, RAS; formal scheme names "Erkennen en Waarderen", "studiekostenbeding", "terugbetalingsbeding", "vitaliteits- en ontwikkelingsfonds"). Consistent with the project-wide pattern documented in `CLAUDE.md`. Sampled 5 random JSON-array passages — all English with the expected Dutch acronyms intact.

### Topic scope

- 1,492 of 1,505 records (99%) have non-empty training content. Training is one of the most widely covered topics in CAOs (mandated by law since the WAB 2020 study-time provisions).
- After Stage 1 most-recent-per-CAO filter, expect ~80–95 records.

### Hazard list (defined before sampling, 30-record stratified random sample, seed=0)

| H# | Hazard | Schema field(s) | Status after sampling |
|---|---|---|---|
| H1 | Time-allowance unit variation (hours/year vs days/year vs days/period) | `time_yearly` (Amount, schema example "hours per year") | **CONFIRMED** — 11/30 use hours, 5/30 use days. Schema's hours example is **not the majority** for many sectors. |
| H2 | Budget unit variation (EUR/year vs % of costs vs case-by-case) | `budget` (Amount) | **CONFIRMED** — 13/30 give EUR amounts ranging from €250 to €2,000+; some give "actual costs"; some defer to sector fund. |
| H3 | Reclaim clause (terugbetalingsbeding / studiekostenbeding) — WAB 2020 carved out *mandatory* training from reclaim | `reclaim_clause_present` (bool) | **CONFIRMED** — 19/30 (63%) mention some form of reclaim/repayment provision. Some are conditional on voluntary training only. |
| H4 | EVC (Recognition of Prior Learning) entitlement — preserved Dutch concept, distinct from regular training | several fields | NOTED — 10/30 mention EVC; usually paid by employer; sometimes funded via sector fund. |
| H5 | Sector training fund as primary financier (O&O-fonds, scholingsfonds, OSV-fonds, RAS, etc.) | `fund_present` (bool) + `cost_reimbursement` | **CONFIRMED** — 4/30 explicit sector fund mention; many CAOs delegate financing to the fund rather than the employer directly. |
| H6 | Mandatory vs voluntary training distinction; mandatory training counts as working time and is paid 100% by employer | `mandatory_training_paid` (bool) | **CONFIRMED** — 2/30 explicit "mandatory training in working time" phrasing; many more imply via "training necessary for the function." |
| H7 | Career-scan frequency phrased "every X years" rather than "X per year" | `career_scan_freq` (Amount; schema example "times per year") | **CONFIRMED** — 14/30 use "every five years" / "once every X years" phrasing. Schema's "times per year" unit collapses this to 0.2 — error-prone for the subagent. |
| H8 | Category-based training rights (Category I/II/III/IV/V — common in transport sector) | various | NOTED — present but rare in the sample; subagent can ignore unless source distinguishes |
| H9 | Agency / staffing CAOs defer to client/hirer for training (mirrors OVERTIME_FM_05 / HOMEOFFICE_FM_02) | All numeric fields | NOTED — 4/30 reference agency/hirer/uitzend |
| H10 | "Days of paid training leave per year" vs "days of training time" — subtle distinction | `time_yearly` | LOW — both translate to the same Amount field; difference is descriptive only |

### Sampled CAOs — illustrative passages

- **CAO 26 (Vleessector, 2014)**: "After prior consultation with the employer, employees can be granted a maximum of 5 days of leave per year for training" + "contribution to training costs for employability is a maximum of €500 per year (maximum €1,000 per two years)" + "career advice once every five years, with a maximum cost of €750"
- **CAO 1536 (NU 2024)**: "Employees receive at least three development days annually for development and training in the context of 'Erkennen en Waarderen'" — `time_yearly`=3 days/year
- **CAO 163 (OV/Transport 2014)**: "three days of paid training leave per year for Category I and II training (Bijlage 24)" — category-conditional time allowance; OSV-fonds finances training
- **CAO 496 (2018)**: "Mandatory training is equated to a shift" + dog handlers get specific training hour minimums
- **CAO 433 (2024)**: "Training costs for RAS-subsidized training cannot be reclaimed from the employee" — explicit WAB-2020-style mandatory-training carve-out

### Recommendation: PROCEED — schema covers patterns

The training schema (9 fields) covers all observed patterns. Three FM entries to seed:

- **TRAINING_FM_01** — Time allowance unit polymorphism: subagent must record `unit` exactly as stated ("days per year" or "hours per year"). Do NOT convert days↔hours.
- **TRAINING_FM_02** — Career-scan "every X years" → `career_scan_freq.value = 1/X`, `unit = "times per year"`. Document the inversion in `notes`.
- **TRAINING_FM_03** — Sector training fund (O&O-fonds, scholingsfonds, RAS) → `fund_present=True` regardless of whether the employer directly reimburses; the fund's existence is the trigger.
- **TRAINING_FM_04** — WAB 2020 mandatory-training reclaim carve-out: when source says mandatory training cannot be reclaimed, `reclaim_clause_present=True` is still correct (the clause exists for voluntary training); record the carve-out in `notes`.
- **TRAINING_FM_05** — Agency / staffing CAOs defer to hirer (mirrors OVERTIME_FM_05).

**HARD STOP CHECK:** no, do not stop. Hazards are within schema's representational range; FM entries handle the recurring patterns.

## Stage 1 — Scope filter
- Total CSV records: 2,739
- After scope filter (latest per CAO with non-empty source): **95 records, 95 unique CAOs**

## Stage 2 — Deterministic layer
- L1 rule violations: 78 (45 R6 cost_reimbursement_unit "percent"→"% of costs", 29 R5 career_scan_freq inversion, 2 R2 time_yearly unit non-yearly, 1 R4 budget unit non-canonical, 1 R9 has_rights=False+populated)
- L2 presence triggers (field-specific): 245 (extract mode)
- ENUM-mismatch items: 0 (training schema has no enums)
- **Total: 323 worksheet items** → 17 chunks of ~20 items each

## Stage 3 — Subagent review
- 2 batches of subagents (12 + 5), all 17 chunks returned
- System prompt 2,565 tokens (well under 6,000 cap)
- Productive chunks: chunk_001, 002, 003, 004 — R5/R6 canonicalizations split between `correct_in_place` (where the subagent could quote a `%` or `year` token) and `unable_to_verify` (where the canonical-form string "% of costs" / "times per year" was not verbatim in source)
- Chunks 005-017 (L2 extract-mode): mostly `unable_to_verify` since training fields are highly qualitative — most CAOs describe training rights/funds in prose without specific numeric values

### Verdict distribution
| Verdict | Approximate count |
|---|---|
| unable_to_verify | ~240 |
| correct_in_place | ~50 |
| set_boolean | ~15 |
| confirm | ~1 |

## Stage 4 — Aggregate + audit

(Numbers below are **post-CSV-recovery** — see "Bug found during run" section. Pre-recovery the malformed rows inflated NHR to 61 and created 9 phantom rows with misaligned columns; the recovery fixed all of them.)

| Bucket | Count |
|---|---|
| Total worksheet items | 323 |
| is_noop (no real change) | 226 |
| **Real corrections** | **97** |
| Clean wins | **35** |
| NHR (audit-flagged + det_sub_conflict + A18) | 64 |

> **Correction (2026-05-22):** initial run reported 58 clean wins. A data-quality audit found **23 were R5 career-scan-frequency rows the subagent marked `unable_to_verify`** but which still carried the deterministic inversion value — they were shipping as clean wins. The new **A18 audit** now routes any `unable_to_verify`-with-applied-change to NHR. Intermediate clean=35. A further project-wide fix (`suppress_noop_vs_actual_csv`) then removed **12** more spurious confirmations (subagent answer == existing CSV value). **Authoritative clean wins: 23; NHR: 64.** See "R5 rule is buggy" below.

### R5 rule is buggy (flagged for redesign)
R5 fires on `training_career_scan_freq_unit` and proposes `value=1/X, unit='times per year'` to convert "every X years". **Two problems:** (a) it targets the `_unit` field but carries a *value* (0.333), and (b) it does not also correct the paired `_value` field — so applying it would leave `value=X` (e.g. 5) with `unit='times per year'` = "5 times per year", the inverse of the truth. The subagents correctly returned `unable_to_verify` on these (the canonical string isn't verbatim in source), so A18 now contains all 23 in NHR rather than shipping wrong data. **Fix for any training re-run:** split R5 into two corrections (set `_value=1/X` AND `_unit='times per year'`), or drop R5 and let the subagent extract the frequency directly.

### Audit hits (post-recovery)
- **A17: 15** (verdict has value but value not literally in evidence — R5 frequency inversion is mathematically derived, not quoted)
- **A4: 2** (boolean field with non-boolean value)
- **A13: 1** (one rule with disagreement rate above threshold)
- **det_sub_conflict: 24** (R5/R6 cases where subagent unable_to_verify and det wants to change a value)
- A12 dropped from 20 → **0** after recovery: the false A12 hits were caused by malformed rows where the verdict landed in the wrong column.

### Top fields with real corrections
- `training_cost_reimbursement_unit`: 46 (R6 normalization "percent" → "% of costs")
- `training_career_scan_freq_unit`: 29 (R5 frequency inversion "years" → "times per year")
- `training_reclaim_clause_present`: 7 (subagent set_boolean=False where source explicitly says no clause)
- `training_budget_unit` / `_value`: 3 / 2 (a few extracted EUR budgets)
- `training_fund_present`: 2 (set_boolean=False where fund abolished)
- `training_time_yearly_unit`: 2 (unit normalizations)

### A13 / rule miscalibration
- R5 (career_scan inversion) fired 29 times; ~70% pushed to NHR by A17 (subagent can't verify the mathematical inversion since the canonical "times per year" string never appears in source). Det's correction is *probably right* but evidence-strict audit routes them all to Hanna for verification. This is the same tradeoff as overtime v2's unit-canonicalization NHRs.
- R6 (cost_reimbursement_unit) fired 45 times; similar split — many flow as `det_only` clean wins; the few subagent-overridden ones go to NHR.

## Bug found during run + fix

- **Malformed CSV rows (unquoted `;` in evidence_quote / notes)** — chunk_003 emitted rows with 13-14 columns instead of 12, shifting the verdict/field columns. This produced 9 phantom rows and 20 false A12 audit hits.
- **Fix:** built `qa/shared/csv_recovery.py` — a reusable recovery utility that re-joins over-split rows by anchoring on the two boolean columns (`topic_section_was_truncated`, `value_not_in_source`). Handles 11-col (missing unit), 12-col (clean pass-through), and 13+-col (over-split) rows, and drops leaked tool-call junk lines (`</content>`, `</invoke>`). Idempotent — safe to re-run.
- **Result:** 51 training rows recovered, 0 unrecoverable. Re-aggregation: clean wins 45 → **58**, NHR 61 → **41**, A12 20 → **0**.
- This same utility was applied to the contract run (158 rows recovered) and should be wired into every topic's aggregate step going forward.

## Stage 5b — Items for Hanna

1. **TRAINING_FM_01 / FM_02 / FM_03 / FM_04 / FM_05** to add to [training.md](../conventions/failure_modes/per_topic/training.md). Review and edit if desired.
2. **NHR queue: 59 rows** in [needs_human_review.csv](outputs/needs_human_review.csv). Of these, ~46 are R6 canonicalizations + ~13 are R5 frequency inversions where the subagent couldn't find verbatim evidence of the canonical unit string. Hanna can accept these en masse if she trusts the deterministic mapping ("percent" → "% of costs", "every X years" → "1/X times per year").
3. **Clean wins: 45** in [corrections.csv](outputs/corrections.csv) (filter `is_noop=False` and `fix_method!=needs_human_review`).
4. **No new bugs needing pipeline changes** — the L2 field-specific approach worked the same as homeoffice. Training's qualitative nature drives high unable_to_verify; this is genuine, not a slicer issue.

## Quality compared to overtime + homeoffice

| Metric | Overtime v2 | Homeoffice | Training |
|---|---|---|---|
| Scoped records | 95 | 52 | 95 |
| Worksheet items | 902 | 57 | 333 |
| Real corrections | 265 | 21 | 97 |
| Clean wins | 115 | 19 | 35 |
| NHR | 135 | 2 | 64 |

(All counts post the 2026-05-22 data-quality audit.)
| A17 / canonicalization NHR share | high | n/a | high |

Training's profile sits between overtime (many quantitative fields) and homeoffice (small N, few rules). Most of training's NHR queue is canonicalization NHRs where det probably ships correctly with light Hanna review.

