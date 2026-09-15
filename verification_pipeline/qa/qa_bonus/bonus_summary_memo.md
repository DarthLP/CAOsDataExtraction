# Bonus — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-22)

### Language profile + shared source

Bonus reads from **`wage_information.md`** (shared with `wage` per the schema). Source is English-translated with retained Dutch terms (eindejaarsuitkering, 13e maand, dertiende maand, jubileumuitkering, vakantiegeld). Verified by sampling — bonus content is interspersed within wage/salary-scale blocks.

**Token weight:** median 3,225 tokens/block, p90 6,358, max 17,755. Per Phase 0.5, per-item caps raised to **soft 3,500 / hard 6,000**. Even so, ~10% of blocks exceed the hard cap → the slicer must anchor on bonus-relevant passages (field keywords) and will truncate the wage-heavy remainder; truncation is flagged (A14/A15) for those.

### Topic scope

- Bonus content frequency across 1,503 non-empty blocks: jubilee/anniversary 649, end-of-year 565, profit-sharing 382, "8.33" 355, performance bonus 311, 13th month 197, sign-on 31.
- After Stage 1 most-recent-per-CAO filter: expect ~95 records.

### Hazard list (defined before sampling)

| H# | Hazard | Schema field(s) | Status |
|---|---|---|---|
| H1 | 13th-month unit polymorphism: "1 monthly wage" vs "8.33% of annual" vs "% of monthly" | `thirteenth_month_amt` (Amount) | **CONFIRMED** — 355 blocks mention 8.33; schema example shows both "monthly wage" and "% of annual salary" forms |
| H2 | End-of-year bonus (eindejaarsuitkering) vs true 13th month — often conflated | `thirteenth_month` (bool) | RISK — eindejaarsuitkering (565) is usually a 13th-month equivalent but may be partial (e.g. 3%); subagent must read the rate |
| H3 | Jubilee / anniversary gratuity (25-year, 12.5-year service) → seniority_loyalty vs retirement_gratuity | `seniority_loyalty_bonus` vs `retire_gratuity_present` | **CONFIRMED** — 649 mention service anniversaries; distinguish mid-career jubilee from retirement exit |
| H4 | Profit-sharing as variable % vs fixed — note field free text | `profit_sharing_present` + `_note` | **CONFIRMED** — 382 mentions |
| H5 | Over-eager `has_bonus_schemes`=True from any allowance mention (mirrors contract boolean over-claim) | `has_bonus_schemes` (bool) | RISK — vakantiegeld (holiday pay) is statutory, NOT a bonus; must not set True on its basis |
| H6 | Vakantiegeld / holiday allowance (8% statutory) mistaken for a bonus | `has_bonus_schemes`, `thirteenth_month` | **CONFIRMED hazard** — 8% holiday pay is statutory wage, not a bonus scheme; common false-positive |
| H7 | Performance/target bonus present as boolean only (no amount field) | `performance_bonus_present` | OK — boolean captures it |
| H8 | Sign-on bonus rare (31) — present flag + amount | `sign_on_bonus_present` + `sign_on_bonus` | LOW |
| H9 | Token overflow truncating bonus passages in wage-heavy blocks | all | NOTED — handled by field-anchored slicer + truncation flag |

### Sampled illustration

- **CAO 615 (Zorgverzekeraars)**: block dominated by function-group/salary-scale tables; bonus content (if any) is a small fraction — confirms the slicer must anchor on bonus keywords, not take the whole block.

### Recommendation: PROCEED — schema covers patterns; watch the vakantiegeld false-positive

FM entries to seed:

- **BONUS_FM_01** — 13th-month unit polymorphism: record the unit exactly ("monthly wage", "% of annual salary", "% of monthly wage"); do not convert. An eindejaarsuitkering at a stated % is the 13th-month equivalent → `thirteenth_month=True` with that %.
- **BONUS_FM_02** — Vakantiegeld / 8% holiday allowance is **statutory wage, NOT a bonus**. Do NOT set `has_bonus_schemes` or `thirteenth_month` True on its basis.
- **BONUS_FM_03** — Jubilee vs retirement gratuity: mid-career service anniversary (e.g. 25-year jubileumuitkering) → `seniority_loyalty_bonus=True`; lump sum *at retirement/exit* → `retire_gratuity_present=True`. Both can be true.
- **BONUS_FM_04** — `has_bonus_schemes` over-claim: set True only for a genuine recurring/structural incentive beyond base salary (mirrors CONTRACT_FM_04 boolean discipline).

**HARD STOP CHECK:** no, do not stop. Hazards within schema range; the main risk (vakantiegeld false-positive) is a convention/FM matter, not a schema gap.

## Stage 1 — Scope filter
- Total CSV records: 2,739 → **95 scoped records, 95 unique CAOs** (source: wage_information.md).

## Stage 2 — Deterministic layer
- L1 rule violations: 1 (1 R5 has_bonus_schemes=False+populated). Bonus booleans are mostly self-consistent in the CSV.
- L2 presence triggers (field-specific): 924 (extract mode)
- **Total: 925 worksheet items → 47 chunks** (per-item caps raised to 3,500/6,000 for the token-heavy wage source).

## Stage 3 — Subagent review
- 47/47 chunks, Sonnet, across ~4 batches (two rate-limit pauses, both cleared). Explicit guardrails in every prompt: vakantiegeld≠bonus, eindejaarsuitkering=13th-month, jubilee vs retirement. **0 malformed CSV rows** (bonus content has few embedded semicolons).
- Subagents showed strong discipline: consistently excluded 8% vakantiegeld as statutory, distinguished mid-career jubilee from retirement gratuity, treated one-off existing-staff payments as non-sign-on.

## Stage 4 — Aggregate + audit (FINAL)

| Bucket | Count |
|---|---|
| Total worksheet items | 925 |
| is_noop (no real change) | 615 + 279 suppressed |
| **Real corrections** | **19** |
| Clean wins | **19** |
| NHR (audit-flagged) | 20 |
| Suppressed: noop-vs-CSV (confirmations) | 279 |
| Suppressed: unit-without-value | 2 |

> **CRITICAL correction (2026-05-22):** the first bonus aggregate reported **298 clean wins** — but **279 were spurious**. The L2 presence scan generates worksheet items with `csv_value_old=""`; when a subagent *confirms* an existing CSV value (e.g. answers `False` on a `*_present` boolean that is already `False`), the aggregator compared `"" → False` and recorded a phantom change. The new `aggregator_lib.suppress_noop_vs_actual_csv` compares the proposed value against the **actual** scoped CSV value and reclassifies confirmations as no-ops. **True bonus clean wins: 19.** This bug was project-wide and all five done topics were re-audited (see status board).

### The 19 genuine corrections
- **15× thirteenth_month_amt** (value+unit pairs for 8 CAOs): eindejaarsuitkering extracted as the 13th-month equivalent at its stated % — e.g. CAO 433 5%, CAO 214 2.25%, CAO 496 2.01%, CAO 163 1.15%, CAO 1612 4%, CAO 50 4%, CAO 721 5%, CAO 1345 3%. All high/medium confidence, value verbatim in source.
- **4× boolean True-flips**: job_allowances_present (509001, 730012), profit_sharing_present (650020), performance_bonus_present (750026) — extractor missed these; subagent found explicit schemes.

### Audit hits
A14=6, A15=4 (truncation on the token-heavy wage source — expected), A17=9 (unit canonicalisation). NHR=20.

### Jubilee/seniority note
The mid-career jubilee → `seniority_loyalty_bonus=True` mapping needed **no corrections** — those records already had the flag set True in the CSV, so subagent confirmations were correctly suppressed as no-ops. The subagents correctly held `retire_gratuity_present=False` for mid-career jubilees.

## Stage 5b — Items for Hanna
1. **BONUS_FM_01..04** added to [bonus.md](../conventions/failure_modes/per_topic/bonus.md).
2. **NHR queue: 20 rows** — mostly A14/A15 truncation + A17 unit. Light review.
3. **Clean wins: 19** ready to ship — 15 thirteenth-month % extractions + 4 boolean flips.
4. **Project-wide bug fixed** (`suppress_noop_vs_actual_csv`): the L2-confirmation inflation affected every topic; all corrected (see status board).
