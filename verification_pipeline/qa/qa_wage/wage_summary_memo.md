# Wage / Salary-Scale Mechanics — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-23)

### Language profile + source
Source `wage_information.md` (shared with **bonus** per the schema). English-translated
with retained Dutch scale vocabulary (trede/periodiek, aanloopschaal, functiejaren,
normperiodiek, inloopperiode). Token-heavy sections (~3,779 tokens/section median),
so per-item caps were raised to **soft 3,500 / hard 6,000** (same override as bonus).

### Schema shape
3 present-booleans, each paired with a free-text rule note (6 fields total):

| Boolean (present-flag) | Paired rule note |
|---|---|
| `wage_entry_step_exp_present` | `wage_entry_step_exp_rule` |
| `wage_pers_allow_max_scale` | `wage_pers_allow_rule` |
| `wage_perf_step_var_present` | `wage_perf_step_var_rule` |

All three describe **discretionary** scale mechanics:
- **entry_step_exp** — placement on a *higher* entry step in return for relevant experience.
- **pers_allow_max_scale** — a personal allowance paid at scale-max, or as a guarantee after reclassification/downgrade.
- **perf_step_var** — a periodic step that is granted/withheld based on a performance appraisal (not automatic).

### Hazard list
| H# | Hazard | Field(s) | Status |
|---|---|---|---|
| H1 | Automatic age/service-year progression (periodieken, functiejaren) read as performance-based | `wage_perf_step_var_present` | **CONFIRMED** — by far the dominant false-positive risk; ~all CAOs have automatic periodieken |
| H2 | `inloopperiode` / `aanloopschaal` / `wachtperiodieken` (a *lower* start for lacking experience) read as a *higher* entry step for experience | `wage_entry_step_exp_present` | **CONFIRMED** — the inverse of the target provision; subagents repeatedly corrected this |
| H3 | Temporary acting-up / substitution allowances (waarneming, combifunctie) read as a scale-max personal allowance | `wage_pers_allow_max_scale` | **CONFIRMED** — these are not at-max/reclassification supplements |
| H4 | Above-scale performance *bonus* read as a *step* grant/withhold | `wage_perf_step_var_present` | NOTED — a bonus is not a step variation (cf. bonus topic) |
| H5 | L2 presence-scan generates an item for one field of a pair but not the other → orphan rule / missed boolean | (pipeline) | **CONFIRMED** — see Stage 5b residuals |

### Recommendation: PROCEED. No HARD STOP. Schema fine; shares wage_information.md with bonus.

## Stage 1 — Scope filter
- 2,739 → **95 scoped records, 95 unique CAOs** (most-recent doc per CAO; wage is universal so every CAO is in scope).

## Stage 2 — Deterministic layer
- L1 (R1 flag-false-with-rule): **0**. L2 field-presence triggers: **163**. ENUM: **0** (these are free-text rule notes, correctly excluded by the `_FREETEXT_SUFFIXES` fix). **163 items → 10 chunks** (3,500/6,000 caps; ~2,564 prompt tokens).

## Stage 3 — Subagent review
- 10/10 chunks, Sonnet, run in two background batches. All chunks returned valid 12-column CSV; **csv_recovery repaired 1 row** (unquoted `;` in a rule-text field), 0 unrecoverable.

## Stage 4 — Aggregate + audit (FINAL)
| Bucket | Count |
|---|---|
| Total worksheet items | 163 |
| **Clean wins** | **3** |
| NHR | 0 |
| Suppressed: noop-vs-CSV | 60 |
| is_noop (total, incl. confirmations) | 160 |

Integrity sweep: total_rows == input_chunk_rows (163), 0 malformed fields, 0 verdict-leak-in-field, 0 duplicate keys, 0 boolean-on-numeric (no numeric fields in this topic).

### The 3 clean wins (both genuine, high-confidence, non-truncated, value-in-source)
1. **CAO 721017** (Huisartsenzorg) — `wage_perf_step_var_present` → **True** + `wage_perf_step_var_rule`: *"Annually, with a positive appraisal, a periodic increase is granted…"* (Art. 4.6). A genuine performance-gated step.
2. **CAO 1471012** — `wage_pers_allow_rule` populated: a fixed monthly guarantee allowance on the scale salary granted after placement in a lower scale.

### Why 160 confirmations (the extractor was already right)
The overwhelming pattern: CAOs use **automatic** scale progression (age/service-year periodieken, functiejaren, start at step/periodiek 0), which is *not* discretionary entry-step-for-experience or performance-gated variation. Subagents confirmed the extractor's existing `False`/empty on 160 items. The A14/A15 audit flagged 14 truncated-source rows, but **all 14 are `is_noop=True` confirmations** — the is_noop guard correctly kept them out of NHR (a no-op on a truncated source needs no human review). None of the 3 clean wins are on truncated sources.

## Stage 5b — Items for Hanna

1. **WAGE_FM_01..03** added to [wage.md](../conventions/failure_modes/per_topic/wage.md).

2. **3 clean wins, 0 NHR.** The extractor's wage-mechanics flags were ~98% accurate on the 163 reviewed items; the discretionary provisions targeted here are genuinely rare (most scale movement is automatic).

3. **L2 scope-gap residuals (surfaced, not auto-fixed per the hard rules).** The L2 presence scan fires per-(record, field); when it fires for one half of a present/rule pair but not the other, two failure shapes result:
   - **Orphan rule (CAO 1471012):** `wage_pers_allow_rule` was extracted, but no `wage_pers_allow_max_scale` item was in the worksheet, so the boolean stays `False` while the rule is populated — exactly the R1 inconsistency pattern. **Suggested fix:** flip `wage_pers_allow_max_scale`→True for 1471012 (or, structurally, have build_chunks always emit both halves of a pair — see WAGE_FM_03).
   - **Never-scanned genuine provisions:** subagents reading a full section for a sibling field noticed real provisions on **CAO 826** (`pers_allow_max_scale`: difference paid as personal allowance to 55+ employees moved to a lower group), **CAO 43** and **CAO 932** (genuine `perf_step_var`), for which **no worksheet item was generated at all**. These are L2 false-negatives the keyword scan missed; they are *not* in the corrections. Worth a keyword-expansion pass if wage is re-run.

4. Confirms the bonus-shared-source override (3,500/6,000 caps) works cleanly for wage too.
