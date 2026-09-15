# Pension — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-25)
Source `pension_information.md` (3.9 MB), English-translated with retained Dutch
(bpfBOUW, ABP, PFZW, PMT, AOW, Witteveen, franchise, excedentregeling, Generatiepact).
The highest-era-complexity topic alongside term. **~30 fields**: master
`pension_has_pension_scheme`; `pension_type` + `selection_rule` (enums); employee
contribution value/unit + range; accrual rate; franchise; normal/early/deferred
retirement ages; premium-total range; booleans (mandatory_participation,
excedent_present, premium_eq_split, hetero_pension, accrual_stat_leaves, accrual_illness_y2).

**Era-baseline gate (Phase 4.5):** Witteveen accrual cap + franchise floor encoded
**post-hoc only** (never in the prompt). Run on **Opus**.

## Stage 1 — Scope: 95 records / 95 CAOs.
## Stage 2 — Deterministic: **2 L1 (R1 master-flag), 1040 L2, 0 ENUM → 53 chunks** (20 items).

## Stage 3 — Subagent review: Opus, 53 chunks.
Calibrated first 3 (clean) then scaled. **Interrupted twice by the weekly rate limit;
resumed cleanly after each reset** and completed all 53. The two cautions (no statutory
fill-in incl. Witteveen/AOW/franchise; conservative booleans) + WTP-transition awareness
held across all 53 chunks.

## Stage 4 — Aggregate + audit + era flag (FINAL)
| Bucket | Count |
|---|---|
| Total worksheet items | 1042 |
| **Clean wins** | **0** |
| NHR | 0 |
| **Era-baseline outliers** | **5** |
| is_noop (incl. confirmations) | 1042 |

Integrity: 1042 rows = 1042 det items, 0 duplicate keys, 0 non-`pension_` fields.
Verdict mix: 696 confirm, 344 unable_to_verify, 2 set_boolean (False). All no-op.

### Why 0 clean wins (verified correct, not a pipeline miss)
Pension is overwhelmingly **fund-deferred**: almost every CAO points pension specifics
to the fund's own regulations (bpfBOUW/ABP/PFZW/PMT/etc.) without stating concrete
accrual %, contribution %, franchise €, or retirement ages — so the L2-flagged empty
fields are correctly empty. **Double-checked:** the records that DO carry concrete values
(509001 accrual 2.05/contrib 1.0/franchise 12232; 637011 1.75/6.0/16253; 148015 1.64/13.89;
43020 2.1/45; 359014 1.875/10; 1296017 1.823/franchise 19795; 51017/10031) **already have
those scalars populated in the dataset** — the extractor captured them, so L2 correctly did
not re-flag them, and the subagents only saw the empty *range* variants (correctly left
empty, since a single value is not a cross-group range). No uncaptured gap.

### Era-baseline outliers (5) — the high value of this topic
| Record | Field | Value | Date | Read |
|---|---|---|---|---|
| **157017** | accrual_rate | **100%** | 2024 | **Extraction error** — "100% accrual maintained" (Generatiepact) misread as a DB accrual rate; impossible (cap 1.875%). |
| **1287018** | accrual_rate | **100%** | 2015 | **Extraction error** — same "100% of pensionable salary" misread. |
| 1165029 | franchise | €10,479/yr | 2017 | Below ~€13k floor — possibly a legit lower franchise; eyeball. |
| 1496024 | franchise | €12,953/yr | 2016 | ≈ the actual 2016 minimum; the approx €13k floor is slightly high for early years (soft flag). |
| 163011 | franchise | €1,329.62/**month** | 2021 | Unit artifact — a *monthly* franchise (~€15,955/yr) compared to the annual floor; fine. |

The two **accrual=100%** flags are genuine extractor errors the era baseline caught — exactly
its purpose. See `outputs/era_outliers.csv`.

## Bug found + fixed (during the double-check)
`era_baselines._parse_date` did not handle the scoped CSV's Dutch **DD/MM/YYYY** dates
(e.g. `01/04/2011`), so the post-hoc era flagging was a **silent no-op** for both term and
pension (0 outliers = failed date parse, not "no breaches"). Fixed (added `%d/%m/%Y` etc.)
+ 2 regression tests; re-ran both topics — **term genuinely 0** (probation extractions ≤ 2-mo
cap, severance empty), **pension 5** (above). This validated the whole Phase 4.5 era effort.

## Stage 5b — Items for Hanna
1. **PENSION_FM_01..04** added to [pension.md](../conventions/failure_modes/per_topic/pension.md).
2. **0 clean wins, 0 NHR, 5 era outliers** — incl. **2 genuine extraction errors (accrual=100% on 157017, 1287018)** to correct, and 3 franchise flags to eyeball (1 monthly-unit artifact, 1 at-2016-floor).
3. Pension extractor data is otherwise sound: concrete values were already captured; fund-deferred fields correctly empty.
4. Era baselines validated in production (post-hoc, deterministic, never shown to the model) — after the date-parse fix.
