# Childcare — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-22)

### Language profile + source
Source `childcare_information.md`, English-translated with retained Dutch (kinderopvang, LRK, mantelzorg). **Sparse topic**: only 230 of 1,506 blocks non-empty (token-median 134) — most CAOs predate or don't address employer childcare support (post-2007 the Wet Kinderopvang shifted childcare funding to a public tax-credit system, so employer schemes largely disappeared).

### Schema shape
Booleans (support_present, inhouse_present, discount_present, priority_access, funding_sector_fund) + amounts (support, support_cap, age_min, age_max, min_fte) + min_tenure_months (float) + 2 soft str-enums (provider_scope, public_coord) + eligibility_note.

### Hazard list
| H# | Hazard | Field(s) | Status |
|---|---|---|---|
| H1 | Post-2007 Wet Kinderopvang: employer childcare contributions were abolished/absorbed into the public system; many CAOs mention childcare only historically or as "ceased" | `childcare_support_present` | **CONFIRMED** — many records have expired/repealed schemes; must NOT set present=True for an abolished scheme |
| H2 | Childcare mentioned only as scheduling/work-hours accommodation (not a monetary benefit) | `childcare_support_present` | **CONFIRMED** — roster flexibility is not childcare support |
| H3 | Support unit polymorphism (€/month, €/child/month, % of costs) | `support` (Amount) | NOTED — record as stated |
| H4 | provider_scope / public_coord soft enums | str fields | NOTED — map to closest enum value |
| H5 | mantelzorg (informal care) vs childcare conflation | `childcare_support_present` | NOTED — keep distinct |

### Recommendation: PROCEED. FM entries:
- **CHILDCARE_FM_01** — Abolished/historical schemes: do NOT set `childcare_support_present=True` for a scheme the CAO says has ceased/expired/been absorbed into the public Wet Kinderopvang system.
- **CHILDCARE_FM_02** — Scheduling/work-hours accommodation and mantelzorg are NOT childcare support; don't set the flag on their basis.
- **CHILDCARE_FM_03** — provider_scope / public_coord soft-enum mapping (any / contracted_only / sector_only / company_only; top_up_after_public_benefit / within_fiscal_max / etc.).

**HARD STOP CHECK:** no. Schema covers the patterns; the dominant pattern (no current employer childcare benefit) is correctly representable as all-False.

## Stage 1 — Scope filter
- 2,739 → **44 scoped records, 44 unique CAOs** (only CAOs with non-empty childcare source; far below 95 because most CAOs lack childcare content).

## Stage 2 — Deterministic layer
- L1: 0. L2: 132 field-specific. **132 items → 7 chunks** (1 batch).

## Stage 3 — Subagent review
- 7/7 chunks, Sonnet, 1 batch. 0 malformed rows. Subagents correctly excluded abolished schemes, scheduling accommodations, and mantelzorg.

## Stage 4 — Aggregate + audit (FINAL)
| Bucket | Count |
|---|---|
| Total worksheet items | 132 |
| **Clean wins** | **2** |
| NHR | 0 |
| Suppressed: noop-vs-CSV | 26 |

- **2 clean wins**: both `eligibility_note` extractions (573014, 301017) describing childcare eligibility scope. No monetary support, cap, FTE, or tenure values were extractable in any of the 44 CAOs — childcare support amounts simply aren't in these (post-2007) agreements.
- **0 NHR.** The extractor's childcare data was accurate: 26 subagent answers confirmed existing values (mostly correct `False`/empty), suppressed as no-ops.

## Stage 5b — Items for Hanna
1. **CHILDCARE_FM_01..03** added to [childcare.md](../conventions/failure_modes/per_topic/childcare.md).
2. **2 clean wins** ready to ship; **0 NHR**. Childcare is a near-empty topic in the modern CAO corpus — the QA mostly confirmed that there's little to correct.
