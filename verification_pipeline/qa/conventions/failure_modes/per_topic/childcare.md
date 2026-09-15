# Per-Topic Failure Modes — Childcare

Added 2026-05-22 from childcare run (44 scoped records, 2 clean wins, 0 NHR). Sparse topic — post-2007 Wet Kinderopvang moved childcare funding to a public tax-credit system, so employer childcare schemes are largely absent/historical in the corpus.

## CHILDCARE_FM_01 — Abolished / historical schemes are not "present"

**When it applies**: a CAO mentions a childcare contribution that has ceased, expired, been repealed, or been absorbed into the public Wet Kinderopvang system (common phrasing: "as of [year] the childcare allowance is discontinued / replaced by the statutory scheme").

**Subagent action**: do NOT set `childcare_support_present=True` for a scheme the source says no longer applies. Set False (or leave as the CSV has it).

**CSV impact**: `childcare_childcare_support_present`, `childcare_funding_sector_fund`.

## CHILDCARE_FM_02 — Scheduling accommodation / mantelzorg ≠ childcare support

**When it applies**: the CAO offers work-hours/roster flexibility for parents, or informal-care (mantelzorg) provisions — but no childcare cost support.

**Subagent action**: these do NOT set `childcare_support_present`. Childcare support means a monetary contribution, discount, in-house facility, or priority access for *childcare costs/places*.

**CSV impact**: `childcare_childcare_support_present`, `childcare_inhouse_present`, `childcare_discount_present`, `childcare_priority_access`.

## CHILDCARE_FM_03 — provider_scope / public_coord soft-enum mapping

**When it applies**: childcare support exists and the source describes which providers qualify or how it interacts with public subsidy.

**Subagent action**:
- `provider_scope` → closest of `any` (all registered/LRK providers), `contracted_only`, `sector_only`, `company_only`, `unspecified`, `other`.
- `public_coord` → closest of `top_up_after_public_benefit`, `within_fiscal_max`, `gross_before_public_benefit`, `unspecified`, `other`.

**CSV impact**: `childcare_provider_scope`, `childcare_public_coord`.

**Note**: across 44 childcare-bearing CAOs, no extractable monetary support/cap/FTE/tenure values were found — modern Dutch CAOs do not carry employer childcare amounts. The extractor's childcare booleans were accurate; QA mostly confirmed there is little to correct.
