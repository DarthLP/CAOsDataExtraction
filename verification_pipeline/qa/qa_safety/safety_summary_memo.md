# Safety & Integrity — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-22)

### Language profile + source
Source `safety_information.md`, English-translated with retained Dutch (RI&E, Arbowet, arbodienst, bedrijfsarts, PSA, BHV, EHBO, PMO/PAGO, vertrouwenspersoon, klokkenluider, OR/PVT). Light token weight (median 825) — default caps fine.

### Schema shape
**12 boolean presence flags + 2 free-text notes** (`psa_measures_note`, `safety_note`); no numeric/amount fields and no umbrella `has_*` boolean. So QA is almost entirely boolean confirmation/flip work — like contract/bonus, most L2 triggers will be confirmations of existing values (handled by the noop-vs-CSV suppressor); genuine corrections are False→True flips the subagent finds in source.

### Hazard list

| H# | Hazard | Schema field(s) | Status |
|---|---|---|---|
| H1 | Harassment vs broader integrity protocol conflation | `harassment_protocol_present` vs `integrity_protocol_present` | NOTED — keep distinct (sexual-harassment/PSA vs general code of conduct) |
| H2 | Confidential counsellor (vertrouwenspersoon) internal vs external | `confidential_counsellor_present` | OK — either sets True |
| H3 | External reporting channel (klokkenluider/whistleblower) vs internal complaints | `reporting_channel_external` | NOTED — only EXTERNAL guarantee sets True |
| H4 | RI&E generic vs RI&E covering PSA specifically | `rie_psa_required` | RISK — schema requires RI&E to cover *psychosocial* risk; a generic Arbo RI&E mention may not qualify |
| H5 | Statutory Arbowet duties mistaken as CAO-specific provisions | all booleans | RISK — RI&E and arbodienst access are partly statutory; set True when the CAO references/guarantees them (the extractor convention is inclusive) |
| H6 | BHV/EHBO first-aid training vs psychosocial safety training | `safety_training_present` | NOTED — schema says "safety or psychosocial risk training"; BHV/EHBO qualifies |
| H7 | Arbodienst access vs preventive medical exam (PMO/PAGO) overlap | `arbodienst_access_provided` vs `preventive_medical_checkup_present` | NOTED — keep distinct |
| H8 | Over-eager `*_present` from any keyword mention (mirrors CONTRACT/BONUS/FRINGE boolean discipline) | all booleans | RISK — boolean discipline |

### Recommendation: PROCEED. FM entries to seed:
- **SAFETY_FM_01** — Statutory-vs-CAO booleans: RI&E (`rie_psa_required`) and arbodienst access are partly statutory; set True when the CAO explicitly references/guarantees them, but `rie_psa_required` specifically needs the RI&E to cover *psychosocial* risk (stress/burnout), not just generic Arbo.
- **SAFETY_FM_02** — Harassment vs integrity: a sexual-harassment/PSA protocol → `harassment_protocol_present`; a general code-of-conduct/integrity policy → `integrity_protocol_present`. Both can be true.
- **SAFETY_FM_03** — `reporting_channel_external` True ONLY for a guaranteed *external* channel (klokkenluidersregeling / external hotline), not an internal complaints procedure.
- **SAFETY_FM_04** — `*_present` boolean discipline: True only for a genuine provision of that exact type (mirrors CONTRACT_FM_04).

**HARD STOP CHECK:** no. Schema covers the patterns.

## Stage 1 — Scope filter
- 2,739 → **95 scoped records, 95 unique CAOs**.

## Stage 2 — Deterministic layer
- L1: 0 (psa_present/note already consistent in the CSV). L2: 409 field-specific (boolean false-negative scan). **409 items → 21 chunks.**

## Stage 3 — Subagent review
- 21/21 chunks, Sonnet, 2 batches. 0 malformed rows. Subagents applied the strict per-type thresholds well: generic Arbo/RI&E, anti-discrimination statements, arbodienst access, and MVO codes were correctly NOT counted as the specific provision asked.

## Stage 4 — Aggregate + audit (FINAL)

| Bucket | Count |
|---|---|
| Total worksheet items | 409 |
| **Clean wins** | **21** |
| NHR (audit-flagged) | 1 |
| Suppressed: noop-vs-CSV (False confirmations) | 368 |

- **21 clean wins** (17 `set_boolean` True-flips + 4 `psa_measures_note` extractions): integrity_protocol 5, psa_prevention_measures 4 (+4 notes), workload_monitoring 2, harassment_protocol 2, rie_psa_required 2, wellbeing_program 1, safety_training 1. All False→True flips where the extractor missed a genuine provision the subagent confirmed in source.
- **368 suppressed**: the safety booleans were overwhelmingly already `False` in the CSV and the source genuinely lacks those provisions, so the subagents' `False` answers correctly matched the CSV (no change). The extractor's safety booleans were highly accurate.
- **NHR: 1** (A17).

## Stage 5b — Items for Hanna
1. **SAFETY_FM_01..04** added to [safety.md](../conventions/failure_modes/per_topic/safety.md).
2. **NHR: 1 row.** Clean wins: **21** ready to ship — boolean presence flips + PSA-measure notes.
3. Numbers reflect the 2026-05-22 aggregator fixes (winner-value precedence + noop-vs-CSV + NHR-noop guard).
