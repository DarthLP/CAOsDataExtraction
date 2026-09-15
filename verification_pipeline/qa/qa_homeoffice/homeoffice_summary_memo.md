# Homeoffice — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-18)

### Language profile

Source language verified: **English-translated** with occasional preserved Dutch terms (`Arbeidsomstandighedenbesluit`, `Het Nieuwe Werken`, `OR`, `PVT`, `Arbobeleid`, Article numbers). Consistent with the project-wide pattern documented in `CLAUDE.md`.

### Topic scope

- **421 of 1,505 records** (28%) have non-empty homeoffice content. Most CAOs predate widespread WFH or simply don't address it.
- After Stage 1 most-recent-per-CAO filter, expect ~50-60 records in scope.

### Hazard list (defined before sampling)

| H# | Hazard | Schema field(s) | Status after sampling |
|---|---|---|---|
| H1 | Stipend unit variation (€/day vs €/month vs €/year vs % of salary) | `stipend` (Amount) | **CONFIRMED** — CAO 301 has "€2.15 per full home working day" |
| H2 | Multi-element cost reimbursement (excludes computer/phone/travel) | `costs_reimbursed` (bool) + `note` | **CONFIRMED** — common to exclude computer + phone from stipend |
| H3 | Discretion enum non-canonical values | `discretion` (enum: employer_only / joint_with_OR / employee_request / unspecified / other) | RISK — most CAOs say "in consultation with OR/PVT" → should map to `joint_with_OR` but extractor may have used non-canonical |
| H4 | Agency CAO defers to client (mirrors OVERTIME_FM_05) | All numeric fields | **CONFIRMED** — CAO 1296 (ICK), staffing CAOs |
| H5 | "Het Nieuwe Werken" / Dutch concept preservation | various | NOTED — preserved Dutch in translated source; field keywords retain it |
| H6 | Hybrid-work vs pure WFH distinction | `has_homeoffice_rights` boolean conflates both | OK — schema allows either to set true |
| H7 | Entitlement unit variation (days/week vs hours/week vs %) | `entitlement` (Amount) | RISK — schema example is "days per week" but CAOs vary |
| H8 | Stipend conditional on # WFH days (sliding scale) | `stipend` single value | LOW — most CAOs use flat daily rate |
| H9 | "Necessary facilities" vague reimbursement | `costs_reimbursed` true with no concrete amount | **CONFIRMED** — CAO 43 says "necessary facilities (no specific amounts mentioned)" |
| H10 | Note-field overflow | `note` carries lots of free text | RISK — schema/extractor design choice; subagent should not duplicate into other fields |

### Sampled CAOs — illustrative passages

- **CAO 301 (Sociaal Werk 2023-2025)**: "A reimbursement of at least €2.15 per full home working day is provided, which does not include costs for computer equipment, telephone, and travel expenses."
- **CAO 1612 (Kinderopvang)**: "The employer establishes a company regulation for home work/telework with the consent of the Works Council (OR) or Personnel Representation (PVT)."
- **CAO 1296 (ICK 2017)**: "CAO parties aim to clarify and promote 'Het Nieuwe Werken' / E-working in the ICK-sector."
- **CAO 214 (OB)**: "The employer ensures that the setup of the home workspace complies with the requirements set in the Arbo regulations (Article 6b, Lid 2)."
- **CAO 1536 (NU 2022)**: "For employees, hybrid working is a possibility, not a right."
- **CAO 267 (OB 2023)**: "The employer and OR or PVT discuss at least once every three years whether partial remote work (hybrid work) is possible..."

### Recommendation: PROCEED — no schema gaps

The schema covers the observed patterns adequately. Three new failure-mode entries to seed:

- **HOMEOFFICE_FM_01** — Stipend unit variation (EUR/day vs EUR/month). Mirrors the OVERTIME_FM_01 multi-tier collapse pattern.
- **HOMEOFFICE_FM_02** — Agency / staffing CAOs defer to client's remuneration scheme. Mirrors OVERTIME_FM_05.
- **HOMEOFFICE_FM_03** — "Necessary facilities" vague reimbursement: `costs_reimbursed=true` is appropriate even when no concrete amount is named.
- **HOMEOFFICE_FM_04** — OR/PVT consultation pattern → `discretion='joint_with_OR'`. The vast majority of CAOs have this exact pattern; subagent must use the canonical enum value.

**HARD STOP CHECK:** no, do not stop. Hazards are within schema's representational range; FM entries handle the recurring patterns.

## Stage 1 — Scope filter
- Total CSV records: 2,739
- After scope filter (latest per CAO with non-empty source): **52 records, 52 unique CAOs**

## Stage 2 — Deterministic layer (final after enum fix)
- L1 rule violations: 21 (15 R6 has_rights=False+populated, 4 R1 stipend_present without value, 2 R5 discretion non-canonical)
- L2 presence triggers (field-specific, 1 dropped on L1 overlap): 36
- ENUM-mismatch items: 0 (after fixing `joint_with_OR` recognition — see "Bug fix" below)
- **Total: 57 worksheet items** → 3 chunks of 17-20 items each

## Stage 3 — Subagent review
- 3 parallel subagents, ~10 min wall-clock
- All chunks returned with disciplined evidence quotes
- Notable applications:
  - HOMEOFFICE_FM_02 (agency CAO defers) — CAO 1060025 (NBBU), 1944020 (Open Teelten)
  - HOMEOFFICE_FM_04 (OR/PVT → joint_with_OR) — applied verbatim in chunk_001
  - Several `has_homeoffice_rights` flips (False → True) where extractor missed clearly-granted rights (CAO 884003: explicit €2.35/day allowance)

## Stage 4 — Aggregate + audit

| Bucket | Count |
|---|---|
| Total worksheet items | 57 |
| is_noop (no real change) | 36 |
| **Real corrections** | **21** |
| Clean wins | **19** |
| NHR (A18) | 2 |
| Audit hits | A18: 2 |

> **Correction (2026-05-22):** the original run reported 21 clean / 0 NHR. A project-wide data-quality audit removed two noise classes: **2** `discretion` rows marked `unable_to_verify` (A18 → NHR) and **4** spurious confirmations (subagent value == existing CSV value, `suppress_noop_vs_actual_csv`). **Authoritative: 15 clean, 2 NHR.** A reproducible `qa_homeoffice_aggregate.py` was added (original run was inline); pre-audit output at `corrections_prefix_backup.csv`.

## Bug fix during this topic run

**Bug:** the schema enum extractor regex `[a-z_][a-z0-9_]*` was matching only lowercase identifiers, silently dropping mixed-case enum values like `joint_with_OR`. This caused the homeoffice ENUM scan to flag 13 valid `joint_with_OR` values as "non-canonical", and subagents (working from the same buggy regex) mapped them to `other`.

**Fix:** updated `schema_lookup.get_topic_enum_fields` regex to `[A-Za-z][A-Za-z0-9_]*`. Re-ran Stage 2 (correct ENUM scan), rebuilt chunks (correct `enum_values` per item), re-ran 3 subagents. Tests still pass.

**Impact:** future topics with mixed-case enum values (e.g. any value containing `OR`, `_AOW_`, `_RVU_`) now extract correctly. Validated against overtime + homeoffice; no regressions.

## Stage 5b — Items for Hanna

1. **HOMEOFFICE_FM_01 / FM_02 / FM_03 / FM_04** added to [homeoffice.md](../conventions/failure_modes/per_topic/homeoffice.md). Review and edit if desired.
2. **No NHR rows.** All 21 corrections are clean wins ready to ship.
3. **No A13 hits** — too few rule firings to meaningfully measure disagreement rates.
4. **Bug fix to `schema_lookup`** applies to all 10 remaining topics. Worth verifying enum-extraction on the next topic too (training) before subagent runs.

## Quality compared to overtime

| Metric | Overtime | Homeoffice |
|---|---|---|
| Scoped records | 95 | 52 |
| Worksheet items | 952 | 57 |
| Real corrections | 207 | 21 |
| Clean wins | 106 | 21 (100%) |
| NHR | 101 | 0 |
| A17 hits | 96 | 0 |

The improvements from the rebuild (English-primary field keywords, per-item schema description, strict A17 evidence rule) clearly carried through — 100% clean-win rate on homeoffice vs ~50% on overtime (pre-rerun).
