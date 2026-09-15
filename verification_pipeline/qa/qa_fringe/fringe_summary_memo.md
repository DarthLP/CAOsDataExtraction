# Fringe Benefits — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-22)

### Language profile + source
Source `fringe_benefits_information.md`, English-translated with retained Dutch (OV, fiets/leasefiets, zorgverzekering, verhuiskostenvergoeding, reiskostenvergoeding). Light token weight (median 648, p90 1,352) — default per-item caps (2,000/4,000) are fine.

### Topic scope
- Content frequency across 1,488 non-empty blocks: OV/transport 1,345, health insurance 816, meal 804, relocation 762, €/km 643, bike 435.
- After Stage 1 filter: ~95 records.

### Hazard list

| H# | Hazard | Schema field(s) | Status |
|---|---|---|---|
| H1 | Commuting allowance unit polymorphism: €/km vs €/month vs OV-card vs % | `commuting_allowance` (Amount; schema example "EUR per km") | **CONFIRMED** — 643 blocks use per-km (often statutory 0.19/0.21/0.23), but many give €/month or OV reimbursement |
| H2 | OV / public-transport reimbursement is the dominant form (1,345) — distinct from €/km | `commuting_allowance_present` + unit | **CONFIRMED** — record unit as stated ("OV-card", "100% public transport") |
| H3 | Statutory km rate (0.19 → 0.21 → 0.23 over years) mistaken as a CAO-specific benefit | `commuting_allowance_value` | NOTED — still record it; it's the reimbursement rate |
| H4 | Meal benefit type as soft enum (free_meals / subsidised_canteen / meal_vouchers / meal_allowance) | `meal_benefit_type` (str) | NOTED — soft enum; record closest stated form |
| H5 | `has_fringe_benefits` over-claim from any allowance mention (mirrors BONUS_FM_04 / CONTRACT_FM_04) | `has_fringe_benefits` (bool) | RISK — boolean discipline |
| H6 | Health-insurance collective discount vs employer contribution — both set the flag | `health_insurance_support_present` + note | OK — schema covers either |
| H7 | Insurance/savings benefit vs health insurance overlap | two separate `_present` flags | NOTED — keep distinct |
| H8 | Relocation/housing support amount as €one-off vs €/month vs "actual costs" | `relocation_allowance` (Amount) | NOTED |

### Recommendation: PROCEED. FM entries to seed:
- **FRINGE_FM_01** — Commuting allowance unit polymorphism: record exactly (`EUR per km`, `EUR per month`, `OV-card`, `% of public transport`); do not convert. Statutory km rate still recorded.
- **FRINGE_FM_02** — `has_fringe_benefits` / `*_present` boolean discipline: True only for a genuine benefit of that type (mirrors BONUS_FM_04).
- **FRINGE_FM_03** — Meal benefit type soft-enum: map to the closest of free_meals / subsidised_canteen / meal_vouchers / meal_allowance; else 'other'/'unspecified'.

**HARD STOP CHECK:** no. Schema covers the patterns.

## Stage 1 — Scope filter
- 2,739 → **95 scoped records, 95 unique CAOs**.

## Stage 2 — Deterministic layer
- L1: 44 R1 (present-flag False + paired data populated, mostly meal_benefit), 1 ENUM. L2: 693 field-specific. **738 items → 37 chunks.**

## Stage 3 — Subagent review
- 37/37 chunks, Sonnet, ~4 batches. 0 malformed CSV rows. Subagents showed good discipline: distinguished commuting cost-allowances from travel-time pay, excluded statutory items, kept health-insurance vs insurance/savings distinct.

## Stage 4 — Aggregate + audit (FINAL)

| Bucket | Count |
|---|---|
| Total worksheet items | 738 |
| **Clean wins** | **34** |
| NHR (audit-flagged) | 13 |
| Suppressed: noop-vs-CSV (confirmations) | 234 |

- **34 clean wins**: 24 `correct_in_place` value extractions (7 commuting rates e.g. 0.19/0.23/0.37 €/km, 7 relocation amounts e.g. €5446 one-off / 12% of salary, 2 meal amounts €5.45/€3.40/day) + 10 `set_boolean` True-flips (insurance_or_savings 4, relocation_present 2, meal_present 2, internet/phone 1, commuting 1).
- **NHR: 13** — all A17, the canonical-unit cases ("EUR per km" / "EUR one-off") where the value is verbatim in source but the canonical unit string isn't (source writes "€0.19/km").

## Stage 5b — Items for Hanna
1. **FRINGE_FM_01..03** added to [fringe.md](../conventions/failure_modes/per_topic/fringe.md).
2. **NHR: 13 rows** (all A17 unit-canonicalization — values are right, units need a glance).
3. **Clean wins: 34** ready to ship — commuting/relocation/meal amounts + benefit-presence flips.

> **Note:** these numbers reflect the 2026-05-22 aggregator fixes (winner-value precedence + noop-vs-CSV + NHR-noop). See README status board for the project-wide correction.
