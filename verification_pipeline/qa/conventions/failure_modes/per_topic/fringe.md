# Per-Topic Failure Modes — Fringe Benefits

Added 2026-05-22 from fringe run (95 scoped records, 34 clean wins, 13 NHR).

## FRINGE_FM_01 — Commuting allowance unit polymorphism

**When it applies**: commuting reimbursement stated as €/km (often the statutory 0.19/0.21/0.23), €/month, OV-card / public-transport reimbursement, or % of fare.

**Subagent action**: record `commuting_allowance` value + unit exactly as stated ("EUR per km", "EUR per month", "OV-card", "% of public transport"); do not convert. The statutory per-km rate is still a valid reimbursement value. Distinguish a commuting *cost* allowance from travel-*time* pay (the latter is wage, not a fringe benefit).

**CSV impact**: `fringe_commuting_allowance_present` + `_value` + `_unit`.

## FRINGE_FM_02 — `has_fringe_benefits` / `*_present` boolean discipline

**When it applies**: a `*_present` flag is set True merely because the topic word appears. Travel-time pay, in-kind provisions without a stated amount, employee-funded schemes, and disability insurance (WGA/WIA) are not the benefit the field measures.

**Subagent action**: set a `*_present` flag True only for a genuine employer-provided benefit of that exact type. Keep `health_insurance_support` (employer contribution / collective discount) distinct from `insurance_or_savings_benefit` (employer-paid financial benefit) and from disability insurance (neither).

**CSV impact**: all `fringe_*_present` booleans.

## FRINGE_FM_03 — Meal benefit type soft-enum

**When it applies**: a meal benefit exists. `meal_benefit_type` is a soft enum.

**Subagent action**: map to the closest of `free_meals`, `subsidised_canteen`, `meal_vouchers`, `meal_allowance`; use `other`/`unspecified` if none fit. Record `meal_benefit_amt` only when a per-meal/per-day amount is stated (e.g. €5.45 per day); leave empty for in-kind/actual-cost meals.

**CSV impact**: `fringe_meal_benefit_present`, `fringe_meal_benefit_type`, `fringe_meal_benefit_amt_value/_unit`.

**Example**: CAO 533 "meal allowance of €5.45 per worked day" → present=True, type=meal_allowance, amt=5.45 EUR per day. CAO 750 free in-kind meals → present=True, type=free_meals, amt empty.
