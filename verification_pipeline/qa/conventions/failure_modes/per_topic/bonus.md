# Per-Topic Failure Modes — Bonus

Added 2026-05-22 from bonus run (95 scoped records, 19 clean wins, 20 NHR). Source shared with wage (`wage_information.md`).

## BONUS_FM_01 — 13th-month / eindejaarsuitkering unit polymorphism

**When it applies**: source states a year-end payment as "1 month salary" / "8.33%" / "X% of annual salary" / "X% of earned wage". An eindejaarsuitkering (end-of-year payment) at a stated percentage IS the 13th-month equivalent.

**Subagent action**: set `thirteenth_month=True` and record `thirteenth_month_amt` value+unit exactly as stated (e.g. value=5, unit="%"; value=1, unit="monthly wage"). Do NOT convert between % and months.

**CSV impact**: `bonus_thirteenth_month`, `bonus_thirteenth_month_amt_value`, `bonus_thirteenth_month_amt_unit`.

**Example**: CAO 433 "year-end bonus of 5%" → thirteenth_month=True, amt=5%. CAO 1345 "3% of earned wage" → amt=3%.

## BONUS_FM_02 — Vakantiegeld / holiday allowance is statutory wage, NOT a bonus

**When it applies**: source mentions the 8% vakantiegeld / vakantietoeslag / holiday allowance. This is statutory wage (every Dutch employee gets it), not a discretionary or structural bonus.

**Subagent action**: do NOT set `has_bonus_schemes`, `thirteenth_month`, or any bonus flag True on the basis of vakantiegeld. Exclude it entirely.

**CSV impact**: prevents false-positives on `bonus_has_bonus_schemes` / `bonus_thirteenth_month`.

## BONUS_FM_03 — Jubilee vs retirement gratuity

**When it applies**: source describes a service-anniversary / jubileumuitkering (e.g. 12.5, 25, 40 years of service) OR a lump sum at retirement/AOW/exit.

**Subagent action**: mid-career service anniversary → `seniority_loyalty_bonus=True`; lump sum *at retirement or end-of-service exit* → `retire_gratuity_present=True` (+ note). A 25-year jubilee paid while still employed is NOT a retirement gratuity. Both can be true if both exist.

**CSV impact**: `bonus_seniority_loyalty_bonus`, `bonus_retire_gratuity_present`, `bonus_retirement_gratuity_note`.

**Example**: CAO 1291 "25/40-year jubilee gratuity" → seniority_loyalty_bonus=True, retire_gratuity_present=False.

## BONUS_FM_04 — has_bonus_schemes / *_present over-claim discipline

**When it applies**: the extractor (or a hasty reading) sets a `*_present` boolean True merely because the topic word appears. One-off payments to *existing* staff (CAO-cycle catch-up, transitional compensation), discretionary allowances, and base-pay step progression are NOT bonus schemes.

**Subagent action**: set a `*_present` flag True only for a genuine recurring/structural incentive of that specific type. A one-off €X to current employees is not a sign-on bonus; appraisal-driven scale progression is not a performance bonus; a turnover commission is not profit-sharing. Mirrors CONTRACT_FM_04 boolean discipline.

**CSV impact**: all `bonus_*_present` booleans + `bonus_has_bonus_schemes`.

**Example**: CAO 824 "one-off €382.50 to employees in service on 1 Jul 2022" → sign_on_bonus_present=False (existing staff, not new hires).
