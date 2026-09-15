"""
FIELD SEMANTICS — is each numeric field a TOTAL or an EXTRA, made EXPLICIT and TESTABLE.

The distinction was implicit ("by schema definition") and easy to get wrong; this registry
states it per field and check_battery.py asserts the pipeline actually treats it that way.

Classes:
  TOTAL_HARD  — the field is the whole amount AND a hard statutory minimum applies:
                blanks get the floor, below-floor values are LIFTED to it (role floor_lift).
                A worker legally receives at least the floor. (vacation, vakantiegeld,
                sick pay, care leave.)
  TOTAL_SOFT  — the field is the whole amount and a statutory floor/default/cap applies
                but is DEVIATABLE or a ceiling (role floor / default / cap): blanks are
                filled, a stated value is kept even if below a floor (lawful deviation) or
                masked if above a cap. (probation caps, ketenregeling, ATW rest/max hours,
                employer minimum notice.)
  TOTAL_OPEN  — the field is the whole amount but there is NO statutory number (or only an
                informational, non-imputed reference): nothing is imputed. (overtime pay %,
                triggers, full-time hours, commuting fiscal norm, pension employee share.)
  EXTRA       — the field is EXPLICITLY the amount ABOVE the statutory baseline, or a benefit
                with no statutory baseline at all: baseline is 0 and a STATUTORY value must
                NEVER be imputed into it (that would invent generosity).
                (severance above transitievergoeding, all bonuses, training budget/time,
                homeoffice stipend, meal/relocation allowances, early-retirement age.)

PRESENCE-GATED ZERO-FILL (promoted 2026-07-06): for the EXTRA amount fields where 0 truly
means "least generous / nothing" (sign +1) AND a presence boolean actually discriminates
(<90% present), a blank is set to 0 when that boolean is False (genuine absence). This is
NOT statutory imputation — it comes from the CAO's own presence flag, so a missing benefit
lowers the magnitude score instead of being skipped. Applies to exactly 5 fields:
  bonus_thirteenth_month_amt   (gate bonus_thirteenth_month)
  fringe_meal_benefit_amt      (gate fringe_meal_benefit_present)
  fringe_relocation_allowance  (gate fringe_relocation_allowance_present)
  homeoffice_stipend           (gate homeoffice_stipend_present)
  homeoffice_entitlement       (gate homeoffice_has_homeoffice_rights)
NOT applied to: pension_retirement_age_early (sign −1, 0=absurd age); training ×3 &
bonus_fixed_lump (boolean ~98% True — a blank = amount unstated, not absent);
severance_extra (no boolean, high extraction noise). Those stay available-case.
The pre-zerofill score is kept as <topic>_z_availcase for comparison.

Rule the test enforces: EXTRA fields must carry NO imputing STATUTORY role
(floor/floor_lift/default) — the zero-fill above is a separate, presence-driven mechanism.
"""

FIELD_SEMANTICS = {
    # ---- absence: all TOTAL_HARD (one-sided dwingend recht) ----
    "leave_vacation_time_value": ("TOTAL_HARD", "min 4x weekly days (BW 7:634)"),
    "leave_vacation_bonus_value": ("TOTAL_HARD", "min 8% (WML 15)"),
    "leave_sickpay_continuation_value": ("TOTAL_HARD", "min 70% yr-1 (BW 7:629)"),
    "leave_sickpay_duration_value": ("TOTAL_HARD", "min 104 wk since 2004 (BW 7:629)"),
    "leave_short_term_care_value": ("TOTAL_HARD", "min 2x weekly hrs (WAZO 5:1)"),
    "leave_short_term_care_pay_value": ("TOTAL_HARD", "min 70% (WAZO 5:6)"),
    "leave_long_term_care_value": ("TOTAL_HARD", "min 6x weekly hrs (WAZO 5:9)"),
    "leave_long_term_care_pay_value": ("TOTAL_HARD", "statutorily unpaid (floor 0)"),
    # ---- term ----
    "term_probation_fixedterm_value": ("TOTAL_SOFT", "cap 2 mo (BW 7:652)"),
    "term_probation_indef_value": ("TOTAL_SOFT", "cap 2 mo (BW 7:652)"),
    "term_notice_min_floor_value": ("TOTAL_SOFT", "floor 1 mo employer (BW 7:672)"),
    "term_employer_notice_value": ("TOTAL_OPEN", "tenure-graded, informational only"),
    "term_employer_notice_range_max": ("TOTAL_OPEN", "top tier 4 mo, informational only"),
    "term_severance_extra_value": ("EXTRA", "severance ABOVE transitievergoeding"),
    # ---- contract ----
    "contract_ketenregeling_max_contracts_value": ("TOTAL_SOFT", "default 3 (BW 7:668a)"),
    "contract_ketenregeling_max_duration_value": ("TOTAL_SOFT", "default 36/24/36 mo (era)"),
    "contract_full_time_hours_value": ("TOTAL_OPEN", "no statute (sector norm)"),
    "contract_workhours_adjustment_tenure_requirement_value": ("TOTAL_OPEN", "Wfw 26wk, informational only"),
    # ---- overtime ----
    "overtime_allowance_value": ("TOTAL_OPEN", "no statutory premium"),
    "overtime_allowance_range_max": ("TOTAL_OPEN", "no statutory premium"),
    "overtime_shift_allowance_range_min": ("TOTAL_OPEN", "no statutory premium"),
    "overtime_unfavourable_hours_allowance_value": ("TOTAL_OPEN", "no statutory premium"),
    "overtime_trigger_daily_value": ("TOTAL_OPEN", "no statute (contractual)"),
    "overtime_trigger_weekly_value": ("TOTAL_OPEN", "no statute (contractual)"),
    "overtime_min_rest_between_shifts_value": ("TOTAL_SOFT", "floor 11h, deviatable to 8h (ATW)"),
    "overtime_max_hours_per_week_value": ("TOTAL_SOFT", "cap 60h (ATW)"),
    "overtime_max_hours_per_day_value": ("TOTAL_SOFT", "cap 12h (ATW)"),
    # ---- training / bonus / fringe / homeoffice — EXTRA (baseline 0) ----
    "training_time_yearly_value": ("EXTRA", "paid training beyond mandatory; no statutory number"),
    "training_budget_value": ("EXTRA", "no statutory number"),
    "training_cost_reimbursement_value": ("EXTRA", "no statutory number"),
    "bonus_thirteenth_month_amt_value": ("EXTRA", "13th month not statutory"),
    "bonus_fixed_annual_lump_value": ("EXTRA", "not statutory"),
    "fringe_commuting_allowance_value": ("TOTAL_OPEN", "fiscal €/km norm, informational only"),
    "fringe_meal_benefit_amt_value": ("EXTRA", "not statutory"),
    "fringe_relocation_allowance_value": ("EXTRA", "not statutory"),
    "homeoffice_stipend_value": ("EXTRA", "thuiswerkvergoeding fiscal norm, not an entitlement"),
    "homeoffice_entitlement_value": ("EXTRA", "no statutory WFH days"),
    # ---- pension ----
    "pension_employee_contrib_value": ("TOTAL_OPEN", "no statutory limit on employee share"),
    "pension_accrual_rate_value": ("TOTAL_OPEN", "Witteveen cap informational; clamp only"),
    "pension_franchise_value": ("TOTAL_OPEN", "fiscal minimum informational; clamp only"),
    "pension_retirement_age_early_value": ("EXTRA", "early-retirement option, no statute"),
}

IMPUTING_ROLES = {"floor", "floor_lift", "default"}   # roles that WRITE a value into a blank
