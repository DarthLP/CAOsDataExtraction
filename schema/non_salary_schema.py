"""
Non-Salary Schema Definitions for CAO Data Extraction

This module contains the Pydantic schema definitions and prompt template
used for non-salary data extraction from CAO documents.

USAGE:
    from schema.non_salary_schema import (
        GeneralInfo, BonusesInfo, WageScalesInfo, PensionInfo, LeaveInfo, 
        TerminationInfo, OvertimeInfo, TrainingInfo, HomeofficeInfo, 
        ContractTypeInfo, SafetyInfo, ChildcareInfo, AIInfo, FringeBenefitsInfo,
        NonSalaryPart1, NonSalaryPart2, NonSalaryPart3, NON_SALARY_PROMPT
    )
"""

from typing import List, Optional
from pydantic import BaseModel, Field

# Import Amount and AmountRange from salary_schema
from .salary_schema import Amount, AmountRange


# ----------------------------
# GENERAL INFORMATION
# ----------------------------
class GeneralInfo(BaseModel):
    """Schema for general contract information (record exactly as stated in the CAO)."""
    start_date_contract: str = Field(
        default="",
        description="CAO validity start date (YYYY-MM-DD)."
    )
    expiry_date_contract: str = Field(
        default="",
        description="CAO validity end date (YYYY-MM-DD)."
    )
    signing_date: str = Field(
        default="",
        description="Date the CAO was signed by the parties (YYYY-MM-DD)."
    )

    # Retroactivity — record only when explicitly stated
    retroactive_applies: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states that (some) terms apply retroactively."
    )
    retroactive_start_date: str = Field(
        default="",
        description="Start date of retroactive application (YYYY-MM-DD). Leave empty if retroactive_applies = false."
    )
    retroactive_end_date: str = Field(
        default="",
        description="End date of retroactive application (YYYY-MM-DD). Leave empty if retroactive_applies = false."
    )
    retroactive_scope_note: str = Field(
        default="",
        description="What is retroactive (e.g., wage scales, allowances). Leave empty if retroactive_applies = false."
    )
    retroactive_backpay_due: Optional[bool] = Field(
        default=None,
        description="Set true only if back-pay for the retro period is explicitly required, false if not explicitly stated — Omit if retroactive_applies = false."
    )
    retroactive_backpay_terms: str = Field(
        default="",
        description="Back-pay rules as stated. Leave empty if retroactive_applies = false OR retroactive_backpay_due = false."
    )
    retroactive_exclusions_note: str = Field(
        default="",
        description="Groups or items explicitly excluded from retroactivity. Leave empty if retroactive_applies = false."
    )
    retroactive_interest_or_surcharge: str = Field(
        default="",
        description="Interest/surcharge on late back-pay, if stated. Leave empty if retroactive_applies = false OR retroactive_backpay_due = false."
    )

    # Scope / classification
    sbi_code_primary: str = Field(
        default="",
        description="Primary SBI code (e.g., '41.20')."
    )
    sbi_code_secondary: str = Field(
        default="",
        description="Secondary SBI code(s), if any (comma-separated)."
    )
    sbi_code_version: str = Field(
        default="",
        description="Version of the SBI classification (e.g., 'SBI 2008')."
    )

    deviation_allowed_company_level: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly permits company-level deviations from CAO terms."
    )
    cao_scope_type: str = Field(
        default="unspecified",
        description="CAO scope type (e.g., 'sectoral', 'single_company', 'group', 'association_limited', 'occupational_niche', 'unspecified', 'other')."
    )
    firm_name: str = Field(
        default="",
        description="Company name — ONLY if cao_scope_type = 'single_company'."
    )
    firm_cao_scope_description: str = Field(
        default="",
        description="Brief description of firm-level scope, as stated."
    )

    # AVV (generally binding)
    avv_applies: bool = Field(
        default=False,
        description="Set true only if the CAO is/was declared generally binding (AVV)."
    )
    avv_start_date: str = Field(
        default="",
        description="AVV start date (YYYY-MM-DD) — ONLY if avv_applies = true."
    )
    avv_end_date: str = Field(
        default="",
        description="AVV end date (YYYY-MM-DD) — ONLY if avv_applies = true."
    )


# ----------------------------
# BONUSES (WAGE)
# ----------------------------
class BonusesInfo(BaseModel):
    has_bonus_schemes: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly provides any recurring or structural bonus/incentive beyond base salary."
    )

    sign_on_bonus_present: bool = Field(
        default=False,
        description="Set true only if the CAO includes a sign-on bonus for new hires."
    )
    sign_on_bonus: Optional[Amount] = Field(
        default=None,
        description="Sign-on bonus amount with unit (e.g., value=500, unit='EUR one-off') — Omit if sign_on_bonus_present = false."
    )

    thirteenth_month_present: bool = Field(
        default=False,
        description="Set true only if the CAO grants a 13th month of salary (or equivalent)."
    )
    thirteenth_month: Optional[Amount] = Field(
        default=None,
        description="13th-month value with unit (e.g., value=1.0, unit='monthly wage' or value=50, unit='% of annual salary') — Omit if thirteenth_month_present = false."
    )

    fixed_annual_lump: Optional[Amount] = Field(
        default=None,
        description="Fixed recurring lump sum per year with unit (e.g., value=1000, unit='EUR per year')."
    )

    profit_sharing_present: bool = Field(
        default=False,
        description="Set true only if a profit-sharing scheme is explicitly stated."
    )
    profit_sharing_note: str = Field(
        default="",
        description="Short description of how profit-sharing is calculated (e.g., '% of company profit'). Leave empty if profit_sharing_present = false."
    )

    performance_bonus_present: bool = Field(
        default=False,
        description="Set true only if a performance/target-based bonus beyond base pay is explicitly stated."
    )

    job_specific_allowances_present: bool = Field(
        default=False,
        description="Set true only if role-linked allowances are explicitly stated (e.g., cashier allowance, driver's license allowance)."
    )
    job_specific_allowances_note: str = Field(
        default="",
        description="Short description of role-linked allowances as stated. Leave empty if job_specific_allowances_present = false."
    )

    qualification_bonus_present: bool = Field(
        default=False,
        description="Set true only if a monetary bonus for obtaining specific diplomas/certifications is explicitly stated."
    )
    qualification_bonus_note: str = Field(
        default="",
        description=(
            "Short note exactly as stated describing qualification-related bonuses — include whether it is one-off or recurring (e.g., monthly), the amount or percentage, eligible diplomas/certifications, and any other stated conditions (e.g., job relevance, repayment if leaving early). Leave empty if qualification_bonus_present = false."
        )
    )

    seniority_or_loyalty_bonus_present: bool = Field(
        default=False,
        description="Set true only if a bonus/gratuity for long service or seniority is explicitly stated."
    )

    retirement_gratuity_present: bool = Field(
        default=False,
        description="Set true only if a lump sum at retirement or long-service exit is explicitly stated."
    )
    retirement_gratuity_note: str = Field(
        default="",
        description="Description/value of lump sum at retirement or long-service exit exactly as stated (e.g., '1 month salary after 25 years'). Leave empty if retirement_gratuity_present = false."
    )


# ----------------------------
# WAGE SCALES & PROGRESSION
# ----------------------------
class WageScalesInfo(BaseModel):
    entry_step_by_experience_present: bool = Field(
        default=False,
        description="Set true only if the CAO allows a higher initial step/trede based on relevant experience/competence."
    )
    entry_step_by_experience_rule: str = Field(
        default="",
        description="Short rule text exactly as stated (e.g., '≥3 yrs relevant exp → start ≥ Trede 3; manager discretion'). Leave empty if entry_step_by_experience_present = false."
    )

    personal_allowance_at_max_scale_present: bool = Field(
        default=False,
        description="Set true only if a personal pay supplement ('persoonlijke toeslag') is granted when an employee reaches the maximum of the wage scale or retains a higher wage after reclassification."
    )
    personal_allowance_rule_text: str = Field(
        default="",
        description="Basis/%/amount, duration, pensionability, and any phase-out or indexation exactly as stated. Leave empty if personal_allowance_at_max_scale_present = false."
    )

    performance_step_variation_present: bool = Field(
        default=False,
        description="Set true only if the employer may grant extra steps or withhold steps based on performance."
    )
    performance_step_variation_rule: str = Field(
        default="",
        description="Criteria/limits exactly as stated (e.g., 'max +2 steps after excellent rating; withholding requires PIP & OR notification'). Leave empty if performance_step_variation_present = false."
    )


# ----------------------------
# PENSION INFORMATION
# ----------------------------
class PensionInfo(BaseModel):
    """Schema for pension information (record values exactly as stated; do not infer statutory comparisons)."""
    has_pension_scheme: bool = Field(
        default=False,
        description="Set true only if any pension scheme beyond AOW is mentioned; false if none is mentioned."
    )
    pension_type: str = Field(
        default="unspecified",
        description="Scheme type (e.g., 'DB' = Defined Benefit, 'DC' = Defined Contribution, 'hybrid' = combination, 'unspecified', 'other')."
    )
    mandatory_participation: bool = Field(
        default=False,
        description="Set true only if participation in a (sector) pension fund is explicitly mandatory."
    )

    # Selection rule for 'typical' group (if needed for single values)
    selection_rule_pension: str = Field(
        default="unspecified",
        description=(
            "How the 'typical' group was chosen when multiple rates exist (e.g., 'majority_headcount', 'office_vs_field_rule', 'base_tier', 'latest_year', 'other', 'unspecified')."
        )
    )

    employee_contrib: Optional[Amount] = Field(
        default=None,
        description="Employee pension contribution for the chosen group with unit (e.g., value=5.5, unit='% of salary')."
    )
    accrual_rate: Optional[Amount] = Field(
        default=None,
        description="Annual accrual rate for the chosen group with unit (e.g., value=1.875, unit='% per year')."
    )
    franchise: Optional[Amount] = Field(
        default=None,
        description="Franchise amount for the CAO period with unit (e.g., value=14400, unit='EUR per year')."
    )

    retirement_age_normal: Optional[Amount] = Field(
        default=None,
        description="Normal retirement age (e.g., value=67, unit='years')."
    )
    retirement_age_early: Optional[Amount] = Field(
        default=None,
        description="Early retirement age (e.g., value=63, unit='years')."
    )
    retirement_age_deferred: Optional[Amount] = Field(
        default=None,
        description="Deferred/postponed retirement age (e.g., value=70, unit='years')."
    )

    accrual_during_statutory_leaves: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states accrual continues during statutory leaves."
    )
    accrual_during_illness_year2: bool = Field(
        default=False,
        description="Set true only if full accrual continues in the 2nd year of illness is explicitly stated."
    )
    excedentregeling_present: bool = Field(
        default=False,
        description="Set true if an 'excedentregeling' (accrual above wage cap) is explicitly offered."
    )
    premium_change_equal_split: bool = Field(
        default=False,
        description="Set true only if future premium changes are explicitly split equally between employer and employee."
    )

    # Heterogeneity (capture ranges; do not infer)
    heterogeneity_present_pension: bool = Field(
        default=False,
        description="Set true if different pension rates are shown for major groups."
    )
    employee_contrib_range: Optional[AmountRange] = Field(
        default=None,
        description="Employee contribution range among major groups (e.g., min=4.5, max=6.5, unit='% of salary') — Omit if heterogeneity_present_pension = false."
    )
    premium_total_range: Optional[AmountRange] = Field(
        default=None,
        description="Total pension premium range among major groups (e.g., min=15, max=25, unit='% of salary') — Omit if heterogeneity_present_pension = false."
    )


# ----------------------------
# LEAVE INFORMATION
# ----------------------------
class LeaveInfo(BaseModel):
    """
    Schema for leave information.
    Policy: record absolute CAO entitlements exactly as stated (durations, pay levels, units).
    Do NOT compare to statutory baselines in the prompt; only set '*_above_statutory' flags
    when the CAO explicitly says so. Any historical comparison to statute is a downstream analysis task.
    """
    has_leave_enhancements: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states any leave improvement or top-up relative to statute."
    )

    # Maternity
    has_above_statutory_maternity: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states an enhancement above statutory for maternity."
    )
    paid_maternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of fully paid maternity leave exactly as stated in the CAO (e.g., value=16, unit='weeks')."
    )
    partially_paid_maternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of partially paid maternity leave as stated (e.g., value=10, unit='weeks')."
    )
    partially_paid_maternity_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during partially paid maternity leave (e.g., value=70, unit='% of salary')."
    )
    unpaid_maternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of additional unpaid maternity leave, as stated (e.g., value=6, unit='weeks')."
    )
    maternity_note: str = Field(
        default="",
        description="Maternity notes exactly as stated."
    )

    # Paternity / partner
    paternity_explicitly_above_statutory: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states any improvement for paternity/partner leave."
    )
    paid_paternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of fully paid paternity/partner leave as stated (e.g., value=6, unit='weeks')."
    )
    partially_paid_paternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of partially paid paternity/partner leave as stated (e.g., value=4, unit='weeks')."
    )
    partially_paid_paternity_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during partially paid paternity/partner leave (e.g., value=70, unit='% of salary')."
    )
    unpaid_paternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of unpaid paternity/partner leave as stated (e.g., value=2, unit='weeks')."
    )

    # Adoption / foster
    adoption_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of adoption/foster leave as stated (e.g., value=10, unit='weeks')."
    )
    adoption_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during adoption/foster leave (e.g., value=100, unit='% of salary')."
    )

    # Parental
    parental_leave_topup_present: bool = Field(
        default=False,
        description="Set true only if an employer top-up for parental leave is explicitly stated."
    )
    parental_leave_topup_pay: Optional[Amount] = Field(
        default=None,
        description="Top-up pay level during parental leave, as stated (e.g., value=70, unit='% of salary')."
    )
    parental_leave_unpaid: Optional[Amount] = Field(
        default=None,
        description="Duration of unpaid parental leave as stated (e.g., value=26, unit='weeks')."
    )

    # Abortion
    abortion_leave_present: bool = Field(
        default=False,
        description="Set true only if a specific abortion leave provision is explicitly mentioned."
    )

    # Sickness
    sick_leave_topup_present: bool = Field(
        default=False,
        description="Set true only if an employer sick-pay top-up is explicitly stated."
    )
    sickpay_duration: Optional[Amount] = Field(
        default=None,
        description="Duration of stated sick-pay continuation/top-up (e.g., value=104, unit='weeks')."
    )
    sickpay_continuation: Optional[Amount] = Field(
        default=None,
        description="Sick-pay continuation rate as stated (e.g., value=70, unit='% of salary')."
    )
    sickpay_extra_insurance_present: bool = Field(
        default=False,
        description="Set true if extra disability/WGA-gap insurance is explicitly included."
    )

    # Care leave
    care_leave_topup_present: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly tops up short-/long-term care leave."
    )
    short_term_care_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of short-term care leave (e.g., value=10, unit='days per year')."
    )
    short_term_care_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during short-term care leave (e.g., value=100, unit='% of salary')."
    )
    long_term_care_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of long-term care leave (e.g., value=6, unit='months')."
    )
    long_term_care_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during long-term care leave (e.g., value=70, unit='% of salary')."
    )

    # Vacation & holiday allowance
    vacation_time: Optional[Amount] = Field(
        default=None,
        description="Typical vacation entitlement for a standard worker (e.g., value=25, unit='days per year')."
    )
    vacation_bonus: Optional[Amount] = Field(
        default=None,
        description="Holiday allowance (vakantiegeld) amount or percentage (e.g., value=8, unit='% of annual salary')."
    )

    # Heterogeneity & notes
    heterogeneity_present_leave: bool = Field(
        default=False,
        description="Set true if major groups have different leave entitlements or pay levels."
    )
    liberation_day_annual: bool = Field(
        default=False,
        description="Set true if 5 May (Liberation Day) is a paid day off every year, as stated."
    )
    liberation_day_lustrum: bool = Field(
        default=False,
        description="Set true if 5 May is a paid day off only in lustrum years (every 5 years), as stated."
    )
    liberation_day_comp_note: str = Field(
        default="",
        description="Compensation note if Liberation Day is not a day off."
    )
    extra_leave_seniority_present: bool = Field(
        default=False,
        description=(
            "Set true only if the CAO explicitly grants extra vacation or leave entitlements based on years of service and/or age."
        )
    )
    extra_leave_seniority_schedule: str = Field(
        default="",
        description=(
            "Compact schedule or rule exactly as stated, showing seniority- or age-based leave increments. Leave empty if extra_leave_seniority_present = false."
        )
    )
    leave_note: str = Field(
        default="",
        description="Any special leaves."
    )


# ----------------------------
# TERMINATION INFORMATION
# ----------------------------
class TerminationInfo(BaseModel):
    """Schema for termination information (record exactly as stated; no statutory inference)."""

    has_termination_rules: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly contains termination/notice rules beyond statutory defaults."
    )

    selection_rule_notice: str = Field(
        default="unspecified",
        description=(
            "How the 'typical' group for notice was chosen when multiple rules exist (e.g., 'majority_headcount', 'base_tier', 'office_vs_field_rule', 'latest_year', 'unspecified', 'other')."
        )
    )

    employer_notice: Optional[Amount] = Field(
        default=None,
        description="Typical employer notice period (employer terminating) (e.g., value=2, unit='months')."
    )

    employee_notice: Optional[Amount] = Field(
        default=None,
        description="Typical employee notice period (employee resigning) (e.g., value=1, unit='months')."
    )

    heterogeneity_present_notice: bool = Field(
        default=False,
        description="Set true if major groups have different notice periods."
    )

    employer_notice_range: Optional[AmountRange] = Field(
        default=None,
        description="Employer notice duration range across main groups (e.g., min=1, max=3, unit='months') — Omit if heterogeneity_present_notice = false."
    )

    employee_notice_range: Optional[AmountRange] = Field(
        default=None,
        description="Employee notice duration range across main groups (e.g., min=1, max=2, unit='months') — Omit if heterogeneity_present_notice = false."
    )

    notice_period_by_tenure_present: bool = Field(
        default=False,
        description=(
            "Set true only if notice periods vary explicitly by years of service (for employer and/or employee)."
        )
    )

    notice_period_by_tenure_rule: str = Field(
        default="",
        description=(
            "Exact rule or schedule for tenure-based notice periods as stated. Leave empty if notice_period_by_tenure_present = false."
        )
    )

    can_shorten_notice_with_uwv_permit: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly allows notice to be shortened with a UWV permit."
    )
    notice_min_floor: Optional[Amount] = Field(
        default=None,
        description="Minimum notice duration that must remain after any shortening (e.g., value=1, unit='months') — Omit if can_shorten_notice_with_uwv_permit = false."
    )

    dismissal_approval: str = Field(
        default="unspecified",
        description="Which approval or authorization route the CAO states is required for a standard dismissal (e.g., 'UWV', 'Judge', 'Both', 'None', 'Conditional', 'unspecified', 'other'). Use 'Conditional' if approval applies only in specific cases or time periods."
    )

    sickness_dismissal_protection: bool = Field(
        default=False,
        description="Set true if the CAO reiterates or extends the dismissal ban/protection during sickness."
    )

    end_at_AOW_age_automatic: bool = Field(
        default=False,
        description="Set true only if the CAO states employment ends automatically at AOW (statutory pension) age."
    )

    probation_allowed: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly allows a probationary period in general."
    )
    probation_fixedterm: Optional[Amount] = Field(
        default=None,
        description="Maximum probation period for fixed-term contracts (e.g., value=2, unit='months'), exactly as stated."
    )
    probation_indefinite: Optional[Amount] = Field(
        default=None,
        description="Maximum probation period for indefinite-term contracts (e.g., value=1, unit='months'), exactly as stated."
    )

    severance_or_ww_supplement_present: bool = Field(
        default=False,
        description="Set true only if the CAO adds severance or WW (unemployment) supplements beyond statutory transition pay."
    )
    severance_extra: Optional[Amount] = Field(
        default=None,
        description="Quantified extra severance if stated (e.g., value=5000, unit='EUR') — Omit if severance_or_ww_supplement_present = false."
    )
    severance_extra_formula_note: str = Field(
        default="",
        description="Short formula or rule text for extra severance. Leave empty if severance_or_ww_supplement_present = false."
    )
    severance_by_tenure_rule_note: str = Field(
        default="",
        description=(
            "Brief formula or schedule exactly as stated if the CAO adds tenure-based severance beyond statutory transition pay. Leave empty if not applicable."
        )
    )


# ----------------------------
# OVERTIME INFORMATION
# ----------------------------
class OvertimeInfo(BaseModel):
    """Schema for overtime information (record exactly as stated; add units whenever a value is present)."""

    has_overtime_rules: bool = Field(
        default=False,
        description="Set true only if the CAO specifies overtime or allowance rules beyond statutory defaults."
    )

    selection_rule_overtime: str = Field(
        default="unspecified",
        description=(
            "How the 'typical' group for overtime was chosen when multiple worker groups exist (e.g., 'majority_headcount', 'base_tier', 'office_vs_field_rule', 'latest_year', 'unspecified', 'other'). Use the same selection logic as in notice and pension fields."
        )
    )

    overtime_trigger_daily: Optional[Amount] = Field(
        default=None,
        description="Daily threshold after which hours count as overtime, exactly as stated (e.g., value=8, unit='hours')."
    )
    overtime_trigger_weekly: Optional[Amount] = Field(
        default=None,
        description="Weekly threshold after which hours count as overtime, exactly as stated (e.g., value=40, unit='hours')."
    )

    # Surcharges
    overtime_compensation_mode: str = Field(
        default="unspecified",
        description="How overtime is compensated according to the CAO (e.g., 'monetary_pay', 'TOIL', 'both', 'unspecified', 'other')."
    )
    stacking_rule: str = Field(
        default="unspecified",
        description="How surcharges (e.g., overtime, night, weekend) interact (e.g., 'highest_only', 'cumulative', 'unclear', 'unspecified', 'other')."
    )
    overtime_allowance: Optional[Amount] = Field(
        default=None,
        description="Typical overtime surcharge (e.g., value=150, unit='% of hourly rate')."
    )
    heterogeneity_present_overtime: bool = Field(
        default=False,
        description="Set true if different overtime rates are shown for major groups."
    )
    overtime_allowance_range: Optional[AmountRange] = Field(
        default=None,
        description="Overtime surcharge range across main cases (e.g., min=125, max=175, unit='% of hourly rate') — Omit if heterogeneity_present_overtime = false."
    )

    # Shift / unfavourable hours
    shift_allowance_present: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly provides a separate shift allowance for working in regular shifts, distinct from overtime pay."
    )
    shift_allowance_range: Optional[AmountRange] = Field(
        default=None,
        description="Shift allowance range (e.g., min=10, max=25, unit='% of hourly rate') — Omit if shift_allowance_present = false."
    )

    unfavourable_hours_allowance: Optional[Amount] = Field(
        default=None,
        description="Maximum allowance for work during unfavourable hours (e.g., value=20, unit='% of hourly rate'). Distinct from overtime pay and from regular shift allowances."
    )

    # Working time bounds and rest
    min_rest_between_shifts: Optional[Amount] = Field(
        default=None,
        description="Minimum rest required between shifts, exactly as stated (e.g., value=11, unit='hours')."
    )

    max_hours_per_day: Optional[Amount] = Field(
        default=None,
        description="Maximum daily working time set by the CAO (e.g., value=10, unit='hours')."
    )

    max_hours_per_week: Optional[Amount] = Field(
        default=None,
        description="Maximum weekly working time set by the CAO (e.g., value=45, unit='hours')."
    )

    compulsory_overtime_annual: Optional[Amount] = Field(
        default=None,
        description="Maximum annual compulsory overtime if specified (e.g., value=200, unit='hours per year')."
    )

    guaranteed_weekends_off_rule_text: str = Field(
        default="",
        description="Verbatim summary if the CAO guarantees a minimum number of weekends off."
    )


# ----------------------------
# TRAINING INFORMATION
# ----------------------------
class TrainingInfo(BaseModel):
    """Schema for training information (record exactly as stated; add units when values are present)."""

    has_training_rights: bool = Field(
        default=False,
        description="Set true only if the CAO grants training/education rights."
    )

    training_time_yearly: Optional[Amount] = Field(
        default=None,
        description="Typical paid training time per year (e.g., value=40, unit='hours per year')."
    )

    training_budget: Optional[Amount] = Field(
        default=None,
        description="Annual monetary training budget (e.g., value=2000, unit='EUR per year')."
    )

    career_scan_freq: Optional[Amount] = Field(
        default=None,
        description="Frequency of employability/career scans (e.g., value=2, unit='times per year')."
    )

    cost_reimbursement: Optional[Amount] = Field(
        default=None,
        description="Percentage or amount of training/study costs reimbursed by the employer or sector fund (e.g., value=100, unit='% of costs')."
    )

    training_fund_present: bool = Field(
        default=False,
        description="Set true if a sectoral/CAO training fund finances training or subsidies."
    )
    reclaim_clause_present: bool = Field(
        default=False,
        description="Set true if the employer may reclaim training costs upon early departure."
    )
    mandatory_training_paid: bool = Field(
        default=False,
        description="Set true only if the CAO states employer pays 100% for mandatory/company-required training."
    )

    training_note: str = Field(
        default="",
        description="Concise verbatim summary of special rules."
    )


# ----------------------------
# HOMEOFFICE / TELEWORK INFORMATION
# ----------------------------
class HomeofficeInfo(BaseModel):
    """Schema for home office / telework information (record exactly as stated)."""

    has_homeoffice_rights: bool = Field(
        default=False,
        description="Set true only if the CAO includes home office / telework provisions."
    )

    homeoffice_entitlement: Optional[Amount] = Field(
        default=None,
        description="Entitled amount of remote work allowed, as explicitly stated in the CAO (e.g., value=2, unit='days per week')."
    )

    homeoffice_stipend_present: bool = Field(
        default=False,
        description="Set true only if a fixed home office allowance is stated."
    )
    homeoffice_stipend: Optional[Amount] = Field(
        default=None,
        description="Home office allowance amount (e.g., value=50, unit='EUR per month') — Omit if homeoffice_stipend_present = false."
    )

    homeoffice_discretion: str = Field(
        default="unspecified",
        description="Who decides on home office arrangements, as explicitly stated in the CAO (e.g., 'employer_only', 'joint_with_OR' = decision made jointly with the Works Council, 'employee_request' = employees may request or decide, 'unspecified', 'other')."
    )

    homeoffice_costs_reimbursed: bool = Field(
        default=False,
        description="Set true if the employer reimburses home office-related costs."
    )
    homeoffice_costs_note: str = Field(
        default="",
        description="Short note on reimbursed cost types. Leave empty if homeoffice_costs_reimbursed = false."
    )

    homeoffice_agreement_required: bool = Field(
        default=False,
        description="Set true if a formal telework agreement/protocol is required."
    )
    homeoffice_health_safety_guarantee: bool = Field(
        default=False,
        description="Set true if the employer commits to meet OSH/Arbo obligations at the home office."
    )
    homeoffice_travel_time_compensation: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states that additional commuting time arising from home-office or hybrid work is compensated."
    )

    homeoffice_note: str = Field(
        default="",
        description="Other remarks (e.g., equipment provision, campaigns, stimulation-only, only statutory minimum provisions)."
    )


# ----------------------------
# CONTRACT TYPE INFORMATION
# ----------------------------
class ContractTypeInfo(BaseModel):
    """Schema for contract type rules (record exactly as stated; units with values)."""

    has_contract_type_rules: bool = Field(
        default=False,
        description="Set true only if the CAO sets explicit rules on contract types of the workers beyond statutory defaults."
    )

    full_time_hours: Optional[Amount] = Field(
        default=None,
        description="Standard full-time working hours, exactly as stated (e.g., value=38, unit='hours per week')."
    )

    part_time_allowed: bool = Field(
        default=False,
        description="Set true only if part-time contracts are explicitly permitted for standard workers."
    )
    part_time_range: Optional[AmountRange] = Field(
        default=None,
        description="Part-time fraction/hours range (e.g., min=0.5, max=0.9, unit='FTE') — Omit if part_time_allowed = false."
    )

    minmax_hours_contract_allowed: bool = Field(
        default=False,
        description="Set true if 'min-max' (bandwidth) contracts are explicitly permitted."
    )
    minmax_hours_range: Optional[AmountRange] = Field(
        default=None,
        description="Min-max contract hours range (e.g., min=20, max=40, unit='hours per week') — Omit if minmax_hours_contract_allowed = false."
    )

    zero_hour_oncall_allowed: bool = Field(
        default=False,
        description="Set true if zero-hour or on-call contracts are explicitly allowed."
    )

    ketenregeling_deviation_present: bool = Field(
        default=False,
        description="Set true only if the CAO deviates from the statutory 'ketenregeling' (fixed-term chain rule)."
    )
    ketenregeling_max_contracts: Optional[Amount] = Field(
        default=None,
        description="Maximum number of successive fixed-term contracts (e.g., value=3, unit='contracts') — Omit if ketenregeling_deviation_present = false."
    )
    ketenregeling_max_duration: Optional[Amount] = Field(
        default=None,
        description="Maximum total duration of the fixed-term chain (e.g., value=24, unit='months') — Omit if ketenregeling_deviation_present = false."
    )

    conversion_rights_temp_to_perm_present: bool = Field(
        default=False,
        description="Set true if the CAO grants extra rights to convert fixed-term to indefinite contracts beyond the law."
    )
    conversion_rights_rule_text: str = Field(
        default="",
        description="Exact rule text on conversion from fixed-term to indefinite contracts, as stated. Leave empty if conversion_rights_temp_to_perm_present = false."    
    )


# ----------------------------
# FRINGE BENEFITS INFORMATION
# ----------------------------
class FringeBenefitsInfo(BaseModel):
    """Schema for fringe benefits (record exactly as stated; units with values)."""

    has_fringe_benefits: bool = Field(
        default=False,
        description="Set true only if the CAO mentions any fringe benefits beyond base pay."
    )

    commuting_allowance_present: bool = Field(
        default=False,
        description="Set true if commuting is reimbursed."
    )
    commuting_allowance: Optional[Amount] = Field(
        default=None,
        description="Commuting allowance amount (e.g., value=0.19, unit='EUR per km') — Omit if commuting_allowance_present = false."
    )

    bike_scheme_present: bool = Field(
        default=False,
        description="Set true if a bicycle/leasefiets scheme is present."
    )
    bike_scheme_note: str = Field(
        default="",
        description="Short note exactly as stated. Leave empty if bike_scheme_present = false."
    )

    internet_or_phone_reimbursement_present: bool = Field(
        default=False,
        description="Set true if internet and/or phone costs are reimbursed."
    )

    meal_benefit_present: bool = Field(
        default=False,
        description="Set true if a meal benefit is provided."
    )
    meal_benefit_type: str = Field(
        default="unspecified",
        description="Type of meal benefit (e.g., 'free_meals', 'subsidised_canteen', 'meal_vouchers', 'meal_allowance', 'other', 'unspecified')."
    )
    meal_benefit_amt: Optional[Amount] = Field(
        default=None,
        description="Meal benefit amount/percentage (e.g., value=5, unit='EUR per day') — Omit if meal_benefit_present = false."
    )

    health_insurance_support_present: bool = Field(
        default=False,
        description="Set true if there is an employer contribution or collective discount for health insurance."
    )
    health_insurance_support_note: str = Field(
        default="",
        description="Short note exactly as stated describing the health insurance support. Leave empty if health_insurance_support_present = false."
    )
    insurance_or_savings_benefit_present: bool = Field(
        default=False,
        description="Set true only if employer-paid financial benefits are explicitly stated."
    )
    insurance_or_savings_benefit_note: str = Field(
        default="",
        description="Short description exactly as stated. Leave empty if insurance_or_savings_benefit_present = false."
    )

    relocation_allowance_present: bool = Field(
        default=False,
        description="Set true if relocation/housing support is provided."
    )
    relocation_allowance: Optional[Amount] = Field(
        default=None,
        description="Relocation allowance value (e.g., value=5000, unit='EUR one-off') — Omit if relocation_allowance_present = false."
    )

    mandatory_certifications_paid: bool = Field(
        default=False,
        description="Set true if the employer covers costs of mandatory licenses/certifications."
    )

    other_fringe_benefits_note: str = Field(
        default="",
        description="Concise catch-all for other benefits."
    )


# ----------------------------
# SAFETY / INTEGRITY INFORMATION
# ----------------------------
class SafetyInfo(BaseModel):
    """Schema for safety and integrity provisions (record exactly as stated)."""

    harassment_protocol_present: bool = Field(
        default=False,
        description="Set true if a sexual harassment/integrity protocol is included."
    )
    harassment_protocol_note: str = Field(
        default="",
        description="Short description of the harassment protocol exactly as stated. Leave empty if harassment_protocol_present = false."
    )

    integrity_protocol_present: bool = Field(
        default=False,
        description="Set true if a broader integrity/behavior protocol is included."
    )

    confidential_counsellor_present: bool = Field(
        default=False,
        description="Set true if an internal or external confidential adviser is explicitly provided."
    )

    reporting_channel_external: bool = Field(
        default=False,
        description="Set true if an external reporting channel is guaranteed."
    )

    safety_training_present: bool = Field(
        default=False,
        description="Set true if the employer/sector funds mandatory safety or psychosocial risk training."
    )

    safety_committee_present: bool = Field(
        default=False,
        description="Set true if a joint safety/health committee is provided."
    )

    rie_psa_required: bool = Field(
        default=False,
        description="True if the CAO requires a Risk Inventory & Evaluation (RI&E) to cover psychosocial risks such as stress or burnout."
    )

    psa_prevention_measures_present: bool = Field(
        default=False,
        description="True if the CAO lists explicit PSA prevention or wellbeing measures."
    )
    psa_measures_note: str = Field(
        default="",
        description="Concise summary of psychosocial-risk or wellbeing measures."
    )

    arbodienst_access_provided: bool = Field(
        default=False,
        description="True if the CAO guarantees employee access to an occupational health service (arbodienst/bedrijfsarts) or sector-funded prevention service."
    )

    preventive_medical_checkup_present: bool = Field(
        default=False,
        description="True if the CAO mentions a Preventive Medical Examination (PMO/PAGO) or health-check entitlement."
    )

    workload_monitoring_present: bool = Field(
        default=False,
        description="True if the CAO includes workload or stress monitoring."
    )

    wellbeing_program_present: bool = Field(
        default=False,
        description="True if the CAO includes wellbeing or vitality programs."
    )

    safety_note: str = Field(
        default="",
        description="Catch-all for unusual obligations."
    )


# ----------------------------
# CHILDCARE INFORMATION
# ----------------------------
class ChildcareInfo(BaseModel):
    """Schema for childcare support (record exactly as stated; units with values)."""

    childcare_support_present: bool = Field(
        default=False,
        description="Set true if the employer provides any childcare benefit/support."
    )

    childcare_support: Optional[Amount] = Field(
        default=None,
        description="Monetary childcare support amount (e.g., value=200, unit='EUR per month')."
    )

    childcare_support_cap: Optional[Amount] = Field(
        default=None,
        description="Maximum employer/sector contribution if a cap is stated (e.g., value=400, unit='EUR per month')."
    )

    childcare_inhouse_present: bool = Field(
        default=False,
        description="Set true if on-site or company-arranged childcare is provided/financed."
    )
    childcare_discount_present: bool = Field(
        default=False,
        description="Set true if discounts at contracted childcare institutions are provided."
    )
    childcare_priority_access: bool = Field(
        default=False,
        description="Set true if priority access or reserved places are provided."
    )

    childcare_age_min: Optional[Amount] = Field(
        default=None,
        description="Minimum covered child age if stated (e.g., value=0, unit='years')."
    )
    childcare_age_max: Optional[Amount] = Field(
        default=None,
        description="Maximum covered child age if stated (e.g., value=12, unit='years')."
    )
    childcare_age_limit_note: str = Field(
        default="",
        description="Free-form age scope details."
    )

    childcare_provider_scope: str = Field(
        default="unspecified",
        description="Which childcare providers qualify for the employer/sector childcare support, as explicitly stated (e.g., 'any', 'contracted_only', 'sector_only', 'company_only', 'unspecified', 'other'): "
        "'any' = all registered providers; "
        "'contracted_only' = only providers with a contract/arrangement; "
        "'sector_only' = sector-specific facilities; "
        "'company_only' = company-run or on-site childcare; "
        "'unspecified' = not stated."
    )

    childcare_public_coord: str = Field(
        default="unspecified",
        description="How childcare benefits interact with public subsidies/fiscal rules (e.g., 'top_up_after_public_benefit', 'within_fiscal_max', 'gross_before_public_benefit', 'unspecified', 'other')."
    )

    childcare_funding_through_sector_fund: bool = Field(
        default=False,
        description="Set true if childcare support is financed via a sector fund."
    )

    childcare_min_tenure_months: Optional[float] = Field(
        default=None,
        description="Minimum tenure required to be eligible for the childcare benefit (months)."
    )
    childcare_min_fte: Optional[Amount] = Field(
        default=None,
        description="Minimum employment fraction (FTE) required for childcare benefit eligibility (e.g., value=0.5, unit='FTE')."
    )

    childcare_benefit_eligibility_note: str = Field(
        default="",
        description="Other eligibility limits or conditions exactly as stated."
    )


# ----------------------------
# AI / ALGORITHMIC MANAGEMENT
# ----------------------------
class AIInfo(BaseModel):
    """Schema for AI/ML/LLM provisions (record exactly as stated)."""

    ai_policy_exists: bool = Field(
        default=False,
        description="Set true only if the CAO contains any AI/algorithmic-management provisions."
    )

    ai_automated_decisions: str = Field(
        default="unspecified",
        description="Are automated AI decisions allowed (e.g., 'never', 'with_human_review', 'unspecified', 'other')."
    )

    ai_transparency_requirements: str = Field(
        default="",
        description="Required disclosures (purpose, data, vendor, logic, worker information). Leave empty if ai_policy_exists = false."
    )

    ai_bias_audit: str = Field(
        default="unspecified",
        description="Frequency/requirement of bias audits (e.g., 'annual', '≥annual', 'none', 'unspecified', 'other')."
    )

    ai_governance_body_present: bool = Field(
        default=False,
        description="Set true if a joint AI/Data/OR governance body/committee exists."
    )

    ai_dispute_rights_note: str = Field(
        default="",
        description="Summary of how workers can contest AI-based decisions. Leave empty if ai_policy_exists = false."
    )

    ai_training_rights_present: bool = Field(
        default=False,
        description="Set true if AI-literacy or upskilling provisions for affected roles are included."
    )
    ai_training_rights_note: str = Field(
        default="",
        description="Hours/budget or redeployment pathways exactly as stated. Leave empty if ai_policy_exists = false."
    )


# ----------------------------
# NON-SALARY PARTS
# ----------------------------
# Folder: non_salary/gen_bon_wag_pen_ter
class NonSalaryPart1(BaseModel):
    """Part 1: General, Bonuses, Wage Scales, Pension, Termination"""
    general_information: GeneralInfo = Field(default_factory=GeneralInfo)
    bonuses_info: BonusesInfo = Field(default_factory=BonusesInfo)
    wage_scales_info: WageScalesInfo = Field(default_factory=WageScalesInfo)
    pension_information: PensionInfo = Field(default_factory=PensionInfo)
    termination_information: TerminationInfo = Field(default_factory=TerminationInfo)

# Folder: non_salary/lea_ove_tra
class NonSalaryPart2(BaseModel):
    """Part 2: Leave, Overtime, Training"""
    leave_information: LeaveInfo = Field(default_factory=LeaveInfo)
    overtime_information: OvertimeInfo = Field(default_factory=OvertimeInfo)
    training_information: TrainingInfo = Field(default_factory=TrainingInfo)

# Folder: non_salary/hom_con_saf_chi_ai_fri
class NonSalaryPart3(BaseModel):
    """Part 3: Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits"""
    homeoffice_information: HomeofficeInfo = Field(default_factory=HomeofficeInfo)
    contract_type_information: ContractTypeInfo = Field(default_factory=ContractTypeInfo)
    safety_information: SafetyInfo = Field(default_factory=SafetyInfo)
    childcare_information: ChildcareInfo = Field(default_factory=ChildcareInfo)
    ai_information: AIInfo = Field(default_factory=AIInfo)
    fringe_benefits_information: FringeBenefitsInfo = Field(default_factory=FringeBenefitsInfo)


# ---------------------------------------------------------------------
# NON-SALARY EXTRACTION PROMPT
# ---------------------------------------------------------------------

NON_SALARY_PROMPT = """Extract structured information from a JSON object derived from the Dutch CAO document.
    
    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no markdown fences, no extra text.

    INPUTS:
    Filename: {filename}
    Source text: {source_json}
    
    EXTRACT SECTIONS: {sections}

    CRITICAL RULES
        - Extract ONLY what is explicitly present in the CAO. Do NOT infer, guess, or hallucinate.
        - Copy numbers/dates/percentages/units EXACTLY as written. Preserve all values literally.
        - Dates MUST be formatted as YYYY-MM-DD (omit or "" if missing).
        - Be precise: no paraphrasing of quantitative terms; no decorative characters or separator lines.
        - Output ONLY valid JSON format matching the provided schema structure.

    EXTRACTION GUIDELINES
        - Extract factual information for each field based on the schema descriptions.
        - Include relevant conditions, exceptions, and legal references in note fields.
        - Missing values: Omit optional fields entirely. Only include optional fields with actual values.
        - Do NOT compare to statutory law or mark "above statutory" unless the CAO explicitly says so.

    AMOUNT & AMOUNT RANGE RULES
        - For Amount fields: record both value and unit as an object (e.g., {{"value": 500, "unit": "EUR one-off"}}).
        - For AmountRange fields: record min, max, and unit as an object (e.g., {{"min": 1, "max": 3, "unit": "months"}}).
        - If no value is present, omit the entire Amount/AmountRange object. Never output a unit without its value.
        - Value fields are numeric (float or null). Unit fields are strings.

    WORKER FOCUS & TYPICAL GROUP
        - Focus on "normal workers" (≈23-65 years, no small groups). If groups differ (e.g., Construction vs UTA) and a single typical cannot be clearly chosen, allow min/max ONLY for key metrics (e.g., notice periods, overtime allowances).
        - When present set heterogeneity_present_* = true when major worker groups have any different terms.
            - When heterogeneity_present_* = true: fill BOTH typical values AND min/max fields for key metrics. When false: fill typical values only; leave min/max as null. 
        - If present in {sections}: in pension_information, termination_information or overtime_information, first choose the typical worker/group using selection_rule_*. Preference order: majority_headcount (largest group) > office_vs_field_rule (core group) > base_tier (lowest service band for ages 23-65) > latest_year (most recent values) > other > unspecified (could not determine).
            - pension_information: From employee_contrib till premium_change_equal_split, populate data ONLY for this group.
            - overtime_information: From overtime_trigger_daily till overtime_allowance, populate data ONLY for this group.
            - termination_information: From employer_notice till heterogeneity_present_notice, populate data ONLY for this group.

    EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT)
        1) READ & ANCHOR
            - Read all general rules and field descriptions of the Pydantic schema.
            - Scan the input to understand its structure and identify corresponding sections.
        2) PROCESS sections in schema order of {sections} (Use a 1→1 mapping between input and output sections, except that wage_information feeds two outputs: bonuses_info and wage_scales_info):
            - Capture literals exactly (numbers, percentages, units, dates).
            - Apply WORKER FOCUS & TYPICAL GROUP rules (heterogeneity, selection rules, pension consistency, overtime consistency).
            - Apply EXTRACTION GUIDELINES, AMOUNT & AMOUNT RANGE RULES, DATA TYPES & MISSING VALUES, and string fields (exact tokens; else "other"/"unspecified").
        3) INCLUDE CROSS-REFERENCED CONTENT: Check if relevant information for any {sections} appears elsewhere in the input. Include it only if contextually consistent and clearly related.
        4) CROSS-FIELD CONSISTENCY
	        - Ensure amounts, ranges, and units are internally coherent (min ≤ typ ≤ max).
	        - Validate all dates (YYYY-MM-DD).
	    5) VERIFY (SOURCE-GROUNDED)
	        - Confirm every extracted number/date/percentage/unit/clause is explicitly present in the input.
	        - Remove or correct anything not grounded.
	    6) VALIDATE (SCHEMA & JSON)
	        - Build one JSON object that conforms exactly to the Pydantic schema (keys, types, null/""/omit conventions).
	        - JSON is UTF-8, syntactically valid (balanced brackets, no trailing commas).
	    7) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
        - Ensure brackets/commas are correct; no trailing commas; all fields present.
    """
