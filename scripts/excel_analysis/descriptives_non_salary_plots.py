"""
CAO Non-Salary Time Trend Plotting Script

This script reads the non-salary Excel/CSV output and generates matplotlib line plots
showing trends over contract start years for key numeric and boolean variables.

USAGE:
    python scripts/excel_analysis/descriptives_non_salary_plots.py

INPUT:
    - outputs/excel/new_results/extracted_data_non_salary.csv (or .xlsx)

OUTPUT:
    - outputs/analysis/figures/ (directory with PNG plots)
      
      Numeric trend plots (with normalized values and CAO counts):
      - non_salary_numeric_hours_trends.png (+ _latest_cao_view version)
      - non_salary_numeric_leave_trends.png (+ _latest_cao_view version)
      - non_salary_numeric_pension_training_trends.png (+ _latest_cao_view version)
      
      Boolean trend plots by domain (with CAO counts in background):
      - non_salary_boolean_overview.png (+ _latest_cao_view version)
      - non_salary_boolean_general.png (+ _latest_cao_view version)
      - non_salary_boolean_bonuses_wages.png (+ _latest_cao_view version)
      - non_salary_boolean_pension.png (+ _latest_cao_view version)
      - non_salary_boolean_termination.png (+ _latest_cao_view version)
      - non_salary_boolean_leave.png (+ _latest_cao_view version)
      - non_salary_boolean_overtime.png (+ _latest_cao_view version)
      - non_salary_boolean_training.png (+ _latest_cao_view version)
      - non_salary_boolean_homeoffice.png (+ _latest_cao_view version)
      - non_salary_boolean_contract_type.png (+ _latest_cao_view version)
      - non_salary_boolean_safety.png (+ _latest_cao_view version)
      - non_salary_boolean_childcare.png (+ _latest_cao_view version)
      - non_salary_boolean_ai.png (+ _latest_cao_view version)
      - non_salary_boolean_fringe.png (+ _latest_cao_view version)
"""

import os
import sys
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
INPUT_EXCEL_PATH = "outputs/excel/new_results/extracted_data_non_salary.csv"
OUTPUT_DIR = "outputs/analysis/figures/"
MIN_OBS_PER_YEAR = 3  # Minimum observations per year to include in plot

# =============================================================================
# VARIABLE DEFINITIONS
# =============================================================================

NUMERIC_TREND_VARS = [
    "contract_full_time_hours_value",
    "leave_vacation_time_value",
    "leave_sickpay_duration_value",
    "leave_sickpay_continuation_value",
    "pension_employee_contrib_value",
    "pension_retire_age_normal_value",
    "overtime_max_hours_per_week_value",
    "training_time_yearly_value",
]

# Boolean variable groupings by domain
BOOLEAN_DOMAIN_GROUPS = {
    "01_general": {
        "filename": "non_salary_boolean_general.png",
        "title": "General CAO Provisions",
        "vars": [
            "general_avv_applies",
            "general_retro_applies",
            "general_retro_backpay_due",
            "general_dev_company_level",
            "TTW",
        ]
    },
    "02_bonuses_wages": {
        "filename": "non_salary_boolean_bonuses_wages.png",
        "title": "Bonuses and Wages",
        "vars": [
            "bonus_has_bonus_schemes",
            "bonus_sign_on_bonus_present",
            "bonus_thirteenth_month",
            "bonus_profit_sharing_present",
            "bonus_performance_bonus_present",
            "bonus_job_allowances_present",
            "bonus_qual_bonus_present",
            "bonus_seniority_loyalty_bonus",
            "bonus_retire_gratuity_present",
            "wage_entry_step_exp_present",
            "wage_pers_allow_max_scale",
        ]
    },
    "03_pension": {
        "filename": "non_salary_boolean_pension.png",
        "title": "Pension Provisions",
        "vars": [
            "pension_has_pension_scheme",
            "pension_mandatory_participation",
            "pension_accrual_stat_leaves",
            "pension_accrual_illness_y2",
            "pension_excedent_present",
            "pension_premium_eq_split",
        ]
    },
    "04_termination": {
        "filename": "non_salary_boolean_termination.png",
        "title": "Termination Rules",
        "vars": [
            "term_has_termination_rules",
            "term_notice_tenure_present",
            "term_shorten_notice_uwv",
            "term_sick_dismissal_prot",
            "term_end_at_AOW_auto",
            "term_probation_allowed",
            "term_severance_ww_supplement",
        ]
    },
    "05_leave": {
        "filename": "non_salary_boolean_leave.png",
        "title": "Leave Enhancements",
        "vars": [
            "leave_has_leave_enhancements",
            "leave_has_above_statutory_maternity",
            "leave_paternity_explicitly_above_statutory",
            "leave_parental_topup_present",
            "leave_parental_tenure_req_present",
            "leave_abortion_present",
            "leave_sick_topup_present",
            "leave_sickpay_extra_insurance_present",
            "leave_care_topup_present",
            "leave_liberation_day_annual",
            "leave_liberation_day_lustrum",
            "leave_extra_seniority_present",
        ]
    },
    "06_overtime": {
        "filename": "non_salary_boolean_overtime.png",
        "title": "Overtime Rules",
        "vars": [
            "overtime_has_overtime_rules",
            "overtime_shift_allowance_present",
        ]
    },
    "07_training": {
        "filename": "non_salary_boolean_training.png",
        "title": "Training Rights",
        "vars": [
            "training_has_training_rights",
            "training_fund_present",
            "training_reclaim_clause_present",
            "training_mandatory_training_paid",
        ]
    },
    "08_homeoffice": {
        "filename": "non_salary_boolean_homeoffice.png",
        "title": "Homeoffice Provisions",
        "vars": [
            "homeoffice_has_homeoffice_rights",
            "homeoffice_stipend_present",
            "homeoffice_costs_reimbursed",
            "homeoffice_agreement_required",
            "homeoffice_health_safety_guarantee",
            "homeoffice_travel_time_compensation",
        ]
    },
    "09_contract_type": {
        "filename": "non_salary_boolean_contract_type.png",
        "title": "Contract Type Rules",
        "vars": [
            "contract_has_contract_type_rules",
            "contract_part_time_allowed",
            "contract_minmax_hours_contract_allowed",
            "contract_zero_hour_oncall_allowed",
            "contract_ketenregeling_deviation_present",
            "contract_conversion_rights_temp_to_perm_present",
            "contract_workhours_adjustment_right_present",
        ]
    },
    "10_safety": {
        "filename": "non_salary_boolean_safety.png",
        "title": "Safety and Wellbeing",
        "vars": [
            "safety_harassment_protocol_present",
            "safety_integrity_protocol_present",
            "safety_confidential_counsellor_present",
            "safety_reporting_channel_external",
            "safety_safety_training_present",
            "safety_safety_committee_present",
            "safety_rie_psa_required",
            "safety_psa_prevention_measures_present",
            "safety_arbodienst_access_provided",
            "safety_preventive_medical_checkup_present",
            "safety_workload_monitoring_present",
            "safety_wellbeing_program_present",
        ]
    },
    "11_childcare": {
        "filename": "non_salary_boolean_childcare.png",
        "title": "Childcare Support",
        "vars": [
            "childcare_childcare_support_present",
            "childcare_inhouse_present",
            "childcare_discount_present",
            "childcare_priority_access",
            "childcare_funding_sector_fund",
        ]
    },
    "12_ai": {
        "filename": "non_salary_boolean_ai.png",
        "title": "AI Policy",
        "vars": [
            "ai_ai_policy_exists",
            "ai_ai_governance_body_present",
            "ai_ai_training_rights_present",
        ]
    },
    "13_fringe": {
        "filename": "non_salary_boolean_fringe.png",
        "title": "Fringe Benefits",
        "vars": [
            "fringe_has_fringe_benefits",
            "fringe_commuting_allowance_present",
            "fringe_health_insurance_support_present",
            "fringe_insurance_or_savings_benefit_present",
            "fringe_relocation_allowance_present",
            "fringe_mandatory_certifications_paid",
            "fringe_bike_scheme_present",
            "fringe_internet_or_phone_reimbursement_present",
            "fringe_meal_benefit_present",
        ]
    },
    "00_overview": {
        "filename": "non_salary_boolean_overview.png",
        "title": "Overview: Domain Presence",
        "vars": [
            "bonus_has_bonus_schemes",
            "pension_has_pension_scheme",
            "term_has_termination_rules",
            "leave_has_leave_enhancements",
            "overtime_has_overtime_rules",
            "training_has_training_rights",
            "contract_has_contract_type_rules",
            "fringe_has_fringe_benefits",
        ]
    },
}

# Clear labels for boolean variables (with explanations where needed)
BOOLEAN_LABELS = {
    # General
    "general_avv_applies": "AVV applies (binding declaration)",
    "general_retro_applies": "Retroactive application applies",
    "general_retro_backpay_due": "Retroactive backpay due",
    "general_dev_company_level": "Company-level deviation allowed",
    "TTW": "TTW (Tussentijdse Wijziging / Interim amendment)",
    
    # Bonuses and wages
    "bonus_has_bonus_schemes": "Has bonus schemes",
    "bonus_sign_on_bonus_present": "Sign-on bonus present",
    "bonus_thirteenth_month": "13th month bonus present",
    "bonus_profit_sharing_present": "Profit sharing present",
    "bonus_performance_bonus_present": "Performance bonus present",
    "bonus_job_allowances_present": "Job-specific allowances present",
    "bonus_qual_bonus_present": "Qualification bonus present",
    "bonus_seniority_loyalty_bonus": "Seniority/loyalty bonus present",
    "bonus_retire_gratuity_present": "Retirement gratuity present",
    "wage_entry_step_exp_present": "Entry step by experience present",
    "wage_pers_allow_max_scale": "Personal allowance at max scale present",
    
    # Pension
    "pension_has_pension_scheme": "Has pension scheme",
    "pension_mandatory_participation": "Mandatory participation in pension",
    "pension_accrual_stat_leaves": "Pension accrual during statutory leaves",
    "pension_accrual_illness_y2": "Pension accrual during illness (year 2+)",
    "pension_excedent_present": "Excedentregeling present",
    "pension_premium_eq_split": "Premium change equal split between parties",
    
    # Termination
    "term_has_termination_rules": "Has termination rules",
    "term_notice_tenure_present": "Notice period varies by tenure",
    "term_shorten_notice_uwv": "Can shorten notice with UWV permit",
    "term_sick_dismissal_prot": "Sickness dismissal protection present",
    "term_end_at_AOW_auto": "Automatic end at AOW age",
    "term_probation_allowed": "Probation period allowed",
    "term_severance_ww_supplement": "Severance or WW supplement present",
    
    # Leave
    "leave_has_leave_enhancements": "Has leave enhancements",
    "leave_has_above_statutory_maternity": "Above-statutory maternity leave",
    "leave_paternity_explicitly_above_statutory": "Above-statutory paternity leave",
    "leave_parental_topup_present": "Parental leave topup present",
    "leave_parental_tenure_req_present": "Parental leave tenure requirement present",
    "leave_abortion_present": "Abortion leave present",
    "leave_sick_topup_present": "Sick leave topup present",
    "leave_sickpay_extra_insurance_present": "Sickpay extra insurance present",
    "leave_care_topup_present": "Care leave topup present",
    "leave_liberation_day_annual": "Liberation day (annual)",
    "leave_liberation_day_lustrum": "Liberation day (lustrum)",
    "leave_extra_seniority_present": "Extra leave by seniority present",
    
    # Overtime
    "overtime_has_overtime_rules": "Has overtime rules",
    "overtime_shift_allowance_present": "Shift allowance present",
    
    # Training
    "training_has_training_rights": "Has training rights",
    "training_fund_present": "Training fund present",
    "training_reclaim_clause_present": "Training reclaim clause present",
    "training_mandatory_training_paid": "Mandatory training paid by employer",
    
    # Homeoffice
    "homeoffice_has_homeoffice_rights": "Has homeoffice rights",
    "homeoffice_stipend_present": "Homeoffice stipend present",
    "homeoffice_costs_reimbursed": "Homeoffice costs reimbursed",
    "homeoffice_agreement_required": "Homeoffice agreement required",
    "homeoffice_health_safety_guarantee": "Homeoffice health/safety guarantee present",
    "homeoffice_travel_time_compensation": "Homeoffice travel time compensation present",
    
    # Contract type
    "contract_has_contract_type_rules": "Has contract type rules",
    "contract_part_time_allowed": "Part-time contracts allowed",
    "contract_minmax_hours_contract_allowed": "Min-max hours contracts allowed",
    "contract_zero_hour_oncall_allowed": "Zero-hour/on-call contracts allowed",
    "contract_ketenregeling_deviation_present": "Ketenregeling deviation present",
    "contract_conversion_rights_temp_to_perm_present": "Conversion rights (temp to perm) present",
    "contract_workhours_adjustment_right_present": "Workhours adjustment right present",
    
    # Safety
    "safety_harassment_protocol_present": "Harassment protocol present",
    "safety_integrity_protocol_present": "Integrity protocol present",
    "safety_confidential_counsellor_present": "Confidential counsellor present",
    "safety_reporting_channel_external": "External reporting channel present",
    "safety_safety_training_present": "Safety training present",
    "safety_safety_committee_present": "Safety committee present",
    "safety_rie_psa_required": "RIE/PSA required",
    "safety_psa_prevention_measures_present": "PSA prevention measures present",
    "safety_arbodienst_access_provided": "Arbodienst access provided",
    "safety_preventive_medical_checkup_present": "Preventive medical checkup present",
    "safety_workload_monitoring_present": "Workload monitoring present",
    "safety_wellbeing_program_present": "Wellbeing program present",
    
    # Childcare
    "childcare_childcare_support_present": "Childcare support present",
    "childcare_inhouse_present": "In-house childcare present",
    "childcare_discount_present": "Childcare discount present",
    "childcare_priority_access": "Childcare priority access present",
    "childcare_funding_sector_fund": "Childcare funding through sector fund present",
    
    # AI
    "ai_ai_policy_exists": "AI policy exists",
    "ai_ai_governance_body_present": "AI governance body present",
    "ai_ai_training_rights_present": "AI training rights present",
    
    # Fringe
    "fringe_has_fringe_benefits": "Has fringe benefits",
    "fringe_commuting_allowance_present": "Commuting allowance present",
    "fringe_health_insurance_support_present": "Health insurance support present",
    "fringe_insurance_or_savings_benefit_present": "Insurance/savings benefit present",
    "fringe_relocation_allowance_present": "Relocation allowance present",
    "fringe_mandatory_certifications_paid": "Mandatory certifications paid by employer",
    "fringe_bike_scheme_present": "Bike scheme present",
    "fringe_internet_or_phone_reimbursement_present": "Internet/phone reimbursement present",
    "fringe_meal_benefit_present": "Meal benefit present",
}

# Variable groupings for figures
NUMERIC_FIGURE_GROUPS = {
    "non_salary_numeric_hours_trends.png": [
        "contract_full_time_hours_value",
        "overtime_max_hours_per_week_value",
    ],
    "non_salary_numeric_leave_trends.png": [
        "leave_vacation_time_value",
        "leave_sickpay_duration_value",
        "leave_sickpay_continuation_value",
    ],
    "non_salary_numeric_pension_training_trends.png": [
        "pension_employee_contrib_value",
        "pension_retire_age_normal_value",
        "training_time_yearly_value",
    ],
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_hours_to_weekly(value: float, unit: str, default_ft_hours: float = 38.0, 
                               assume_percentage_if_unknown: bool = False,
                               var_name: str = "") -> Optional[float]:
    """
    Normalize time values to hours per week, or percentages to percentage scale.
    
    Handles comprehensive unit formats including:
    - Hours: per week, per year, per month, per X weeks
    - Days: days, days per year, days per calendar year, etc.
    - Percentages: % of hours worked, % of agreed working hours, etc.
    - Multipliers: times agreed working hours per week, times average weekly hours, etc.
    - Weeks, months, years
    - Pension: fractions, shares, percentages of premium
    - Training: hours, days, weeks, percentages
    
    For percentages and multipliers, uses default_ft_hours (default 38) as base.
    For pension contributions, normalizes fractions/shares to percentage scale.
    
    Args:
        value: Numeric value
        unit: Unit string (case-insensitive)
        default_ft_hours: Default full-time hours per week for percentage/multiplier conversions (default 38)
        assume_percentage_if_unknown: If True, assume percentage when no known unit matches (for leave continuation)
        var_name: Variable name to determine normalization strategy (e.g., pension_employee_contrib_value)
        
    Returns:
        Normalized value in hours per week (for time) or percentage (for pension), or None if conversion not possible
    """
    if pd.isna(value) or value is None:
        return None
    
    if pd.isna(unit) or unit is None or unit == "":
        return None
    
    unit_lower = str(unit).lower().strip()
    val = float(value)
    
    # ========================================================================
    # HOURS-BASED UNITS
    # ========================================================================
    
    # Hours per week (already in correct unit)
    if any(x in unit_lower for x in ['hours per week', 'hours/week', 'hours_per_week', 
                                      'hours per workweek', 'hours per payroll period',
                                      'hours per wage period', 'hours per pay period',
                                      'hours per work week', 'hours per working week',
                                      'average weekly hours', 'wage hours']):
        return val
    
    # Hours per year -> divide by 52
    if any(x in unit_lower for x in ['hours per year', 'hours/year', 'hours_per_year', 
                                      'hours annually', 'hours (average over one year)',
                                      'hours per calendar year', 'hours per full vacation year',
                                      'hours per vacation year', 'hours per year for 36-hour week',
                                      'hours per year for 36-hour work week',
                                      'hours per year for 36-hour workweek',
                                      'hours per year for 38-hour week',
                                      'hours per year for 40-hour workweek support staff',
                                      'hours per year for full-time',
                                      'hours per year for full-time employment',
                                      'hours annually for 36-hour work week',
                                      'hours annually for full-time employment',
                                      'hours per academic year', 'hours per course year',
                                      'hours per school year', 'clock hours per school year',
                                      'to 30.4 hours per year']):
        return val / 52.0
    
    # Hours per month -> multiply by 12/52
    if any(x in unit_lower for x in ['hours per month', 'hours/month',
                                      'hours per full working month',
                                      'hours per fully worked month']):
        return val * 12.0 / 52.0
    
    # Hours per 4 weeks -> divide by 4
    if any(x in unit_lower for x in ['hours per 4 weeks', 'hours per 4-week', 
                                      'hours per four weeks', 'hours per 4-week pay period',
                                      'hours per 4-week period', 'hours per week (average over 4 weeks)',
                                      'hours per week (averaged over 4 weeks)', 
                                      'hours per week (averaged over four weeks)',
                                      'hours per week (over 4-week period)',
                                      'hours per week average over 4-week period',
                                      'hours (average over 4 weeks)']):
        return val / 4.0
    
    # Hours per 13 weeks -> divide by 13
    if any(x in unit_lower for x in ['hours per 13 weeks', 'hours per 13-week', 
                                      'hours per week (average over 13 weeks)',
                                      'hours per week (average over 13-week period)',
                                      'hours per week average over 13-week period',
                                      'hours per week over 13 weeks',
                                      'hours average in 13 weeks']):
        return val / 13.0
    
    # Hours per 16 weeks -> divide by 16
    if 'hours per 16 weeks' in unit_lower or 'hours per week (average over 16 weeks)' in unit_lower:
        return val / 16.0
    
    # Hours per 26 weeks -> divide by 26
    if any(x in unit_lower for x in ['hours per 26 weeks', 'hours per week (average over 26 weeks)',
                                      'hours per 26-week period']):
        return val / 26.0
    
    # Just "hours" - assume per week if reasonable, otherwise ambiguous
    if unit_lower in ['hours', 'clock hours', 'paid hours']:
        # If value is reasonable for weekly hours (e.g., 20-60), assume weekly
        if 20 <= val <= 60:
            return val
        # Otherwise ambiguous, return None
        return None
    
    # ========================================================================
    # DAYS-BASED UNITS
    # ========================================================================
    # Convert days to hours per week: assume 7.6 hours per working day (38 hours / 5 days)
    # Then convert to weekly: days_per_year / 52 * 7.6, or days_per_week * 7.6
    
    if 'days' in unit_lower:
        hours_per_day = default_ft_hours / 5.0  # Assume 5-day work week
        
        # Days per year -> convert to hours per week
        if any(x in unit_lower for x in ['days per year', 'days/year', 'days annually',
                                          'days per calendar year', 'days per full calendar year',
                                          'days per full vacation year', 'days per vacation year',
                                          'workdays per year', 'working days per year',
                                          'working days per calendar year',
                                          'day per year', 'day per calendar year',
                                          'days per 12 months', 'days/shifts per year',
                                          'training days per 12 months', 'school days per year',
                                          'school days per school year', 'paid study days per year',
                                          'paid day per year', 'paid training days']):
            return (val / 52.0) * hours_per_day
        
        # Days per week (implicit or explicit)
        if any(x in unit_lower for x in ['per week', 'weekly', 'day per week']):
            return val * hours_per_day
        
        # Training/school/paid days
        if any(x in unit_lower for x in ['training days', 'school days', 'paid day',
                                          'paid day of leave', 'paid day off', 'paid development day',
                                          'paid study days', 'development days', 'half-days',
                                          'half-days per year', 'days/shifts']):
            # Assume these are per year
            return (val / 52.0) * hours_per_day
        
        # Just "days" - assume per year if value is reasonable (e.g., 20-30), otherwise per week
        if 20 <= val <= 30:
            # Likely days per year (vacation days)
            return (val / 52.0) * hours_per_day
        else:
            # Likely days per week
            return val * hours_per_day
    
    # Workdays (same as days)
    if 'workdays' in unit_lower or 'working days' in unit_lower:
        hours_per_day = default_ft_hours / 5.0
        if 'per year' in unit_lower:
            return (val / 52.0) * hours_per_day
        return val * hours_per_day
    
    # ========================================================================
    # PENSION CONTRIBUTION UNITS (fractions, shares, percentages)
    # ========================================================================
    # For pension_employee_contrib_value, normalize fractions/shares to percentage scale
    
    if 'pension_employee_contrib' in var_name.lower():
        # Fraction-based units -> convert to percentage
        if any(x in unit_lower for x in ['fraction of premium', 'fraction of total premium',
                                          'share of premium', 'third of premium',
                                          'up to 50% of premium', 'of employer premium',
                                          'of total premium', 'of non-vpl premium']):
            # Fractions are typically 0-1, convert to percentage (0-100)
            if val <= 1.0:
                return val * 100.0  # Fraction to percentage
            else:
                return val  # Already in percentage
        
        # Percentage units for pension
        if '%' in unit_lower or 'percent' in unit_lower:
            return val  # Already in percentage scale
        
        # EUR or monetary - cannot normalize, return None
        if 'eur' in unit_lower or 'euro' in unit_lower or unit_lower == 'eur':
            return None  # Monetary values cannot be normalized to percentage
        
        # If unit is blank or unknown for pension, assume it's already a percentage
        if assume_percentage_if_unknown or unit_lower == '' or unit_lower == '(blanks)':
            # If value is in reasonable percentage range (0-100), return as-is
            if 0 <= val <= 100:
                return val
            # If value is in fraction range (0-1), convert to percentage
            if 0 <= val <= 1:
                return val * 100.0
    
    # ========================================================================
    # PERCENTAGE-BASED UNITS (for time-related variables)
    # ========================================================================
    # Convert percentage to absolute hours using default_ft_hours as base
    
    if '%' in unit_lower or 'percent' in unit_lower:
        # Percentage of hours worked, agreed hours, etc.
        if any(x in unit_lower for x in ['% of hours worked', 'percent of hours worked',
                                          '% of agreed working hours', 'percent of agreed working hours',
                                          '% of agreed annual working hours', 'percent of agreed annual working hours',
                                          '% of agreed working hours per calendar year',
                                          'percent of agreed working hours per calendar year',
                                          '% of annual working hours', 'percent of annual working hours',
                                          '% of contractual working hours', 'percent of contractual working hours',
                                          '% of paid hour', '% of paid hours',
                                          'percent of paid hour', 'percent of paid hours',
                                          '% of working hours', 'percent of working hours',
                                          'percent of study time within working hours']):
            # Value is percentage (e.g., 50 means 50%)
            return (val / 100.0) * default_ft_hours
        # Generic percentage - assume of full-time hours
        return (val / 100.0) * default_ft_hours
    
    # ========================================================================
    # MULTIPLIER-BASED UNITS
    # ========================================================================
    # "times" or "x" means multiply by base hours
    
    if 'times' in unit_lower or (unit_lower.startswith('x ') and 'hours' in unit_lower):
        if any(x in unit_lower for x in ['times agreed working hours per week',
                                          'times average weekly hours',
                                          'times average weekly hours annually',
                                          'times the average number of hours per week',
                                          'times weekly working hours',
                                          'x average weekly hours',
                                          'x average weekly working hours annually']):
            # Value is multiplier (e.g., 1.5 means 1.5x weekly hours)
            return val * default_ft_hours
        # Generic multiplier
        return val * default_ft_hours
    
    # ========================================================================
    # TIME PERIOD UNITS (weeks, months, years)
    # ========================================================================
    
    # Weeks -> assume hours per week (value is already in hours)
    if unit_lower in ['weeks', 'week']:
        # If value is reasonable for weekly hours, return as-is
        if 20 <= val <= 60:
            return val
        # Otherwise ambiguous
        return None
    
    # Weeks at X% salary -> assume this is weeks of leave at percentage salary, convert to hours
    # Pattern: "weeks at 100% salary", "weeks at 70% or 85% salary", etc.
    if 'weeks at' in unit_lower and ('%' in unit_lower or 'percent' in unit_lower or 'salary' in unit_lower):
        # Extract percentage if possible (e.g., "70%" or "85%")
        # Handle multiple percentages by taking the first one found
        percent_matches = re.findall(r'(\d+)%', unit_lower)
        if percent_matches:
            # Use the first percentage found (or average if multiple, but first is simpler)
            percent_val = float(percent_matches[0])
            # Convert: weeks duration * hours per week * percentage / 52 weeks per year
            # This gives equivalent hours per week
            return (val * default_ft_hours * (percent_val / 100.0)) / 52.0
        else:
            # Default to 100% if no percentage found
            return (val * default_ft_hours) / 52.0
    
    # Monthly supplement -> convert monthly amount to weekly
    if 'monthly supplement' in unit_lower or unit_lower == 'monthly supplement':
        # Assume value is monthly amount, convert to weekly
        return val * 12.0 / 52.0
    
    # Months -> convert to hours per week
    if unit_lower in ['months', 'month']:
        # If value seems like hours per month (e.g., 150-200), convert
        if 100 <= val <= 200:
            return val * 12.0 / 52.0
        # If value seems like months duration (e.g., 1-12), convert to hours
        # Assume 1 month = 4.33 weeks, so multiply by weekly hours
        if 1 <= val <= 24:
            return val * 4.33 * default_ft_hours / 52.0  # Convert months to hours per week
        # Otherwise ambiguous
        return None
    
    # Year/years -> convert to hours per week
    if unit_lower in ['year', 'years']:
        # If value seems like hours per year (e.g., 1800-2100), convert
        if 1500 <= val <= 2500:
            return val / 52.0
        # If value seems like years duration (e.g., 0.5-5), convert to hours
        # Assume 1 year = 52 weeks, so multiply by weekly hours
        if 0.1 <= val <= 10:
            return val * 52.0 * default_ft_hours / 52.0  # Convert years to hours per week
        # Otherwise ambiguous
        return None
    
    # Weeks -> handle as duration (weeks of leave/time off) or as hours per week
    if unit_lower in ['weeks', 'week']:
        # If value seems like weeks duration (e.g., 1-52), convert to equivalent hours per week
        # Example: 5 weeks of leave = (5 weeks * 38 hours/week) / 52 weeks = equivalent hours per week
        if 1 <= val <= 104:  # Up to 2 years - likely duration
            return val * default_ft_hours / 52.0  # Convert weeks duration to equivalent hours per week
        # If value is reasonable for weekly hours (e.g., 20-60), assume it's already hours per week
        if 20 <= val <= 60:
            return val
        # Otherwise ambiguous
        return None
    
    # ========================================================================
    # FALLBACK: If unit contains "per week" or "weekly", assume already weekly
    # ========================================================================
    if 'per week' in unit_lower or 'weekly' in unit_lower:
        return val
    
    # ========================================================================
    # DEFAULT FOR LEAVE/SICKPAY CONTINUATION: Assume percentage if no match
    # ========================================================================
    # For leave continuation values, if we haven't matched any known time unit,
    # assume it's a percentage (common case)
    if assume_percentage_if_unknown:
        # Only assume percentage if value is in reasonable range (0-200%)
        if 0 <= val <= 200:
            return (val / 100.0) * default_ft_hours
        # If value is out of percentage range, return None
        return None
    
    # If no match and not assuming percentage, return None (unknown unit)
    return None


def normalize_boolean(series: pd.Series) -> pd.Series:
    """
    Normalize boolean values to True/False.
    
    Args:
        series: Series with potentially mixed boolean representations
        
    Returns:
        Series with normalized boolean values (True/False/NaN)
    """
    result = series.copy()
    
    # Map common boolean representations
    bool_map = {
        1: True, 0: False,
        "1": True, "0": False,
        "yes": True, "no": False,
        "Yes": True, "No": False,
        "YES": True, "NO": False,
        "true": True, "false": False,
        "True": True, "False": False,
        "TRUE": True, "FALSE": False,
    }
    
    # Apply mapping where applicable
    for val, bool_val in bool_map.items():
        result = result.replace(val, bool_val)
    
    # Convert to boolean, keeping NaN
    result = result.astype(object)
    result = result.where(result.isin([True, False, np.nan]), np.nan)
    
    return result


def load_data(input_path: str) -> pd.DataFrame:
    """
    Load data from Excel or CSV file.
    
    Args:
        input_path: Path to input file (Excel or CSV)
        
    Returns:
        DataFrame with loaded data
    """
    input_file = Path(input_path)
    
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Try to read as Excel first, then CSV
    if input_file.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(input_path)
    else:
        # Try CSV with semicolon separator (as used in descriptives script)
        df = pd.read_csv(input_path, sep=';', encoding='utf-8')
    
    return df


def build_latest_cao_forward_fill(df: pd.DataFrame, cao_col: str = "cao_number",
                                  date_col: str = "ingangsdatum") -> pd.DataFrame:
    """
    Build forward-filled CAO view where each CAO's latest contract data is used
    for all subsequent years until a newer contract appears.
    
    Example: If CAO 134 has contracts in 2013 and 2019:
    - Years 2013-2018: use 2013 contract data
    - Years 2019+: use 2019 contract data
    
    Args:
        df: Input DataFrame with contract data (may already have start_year column)
        cao_col: Column name for CAO number
        date_col: Column name for contract start date
        
    Returns:
        DataFrame with forward-filled contract data (one row per CAO-year combination)
    """
    if cao_col not in df.columns:
        print(f"  Warning: Column '{cao_col}' not found for latest CAO forward-fill")
        return pd.DataFrame()
    
    df_copy = df.copy()
    
    # Create start_year if it doesn't exist
    if "start_year" not in df_copy.columns:
        if date_col not in df_copy.columns:
            print(f"  Warning: Neither 'start_year' nor '{date_col}' found")
            return pd.DataFrame()
        # CAO metadata dates (ingangsdatum, expiratiedatum, datum_kennisgeving) are in DD/MM/YYYY format
        dayfirst = date_col in ['ingangsdatum', 'expiratiedatum', 'datum_kennisgeving']
        df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce', dayfirst=dayfirst)
        df_copy["start_year"] = df_copy[date_col].dt.year
    else:
        # Ensure start_year is numeric
        df_copy["start_year"] = pd.to_numeric(df_copy["start_year"], errors='coerce')
    
    # Filter to valid rows
    valid_mask = df_copy[cao_col].notna() & df_copy["start_year"].notna()
    df_copy = df_copy[valid_mask].copy()
    
    if len(df_copy) == 0:
        return pd.DataFrame()
    
    # Get year range from actual contract start years
    min_year = int(df_copy["start_year"].min())
    max_year = int(df_copy["start_year"].max())
    all_years = range(min_year, max_year + 1)
    
    # Sort by CAO and start_year
    df_copy = df_copy.sort_values([cao_col, "start_year"])
    
    # For each CAO, forward-fill contract data
    result_rows = []
    
    for cao_num in df_copy[cao_col].unique():
        cao_data = df_copy[df_copy[cao_col] == cao_num].copy()
        
        # Get contract years for this CAO (actual contract start years)
        contract_years = sorted([int(y) for y in cao_data["start_year"].unique()])
        
        # For each year in the range, determine which contract applies
        for year in all_years:
            # Find the most recent contract that started on or before this year
            applicable_contracts = [cy for cy in contract_years if cy <= year]
            
            if applicable_contracts:
                # Use the latest contract that started before or in this year
                applicable_year = max(applicable_contracts)
                contract_row = cao_data[cao_data["start_year"] == applicable_year].iloc[0].copy()
                contract_row["start_year"] = year  # Set to current year for aggregation
                result_rows.append(contract_row)
    
    if len(result_rows) == 0:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(result_rows)
    return result_df


def prepare_year_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse date column and create start_year column.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with start_year column added
    """
    df = df.copy()
    
    if "ingangsdatum" not in df.columns:
        raise ValueError("Column 'ingangsdatum' not found in data")
    
    # Parse date column (CAO metadata dates are in DD/MM/YYYY format)
    df["ingangsdatum"] = pd.to_datetime(
        df["ingangsdatum"], errors='coerce', dayfirst=True
    )
    
    # Create start_year column
    df["start_year"] = df["ingangsdatum"].dt.year
    
    # Drop rows where start_year is NaN
    initial_len = len(df)
    df = df[df["start_year"].notna()].copy()
    dropped = initial_len - len(df)
    
    if dropped > 0:
        print(f"  Dropped {dropped} rows with missing start_year")
    
    return df


def compute_numeric_trends(df: pd.DataFrame, var_name: str, start_year_col: str, 
                          min_obs: int = 3, normalize_hours: bool = False,
                          default_ft_hours: float = 38.0) -> Tuple[Optional[pd.Series], Optional[pd.Series]]:
    """
    Compute yearly averages for a numeric variable.
    
    Args:
        df: DataFrame with data
        var_name: Name of numeric variable column
        start_year_col: Name of start year column
        min_obs: Minimum observations per year to include
        normalize_hours: If True, normalize hours values using unit column
        
    Returns:
        Tuple of (means Series, counts Series) or (None, None) if insufficient data
    """
    if var_name not in df.columns:
        return None, None
    
    # Check if we need to normalize hours
    if normalize_hours:
        unit_col = var_name.replace('_value', '_unit')
        if unit_col in df.columns:
            # Normalize values based on units
            normalized_values = []
            for idx, row in df.iterrows():
                value = row[var_name]
                unit = row[unit_col] if unit_col in row else None
                # Try to get contract_full_time_hours for this row if available (for better % conversion)
                ft_hours = default_ft_hours
                if "contract_full_time_hours_value" in df.columns:
                    row_ft_hours = row.get("contract_full_time_hours_value")
                    if pd.notna(row_ft_hours):
                        # Check if it's already normalized or needs normalization
                        ft_unit_col = "contract_full_time_hours_unit"
                        if ft_unit_col in df.columns:
                            ft_unit = row.get(ft_unit_col)
                            normalized_ft = normalize_hours_to_weekly(row_ft_hours, ft_unit, default_ft_hours)
                            if normalized_ft is not None:
                                ft_hours = normalized_ft
                        else:
                            # Assume already in hours per week if reasonable
                            if 20 <= row_ft_hours <= 60:
                                ft_hours = row_ft_hours
                
                # For leave_sickpay_continuation_value, assume percentage if unit doesn't match known time units
                assume_pct = var_name == "leave_sickpay_continuation_value"
                # For pension_employee_contrib_value, also assume percentage if unknown
                if var_name == "pension_employee_contrib_value":
                    assume_pct = True
                normalized = normalize_hours_to_weekly(value, unit, ft_hours, 
                                                       assume_percentage_if_unknown=assume_pct,
                                                       var_name=var_name)
                normalized_values.append(normalized)
            numeric_series = pd.Series(normalized_values, index=df.index)
        else:
            # No unit column, use raw values
            numeric_series = pd.to_numeric(df[var_name], errors='coerce')
    else:
        # Coerce to numeric
        numeric_series = pd.to_numeric(df[var_name], errors='coerce')
    
    # Create temporary dataframe for grouping
    temp_df = pd.DataFrame({
        start_year_col: df[start_year_col],
        var_name: numeric_series
    })
    
    # Group by year and compute mean and count (only on non-NaN values)
    grouped = temp_df.groupby(start_year_col)[var_name].agg([
        ('mean', lambda x: x.dropna().mean() if x.notna().sum() > 0 else np.nan),
        ('count', lambda x: x.notna().sum())
    ])
    
    # Filter years with sufficient observations
    grouped = grouped[grouped['count'] >= min_obs]
    
    if len(grouped) == 0:
        return None, None
    
    years = grouped.index
    means = grouped['mean']
    counts = grouped['count']
    
    return pd.Series(means.values, index=years), pd.Series(counts.values, index=years)


def compute_boolean_trends(df: pd.DataFrame, var_name: str, start_year_col: str,
                           min_obs: int = 3) -> Tuple[Optional[pd.Series], Optional[pd.Series]]:
    """
    Compute yearly shares of TRUE for a boolean variable.
    
    Args:
        df: DataFrame with data
        var_name: Name of boolean variable column
        start_year_col: Name of start year column
        min_obs: Minimum observations per year to include
        
    Returns:
        Tuple of (years, shares) Series, and (years, counts) Series for sample sizes
    """
    if var_name not in df.columns:
        return None, None
    
    # Normalize boolean
    bool_series = normalize_boolean(df[var_name])
    
    # Create temporary dataframe for grouping
    temp_df = pd.DataFrame({
        start_year_col: df[start_year_col],
        var_name: bool_series
    })
    
    # Group by year and compute share of TRUE and count
    def compute_share(x):
        valid = x.dropna()
        if len(valid) == 0:
            return np.nan
        return (valid == True).mean()
    
    grouped = temp_df.groupby(start_year_col)[var_name].agg([
        ('share_true', compute_share),
        ('count', 'count')
    ])
    
    # Filter years with sufficient observations
    grouped = grouped[grouped['count'] >= min_obs]
    
    if len(grouped) == 0:
        return None, None
    
    years = grouped.index
    shares = grouped['share_true']
    counts = grouped['count']
    
    return pd.Series(shares.values, index=years), pd.Series(counts.values, index=years)


def plot_numeric_trends(df: pd.DataFrame, start_year_col: str, output_dir: Path, 
                       min_obs: int = 3, use_latest_cao_view: bool = False) -> None:
    """
    Plot numeric variable trends grouped by figure.
    
    Args:
        df: DataFrame with data
        start_year_col: Name of start year column
        output_dir: Directory to save plots
        min_obs: Minimum observations per year
        use_latest_cao_view: If True, use forward-filled latest CAO view
    """
    # Use latest CAO view if requested
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print(f"  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_plot = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                date_col="ingangsdatum")
        if len(df_plot) == 0:
            print(f"  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
        suffix = "_latest_cao_view"
    else:
        df_plot = df
        suffix = ""
    
    for fig_filename, var_list in NUMERIC_FIGURE_GROUPS.items():
        # Add suffix to filename if using latest CAO view
        base_filename = fig_filename.replace('.png', '')
        fig_filename_with_suffix = f"{base_filename}{suffix}.png" if suffix else fig_filename
        print(f"\nCreating figure: {fig_filename_with_suffix}")
        
        plot_data = []
        sample_sizes = {}
        
        for var_name in var_list:
            if var_name not in df_plot.columns:
                print(f"  [WARN] Column '{var_name}' not found; skipping numeric trend.")
                continue
            
            # Check if this is a variable that needs normalization
            normalize_hours = var_name in ["contract_full_time_hours_value", 
                                          "overtime_max_hours_per_week_value",
                                          "leave_vacation_time_value",
                                          "leave_sickpay_duration_value",
                                          "leave_sickpay_continuation_value",
                                          "pension_employee_contrib_value",
                                          "training_time_yearly_value"]
            # Note: pension_retire_age_normal_value is in years (age), no normalization needed
            
            means, counts = compute_numeric_trends(df_plot, var_name, start_year_col, min_obs, 
                                                   normalize_hours=normalize_hours,
                                                   default_ft_hours=38.0)
            
            if means is None or len(means) == 0:
                print(f"  [WARN] Column '{var_name}' has insufficient data; skipping.")
                continue
            
            plot_data.append((var_name, means))
            sample_sizes[var_name] = counts
            
            # Print sample sizes and detect potential outliers
            print(f"  {var_name}:")
            mean_values = means.values
            median_val = np.median(mean_values[~np.isnan(mean_values)])
            std_val = np.std(mean_values[~np.isnan(mean_values)])
            
            for year in means.index:
                n = counts.loc[year]
                mean_val = means.loc[year]
                # Flag potential outliers (more than 2 standard deviations from median)
                is_outlier = abs(mean_val - median_val) > 2 * std_val if not np.isnan(std_val) and std_val > 0 else False
                outlier_flag = " [OUTLIER?]" if is_outlier else ""
                small_sample_flag = " [SMALL SAMPLE]" if n < 5 else ""
                print(f"    Year {int(year)}: n={int(n)}, mean={mean_val:.2f}{outlier_flag}{small_sample_flag}")
        
        if len(plot_data) == 0:
            print(f"  [INFO] No data available for figure {fig_filename}; skipping.")
            continue
        
        # Compute CAO counts per year
        cao_counts = {}
        # Collect all years from plot data
        all_years = set()
        for _, means in plot_data:
            all_years.update(means.index)
        
        # Fill in CAO counts for all years in plot
        if "cao_number" in df_plot.columns:
            for year in all_years:
                year_data = df_plot[df_plot[start_year_col] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
        
        # Create plot with secondary axis for CAO counts
        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        for var_name, means in plot_data:
            ax1.plot(means.index, means.values, marker='o', label=var_name, linewidth=2)
        
        ax1.set_xlabel("Contract start year", fontsize=12)
        # Set x-axis limits and ticks (2007-2027, every 2 years)
        ax1.set_xlim(2007, 2027)
        ax1.set_xticks(range(2007, 2028, 2))
        # Determine y-axis label based on variables in the plot
        has_hours_vars = any("hours" in v for v, _ in plot_data)
        has_leave_vars = any("leave" in v for v, _ in plot_data)
        has_training_vars = any("training" in v for v, _ in plot_data)
        has_pension_contrib = any("pension_employee_contrib" in v for v, _ in plot_data)
        has_pension_age = any("pension_retirement_age" in v for v, _ in plot_data)
        
        if has_pension_contrib:
            ylabel = "Average value (percentage, normalized)"
        elif has_pension_age:
            ylabel = "Average value (age in years)"
        elif has_hours_vars or has_leave_vars or has_training_vars:
            ylabel = "Average value (hours per week, normalized)"
        else:
            ylabel = "Average value"
        ax1.set_ylabel(ylabel, fontsize=12)
        title_base = base_filename.replace('_', ' ').title()
        title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
        ax1.set_title(f"{title_base}{title_suffix}", fontsize=14)
        ax1.grid(True, alpha=0.3)
        ax1.legend(fontsize=10)
        
        # Add secondary axis for CAO counts
        if cao_counts:
            years = sorted(cao_counts.keys())
            counts_list = [cao_counts[y] for y in years]
            ax2 = ax1.twinx()
            ax2.bar(years, counts_list, alpha=0.2, color='gray', label='Number of CAOs')
            ax2.set_ylabel("Number of CAOs", fontsize=12, color='gray')
            ax2.tick_params(axis='y', labelcolor='gray')
            # Add count annotations on bars
            for year, count in zip(years, counts_list):
                if count > 0:  # Only annotate if count > 0
                    ax2.text(year, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=8, color='gray')
        
        plt.tight_layout()
        
        # Save plot to numeric subfolder
        numeric_dir = output_dir / "numeric"
        numeric_dir.mkdir(parents=True, exist_ok=True)
        output_path = numeric_dir / fig_filename_with_suffix
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Saved: {output_path}")


def plot_boolean_trends_by_domain(df: pd.DataFrame, start_year_col: str, output_dir: Path,
                                  min_obs: int = 3, use_latest_cao_view: bool = False) -> None:
    """
    Plot boolean variable trends grouped by domain (share of TRUE over time).
    
    Creates separate plots for each domain group.
    
    Args:
        df: DataFrame with data
        start_year_col: Name of start year column
        output_dir: Directory to save plots
        min_obs: Minimum observations per year
        use_latest_cao_view: If True, use forward-filled latest CAO view
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    
    # Use latest CAO view if requested
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print(f"  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_plot = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                date_col="ingangsdatum")
        if len(df_plot) == 0:
            print(f"  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
    else:
        df_plot = df
    
    # Plot each domain group
    for domain_key, domain_info in sorted(BOOLEAN_DOMAIN_GROUPS.items()):
        base_filename = domain_info["filename"].replace('.png', '')
        fig_filename = f"{base_filename}{suffix}.png"
        title = domain_info["title"]
        var_list = domain_info["vars"]
        
        print(f"\nCreating figure: {fig_filename}")
        
        plot_data = []
        sample_sizes = {}
        
        # Collect data for this domain
        for var_name in var_list:
            if var_name not in df_plot.columns:
                print(f"  [WARN] Column '{var_name}' not found; skipping boolean trend.")
                continue
            
            shares, counts = compute_boolean_trends(df_plot, var_name, start_year_col, min_obs)
            
            if shares is None or len(shares) == 0:
                print(f"  [WARN] Column '{var_name}' has insufficient data; skipping.")
                continue
            
            # Check if variable has any variation (not all True or all False)
            unique_shares = shares.dropna().unique()
            if len(unique_shares) <= 1:
                print(f"  [WARN] Column '{var_name}' has no variation; skipping.")
                continue
            
            plot_data.append((var_name, shares))
            sample_sizes[var_name] = counts
            
            # Print sample sizes for this variable
            print(f"  {var_name}:")
            for year in shares.index:
                n = counts.loc[year]
                share_val = shares.loc[year]
                print(f"    Year {int(year)}: n={int(n)}, share={share_val:.2%}")
        
        if len(plot_data) == 0:
            print(f"  [INFO] No data available for figure {fig_filename}; skipping.")
            continue
        
        # Compute CAO counts per year
        cao_counts = {}
        if "cao_number" in df_plot.columns:
            # Collect all years from plot data
            all_years = set()
            for _, shares in plot_data:
                all_years.update(shares.index)
            
            for year in all_years:
                year_data = df_plot[df_plot[start_year_col] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
        
        # Create plot with secondary axis for CAO counts
        fig, ax1 = plt.subplots(figsize=(12, 7))
        
        # Plot shares on primary axis
        for var_name, shares in plot_data:
            # Use clear label with explanation if available
            label = BOOLEAN_LABELS.get(var_name, var_name.replace('_', ' ').title())
            ax1.plot(shares.index, shares.values * 100, marker='o', label=label, 
                    linewidth=2, markersize=6)
        
        ax1.set_xlabel("Contract start year", fontsize=12)
        ax1.set_ylabel("Share of contracts with feature (%)", fontsize=12)
        # Set x-axis limits and ticks (2007-2027, every 2 years)
        ax1.set_xlim(2007, 2027)
        ax1.set_xticks(range(2007, 2028, 2))
        title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
        ax1.set_title(f"{title}{title_suffix}", fontsize=14)
        ax1.grid(True, alpha=0.3)
        
        # Determine legend placement based on number of variables
        n_vars = len(plot_data)
        if n_vars <= 4:
            ncol = 1
            bbox_y = -0.08
        elif n_vars <= 8:
            ncol = 2
            bbox_y = -0.12
        else:
            ncol = 3
            bbox_y = -0.15
        
        ax1.legend(fontsize=8, loc='upper center', bbox_to_anchor=(0.5, bbox_y), 
                  ncol=ncol, framealpha=0.9, columnspacing=1.0, handlelength=1.5)
        
        # Add secondary axis for CAO counts only (very light background)
        if cao_counts:
            years = sorted(cao_counts.keys())
            cao_list = [cao_counts[y] for y in years]
            ax2 = ax1.twinx()
            # Very light bars (alpha=0.1) so they don't interfere with the main plot
            ax2.bar(years, cao_list, alpha=0.1, color='gray', label='Number of CAOs')
            ax2.set_ylabel("Number of CAOs", fontsize=12, color='gray')
            ax2.tick_params(axis='y', labelcolor='gray')
            # Add count annotations on bars (very light)
            for year, count in zip(years, cao_list):
                if count > 0:
                    ax2.text(year, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='gray', alpha=0.6)
        
        # Adjust layout to accommodate legend outside plot area at bottom
        plt.tight_layout(rect=[0, abs(bbox_y) + 0.02, 1, 1])
        
        # Save plot to appropriate boolean subfolder (remove suffix since folder indicates type)
        base_filename = fig_filename.replace('_latest_cao_view.png', '.png').replace('.png', '')
        final_filename = f"{base_filename}.png"
        
        if use_latest_cao_view:
            boolean_dir = output_dir / "boolean" / "latest_cao_view"
        else:
            boolean_dir = output_dir / "boolean" / "new_cao_yearly"
        boolean_dir.mkdir(parents=True, exist_ok=True)
        output_path = boolean_dir / final_filename
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Saved: {output_path}")


def plot_contract_counts_comparison(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot comparison of contract counts per year for ingangsdatum vs general_start_date.
    
    Args:
        df: DataFrame with data
        output_dir: Directory to save plots
    """
    print("\nCreating figure: non_salary_contract_counts_comparison.png")
    
    df_copy = df.copy()
    
    # Check if both date columns exist
    if "ingangsdatum" not in df_copy.columns:
        print(f"  [WARN] Column 'ingangsdatum' not found; skipping contract counts comparison.")
        print(f"  Available columns: {list(df_copy.columns[:20])}...")
        return
    
    # Try to find the contract start date column (could be general_start_date or start_date)
    contract_date_col = None
    if "general_start_date" in df_copy.columns:
        contract_date_col = "general_start_date"
    elif "start_date" in df_copy.columns:
        contract_date_col = "start_date"
    else:
        print(f"  [WARN] Column 'general_start_date' or 'start_date' not found; skipping contract counts comparison.")
        print(f"  Available columns: {list(df_copy.columns[:20])}...")
        return
    
    # Parse both date columns (CAO metadata dates are in DD/MM/YYYY format)
    df_copy["ingangsdatum"] = pd.to_datetime(df_copy["ingangsdatum"], errors='coerce', dayfirst=True)
    df_copy[contract_date_col] = pd.to_datetime(df_copy[contract_date_col], errors='coerce')
    
    # Extract years from both columns
    df_copy["year_ingangsdatum"] = df_copy["ingangsdatum"].dt.year
    df_copy["year_contract_start"] = df_copy[contract_date_col].dt.year
    
    # Diagnostic information
    total_rows = len(df_copy)
    ingangsdatum_notna = df_copy["ingangsdatum"].notna().sum()
    contract_start_notna = df_copy[contract_date_col].notna().sum()
    ingangsdatum_year_notna = df_copy["year_ingangsdatum"].notna().sum()
    contract_start_year_notna = df_copy["year_contract_start"].notna().sum()
    
    print(f"  Diagnostic info:")
    print(f"    Total rows in dataset: {total_rows}")
    print(f"    Rows with non-null ingangsdatum: {ingangsdatum_notna}")
    print(f"    Rows with non-null {contract_date_col}: {contract_start_notna}")
    print(f"    Rows with valid ingangsdatum year: {ingangsdatum_year_notna}")
    print(f"    Rows with valid {contract_date_col} year: {contract_start_year_notna}")
    
    # Check for rows where one date exists but the other doesn't
    both_dates = (df_copy["ingangsdatum"].notna() & df_copy[contract_date_col].notna()).sum()
    only_ingangsdatum = (df_copy["ingangsdatum"].notna() & df_copy[contract_date_col].isna()).sum()
    only_contract_start = (df_copy["ingangsdatum"].isna() & df_copy[contract_date_col].notna()).sum()
    neither_date = (df_copy["ingangsdatum"].isna() & df_copy[contract_date_col].isna()).sum()
    
    print(f"    Rows with both dates: {both_dates}")
    print(f"    Rows with only ingangsdatum: {only_ingangsdatum}")
    print(f"    Rows with only {contract_date_col}: {only_contract_start}")
    print(f"    Rows with neither date: {neither_date}")
    
    # Count contracts per year for ingangsdatum (only rows with valid year)
    ingangsdatum_counts = df_copy[df_copy["year_ingangsdatum"].notna()].groupby("year_ingangsdatum").size()
    ingangsdatum_counts = ingangsdatum_counts.sort_index()
    
    # Count contracts per year for general_start_date (only rows with valid year)
    contract_start_counts = df_copy[df_copy["year_contract_start"].notna()].groupby("year_contract_start").size()
    contract_start_counts = contract_start_counts.sort_index()
    
    # Verify totals
    total_ingangsdatum_counted = ingangsdatum_counts.sum()
    total_contract_start_counted = contract_start_counts.sum()
    print(f"    Total counted (ingangsdatum): {total_ingangsdatum_counted} (should be {ingangsdatum_year_notna})")
    print(f"    Total counted (contract_start): {total_contract_start_counted} (should be {contract_start_year_notna})")
    
    # Get the union of all years
    all_years = sorted(set(ingangsdatum_counts.index) | set(contract_start_counts.index))
    
    if len(all_years) == 0:
        print(f"  [WARN] No data available; skipping contract counts comparison.")
        return
    
    # Determine year range from data (convert to int to avoid float issues)
    min_year = int(min(all_years))
    max_year = int(max(all_years))
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Prepare data for bar chart
    ingangsdatum_values = [ingangsdatum_counts.get(y, 0) for y in all_years]
    contract_start_values = [contract_start_counts.get(y, 0) for y in all_years]
    
    # Set bar width and positions
    bar_width = 0.35
    x_pos = range(len(all_years))
    x_pos_ing = [x - bar_width/2 for x in x_pos]
    x_pos_contract = [x + bar_width/2 for x in x_pos]
    
    # Create bars
    bars1 = ax.bar(x_pos_ing, ingangsdatum_values, bar_width, label='ingangsdatum (website date)', 
                   color='#2E86AB', alpha=0.8)
    bars2 = ax.bar(x_pos_contract, contract_start_values, bar_width, label=f'{contract_date_col} (PDF date)', 
                   color='#A23B72', alpha=0.8)
    
    # Set x-axis labels and ticks
    ax.set_xlabel("Year", fontsize=12)
    ax.set_ylabel("Number of contracts", fontsize=12)
    ax.set_title(f"Contract Counts Comparison: ingangsdatum vs {contract_date_col}", fontsize=14)
    ax.set_xticks(x_pos)
    ax.set_xticklabels([str(int(y)) for y in all_years], rotation=45, ha='right')
    
    # Add grid
    ax.grid(True, alpha=0.3, axis='y')
    ax.legend(fontsize=11, loc='best', framealpha=0.9)
    
    # Add value annotations on bars
    for i, (val_ing, val_contract) in enumerate(zip(ingangsdatum_values, contract_start_values)):
        if val_ing > 0:
            ax.text(x_pos_ing[i], val_ing, f'{int(val_ing)}', ha='center', va='bottom', 
                   fontsize=8, color='#2E86AB', fontweight='bold')
        if val_contract > 0:
            ax.text(x_pos_contract[i], val_contract, f'{int(val_contract)}', ha='center', va='bottom', 
                   fontsize=8, color='#A23B72', fontweight='bold')
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / "non_salary_contract_counts_comparison.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")
    print(f"  Year range: {min_year} - {max_year}")
    print(f"  Years with ingangsdatum data: {len(ingangsdatum_counts)}")
    print(f"  Years with contract_start data: {len(contract_start_counts)}")
    print(f"  Total contracts (ingangsdatum): {sum(ingangsdatum_values)}")
    print(f"  Total contracts (contract_start): {sum(contract_start_values)}")
    print(f"  Note: Counts only include rows with valid dates. Missing dates:")
    print(f"    - {total_rows - ingangsdatum_notna} rows missing ingangsdatum")
    print(f"    - {total_rows - contract_start_notna} rows missing {contract_date_col}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point for plotting script."""
    print("="*80)
    print("CAO Non-Salary Time Trend Plotting Script")
    print("="*80)
    
    # Load data
    print(f"\nLoading data from: {INPUT_EXCEL_PATH}")
    try:
        df = load_data(INPUT_EXCEL_PATH)
        print(f"  Loaded {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"  ERROR: Could not load input file: {e}")
        return
    
    if len(df) == 0:
        print("  ERROR: Input file is empty")
        return
    
    # Create output directory
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")
    
    # Generate contract counts comparison plot (before preparing year column)
    print("\n" + "="*80)
    print("Generating contract counts comparison plot...")
    print("="*80)
    try:
        plot_contract_counts_comparison(df, output_dir)
    except Exception as e:
        print(f"  ERROR in contract counts comparison: {e}")
        import traceback
        traceback.print_exc()
    
    # Prepare year column
    print("\nPreparing year column...")
    try:
        df = prepare_year_column(df)
        print(f"  Data after year extraction: {len(df)} rows")
        print(f"  Year range: {int(df['start_year'].min())} - {int(df['start_year'].max())}")
    except Exception as e:
        print(f"  ERROR: Could not prepare year column: {e}")
        return
    
    # Generate plots
    print("\n" + "="*80)
    print("Generating plots...")
    print("="*80)
    
    # Generate standard numeric plots
    try:
        plot_numeric_trends(df, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in numeric trends: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View numeric plots
    try:
        plot_numeric_trends(df, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=True)
    except Exception as e:
        print(f"  ERROR in numeric trends (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    # Generate standard boolean plots by domain
    try:
        plot_boolean_trends_by_domain(df, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in boolean trends: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View boolean plots by domain
    try:
        plot_boolean_trends_by_domain(df, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=True)
    except Exception as e:
        print(f"  ERROR in boolean trends (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*80)
    print("Script completed successfully!")
    print("="*80)


if __name__ == "__main__":
    main()

