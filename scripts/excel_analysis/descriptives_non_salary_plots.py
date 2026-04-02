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
      
      Numeric trend plots: one combined chart per figure with line legends, optional right y-axis for mixed scales, and CAO counts as light background bars:
      - non_salary_numeric_hours_trends.png (+ _latest_cao_view version)
      - non_salary_numeric_leave_trends.png (+ _latest_cao_view version)
      - non_salary_numeric_pension_training_trends.png (+ _latest_cao_view version)
      Unit conversion rules: scripts/excel_analysis/non_salary_unit_normalization.py
      
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
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.excel_analysis.analysis_utils import (
    build_latest_cao_forward_fill_by_file,
    parse_cao_date_series,
    filter_non_salary_for_plot,
    get_plot_color_cycle,
    enforce_integer_year_axis,
    parse_updated_topics_cell,
)
from scripts.excel_analysis.non_salary_unit_normalization import (
    normalize_for_plot,
    NUMERIC_VAR_LEGEND_LABELS,
    NUMERIC_VAR_YLABELS,
)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
INPUT_EXCEL_PATH = "outputs/excel/new_results/extracted_data_non_salary.csv"
OUTPUT_DIR = "outputs/analysis/figures/"
MIN_OBS_PER_YEAR = 3  # Minimum observations per year to include in plot

# Columns that mix numeric and text in CSV exports; force str so read_csv does not
# chunk-infer mixed dtypes ( DtypeWarning ) without using low_memory=False.
_MIXED_TYPE_STRING_COLS: Tuple[str, ...] = (
    "childcare_support_cap_unit",
    "childcare_min_fte_unit",
)

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
            "leave_parental_statutory_ref",
            "leave_parental_exceptions",
            "leave_parental_eligibility_present",
            "leave_parental_topup_present",
            "leave_abortion_present",
            "leave_sick_topup_present",
            "leave_sickpay_extra_insurance_present",
            "leave_care_statutory_ref",
            "leave_care_exceptions",
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

def plot_document_type_and_topics(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot general_document_type distribution and top updated topics.
    """
    if "general_document_type" in df.columns:
        counts = df["general_document_type"].fillna("unknown").astype(str).str.lower().value_counts()
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(counts.index.tolist(), counts.values.tolist(), color=get_plot_color_cycle(len(counts)))
        ax.set_title("Non-salary document type distribution", fontsize=14)
        ax.set_xlabel("general_document_type")
        ax.set_ylabel("Count")
        plt.xticks(rotation=35, ha="right")
        plt.tight_layout()
        plt.savefig(output_dir / "non_salary_document_type_distribution.png", dpi=300, bbox_inches="tight")
        plt.close()

    if "general_updated_topics" in df.columns:
        parsed = df["general_updated_topics"].apply(lambda v: parse_updated_topics_cell(v)[0] if pd.notna(v) else [])
        flat = [item for sublist in parsed for item in sublist]
        if flat:
            vc = pd.Series(flat).value_counts().head(10)
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.bar(vc.index.tolist(), vc.values.tolist(), color=get_plot_color_cycle(len(vc)))
            ax.set_title("Top 10 general_updated_topics mentions", fontsize=14)
            ax.set_xlabel("updated topic")
            ax.set_ylabel("Count")
            plt.xticks(rotation=35, ha="right")
            plt.tight_layout()
            plt.savefig(output_dir / "non_salary_updated_topics_top10.png", dpi=300, bbox_inches="tight")
            plt.close()

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
    "leave_parental_statutory_ref": "Parental leave: mentions statutory law",
    "leave_parental_exceptions": "Parental leave: exceptions to statutory",
    "leave_parental_eligibility_present": "Parental leave eligibility (tenure/contract) present",
    "leave_abortion_present": "Abortion leave present",
    "leave_sick_topup_present": "Sick leave topup present",
    "leave_sickpay_extra_insurance_present": "Sickpay extra insurance present",
    "leave_care_statutory_ref": "Care leave: mentions statutory law",
    "leave_care_exceptions": "Care leave: exceptions to statutory",
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

def normalize_boolean(series: pd.Series) -> pd.Series:
    """
    Normalize boolean values to True/False.
    
    Args:
        series: Series with potentially mixed boolean representations
        
    Returns:
        Series with normalized boolean values (True/False/NaN)
    """
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
    
    # Apply mapping without Series.replace (avoids deprecated downcasting warnings).
    result = series.astype(object).copy()
    for val, bool_val in bool_map.items():
        result = result.mask(result == val, bool_val)

    # Convert to boolean, keeping NaN
    result = result.astype(object)
    result = result.where(result.isin([True, False, np.nan]), np.nan)
    
    return result


def log_memory(label: str, frame: pd.DataFrame) -> None:
    """
    Log approximate DataFrame memory in MB for run diagnostics.

    Args:
        label: Checkpoint label
        frame: DataFrame to inspect

    Returns:
        None
    """
    try:
        mem_mb = frame.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"  [MEM] {label}: {mem_mb:,.2f} MB")
    except Exception:
        pass


def load_data(input_path: str) -> pd.DataFrame:
    """
    Load data from Excel or CSV file.

    For semicolon CSVs, reads the header once then loads with str dtypes only for
    columns listed in _MIXED_TYPE_STRING_COLS (avoids mixed-type DtypeWarning
    without low_memory=False).

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
        # Header-only read to build targeted dtypes (minimal RAM); avoids DtypeWarning
        # on columns that mix types without setting low_memory=False.
        header = pd.read_csv(input_path, sep=";", encoding="utf-8", nrows=0)
        dtype_spec = {
            name: str
            for name in _MIXED_TYPE_STRING_COLS
            if name in header.columns
        }
        read_kw: dict = {"sep": ";", "encoding": "utf-8"}
        if dtype_spec:
            read_kw["dtype"] = dtype_spec
        df = pd.read_csv(input_path, **read_kw)
    
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
    df_copy = df.copy()
    if "start_year" not in df_copy.columns:
        if date_col not in df_copy.columns:
            print(f"  Warning: Neither 'start_year' nor '{date_col}' found")
            return pd.DataFrame()
        dayfirst = date_col in ['ingangsdatum', 'expiratiedatum', 'datum_kennisgeving']
        df_copy[date_col] = parse_cao_date_series(df_copy[date_col], dayfirst=dayfirst)
        df_copy["start_year"] = df_copy[date_col].dt.year
    return build_latest_cao_forward_fill_by_file(
        df_copy,
        cao_col=cao_col,
        year_col="start_year",
        file_col="file_name",
        order_date_col=date_col if date_col in df_copy.columns else None,
    )


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
    df["ingangsdatum"] = parse_cao_date_series(df["ingangsdatum"], dayfirst=True)
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
                          default_ft_hours: float = 38.0,
                          agg_kind: str = "mean") -> Tuple[Optional[pd.Series], Optional[pd.Series]]:
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
    
    # Unit-based normalization (canonical scale per variable; see non_salary_unit_normalization)
    if normalize_hours:
        unit_col = var_name.replace("_value", "_unit")
        if unit_col in df.columns:
            normalized_values = []
            for _, row in df.iterrows():
                normalized = normalize_for_plot(
                    var_name,
                    row[var_name],
                    row.get(unit_col),
                    default_ft_hours=default_ft_hours,
                    row=row,
                )
                normalized_values.append(normalized)
            numeric_series = pd.Series(normalized_values, index=df.index)
        else:
            numeric_series = pd.to_numeric(df[var_name], errors="coerce")
    else:
        # Coerce to numeric
        numeric_series = pd.to_numeric(df[var_name], errors='coerce')
    
    # Create temporary dataframe for grouping
    temp_df = pd.DataFrame({
        start_year_col: df[start_year_col],
        var_name: numeric_series
    })
    
    agg_kind = agg_kind.lower().strip()
    if agg_kind not in {"mean", "median"}:
        agg_kind = "mean"
    agg_fn = np.mean if agg_kind == "mean" else np.median
    grouped = temp_df.groupby(start_year_col)[var_name].agg([
        ('value', lambda x: agg_fn(x.dropna()) if x.notna().sum() > 0 else np.nan),
        ('count', lambda x: x.notna().sum())
    ])
    
    # Filter years with sufficient observations
    grouped = grouped[grouped['count'] >= min_obs]
    
    if len(grouped) == 0:
        return None, None
    
    years = grouped.index
    means = grouped['value']
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
                       min_obs: int = 3, use_latest_cao_view: bool = False,
                       agg_kind: str = "mean",
                       df_latest_view: Optional[pd.DataFrame] = None) -> None:
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
    agg_kind = agg_kind.lower().strip()
    if agg_kind not in {"mean", "median"}:
        agg_kind = "mean"
    if use_latest_cao_view:
        df_plot = df_latest_view if df_latest_view is not None else build_latest_cao_forward_fill(
            df, cao_col="cao_number", date_col="ingangsdatum"
        )
        if len(df_plot) == 0:
            print(f"  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
        suffix = "_latest_cao_view"
    else:
        df_plot = df
        suffix = ""
    if agg_kind == "median":
        suffix = f"{suffix}_median" if suffix else "_median"
    
    df_plot_local = filter_non_salary_for_plot(df_plot)
    for fig_filename, var_list in NUMERIC_FIGURE_GROUPS.items():
        # Add suffix to filename if using latest CAO view
        base_filename = fig_filename.replace('.png', '')
        fig_filename_with_suffix = f"{base_filename}{suffix}.png" if suffix else fig_filename
        print(f"\nCreating figure: {fig_filename_with_suffix}")
        
        plot_data = []
        sample_sizes = {}
        
        for var_name in var_list:
            if var_name not in df_plot_local.columns:
                print(f"  [WARN] Column '{var_name}' not found; skipping numeric trend.")
                continue
            
            normalize_hours = var_name in [
                "contract_full_time_hours_value",
                "overtime_max_hours_per_week_value",
                "leave_vacation_time_value",
                "leave_sickpay_duration_value",
                "leave_sickpay_continuation_value",
                "pension_employee_contrib_value",
                "pension_retire_age_normal_value",
                "training_time_yearly_value",
            ]
            
            means, counts = compute_numeric_trends(
                df_plot_local,
                var_name,
                start_year_col,
                min_obs,
                normalize_hours=normalize_hours,
                default_ft_hours=38.0,
                agg_kind=agg_kind,
            )
            
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
                print(f"    Year {int(year)}: n={int(n)}, {agg_kind}={mean_val:.2f}{outlier_flag}{small_sample_flag}")
        
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
        if "cao_number" in df_plot_local.columns:
            for year in all_years:
                year_data = df_plot_local[df_plot_local[start_year_col] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
        
        # Single combined chart per figure with optional dual y-axis for mixed scales/units.
        fig, ax_left = plt.subplots(figsize=(12, 7))
        colors = get_plot_color_cycle(max(len(plot_data), 1))
        title_base = base_filename.replace("_", " ").title()
        if use_latest_cao_view:
            title_suffix = " (Latest CAO View)"
        else:
            title_suffix = ""
        if agg_kind == "median":
            title_suffix = f"{title_suffix} (Median)"
        ax_left.set_title(f"{title_base}{title_suffix}", fontsize=14)

        series_meta = []
        for var_name, means in plot_data:
            y_label = NUMERIC_VAR_YLABELS.get(var_name, "Mean")
            valid = means.dropna()
            median_value = float(np.median(valid.values)) if len(valid) > 0 else 1.0
            series_meta.append({
                "var_name": var_name,
                "means": means,
                "legend_label": NUMERIC_VAR_LEGEND_LABELS.get(var_name, var_name),
                "y_label": y_label,
                "median": abs(median_value) if median_value != 0 else 1.0,
            })

        y_labels = sorted({item["y_label"] for item in series_meta})
        medians = [max(item["median"], 1e-6) for item in series_meta]
        scale_ratio = (max(medians) / min(medians)) if medians else 1.0
        use_right_axis = len(series_meta) > 1 and (len(y_labels) > 1 or scale_ratio >= 3.0)
        ax_right = ax_left.twinx() if use_right_axis else None

        if use_right_axis:
            split_threshold = np.sqrt(max(medians) * min(medians))
            left_series = [item for item in series_meta if item["median"] <= split_threshold]
            right_series = [item for item in series_meta if item["median"] > split_threshold]
            if len(left_series) == 0:
                left_series = [min(series_meta, key=lambda x: x["median"])]
                right_series = [item for item in series_meta if item["var_name"] != left_series[0]["var_name"]]
            if len(right_series) == 0:
                right_series = [max(series_meta, key=lambda x: x["median"])]
                left_series = [item for item in series_meta if item["var_name"] != right_series[0]["var_name"]]
        else:
            left_series = series_meta
            right_series = []

        def _axis_ylabel(items: List[Dict[str, object]], fallback: str = "Value") -> str:
            labels = sorted({str(item["y_label"]) for item in items})
            if len(labels) == 1:
                return labels[0]
            if len(labels) > 1:
                return " / ".join(labels)
            return fallback

        line_handles = []
        for i, item in enumerate(left_series):
            line, = ax_left.plot(
                item["means"].index.astype(int),
                item["means"].values,
                marker="o",
                label=item["legend_label"],
                linewidth=2,
                color=colors[i % len(colors)],
            )
            line_handles.append(line)

        if use_right_axis and ax_right is not None:
            for j, item in enumerate(right_series):
                idx = len(left_series) + j
                line, = ax_right.plot(
                    item["means"].index.astype(int),
                    item["means"].values,
                    marker="o",
                    label=item["legend_label"],
                    linewidth=2,
                    linestyle="--",
                    color=colors[idx % len(colors)],
                )
                line_handles.append(line)
            ax_right.set_ylabel(_axis_ylabel(right_series, "Multiple metrics (see legend)"), fontsize=11)
            ax_right.tick_params(axis="y")

        # Label first and last points for retirement age in latest-view pension/training plot.
        if use_latest_cao_view and "pension_training" in base_filename:
            def _annotate_retirement_endpoints(series_items: List[Dict[str, object]], target_ax: plt.Axes) -> None:
                for item in series_items:
                    if item["var_name"] != "pension_retire_age_normal_value":
                        continue
                    s = item["means"].dropna()
                    if len(s) == 0:
                        return
                    first_x = int(s.index.min())
                    last_x = int(s.index.max())
                    first_y = float(s.loc[first_x])
                    last_y = float(s.loc[last_x])
                    target_ax.text(first_x, first_y, f"{first_y:.1f}", fontsize=8, ha="right", va="bottom")
                    if last_x != first_x:
                        target_ax.text(last_x, last_y, f"{last_y:.1f}", fontsize=8, ha="left", va="bottom")
                    return
            _annotate_retirement_endpoints(left_series, ax_left)
            if use_right_axis and ax_right is not None:
                _annotate_retirement_endpoints(right_series, ax_right)

        ax_left.set_xlabel("Contract start year", fontsize=12)
        ax_left.set_ylabel(_axis_ylabel(left_series, "Value"), fontsize=11)
        enforce_integer_year_axis(ax_left, [int(y) for y in all_years])
        ax_left.grid(True, alpha=0.3)

        # CAO counts on dedicated count axis to preserve line readability.
        if cao_counts:
            years = sorted(cao_counts.keys())
            counts_list = [cao_counts[y] for y in years]
            ax_counts = ax_left.twinx()
            if use_right_axis:
                ax_counts.spines["right"].set_position(("axes", 1.10))
            ax_counts.bar(years, counts_list, alpha=0.08, color="gray", label="Number of CAOs", zorder=0)
            ax_counts.set_ylabel("Number of CAOs", fontsize=10, color="gray")
            ax_counts.tick_params(axis="y", labelcolor="gray")
            for year, count in zip(years, counts_list):
                if count > 0:
                    ax_counts.text(year, count, f"{int(count)}", ha="center", va="bottom", fontsize=7, color="gray", alpha=0.7)
            ax_counts.set_zorder(0)
            ax_left.set_zorder(2)
            ax_left.patch.set_alpha(0)
            if use_right_axis and ax_right is not None:
                ax_right.set_zorder(3)
                ax_right.patch.set_alpha(0)

        # Combined legend for all numeric lines.
        legend_cols = 1 if len(line_handles) <= 4 else 2
        ax_left.legend(
            handles=line_handles,
            labels=[h.get_label() for h in line_handles],
            fontsize=9,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.12),
            ncol=legend_cols,
            framealpha=0.9,
        )

        plt.tight_layout(rect=[0, 0.06, 1, 1])
        
        # Save plot to numeric subfolder
        numeric_dir = output_dir / "numeric"
        numeric_dir.mkdir(parents=True, exist_ok=True)
        output_path = numeric_dir / fig_filename_with_suffix
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Saved: {output_path}")


def plot_boolean_trends_by_domain(df: pd.DataFrame, start_year_col: str, output_dir: Path,
                                  min_obs: int = 3, use_latest_cao_view: bool = False,
                                  df_latest_view: Optional[pd.DataFrame] = None) -> None:
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
        df_plot = df_latest_view if df_latest_view is not None else build_latest_cao_forward_fill(
            df, cao_col="cao_number", date_col="ingangsdatum"
        )
        if len(df_plot) == 0:
            print(f"  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
    else:
        df_plot = df
    
    df_plot_local = filter_non_salary_for_plot(df_plot)
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
            if var_name not in df_plot_local.columns:
                print(f"  [WARN] Column '{var_name}' not found; skipping boolean trend.")
                continue
            
            shares, counts = compute_boolean_trends(df_plot_local, var_name, start_year_col, min_obs)
            
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
        all_years = set()
        for _, shares in plot_data:
            all_years.update(shares.index)
        if "cao_number" in df_plot_local.columns:
            for year in all_years:
                year_data = df_plot_local[df_plot_local[start_year_col] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
        
        # Create plot with secondary axis for CAO counts
        fig, ax1 = plt.subplots(figsize=(12, 7))
        
        # Plot shares on primary axis
        colors = get_plot_color_cycle(len(plot_data))
        for i, (var_name, shares) in enumerate(plot_data):
            # Use clear label with explanation if available
            label = BOOLEAN_LABELS.get(var_name, var_name.replace('_', ' ').title())
            ax1.plot(shares.index.astype(int), shares.values * 100, marker='o', label=label,
                    linewidth=2, markersize=6, color=colors[i])
        
        ax1.set_xlabel("Contract start year", fontsize=12)
        ax1.set_ylabel("Share of contracts with feature (%)", fontsize=12)
        # Set x-axis limits and ticks (2007-2027, every 2 years)
        enforce_integer_year_axis(ax1, [int(y) for y in all_years])
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
    df_copy["ingangsdatum"] = parse_cao_date_series(df_copy["ingangsdatum"], dayfirst=True)
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
        log_memory("raw_non_salary", df)
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

    try:
        plot_document_type_and_topics(df, output_dir)
    except Exception as e:
        print(f"  ERROR in document type/topics plots: {e}")
        import traceback
        traceback.print_exc()
    
    # Prepare year column
    print("\nPreparing year column...")
    try:
        df = prepare_year_column(df)
        print(f"  Data after year extraction: {len(df)} rows")
        print(f"  Year range: {int(df['start_year'].min())} - {int(df['start_year'].max())}")
        log_memory("prepared_non_salary", df)
    except Exception as e:
        print(f"  ERROR: Could not prepare year column: {e}")
        return

    print("\nBuilding filtered analysis sample...")
    try:
        df_analysis = filter_non_salary_for_plot(df)
        print(f"  Analysis sample rows: {len(df_analysis)}")
        if "cao_number" in df_analysis.columns:
            print(f"  Analysis sample unique CAOs: {df_analysis['cao_number'].nunique()}")
    except Exception as e:
        print(f"  Warning: Could not build filtered analysis sample: {e}")
        df_analysis = df

    print("\nBuilding latest CAO view once...")
    try:
        # Build latest-view from filtered full-CAO analysis sample so active versions
        # cannot resolve to annex/partial-only rows that are later filtered out.
        df_latest_view = build_latest_cao_forward_fill(df_analysis, cao_col="cao_number", date_col="ingangsdatum")
        print(f"  Latest CAO view rows: {len(df_latest_view)}")
        if len(df_latest_view) > 0:
            log_memory("latest_non_salary", df_latest_view)
            if "cao_number" in df_latest_view.columns:
                print(f"  Latest CAO view unique CAOs: {df_latest_view['cao_number'].nunique()}")
    except Exception as e:
        print(f"  Warning: Could not build latest CAO view: {e}")
        df_latest_view = pd.DataFrame()
    
    # Generate plots
    print("\n" + "="*80)
    print("Generating plots...")
    print("="*80)
    print("Recommendation: run heavy scripts sequentially in a single process.")
    
    # Generate standard numeric plots (means)
    try:
        plot_numeric_trends(
            df_analysis, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=False, agg_kind="mean"
        )
    except Exception as e:
        print(f"  ERROR in numeric trends: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View numeric plots (means)
    try:
        plot_numeric_trends(
            df_analysis,
            "start_year",
            output_dir,
            MIN_OBS_PER_YEAR,
            use_latest_cao_view=True,
            agg_kind="mean",
            df_latest_view=df_latest_view,
        )
    except Exception as e:
        print(f"  ERROR in numeric trends (latest CAO view): {e}")
        import traceback
        traceback.print_exc()

    # Generate standard numeric plots (medians)
    try:
        plot_numeric_trends(
            df_analysis, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=False, agg_kind="median"
        )
    except Exception as e:
        print(f"  ERROR in numeric trends (median): {e}")
        import traceback
        traceback.print_exc()

    # Generate Latest CAO View numeric plots (medians)
    try:
        plot_numeric_trends(
            df_analysis,
            "start_year",
            output_dir,
            MIN_OBS_PER_YEAR,
            use_latest_cao_view=True,
            agg_kind="median",
            df_latest_view=df_latest_view,
        )
    except Exception as e:
        print(f"  ERROR in numeric trends (latest CAO view, median): {e}")
        import traceback
        traceback.print_exc()
    
    # Generate standard boolean plots by domain
    try:
        plot_boolean_trends_by_domain(
            df_analysis, "start_year", output_dir, MIN_OBS_PER_YEAR, use_latest_cao_view=False
        )
    except Exception as e:
        print(f"  ERROR in boolean trends: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View boolean plots by domain
    try:
        plot_boolean_trends_by_domain(
            df_analysis,
            "start_year",
            output_dir,
            MIN_OBS_PER_YEAR,
            use_latest_cao_view=True,
            df_latest_view=df_latest_view,
        )
    except Exception as e:
        print(f"  ERROR in boolean trends (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*80)
    print("Script completed successfully!")
    print("="*80)


if __name__ == "__main__":
    main()

