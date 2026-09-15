"""
02_non_salary.py: Generates Non-Salary section tables, figures, and macros.

Inputs:
    - qa/corrected_dataset.csv

Outputs:
    - Reports/Analysis/tables/tab_domain_coverage.tex
    - Reports/Analysis/figures/boolean/latest_cao_view/*.png
    - Reports/Analysis/figures/boolean/new_cao_yearly/*.png
    - Reports/Analysis/figures/numeric/*.png
    - Emits macros into Reports/Analysis/tables/macros.tex
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Import common configuration
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from common import (
    REPO_ROOT,
    CAOS_REPO_ROOT,
    FIGURES_DIR,
    TABLES_DIR,
    emit_macro,
    emit_table,
    save_macros,
    set_macro,
    setup_matplotlib,
)

# Import analysis helpers from CAOsDataExtraction
from scripts.excel_analysis.analysis_utils import (
    parse_cao_date_series,
    filter_non_salary_for_plot,
    build_latest_cao_forward_fill_by_file,
)
from scripts.excel_analysis.descriptives_non_salary import (
    create_domain_coverage_latest_sheet,
    build_latest_cao_view,
    normalize_boolean,
    DOMAIN_FLAGS,
)
import scripts.excel_analysis.descriptives_non_salary_plots as dnsp

EARLY_WINDOW = (2011, 2013)
LATE_WINDOW = (2021, 2023)

DOMAIN_DISPLAY_NAMES = {
    "bonus": "Bonuses and wage add-ons",
    "pension": "Pensions",
    "leave": "Leave",
    "termination": "Termination",
    "overtime": "Overtime",
    "training": "Training",
    "homeoffice": "Homeoffice",
    "contract": "Contract-type rules",
    "fringe": "Fringe benefits",
    "safety": "Safety and wellbeing",
    "childcare": "Childcare",
    "ai": "AI-related policies",
}

def _bool_share(df: pd.DataFrame, col: str) -> float:
    b = normalize_boolean(df[col])
    return (b == True).mean() if len(df) else 0.0


def _bool_share_window(df_view: pd.DataFrame, col: str, window: tuple[int, int]) -> float:
    sub = df_view[df_view["start_year"].between(*window)]
    return _bool_share(sub, col)


def compute_incidence_narrative_stats(df_latest: pd.DataFrame, df_latest_view: pd.DataFrame) -> dict:
    """Point shares (latest-CAO cross-section) and early/late trend shares (panel) for the
    booleans read off the incidence figures in the Bonuses/Fringe/Training prose."""
    stats = {
        "job_allowances_share": _bool_share(df_latest, "bonus_job_allowances_present"),
        "entry_step_share": _bool_share(df_latest, "wage_entry_step_exp_present"),
        "commuting_share": _bool_share(df_latest, "fringe_commuting_allowance_present"),
        "meal_share": _bool_share(df_latest, "fringe_meal_benefit_present"),
        "insurance_share": _bool_share(df_latest, "fringe_insurance_or_savings_benefit_present"),
        "relocation_share": _bool_share(df_latest, "fringe_relocation_allowance_present"),
        "bike_share": _bool_share(df_latest, "fringe_bike_scheme_present"),
        "internet_phone_share": _bool_share(df_latest, "fringe_internet_or_phone_reimbursement_present"),
        "training_fund_share": _bool_share(df_latest, "training_fund_present"),
    }
    for key, col in [
        ("seniority_bonus", "bonus_seniority_loyalty_bonus"),
        ("pers_allow_max_scale", "wage_pers_allow_max_scale"),
        ("training_fund", "training_fund_present"),
    ]:
        stats[f"{key}_early"] = _bool_share_window(df_latest_view, col, EARLY_WINDOW)
        stats[f"{key}_late"] = _bool_share_window(df_latest_view, col, LATE_WINDOW)
    return stats


def compute_numeric_narrative_stats(df_latest_view: pd.DataFrame) -> dict:
    """Unit-aware numeric anchors for the hours/pension/training trends paragraph.

    Several numeric fields mix units within one column (e.g. training time in days vs.
    hours, full-time hours per week vs. per year) -- filtered to the unit the prose
    describes rather than averaged across incompatible units.
    """
    ft_unit = df_latest_view["contract_full_time_hours_unit"].astype(str).str.lower()
    ft_weekly_mask = ft_unit.str.contains("week") & ~ft_unit.str.contains("year|pay period")
    ft_val = pd.to_numeric(df_latest_view["contract_full_time_hours_value"], errors="coerce")
    ft_weekly_median = ft_val[ft_weekly_mask].median()

    years = sorted(y for y in df_latest_view["start_year"].dropna().unique() if 2011 <= y <= 2024)
    ot_yearly_medians = []
    for y in years:
        sub = df_latest_view[df_latest_view["start_year"] == y]
        v = pd.to_numeric(sub["overtime_max_hours_per_week_value"], errors="coerce").median()
        if pd.notna(v):
            ot_yearly_medians.append(v)

    retire_early = pd.to_numeric(
        df_latest_view[df_latest_view["start_year"].between(*EARLY_WINDOW)]["pension_retire_age_normal_value"],
        errors="coerce",
    ).median()
    retire_late = pd.to_numeric(
        df_latest_view[df_latest_view["start_year"].between(2023, 2025)]["pension_retire_age_normal_value"],
        errors="coerce",
    ).median()

    tr_unit = df_latest_view["training_time_yearly_unit"].astype(str).str.lower()
    tr_hour_mask = tr_unit.str.contains("hour")
    tr_val = pd.to_numeric(df_latest_view["training_time_yearly_value"], errors="coerce")
    tr_hours_median = tr_val[tr_hour_mask].median()
    tr_hours_n = tr_val[tr_hour_mask].notna().sum()

    pen_unit = df_latest_view["pension_employee_contrib_unit"].astype(str).str.lower()
    pen_pct_mask = pen_unit.str.contains("%|percent")
    pen_val = pd.to_numeric(df_latest_view["pension_employee_contrib_value"], errors="coerce")
    pen_yearly_medians = []
    for y in years:
        v = pen_val[pen_pct_mask & (df_latest_view["start_year"] == y)].median()
        if pd.notna(v):
            pen_yearly_medians.append(v)

    return {
        "ft_weekly_median": ft_weekly_median,
        "ot_weekly_low": min(ot_yearly_medians) if ot_yearly_medians else np.nan,
        "ot_weekly_high": max(ot_yearly_medians) if ot_yearly_medians else np.nan,
        "retire_age_early": retire_early,
        "retire_age_late": retire_late,
        "training_hours_median": tr_hours_median,
        "training_hours_n": tr_hours_n,
        "pension_contrib_low": min(pen_yearly_medians) if pen_yearly_medians else np.nan,
        "pension_contrib_high": max(pen_yearly_medians) if pen_yearly_medians else np.nan,
    }


def main():
    print("=== Running 02_non_salary.py ===")
    setup_matplotlib()
    
    corr_path = REPO_ROOT / "qa" / "corrected_dataset.csv"
    print(f"Loading data: {corr_path}")
    df = pd.read_csv(corr_path, sep=";", low_memory=False)
    
    if "ingangsdatum" in df.columns:
        df["ingangsdatum"] = parse_cao_date_series(df["ingangsdatum"], dayfirst=True)
        df["start_year"] = df["ingangsdatum"].dt.year
        
    df_filtered = filter_non_salary_for_plot(df)
    if "ingangsdatum" in df_filtered.columns and "start_year" not in df_filtered.columns:
        df_filtered["start_year"] = parse_cao_date_series(df_filtered["ingangsdatum"], dayfirst=True).dt.year
    df_latest_filtered = build_latest_cao_view(df_filtered)
    
    print(f"Dataset: total={len(df)}, filtered={len(df_filtered)}, latest_filtered={len(df_latest_filtered)}")
    
    # 1. Compute Domain Coverage Table
    print("Computing domain coverage table...")
    df_cov = create_domain_coverage_latest_sheet(df_latest_filtered)
    
    table_rows = []
    for _, row in df_cov.iterrows():
        dom_key = row["domain"]
        disp_name = DOMAIN_DISPLAY_NAMES.get(dom_key, dom_key.replace("_", " ").capitalize())
        count = int(row["n_cao_with_domain"])
        share = row["share_cao_with_domain"]
        table_rows.append({
            "Domain": disp_name,
            "CAOs with domain": f"{count:,}",
            "Share of CAOs": f"{share * 100:.0f}\\%",
        })
        
        # Emit macros for each domain
        dom_macro_key = "".join(p.capitalize() for p in dom_key.split("_"))
        set_macro(f"NonSal{dom_macro_key}Count", f"{count:,}")
        set_macro(f"NonSal{dom_macro_key}Share", f"{share * 100:.0f}%")
        
    df_tab_cov = pd.DataFrame(table_rows)
    emit_table(
        df_tab_cov,
        TABLES_DIR / "tab_domain_coverage.tex",
        col_align="lrr",
        headers=["Domain", "CAOs with domain", "Share of CAOs"],
        escape=False,
    )
    save_macros()
    
    # 2. Generate Non-Salary Plots
    print("Generating boolean plots by domain...")
    min_obs = 3
    df_latest_view = build_latest_cao_forward_fill_by_file(
        df_filtered,
        cao_col="cao_number",
        year_col="start_year",
        file_col="file_name",
        order_date_col="ingangsdatum",
    )
    
    # Generate latest CAO view boolean plots
    dnsp.plot_boolean_trends_by_domain(
        df_filtered,
        "start_year",
        FIGURES_DIR,
        min_obs=min_obs,
        use_latest_cao_view=True,
        df_latest_view=df_latest_view,
    )
    
    # Generate yearly contract episode boolean plots (needed for AI and general panel)
    dnsp.plot_boolean_trends_by_domain(
        df_filtered,
        "start_year",
        FIGURES_DIR,
        min_obs=min_obs,
        use_latest_cao_view=False,
    )
    
    # Generate numeric trend plots
    print("Generating numeric trend plots...")
    dnsp.plot_numeric_trends(
        df_filtered,
        "start_year",
        FIGURES_DIR,
        min_obs=min_obs,
        use_latest_cao_view=True,
        agg_kind="mean",
        df_latest_view=df_latest_view,
    )

    # 3. Incidence and numeric narrative macros (Bonuses/Fringe/Training prose + numeric trends)
    print("Computing incidence and numeric narrative stats...")
    inc_stats = compute_incidence_narrative_stats(df_latest_filtered, df_latest_view)
    num_stats = compute_numeric_narrative_stats(df_latest_view)

    set_macro("NonSalJobAllowShare", f"{inc_stats['job_allowances_share'] * 100:.0f}%")
    set_macro("NonSalEntryStepShare", f"{inc_stats['entry_step_share'] * 100:.0f}%")
    set_macro("NonSalSeniorityBonusEarly", f"{inc_stats['seniority_bonus_early'] * 100:.0f}%")
    set_macro("NonSalSeniorityBonusLate", f"{inc_stats['seniority_bonus_late'] * 100:.0f}%")
    set_macro("NonSalPersAllowMaxEarly", f"{inc_stats['pers_allow_max_scale_early'] * 100:.0f}%")
    set_macro("NonSalPersAllowMaxLate", f"{inc_stats['pers_allow_max_scale_late'] * 100:.0f}%")

    set_macro("NonSalCommutingShare", f"{inc_stats['commuting_share'] * 100:.0f}%")
    set_macro("NonSalMealShare", f"{inc_stats['meal_share'] * 100:.0f}%")
    set_macro("NonSalInsuranceShare", f"{inc_stats['insurance_share'] * 100:.0f}%")
    set_macro("NonSalRelocationShare", f"{inc_stats['relocation_share'] * 100:.0f}%")
    set_macro("NonSalBikeShare", f"{inc_stats['bike_share'] * 100:.0f}%")
    set_macro("NonSalInternetPhoneShare", f"{inc_stats['internet_phone_share'] * 100:.0f}%")

    set_macro("NonSalTrainingFundEarly", f"{inc_stats['training_fund_early'] * 100:.0f}%")
    set_macro("NonSalTrainingFundLate", f"{inc_stats['training_fund_late'] * 100:.0f}%")

    set_macro("NonSalFTHoursWeeklyMedian", f"{num_stats['ft_weekly_median']:.0f}")
    set_macro("NonSalOvertimeMaxWeeklyLow", f"{num_stats['ot_weekly_low']:.0f}")
    set_macro("NonSalOvertimeMaxWeeklyHigh", f"{num_stats['ot_weekly_high']:.0f}")
    set_macro("NonSalRetireAgeEarly", f"{num_stats['retire_age_early']:.0f}")
    set_macro("NonSalRetireAgeLate", f"{num_stats['retire_age_late']:.0f}")
    set_macro("NonSalTrainingHoursMedian", f"{num_stats['training_hours_median']:.0f}")
    set_macro("NonSalTrainingHoursN", f"{int(num_stats['training_hours_n']):,}")
    set_macro("NonSalPensionContribLow", f"{num_stats['pension_contrib_low']:.1f}\\%")
    set_macro("NonSalPensionContribHigh", f"{num_stats['pension_contrib_high']:.1f}\\%")

    save_macros()

    print("✓ 02_non_salary.py completed successfully!")

if __name__ == "__main__":
    main()
