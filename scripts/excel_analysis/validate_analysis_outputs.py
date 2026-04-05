"""
Analysis Outputs Validation Script

This script validates that core salary and non-salary analysis outputs were created
after a full run. It checks workbook, figure, and regression-related CSV existence
to support memory-stability and functional-parity acceptance checks.

USAGE:
    conda run -n caos-extract python scripts/excel_analysis/validate_analysis_outputs.py

OUTPUT:
    Prints a pass/fail report and exits with code 0 on success, 1 on missing outputs.
    Expects ``outputs/analysis/salary_plot_years_dropped.csv`` (from salary plots; header-only is valid).
    ``REQUIRED_FILES`` includes the salary descriptive PNG inventory, band/conversion diagnostics CSV, and related
    analysis CSVs (contract-year increase files must use ``salary_increase_percent_by_contract_year*.png``, not the
    obsolete ``salary_increase_percent_by_salary_year*.png`` names). Latest-view FT hours and points-per-row PNGs are
    not required (removed from the salary plots script).

    Salary-side CSVs in outputs/analysis/ use semicolon (;) as the separator.
"""

from pathlib import Path
from typing import List


REQUIRED_FILES: List[str] = [
    "outputs/analysis/salary_descriptives.xlsx",
    "outputs/analysis/non_salary_descriptives.xlsx",
    "outputs/analysis/salary_increase_events_derived.csv",
    "outputs/analysis/salary_increase_conversion_diagnostics.csv",
    "outputs/analysis/salary_increase_csv_vs_diff_comparison.csv",
    "outputs/analysis/salary_monthly_band_summary.csv",
    "outputs/analysis/salary_band_and_conversion_diagnostics.csv",
    "outputs/analysis/salary_plot_years_dropped.csv",
    # Salary descriptive PNGs (CAO-equal weighting plan; no MIN_OBS year drops)
    "outputs/analysis/figures/salary/salary_ft_hours_by_contract_year.png",
    "outputs/analysis/figures/salary/salary_amount_monthly_eur_band_eligible_by_salary_year.png",
    "outputs/analysis/figures/salary/salary_amount_monthly_eur_band_eligible_by_salary_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_amount_monthly_eur_band_eligible_by_contract_year.png",
    "outputs/analysis/figures/salary/salary_amount_monthly_eur_band_eligible_by_contract_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_increase_percent_by_contract_year.png",
    "outputs/analysis/figures/salary/salary_increase_percent_by_contract_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_boolean_shares_by_contract_year.png",
    "outputs/analysis/figures/salary/salary_boolean_shares_by_contract_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_points_per_row_by_year.png",
    "outputs/analysis/figures/salary/salary_increase_diff_only_by_salary_year.png",
    "outputs/analysis/figures/salary/salary_increase_merged_pref_csv_by_salary_year.png",
    "outputs/analysis/figures/salary/salary_increase_csv_only_by_salary_year.png",
    "outputs/analysis/figures/salary/salary_increase_merged_pref_csv_by_salary_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_increase_diff_only_by_salary_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_increase_csv_only_by_salary_year_latest_cao_view.png",
    "outputs/analysis/figures/salary/salary_increase_series_comparison_by_year.png",
    "outputs/analysis/figures/salary/salary_increase_shift_by_new_file_year.png",
    "outputs/analysis/figures/salary/salary_increase_spaghetti_selected_caos.png",
]


def main() -> int:
    """
    Validate expected analysis outputs exist.

    Args:
        None

    Returns:
        Exit code (0 if all required files exist, otherwise 1)
    """
    missing = [p for p in REQUIRED_FILES if not Path(p).exists()]
    print("=" * 80)
    print("Analysis Output Validation")
    print("=" * 80)
    if missing:
        print("Missing required outputs:")
        for p in missing:
            print(f"  - {p}")
        print("\nValidation failed.")
        return 1
    print("All required outputs exist.")
    print("Validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
