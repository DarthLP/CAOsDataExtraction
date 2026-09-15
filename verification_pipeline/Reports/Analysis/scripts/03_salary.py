"""
03_salary.py: Generates Salary section tables, figures, and macros from parser salary v2.

Inputs:
    - CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv (filtered to Tier A+B)

Outputs:
    - Reports/Analysis/tables/tab_salary_structure.tex
    - Reports/Analysis/figures/salary/*.png (regenerated from parser v2 via descriptives_salary_plots.py
      -- NOT copied from the old raw-LLM reference figures)
    - Emits macros into Reports/Analysis/tables/macros.tex

Flags:
    --skip-plots  Skip the ~15 min figure regeneration (macros/tables only; for iterating on prose).
                  The figures already on disk are left as-is.
"""

from __future__ import annotations

import os
import re
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
    ANALYSIS_DIR,
    FIGURES_DIR,
    TABLES_DIR,
    emit_macro,
    emit_table,
    save_macros,
    set_macro,
    setup_matplotlib,
)

from scripts.excel_analysis.analysis_utils import (
    parse_cao_date_series,
    coerce_salary_amount_scalar,
)
from scripts.excel_analysis.descriptives_non_salary import normalize_boolean

SALARY_V2_CSV = CAOS_REPO_ROOT / "outputs" / "parser_salary" / "extracted_data_salary_v2.csv"
SALARY_FIG_DIR = FIGURES_DIR / "salary"

REQUIRED_FIGURES = [
    "salary_boolean_shares_by_contract_year.png",
    "salary_points_per_row_by_year.png",
    "salary_amount_monthly_eur_band_eligible_by_salary_year.png",
    "salary_amount_monthly_eur_band_eligible_by_contract_year.png",
    "salary_amount_monthly_eur_band_eligible_by_contract_year_latest_cao_view.png",
    "salary_increase_merged_pref_csv_by_salary_year.png",
    "salary_increase_series_comparison_by_year.png",
    "salary_increase_merged_pref_csv_by_salary_year_latest_cao_view.png",
    "salary_increase_percent_by_contract_year.png",
    "salary_increase_shift_by_new_file_year.png",
]

N_SALARY_SLOTS = 80  # salary_1_amount .. salary_80_amount in the v2 schema
MONTHLY_CAP_EUR = 50_000  # matches the cap documented in Salary.tex


def regenerate_salary_figures():
    """Run the CAOsDataExtraction salary plotting script against parser v2, tier A+B only.

    Replaces the old behaviour of copying pre-built figures from the raw-LLM-extraction
    reference report (`inputs/CAO Descriptive Summary/figures/salary/`) -- those figures
    described a dataset this report no longer uses.
    """
    import scripts.excel_analysis.descriptives_salary_plots as dsp

    dsp.INPUT_CSV = str(SALARY_V2_CSV)
    dsp.OUTPUT_FIG_DIR = str(SALARY_FIG_DIR) + "/"

    _orig_read_csv = pd.read_csv

    def _tier_filter_read(*args, **kwargs):
        df_raw = _orig_read_csv(*args, **kwargs)
        if isinstance(df_raw, pd.DataFrame) and "confidence_tier" in df_raw.columns:
            df_raw = df_raw[df_raw["confidence_tier"].isin(["A", "B"])].reset_index(drop=True)
        return df_raw

    dsp.pd.read_csv = _tier_filter_read

    # dsp.main() writes diagnostics CSVs (salary_band_and_conversion_diagnostics.csv,
    # salary_monthly_band_summary.csv) to a hardcoded relative "outputs/analysis" path.
    # Run it from CAOS_REPO_ROOT so those land in the CAOsDataExtraction repo's own
    # outputs/analysis/ (their natural home) instead of littering Reports/Analysis/.
    prev_cwd = os.getcwd()
    try:
        os.chdir(CAOS_REPO_ROOT)
        SALARY_FIG_DIR.mkdir(parents=True, exist_ok=True)
        dsp.main()
    finally:
        dsp.pd.read_csv = _orig_read_csv
        os.chdir(prev_cwd)

    missing = [f for f in REQUIRED_FIGURES if not (SALARY_FIG_DIR / f).exists()]
    if missing:
        raise RuntimeError(f"Salary figure regeneration did not produce: {missing}")


def compute_salary_structure(csv_path: Path) -> dict:
    """Compute exact structural statistics on tier A+B rows."""
    print(f"Reading metadata columns from {csv_path}...")
    cols = [
        "cao_number", "file_name", "jobgroup", "step_label",
        "worker_type", "age_group", "education", "ft_hours",
        "confidence_tier"
    ]
    df = pd.read_csv(csv_path, sep=";", usecols=cols, low_memory=False)
    n_all_rows = len(df)
    n_all_tier_rows = df["confidence_tier"].notna().sum()
    df_ab = df[df["confidence_tier"].isin(["A", "B"])].reset_index(drop=True)

    n_rows = len(df_ab)
    n_caos = df_ab["cao_number"].nunique()
    unique_files = df_ab[["cao_number", "file_name"]].drop_duplicates()
    n_files = len(unique_files)

    n_jobgroups = df_ab["jobgroup"].nunique()
    n_steps = df_ab["step_label"].nunique()
    n_worker_types = df_ab["worker_type"].nunique()
    n_age_groups = df_ab["age_group"].nunique()
    n_education = df_ab["education"].nunique()

    ft_num = pd.to_numeric(df_ab["ft_hours"], errors="coerce")
    ft_weekly = ft_num.apply(lambda x: x if pd.isna(x) or x <= 200 else x / 52.0).dropna()
    ft_weekly = ft_weekly[(ft_weekly >= 10) & (ft_weekly <= 60)]

    share_ft = len(ft_weekly) / n_rows if n_rows > 0 else 0.0
    mean_ft = ft_weekly.mean() if len(ft_weekly) > 0 else 0.0
    median_ft = ft_weekly.median() if len(ft_weekly) > 0 else 0.0

    return {
        "n_rows": n_rows,
        "n_files": n_files,
        "n_caos": n_caos,
        "n_jobgroups": n_jobgroups,
        "n_steps": n_steps,
        "n_worker_types": n_worker_types,
        "n_age_groups": n_age_groups,
        "n_education": n_education,
        "share_ft": share_ft,
        "mean_ft": mean_ft,
        "median_ft": median_ft,
        "share_tier_ab": n_rows / n_all_tier_rows if n_all_tier_rows > 0 else 0.0,
    }


def compute_coverage_and_flag_stats(csv_path: Path) -> dict:
    """Peak CAO coverage by contract start year, and TTW / entry-flag yearly share ranges.

    Independent, unweighted descriptive aggregates on the tier A+B rows -- narrative-level
    anchors for the prose, not the CAO-equal-weighted series plotted in the figures.
    """
    cols = ["cao_number", "confidence_tier", "ingangsdatum", "TTW", "is_entry"]
    df = pd.read_csv(csv_path, sep=";", usecols=cols, low_memory=False)
    df = df[df["confidence_tier"].isin(["A", "B"])].reset_index(drop=True)
    df["contract_start_year"] = parse_cao_date_series(df["ingangsdatum"], dayfirst=True).dt.year

    by_year = df.dropna(subset=["contract_start_year"]).copy()
    by_year["contract_start_year"] = by_year["contract_start_year"].astype(int)

    caos_per_year = by_year.groupby("contract_start_year")["cao_number"].nunique()
    peak_year = int(caos_per_year.idxmax())
    peak_caos = int(caos_per_year.max())

    # Mature years: at least 10 distinct CAOs contributing salary rows that year.
    mature_years = caos_per_year[caos_per_year >= 10].index
    df_mature = by_year[by_year["contract_start_year"].isin(mature_years)].copy()
    df_mature["ttw_bool"] = normalize_boolean(df_mature["TTW"])
    df_mature["entry_bool"] = normalize_boolean(df_mature["is_entry"])

    ttw_share_by_year = df_mature.groupby("contract_start_year")["ttw_bool"].apply(
        lambda s: (s == True).mean()
    )
    entry_share_by_year = df_mature.groupby("contract_start_year")["entry_bool"].apply(
        lambda s: (s == True).mean()
    )

    return {
        "peak_year": peak_year,
        "peak_caos": peak_caos,
        "ttw_share_min": ttw_share_by_year.min() if len(ttw_share_by_year) else 0.0,
        "ttw_share_max": ttw_share_by_year.max() if len(ttw_share_by_year) else 0.0,
        "entry_share_min": entry_share_by_year.min() if len(entry_share_by_year) else 0.0,
        "entry_share_max": entry_share_by_year.max() if len(entry_share_by_year) else 0.0,
    }


def compute_points_per_row_stats(csv_path: Path) -> dict:
    """Range of the (contract-start-year) median count of populated salary slots per row."""
    amount_cols = [f"salary_{k}_amount" for k in range(1, N_SALARY_SLOTS + 1)]
    cols = ["confidence_tier", "ingangsdatum"] + amount_cols
    df = pd.read_csv(csv_path, sep=";", usecols=cols, low_memory=False)
    df = df[df["confidence_tier"].isin(["A", "B"])].reset_index(drop=True)
    df["contract_start_year"] = parse_cao_date_series(df["ingangsdatum"], dayfirst=True).dt.year

    amounts_numeric = df[amount_cols].apply(lambda s: s.map(coerce_salary_amount_scalar))
    df["n_points"] = (amounts_numeric > 0).sum(axis=1)

    df_valid = df.dropna(subset=["contract_start_year"])
    df_valid = df_valid[df_valid["n_points"] > 0]
    n_caos_col = df_valid.groupby("contract_start_year")["n_points"].count()
    mature_years = n_caos_col[n_caos_col >= 30].index
    median_by_year = df_valid[df_valid["contract_start_year"].isin(mature_years)].groupby(
        "contract_start_year"
    )["n_points"].median()

    return {
        "points_median_min": median_by_year.min() if len(median_by_year) else 0.0,
        "points_median_max": median_by_year.max() if len(median_by_year) else 0.0,
        "points_mean": df_valid["n_points"].mean() if len(df_valid) else 0.0,
    }


def _monthly_eur_for_slot(df: pd.DataFrame, k: int) -> pd.DataFrame:
    """Convert one salary_k slot to normalized gross monthly EUR, per the rule in Salary.tex:
    monthly unchanged; 4-weekly x13/12; hourly x weekly-hours x52/12; daily x5x4.33;
    weekly x52/12; annual /12. Capped at EUR 50,000/month, amounts <=0 dropped.
    """
    amount = df[f"salary_{k}_amount"].map(coerce_salary_amount_scalar)
    unit = df[f"salary_{k}_unit"].astype(str).str.lower().str.strip()
    ft_weekly = pd.to_numeric(df["ft_hours"], errors="coerce").apply(
        lambda x: x if pd.isna(x) or x <= 200 else x / 52.0
    )

    monthly = pd.Series(np.nan, index=df.index, dtype=float)
    # Unrecognized units (blank, or "period" -- meaning not tied to a fixed calendar unit)
    # are left NaN and dropped below. Vocabulary verified against the actual column values.
    for u in ["monthly", "4-week", "weekly", "daily", "hourly", "annual"]:
        mask = unit.eq(u)
        if u == "monthly":
            monthly.loc[mask] = amount.loc[mask]
        elif u == "4-week":
            monthly.loc[mask] = amount.loc[mask] * (13 / 12)
        elif u == "weekly":
            monthly.loc[mask] = amount.loc[mask] * (52 / 12)
        elif u == "daily":
            monthly.loc[mask] = amount.loc[mask] * 5 * 4.33
        elif u == "hourly":
            monthly.loc[mask] = amount.loc[mask] * ft_weekly.loc[mask] * (52 / 12)
        elif u == "annual":
            monthly.loc[mask] = amount.loc[mask] / 12

    monthly = monthly.where((monthly > 0) & (monthly <= MONTHLY_CAP_EUR))
    return monthly


def compute_salary_level_and_increase_stats(csv_path: Path) -> dict:
    """Salary-year and contract-year level anchors, and wage-increase-by-decade anchors.

    Independent unweighted reconstruction of the conversion rule already documented in
    Salary.tex (monthly-EUR normalization + EUR 50,000 cap). This is a flat average over
    every tier A+B wage-scale row -- NOT the CAO-equal-weighted, band-eligible series the
    figures plot (descriptives_salary_plots.py's attach_cao_equal_weights). Verified the two
    are materially different for salary LEVELS (this flat mean runs ~50% above the figure's
    CAO-balanced line, since a CAO's senior wage steps outnumber its entry steps in the raw
    row count) -- Salary.tex explicitly labels these as a separate unweighted statistic, not
    a description of the plotted line. For wage INCREASE percentages the two track much more
    closely (a % raise is roughly step-invariant), so no such caveat was needed there.
    """
    amount_cols = [f"salary_{k}_amount" for k in range(1, N_SALARY_SLOTS + 1)]
    unit_cols = [f"salary_{k}_unit" for k in range(1, N_SALARY_SLOTS + 1)]
    start_cols = [f"salary_{k}_start_date" for k in range(1, N_SALARY_SLOTS + 1)]
    incr_cols = [f"salary_{k}_increase_percent" for k in range(1, N_SALARY_SLOTS + 1)]
    cols = (
        ["cao_number", "confidence_tier", "ingangsdatum", "ft_hours"]
        + amount_cols + unit_cols + start_cols + incr_cols
    )
    df = pd.read_csv(csv_path, sep=";", usecols=cols, low_memory=False)
    df = df[df["confidence_tier"].isin(["A", "B"])].reset_index(drop=True)
    df["contract_start_year"] = parse_cao_date_series(df["ingangsdatum"], dayfirst=True).dt.year

    long_frames = []
    for k in range(1, N_SALARY_SLOTS + 1):
        monthly = _monthly_eur_for_slot(df, k)
        salary_year = parse_cao_date_series(df[f"salary_{k}_start_date"], dayfirst=True).dt.year
        incr = pd.to_numeric(
            df[f"salary_{k}_increase_percent"].astype(str).str.replace(",", "."), errors="coerce"
        )
        long_frames.append(pd.DataFrame({
            "contract_start_year": df["contract_start_year"],
            "salary_start_year": salary_year,
            "monthly_eur": monthly,
            "increase_pct": incr,
        }))
    df_long = pd.concat(long_frames, ignore_index=True)

    by_salary_year = df_long.dropna(subset=["salary_start_year", "monthly_eur"]).copy()
    by_salary_year["salary_start_year"] = by_salary_year["salary_start_year"].astype(int)
    level_by_salary_year = by_salary_year.groupby("salary_start_year")["monthly_eur"].mean()

    by_contract_year = df_long.dropna(subset=["contract_start_year", "monthly_eur"]).copy()
    by_contract_year["contract_start_year"] = by_contract_year["contract_start_year"].astype(int)
    level_by_contract_year = by_contract_year.groupby("contract_start_year")["monthly_eur"].mean()

    incr_by_year = df_long.dropna(subset=["salary_start_year", "increase_pct"]).copy()
    incr_by_year["salary_start_year"] = incr_by_year["salary_start_year"].astype(int)
    incr_yearly_mean = incr_by_year.groupby("salary_start_year")["increase_pct"].mean()

    decade_years = [y for y in incr_yearly_mean.index if 2010 <= y <= 2019]
    recent_years = [y for y in incr_yearly_mean.index if y >= 2020]
    decade_avg = incr_yearly_mean.loc[decade_years].mean() if decade_years else np.nan
    peak_year = int(incr_yearly_mean.loc[recent_years].idxmax()) if recent_years else None
    peak_val = incr_yearly_mean.loc[recent_years].max() if recent_years else np.nan

    def _bounded(s: pd.Series, lo: int, hi: int):
        sub = s[(s.index >= lo) & (s.index <= hi)]
        return sub

    early_salary = _bounded(level_by_salary_year, 2008, 2009)
    late_salary = _bounded(level_by_salary_year, 2022, 2023)
    recent_salary = level_by_salary_year[level_by_salary_year.index >= 2024]

    early_contract = _bounded(level_by_contract_year, 2004, 2004)
    late_contract = _bounded(level_by_contract_year, 2024, 2024)
    latest_contract = _bounded(level_by_contract_year, 2025, 2025)

    return {
        "salary_level_early_eur": early_salary.mean() if len(early_salary) else np.nan,
        "salary_level_late_eur": late_salary.mean() if len(late_salary) else np.nan,
        "salary_level_recent_eur": recent_salary.mean() if len(recent_salary) else np.nan,
        "contract_level_early_eur": early_contract.mean() if len(early_contract) else np.nan,
        "contract_level_early_year": 2004,
        "contract_level_late_eur": late_contract.mean() if len(late_contract) else np.nan,
        "contract_level_late_year": 2024,
        "contract_level_latest_eur": latest_contract.mean() if len(latest_contract) else np.nan,
        "contract_level_latest_year": 2025,
        "increase_decade_avg_pct": decade_avg,
        "increase_peak_pct": peak_val,
        "increase_peak_year": peak_year,
    }


def main():
    print("=== Running 03_salary.py ===")
    setup_matplotlib()

    assert SALARY_V2_CSV.exists(), f"Salary parser v2 CSV not found: {SALARY_V2_CSV}"

    if "--skip-plots" in sys.argv:
        print("--skip-plots: leaving existing figures on disk unchanged.")
    else:
        print("Regenerating salary figures from parser v2 (tier A+B) -- this takes ~15 min...")
        regenerate_salary_figures()

    # 2. Compute salary structure
    stats = compute_salary_structure(SALARY_V2_CSV)

    assert stats["n_rows"] == 343111, f"Expected 343,111 rows, got {stats['n_rows']}"
    assert stats["n_caos"] == 223, f"Expected 223 CAOs, got {stats['n_caos']}"

    print(f"Salary stats: {stats['n_rows']:,} rows, {stats['n_caos']} CAOs, {stats['n_files']:,} files")

    print("Computing coverage/flag stats...")
    cov_stats = compute_coverage_and_flag_stats(SALARY_V2_CSV)
    print("Computing points-per-row stats...")
    points_stats = compute_points_per_row_stats(SALARY_V2_CSV)
    print("Computing salary-level and increase stats...")
    level_stats = compute_salary_level_and_increase_stats(SALARY_V2_CSV)

    # 3. Emit Table 3.1
    structure_rows = [
        {"Statistic": "Total salary rows", "Value": f"{stats['n_rows']:,}"},
        {"Statistic": "Unique files (CAO $\\times$ PDF)", "Value": f"{stats['n_files']:,}"},
        {"Statistic": "Unique CAOs", "Value": f"{stats['n_caos']:,}"},
        {"Statistic": "Distinct job groups", "Value": f"{stats['n_jobgroups']:,}"},
        {"Statistic": "Distinct steps", "Value": f"{stats['n_steps']:,}"},
        {"Statistic": "Distinct worker types", "Value": f"{stats['n_worker_types']:,}"},
        {"Statistic": "Distinct age groups", "Value": f"{stats['n_age_groups']:,}"},
        {"Statistic": "Distinct education categories", "Value": f"{stats['n_education']:,}"},
        {"Statistic": "Share of rows with full-time hours observed", "Value": f"{stats['share_ft'] * 100:.0f}\\%"},
        {"Statistic": "Mean full-time weekly hours", "Value": f"{stats['mean_ft']:.2f}"},
        {"Statistic": "Median full-time weekly hours", "Value": f"{stats['median_ft']:.0f}"},
    ]
    df_tab_sal = pd.DataFrame(structure_rows)
    emit_table(
        df_tab_sal,
        TABLES_DIR / "tab_salary_structure.tex",
        col_align="lr",
        headers=["Characteristic", "Value"],
        escape=False,
    )

    # 4. Emit macros
    set_macro("SalTotalRows", f"{stats['n_rows']:,}")
    set_macro("SalUniqueFiles", f"{stats['n_files']:,}")
    set_macro("SalUniqueCAOs", f"{stats['n_caos']:,}")
    set_macro("SalDistinctJobGroups", f"{stats['n_jobgroups']:,}")
    set_macro("SalDistinctSteps", f"{stats['n_steps']:,}")
    set_macro("SalDistinctWorkerTypes", f"{stats['n_worker_types']:,}")
    set_macro("SalDistinctAgeGroups", f"{stats['n_age_groups']:,}")
    set_macro("SalDistinctEducationCategories", f"{stats['n_education']:,}")
    set_macro("SalShareFTHoursObserved", f"{stats['share_ft'] * 100:.0f}%")
    set_macro("SalMeanFTWeeklyHours", f"{stats['mean_ft']:.2f}")
    set_macro("SalMedianFTWeeklyHours", f"{stats['median_ft']:.0f}")
    set_macro("SalTierABShare", f"{stats['share_tier_ab'] * 100:.1f}%")

    set_macro("SalPeakCaoYear", str(cov_stats["peak_year"]))
    set_macro("SalPeakCaoCount", f"{cov_stats['peak_caos']:,}")
    set_macro("SalTTWShareLow", f"{cov_stats['ttw_share_min'] * 100:.0f}%")
    set_macro("SalTTWShareHigh", f"{cov_stats['ttw_share_max'] * 100:.0f}%")
    set_macro("SalEntryShareLow", f"{cov_stats['entry_share_min'] * 100:.0f}%")
    set_macro("SalEntryShareHigh", f"{cov_stats['entry_share_max'] * 100:.0f}%")

    set_macro("SalPointsPerRowLow", f"{points_stats['points_median_min']:.1f}")
    set_macro("SalPointsPerRowHigh", f"{points_stats['points_median_max']:.1f}")

    def fmt_eur(x):
        return f"EUR~{int(round(x)):,}" if pd.notna(x) else "n/a"

    set_macro("SalLevelSalaryYearEarly", fmt_eur(level_stats["salary_level_early_eur"]))
    set_macro("SalLevelSalaryYearLate", fmt_eur(level_stats["salary_level_late_eur"]))
    set_macro("SalLevelSalaryYearRecent", fmt_eur(level_stats["salary_level_recent_eur"]))
    set_macro("SalLevelContractYearEarly", fmt_eur(level_stats["contract_level_early_eur"]))
    set_macro("SalLevelContractYearEarlyYear", str(level_stats["contract_level_early_year"]))
    set_macro("SalLevelContractYearLate", fmt_eur(level_stats["contract_level_late_eur"]))
    set_macro("SalLevelContractYearLateYear", str(level_stats["contract_level_late_year"]))
    set_macro("SalLevelContractYearLatest", fmt_eur(level_stats["contract_level_latest_eur"]))
    set_macro("SalLevelContractYearLatestYear", str(level_stats["contract_level_latest_year"]))

    if pd.notna(level_stats["increase_decade_avg_pct"]):
        set_macro("SalIncreaseDecadeAvg", f"{level_stats['increase_decade_avg_pct']:.1f}\\%")
    if pd.notna(level_stats["increase_peak_pct"]):
        set_macro("SalIncreasePeakPct", f"{level_stats['increase_peak_pct']:.1f}\\%")
    if level_stats["increase_peak_year"] is not None:
        set_macro("SalIncreasePeakYear", str(level_stats["increase_peak_year"]))

    save_macros()

    print("✓ 03_salary.py completed successfully!")

if __name__ == "__main__":
    main()
