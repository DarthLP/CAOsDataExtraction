"""
CAO Salary Time Trend Plotting Script

This script reads the salary CSV output and generates matplotlib line plots
showing trends over contract start years and salary start years for key salary variables.

USAGE:
    python scripts/excel_analysis/descriptives_salary_plots.py

INPUT:
    - outputs/excel/new_results/extracted_data_salary.csv

OUTPUT:
    - outputs/analysis/figures/salary/ (directory with PNG plots)
      
      Time trend plots:
      - salary_ft_hours_by_contract_year.png
      - salary_amount_primary_unit_by_salary_year.png
      - salary_increase_percent_by_salary_year.png
      - salary_boolean_shares_by_contract_year.png
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

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
INPUT_CSV = "outputs/excel/new_results/extracted_data_salary.csv"
OUTPUT_FIG_DIR = "outputs/analysis/figures/salary/"
MIN_OBS_PER_YEAR = 3  # Minimum observations per year to include in plot

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def coerce_bool(series: pd.Series) -> pd.Series:
    """
    Coerce boolean-like values to True/False/NaN.
    
    Args:
        series: Series with potentially mixed boolean representations
        
    Returns:
        Series with normalized boolean values (True/False/NaN)
    """
    result = series.copy()
    
    # Convert to string lower-case where not null
    result_str = result.astype(str).str.lower()
    
    # Map to boolean
    bool_map = {
        "true": True, "1": True, "yes": True, "y": True,
        "false": False, "0": False, "no": False, "n": False
    }
    
    # Apply mapping
    for val, bool_val in bool_map.items():
        result = result.where(result_str != val, bool_val)
    
    # Keep only True/False/NaN
    result = result.where(result.isin([True, False, np.nan]), np.nan)
    
    return result


def build_latest_cao_forward_fill(df: pd.DataFrame, cao_col: str = "cao_number",
                                  date_col: str = "contract_start_year") -> pd.DataFrame:
    """
    Build forward-filled CAO view where each CAO's latest contract data is used
    for all subsequent years until a newer contract appears.
    
    Example: If CAO 134 has contracts in 2013 and 2019:
    - Years 2013-2018: use 2013 contract data
    - Years 2019+: use 2019 contract data
    
    Args:
        df: Input DataFrame with contract data
        cao_col: Column name for CAO number
        date_col: Column name for contract start year (or date column)
        
    Returns:
        DataFrame with forward-filled contract data (one row per CAO-year combination)
    """
    if cao_col not in df.columns:
        print(f"  Warning: Column '{cao_col}' not found for latest CAO forward-fill")
        return pd.DataFrame()
    
    df_copy = df.copy()
    
    # Create start_year if it doesn't exist
    if "contract_start_year" not in df_copy.columns:
        if date_col in df_copy.columns and "date" in date_col.lower():
            # If date_col is a date column, extract year
            df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce')
            df_copy["contract_start_year"] = df_copy[date_col].dt.year
        else:
            print(f"  Warning: Cannot create contract_start_year")
            return pd.DataFrame()
    else:
        # Ensure contract_start_year is numeric
        df_copy["contract_start_year"] = pd.to_numeric(df_copy["contract_start_year"], errors='coerce')
    
    # Filter to valid rows
    valid_mask = df_copy[cao_col].notna() & df_copy["contract_start_year"].notna()
    df_copy = df_copy[valid_mask].copy()
    
    if len(df_copy) == 0:
        return pd.DataFrame()
    
    # Get year range from actual contract start years
    min_year = int(df_copy["contract_start_year"].min())
    max_year = int(df_copy["contract_start_year"].max())
    all_years = range(min_year, max_year + 1)
    
    # Sort by CAO and contract_start_year
    df_copy = df_copy.sort_values([cao_col, "contract_start_year"])
    
    # For each CAO, forward-fill contract data
    result_rows = []
    
    for cao_num in df_copy[cao_col].unique():
        cao_data = df_copy[df_copy[cao_col] == cao_num].copy()
        
        # Get contract years for this CAO (actual contract start years)
        contract_years = sorted([int(y) for y in cao_data["contract_start_year"].unique()])
        
        # For each year in the range, determine which contract applies
        for year in all_years:
            # Find the most recent contract that started on or before this year
            applicable_contracts = [cy for cy in contract_years if cy <= year]
            
            if applicable_contracts:
                # Use the latest contract that started before or in this year
                applicable_year = max(applicable_contracts)
                contract_row = cao_data[cao_data["contract_start_year"] == applicable_year].iloc[0].copy()
                contract_row["contract_start_year"] = year  # Set to current year for aggregation
                result_rows.append(contract_row)
    
    if len(result_rows) == 0:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(result_rows)
    return result_df


def build_long_salary_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build long format DataFrame from wide salary format.
    
    Args:
        df: Wide format DataFrame with salary_k_* columns
        
    Returns:
        Long format DataFrame with one row per salary point
    """
    SLOT_RANGE = range(1, 12)
    long_rows = []
    
    for k in SLOT_RANGE:
        amount_col = f"salary_{k}_amount"
        start_date_col = f"salary_{k}_start_date"
        
        # Check if at least one core column exists
        if amount_col not in df.columns and start_date_col not in df.columns:
            continue
        
        # Create temp DataFrame for this slot
        temp_df = df.copy()
        temp_df["salary_index"] = k
        
        # Map salary point fields
        if start_date_col in df.columns:
            temp_df["salary_start_date"] = temp_df[start_date_col]
        else:
            temp_df["salary_start_date"] = np.nan
        
        if f"salary_{k}_end_date" in df.columns:
            temp_df["salary_end_date"] = temp_df[f"salary_{k}_end_date"]
        else:
            temp_df["salary_end_date"] = np.nan
        
        if amount_col in df.columns:
            temp_df["salary_amount"] = temp_df[amount_col]
        else:
            temp_df["salary_amount"] = np.nan
        
        for field in ["unit", "table_label", "increase_percent", "holiday_in_amount", 
                     "hours_basis_ft_week", "note"]:
            col_name = f"salary_{k}_{field}"
            if col_name in df.columns:
                temp_df[f"salary_{field}"] = temp_df[col_name]
            else:
                temp_df[f"salary_{field}"] = np.nan
        
        # Select columns for long format
        keep_cols = [
            "cao_number", "id", "TTW", "ingangsdatum", "expiratiedatum", 
            "datum_kennisgeving", "file_name",
            "jobgroup", "step_label", "worker_type", "is_entry", "age_group",
            "education", "ft_hours", "ft_hours_weekly", "permanency", "hours_type",
            "row_note",
            "salary_index", "salary_start_date", "salary_end_date", "salary_amount",
            "salary_unit", "salary_table_label", "salary_increase_percent",
            "salary_holiday_in_amount", "salary_hours_basis_ft_week", "salary_note"
        ]
        
        # Only keep columns that exist
        available_cols = [col for col in keep_cols if col in temp_df.columns]
        temp_df = temp_df[available_cols].copy()
        
        long_rows.append(temp_df)
    
    if len(long_rows) == 0:
        return pd.DataFrame()
    
    df_long = pd.concat(long_rows, ignore_index=True)
    
    # Parse salary_start_date
    if "salary_start_date" in df_long.columns:
        df_long["salary_start_date"] = pd.to_datetime(df_long["salary_start_date"], errors='coerce')
        df_long["salary_start_year"] = df_long["salary_start_date"].dt.year
    
    return df_long


# =============================================================================
# PLOTTING FUNCTIONS
# =============================================================================

def plot_ft_hours_by_contract_year(df: pd.DataFrame, output_dir: Path, use_latest_cao_view: bool = False) -> None:
    """
    Plot average full-time hours by contract start year.
    
    Args:
        df: Wide format DataFrame
        output_dir: Directory to save plot
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_ft_hours_by_contract_year{suffix}.png"
    print(f"\nCreating figure: {filename}")
    
    # Use latest CAO view if requested
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print("  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_plot = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                date_col="contract_start_year")
        if len(df_plot) == 0:
            print("  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
    else:
        df_plot = df.copy()
    
    if "contract_start_year" not in df_plot.columns or "ft_hours_weekly" not in df_plot.columns:
        print("  [INFO] Missing required columns; skipping figure")
        return
    
    # Group by contract_start_year
    df_plot = df_plot[df_plot["contract_start_year"].notna() & df_plot["ft_hours_weekly"].notna()].copy()
    
    if len(df_plot) == 0:
        print("  [INFO] No data available; skipping figure")
        return
    
    grouped = df_plot.groupby("contract_start_year")["ft_hours_weekly"].agg([
        ('avg_ft_hours_weekly', 'mean'),
        ('count', 'count')
    ])
    
    # Filter years with ≥3 observations
    grouped = grouped[grouped['count'] >= MIN_OBS_PER_YEAR]
    
    if len(grouped) == 0:
        print("  [INFO] No years with sufficient data; skipping figure")
        return
    
    # Compute CAO counts per year (cumulative for latest CAO view)
    # For latest CAO view, use the latest CAO forward-filled view of the original wide format
    cao_counts = {}
    if use_latest_cao_view:
        # Build latest CAO view from original df for cumulative counting
        # This ensures consistency across all plots (should match salary points per row plot)
        if "cao_number" in df.columns and "contract_start_year" in df.columns:
            df_wide_latest = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                          date_col="contract_start_year")
            if len(df_wide_latest) > 0:
                # Get all years from the latest CAO view
                df_wide_years = df_wide_latest[df_wide_latest["contract_start_year"].notna()].copy()
                if len(df_wide_years) > 0:
                    years_sorted = sorted(grouped.index)
                    
                    # Cumulative: count unique CAOs up to and including each year from latest CAO view
                    seen_caos = set()
                    for year in years_sorted:
                        # Find all CAOs with contracts up to this year in the latest CAO view
                        year_data_wide = df_wide_years[df_wide_years["contract_start_year"] <= year]
                        if len(year_data_wide) > 0:
                            seen_caos.update(year_data_wide["cao_number"].dropna().unique())
                        cao_counts[year] = len(seen_caos)
        else:
            # Fallback to df_plot if original df not available
            if "cao_number" in df_plot.columns:
                years_sorted = sorted(grouped.index)
                seen_caos = set()
                for year in years_sorted:
                    year_data = df_plot[df_plot["contract_start_year"] == year]
                    if len(year_data) > 0:
                        seen_caos.update(year_data["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
    else:
        # Regular view: count unique CAOs per year
        if "cao_number" in df_plot.columns:
            for year in grouped.index:
                year_data = df_plot[df_plot["contract_start_year"] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
    
    # Create plot
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(grouped.index, grouped['avg_ft_hours_weekly'], marker='o', linewidth=2, markersize=6)
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Average full-time hours per week", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Average full-time weekly hours by contract start year{title_suffix}", fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Add secondary axis for background counts
    if cao_counts or len(grouped) > 0:
        ax2 = ax1.twinx()
        years = sorted(grouped.index)
        
        # For latest CAO view, show cumulative CAO counts only
        if use_latest_cao_view:
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                # Single bar showing cumulative CAO count
                ax2.bar(years, cao_list, alpha=0.1, color='gray', label='Cumulative CAOs')
                # Annotations
                for year, cao_count in zip(years, cao_list):
                    if cao_count > 0:
                        ax2.text(year, cao_count, f'{int(cao_count)}', ha='center', va='bottom', 
                                fontsize=7, color='gray', alpha=0.6)
        else:
            # For regular view, show both CAO and row counts
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                ax2.bar([y - 0.2 for y in years], cao_list, width=0.4, alpha=0.1, 
                       color='blue', label='Number of CAOs')
                for year, count in zip(years, cao_list):
                    if count > 0:
                        ax2.text(year - 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                                fontsize=7, color='blue', alpha=0.5)
            
            row_counts = [grouped.loc[y, 'count'] for y in years]
            ax2.bar([y + 0.2 for y in years], row_counts, width=0.4, alpha=0.1, 
                   color='green', label='Number of rows')
            for year, count in zip(years, row_counts):
                if count > 0:
                    ax2.text(year + 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='green', alpha=0.5)
        
        ax2.set_ylabel("Number of CAOs / Rows", fontsize=12, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_salary_amount_primary_unit_by_salary_year(df: pd.DataFrame, df_long: pd.DataFrame, 
                                                   output_dir: Path, 
                                                   use_latest_cao_view: bool = False) -> None:
    """
    Plot average salary amount (primary unit) by salary start year.
    
    Args:
        df: Wide format DataFrame (needed for latest CAO view)
        df_long: Long format DataFrame
        output_dir: Directory to save plot
        use_latest_cao_view: If True, build latest CAO view from wide format first
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_amount_primary_unit_by_salary_year{suffix}.png"
    print(f"\nCreating figure: {filename}")
    
    # For latest CAO view, build from wide format first
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print("  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_wide_latest = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                       date_col="contract_start_year")
        if len(df_wide_latest) == 0:
            print("  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_wide_latest)} CAO-year combinations")
        # Convert to long format
        df_long = build_long_salary_df(df_wide_latest)
        if len(df_long) == 0:
            print("  [WARN] Long format from latest CAO view is empty; skipping.")
            return
    
    if len(df_long) == 0:
        print("  [INFO] Long format DataFrame is empty; skipping figure")
        return
    
    if "salary_unit" not in df_long.columns or "salary_amount" not in df_long.columns:
        print("  [INFO] Missing required columns; skipping figure")
        return
    
    # Identify primary_unit
    unit_series = df_long["salary_unit"].dropna()
    if len(unit_series) == 0:
        print("  [INFO] No salary_unit data; skipping figure")
        return
    
    primary_unit = unit_series.mode()[0] if len(unit_series.mode()) > 0 else None
    if primary_unit is None:
        print("  [INFO] Could not determine primary_unit; skipping figure")
        return
    
    # Filter to primary_unit rows with non-NaN salary_amount and salary_start_year
    df_filtered = df_long[
        (df_long["salary_unit"] == primary_unit) &
        df_long["salary_amount"].notna() &
        df_long["salary_start_year"].notna()
    ].copy()
    
    if len(df_filtered) == 0:
        print("  [INFO] No data after filtering; skipping figure")
        return
    
    # Coerce salary_amount to numeric
    df_filtered["salary_amount"] = pd.to_numeric(df_filtered["salary_amount"], errors='coerce')
    df_filtered = df_filtered[df_filtered["salary_amount"].notna()]
    
    if len(df_filtered) == 0:
        print("  [INFO] No numeric salary_amount data; skipping figure")
        return
    
    # Group by salary_start_year
    grouped = df_filtered.groupby("salary_start_year")["salary_amount"].agg([
        ('avg_amount_year', 'mean'),
        ('count', 'count')
    ])
    
    # Filter years with ≥3 observations
    grouped = grouped[grouped['count'] >= MIN_OBS_PER_YEAR]
    
    if len(grouped) == 0:
        print("  [INFO] No years with sufficient data; skipping figure")
        return
    
    # Compute CAO counts per year (cumulative for latest CAO view)
    # For latest CAO view, use the wide format to get all CAOs with contracts, not just those with salary data
    cao_counts = {}
    if use_latest_cao_view:
        # Use the original wide format DataFrame to compute cumulative CAOs by contract_start_year
        # This ensures consistency across all plots (should match salary points per row plot)
        if "cao_number" in df.columns and "contract_start_year" in df.columns:
            # Get all years from the wide format
            df_wide_years = df[df["contract_start_year"].notna()].copy()
            if len(df_wide_years) > 0:
                years_sorted = sorted(grouped.index)
                
                # Cumulative: count unique CAOs up to and including each year from wide format
                seen_caos = set()
                for year in years_sorted:
                    # Find all CAOs with contracts up to this year in the wide format
                    year_data_wide = df_wide_years[df_wide_years["contract_start_year"] <= year]
                    if len(year_data_wide) > 0:
                        seen_caos.update(year_data_wide["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
        else:
            # Fallback to filtered long format if wide format not available
            if "cao_number" in df_filtered.columns:
                years_sorted = sorted(grouped.index)
                seen_caos = set()
                for year in years_sorted:
                    year_data = df_filtered[df_filtered["salary_start_year"] == year]
                    if len(year_data) > 0:
                        seen_caos.update(year_data["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
    else:
        # Regular view: count unique CAOs per year from filtered data
        if "cao_number" in df_filtered.columns:
            for year in grouped.index:
                year_data = df_filtered[df_filtered["salary_start_year"] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
    
    # Create plot
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(grouped.index, grouped['avg_amount_year'], marker='o', linewidth=2, markersize=6)
    ax1.set_xlabel("Salary start year", fontsize=12)
    ax1.set_ylabel(f"Average salary ({primary_unit})", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Average salary over time ({primary_unit}){title_suffix}", fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Add secondary axis for background counts
    if cao_counts or len(grouped) > 0:
        ax2 = ax1.twinx()
        years = sorted(grouped.index)
        
        # For latest CAO view, show cumulative CAO counts only
        if use_latest_cao_view:
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                # Single bar showing cumulative CAO count
                ax2.bar(years, cao_list, alpha=0.1, color='gray', label='Cumulative CAOs')
                # Annotations
                for year, cao_count in zip(years, cao_list):
                    if cao_count > 0:
                        ax2.text(year, cao_count, f'{int(cao_count)}', ha='center', va='bottom', 
                                fontsize=7, color='gray', alpha=0.6)
        else:
            # For regular view, show both CAO and row counts
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                ax2.bar([y - 0.2 for y in years], cao_list, width=0.4, alpha=0.1, 
                       color='blue', label='Number of CAOs')
                for year, count in zip(years, cao_list):
                    if count > 0:
                        ax2.text(year - 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                                fontsize=7, color='blue', alpha=0.5)
            
            row_counts = [grouped.loc[y, 'count'] for y in years]
            ax2.bar([y + 0.2 for y in years], row_counts, width=0.4, alpha=0.1, 
                   color='green', label='Number of rows')
            for year, count in zip(years, row_counts):
                if count > 0:
                    ax2.text(year + 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='green', alpha=0.5)
        
        ax2.set_ylabel("Number of CAOs / Rows", fontsize=12, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_increase_percent_by_salary_year(df: pd.DataFrame, df_long: pd.DataFrame, 
                                        output_dir: Path,
                                        use_latest_cao_view: bool = False) -> None:
    """
    Plot average increase percent by salary start year.
    
    Args:
        df: Wide format DataFrame (needed for latest CAO view)
        df_long: Long format DataFrame
        output_dir: Directory to save plot
        use_latest_cao_view: If True, build latest CAO view from wide format first
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_increase_percent_by_salary_year{suffix}.png"
    print(f"\nCreating figure: {filename}")
    
    # For latest CAO view, build from wide format first
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print("  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_wide_latest = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                       date_col="contract_start_year")
        if len(df_wide_latest) == 0:
            print("  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_wide_latest)} CAO-year combinations")
        # Convert to long format
        df_long = build_long_salary_df(df_wide_latest)
        if len(df_long) == 0:
            print("  [WARN] Long format from latest CAO view is empty; skipping.")
            return
    
    if len(df_long) == 0:
        print("  [INFO] Long format DataFrame is empty; skipping figure")
        return
    
    if "salary_increase_percent" not in df_long.columns:
        print("  [INFO] salary_increase_percent column not found; skipping figure")
        return
    
    # Coerce to numeric
    s_inc = pd.to_numeric(df_long["salary_increase_percent"], errors='coerce')
    df_plot = df_long.copy()
    df_plot["salary_increase_percent_numeric"] = s_inc
    
    # Filter to rows with non-NaN increase_percent and salary_start_year
    df_filtered = df_plot[
        df_plot["salary_increase_percent_numeric"].notna() &
        df_plot["salary_start_year"].notna()
    ].copy()
    
    if len(df_filtered) == 0:
        print("  [INFO] No data after filtering; skipping figure")
        return
    
    # Group by salary_start_year
    grouped = df_filtered.groupby("salary_start_year")["salary_increase_percent_numeric"].agg([
        ('avg_inc_year', 'mean'),
        ('count', 'count')
    ])
    
    # Filter years with ≥3 observations
    grouped = grouped[grouped['count'] >= MIN_OBS_PER_YEAR]
    
    if len(grouped) == 0:
        print("  [INFO] No years with sufficient data; skipping figure")
        return
    
    # Compute CAO counts per year (cumulative for latest CAO view)
    # For latest CAO view, use the wide format to get all CAOs with contracts, not just those with increase_percent
    cao_counts = {}
    if use_latest_cao_view:
        # Use the original wide format DataFrame to compute cumulative CAOs by contract_start_year
        # This ensures consistency across all plots (should match salary points per row plot)
        if "cao_number" in df.columns and "contract_start_year" in df.columns:
            # Get all years from the wide format
            df_wide_years = df[df["contract_start_year"].notna()].copy()
            if len(df_wide_years) > 0:
                years_sorted = sorted(grouped.index)
                
                # Cumulative: count unique CAOs up to and including each year from wide format
                seen_caos = set()
                for year in years_sorted:
                    # Find all CAOs with contracts up to this year in the wide format
                    year_data_wide = df_wide_years[df_wide_years["contract_start_year"] <= year]
                    if len(year_data_wide) > 0:
                        seen_caos.update(year_data_wide["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
        else:
            # Fallback to filtered long format if wide format not available
            if "cao_number" in df_filtered.columns:
                years_sorted = sorted(grouped.index)
                seen_caos = set()
                for year in years_sorted:
                    year_data = df_filtered[df_filtered["salary_start_year"] == year]
                    if len(year_data) > 0:
                        seen_caos.update(year_data["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
    else:
        # Regular view: count unique CAOs per year from filtered data
        if "cao_number" in df_filtered.columns:
            for year in grouped.index:
                year_data = df_filtered[df_filtered["salary_start_year"] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
    
    # Create plot
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(grouped.index, grouped['avg_inc_year'], marker='o', linewidth=2, markersize=6)
    ax1.set_xlabel("Salary start year", fontsize=12)
    ax1.set_ylabel("Average general wage increase (%)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Average general wage increase by salary start year{title_suffix}", fontsize=14)
    ax1.grid(True, alpha=0.3)
    
    # Add secondary axis for background counts
    if cao_counts or len(grouped) > 0:
        ax2 = ax1.twinx()
        years = sorted(grouped.index)
        
        # For latest CAO view, show cumulative CAO counts only
        if use_latest_cao_view:
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                # Single bar showing cumulative CAO count
                ax2.bar(years, cao_list, alpha=0.1, color='gray', label='Cumulative CAOs')
                # Annotations
                for year, cao_count in zip(years, cao_list):
                    if cao_count > 0:
                        ax2.text(year, cao_count, f'{int(cao_count)}', ha='center', va='bottom', 
                                fontsize=7, color='gray', alpha=0.6)
        else:
            # For regular view, show both CAO and row counts
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                ax2.bar([y - 0.2 for y in years], cao_list, width=0.4, alpha=0.1, 
                       color='blue', label='Number of CAOs')
                for year, count in zip(years, cao_list):
                    if count > 0:
                        ax2.text(year - 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                                fontsize=7, color='blue', alpha=0.5)
            
            row_counts = [grouped.loc[y, 'count'] for y in years]
            ax2.bar([y + 0.2 for y in years], row_counts, width=0.4, alpha=0.1, 
                   color='green', label='Number of rows')
            for year, count in zip(years, row_counts):
                if count > 0:
                    ax2.text(year + 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='green', alpha=0.5)
        
        ax2.set_ylabel("Number of CAOs / Rows", fontsize=12, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_boolean_shares_by_contract_year(df: pd.DataFrame, output_dir: Path,
                                         use_latest_cao_view: bool = False) -> None:
    """
    Plot share of rows with boolean features by contract start year.
    
    Args:
        df: Wide format DataFrame
        output_dir: Directory to save plot
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_boolean_shares_by_contract_year{suffix}.png"
    print(f"\nCreating figure: {filename}")
    
    # Use latest CAO view if requested
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print("  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_plot = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                date_col="contract_start_year")
        if len(df_plot) == 0:
            print("  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
    else:
        df_plot = df.copy()
    
    if "contract_start_year" not in df_plot.columns:
        print("  [INFO] contract_start_year column not found; skipping figure")
        return
    
    # Variables to plot
    bool_vars = []
    if "TTW" in df.columns:
        bool_vars.append("TTW")
    if "is_entry" in df.columns:
        bool_vars.append("is_entry")
    
    if len(bool_vars) == 0:
        print("  [INFO] No boolean variables available; skipping figure")
        return
    
    # Collect plot data
    plot_data = {}
    for var in bool_vars:
        bool_series = coerce_bool(df_plot[var])
        df_plot_var = df_plot[df_plot["contract_start_year"].notna()].copy()
        df_plot_var[var + "_bool"] = bool_series
        
        # Compute share as percentage of ALL rows (including NaN)
        # This gives the true prevalence, not just among explicitly set values
        def compute_share_all_rows(x):
            # Count True values among all rows (including NaN)
            n_true = (x == True).sum()
            n_total = len(x)
            return n_true / n_total if n_total > 0 else 0.0
        
        # Group by contract_start_year
        grouped = df_plot_var.groupby("contract_start_year")[var + "_bool"].agg([
            ('share_true_year', compute_share_all_rows),
            ('count', 'count'),
            ('n_true', lambda x: (x == True).sum()),
            ('n_nonmissing', lambda x: x.notna().sum())
        ])
        
        # Filter years with ≥3 total observations (not just non-missing)
        grouped = grouped[grouped['count'] >= MIN_OBS_PER_YEAR]
        
        if len(grouped) > 0:
            plot_data[var] = grouped['share_true_year']
            # Print diagnostic info
            print(f"  {var}:")
            for year in grouped.index[:5]:  # Print first 5 years
                row = grouped.loc[year]
                print(f"    Year {int(year)}: {row['n_true']:.0f} True / {row['count']:.0f} total = {row['share_true_year']*100:.1f}% (non-missing: {row['n_nonmissing']:.0f})")
    
    if len(plot_data) == 0:
        print("  [INFO] No data available for any variables; skipping figure")
        return
    
    # Compute CAO counts per year (cumulative for latest CAO view)
    cao_counts = {}
    if "cao_number" in df_plot.columns:
        # Get all years from plot_data
        all_years = set()
        for shares in plot_data.values():
            all_years.update(shares.index)
        
        years_sorted = sorted(all_years)
        if use_latest_cao_view:
            # Cumulative: count unique CAOs up to and including each year
            seen_caos = set()
            for year in years_sorted:
                year_data = df_plot[df_plot["contract_start_year"] == year]
                if len(year_data) > 0:
                    seen_caos.update(year_data["cao_number"].dropna().unique())
                cao_counts[year] = len(seen_caos)
        else:
            # Regular view: count unique CAOs per year
            for year in all_years:
                year_data = df_plot[df_plot["contract_start_year"] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
    
    # Create plot
    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    for var, shares in plot_data.items():
        label = var.replace('_', ' ').title()
        ax1.plot(shares.index, shares.values * 100, marker='o', label=label, 
                linewidth=2, markersize=6)
    
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Share of rows with feature (%)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Share of rows with selected features over time{title_suffix}", fontsize=14)
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Add secondary axis for background counts
    if cao_counts:
        ax2 = ax1.twinx()
        years = sorted(all_years)
        
        # Compute row counts per year
        row_counts = []
        for year in years:
            year_data = df_plot[df_plot["contract_start_year"] == year]
            row_counts.append(len(year_data))
        
        # For latest CAO view, show cumulative CAO counts only
        if use_latest_cao_view:
            cao_list = [cao_counts.get(y, 0) for y in years]
            # Single bar showing cumulative CAO count
            ax2.bar(years, cao_list, alpha=0.1, color='gray', label='Cumulative CAOs')
            # Annotations
            for year, cao_count in zip(years, cao_list):
                if cao_count > 0:
                    ax2.text(year, cao_count, f'{int(cao_count)}', ha='center', va='bottom', 
                            fontsize=7, color='gray', alpha=0.6)
        else:
            # For regular view, show both CAO and row counts side by side
            cao_list = [cao_counts.get(y, 0) for y in years]
            ax2.bar([y - 0.2 for y in years], cao_list, width=0.4, alpha=0.1, 
                   color='blue', label='Number of CAOs')
            for year, count in zip(years, cao_list):
                if count > 0:
                    ax2.text(year - 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='blue', alpha=0.5)
            
            ax2.bar([y + 0.2 for y in years], row_counts, width=0.4, alpha=0.1, 
                   color='green', label='Number of rows')
            for year, count in zip(years, row_counts):
                if count > 0:
                    ax2.text(year + 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='green', alpha=0.5)
        
        ax2.set_ylabel("Number of CAOs / Rows", fontsize=12, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_salary_points_per_row_by_year(df: pd.DataFrame, output_dir: Path,
                                       use_latest_cao_view: bool = False) -> None:
    """
    Plot average number of salary points per row by contract start year.
    
    Args:
        df: Wide format DataFrame with n_salary_points_per_row
        output_dir: Directory to save plot
        use_latest_cao_view: If True, use latest CAO forward-filled view
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_points_per_row_by_year{suffix}.png"
    print(f"\nCreating figure: {filename}")
    
    # Use latest CAO view if requested
    if use_latest_cao_view:
        if "cao_number" not in df.columns:
            print("  [WARN] Column 'cao_number' not found; cannot create latest CAO view.")
            return
        df_plot = build_latest_cao_forward_fill(df, cao_col="cao_number", 
                                                date_col="contract_start_year")
        if len(df_plot) == 0:
            print("  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  Using latest CAO forward-filled view: {len(df_plot)} CAO-year combinations")
    else:
        df_plot = df.copy()
    
    if "contract_start_year" not in df_plot.columns:
        print("  [INFO] contract_start_year column not found; skipping figure")
        return
    
    # Compute n_salary_points_per_row if not present
    if "n_salary_points_per_row" not in df_plot.columns:
        print("  Computing n_salary_points_per_row...")
        df_plot["n_salary_points_per_row"] = 0
        SLOT_RANGE = range(1, 12)
        for k in SLOT_RANGE:
            amount_col = f"salary_{k}_amount"
            if amount_col in df_plot.columns:
                df_plot["n_salary_points_per_row"] += df_plot[amount_col].notna().astype(int)
    
    # Filter to rows with valid contract_start_year and n_salary_points_per_row
    df_filtered = df_plot[
        df_plot["contract_start_year"].notna() & 
        df_plot["n_salary_points_per_row"].notna()
    ].copy()
    
    if len(df_filtered) == 0:
        print("  [INFO] No data after filtering; skipping figure")
        return
    
    # Group by contract_start_year
    grouped = df_filtered.groupby("contract_start_year")["n_salary_points_per_row"].agg([
        ('avg_points_per_row', 'mean'),
        ('median_points_per_row', 'median'),
        ('count', 'count')
    ])
    
    # Filter years with ≥3 observations
    grouped = grouped[grouped['count'] >= MIN_OBS_PER_YEAR]
    
    if len(grouped) == 0:
        print("  [INFO] No years with sufficient data; skipping figure")
        return
    
    # Compute CAO counts per year (cumulative for latest CAO view)
    cao_counts = {}
    if "cao_number" in df_filtered.columns:
        years_sorted = sorted(grouped.index)
        if use_latest_cao_view:
            # Cumulative: count unique CAOs up to and including each year
            seen_caos = set()
            for year in years_sorted:
                year_data = df_filtered[df_filtered["contract_start_year"] == year]
                if len(year_data) > 0:
                    seen_caos.update(year_data["cao_number"].dropna().unique())
                cao_counts[year] = len(seen_caos)
        else:
            # Regular view: count unique CAOs per year
            for year in grouped.index:
                year_data = df_filtered[df_filtered["contract_start_year"] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
    
    # Create plot with two lines: mean and median
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(grouped.index, grouped['avg_points_per_row'], marker='o', label='Mean', 
            linewidth=2, markersize=6)
    ax1.plot(grouped.index, grouped['median_points_per_row'], marker='s', label='Median', 
            linewidth=2, markersize=6, linestyle='--')
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Number of salary points per row", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Average number of salary points per row over time{title_suffix}", fontsize=14)
    ax1.legend(fontsize=10, loc='best')
    ax1.grid(True, alpha=0.3)
    
    # Add secondary axis for background counts
    if cao_counts or len(grouped) > 0:
        ax2 = ax1.twinx()
        years = sorted(grouped.index)
        
        # For latest CAO view, show cumulative CAO counts only
        if use_latest_cao_view:
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                # Single bar showing cumulative CAO count
                ax2.bar(years, cao_list, alpha=0.1, color='gray', label='Cumulative CAOs')
                # Annotations
                for year, cao_count in zip(years, cao_list):
                    if cao_count > 0:
                        ax2.text(year, cao_count, f'{int(cao_count)}', ha='center', va='bottom', 
                                fontsize=7, color='gray', alpha=0.6)
        else:
            # For regular view, show both CAO and row counts
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                ax2.bar([y - 0.2 for y in years], cao_list, width=0.4, alpha=0.1, 
                       color='blue', label='Number of CAOs')
                for year, count in zip(years, cao_list):
                    if count > 0:
                        ax2.text(year - 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                                fontsize=7, color='blue', alpha=0.5)
            
            row_counts = [grouped.loc[y, 'count'] for y in years]
            ax2.bar([y + 0.2 for y in years], row_counts, width=0.4, alpha=0.1, 
                   color='green', label='Number of rows')
            for year, count in zip(years, row_counts):
                if count > 0:
                    ax2.text(year + 0.2, count, f'{int(count)}', ha='center', va='bottom', 
                            fontsize=7, color='green', alpha=0.5)
        
        ax2.set_ylabel("Number of CAOs / Rows", fontsize=12, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")
    
    # Print some diagnostic info
    print(f"  Summary:")
    for year in grouped.index[:5]:  # Print first 5 years
        row = grouped.loc[year]
        print(f"    Year {int(year)}: mean={row['avg_points_per_row']:.2f}, "
              f"median={row['median_points_per_row']:.1f}, n={int(row['count'])}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point for plotting script."""
    print("="*80)
    print("CAO Salary Time Trend Plotting Script")
    print("="*80)
    
    # Load data
    print(f"\nLoading data from: {INPUT_CSV}")
    try:
        df = pd.read_csv(INPUT_CSV, sep=';', encoding='utf-8')
        print(f"  Loaded {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        print(f"  ERROR: Could not load input file: {e}")
        return
    
    if len(df) == 0:
        print("  ERROR: Input file is empty")
        return
    
    # Parse date columns (CAO metadata dates are in DD/MM/YYYY format)
    date_cols = ["ingangsdatum", "expiratiedatum", "datum_kennisgeving"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
    
    # Parse salary date columns
    SLOT_RANGE = range(1, 12)
    for k in SLOT_RANGE:
        for date_type in ["start_date", "end_date"]:
            col = f"salary_{k}_{date_type}"
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Create contract_start_year
    if "ingangsdatum" in df.columns:
        df["contract_start_year"] = df["ingangsdatum"].dt.year
    else:
        print("  Warning: ingangsdatum not found, cannot create contract_start_year")
        df["contract_start_year"] = np.nan
    
    # Create ft_hours_weekly
    if "ft_hours" in df.columns:
        ft_hours_numeric = pd.to_numeric(df["ft_hours"], errors='coerce')
        df["ft_hours_weekly"] = ft_hours_numeric.apply(
            lambda x: x if pd.isna(x) or x <= 200 else x / 52.0
        )
    else:
        print("  Warning: ft_hours column not found")
        df["ft_hours_weekly"] = np.nan
    
    # Create n_salary_points_per_row
    print("\nComputing n_salary_points_per_row...")
    SLOT_RANGE = range(1, 12)
    df["n_salary_points_per_row"] = 0
    for k in SLOT_RANGE:
        amount_col = f"salary_{k}_amount"
        if amount_col in df.columns:
            df["n_salary_points_per_row"] += df[amount_col].notna().astype(int)
    
    # Build long format DataFrame
    print("\nBuilding long format DataFrame...")
    try:
        df_long = build_long_salary_df(df)
        print(f"  Long format: {len(df_long)} rows")
    except Exception as e:
        print(f"  Warning: Error building long format: {e}")
        df_long = pd.DataFrame()
    
    # Create output directory
    output_dir = Path(OUTPUT_FIG_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")
    
    # Generate plots
    print("\n" + "="*80)
    print("Generating plots...")
    print("="*80)
    
    # Generate standard plots
    try:
        plot_ft_hours_by_contract_year(df, output_dir, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in ft_hours plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_salary_amount_primary_unit_by_salary_year(df, df_long, output_dir, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in salary amount plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_increase_percent_by_salary_year(df, df_long, output_dir, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in increase percent plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_boolean_shares_by_contract_year(df, output_dir, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in boolean shares plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_salary_points_per_row_by_year(df, output_dir, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in salary points per row plot: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View plots
    print("\n" + "="*80)
    print("Generating Latest CAO View plots...")
    print("="*80)
    
    try:
        plot_ft_hours_by_contract_year(df, output_dir, use_latest_cao_view=True)
    except Exception as e:
        print(f"  ERROR in ft_hours plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_salary_amount_primary_unit_by_salary_year(df, df_long, output_dir, use_latest_cao_view=True)
    except Exception as e:
        print(f"  ERROR in salary amount plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_increase_percent_by_salary_year(df, df_long, output_dir, use_latest_cao_view=True)
    except Exception as e:
        print(f"  ERROR in increase percent plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_salary_points_per_row_by_year(df, output_dir, use_latest_cao_view=True)
    except Exception as e:
        print(f"  ERROR in salary points per row plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*80)
    print("Script completed successfully!")
    print("="*80)


if __name__ == "__main__":
    main()

