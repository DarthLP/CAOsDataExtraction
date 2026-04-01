"""
CAO Salary Time Trend Plotting Script

This script reads the salary CSV output and generates matplotlib line plots
showing trends over contract start years and salary start years for key salary variables.

Normalization for level trends: ``normalize_salary_slot_to_monthly`` and
``compute_analysis_monthly_floor_and_band_ok`` match ``derive_salary_increase_series``
(NL statutory monthly floor + ``SALARY_ANALYSIS_MONTHLY_CAP_EUR`` on gross monthly EUR).

USAGE:
    python scripts/excel_analysis/descriptives_salary_plots.py

INPUT:
    - outputs/excel/new_results/extracted_data_salary.csv

OUTPUT:
    - outputs/analysis/salary_monthly_band_summary.csv (refreshed when increase events are derived)
    - outputs/analysis/figures/salary/ (directory with PNG plots)

      Time trend plots:
      - salary_ft_hours_by_contract_year.png
      - salary_amount_monthly_eur_band_eligible_by_salary_year.png (normalized EUR/month, band-eligible)
      - outputs/analysis/salary_plot_years_dropped.csv (years excluded from plots due to n < MIN_OBS_PER_YEAR)
      - salary_increase_percent_by_salary_year.png
      - salary_boolean_shares_by_contract_year.png
      - Derived increase plots (diff / merged / CSV, comparison, shift, spaghetti)
"""

import gc
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.excel_analysis.analysis_utils import (
    build_latest_cao_forward_fill_by_file,
    build_long_salary_from_wide,
    coerce_salary_amount_scalar,
    detect_salary_slot_indices,
    enforce_integer_year_axis,
    get_plot_color_cycle,
    parse_cao_date_series,
)
from scripts.excel_analysis.salary_increase_derivation import (
    compute_analysis_monthly_floor_and_band_ok,
    compute_band_summary_stats,
    derive_salary_increase_series,
    normalize_salary_slot_to_monthly,
)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
INPUT_CSV = "outputs/excel/new_results/extracted_data_salary.csv"
OUTPUT_FIG_DIR = "outputs/analysis/figures/salary/"
SALARY_PLOT_DROPPED_YEARS_CSV = "outputs/analysis/salary_plot_years_dropped.csv"
MIN_OBS_PER_YEAR = 10  # Minimum observations per year to include in plot
LOW_N_NOTE_UPPER = 20  # Included years with MIN_OBS_PER_YEAR <= n < this may look unstable visually

# Accumulates rows for SALARY_PLOT_DROPPED_YEARS_CSV; reset at start of main().
_salary_plot_dropped_rows: List[Dict[str, Any]] = []


def reset_salary_plot_dropped_year_log() -> None:
    """Clear the in-memory log of years dropped from plots before a script run."""

    global _salary_plot_dropped_rows
    _salary_plot_dropped_rows = []


def append_salary_plot_dropped_years(
    figure_key: str,
    view: str,
    grouped_with_count: pd.DataFrame,
) -> List[int]:
    """
    Record years with 0 < count < MIN_OBS_PER_YEAR for transparency CSV; return sorted year list for fig notes.

    Args:
        figure_key: Stable identifier for the figure (e.g. ``ft_hours_by_contract_year``).
        view: ``regular`` or ``latest_cao_view``.
        grouped_with_count: Groupby result with a ``count`` column before applying MIN_OBS filter.

    Returns:
        Sorted list of excluded integer years (for compact figure footnotes).
    """
    dropped_years: List[int] = []
    if grouped_with_count is None or len(grouped_with_count) == 0 or "count" not in grouped_with_count.columns:
        return dropped_years
    for year, row in grouped_with_count.iterrows():
        n = int(row["count"]) if pd.notna(row["count"]) else 0
        if 0 < n < MIN_OBS_PER_YEAR:
            try:
                y_int = int(year)
            except (TypeError, ValueError):
                continue
            _salary_plot_dropped_rows.append(
                {
                    "figure_key": figure_key,
                    "view": view,
                    "year": y_int,
                    "n_obs": n,
                    "drop_reason": "below_min_obs_per_year",
                    "min_obs_threshold": MIN_OBS_PER_YEAR,
                }
            )
            dropped_years.append(y_int)
    return sorted(dropped_years)


def write_salary_plot_dropped_years_csv(path: Optional[Path] = None) -> None:
    """
    Write accumulated dropped-year rows to CSV (semicolon, European decimal for consistency).

    Args:
        path: Output path; defaults to SALARY_PLOT_DROPPED_YEARS_CSV under project-relative ``outputs/analysis``.
    """
    out = path or Path(SALARY_PLOT_DROPPED_YEARS_CSV)
    out.parent.mkdir(parents=True, exist_ok=True)
    if not _salary_plot_dropped_rows:
        pd.DataFrame(
            columns=[
                "figure_key",
                "view",
                "year",
                "n_obs",
                "drop_reason",
                "min_obs_threshold",
            ]
        ).to_csv(out, index=False, sep=";", decimal=",")
        return
    pd.DataFrame(_salary_plot_dropped_rows).to_csv(out, index=False, sep=";", decimal=",")


def add_figure_excluded_years_footnote(fig: plt.Figure, excluded_years: List[int], max_show: int = 25) -> None:
    """
    Add a one-line footnote listing years excluded for low n; use after axes are populated.

    Args:
        fig: Figure containing the plot.
        excluded_years: Sorted years with 0 < n_obs < MIN_OBS_PER_YEAR for this figure.
        max_show: Truncate list with "+N more" when longer than this.
    """
    if not excluded_years:
        msg = f"Years with n < {MIN_OBS_PER_YEAR} (excluded from plot): none"
    else:
        ys = sorted(excluded_years)
        if len(ys) > max_show:
            tail = ", ".join(str(y) for y in ys[:max_show])
            msg = f"Years with n < {MIN_OBS_PER_YEAR} (excluded from plot): {tail} … +{len(ys) - max_show} more"
        else:
            msg = f"Years with n < {MIN_OBS_PER_YEAR} (excluded from plot): {', '.join(str(y) for y in ys)}"
    fig.text(0.5, 0.02, msg, ha="center", fontsize=8, style="italic", wrap=True)


def add_figure_low_n_included_years_note(
    fig: plt.Figure,
    grouped_with_count: pd.DataFrame,
    *,
    year_col_label: str = "year",
    upper: int = LOW_N_NOTE_UPPER,
    max_show: int = 25,
) -> None:
    """
    Add a one-line note listing included years with relatively low n.

    This is useful when box/whiskers may look visually minimal or unstable despite meeting the inclusion threshold.

    Args:
        fig: Figure containing the plot.
        grouped_with_count: Groupby result with a ``count`` column (pre-filter).
        year_col_label: Label used in the message.
        upper: Upper bound for inclusion in this note (years with MIN_OBS_PER_YEAR <= n < upper).
        max_show: Truncate list with "+N more" when longer than this.
    """
    if grouped_with_count is None or len(grouped_with_count) == 0 or "count" not in grouped_with_count.columns:
        return
    years_low: List[int] = []
    for year, row in grouped_with_count.iterrows():
        n = int(row["count"]) if pd.notna(row["count"]) else 0
        if MIN_OBS_PER_YEAR <= n < int(upper):
            try:
                years_low.append(int(year))
            except (TypeError, ValueError):
                continue
    years_low = sorted(set(years_low))
    if not years_low:
        return
    if len(years_low) > max_show:
        shown = ", ".join(str(y) for y in years_low[:max_show])
        msg = (
            f"Included {year_col_label}s with {MIN_OBS_PER_YEAR} ≤ n < {int(upper)} "
            f"(variance may be unstable): {shown} … +{len(years_low) - max_show} more"
        )
    else:
        msg = (
            f"Included {year_col_label}s with {MIN_OBS_PER_YEAR} ≤ n < {int(upper)} "
            f"(variance may be unstable): {', '.join(str(y) for y in years_low)}"
        )
    fig.text(0.5, 0.045, msg, ha="center", fontsize=8, style="italic", wrap=True)

# Columns copied onto each long salary row (subset must exist on the wide frame).
SALARY_LONG_IDENTITY_COLS: List[str] = [
    "cao_number", "id", "TTW", "ingangsdatum", "expiratiedatum",
    "datum_kennisgeving", "file_name",
    "jobgroup", "step_label", "worker_type", "is_entry", "age_group",
    "education", "ft_hours", "ft_hours_weekly", "permanency", "hours_type",
    "row_note",
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def columns_for_latest_cao_salary_wide(df: pd.DataFrame) -> List[str]:
    """
    List wide columns needed for latest-view state plots (subset before forward-fill expand).

    Args:
        df: Full wide salary DataFrame

    Returns:
        Column names to pass as value_cols_to_keep when building the expanded panel
    """
    cols: List[str] = []
    for name in (
        "cao_number", "id", "contract_start_year", "file_name", "ingangsdatum",
        "ft_hours_weekly", "TTW", "is_entry", "n_salary_points_per_row",
    ):
        if name in df.columns:
            cols.append(name)
    for k in detect_salary_slot_indices(df.columns.tolist()):
        for field in ("start_date", "amount", "unit", "hours_basis_ft_week"):
            c = f"salary_{k}_{field}"
            if c in df.columns:
                cols.append(c)
    return cols

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


def build_latest_cao_forward_fill(
    df: pd.DataFrame,
    cao_col: str = "cao_number",
    date_col: str = "contract_start_year",
    value_cols_to_keep: Optional[List[str]] = None,
) -> pd.DataFrame:
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
        value_cols_to_keep: Optional subset of value columns to retain (reduces RAM)
        
    Returns:
        DataFrame with forward-filled contract data (one row per CAO-year combination)
    """
    df_copy = df.copy()
    if "contract_start_year" not in df_copy.columns:
        if date_col in df_copy.columns and "date" in date_col.lower():
            df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors="coerce")
            df_copy["contract_start_year"] = df_copy[date_col].dt.year
        else:
            print("  Warning: Cannot create contract_start_year")
            return pd.DataFrame()
    return build_latest_cao_forward_fill_by_file(
        df_copy,
        cao_col=cao_col,
        year_col="contract_start_year",
        file_col="file_name",
        order_date_col="ingangsdatum" if "ingangsdatum" in df_copy.columns else None,
        value_cols_to_keep=value_cols_to_keep,
    )


def build_long_salary_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build long format DataFrame from wide salary format.
    
    Args:
        df: Wide format DataFrame with salary_k_* columns
        
    Returns:
        Long format DataFrame with one row per salary point
    """
    return build_long_salary_from_wide(df, identity_cols=SALARY_LONG_IDENTITY_COLS)


def enrich_long_salary_with_monthly_and_band(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Per long row: normalize amount to monthly EUR, then apply statutory floor + analysis cap band.

    Args:
        df_long: Output of ``build_long_salary_from_wide`` / ``build_long_salary_df`` with
            salary_amount, salary_unit, salary_start_date, and optionally salary_hours_basis_ft_week, ft_hours.

    Returns:
        Copy with columns amount_monthly, conversion_ok, analysis_monthly_floor_eur, analysis_monthly_band_ok.
    """
    if len(df_long) == 0:
        return df_long
    out = df_long.copy()
    if "salary_amount" not in out.columns or "salary_unit" not in out.columns:
        return out
    if "salary_start_date" not in out.columns:
        out["salary_start_date"] = pd.NaT
    has_slot_h = "salary_hours_basis_ft_week" in out.columns
    row_ft = out["ft_hours"] if "ft_hours" in out.columns else pd.Series(np.nan, index=out.index)
    amounts_m: List[Optional[float]] = []
    oks: List[bool] = []
    for i in range(len(out)):
        amt = out["salary_amount"].iloc[i]
        unit = out["salary_unit"].iloc[i]
        sh = out["salary_hours_basis_ft_week"].iloc[i] if has_slot_h else np.nan
        rf = row_ft.iloc[i]
        m, ok, _ = normalize_salary_slot_to_monthly(amt, unit, sh, rf)
        amounts_m.append(m)
        oks.append(bool(ok))
    floor_arr, band_ok = compute_analysis_monthly_floor_and_band_ok(
        np.array(oks, dtype=bool),
        out["salary_start_date"],
        amounts_m,
    )
    out["amount_monthly"] = amounts_m
    out["conversion_ok"] = oks
    out["analysis_monthly_floor_eur"] = floor_arr
    out["analysis_monthly_band_ok"] = band_ok
    return out


def add_yearly_variance_layer(
    ax: plt.Axes,
    df: pd.DataFrame,
    year_col: str,
    value_col: str,
    color: Any,
    *,
    percent_increase_scale: bool = False,
    hide_boxplot_fliers: bool = False,
) -> None:
    """
    Add whisker/box variance layer by year and overlay yearly means (mean = colored line; box midline = median).

    Args:
        ax: Matplotlib axis
        df: Input DataFrame
        year_col: Year column name
        value_col: Numeric value column name
        color: Main color for mean line
        percent_increase_scale: If True, hide fliers and set y-limits to include full whiskers and yearly means
            together with global 5th–95th percentiles, bounded below at −50; top may extend past 100% when needed
            so whiskers are not clipped.
        hide_boxplot_fliers: If True, omit boxplot fliers (e.g. band-eligible EUR/month) without percent-style
            y-limits; orthogonal to ``percent_increase_scale``.
    """
    if year_col not in df.columns or value_col not in df.columns:
        return
    d = df[df[year_col].notna() & df[value_col].notna()].copy()
    if len(d) == 0:
        return
    d[year_col] = pd.to_numeric(d[year_col], errors="coerce")
    d[value_col] = pd.to_numeric(d[value_col], errors="coerce")
    d = d[d[year_col].notna() & d[value_col].notna()]
    if len(d) == 0:
        return
    years = sorted(int(y) for y in d[year_col].unique())
    box_data = [d.loc[d[year_col] == y, value_col].values for y in years]
    show_fliers = not (percent_increase_scale or hide_boxplot_fliers)
    bp = ax.boxplot(
        box_data,
        positions=years,
        widths=0.6,
        patch_artist=True,
        boxprops=dict(facecolor="lightgray", alpha=0.25),
        medianprops=dict(color="dimgray"),
        whiskerprops=dict(color="gray", alpha=0.6),
        capprops=dict(color="gray", alpha=0.6),
        flierprops=dict(marker=".", markersize=2, alpha=0.2),
        showfliers=show_fliers,
    )
    mean_by_year = d.groupby(year_col)[value_col].mean()
    mean_vals = mean_by_year.values.astype(float)
    ax.plot(mean_by_year.index.astype(int), mean_vals, color=color, marker="o", linewidth=2.2, label="Mean")
    enforce_integer_year_axis(ax, years)
    if percent_increase_scale:
        vals = d[value_col].to_numpy(dtype=float)
        vals = vals[np.isfinite(vals)]
        if len(vals) > 0 and bp is not None and "whiskers" in bp and len(bp["whiskers"]) > 0:
            p5, p95 = np.nanpercentile(vals, [5.0, 95.0])
            whisk_y = np.concatenate([np.asarray(seg.get_ydata(), dtype=float) for seg in bp["whiskers"]])
            whisk_y = whisk_y[np.isfinite(whisk_y)]
            y_lo_w = float(np.min(whisk_y)) if len(whisk_y) else float(np.min(vals))
            y_hi_w = float(np.max(whisk_y)) if len(whisk_y) else float(np.max(vals))
            mmin = float(np.nanmin(mean_vals))
            mmax = float(np.nanmax(mean_vals))
            span = float(p95 - p5) if p95 > p5 else 1.0
            pad = max(0.05 * span, 0.12)
            bot_raw = min(p5, y_lo_w, mmin) - pad
            top_raw = max(p95, y_hi_w, mmax) + pad
            bot = max(bot_raw, -50.0)
            top_capped = min(top_raw, 100.0)
            top = max(top_capped, y_hi_w, mmax)
            if top <= bot:
                bot, top = -5.0, 25.0
            ax.set_ylim(bot, top)


# =============================================================================
# PLOTTING FUNCTIONS
# =============================================================================

def plot_ft_hours_by_contract_year(df: pd.DataFrame, output_dir: Path, use_latest_cao_view: bool = False,
                                   df_latest_wide: Optional[pd.DataFrame] = None) -> None:
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
        df_plot = df_latest_wide if df_latest_wide is not None else build_latest_cao_forward_fill(
            df, cao_col="cao_number", date_col="contract_start_year"
        )
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
    
    view = "latest_cao_view" if use_latest_cao_view else "regular"
    grouped_all = df_plot.groupby("contract_start_year")["ft_hours_weekly"].agg([
        ('avg_ft_hours_weekly', 'mean'),
        ('count', 'count')
    ])
    excluded_years = append_salary_plot_dropped_years("ft_hours_by_contract_year", view, grouped_all)
    grouped = grouped_all[grouped_all['count'] >= MIN_OBS_PER_YEAR]
    
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
            df_wide_latest = df_latest_wide if df_latest_wide is not None else build_latest_cao_forward_fill(
                df, cao_col="cao_number", date_col="contract_start_year"
            )
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
    
    # Create plot (boxplots only for years meeting MIN_OBS_PER_YEAR)
    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    years_ok = grouped.index
    df_box = df_plot[df_plot["contract_start_year"].isin(years_ok)]
    add_yearly_variance_layer(
        ax1,
        df_box,
        "contract_start_year",
        "ft_hours_weekly",
        main_color,
        hide_boxplot_fliers=True,
    )
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

    add_figure_excluded_years_footnote(fig, excluded_years)
    add_figure_low_n_included_years_note(fig, grouped_all, year_col_label="year")
    plt.tight_layout(rect=[0, 0.10, 1, 1])
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_salary_amount_monthly_band_eligible_by_salary_year(
    df: pd.DataFrame,
    df_long: pd.DataFrame,
    output_dir: Path,
    use_latest_cao_view: bool = False,
    df_latest_wide: Optional[pd.DataFrame] = None,
) -> None:
    """
    Plot mean normalized gross monthly EUR by salary start year, restricted to band-eligible rows.

    Uses the same conversion and floor/cap rules as ``derive_salary_increase_series``.
    Boxplot fliers are hidden so the y-axis follows box/whisker extent; the overlaid line is the yearly mean;
    the horizontal midline in each box is the median.

    Args:
        df: Wide format DataFrame (for latest-view CAO counts)
        df_long: Long-format salary rows
        output_dir: Directory for PNG output
        use_latest_cao_view: If True, build long data from latest forward-filled wide panel
        df_latest_wide: Optional prebuilt latest wide panel
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_amount_monthly_eur_band_eligible_by_salary_year{suffix}.png"
    print(f"\nCreating figure: {filename}")

    df_long_plot = df_long
    if use_latest_cao_view:
        df_wide_latest = df_latest_wide if df_latest_wide is not None else build_latest_cao_forward_fill(
            df,
            cao_col="cao_number",
            date_col="contract_start_year",
            value_cols_to_keep=columns_for_latest_cao_salary_wide(df),
        )
        if len(df_wide_latest) == 0:
            print("  [WARN] Latest CAO view is empty; skipping.")
            return
        print(f"  [INFO] Latest-view: normalized monthly from state panel ({len(df_wide_latest)} wide rows).")
        id_cols = [c for c in SALARY_LONG_IDENTITY_COLS if c in df_wide_latest.columns]
        df_long_plot = build_long_salary_from_wide(
            df_wide_latest,
            identity_cols=id_cols,
            salary_fields=["start_date", "amount", "unit", "hours_basis_ft_week"],
        )

    if len(df_long_plot) == 0:
        print("  [INFO] Long format DataFrame is empty; skipping figure")
        return

    enriched = enrich_long_salary_with_monthly_and_band(df_long_plot)
    if "analysis_monthly_band_ok" not in enriched.columns:
        print("  [INFO] Enrichment failed; skipping figure")
        return

    df_filtered = enriched[
        enriched["analysis_monthly_band_ok"]
        & enriched["amount_monthly"].notna()
        & enriched["salary_start_year"].notna()
    ].copy()
    if len(df_filtered) == 0:
        print("  [INFO] No band-eligible salary rows; skipping figure")
        return

    df_filtered["amount_monthly"] = pd.to_numeric(df_filtered["amount_monthly"], errors="coerce")
    df_filtered = df_filtered[df_filtered["amount_monthly"].notna()]
    if len(df_filtered) == 0:
        print("  [INFO] No numeric amount_monthly; skipping figure")
        return

    view = "latest_cao_view" if use_latest_cao_view else "regular"
    grouped = df_filtered.groupby("salary_start_year")["amount_monthly"].agg(
        [("avg_amount_year", "mean"), ("count", "count")]
    )
    excluded_years = append_salary_plot_dropped_years(
        "salary_amount_monthly_eur_band_eligible", view, grouped
    )
    grouped = grouped[grouped["count"] >= MIN_OBS_PER_YEAR]
    if len(grouped) == 0:
        print("  [INFO] No years with sufficient band-eligible data; skipping figure")
        return

    cao_counts: Dict[Any, int] = {}
    if use_latest_cao_view:
        if "cao_number" in df.columns and "contract_start_year" in df.columns:
            df_wide_years = df[df["contract_start_year"].notna()].copy()
            if len(df_wide_years) > 0:
                years_sorted = sorted(grouped.index)
                seen_caos: set = set()
                for year in years_sorted:
                    year_data_wide = df_wide_years[df_wide_years["contract_start_year"] <= year]
                    if len(year_data_wide) > 0:
                        seen_caos.update(year_data_wide["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
        elif "cao_number" in df_filtered.columns:
            years_sorted = sorted(grouped.index)
            seen_caos = set()
            for year in years_sorted:
                year_data = df_filtered[df_filtered["salary_start_year"] == year]
                if len(year_data) > 0:
                    seen_caos.update(year_data["cao_number"].dropna().unique())
                cao_counts[year] = len(seen_caos)
    else:
        if "cao_number" in df_filtered.columns:
            for year in grouped.index:
                year_data = df_filtered[df_filtered["salary_start_year"] == year]
                cao_counts[year] = year_data["cao_number"].nunique() if len(year_data) > 0 else 0

    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    years_ok = grouped.index
    df_box = df_filtered[df_filtered["salary_start_year"].isin(years_ok)]
    add_yearly_variance_layer(
        ax1,
        df_box,
        "salary_start_year",
        "amount_monthly",
        main_color,
        hide_boxplot_fliers=True,
    )
    ax1.set_xlabel("Salary start year", fontsize=12)
    ax1.set_ylabel("Gross monthly EUR (normalized, band-eligible)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(
        f"Average salary (EUR/month, band-eligible: NL statutory min + analysis cap){title_suffix}",
        fontsize=13,
    )
    ax1.grid(True, alpha=0.3)

    if cao_counts or len(grouped) > 0:
        ax2 = ax1.twinx()
        years = sorted(grouped.index)
        if use_latest_cao_view:
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                ax2.bar(years, cao_list, alpha=0.1, color="gray", label="Cumulative CAOs")
                for year, cao_count in zip(years, cao_list):
                    if cao_count > 0:
                        ax2.text(
                            year,
                            cao_count,
                            f"{int(cao_count)}",
                            ha="center",
                            va="bottom",
                            fontsize=7,
                            color="gray",
                            alpha=0.6,
                        )
        else:
            if cao_counts:
                cao_list = [cao_counts.get(y, 0) for y in years]
                ax2.bar(
                    [y - 0.2 for y in years],
                    cao_list,
                    width=0.4,
                    alpha=0.1,
                    color="blue",
                    label="Number of CAOs",
                )
                for year, count in zip(years, cao_list):
                    if count > 0:
                        ax2.text(
                            year - 0.2,
                            count,
                            f"{int(count)}",
                            ha="center",
                            va="bottom",
                            fontsize=7,
                            color="blue",
                            alpha=0.5,
                        )
            row_counts = [grouped.loc[y, "count"] for y in years]
            ax2.bar(
                [y + 0.2 for y in years],
                row_counts,
                width=0.4,
                alpha=0.1,
                color="green",
                label="Number of rows",
            )
            for year, count in zip(years, row_counts):
                if count > 0:
                    ax2.text(
                        year + 0.2,
                        count,
                        f"{int(count)}",
                        ha="center",
                        va="bottom",
                        fontsize=7,
                        color="green",
                        alpha=0.5,
                    )
        ax2.set_ylabel("Number of CAOs / Rows", fontsize=12, color="gray")
        ax2.tick_params(axis="y", labelcolor="gray")

    add_figure_excluded_years_footnote(fig, excluded_years)
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {output_path}")
    if use_latest_cao_view:
        gc.collect()


def plot_increase_percent_by_salary_year(
    df: pd.DataFrame,
    df_long: pd.DataFrame,
    output_dir: Path,
    use_latest_cao_view: bool = False,
    increase_events: Optional[pd.DataFrame] = None,
) -> None:
    """
    Plot average merged increase by contract-start cohort year (mean-of-means).
    
    Args:
        df: Wide format DataFrame (needed for latest CAO view)
        df_long: Long format DataFrame (kept for API compatibility)
        output_dir: Directory to save plot
        use_latest_cao_view: If True, title/secondary axis reflect latest-view run
        increase_events: Optional derived events table from derive_salary_increase_series(df)
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_increase_percent_by_salary_year{suffix}.png"
    print(f"\nCreating figure: {filename}")

    events = increase_events
    if events is None or len(events) == 0:
        try:
            events = derive_salary_increase_series(df).get("events", pd.DataFrame())
        except Exception:
            events = pd.DataFrame()
    if len(events) == 0:
        print("  [INFO] No derived increase events available; skipping figure")
        return

    required_cols = {"cao_number", "file_name", "ingangsdatum", "increase_merged_pref_csv"}
    if not required_cols.issubset(set(events.columns)):
        print("  [INFO] Missing required merged-increase columns; skipping figure")
        return

    df_plot = events.copy()
    df_plot["increase_merged_pref_csv"] = pd.to_numeric(df_plot["increase_merged_pref_csv"], errors="coerce")
    df_plot["ingangsdatum"] = parse_cao_date_series(df_plot["ingangsdatum"], dayfirst=True)
    df_plot["contract_start_year"] = pd.to_datetime(df_plot["ingangsdatum"], errors="coerce").dt.year

    # Contract-level mean (contract = CAO + file), then cohort mean by contract start year
    contract_means = (
        df_plot[
            df_plot["increase_merged_pref_csv"].notna()
            & df_plot["contract_start_year"].notna()
            & df_plot["cao_number"].notna()
            & df_plot["file_name"].notna()
        ]
        .groupby(["cao_number", "file_name", "contract_start_year"], dropna=False)["increase_merged_pref_csv"]
        .mean()
        .reset_index(name="contract_mean_increase")
    )

    if len(contract_means) == 0:
        print("  [INFO] No contract-level merged increases available after filtering; skipping figure")
        return
    
    # Latest-only: forward-fill each CAO's file-mean state between file years.
    # Regular view remains the contract-cohort mean-of-means by observed contract_start_year.
    df_plot_for_year = contract_means
    if use_latest_cao_view:
        cao_year_means = (
            contract_means
            .groupby(["cao_number", "contract_start_year"], dropna=False)["contract_mean_increase"]
            .mean()
            .reset_index(name="contract_mean_increase")
            .sort_values(["cao_number", "contract_start_year"])
        )
        if len(cao_year_means) == 0:
            print("  [INFO] No CAO-year means for latest forward-fill; skipping figure")
            return
        if "contract_start_year" in df.columns:
            global_max_year = int(pd.to_numeric(df["contract_start_year"], errors="coerce").dropna().max())
        else:
            global_max_year = int(cao_year_means["contract_start_year"].max())
        ff_rows: List[Dict[str, Any]] = []
        for cao, grp in cao_year_means.groupby("cao_number"):
            g = grp.sort_values("contract_start_year").reset_index(drop=True)
            years = g["contract_start_year"].astype(int).tolist()
            vals = g["contract_mean_increase"].astype(float).tolist()
            for i, y0 in enumerate(years):
                y1 = years[i + 1] - 1 if i + 1 < len(years) else global_max_year
                if y1 < y0:
                    continue
                for y in range(int(y0), int(y1) + 1):
                    ff_rows.append(
                        {
                            "cao_number": cao,
                            "contract_start_year": y,
                            "contract_mean_increase": vals[i],
                        }
                    )
        df_plot_for_year = pd.DataFrame(ff_rows)
        if len(df_plot_for_year) == 0:
            print("  [INFO] Latest forward-filled panel is empty; skipping figure")
            return

    view_ip = "latest_cao_view" if use_latest_cao_view else "regular"
    grouped_all = df_plot_for_year.groupby("contract_start_year")["contract_mean_increase"].agg([
        ('avg_inc_year', 'mean'),
        ('count', 'count')
    ])
    excluded_ip = append_salary_plot_dropped_years("salary_increase_merged_contract_cohort_by_year", view_ip, grouped_all)
    grouped = grouped_all[grouped_all['count'] >= MIN_OBS_PER_YEAR]
    
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
            # Fallback to plotted data if wide format not available
            if "cao_number" in df_plot_for_year.columns:
                years_sorted = sorted(grouped.index)
                seen_caos = set()
                for year in years_sorted:
                    year_data = df_plot_for_year[df_plot_for_year["contract_start_year"] == year]
                    if len(year_data) > 0:
                        seen_caos.update(year_data["cao_number"].dropna().unique())
                    cao_counts[year] = len(seen_caos)
    else:
        # Regular view: count unique CAOs per year from filtered data
        if "cao_number" in df_plot_for_year.columns:
            for year in grouped.index:
                year_data = df_plot_for_year[df_plot_for_year["contract_start_year"] == year]
                if len(year_data) > 0:
                    cao_counts[year] = year_data["cao_number"].nunique()
                else:
                    cao_counts[year] = 0
    
    # Create plot
    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    years_ip = grouped.index
    df_box_ip = df_plot_for_year[df_plot_for_year["contract_start_year"].isin(years_ip)]
    add_yearly_variance_layer(
        ax1,
        df_box_ip,
        "contract_start_year",
        "contract_mean_increase",
        main_color,
        hide_boxplot_fliers=True,
    )
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Average increase (%)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Average increase (merged - prefer CSV) by contract start year{title_suffix}", fontsize=14)
    ax1.set_ylim(-2, 10)
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
    
    add_figure_excluded_years_footnote(fig, excluded_ip)
    add_figure_low_n_included_years_note(fig, grouped_all, year_col_label="year")
    plt.tight_layout(rect=[0, 0.10, 1, 1])
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_boolean_shares_by_contract_year(df: pd.DataFrame, output_dir: Path,
                                         use_latest_cao_view: bool = False,
                                         df_latest_wide: Optional[pd.DataFrame] = None) -> None:
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
        df_plot = df_latest_wide if df_latest_wide is not None else build_latest_cao_forward_fill(
            df, cao_col="cao_number", date_col="contract_start_year"
        )
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
    view_bool = "latest_cao_view" if use_latest_cao_view else "regular"
    excluded_bool_union: set = set()
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
        excluded_bool_union.update(
            append_salary_plot_dropped_years(f"boolean_shares_{var}", view_bool, grouped)
        )
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
        if var == "TTW":
            label = "TTW - temporary CAO update"
        else:
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
    
    add_figure_excluded_years_footnote(fig, sorted(excluded_bool_union))
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_salary_points_per_row_by_year(df: pd.DataFrame, output_dir: Path,
                                       use_latest_cao_view: bool = False,
                                       df_latest_wide: Optional[pd.DataFrame] = None) -> None:
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
        df_plot = df_latest_wide if df_latest_wide is not None else build_latest_cao_forward_fill(
            df, cao_col="cao_number", date_col="contract_start_year"
        )
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
        SLOT_RANGE = detect_salary_slot_indices(df_plot.columns.tolist())
        for k in SLOT_RANGE:
            amount_col = f"salary_{k}_amount"
            if amount_col in df_plot.columns:
                _am = df_plot[amount_col].map(coerce_salary_amount_scalar)
                df_plot["n_salary_points_per_row"] += (_am.notna() & (_am > 0)).astype(int)
    
    # Filter to rows with valid contract_start_year and n_salary_points_per_row
    df_filtered = df_plot[
        df_plot["contract_start_year"].notna() & 
        df_plot["n_salary_points_per_row"].notna()
    ].copy()
    
    if len(df_filtered) == 0:
        print("  [INFO] No data after filtering; skipping figure")
        return
    
    view_pts = "latest_cao_view" if use_latest_cao_view else "regular"
    grouped = df_filtered.groupby("contract_start_year")["n_salary_points_per_row"].agg([
        ('avg_points_per_row', 'mean'),
        ('median_points_per_row', 'median'),
        ('count', 'count')
    ])
    excluded_pts = append_salary_plot_dropped_years("salary_points_per_row_by_year", view_pts, grouped)
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
    colors = get_plot_color_cycle(2)
    ax1.plot(grouped.index.astype(int), grouped['avg_points_per_row'], marker='o', label='Mean',
            linewidth=2, markersize=6, color=colors[0])
    ax1.plot(grouped.index.astype(int), grouped['median_points_per_row'], marker='s', label='Median',
            linewidth=2, markersize=6, linestyle='--', color=colors[1])
    enforce_integer_year_axis(ax1, [int(y) for y in grouped.index.tolist()])
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
    
    add_figure_excluded_years_footnote(fig, excluded_pts)
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    
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


def _plot_single_increase_series(events: pd.DataFrame, output_dir: Path, column: str, filename: str, label: str) -> None:
    """
    Plot one increase series with variance layer and yearly mean line.

    Uses hidden boxplot fliers and a fixed y-axis range for comparability across single-series increase charts.
    """
    d = events[events[column].notna() & events["salary_start_year"].notna()].copy()
    if len(d) == 0:
        return
    d["salary_start_year"] = pd.to_numeric(d["salary_start_year"], errors="coerce")
    d[column] = pd.to_numeric(d[column], errors="coerce")
    d = d[d["salary_start_year"].notna() & d[column].notna()]
    if len(d) == 0:
        return
    fig, ax = plt.subplots(figsize=(11, 6))
    color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(
        ax, d, "salary_start_year", column, color, hide_boxplot_fliers=True
    )
    ax.set_title(f"{label} by salary start year", fontsize=12)
    ax.set_xlabel("Salary start year", fontsize=12)
    ax.set_ylabel("Average increase (%)", fontsize=12)
    ax.set_ylim(-2, 10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / filename, dpi=300, bbox_inches="tight")
    plt.close()


def plot_increase_series_comparison(events: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot diff-only, merged-pref-csv and csv-only yearly means in one figure.
    """
    cols = ["increase_diff_only", "increase_merged_pref_csv", "increase_csv_only"]
    labels = ["Diff only", "Merged (prefer CSV)", "CSV only"]
    colors = get_plot_color_cycle(3)
    fig, ax = plt.subplots(figsize=(12, 6))
    years_union: List[int] = []
    for col, lab, color in zip(cols, labels, colors):
        d = events[events[col].notna() & events["salary_start_year"].notna()].copy()
        if len(d) == 0:
            continue
        d["salary_start_year"] = pd.to_numeric(d["salary_start_year"], errors="coerce")
        d[col] = pd.to_numeric(d[col], errors="coerce")
        d = d[d["salary_start_year"].notna() & d[col].notna()]
        if len(d) == 0:
            continue
        grouped = d.groupby("salary_start_year")[col].mean()
        ax.plot(grouped.index.astype(int), grouped.values, marker="o", linewidth=2.2, color=color, label=lab)
        years_union.extend([int(y) for y in grouped.index.tolist()])
    if not years_union:
        plt.close()
        return
    enforce_integer_year_axis(ax, years_union)
    ax.set_title("Average general wage increase comparison by salary start year", fontsize=12)
    ax.set_xlabel("Salary start year", fontsize=12)
    ax.set_ylabel("Average increase (%)", fontsize=12)
    ax.set_ylim(0, 4)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=10, loc="best")
    plt.tight_layout()
    plt.savefig(output_dir / "salary_increase_series_comparison_by_year.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_shift_by_new_file_year(events: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot yearly average shift in file-level means between consecutive files within CAO.
    """
    required_cols = {"cao_number", "file_name", "ingangsdatum"}
    if not required_cols.issubset(set(events.columns)):
        return
    d = events.copy()
    d["ingangsdatum"] = parse_cao_date_series(d["ingangsdatum"], dayfirst=True)
    series_cols = ["increase_diff_only", "increase_merged_pref_csv", "increase_csv_only"]
    file_means = (
        d.groupby(["cao_number", "file_name", "ingangsdatum"], dropna=False)[series_cols]
        .mean()
        .reset_index()
        .sort_values(["cao_number", "ingangsdatum", "file_name"])
    )
    rows: List[Dict[str, Any]] = []
    for cao, grp in file_means.groupby("cao_number"):
        grp = grp.sort_values(["ingangsdatum", "file_name"]).reset_index(drop=True)
        for i in range(1, len(grp)):
            prev_row = grp.iloc[i - 1]
            new_row = grp.iloc[i]
            year = new_row["ingangsdatum"].year if pd.notna(new_row["ingangsdatum"]) else np.nan
            rows.append(
                {
                    "cao_number": cao,
                    "year_new_file": year,
                    "shift_diff_only": new_row["increase_diff_only"] - prev_row["increase_diff_only"],
                    "shift_merged_pref_csv": new_row["increase_merged_pref_csv"] - prev_row["increase_merged_pref_csv"],
                    "shift_csv_only": new_row["increase_csv_only"] - prev_row["increase_csv_only"],
                }
            )
    shifts = pd.DataFrame(rows)
    if len(shifts) == 0:
        return
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = get_plot_color_cycle(3)
    mapping = [
        ("shift_diff_only", "Diff only", colors[0]),
        ("shift_merged_pref_csv", "Merged (prefer CSV)", colors[1]),
        ("shift_csv_only", "CSV only", colors[2]),
    ]
    years_union: List[int] = []
    for col, label, color in mapping:
        s = shifts[shifts[col].notna() & shifts["year_new_file"].notna()].copy()
        if len(s) == 0:
            continue
        grouped = s.groupby("year_new_file")[col].mean()
        ax.plot(grouped.index.astype(int), grouped.values, marker="o", color=color, linewidth=2, label=label)
        years_union.extend([int(y) for y in grouped.index.tolist()])
    if not years_union:
        plt.close()
        return
    enforce_integer_year_axis(ax, years_union)
    ax.set_title("Average increase shift by new file year", fontsize=14)
    ax.set_xlabel("New file ingangsdatum year", fontsize=12)
    ax.set_ylabel("Average shift (pp)", fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(output_dir / "salary_increase_shift_by_new_file_year.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_spaghetti_selected_caos(events: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot selected CAO lines (yearly top/bottom performers union) plus mean over all band-eligible CAOs.

    The thick black line is the mean of ``increase_merged_pref_csv`` over every CAO in the
    deduped panel (latest file slice per CAO × salary year), not only the highlighted subset.
    """
    d = events[events["increase_merged_pref_csv"].notna() & events["salary_start_year"].notna()].copy()
    if len(d) == 0:
        return
    d["salary_start_year"] = pd.to_numeric(d["salary_start_year"], errors="coerce")
    d = d[d["salary_start_year"].notna()]
    if len(d) == 0:
        return
    agg = (
        d.groupby(["cao_number", "salary_start_year", "ingangsdatum", "file_name"], dropna=False)["increase_merged_pref_csv"]
        .mean()
        .reset_index()
    )
    agg["ingangsdatum"] = parse_cao_date_series(agg["ingangsdatum"], dayfirst=True)
    agg = agg.sort_values(["cao_number", "salary_start_year", "ingangsdatum", "file_name"])
    dedup = (
        agg.groupby(["cao_number", "salary_start_year"], as_index=False)
        .tail(1)
        .copy()
    )
    top_bottom_ids: set = set()
    for year, grp in dedup.groupby("salary_start_year"):
        g = grp.sort_values("increase_merged_pref_csv")
        top_bottom_ids.update(g.head(3)["cao_number"].tolist())
        top_bottom_ids.update(g.tail(3)["cao_number"].tolist())
    selected = dedup[dedup["cao_number"].isin(top_bottom_ids)].copy()
    counts = selected.groupby("cao_number")["salary_start_year"].nunique()
    selected = selected[selected["cao_number"].isin(counts[counts >= 2].index)]
    if len(selected) == 0:
        return
    fig, ax = plt.subplots(figsize=(13, 7))
    colors = get_plot_color_cycle(max(1, selected["cao_number"].nunique()))
    for i, (cao, grp) in enumerate(selected.groupby("cao_number")):
        grp = grp.sort_values("salary_start_year")
        ax.plot(grp["salary_start_year"].astype(int), grp["increase_merged_pref_csv"], color=colors[i], alpha=0.4, linewidth=1)
    grand_all = dedup.groupby("salary_start_year")["increase_merged_pref_csv"].mean()
    ax.plot(
        grand_all.index.astype(int),
        grand_all.values,
        color="black",
        linewidth=3,
        label="Mean (all CAOs, band-eligible)",
    )
    years_axis = sorted(set(grand_all.index.astype(int).tolist()) | set(selected["salary_start_year"].astype(int).unique()))
    enforce_integer_year_axis(ax, years_axis if years_axis else [int(y) for y in grand_all.index.tolist()])
    ax.set_title(
        "Selected CAO salary-increase trajectories (top/bottom yearly union)\n"
        "Thin colored lines: highlighted CAOs; black line: mean over all dedup-valid CAOs",
        fontsize=12,
    )
    ax.set_xlabel("Salary start year", fontsize=12)
    ax.set_ylabel("Average increase (%)", fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)
    plt.tight_layout()
    plt.savefig(output_dir / "salary_increase_spaghetti_selected_caos.png", dpi=300, bbox_inches="tight")
    plt.close()


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
        log_memory("raw_wide", df)
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
            df[col] = parse_cao_date_series(df[col], dayfirst=True)
    
    # Parse salary date columns
    SLOT_RANGE = detect_salary_slot_indices(df.columns.tolist())
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
    SLOT_RANGE = detect_salary_slot_indices(df.columns.tolist())
    df["n_salary_points_per_row"] = 0
    for k in SLOT_RANGE:
        amount_col = f"salary_{k}_amount"
        if amount_col in df.columns:
            _am = df[amount_col].map(coerce_salary_amount_scalar)
            df["n_salary_points_per_row"] += (_am.notna() & (_am > 0)).astype(int)
    
    # Build long format DataFrame
    print("\nBuilding long format DataFrame...")
    try:
        df_long = build_long_salary_df(df)
        print(f"  Long format: {len(df_long)} rows")
        log_memory("long_regular", df_long)
    except Exception as e:
        print(f"  Warning: Error building long format: {e}")
        df_long = pd.DataFrame()

    print("\nBuilding latest CAO view wide panel (subset columns)...")
    try:
        wide_keep = columns_for_latest_cao_salary_wide(df)
        df_latest_wide = build_latest_cao_forward_fill(
            df,
            cao_col="cao_number",
            date_col="contract_start_year",
            value_cols_to_keep=wide_keep,
        )
        print(f"  Latest wide rows: {len(df_latest_wide)}")
        if len(df_latest_wide) > 0:
            log_memory("latest_wide", df_latest_wide)
    except Exception as e:
        print(f"  Warning: Error building latest CAO view artifacts: {e}")
        df_latest_wide = pd.DataFrame()

    band_summary: Dict[str, Any] = {}
    try:
        increase_payload = derive_salary_increase_series(df)
        increase_events = increase_payload["events"]
        band_summary = increase_payload.get("band_summary") or compute_band_summary_stats(increase_events)
        print(f"  Derived salary increase events: {len(increase_events)}")
        if band_summary:
            print(
                f"  Monthly band: eligible={band_summary.get('n_band_eligible', 0)} "
                f"| below_floor={band_summary.get('n_dropped_below_floor', 0)} "
                f"| above_cap={band_summary.get('n_dropped_above_cap', 0)} "
                f"| missing_date={band_summary.get('n_dropped_missing_salary_date', 0)} "
                f"(conversion_ok={band_summary.get('n_conversion_ok', 0)})"
            )
        diagnostics_dir = Path("outputs/analysis")
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        band_summary_path = diagnostics_dir / "salary_monthly_band_summary.csv"
        pd.DataFrame([band_summary]).to_csv(band_summary_path, index=False, sep=";", decimal=",")
        print(f"  Wrote {band_summary_path}")
    except Exception as e:
        print(f"  Warning: Error deriving salary increase events: {e}")
        increase_events = pd.DataFrame()
        band_summary = compute_band_summary_stats(increase_events)
        try:
            diagnostics_dir = Path("outputs/analysis")
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame([band_summary]).to_csv(
                diagnostics_dir / "salary_monthly_band_summary.csv", index=False, sep=";", decimal=","
            )
        except Exception:
            pass

    # Create output directory
    output_dir = Path(OUTPUT_FIG_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nOutput directory: {output_dir}")
    reset_salary_plot_dropped_year_log()

    # Generate plots
    print("\n" + "="*80)
    print("Generating plots...")
    print("="*80)
    print("Recommendation: run heavy scripts sequentially in a single process.")
    
    # Generate standard plots
    try:
        plot_ft_hours_by_contract_year(df, output_dir, use_latest_cao_view=False)
    except Exception as e:
        print(f"  ERROR in ft_hours plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_salary_amount_monthly_band_eligible_by_salary_year(
            df, df_long, output_dir, use_latest_cao_view=False
        )
    except Exception as e:
        print(f"  ERROR in salary amount monthly band-eligible plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_increase_percent_by_salary_year(
            df, df_long, output_dir, use_latest_cao_view=False, increase_events=increase_events
        )
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

    try:
        _plot_single_increase_series(
            increase_events, output_dir, "increase_diff_only",
            "salary_increase_diff_only_by_salary_year.png", "Derived increase (diff only)"
        )
        _plot_single_increase_series(
            increase_events, output_dir, "increase_merged_pref_csv",
            "salary_increase_merged_pref_csv_by_salary_year.png", "Merged increase (prefer CSV)"
        )
        _plot_single_increase_series(
            increase_events, output_dir, "increase_csv_only",
            "salary_increase_csv_only_by_salary_year.png", "CSV-reported increase"
        )
        plot_increase_series_comparison(increase_events, output_dir)
        plot_shift_by_new_file_year(increase_events, output_dir)
        plot_spaghetti_selected_caos(increase_events, output_dir)
    except Exception as e:
        print(f"  ERROR in derived increase plots: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View plots
    print("\n" + "="*80)
    print("Generating Latest CAO View plots...")
    print("="*80)
    
    try:
        plot_ft_hours_by_contract_year(df, output_dir, use_latest_cao_view=True, df_latest_wide=df_latest_wide)
    except Exception as e:
        print(f"  ERROR in ft_hours plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()
    
    try:
        plot_salary_amount_monthly_band_eligible_by_salary_year(
            df, df_long, output_dir, use_latest_cao_view=True, df_latest_wide=df_latest_wide,
        )
    except Exception as e:
        print(f"  ERROR in salary amount monthly band-eligible plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()
    
    try:
        plot_increase_percent_by_salary_year(
            df, df_long, output_dir, use_latest_cao_view=True, increase_events=increase_events
        )
    except Exception as e:
        print(f"  ERROR in increase percent plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()
    
    try:
        plot_salary_points_per_row_by_year(df, output_dir, use_latest_cao_view=True, df_latest_wide=df_latest_wide)
    except Exception as e:
        print(f"  ERROR in salary points per row plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()
    
    write_salary_plot_dropped_years_csv()
    print(f"  Wrote dropped-years log: {SALARY_PLOT_DROPPED_YEARS_CSV}")

    print("\n" + "="*80)
    print("Script completed successfully!")
    print("="*80)


if __name__ == "__main__":
    main()

