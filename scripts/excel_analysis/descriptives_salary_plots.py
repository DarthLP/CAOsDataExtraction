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
    - outputs/analysis/salary_band_and_conversion_diagnostics.csv (row / CAO / aggregate QA)
    - outputs/analysis/figures/salary/ (PNG directory; inventory below matches analysis plan section 7)
    - outputs/analysis/salary_plot_years_dropped.csv (header-only; no minimum-n year exclusion)

    Band-eligible salary (EUR/month), CAO-equal weights, weighted ``bxp``; twin axis = cohort-local CAO count
    (distinct ``cao_number`` with ≥1 row in the same analytic frame as the box/mean layer):

    - salary_amount_monthly_eur_band_eligible_by_salary_year.png — x = **Salary start year**
    - salary_amount_monthly_eur_band_eligible_by_salary_year_latest_cao_view.png — x = **Salary year** ``T``; per-key latest slot with ``salary_start_date`` ≤ end(``T``) on the **snapped** active file (``snap_active_table_to_band_eligible_salary_files``), plus **file-transition carry** when the new file has no such rows yet (see ``build_salary_year_latest_gap_panel``).
    - salary_amount_monthly_eur_band_eligible_by_contract_year.png — x = **Contract start year** (governing file; all band-eligible slots on that file).
    - salary_amount_monthly_eur_band_eligible_by_contract_year_latest_cao_view.png — x = **Calendar year** ``T``; **all** band-eligible long slots on the **snapped** active file per ``(CAO, T)`` (no date-vs-``T`` filter).

    Contract-cohort merged increase (renamed from misleading ``*_by_salary_year``):

    - salary_increase_percent_by_contract_year.png / _latest_cao_view — x = **Contract start year**

    Increase by **salary start year** (overlap + weights) and **Salary year** latest (active file + 0% if no event):

    - salary_increase_diff_only_by_salary_year.png
    - salary_increase_merged_pref_csv_by_salary_year.png
    - salary_increase_csv_only_by_salary_year.png
    - salary_increase_diff_only_by_salary_year_latest_cao_view.png
    - salary_increase_merged_pref_csv_by_salary_year_latest_cao_view.png
    - salary_increase_csv_only_by_salary_year_latest_cao_view.png (same **snapped** active file as salary latest)

    Derived increase (weighted means; twins per plan):

    - salary_increase_series_comparison_by_year.png (twin: merged series ``S_y``)
    - salary_increase_shift_by_new_file_year.png (twin: merged shift ``S_y``)
    - salary_increase_spaghetti_selected_caos.png (twin: grand-line ``S_y``)

    Other (weights + twin where applicable):

    - salary_ft_hours_by_contract_year.png
    - salary_boolean_shares_by_contract_year.png (+ _latest_cao_view)
    - salary_points_per_row_by_year.png (band-eligible slot counts per wide row)
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
    SALARY_ANALYSIS_MONTHLY_CAP_EUR,
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
from scripts.excel_analysis.salary_eligibility_diagnostics import (
    count_band_eligible_slots_per_wide_row,
    write_salary_band_and_conversion_diagnostics_csv,
)
from scripts.excel_analysis.salary_plot_cohort_utils import (
    attach_cao_equal_weights,
    build_contract_year_latest_salary_calendar_panel,
    build_latest_increase_salary_year_panel,
    build_salary_year_latest_gap_panel,
    filter_long_by_governing_file_contract,
    filter_newest_file_overlap_salary_start_year,
    governing_file_keys_contract_cohort,
    latest_cao_active_file_table,
    snap_active_table_to_band_eligible_salary_files,
    weighted_boxplot_stats_for_bxp,
    weighted_percentile_nw,
    weighted_quantile,
)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
INPUT_CSV = "outputs/excel/new_results/extracted_data_salary.csv"
OUTPUT_FIG_DIR = "outputs/analysis/figures/salary/"
SALARY_PLOT_DROPPED_YEARS_CSV = "outputs/analysis/salary_plot_years_dropped.csv"


def reset_salary_plot_dropped_year_log() -> None:
    """Legacy no-op: plots no longer exclude years by minimum n."""

    return


def append_salary_plot_dropped_years(
    figure_key: str,
    view: str,
    grouped_with_count: pd.DataFrame,
) -> List[int]:
    """
    Legacy hook: year exclusion removed; returns an empty list.

    Args:
        figure_key: Unused stable identifier.
        view: Unused.
        grouped_with_count: Unused.

    Returns:
        Empty list.
    """
    return []


def write_salary_plot_dropped_years_csv(path: Optional[Path] = None) -> None:
    """
    Write header-only placeholder CSV (no rows dropped by minimum-n rule).

    Args:
        path: Output path; defaults to SALARY_PLOT_DROPPED_YEARS_CSV under project-relative ``outputs/analysis``.
    """
    out = path or Path(SALARY_PLOT_DROPPED_YEARS_CSV)
    out.parent.mkdir(parents=True, exist_ok=True)
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


def add_figure_excluded_years_footnote(fig: plt.Figure, excluded_years: List[int], max_show: int = 25) -> None:
    """Disabled: no minimum-n exclusion footnotes."""

    return


def add_figure_low_n_included_years_note(
    fig: plt.Figure,
    grouped_with_count: pd.DataFrame,
    *,
    year_col_label: str = "year",
    upper: int = 20,
    max_show: int = 25,
) -> None:
    """Disabled: no low-n inclusion notes."""

    return

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
        "ft_hours_weekly", "TTW", "is_entry",
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
        Copy with columns amount_monthly, conversion_ok, conversion_reason, analysis_monthly_floor_eur,
        analysis_monthly_band_ok, analysis_drop_reason_band (aligned with increase derivation / diagnostics).
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
    conv_reasons: List[str] = []
    for i in range(len(out)):
        amt = out["salary_amount"].iloc[i]
        unit = out["salary_unit"].iloc[i]
        sh = out["salary_hours_basis_ft_week"].iloc[i] if has_slot_h else np.nan
        rf = row_ft.iloc[i]
        m, ok, reason = normalize_salary_slot_to_monthly(amt, unit, sh, rf)
        amounts_m.append(m)
        oks.append(bool(ok))
        conv_reasons.append(str(reason) if reason else "")
    floor_arr, band_ok = compute_analysis_monthly_floor_and_band_ok(
        np.array(oks, dtype=bool),
        out["salary_start_date"],
        amounts_m,
    )
    amt_f = pd.to_numeric(pd.Series(amounts_m), errors="coerce").to_numpy(dtype=float)
    conv_ok = np.asarray(oks, dtype=bool)
    sd_series = pd.to_datetime(out["salary_start_date"], errors="coerce")
    nat_sd = sd_series.isna().to_numpy()
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    floor_fin = np.isfinite(floor_arr)
    amt_fin = np.isfinite(amt_f)
    drop_r = np.full(len(out), "", dtype=object)
    drop_r[conv_ok & nat_sd] = "missing_salary_date_for_floor"
    drop_r[conv_ok & ~nat_sd & amt_fin & (amt_f > cap)] = "above_analysis_cap"
    drop_r[conv_ok & ~nat_sd & amt_fin & floor_fin & (amt_f <= cap) & (amt_f < floor_arr)] = (
        "below_statutory_monthly_floor"
    )
    out["amount_monthly"] = amounts_m
    out["conversion_ok"] = oks
    out["conversion_reason"] = conv_reasons
    out["analysis_monthly_floor_eur"] = floor_arr
    out["analysis_monthly_band_ok"] = band_ok
    out["analysis_drop_reason_band"] = drop_r
    return out


def build_governed_band_eligible_slot_long(
    df_long: pd.DataFrame,
    enriched: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Long salary rows that are band-eligible and on the governing contract file per ``(CAO, contract cohort)``.

    Same filters as ``plot_salary_amount_monthly_band_eligible_by_contract_year`` uses for ``df_slot``. Used to
    snap latest-view active files and to avoid rebuilding the long frame when passed from ``main``.

    Args:
        df_long: Output of ``build_long_salary_df`` / ``build_long_salary_from_wide``.
        enriched: Optional precomputed ``enrich_long_salary_with_monthly_and_band(df_long)`` to avoid a second pass.

    Returns:
        Governed ``df_slot`` copy, or empty DataFrame if no qualifying rows or required columns missing.
    """
    if len(df_long) == 0:
        return pd.DataFrame()
    if enriched is None:
        enriched = enrich_long_salary_with_monthly_and_band(df_long)
    if "analysis_monthly_band_ok" not in enriched.columns:
        return pd.DataFrame()
    required = {"cao_number", "file_name", "ingangsdatum"}
    if not required.issubset(set(enriched.columns)):
        return pd.DataFrame()
    df_slot = enriched[
        enriched["analysis_monthly_band_ok"]
        & enriched["amount_monthly"].notna()
        & enriched["cao_number"].notna()
        & enriched["file_name"].notna()
    ].copy()
    if len(df_slot) == 0:
        return pd.DataFrame()
    df_slot["amount_monthly"] = pd.to_numeric(df_slot["amount_monthly"], errors="coerce")
    df_slot = df_slot[df_slot["amount_monthly"].notna()]
    df_slot["ingangsdatum"] = parse_cao_date_series(df_slot["ingangsdatum"], dayfirst=True)
    df_slot["contract_start_year"] = pd.to_datetime(df_slot["ingangsdatum"], errors="coerce").dt.year
    df_slot = df_slot[df_slot["contract_start_year"].notna()]
    df_slot = filter_long_by_governing_file_contract(df_slot)
    return df_slot.reset_index(drop=True)


def add_yearly_variance_layer(
    ax: plt.Axes,
    df: pd.DataFrame,
    year_col: str,
    value_col: str,
    color: Any,
    *,
    weight_col: Optional[str] = "cao_weight",
    percent_increase_scale: bool = False,
    hide_boxplot_fliers: bool = False,
) -> None:
    """
    Weighted box/whisker layer by year (``bxp`` with precomputed weighted quantiles) + weighted mean line.

    Args:
        ax: Matplotlib axis.
        df: Rows must include ``cao_weight`` when ``weight_col`` is set (CAO-equal weights). If ``weight_col``
            is missing from columns, unit weights are used.
        year_col: Cohort year column.
        value_col: Numeric value column.
        color: Mean line color.
        weight_col: Column with observation weights; if None or missing, all weights are 1.
        percent_increase_scale: If True, y-limits from weighted 5th/95th and box whiskers, bounded below at −50.
        hide_boxplot_fliers: If True, ``showfliers=False`` for ``bxp`` (always used for weighted boxes).
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
    wc = weight_col if weight_col and weight_col in d.columns else None
    if wc is None:
        d = d.copy()
        d["_unit_w"] = 1.0
        wc = "_unit_w"
    years = sorted(int(y) for y in d[year_col].unique())
    stats_for_bxp: List[Dict[str, Any]] = []
    mean_vals: List[float] = []
    for y in years:
        sub = d.loc[d[year_col] == y]
        xv = sub[value_col].to_numpy(dtype=float)
        wv = sub[wc].to_numpy(dtype=float)
        st = weighted_boxplot_stats_for_bxp(xv, wv)
        stats_for_bxp.append(
            {
                "med": st["med"],
                "q1": st["q1"],
                "q3": st["q3"],
                "whislo": st["whislo"],
                "whishi": st["whishi"],
                "fliers": [],
            }
        )
        okm = np.isfinite(xv) & np.isfinite(wv) & (wv > 0)
        if okm.any() and float(wv[okm].sum()) > 0:
            mean_vals.append(float(np.average(xv[okm], weights=wv[okm])))
        else:
            mean_vals.append(float("nan"))
    show_fliers = not (percent_increase_scale or hide_boxplot_fliers)
    ax.bxp(
        stats_for_bxp,
        positions=years,
        widths=0.6,
        patch_artist=True,
        showfliers=show_fliers,
        boxprops=dict(facecolor="lightgray", alpha=0.25),
        medianprops=dict(color="dimgray"),
        whiskerprops=dict(color="gray", alpha=0.6),
        capprops=dict(color="gray", alpha=0.6),
        flierprops=dict(marker=".", markersize=2, alpha=0.2),
    )
    ax.plot(years, mean_vals, color=color, marker="o", linewidth=2.2, label="Mean")
    enforce_integer_year_axis(ax, years)
    if percent_increase_scale:
        vals = d[value_col].to_numpy(dtype=float)
        ww = d[wc].to_numpy(dtype=float)
        mfin = np.isfinite(vals) & np.isfinite(ww) & (ww > 0)
        vals = vals[mfin]
        ww = ww[mfin]
        if len(vals) > 0:
            p5 = weighted_percentile_nw(vals, ww, 5.0)
            p95 = weighted_percentile_nw(vals, ww, 95.0)
            whisk_lo = min(st["whislo"] for st in stats_for_bxp if np.isfinite(st.get("whislo", np.nan)))
            whisk_hi = max(st["whishi"] for st in stats_for_bxp if np.isfinite(st.get("whishi", np.nan)))
            if not np.isfinite(whisk_lo):
                whisk_lo = float(np.min(vals))
            if not np.isfinite(whisk_hi):
                whisk_hi = float(np.max(vals))
            mmin = float(np.nanmin(mean_vals))
            mmax = float(np.nanmax(mean_vals))
            span = float(p95 - p5) if p95 > p5 else 1.0
            pad = max(0.05 * span, 0.12)
            bot_raw = min(p5, whisk_lo, mmin) - pad
            top_raw = max(p95, whisk_hi, mmax) + pad
            bot = max(bot_raw, -50.0)
            top_capped = min(top_raw, 100.0)
            top = max(top_capped, whisk_hi, mmax)
            if top <= bot:
                bot, top = -5.0, 25.0
            ax.set_ylim(bot, top)


def _twin_axis_only_cao_counts(
    ax1: plt.Axes,
    df_for_counts: pd.DataFrame,
    year_col: str,
    years: List[int],
) -> None:
    """
    Draw a light twin axis: one bar per year = distinct ``cao_number`` in ``df_for_counts`` for that year.

    Args:
        ax1: Primary axes.
        df_for_counts: Same frame as box/mean layer for each year.
        year_col: Cohort column.
        years: Sorted year ticks to annotate.
    """
    if "cao_number" not in df_for_counts.columns or not years:
        return
    ax2 = ax1.twinx()
    cao_list = [
        df_for_counts.loc[df_for_counts[year_col] == y, "cao_number"].dropna().nunique() for y in years
    ]
    ax2.bar(years, cao_list, alpha=0.1, color="steelblue", label="CAOs in year")
    for year, n in zip(years, cao_list):
        if n > 0:
            ax2.text(year, n, f"{int(n)}", ha="center", va="bottom", fontsize=7, color="steelblue", alpha=0.65)
    ax2.set_ylabel("Number of CAOs", fontsize=12, color="gray")
    ax2.tick_params(axis="y", labelcolor="gray")


# =============================================================================
# PLOTTING FUNCTIONS
# =============================================================================

def plot_ft_hours_by_contract_year(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot full-time hours by contract start year with CAO-equal weights and cohort-local CAO twin axis.

    Args:
        df: Wide format DataFrame
        output_dir: Directory to save plot
    """
    filename = "salary_ft_hours_by_contract_year.png"
    print(f"\nCreating figure: {filename}")

    df_plot = df.copy()

    if "contract_start_year" not in df_plot.columns or "ft_hours_weekly" not in df_plot.columns:
        print("  [INFO] Missing required columns; skipping figure")
        return

    df_plot = df_plot[df_plot["contract_start_year"].notna() & df_plot["ft_hours_weekly"].notna()].copy()
    if len(df_plot) == 0 or "cao_number" not in df_plot.columns:
        print("  [INFO] No data available; skipping figure")
        return

    df_plot["contract_start_year"] = pd.to_numeric(df_plot["contract_start_year"], errors="coerce").astype(int)
    df_plot["ft_hours_weekly"] = pd.to_numeric(df_plot["ft_hours_weekly"], errors="coerce")
    df_plot = df_plot[df_plot["ft_hours_weekly"].notna()]
    df_w = attach_cao_equal_weights(df_plot, "cao_number", "contract_start_year")
    years = sorted(df_w["contract_start_year"].unique().tolist())
    if not years:
        print("  [INFO] No years for ft_hours plot; skipping")
        return

    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(
        ax1,
        df_w,
        "contract_start_year",
        "ft_hours_weekly",
        main_color,
        weight_col="cao_weight",
        hide_boxplot_fliers=True,
    )
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Average full-time hours per week", fontsize=12)
    ax1.set_title("Average full-time weekly hours by contract start year", fontsize=14)
    ax1.grid(True, alpha=0.3)
    _twin_axis_only_cao_counts(ax1, df_w, "contract_start_year", years)
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def plot_salary_amount_monthly_band_eligible_by_salary_year(
    df: pd.DataFrame,
    df_long: pd.DataFrame,
    output_dir: Path,
    use_latest_cao_view: bool = False,
    df_latest_wide: Optional[pd.DataFrame] = None,
    df_slot_governed: Optional[pd.DataFrame] = None,
) -> None:
    """
    Band-eligible EUR/month: normal = salary **start** year (overlap + CAO weights); latest = **Salary year**
    active pay (**snapped** forward-fill file + dedup slot keys + CAO weights).

    Args:
        df: Full wide frame (for active salary construction in latest view).
        df_long: Long rows from full wide extract (normal view).
        output_dir: PNG directory.
        use_latest_cao_view: If True, x-axis label *Salary year*.
        df_latest_wide: Prebuilt forward-filled panel.
        df_slot_governed: For latest view: governed band-eligible long rows for snapping; if None, built from ``df_long``.
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_amount_monthly_eur_band_eligible_by_salary_year{suffix}.png"
    print(f"\nCreating figure: {filename}")

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
        active_t = latest_cao_active_file_table(df_wide_latest)
        slot_for_snap = (
            df_slot_governed.copy()
            if df_slot_governed is not None and len(df_slot_governed) > 0
            else build_governed_band_eligible_slot_long(df_long)
        )
        if len(slot_for_snap) > 0:
            active_t = snap_active_table_to_band_eligible_salary_files(active_t, slot_for_snap)
        df_active = build_salary_year_latest_gap_panel(df, active_t)
        if len(df_active) == 0:
            print("  [INFO] No active band-eligible salary rows for latest view; skipping")
            return
        df_w = attach_cao_equal_weights(df_active, "cao_number", "salary_year")
        year_col = "salary_year"
        x_label = "Salary year"
    else:
        if len(df_long) == 0:
            print("  [INFO] Long format DataFrame is empty; skipping figure")
            return
        enriched = enrich_long_salary_with_monthly_and_band(df_long)
        if "analysis_monthly_band_ok" not in enriched.columns:
            print("  [INFO] Enrichment failed; skipping figure")
            return
        df_filtered = enriched[
            enriched["analysis_monthly_band_ok"]
            & enriched["amount_monthly"].notna()
            & enriched["salary_start_year"].notna()
        ].copy()
        need_o = {"cao_number", "salary_start_year", "ingangsdatum", "file_name"}
        if need_o.issubset(df_filtered.columns):
            df_filtered = filter_newest_file_overlap_salary_start_year(df_filtered)
        df_filtered["amount_monthly"] = pd.to_numeric(df_filtered["amount_monthly"], errors="coerce")
        df_filtered = df_filtered[df_filtered["amount_monthly"].notna()]
        if len(df_filtered) == 0:
            print("  [INFO] No band-eligible salary rows; skipping figure")
            return
        df_w = attach_cao_equal_weights(df_filtered, "cao_number", "salary_start_year")
        year_col = "salary_start_year"
        x_label = "Salary start year"

    years_plot = sorted(df_w[year_col].dropna().unique().astype(int).tolist())
    if not years_plot:
        print("  [INFO] No years to plot for salary-by-year figure")
        return

    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(
        ax1,
        df_w,
        year_col,
        "amount_monthly",
        main_color,
        weight_col="cao_weight",
        hide_boxplot_fliers=True,
    )
    ax1.set_xlabel(x_label, fontsize=12)
    ax1.set_ylabel("Gross monthly EUR (normalized, band-eligible)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(
        f"Average salary (EUR/month, band-eligible: NL statutory min + analysis cap){title_suffix}",
        fontsize=13,
    )
    ax1.grid(True, alpha=0.3)
    _twin_axis_only_cao_counts(ax1, df_w, year_col, years_plot)
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {output_path}")
    if use_latest_cao_view:
        gc.collect()


def plot_salary_amount_monthly_band_eligible_by_contract_year(
    df: pd.DataFrame,
    df_long: pd.DataFrame,
    output_dir: Path,
    use_latest_cao_view: bool = False,
    df_latest_wide: Optional[pd.DataFrame] = None,
    df_slot_governed: Optional[pd.DataFrame] = None,
) -> None:
    """
    Band-eligible EUR/month by **contract start year** (regular) or **calendar year** (Latest CAO): governing file
    per (CAO, contract cohort) on long data; Latest view repeats **all** band-eligible slots on the **snapped**
    active file across each calendar year ``T`` (see ``snap_active_table_to_band_eligible_salary_files`` and
    ``build_contract_year_latest_salary_calendar_panel``).

    Args:
        df: Wide format DataFrame.
        df_long: Long-format salary rows.
        output_dir: PNG directory.
        use_latest_cao_view: Calendar-year x-axis with slot-level replication from active file.
        df_latest_wide: Optional forward-fill panel; built if None when latest view.
        df_slot_governed: Optional precomputed ``build_governed_band_eligible_slot_long``; if None, built from ``df_long``.
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_amount_monthly_eur_band_eligible_by_contract_year{suffix}.png"
    print(f"\nCreating figure: {filename}")

    if df_slot_governed is not None:
        df_slot = df_slot_governed.copy()
    else:
        if len(df_long) == 0:
            print("  [INFO] Long format DataFrame is empty; skipping figure")
            return
        df_slot = build_governed_band_eligible_slot_long(df_long)
    if len(df_slot) == 0:
        print("  [INFO] No governed band-eligible salary rows; skipping contract-year salary figure")
        return

    val_col = "amount_monthly"
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
        active_t = latest_cao_active_file_table(df_wide_latest)
        active_t = snap_active_table_to_band_eligible_salary_files(active_t, df_slot)
        df_w = build_contract_year_latest_salary_calendar_panel(df_slot, active_t)
        if len(df_w) == 0:
            print("  [INFO] No rows for latest contract-year calendar panel; skipping figure")
            return
        df_w = attach_cao_equal_weights(df_w, "cao_number", "calendar_year")
        year_col = "calendar_year"
        x_label = "Calendar year"
    else:
        df_w = attach_cao_equal_weights(df_slot, "cao_number", "contract_start_year")
        year_col = "contract_start_year"
        x_label = "Contract start year"

    years_ip = sorted(df_w[year_col].dropna().unique().astype(int).tolist())
    if not years_ip:
        return
    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(
        ax1,
        df_w,
        year_col,
        val_col,
        main_color,
        weight_col="cao_weight",
        hide_boxplot_fliers=True,
    )
    ax1.set_xlabel(x_label, fontsize=12)
    ax1.set_ylabel("Gross monthly EUR (normalized, band-eligible)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(
        f"Average salary (EUR/month, band-eligible: NL statutory min + analysis cap){title_suffix}",
        fontsize=13,
    )
    ax1.grid(True, alpha=0.3)
    _twin_axis_only_cao_counts(ax1, df_w, year_col, years_ip)
    plt.tight_layout(rect=[0, 0.10, 1, 1])
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {output_path}")
    if use_latest_cao_view:
        gc.collect()


def plot_increase_percent_by_contract_year(
    df: pd.DataFrame,
    df_long: pd.DataFrame,
    output_dir: Path,
    use_latest_cao_view: bool = False,
    increase_events: Optional[pd.DataFrame] = None,
) -> None:
    """
    Merged increase (prefer CSV) by **contract start year** with governing-file filter and CAO-equal weights.

    Args:
        df: Wide format DataFrame (for forward-fill horizon).
        df_long: Unused API compatibility.
        output_dir: PNG directory.
        use_latest_cao_view: Forward-fill per-CAO contract-mean increase across calendar years.
        increase_events: Optional pre-derived events.
    """
    suffix = "_latest_cao_view" if use_latest_cao_view else ""
    filename = f"salary_increase_percent_by_contract_year{suffix}.png"
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

    required_cols = {"cao_number", "file_name", "ingangsdatum", "increase_merged_pref_csv", "analysis_monthly_band_ok"}
    if not required_cols.issubset(set(events.columns)):
        print("  [INFO] Missing required merged-increase columns; skipping figure")
        return

    df_plot = events.copy()
    df_plot["increase_merged_pref_csv"] = pd.to_numeric(df_plot["increase_merged_pref_csv"], errors="coerce")
    df_plot["ingangsdatum"] = parse_cao_date_series(df_plot["ingangsdatum"], dayfirst=True)
    df_plot["contract_start_year"] = pd.to_datetime(df_plot["ingangsdatum"], errors="coerce").dt.year
    df_plot = df_plot[
        df_plot["analysis_monthly_band_ok"].fillna(False)
        & df_plot["increase_merged_pref_csv"].notna()
        & df_plot["contract_start_year"].notna()
        & df_plot["cao_number"].notna()
        & df_plot["file_name"].notna()
    ]
    if len(df_plot) == 0:
        print("  [INFO] No band-eligible merged increases; skipping contract-year increase plot")
        return
    gf = governing_file_keys_contract_cohort(df_plot)
    if len(gf) == 0:
        print("  [INFO] No governing file keys; skipping")
        return
    df_plot = df_plot.merge(gf, on=["cao_number", "contract_start_year", "file_name"], how="inner")
    if len(df_plot) == 0:
        print("  [INFO] No events after governing-file join; skipping")
        return

    val_col = "increase_merged_pref_csv"
    if use_latest_cao_view:
        df_w = attach_cao_equal_weights(df_plot, "cao_number", "contract_start_year")
        cy_rows: List[Dict[str, Any]] = []
        for (cao, cy), g in df_w.groupby(["cao_number", "contract_start_year"], dropna=False):
            cy_rows.append(
                {
                    "cao_number": cao,
                    "contract_start_year": cy,
                    "contract_mean_increase": float(
                        np.average(g[val_col].to_numpy(dtype=float), weights=g["cao_weight"].to_numpy(dtype=float))
                    ),
                }
            )
        cao_year_vals = pd.DataFrame(cy_rows)
        if len(cao_year_vals) == 0:
            print("  [INFO] No CAO-year increase means; skipping")
            return
        if "contract_start_year" in df.columns:
            global_max_year = int(pd.to_numeric(df["contract_start_year"], errors="coerce").dropna().max())
        else:
            global_max_year = int(pd.to_numeric(cao_year_vals["contract_start_year"], errors="coerce").max())
        ff_rows: List[Dict[str, Any]] = []
        for cao, grp in cao_year_vals.groupby("cao_number"):
            g = grp.sort_values("contract_start_year").reset_index(drop=True)
            years_l = g["contract_start_year"].astype(int).tolist()
            vals = g["contract_mean_increase"].astype(float).tolist()
            for i, y0 in enumerate(years_l):
                y1 = years_l[i + 1] - 1 if i + 1 < len(years_l) else global_max_year
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
        df_w = pd.DataFrame(ff_rows)
        df_w["cao_weight"] = 1.0
        val_col = "contract_mean_increase"
    else:
        df_w = attach_cao_equal_weights(df_plot, "cao_number", "contract_start_year")

    years_ip = sorted(df_w["contract_start_year"].dropna().unique().astype(int).tolist())
    if not years_ip:
        return
    fig, ax1 = plt.subplots(figsize=(10, 6))
    main_color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(
        ax1,
        df_w,
        "contract_start_year",
        val_col,
        main_color,
        weight_col="cao_weight",
        hide_boxplot_fliers=True,
    )
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Average increase (%)", fontsize=12)
    title_suffix = " (Latest CAO View)" if use_latest_cao_view else ""
    ax1.set_title(f"Average increase (merged - prefer CSV) by contract start year{title_suffix}", fontsize=14)
    ax1.set_ylim(-4, 12)
    ax1.grid(True, alpha=0.3)
    _twin_axis_only_cao_counts(ax1, df_w, "contract_start_year", years_ip)
    plt.tight_layout(rect=[0, 0.10, 1, 1])
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {output_path}")


def plot_boolean_shares_by_contract_year(df: pd.DataFrame, output_dir: Path,
                                         use_latest_cao_view: bool = False,
                                         df_latest_wide: Optional[pd.DataFrame] = None) -> None:
    """
    Plot share of rows with boolean features by contract start year.

    Cohort-local CAO count on twin axis only (no row counts).

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
    
    # Variables to plot (use plotted frame so latest forward-fill columns match)
    bool_vars = []
    if "TTW" in df_plot.columns:
        bool_vars.append("TTW")
    if "is_entry" in df_plot.columns:
        bool_vars.append("is_entry")
    
    if len(bool_vars) == 0:
        print("  [INFO] No boolean variables available; skipping figure")
        return
    
    # Collect plot data (all contract years with ≥1 row; no MIN_OBS_PER_YEAR filter)
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
    
    cao_counts: Dict[Any, int] = {}
    if "cao_number" in df_plot.columns:
        all_years = set()
        for shares in plot_data.values():
            all_years.update(shares.index)
        csy = pd.to_numeric(df_plot["contract_start_year"], errors="coerce")
        for year in all_years:
            yv = int(year) if pd.notna(year) else year
            year_data = df_plot.loc[csy == yv]
            cao_counts[year] = year_data["cao_number"].dropna().nunique() if len(year_data) > 0 else 0
    
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
    
    if cao_counts:
        ax2 = ax1.twinx()
        years = sorted(all_years)
        cao_list = [cao_counts.get(y, 0) for y in years]
        ax2.bar(years, cao_list, alpha=0.1, color="steelblue", label="CAOs in year")
        for year, n in zip(years, cao_list):
            if n > 0:
                ax2.text(year, n, f"{int(n)}", ha="center", va="bottom", fontsize=7, color="steelblue", alpha=0.6)
        ax2.set_ylabel("Number of CAOs", fontsize=12, color="gray")
        ax2.tick_params(axis="y", labelcolor="gray")
    
    plt.tight_layout()
    
    # Save plot
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved: {output_path}")


def plot_salary_points_per_row_by_year(
    df: pd.DataFrame,
    df_long: pd.DataFrame,
    output_dir: Path,
    enriched_long: Optional[pd.DataFrame] = None,
) -> None:
    """
    Plot average number of **band-eligible** salary slots per wide row by contract start year.

    Counts match ``enrich_long_salary_with_monthly_and_band`` (NL floor + analysis cap), not raw positive amounts.

    Args:
        df: Wide format DataFrame (same row order as long build; ``row_id`` = ``iloc`` position).
        df_long: Long salary rows from ``build_long_salary_df`` (includes ``row_id``).
        output_dir: Directory to save plot
        enriched_long: Pre-enriched long frame (same as ``enrich_long_salary_with_monthly_and_band``); if None, enriched once here.
    """
    filename = "salary_points_per_row_by_year.png"
    print(f"\nCreating figure: {filename}")

    df_plot = df.copy()

    if "contract_start_year" not in df_plot.columns:
        print("  [INFO] contract_start_year column not found; skipping figure")
        return

    print("  Computing band-eligible salary points per row...")
    if enriched_long is not None and len(enriched_long):
        enriched = enriched_long
    else:
        enriched = enrich_long_salary_with_monthly_and_band(df_long) if len(df_long) else df_long
    counts = count_band_eligible_slots_per_wide_row(enriched)
    n_band = np.zeros(len(df_plot), dtype=np.float64)
    for rid, c in counts.items():
        ri = int(rid)
        if 0 <= ri < len(n_band):
            n_band[ri] = float(c)
    df_plot["n_salary_points_per_row"] = n_band
    
    # Filter to rows with valid contract_start_year and n_salary_points_per_row
    df_filtered = df_plot[
        df_plot["contract_start_year"].notna() & 
        df_plot["n_salary_points_per_row"].notna()
    ].copy()
    
    if len(df_filtered) == 0:
        print("  [INFO] No data after filtering; skipping figure")
        return
    
    if "cao_number" not in df_filtered.columns:
        print("  [INFO] Missing cao_number; skipping points-per-row plot")
        return
    df_w = attach_cao_equal_weights(df_filtered, "cao_number", "contract_start_year")
    years = sorted(df_w["contract_start_year"].dropna().unique().astype(int).tolist())
    if not years:
        print("  [INFO] No years for points-per-row plot")
        return
    mean_pts: List[float] = []
    med_pts: List[float] = []
    for y in years:
        sub = df_w.loc[df_w["contract_start_year"] == y]
        xv = sub["n_salary_points_per_row"].to_numpy(dtype=float)
        wv = sub["cao_weight"].to_numpy(dtype=float)
        mean_pts.append(float(np.average(xv, weights=wv)) if np.nansum(wv) > 0 else float("nan"))
        med_pts.append(float(weighted_quantile(xv, wv, 0.5)))

    fig, ax1 = plt.subplots(figsize=(10, 6))
    colors = get_plot_color_cycle(2)
    ax1.plot(years, mean_pts, marker="o", label="Mean (CAO-equal)", linewidth=2, markersize=6, color=colors[0])
    ax1.plot(years, med_pts, marker="s", label="Median (weighted)", linewidth=2, markersize=6, linestyle="--", color=colors[1])
    enforce_integer_year_axis(ax1, years)
    ax1.set_xlabel("Contract start year", fontsize=12)
    ax1.set_ylabel("Band-eligible salary points per row", fontsize=12)
    ax1.set_title("Average number of band-eligible salary points per row over time", fontsize=14)
    ax1.legend(fontsize=10, loc="best")
    ax1.grid(True, alpha=0.3)
    _twin_axis_only_cao_counts(ax1, df_w, "contract_start_year", years)
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    output_path = output_dir / filename
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {output_path}")
    print("  Summary (first 5 years):")
    for i, y in enumerate(years[:5]):
        print(f"    Year {y}: mean={mean_pts[i]:.2f}, median={med_pts[i]:.1f}")


def _plot_single_increase_series(events: pd.DataFrame, output_dir: Path, column: str, filename: str, label: str) -> None:
    """
    One increase series by salary start year: overlap resolution, CAO-equal weights, weighted ``bxp``.

    Uses fixed y-axis range for comparability across single-series increase charts.
    """
    req = {"analysis_monthly_band_ok", "cao_number", "salary_start_year", "ingangsdatum", "file_name"}
    if not req.issubset(events.columns):
        return
    d = events[
        events[column].notna()
        & events["salary_start_year"].notna()
        & events["analysis_monthly_band_ok"].fillna(False)
    ].copy()
    if len(d) == 0:
        return
    d["salary_start_year"] = pd.to_numeric(d["salary_start_year"], errors="coerce")
    d[column] = pd.to_numeric(d[column], errors="coerce")
    d = d[d["salary_start_year"].notna() & d[column].notna()]
    if len(d) == 0:
        return
    d = filter_newest_file_overlap_salary_start_year(d)
    d = attach_cao_equal_weights(d, "cao_number", "salary_start_year")
    fig, ax = plt.subplots(figsize=(11, 6))
    color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(
        ax, d, "salary_start_year", column, color, weight_col="cao_weight", hide_boxplot_fliers=True
    )
    ax.set_title(f"{label} by salary start year", fontsize=12)
    ax.set_xlabel("Salary start year", fontsize=12)
    ax.set_ylabel("Average increase (%)", fontsize=12)
    ax.set_ylim(-4, 12)
    ax.grid(True, alpha=0.3)
    years_ax = sorted(d["salary_start_year"].dropna().unique().astype(int).tolist())
    _twin_axis_only_cao_counts(ax, d, "salary_start_year", years_ax)
    plt.tight_layout()
    plt.savefig(output_dir / filename, dpi=300, bbox_inches="tight")
    plt.close()


def _plot_single_increase_series_salary_year_latest(
    events: pd.DataFrame,
    df_latest_wide: pd.DataFrame,
    output_dir: Path,
    column: str,
    filename: str,
    label: str,
    df_slot_governed: Optional[pd.DataFrame] = None,
    df_long: Optional[pd.DataFrame] = None,
) -> None:
    """
    Latest CAO view: x-axis **Salary year**, ``bxp`` layer with CAO-equal weights, 0% when active file has no event in ``T``.

    Uses ``snap_active_table_to_band_eligible_salary_files`` when governed slot long rows are available so the
    effective contract file matches the salary latest figures.

    Args:
        events: Derived increase events (``derive_salary_increase_series``).
        df_latest_wide: Forward-filled wide panel (``contract_start_year`` = calendar ``T``).
        output_dir: Directory for PNG output.
        column: Increase column to plot (e.g. ``increase_merged_pref_csv``).
        filename: Output basename.
        label: Title / legend label fragment.
        df_slot_governed: Governed band-eligible long salary rows; if None and ``df_long`` is set, built via helper.
        df_long: Used only when ``df_slot_governed`` is None to build slots for snapping.

    Returns:
        None; writes ``filename`` under ``output_dir``.
    """
    if len(df_latest_wide) == 0:
        return
    active_t = latest_cao_active_file_table(df_latest_wide)
    slot_for_snap = df_slot_governed
    if (slot_for_snap is None or len(slot_for_snap) == 0) and df_long is not None and len(df_long) > 0:
        slot_for_snap = build_governed_band_eligible_slot_long(df_long)
    if slot_for_snap is not None and len(slot_for_snap) > 0:
        active_t = snap_active_table_to_band_eligible_salary_files(active_t, slot_for_snap)
    panel = build_latest_increase_salary_year_panel(events, active_t, value_col=column)
    if len(panel) == 0:
        return
    d = attach_cao_equal_weights(panel, "cao_number", "salary_year")
    fig, ax = plt.subplots(figsize=(11, 6))
    color = get_plot_color_cycle(1)[0]
    add_yearly_variance_layer(ax, d, "salary_year", column, color, weight_col="cao_weight", hide_boxplot_fliers=True)
    ax.set_title(f"{label} by salary year (Latest CAO View)", fontsize=12)
    ax.set_xlabel("Salary year", fontsize=12)
    ax.set_ylabel("Average increase (%)", fontsize=12)
    ax.set_ylim(-4, 12)
    ax.grid(True, alpha=0.3)
    years_ax = sorted(d["salary_year"].dropna().unique().astype(int).tolist())
    _twin_axis_only_cao_counts(ax, d, "salary_year", years_ax)
    plt.tight_layout()
    plt.savefig(output_dir / filename, dpi=300, bbox_inches="tight")
    plt.close()


def plot_increase_series_comparison(events: pd.DataFrame, output_dir: Path) -> None:
    """
    Plot diff-only, merged-pref-csv and csv-only **weighted** yearly means (CAO-equal within year).
    """
    req = {"analysis_monthly_band_ok", "cao_number", "salary_start_year", "ingangsdatum", "file_name"}
    if not req.issubset(events.columns):
        return
    cols = ["increase_diff_only", "increase_merged_pref_csv", "increase_csv_only"]
    labels = ["Diff only", "Merged (prefer CSV)", "CSV only"]
    colors = get_plot_color_cycle(3)
    fig, ax = plt.subplots(figsize=(12, 6))
    years_union: List[int] = []
    merged_for_twin: Optional[pd.DataFrame] = None
    for col, lab, color in zip(cols, labels, colors):
        d = events[
            events[col].notna()
            & events["salary_start_year"].notna()
            & events["analysis_monthly_band_ok"].fillna(False)
        ].copy()
        if len(d) == 0:
            continue
        d["salary_start_year"] = pd.to_numeric(d["salary_start_year"], errors="coerce")
        d[col] = pd.to_numeric(d[col], errors="coerce")
        d = d[d["salary_start_year"].notna() & d[col].notna()]
        if len(d) == 0:
            continue
        d = filter_newest_file_overlap_salary_start_year(d)
        d = attach_cao_equal_weights(d, "cao_number", "salary_start_year")
        wmean_by_y: List[Tuple[int, float]] = []
        for y in sorted(d["salary_start_year"].unique()):
            sub = d.loc[d["salary_start_year"] == y]
            xv = sub[col].to_numpy(dtype=float)
            wv = sub["cao_weight"].to_numpy(dtype=float)
            if np.nansum(wv) <= 0:
                continue
            wmean_by_y.append((int(y), float(np.average(xv, weights=wv))))
        if not wmean_by_y:
            continue
        ys, vs = zip(*wmean_by_y)
        ax.plot(list(ys), list(vs), marker="o", linewidth=2.2, color=color, label=lab)
        years_union.extend(int(y) for y in ys)
        if col == "increase_merged_pref_csv":
            merged_for_twin = d.copy()
    if not years_union:
        plt.close()
        return
    enforce_integer_year_axis(ax, years_union)
    if merged_for_twin is not None and len(merged_for_twin):
        yticks = sorted(set(int(y) for y in years_union))
        _twin_axis_only_cao_counts(ax, merged_for_twin, "salary_start_year", yticks)
    ax.set_title("Average general wage increase comparison by salary start year", fontsize=12)
    ax.set_xlabel("Salary start year", fontsize=12)
    ax.set_ylabel("Average increase (%)", fontsize=12)
    ax.set_ylim(0, 6)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=10, loc="best")
    plt.tight_layout()
    plt.savefig(output_dir / "salary_increase_series_comparison_by_year.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_shift_by_new_file_year(events: pd.DataFrame, output_dir: Path) -> None:
    """
    Yearly **weighted** average shift in file-level mean increases between consecutive files within each CAO.
    """
    required_cols = {"cao_number", "file_name", "ingangsdatum", "analysis_monthly_band_ok"}
    if not required_cols.issubset(set(events.columns)):
        return
    d = events[events["analysis_monthly_band_ok"].fillna(False)].copy()
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
            year = int(new_row["ingangsdatum"].year) if pd.notna(new_row["ingangsdatum"]) else np.nan
            rows.append(
                {
                    "cao_number": cao,
                    "year_new_file": year,
                    "_ing_new": new_row["ingangsdatum"],
                    "shift_diff_only": new_row["increase_diff_only"] - prev_row["increase_diff_only"],
                    "shift_merged_pref_csv": new_row["increase_merged_pref_csv"] - prev_row["increase_merged_pref_csv"],
                    "shift_csv_only": new_row["increase_csv_only"] - prev_row["increase_csv_only"],
                }
            )
    shifts = pd.DataFrame(rows)
    if len(shifts) == 0:
        return
    shifts = shifts.sort_values(["cao_number", "year_new_file", "_ing_new"], ascending=[True, True, False])
    shifts = shifts.drop_duplicates(["cao_number", "year_new_file"], keep="first")
    shifts = shifts.drop(columns=["_ing_new"], errors="ignore")

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
        s = attach_cao_equal_weights(s, "cao_number", "year_new_file")
        pts: List[Tuple[int, float]] = []
        for y in sorted(s["year_new_file"].unique()):
            sub = s.loc[s["year_new_file"] == y]
            xv = sub[col].to_numpy(dtype=float)
            wv = sub["cao_weight"].to_numpy(dtype=float)
            if np.nansum(wv) <= 0:
                continue
            pts.append((int(y), float(np.average(xv, weights=wv))))
        if not pts:
            continue
        ys, vs = zip(*pts)
        ax.plot(list(ys), list(vs), marker="o", color=color, linewidth=2, label=label)
        years_union.extend(int(y) for y in ys)
    if not years_union:
        plt.close()
        return
    enforce_integer_year_axis(ax, years_union)
    sh_merged = shifts[shifts["shift_merged_pref_csv"].notna() & shifts["year_new_file"].notna()].copy()
    if len(sh_merged):
        sh_merged = attach_cao_equal_weights(sh_merged, "cao_number", "year_new_file")
        yticks = sorted(set(int(y) for y in years_union))
        _twin_axis_only_cao_counts(ax, sh_merged, "year_new_file", yticks)
    ax.set_title("Average increase shift by new contract (contract start year)", fontsize=14)
    ax.set_xlabel("Contract start year", fontsize=12)
    ax.set_ylabel("Average shift (pp)", fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(output_dir / "salary_increase_shift_by_new_file_year.png", dpi=300, bbox_inches="tight")
    plt.close()


def plot_spaghetti_selected_caos(events: pd.DataFrame, output_dir: Path) -> None:
    """
    Selected CAO trajectories plus **CAO-equal weighted** grand mean of merged increase by salary start year.
    """
    req = {"analysis_monthly_band_ok", "cao_number", "salary_start_year", "ingangsdatum", "file_name", "increase_merged_pref_csv"}
    if not req.issubset(events.columns):
        return
    d = events[
        events["increase_merged_pref_csv"].notna()
        & events["salary_start_year"].notna()
        & events["analysis_monthly_band_ok"].fillna(False)
    ].copy()
    if len(d) == 0:
        return
    d["salary_start_year"] = pd.to_numeric(d["salary_start_year"], errors="coerce")
    d = d[d["salary_start_year"].notna()]
    if len(d) == 0:
        return
    d = filter_newest_file_overlap_salary_start_year(d)
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
    grand_w = attach_cao_equal_weights(dedup, "cao_number", "salary_start_year")
    g_years = sorted(grand_w["salary_start_year"].unique())
    grand_vals: List[float] = []
    for y in g_years:
        sub = grand_w.loc[grand_w["salary_start_year"] == y]
        xv = sub["increase_merged_pref_csv"].to_numpy(dtype=float)
        wv = sub["cao_weight"].to_numpy(dtype=float)
        grand_vals.append(float(np.average(xv, weights=wv)) if np.nansum(wv) > 0 else float("nan"))
    ax.plot(
        [int(y) for y in g_years],
        grand_vals,
        color="black",
        linewidth=3,
        label="Mean (all CAOs, band-eligible, weighted)",
    )
    years_axis = sorted(set(int(y) for y in g_years) | set(selected["salary_start_year"].astype(int).unique()))
    enforce_integer_year_axis(ax, years_axis if years_axis else [int(y) for y in g_years])
    twin_years = sorted(int(y) for y in grand_w["salary_start_year"].dropna().unique())
    if twin_years:
        _twin_axis_only_cao_counts(ax, grand_w, "salary_start_year", twin_years)
    ax.set_title(
        "Selected CAO salary-increase trajectories (top/bottom yearly union)\n"
        "Thin colored lines: highlighted CAOs; black line: CAO-equal weighted mean (merged series)",
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
        df = pd.read_csv(INPUT_CSV, sep=';', encoding="utf-8", low_memory=False)
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

    # Consolidate blocks after many column updates (avoids PerformanceWarning on new columns).
    df = df.copy()

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
    
    # Build long format DataFrame
    print("\nBuilding long format DataFrame...")
    try:
        df_long = build_long_salary_df(df)
        print(f"  Long format: {len(df_long)} rows")
        log_memory("long_regular", df_long)
    except Exception as e:
        print(f"  Warning: Error building long format: {e}")
        df_long = pd.DataFrame()

    df_long_enriched = pd.DataFrame()
    if len(df_long) > 0:
        print("\nEnriching long salary (monthly EUR + band flags; shared by diagnostics, plots, governed slots)…")
        df_long_enriched = enrich_long_salary_with_monthly_and_band(df_long)
        log_memory("long_enriched", df_long_enriched)

    try:
        diag_path = write_salary_band_and_conversion_diagnostics_csv(df, df_long, enriched_long=df_long_enriched)
        print(f"\nWrote salary band/conversion diagnostics: {diag_path}")
    except Exception as e:
        print(f"  Warning: Could not write salary_band_and_conversion_diagnostics.csv: {e}")

    print("\nBuilding governed band-eligible slot long (contract cohort + latest snap)...")
    df_slot_governed = build_governed_band_eligible_slot_long(df_long, enriched=df_long_enriched if len(df_long_enriched) else None)
    print(f"  Governed slot rows: {len(df_slot_governed)}")

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
        plot_ft_hours_by_contract_year(df, output_dir)
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
        plot_salary_amount_monthly_band_eligible_by_contract_year(
            df, df_long, output_dir, use_latest_cao_view=False, df_slot_governed=df_slot_governed
        )
    except Exception as e:
        print(f"  ERROR in salary amount monthly band-eligible (contract year) plot: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        plot_increase_percent_by_contract_year(
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
        plot_salary_points_per_row_by_year(
            df, df_long, output_dir, enriched_long=df_long_enriched if len(df_long_enriched) else None
        )
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
        if len(df_latest_wide):
            _plot_single_increase_series_salary_year_latest(
                increase_events,
                df_latest_wide,
                output_dir,
                "increase_merged_pref_csv",
                "salary_increase_merged_pref_csv_by_salary_year_latest_cao_view.png",
                "Merged increase (prefer CSV)",
                df_slot_governed=df_slot_governed,
                df_long=df_long,
            )
            _plot_single_increase_series_salary_year_latest(
                increase_events,
                df_latest_wide,
                output_dir,
                "increase_diff_only",
                "salary_increase_diff_only_by_salary_year_latest_cao_view.png",
                "Derived increase (diff only)",
                df_slot_governed=df_slot_governed,
                df_long=df_long,
            )
            _plot_single_increase_series_salary_year_latest(
                increase_events,
                df_latest_wide,
                output_dir,
                "increase_csv_only",
                "salary_increase_csv_only_by_salary_year_latest_cao_view.png",
                "CSV-reported increase",
                df_slot_governed=df_slot_governed,
                df_long=df_long,
            )
    except Exception as e:
        print(f"  ERROR in derived increase plots: {e}")
        import traceback
        traceback.print_exc()
    
    # Generate Latest CAO View plots
    print("\n" + "="*80)
    print("Generating Latest CAO View plots...")
    print("="*80)
    
    try:
        plot_salary_amount_monthly_band_eligible_by_salary_year(
            df,
            df_long,
            output_dir,
            use_latest_cao_view=True,
            df_latest_wide=df_latest_wide,
            df_slot_governed=df_slot_governed,
        )
    except Exception as e:
        print(f"  ERROR in salary amount monthly band-eligible plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()

    try:
        plot_salary_amount_monthly_band_eligible_by_contract_year(
            df,
            df_long,
            output_dir,
            use_latest_cao_view=True,
            df_latest_wide=df_latest_wide,
            df_slot_governed=df_slot_governed,
        )
    except Exception as e:
        print(f"  ERROR in salary amount monthly band-eligible (contract year) plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()
    
    try:
        plot_increase_percent_by_contract_year(
            df, df_long, output_dir, use_latest_cao_view=True, increase_events=increase_events
        )
    except Exception as e:
        print(f"  ERROR in increase percent plot (latest CAO view): {e}")
        import traceback
        traceback.print_exc()
    finally:
        gc.collect()
    
    try:
        plot_boolean_shares_by_contract_year(df, output_dir, use_latest_cao_view=True, df_latest_wide=df_latest_wide)
    except Exception as e:
        print(f"  ERROR in boolean shares plot (latest CAO view): {e}")
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

