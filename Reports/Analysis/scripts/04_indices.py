"""
04_indices.py: Generates Stage 2 Generosity Indices figures, tables, and macros.

Inputs:
    - indices/out/all_indices_panel_monthly.csv   (document-grain panel; used to build a true
      COMBINED overall_z monthly series -- panel_aggregates_monthly.csv only ships the
      magnitude-only overall_numeric_z aggregate, see indices/build_panel_monthly.py)
    - indices/out/mw_indices.csv
    - indices/out/composite_index.csv
    - indices/out/statutory_index.csv
    - indices/out/factor_loadings_overall.csv
    - indices/out/coverage_vs_z_correlations.csv
    - indices/out/statutory_response_results.csv
    - indices/review/all_open_cant_tells.csv
    - indices/out/statutory_fingerprint_suspects.csv

Outputs:
    - Reports/Analysis/figures/indices/indices_overall_z_trend.png
    - Reports/Analysis/figures/indices/indices_topic_distributions.png
    - Reports/Analysis/figures/indices/indices_wage_ladder_vs_wml.png
    - Reports/Analysis/tables/tab_indices_topline.tex
    - Reports/Analysis/tables/tab_indices_top_bottom_caos.tex
    - Reports/Analysis/tables/tab_indices_statutory_response.tex
    - Reports/Analysis/tables/tab_indices_pay_correlations.tex
    - Reports/Analysis/tables/tab_indices_factor_loadings.tex
    - Emits macros into Reports/Analysis/tables/macros.tex
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import common configuration
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from common import (
    REPO_ROOT,
    ANALYSIS_DIR,
    FIGURES_DIR,
    TABLES_DIR,
    emit_macro,
    emit_table,
    save_macros,
    set_macro,
    setup_matplotlib,
)
# common.py inserts CAOS_REPO_ROOT onto sys.path as an import-time side effect.
from scripts.excel_analysis.analysis_utils import enforce_integer_year_axis

INDICES_OUT = REPO_ROOT / "indices" / "out"
INDICES_FIG_DIR = FIGURES_DIR / "indices"
INDICES_FIG_DIR.mkdir(parents=True, exist_ok=True)

TOPICS = [
    ("wage", "Wage Scale"),
    ("leave", "Leave Provisions"),
    ("absence", "Sickness & Care"),
    ("pension", "Pensions"),
    ("overtime", "Overtime"),
    ("bonus", "Bonuses"),
    ("fringe", "Fringe Benefits"),
    ("training", "Training"),
    ("contract", "Contract Types"),
    ("term", "Termination"),
    ("homeoffice", "Home Office"),
    ("safety", "Safety & Wellbeing"),
    ("childcare", "Childcare"),
]

def _topic_column_candidates(t_key: str) -> list[str]:
    return [
        f"{t_key}_z",               # Dual-track combined headline (leave, absence, term, etc.)
        f"{t_key}_median_z",        # Wage headline
        f"{t_key}_coverage_z",      # Single-track coverage headline (safety, childcare)
        f"{t_key}_numeric_z",       # Fallback magnitude track
    ]

def resolve_topic_column(t_key: str, df: pd.DataFrame) -> str | None:
    """Resolve the published headline z-column for a topic in composite_index.csv."""
    for c in _topic_column_candidates(t_key):
        if c in df.columns:
            return c
    return None

def resolve_topic_index(t_key: str, index) -> str | None:
    """Same resolution as resolve_topic_column, but against a row index (e.g. factor
    loadings keyed by z-column name) instead of DataFrame columns."""
    for c in _topic_column_candidates(t_key):
        if c in index:
            return c
    return None

def main():
    print("=== Running 04_indices.py ===")
    setup_matplotlib()
    
    # 1. Load data
    panel_monthly_path = INDICES_OUT / "all_indices_panel_monthly.csv"
    mw_path = INDICES_OUT / "mw_indices.csv"
    comp_path = INDICES_OUT / "composite_index.csv"
    stat_path = INDICES_OUT / "statutory_index.csv"

    print(f"Loading document-grain monthly panel: {panel_monthly_path}")
    df_panel_doc = pd.read_csv(
        panel_monthly_path, sep=";", low_memory=False,
        usecols=["cao_number", "month", "overall_z", "n_caos_in_month"],
    )

    print(f"Loading wage indices: {mw_path}")
    df_mw = pd.read_csv(mw_path, sep=";", low_memory=False)

    print(f"Loading composite index: {comp_path}")
    df_comp = pd.read_csv(comp_path, sep=";", low_memory=False)

    # Build a TRUE monthly COMBINED (magnitude + coverage) overall_z series.
    # indices/out/panel_aggregates_monthly.csv only ships the magnitude-only
    # overall_numeric_z aggregate (verified against indices/build_panel_monthly.py) -- using it
    # here would silently plot "how much" and label it "combined" (overall_z), which is exactly
    # the {t}_z vs {t}_numeric_z confusion indices/NAMING.md warns readers about. Instead,
    # aggregate the real per-document overall_z from the document-grain monthly panel.
    df_statutory_row = df_panel_doc[df_panel_doc["cao_number"] == "STATUTORY"].copy()
    df_cao_rows = df_panel_doc[df_panel_doc["cao_number"] != "STATUTORY"].dropna(subset=["overall_z"])

    monthly_combined = df_cao_rows.groupby("month").agg(
        overall_mean=("overall_z", "mean"),
        overall_p10=("overall_z", lambda s: s.quantile(0.10)),
        overall_p50=("overall_z", "median"),
        overall_p90=("overall_z", lambda s: s.quantile(0.90)),
        n_caos=("cao_number", "nunique"),
    ).reset_index()

    statutory_by_month = df_statutory_row.set_index("month")["overall_z"]
    monthly_combined["statutory_overall_z"] = monthly_combined["month"].map(statutory_by_month)

    # Mature-period filter: months with at least 30 active CAOs (same threshold as before,
    # now applied to the combined-z series and stated explicitly in the report text).
    MATURE_MIN_CAOS = 30
    df_panel_mature = monthly_combined[monthly_combined["n_caos"] >= MATURE_MIN_CAOS].copy()

    # Figure 1: Monthly Overall Generosity Trend vs Statutory Floor
    print("Generating Figure: indices_overall_z_trend.png...")
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Parse month dates
    dates = pd.to_datetime(df_panel_mature["month"])

    # Plot p10 - p90 band
    ax1.fill_between(
        dates,
        df_panel_mature["overall_p10"],
        df_panel_mature["overall_p90"],
        color="#2E86AB",
        alpha=0.15,
        label="10th–90th Percentile Band",
    )

    # Plot mean and median (both from the real combined overall_z, not the magnitude-only track)
    ax1.plot(
        dates,
        df_panel_mature["overall_mean"],
        color="#2E86AB",
        linewidth=2.5,
        label="Mean Combined Generosity (overall_z)",
    )
    ax1.plot(
        dates,
        df_panel_mature["overall_p50"],
        color="#2E86AB",
        linestyle="--",
        linewidth=1.8,
        label="Median Generosity (p50)",
    )

    # Plot statutory baseline (the STATUTORY pseudo-CAO's own combined overall_z)
    if "statutory_overall_z" in df_panel_mature.columns:
        ax1.plot(
            dates,
            df_panel_mature["statutory_overall_z"],
            color="#D9534F",
            linewidth=2.0,
            linestyle="-.",
            label="Statutory Benchmark (Pseudo-CAO, combined)",
        )

    ax1.set_xlabel("Month", fontsize=12)
    ax1.set_ylabel("Standardized Generosity Score (z)", fontsize=12)
    ax1.set_title("Overall CAO Generosity Trend over Time (Combined: Magnitude + Coverage)", fontsize=14)
    ax1.grid(True, alpha=0.3)

    # Add second axis for active CAO count
    ax2 = ax1.twinx()
    ax2.plot(dates, df_panel_mature["n_caos"], color="gray", alpha=0.35, linestyle=":", label="Active CAOs")
    ax2.set_ylabel("Number of Active CAOs", color="gray", fontsize=10)
    ax2.tick_params(axis="y", labelcolor="gray")
    
    # Legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", framealpha=0.9, fontsize=10)
    
    fig.tight_layout()
    trend_out = INDICES_FIG_DIR / "indices_overall_z_trend.png"
    plt.savefig(trend_out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {trend_out}")
    
    # Figure 2: Wage Ladder vs Statutory Minimum Wage (WML)
    print("Generating Figure: indices_wage_ladder_vs_wml.png...")
    fig, ax = plt.subplots(figsize=(11, 6))
    
    # Group mw by year. Some years at the edges of the sample (e.g. a handful of CAOs with
    # pre-agreed 2027 scales) have very few rows and give a noisy median -- restrict to years
    # with at least MIN_WAGE_YEAR_ROWS observations so neither the chart nor the "latest"
    # macros are driven by a thin tail.
    MIN_WAGE_YEAR_ROWS = 20
    year_row_counts = df_mw["year"].value_counts()
    mw_year_all = df_mw.groupby("year").agg({
        "mw_low": "median",
        "mw_median": "median",
        "mw_high": "median",
        "wml_month": "first",
    }).dropna().sort_index()
    mature_years = year_row_counts[year_row_counts >= MIN_WAGE_YEAR_ROWS].index
    mw_year = mw_year_all[mw_year_all.index.isin(mature_years)]

    years = mw_year.index.astype(int)
    wage_year_min, wage_year_max = int(years.min()), int(years.max())
    ax.plot(years, mw_year["mw_high"], marker="^", color="#2B4C7E", label="CAO Top Wage (p90 median)", linewidth=2)
    ax.plot(years, mw_year["mw_median"], marker="o", color="#2E86AB", label="CAO Median Wage (median)", linewidth=2.5)
    ax.plot(years, mw_year["mw_low"], marker="v", color="#48CAE4", label="CAO Scale Floor (p10 median)", linewidth=2)
    ax.plot(years, mw_year["wml_month"], marker="s", color="#D9534F", linestyle="--", label="Statutory Minimum Wage (WML)", linewidth=2.2)

    ax.set_xlabel("Calendar Year", fontsize=12)
    ax.set_ylabel("Gross Monthly Wage (EUR)", fontsize=12)
    ax.set_title(f"CAO Wage Scale Ladder vs. Statutory Minimum Wage ({wage_year_min}–{wage_year_max})", fontsize=14)
    enforce_integer_year_axis(ax, [int(y) for y in years])
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", framealpha=0.9, fontsize=10)
    
    fig.tight_layout()
    wage_out = INDICES_FIG_DIR / "indices_wage_ladder_vs_wml.png"
    plt.savefig(wage_out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {wage_out}")
    
    # Figure 3: Per-topic Z-Score Distributions
    print("Generating Figure: indices_topic_distributions.png...")
    fig, ax = plt.subplots(figsize=(12, 7))
    
    topic_data = []
    topic_labels = []
    for t_key, t_disp in reversed(TOPICS):
        z_col = resolve_topic_column(t_key, df_comp)
        if z_col and z_col in df_comp.columns:
            s = df_comp[z_col].dropna()
            if len(s) > 0:
                topic_data.append(s)
                topic_labels.append(t_disp)
                
    bp = ax.boxplot(topic_data, vert=False, patch_artist=True, tick_labels=topic_labels,
                    boxprops=dict(facecolor="#90E0EF", color="#2E86AB", alpha=0.7),
                    medianprops=dict(color="#03045E", linewidth=2),
                    whiskerprops=dict(color="#2E86AB"),
                    capprops=dict(color="#2E86AB"),
                    flierprops=dict(marker=".", markerfacecolor="gray", markersize=3, alpha=0.4))
    
    ax.axvline(0, color="gray", linestyle="--", alpha=0.7)
    ax.set_xlabel("Standardized Generosity Score (z)", fontsize=12)
    ax.set_title("Cross-Agreement Distribution of Domain Generosity Scores", fontsize=14)
    ax.grid(True, alpha=0.3, axis="x")
    
    fig.tight_layout()
    topic_out = INDICES_FIG_DIR / "indices_topic_distributions.png"
    plt.savefig(topic_out, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  ✓ Saved: {topic_out}")
    
    # Table 4.1: Topline Indices Summary Table
    print("Emitting Table: tab_indices_topline.tex...")
    topline_rows = []
    
    # Add Overall row
    ov_s = df_comp["overall_z"].dropna()
    topline_rows.append({
        "Dimension": "Overall Generosity (Combined)",
        "Mean": f"{ov_s.mean():.2f}",
        "Std": f"{ov_s.std():.2f}",
        "P10": f"{ov_s.quantile(0.10):.2f}",
        "Median": f"{ov_s.median():.2f}",
        "P90": f"{ov_s.quantile(0.90):.2f}",
    })
    
    # Per-topic rows
    for t_key, t_disp in TOPICS:
        z_col = resolve_topic_column(t_key, df_comp)
        if z_col and z_col in df_comp.columns:
            s = df_comp[z_col].dropna()
            # Coverage-only tracks (no magnitude track, e.g. safety/childcare) are marked
            # with a footnote reference -- their distribution reflects a coarse boolean-
            # style coverage indicator rather than a graded generosity magnitude.
            dim_label = f"{t_disp}$^{{c}}$" if z_col.endswith("_coverage_z") else t_disp
            topline_rows.append({
                "Dimension": dim_label,
                "Mean": f"{s.mean():.2f}",
                "Std": f"{s.std():.2f}",
                "P10": f"{s.quantile(0.10):.2f}",
                "Median": f"{s.median():.2f}",
                "P90": f"{s.quantile(0.90):.2f}",
            })

    df_topline = pd.DataFrame(topline_rows)
    emit_table(
        df_topline,
        TABLES_DIR / "tab_indices_topline.tex",
        col_align="lrrrrr",
        headers=["Domain / Index", "Mean", "Std", "P10", "Median", "P90"],
        escape=False,
    )
    
    # Table 4.4: Generosity vs. pay-level correlations
    print("Emitting Table: tab_indices_pay_correlations.tex...")
    pay_corr_path = INDICES_OUT / "coverage_vs_z_correlations.csv"
    df_pay_corr = pd.read_csv(pay_corr_path, sep=";", low_memory=False)
    pay_rows = []
    for t_key, t_disp in TOPICS:
        row = df_pay_corr[df_pay_corr["topic"] == t_key]
        if len(row) == 0:
            continue
        row = row.iloc[0]
        pay_rows.append({
            "Domain": t_disp,
            "N": f"{int(row['n']):,}",
            "Pearson r": f"{row['combined_pay_pearson']:.2f}",
            "Spearman rho": f"{row['combined_pay_spearman']:.2f}",
        })
    df_pay_corr_tab = pd.DataFrame(pay_rows)
    emit_table(
        df_pay_corr_tab,
        TABLES_DIR / "tab_indices_pay_correlations.tex",
        col_align="lrrr",
        headers=["Domain", "N", "Pearson $r$", "Spearman $\\rho$"],
        escape=False,
    )

    # Table 4.5: Factor loadings (combined magnitude+coverage section)
    print("Emitting Table: tab_indices_factor_loadings.tex...")
    fl_path = INDICES_OUT / "factor_loadings_overall.csv"
    df_fl = pd.read_csv(fl_path, sep=";", low_memory=False, index_col=0)
    df_fl_combined = df_fl[df_fl["section"] == "combined"].copy()

    def _fmt2(x):
        x = round(float(x), 2)
        return f"{x + 0.0:.2f}"  # +0.0 clamps -0.00 to 0.00

    fl_rows = []
    for t_key, t_disp in TOPICS:
        z_col = resolve_topic_index(t_key, df_fl_combined.index)
        if z_col is None:
            continue
        row = df_fl_combined.loc[z_col]
        fl_rows.append({
            "Domain": t_disp,
            "F1": _fmt2(row['f1']),
            "F2": _fmt2(row['f2']),
            "F3": _fmt2(row['f3']),
            "Communality": _fmt2(row['communality']),
        })
    df_fl_tab = pd.DataFrame(fl_rows)
    emit_table(
        df_fl_tab,
        TABLES_DIR / "tab_indices_factor_loadings.tex",
        col_align="lrrrr",
        headers=["Domain", "F1", "F2", "F3", "Communality"],
        escape=False,
    )

    # Emit macros
    print("Emitting Indices macros...")
    set_macro("IndPanelStartMonth", str(df_panel_mature["month"].min()))
    set_macro("IndPanelEndMonth", str(df_panel_mature["month"].max()))
    set_macro("IndKmoStat", "0.64")
    set_macro("IndMatureMinCaos", str(MATURE_MIN_CAOS))
    set_macro("IndCompositeTotalDocs", f"{len(df_comp):,}")

    latest_mw_year = mw_year.iloc[-1] if len(mw_year) > 0 else None
    if latest_mw_year is not None:
        set_macro("IndWageMedianLatest", f"EUR {int(latest_mw_year['mw_median']):,}")
        set_macro("IndWmlMonthLatest", f"EUR {int(latest_mw_year['wml_month']):,}")
        ratio_wml = latest_mw_year['mw_median'] / latest_mw_year['wml_month'] if latest_mw_year['wml_month'] > 0 else 0
        set_macro("IndWageToWmlRatio", f"{ratio_wml:.2f}")
    set_macro("IndWageYearMin", str(wage_year_min))
    set_macro("IndWageYearMax", str(wage_year_max))
    set_macro("IndWageYearMinRows", str(MIN_WAGE_YEAR_ROWS))
        
    save_macros()
    print("✓ 04_indices.py completed successfully!")

if __name__ == "__main__":
    main()

