#!/usr/bin/env python3
"""
part1_analysis.py

Description:
    CAO date-based analyses on an input dataset containing CAO numbers and validity periods.

    Performs:
      1) Earliest & latest dates per CAO, gap coverage check, list gaps.
      2) Histograms: earliest start years, latest expiry years, number of files per CAO.
      3) Temporal trends in renewals: gaps between periods; mean/median per CAO and overall; histogram of gap lengths.
      4) CAO size vs coverage period: scatter + Pearson/Spearman correlations; coverage stats table.
      5) Seasonality of start/end dates: four bar charts (months) with consistent colors.

Usage:
    python part1_analysis.py \
        --cao-info "/absolute/path/to/extracted_cao_info.(xlsx|csv)" \
        --outdir "/absolute/path/to/analysis_output" \
        [--sheet "Sheet1"]

Notes:
    - Accepts .xlsx or .csv for the CAO info dataset.
    - Tries to infer column names for CAO number, start date, and end date using flexible matching.
    - Dates are parsed from multiple formats; non-parsable dates are dropped with a warning in the summary tables.
    - Outputs plots as .png into plots/part1 and tables as .csv into tables/part1 under the provided outdir.
    - Console output is minimal and focuses on key completion messages.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pandas import ExcelWriter


# -----------------------------
# Helpers
# -----------------------------

CAO_SYNONYMS = [
    "cao", "cao_nummer", "cao number", "cao_number", "caoid", "cao_id", "number", "nummer"
]

START_DATE_SYNONYMS = [
    "ingangsdatum", "ingang", "startdatum", "start date", "start_date", "start", "begin"
]

END_DATE_SYNONYMS = [
    "expiratiedatum", "expiratie", "einddatum", "einde", "end date", "end_date", "end", "expiry"
]


def normalize_column_name(name: str) -> str:
    return str(name).strip().lower().replace("\n", " ").replace("\r", " ").replace("_", " ")


def find_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    normalized = {normalize_column_name(c): c for c in df.columns}
    for cand in candidates:
        for col_norm, original in normalized.items():
            if cand in col_norm:
                return original
    return None


def parse_dates(series: pd.Series) -> pd.Series:
    # Try robust parsing with dayfirst and coercion
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    # If too many NaT, try without dayfirst as a second attempt
    if parsed.isna().mean() > 0.5:
        alt = pd.to_datetime(series, errors="coerce", dayfirst=False)
        # Use alt where it parsed and original failed
        parsed = parsed.fillna(alt)
    return parsed


def ensure_dirs(base_outdir: str) -> Dict[str, str]:
    plots_dir = os.path.join(base_outdir, "plots", "part1")
    tables_dir = os.path.join(base_outdir, "tables", "part1")
    minor_tables_dir = os.path.join(tables_dir, "details")
    os.makedirs(plots_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)
    os.makedirs(minor_tables_dir, exist_ok=True)
    return {"plots": plots_dir, "tables": tables_dir, "minor_tables": minor_tables_dir}


@dataclass
class Gap:
    gap_start: pd.Timestamp
    gap_end: pd.Timestamp
    gap_days: float


def compute_period_gaps(periods: List[Tuple[pd.Timestamp, pd.Timestamp]]) -> List[Gap]:
    if not periods:
        return []
    # Sort by start date
    periods_sorted = sorted(periods, key=lambda x: (x[0], x[1]))
    gaps: List[Gap] = []
    prev_start, prev_end = periods_sorted[0]
    for cur_start, cur_end in periods_sorted[1:]:
        if pd.notna(prev_end) and pd.notna(cur_start) and cur_start > prev_end:
            gaps.append(Gap(
                gap_start=prev_end,  # gap is (prev_end, cur_start)
                gap_end=cur_start,
                gap_days=(cur_start - prev_end).days
            ))
        # Merge overlapping/contiguous periods to follow coverage
        if pd.isna(prev_end) or (pd.notna(cur_end) and cur_end > prev_end):
            prev_end = cur_end if pd.notna(cur_end) else prev_end
    return gaps


def months_between(a: pd.Timestamp, b: pd.Timestamp) -> float:
    return (b - a).days / 30.4375


def _read_csv_robust(path: str) -> pd.DataFrame:
    # Try with automatic sep detection
    try:
        return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        pass
    # Try UTF-8 BOM
    try:
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    except Exception:
        pass
    # Try common delimiters
    for enc in ("utf-8", "latin-1"):
        for sep in (";", ",", "\t"):
            try:
                return pd.read_csv(path, sep=sep, encoding=enc)
            except Exception:
                continue
    # Last resort
    return pd.read_csv(path)


def load_cao_info(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        # If sheet is None, read the first sheet (0) to avoid returning a dict
        df = pd.read_excel(path, sheet_name=(sheet if sheet is not None else 0))
    else:
        df = _read_csv_robust(path)
    return df


def write_csv_with_description(
    out_path: str,
    description_lines: List[str],
    column_descriptions: Dict[str, str],
    df: pd.DataFrame,
) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        # Description block
        f.write("Description\n")
        for line in description_lines:
            f.write(f"- {line}\n")
        f.write("\n")
        # Column descriptions
        f.write("Column descriptions\n")
        for col in df.columns:
            desc = column_descriptions.get(col, "")
            f.write(f"- {col}: {desc}\n")
        f.write("\n")
    # Append data with header
    df.to_csv(out_path, mode="a", index=False)


def write_excel_with_description(
    out_path: str,
    description_lines: List[str],
    column_descriptions: Dict[str, str],
    df: pd.DataFrame,
) -> None:
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # README sheet
        info_rows: List[Dict[str, str]] = []
        for line in description_lines:
            info_rows.append({"section": "Description", "text": line})
        info_rows.append({"section": "", "text": ""})
        info_rows.append({"section": "Column descriptions", "text": ""})
        info_df = pd.DataFrame(info_rows)
        info_df.to_excel(writer, index=False, sheet_name="README")

        col_desc_df = pd.DataFrame({
            "column": list(df.columns),
            "description": [column_descriptions.get(col, "") for col in df.columns],
        })
        # Start below the first table
        col_desc_df.to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)

        # Data sheet
        df.to_excel(writer, index=False, sheet_name="Data")


def main() -> None:
    parser = argparse.ArgumentParser(description="CAO date-based analyses (Part 1)")
    parser.add_argument("--cao-info", required=True, help="Absolute path to extracted_cao_info (.xlsx or .csv)")
    parser.add_argument("--outdir", required=True, help="Absolute path to output root directory (analysis_output)")
    parser.add_argument("--sheet", default=None, help="Excel sheet name if reading from .xlsx")
    args = parser.parse_args()

    outdirs = ensure_dirs(args.outdir)
    plots_dir = outdirs["plots"]
    tables_dir = outdirs["tables"]
    minor_tables_dir = outdirs["minor_tables"]

    df_raw = load_cao_info(args.cao_info, args.sheet)
    # Identify required columns
    cao_col = find_column(df_raw, CAO_SYNONYMS)
    start_col = find_column(df_raw, START_DATE_SYNONYMS)
    end_col = find_column(df_raw, END_DATE_SYNONYMS)

    if not cao_col or not start_col or not end_col:
        # Save an audit file to help user map columns
        audit_path = os.path.join(tables_dir, "column_audit_part1.csv")
        pd.DataFrame({"columns": list(df_raw.columns)}).to_csv(audit_path, index=False)
        raise SystemExit(
            f"Required columns not found. Please ensure CAO, start, and end columns exist. "
            f"Saved available columns to: {audit_path}"
        )

    df = df_raw[[cao_col, start_col, end_col]].copy()
    df.rename(columns={cao_col: "cao_number", start_col: "start_date", end_col: "end_date"}, inplace=True)
    df["start_date"] = parse_dates(df["start_date"])
    df["end_date"] = parse_dates(df["end_date"])
    df = df.dropna(subset=["cao_number", "start_date", "end_date"]).copy()

    # 1) Earliest & latest dates per CAO and coverage check
    earliest = df.groupby("cao_number")["start_date"].min().rename("earliest_start")
    latest = df.groupby("cao_number")["end_date"].max().rename("latest_end")
    file_counts = df.groupby("cao_number").size().rename("num_files")

    coverage_rows: List[Dict[str, object]] = []
    gaps_rows: List[Dict[str, object]] = []
    renewal_gap_rows: List[Dict[str, object]] = []

    for cao, group in df.groupby("cao_number"):
        periods = [(r.start_date, r.end_date) for r in group.sort_values(["start_date", "end_date"]).itertuples(index=False)]
        gaps = compute_period_gaps(periods)
        # Renewals (gap between one period end and next start)
        group_sorted = group.sort_values(["start_date"]) [["start_date", "end_date"]].reset_index(drop=True)
        for i in range(1, len(group_sorted)):
            prev_end = group_sorted.loc[i-1, "end_date"]
            cur_start = group_sorted.loc[i, "start_date"]
            if pd.notna(prev_end) and pd.notna(cur_start):
                gap_months = (cur_start - prev_end).days / 30.4375
                renewal_gap_rows.append({
                    "cao_number": cao,
                    "prev_end": prev_end,
                    "next_start": cur_start,
                    "gap_days": (cur_start - prev_end).days,
                    "gap_months": gap_months,
                })

        total_gap_days = float(np.sum([g.gap_days for g in gaps])) if gaps else 0.0
        coverage_rows.append({
            "cao_number": cao,
            "earliest_start": earliest.loc[cao],
            "latest_end": latest.loc[cao],
            "num_files": int(file_counts.loc[cao]),
            "num_gaps": len(gaps),
            "total_gap_days": total_gap_days,
            "is_fully_covered": len(gaps) == 0,
            "coverage_months": months_between(earliest.loc[cao], latest.loc[cao]) if pd.notna(earliest.loc[cao]) and pd.notna(latest.loc[cao]) else np.nan,
        })
        for g in gaps:
            gaps_rows.append({
                "cao_number": cao,
                "gap_start": g.gap_start,
                "gap_end": g.gap_end,
                "gap_days": g.gap_days,
                "gap_months": g.gap_days / 30.4375,
            })

    coverage_df = pd.DataFrame(coverage_rows).sort_values(["cao_number"]).reset_index(drop=True)
    gaps_df = pd.DataFrame(gaps_rows).sort_values(["cao_number", "gap_start"]).reset_index(drop=True)
    renewals_df = pd.DataFrame(renewal_gap_rows).sort_values(["cao_number", "prev_end"]).reset_index(drop=True)

    # Coverage summary (Excel with README)
    coverage_xlsx = os.path.join(tables_dir, "cao_coverage_summary.xlsx")
    write_excel_with_description(
        coverage_xlsx,
        description_lines=[
            "One row per CAO summarizing earliest and latest dates, number of files, and coverage metrics.",
        ],
        column_descriptions={
            "cao_number": "CAO identifier.",
            "earliest_start": "Earliest start_date found for the CAO.",
            "latest_end": "Latest end_date found for the CAO.",
            "num_files": "Number of period entries for this CAO in the input.",
            "num_gaps": "Number of uncovered intervals inside [earliest_start, latest_end].",
            "total_gap_days": "Sum of uncovered days across all gaps.",
            "is_fully_covered": "True if no uncovered intervals exist.",
            "coverage_months": "(latest_end - earliest_start) in months (days/30.4375).",
        },
        df=coverage_df,
    )

    # Write detailed gap tables with descriptions into subfolder
    if not gaps_df.empty:
        gaps_xlsx = os.path.join(minor_tables_dir, "cao_coverage_gaps.xlsx")
        write_excel_with_description(
            gaps_xlsx,
            description_lines=[
                "Uncovered time windows within each CAO.",
                "Only positive gaps are included (when the next period starts after the previous ends).",
            ],
            column_descriptions={
                "cao_number": "CAO identifier.",
                "gap_start": "End date of the previous covered period.",
                "gap_end": "Start date of the next covered period.",
                "gap_days": "Length of uncovered window in days.",
                "gap_months": "Length of uncovered window in months (days/30.4375).",
            },
            df=gaps_df,
        )

    if not renewals_df.empty:
        renewals_xlsx = os.path.join(minor_tables_dir, "cao_renewal_gaps.xlsx")
        write_excel_with_description(
            renewals_xlsx,
            description_lines=[
                "All transitions between consecutive periods within each CAO (renewals).",
                "Gaps can be negative (overlap), zero (contiguous), or positive (break).",
            ],
            column_descriptions={
                "cao_number": "CAO identifier.",
                "prev_end": "End date of the previous period.",
                "next_start": "Start date of the next period.",
                "gap_days": "Transition gap in days (next_start - prev_end).",
                "gap_months": "Transition gap in months (days/30.4375).",
            },
            df=renewals_df,
        )

    # 2) Histograms of earliest start years, latest expiry years, number of files per CAO
    earliest_years = coverage_df.dropna(subset=["earliest_start"]).copy()
    earliest_years["year"] = earliest_years["earliest_start"].dt.year
    latest_years = coverage_df.dropna(subset=["latest_end"]).copy()
    latest_years["year"] = latest_years["latest_end"].dt.year

    sns.set_theme(style="whitegrid")

    # Use an explicit bar plot centered on integer years; include zero-count years in range
    fig, ax = plt.subplots(figsize=(8, 5))
    e_counts = earliest_years["year"].astype(int).value_counts().sort_index()
    if not e_counts.empty:
        full_years = list(range(int(e_counts.index.min()), int(e_counts.index.max()) + 1))
        e_counts = e_counts.reindex(full_years, fill_value=0)
    ax.bar(e_counts.index, e_counts.values, width=0.8, color="#4C78A8", align="center")
    ax.set_title("Histogram of Earliest Start Years (per CAO)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of CAOs")
    if not e_counts.empty:
        years = e_counts.index.tolist()
        ax.set_xticks(years)
        ax.set_xlim(min(years) - 0.5, max(years) + 0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "hist_earliest_start_years.png"), dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 5))
    l_counts = latest_years["year"].astype(int).value_counts().sort_index()
    if not l_counts.empty:
        full_years_l = list(range(int(l_counts.index.min()), int(l_counts.index.max()) + 1))
        l_counts = l_counts.reindex(full_years_l, fill_value=0)
    ax.bar(l_counts.index, l_counts.values, width=0.8, color="#F58518", align="center")
    ax.set_title("Histogram of Latest Expiry Years (per CAO)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of CAOs")
    if not l_counts.empty:
        years = l_counts.index.tolist()
        ax.set_xticks(years)
        ax.set_xlim(min(years) - 0.5, max(years) + 0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "hist_latest_expiry_years.png"), dpi=150)
    plt.close(fig)

    # Center bars on integer counts of files per CAO
    fig, ax = plt.subplots(figsize=(8, 5))
    counts_per_files = coverage_df["num_files"].astype(int).value_counts().sort_index()
    if not counts_per_files.empty:
        x_min, x_max = int(counts_per_files.index.min()), int(counts_per_files.index.max())
        full_x = list(range(x_min, x_max + 1))
        counts_per_files = counts_per_files.reindex(full_x, fill_value=0)
        ax.bar(full_x, counts_per_files.values, width=0.8, color="#54A24B", align="center")
        ax.set_xticks(full_x)
        ax.set_xlim(x_min - 0.5, x_max + 0.5)
    ax.set_title("Histogram of Number of Files (per CAO)")
    ax.set_xlabel("Number of Files")
    ax.set_ylabel("Number of CAOs")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "hist_num_files_per_cao.png"), dpi=150)
    plt.close(fig)

    # 3) Temporal trends in renewals
    if not renewals_df.empty:
        # Per CAO stats
        per_cao_stats = renewals_df.groupby("cao_number")["gap_months"].agg(["count", "mean", "median"]).reset_index()
        per_cao_stats.rename(columns={"count": "num_renewals", "mean": "avg_gap_months", "median": "median_gap_months"}, inplace=True)
        # Positive-only gap statistics (exclude overlaps/contiguous)
        pos = renewals_df[renewals_df["gap_months"] > 0].groupby("cao_number")["gap_months"].agg(
            avg_positive_gap_months="mean",
            median_positive_gap_months="median",
        ).reset_index()
        per_cao_stats = pd.merge(per_cao_stats, pos, on="cao_number", how="left")
        write_excel_with_description(
            os.path.join(tables_dir, "renewal_gap_stats_per_cao.xlsx"),
            description_lines=[
                "Renewal gap statistics per CAO computed from consecutive period transitions.",
                "Positive-only metrics exclude overlaps/contiguity (gap_months <= 0).",
            ],
            column_descriptions={
                "cao_number": "CAO identifier.",
                "num_renewals": "Number of consecutive transitions considered.",
                "avg_gap_months": "Average gap (months) including negative/zero (overlap/contiguous).",
                "median_gap_months": "Median gap (months) including negative/zero.",
                "avg_positive_gap_months": "Average gap (months) using only positive gaps (breaks).",
                "median_positive_gap_months": "Median gap (months) using only positive gaps.",
            },
            df=per_cao_stats,
        )

        # Overall histogram
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(renewals_df["gap_months"], bins=30, color="#E45756")
        ax.set_title("Histogram of Renewal Gap Lengths (months, overall)")
        ax.set_xlabel("Gap length (months)")
        ax.set_ylabel("Number of renewals")
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, "hist_renewal_gap_lengths_overall.png"), dpi=150)
        plt.close(fig)

        # Overall aggregates (all transitions and positive-only transitions)
        overall_all = pd.DataFrame({
            "subset": ["all"],
            "overall_avg_gap_months": [renewals_df["gap_months"].mean()],
            "overall_median_gap_months": [renewals_df["gap_months"].median()],
            "total_renewals": [len(renewals_df)],
        })
        pos_df = renewals_df[renewals_df["gap_months"] > 0]
        overall_pos = pd.DataFrame({
            "subset": ["positive_only"],
            "overall_avg_gap_months": [pos_df["gap_months"].mean() if not pos_df.empty else np.nan],
            "overall_median_gap_months": [pos_df["gap_months"].median() if not pos_df.empty else np.nan],
            "total_renewals": [len(pos_df)],
        })
        overall_stats = pd.concat([overall_all, overall_pos], ignore_index=True)
        write_excel_with_description(
            os.path.join(tables_dir, "renewal_gap_stats_overall.xlsx"),
            description_lines=[
                "Overall renewal gap statistics across all CAOs.",
                "Two subsets: 'all' includes overlaps/contiguous, 'positive_only' includes only breaks.",
            ],
            column_descriptions={
                "subset": "Which subset the row summarizes: 'all' or 'positive_only'.",
                "overall_avg_gap_months": "Average gap (months) in the subset.",
                "overall_median_gap_months": "Median gap (months) in the subset.",
                "total_renewals": "Number of transitions in the subset.",
            },
            df=overall_stats,
        )

    # 4) CAO size vs coverage period
    scatter_df = coverage_df.dropna(subset=["coverage_months"]).copy()
    if not scatter_df.empty:
        x = scatter_df["num_files"].astype(float)
        y = scatter_df["coverage_months"].astype(float)
        pearson = x.corr(y, method="pearson")
        spearman = x.corr(y, method="spearman")

        fig, ax = plt.subplots(figsize=(7, 6))
        sns.regplot(x="num_files", y="coverage_months", data=scatter_df, scatter_kws={"alpha": 0.6}, line_kws={"color": "#E45756"}, ax=ax)
        ax.set_title(f"Files vs Coverage Length (months)\nPearson={pearson:.3f}, Spearman={spearman:.3f}")
        ax.set_xlabel("Number of Files (per CAO)")
        ax.set_ylabel("Coverage Length (months)")
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, "scatter_files_vs_coverage_months.png"), dpi=150)
        plt.close(fig)

        # Correlation values are shown directly in the plot title; no table output needed.

        # Coverage stats table already saved as coverage_df

    coverage_df.to_csv(os.path.join(tables_dir, "cao_coverage_summary.csv"), index=False)

    # 5) Seasonality of Start/End Dates (all periods only)
    all_start_months = df.dropna(subset=["start_date"]).copy()
    all_start_months["month"] = all_start_months["start_date"].dt.month
    all_end_months = df.dropna(subset=["end_date"]).copy()
    all_end_months["month"] = all_end_months["end_date"].dt.month

    month_order = list(range(1, 13))
    color_palette = {
        "all_start": "#54A24B",
        "all_end": "#E45756",
    }

    def bar_months(data: pd.Series, title: str, color: str, outname: str) -> None:
        counts = data.value_counts().reindex(month_order, fill_value=0)
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.bar(counts.index, counts.values, color=color)
        ax.set_xticks(month_order)
        ax.set_xlabel("Month")
        ax.set_ylabel("Frequency")
        ax.set_title(title)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, outname), dpi=150)
        plt.close(fig)

    bar_months(all_start_months["month"], "Seasonality: All Start Months (all periods)", color_palette["all_start"], "seasonality_all_start_months.png")
    bar_months(all_end_months["month"], "Seasonality: All Expiry Months (all periods)", color_palette["all_end"], "seasonality_all_expiry_months.png")

    print(f"Saved Part 1 plots to: {plots_dir}")
    print(f"Saved Part 1 tables to: {tables_dir}")


if __name__ == "__main__":
    main()


