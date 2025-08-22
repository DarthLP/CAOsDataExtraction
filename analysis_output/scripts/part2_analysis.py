#!/usr/bin/env python3
"""
part2_analysis.py

Description:
    Analyses on extracted results per file (salary tables, benefits, correlations, missingness),
    merging with CAO coverage length from Part 1 when available.

    Performs:
      1) Salary tables analysis and histogram (% and counts).
      2) Salary table completeness over time (by CAO earliest start year), with line chart and secondary axis for file counts.
      3) Benefits analysis (% and counts), histogram per category.
      4) Cross-topic co-occurrence (heatmap and top 10 pairs table).
      5) Missing data by CAO (heatmap and table).
      6) Benefit prevalence over time (multi-line chart by year).
      7) Correlations: coverage length vs salary tables and benefits (scatter + regression + correlations).
      8) Benefit richness by sector (if sector available) with bar chart and table.

Usage:
    python part2_analysis.py \
        --extracted "/absolute/path/to/results/extracted_data.xlsx" \
        --outdir "/absolute/path/to/analysis_output" \
        [--coverage "/absolute/path/to/analysis_output/tables/part1/cao_coverage_summary.csv"] \
        [--sheet "Sheet1"]

Notes:
    - Accepts .xlsx or .csv for the extracted results dataset.
    - Tries to infer columns for CAO number, start date, end date, salary table presence/count, and benefits.
    - Dates are parsed from multiple formats.
    - Outputs plots as .png into plots/part2 and tables as .csv into tables/part2 under the provided outdir.
    - Console output is minimal and focuses on key completion messages.
"""

from __future__ import annotations

import argparse
import os
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

FILE_ID_SYNONYMS = [
    "file", "file id", "file_id", "filename", "file name", "source_file", "pdf", "pdf_name", "original_pdf_name", "json", "json_name"
]

BENEFIT_COLUMN_PATTERNS = {
    # Any columns beginning with or containing these tokens count as presence if non-empty
    "pension": ["pension", "retire"],
    "leave": ["leave", "vacation", "maternity", "vakantie", "verlof"],
    "termination": ["term_", "termination", "ontslag", "beëindiging", "beeindiging", "probation"],
    "overtime": ["overtime", "shift_compensation", "max_hrs", "min_hrs"],
    "training": ["training", "opleiding"],
    "homeoffice": ["homeoffice", "thuiswerk", "remote", "hybrid"],
}


def normalize_column_name(name: str) -> str:
    return str(name).strip().lower().replace("\n", " ").replace("\r", " ").replace("_", " ")


def find_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    normalized = {normalize_column_name(c): c for c in df.columns}
    for cand in candidates:
        for col_norm, original in normalized.items():
            if cand in col_norm:
                return original
    return None


def find_benefit_column_lists(df: pd.DataFrame) -> Dict[str, List[str]]:
    lower_cols = [str(c).strip().lower() for c in df.columns]
    result: Dict[str, List[str]] = {b: [] for b in BENEFIT_COLUMN_PATTERNS.keys()}
    reverse_matches: Dict[str, List[str]] = {}
    for benefit, patterns in BENEFIT_COLUMN_PATTERNS.items():
        cols: List[str] = []
        for i, lc in enumerate(lower_cols):
            for pat in patterns:
                # match prefix or substring
                if lc.startswith(pat) or (pat in lc):
                    cols.append(df.columns[i])
                    reverse_matches.setdefault(df.columns[i], []).append(benefit)
                    break
        # de-duplicate while preserving order
        seen = set()
        deduped = []
        for c in cols:
            if c not in seen:
                seen.add(c)
                deduped.append(c)
        result[benefit] = deduped
    # Attach reverse matches for auditing by storing as attribute
    result["__reverse__"] = [reverse_matches]  # type: ignore
    return result


def write_benefit_mapping_audit(
    df_raw: pd.DataFrame,
    benefit_col_lists: Dict[str, List[str]],
    tables_dir: str,
) -> None:
    # Flatten mapping
    rows: List[Dict[str, object]] = []
    reverse_list = benefit_col_lists.get("__reverse__", [{}])  # type: ignore
    reverse_map: Dict[str, List[str]] = reverse_list[0] if reverse_list else {}
    for benefit, cols in benefit_col_lists.items():
        if benefit == "__reverse__":
            continue
        for col in cols:
            s = df_raw[col]
            non_empty = ((~s.isna()) & (s.astype(str).str.strip() != "")).sum()
            rows.append({
                "benefit": benefit,
                "column": col,
                "non_empty_rows": int(non_empty),
                "matched_multiple_benefits": ",".join(reverse_map.get(col, [])) if len(reverse_map.get(col, [])) > 1 else "",
            })
    audit_df = pd.DataFrame(rows).sort_values(["benefit", "column"]) if rows else pd.DataFrame(columns=["benefit","column","non_empty_rows","matched_multiple_benefits"])
    out_path = os.path.join(tables_dir, "benefit_column_mapping.xlsx")
    # write as Excel with README
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        info_rows: List[Dict[str, str]] = [
            {"section": "Description", "text": "Audit mapping showing which raw columns were matched to each benefit topic and how many non-empty rows each column has."},
        ]
        info_rows.append({"section": "", "text": ""})
        info_rows.append({"section": "Column descriptions", "text": ""})
        info_df = pd.DataFrame(info_rows)
        info_df.to_excel(writer, index=False, sheet_name="README")
        col_desc_df = pd.DataFrame({
            "column": list(audit_df.columns),
            "description": [
                {
                    "benefit": "Benefit topic the column was mapped to.",
                    "column": "Original column name in extracted_data.",
                    "non_empty_rows": "Count of rows where the column has a non-empty value.",
                    "matched_multiple_benefits": "If the column matched multiple benefit patterns, they are listed here.",
                }.get(c, "") for c in audit_df.columns
            ],
        })
        col_desc_df.to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
        audit_df.to_excel(writer, index=False, sheet_name="Data")


def parse_dates(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    if parsed.isna().mean() > 0.5:
        alt = pd.to_datetime(series, errors="coerce", dayfirst=False)
        parsed = parsed.fillna(alt)
    return parsed


def ensure_dirs(base_outdir: str) -> Dict[str, str]:
    plots_dir = os.path.join(base_outdir, "plots", "part2")
    tables_dir = os.path.join(base_outdir, "tables", "part2")
    os.makedirs(plots_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)
    return {"plots": plots_dir, "tables": tables_dir}


def load_extracted(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        df = pd.read_excel(path, sheet_name=(sheet if sheet is not None else 0))
    else:
        df = pd.read_csv(path)
    return df


def load_coverage(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(path, sheet_name=0)
    # Robust CSV load
    try:
        return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        pass
    try:
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    except Exception:
        pass
    for enc in ("utf-8", "latin-1"):
        for sep in (";", ",", "\t"):
            try:
                return pd.read_csv(path, sep=sep, encoding=enc)
            except Exception:
                continue
    return pd.read_csv(path, encoding_errors="ignore")


def main() -> None:
    parser = argparse.ArgumentParser(description="CAO content analyses (Part 2)")
    parser.add_argument("--extracted", required=True, help="Absolute path to extracted_data (.xlsx or .csv)")
    parser.add_argument("--outdir", required=True, help="Absolute path to output root directory (analysis_output)")
    parser.add_argument("--coverage", default=None, help="Optional path to Part 1 coverage summary CSV to enable correlation analyses")
    parser.add_argument("--sheet", default=None, help="Excel sheet name if reading from .xlsx")
    args = parser.parse_args()

    outdirs = ensure_dirs(args.outdir)
    plots_dir = outdirs["plots"]
    tables_dir = outdirs["tables"]

    df_raw = load_extracted(args.extracted, args.sheet)

    # Identify columns
    cao_col = find_column(df_raw, CAO_SYNONYMS)
    start_col = find_column(df_raw, START_DATE_SYNONYMS)
    end_col = find_column(df_raw, END_DATE_SYNONYMS)

    if not cao_col or not start_col or not end_col:
        audit_path = os.path.join(tables_dir, "column_audit_part2.csv")
        pd.DataFrame({"columns": list(df_raw.columns)}).to_csv(audit_path, index=False)
        raise SystemExit(
            f"Required columns not found (CAO, start, end). Saved available columns to: {audit_path}"
        )

    df = df_raw.copy()
    df.rename(columns={cao_col: "cao_number", start_col: "start_date", end_col: "end_date"}, inplace=True)
    df["start_date"] = parse_dates(df["start_date"])
    df["end_date"] = parse_dates(df["end_date"])
    df = df.dropna(subset=["cao_number"]).copy()

    # Identify file identifier (grouping key for multi-row per file)
    file_col = find_column(df_raw, FILE_ID_SYNONYMS)
    if file_col is None:
        # Fallback: synthesize a file id based on CAO and dates and row index
        df["file_id"] = (
            df["cao_number"].astype(str) + "__" + df["start_date"].astype(str) + "__" + df["end_date"].astype(str) + "__" + df.reset_index().index.astype(str)
        )
    else:
        df.rename(columns={file_col: "file_id"}, inplace=True)

    # Benefit presence across possibly multiple columns per topic
    benefit_col_lists = find_benefit_column_lists(df)
    # Write an audit to verify mapping correctness
    write_benefit_mapping_audit(df_raw, benefit_col_lists, tables_dir)
    def any_non_empty_across(row: pd.Series, cols: List[str]) -> bool:
        if not cols:
            return False
        vals = [row[c] for c in cols if c in row.index]
        if not vals:
            return False
        series = pd.Series(vals)
        return ((~series.isna()) & (series.astype(str).str.strip() != "")).any()
    for benefit in BENEFIT_COLUMN_PATTERNS.keys():
        cols = benefit_col_lists.get(benefit, [])
        df[benefit] = df.apply(lambda r: any_non_empty_across(r, cols), axis=1)

    # Salary tables per file: count non-empty among salary_1..salary_7; if more_salaries filled → at least 8
    normalized_to_original = {normalize_column_name(c): c for c in df.columns}
    salary_base_cols: List[str] = []
    for i in range(1, 8):
        key = f"salary {i}"
        if key in normalized_to_original:
            salary_base_cols.append(normalized_to_original[key])
    more_key = "more salaries"
    more_col = normalized_to_original.get(more_key)

    def col_has_value(s: pd.Series) -> pd.Series:
        return (~s.isna()) & (s.astype(str).str.strip() != "")

    # Compute presence per file for each salary_i and more_salaries
    base_presence_per_file: List[pd.Series] = []
    for col in salary_base_cols:
        pres = df.groupby("file_id")[col].apply(lambda s: col_has_value(s).any()).astype(int)
        base_presence_per_file.append(pres.rename(col))
    if base_presence_per_file:
        base_presence_df = pd.concat(base_presence_per_file, axis=1)
        base_counts = base_presence_df.sum(axis=1)
    else:
        base_counts = pd.Series(0, index=df["file_id"].drop_duplicates(), dtype=int)
    # If more_salaries is explicitly "yes" (case-insensitive) AND all base salary_i are filled, then treat as 8+
    if more_col is not None:
        more_yes = df.groupby("file_id")[more_col].apply(
            lambda s: s.astype(str).str.strip().str.lower().eq("yes").any()
        )
        base_all_filled = base_presence_df.all(axis=1) if base_presence_per_file else pd.Series(False, index=base_counts.index)
        more_condition = more_yes.reindex(base_counts.index).fillna(False) & base_all_filled.reindex(base_counts.index).fillna(False)
    else:
        more_condition = pd.Series(False, index=base_counts.index)

    # Numeric and label representations
    salary_count_numeric = base_counts.where(~more_condition, 8)
    salary_count_label = base_counts.astype(str).where(~more_condition, "8+")

    # Aggregate to file-level: many rows per file, benefits usually 1 row, wage can be multiple rows
    # - salary_table_count_file: sum of row-level counts per file
    # - benefit presence per file: any non-empty across rows
    # - carry CAO, start/end dates per file (take min start, max end)
    presence_row_df = df[["pension", "leave", "termination", "overtime", "training", "homeoffice"]].astype(bool)
    df_presence = pd.concat([df[["file_id", "cao_number", "start_date", "end_date"]], presence_row_df], axis=1)

    agg_dict = {b: "max" for b in ["pension", "leave", "termination", "overtime", "training", "homeoffice"]}
    file_df = df_presence.groupby("file_id").agg({
        "cao_number": "first",
        "start_date": "min",
        "end_date": "max",
        **agg_dict,
    }).reset_index()
    # Attach computed salary table count
    file_df = file_df.merge(salary_count_numeric.rename("salary_table_count").reset_index(), on="file_id", how="left")
    file_df = file_df.merge(salary_count_label.rename("salary_table_count_label").reset_index(), on="file_id", how="left")

    # Ensure boolean ints for presence
    for b in ["pension", "leave", "termination", "overtime", "training", "homeoffice"]:
        file_df[b] = file_df[b].astype(int)

    # 1) Salary Tables Analysis (file-level)
    per_file_counts = file_df[["salary_table_count"]].copy()
    sns.set_theme(style="whitegrid")
    fig, ax1 = plt.subplots(figsize=(10, 5))
    counts = per_file_counts["salary_table_count"].astype(float)
    hist_vals, bin_edges = np.histogram(counts, bins=30)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    bar_width = (bin_edges[1] - bin_edges[0]) * 0.9
    total_files = max(float(len(counts)), 1.0)
    percents = (hist_vals / total_files) * 100.0

    # Left axis: percent bars
    ax1.bar(bin_centers, percents, width=bar_width, color="#54A24B", alpha=0.85, align="center")
    # Show all integer x ticks in range
    if len(bin_centers) > 1:
        xmin, xmax = int(np.floor(bin_edges[0])), int(np.ceil(bin_edges[-1]))
        xticks = list(range(xmin, xmax + 1))
        ax1.set_xticks(xticks)
    ax1.set_xlabel("Number of salary tables per file")
    ax1.set_ylabel("% of files")
    max_pct = float(percents.max()) if len(percents) else 0.0
    ax1.set_ylim(0, max_pct * 1.10 if max_pct > 0 else 1.0)

    # Right axis: counts aligned so that max percent aligns to corresponding count
    ax2 = ax1.twinx()
    ax2.set_ylabel("Number of files")
    ax2.set_ylim(0, (total_files * (max_pct / 100.0)) * 1.10 if max_pct > 0 else total_files)
    ax2.grid(False)

    ax1.set_title("Salary Tables per File: Percent and Count (dual axis)")

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "hist_salary_tables_per_file_percent_and_count.png"), dpi=150)
    plt.close(fig)

    # Table of CAOs: % of files with >=1 salary table
    file_df["has_salary"] = (file_df["salary_table_count"].astype(float) >= 1).astype(int)
    cao_salary_stats = file_df.groupby("cao_number")["has_salary"].agg(["mean", "count", "sum"]).reset_index()
    cao_salary_stats.rename(columns={"mean": "pct_with_salary", "count": "num_files", "sum": "num_with_salary"}, inplace=True)
    cao_salary_stats["pct_with_salary"] = cao_salary_stats["pct_with_salary"] * 100.0
    # Write Excel with README
    out_xlsx = os.path.join(tables_dir, "cao_salary_table_presence.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        info_rows = [
            {"section": "Description", "text": "Per-CAO summary of files with at least one salary table."},
            {"section": "", "text": ""},
            {"section": "Column descriptions", "text": ""},
        ]
        pd.DataFrame(info_rows).to_excel(writer, index=False, sheet_name="README")
        desc_map = {
            "cao_number": "CAO identifier.",
            "pct_with_salary": "% of files within the CAO that have ≥1 salary table.",
            "num_files": "Total number of files in the CAO.",
            "num_with_salary": "Number of files with ≥1 salary table.",
        }
        pd.DataFrame({
            "column": list(cao_salary_stats.columns),
            "description": [desc_map.get(c, "") for c in cao_salary_stats.columns],
        }).to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
        cao_salary_stats.to_excel(writer, index=False, sheet_name="Data")

    # 2) Salary Table Completeness Over Time (by CAO earliest start year)
    file_df["start_year"] = file_df["start_date"].dt.year.astype("Int64")
    year_stats = file_df.groupby("start_year")["has_salary"].agg(["mean", "count", "sum"]).reset_index()
    year_stats.rename(columns={"mean": "pct_with_salary", "count": "num_files", "sum": "num_with_salary"}, inplace=True)
    year_stats["pct_with_salary"] = year_stats["pct_with_salary"] * 100.0
    year_stats = year_stats.sort_values("start_year")
    out_xlsx = os.path.join(tables_dir, "salary_table_completeness_by_year.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        info_rows = [
            {"section": "Description", "text": "% of files with ≥1 salary table by earliest start year (file-level)."},
            {"section": "", "text": ""},
            {"section": "Column descriptions", "text": ""},
        ]
        pd.DataFrame(info_rows).to_excel(writer, index=False, sheet_name="README")
        desc_map = {
            "start_year": "File earliest start year.",
            "mean": "Proportion of files with ≥1 salary table.",
            "count": "Number of files with that start year.",
            "sum": "Number of files with ≥1 salary table in that year.",
            "pct_with_salary": "% of files with ≥1 salary table in that year.",
            "num_files": "Total files in that year.",
            "num_with_salary": "Files with ≥1 salary table in that year.",
        }
        pd.DataFrame({
            "column": list(year_stats.columns),
            "description": [desc_map.get(c, "") for c in year_stats.columns],
        }).to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
        year_stats.to_excel(writer, index=False, sheet_name="Data")

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(year_stats["start_year"], year_stats["pct_with_salary"], marker="o", color="#4C78A8", label="% with salary table")
    ax.set_xlabel("Year")
    ax.set_ylabel("% of files with ≥1 salary table")
    ax.set_title("Salary Table Completeness Over Time")
    # Force left axis to start at 0
    max_pct_comp = float(year_stats["pct_with_salary"].max()) if len(year_stats) else 0.0
    ax.set_ylim(0, max(100.0, max_pct_comp * 1.10))
    ax2 = ax.twinx()
    ax2.bar(year_stats["start_year"], year_stats["num_files"], color="#E45756", alpha=0.25, edgecolor='none', linewidth=0, label="# files")
    ax2.set_ylabel("Number of files")
    ax2.grid(False)
    # Force integer year ticks and tight x-limits around years
    unique_years = sorted(year_stats["start_year"].dropna().astype(int).unique())
    if unique_years:
      ax.set_xticks(unique_years)
      ax.set_xlim(unique_years[0] - 0.5, unique_years[-1] + 0.5)
    # Combine legends
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "line_salary_completeness_over_time.png"), dpi=150)
    plt.close(fig)

    # 3) Benefits Analysis
    benefit_cols = ["pension", "leave", "termination", "overtime", "training", "homeoffice"]
    # file_df already has binary presence per file
    presence = file_df[["file_id"] + benefit_cols].copy()

    benefit_summary = presence[benefit_cols].mean().to_frame("pct").reset_index().rename(columns={"index": "benefit"})
    benefit_summary["pct"] = benefit_summary["pct"] * 100.0
    benefit_counts = presence[benefit_cols].sum().to_frame("count").reset_index().rename(columns={"index": "benefit"})
    benefit_summary = pd.merge(benefit_summary, benefit_counts, on="benefit", how="left")
    out_xlsx_benefits = os.path.join(tables_dir, "benefit_presence_summary.xlsx")
    with pd.ExcelWriter(out_xlsx_benefits, engine="openpyxl") as writer:
        info_rows = [
            {"section": "Description", "text": "Summary of benefit presence across files (file-level)."},
            {"section": "", "text": ""},
            {"section": "Column descriptions", "text": ""},
        ]
        pd.DataFrame(info_rows).to_excel(writer, index=False, sheet_name="README")
        desc_map = {
            "benefit": "Benefit category (pension, leave, termination, overtime, training, homeoffice).",
            "pct": "% of files with ≥1 entry for the benefit.",
            "count": "Count of files with ≥1 entry for the benefit.",
        }
        pd.DataFrame({
            "column": list(benefit_summary.columns),
            "description": [desc_map.get(c, "") for c in benefit_summary.columns],
        }).to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
        benefit_summary.to_excel(writer, index=False, sheet_name="Data")

    # Combined percent + count chart with twin y-axes
    categories = benefit_summary["benefit"].tolist()
    percents = benefit_summary["pct"].astype(float).tolist()
    counts = benefit_summary["count"].astype(float).tolist()
    total_files = float(len(presence))

    fig, ax1 = plt.subplots(figsize=(10, 5))
    x_idx = list(range(len(categories)))
    bars = ax1.bar(x_idx, percents, color="#54A24B", alpha=0.85, width=0.8, align="center")
    ax1.set_ylabel("% of files with ≥1 entry")
    ax1.set_ylim(0, max(100.0, (max(percents) if percents else 0) * 1.10))
    ax1.set_xlabel("Benefit category")
    ax1.set_title("Benefits Presence: Percent and Count (dual axis)")
    ax1.set_xticks(x_idx)
    ax1.set_xticklabels(categories, rotation=20)

    # Right axis only for scale in counts; no plot on this axis
    ax2 = ax1.twinx()
    ax2.set_ylabel("Count of files with ≥1 entry")
    # Align scales: 100% on left equals total_files on right
    ax2.set_ylim(0, max(total_files, (max(counts) if counts else 0) * 1.10))
    ax2.grid(False)
    # No legend needed

    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "hist_benefits_percent_and_count.png"), dpi=150)
    plt.close(fig)

    # 4) Cross-Topic Co-Occurrence of Benefits (removed per request)

    # 5) Missing Data by CAO
    # For each CAO: % missing per column
    cols_for_missing = ["salary_table_count"] + benefit_cols
    missing_rows: List[Dict[str, float]] = []
    for cao, grp in file_df.groupby("cao_number"):
        entry: Dict[str, float] = {"cao_number": cao}
        # Define missing at file-level: salary missing if count == 0; benefit missing if 0
        for col in cols_for_missing:
            if col == "salary_table_count":
                miss = (grp[col].fillna(0) == 0).mean() * 100.0
            else:
                miss = (grp[col].fillna(0) == 0).mean() * 100.0
            entry[f"missing_{col}"] = miss
        missing_rows.append(entry)
    missing_df = pd.DataFrame(missing_rows)
    out_xlsx_missing = os.path.join(tables_dir, "missingness_by_cao.xlsx")
    with pd.ExcelWriter(out_xlsx_missing, engine="openpyxl") as writer:
        info_rows = [
            {"section": "Description", "text": "% missing per CAO for salary and each benefit (file-level)."},
            {"section": "", "text": ""},
            {"section": "Column descriptions", "text": ""},
        ]
        pd.DataFrame(info_rows).to_excel(writer, index=False, sheet_name="README")
        desc_map = {"cao_number": "CAO identifier."}
        for col in ["salary_table_count"] + benefit_cols:
            desc_map[f"missing_{col}"] = f"% of files missing {col} (0 or empty)."
        pd.DataFrame({
            "column": list(missing_df.columns),
            "description": [desc_map.get(c, "") for c in missing_df.columns],
        }).to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
        missing_df.to_excel(writer, index=False, sheet_name="Data")

    # Heatmap (columns vs CAO). If many CAOs, this may be wide; still save.
    if not missing_df.empty:
        heat = missing_df.set_index("cao_number").sort_index()
        fig, ax = plt.subplots(figsize=(min(18, 2 + 0.4 * heat.shape[0]), 6))
        sns.heatmap(heat.T, cmap="Reds", cbar_kws={"label": "% missing"}, ax=ax)
        ax.set_title("Missing Data by CAO (% missing)")
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, "heatmap_missingness_by_cao.png"), dpi=150)
        plt.close(fig)

    # 6) Benefit Prevalence Over Time
    year_presence = file_df.groupby(file_df["start_date"].dt.year)[benefit_cols].mean()
    year_presence = year_presence.fillna(0.0) * 100.0
    year_presence.index.name = "year"
    year_presence = year_presence.reset_index()
    out_xlsx = os.path.join(tables_dir, "benefit_prevalence_over_time.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        info_rows = [
            {"section": "Description", "text": "% of files containing each benefit by file start year (file-level)."},
            {"section": "", "text": ""},
            {"section": "Column descriptions", "text": ""},
        ]
        pd.DataFrame(info_rows).to_excel(writer, index=False, sheet_name="README")
        desc_map = {"year": "File earliest start year."}
        desc_map.update({b: f"% of files with {b}." for b in benefit_cols})
        pd.DataFrame({
            "column": list(year_presence.columns),
            "description": [desc_map.get(c, "") for c in year_presence.columns],
        }).to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
        year_presence.to_excel(writer, index=False, sheet_name="Data")

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#4C78A8", "#F58518", "#E45756", "#72B7B2", "#54A24B", "#EECA3B"]
    for i, b in enumerate(benefit_cols):
        ax.plot(year_presence["year"], year_presence[b], marker="o", label=b, color=colors[i % len(colors)])
    ax.set_xlabel("Year")
    ax.set_ylabel("% of files containing benefit")
    ax.set_title("Benefit Prevalence Over Time")
    ax.legend(loc="best")
    # Integer ticks and tight x-limits
    uyears2 = sorted(year_presence["year"].dropna().astype(int).unique())
    if uyears2:
      ax.set_xticks(uyears2)
      ax.set_xlim(uyears2[0] - 0.5, uyears2[-1] + 0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "line_benefit_prevalence_over_time.png"), dpi=150)
    plt.close(fig)

    # New plot: number of files per year (start vs end years)
    start_year_counts = file_df.groupby(file_df["start_date"].dt.year).size().reset_index(name="num_files_start")
    end_year_counts = file_df.groupby(file_df["end_date"].dt.year).size().reset_index(name="num_files_end")
    years = sorted(set(start_year_counts.iloc[:, 0]).union(set(end_year_counts.iloc[:, 0])))
    start_map = dict(zip(start_year_counts.iloc[:, 0], start_year_counts["num_files_start"]))
    end_map = dict(zip(end_year_counts.iloc[:, 0], end_year_counts["num_files_end"]))
    start_series = [start_map.get(y, 0) for y in years]
    end_series = [end_map.get(y, 0) for y in years]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(years, start_series, marker="o", color="#4C78A8", label="files by start year")
    ax.plot(years, end_series, marker="o", color="#E45756", label="files by end year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of files")
    ax.set_title("Files per Year: Start vs End Dates")
    ax.set_xticks(years)
    if years:
        ax.set_xlim(years[0] - 0.5, years[-1] + 0.5)
    ax.legend(loc="best")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "line_num_files_per_year_start_vs_end.png"), dpi=150)
    plt.close(fig)

    # 7) Correlation: Coverage Period vs Salary Tables & Benefits (plots removed per request)
    if args.coverage and os.path.exists(args.coverage):
        cov = load_coverage(args.coverage)
        if "cao_number" in cov.columns and "coverage_months" in cov.columns:
            per_cao = file_df.groupby("cao_number").agg({
                "salary_table_count": "mean",
                **{b: "mean" for b in benefit_cols}
            }).reset_index()
            per_cao[benefit_cols] = per_cao[benefit_cols].astype(float)
            per_cao["avg_benefits"] = per_cao[benefit_cols].sum(axis=1)
            per_cao.rename(columns={"salary_table_count": "avg_salary_tables"}, inplace=True)

            merged = pd.merge(cov[["cao_number", "coverage_months"]], per_cao[["cao_number", "avg_salary_tables", "avg_benefits"]], on="cao_number", how="inner")

    # 8) Benefit Richness by Sector (if sector available)
    sector_col = find_column(df, ["sector"]) or ("sector" if "sector" in df.columns else None)
    if sector_col:
        df.rename(columns={sector_col: "sector"}, inplace=True)
        # Average number of benefits per file
        benefit_presence_file = presence[benefit_cols].sum(axis=1)
        sector_stats = pd.DataFrame({
            "sector": df["sector"],
            "benefit_count": benefit_presence_file,
        })
        sector_cao_stats = sector_stats.join(df[["cao_number"]])
        # Average per CAO first
        per_cao_sector = sector_cao_stats.groupby(["sector", "cao_number"]) ["benefit_count"].mean().reset_index()
        by_sector = per_cao_sector.groupby("sector")["benefit_count"].mean().reset_index().rename(columns={"benefit_count": "avg_benefits_per_cao"})
        out_xlsx = os.path.join(tables_dir, "benefit_richness_by_sector.xlsx")
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            info_rows = [
                {"section": "Description", "text": "Average number of benefits per CAO within each sector."},
                {"section": "", "text": ""},
                {"section": "Column descriptions", "text": ""},
            ]
            pd.DataFrame(info_rows).to_excel(writer, index=False, sheet_name="README")
            desc_map = {
                "sector": "Sector name.",
                "avg_benefits_per_cao": "Average benefit count per CAO (file-level presence averaged within CAO, then averaged within sector).",
            }
            pd.DataFrame({
                "column": list(by_sector.columns),
                "description": [desc_map.get(c, "") for c in by_sector.columns],
            }).to_excel(writer, index=False, sheet_name="README", startrow=len(info_rows) + 2)
            by_sector.to_excel(writer, index=False, sheet_name="Data")

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.bar(by_sector["sector"], by_sector["avg_benefits_per_cao"], color="#72B7B2")
        ax.set_xlabel("Sector")
        ax.set_ylabel("Average benefit count per CAO")
        ax.set_title("Benefit Richness by Sector")
        plt.xticks(rotation=20)
        plt.tight_layout()
        plt.savefig(os.path.join(plots_dir, "bar_benefit_richness_by_sector.png"), dpi=150)
        plt.close(fig)

    print(f"Saved Part 2 plots to: {plots_dir}")
    print(f"Saved Part 2 tables to: {tables_dir}")


if __name__ == "__main__":
    main()


