"""
01_general.py: Computes General section statistics, figures, tables, and macros.

Inputs:
    - qa/corrected_dataset.csv (canonical corrected data)
    - CAOsDataExtraction/performance_logs/llm_analysis/max_tokens_truncated_4/
      (the current truncated-extraction folder; folders _1/_2/_3 are superseded funnel stages)

Outputs:
    - Reports/Analysis/figures/non_salary_contract_counts_comparison.png
    - Reports/Analysis/tables/tab_general_summary.tex
    - Reports/Analysis/tables/tab_truncation_concentration.tex
    - Emits macros into Reports/Analysis/tables/macros.tex
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from collections import Counter
import pandas as pd
import numpy as np

# Import common utilities
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

# Import plotting and utility functions from CAOsDataExtraction
from scripts.excel_analysis.analysis_utils import (
    parse_cao_date_series,
)
from scripts.excel_analysis.descriptives_non_salary import (
    normalize_boolean,
)
from scripts.excel_analysis.descriptives_non_salary_plots import (
    plot_contract_counts_comparison,
)

TRUNCATED_FILES_DIR = CAOS_REPO_ROOT / "performance_logs" / "llm_analysis" / "max_tokens_truncated_4"
STABLE_WINDOW_START = 2011  # after the 2008-2010 ramp-up
STABLE_WINDOW_END = 2023    # before the 2024 peak and 2025-2026 right-censoring

def compute_general_stats(df: pd.DataFrame) -> dict:
    """Calculate key general dataset summary statistics."""
    n_total = len(df)
    n_caos = df["cao_number"].nunique() if "cao_number" in df.columns else df["id"].nunique()

    # Sectoral share
    if "general_cao_scope_type" not in df.columns:
        raise KeyError("general_cao_scope_type column missing from corrected dataset")
    scope_s = df["general_cao_scope_type"].dropna().str.lower()
    n_sectoral = scope_s.str.contains("sectoral|bedrijfstak").sum()
    share_sectoral = n_sectoral / n_total if n_total > 0 else 0.0

    # Retroactive provisions
    if "general_retro_applies" not in df.columns:
        raise KeyError("general_retro_applies column missing from corrected dataset")
    retro_bool = normalize_boolean(df["general_retro_applies"])
    n_retro = (retro_bool == True).sum()
    share_retro = n_retro / n_total if n_total > 0 else 0.0

    retro_df = df[retro_bool == True]
    if "general_retro_backpay_due" not in retro_df.columns:
        raise KeyError("general_retro_backpay_due column missing from corrected dataset")
    backpay_bool = normalize_boolean(retro_df["general_retro_backpay_due"])
    share_backpay_cond = (backpay_bool == True).mean() if len(retro_df) > 0 else 0.0

    if "general_retro_start_date" not in retro_df.columns or "general_retro_end_date" not in retro_df.columns:
        raise KeyError("general_retro_start_date/end_date columns missing from corrected dataset")
    s_dates = pd.to_datetime(retro_df["general_retro_start_date"], errors='coerce')
    e_dates = pd.to_datetime(retro_df["general_retro_end_date"], errors='coerce')
    lens = (e_dates - s_dates).dt.days.dropna()
    median_retro_days = int(lens.median()) if len(lens) > 0 else 0

    if "general_retro_int_surcharge" not in retro_df.columns:
        raise KeyError("general_retro_int_surcharge column missing from corrected dataset")
    surch = retro_df["general_retro_int_surcharge"].dropna()
    surch_count = (surch.astype(str).str.strip() != "").sum()
    share_surch = surch_count / n_total if n_total > 0 else 0.0

    # Date comparison: ingangsdatum (website) vs general_start_date (PDF)
    if "general_start_date" not in df.columns or "ingangsdatum" not in df.columns:
        raise KeyError("ingangsdatum/general_start_date columns missing from corrected dataset")
    contract_col = "general_start_date"
    d_web = parse_cao_date_series(df["ingangsdatum"], dayfirst=True)
    d_pdf = pd.to_datetime(df[contract_col], errors='coerce')

    both_mask = d_web.notna() & d_pdf.notna()
    n_both = both_mask.sum()
    diff_days = (d_pdf[both_mask] - d_web[both_mask]).dt.days
    n_exact = (diff_days == 0).sum()
    share_exact = n_exact / n_both if n_both > 0 else 0.0
    n_large_diff = (diff_days.abs() > 30).sum()
    share_large_diff = n_large_diff / n_both if n_both > 0 else 0.0

    # Document types
    if "document_type" not in df.columns and "general_document_type" not in df.columns:
        raise KeyError("document_type/general_document_type column missing from corrected dataset")
    doc_type_col = "document_type" if "document_type" in df.columns else "general_document_type"
    full_mask = df[doc_type_col].astype(str).str.contains("full", case=False, na=False)
    n_full = full_mask.sum()

    # AVV coverage, company-level deviation, TTW -- pooled shares (all contract episodes)
    if "general_avv_applies" not in df.columns:
        raise KeyError("general_avv_applies column missing from corrected dataset")
    avv_bool = normalize_boolean(df["general_avv_applies"])
    share_avv = (avv_bool == True).mean()

    if "general_dev_company_level" not in df.columns:
        raise KeyError("general_dev_company_level column missing from corrected dataset")
    dev_bool = normalize_boolean(df["general_dev_company_level"])

    if "TTW" not in df.columns:
        raise KeyError("TTW column missing from corrected dataset")
    ttw_bool = normalize_boolean(df["TTW"])

    year = d_web.dt.year
    window_mask = year.between(STABLE_WINDOW_START, STABLE_WINDOW_END)
    dev_by_year = (dev_bool[window_mask] == True).groupby(year[window_mask]).mean()
    ttw_by_year = (ttw_bool[window_mask] == True).groupby(year[window_mask]).mean()

    return {
        "n_total": n_total,
        "n_caos": n_caos,
        "share_sectoral": share_sectoral,
        "share_retro": share_retro,
        "share_backpay_cond": share_backpay_cond,
        "median_retro_days": median_retro_days,
        "surch_count": surch_count,
        "share_surch": share_surch,
        "n_both_dates": n_both,
        "share_exact_date": share_exact,
        "share_large_diff": share_large_diff,
        "n_full_cao": n_full,
        "share_avv": share_avv,
        "dev_share_min": dev_by_year.min() if len(dev_by_year) else 0.0,
        "dev_share_max": dev_by_year.max() if len(dev_by_year) else 0.0,
        "ttw_share_max": ttw_by_year.max() if len(ttw_by_year) else 0.0,
    }


def compute_peak_and_stable_range(df: pd.DataFrame) -> dict:
    """Peak contract-count year (PDF start date) and the stable-window count range."""
    d_pdf = pd.to_datetime(df["general_start_date"], errors='coerce')
    year = d_pdf.dt.year.dropna().astype(int)
    counts_by_year = year.value_counts().sort_index()

    peak_year = int(counts_by_year.idxmax())
    peak_count = int(counts_by_year.max())

    window = counts_by_year[(counts_by_year.index >= STABLE_WINDOW_START) & (counts_by_year.index <= STABLE_WINDOW_END)]
    return {
        "peak_year": peak_year,
        "peak_count": peak_count,
        "stable_low": int(window.min()) if len(window) else 0,
        "stable_high": int(window.max()) if len(window) else 0,
    }


def compute_truncation_stats() -> tuple[dict, pd.DataFrame]:
    """Count truncated-extraction files and per-CAO concentration from the current
    (folder-4) truncated-files set. Folders _1/_2/_3 are earlier, superseded funnel stages.
    """
    if not TRUNCATED_FILES_DIR.exists():
        raise FileNotFoundError(f"Truncated-files folder not found: {TRUNCATED_FILES_DIR}")

    filenames = [f.name for f in TRUNCATED_FILES_DIR.iterdir() if f.is_file()]
    cao_numbers = []
    for fn in filenames:
        m = re.match(r"^(\d+)_", fn)
        if m:
            cao_numbers.append(m.group(1))

    n_files = len(filenames)
    n_caos = len(set(cao_numbers))
    counts = Counter(cao_numbers)
    top5 = counts.most_common(5)

    df_top5 = pd.DataFrame(
        [{"CAO Number": cao, "Truncated Files": n} for cao, n in top5]
    )
    return {"n_files": n_files, "n_caos": n_caos, "top5": top5}, df_top5


def main():
    print("=== Running 01_general.py ===")
    setup_matplotlib()

    corr_path = REPO_ROOT / "qa" / "corrected_dataset.csv"

    print(f"Loading corrected dataset: {corr_path}")
    df_corr = pd.read_csv(corr_path, sep=";", low_memory=False)
    assert len(df_corr) == 2739, f"Expected 2,739 rows, got {len(df_corr)}"
    assert df_corr["cao_number"].nunique() == 242, f"Expected 242 CAOs, got {df_corr['cao_number'].nunique()}"

    # 1. Compute general stats
    print("Computing general stats...")
    gen_stats = compute_general_stats(df_corr)

    # 2. Peak / stable-range contract counts
    print("Computing peak and stable contract-count range...")
    peak_stats = compute_peak_and_stable_range(df_corr)

    # 3. Truncation stats
    print("Computing truncation stats...")
    trunc_stats, df_trunc_top5 = compute_truncation_stats()

    # 4. Generate contract counts comparison figure
    print("Generating contract counts figure...")
    plot_contract_counts_comparison(df_corr, FIGURES_DIR)

    # 5. Generate LaTeX table fragments
    print("Emitting tables...")

    # Table 1: General Summary Table
    tab_gen_rows = [
        {"Statistic": "Total contract episodes", "Value": f"{gen_stats['n_total']:,}"},
        {"Statistic": "Unique CAOs", "Value": f"{gen_stats['n_caos']:,}"},
        {"Statistic": "Share sectoral CAOs", "Value": f"{gen_stats['share_sectoral']*100:.2f}\\%"},
        {"Statistic": "Contracts with retroactive application", "Value": f"{gen_stats['share_retro']*100:.1f}\\%"},
        {"Statistic": "Retroactive contracts with backpay (conditional)", "Value": f"{gen_stats['share_backpay_cond']*100:.1f}\\%"},
        {"Statistic": "Median retroactive period", "Value": f"{gen_stats['median_retro_days']} days"},
        {"Statistic": "Contracts with interest/surcharge on backpay", "Value": f"{gen_stats['share_surch']*100:.1f}\\%"},
        {"Statistic": "Contracts with both start dates observed", "Value": f"{gen_stats['n_both_dates']:,}"},
        {"Statistic": "Exact match between website and PDF date", "Value": f"{gen_stats['share_exact_date']*100:.1f}\\%"},
        {"Statistic": "$|$date difference$|>30$ days", "Value": f"{gen_stats['share_large_diff']*100:.2f}\\%"},
        {"Statistic": "Full-CAO files", "Value": f"{gen_stats['n_full_cao']:,}"},
    ]
    df_tab_gen = pd.DataFrame(tab_gen_rows)
    emit_table(df_tab_gen, TABLES_DIR / "tab_general_summary.tex", col_align="lr", headers=["Metric", "Value"], escape=False)

    # Table: Truncation concentration (top 5 CAOs by truncated-file count)
    emit_table(
        df_trunc_top5,
        TABLES_DIR / "tab_truncation_concentration.tex",
        col_align="lr",
        headers=["CAO Number", "Truncated Files"],
        escape=False,
    )

    # 6. Emit macros
    print("Emitting macros to tables/macros.tex...")
    set_macro("GenTotalContracts", f"{gen_stats['n_total']:,}")
    set_macro("GenUniqueCAOs", f"{gen_stats['n_caos']:,}")
    set_macro("GenShareSectoral", f"{gen_stats['share_sectoral']*100:.1f}%")
    set_macro("GenShareRetro", f"{gen_stats['share_retro']*100:.1f}%")
    set_macro("GenShareBackpayCond", f"{gen_stats['share_backpay_cond']*100:.1f}%")
    set_macro("GenMedianRetroDays", f"{gen_stats['median_retro_days']}")
    set_macro("GenShareSurcharge", f"{gen_stats['share_surch']*100:.1f}%")
    set_macro("GenBothDatesCount", f"{gen_stats['n_both_dates']:,}")
    set_macro("GenExactDateMatch", f"{gen_stats['share_exact_date']*100:.1f}%")
    set_macro("GenDateDiffGtThirty", f"{gen_stats['share_large_diff']*100:.2f}%")
    set_macro("GenFullCAOCount", f"{gen_stats['n_full_cao']:,}")
    set_macro("GenShareAvv", f"{gen_stats['share_avv']*100:.0f}%")
    set_macro("GenDevShareMin", f"{gen_stats['dev_share_min']*100:.0f}%")
    set_macro("GenDevShareMax", f"{gen_stats['dev_share_max']*100:.0f}%")
    set_macro("GenTTWShareMax", f"{gen_stats['ttw_share_max']*100:.0f}%")

    set_macro("GenPeakYear", str(peak_stats["peak_year"]))
    set_macro("GenPeakCount", f"{peak_stats['peak_count']:,}")
    set_macro("GenStableLow", f"{peak_stats['stable_low']:,}")
    set_macro("GenStableHigh", f"{peak_stats['stable_high']:,}")
    set_macro("GenStableStartYear", str(STABLE_WINDOW_START))
    set_macro("GenStableEndYear", str(STABLE_WINDOW_END))

    set_macro("GenTruncatedFiles", f"{trunc_stats['n_files']:,}")
    set_macro("GenTruncatedCaos", f"{trunc_stats['n_caos']:,}")

    save_macros()
    print("✓ 01_general.py completed successfully!")

if __name__ == "__main__":
    main()
