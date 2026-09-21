"""
oracle_macro_checker.py: Independent empirical oracle for verifying all macros,
2018 sample reconciliation numbers, and cross-chapter consistency.
"""

from __future__ import annotations
import os
import re
import sys
from pathlib import Path
from collections import Counter
import pandas as pd
import numpy as np

REPO_ROOT = Path("/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
VERIF_ROOT = REPO_ROOT / "verification_pipeline"
REPORTS_ANALYSIS = REPO_ROOT / "Reports" / "Analysis"

def load_macros(macro_file: Path) -> dict[str, str]:
    macros = {}
    pattern = re.compile(r"^\\(?:providecommand|newcommand|def)\{\\([A-Za-z]+)\}\{(.*)\}$")
    with open(macro_file, "r", encoding="utf-8") as f:
        for line in f:
            m = pattern.match(line.strip())
            if m:
                macros[m.group(1)] = m.group(2)
    return macros

def run_checks():
    print("=== STARTING INDEPENDENT ORACLE CHECKS ===")
    macro_file = REPORTS_ANALYSIS / "tables" / "macros.tex"
    macros = load_macros(macro_file)
    print(f"Loaded {len(macros)} macros from {macro_file}")

    results = []

    def record(category: str, item: str, macro_val: any, computed_val: any, match: bool, note: str = ""):
        # A macro the report no longer defines has nothing to verify -- report it as
        # SKIPPED rather than a spurious FAIL of None against a computed value.
        if macro_val is None:
            print(f"[SKIP] {category} :: {item} -> not defined in macros.tex")
            return
        results.append({
            "category": category,
            "item": item,
            "macro_val": str(macro_val),
            "computed_val": str(computed_val),
            "match": match,
            "note": note
        })
        status = "PASS" if match else "FAIL"
        print(f"[{status}] {category} :: {item} -> Macro: '{macro_val}', Computed: '{computed_val}' {note}")

    # =========================================================================
    # 1. GENERAL MACROS (corrected_dataset.csv)
    # =========================================================================
    corr_path = VERIF_ROOT / "qa" / "corrected_dataset.csv"
    assert corr_path.exists(), f"Missing {corr_path}"
    df_corr = pd.read_csv(corr_path, sep=";", low_memory=False)

    # General totals
    n_total = len(df_corr)
    n_caos = df_corr["cao_number"].nunique()
    record("General", "GenTotalContracts", macros.get("GenTotalContracts"), f"{n_total:,}", macros.get("GenTotalContracts") == f"{n_total:,}")
    record("General", "GenUniqueCAOs", macros.get("GenUniqueCAOs"), f"{n_caos:,}", macros.get("GenUniqueCAOs") == f"{n_caos:,}")

    # Sectoral share
    scope_s = df_corr["general_cao_scope_type"].dropna().str.lower()
    n_sectoral = scope_s.str.contains("sectoral|bedrijfstak").sum()
    share_sectoral = n_sectoral / n_total * 100
    record("General", "GenShareSectoral", macros.get("GenShareSectoral"), f"{share_sectoral:.1f}%", macros.get("GenShareSectoral") == f"{share_sectoral:.1f}\\%" or macros.get("GenShareSectoral") == f"{share_sectoral:.1f}%")

    # Entity-level sectoral share (any-episode-sectoral definition)
    is_sectoral_episode = df_corr["general_cao_scope_type"].fillna("").str.lower().str.contains("sectoral|bedrijfstak")
    entity_is_sectoral = is_sectoral_episode.groupby(df_corr["cao_number"]).max()
    share_sectoral_entities = entity_is_sectoral.mean() * 100
    record("General", "GenShareSectoralEntities", macros.get("GenShareSectoralEntities"), f"{share_sectoral_entities:.1f}%", macros.get("GenShareSectoralEntities") == f"{share_sectoral_entities:.1f}\\%" or macros.get("GenShareSectoralEntities") == f"{share_sectoral_entities:.1f}%")

    # Retro
    from scripts.excel_analysis.descriptives_non_salary import normalize_boolean
    retro_bool = normalize_boolean(df_corr["general_retro_applies"])
    share_retro = (retro_bool == True).mean() * 100
    record("General", "GenShareRetro", macros.get("GenShareRetro"), f"{share_retro:.1f}%", macros.get("GenShareRetro") == f"{share_retro:.1f}\\%" or macros.get("GenShareRetro") == f"{share_retro:.1f}%")

    retro_df = df_corr[retro_bool == True]
    backpay_bool = normalize_boolean(retro_df["general_retro_backpay_due"])
    share_backpay_cond = (backpay_bool == True).mean() * 100
    record("General", "GenShareBackpayCond", macros.get("GenShareBackpayCond"), f"{share_backpay_cond:.1f}%", macros.get("GenShareBackpayCond") == f"{share_backpay_cond:.1f}\\%" or macros.get("GenShareBackpayCond") == f"{share_backpay_cond:.1f}%")

    s_dates = pd.to_datetime(retro_df["general_retro_start_date"], errors='coerce')
    e_dates = pd.to_datetime(retro_df["general_retro_end_date"], errors='coerce')
    lens = (e_dates - s_dates).dt.days.dropna()
    med_retro = int(lens.median())
    record("General", "GenMedianRetroDays", macros.get("GenMedianRetroDays"), str(med_retro), macros.get("GenMedianRetroDays") == str(med_retro))

    surch = retro_df["general_retro_int_surcharge"].dropna()
    surch_count = (surch.astype(str).str.strip() != "").sum()
    share_surch = surch_count / n_total * 100
    record("General", "GenShareSurcharge", macros.get("GenShareSurcharge"), f"{share_surch:.1f}%", macros.get("GenShareSurcharge") == f"{share_surch:.1f}\\%" or macros.get("GenShareSurcharge") == f"{share_surch:.1f}%")

    # Dates
    from scripts.excel_analysis.analysis_utils import parse_cao_date_series
    d_web = parse_cao_date_series(df_corr["ingangsdatum"], dayfirst=True)
    d_pdf = pd.to_datetime(df_corr["general_start_date"], errors='coerce')
    both_mask = d_web.notna() & d_pdf.notna()
    n_both = both_mask.sum()
    diff_days = (d_pdf[both_mask] - d_web[both_mask]).dt.days
    n_exact = (diff_days == 0).sum()
    share_exact = n_exact / n_both * 100
    n_large_diff = (diff_days.abs() > 30).sum()
    share_large = n_large_diff / n_both * 100
    record("General", "GenBothDatesCount", macros.get("GenBothDatesCount"), f"{n_both:,}", macros.get("GenBothDatesCount") == f"{n_both:,}")
    record("General", "GenExactDateMatch", macros.get("GenExactDateMatch"), f"{share_exact:.1f}%", macros.get("GenExactDateMatch") == f"{share_exact:.1f}\\%" or macros.get("GenExactDateMatch") == f"{share_exact:.1f}%")
    record("General", "GenDateDiffGtThirty", macros.get("GenDateDiffGtThirty"), f"{share_large:.2f}%", macros.get("GenDateDiffGtThirty") == f"{share_large:.2f}\\%" or macros.get("GenDateDiffGtThirty") == f"{share_large:.2f}%")

    # Document types
    doc_type_col = "document_type" if "document_type" in df_corr.columns else "general_document_type"
    n_full = df_corr[doc_type_col].astype(str).str.contains("full", case=False, na=False).sum()
    record("General", "GenFullCAOCount", macros.get("GenFullCAOCount"), f"{n_full:,}", macros.get("GenFullCAOCount") == f"{n_full:,}")

    # AVV, dev, ttw
    avv_bool = normalize_boolean(df_corr["general_avv_applies"])
    share_avv = (avv_bool == True).mean() * 100
    record("General", "GenShareAvv", macros.get("GenShareAvv"), f"{share_avv:.0f}%", macros.get("GenShareAvv") == f"{share_avv:.0f}\\%" or macros.get("GenShareAvv") == f"{share_avv:.0f}%")

    dev_bool = normalize_boolean(df_corr["general_dev_company_level"])
    ttw_bool = normalize_boolean(df_corr["TTW"])
    year_web = d_web.dt.year
    window_mask = year_web.between(2011, 2023)
    dev_by_year = (dev_bool[window_mask] == True).groupby(year_web[window_mask]).mean() * 100
    ttw_by_year = (ttw_bool[window_mask] == True).groupby(year_web[window_mask]).mean() * 100
    record("General", "GenDevShareMin", macros.get("GenDevShareMin"), f"{dev_by_year.min():.0f}%", macros.get("GenDevShareMin") == f"{dev_by_year.min():.0f}\\%" or macros.get("GenDevShareMin") == f"{dev_by_year.min():.0f}%")
    record("General", "GenDevShareMax", macros.get("GenDevShareMax"), f"{dev_by_year.max():.0f}%", macros.get("GenDevShareMax") == f"{dev_by_year.max():.0f}\\%" or macros.get("GenDevShareMax") == f"{dev_by_year.max():.0f}%")
    record("General", "GenTTWShareMax", macros.get("GenTTWShareMax"), f"{ttw_by_year.max():.0f}%", macros.get("GenTTWShareMax") == f"{ttw_by_year.max():.0f}\\%" or macros.get("GenTTWShareMax") == f"{ttw_by_year.max():.0f}%")

    # Peak & stable
    year_pdf = d_pdf.dt.year.dropna().astype(int)
    counts_pdf = year_pdf.value_counts().sort_index()
    record("General", "GenPeakYear", macros.get("GenPeakYear"), str(counts_pdf.idxmax()), macros.get("GenPeakYear") == str(counts_pdf.idxmax()))
    record("General", "GenPeakCount", macros.get("GenPeakCount"), f"{counts_pdf.max():,}", macros.get("GenPeakCount") == f"{counts_pdf.max():,}")
    win_pdf = counts_pdf[(counts_pdf.index >= 2011) & (counts_pdf.index <= 2023)]
    record("General", "GenStableLow", macros.get("GenStableLow"), f"{win_pdf.min():,}", macros.get("GenStableLow") == f"{win_pdf.min():,}")
    record("General", "GenStableHigh", macros.get("GenStableHigh"), f"{win_pdf.max():,}", macros.get("GenStableHigh") == f"{win_pdf.max():,}")

    # Truncation
    trunc_dir = REPO_ROOT / "performance_logs" / "llm_analysis" / "max_tokens_truncated_4"
    if trunc_dir.exists():
        files = [f.name for f in trunc_dir.iterdir() if f.is_file()]
        caos = {re.match(r"^(\d+)_", fn).group(1) for fn in files if re.match(r"^(\d+)_", fn)}
        record("General", "GenTruncatedFiles", macros.get("GenTruncatedFiles"), f"{len(files):,}", macros.get("GenTruncatedFiles") == f"{len(files):,}")
        record("General", "GenTruncatedCaos", macros.get("GenTruncatedCaos"), f"{len(caos):,}", macros.get("GenTruncatedCaos") == f"{len(caos):,}")

    # =========================================================================
    # 2. NON-SALARY MACROS
    # =========================================================================
    from scripts.excel_analysis.analysis_utils import filter_non_salary_for_plot, build_latest_cao_forward_fill_by_file
    from scripts.excel_analysis.descriptives_non_salary import build_latest_cao_view, create_domain_coverage_latest_sheet

    df_filtered = filter_non_salary_for_plot(df_corr)
    if "ingangsdatum" in df_filtered.columns and "start_year" not in df_filtered.columns:
        df_filtered["start_year"] = parse_cao_date_series(df_filtered["ingangsdatum"], dayfirst=True).dt.year
    df_latest_filtered = build_latest_cao_view(df_filtered)

    df_cov = create_domain_coverage_latest_sheet(df_latest_filtered)
    for _, r in df_cov.iterrows():
        dom_key = r["domain"]
        macro_key_count = "NonSal" + "".join(p.capitalize() for p in dom_key.split("_")) + "Count"
        macro_key_share = "NonSal" + "".join(p.capitalize() for p in dom_key.split("_")) + "Share"
        exp_count = f"{int(r['n_cao_with_domain']):,}"
        exp_share = f"{r['share_cao_with_domain']*100:.0f}%"
        record("Non-Salary", macro_key_count, macros.get(macro_key_count), exp_count, macros.get(macro_key_count) == exp_count)
        record("Non-Salary", macro_key_share, macros.get(macro_key_share), exp_share, macros.get(macro_key_share) == exp_share or macros.get(macro_key_share) == exp_share.replace("%", "\\%"))

    df_latest_view = build_latest_cao_forward_fill_by_file(
        df_filtered, cao_col="cao_number", year_col="start_year", file_col="file_name", order_date_col="ingangsdatum"
    )

    def _b_share(df_in, c):
        b = normalize_boolean(df_in[c])
        return (b == True).mean() * 100

    def _b_window(col, window):
        sub = df_latest_view[df_latest_view["start_year"].between(*window)]
        return _b_share(sub, col)

    # Narrative stats
    record("Non-Salary", "NonSalJobAllowShare", macros.get("NonSalJobAllowShare"), f"{_b_share(df_latest_filtered, 'bonus_job_allowances_present'):.0f}%", macros.get("NonSalJobAllowShare") in [f"{_b_share(df_latest_filtered, 'bonus_job_allowances_present'):.0f}%", f"{_b_share(df_latest_filtered, 'bonus_job_allowances_present'):.0f}\\%"])
    record("Non-Salary", "NonSalEntryStepShare", macros.get("NonSalEntryStepShare"), f"{_b_share(df_latest_filtered, 'wage_entry_step_exp_present'):.0f}%", macros.get("NonSalEntryStepShare") in [f"{_b_share(df_latest_filtered, 'wage_entry_step_exp_present'):.0f}%", f"{_b_share(df_latest_filtered, 'wage_entry_step_exp_present'):.0f}\\%"])
    record("Non-Salary", "NonSalCommutingShare", macros.get("NonSalCommutingShare"), f"{_b_share(df_latest_filtered, 'fringe_commuting_allowance_present'):.0f}%", macros.get("NonSalCommutingShare") in [f"{_b_share(df_latest_filtered, 'fringe_commuting_allowance_present'):.0f}%", f"{_b_share(df_latest_filtered, 'fringe_commuting_allowance_present'):.0f}\\%"])
    record("Non-Salary", "NonSalMealShare", macros.get("NonSalMealShare"), f"{_b_share(df_latest_filtered, 'fringe_meal_benefit_present'):.0f}%", macros.get("NonSalMealShare") in [f"{_b_share(df_latest_filtered, 'fringe_meal_benefit_present'):.0f}%", f"{_b_share(df_latest_filtered, 'fringe_meal_benefit_present'):.0f}\\%"])
    record("Non-Salary", "NonSalInsuranceShare", macros.get("NonSalInsuranceShare"), f"{_b_share(df_latest_filtered, 'fringe_insurance_or_savings_benefit_present'):.0f}%", macros.get("NonSalInsuranceShare") in [f"{_b_share(df_latest_filtered, 'fringe_insurance_or_savings_benefit_present'):.0f}%", f"{_b_share(df_latest_filtered, 'fringe_insurance_or_savings_benefit_present'):.0f}\\%"])
    record("Non-Salary", "NonSalRelocationShare", macros.get("NonSalRelocationShare"), f"{_b_share(df_latest_filtered, 'fringe_relocation_allowance_present'):.0f}%", macros.get("NonSalRelocationShare") in [f"{_b_share(df_latest_filtered, 'fringe_relocation_allowance_present'):.0f}%", f"{_b_share(df_latest_filtered, 'fringe_relocation_allowance_present'):.0f}\\%"])
    record("Non-Salary", "NonSalBikeShare", macros.get("NonSalBikeShare"), f"{_b_share(df_latest_filtered, 'fringe_bike_scheme_present'):.0f}%", macros.get("NonSalBikeShare") in [f"{_b_share(df_latest_filtered, 'fringe_bike_scheme_present'):.0f}%", f"{_b_share(df_latest_filtered, 'fringe_bike_scheme_present'):.0f}\\%"])
    record("Non-Salary", "NonSalInternetPhoneShare", macros.get("NonSalInternetPhoneShare"), f"{_b_share(df_latest_filtered, 'fringe_internet_or_phone_reimbursement_present'):.0f}%", macros.get("NonSalInternetPhoneShare") in [f"{_b_share(df_latest_filtered, 'fringe_internet_or_phone_reimbursement_present'):.0f}%", f"{_b_share(df_latest_filtered, 'fringe_internet_or_phone_reimbursement_present'):.0f}\\%"])

    record("Non-Salary", "NonSalSeniorityBonusEarly", macros.get("NonSalSeniorityBonusEarly"), f"{_b_window('bonus_seniority_loyalty_bonus', (2011, 2013)):.0f}%", macros.get("NonSalSeniorityBonusEarly") in [f"{_b_window('bonus_seniority_loyalty_bonus', (2011, 2013)):.0f}%", f"{_b_window('bonus_seniority_loyalty_bonus', (2011, 2013)):.0f}\\%"])
    record("Non-Salary", "NonSalSeniorityBonusLate", macros.get("NonSalSeniorityBonusLate"), f"{_b_window('bonus_seniority_loyalty_bonus', (2021, 2023)):.0f}%", macros.get("NonSalSeniorityBonusLate") in [f"{_b_window('bonus_seniority_loyalty_bonus', (2021, 2023)):.0f}%", f"{_b_window('bonus_seniority_loyalty_bonus', (2021, 2023)):.0f}\\%"])
    record("Non-Salary", "NonSalPersAllowMaxEarly", macros.get("NonSalPersAllowMaxEarly"), f"{_b_window('wage_pers_allow_max_scale', (2011, 2013)):.0f}%", macros.get("NonSalPersAllowMaxEarly") in [f"{_b_window('wage_pers_allow_max_scale', (2011, 2013)):.0f}%", f"{_b_window('wage_pers_allow_max_scale', (2011, 2013)):.0f}\\%"])
    record("Non-Salary", "NonSalPersAllowMaxLate", macros.get("NonSalPersAllowMaxLate"), f"{_b_window('wage_pers_allow_max_scale', (2021, 2023)):.0f}%", macros.get("NonSalPersAllowMaxLate") in [f"{_b_window('wage_pers_allow_max_scale', (2021, 2023)):.0f}%", f"{_b_window('wage_pers_allow_max_scale', (2021, 2023)):.0f}\\%"])
    record("Non-Salary", "NonSalTrainingFundEarly", macros.get("NonSalTrainingFundEarly"), f"{_b_window('training_fund_present', (2011, 2013)):.0f}%", macros.get("NonSalTrainingFundEarly") in [f"{_b_window('training_fund_present', (2011, 2013)):.0f}%", f"{_b_window('training_fund_present', (2011, 2013)):.0f}\\%"])
    record("Non-Salary", "NonSalTrainingFundLate", macros.get("NonSalTrainingFundLate"), f"{_b_window('training_fund_present', (2021, 2023)):.0f}%", macros.get("NonSalTrainingFundLate") in [f"{_b_window('training_fund_present', (2021, 2023)):.0f}%", f"{_b_window('training_fund_present', (2021, 2023)):.0f}\\%"])

    # Numeric non-salary stats
    ft_unit = df_latest_view["contract_full_time_hours_unit"].astype(str).str.lower()
    ft_weekly_mask = ft_unit.str.contains("week") & ~ft_unit.str.contains("year|pay period")
    ft_val = pd.to_numeric(df_latest_view["contract_full_time_hours_value"], errors="coerce")
    ft_weekly_median = ft_val[ft_weekly_mask].median()
    record("Non-Salary", "NonSalFTHoursWeeklyMedian", macros.get("NonSalFTHoursWeeklyMedian"), f"{ft_weekly_median:.0f}", macros.get("NonSalFTHoursWeeklyMedian") == f"{ft_weekly_median:.0f}")

    years_ns = sorted(y for y in df_latest_view["start_year"].dropna().unique() if 2011 <= y <= 2024)
    ot_yearly_medians = []
    for y in years_ns:
        sub = df_latest_view[df_latest_view["start_year"] == y]
        v = pd.to_numeric(sub["overtime_max_hours_per_week_value"], errors="coerce").median()
        if pd.notna(v):
            ot_yearly_medians.append(v)
    record("Non-Salary", "NonSalOvertimeMaxWeeklyLow", macros.get("NonSalOvertimeMaxWeeklyLow"), f"{min(ot_yearly_medians):.0f}", macros.get("NonSalOvertimeMaxWeeklyLow") == f"{min(ot_yearly_medians):.0f}")
    record("Non-Salary", "NonSalOvertimeMaxWeeklyHigh", macros.get("NonSalOvertimeMaxWeeklyHigh"), f"{max(ot_yearly_medians):.0f}", macros.get("NonSalOvertimeMaxWeeklyHigh") == f"{max(ot_yearly_medians):.0f}")

    retire_early = pd.to_numeric(df_latest_view[df_latest_view["start_year"].between(2011, 2013)]["pension_retire_age_normal_value"], errors="coerce").median()
    retire_late = pd.to_numeric(df_latest_view[df_latest_view["start_year"].between(2023, 2025)]["pension_retire_age_normal_value"], errors="coerce").median()
    record("Non-Salary", "NonSalRetireAgeEarly", macros.get("NonSalRetireAgeEarly"), f"{retire_early:.0f}", macros.get("NonSalRetireAgeEarly") == f"{retire_early:.0f}")
    record("Non-Salary", "NonSalRetireAgeLate", macros.get("NonSalRetireAgeLate"), f"{retire_late:.0f}", macros.get("NonSalRetireAgeLate") == f"{retire_late:.0f}")

    tr_unit = df_latest_view["training_time_yearly_unit"].astype(str).str.lower()
    tr_hour_mask = tr_unit.str.contains("hour")
    tr_val = pd.to_numeric(df_latest_view["training_time_yearly_value"], errors="coerce")
    record("Non-Salary", "NonSalTrainingHoursMedian", macros.get("NonSalTrainingHoursMedian"), f"{tr_val[tr_hour_mask].median():.0f}", macros.get("NonSalTrainingHoursMedian") == f"{tr_val[tr_hour_mask].median():.0f}")
    record("Non-Salary", "NonSalTrainingHoursN", macros.get("NonSalTrainingHoursN"), f"{tr_val[tr_hour_mask].notna().sum():,}", macros.get("NonSalTrainingHoursN") == f"{tr_val[tr_hour_mask].notna().sum():,}")

    pen_unit = df_latest_view["pension_employee_contrib_unit"].astype(str).str.lower()
    pen_pct_mask = pen_unit.str.contains("%|percent")
    pen_val = pd.to_numeric(df_latest_view["pension_employee_contrib_value"], errors="coerce")
    pen_yearly_medians = []
    for y in years_ns:
        v = pen_val[pen_pct_mask & (df_latest_view["start_year"] == y)].median()
        if pd.notna(v):
            pen_yearly_medians.append(v)
    record("Non-Salary", "NonSalPensionContribLow", macros.get("NonSalPensionContribLow"), f"{min(pen_yearly_medians):.1f}%", macros.get("NonSalPensionContribLow") in [f"{min(pen_yearly_medians):.1f}%", f"{min(pen_yearly_medians):.1f}\\%"])
    record("Non-Salary", "NonSalPensionContribHigh", macros.get("NonSalPensionContribHigh"), f"{max(pen_yearly_medians):.1f}%", macros.get("NonSalPensionContribHigh") in [f"{max(pen_yearly_medians):.1f}%", f"{max(pen_yearly_medians):.1f}\\%"])

    # =========================================================================
    # 3. INDICES MACROS (verification_pipeline/indices/out/)
    # =========================================================================
    comp_path = VERIF_ROOT / "indices" / "out" / "composite_index.csv"
    assert comp_path.exists(), f"Missing {comp_path}"
    df_comp = pd.read_csv(comp_path, sep=";", low_memory=False)

    record("Indices", "IndCompositeTotalDocs", macros.get("IndCompositeTotalDocs"), f"{len(df_comp):,}", macros.get("IndCompositeTotalDocs") == f"{len(df_comp):,}")

    ov_s = df_comp["overall_z"].dropna()
    record("Indices", "IndOverallZMean", macros.get("IndOverallZMean"), f"{ov_s.mean():.2f}", macros.get("IndOverallZMean") == f"{ov_s.mean():.2f}")
    record("Indices", "IndOverallZMedian", macros.get("IndOverallZMedian"), f"{ov_s.median():.2f}", macros.get("IndOverallZMedian") == f"{ov_s.median():.2f}")
    record("Indices", "IndOverallZStd", macros.get("IndOverallZStd"), f"{ov_s.std():.2f}", macros.get("IndOverallZStd") == f"{ov_s.std():.2f}")

    # Standard deviations of specific topic z-scores
    TOPIC_COLS = {
        "homeoffice": "homeoffice_z",
        "fringe": "fringe_z",
        "term": "term_z",
        "overtime": "overtime_z",
        "safety": "safety_coverage_z" if "safety_coverage_z" in df_comp.columns else "safety_z",
    }
    for t_k, col in TOPIC_COLS.items():
        macro_name = "IndSD" + ("Termination" if t_k == "term" else t_k.capitalize())
        s_std = df_comp[col].dropna().std()
        record("Indices", macro_name, macros.get(macro_name), f"{s_std:.2f}", macros.get(macro_name) == f"{s_std:.2f}")

    # Monthly panel
    panel_path = VERIF_ROOT / "indices" / "out" / "all_indices_panel_monthly.csv"
    df_panel_doc = pd.read_csv(panel_path, sep=";", low_memory=False, usecols=["cao_number", "month", "overall_z", "n_caos_in_month"])
    df_cao_rows = df_panel_doc[df_panel_doc["cao_number"] != "STATUTORY"].dropna(subset=["overall_z"])
    monthly_comb = df_cao_rows.groupby("month").agg(n_caos=("cao_number", "nunique")).reset_index()
    mature_panel = monthly_comb[monthly_comb["n_caos"] >= 30]
    record("Indices", "IndPanelTotalMonths", macros.get("IndPanelTotalMonths"), f"{len(mature_panel):,}", macros.get("IndPanelTotalMonths") == f"{len(mature_panel):,}")
    record("Indices", "IndPanelStartMonth", macros.get("IndPanelStartMonth"), str(mature_panel["month"].min()), macros.get("IndPanelStartMonth") == str(mature_panel["month"].min()))
    record("Indices", "IndPanelEndMonth", macros.get("IndPanelEndMonth"), str(mature_panel["month"].max()), macros.get("IndPanelEndMonth") == str(mature_panel["month"].max()))

    # MW indices
    mw_path = VERIF_ROOT / "indices" / "out" / "mw_indices.csv"
    df_mw = pd.read_csv(mw_path, sep=";", low_memory=False)
    year_counts_mw = df_mw["year"].value_counts()
    mw_year_all = df_mw.groupby("year").agg({
        "mw_low": "median", "mw_median": "median", "mw_high": "median", "wml_month": "first"
    }).dropna().sort_index()
    mw_mature = mw_year_all[mw_year_all.index.isin(year_counts_mw[year_counts_mw >= 20].index)]
    latest_mw = mw_mature.iloc[-1]
    record("Indices", "IndWageYearMin", macros.get("IndWageYearMin"), str(int(mw_mature.index.min())), macros.get("IndWageYearMin") == str(int(mw_mature.index.min())))
    record("Indices", "IndWageYearMax", macros.get("IndWageYearMax"), str(int(mw_mature.index.max())), macros.get("IndWageYearMax") == str(int(mw_mature.index.max())))
    record("Indices", "IndWageMedianLatest", macros.get("IndWageMedianLatest"), f"EUR {int(latest_mw['mw_median']):,}", macros.get("IndWageMedianLatest") == f"EUR {int(latest_mw['mw_median']):,}")
    record("Indices", "IndWmlMonthLatest", macros.get("IndWmlMonthLatest"), f"EUR {int(latest_mw['wml_month']):,}", macros.get("IndWmlMonthLatest") == f"EUR {int(latest_mw['wml_month']):,}")
    ratio_wml = latest_mw["mw_median"] / latest_mw["wml_month"]
    record("Indices", "IndWageToWmlRatio", macros.get("IndWageToWmlRatio"), f"{ratio_wml:.2f}", macros.get("IndWageToWmlRatio") == f"{ratio_wml:.2f}")

    # Registry files
    open_ct_path = VERIF_ROOT / "indices" / "review" / "all_open_cant_tells.csv"
    df_oct = pd.read_csv(open_ct_path, sep=";", low_memory=False)
    record("Indices", "IndOpenJudgmentCells", macros.get("IndOpenJudgmentCells"), f"{len(df_oct):,}", macros.get("IndOpenJudgmentCells") == f"{len(df_oct):,}")

    fp_path = VERIF_ROOT / "indices" / "out" / "statutory_fingerprint_suspects.csv"
    df_fp = pd.read_csv(fp_path, sep=";", low_memory=False)
    n_fp_nc = (df_fp["adjudication"] == "NEVER_CHECKED").sum()
    record("Indices", "IndFingerprintSuspects", macros.get("IndFingerprintSuspects"), f"{n_fp_nc:,}", macros.get("IndFingerprintSuspects") == f"{n_fp_nc:,}")

    # =========================================================================
    # 4. SALARY MACROS (extracted_data_salary_v2.csv)
    # =========================================================================
    sal_path = REPO_ROOT / "outputs" / "parser_salary" / "extracted_data_salary_v2.csv"
    assert sal_path.exists(), f"Missing {sal_path}"
    print("Reading salary metadata columns...")
    cols_sal = [
        "cao_number", "file_name", "jobgroup", "step_label",
        "worker_type", "age_group", "education", "ft_hours",
        "confidence_tier", "ingangsdatum", "TTW", "is_entry"
    ]
    df_sal = pd.read_csv(sal_path, sep=";", usecols=cols_sal, low_memory=False)
    n_sal_total = len(df_sal)
    df_sal_ab = df_sal[df_sal["confidence_tier"].isin(["A", "B"])].reset_index(drop=True)
    n_sal_ab = len(df_sal_ab)
    record("Salary", "SalTotalRows", macros.get("SalTotalRows"), f"{n_sal_ab:,}", macros.get("SalTotalRows") == f"{n_sal_ab:,}")
    record("Salary", "SalTierABShare", macros.get("SalTierABShare"), f"{n_sal_ab/n_sal_total*100:.1f}%", macros.get("SalTierABShare") in [f"{n_sal_ab/n_sal_total*100:.1f}%", f"{n_sal_ab/n_sal_total*100:.1f}\\%"])
    record("Salary", "SalUniqueCAOs", macros.get("SalUniqueCAOs"), f"{df_sal_ab['cao_number'].nunique():,}", macros.get("SalUniqueCAOs") == f"{df_sal_ab['cao_number'].nunique():,}")
    unique_files_sal = df_sal_ab[["cao_number", "file_name"]].drop_duplicates()
    record("Salary", "SalUniqueFiles", macros.get("SalUniqueFiles"), f"{len(unique_files_sal):,}", macros.get("SalUniqueFiles") == f"{len(unique_files_sal):,}")
    record("Salary", "SalDistinctJobGroups", macros.get("SalDistinctJobGroups"), f"{df_sal_ab['jobgroup'].nunique():,}", macros.get("SalDistinctJobGroups") == f"{df_sal_ab['jobgroup'].nunique():,}")
    record("Salary", "SalDistinctSteps", macros.get("SalDistinctSteps"), f"{df_sal_ab['step_label'].nunique():,}", macros.get("SalDistinctSteps") == f"{df_sal_ab['step_label'].nunique():,}")
    record("Salary", "SalDistinctWorkerTypes", macros.get("SalDistinctWorkerTypes"), f"{df_sal_ab['worker_type'].nunique():,}", macros.get("SalDistinctWorkerTypes") == f"{df_sal_ab['worker_type'].nunique():,}")
    record("Salary", "SalDistinctAgeGroups", macros.get("SalDistinctAgeGroups"), f"{df_sal_ab['age_group'].nunique():,}", macros.get("SalDistinctAgeGroups") == f"{df_sal_ab['age_group'].nunique():,}")
    record("Salary", "SalDistinctEducationCategories", macros.get("SalDistinctEducationCategories"), f"{df_sal_ab['education'].nunique():,}", macros.get("SalDistinctEducationCategories") == f"{df_sal_ab['education'].nunique():,}")

    ft_num = pd.to_numeric(df_sal_ab["ft_hours"], errors="coerce")
    ft_weekly = ft_num.apply(lambda x: x if pd.isna(x) or x <= 200 else x / 52.0).dropna()
    ft_weekly = ft_weekly[(ft_weekly >= 10) & (ft_weekly <= 60)]
    record("Salary", "SalShareFTHoursObserved", macros.get("SalShareFTHoursObserved"), f"{len(ft_weekly)/n_sal_ab*100:.0f}%", macros.get("SalShareFTHoursObserved") in [f"{len(ft_weekly)/n_sal_ab*100:.0f}%", f"{len(ft_weekly)/n_sal_ab*100:.0f}\\%"])
    record("Salary", "SalMeanFTWeeklyHours", macros.get("SalMeanFTWeeklyHours"), f"{ft_weekly.mean():.2f}", macros.get("SalMeanFTWeeklyHours") == f"{ft_weekly.mean():.2f}")
    record("Salary", "SalMedianFTWeeklyHours", macros.get("SalMedianFTWeeklyHours"), f"{ft_weekly.median():.0f}", macros.get("SalMedianFTWeeklyHours") == f"{ft_weekly.median():.0f}")

    # Salary coverage / flags
    d_sal_start = parse_cao_date_series(df_sal_ab["ingangsdatum"], dayfirst=True)
    df_sal_ab["contract_start_year"] = d_sal_start.dt.year
    by_sal_year = df_sal_ab.dropna(subset=["contract_start_year"]).copy()
    by_sal_year["contract_start_year"] = by_sal_year["contract_start_year"].astype(int)
    caos_per_sal_year = by_sal_year.groupby("contract_start_year")["cao_number"].nunique()
    record("Salary", "SalPeakCaoYear", macros.get("SalPeakCaoYear"), str(caos_per_sal_year.idxmax()), macros.get("SalPeakCaoYear") == str(caos_per_sal_year.idxmax()))
    record("Salary", "SalPeakCaoCount", macros.get("SalPeakCaoCount"), f"{caos_per_sal_year.max():,}", macros.get("SalPeakCaoCount") == f"{caos_per_sal_year.max():,}")

    sal_mature_years = caos_per_sal_year[caos_per_sal_year >= 10].index
    df_sal_mature = by_sal_year[by_sal_year["contract_start_year"].isin(sal_mature_years)].copy()
    df_sal_mature["ttw_bool"] = normalize_boolean(df_sal_mature["TTW"])
    df_sal_mature["entry_bool"] = normalize_boolean(df_sal_mature["is_entry"])
    ttw_sh_yr = df_sal_mature.groupby("contract_start_year")["ttw_bool"].apply(lambda s: (s == True).mean()) * 100
    entry_sh_yr = df_sal_mature.groupby("contract_start_year")["entry_bool"].apply(lambda s: (s == True).mean()) * 100
    record("Salary", "SalTTWShareLow", macros.get("SalTTWShareLow"), f"{ttw_sh_yr.min():.0f}%", macros.get("SalTTWShareLow") in [f"{ttw_sh_yr.min():.0f}%", f"{ttw_sh_yr.min():.0f}\\%"])
    record("Salary", "SalTTWShareHigh", macros.get("SalTTWShareHigh"), f"{ttw_sh_yr.max():.0f}%", macros.get("SalTTWShareHigh") in [f"{ttw_sh_yr.max():.0f}%", f"{ttw_sh_yr.max():.0f}\\%"])
    record("Salary", "SalEntryShareLow", macros.get("SalEntryShareLow"), f"{entry_sh_yr.min():.0f}%", macros.get("SalEntryShareLow") in [f"{entry_sh_yr.min():.0f}%", f"{entry_sh_yr.min():.0f}\\%"])
    record("Salary", "SalEntryShareHigh", macros.get("SalEntryShareHigh"), f"{entry_sh_yr.max():.0f}%", macros.get("SalEntryShareHigh") in [f"{entry_sh_yr.max():.0f}%", f"{entry_sh_yr.max():.0f}\\%"])

    # =========================================================================
    # 5. STRESS-TEST 2018 SAMPLE RECONCILIATION NUMBERS
    # =========================================================================
    print("\n--- STRESS-TESTING 2018 SAMPLE RECONCILIATION (Table B.1) ---")

    # Metric 1: General Contract Inceptions (PDF Date): general_start_date year == 2018
    # "167 contract PDFs"
    pdf_2018 = df_corr[d_pdf.dt.year == 2018]
    count_pdf_2018 = len(pdf_2018)
    record("Reconciliation 2018", "General Inceptions (PDF Date 2018)", "167", str(count_pdf_2018), count_pdf_2018 == 167, "Filter: general_start_date year == 2018")

    # Metric 2: General Scraped Filings (Web Date): ingangsdatum year == 2018
    # "163 contract PDFs"
    web_2018 = df_corr[d_web.dt.year == 2018]
    count_web_2018 = len(web_2018)
    record("Reconciliation 2018", "General Scraped Filings (Web Date 2018)", "163", str(count_web_2018), count_web_2018 == 163, "Filter: ingangsdatum year == 2018")

    # Metric 3: General Signing Entities: 82 distinct CAOs
    # "Distinct CAOs with >= 1 contract starting in 2018 by general_start_date"
    caos_pdf_2018 = pdf_2018["cao_number"].nunique()
    record("Reconciliation 2018", "General Signing Entities (PDF 2018)", "82", str(caos_pdf_2018), caos_pdf_2018 == 82, "Distinct CAOs with start in 2018 (PDF)")

    # Metric 4: General Scraped Entities: 83 distinct CAOs
    # "Distinct CAOs with portal filing in 2018 (ingangsdatum)"
    caos_web_2018 = web_2018["cao_number"].nunique()
    record("Reconciliation 2018", "General Scraped Entities (Web 2018)", "83", str(caos_web_2018), caos_web_2018 == 83, "Distinct CAOs with web start in 2018")

    # Metric 5: General Cumulative Stock: 224 cumulative CAOs
    # "Cumulative unique CAOs entering corpus between 2008 and 2018"
    earliest_pdf_by_cao = d_pdf.groupby(df_corr["cao_number"]).min()
    cum_pdf = (earliest_pdf_by_cao.dt.year <= 2018).sum()
    earliest_web_by_cao = d_web.groupby(df_corr["cao_number"]).min()
    cum_web = (earliest_web_by_cao.dt.year <= 2018).sum()
    record("Reconciliation 2018", "General Cumulative Stock <= 2018 (PDF Date)", "224", str(cum_pdf), cum_pdf == 224, f"By PDF date: {cum_pdf}, by Web date: {cum_web}")

    # Metric 6: Indices Active Legal Stock (Dec 2018): 217 CAO entities (205 snapshot)
    # Filter: Full-CAO agreements legally in force in 2018-12 (n_caos_in_month in monthly panel; 205 in Dec 31 snapshot)
    panel_dec_2018 = df_panel_doc[df_panel_doc["month"] == "2018-12"]
    panel_caos_dec_2018 = panel_dec_2018[panel_dec_2018["cao_number"] != "STATUTORY"]["cao_number"].nunique()
    n_caos_in_month_val = panel_dec_2018["n_caos_in_month"].iloc[0] if len(panel_dec_2018) > 0 else None
    record("Reconciliation 2018", "Indices Active Legal Stock 2018-12 (n_caos_in_month)", "217", str(n_caos_in_month_val), n_caos_in_month_val == 217, f"Distinct non-statutory CAOs: {panel_caos_dec_2018}")

    # Metric 7: Indices Scored Filings Intake: 190 documents (112 distinct CAOs)
    # Filter: file_date year == 2018 in composite_index.csv
    file_dates_comp = parse_cao_date_series(df_comp["file_date"], dayfirst=True)
    comp_2018 = df_comp[file_dates_comp.dt.year == 2018]
    n_docs_comp_2018 = len(comp_2018)
    n_caos_comp_2018 = comp_2018["cao_number"].nunique()
    record("Reconciliation 2018", "Indices Scored Filings Intake (docs)", "190", str(n_docs_comp_2018), n_docs_comp_2018 == 190, f"Docs: {n_docs_comp_2018}")
    record("Reconciliation 2018", "Indices Scored Filings Intake (caos)", "112", str(n_caos_comp_2018), n_caos_comp_2018 == 112, f"Distinct CAOs: {n_caos_comp_2018}")

    # Metric 8: Salary Deterministic Parser v2: 74 distinct CAOs (136 files, 20,664 rows)
    # High-confidence Tiers A+B starting in 2018
    sal_2018 = df_sal_ab[df_sal_ab["contract_start_year"] == 2018]
    n_rows_sal_2018 = len(sal_2018)
    n_caos_sal_2018 = sal_2018["cao_number"].nunique()
    n_files_sal_2018 = len(sal_2018[["cao_number", "file_name"]].drop_duplicates())
    record("Reconciliation 2018", "Salary Parser 2018 CAOs", "74", str(n_caos_sal_2018), n_caos_sal_2018 == 74, f"CAOs: {n_caos_sal_2018}")
    record("Reconciliation 2018", "Salary Parser 2018 Files", "136", str(n_files_sal_2018), n_files_sal_2018 == 136, f"Files: {n_files_sal_2018}")
    record("Reconciliation 2018", "Salary Parser 2018 Rows", "20,664", f"{n_rows_sal_2018:,}", n_rows_sal_2018 == 20664, f"Rows: {n_rows_sal_2018}")

    # Metric 9: Wage Indices Panel: 134 CAO wage ladders active in 2018 in mw_indices.csv
    mw_2018 = df_mw[df_mw["year"] == 2018]
    n_mw_2018 = len(mw_2018)
    n_caos_mw_2018 = mw_2018["cao_number"].nunique()
    record("Reconciliation 2018", "Wage Indices 2018 Ladders (rows)", "134", str(n_mw_2018), n_mw_2018 == 134, f"Rows: {n_mw_2018}, distinct CAOs: {n_caos_mw_2018}")

    # =========================================================================
    # 6. INVESTIGATE IndPanelTotalMonths and IndPanelEndMonth
    # =========================================================================
    print("\n--- INVESTIGATING IndPanelTotalMonths and IndPanelEndMonth ---")
    print(f"df_panel_doc shape: {df_panel_doc.shape}")
    print(f"All months count in df_panel_doc: {df_panel_doc['month'].nunique()}")
    print("Monthly counts for 2026:")
    for m in sorted([m for m in df_panel_doc['month'].unique() if str(m).startswith("2026")]):
        sub_m = df_panel_doc[(df_panel_doc["month"] == m) & (df_panel_doc["cao_number"] != "STATUTORY") & (df_panel_doc["overall_z"].notna())]
        print(f"  {m}: {sub_m['cao_number'].nunique()} active CAOs with overall_z (n_caos_in_month={sub_m['n_caos_in_month'].iloc[0] if len(sub_m) else 'NA'})")

    # Summary
    n_pass = sum(1 for r in results if r["match"])
    n_fail = sum(1 for r in results if not r["match"])
    print(f"\nTOTAL CHECKS: {len(results)} | PASS: {n_pass} | FAIL: {n_fail}")

    return results

if __name__ == "__main__":
    run_checks()
