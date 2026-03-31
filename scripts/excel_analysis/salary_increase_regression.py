"""
Salary Increase Regression Script

This script estimates fixed-effects regressions for salary increase series and
writes regression tables to CSV files. It runs event-level regressions for
increase_merged_pref_csv, increase_diff_only, and increase_csv_only, plus a
transition-level regression on file-to-file mean increase shifts.

How to run:
    conda run -n caos-extract python scripts/excel_analysis/salary_increase_regression.py

Outputs:
    - outputs/analysis/salary_regression_event_level.csv (sep ';')
    - outputs/analysis/salary_regression_transition_level.csv (sep ';')
    - outputs/analysis/salary_regression_fit_metrics.csv (R², RMSE, one row per model)
    - outputs/analysis/figures/salary_regression/*.png (coefficient plots)
    Each coefficient table includes ``Coefficient``, ``formula``, ``ref_year``,
    ``se_invalid``, inference columns, and sample counts.
"""

from __future__ import annotations

import os
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.excel_analysis.analysis_utils import detect_salary_slot_indices, parse_cao_date_series
from scripts.excel_analysis.salary_increase_derivation import derive_salary_increase_series
from scripts.excel_analysis.salary_regression_plotting import (
    extract_fit_metrics,
    plot_event_regression,
    plot_transition_regression,
)

INPUT_CSV = "outputs/excel/new_results/extracted_data_salary.csv"
OUT_DIR = Path("outputs/analysis")
FIG_DIR = OUT_DIR / "figures" / "salary_regression"
# Minimum events per calendar year kept for FE models (sparse years dropped; logged).
MIN_OBS_PER_YEAR_REGRESSION = 10

_STAT_COLS = ("Estimate", "Std. Error", "t value", "Pr(>|t|)", "2.5%", "97.5%")


@dataclass
class RegressionRunResult:
    """
    Container for one regression run: tidy table, fitted model, and plot metadata.

    Attributes:
        tidy: Coefficient-level DataFrame for CSV export (possibly empty or error stub).
        model: Fitted pyfixest ``Feols``, or None if skipped or failed.
        formula: Estimated formula string.
        ref_year: Omitted factor reference year.
        n_obs: Row count in estimation sample.
        n_clusters_cao: Distinct CAO cluster count.
        years_in_sample: Sorted calendar years entering the factor after filters.
        outcome: Dependent variable name; ``delta_file_mean_increase`` for transition.
        error: Set when estimation raised; else None.
    """

    tidy: pd.DataFrame
    model: Any
    formula: str
    ref_year: int
    n_obs: int
    n_clusters_cao: int
    years_in_sample: List[int]
    outcome: Optional[str] = None
    error: Optional[str] = None


def _usecols_for_salary_derivation(header_cols: List[str]) -> List[str]:
    """
    Build the column subset needed by ``derive_salary_increase_series`` for CSV load.

    Args:
        header_cols: Column names from a header-only read of the salary CSV.

    Returns:
        Ordered unique names present in ``header_cols`` (base ids + salary slot fields).
    """
    base = ("cao_number", "file_name", "ingangsdatum", "ft_hours")
    ordered: List[str] = [c for c in base if c in header_cols]
    slot_suffixes = (
        "start_date",
        "amount",
        "unit",
        "increase_percent",
        "hours_basis_ft_week",
    )
    for k in detect_salary_slot_indices(header_cols):
        for suf in slot_suffixes:
            name = f"salary_{k}_{suf}"
            if name in header_cols and name not in ordered:
                ordered.append(name)
    return ordered


def _read_salary_csv_for_regression(path: str) -> pd.DataFrame:
    """
    Load wide salary CSV restricting to columns the increase derivation reads.

    A header-only pass builds ``usecols``, reducing peak memory and avoiding
    pandas ``DtypeWarning`` from mixed-type inference on unused wide columns.

    Args:
        path: Path to ``extracted_data_salary.csv`` (semicolon-separated).

    Returns:
        Subset-column DataFrame suitable for ``derive_salary_increase_series``.
    """
    header = pd.read_csv(path, sep=";", encoding="utf-8", nrows=0)
    usecols = _usecols_for_salary_derivation(header.columns.tolist())
    if not usecols:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            return pd.read_csv(path, sep=";", encoding="utf-8")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pd.errors.DtypeWarning)
        return pd.read_csv(path, sep=";", encoding="utf-8", usecols=usecols)


def _finalize_regression_tidy(
    tidy: pd.DataFrame,
    *,
    formula: str,
    ref_year: int,
    outcome: Optional[str] = None,
    n_obs: Optional[int] = None,
    n_clusters_cao: Optional[int] = None,
) -> pd.DataFrame:
    """
    Turn pyfixest tidy output into a CSV-ready frame with coefficient names and metadata.

    PyFixEst stores term names in the index (name ``Coefficient``); this function
    resets the index and prepends ``formula``, ``ref_year``, and ``se_invalid``.

    Args:
        tidy: Return value of ``Feols.tidy()`` before ``reset_index``.
        formula: Estimated model formula string.
        ref_year: Omitted reference category for the year factor in the formula.
        outcome: Dependent variable column name for event-level models; omit otherwise.
        n_obs: Estimation sample row count; omitted if None.
        n_clusters_cao: Distinct CAO clusters; omitted if None.

    Returns:
        DataFrame with columns ordered for readability (metadata, estimates, flags, n_obs).
    """
    out = tidy.reset_index()
    if "Std. Error" in out.columns:
        se = pd.to_numeric(out["Std. Error"], errors="coerce")
        se_arr = se.to_numpy(dtype=float)
        out["se_invalid"] = se.isna() | ~np.isfinite(se_arr)
    else:
        out["se_invalid"] = False
    out["formula"] = formula
    out["ref_year"] = ref_year
    if outcome is not None:
        out["outcome"] = outcome
    if n_obs is not None:
        out["n_obs"] = n_obs
    if n_clusters_cao is not None:
        out["n_clusters_cao"] = n_clusters_cao
    preferred: List[str] = []
    for c in ("Coefficient", "outcome", "formula", "ref_year"):
        if c in out.columns:
            preferred.append(c)
    preferred.extend(c for c in _STAT_COLS if c in out.columns)
    for c in ("se_invalid", "error", "n_obs", "n_clusters_cao"):
        if c in out.columns:
            preferred.append(c)
    rest = [c for c in out.columns if c not in preferred]
    return out[preferred + rest]


def run_event_level_regression(df_events: pd.DataFrame, outcome: str) -> RegressionRunResult:
    """
    Run event-level FE regression with CAO and year effects.

    Drops calendar years with fewer than ``MIN_OBS_PER_YEAR_REGRESSION`` events
    (logged). Uses ``i(salary_start_year, ref=<min year in sample>)`` for a
    valid PyFixEst reference level.

    Args:
        df_events: Event-level frame from ``derive_salary_increase_series``.
        outcome: Name of the dependent variable column.

    Returns:
        ``RegressionRunResult`` with tidy coefficients, optional ``Feols`` model,
        and sample years for plotting.
    """
    empty = RegressionRunResult(
        tidy=pd.DataFrame(),
        model=None,
        formula="",
        ref_year=0,
        n_obs=0,
        n_clusters_cao=0,
        years_in_sample=[],
        outcome=outcome,
        error=None,
    )
    data = df_events[df_events[outcome].notna() & df_events["salary_start_year"].notna()].copy()
    if len(data) == 0:
        return empty
    data["salary_start_year"] = data["salary_start_year"].astype(int)
    data["new_file_effect"] = 1 - data["is_first_salary_in_file"]
    year_counts = data.groupby("salary_start_year").size()
    keep_years = year_counts[year_counts >= MIN_OBS_PER_YEAR_REGRESSION].index
    sparse_years = sorted(set(data["salary_start_year"].unique()) - set(keep_years))
    if sparse_years:
        print(
            f"  [INFO] {outcome}: excluding years with <{MIN_OBS_PER_YEAR_REGRESSION} events: {sparse_years}"
        )
    data = data[data["salary_start_year"].isin(keep_years)]
    if len(data) == 0:
        return empty

    ref_year = int(data["salary_start_year"].min())
    formula = (
        f"{outcome} ~ new_file_effect + i(salary_start_year, ref={ref_year}) "
        f"+ new_file_effect:i(salary_start_year) | cao_number"
    )
    n_cao = int(data["cao_number"].nunique())
    n_obs = len(data)
    years_in_sample = sorted(int(y) for y in data["salary_start_year"].unique())
    try:
        import pyfixest as pf

        model = pf.feols(formula, data=data, vcov={"CRV1": "cao_number"})
        tidy = model.tidy()
        tidy_df = _finalize_regression_tidy(
            tidy,
            formula=formula,
            ref_year=ref_year,
            outcome=outcome,
            n_obs=n_obs,
            n_clusters_cao=n_cao,
        )
        return RegressionRunResult(
            tidy=tidy_df,
            model=model,
            formula=formula,
            ref_year=ref_year,
            n_obs=n_obs,
            n_clusters_cao=n_cao,
            years_in_sample=years_in_sample,
            outcome=outcome,
            error=None,
        )
    except Exception as exc:
        err = str(exc)
        tidy_df = pd.DataFrame(
            [
                {
                    "Coefficient": np.nan,
                    "outcome": outcome,
                    "formula": formula,
                    "ref_year": ref_year,
                    "se_invalid": False,
                    "error": err,
                    "n_obs": n_obs,
                    "n_clusters_cao": n_cao,
                }
            ]
        )
        return RegressionRunResult(
            tidy=tidy_df,
            model=None,
            formula=formula,
            ref_year=ref_year,
            n_obs=n_obs,
            n_clusters_cao=n_cao,
            years_in_sample=years_in_sample,
            outcome=outcome,
            error=err,
        )


def run_transition_regression(df_events: pd.DataFrame) -> RegressionRunResult:
    """
    Run transition-level regression on file-to-file mean increase changes.

    Drops transition years with fewer than ``MIN_OBS_PER_YEAR_REGRESSION`` rows
    (logged). Year dummies use ``ref=<min transition_year in sample>``.

    Args:
        df_events: Event-level salary increases including file and ingangsdatum.

    Returns:
        ``RegressionRunResult`` with ``outcome`` set to ``delta_file_mean_increase``
        for fit-metrics labeling.
    """
    cols = ["increase_merged_pref_csv", "cao_number", "file_name", "ingangsdatum"]
    d = df_events[cols].copy()
    d = d[d["increase_merged_pref_csv"].notna()]
    d["ingangsdatum"] = parse_cao_date_series(d["ingangsdatum"], dayfirst=True)
    file_means = (
        d.groupby(["cao_number", "file_name", "ingangsdatum"], dropna=False)["increase_merged_pref_csv"]
        .mean()
        .reset_index()
        .sort_values(["cao_number", "ingangsdatum", "file_name"])
    )
    rows = []
    for cao, grp in file_means.groupby("cao_number"):
        grp = grp.sort_values(["ingangsdatum", "file_name"]).reset_index(drop=True)
        for i in range(1, len(grp)):
            prev_val = grp.loc[i - 1, "increase_merged_pref_csv"]
            new_val = grp.loc[i, "increase_merged_pref_csv"]
            year = grp.loc[i, "ingangsdatum"].year if pd.notna(grp.loc[i, "ingangsdatum"]) else np.nan
            rows.append(
                {
                    "cao_number": cao,
                    "transition_year": year,
                    "delta_file_mean_increase": new_val - prev_val,
                }
            )
    trans = pd.DataFrame(rows)
    trans = trans[trans["delta_file_mean_increase"].notna() & trans["transition_year"].notna()]
    tout = "delta_file_mean_increase"
    empty = RegressionRunResult(
        tidy=pd.DataFrame(),
        model=None,
        formula="",
        ref_year=0,
        n_obs=0,
        n_clusters_cao=0,
        years_in_sample=[],
        outcome=tout,
        error=None,
    )
    if len(trans) == 0:
        return empty
    trans["transition_year"] = trans["transition_year"].astype(int)
    year_counts = trans.groupby("transition_year").size()
    keep_years = year_counts[year_counts >= MIN_OBS_PER_YEAR_REGRESSION].index
    sparse_ty = sorted(set(trans["transition_year"].unique()) - set(keep_years))
    if sparse_ty:
        print(
            f"  [INFO] transition model: excluding years with <{MIN_OBS_PER_YEAR_REGRESSION} obs: {sparse_ty}"
        )
    trans = trans[trans["transition_year"].isin(keep_years)]
    if len(trans) == 0:
        return empty
    ref_ty = int(trans["transition_year"].min())
    formula = f"delta_file_mean_increase ~ i(transition_year, ref={ref_ty}) | cao_number"
    n_cao = int(trans["cao_number"].nunique())
    n_obs = len(trans)
    years_in_sample = sorted(int(y) for y in trans["transition_year"].unique())
    try:
        import pyfixest as pf

        model = pf.feols(formula, data=trans, vcov={"CRV1": "cao_number"})
        tidy = model.tidy()
        tidy_df = _finalize_regression_tidy(
            tidy,
            formula=formula,
            ref_year=ref_ty,
            outcome=None,
            n_obs=n_obs,
            n_clusters_cao=n_cao,
        )
        return RegressionRunResult(
            tidy=tidy_df,
            model=model,
            formula=formula,
            ref_year=ref_ty,
            n_obs=n_obs,
            n_clusters_cao=n_cao,
            years_in_sample=years_in_sample,
            outcome=tout,
            error=None,
        )
    except Exception as exc:
        err = str(exc)
        tidy_df = pd.DataFrame(
            [
                {
                    "Coefficient": np.nan,
                    "formula": formula,
                    "ref_year": ref_ty,
                    "se_invalid": False,
                    "error": err,
                    "n_obs": n_obs,
                    "n_clusters_cao": n_cao,
                }
            ]
        )
        return RegressionRunResult(
            tidy=tidy_df,
            model=None,
            formula=formula,
            ref_year=ref_ty,
            n_obs=n_obs,
            n_clusters_cao=n_cao,
            years_in_sample=years_in_sample,
            outcome=tout,
            error=err,
        )


def main() -> None:
    """
    Load salary data, derive increase series, run regressions, and save outputs.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    df = _read_salary_csv_for_regression(INPUT_CSV)
    payload = derive_salary_increase_series(df)
    events = payload["events"]

    event_tables = []
    fit_rows: List[dict] = []
    for outcome in ["increase_merged_pref_csv", "increase_diff_only", "increase_csv_only"]:
        res_ev = run_event_level_regression(events, outcome)
        event_tables.append(res_ev.tidy)
        fit_rows.append(
            extract_fit_metrics(
                res_ev.model,
                model_kind="event",
                outcome=res_ev.outcome,
                formula=res_ev.formula,
                ref_year=res_ev.ref_year,
                n_obs=res_ev.n_obs,
                n_clusters_cao=res_ev.n_clusters_cao,
                error=res_ev.error,
            )
        )
        if res_ev.model is not None and res_ev.years_in_sample and not res_ev.error and res_ev.outcome:
            plot_event_regression(
                res_ev.model,
                res_ev.outcome,
                res_ev.ref_year,
                res_ev.years_in_sample,
                res_ev.n_obs,
                res_ev.n_clusters_cao,
                FIG_DIR / f"salary_regression_event_{res_ev.outcome}.png",
            )
    event_out = pd.concat(event_tables, ignore_index=True) if event_tables else pd.DataFrame()
    event_out.to_csv(OUT_DIR / "salary_regression_event_level.csv", index=False, sep=";", decimal=",")

    res_tr = run_transition_regression(events)
    res_tr.tidy.to_csv(OUT_DIR / "salary_regression_transition_level.csv", index=False, sep=";", decimal=",")
    fit_rows.append(
        extract_fit_metrics(
            res_tr.model,
            model_kind="transition",
            outcome=res_tr.outcome,
            formula=res_tr.formula,
            ref_year=res_tr.ref_year,
            n_obs=res_tr.n_obs,
            n_clusters_cao=res_tr.n_clusters_cao,
            error=res_tr.error,
        )
    )
    if res_tr.model is not None and res_tr.years_in_sample and not res_tr.error:
        plot_transition_regression(
            res_tr.model,
            res_tr.ref_year,
            res_tr.years_in_sample,
            res_tr.n_obs,
            res_tr.n_clusters_cao,
            FIG_DIR / "salary_regression_transition_delta_file_mean_increase.png",
        )

    pd.DataFrame(fit_rows).to_csv(
        OUT_DIR / "salary_regression_fit_metrics.csv", index=False, sep=";", decimal=","
    )
    print("Wrote regression outputs in outputs/analysis/ (tables, fit_metrics, figures/salary_regression/)")


if __name__ == "__main__":
    main()
