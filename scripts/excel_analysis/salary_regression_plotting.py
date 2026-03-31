"""
Salary regression fit metrics and coefficient plots.

Builds fit-summary rows from pyfixest ``Feols`` objects and matplotlib figures
for event-level (two panels: NF=0 year path; implied new_file_effect by year)
and transition-level year coefficients.

**Shaded / colored bands on the figures:** each band is an **approximate 95%**
**confidence interval** for the corresponding plotted coefficient (or linear
combination in panel B of the event plot), computed as **estimate ± 1.96 ×**
**standard error**. Standard errors come from the same **cluster-robust**
variance matrix as the regression (**CRV1** clustering on ``cao_number``).
Where SE is zero (e.g. omitted reference year fixed at 0) or missing, no band
is drawn for that point.

Used by ``salary_increase_regression.py`` after each ``feols`` fit.
"""

from __future__ import annotations

# Legend / caption text for the semi-transparent fill between coefficient CIs.
CI_BAND_LABEL = "95% CI (estimate ± 1.96×SE, CRV1 cluster-robust)"
CI_BAND_CAPTION = (
    "Shaded regions: 95% confidence bands (estimate ± 1.96×SE) from the "
    "cluster-robust (CRV1, cao_number) variance matrix."
)


def _fig_caption_bottom(
    fig: Any,
    paragraphs: List[str],
    fontsize: int = 8,
    *,
    top: Optional[float] = None,
    hspace: Optional[float] = None,
    left: Optional[float] = None,
    right: Optional[float] = None,
) -> None:
    """
    Place wrapped explanatory text at the bottom of the figure and reserve margin.

    Avoids long suptitles that get clipped at the top. Call after plotting on all axes.

    Args:
        fig: Matplotlib figure.
        paragraphs: Blocks of text (each block wrapped separately).
        fontsize: Caption font size.
        top, hspace, left, right: Optional ``subplots_adjust`` kwargs applied together
            with ``bottom`` so margins are set in one call.
    """
    wrapped_blocks: List[str] = []
    for p in paragraphs:
        wrapped_blocks.append(
            textwrap.fill(p, width=105, break_long_words=False, break_on_hyphens=False)
        )
    full_text = "\n\n".join(wrapped_blocks)
    n_lines = full_text.count("\n") + 1
    bottom_frac = min(0.10 + 0.024 * max(n_lines, 4), 0.48)
    adj: Dict[str, float] = {"bottom": bottom_frac}
    if top is not None:
        adj["top"] = top
    if hspace is not None:
        adj["hspace"] = hspace
    if left is not None:
        adj["left"] = left
    if right is not None:
        adj["right"] = right
    fig.subplots_adjust(**adj)
    fig.text(
        0.5,
        0.012,
        full_text,
        transform=fig.transFigure,
        ha="center",
        va="bottom",
        fontsize=fontsize,
    )

import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from scripts.excel_analysis.analysis_utils import enforce_integer_year_axis


def extract_fit_metrics(
    model: Any,
    *,
    model_kind: str,
    outcome: Optional[str],
    formula: str,
    ref_year: int,
    n_obs: int,
    n_clusters_cao: int,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Collect R² / RMSE and identifiers for one ``Feols`` fit.

    Args:
        model: Fitted ``Feols`` instance, or None if estimation failed.
        model_kind: ``event`` or ``transition``.
        outcome: Dependent variable name; empty string allowed for transition row label.
        formula: Estimated formula string.
        ref_year: Omitted reference year in the factor specification.
        n_obs: Number of rows in the estimation sample.
        n_clusters_cao: Distinct CAO values used for clustering.
        error: Non-empty if the model did not converge or raised.

    Returns:
        Dictionary suitable for one row of ``salary_regression_fit_metrics.csv``.
    """
    row: Dict[str, Any] = {
        "model_kind": model_kind,
        "outcome": outcome if outcome is not None else "",
        "formula": formula,
        "ref_year": ref_year,
        "n_obs": n_obs,
        "n_clusters_cao": n_clusters_cao,
        "r2": np.nan,
        "r2_within": np.nan,
        "adj_r2": np.nan,
        "adj_r2_within": np.nan,
        "rmse": np.nan,
        "error": error or "",
    }
    if model is None or error:
        return row
    row["r2"] = float(getattr(model, "_r2", np.nan))
    row["r2_within"] = float(getattr(model, "_r2_within", np.nan))
    row["adj_r2"] = float(getattr(model, "_adj_r2", np.nan))
    row["adj_r2_within"] = float(getattr(model, "_adj_r2_within", np.nan))
    row["rmse"] = float(getattr(model, "_rmse", np.nan))
    return row


def _coef_vector(model: Any) -> Tuple[List[str], np.ndarray]:
    """
    Map coefficient names to the estimated beta vector.

    Args:
        model: Fitted ``Feols``.

    Returns:
        Tuple of string names and 1-D beta array (aligned with vcov).
    """
    names = [str(x) for x in model._coefnames]
    beta = np.asarray(model._beta_hat, dtype=float).ravel()
    return names, beta


def _coef_index(names: List[str]) -> Dict[str, int]:
    """Return name -> position in beta/vcov for each reported coefficient."""
    return {n: i for i, n in enumerate(names)}


def _linear_combo_var(indices: List[int], vcov: np.ndarray) -> float:
    """
    Variance of sum of coefficients at given indices: Var(sum beta_k).

    Args:
        indices: Row/column indices into vcov (same order as names).
        vcov: Symmetric variance-covariance matrix.

    Returns:
        Scalar variance; NaN if any index invalid or non-finite submatrix.
    """
    if not indices:
        return float("nan")
    idx = np.array(indices, dtype=int)
    if idx.max() >= vcov.shape[0] or idx.min() < 0:
        return float("nan")
    sub = vcov[np.ix_(idx, idx)]
    if not np.all(np.isfinite(sub)):
        return float("nan")
    w = np.ones(len(idx))
    return float(w @ sub @ w)


def _nf0_year_path(
    names: List[str],
    beta: np.ndarray,
    vcov: np.ndarray,
    years: List[int],
    ref_year: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Coefficients for salary_start_year dummies (NF=0 path); ref year at 0.

    Args:
        names: Coefficient names from the model.
        beta: Estimated coefficients.
        vcov: Variance-covariance matrix.
        years: Sorted salary years in the estimation sample.
        ref_year: Omitted reference year.

    Returns:
        Triple of arrays (year, estimate, se) aligned to ``years``.
    """
    ix = _coef_index(names)
    y_arr = np.array(years, dtype=int)
    est = np.full(len(years), np.nan)
    se = np.full(len(years), np.nan)
    for i, y in enumerate(years):
        if y == ref_year:
            est[i] = 0.0
            se[i] = 0.0
            continue
        key = f"salary_start_year::{y}"
        if key not in ix:
            continue
        j = ix[key]
        est[i] = beta[j]
        se[i] = np.sqrt(vcov[j, j]) if np.isfinite(vcov[j, j]) and vcov[j, j] >= 0 else np.nan
    return y_arr, est, se


def _implied_nf_by_year(
    names: List[str],
    beta: np.ndarray,
    vcov: np.ndarray,
    years: List[int],
    ref_year: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Implied new_file_effect contrast by year: beta_NF + beta_{NF:year} (0 interaction at ref).

    Args:
        names: Coefficient names.
        beta: Estimated coefficients.
        vcov: Variance-covariance matrix.
        years: Sorted years in sample.
        ref_year: Omitted reference year for the factor.

    Returns:
        Triple (year, estimate, se).
    """
    ix = _coef_index(names)
    nf_key = "new_file_effect"
    if nf_key not in ix:
        return np.array(years), np.full(len(years), np.nan), np.full(len(years), np.nan)
    i_nf = ix[nf_key]
    y_arr = np.array(years, dtype=int)
    est = np.full(len(years), np.nan)
    se = np.full(len(years), np.nan)
    for k, y in enumerate(years):
        if y == ref_year:
            est[k] = beta[i_nf]
            se[k] = np.sqrt(vcov[i_nf, i_nf]) if np.isfinite(vcov[i_nf, i_nf]) and vcov[i_nf, i_nf] >= 0 else np.nan
            continue
        inter_key = f"new_file_effect:salary_start_year::{y}"
        if inter_key not in ix:
            est[k] = np.nan
            se[k] = np.nan
            continue
        i_int = ix[inter_key]
        est[k] = beta[i_nf] + beta[i_int]
        se[k] = np.sqrt(_linear_combo_var([i_nf, i_int], vcov))
    return y_arr, est, se


def _interaction_only_by_year(
    names: List[str],
    beta: np.ndarray,
    vcov: np.ndarray,
    years: List[int],
    ref_year: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Interaction coefficients new_file_effect:salary_start_year::y (0 at ref).

    Args:
        names: Coefficient names.
        beta: Estimated coefficients.
        vcov: Variance-covariance matrix.
        years: Sorted years in sample.
        ref_year: Omitted reference year.

    Returns:
        Triple (year, estimate, se).
    """
    ix = _coef_index(names)
    y_arr = np.array(years, dtype=int)
    est = np.full(len(years), np.nan)
    se = np.full(len(years), np.nan)
    for k, y in enumerate(years):
        if y == ref_year:
            est[k] = 0.0
            se[k] = 0.0
            continue
        key = f"new_file_effect:salary_start_year::{y}"
        if key not in ix:
            continue
        j = ix[key]
        est[k] = beta[j]
        se[k] = np.sqrt(vcov[j, j]) if np.isfinite(vcov[j, j]) and vcov[j, j] >= 0 else np.nan
    return y_arr, est, se


def _plot_ci_band(
    ax: Any,
    x: np.ndarray,
    y: np.ndarray,
    se: np.ndarray,
    color: str,
    *,
    label: str = CI_BAND_LABEL,
) -> None:
    """
    Draw a semi-transparent vertical band: approximate 95% CI where SE > 0.

    Args:
        ax: Matplotlib axes.
        x: Year (or x) positions aligned with y and se.
        y: Point estimates.
        se: Standard errors (cluster-robust); band is y ± 1.96 se.
        color: Matplotlib color for the fill.
        label: Legend entry explaining the shaded area.
    """
    m = np.isfinite(y) & np.isfinite(se) & (se > 0)
    if m.any():
        lo = y[m] - 1.96 * se[m]
        hi = y[m] + 1.96 * se[m]
        ax.fill_between(
            x[m],
            lo,
            hi,
            alpha=0.2,
            color=color,
            linewidth=0,
            label=label,
            zorder=1,
        )


def plot_event_regression(
    model: Any,
    outcome: str,
    ref_year: int,
    years_in_sample: List[int],
    n_obs: int,
    n_clusters_cao: int,
    out_path: Path,
) -> None:
    """
    Two-panel figure: (1) year dummies for NF=0 vs ref; (2) implied NF contrast by year.

    Args:
        model: Fitted event-level ``Feols``.
        outcome: Dependent variable column name (y-axis description).
        ref_year: Omitted salary_start_year in the formula.
        years_in_sample: Sorted list of salary years kept in the regression sample.
        n_obs: Estimation sample size (annotated on figure).
        n_clusters_cao: Number of CAO clusters (annotated on figure).
        out_path: Path to write PNG.
    """
    names, beta = _coef_vector(model)
    vcov = np.asarray(model._vcov, dtype=float)
    years = sorted(int(y) for y in years_in_sample)

    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(10, 8), sharex=True, constrained_layout=False)

    y0, e0, s0 = _nf0_year_path(names, beta, vcov, years, ref_year)
    c0 = "C0"
    ax0.plot(y0, e0, marker="o", markersize=4, color=c0, label="salary_start_year coeffs (NF=0)", zorder=3)
    _plot_ci_band(ax0, y0, e0, s0, c0, label=CI_BAND_LABEL)
    ax0.set_ylabel(f"Coefficient ({outcome})\nvs ref year {ref_year}, first-in-file events")
    ax0.axhline(0.0, color="gray", linewidth=0.8, linestyle="--")
    ax0.legend(loc="best", fontsize=8)
    ax0.set_title(
        f"A: {outcome} — calendar year (NF=0 / first-in-file step), vs ref {ref_year}",
        fontsize=10,
    )

    y1, e1, s1 = _implied_nf_by_year(names, beta, vcov, years, ref_year)
    c1 = "C1"
    ax1.plot(y1, e1, marker="o", markersize=4, color=c1, label="β_new_file + β_{new×year}", zorder=3)
    _plot_ci_band(ax1, y1, e1, s1, c1, label=CI_BAND_LABEL)
    ax1.set_ylabel(f"Implied new_file_effect\n({outcome} units, non-first vs first in file)")
    ax1.set_xlabel("Salary start year")
    ax1.axhline(0.0, color="gray", linewidth=0.8, linestyle="--")
    ax1.legend(loc="best", fontsize=8)
    ax1.set_title(
        f"B: {outcome} — implied new_file_effect by salary year (ref {ref_year}: main NF coef only)",
        fontsize=10,
    )

    enforce_integer_year_axis(ax0, years)
    enforce_integer_year_axis(ax1, years)
    _fig_caption_bottom(
        fig,
        [
            f"Event-level fixed-effects regression (CAO FE). Dependent variable: {outcome}.",
            (
                f"Formula: {outcome} ~ new_file_effect + i(salary_start_year, ref={ref_year}) + "
                f"new_file_effect:i(salary_start_year) | cao_number"
            ),
            (
                f"Inference: CRV1 cluster by cao_number. Reference salary year (baseline 0 in panel A): "
                f"{ref_year}. n_obs={n_obs}, n_clusters (CAO)={n_clusters_cao}."
            ),
            (
                f"{CI_BAND_CAPTION} Panel B: interval uses Var(β_NF + β_NF×y) from the vcov "
                f"(covariance between main and interaction terms included)."
            ),
        ],
        top=0.96,
        hspace=0.35,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight", pad_inches=0.35)
    plt.close(fig)

    # Optional third series: interaction-only deviation (small inset or second file — plan: optional on bottom)
    # Add compact second figure file for interaction-only to avoid clutter
    fig2, ax = plt.subplots(figsize=(10, 4.5), constrained_layout=False)
    y2, e2, s2 = _interaction_only_by_year(names, beta, vcov, years, ref_year)
    c2 = "C2"
    ax.plot(y2, e2, marker="o", markersize=4, color=c2, label="new_file_effect × year (increment vs ref)", zorder=3)
    _plot_ci_band(ax, y2, e2, s2, c2, label=CI_BAND_LABEL)
    ax.axhline(0.0, color="gray", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Salary start year")
    ax.set_ylabel(f"Increment to NF contrast vs {ref_year}\n(same outcome units)")
    ax.set_title(
        f"NF × salary_start_year interactions only (0 at ref {ref_year}) — {outcome}",
        fontsize=10,
    )
    ax.legend(loc="best", fontsize=8)
    enforce_integer_year_axis(ax, years)
    _fig_caption_bottom(
        fig2,
        [
            (
                f"Same model as the main event figure for outcome '{outcome}'; n_obs={n_obs}. "
                f"Deviation of the non-first-vs-first-in-file contrast from its value in ref year {ref_year}."
            ),
            CI_BAND_CAPTION,
        ],
        top=0.90,
    )
    p2 = out_path.with_name(out_path.stem + "_nf_year_interactions_only" + out_path.suffix)
    fig2.savefig(p2, dpi=150, bbox_inches="tight", pad_inches=0.35)
    plt.close(fig2)


def plot_transition_regression(
    model: Any,
    ref_year: int,
    years_in_sample: List[int],
    n_obs: int,
    n_clusters_cao: int,
    out_path: Path,
) -> None:
    """
    Plot transition_year dummy coefficients vs calendar year (omitted ref = 0).

    Args:
        model: Fitted transition-level ``Feols``.
        ref_year: Omitted transition_year.
        years_in_sample: Sorted transition years in the estimation sample.
        n_obs: Estimation sample size.
        n_clusters_cao: Number of CAO clusters.
        out_path: PNG path.
    """
    names, beta = _coef_vector(model)
    vcov = np.asarray(model._vcov, dtype=float)
    years = sorted(int(y) for y in years_in_sample)
    ix = _coef_index(names)
    y_arr = np.array(years, dtype=int)
    est = np.full(len(years), np.nan)
    se = np.full(len(years), np.nan)
    for i, y in enumerate(years):
        if y == ref_year:
            est[i] = 0.0
            se[i] = 0.0
            continue
        key = f"transition_year::{y}"
        if key not in ix:
            continue
        j = ix[key]
        est[i] = beta[j]
        se[i] = np.sqrt(vcov[j, j]) if np.isfinite(vcov[j, j]) and vcov[j, j] >= 0 else np.nan

    fig, ax = plt.subplots(figsize=(10, 5), constrained_layout=False)
    c0 = "C0"
    ax.plot(y_arr, est, marker="o", markersize=4, color=c0, label="transition_year coefficient", zorder=3)
    _plot_ci_band(ax, y_arr, est, se, c0, label=CI_BAND_LABEL)
    ax.axhline(0.0, color="gray", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Transition year (calendar year of ingangsdatum for the new file)")
    ax.set_ylabel("Coefficient (delta_file_mean_increase)\nvs omitted reference year")
    ax.set_title("Transition-level FE: delta_file_mean_increase by transition year", fontsize=11)
    ax.legend(loc="best", fontsize=8)
    enforce_integer_year_axis(ax, years)
    _fig_caption_bottom(
        fig,
        [
            (
                f"Formula: delta_file_mean_increase ~ i(transition_year, ref={ref_year}) | cao_number. "
                f"CRV1 cluster: cao_number. Omitted reference year baseline = 0. "
                f"n_obs={n_obs}, n_clusters (CAO)={n_clusters_cao}."
            ),
            CI_BAND_CAPTION,
        ],
        top=0.90,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight", pad_inches=0.35)
    plt.close(fig)
