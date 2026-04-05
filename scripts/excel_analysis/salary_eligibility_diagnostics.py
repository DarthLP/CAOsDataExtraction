"""
Salary band and conversion diagnostics (combined CSV).

Builds one semicolon-separated CSV for QA: **aggregated** counts for wide slots
that never enter the long table (per-slot column scans; no per-cell wide rows),
long rows that fail conversion or monthly band, per-CAO summaries when no
band-eligible slot exists anywhere, and global reason counts.

How to run:
    Normally produced by ``descriptives_salary_plots.main()`` after the long
    salary frame is built. To reuse the builder in tests, call
    ``build_salary_band_and_conversion_diagnostics`` then ``to_csv``.

Output:
    ``outputs/analysis/salary_band_and_conversion_diagnostics.csv`` (``sep=';'``,
    ``decimal=','``) with a ``record_type`` column: ``row_exclusion``,
    ``cao_summary``, ``reason_aggregate``.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from scripts.excel_analysis.analysis_utils import (
    SALARY_ANALYSIS_MONTHLY_CAP_EUR,
    coerce_salary_amount_scalar,
    detect_salary_slot_indices,
)
from scripts.excel_analysis.salary_increase_derivation import (
    compute_analysis_monthly_floor_and_band_ok,
    normalize_salary_slot_to_monthly,
)


def _enrich_long_with_band_reasons(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Mirror plot enrichment: monthly EUR, floor, band_ok, plus conversion_reason
    and analysis_drop_reason_band (same logic as ``derive_salary_increase_series``).

    Args:
        df_long: Long salary rows (must include ``row_id`` from
            ``build_long_salary_from_wide``).

    Returns:
        Copy with amount_monthly, conversion_ok, conversion_reason,
        analysis_monthly_floor_eur, analysis_monthly_band_ok,
        analysis_drop_reason_band.
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
    reasons: List[str] = []
    for i in range(len(out)):
        amt = out["salary_amount"].iloc[i]
        unit = out["salary_unit"].iloc[i]
        sh = out["salary_hours_basis_ft_week"].iloc[i] if has_slot_h else np.nan
        rf = row_ft.iloc[i]
        m, ok, reason = normalize_salary_slot_to_monthly(amt, unit, sh, rf)
        amounts_m.append(m)
        oks.append(bool(ok))
        reasons.append(str(reason) if reason else "")
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
    out["conversion_reason"] = reasons
    out["analysis_monthly_floor_eur"] = floor_arr
    out["analysis_monthly_band_ok"] = band_ok
    out["analysis_drop_reason_band"] = drop_r
    return out


def _wide_slot_exclusion_counts(df_wide: pd.DataFrame) -> Counter:
    """
    Count wide slots that never enter the long table (vectorized per slot column).

    Row-level ``row_exclusion`` records for wide slots are **omitted** here: on a full
    extract, emitting tens of millions of rows made the diagnostics CSV and script
    prohibitively slow. Global counts appear under ``record_type == reason_aggregate``.

    Args:
        df_wide: Full wide salary extract.

    Returns:
        Counter of exclusion codes.
    """
    ctr: Counter = Counter()
    if len(df_wide) == 0:
        return ctr
    slot_indices = detect_salary_slot_indices(df_wide.columns.tolist())
    for k in slot_indices:
        start_col = f"salary_{k}_start_date"
        amount_col = f"salary_{k}_amount"
        if start_col not in df_wide.columns or amount_col not in df_wide.columns:
            continue
        sd = pd.to_datetime(df_wide[start_col], errors="coerce")
        raw_amt = df_wide[amount_col]
        coerced = raw_amt.map(coerce_salary_amount_scalar)
        raw_str = raw_amt.astype(str).str.strip()
        has_amt = raw_amt.notna() & raw_str.ne("") & raw_str.str.lower().ne("nan")
        missing_start = sd.isna() & has_amt
        ctr["wide_missing_salary_start_date"] += int(missing_start.sum())
        unparseable = sd.notna() & pd.isna(coerced)
        ctr["wide_amount_unparseable"] += int(unparseable.sum())
        cnum = pd.to_numeric(coerced, errors="coerce")
        nonpos = sd.notna() & cnum.notna() & (cnum <= 0)
        ctr["wide_no_positive_amount"] += int(nonpos.sum())
    return ctr


def _long_exclusion_rows(enriched: pd.DataFrame) -> Tuple[List[Dict[str, Any]], Counter]:
    """
    Long rows that fail conversion or monthly band (after positive-amount long gate).

    Args:
        enriched: Output of ``_enrich_long_with_band_reasons``.

    Returns:
        Diagnostic dicts and reason Counter (prefer band text, else conversion_reason).
    """
    rows: List[Dict[str, Any]] = []
    ctr: Counter = Counter()
    if len(enriched) == 0:
        return rows, ctr
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    conv = enriched["conversion_ok"].fillna(False).to_numpy()
    band_ok = enriched["analysis_monthly_band_ok"].fillna(False).to_numpy()
    mask = ~conv | ~band_ok
    if not mask.any():
        return rows, ctr
    sub = enriched.loc[mask]
    cr = sub["conversion_reason"].fillna("").astype(str).to_numpy()
    adb = sub["analysis_drop_reason_band"].fillna("").astype(str).to_numpy()
    dr_arr = np.where(
        ~sub["conversion_ok"].fillna(False).to_numpy(),
        np.where(cr != "", cr, "conversion_failed"),
        np.where(~sub["analysis_monthly_band_ok"].fillna(False).to_numpy(), np.where(adb != "", adb, "outside_monthly_band"), "excluded"),
    )
    for d in dr_arr:
        ctr[str(d)] += 1
    rid_series = sub["row_id"] if "row_id" in sub.columns else pd.Series(np.nan, index=sub.index)
    idx_series = sub["salary_index"] if "salary_index" in sub.columns else pd.Series(np.nan, index=sub.index)
    rid_vals = rid_series.to_numpy()
    idx_vals = idx_series.to_numpy()
    cao_vals = sub["cao_number"].to_numpy() if "cao_number" in sub.columns else np.full(len(sub), np.nan)
    fn_vals = sub["file_name"].to_numpy() if "file_name" in sub.columns else np.full(len(sub), np.nan)
    sd_vals = sub["salary_start_date"].to_numpy() if "salary_start_date" in sub.columns else np.full(len(sub), pd.NaT)
    su_vals = sub["salary_unit"].to_numpy() if "salary_unit" in sub.columns else np.full(len(sub), np.nan)
    sa_vals = sub["salary_amount"].to_numpy() if "salary_amount" in sub.columns else np.full(len(sub), np.nan)
    am_vals = sub["amount_monthly"].to_numpy() if "amount_monthly" in sub.columns else np.full(len(sub), np.nan)
    fl_vals = sub["analysis_monthly_floor_eur"].to_numpy() if "analysis_monthly_floor_eur" in sub.columns else np.full(len(sub), np.nan)
    conv_vals = sub["conversion_ok"].to_numpy()
    cr_out = sub["conversion_reason"].fillna("").astype(str).to_numpy()
    band_vals = sub["analysis_monthly_band_ok"].to_numpy()
    adb_out = sub["analysis_drop_reason_band"].fillna("").astype(str).to_numpy()
    for i in range(len(sub)):
        rid = int(rid_vals[i]) if pd.notna(rid_vals[i]) else -1
        sidx = int(idx_vals[i]) if pd.notna(idx_vals[i]) else -1
        rows.append(
            {
                "record_type": "row_exclusion",
                "exclusion_phase": "long_row",
                "drop_reason": str(dr_arr[i]),
                "cao_number": cao_vals[i],
                "file_name": fn_vals[i],
                "row_id": rid,
                "salary_index": sidx,
                "salary_start_date": sd_vals[i],
                "salary_unit_raw": su_vals[i],
                "salary_amount_raw": sa_vals[i],
                "amount_monthly": am_vals[i],
                "analysis_monthly_floor_eur": fl_vals[i],
                "SALARY_ANALYSIS_MONTHLY_CAP_EUR": cap,
                "conversion_ok": bool(conv_vals[i]) if pd.notna(conv_vals[i]) else False,
                "conversion_reason": cr_out[i],
                "analysis_monthly_band_ok": bool(band_vals[i]) if pd.notna(band_vals[i]) else False,
                "analysis_drop_reason_band": adb_out[i],
                "primary_reasons": np.nan,
                "n_rows": np.nan,
            }
        )
    return rows, ctr


def _cao_summary_rows(
    df_wide: pd.DataFrame,
    enriched: pd.DataFrame,
    wide_ctr: Counter,
    long_ctr: Counter,
) -> List[Dict[str, Any]]:
    """
    One row per CAO that has no band-eligible long slot anywhere.

    Uses a single pass over ``enriched`` (groupby) — **not** one full scan per CAO,
    which is prohibitive on large extracts (hundreds of thousands of long rows).

    Args:
        df_wide: Wide extract (for CAO universe).
        enriched: Enriched long frame.
        wide_ctr: Wide exclusion counts (unused for text; kept for API symmetry).
        long_ctr: Long exclusion counts.

    Returns:
        List of ``cao_summary`` dicts.
    """
    _ = wide_ctr
    _ = long_ctr
    out: List[Dict[str, Any]] = []
    if "cao_number" not in df_wide.columns:
        return out
    cao_eligible: set = set()
    if len(enriched) > 0 and "analysis_monthly_band_ok" in enriched.columns:
        cao_eligible = set(
            enriched.loc[enriched["analysis_monthly_band_ok"].fillna(False), "cao_number"].dropna().unique()
        )
    all_caos = df_wide["cao_number"].dropna().unique()
    failed_caos = [c for c in all_caos if c not in cao_eligible]
    if not failed_caos:
        return out
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    fc_set = set(failed_caos)
    primary_by_cao: Dict[Any, str] = {}
    if len(enriched) > 0:
        sub = enriched[enriched["cao_number"].isin(fc_set)].copy()
        conv = sub["conversion_ok"].fillna(False)
        band = sub["analysis_monthly_band_ok"].fillna(False)
        diag = np.where(
            ~conv.to_numpy(),
            np.where(sub["conversion_reason"].fillna("").astype(str).to_numpy() != "", sub["conversion_reason"].astype(str).to_numpy(), "conversion_failed"),
            np.where(
                ~band.to_numpy(),
                np.where(sub["analysis_drop_reason_band"].fillna("").astype(str).to_numpy() != "", sub["analysis_drop_reason_band"].astype(str).to_numpy(), "outside_monthly_band"),
                "",
            ),
        )
        sub["diag_reason"] = diag
        sub = sub[sub["diag_reason"].astype(str).str.len() > 0]
        if len(sub) > 0:
            agg = (
                sub.groupby(["cao_number", "diag_reason"])
                .size()
                .reset_index(name="cnt")
                .sort_values(["cao_number", "cnt"], ascending=[True, False])
            )
            for cao, grp in agg.groupby("cao_number", sort=False):
                top = grp.head(12)
                primary_by_cao[cao] = "; ".join(f"{row['diag_reason']}:{int(row['cnt'])}" for _, row in top.iterrows())
    for cao in failed_caos:
        primary = primary_by_cao.get(cao, "no_long_salary_rows_or_all_wide_excluded")
        out.append(
            {
                "record_type": "cao_summary",
                "exclusion_phase": np.nan,
                "drop_reason": np.nan,
                "cao_number": cao,
                "file_name": np.nan,
                "row_id": np.nan,
                "salary_index": np.nan,
                "salary_start_date": np.nan,
                "salary_unit_raw": np.nan,
                "salary_amount_raw": np.nan,
                "amount_monthly": np.nan,
                "analysis_monthly_floor_eur": np.nan,
                "SALARY_ANALYSIS_MONTHLY_CAP_EUR": cap,
                "conversion_ok": np.nan,
                "conversion_reason": np.nan,
                "analysis_monthly_band_ok": np.nan,
                "analysis_drop_reason_band": np.nan,
                "primary_reasons": primary,
                "n_rows": np.nan,
            }
        )
    return out


def _reason_aggregate_rows(wide_ctr: Counter, long_ctr: Counter) -> List[Dict[str, Any]]:
    """Global ``drop_reason`` -> ``n_rows`` rows."""
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    merged: Counter = Counter()
    merged.update(wide_ctr)
    merged.update(long_ctr)
    rows: List[Dict[str, Any]] = []
    for reason, n in sorted(merged.items(), key=lambda x: (-x[1], x[0])):
        rows.append(
            {
                "record_type": "reason_aggregate",
                "exclusion_phase": np.nan,
                "drop_reason": reason,
                "cao_number": np.nan,
                "file_name": np.nan,
                "row_id": np.nan,
                "salary_index": np.nan,
                "salary_start_date": np.nan,
                "salary_unit_raw": np.nan,
                "salary_amount_raw": np.nan,
                "amount_monthly": np.nan,
                "analysis_monthly_floor_eur": np.nan,
                "SALARY_ANALYSIS_MONTHLY_CAP_EUR": cap,
                "conversion_ok": np.nan,
                "conversion_reason": np.nan,
                "analysis_monthly_band_ok": np.nan,
                "analysis_drop_reason_band": np.nan,
                "primary_reasons": np.nan,
                "n_rows": int(n),
            }
        )
    return rows


def build_salary_band_and_conversion_diagnostics(
    df_wide: pd.DataFrame,
    df_long: pd.DataFrame,
    enriched_long: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Assemble the combined diagnostics table (all ``record_type`` sections).

    Args:
        df_wide: Wide salary extract (same as plotting script input).
        df_long: Long salary rows from ``build_long_salary_from_wide`` / ``build_long_salary_df``.
        enriched_long: Optional frame from ``enrich_long_salary_with_monthly_and_band`` (same row
            order as ``df_long``). When provided, skips a duplicate Python enrichment pass.

    Returns:
        Single DataFrame ready for CSV export.
    """
    wide_ctr = _wide_slot_exclusion_counts(df_wide)
    wide_rows: List[Dict[str, Any]] = []
    if enriched_long is not None and len(enriched_long) and "analysis_drop_reason_band" in enriched_long.columns:
        enriched = enriched_long
    elif len(df_long):
        enriched = _enrich_long_with_band_reasons(df_long)
    else:
        enriched = df_long
    long_rows, long_ctr = _long_exclusion_rows(enriched)
    cao_rows = _cao_summary_rows(df_wide, enriched, wide_ctr, long_ctr)
    agg_rows = _reason_aggregate_rows(wide_ctr, long_ctr)
    all_rows = wide_rows + long_rows + cao_rows + agg_rows
    if not all_rows:
        cols = [
            "record_type",
            "exclusion_phase",
            "drop_reason",
            "n_rows",
            "cao_number",
            "file_name",
            "row_id",
            "salary_index",
            "salary_start_date",
            "salary_unit_raw",
            "salary_amount_raw",
            "amount_monthly",
            "analysis_monthly_floor_eur",
            "SALARY_ANALYSIS_MONTHLY_CAP_EUR",
            "conversion_ok",
            "conversion_reason",
            "analysis_monthly_band_ok",
            "analysis_drop_reason_band",
            "primary_reasons",
        ]
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(all_rows)


def write_salary_band_and_conversion_diagnostics_csv(
    df_wide: pd.DataFrame,
    df_long: pd.DataFrame,
    path: str = "outputs/analysis/salary_band_and_conversion_diagnostics.csv",
    enriched_long: Optional[pd.DataFrame] = None,
) -> Path:
    """
    Write ``build_salary_band_and_conversion_diagnostics`` to CSV.

    Args:
        df_wide: Wide salary DataFrame.
        df_long: Long salary DataFrame.
        path: Output path (parent dirs created as needed).
        enriched_long: Pre-enriched long frame when available (avoids a second full scan).

    Returns:
        Resolved ``Path`` to the written file.
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = build_salary_band_and_conversion_diagnostics(df_wide, df_long, enriched_long=enriched_long)
    df.to_csv(out, index=False, sep=";", decimal=",", encoding="utf-8")
    return out


def count_band_eligible_slots_per_wide_row(df_long: pd.DataFrame) -> pd.Series:
    """
    Count band-eligible slots per wide row id (positional ``row_id``).

    Args:
        df_long: Long rows with ``row_id`` and columns produced by
            ``enrich_long_salary_with_monthly_and_band`` or
            ``_enrich_long_with_band_reasons``.

    Returns:
        Series indexed by ``row_id`` with integer counts (missing ids = no rows in long).
    """
    if len(df_long) == 0 or "row_id" not in df_long.columns:
        return pd.Series(dtype=np.int64)
    if "analysis_monthly_band_ok" not in df_long.columns:
        return pd.Series(dtype=np.int64)
    sub = df_long[df_long["analysis_monthly_band_ok"].fillna(False)]
    if len(sub) == 0:
        return pd.Series(dtype=np.int64)
    return sub.groupby("row_id", sort=False).size()
