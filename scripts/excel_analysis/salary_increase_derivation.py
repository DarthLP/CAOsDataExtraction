"""
Salary Increase Derivation Utilities

This module derives salary-increase series used by salary descriptives and plots.
It builds event-level salary rows from wide salary slots, normalizes salary amounts
to monthly units, computes within-row consecutive percentage differences, keeps the
CSV-reported increase series, and creates a merged preferred series.

How to use:
    from scripts.excel_analysis.salary_increase_derivation import derive_salary_increase_series
    payload = derive_salary_increase_series(df_wide_salary)
    events = payload["events"]
    dropped = payload["conversion_diagnostics"]

Outputs:
    - events: event-level DataFrame with increase_diff_only / increase_csv_only /
      increase_merged_pref_csv and identifiers. Slots with non-positive or
      non-numeric amounts are omitted; implausible monthly normalization yields
      conversion_ok False. is_first_salary_in_file is 1 for every event on the
      earliest salary_start_date within the same (cao_number, file_name).
      analysis_monthly_floor_eur / analysis_monthly_band_ok / analysis_drop_reason_band
      implement statutory monthly floor (NL 1-Jan schedule) plus SALARY_ANALYSIS_MONTHLY_CAP_EUR.
      increase_merged_pref_csv is CSV if present else diff-based, without masking on
      analysis_monthly_band_ok (plots and regressions that must stay band-only filter explicitly).
    - conversion_diagnostics: dropped conversion cases, invalid diff pairs, and
      conversion_ok rows outside the monthly band (below floor / above cap / missing date).
    - comparison: rows with both CSV and diff increases; includes abs_diff,
      within_0_1pp, abs_diff_gt_0_1, sign_disagreement (NaN columns when empty).
    - band_summary: event-level counts and shares (of conversion_ok) for band exclusions.

Shared helpers (same rules as the band above):
    - normalize_salary_slot_to_monthly: one slot to EUR/month + conversion_ok.
    - compute_analysis_monthly_floor_and_band_ok: vectorized floor array and band_ok mask.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from scripts.excel_analysis.analysis_utils import (
    SALARY_ANALYSIS_MONTHLY_CAP_EUR,
    coerce_salary_amount_scalar,
    convert_salary_to_monthly,
    detect_salary_slot_indices,
    parse_cao_date_series,
)
from scripts.excel_analysis.nl_minimum_wage_monthly import minimum_monthly_gross_eur_series


def _normalize_event_amount(amount: Any, unit: Any, slot_hours: Any, row_ft_hours: Any) -> Tuple[Optional[float], bool, str]:
    """
    Normalize event amount to monthly equivalent using canonical converter.

    Args:
        amount: Raw salary amount
        unit: Raw salary unit
        slot_hours: Slot-level full-time weekly hours basis
        row_ft_hours: Row-level full-time hours fallback

    Returns:
        Tuple of (normalized_amount, conversion_ok, reason)
    """
    amount_num = coerce_salary_amount_scalar(amount)
    if amount_num is None:
        return None, False, "amount_not_numeric"
    if pd.isna(unit) or str(unit).strip() == "":
        return None, False, "missing_unit"

    slot_hours_num = pd.to_numeric(pd.Series([slot_hours]), errors="coerce").iloc[0]
    row_hours_num = pd.to_numeric(pd.Series([row_ft_hours]), errors="coerce").iloc[0]
    hours_for_conversion = slot_hours_num if pd.notna(slot_hours_num) else row_hours_num

    normalized = convert_salary_to_monthly(amount_num, str(unit), None if pd.isna(hours_for_conversion) else float(hours_for_conversion))
    if normalized is None or pd.isna(normalized):
        return None, False, "unsupported_or_invalid_unit_conversion"
    return float(normalized), True, ""


def normalize_salary_slot_to_monthly(
    amount: Any, unit: Any, slot_hours: Any, row_ft_hours: Any
) -> Tuple[Optional[float], bool, str]:
    """
    Normalize one salary slot to gross monthly EUR using the same rules as increase events.

    Args:
        amount: Raw salary amount (scalar).
        unit: Pay unit string.
        slot_hours: Slot-level full-time weekly hours basis (optional).
        row_ft_hours: Row-level ft_hours fallback (optional).

    Returns:
        (monthly_eur_or_none, conversion_ok, empty_reason_or_drop_code).
    """
    return _normalize_event_amount(amount, unit, slot_hours, row_ft_hours)


def _empty_band_summary() -> Dict[str, Any]:
    """Return zeroed band exclusion summary for empty event frames."""
    nan = float("nan")
    return {
        "n_increase_events": 0,
        "n_conversion_ok": 0,
        "n_band_eligible": 0,
        "n_dropped_missing_salary_date": 0,
        "n_dropped_below_floor": 0,
        "n_dropped_above_cap": 0,
        "share_missing_date_of_conversion_ok": nan,
        "share_below_floor_of_conversion_ok": nan,
        "share_above_cap_of_conversion_ok": nan,
        "share_band_eligible_of_conversion_ok": nan,
    }


def compute_analysis_monthly_floor_and_band_ok(
    conversion_ok: np.ndarray,
    salary_start_date: pd.Series,
    amount_monthly: Any,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Vectorized NL statutory monthly floor plus analysis cap (same rules as derive_salary_increase_series).

    Args:
        conversion_ok: Boolean array, length n — unit conversion succeeded.
        salary_start_date: Series of datetimes (salary table effective dates).
        amount_monthly: Length-n amounts after convert_salary_to_monthly (array, list, or Series).

    Returns:
        Tuple ``(analysis_monthly_floor_eur, analysis_monthly_band_ok)`` as float64 / bool ndarrays.
    """
    sd_series = pd.to_datetime(salary_start_date, errors="coerce")
    floor_arr = minimum_monthly_gross_eur_series(sd_series)
    amt_f = pd.to_numeric(pd.Series(amount_monthly), errors="coerce").to_numpy(dtype=float)
    conv_ok = np.asarray(conversion_ok, dtype=bool)
    nat_sd = sd_series.isna().to_numpy()
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    floor_fin = np.isfinite(floor_arr)
    amt_fin = np.isfinite(amt_f)
    band_ok = conv_ok & ~nat_sd & amt_fin & floor_fin & (amt_f >= floor_arr) & (amt_f <= cap)
    return floor_arr, band_ok


def compute_band_summary_stats(events_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Aggregate band exclusion counts and shares (denominator: conversion_ok events).

    Args:
        events_df: Output frame from derive_salary_increase_series with band columns.

    Returns:
        Dict with counts and share_* among n_conversion_ok (NaN shares if none).
    """
    if len(events_df) == 0 or "conversion_ok" not in events_df.columns:
        return _empty_band_summary()
    conv = events_df["conversion_ok"].to_numpy()
    band_ok = (
        events_df["analysis_monthly_band_ok"].to_numpy()
        if "analysis_monthly_band_ok" in events_df.columns
        else np.zeros(len(events_df), dtype=bool)
    )
    nat = events_df["salary_start_date"].isna().to_numpy()
    amt = pd.to_numeric(events_df["amount_monthly"], errors="coerce").to_numpy(dtype=float)
    floor = pd.to_numeric(events_df["analysis_monthly_floor_eur"], errors="coerce").to_numpy(dtype=float)
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    n_conv = int(conv.sum())
    n_missing = int((conv & nat).sum())
    n_above = int((conv & ~nat & np.isfinite(amt) & (amt > cap)).sum())
    n_below = int(
        (conv & ~nat & np.isfinite(amt) & np.isfinite(floor) & (amt <= cap) & (amt < floor)).sum()
    )
    n_eligible = int(band_ok.sum())
    denom = float(n_conv) if n_conv else float("nan")

    def _share(n: int) -> float:
        return float(n) / denom if n_conv else float("nan")

    return {
        "n_increase_events": len(events_df),
        "n_conversion_ok": n_conv,
        "n_band_eligible": n_eligible,
        "n_dropped_missing_salary_date": n_missing,
        "n_dropped_below_floor": n_below,
        "n_dropped_above_cap": n_above,
        "share_missing_date_of_conversion_ok": _share(n_missing),
        "share_below_floor_of_conversion_ok": _share(n_below),
        "share_above_cap_of_conversion_ok": _share(n_above),
        "share_band_eligible_of_conversion_ok": _share(n_eligible),
    }


def derive_salary_increase_series(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Build event-level salary increase series with strict within-row derivation.

    Args:
        df: Wide salary DataFrame.

    Returns:
        Dictionary containing:
            - events: event-level DataFrame with derived increase columns
            - conversion_diagnostics: dropped conversion cases for QA
            - comparison: subset where both csv and diff exist with abs_diff/sign flags
            - band_summary: counts/shares for monthly band exclusions
    """
    if len(df) == 0:
        empty = pd.DataFrame()
        return {
            "events": empty,
            "conversion_diagnostics": empty,
            "comparison": empty,
            "band_summary": _empty_band_summary(),
        }

    # Use positional row ids (0..n-1) and avoid df.copy() — saves a full wide-frame RAM duplicate.
    n = len(df)
    row_pos = np.arange(n, dtype=np.int64)

    if "ingangsdatum" in df.columns:
        ingangsdatum_series = parse_cao_date_series(df["ingangsdatum"], dayfirst=True)
    else:
        ingangsdatum_series = pd.Series(pd.NaT, index=df.index)

    slot_indices = detect_salary_slot_indices(df.columns.tolist())
    diagnostics_rows: List[Dict[str, Any]] = []
    event_chunks: List[pd.DataFrame] = []

    ft_hours_series = df["ft_hours"] if "ft_hours" in df.columns else pd.Series(np.nan, index=df.index)

    for slot in slot_indices:
        start_col = f"salary_{slot}_start_date"
        amount_col = f"salary_{slot}_amount"
        unit_col = f"salary_{slot}_unit"
        inc_col = f"salary_{slot}_increase_percent"
        hours_col = f"salary_{slot}_hours_basis_ft_week"
        if start_col not in df.columns or amount_col not in df.columns:
            continue

        start_date = pd.to_datetime(df[start_col], errors="coerce")
        amt_try = df[amount_col].map(coerce_salary_amount_scalar)
        mask = start_date.notna() & amt_try.notna() & (amt_try > 0)
        if not mask.any():
            continue

        mcount = mask.sum()
        amounts = df.loc[mask, amount_col].to_numpy()
        units = df.loc[mask, unit_col].to_numpy() if unit_col in df.columns else np.full(mcount, np.nan, dtype=object)
        slot_h = df.loc[mask, hours_col].to_numpy() if hours_col in df.columns else np.full(mcount, np.nan)
        row_ft = ft_hours_series.loc[mask].to_numpy()
        inc_csv_vals = (
            pd.to_numeric(df.loc[mask, inc_col], errors="coerce").to_numpy()
            if inc_col in df.columns
            else np.full(mcount, np.nan)
        )

        norm_monthly: List[Optional[float]] = []
        ok_flags: List[bool] = []
        reasons: List[str] = []
        for i in range(mcount):
            normalized, ok, reason = _normalize_event_amount(amounts[i], units[i], slot_h[i], row_ft[i])
            norm_monthly.append(normalized)
            ok_flags.append(ok)
            reasons.append(reason)
            if not ok:
                sd_i = start_date.loc[mask].iloc[i]
                diagnostics_rows.append(
                    {
                        "row_id": int(row_pos[mask.to_numpy()][i]),
                        "cao_number": df.loc[mask, "cao_number"].iloc[i] if "cao_number" in df.columns else np.nan,
                        "file_name": df.loc[mask, "file_name"].iloc[i] if "file_name" in df.columns else np.nan,
                        "salary_index_new": slot,
                        "salary_start_date_new": sd_i,
                        "salary_amount_raw_new": amounts[i],
                        "salary_unit_raw_new": units[i],
                        "salary_hours_basis_ft_week_new": slot_h[i],
                        "drop_reason": reason,
                    }
                )

        sd_series = start_date.loc[mask].reset_index(drop=True)
        chunk = pd.DataFrame(
            {
                "row_id": row_pos[mask.to_numpy()],
                "cao_number": df.loc[mask, "cao_number"].to_numpy() if "cao_number" in df.columns else np.full(mcount, np.nan),
                "file_name": df.loc[mask, "file_name"].to_numpy() if "file_name" in df.columns else np.full(mcount, np.nan),
                "ingangsdatum": ingangsdatum_series.loc[mask].to_numpy(),
                "salary_index": slot,
                "salary_start_date": sd_series.to_numpy(),
                "salary_start_year": sd_series.dt.year.astype("float64").to_numpy(),
                "salary_amount_raw": amounts,
                "salary_unit_raw": units,
                "salary_hours_basis_ft_week": slot_h,
                "amount_monthly": norm_monthly,
                "increase_csv_only": inc_csv_vals,
                "conversion_ok": ok_flags,
                "conversion_reason": reasons,
            }
        )
        chunk["increase_csv_only"] = pd.to_numeric(chunk["increase_csv_only"], errors="coerce")
        event_chunks.append(chunk)

    if not event_chunks:
        empty = pd.DataFrame()
        return {
            "events": empty,
            "conversion_diagnostics": pd.DataFrame(diagnostics_rows),
            "comparison": empty,
            "band_summary": _empty_band_summary(),
        }

    events_df = pd.concat(event_chunks, ignore_index=True)

    events_df["increase_diff_only"] = np.nan
    events_df = events_df.sort_values(["row_id", "salary_start_date", "salary_index"]).reset_index(drop=True)

    # Monthly band: NL statutory floor (Jan 1 schedule) + analysis cap on normalized amounts.
    sd_series = events_df["salary_start_date"]
    floor_arr, analysis_monthly_band_ok = compute_analysis_monthly_floor_and_band_ok(
        events_df["conversion_ok"].to_numpy(),
        sd_series,
        events_df["amount_monthly"],
    )
    amt_f = pd.to_numeric(events_df["amount_monthly"], errors="coerce").to_numpy(dtype=float)
    conv_ok = events_df["conversion_ok"].to_numpy()
    nat_sd = sd_series.isna().to_numpy()
    cap = float(SALARY_ANALYSIS_MONTHLY_CAP_EUR)
    floor_fin = np.isfinite(floor_arr)
    amt_fin = np.isfinite(amt_f)
    events_df["analysis_monthly_floor_eur"] = floor_arr
    events_df["analysis_monthly_band_ok"] = analysis_monthly_band_ok
    reasons = np.full(len(events_df), "", dtype=object)
    m_conv = conv_ok
    reasons[m_conv & nat_sd] = "missing_salary_date_for_floor"
    reasons[m_conv & ~nat_sd & amt_fin & (amt_f > cap)] = "above_analysis_cap"
    reasons[m_conv & ~nat_sd & amt_fin & floor_fin & (amt_f <= cap) & (amt_f < floor_arr)] = (
        "below_statutory_monthly_floor"
    )
    events_df["analysis_drop_reason_band"] = reasons

    band_fail = m_conv & ~analysis_monthly_band_ok
    if band_fail.any():
        for idx in np.flatnonzero(band_fail):
            diagnostics_rows.append(
                {
                    "row_id": int(events_df.at[idx, "row_id"]),
                    "cao_number": events_df.at[idx, "cao_number"],
                    "file_name": events_df.at[idx, "file_name"],
                    "salary_index_new": int(events_df.at[idx, "salary_index"]),
                    "salary_start_date_new": events_df.at[idx, "salary_start_date"],
                    "salary_amount_monthly_new": events_df.at[idx, "amount_monthly"],
                    "analysis_monthly_floor_eur": events_df.at[idx, "analysis_monthly_floor_eur"],
                    "drop_reason": str(events_df.at[idx, "analysis_drop_reason_band"]),
                }
            )

    # Vectorized consecutive within-row percentage change (avoids per-row groupby + .at writes).
    row_ids = events_df["row_id"].to_numpy()
    grp_start = row_ids != np.roll(row_ids, 1)
    grp_start[0] = True
    prev_amt = np.roll(events_df["amount_monthly"].to_numpy(), 1)
    prev_amt[grp_start] = np.nan
    curr_amt = events_df["amount_monthly"].to_numpy()
    prev_amt_f = pd.to_numeric(pd.Series(prev_amt), errors="coerce").to_numpy(dtype=float)
    curr_amt_f = pd.to_numeric(pd.Series(curr_amt), errors="coerce").to_numpy(dtype=float)
    valid = ~grp_start & np.isfinite(prev_amt_f) & np.isfinite(curr_amt_f) & (prev_amt_f > 0.0)
    band_ok_arr = events_df["analysis_monthly_band_ok"].to_numpy()
    prev_band = np.roll(band_ok_arr, 1)
    prev_band[grp_start] = False
    valid = valid & prev_band & band_ok_arr
    increase = np.full(len(events_df), np.nan, dtype=float)
    increase[valid] = (curr_amt_f[valid] - prev_amt_f[valid]) / prev_amt_f[valid] * 100.0
    events_df["increase_diff_only"] = increase

    bad_mask = ~grp_start & ~valid
    if bad_mask.any():
        bad_idx = np.flatnonzero(bad_mask)
        prev_idx_arr = bad_idx - 1
        for j, idx in enumerate(bad_idx):
            pidx = int(prev_idx_arr[j])
            diagnostics_rows.append(
                {
                    "row_id": int(events_df.at[idx, "row_id"]),
                    "cao_number": events_df.at[idx, "cao_number"],
                    "file_name": events_df.at[idx, "file_name"],
                    "salary_index_old": int(events_df.at[pidx, "salary_index"]),
                    "salary_index_new": int(events_df.at[idx, "salary_index"]),
                    "salary_start_date_old": events_df.at[pidx, "salary_start_date"],
                    "salary_start_date_new": events_df.at[idx, "salary_start_date"],
                    "salary_amount_monthly_old": events_df.at[pidx, "amount_monthly"],
                    "salary_amount_monthly_new": events_df.at[idx, "amount_monthly"],
                    "drop_reason": "missing_or_nonpositive_previous_monthly_amount",
                }
            )

    merged = events_df["increase_csv_only"].where(
        events_df["increase_csv_only"].notna(),
        events_df["increase_diff_only"],
    )
    events_df["increase_merged_pref_csv"] = merged
    events_df["is_first_salary_in_file"] = 0
    file_first = events_df.groupby(["cao_number", "file_name"], dropna=False)["salary_start_date"].transform("min")
    same_day = events_df["salary_start_date"].notna() & (events_df["salary_start_date"] == file_first)
    events_df.loc[same_day, "is_first_salary_in_file"] = 1

    comparison = events_df[
        events_df["increase_csv_only"].notna() & events_df["increase_diff_only"].notna()
    ].copy()
    comparison["abs_diff"] = np.nan
    comparison["within_0_1pp"] = np.nan
    comparison["abs_diff_gt_0_1"] = np.nan
    comparison["sign_disagreement"] = np.nan
    if len(comparison) > 0:
        comparison["abs_diff"] = (comparison["increase_csv_only"] - comparison["increase_diff_only"]).abs()
        comparison["within_0_1pp"] = comparison["abs_diff"] <= 0.1
        comparison["abs_diff_gt_0_1"] = comparison["abs_diff"] > 0.1
        comparison["sign_disagreement"] = np.sign(comparison["increase_csv_only"]) != np.sign(
            comparison["increase_diff_only"]
        )

    band_summary = compute_band_summary_stats(events_df)

    return {
        "events": events_df,
        "conversion_diagnostics": pd.DataFrame(diagnostics_rows),
        "comparison": comparison,
        "band_summary": band_summary,
    }
