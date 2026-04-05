"""
Cohort construction and CAO-equal inverse-frequency weights for salary descriptive plots.

Provides weighted quantiles and Matplotlib ``bxp``-ready box statistics (no reliance on
library weighted ``boxplot``), overlap resolution for salary-start-year cohorts, governing-file
selection for contract cohort years, **Latest CAO** salary panels (**contract**: calendar year + all slots on active file;
**salary year**: effective per-key slots + file-transition gap carry), ``snap_active_table_to_band_eligible_salary_files``
so the nominal forward-filled file is not used until that file has ≥1 band-eligible long row (carry prior file), and Latest increase panels with optional 0% imputation.

USAGE:
    Imported by ``descriptives_salary_plots.py`` only.

PARAMETERS / RETURNS:
    See each function docstring.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from scripts.excel_analysis.analysis_utils import (
    coerce_salary_amount_scalar,
    detect_salary_slot_indices,
)
from scripts.excel_analysis.salary_increase_derivation import (
    compute_analysis_monthly_floor_and_band_ok,
    normalize_salary_slot_to_monthly,
)


def weighted_quantile(x: np.ndarray, w: np.ndarray, q: float) -> float:
    """
    Linear-interpolated weighted quantile for ``q`` in ``[0, 1]``.

    Args:
        x: Sample values.
        w: Non-negative weights (same length as ``x``).
        q: Quantile level.

    Returns:
        Interpolated quantile, or ``nan`` if undefined.
    """
    x = np.asarray(x, dtype=float)
    w = np.asarray(w, dtype=float)
    m = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not m.any():
        return float("nan")
    x = x[m]
    w = w[m]
    sw = float(w.sum())
    if sw <= 0:
        return float("nan")
    w = w / sw
    o = np.argsort(x, kind="mergesort")
    x = x[o]
    w = w[o]
    c = np.cumsum(w)
    if q <= c[0]:
        return float(x[0])
    if q >= c[-1]:
        return float(x[-1])
    j = int(np.searchsorted(c, q, side="left"))
    if j <= 0:
        return float(x[0])
    if j >= len(x):
        return float(x[-1])
    c_lo = c[j - 1]
    c_hi = c[j]
    if abs(c_hi - c_lo) < 1e-15:
        return float(x[j])
    t = (q - c_lo) / (c_hi - c_lo)
    return float(x[j - 1] * (1.0 - t) + x[j] * t)


def weighted_boxplot_stats_for_bxp(x: np.ndarray, w: np.ndarray) -> Dict[str, float]:
    """
    Tukey-style box stats (median, quartiles, whiskers inside 1.5×IQR) for ``bxp``.

    Args:
        x: Values.
        w: Weights (normalized internally).

    Returns:
        Dict with keys ``med``, ``q1``, ``q3``, ``whislo``, ``whishi`` (all floats).
    """
    x = np.asarray(x, dtype=float)
    w = np.asarray(w, dtype=float)
    m = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if not m.any():
        return {"med": float("nan"), "q1": float("nan"), "q3": float("nan"), "whislo": float("nan"), "whishi": float("nan")}
    x = x[m]
    w = w[m]
    if len(x) == 1:
        v = float(x[0])
        return {"med": v, "q1": v, "q3": v, "whislo": v, "whishi": v}
    q1 = weighted_quantile(x, w, 0.25)
    med = weighted_quantile(x, w, 0.5)
    q3 = weighted_quantile(x, w, 0.75)
    if not (np.isfinite(q1) and np.isfinite(q3)):
        v = float(np.average(x, weights=w))
        return {"med": v, "q1": v, "q3": v, "whislo": v, "whishi": v}
    iqr = q3 - q1
    if not np.isfinite(iqr) or iqr < 0:
        iqr = 0.0
    fence_lo = q1 - 1.5 * iqr
    fence_hi = q3 + 1.5 * iqr
    inside = (x >= fence_lo) & (x <= fence_hi)
    if inside.any():
        whislo = float(np.min(x[inside]))
        whishi = float(np.max(x[inside]))
    else:
        whislo = float(np.min(x))
        whishi = float(np.max(x))
    return {"med": float(med), "q1": float(q1), "q3": float(q3), "whislo": whislo, "whishi": whishi}


def attach_cao_equal_weights(df: pd.DataFrame, cao_col: str, cohort_col: str, out_col: str = "cao_weight") -> pd.DataFrame:
    """
    CAO-equal weights: within each cohort year ``y``, row ``i`` in CAO ``c`` gets ``w_i = 1 / n_{c,y}``.

    ``n_{c,y}`` counts **rows** in this frame for that CAO and cohort after all figure-specific filters (overlap,
    governing file, deduplication, synthetic 0% rows, etc.). Weighted cohort means then match the mean of per-CAO
    means on the same retained rows.

    Args:
        df: Final retained sample ``S`` for one figure (all cohort years stacked).
        cao_col: CAO identifier column.
        cohort_col: Cohort year column.
        out_col: Output weight column name.

    Returns:
        Copy of ``df`` with ``out_col``.
    """
    if len(df) == 0:
        return df.copy()
    out = df.copy()
    grp = out.groupby([cao_col, cohort_col], dropna=False).transform("size")
    out[out_col] = 1.0 / grp.astype(float)
    return out


def filter_newest_file_overlap_salary_start_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Within each ``(cao_number, salary_start_year)``, keep rows only from the newest ``ingangsdatum``;
    tie-break lexicographically larger ``file_name``.

    Args:
        df: Long or event frame with ``cao_number``, ``salary_start_year``, ``ingangsdatum``, ``file_name``.

    Returns:
        Filtered copy.
    """
    need = {"cao_number", "salary_start_year", "ingangsdatum", "file_name"}
    if len(df) == 0 or not need.issubset(df.columns):
        return df.copy()
    w = df.copy()
    w["_ing"] = pd.to_datetime(w["ingangsdatum"], errors="coerce")
    keys = ["cao_number", "salary_start_year"]
    picked = (
        w.sort_values(keys + ["_ing", "file_name"], ascending=[True, True, False, False])
        .drop_duplicates(keys, keep="first")[keys + ["file_name"]]
    )
    return df.merge(picked, on=keys + ["file_name"], how="inner")


def governing_file_keys_contract_cohort(df_rows: pd.DataFrame) -> pd.DataFrame:
    """
    For each ``(cao_number, contract_start_year)``, choose one ``file_name`` (latest ``ingangsdatum``, tie ``file_name``).

    Args:
        df_rows: Rows with ``cao_number``, ``file_name``, ``ingangsdatum``, ``contract_start_year``.

    Returns:
        DataFrame columns ``cao_number``, ``contract_start_year``, ``file_name`` (one row per key).
    """
    if len(df_rows) == 0:
        return pd.DataFrame(columns=["cao_number", "contract_start_year", "file_name"])
    w = df_rows[["cao_number", "file_name", "ingangsdatum", "contract_start_year"]].drop_duplicates()
    w = w[w["contract_start_year"].notna() & w["cao_number"].notna() & w["file_name"].notna()]
    w["_ing"] = pd.to_datetime(w["ingangsdatum"], errors="coerce")
    w = w[w["_ing"].notna()]
    w["_y"] = w["_ing"].dt.year.astype(int)
    w["contract_start_year"] = pd.to_numeric(w["contract_start_year"], errors="coerce").astype("Int64")
    w = w.dropna(subset=["contract_start_year"])
    w["contract_start_year"] = w["contract_start_year"].astype(int)
    w = w[w["_y"] == w["contract_start_year"]]
    keys = ["cao_number", "contract_start_year"]
    picked = (
        w.sort_values(keys + ["_ing", "file_name"], ascending=[True, True, False, False])
        .drop_duplicates(keys, keep="first")[keys + ["file_name"]]
    )
    return picked


def filter_long_by_governing_file_contract(
    df_long: pd.DataFrame,
) -> pd.DataFrame:
    """
    Restrict long salary rows to governing ``file_name`` per ``(cao_number, contract_start_year)``.

    Args:
        df_long: Enriched long frame with ``ingangsdatum``, ``contract_start_year``, etc.

    Returns:
        Inner-join filtered copy.
    """
    if len(df_long) == 0:
        return df_long.copy()
    gf = governing_file_keys_contract_cohort(df_long)
    if len(gf) == 0:
        return df_long.iloc[0:0].copy()
    out = df_long.merge(gf, on=["cao_number", "contract_start_year", "file_name"], how="inner")
    return out


def latest_cao_active_file_table(df_latest_wide: pd.DataFrame) -> pd.DataFrame:
    """
    One row per ``(cao_number, salary_year, file_name)`` from the forward-filled panel (first row per CAO×year).

    Args:
        df_latest_wide: Output of ``build_latest_cao_forward_fill`` (must have ``contract_start_year`` column used as calendar year).

    Returns:
        Columns ``cao_number``, ``salary_year``, ``file_name``.
    """
    if len(df_latest_wide) == 0 or "contract_start_year" not in df_latest_wide.columns:
        return pd.DataFrame(columns=["cao_number", "salary_year", "file_name"])
    need = ["cao_number", "contract_start_year", "file_name"]
    if not all(c in df_latest_wide.columns for c in need):
        return pd.DataFrame(columns=["cao_number", "salary_year", "file_name"])
    t = (
        df_latest_wide[need]
        .dropna(subset=["cao_number", "contract_start_year", "file_name"])
        .drop_duplicates(["cao_number", "contract_start_year", "file_name"])
        .copy()
    )
    t = t.rename(columns={"contract_start_year": "salary_year"})
    t["salary_year"] = pd.to_numeric(t["salary_year"], errors="coerce").astype("Int64")
    return t.dropna(subset=["salary_year"]).assign(salary_year=lambda z: z["salary_year"].astype(int))


def snap_active_table_to_band_eligible_salary_files(
    active_table: pd.DataFrame,
    df_slot: pd.DataFrame,
) -> pd.DataFrame:
    """
    Adjust nominal latest-CAO ``active_table`` so ``file_name`` always refers to a contract file that has at least
    one band-eligible long salary row for that CAO in ``df_slot``.

    Walks each CAO's calendar years in order. If the nominal ``(cao_number, file_name)`` appears in ``df_slot``,
    that file becomes the carried effective file; otherwise the previous effective file is reused. Rows where no
    effective file exists yet (no prior carry and nominal file has no eligible rows) are omitted.

    Args:
        active_table: Columns ``cao_number``, ``salary_year``, ``file_name`` (from ``latest_cao_active_file_table``).
        df_slot: Governed, band-eligible long salary frame with ``cao_number`` and ``file_name``.

    Returns:
        DataFrame with the same three columns; subset of years per CAO where an effective file was resolved.
        If ``df_slot`` is empty, returns ``active_table`` unchanged.
    """
    if len(active_table) == 0:
        return active_table.copy()
    need_at = {"cao_number", "salary_year", "file_name"}
    if not need_at.issubset(active_table.columns):
        return active_table.copy()
    if len(df_slot) == 0 or not {"cao_number", "file_name"}.issubset(df_slot.columns):
        return active_table.copy()
    pairs = (
        df_slot[["cao_number", "file_name"]]
        .dropna(subset=["cao_number", "file_name"])
        .drop_duplicates()
    )
    if len(pairs) == 0:
        return active_table.copy()
    eligible = set(map(tuple, pairs.to_numpy()))
    at = active_table.copy()
    at["salary_year"] = pd.to_numeric(at["salary_year"], errors="coerce")
    at = at.dropna(subset=["salary_year", "cao_number"])
    at["salary_year"] = at["salary_year"].astype(int)
    at = at.sort_values(["cao_number", "salary_year"], kind="mergesort")
    out_rows: List[Dict[str, Any]] = []
    for cao, grp in at.groupby("cao_number", sort=False):
        carry: Any = None
        for _, row in grp.iterrows():
            T = int(row["salary_year"])
            nom = row["file_name"]
            if pd.notna(nom) and (cao, nom) in eligible:
                carry = nom
                snapped = carry
            else:
                snapped = carry
            if snapped is not None and pd.notna(snapped):
                out_rows.append({"cao_number": cao, "salary_year": T, "file_name": snapped})
    return pd.DataFrame(out_rows).reset_index(drop=True)


def _wide_row_id(row: pd.Series, pos_idx: Any) -> Any:
    """Stable wide-row identifier: ``id`` if present/non-null, else positional index."""
    if "id" in row.index and pd.notna(row.get("id")):
        return row.get("id")
    return pos_idx


def effective_band_eligible_slots_cao_file_year_end(
    df_wide: pd.DataFrame,
    cao_number: Any,
    file_name: Any,
    year_t: int,
) -> pd.DataFrame:
    """
    Band-eligible salary slots for one CAO and file: latest ``salary_start_date`` ≤ 31 Dec ``year_t`` per ``(row_id, salary_index)``.

    Args:
        df_wide: Wide salary frame.
        cao_number: CAO id.
        file_name: Contract file name.
        year_t: Calendar year ``T``.

    Returns:
        DataFrame columns include ``cao_number``, ``file_name``, ``row_id``, ``salary_index``,
        ``salary_start_date``, ``amount_monthly``, ``ingang`` metadata; **no** ``salary_year`` column.
    """
    if len(df_wide) == 0:
        return pd.DataFrame()
    end_t = pd.Timestamp(year=int(year_t), month=12, day=31)
    sub = df_wide[(df_wide["cao_number"] == cao_number) & (df_wide["file_name"] == file_name)].copy()
    if len(sub) == 0:
        return pd.DataFrame()
    slot_indices = detect_salary_slot_indices(df_wide.columns.tolist())
    rows_out: List[Dict[str, Any]] = []
    for pos_idx in sub.index:
        row = sub.loc[pos_idx]
        row_id = _wide_row_id(row, pos_idx)
        for k in slot_indices:
            start_col = f"salary_{k}_start_date"
            amt_col = f"salary_{k}_amount"
            unit_col = f"salary_{k}_unit"
            hcol = f"salary_{k}_hours_basis_ft_week"
            if start_col not in df_wide.columns or amt_col not in df_wide.columns:
                continue
            sd = pd.to_datetime(row.get(start_col), errors="coerce")
            if pd.isna(sd) or sd > end_t:
                continue
            amt_raw = row.get(amt_col)
            amt = coerce_salary_amount_scalar(amt_raw)
            if amt is None or amt <= 0:
                continue
            unit = row.get(unit_col) if unit_col in row.index else np.nan
            sh = row.get(hcol) if hcol in row.index else np.nan
            rf = row.get("ft_hours", np.nan)
            m, ok, _ = normalize_salary_slot_to_monthly(amt, unit, sh, rf)
            if not ok or m is None:
                continue
            floor_arr, band_ok = compute_analysis_monthly_floor_and_band_ok(
                np.array([True], dtype=bool),
                pd.Series([sd]),
                [m],
            )
            if not bool(band_ok[0]):
                continue
            rows_out.append(
                {
                    "cao_number": cao_number,
                    "file_name": file_name,
                    "row_id": row_id,
                    "salary_index": k,
                    "salary_start_date": sd,
                    "amount_monthly": float(m),
                    "analysis_monthly_band_ok": True,
                    "ingangsdatum": row.get("ingangsdatum"),
                }
            )
    if not rows_out:
        return pd.DataFrame()
    df_e = pd.DataFrame(rows_out)
    keys = ["cao_number", "file_name", "row_id", "salary_index"]
    return (
        df_e.sort_values(keys + ["salary_start_date"], ascending=[True, True, True, True, False])
        .drop_duplicates(keys, keep="first")
        .reset_index(drop=True)
    )


def build_active_band_eligible_salary_rows_for_salary_year(
    df_wide: pd.DataFrame,
    active_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each ``(cao, salary_year T, file)`` in ``active_table``, emit one row per active slot with latest start ≤ end of ``T``.

    Args:
        df_wide: Full wide salary CSV-style frame.
        active_table: From ``latest_cao_active_file_table`` (``cao_number``, ``salary_year``, ``file_name``).

    Returns:
        Long-form DataBand with ``cao_number``, ``salary_year``, ``file_name``, ``row_id``, ``salary_index``,
        ``salary_start_date``, ``amount_monthly``, ``analysis_monthly_band_ok``, ``ingangsdatum``, etc.
    """
    if len(active_table) == 0 or len(df_wide) == 0:
        return pd.DataFrame()
    slot_indices = detect_salary_slot_indices(df_wide.columns.tolist())
    rows_out: List[Dict[str, Any]] = []
    ft_hours_series = df_wide["ft_hours"] if "ft_hours" in df_wide.columns else pd.Series(np.nan, index=df_wide.index)
    for _, arow in active_table.iterrows():
        cao = arow["cao_number"]
        T = int(arow["salary_year"])
        fn = arow["file_name"]
        end_t = pd.Timestamp(year=T, month=12, day=31)
        sub = df_wide[(df_wide["cao_number"] == cao) & (df_wide["file_name"] == fn)].copy()
        if len(sub) == 0:
            continue
        for pos_idx in sub.index:
            row = sub.loc[pos_idx]
            row_id = row["id"] if "id" in row.index and pd.notna(row.get("id")) else pos_idx
            for k in slot_indices:
                start_col = f"salary_{k}_start_date"
                amt_col = f"salary_{k}_amount"
                unit_col = f"salary_{k}_unit"
                hcol = f"salary_{k}_hours_basis_ft_week"
                if start_col not in df_wide.columns or amt_col not in df_wide.columns:
                    continue
                sd = pd.to_datetime(row.get(start_col), errors="coerce")
                if pd.isna(sd) or sd > end_t:
                    continue
                amt_raw = row.get(amt_col)
                amt = coerce_salary_amount_scalar(amt_raw)
                if amt is None or amt <= 0:
                    continue
                unit = row.get(unit_col) if unit_col in row.index else np.nan
                sh = row.get(hcol) if hcol in row.index else np.nan
                rf = row.get("ft_hours", np.nan)
                m, ok, _ = normalize_salary_slot_to_monthly(amt, unit, sh, rf)
                if not ok or m is None:
                    continue
                floor_arr, band_ok = compute_analysis_monthly_floor_and_band_ok(
                    np.array([True], dtype=bool),
                    pd.Series([sd]),
                    [m],
                )
                if not bool(band_ok[0]):
                    continue
                rows_out.append(
                    {
                        "cao_number": cao,
                        "salary_year": T,
                        "file_name": fn,
                        "row_id": row_id,
                        "salary_index": k,
                        "salary_start_date": sd,
                        "amount_monthly": float(m),
                        "analysis_monthly_band_ok": True,
                        "ingangsdatum": row.get("ingangsdatum"),
                    }
                )
    if not rows_out:
        return pd.DataFrame()
    df_out = pd.DataFrame(rows_out)
    return df_out


def dedupe_latest_active_slot_per_key(df_active: pd.DataFrame) -> pd.DataFrame:
    """
    Per ``(cao_number, salary_year, file_name, row_id, salary_index)``, keep row with max ``salary_start_date`` ≤ year end.

    Args:
        df_active: Output of ``build_active_band_eligible_salary_rows_for_salary_year`` (possibly duplicates).

    Returns:
        Deduplicated DataFrame.
    """
    if len(df_active) == 0:
        return df_active
    keys = ["cao_number", "salary_year", "file_name", "row_id", "salary_index"]
    return (
        df_active.sort_values(keys + ["salary_start_date"], ascending=[True, True, True, True, True, False])
        .drop_duplicates(keys, keep="first")
    )


def build_contract_year_latest_salary_calendar_panel(
    df_slot_band_eligible: pd.DataFrame,
    active_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Latest CAO **contract** salary figure: x-axis is **calendar year** ``T``.

    For each ``(cao_number, file_name, T)`` in ``active_table``, include **every** band-eligible long slot row
    on that file (no ``salary_start_date`` vs ``T`` filter). Replicates each slot row across all calendar years
    where that CAO×file is active. Dedupes duplicate ``(cao_number, calendar_year, file_name, row_id, salary_index)``
    keeping latest ``salary_start_date``.

    Args:
        df_slot_band_eligible: Enriched long salary rows with ``analysis_monthly_band_ok`` True, ``amount_monthly``, etc.
        active_table: From ``latest_cao_active_file_table`` (``cao_number``, ``salary_year``, ``file_name``).

    Returns:
        Long frame with ``calendar_year`` (integer ``T``), ready for ``attach_cao_equal_weights(cao, calendar_year)``.
    """
    if len(df_slot_band_eligible) == 0 or len(active_table) == 0:
        return pd.DataFrame()
    need = {"cao_number", "file_name", "amount_monthly", "salary_index"}
    if not need.issubset(df_slot_band_eligible.columns):
        return pd.DataFrame()
    d = df_slot_band_eligible.copy()
    if "row_id" not in d.columns:
        if "id" in d.columns:
            d["row_id"] = d["id"]
        else:
            d["row_id"] = np.arange(len(d), dtype=np.int64)
    at = active_table.rename(columns={"salary_year": "calendar_year"}).copy()
    at["calendar_year"] = pd.to_numeric(at["calendar_year"], errors="coerce").astype("Int64")
    at = at.dropna(subset=["calendar_year"])
    at["calendar_year"] = at["calendar_year"].astype(int)
    m = d.merge(at, on=["cao_number", "file_name"], how="inner")
    if len(m) == 0:
        return pd.DataFrame()
    keys = ["cao_number", "calendar_year", "file_name", "row_id", "salary_index"]
    if "salary_start_date" in m.columns:
        m = m.copy()
        m["_sd"] = pd.to_datetime(m["salary_start_date"], errors="coerce")
        m = m.sort_values(keys + ["_sd"], ascending=[True, True, True, True, True, False])
        m = m.drop_duplicates(keys, keep="first").drop(columns=["_sd"], errors="ignore")
    else:
        m = m.drop_duplicates(keys, keep="first")
    return m.reset_index(drop=True)


def build_salary_year_latest_gap_panel(
    df_wide: pd.DataFrame,
    active_table: pd.DataFrame,
) -> pd.DataFrame:
    """
    Latest CAO **Salary year** panel: per calendar ``T`` Effective slots = latest band-eligible step with
    ``salary_start_date`` ≤ end(``T``) per ``(row_id, salary_index)`` on the active file.

    **File-transition gap:** If that set is empty for ``(cao, T)`` but the CAO had a non-empty effective set in
    ``T-1``, reuse that entire row set for ``T`` (e.g. new contract file dated ``T`` but first step starts in ``T+1``).

    Args:
        df_wide: Full wide salary DataFrame.
        active_table: ``cao_number``, ``salary_year``, ``file_name`` (``salary_year`` = calendar ``T``).

    Returns:
        Long frame with ``salary_year`` (= ``T``), ``amount_monthly``, ``row_id``, ``salary_index``, etc.
    """
    if len(active_table) == 0 or len(df_wide) == 0:
        return pd.DataFrame()
    at = active_table.drop_duplicates(["cao_number", "salary_year"]).copy()
    at["salary_year"] = pd.to_numeric(at["salary_year"], errors="coerce").astype(int)
    years = sorted(at["salary_year"].unique())
    snapshot: Dict[Any, pd.DataFrame] = {}
    out_parts: List[pd.DataFrame] = []

    for T in years:
        t_rows = at.loc[at["salary_year"] == T, ["cao_number", "file_name"]]
        for _, crow in t_rows.iterrows():
            cao = crow["cao_number"]
            fn = crow["file_name"]
            fresh = effective_band_eligible_slots_cao_file_year_end(df_wide, cao, fn, int(T))
            if len(fresh) == 0 and cao in snapshot:
                block = snapshot[cao].copy()
                block["salary_year"] = int(T)
                out_parts.append(block)
            elif len(fresh) > 0:
                block = fresh.copy()
                block["salary_year"] = int(T)
                out_parts.append(block)
                snapshot[cao] = fresh.copy()

    if not out_parts:
        return pd.DataFrame()
    return pd.concat(out_parts, ignore_index=True)


def build_latest_increase_salary_year_panel(
    increase_events: pd.DataFrame,
    active_table: pd.DataFrame,
    value_col: str = "increase_merged_pref_csv",
) -> pd.DataFrame:
    """
    Per ``(cao, salary_year T)`` from ``active_table``, attach events in ``T`` on that file; else one 0% row if eligible.

    Args:
        increase_events: Event frame from ``derive_salary_increase_series``.
        active_table: ``cao_number``, ``salary_year``, ``file_name``.
        value_col: Numeric increase column (merged by default).

    Returns:
        DataFrame with ``cao_number``, ``salary_year``, ``file_name``, ``ingangsdatum``, cohort value column, synthetic flag.
    """
    if len(active_table) == 0:
        return pd.DataFrame()
    if increase_events is None or len(increase_events) == 0:
        ev = pd.DataFrame()
    else:
        ev = increase_events.copy()
    need_ev = {"cao_number", "file_name", "ingangsdatum", "salary_start_year", value_col, "analysis_monthly_band_ok"}
    out_rows: List[Dict[str, Any]] = []
    for _, arow in active_table.iterrows():
        cao = arow["cao_number"]
        T = int(arow["salary_year"])
        fn = arow["file_name"]
        if len(ev) and need_ev.issubset(ev.columns):
            mask = (
                (ev["cao_number"] == cao)
                & (ev["file_name"] == fn)
                & (pd.to_numeric(ev["salary_start_year"], errors="coerce") == T)
                & ev["analysis_monthly_band_ok"].fillna(False)
                & pd.to_numeric(ev[value_col], errors="coerce").notna()
            )
            sub = ev.loc[mask]
        else:
            sub = pd.DataFrame()
        if len(sub) > 0:
            for _, er in sub.iterrows():
                out_rows.append(
                    {
                        "cao_number": cao,
                        "salary_year": T,
                        "file_name": fn,
                        "ingangsdatum": er.get("ingangsdatum"),
                        value_col: float(pd.to_numeric(er[value_col], errors="coerce")),
                        "_synthetic_zero": False,
                    }
                )
        else:
            out_rows.append(
                {
                    "cao_number": cao,
                    "salary_year": T,
                    "file_name": fn,
                    "ingangsdatum": np.nan,
                    value_col: 0.0,
                    "_synthetic_zero": True,
                }
            )
    return pd.DataFrame(out_rows)


def weighted_percentile_nw(x: np.ndarray, w: np.ndarray, p: float) -> float:
    """Plain wrapper: ``p`` in [0,100] scale for compatibility with ``nanpercentile`` call sites."""
    return weighted_quantile(x, w, p / 100.0)
