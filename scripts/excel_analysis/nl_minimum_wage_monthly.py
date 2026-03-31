"""
Dutch statutory gross minimum wage (reference monthly amounts, 1 January schedule).

Loads ``conf/nl_statutory_minimum_monthly_gross_eur.csv`` and resolves the applicable
monthly minimum for a given effective date (last row with valid_from <= date).
Dates before the first table entry use the first row; dates on or after the last
``valid_from`` use the last row.

How to use:
    from scripts.excel_analysis.nl_minimum_wage_monthly import minimum_monthly_gross_eur_series
    floors = minimum_monthly_gross_eur_series(df[\"salary_start_date\"])
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CSV = _REPO_ROOT / "conf" / "nl_statutory_minimum_monthly_gross_eur.csv"


@lru_cache(maxsize=1)
def _load_table(csv_path: Optional[str] = None) -> tuple[np.ndarray, np.ndarray]:
    """
    Load statutory schedule as sorted int64 day ordinals and monthly EUR array.

    Args:
        csv_path: Optional path to CSV; defaults to conf/nl_statutory_minimum_monthly_gross_eur.csv.

    Returns:
        Tuple of (valid_from_ordinals int64, monthly_amounts float64).
    """
    path = Path(csv_path) if csv_path else _DEFAULT_CSV
    if not path.is_file():
        raise FileNotFoundError(f"NL minimum wage CSV not found: {path}")
    df = pd.read_csv(path)
    if "valid_from" not in df.columns or "monthly_gross_eur" not in df.columns:
        raise ValueError("CSV must contain valid_from and monthly_gross_eur columns")
    df = df.sort_values("valid_from").reset_index(drop=True)
    vd = pd.to_datetime(df["valid_from"], errors="coerce")
    if vd.isna().any():
        raise ValueError("Invalid valid_from in NL minimum wage CSV")
    monthly = pd.to_numeric(df["monthly_gross_eur"], errors="coerce").to_numpy(dtype=np.float64)
    if np.isnan(monthly).any():
        raise ValueError("Invalid monthly_gross_eur in NL minimum wage CSV")
    ords = vd.dt.normalize().values.astype("datetime64[D]").astype(np.int64)
    return ords, monthly


def minimum_monthly_gross_eur(
    as_of: Any,
    csv_path: Optional[str] = None,
) -> float:
    """
    Return statutory gross monthly minimum (EUR) for a single timestamp/date.

    Args:
        as_of: pandas Timestamp, datetime, or date; NaT/None yields NaN.
        csv_path: Optional override path to the statutory CSV.

    Returns:
        Monthly amount in EUR, or np.nan if as_of is missing.
    """
    if as_of is None or (isinstance(as_of, float) and np.isnan(as_of)):
        return float("nan")
    ts = pd.Timestamp(as_of)
    if pd.isna(ts):
        return float("nan")
    day = ts.normalize()
    first_ord, monthly = _load_table(csv_path)
    d_ord = np.datetime64(day.date(), "D").astype(np.int64)
    # Before first valid_from: use first schedule row (project convention).
    d_ord = max(d_ord, int(first_ord[0]))
    idx = int(np.searchsorted(first_ord, d_ord, side="right") - 1)
    idx = max(0, min(idx, len(monthly) - 1))
    return float(monthly[idx])


def minimum_monthly_gross_eur_series(
    dates: pd.Series,
    csv_path: Optional[str] = None,
) -> np.ndarray:
    """
    Vectorized statutory gross monthly minimum (EUR) for a Series of datetimes.

    Args:
        dates: Series of datelike values; NaT positions yield NaN.

    Returns:
        float64 ndarray aligned to ``dates`` index.
    """
    first_ord, monthly = _load_table(csv_path)
    ts = pd.to_datetime(dates, errors="coerce")
    n = len(ts)
    out = np.full(n, np.nan, dtype=np.float64)
    if n == 0:
        return out
    mask_ok = ts.notna()
    if not mask_ok.any():
        return out
    norm = ts.dt.normalize()
    d_ord = norm[mask_ok].values.astype("datetime64[D]").astype(np.int64)
    d_ord_clamped = np.maximum(d_ord, int(first_ord[0]))
    idx = np.searchsorted(first_ord, d_ord_clamped, side="right") - 1
    idx = np.clip(idx, 0, len(monthly) - 1)
    out[np.flatnonzero(mask_ok.to_numpy())] = monthly[idx]
    return out
