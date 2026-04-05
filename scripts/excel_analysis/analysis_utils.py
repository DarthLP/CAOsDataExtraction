"""
Excel Analysis Utilities

This module provides utility functions for analyzing Excel output data from CAO extraction,
including unit conversions (e.g. monthly, hourly, weekly, daily/`d`, annual), statistical summaries, and data processing helpers.

USAGE:
    from scripts.excel_analysis.analysis_utils import (
        convert_salary_to_monthly, calculate_descriptive_stats,
        create_boolean_summary, group_cao_timeline
    )
"""

import ast
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import re
from datetime import datetime
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator

# Plausible gross monthly salary range (EUR) after unit normalization; outside → caller may mark conversion_ok False.
PLAUSIBLE_MONTHLY_MIN = 1.0
PLAUSIBLE_MONTHLY_MAX = 100_000.0

# Upper cap (EUR monthly, after normalization) for salary-increase analysis band; excludes extreme OCR/extraction errors.
SALARY_ANALYSIS_MONTHLY_CAP_EUR = 50_000.0

_FT_WEEKLY_MAX_REASONABLE = 72.0
_ANNUAL_HOURS_THRESHOLD = 200.0


def coerce_salary_amount_scalar(amount: Any) -> Optional[float]:
    """
    Parse a salary amount cell with European and US-style grouping (comma vs dot as decimal).

    Distinguishes ``2.230,91`` (NL) from ``2,230.91`` (US) using the rightmost separator.
    Multiple dots without a comma are treated as thousands markers (e.g. ``1.234.567``).

    Args:
        amount: Raw scalar from CSV (str, int, float)

    Returns:
        Float or None if missing / not parseable.
    """
    if amount is None or (isinstance(amount, float) and pd.isna(amount)):
        return None
    if isinstance(amount, str):
        s = amount.strip().replace(" ", "").replace("\u00a0", "").replace('"', "")
        if not s or s.lower() in ("nan", "none", "null"):
            return None
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if "," in s and "." in s:
            if last_comma > last_dot:
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif "," in s:
            s = s.replace(",", ".")
        elif s.count(".") > 1:
            s = s.replace(".", "")
        try:
            return float(s)
        except ValueError:
            return None
    out = pd.to_numeric(pd.Series([amount]), errors="coerce").iloc[0]
    if pd.isna(out):
        return None
    return float(out)


def coerce_ft_hours_per_week_for_conversion(ft_hours: Optional[float]) -> float:
    """
    Interpret ft_hours as weekly hours for hourly↔monthly conversion.
    Values > _ANNUAL_HOURS_THRESHOLD are treated as annual hours and divided by 52.
    Missing or non-positive values fall back to 40.

    Args:
        ft_hours: Raw full-time hours (weekly or sometimes annual from source).

    Returns:
        Hours per week suitable for multiply with hourly wage.
    """
    if ft_hours is None or pd.isna(ft_hours):
        return 40.0
    try:
        h = float(ft_hours)
    except (TypeError, ValueError):
        return 40.0
    if h <= 0:
        return 40.0
    if h > _ANNUAL_HOURS_THRESHOLD:
        h = h / 52.0
    if h > _FT_WEEKLY_MAX_REASONABLE:
        return 40.0
    return h


def _is_four_weekly_unit(unit_lower: str) -> bool:
    """True if unit string denotes a 4-weekly pay period (before generic 'weekly')."""
    u = unit_lower.strip()
    if not u:
        return False
    if re.search(r"4\s*[-]?\s*w", u, re.I):
        return True
    if "4-week" in u or "4 week" in u:
        return True
    if "four" in u and "week" in u:
        return True
    if u in ("4-w", "4w"):
        return True
    return False


def _is_monthly_unit(unit_lower: str) -> bool:
    u = unit_lower.strip()
    if u in ("m", "mo", "/m"):
        return True
    return "month" in u


def _is_hourly_unit(unit_lower: str) -> bool:
    """
    True for a wage rate per clock hour (not a fixed fee for an ``N-hour`` slice like ``3-hour activity``).

    Args:
        unit_lower: Pay unit string, lowercased.

    Returns:
        True for compact hour codes, explicit ``hourly`` / ``per hour`` / ``/hour``, or generic ``hour``/``uur``
        unless the unit looks like ``<n>-hour`` duration (activity / block), which is not hourly pay.
    """
    u = unit_lower.strip()
    if u in ("h", "hr", "hrs", "uur", "u"):
        return True
    if "hourly" in u or "per hour" in u or "/hour" in u:
        return True
    if re.search(r"\d+\s*-\s*hour", u):
        return False
    return "hour" in u or u == "uur"


def _is_annual_unit(unit_lower: str) -> bool:
    u = unit_lower.strip()
    if u in ("a", "y", "yr", "yrs"):
        return True
    if "annual" in u or "yearly" in u:
        return True
    if re.search(r"\byear\b", u):
        return True
    if "per year" in u or "/year" in u or "/jaar" in u:
        return True
    return False


def _is_weekly_unit(unit_lower: str) -> bool:
    """Weekly pay (not caught as 4-weekly)."""
    u = unit_lower.strip()
    if u in ("w",):
        return True
    if _is_four_weekly_unit(u):
        return False
    return "week" in u


def _is_daily_unit(unit_lower: str) -> bool:
    """
    True if the unit denotes a per-working-day rate: ``d``, ``daily``, or offshore day equivalents.

    Args:
        unit_lower: Pay unit string, lowercased and stripped by the caller.

    Returns:
        True for ``d``, ``daily``, ``offshore day``, truncated ``offshore da``, or ``per offshore`` variants.
    """
    u = unit_lower.strip()
    if not u:
        return False
    if u == "d":
        return True
    if "daily" in u:
        return True
    if re.search(r"\boffshore\s+day\b", u):
        return True
    if re.search(r"\boffshore\s+da$", u):
        return True
    if re.search(r"\bper\s+offshore\b", u):
        return True
    return False


def normalize_salary_amounts_with_units(amounts: pd.Series, units: pd.Series, ft_hours: Optional[pd.Series] = None) -> pd.Series:
    """
    Normalize salary amounts to monthly equivalent for entire series.
    
    Args:
        amounts: Series of salary amounts
        units: Series of corresponding units
        ft_hours: Series of full-time hours (optional)
        
    Returns:
        Series of normalized monthly amounts
    """
    normalized = []
    
    for i in range(len(amounts)):
        amount = amounts.iloc[i] if i < len(amounts) else None
        unit = units.iloc[i] if i < len(units) else None
        ft_hour = ft_hours.iloc[i] if ft_hours is not None and i < len(ft_hours) else None
        
        if pd.notna(amount) and pd.notna(unit):
            normalized.append(convert_salary_to_monthly(amount, unit, ft_hour))
        else:
            normalized.append(None)
    
    return pd.Series(normalized, index=amounts.index)


def convert_salary_to_monthly(amount: Any, unit: Any, ft_hours: Optional[float] = None) -> Optional[float]:
    """
    Convert salary amounts to monthly equivalent using monthly as standard.
    Supports full words (monthly, hourly, daily, …) and compact schema codes (m, h, 4-w, w, d, a).
    Four-weekly is detected before generic weekly to avoid mis-classification.
    Daily rates (including offshore day / per offshore) assume a standard 5-day week: monthly = amount × 4.33 × 5.
    Units like ``3-hour activity`` are excluded from hourly conversion (not €/hour).
    ft_hours is coerced via coerce_ft_hours_per_week_for_conversion (annual hours ÷ 52 when > 200).
    Amounts are parsed with ``coerce_salary_amount_scalar`` (EU/US decimal rules); results are
    rounded to **2 decimal places** so semicolon CSVs opened in NL/DE Excel are not misread when
    ``.`` is the thousands separator (long tails from e.g. ``12/13`` would otherwise look like extra digit groups).

    Args:
        amount: The salary amount (numeric or string from Excel/CSV)
        unit: The pay period unit string
        ft_hours: Full-time hours per week or annual hours from source (optional)

    Returns:
        Monthly equivalent rounded to cents, or None if conversion not possible
    """
    if unit is None or (isinstance(unit, float) and pd.isna(unit)):
        return None
    unit_stripped = str(unit).strip()
    if not unit_stripped or unit_stripped.lower() == "nan":
        return None

    amt = coerce_salary_amount_scalar(amount)
    if amt is None:
        return None

    unit_lower = unit_stripped.lower()
    h_week = coerce_ft_hours_per_week_for_conversion(
        None if ft_hours is None or pd.isna(ft_hours) else float(ft_hours)
    )
    weeks_per_month = 4.33

    monthly: Optional[float] = None
    if _is_monthly_unit(unit_lower):
        monthly = amt
    elif _is_four_weekly_unit(unit_lower):
        monthly = amt * (12.0 / 13.0)
    elif _is_hourly_unit(unit_lower):
        monthly = amt * h_week * weeks_per_month
    elif _is_daily_unit(unit_lower):
        workdays_per_week = 5.0
        monthly = amt * workdays_per_week * weeks_per_month
    elif _is_weekly_unit(unit_lower):
        monthly = amt * weeks_per_month
    elif _is_annual_unit(unit_lower):
        monthly = amt / 12.0

    if monthly is None:
        return None
    return round(monthly, 2)


def is_plausible_monthly_equivalent(monthly_amount: Optional[float]) -> bool:
    """
    Return True if normalized monthly amount is within configured plausible bounds.

    Args:
        monthly_amount: Value after convert_salary_to_monthly (or None)

    Returns:
        False if None/NaN or outside [PLAUSIBLE_MONTHLY_MIN, PLAUSIBLE_MONTHLY_MAX].
    """
    if monthly_amount is None or pd.isna(monthly_amount):
        return False
    try:
        m = float(monthly_amount)
    except (TypeError, ValueError):
        return False
    return PLAUSIBLE_MONTHLY_MIN <= m <= PLAUSIBLE_MONTHLY_MAX


def calculate_descriptive_stats(series: pd.Series) -> Dict[str, Any]:
    """
    Calculate comprehensive descriptive statistics for a numeric series.
    
    Args:
        series: Pandas Series with numeric data
        
    Returns:
        Dictionary with descriptive statistics
    """
    # Convert to numeric, coercing errors to NaN
    numeric_series = pd.to_numeric(series, errors='coerce')
    
    # Remove missing values
    clean_series = numeric_series.dropna()
    
    if len(clean_series) == 0:
        return {
            'count': 0,
            'missing_count': len(series),
            'missing_pct': 100.0,
            'mean': None,
            'median': None,
            'std': None,
            'min': None,
            'max': None,
            'q25': None,
            'q75': None
        }
    
    stats = {
        'count': len(clean_series),
        'missing_count': len(series) - len(clean_series),
        'missing_pct': ((len(series) - len(clean_series)) / len(series)) * 100,
        'mean': clean_series.mean(),
        'median': clean_series.median(),
        'std': clean_series.std(),
        'min': clean_series.min(),
        'max': clean_series.max(),
        'q25': clean_series.quantile(0.25),
        'q75': clean_series.quantile(0.75)
    }
    
    return stats


def create_boolean_summary(series: pd.Series) -> Dict[str, Any]:
    """
    Create summary statistics for boolean fields.
    
    Args:
        series: Pandas Series with boolean data
        
    Returns:
        Dictionary with boolean summary statistics
    """
    total_count = len(series)
    
    # Count different values
    true_count = (series == True).sum()
    false_count = (series == False).sum()
    missing_count = series.isna().sum()
    
    return {
        'total_count': total_count,
        'true_count': true_count,
        'true_pct': (true_count / total_count) * 100 if total_count > 0 else 0,
        'false_count': false_count,
        'false_pct': (false_count / total_count) * 100 if total_count > 0 else 0,
        'missing_count': missing_count,
        'missing_pct': (missing_count / total_count) * 100 if total_count > 0 else 0
    }


def create_categorical_summary(series: pd.Series, top_n: int = 10) -> Dict[str, Any]:
    """
    Create summary statistics for categorical/text fields.
    
    Args:
        series: Pandas Series with categorical data
        top_n: Number of top values to include
        
    Returns:
        Dictionary with categorical summary statistics
    """
    total_count = len(series)
    missing_count = series.isna().sum()
    non_missing = series.dropna()
    
    summary = {
        'total_count': total_count,
        'missing_count': missing_count,
        'missing_pct': (missing_count / total_count) * 100 if total_count > 0 else 0,
        'unique_count': non_missing.nunique() if len(non_missing) > 0 else 0
    }
    
    if len(non_missing) > 0:
        try:
            value_counts = non_missing.value_counts().head(top_n)
            summary['top_values'] = value_counts.to_dict()
            summary['top_values_pct'] = (value_counts / len(non_missing) * 100).to_dict()
        except Exception as e:
            # If value_counts fails, just provide basic info
            summary['top_values'] = {}
            summary['top_values_pct'] = {}
    
    return summary


def group_cao_timeline(df: pd.DataFrame, cao_number_col: str = 'cao_number', 
                      date_col: str = 'ingangsdatum') -> Dict[str, pd.DataFrame]:
    """
    Group CAO data by CAO number and sort by date for longitudinal analysis.
    
    Args:
        df: DataFrame with CAO data
        cao_number_col: Column name for CAO number
        date_col: Column name for date
        
    Returns:
        Dictionary mapping CAO numbers to sorted DataFrames
    """
    cao_groups = {}
    
    for cao_num, group in df.groupby(cao_number_col):
        # Convert date column to datetime for sorting (CAO metadata dates are in DD/MM/YYYY format)
        group_copy = group.copy()
        dayfirst = date_col in ['ingangsdatum', 'expiratiedatum', 'datum_kennisgeving']
        group_copy[date_col] = parse_cao_date_series(group_copy[date_col], dayfirst=dayfirst)
        
        # Sort by date
        group_copy = group_copy.sort_values(date_col)
        cao_groups[str(cao_num)] = group_copy
    
    return cao_groups


def calculate_longitudinal_changes(cao_groups: Dict[str, pd.DataFrame], 
                                 numeric_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Calculate longitudinal changes for numeric variables across CAO timelines.
    
    Args:
        cao_groups: Dictionary of CAO groups from group_cao_timeline
        numeric_cols: List of numeric column names to analyze
        
    Returns:
        Dictionary with longitudinal change statistics
    """
    results = {}
    
    for cao_num, group in cao_groups.items():
        if len(group) < 2:  # Need at least 2 time periods
            continue
            
        cao_results = {}
        
        for col in numeric_cols:
            if col not in group.columns:
                continue
                
            # Check if this is a salary amount column that needs unit normalization
            if 'amount' in col.lower() and 'salary' in col.lower():
                # Get corresponding unit column
                unit_col = col.replace('amount', 'unit')
                if unit_col in group.columns:
                    # Normalize amounts to monthly equivalent before calculating changes
                    normalized_amounts = normalize_salary_amounts_with_units(
                        group[col], group[unit_col], group.get('ft_hours', None)
                    )
                    
                    # Remove NaN values
                    normalized_amounts = normalized_amounts.dropna()
                    
                    if len(normalized_amounts) < 2:
                        continue
                    
                    # Calculate changes between consecutive periods using normalized amounts
                    changes = normalized_amounts.diff().dropna()
                    
                    if len(changes) > 0:
                        # Round values to reasonable precision and handle NaN values
                        avg_change = changes.mean()
                        median_change = changes.median()
                        std_change = changes.std()
                        
                        cao_results[col] = {
                            'periods_count': len(normalized_amounts),
                            'changes_count': len(changes),
                            'avg_change': round(avg_change, 2) if pd.notna(avg_change) else 0,
                            'median_change': round(median_change, 2) if pd.notna(median_change) else 0,
                            'std_change': round(std_change, 2) if pd.notna(std_change) else 0,
                            'min_change': round(changes.min(), 2) if pd.notna(changes.min()) else 0,
                            'max_change': round(changes.max(), 2) if pd.notna(changes.max()) else 0,
                            'positive_changes': int((changes > 0).sum()),
                            'negative_changes': int((changes < 0).sum()),
                            'zero_changes': int((changes == 0).sum()),
                            'unit_normalized': True  # Flag to indicate unit normalization was applied
                        }
                else:
                    # No unit column found, use original amounts (but flag as not normalized)
                    try:
                        values = pd.to_numeric(group[col], errors='coerce').dropna()
                        if len(values) < 2:
                            continue
                        
                        changes = values.diff().dropna()
                        
                        if len(changes) > 0:
                            avg_change = changes.mean()
                            median_change = changes.median()
                            std_change = changes.std()
                            
                            cao_results[col] = {
                                'periods_count': len(values),
                                'changes_count': len(changes),
                                'avg_change': round(avg_change, 2) if pd.notna(avg_change) else 0,
                                'median_change': round(median_change, 2) if pd.notna(median_change) else 0,
                                'std_change': round(std_change, 2) if pd.notna(std_change) else 0,
                                'min_change': round(changes.min(), 2) if pd.notna(changes.min()) else 0,
                                'max_change': round(changes.max(), 2) if pd.notna(changes.max()) else 0,
                                'positive_changes': int((changes > 0).sum()),
                                'negative_changes': int((changes < 0).sum()),
                                'zero_changes': int((changes == 0).sum()),
                                'unit_normalized': False  # Flag to indicate no unit normalization
                            }
                    except Exception as e:
                        continue
            else:
                # For non-salary columns, use original logic
                try:
                    values = pd.to_numeric(group[col], errors='coerce').dropna()
                    if len(values) < 2:
                        continue
                    
                    changes = values.diff().dropna()
                    
                    if len(changes) > 0:
                        avg_change = changes.mean()
                        median_change = changes.median()
                        std_change = changes.std()
                        
                        cao_results[col] = {
                            'periods_count': len(values),
                            'changes_count': len(changes),
                            'avg_change': round(avg_change, 2) if pd.notna(avg_change) else 0,
                            'median_change': round(median_change, 2) if pd.notna(median_change) else 0,
                            'std_change': round(std_change, 2) if pd.notna(std_change) else 0,
                            'min_change': round(changes.min(), 2) if pd.notna(changes.min()) else 0,
                            'max_change': round(changes.max(), 2) if pd.notna(changes.max()) else 0,
                            'positive_changes': int((changes > 0).sum()),
                            'negative_changes': int((changes < 0).sum()),
                            'zero_changes': int((changes == 0).sum()),
                            'unit_normalized': False
                        }
                except Exception as e:
                    continue
        
        if cao_results:
            results[cao_num] = cao_results
    
    return results


def create_crosstab_summary(df: pd.DataFrame, bool_col: str, 
                           group_col: str) -> Dict[str, Any]:
    """
    Create cross-tabulation summary for boolean variable by grouping variable.
    
    Args:
        df: DataFrame with data
        bool_col: Boolean column name
        group_col: Grouping column name
        
    Returns:
        Dictionary with cross-tabulation results
    """
    if bool_col not in df.columns or group_col not in df.columns:
        return {}
    
    # Create cross-tabulation
    crosstab = pd.crosstab(df[group_col], df[bool_col], margins=True)
    
    # Calculate percentages
    crosstab_pct = pd.crosstab(df[group_col], df[bool_col], normalize='index') * 100
    
    return {
        'counts': crosstab.to_dict(),
        'percentages': crosstab_pct.to_dict(),
        'total_true': df[bool_col].sum(),
        'total_false': (df[bool_col] == False).sum(),
        'total_missing': df[bool_col].isna().sum()
    }


def parse_cao_date_series(date_series: pd.Series, dayfirst: bool = True) -> pd.Series:
    """
    Parse a series of date strings robustly for CAO metadata (ingangsdatum, etc.).
    Tries DD/MM/YYYY first, then ISO YYYY-MM-DD, then dateutil for remaining.
    
    Args:
        date_series: Series of date strings (or already datetime)
        dayfirst: Prefer DD/MM/YYYY when True (default for CAO metadata)
        
    Returns:
        Series of datetime64[ns], with NaT for unparseable/empty values
    """
    if date_series.empty:
        return pd.Series(dtype='datetime64[ns]')
    # Already datetime
    if pd.api.types.is_datetime64_any_dtype(date_series):
        return date_series
    out = pd.to_datetime(date_series, errors='coerce', dayfirst=dayfirst)
    # For values still NaT, try ISO format
    still_nat = out.isna() & date_series.notna()
    raw = date_series.astype(str).str.strip()
    still_nat = still_nat & (raw != '') & (raw.str.lower() != 'nan')
    if still_nat.any():
        try:
            iso_parsed = pd.to_datetime(date_series.loc[still_nat], format='%Y-%m-%d', errors='coerce')
            out = out.fillna(iso_parsed)
        except (ValueError, TypeError):
            pass
    # Last resort: dateutil for remaining non-empty strings
    still_nat = out.isna() & date_series.notna()
    raw = date_series.astype(str).str.strip()
    still_nat = still_nat & (raw != '') & (raw.str.lower() != 'nan')
    if still_nat.any():
        try:
            from dateutil import parser as dateutil_parser
            def parse_one(v):
                try:
                    return dateutil_parser.parse(str(v), dayfirst=dayfirst)
                except (ValueError, TypeError):
                    return pd.NaT
            filled = date_series.loc[still_nat].apply(parse_one)
            out = out.fillna(filled)
        except ImportError:
            pass
    return out


def extract_year_from_date(date_series: pd.Series, dayfirst: bool = True) -> pd.Series:
    """
    Extract year from date series for temporal analysis.
    
    Args:
        date_series: Series with date values (CAO metadata dates are in DD/MM/YYYY format)
        dayfirst: Whether to interpret dates as DD/MM/YYYY format (default True for CAO dates)
        
    Returns:
        Series with years
    """
    return parse_cao_date_series(date_series, dayfirst=dayfirst).dt.year


def analyze_amount_ranges(df: pd.DataFrame, min_col: str, max_col: str, 
                         unit_col: str) -> Dict[str, Any]:
    """
    Analyze AmountRange fields (min, max, unit).
    
    Args:
        df: DataFrame with data
        min_col: Column name for minimum values
        max_col: Column name for maximum values
        unit_col: Column name for units
        
    Returns:
        Dictionary with range analysis
    """
    if not all(col in df.columns for col in [min_col, max_col, unit_col]):
        return {}
    
    # Filter rows where both min and max are available
    valid_ranges = df.dropna(subset=[min_col, max_col])
    
    if len(valid_ranges) == 0:
        return {'count': 0}
    
    # Calculate range width
    valid_ranges = valid_ranges.copy()
    valid_ranges['range_width'] = valid_ranges[max_col] - valid_ranges[min_col]
    
    # Group by unit for analysis
    unit_analysis = {}
    for unit, unit_group in valid_ranges.groupby(unit_col):
        unit_analysis[unit] = {
            'count': len(unit_group),
            'min_stats': calculate_descriptive_stats(unit_group[min_col]),
            'max_stats': calculate_descriptive_stats(unit_group[max_col]),
            'range_width_stats': calculate_descriptive_stats(unit_group['range_width'])
        }
    
    return {
        'total_count': len(valid_ranges),
        'missing_count': len(df) - len(valid_ranges),
        'by_unit': unit_analysis
    }


def identify_salary_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Identify different types of salary-related columns in the dataset.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary categorizing columns by type
    """
    columns = df.columns.tolist()
    
    # Identify salary amount columns (salary_X_amount)
    salary_amount_cols = [col for col in columns if re.match(r'salary_\d+_amount', col)]
    
    # Identify unit columns (salary_X_unit)
    salary_unit_cols = [col for col in columns if re.match(r'salary_\d+_unit', col)]
    
    # Identify other salary-related columns
    other_salary_cols = [col for col in columns if col.startswith('salary_') and 
                        col not in salary_amount_cols and col not in salary_unit_cols]
    
    # Identify boolean columns
    bool_cols = [col for col in columns if df[col].dtype == 'bool' or 
                df[col].isin([True, False, np.nan]).all()]
    
    # Identify numeric columns (excluding salary amounts)
    numeric_cols = [col for col in columns if pd.api.types.is_numeric_dtype(df[col]) and 
                   col not in salary_amount_cols]
    
    # Identify categorical columns
    categorical_cols = [col for col in columns if col not in salary_amount_cols + 
                       salary_unit_cols + other_salary_cols + bool_cols + numeric_cols]
    
    return {
        'salary_amounts': salary_amount_cols,
        'salary_units': salary_unit_cols,
        'other_salary': other_salary_cols,
        'boolean': bool_cols,
        'numeric': numeric_cols,
        'categorical': categorical_cols
    }


def identify_non_salary_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Identify different types of non-salary columns in the dataset.
    
    Args:
        df: DataFrame with non-salary data
        
    Returns:
        Dictionary categorizing columns by type
    """
    columns = df.columns.tolist()
    
    # Identify amount columns (ending with _value)
    amount_cols = [col for col in columns if col.endswith('_value')]
    
    # Identify unit columns (ending with _unit)
    unit_cols = [col for col in columns if col.endswith('_unit')]
    
    # Identify range columns (ending with _min or _max)
    range_min_cols = [col for col in columns if col.endswith('_min')]
    range_max_cols = [col for col in columns if col.endswith('_max')]
    
    # Identify boolean columns
    bool_cols = []
    for col in columns:
        try:
            if df[col].dtype == 'bool':
                bool_cols.append(col)
            else:
                # Check if all values are boolean-like
                unique_vals = df[col].dropna().unique()
                if len(unique_vals) <= 3 and all(val in [True, False, np.nan, 'True', 'False', 'true', 'false', 1, 0] for val in unique_vals):
                    bool_cols.append(col)
        except Exception as e:
            # Skip columns that cause comparison errors
            print(f"    Warning: Skipping boolean check for column {col}: {e}")
            continue
    
    # Identify numeric columns (excluding amounts and ranges)
    numeric_cols = []
    for col in columns:
        try:
            if pd.api.types.is_numeric_dtype(df[col]) and col not in amount_cols + range_min_cols + range_max_cols:
                numeric_cols.append(col)
        except Exception:
            # Skip columns that cause errors
            continue
    
    # Identify categorical columns
    categorical_cols = [col for col in columns if col not in amount_cols + 
                       unit_cols + range_min_cols + range_max_cols + bool_cols + numeric_cols]
    
    return {
        'amounts': amount_cols,
        'units': unit_cols,
        'range_mins': range_min_cols,
        'range_maxs': range_max_cols,
        'boolean': bool_cols,
        'numeric': numeric_cols,
        'categorical': categorical_cols
    }


def detect_salary_slot_indices(columns: List[str]) -> List[int]:
    """
    Detect available salary slot indices from wide salary columns.

    Args:
        columns: Iterable of column names

    Returns:
        Sorted list of detected integer slot indices
    """
    slot_indices: Set[int] = set()
    for col in columns:
        match = re.match(
            r"salary_(\d+)_(?:amount|start_date|end_date|unit|increase_percent|hours_basis_ft_week)$",
            str(col),
        )
        if match:
            slot_indices.add(int(match.group(1)))
    return sorted(slot_indices)


def build_long_salary_from_wide(
    df: pd.DataFrame,
    identity_cols: List[str],
    salary_fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Build long salary rows from wide salary slot columns efficiently.

    Args:
        df: Wide salary DataFrame containing salary_k_* columns
        identity_cols: Base columns copied to each long row
        salary_fields: Slot fields to map from salary_k_<field> to salary_<field>

    Returns:
        Long-format salary DataFrame with one row per detected slot observation
    """
    if len(df) == 0:
        return pd.DataFrame()
    if salary_fields is None:
        salary_fields = [
            "start_date",
            "end_date",
            "amount",
            "unit",
            "table_label",
            "increase_percent",
            "holiday_in_amount",
            "hours_basis_ft_week",
            "note",
        ]

    slot_indices = detect_salary_slot_indices(df.columns.tolist())
    if not slot_indices:
        return pd.DataFrame()

    base_cols = [c for c in identity_cols if c in df.columns]
    slices: List[pd.DataFrame] = []
    for k in slot_indices:
        slot_cols = [f"salary_{k}_{field}" for field in salary_fields if f"salary_{k}_{field}" in df.columns]
        if not slot_cols:
            continue
        # Keep only required identity + slot columns; avoid full-frame copy.
        tmp = df.loc[:, base_cols + slot_cols].copy()
        rename_map = {f"salary_{k}_{field}": f"salary_{field}" for field in salary_fields if f"salary_{k}_{field}" in tmp.columns}
        tmp = tmp.rename(columns=rename_map)
        tmp["salary_index"] = k
        # Positional wide row id (0..n-1), aligned with ``iloc`` / ``derive_salary_increase_series`` row_id.
        tmp["row_id"] = np.arange(len(df), dtype=np.int64)
        slices.append(tmp)

    if not slices:
        return pd.DataFrame()
    out = pd.concat(slices, ignore_index=True)
    if "salary_start_date" in out.columns:
        out["salary_start_date"] = pd.to_datetime(out["salary_start_date"], errors="coerce")
        out["salary_start_year"] = out["salary_start_date"].dt.year
    if "salary_amount" in out.columns:
        _amt = out["salary_amount"].map(coerce_salary_amount_scalar)
        _ok = _amt.notna() & (_amt > 0)
        out = out.loc[_ok].reset_index(drop=True)
    return out


def build_latest_cao_forward_fill_by_file(
    df: pd.DataFrame,
    cao_col: str,
    year_col: str,
    file_col: str = "file_name",
    order_date_col: Optional[str] = "ingangsdatum",
    value_cols_to_keep: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Expand CAO-year view by active file/version and keep all rows in that file.

    Args:
        df: Input DataFrame with CAO, year, and file columns
        cao_col: CAO identifier column
        year_col: Integer-like year column used for trend axis
        file_col: File/version identifier column
        order_date_col: Optional date column used to break ties within same year
        value_cols_to_keep: If set, restrict merged rows to these value columns plus
            keys (cao, year, file, optional order date). Omit to keep all columns.

    Returns:
        DataFrame where each CAO-year is mapped to the latest known active file,
        carrying all rows belonging to that file.
    """
    required = [cao_col, year_col, file_col]
    if any(c not in df.columns for c in required):
        return pd.DataFrame()
    work = df.copy()
    if value_cols_to_keep is not None:
        base_keep = [cao_col, year_col, file_col]
        if order_date_col and order_date_col in work.columns:
            base_keep.append(order_date_col)
        extra = [c for c in value_cols_to_keep if c in work.columns and c not in base_keep]
        work = work[base_keep + extra].copy()
    work[year_col] = pd.to_numeric(work[year_col], errors="coerce")
    work = work[work[cao_col].notna() & work[year_col].notna() & work[file_col].notna()].copy()
    if len(work) == 0:
        return pd.DataFrame()
    work[year_col] = work[year_col].astype(int)

    version_cols = [cao_col, year_col, file_col]
    if order_date_col and order_date_col in work.columns:
        version_cols.append(order_date_col)
    versions = work[version_cols].drop_duplicates().copy()
    if order_date_col and order_date_col in versions.columns:
        versions = versions.sort_values([cao_col, year_col, order_date_col, file_col])
    else:
        versions = versions.sort_values([cao_col, year_col, file_col])
    versions = versions.groupby([cao_col, year_col], as_index=False).tail(1)

    # Build a global-horizon active panel:
    # each CAO starts at its own first observed year, then forward-fills through
    # the dataset-wide max year so late-year snapshots retain previously active CAOs.
    global_max_year = int(versions[year_col].max())
    years = versions.groupby(cao_col)[year_col].agg(["min"]).reset_index()
    years["year_range"] = years["min"].apply(
        lambda min_y: np.arange(int(min_y), global_max_year + 1)
    )
    expanded = years[[cao_col, "year_range"]].explode("year_range").rename(columns={"year_range": year_col})

    version_lookup = versions[[cao_col, year_col, file_col]].copy()
    expanded = expanded.merge(version_lookup, on=[cao_col, year_col], how="left")
    expanded = expanded.sort_values([cao_col, year_col])
    expanded[file_col] = expanded.groupby(cao_col)[file_col].ffill()
    expanded = expanded[expanded[file_col].notna()].copy()

    merged = expanded.merge(
        work,
        on=[cao_col, file_col],
        how="left",
        suffixes=("_expanded", ""),
    )
    if f"{year_col}_expanded" in merged.columns:
        merged[year_col] = merged[f"{year_col}_expanded"]
        merged = merged.drop(columns=[f"{year_col}_expanded"])
    return merged


def get_plot_color_cycle(n: int) -> List[Any]:
    """
    Return n visually distinct plot colors from tab20 cycle.

    Args:
        n: Number of colors required

    Returns:
        List of matplotlib color values
    """
    cmap = plt.get_cmap("tab20")
    base = [cmap(i) for i in range(cmap.N)]
    if n <= len(base):
        return base[:n]
    colors: List[Any] = []
    while len(colors) < n:
        colors.extend(base)
    return colors[:n]


def enforce_integer_year_axis(ax: Any, years: List[int]) -> None:
    """
    Force integer x-axis ticks for year-based plots.

    Args:
        ax: Matplotlib axis object
        years: Year values that should appear on x-axis
    """
    years_clean = sorted({int(y) for y in years if pd.notna(y)})
    if not years_clean:
        return
    ax.set_xticks(years_clean)
    ax.set_xticklabels([str(y) for y in years_clean], rotation=0)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))


def parse_updated_topics_cell(value: Any) -> Tuple[List[str], int]:
    """
    Parse general_updated_topics cell safely into token list.

    Args:
        value: Raw cell value from CSV

    Returns:
        Tuple of (parsed_topics_lowercase, parse_failed_flag)
    """
    if pd.isna(value):
        return [], 0
    text = str(value).strip()
    if text in {"", "[]", "nan", "None"}:
        return [], 0
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            tokens = [str(x).strip().lower() for x in parsed if str(x).strip()]
            return tokens, 0
        return [], 1
    except (ValueError, SyntaxError):
        return [], 1


def filter_non_salary_for_plot(
    df: pd.DataFrame,
    doc_col: str = "general_document_type",
    topics_col: str = "general_updated_topics",
) -> pd.DataFrame:
    """
    Filter non-salary rows to the analysis sample used in descriptives and plots.

    Rules:
    - Exclude protocol.
    - Keep only full_cao_original and full_cao_update (no topic-keyword gating).

    Optional columns `_parsed_topics` and `parse_failed` are still attached when
    `topics_col` exists, for diagnostics and topic listing sheets on the raw frame.

    Args:
        df: Input DataFrame
        doc_col: Document type column name
        topics_col: Updated topics column name (parsed for helpers only)

    Returns:
        Filtered DataFrame; may include _parsed_topics / parse_failed when topics_col present
    """
    if len(df) == 0:
        return df.copy()
    out = df.copy()
    if doc_col not in out.columns:
        out["_doc_type_norm"] = "other"
    else:
        out["_doc_type_norm"] = out[doc_col].fillna("other").astype(str).str.strip().str.lower()
    if topics_col in out.columns:
        parsed = out[topics_col].apply(parse_updated_topics_cell)
        out["_parsed_topics"] = parsed.apply(lambda x: x[0])
        out["parse_failed"] = parsed.apply(lambda x: x[1])
    else:
        out["_parsed_topics"] = [[] for _ in range(len(out))]
        out["parse_failed"] = 0

    full_mask = out["_doc_type_norm"].isin({"full_cao_original", "full_cao_update"})
    protocol_mask = out["_doc_type_norm"].eq("protocol")
    keep = (~protocol_mask) & full_mask
    return out[keep].copy()
