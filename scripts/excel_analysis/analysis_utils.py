"""
Excel Analysis Utilities

This module provides utility functions for analyzing Excel output data from CAO extraction,
including unit conversions, statistical summaries, and data processing helpers.

USAGE:
    from scripts.excel_analysis.analysis_utils import (
        convert_salary_to_monthly, calculate_descriptive_stats, 
        create_boolean_summary, group_cao_timeline
    )
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import re
from datetime import datetime


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


def convert_salary_to_monthly(amount: float, unit: str, ft_hours: Optional[float] = None) -> Optional[float]:
    """
    Convert salary amounts to monthly equivalent using monthly as standard.
    
    Args:
        amount: The salary amount
        unit: The unit (e.g., 'monthly', 'hourly', '4-week', 'weekly', 'annual')
        ft_hours: Full-time hours per week (if available)
        
    Returns:
        Monthly equivalent amount or None if conversion not possible
    """
    if pd.isna(amount) or not unit:
        return None
    
    unit_lower = unit.lower().strip()
    
    # Monthly is already standard
    if 'month' in unit_lower:
        return amount
    
    # Hourly to monthly (including 'uur' which is Dutch for hour)
    elif 'hour' in unit_lower or unit_lower == 'uur':
        hours_per_week = ft_hours if ft_hours and not pd.isna(ft_hours) else 40
        weeks_per_month = 4.33  # Average weeks per month
        return amount * hours_per_week * weeks_per_month
    
    # 4-weekly to monthly (including 'Pure 4-weekly')
    elif '4-week' in unit_lower or 'four' in unit_lower or 'pure' in unit_lower:
        return amount * (12 / 13)  # 12 months / 13 four-week periods
    
    # Weekly to monthly
    elif 'week' in unit_lower:
        return amount * 4.33  # Average weeks per month
    
    # Annual to monthly
    elif 'annual' in unit_lower or 'year' in unit_lower:
        return amount / 12
    
    # If unit not recognized, return None
    return None


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
