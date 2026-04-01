"""
Non-salary numeric unit normalization for trend plots.

Maps each *_value column to a canonical numeric scale using the paired *_unit string.
Rows with missing values, blank/placeholder units, or non-convertible units yield None
and are excluded from yearly means.

Summary:
- contract_full_time_hours_value, overtime_max_hours_per_week_value: hours per week
- leave_vacation_time_value, training_time_yearly_value: hours per year equivalent
- leave_sickpay_duration_value: weeks
- leave_sickpay_continuation_value: percent (0-100)
- pension_employee_contrib_value: percent (0-100)
- pension_retire_age_normal_value: years (age)

Parameters:
- value: raw numeric from CSV
- unit: unit string from CSV
- default_ft_hours: default full-time hours/week when row-level FT is unavailable
- row: optional pandas Series for contract_full_time_hours-based % conversion

Returns:
- float on canonical scale, or None if excluded
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

# Human-readable y-axis labels for each normalized variable (plot subpanels)
NUMERIC_VAR_YLABELS: Dict[str, str] = {
    "contract_full_time_hours_value": "Mean (hours/week)",
    "overtime_max_hours_per_week_value": "Mean (hours/week)",
    "leave_vacation_time_value": "Mean (hours/year)",
    "leave_sickpay_duration_value": "Mean (weeks)",
    "leave_sickpay_continuation_value": "Mean (%)",
    "pension_employee_contrib_value": "Mean (%)",
    "pension_retire_age_normal_value": "Mean (age, years)",
    "training_time_yearly_value": "Mean (hours/year)",
}

# Legend labels for multi-panel numeric trend figures
NUMERIC_VAR_LEGEND_LABELS: Dict[str, str] = {
    "contract_full_time_hours_value": "Full-time hours",
    "overtime_max_hours_per_week_value": "Overtime max (hours/wk)",
    "leave_vacation_time_value": "Vacation time (h/year)",
    "leave_sickpay_duration_value": "Sick pay duration (weeks)",
    "leave_sickpay_continuation_value": "Sick pay continuation (%)",
    "pension_employee_contrib_value": "Pension employee contrib. (%)",
    "pension_retire_age_normal_value": "Normal retirement age (years)",
    "training_time_yearly_value": "Training time (h/year)",
}


def _is_blank_unit(unit: Any) -> bool:
    """Return True if unit should be treated as missing."""
    if unit is None or (isinstance(unit, float) and pd.isna(unit)):
        return True
    s = str(unit).strip().lower()
    return s in {"", "nan", "none", "(blanks)", "0"}


def _hours_per_workday(default_ft_hours: float) -> float:
    """Assumed hours per workday (5-day week)."""
    return default_ft_hours / 5.0


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _continuation_is_tiered_prose(unit_lower: str) -> bool:
    """Heuristic: multi-period tier text — exclude from scalar continuation %."""
    if len(unit_lower) > 160:
        return True
    # Multiple distinct week/month bands described
    if unit_lower.count("percent") > 3 or unit_lower.count("%") > 4:
        return True
    if unit_lower.count(";") >= 2 and ("week" in unit_lower or "month" in unit_lower):
        return True
    return False


def normalize_contract_or_overtime_hours(
    val: float,
    unit_lower: str,
    default_ft_hours: float,
) -> Optional[float]:
    """
    Normalize contract full-time hours or overtime cap to hours per week.

    Parameters:
        val: Raw numeric from extraction.
        unit_lower: Lowercased unit string.
        default_ft_hours: Full-time hours per week for FTE and % conversions.

    Returns:
        Hours per week, or None if the unit is excluded or ambiguous.
    """
    # FTE
    if "fte" in unit_lower:
        if 0 < val <= 1.25:
            return val * default_ft_hours
        return None

    # Exclude ambiguous payroll period totals from weekly-hour trends.
    if "hours per pay period" in unit_lower or "hours per payroll period" in unit_lower:
        return None

    # Hours per week (wide match)
    if any(
        x in unit_lower
        for x in (
            "hours per week",
            "hours/week",
            "hours_per_week",
            "hours_per_week_average",
            "hours_overtime",
            "hours overtime",
            "hours per workweek",
            "hours per work week",
            "hours per working week",
            "hours per wage period",
            "hours per 24-hour period",
            "hours per day",
            "hours per week (average",
            "hours per week on",
            "hours per week or average",
            "wage hours",
            "average weekly hours",
            "hours per week average",
            "hours per week,",
            "hours per week;",
        )
    ):
        return val

    # Overtime caps stated as weekly averages over N weeks — value is already hours/week
    if "hours per week" in unit_lower or "hours/week" in unit_lower:
        return val
    if "hours average" in unit_lower and "week" in unit_lower:
        return val
    if "hours averaged" in unit_lower and "week" in unit_lower:
        return val

    if unit_lower in ("hours", "paid hours") and 20 <= val <= 60:
        return val

    # Annual / calendar hours -> weekly
    if any(
        x in unit_lower
        for x in (
            "hours per year",
            "hours/year",
            "hours_per_year",
            "hours annually",
            "hours per calendar year",
            "clock hours annually",
            "clock hours per year",
            "hours per holiday year",
            "hours per vacation year",
        )
    ):
        return val / 52.0

    if any(
        x in unit_lower
        for x in (
            "hours per month",
            "hours/month",
            "hours per full working month",
            "hours per fully worked month",
            "hours per full month",
        )
    ):
        return val * 12.0 / 52.0

    # 4-week references
    if any(
        x in unit_lower
        for x in (
            "hours per 4 weeks",
            "hours per 4-week",
            "hours per four weeks",
            "hours per 4-week period",
            "hours per period of four weeks",
            "hours per four-week period",
        )
    ):
        return val / 4.0

    # 13 / 16 / 26 week averaging
    if "13" in unit_lower and ("week" in unit_lower or "average" in unit_lower):
        return val / 13.0
    if "16" in unit_lower and "week" in unit_lower and "average" in unit_lower:
        return val / 16.0
    if "26" in unit_lower and "week" in unit_lower and ("average" in unit_lower or "over" in unit_lower):
        return val / 26.0

    # Percent of contractual/agreed/annual working time -> weekly hours
    if "%" in unit_lower or "percent" in unit_lower:
        if any(
            x in unit_lower
            for x in (
                "contractual",
                "agreed working",
                "agreed annual",
                "annual working",
                "working hours",
                "paid hour",
                "stated working",
            )
        ):
            return (val / 100.0) * default_ft_hours
        # Bare "percent" or "%" for nominal hours — exclude
        if unit_lower.strip() in ("%", "percent", "percentage", "% "):
            return None
        return None

    # Totals over N weeks → average weekly hours
    if "hours over 13 weeks" in unit_lower or "hours over 13 week" in unit_lower:
        return val / 13.0
    if "hours over 16 weeks" in unit_lower:
        return val / 16.0
    if "hours over 26 weeks" in unit_lower:
        return val / 26.0
    if "hours over 4 weeks" in unit_lower or "hours over a 4-week" in unit_lower:
        return val / 4.0
    if "hours in four weeks" in unit_lower:
        return val / 4.0
    if "hours of overtime" in unit_lower and val < 200:
        return val / 52.0

    if "per week" in unit_lower or "weekly" in unit_lower:
        return val

    return None


def _vacation_or_training_to_hours_yearly(
    val: float,
    unit_lower: str,
    default_ft_hours: float,
    *,
    bare_days_as_yearly: bool,
    max_annual_hours: float,
) -> Optional[float]:
    def _annualize_weekly_hours(v: float) -> Optional[float]:
        if 0 <= v <= 80:
            return v * 52.0
        return None

    def _valid_annual_hours(v: float) -> Optional[float]:
        # Guard against non-scalar extraction artifacts while keeping realistic ranges.
        if 0 <= v <= max_annual_hours:
            return v
        return None

    # Drop high-risk, non-scalar, or mixed-formula unit texts.
    if any(
        x in unit_lower
        for x in (
            "season",
            "or shifts",
            "days/shifts",
            "per worked hour",
            "per hour worked",
            "hours per hour worked",
            "hours per worked hour",
            "hours per hour",
            "hours per stage hour",
            "hour per course and exam hour",
            "module",
            "sustainable employability",
            "medical examination",
            "full study leave",
            "up to",
            "ranging from",
            "plus",
            "minimum",
        )
    ):
        return None

    if any(
        x in unit_lower
        for x in (
            "hours per academic year",
            "hours per school year",
            "hours/school year",
            "hours per study year",
            "hours per practical learning year",
            "hours per practical training year",
            "hours per course year",
            "clock hours per school year",
            "attendance hours per year",
            "pdi-hours annually",
            "hours yearly",
            "hours/year",
            "hours annually",
        )
    ):
        return _valid_annual_hours(val)

    """Shared: vacation leave and training yearly amounts -> hours/year equivalent."""
    hpd = _hours_per_workday(default_ft_hours)

    # Reuse contract-style hour parsing (weekly) and convert to annual.
    r = normalize_contract_or_overtime_hours(val, unit_lower, default_ft_hours)
    if r is not None:
        return _valid_annual_hours(r * 52.0)

    # Days (vacation / training)
    if "day" in unit_lower or "days" in unit_lower or "shift" in unit_lower:
        if any(
            x in unit_lower
            for x in (
                "per year",
                "annually",
                "calendar year",
                "vacation year",
                "holiday year",
                "per calendar",
                "days/year",
                "days annually",
                "working days per",
                "workdays per",
                "statutory",
                "training day",
                "school day",
                "paid day",
                "half-day",
                "development day",
            )
        ):
            return _valid_annual_hours(val * hpd)
        if "per week" in unit_lower or "day per week" in unit_lower:
            return _annualize_weekly_hours(val * hpd)
        if unit_lower.strip() in ("days", "day", "working days", "workdays"):
            if bare_days_as_yearly:
                return _valid_annual_hours(val * hpd)
            if 15 <= val <= 35:
                return _valid_annual_hours(val * hpd)
            return _annualize_weekly_hours(val * hpd)

    if "workdays" in unit_lower or "working days" in unit_lower:
        if "year" in unit_lower:
            return _valid_annual_hours(val * hpd)
        return _annualize_weekly_hours(val * hpd)

    # Weeks as calendar time (not hours)
    if unit_lower in ("weeks", "week") and 0.1 <= val <= 55:
        return _valid_annual_hours(val * default_ft_hours)

    # Multipliers
    if "times" in unit_lower or unit_lower.startswith("x "):
        if 0 <= val <= 12:
            return _valid_annual_hours(val * default_ft_hours)
        return None

    # Percent of working hours / paid hours -> weekly equiv
    if "%" in unit_lower or "percent" in unit_lower:
        if any(
            x in unit_lower
            for x in (
                "working hour",
                "paid hour",
                "contractual",
                "agreed",
                "annual working",
                "study",
                "effective working",
            )
        ):
            return _annualize_weekly_hours((val / 100.0) * default_ft_hours)
        if "vacation" in unit_lower and "hour" in unit_lower:
            return _annualize_weekly_hours((val / 100.0) * default_ft_hours)
        return None

    # Hours ambiguous
    if unit_lower == "hours":
        if val > 200:
            return _valid_annual_hours(val)
        if 20 <= val <= 60:
            return _annualize_weekly_hours(val)
        return None

    # One-off / non-comparable text — exclude
    if any(x in unit_lower for x in ("exam only", "course and exam")):
        return None

    if "per week" in unit_lower or "weekly" in unit_lower:
        return _annualize_weekly_hours(val)

    return None


def normalize_leave_vacation(
    val: float,
    unit_lower: str,
    default_ft_hours: float,
) -> Optional[float]:
    """
    Normalize vacation time to hours per year equivalent.

    Parameters:
        val: Raw numeric.
        unit_lower: Lowercased unit string.
        default_ft_hours: FT hours/week (from row or default).

    Returns:
        Hours per year equivalent, or None.
    """
    return _vacation_or_training_to_hours_yearly(
        val,
        unit_lower,
        default_ft_hours,
        bare_days_as_yearly=True,
        max_annual_hours=600.0,
    )


def normalize_training_time(
    val: float,
    unit_lower: str,
    default_ft_hours: float,
) -> Optional[float]:
    """
    Normalize yearly training entitlement to hours per year equivalent.

    Parameters:
        val: Raw numeric.
        unit_lower: Lowercased unit string.
        default_ft_hours: FT hours/week (from row or default).

    Returns:
        Hours per year equivalent, or None.
    """
    return _vacation_or_training_to_hours_yearly(
        val,
        unit_lower,
        default_ft_hours,
        bare_days_as_yearly=True,
        max_annual_hours=1000.0,
    )


def normalize_sickpay_duration(
    val: float,
    unit_lower: str,
) -> Optional[float]:
    """
    Normalize sick-pay duration to weeks.

    Parameters:
        val: Raw numeric.
        unit_lower: Lowercased unit string.

    Returns:
        Duration in weeks, or None.
    """
    # Prioritize explicit week semantics before generic month-token checks.
    if "week" in unit_lower:
        if 0 <= val <= 260:
            return val
        return None
    if "month" in unit_lower:
        if 0 <= val <= 120:
            return val * 4.348  # weeks per month (avg)
        return None
    if unit_lower in ("year", "years") or ("year" in unit_lower and "illness" in unit_lower):
        if 0 <= val <= 10:
            return val * 52.0
        return None
    return None


def normalize_sickpay_continuation(
    val: float,
    unit_lower: str,
    assume_if_unknown: bool,
) -> Optional[float]:
    """
    Normalize sick-pay continuation to a wage-replacement percentage (0-100).

    Parameters:
        val: Raw numeric (interpreted as percent when unit matches).
        unit_lower: Lowercased unit string.
        assume_if_unknown: If True, treat plausible values as percent when unit is sparse.

    Returns:
        Percent 0-100, or None (e.g. tiered prose excluded).
    """
    if _continuation_is_tiered_prose(unit_lower):
        return None

    if "%" in unit_lower or "percent" in unit_lower or "percentage" in unit_lower:
        if 0 <= val <= 100:
            return val
        if 0 < val <= 1:
            return val * 100.0
        return None

    if "weeks at" in unit_lower and ("salary" in unit_lower or "pay" in unit_lower or "%" in unit_lower):
        # Mis-tagged duration as continuation — exclude from % series
        return None

    if assume_if_unknown:
        if 0 <= val <= 100:
            return val
        if 0 < val <= 1:
            return val * 100.0
        return None
    return None


def normalize_pension_contrib(
    val: float,
    unit_lower: str,
    assume_if_unknown: bool,
) -> Optional[float]:
    """
    Normalize employee pension contribution to percent of premium/basis (0-100).

    Parameters:
        val: Raw numeric.
        unit_lower: Lowercased unit string.
        assume_if_unknown: If True, coerce plausible values when unit is sparse.

    Returns:
        Percent 0-100, or None for EUR or unknown.
    """
    if "eur" in unit_lower or "euro" in unit_lower:
        return None
    # Opaque formula-like text cannot be mapped to a stable scalar percentage.
    if any(
        x in unit_lower
        for x in (
            "derived from",
            "translated",
            "formula",
            "half of the",
            "half of that",
            "associated premium percentage",
        )
    ):
        return None

    if any(
        x in unit_lower
        for x in (
            "fraction of premium",
            "fraction of total premium",
            "share of premium",
            "share of total premium",
            "third of premium",
            "third of total premium",
            "quarter of total premium",
            "part of total premium",
        )
    ):
        if val <= 1.0:
            return val * 100.0
        if val <= 100:
            return val
        return None

    if "%" in unit_lower or "percent" in unit_lower or "percentage" in unit_lower:
        # For explicit percent units, interpret values as percentages directly.
        if val >= 0 and val <= 100:
            return val
        return None
    if "fraction" in unit_lower:
        if val <= 1.0 and val >= 0:
            return val * 100.0
        if val <= 100:
            return val
        return None
    if "third" in unit_lower or "quarter" in unit_lower or "share" in unit_lower:
        if val <= 1.0 and val >= 0:
            return val * 100.0
        if val <= 100:
            return val
        return None

    if assume_if_unknown:
        if 0 <= val <= 100:
            return val
        if 0 < val <= 1:
            return val * 100.0
        return None
    return None


def normalize_retire_age(
    val: float,
    unit_lower: str,
) -> Optional[float]:
    """
    Normalize normal retirement age to years (numeric age).

    Parameters:
        val: Raw numeric age.
        unit_lower: Lowercased unit string.

    Returns:
        Age in years in a plausible band, or None.
    """
    if any(
        x in unit_lower
        for x in (
            "aow",
            "years",
            "year",
            "age",
            "pensionable",
            "statutory",
            "gerechtigde",
            "entitlement",
            "commences",
            "benefit age",
        )
    ):
        if 60 <= val <= 75:
            return val
        return None
    # Numeric age with minimal or placeholder unit
    if unit_lower in ("0", "", "age"):
        if 60 <= val <= 75:
            return val
    return None


def normalize_for_plot(
    var_name: str,
    value: Any,
    unit: Any,
    *,
    default_ft_hours: float = 38.0,
    row: Optional[pd.Series] = None,
) -> Optional[float]:
    """
    Dispatch normalization by variable name.

    Args:
        var_name: Column name ending in _value
        value: Cell value
        unit: Unit string from paired *_unit column
        default_ft_hours: Fallback FT hours/week
        row: Full row for resolving contract FT hours for % conversions

    Returns:
        Canonical float, or None to exclude from aggregation
    """
    val = _coerce_float(value)
    if val is None:
        return None

    row = row if row is not None else pd.Series(dtype=object)

    # Pension age: allow numeric age when unit is blank or placeholder "0"
    if var_name == "pension_retire_age_normal_value":
        if _is_blank_unit(unit) or str(unit).strip() == "0":
            if 60 <= val <= 75:
                return val
            return None

    if _is_blank_unit(unit):
        return None

    unit_lower = str(unit).lower().strip()

    # Row-level full-time hours for % bases
    ft = default_ft_hours
    if "contract_full_time_hours_value" in row.index:
        raw_ft = row.get("contract_full_time_hours_value")
        ft_v = _coerce_float(raw_ft)
        if ft_v is not None:
            ft_unit = row.get("contract_full_time_hours_unit")
            if not _is_blank_unit(ft_unit):
                norm_ft = normalize_contract_or_overtime_hours(ft_v, str(ft_unit).lower().strip(), default_ft_hours)
                if norm_ft is not None:
                    ft = norm_ft
            elif 20 <= ft_v <= 60:
                ft = ft_v

    if var_name in ("contract_full_time_hours_value", "overtime_max_hours_per_week_value"):
        return normalize_contract_or_overtime_hours(val, unit_lower, ft)

    if var_name == "leave_vacation_time_value":
        return normalize_leave_vacation(val, unit_lower, ft)

    if var_name == "training_time_yearly_value":
        return normalize_training_time(val, unit_lower, ft)

    if var_name == "leave_sickpay_duration_value":
        return normalize_sickpay_duration(val, unit_lower)

    if var_name == "leave_sickpay_continuation_value":
        return normalize_sickpay_continuation(val, unit_lower, assume_if_unknown=True)

    if var_name == "pension_employee_contrib_value":
        return normalize_pension_contrib(val, unit_lower, assume_if_unknown=True)

    if var_name == "pension_retire_age_normal_value":
        return normalize_retire_age(val, unit_lower)

    return None


def normalize_hours_to_weekly(
    value: float,
    unit: str,
    default_ft_hours: float = 38.0,
    assume_percentage_if_unknown: bool = False,
    var_name: str = "",
) -> Optional[float]:
    """
    Backwards-compatible wrapper for older call sites.

    Deprecated: prefer normalize_for_plot with explicit var_name.
    """
    vn = var_name if var_name else "contract_full_time_hours_value"
    return normalize_for_plot(vn, value, unit, default_ft_hours=default_ft_hours, row=None)
