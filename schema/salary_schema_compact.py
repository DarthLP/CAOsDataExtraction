"""
Compact Salary Schema Definitions for CAO Data Extraction

This module contains compact Pydantic schema definitions used for salary data extraction
when standard schema attempts fail due to token limits. The compact schema removes
table_label and uses abbreviated unit field to reduce output size.

USAGE:
    from schema.salary_schema_compact import SalaryPointCompact, SalaryRowCompact, SalaryExtractionSchemaCompact
"""

from typing import List, Optional
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------
# AMOUNT CLASSES
# ---------------------------------------------------------------------

class Amount(BaseModel):
    """Value-unit pair for amounts, durations, percentages, etc."""
    value: Optional[float] = None
    unit: Optional[str] = None


class AmountRange(BaseModel):
    """Compact min/max range with shared unit."""
    min: Optional[float] = None
    max: Optional[float] = None
    unit: Optional[str] = None


# ---------------------------------------------------------------------
# SALARY POINT COMPACT: one effective value in time (no table_label)
# ---------------------------------------------------------------------
class SalaryPointCompact(BaseModel):
    """
    One effective salary value valid for a specific period.

    Each SalaryPointCompact represents a single salary entry from a wage table or 
    general increase clause — defined by its start date and (if stated) end date. 
    It records the raw printed amount, pay unit, and optional context such as 
    general increase or inclusion of holiday allowance.
    """

    start_date: str = Field(
        ...,
        description="Date the salary amount becomes effective (YYYY-MM-DD format)."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="Date the salary amount ceases to apply, if explicitly stated (YYYY-MM-DD format). "
                    "Omit if not given or still in force at contract expiry."
    )

    amount: float = Field(
        ...,
        description="Gross salary amount as printed in the CAO (no conversions or derivations)."
    )

    unit: str = Field(
        ...,
        description="Output ONLY the abbreviation for the pay period unit as stated or inferred literally from the CAO. "
                    "Use: 'm' for monthly, '4-w' for 4-weekly, 'w' for weekly, 'h' for hourly, 'a' for annual."
    )

    inc_pct: Optional[float] = Field(
        default=None,
        description="If the CAO specifies a general percentage increase for this table or time period (e.g., 3.00 for a +3% wage rise), record it here; otherwise omit."
    )

    holiday_incl: Optional[bool] = Field(
        default=None,
        description="True if the printed amount explicitly includes holiday allowance; "
                    "Omit if not stated or if false."
    )

    note: Optional[str] = Field(
        default=None,
        description="Footnotes, exceptions, or remarks directly tied to this wage entry. "
                    "Keep concise but faithful to the original text, very precisely. If full-time hours per week deviate from the general CAO baseline, mention it here. Omit if not present."
    )


# ---------------------------------------------------------------------
# SALARY ROW COMPACT: one job group × step × optional age/education
# ---------------------------------------------------------------------
class SalaryRowCompact(BaseModel):
    """
    One complete wage-scale cell representing a combination of:
    (worker type) × (job group) × [optional step/trede] × [optional age band] × [optional education level] × [optional contract type] × timeline.

    The 'timeline' field contains the series of salary values (`SalaryPointCompact`) 
    over time, as published in successive CAO wage tables.
    """

    # ---- Identification / scoping ----
    jobgroup: str = Field(
        ...,
        description="Job group or salary scale label/code. If a descriptive subtitle is given, append it in parentheses (e.g., 'F-45-9 (workers with high school diploma)')."
    )

    step: Optional[str] = Field(
        default=None,
        description="Printed label of the step/trede (e.g., 'trede 0', 'periodiek 3', 'aanloopschaal C'); omit if not printed."
    )

    worker: Optional[str] = Field(
        default=None,
        description="Worker type or category. Omit if generic (e.g., 'employee', 'standard worker') AND only one worker type exists. Keep when meaningful (e.g., 'Construction worker', 'UTA employee')."
    )

    is_entry: Optional[bool] = Field(
        default=None,
        description="True if explicitly described as an entry/aanloop scale; "
                    "False if explicitly standard; Omit if not stated."
    )

    # ---- Filters (only when printed) ----
    age_group: Optional[str] = Field(
        default=None,
        description="Printed age band of the wage table (e.g., '23 jaar', '21+', 'adult'). Consider only age bands capturing at least some workers aged between 23-65. Omit if not printed."
    )

    education: Optional[str] = Field(
        default=None,
        description="Printed education level qualifier (e.g., 'MBO-2', 'HBO-bachelor'), if stated; omit if not printed."
    )

    ft_hours: Optional[float] = Field(
        default=None,
        description="Full-time weekly hours baseline for this CAO (e.g., 37). Record only if explicitly stated, not derived. Omit if not printed."
    )

    # ---- Contract type information ----
    permanency: Optional[str] = Field(
        default=None,
        description="Contract permanency type as explicitly stated (e.g., 'permanent', 'temporary', 'fixed-term'). "
                    "Omit if not printed or not applicable to this salary row."
    )

    hours_type: Optional[str] = Field(
        default=None,
        description="Work arrangement type as explicitly stated (e.g., 'full-time', 'part-time'). "
                    "Omit if not printed or not applicable to this salary row."
    )

    # ---- Salary timeline ----
    timeline: List[SalaryPointCompact] = Field(
        default_factory=list,
        description="Chronological list of salary points (each from a wage table or increase clause)."
    )

    # ---- Meta / context ----
    row_note: Optional[str] = Field(
        default=None,
        description="Row-level remarks that apply across all timeline points "
                    "(e.g., 'All amounts exclude 8% holiday allowance unless noted', "
                    "'Scale F merges into G from 2026-01-01'). Keep concise but faithful to the original text, very precisely. Omit if not present."
    )


class SalaryExtractionSchemaCompact(BaseModel):
    """Top-level container for all extracted wage-related information from one CAO (compact version)."""
    salary_information: List[SalaryRowCompact] = Field(
        default_factory=list,
        description=(
            "List of all wage-scale rows extracted from the CAO, "
            "each describing one (job group) × (step) × [optional worker type] × "
            "[optional age band] × [optional education level] × [optional contract type] × timeline."
        )
    )

