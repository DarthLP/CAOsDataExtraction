"""
Salary Schema Definitions for CAO Data Extraction

This module contains the Pydantic schema definitions and prompt template
used for salary data extraction from CAO documents.

USAGE:
    from schema.salary_schema import Amount, AmountRange, SalaryPoint, SalaryRow, SalaryExtractionSchema, SALARY_PROMPT
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
# SALARY POINT: one effective value in time
# ---------------------------------------------------------------------
class SalaryPoint(BaseModel):
    """
    One effective salary value valid for a specific period.

    Each SalaryPoint represents a single salary entry from a wage table or 
    general increase clause — defined by its start date and (if stated) end date. 
    It records the raw printed amount, pay unit, and optional context such as 
    general increase, working hours basis, or inclusion of holiday allowance.
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
        description="Pay period unit as stated (e.g., 'monthly', '4-week', 'weekly', 'hourly', 'annual')."
    )

    table_label: str = Field(
        default="",
        description="Concise identifier of the wage table or adjustment version (e.g., 'per 1 Nov 2023', 'Table A - 2024 rates')."
    )

    increase_percent: Optional[float] = Field(
        default=None,
        description="If the CAO specifies a general percentage increase for this table or time period (e.g., 3.00 for a +3% wage rise), record it here; otherwise omit."
    )

    holiday_in_amount: Optional[bool] = Field(
        default=None,
        description="True if the printed amount explicitly includes holiday allowance; "
                    "Omit if not stated or if false."
    )

    hours_basis_ft_week: Optional[float] = Field(
        default=None,
        description="Full-time weekly hours underlying this amount (e.g., 36, 37, 38, 40), "
                    "recorded only if explicitly for the table version; omit if not mentioned."
    )

    note: Optional[str] = Field(
        default=None,
        description="Footnotes, exceptions, or remarks directly tied to this wage entry. "
                    "Keep concise but faithful to the original text. Omit if not present."
    )


# ---------------------------------------------------------------------
# SALARY ROW: one job group × step × optional age/education
# ---------------------------------------------------------------------
class SalaryRow(BaseModel):
    """
    One complete wage-scale cell representing a combination of:
    (worker type) × (job group) × [optional step/trede] × [optional age band] × [optional education level] × [optional contract type] × timeline.

    The 'timeline' field contains the series of salary values (`SalaryPoint`) 
    over time, as published in successive CAO wage tables.
    """

    # ---- Identification / scoping ----
    jobgroup: str = Field(
        ...,
        description="Job group or salary scale label/code. If a descriptive subtitle is given, append it in parentheses (e.g., 'F-45-9 (workers with high school diploma)')."
    )

    step_label: Optional[str] = Field(
        default=None,
        description="Printed label of the step/trede (e.g., 'trede 0', 'periodiek 3', 'aanloopschaal C'); omit if not printed."
    )

    worker_type: Optional[str] = Field(
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
        description="Printed age band of the wage table (e.g., '23 jaar', '21+'). Consider only age bands capturing at least some workers aged between 23-65. Omit if not printed."
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
    timeline: List[SalaryPoint] = Field(
        default_factory=list,
        description="Chronological list of salary points (each from a wage table or increase clause)."
    )

    # ---- Meta / context ----
    row_note: Optional[str] = Field(
        default=None,
        description="Row-level remarks that apply across all timeline points "
                    "(e.g., 'All amounts exclude 8% holiday allowance unless noted', "
                    "'Scale F merges into G from 2026-01-01'). Keep concise but faithful to the original text. Omit if not present."
    )


class SalaryExtractionSchema(BaseModel):
    """Top-level container for all extracted wage-related information from one CAO."""
    salary_information: List[SalaryRow] = Field(
        default_factory=list,
        description=(
            "List of all wage-scale rows extracted from the CAO, "
            "each describing one (job group) × (step) × [optional worker type] × "
            "[optional age band] × [optional education level] × [optional contract type] × timeline."
        )
    )


# ---------------------------------------------------------------------
# SALARY EXTRACTION PROMPT
# ---------------------------------------------------------------------

SALARY_PROMPT = """Extract structured salary data from a JSON object derived from the Dutch CAO document.

    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no hallucination, no guessing, no markdown fences, no extra text.

    INPUTS
    Filename: {filename}
    Source text: {source_json}

    CRITICAL RULES
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
        - Missing values: Omit optional fields entirely. Only include optional fields with actual values.
        - Output ONLY valid JSON format matching the provided schema structure.

    TABLE SELECTION
        - Include ONLY standard/regular wage tables. 
        - EXCLUDE allowances, bonuses, overtime, irregular hours, reimbursements, and non-standard worker roles like apprentices, interns, trainees, or foremen.
        - If multiple tables exist for different worker types, time periods, education levels, job groups, steps, or age bands under this standard wage type, include all of them. 
        - Record the unit exactly as printed. If the same baseline is printed in multiple units for the SAME workers/period/step/education/age, choose ONE using this order: monthly > hourly > 4-week > weekly > annual.
        - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present). Keep tables that differ by time period, worker type, education level, job group/function scale, steps (periodieken/trede), age bands, or contract type.

    TABLE AGE GROUP SELECTION
        - Create distinct SalaryRow objects for each adult-eligible age band present:
            - Open-ended adult bands (e.g., "22+", "21 and older"), OR
            - Bands that intersect ages 23-65.
        - IGNORE age and job groups limited to workers under 23 (e.g., "16-20", "20") unless the group is open-ended ("20+") or spans older ages ("18-65").

    TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE
        - Extract ALL job groups visible in the standard wage table.
        - If steps/trede (periodieken) are shown, create a separate SalaryRow per jobgroup × step × [worker type] (× [age] × [education] × [contract type]).
        - If education tiers (e.g., MBO/HBO) determine different wages, create separate rows per jobgroup × step × [worker type] × education (× [age] × [contract type]).
        - If contract permanency or contract hours (work arrangement) determine different wages, create separate rows per jobgroup × step × [worker type] × contract type (× [age] × [education]).
        - Worker type field: OMIT if the value is generic (e.g. 'employee', 'standard worker') AND there is only one worker type in the entire CAO. KEEP when it provides meaningful distinction between different worker categories.

    TABLE AMOUNTS, PERCENTAGES, DATES
        - Salary amount: output as a number using a dot as the decimal separator (e.g., 2300.00). Do NOT use quotes, commas or thousands separators.
        - increase_percent: include only if the table or a relating clause explicitly states a general % for that version.
        - Dates: Use YYYY-MM-DD format (e.g., "2023-11-01"). Do NOT invent or infer dates.
    
    TABLE TIMELINE CONSTRUCTION
        - For each (jobgroup × step × [worker type] × [age] × [education] × [contract type]), build `timeline` with a SalaryPoint per table version that prints salary amounts.
        - Each SalaryPoint MUST have a printed amount. If only a % increase is announced but no new amounts are printed, DO NOT add a timeline point; instead mention the % in a note.
        - Use start_date exactly as the table heading or clause states, converting to YYYY-MM-DD format (e.g., "per 1 Nov 2023" → "2023-11-01"). If day is not printed, use the first day of the month, same for month.
        - Align timeline points for the SAME (jobgroup × step × [worker type] × [age] × [education] × [contract type]) across time periods / table versions. Do not impute missing values.

    WORKFLOW STEPS (INTERNAL - DO NOT OUTPUT)
        1) READ & ANCHOR
            - Review all general rules and field descriptions in the Pydantic output schema.
            - Read the input text to understand its structure, content, and table layout.
        2) LOCATE & MARK all standard wage tables according to the TABLE SELECTION rules.
        3) DETECT AGE GROUPS: Within each selected table, extract all age groups meeting the TABLE AGE GROUP SELECTION criteria.
        4) DETECT JOB GROUPS, STEPS, EDUCATION LEVELS & CONTRACT TYPES: For each table version, identify job groups, steps, worker types, education levels, and contract types following the TABLE JOB GROUP, STEP, EDUCATION, CONTRACT TYPE rules.
        5) CONSTRUCT TIMELINE STRUCTURE:
            5.1) Apply TABLE TIMELINE CONSTRUCTION rules to align job groups, steps, worker types, education levels, age bands, and contract types across table versions.
            5.2) Build one SalaryRow for every unique detected combination of (jobgroup × step × [worker type] × [age] × [education] × [contract type]).
            5.3) Build timeline: For each SalaryRow, create one SalaryPoint per version/time period where that combination appears, then normalize labels (jobgroup/step/worker type/age/education/contract type), deduplicate identical periods, and align all points that refer to the same combination across versions (no imputation).
        6) SORT & CLEAN each row's timeline chronologically by start_date. Omit or nullify any fields not explicitly printed in the source.
        7) VERIFY (SOURCE-GROUNDED) that every extracted number/date/percentage/unit/clause is explicitly present in the input. Remove or correct anything not grounded.
        8) VALIDATE (SCHEMA & JSON) that the output is a valid JSON object that conforms exactly to the Pydantic schema (keys, types, null/""/omit conventions).
        9) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY a single valid JSON. No comments, no trailing commas, no text before/after.
        - Do NOT include fields not defined above.
        - Schema summary (orientation only; responseSchema enforces structure):
            Output a single JSON object:
            {{
            "salary_information": [ SalaryRow, ... ]
            }}
        
    """
