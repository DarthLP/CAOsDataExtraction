"""
Super Compact Salary Schema Definitions for CAO Data Extraction

This module contains ultra-super-compact Pydantic schema definitions used for salary data extraction
when compact and split schema attempts fail due to token limits. The super compact schema removes
all optional metadata fields, keeping only essential salary information to minimize JSON output size.
The timeline is represented as parallel arrays (sd, am) and a unit field (un) for maximum
token efficiency. The unit field is a single value if all points share the same unit, or an array if they differ.

USAGE:
    from schema.salary_schema_super_compact import (
        SalaryRowSuperCompact, SalaryExtractionSchemaSuperCompact,
        SALARY_PROMPT_SUPER_COMPACT
    )
"""

from typing import List, Optional, Union
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------
# SALARY POINT SUPER COMPACT: minimal essential fields only
# NOTE: This class is commented out as we now use parallel arrays in SalaryRowSuperCompact
# ---------------------------------------------------------------------
# class SalaryPointSuperCompact(BaseModel):
#     """
#     One effective salary value valid for a specific period (ultra-minimal version).
#     
#     Each SalaryPointSuperCompact represents a single salary entry from a wage table or 
#     general increase clause — defined by its start date. It records only the essential
#     salary amount and start date.
#     """
#
#     sd: str = Field(
#         ...,
#         description="start_date: Date the salary amount becomes effective (YYYY-MM-DD format)."
#     )
#
#     am: float = Field(
#         ...,
#         description="amount: Gross salary amount as printed in the CAO (no conversions or derivations)."
#     )


# ---------------------------------------------------------------------
# SALARY ROW SUPER COMPACT: minimal essential fields only
# ---------------------------------------------------------------------
class SalaryRowSuperCompact(BaseModel):
    """
    One complete wage-scale cell representing a combination of:
    (job group) × [optional step] × [optional worker type] × [optional age band] × [optional education level] × [optional contract type] × timeline.
    
    The timeline is represented as parallel arrays (sd, am) and a unit field (un) aligned by index position.
    Arrays sd and am must have the same length. The un field is either a single string if all timeline points share the same unit,
    or an array of strings (same length as sd/am) if units differ across timeline points.
    """

    # ---- Identification / scoping ----
    jg: str = Field(
        ...,
        description="jobgroup: Job group or salary scale label/code. If descriptive subtitle given, append in parentheses (e.g., 'F-45-9 (workers with high school diploma)'). Omit unimportant information like 'scale 10' or generic labels that don't add meaningful distinction."
    )

    st: Optional[str] = Field(
        default=None,
        description="step: Printed label of step/trede (e.g., 'trede 0', 'periodiek 3', 'aanloopschaal C'). Omit condition: if not printed."
    )

    wr: Optional[str] = Field(
        default=None,
        description="worker: Worker type or category. Omit if generic (e.g., 'employee', 'standard worker') AND only one worker type exists. Keep when meaningful (e.g., 'Construction worker', 'UTA employee'). Omit condition: if generic AND only one worker type exists."
    )

    # ---- Filters (only when printed) ----
    ag: Optional[str] = Field(
        default=None,
        description="age_group: Printed age band of wage table (e.g., '23 jaar', '21+', 'adult'). Consider only age bands capturing at least some workers aged 23-65. Omit condition: if not printed."
    )

    eu: Optional[str] = Field(
        default=None,
        description="education: Printed education level qualifier (e.g., 'MBO-2', 'HBO-bachelor'). Omit condition: if not printed."
    )

    pe: Optional[str] = Field(
        default=None,
        description="permanency: Contract permanency type as explicitly stated (e.g., 'permanent', 'temporary', 'fixed-term'). Omit condition: if not printed or not applicable."
    )

    # ---- Salary timeline - parallel arrays aligned by index ----
    sd: List[str] = Field(
        ...,
        description="start_date: Array of start dates (YYYY-MM-DD format). CRITICAL: Must have same length as am array. Each index position represents one timeline point."
    )

    am: List[float] = Field(
        ...,
        description="amount: Array of gross salary amounts as printed in the CAO (no conversions or derivations). CRITICAL: Must have same length as sd array. Each index position represents one timeline point."
    )

    un: Union[str, List[str]] = Field(
        ...,
        description="unit: Pay period unit abbreviation(s). Use: 'm' for monthly, '4-w' for 4-weekly, 'w' for weekly, 'h' for hourly, 'a' for annual. If all timeline points share the same unit, use a single value (e.g., 'm'). If units differ across timeline points, use an array with the same length as sd and am arrays, where each index position corresponds to the same timeline point as sd and am."
    )



class SalaryExtractionSchemaSuperCompact(BaseModel):
    """
    Top-level container for all extracted wage-related information from one CAO (super compact version).
    """
    si: List[SalaryRowSuperCompact] = Field(
        default_factory=list,
        description="salary_information: List of all wage-scale rows extracted from the CAO, each describing one (job group) × [optional step] × [optional worker type] × [optional age band] × [optional education level] × [optional contract type] × timeline."
    )


# ---------------------------------------------------------------------
# SALARY EXTRACTION PROMPT - SUPER COMPACT VERSION
# ---------------------------------------------------------------------

SALARY_PROMPT_SUPER_COMPACT = """Extract structured salary data from a JSON object derived from the Dutch CAO document.

    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no hallucination, no guessing, no markdown fences, no extra text.

    INPUTS
    Filename: {filename}
    Source text: {source_json}

    FIELD NAME ABBREVIATIONS
    Field names are abbreviations to minimize JSON output size. Each field description follows this structure: original field name: description. Omit condition (if applicable): when to omit this field.

    CRITICAL RULES
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
        - Missing values: Omit optional fields entirely when the omit condition is met. Only include optional fields with actual values.
        - DO NOT include null values in JSON output. If an optional field has no value, omit the entire field/key from the JSON object. Including null wastes tokens and may cause truncation.
        - ARRAY ALIGNMENT: Arrays sd and am MUST have the same length. Each index position represents one timeline point. The un field must be either a single string (if all units are the same) or an array with the same length as sd/am (if units differ).
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
        - If contract permanency determine different wages, create separate rows per jobgroup × step × [worker type] × contract type (× [age] × [education]).
        - Worker type field: OMIT if the value is generic (e.g. 'employee', 'standard worker') AND there is only one worker type in the entire CAO. KEEP when it provides meaningful distinction between different worker categories.

    TABLE AMOUNTS, DATES
        - Salary amount: output as a number using a dot as the decimal separator (e.g., 2300.00). Do NOT use quotes, commas or thousands separators.
        - Dates: Use YYYY-MM-DD format (e.g., "2023-11-01"). Do NOT invent or infer dates. Only include start_date (sd array). Do NOT include end_date.
    
    TABLE TIMELINE CONSTRUCTION - PARALLEL ARRAYS
        - CRITICAL: Arrays sd and am MUST have the same length and align by index position.
        - For each (jobgroup × step × [worker type] × [age] × [education] × [contract type]), build parallel arrays with one element per table version that prints salary amounts.
        - Each timeline point MUST have a printed amount. If only a % increase is announced but no new amounts are printed, DO NOT add a timeline point; instead omit it entirely.
        - Use start_date (sd array) exactly as the table heading or clause states, converting to YYYY-MM-DD format (e.g., "per 1 Nov 2023" → "2023-11-01"). If day is not printed, use the first day of the month, same for month.
        - Align timeline points for the SAME (jobgroup × step × [worker type] × [age] × [education] × [contract type]) across time periods / table versions. Do not impute missing values.
        - Unit (un field): Extract the unit for each timeline point. If all timeline points have the same unit, use a single string value (e.g., 'm'). If units differ across timeline points, use an array with the same length as sd and am arrays, with each index position matching the corresponding timeline point. Use abbreviations: 'm' for monthly, '4-w' for 4-weekly, 'w' for weekly, 'h' for hourly, 'a' for annual.

    WORKFLOW STEPS (INTERNAL - DO NOT OUTPUT)
        1) READ & ANCHOR
            - Review all general rules and field descriptions in the Pydantic output schema.
            - Read the input text to understand its structure, content, and table layout.
        2) LOCATE & MARK all standard wage tables according to the TABLE SELECTION rules.
        3) DETECT AGE GROUPS: Within each selected table, extract all age groups meeting the TABLE AGE GROUP SELECTION criteria.
        4) DETECT JOB GROUPS, STEPS, EDUCATION LEVELS & CONTRACT TYPES: For each table version, identify job groups, steps, worker types, education levels, and contract types following the TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE rules.
        5) CONSTRUCT TIMELINE STRUCTURE:
            5.1) Apply TABLE TIMELINE CONSTRUCTION rules to align job groups, steps, worker types, education levels, age bands, and contract types across table versions.
            5.2) Build one SalaryRow for every unique detected combination of (jobgroup × step × [worker type] × [age] × [education] × [contract type]).
            5.3) Build parallel arrays: For each SalaryRow, create parallel arrays (sd, am, un, ip) with one element per version/time period where that combination appears. Ensure all arrays have the same length - use null in ip array for missing increment percentages. Normalize labels (jobgroup/step/worker type/age/education/contract type), deduplicate identical periods, and align all timeline points that refer to the same combination across versions (no imputation).
        6) SORT & CLEAN each row's parallel arrays chronologically by start_date (sd array). Ensure all arrays remain aligned by index after sorting. Omit all optional fields not explicitly printed in the source.
        7) VERIFY (SOURCE-GROUNDED) that every extracted number/date/percentage/unit/clause is explicitly present in the input. Remove or correct anything not grounded.
        8) VALIDATE (SCHEMA & JSON) that the output is a valid JSON object that conforms exactly to the Pydantic schema (keys, types, null/""/omit conventions).
        9) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY a single valid JSON. No comments, no trailing commas, no text before/after.
        - Do NOT include fields not defined above.
        - Schema summary (orientation only; responseSchema enforces structure):
            Output a single JSON object:
            {{
            "si": [ SalaryRow, ... ]
            }}
            Each SalaryRow contains parallel arrays: sd (start dates), am (amounts), and a un field (unit).
            Arrays sd and am must have the same length, aligned by index position.
            The un field is either a single string (if all units are the same) or an array with the same length as sd/am (if units differ).
        
    """