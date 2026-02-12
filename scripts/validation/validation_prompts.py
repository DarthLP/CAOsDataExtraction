"""
Validation prompts and schema helpers for extraction validation.

This module provides:
- Schema-to-text extraction from Pydantic models (field names + descriptions)
- Salary schema variant detection and field mapping
- Validation prompt templates for salary and non-salary
- Pydantic output schema for validation results

USAGE:
    from scripts.validation.validation_prompts import (
        build_salary_schema_text,
        build_non_salary_schema_text,
        detect_salary_schema_variant,
        build_validation_prompt,
        ValidationOutputSchema
    )
"""

import json
from typing import Any, Dict, List, Optional, Type
from pydantic import BaseModel, Field

# Salary schema variant constants
SALARY_VARIANT_REGULAR = "regular"
SALARY_VARIANT_COMPACT = "compact"
SALARY_VARIANT_SUPER_COMPACT = "super_compact"

# Field mapping: abbreviated name -> full/canonical name (for salary compact/super_compact)
SALARY_ABBREV_TO_FULL = {
    "si": "salary_information",
    "jg": "jobgroup",
    "st": "step",
    "wr": "worker",
    "ie": "is_entry",
    "ag": "age_group",
    "eu": "education",
    "fh": "ft_hours",
    "pe": "permanency",
    "ht": "hours_type",
    "hi": "holiday_incl",
    "tl": "timeline",
    "rn": "row_note",
    "sd": "start_date",
    "ed": "end_date",
    "am": "amount",
    "un": "unit",
    "ip": "inc_pct",
    "nt": "note",
}

# Super compact intentionally removed fields - do NOT flag as missing
SALARY_SUPER_COMPACT_REMOVED_FIELDS = {"ed", "nt", "ie", "fh", "ht", "hi", "rn", "table_label"}


def extract_model_fields_text(model: Type[BaseModel], indent: str = "") -> str:
    """
    Extract field names and descriptions from a Pydantic model.

    Args:
        model: Pydantic model class
        indent: Indentation prefix for output

    Returns:
        Formatted string of field_name: description
    """
    lines = []
    for field_name, field_info in model.model_fields.items():
        desc = getattr(field_info, "description", None) or ""
        # Handle multiline descriptions
        desc = desc.replace("\n", " ").strip()
        lines.append(f'{indent}- **{field_name}**: {desc}')
    return "\n".join(lines) if lines else ""


def extract_nested_schema_text(model: Type[BaseModel], section_name: str = "") -> str:
    """
    Extract schema text for a model, including nested models (e.g. GeneralInfo has many fields).

    Args:
        model: Pydantic model class
        section_name: Optional section header

    Returns:
        Formatted schema text
    """
    lines = []
    if section_name:
        lines.append(f"### {section_name}")
    for field_name, field_info in model.model_fields.items():
        desc = getattr(field_info, "description", None) or ""
        desc = desc.replace("\n", " ").strip()
        # Check if it's a nested BaseModel
        ann = field_info.annotation
        if hasattr(ann, "model_fields") and issubclass(ann, BaseModel):
            lines.append(f"- **{field_name}**: (nested object with fields below)")
            for nested_name, nested_info in ann.model_fields.items():
                nested_desc = getattr(nested_info, "description", None) or ""
                nested_desc = nested_desc.replace("\n", " ").strip()
                lines.append(f"  - {nested_name}: {nested_desc}")
        else:
            lines.append(f"- **{field_name}**: {desc}")
    return "\n".join(lines) if lines else ""


def detect_salary_schema_variant(data: Dict[str, Any]) -> str:
    """
    Detect which salary schema variant was used for the extraction output.

    Args:
        data: The salary extraction JSON (top-level dict)

    Returns:
        One of: "regular", "compact", "super_compact"
    """
    rows = data.get("salary_information") or data.get("si", [])
    if not rows:
        return SALARY_VARIANT_REGULAR  # Default

    first_row = rows[0] if isinstance(rows[0], dict) else {}
    if not first_row:
        return SALARY_VARIANT_REGULAR

    # Check for regular schema (full field names)
    if "salary_information" in data and "jobgroup" in first_row:
        return SALARY_VARIANT_REGULAR

    # Has "si" - compact or super_compact
    if "jg" in first_row:
        # Super compact: timeline is parallel arrays (sd, am) at row level, not nested objects
        # Compact: timeline is "tl" with list of point objects {sd, am, un, ...}
        if "tl" in first_row:
            tl_val = first_row["tl"]
            if isinstance(tl_val, list) and tl_val:
                first_point = tl_val[0]
                if isinstance(first_point, dict) and "sd" in first_point and "am" in first_point:
                    return SALARY_VARIANT_COMPACT  # Nested timeline objects
        # Super compact: sd and am are arrays at row level
        if "sd" in first_row and "am" in first_row:
            if isinstance(first_row["sd"], list) and isinstance(first_row["am"], list):
                return SALARY_VARIANT_SUPER_COMPACT
        # Compact with tl (list of point objects)
        if "tl" in first_row:
            return SALARY_VARIANT_COMPACT
        # Fallback: has ed, nt, rn -> compact; else super_compact
        if any(k in first_row for k in ["ed", "nt", "rn"]):
            return SALARY_VARIANT_COMPACT
        return SALARY_VARIANT_SUPER_COMPACT

    return SALARY_VARIANT_REGULAR


def build_salary_schema_text(variant: str) -> str:
    """
    Build schema description text for salary validation based on detected variant.

    Args:
        variant: One of "regular", "compact", "super_compact"

    Returns:
        Formatted schema text for the validation prompt
    """
    try:
        from schema.salary_schema import SalaryRow, SalaryPoint
        from schema.salary_schema_compact import SalaryRowCompact, SalaryPointCompact
        from schema.salary_schema_super_compact import SalaryRowSuperCompact
    except ImportError:
        return "## Salary Schema\n(Schema modules not available - using generic descriptions.)"

    lines = ["## Expected Salary Schema (Field Definitions)", ""]
    lines.append("Use these definitions to judge hallucination, completeness, and accuracy.")
    lines.append("")

    if variant == SALARY_VARIANT_REGULAR:
        lines.append("### SalaryRow (per wage-scale row)")
        lines.append(extract_model_fields_text(SalaryRow))
        lines.append("")
        lines.append("### SalaryPoint (per timeline entry)")
        lines.append(extract_model_fields_text(SalaryPoint))
    elif variant == SALARY_VARIANT_COMPACT:
        lines.append("**This extraction uses abbreviated field names.** Mapping:")
        lines.append("si=salary_information, jg=jobgroup, st=step, wr=worker, ie=is_entry,")
        lines.append("ag=age_group, eu=education, fh=ft_hours, pe=permanency, ht=hours_type,")
        lines.append("hi=holiday_incl, tl=timeline, rn=row_note, sd=start_date, ed=end_date,")
        lines.append("am=amount, un=unit, ip=inc_pct, nt=note.")
        lines.append("")
        lines.append("### SalaryRow (abbreviated: jg, st, wr, tl, etc.)")
        lines.append(extract_model_fields_text(SalaryRowCompact))
        lines.append("")
        lines.append("### SalaryPoint in timeline (abbreviated: sd, ed, am, un, ip, nt)")
        lines.append(extract_model_fields_text(SalaryPointCompact))
    else:  # super_compact
        lines.append("**This extraction uses super-compact format.** Mapping:")
        lines.append("si=salary_information, jg=jobgroup, st=step, wr=worker, ag=age_group,")
        lines.append("eu=education, pe=permanency, sd=start_date array, am=amount array, un=unit.")
        lines.append("")
        lines.append("**Note:** Fields ed, nt, ie, fh, ht, hi, rn, table_label are intentionally ")
        lines.append("omitted in super-compact format. Do NOT flag them as missing.")
        lines.append("")
        lines.append("### SalaryRowSuperCompact (parallel arrays sd, am, un)")
        lines.append(extract_model_fields_text(SalaryRowSuperCompact))

    return "\n".join(lines)


def build_non_salary_schema_text() -> str:
    """
    Build schema description text for non-salary validation.

    Returns:
        Formatted schema text for the validation prompt
    """
    try:
        from schema.non_salary_schema import (
            GeneralInfo, BonusesInfo, WageScalesInfo, PensionInfo, TerminationInfo,
            LeaveInfo, OvertimeInfo, TrainingInfo, HomeofficeInfo, ContractTypeInfo,
            SafetyInfo, ChildcareInfo, AIInfo, FringeBenefitsInfo
        )
    except ImportError:
        return "## Non-Salary Schema\n(Schema modules not available.)"

    lines = ["## Expected Non-Salary Schema (Field Definitions)", ""]
    lines.append("Use these definitions to judge hallucination, completeness, and accuracy.")
    lines.append("")

    sections = [
        ("general_information", GeneralInfo, "General contract information"),
        ("bonuses_info", BonusesInfo, "Bonus and incentive schemes"),
        ("wage_scales_info", WageScalesInfo, "Wage scale progression rules"),
        ("pension_information", PensionInfo, "Pension scheme details"),
        ("termination_information", TerminationInfo, "Termination and notice rules"),
        ("leave_information", LeaveInfo, "Leave entitlements"),
        ("overtime_information", OvertimeInfo, "Overtime rules"),
        ("training_information", TrainingInfo, "Training provisions"),
        ("homeoffice_information", HomeofficeInfo, "Home office/telework"),
        ("contract_type_information", ContractTypeInfo, "Contract type rules"),
        ("safety_information", SafetyInfo, "Safety and integrity"),
        ("childcare_information", ChildcareInfo, "Childcare support"),
        ("ai_information", AIInfo, "AI/algorithmic policies"),
        ("fringe_benefits_information", FringeBenefitsInfo, "Fringe benefits"),
    ]
    for section_key, model, section_label in sections:
        lines.append(f"### {section_key} ({section_label})")
        lines.append(extract_model_fields_text(model))
        lines.append("")

    return "\n".join(lines)


# Pydantic schema for validation output
class HallucinationIssue(BaseModel):
    variable: str
    current_output: str
    correction: str


class CompletenessIssue(BaseModel):
    variable: str
    missing_info: str
    current_output: Optional[str] = None


class TemporalValidityIssue(BaseModel):
    description: str


class VariableStat(BaseModel):
    variable: str
    pct_missing: float = 0.0
    pct_wrong: float = 0.0


class ValidationOutputSchema(BaseModel):
    """Output schema for the validation LLM response."""
    hallucination: Dict[str, Any] = Field(
        default_factory=dict,
        description="Hallucination check: score 0-1, rationale, issues list"
    )
    completeness: Dict[str, Any] = Field(
        default_factory=dict,
        description="Completeness check: score 0-1, rationale, issues list"
    )
    accuracy: Dict[str, Any] = Field(
        default_factory=dict,
        description="Accuracy check: score 0-1, rationale"
    )
    temporal_validity: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Temporal validity (salary only): score 0-1, rationale, issues"
    )
    overall: Dict[str, Any] = Field(
        default_factory=dict,
        description="Overall pass/fail and rationale"
    )
    variable_stats: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Per-variable stats: pct_missing, pct_wrong"
    )


VALIDATION_OUTPUT_JSON_EXAMPLE = """
Example output format (return valid JSON only):
{
  "hallucination": {"score": 1.0, "rationale": "...", "issues": []},
  "completeness": {"score": 0.9, "rationale": "...", "issues": [{"variable": "...", "missing_info": "...", "current_output": "..."}]},
  "accuracy": {"score": 0.95, "rationale": "..."},
  "temporal_validity": {"score": 1.0, "rationale": "...", "issues": []},
  "overall": {"pass": true, "rationale": "..."},
  "variable_stats": [{"variable": "jobgroup", "pct_missing": 0.0, "pct_wrong": 0.0}]
}
"""


def build_validation_prompt(
    extraction_type: str,
    extracted_json: Dict[str, Any],
    filename: str,
    schema_text: str,
    include_temporal_validity: bool = True
) -> str:
    """
    Build the full validation prompt.

    Args:
        extraction_type: "salary" or "non_salary"
        extracted_json: The extracted JSON to validate
        filename: Original filename for context
        schema_text: Pre-built schema description text
        include_temporal_validity: Whether to include temporal validity (salary only)

    Returns:
        Full prompt string
    """
    json_str = json.dumps(extracted_json, indent=2, ensure_ascii=False)

    lines = [
        "# TASK: Validation of CAO Extraction",
        "",
        "You are validating extracted data against the source CAO document. The source document is attached as a markdown file.",
        "",
        "Compare the extraction output below to the attached markdown. Assess:",
        "",
        "1. **Hallucination**: Any variable whose value is NOT supported by the source? List: variable, current_output, correction.",
        "2. **Completeness**: Any variable where the source contains information that is MISSING or wrong in the extraction? List: variable, missing_info, current_output.",
        "3. **Accuracy**: How faithfully does the extraction represent the source? Score 0-1.",
    ]
    if include_temporal_validity:
        lines.extend([
            "4. **Temporal validity**: (Salary only) Are dates in logical order, no overlaps or gaps? Score 0-1.",
            "5. **Overall**: Pass/fail based on combined assessment.",
        ])
    else:
        lines.extend([
            "4. **Overall**: Pass/fail based on combined assessment.",
        ])
    lines.extend([
        "",
        "---",
        "",
        schema_text,
        "",
        "---",
        "",
        f"## File: {filename}",
        "",
        "## Extraction output to validate",
        "",
        "```json",
        json_str,
        "```",
        "",
        "---",
        "",
        "## Instructions",
        "",
    ])
    if extraction_type == "non_salary":
        lines.append("- Give extra attention to childcare_information fields (especially min_tenure_months) when validating.")
        lines.append("")
    lines.extend([
        "Return a single JSON object with: hallucination (score, rationale, issues), completeness (score, rationale, issues), accuracy (score, rationale), "
    ])
    if include_temporal_validity:
        lines.append("temporal_validity (score, rationale, issues), ")
    lines.extend([
        "overall (pass, rationale), and optionally variable_stats (list of {variable, pct_missing, pct_wrong}).",
        "",
        "Each score is 0.0 to 1.0 (1=perfect). Issues are lists of objects; use empty list if none.",
        "",
        VALIDATION_OUTPUT_JSON_EXAMPLE,
    ])
    return "\n".join(lines)
