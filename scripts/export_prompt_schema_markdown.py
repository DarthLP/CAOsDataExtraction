#!/usr/bin/env python3
"""
Export prompt and schema references for non-salary and salary extraction.

PURPOSE:
    Build two markdown reference files under outputs/llm_extracted/excel_test:
    1) NON_SALARY_PROMPTS_AND_SCHEMA.md
    2) SALARY_PROMPTS_AND_SCHEMA.md

    The non-salary file includes:
    - Full general NON_SALARY_PROMPT
    - One topic-specific prompt section per topic (general first, then topic-specific)
    - Flat leaf-field schema listing (no nested class blocks), grouped by topic
      with strict completeness validation to ensure no missing or extra fields.

    The salary file includes:
    - Standard prompt + schema (with clear usage label)
    - Super-compact prompt + schema (with clear usage label)

USAGE:
    conda run -n caos-extract python scripts/export_prompt_schema_markdown.py
    conda run -n caos-extract python scripts/export_prompt_schema_markdown.py \
        --out-dir outputs/llm_extracted/excel_test
"""

from __future__ import annotations

import argparse
import inspect
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type, Union, get_args, get_origin

from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from schema.non_salary_schema import NON_SALARY_PROMPT, NonSalaryPart1, NonSalaryPart2, NonSalaryPart3
from schema.salary_schema import (
    SALARY_PROMPT,
    Amount,
    AmountRange,
    SalaryExtractionSchema,
    SalaryPoint,
    SalaryRow,
)
from schema.salary_schema_super_compact import (
    SALARY_PROMPT_SUPER_COMPACT,
    SalaryExtractionSchemaSuperCompact,
    SalaryRowSuperCompact,
)


DEFAULT_OUT_DIR = PROJECT_ROOT / "outputs/llm_extracted/excel_test"
TERMINAL_COMPOSITE_MODELS = {Amount, AmountRange}
P4_PART_SECTIONS: Dict[str, str] = {
    "Part 1": "general_information, bonuses_info, wage_scales_info, pension_information, termination_information",
    "Part 2": "leave_information, overtime_information, training_information",
    "Part 3": "homeoffice_information, contract_type_information, safety_information, childcare_information, ai_information, fringe_benefits_information",
}
TOPIC_SUMMARY_BY_NAME: Dict[str, str] = {
    "general_information": "General: CAO dates, scope, retroactivity, AVV status, SBI codes, document type",
    "bonuses_info": "Bonuses: Sign-on, 13th month, profit sharing, performance, job allowances, qualification, seniority/loyalty, retirement gratuity",
    "wage_scales_info": "Wage Scales: Entry step rules, personal allowances, performance step variations",
    "pension_information": "Pension: Scheme type, contributions, accrual rates, retirement ages, heterogeneity",
    "termination_information": "Termination: Notice periods, severance, dismissal rules, probation, tenure-based rules",
    "leave_information": "Leave: Maternity, paternity, adoption, parental, sick, care, vacation, and special leave entitlements",
    "overtime_information": "Overtime: Trigger thresholds, compensation modes, allowances, compulsory limits, rest requirements",
    "training_information": "Training: Time allowances, budgets, funds, cost reimbursement, mandatory training provisions",
    "homeoffice_information": "Homeoffice: Remote work entitlements, stipends, cost reimbursement, agreements, health/safety guarantees",
    "contract_type_information": "Contract Types: Full-time hours, part-time, min-max contracts, ketenregeling, conversion rights, work hours adjustment",
    "safety_information": "Safety: Harassment/integrity protocols, confidential counsellors, reporting channels, safety training, wellbeing programs",
    "childcare_information": "Childcare: Support amounts, in-house facilities, discounts, priority access, age limits, funding sources",
    "ai_information": "AI: Policy existence, automated decisions, governance bodies, training rights, transparency requirements",
    "fringe_benefits_information": "Fringe Benefits: Commuting, bike schemes, meal benefits, health insurance, relocation, certifications",
}


def topic_to_part_mapping() -> Dict[str, str]:
    """
    Map each non-salary topic name to its p4 extraction part.

    Parameters:
        None.

    Returns:
        Dict[str, str]: Topic name -> part label.
    """
    mapping: Dict[str, str] = {}
    for part_name, section_csv in P4_PART_SECTIONS.items():
        for topic in [item.strip() for item in section_csv.split(",") if item.strip()]:
            mapping[topic] = part_name
    return mapping


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for output location.

    Parameters:
        None.

    Returns:
        argparse.Namespace: Parsed arguments with output directory path.
    """
    parser = argparse.ArgumentParser(
        description="Export non-salary and salary prompt/schema markdown files."
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUT_DIR})",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    """
    Build an ISO timestamp for markdown headers.

    Parameters:
        None.

    Returns:
        str: UTC timestamp in ISO format.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def model_to_code_block(model: Type[BaseModel]) -> str:
    """
    Return exact source code for a Pydantic model class.

    Parameters:
        model: Model class to serialize as source code.

    Returns:
        str: Python class source code text.
    """
    return inspect.getsource(model).rstrip()


def resolve_inner_type(annotation: Any) -> Any:
    """
    Unwrap Optional/Union wrappers and return the inner annotation.

    Parameters:
        annotation: Original type annotation from a model field.

    Returns:
        Any: Unwrapped annotation if Optional/Union, otherwise original annotation.
    """
    origin = get_origin(annotation)
    if origin in (Union,):
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return resolve_inner_type(args[0])
    return annotation


def annotation_to_label(annotation: Any) -> str:
    """
    Convert an annotation into a compact human-readable label.

    Parameters:
        annotation: Python typing annotation.

    Returns:
        str: Readable type label for markdown output.
    """
    annotation = resolve_inner_type(annotation)
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (list, List):
        if args:
            return f"list[{annotation_to_label(args[0])}]"
        return "list[Any]"
    if origin in (dict, Dict):
        if len(args) == 2:
            return f"dict[{annotation_to_label(args[0])}, {annotation_to_label(args[1])}]"
        return "dict[Any, Any]"
    if origin in (Union,):
        parts = [annotation_to_label(arg) for arg in args]
        return f"Union[{', '.join(parts)}]"
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def nested_model_from_annotation(annotation: Any) -> Optional[Type[BaseModel]]:
    """
    Detect whether an annotation points to a nested Pydantic model.

    Parameters:
        annotation: Field annotation from Pydantic model metadata.

    Returns:
        Optional[Type[BaseModel]]: Nested model class if detected, otherwise None.
    """
    annotation = resolve_inner_type(annotation)
    origin = get_origin(annotation)
    args = get_args(annotation)
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    if origin in (list, List) and args:
        inner = resolve_inner_type(args[0])
        if isinstance(inner, type) and issubclass(inner, BaseModel):
            return inner
    return None


def iter_leaf_fields(model: Type[BaseModel], prefix: str = "") -> Iterable[Tuple[str, str, str]]:
    """
    Recursively flatten model fields into leaf field entries.

    Parameters:
        model: Model class to flatten.
        prefix: Prefix used for nested dot-path generation.

    Returns:
        Iterable[Tuple[str, str, str]]: Sequence of (field_path, type_label, description).
    """
    for field_name, field_info in model.model_fields.items():
        path = f"{prefix}.{field_name}" if prefix else field_name
        nested_model = nested_model_from_annotation(field_info.annotation)
        if nested_model is not None and nested_model not in TERMINAL_COMPOSITE_MODELS:
            yield from iter_leaf_fields(nested_model, prefix=path)
            continue
        type_label = annotation_to_label(field_info.annotation)
        description = (field_info.description or "").strip()
        yield (path, type_label, description)


def ordered_topic_models() -> List[Tuple[str, Type[BaseModel]]]:
    """
    Build the non-salary topic order from Part1/Part2/Part3 model definitions.

    Parameters:
        None.

    Returns:
        List[Tuple[str, Type[BaseModel]]]: Ordered list of (topic_name, topic_model).
    """
    topics: List[Tuple[str, Type[BaseModel]]] = []
    for part_model in (NonSalaryPart1, NonSalaryPart2, NonSalaryPart3):
        for topic_name, field_info in part_model.model_fields.items():
            nested_model = nested_model_from_annotation(field_info.annotation)
            if nested_model is None:
                raise TypeError(
                    f"Expected nested model for topic '{topic_name}' in {part_model.__name__}."
                )
            topics.append((topic_name, nested_model))
    return topics


def topic_field_description_map() -> Dict[str, str]:
    """
    Build a map of topic names to explicit field-level descriptions in Part models.

    Parameters:
        None.

    Returns:
        Dict[str, str]: Topic name -> stripped field description text.
    """
    description_map: Dict[str, str] = {}
    for part_model in (NonSalaryPart1, NonSalaryPart2, NonSalaryPart3):
        for topic_name, field_info in part_model.model_fields.items():
            desc = (field_info.description or "").strip()
            if desc:
                description_map[topic_name] = desc
    return description_map


def build_non_salary_markdown() -> str:
    """
    Build non-salary markdown content with prompts and flattened schema entries.

    Parameters:
        None.

    Returns:
        str: Full markdown payload for NON_SALARY_PROMPTS_AND_SCHEMA.md.
    """
    timestamp = now_utc_iso()
    topics = ordered_topic_models()
    topic_part_map = topic_to_part_mapping()
    topic_field_desc_map = topic_field_description_map()

    lines: List[str] = []
    lines.append("# NON_SALARY_PROMPTS_AND_SCHEMA")
    lines.append("")
    lines.append(f"- Generated UTC: `{timestamp}`")
    lines.append("- Source prompt: `schema/non_salary_schema.py::NON_SALARY_PROMPT`")
    lines.append("- Source schema: `schema/non_salary_schema.py`")
    lines.append("")
    lines.append("## General Prompt")
    lines.append("")
    lines.append("```text")
    lines.append(
        NON_SALARY_PROMPT.format(
            filename="{filename}",
            source_json="{source_json}",
            sections="{sections}",
        ).rstrip()
    )
    lines.append("```")
    lines.append("")
    lines.append("## Field Type Notes")
    lines.append("")
    lines.append("- `Amount`: object with `value` (number) and `unit` (string).")
    lines.append("- `AmountRange`: object with `min` (number), `max` (number), and `unit` (string).")
    lines.append("- In topic field lists below, these are kept as single fields (not split into sub-keys).")
    lines.append("")
    lines.append("## Topic Descriptions")
    lines.append("")

    for topic_name, topic_model in topics:
        part_name = topic_part_map.get(topic_name)
        if part_name is None:
            raise ValueError(f"Topic '{topic_name}' is missing in p4 part mapping.")
        topic_summary = TOPIC_SUMMARY_BY_NAME.get(topic_name, "")
        field_level_description = topic_field_desc_map.get(topic_name, "").strip()
        raw_doc = topic_model.__doc__ if isinstance(topic_model.__doc__, str) else ""
        class_doc_description = raw_doc.strip()
        schema_description = field_level_description or class_doc_description
        if not schema_description:
            schema_description = f"Schema for `{topic_name}` (record exactly as stated in the CAO)."

        lines.append(f"### {topic_name}")
        lines.append("")
        lines.append(f"#### Topic Description (from p4 {part_name})")
        lines.append("")
        lines.append(topic_summary if topic_summary else "(no topic summary)")
        lines.append("")
        lines.append("#### Schema Description")
        lines.append("")
        lines.append(schema_description)
        lines.append("")
        topic_entries = list(iter_leaf_fields(topic_model, prefix=topic_name))
        lines.append("#### Fields")
        lines.append("")
        lines.append(
            "Leaf fields only. Every entry below is copied from source model metadata using dot-path notation."
        )
        lines.append("")

        for path, type_label, description in topic_entries:
            safe_desc = description if description else "(no description)"
            lines.append(f"`{path}` | `{type_label}`")
            lines.append(safe_desc)
            lines.append("")

    expected_leaf_paths: Set[str] = set()
    for topic_name, topic_model in topics:
        topic_entries = list(iter_leaf_fields(topic_model, prefix=topic_name))
        for path, _type_label, _description in topic_entries:
            expected_leaf_paths.add(path)

    exported_leaf_paths: Set[str] = set()
    for line in lines:
        if line.startswith("`") and "` | `" in line:
            field_path = line.split("` | `", 1)[0].strip("`")
            exported_leaf_paths.add(field_path)

    lines.append("## Completeness Check")
    lines.append("")
    lines.append(
        f"- Leaf fields validated 1:1: `{len(exported_leaf_paths)}` exported, `{len(expected_leaf_paths)}` expected."
    )
    lines.append("")

    missing = sorted(expected_leaf_paths - exported_leaf_paths)
    extra = sorted(exported_leaf_paths - expected_leaf_paths)
    if missing or extra:
        mismatch_lines = [
            "Non-salary leaf field completeness check failed.",
            f"Missing fields ({len(missing)}): {missing}",
            f"Extra fields ({len(extra)}): {extra}",
        ]
        raise ValueError("\n".join(mismatch_lines))

    lines.append("- Missing fields: `0`")
    lines.append("- Extra fields: `0`")
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def build_salary_markdown() -> str:
    """
    Build salary markdown content with standard and super-compact variants.

    Parameters:
        None.

    Returns:
        str: Full markdown payload for SALARY_PROMPTS_AND_SCHEMA.md.
    """
    timestamp = now_utc_iso()
    lines: List[str] = []
    lines.append("# SALARY_PROMPTS_AND_SCHEMA")
    lines.append("")
    lines.append(f"- Generated UTC: `{timestamp}`")
    lines.append("- Standard source: `schema/salary_schema.py`")
    lines.append("- Super-compact source: `schema/salary_schema_super_compact.py`")
    lines.append("")
    lines.append("## Standard Salary Variant")
    lines.append("")
    lines.append("Usage: **Used for standard/full extraction.**")
    lines.append("")
    lines.append("### Prompt (`SALARY_PROMPT`)")
    lines.append("")
    lines.append("```text")
    lines.append(
        SALARY_PROMPT.format(
            filename="{filename}",
            source_json="{source_json}",
        ).rstrip()
    )
    lines.append("```")
    lines.append("")
    lines.append("### Schema (`salary_schema.py`)")
    lines.append("")
    lines.append("```python")
    for model in (Amount, AmountRange, SalaryPoint, SalaryRow, SalaryExtractionSchema):
        lines.append(model_to_code_block(model))
        lines.append("")
    lines.append("```")
    lines.append("")
    lines.append("## Super-Compact Salary Variant")
    lines.append("")
    lines.append("Usage: **Used when token pressure/compact output format is required.**")
    lines.append("")
    lines.append("### Prompt (`SALARY_PROMPT_SUPER_COMPACT`)")
    lines.append("")
    lines.append("```text")
    lines.append(
        SALARY_PROMPT_SUPER_COMPACT.format(
            filename="{filename}",
            source_json="{source_json}",
        ).rstrip()
    )
    lines.append("```")
    lines.append("")
    lines.append("### Schema (`salary_schema_super_compact.py`)")
    lines.append("")
    lines.append("```python")
    for model in (SalaryRowSuperCompact, SalaryExtractionSchemaSuperCompact):
        lines.append(model_to_code_block(model))
        lines.append("")
    lines.append("```")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_exports(out_dir: Path) -> Tuple[Path, Path]:
    """
    Render and write both markdown export files to disk.

    Parameters:
        out_dir: Target output directory.

    Returns:
        Tuple[Path, Path]: Paths to non-salary and salary markdown outputs.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    non_salary_path = out_dir / "NON_SALARY_PROMPTS_AND_SCHEMA.md"
    salary_path = out_dir / "SALARY_PROMPTS_AND_SCHEMA.md"
    non_salary_path.write_text(build_non_salary_markdown(), encoding="utf-8")
    salary_path.write_text(build_salary_markdown(), encoding="utf-8")
    return non_salary_path, salary_path


def main() -> None:
    """
    Execute markdown export workflow.

    Parameters:
        None.

    Returns:
        None.
    """
    args = parse_args()
    out_dir = args.out_dir.resolve()
    non_salary_path, salary_path = write_exports(out_dir)
    print(f"Wrote: {non_salary_path}")
    print(f"Wrote: {salary_path}")


if __name__ == "__main__":
    main()
