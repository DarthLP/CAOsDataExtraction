#!/usr/bin/env python3
"""
Export p3 extraction content as Markdown for all sections.

PURPOSE:
    Generate upload-friendly Markdown outputs under outputs/llm_extracted/excel_test/all_sections
    using p3 new_flow extraction JSON joined with extracted_cao_info metadata. The export includes:
    - by_topic/<section>.md for each p3 section
    - ALL.md with all sections
    - ALL_EXCEPT_SALARY.md with all sections except wage_information

INPUTS:
    - Excel CAO selection under inputs/excel/ (*.xlsx) or explicit --excel
    - Metadata CSVs: extracted_cao_info.csv (+ optional extracted_cao_info_extra.csv)
    - p3 extraction JSON: outputs/llm_extracted/new_flow/{cao_number}/*_extract.json

OUTPUTS:
    - outputs/llm_extracted/excel_test/all_sections/by_topic/*.md
    - outputs/llm_extracted/excel_test/all_sections/ALL.md
    - outputs/llm_extracted/excel_test/all_sections/ALL_EXCEPT_SALARY.md
    - outputs/llm_extracted/excel_test/all_sections/export_report.txt

USAGE:
    conda run -n caos-extract python scripts/export_p3_all_sections_markdown.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.cao_p3_export_helpers import (  # noqa: E402
    DEFAULT_EXCEL_DIR,
    DEFAULT_NEW_FLOW,
    INFO_EXTRA,
    INFO_MAIN,
    ExportReport,
    build_p3_rows,
    discover_excel,
    load_extracted_cao_info,
    read_ordered_cao_list,
    sort_key_combined,
)


DEFAULT_OUT_DIR = PROJECT_ROOT / "outputs/llm_extracted/excel_test/all_sections"
SECTION_ORDER = [
    "general_information",
    "wage_information",
    "pension_information",
    "leave_information",
    "termination_information",
    "overtime_information",
    "training_information",
    "homeoffice_information",
    "contract_type_information",
    "safety_information",
    "childcare_information",
    "AI_information",
    "fringe_benefits_information",
]


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(
        description="Export all p3 sections to markdown (by topic, all, all except salary)."
    )
    parser.add_argument(
        "--excel",
        type=Path,
        default=None,
        help="Path to .xlsx (default: sole file under inputs/excel/)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--new-flow",
        type=Path,
        default=DEFAULT_NEW_FLOW,
        help=f"p3 new_flow root (default: {DEFAULT_NEW_FLOW})",
    )
    parser.add_argument(
        "--info-main",
        type=Path,
        default=INFO_MAIN,
        help="extracted_cao_info.csv (main)",
    )
    parser.add_argument(
        "--info-extra",
        type=Path,
        default=INFO_EXTRA,
        help="extracted_cao_info_extra.csv (optional)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max unique CAO codes from Excel in row order (default: 100). Use 0 for no limit.",
    )
    return parser.parse_args()


def row_header(row: Dict[str, Any], index: int) -> List[str]:
    """
    Build a standard markdown header block for one extraction row.

    Args:
        row: Joined row from build_p3_rows.
        index: 1-based row index.

    Returns:
        Markdown lines for metadata heading.
    """
    return [
        f"## {index}. CAO {row['cao_number']} - {row['source_file_name']}",
        "",
        f"- cao_number: `{row['cao_number']}`",
        f"- source_file_name: `{row['source_file_name']}`",
        f"- ingangsdatum: `{row['ingangsdatum']}`",
        f"- datum_kennisgeving: `{row['datum_kennisgeving']}`",
        "",
    ]


def section_payload(data: Dict[str, Any], key: str) -> Any:
    """
    Read a section from extraction JSON safely.

    Args:
        data: Parsed extraction JSON.
        key: Section key to retrieve.

    Returns:
        Section payload, defaulting to [] when missing/null.
    """
    value = data.get(key, [])
    if value is None:
        return []
    return value


def write_by_topic(out_dir: Path, rows: List[Dict[str, Any]]) -> None:
    """
    Write one markdown file per topic key.

    Args:
        out_dir: Root output directory.
        rows: Joined extraction rows sorted by timeline.
    """
    topic_dir = out_dir / "by_topic"
    topic_dir.mkdir(parents=True, exist_ok=True)
    for key in SECTION_ORDER:
        lines: List[str] = [f"# {key}", "", f"Records: {len(rows)}", ""]
        for idx, row in enumerate(rows, 1):
            lines.extend(row_header(row, idx))
            lines.append("```json")
            lines.append(
                json.dumps(
                    section_payload(row.get("extraction_data", {}), key),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            lines.append("```")
            lines.append("")
        (topic_dir / f"{key}.md").write_text("\n".join(lines), encoding="utf-8")


def write_all_variants(out_dir: Path, rows: List[Dict[str, Any]]) -> None:
    """
    Write ALL.md and ALL_EXCEPT_SALARY.md.

    Args:
        out_dir: Root output directory.
        rows: Joined extraction rows sorted by timeline.
    """
    all_lines: List[str] = ["# ALL", "", f"Records: {len(rows)}", ""]
    non_salary_lines: List[str] = [
        "# ALL_EXCEPT_SALARY",
        "",
        f"Records: {len(rows)}",
        "",
    ]

    for idx, row in enumerate(rows, 1):
        all_lines.extend(row_header(row, idx))
        non_salary_lines.extend(row_header(row, idx))
        extraction_data = row.get("extraction_data", {})
        for key in SECTION_ORDER:
            all_lines.append(f"### {key}")
            all_lines.append("")
            all_lines.append("```json")
            all_lines.append(
                json.dumps(section_payload(extraction_data, key), ensure_ascii=False, indent=2)
            )
            all_lines.append("```")
            all_lines.append("")

            if key != "wage_information":
                non_salary_lines.append(f"### {key}")
                non_salary_lines.append("")
                non_salary_lines.append("```json")
                non_salary_lines.append(
                    json.dumps(
                        section_payload(extraction_data, key),
                        ensure_ascii=False,
                        indent=2,
                    )
                )
                non_salary_lines.append("```")
                non_salary_lines.append("")

    (out_dir / "ALL.md").write_text("\n".join(all_lines), encoding="utf-8")
    (out_dir / "ALL_EXCEPT_SALARY.md").write_text(
        "\n".join(non_salary_lines), encoding="utf-8"
    )


def main() -> None:
    """
    Build joined rows and write all-sections markdown outputs.

    Returns:
        None.
    """
    args = parse_args()
    limit: Optional[int] = None if args.limit == 0 else args.limit

    if args.excel is None:
        excel_path = discover_excel(DEFAULT_EXCEL_DIR)
    else:
        excel_path = args.excel
        if not excel_path.is_file():
            raise FileNotFoundError(f"Excel not found: {excel_path}")

    cao_list = read_ordered_cao_list(excel_path, limit)
    if not cao_list:
        raise ValueError("No CAO codes read from Excel.")

    info_df = load_extracted_cao_info(args.info_main, args.info_extra)
    report = ExportReport()
    rows = build_p3_rows(cao_list, info_df, args.new_flow, report)
    rows.sort(key=sort_key_combined)

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    write_by_topic(out_dir, rows)
    write_all_variants(out_dir, rows)

    report_path = out_dir / "export_report.txt"
    report_path.write_text("\n".join(report.lines()), encoding="utf-8")

    print(f"Wrote markdown exports under {out_dir}")
    print(f"  Rows processed: {len(rows)}")
    print(f"  Report: {report_path}")


if __name__ == "__main__":
    main()
