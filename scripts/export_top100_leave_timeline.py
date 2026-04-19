#!/usr/bin/env python3
"""
Export leave_information from p3 LLM extraction (new_flow JSON) for a CAO list from Excel.

PURPOSE:
    Build merged CSV/JSONL exports (full and pre-2015) plus one JSONL per CAO under per_cao/,
    joining timeline metadata from extracted_cao_info (ingangsdatum, datum_kennisgeving, pdf_name).
    Data is read only from p3 outputs (outputs/llm_extracted/new_flow), not p4 analysis.

INPUTS:
    - Excel: CAO codes under inputs/excel/ (*.xlsx) or path via --excel. Column: code (fallback:
      cao_number, CAO). Row order defines priority; first N unique codes used when --limit is set.
    - Metadata: inputs/pdfs/input_pdfs/extracted_cao_info.csv and optional
      inputs/pdfs/input_pdfs_extra/extracted_cao_info_extra.csv (semicolon-separated).
    - Extractions: outputs/llm_extracted/new_flow/{cao_number}/*_extract.json

OUTPUTS (default root outputs/llm_extracted/excel_test/):
    - top100_leave_all.csv / top100_leave_all.jsonl
    - top100_leave_pre2015.csv / top100_leave_pre2015.jsonl (ingangsdatum < 2015-01-01)
    - per_cao/{cao}_Leave_Timeline.jsonl (lines omit cao_number; sorted by ingangsdatum)
    - export_report.txt (missing data / join gaps)

USAGE:
    From repository root with conda env caos-extract:
        conda run -n caos-extract python scripts/export_top100_leave_timeline.py
        conda run -n caos-extract python scripts/export_top100_leave_timeline.py \
            --excel inputs/excel/100_biggest_CAOs.xlsx --limit 100

    CSV delimiter is ';' (project convention). leave_information is stored as a JSON string in CSV.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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
    sort_key_per_line,
)


DEFAULT_OUT_DIR = PROJECT_ROOT / "outputs/llm_extracted/excel_test"
PRE_2015_CUTOFF = pd.Timestamp("2015-01-01")


def row_to_public_dict(row: Dict[str, Any], include_cao: bool) -> Dict[str, Any]:
    """
    Build JSON/CSV-safe dict without internal sort fields.

    Args:
        row: Internal row with _ing_dt and extraction_data.
        include_cao: Whether to include cao_number (all-in-one exports).

    Returns:
        Dict for serialization.
    """
    leave_data = row.get("extraction_data", {}).get("leave_information", [])
    data: Dict[str, Any] = {
        "source_file_name": row["source_file_name"],
        "ingangsdatum": row["ingangsdatum"],
        "datum_kennisgeving": row["datum_kennisgeving"],
        "leave_information": leave_data if leave_data is not None else [],
    }
    if include_cao:
        data["cao_number"] = row["cao_number"]
    return data


def write_csv(path: Path, rows: List[Dict[str, Any]], include_cao: bool) -> None:
    """
    Write semicolon CSV with leave_information JSON-encoded.

    Args:
        path: Output file path.
        rows: Records with public fields plus _ing_dt (ignored for values).
        include_cao: Include cao_number column.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "cao_number",
        "source_file_name",
        "ingangsdatum",
        "datum_kennisgeving",
        "leave_information",
    ]
    if not include_cao:
        fieldnames = [name for name in fieldnames if name != "cao_number"]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for row in rows:
            payload = row_to_public_dict(row, include_cao=include_cao)
            payload["leave_information"] = json.dumps(
                payload["leave_information"], ensure_ascii=False
            )
            writer.writerow(payload)


def write_jsonl(path: Path, rows: List[Dict[str, Any]], include_cao: bool) -> None:
    """
    Write UTF-8 JSONL; one canonical JSON object per line.

    Args:
        path: Output file.
        rows: Row dicts.
        include_cao: Include cao_number in each line.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            payload = row_to_public_dict(row, include_cao=include_cao)
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def write_per_cao_jsonl(out_dir: Path, rows: List[Dict[str, Any]]) -> None:
    """
    Write per_cao/{cao}_Leave_Timeline.jsonl sorted by timeline; lines omit cao_number.

    Args:
        out_dir: excel_test root (per_cao created beneath).
        rows: All rows (with _ing_dt).
    """
    per_dir = out_dir / "per_cao"
    per_dir.mkdir(parents=True, exist_ok=True)
    by_cao: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_cao[row["cao_number"]].append(row)
    for cao, grouped_rows in sorted(by_cao.items(), key=lambda item: int(item[0])):
        ordered_rows = sorted(grouped_rows, key=sort_key_per_line)
        out_path = per_dir / f"{cao}_Leave_Timeline.jsonl"
        with open(out_path, "w", encoding="utf-8") as handle:
            for row in ordered_rows:
                payload = row_to_public_dict(row, include_cao=False)
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(
        description="Export p3 leave_information timelines for top CAOs from Excel."
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


def main() -> None:
    """
    Load inputs, build rows, and write exports.

    Returns:
        None.
    """
    args = parse_args()
    limit = None if args.limit == 0 else args.limit

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
    write_csv(out_dir / "top100_leave_all.csv", rows, include_cao=True)
    write_jsonl(out_dir / "top100_leave_all.jsonl", rows, include_cao=True)

    pre_2015_rows = [
        row for row in rows if pd.notna(row["_ing_dt"]) and row["_ing_dt"] < PRE_2015_CUTOFF
    ]
    pre_2015_rows.sort(key=sort_key_combined)
    write_csv(out_dir / "top100_leave_pre2015.csv", pre_2015_rows, include_cao=True)
    write_jsonl(out_dir / "top100_leave_pre2015.jsonl", pre_2015_rows, include_cao=True)

    write_per_cao_jsonl(out_dir, rows)

    report_path = out_dir / "export_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report.lines()), encoding="utf-8")

    print(f"Wrote exports under {out_dir}")
    print(f"  Rows (all): {len(rows)}; pre-2015: {len(pre_2015_rows)}")
    print(f"  Report: {report_path}")


if __name__ == "__main__":
    main()
