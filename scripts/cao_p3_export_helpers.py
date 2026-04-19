#!/usr/bin/env python3
"""
Shared helpers for p3-based CAO exports.

PURPOSE:
    Centralize Excel CAO selection, metadata loading, p3 new_flow row building,
    date parsing, and timeline sort logic so multiple export scripts remain
    behaviorally consistent.

USAGE:
    from scripts.cao_p3_export_helpers import (
        DEFAULT_EXCEL_DIR, DEFAULT_NEW_FLOW, INFO_MAIN, INFO_EXTRA,
        ExportReport, discover_excel, read_ordered_cao_list,
        load_extracted_cao_info, build_p3_rows, sort_key_combined, sort_key_per_line
    )
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.excel_analysis.analysis_utils import parse_cao_date_series  # noqa: E402


DEFAULT_NEW_FLOW = PROJECT_ROOT / "outputs/llm_extracted/new_flow"
DEFAULT_EXCEL_DIR = PROJECT_ROOT / "inputs/excel"
INFO_MAIN = PROJECT_ROOT / "inputs/pdfs/input_pdfs/extracted_cao_info.csv"
INFO_EXTRA = PROJECT_ROOT / "inputs/pdfs/input_pdfs_extra/extracted_cao_info_extra.csv"


@dataclass
class ExportReport:
    """
    Collects data quality issues for text report outputs.

    Attributes:
        cao_no_new_flow: CAO ids with no new_flow folder.
        cao_no_extract_json: CAO ids with folder but no *_extract.json.
        json_no_metadata_match: Extract files not matched to extracted_cao_info.
        missing_ingangsdatum: Rows with missing/unparseable ingangsdatum.
    """

    cao_no_new_flow: List[str] = field(default_factory=list)
    cao_no_extract_json: List[str] = field(default_factory=list)
    json_no_metadata_match: List[str] = field(default_factory=list)
    missing_ingangsdatum: List[str] = field(default_factory=list)

    def lines(self) -> List[str]:
        """
        Build a plain-text report payload.

        Returns:
            List of lines suitable for joining with newline.
        """
        return [
            "=== CAOs in Excel list with no new_flow folder ===",
            *self.cao_no_new_flow,
            "",
            "=== CAOs with new_flow but no *_extract.json ===",
            *self.cao_no_extract_json,
            "",
            "=== Extract JSON files with no matching extracted_cao_info row (cao + stem) ===",
            *self.json_no_metadata_match,
            "",
            "=== Rows with missing or unparseable ingangsdatum (after join / fallback) ===",
            *self.missing_ingangsdatum,
            "",
        ]


def normalize_cao_code(value: Any) -> Optional[str]:
    """
    Normalize a spreadsheet cell to numeric CAO id string.

    Args:
        value: Raw value from Excel.

    Returns:
        String CAO number or None if unparsable.
    """
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        fval = float(value)
        if fval != int(fval):
            return None
        return str(int(fval))
    sval = str(value).strip()
    if sval.isdigit():
        return sval
    try:
        fval = float(sval)
        if fval == int(fval):
            return str(int(fval))
    except ValueError:
        return None
    return None


def discover_excel(default_dir: Path) -> Path:
    """
    Select workbook under a directory when unambiguous.

    Args:
        default_dir: Directory to search for *.xlsx.

    Returns:
        Chosen workbook path.

    Raises:
        FileNotFoundError: No workbook exists.
        ValueError: Multiple workbooks exist and explicit --excel is required.
    """
    paths = sorted(default_dir.glob("*.xlsx"))
    if not paths:
        raise FileNotFoundError(f"No .xlsx files under {default_dir}")
    if len(paths) > 1:
        names = ", ".join(p.name for p in paths)
        raise ValueError(
            f"Multiple Excel files under {default_dir}: {names}. Pass --excel explicitly."
        )
    return paths[0]


def read_ordered_cao_list(excel_path: Path, limit: Optional[int]) -> List[str]:
    """
    Read ordered unique CAO ids from Excel.

    Args:
        excel_path: Workbook path.
        limit: Maximum unique CAOs (None means all).

    Returns:
        Ordered CAO id list.
    """
    df = pd.read_excel(excel_path)
    column = None
    for name in ("code", "cao_number", "CAO"):
        if name in df.columns:
            column = name
            break
    if column is None:
        raise ValueError(
            f"No CAO column found (code, cao_number, CAO). Columns: {list(df.columns)}"
        )

    result: List[str] = []
    seen = set()
    for raw in df[column]:
        cao = normalize_cao_code(raw)
        if cao is None or cao in seen:
            continue
        seen.add(cao)
        result.append(cao)
        if limit is not None and len(result) >= limit:
            break
    return result


def load_extracted_cao_info(main_path: Path, extra_path: Path) -> pd.DataFrame:
    """
    Load and deduplicate extracted_cao_info metadata tables.

    Args:
        main_path: Primary metadata CSV.
        extra_path: Optional secondary metadata CSV.

    Returns:
        Combined dataframe or empty dataframe if neither file exists.
    """
    frames: List[pd.DataFrame] = []
    for path in (main_path, extra_path):
        if path.exists():
            frames.append(pd.read_csv(path, sep=";", encoding="utf-8"))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if {"cao_number", "pdf_name", "id"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["cao_number", "pdf_name", "id"])
    df["cao_number"] = df["cao_number"].astype(str).str.strip()
    return df


def markdown_stem_from_extract_json(path: Path) -> str:
    """
    Get markdown/PDF stem from p3 output filename.

    Args:
        path: JSON file path, typically *_extract.json.

    Returns:
        Stem without trailing _extract.
    """
    stem = path.stem
    if stem.endswith("_extract"):
        return stem[: -len("_extract")]
    return stem


def parse_one_date(raw: Any) -> pd.Timestamp:
    """
    Parse one metadata date string using CAO day-first semantics.

    Args:
        raw: Raw date-like value.

    Returns:
        Parsed pandas Timestamp or NaT.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return pd.NaT
    sval = str(raw).strip()
    if not sval or sval.lower() == "nan":
        return pd.NaT
    return parse_cao_date_series(pd.Series([sval]), dayfirst=True).iloc[0]


def find_info_row(info_df: pd.DataFrame, cao_number: str, md_stem: str) -> Optional[pd.Series]:
    """
    Locate extracted_cao_info row by CAO id and PDF stem.

    Args:
        info_df: Metadata dataframe.
        cao_number: CAO id string.
        md_stem: Stem derived from *_extract.json filename.

    Returns:
        Matching row or None.
    """
    if info_df.empty or "pdf_name" not in info_df.columns:
        return None
    subset = info_df[info_df["cao_number"] == cao_number]
    for _, row in subset.iterrows():
        pdf_name = row.get("pdf_name", "")
        if pd.isna(pdf_name):
            continue
        if Path(str(pdf_name)).stem == md_stem:
            return row
    return None


def build_p3_rows(
    cao_list: List[str],
    info_df: pd.DataFrame,
    new_flow_root: Path,
    report: ExportReport,
) -> List[Dict[str, Any]]:
    """
    Build joined rows from p3 extraction + metadata.

    Args:
        cao_list: Ordered CAO ids selected from Excel.
        info_df: extracted_cao_info dataframe.
        new_flow_root: Root of p3 new_flow output.
        report: Mutable report accumulator.

    Returns:
        Rows containing metadata, parsed dates, and extraction payload:
        {
            "cao_number": str,
            "source_file_name": str,
            "ingangsdatum": str,              # ISO or ""
            "datum_kennisgeving": str,        # ISO or ""
            "extraction_data": dict,          # parsed *_extract.json object
            "_ing_dt": Timestamp | NaT
        }
    """
    rows: List[Dict[str, Any]] = []
    for cao in cao_list:
        cao_dir = new_flow_root / cao
        if not cao_dir.is_dir():
            report.cao_no_new_flow.append(cao)
            continue

        json_files = sorted(cao_dir.glob("*_extract.json"))
        if not json_files:
            report.cao_no_extract_json.append(cao)
            continue

        for jpath in json_files:
            md_stem = markdown_stem_from_extract_json(jpath)
            try:
                with open(jpath, encoding="utf-8") as handle:
                    extraction_data = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                report.json_no_metadata_match.append(
                    f"{cao} {jpath.name} (read error: {exc})"
                )
                continue

            match = find_info_row(info_df, cao, md_stem)
            if match is None:
                report.json_no_metadata_match.append(f"{cao} {jpath.name} stem={md_stem}")
                source_file_name = f"{md_stem}.pdf"
                ing_raw = ""
                kenn_raw = ""
            else:
                source_file_name = (
                    str(match.get("pdf_name", "") or "").strip() or f"{md_stem}.pdf"
                )
                ing_raw = match.get("ingangsdatum", "")
                kenn_raw = match.get("datum_kennisgeving", "")

            ing_dt = parse_one_date(ing_raw)
            kenn_dt = parse_one_date(kenn_raw)
            ing_iso = ing_dt.strftime("%Y-%m-%d") if pd.notna(ing_dt) else ""
            kenn_iso = kenn_dt.strftime("%Y-%m-%d") if pd.notna(kenn_dt) else ""

            if ing_iso == "":
                report.missing_ingangsdatum.append(
                    f"{cao} {jpath.name} source_file_name={source_file_name}"
                )

            rows.append(
                {
                    "cao_number": cao,
                    "source_file_name": source_file_name,
                    "ingangsdatum": ing_iso,
                    "datum_kennisgeving": kenn_iso,
                    "extraction_data": extraction_data,
                    "_ing_dt": ing_dt,
                }
            )
    return rows


def sort_key_combined(row: Dict[str, Any]) -> Tuple[int, pd.Timestamp, str]:
    """
    Sort key for combined outputs.

    Args:
        row: Joined row dictionary.

    Returns:
        Tuple (cao_number numeric, ingangsdatum, source_file_name).
    """
    cao_number = int(row["cao_number"])
    ing_dt = row["_ing_dt"]
    if pd.isna(ing_dt):
        ing_dt = pd.Timestamp.max
    return (cao_number, ing_dt, row["source_file_name"])


def sort_key_per_line(row: Dict[str, Any]) -> Tuple[pd.Timestamp, str]:
    """
    Sort key for per-CAO timeline output.

    Args:
        row: Joined row dictionary.

    Returns:
        Tuple (ingangsdatum, source_file_name) with missing dates last.
    """
    ing_dt = row["_ing_dt"]
    if pd.isna(ing_dt):
        ing_dt = pd.Timestamp.max
    return (ing_dt, row["source_file_name"])
