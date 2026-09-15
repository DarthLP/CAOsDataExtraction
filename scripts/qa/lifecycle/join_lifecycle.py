"""
Attach extract metadata to lifecycle results and write a slim QA CSV only.

Loads ``lifecycle_results.csv`` and joins ``id`` + ``general_document_type`` from
``extracted_data_non_salary.csv`` on ``(cao_number, file_name)``. Writes only
``lifecycle_summary.csv`` (no full copies of the wide extract files).

Usage (from repository root)::

    conda run -n caos-extract python scripts/qa/lifecycle/join_lifecycle.py
    conda run -n caos-extract python scripts/qa/lifecycle/join_lifecycle.py \\
        --lifecycle-csv outputs/qa/lifecycle/lifecycle_results_filename_only.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

LIFECYCLE_COLS = [
    "lifecycle_stage",
    "is_signed",
    "is_filed_szw",
    "is_avv_declared",
    "lifecycle_evidence",
    "lifecycle_source",
]

SUMMARY_COLS = [
    "id",
    "cao_number",
    "file_name",
    *LIFECYCLE_COLS,
    "general_document_type",
]


def _project_root() -> Path:
    """Return repository root path."""
    return _ROOT


def _coerce_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with string ``cao_number`` and stripped ``file_name``."""
    out = df.copy()
    out["cao_number"] = out["cao_number"].astype(str)
    out["file_name"] = out["file_name"].astype(str).str.strip()
    return out


def _validate_extract_coverage(
    lifecycle: pd.DataFrame,
    non_salary_path: Path,
    label: str = "non_salary extract",
) -> None:
    """
    Check that extract rows match lifecycle keys; exit if >5% extract rows lack lifecycle.

    Parameters:
        lifecycle: Per-markdown classification rows (unique on cao_number, file_name).
        non_salary_path: Path to ``extracted_data_non_salary.csv``.
        label: Log label for printed stats.
    """
    if not non_salary_path.is_file():
        print(f"WARNING: cannot validate join — missing {non_salary_path}")
        return
    ns = pd.read_csv(
        non_salary_path,
        sep=";",
        usecols=["cao_number", "file_name"],
    )
    ns = _coerce_keys(ns)
    n_in = len(ns)
    merged = ns.merge(
        lifecycle,
        on=["cao_number", "file_name"],
        how="left",
        validate="many_to_one",
    )
    matched = int(merged["lifecycle_stage"].notna().sum())
    n_unmatched = n_in - matched
    print(f"\n--- join check ({label}) ---")
    print(f"  extract rows: {n_in}")
    print(f"  matched to lifecycle: {matched}")
    print(f"  unmatched: {n_unmatched} ({(n_unmatched / n_in if n_in else 0):.2%})")
    if n_unmatched:
        sample = merged.loc[merged["lifecycle_stage"].isna(), ["cao_number", "file_name"]].head(5)
        print("  sample unmatched (cao_number, file_name):")
        for _, r in sample.iterrows():
            print(f"    {r['cao_number']} | {r['file_name']}")
    if n_in and n_unmatched / n_in > 0.05:
        print(
            f"ERROR: unmatched fraction {n_unmatched / n_in:.2%} exceeds 5% — fix key normalization.",
            file=sys.stderr,
        )
        sys.exit(1)

    n_lc = len(lifecycle)
    in_extract = lifecycle.merge(ns.drop_duplicates(["cao_number", "file_name"]), on=["cao_number", "file_name"], how="inner")
    n_lc_only = n_lc - len(in_extract)
    if n_lc_only:
        print(f"  lifecycle rows with no non_salary extract row: {n_lc_only} (markdown-only files OK)")


def _build_lifecycle_summary(
    lifecycle: pd.DataFrame,
    non_salary_path: Path,
    out_path: Path,
) -> None:
    """
    Write slim QA CSV: id, keys, lifecycle columns, and ``general_document_type``.

    Parameters:
        lifecycle: Lifecycle classification rows (one per markdown file).
        non_salary_path: Path to ``extracted_data_non_salary.csv``.
        out_path: Output path ``lifecycle_summary.csv``.
    """
    if not non_salary_path.is_file():
        print(f"ERROR: missing {non_salary_path}", file=sys.stderr)
        sys.exit(1)
    ns = pd.read_csv(
        non_salary_path,
        sep=";",
        usecols=["cao_number", "id", "file_name", "general_document_type"],
    )
    ns = _coerce_keys(ns)
    dupes = ns.duplicated(subset=["cao_number", "file_name"], keep=False).sum()
    if dupes:
        print(
            f"WARNING: non_salary extract has {dupes} duplicate (cao_number, file_name) rows; "
            "keeping first occurrence for lifecycle_summary."
        )
    ns = ns.drop_duplicates(subset=["cao_number", "file_name"], keep="first")
    summary = lifecycle.merge(ns, on=["cao_number", "file_name"], how="left", validate="one_to_one")
    summary = summary[SUMMARY_COLS]
    summary.to_csv(out_path, sep=";", index=False)
    print(f"Wrote lifecycle_summary: {out_path} ({len(summary)} rows)")


def _print_full_cao_lifecycle_health(lifecycle: pd.DataFrame, non_salary_path: Path) -> None:
    """
    Print share of definitive-like stages among rows tagged as full CAO in the extract.

    Uses a left-join of lifecycle onto deduped non-salary ``general_document_type``.
    """
    if not non_salary_path.is_file():
        return
    ns = pd.read_csv(
        non_salary_path,
        sep=";",
        usecols=["cao_number", "file_name", "general_document_type"],
    )
    ns["cao_number"] = ns["cao_number"].astype(str)
    ns["file_name"] = ns["file_name"].astype(str).str.strip()
    ns = ns.drop_duplicates(subset=["cao_number", "file_name"], keep="first")
    m = lifecycle.merge(ns, on=["cao_number", "file_name"], how="left", validate="one_to_one")
    full = m["general_document_type"].isin(["full_cao_original", "full_cao_update"])
    sub = m.loc[full, "lifecycle_stage"]
    if len(sub) == 0:
        print("\n--- full-CAO lifecycle health (no full_cao rows in extract) ---")
        return
    good = sub.isin(["definitive_signed", "definitive_unsigned", "consolidated_amendments"])
    un = (sub == "unspecified").mean()
    print("\n--- full-CAO rows only (extract document type) ---")
    print(
        f"  definitive_signed|definitive_unsigned|consolidated_amendments: {float(good.mean()):.1%} "
        f"({int(good.sum())}/{len(sub)})"
    )
    print(f"  unspecified among full-CAO: {float(un):.1%}")


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for join_lifecycle."""
    parser = argparse.ArgumentParser(
        description="Build slim lifecycle_summary.csv (no wide extract copies)."
    )
    parser.add_argument(
        "--lifecycle-csv",
        type=Path,
        default=None,
        help="Lifecycle results CSV (default: outputs/qa/lifecycle/lifecycle_results.csv).",
    )
    return parser.parse_args()


def main() -> None:
    """
    Build ``lifecycle_summary.csv`` and validate extract key coverage.

    Does not write ``extracted_data_*_with_lifecycle.csv`` (wide extracts stay in
    ``outputs/excel/new_results/`` only).
    """
    args = _parse_args()
    root = _project_root()
    qa_dir = root / "outputs" / "qa" / "lifecycle"
    lifecycle_path = args.lifecycle_csv or (qa_dir / "lifecycle_results.csv")
    non_salary_in = root / "outputs" / "excel" / "new_results" / "extracted_data_non_salary.csv"

    if not lifecycle_path.is_file():
        print(f"ERROR: run derive_lifecycle.py first — missing {lifecycle_path}", file=sys.stderr)
        sys.exit(1)

    lifecycle = pd.read_csv(lifecycle_path, sep=";")
    lifecycle = _coerce_keys(lifecycle)

    qa_dir.mkdir(parents=True, exist_ok=True)

    _validate_extract_coverage(lifecycle, non_salary_in)
    _print_full_cao_lifecycle_health(lifecycle, non_salary_in)
    _build_lifecycle_summary(lifecycle, non_salary_in, qa_dir / "lifecycle_summary.csv")


if __name__ == "__main__":
    main()
