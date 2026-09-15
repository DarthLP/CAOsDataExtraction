"""
Walk parsed CAO markdown files and write per-file lifecycle classification CSV.

This script scans ``outputs/parsed_pdfs/parsed_pdfs_markdown/<cao_folder>/*.md``,
runs ``classifier.classify_file`` on each file, and writes a lifecycle results CSV.

Two content modes (see ``--content-mode``):

- **full** (default): read first/last pages of each markdown and match content cues
  (original plan). Requires files to be present locally; slow if macOS/iCloud must
  download each file on first open.
- **filename_only**: when any filename token matches, skip opening the file; otherwise
  still read content. Fast on cloud-synced trees; misses cover-page-only signals.

Usage (from repository root, conda env ``caos-extract``)::

    # Full run (canonical for join / mismatches)
    python scripts/qa/lifecycle/derive_lifecycle.py --content-mode full

    # Quick run without downloading every markdown from iCloud
    python scripts/qa/lifecycle/derive_lifecycle.py --content-mode filename_only
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Repository root on sys.path (allows ``import scripts...`` when run as a file).
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from scripts.qa.lifecycle.classifier import ContentMode, classify_file


def _project_root() -> Path:
    """Return absolute path to CAOsDataExtraction repository root."""
    return _ROOT


def _default_output_name(content_mode: ContentMode) -> str:
    """Return default lifecycle results CSV basename for a content mode."""
    if content_mode == "filename_only":
        return "lifecycle_results_filename_only.csv"
    return "lifecycle_results.csv"


def _sample_extracted_file_names(project_root: Path, n: int = 5) -> None:
    """
    Print sample ``file_name`` values from the non-salary extract for join QA.

    Parameters:
        project_root: Repository root path.
        n: Number of sample rows to print (default 5).
    """
    csv_path = project_root / "outputs" / "excel" / "new_results" / "extracted_data_non_salary.csv"
    if not csv_path.is_file():
        print(f"(No sample) missing extract CSV: {csv_path}")
        return
    try:
        df = pd.read_csv(csv_path, sep=";", nrows=n, usecols=["cao_number", "file_name"])
    except (ValueError, KeyError, OSError) as e:
        print(f"(No sample) could not read extract CSV: {e}")
        return
    print("Sample (cao_number, file_name) from extracted_data_non_salary.csv:")
    for _, row in df.iterrows():
        print(f"  {row['cao_number']!s} | {row['file_name']!s}")


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for derive_lifecycle."""
    parser = argparse.ArgumentParser(description="Derive per-file lifecycle labels from parsed markdown.")
    parser.add_argument(
        "--content-mode",
        choices=("full", "filename_only"),
        default="full",
        help="full: read head/tail of every file (plan default). "
        "filename_only: skip disk read when filename cues match.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: outputs/qa/lifecycle/<mode-specific name>).",
    )
    return parser.parse_args()


def main() -> None:
    """
    Classify every markdown file under parsed_pdfs_markdown and write lifecycle CSV.

    Side effects:
        Creates ``outputs/qa/lifecycle/`` if needed; writes the results CSV.
        Prints summary counts to stdout.
    """
    args = _parse_args()
    content_mode: ContentMode = args.content_mode

    project_root = _project_root()
    md_root = project_root / "outputs" / "parsed_pdfs" / "parsed_pdfs_markdown"
    out_dir = project_root / "outputs" / "qa" / "lifecycle"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = args.output or (out_dir / _default_output_name(content_mode))

    if not md_root.is_dir():
        print(f"ERROR: Markdown root not found: {md_root}", file=sys.stderr)
        sys.exit(1)

    print(f"Content mode: {content_mode}", flush=True)
    if content_mode == "full":
        print(
            "Tip: if this is very slow, markdown files may be iCloud placeholders. "
            "Download the folder in Finder first, or run --content-mode filename_only for a quick pass.",
            flush=True,
        )

    _sample_extracted_file_names(project_root)

    rows: list[dict[str, object]] = []
    empty_dirs = 0
    n_done = 0
    t0 = time.perf_counter()
    print("Classifying markdown files...", flush=True)
    for cao_folder in sorted(md_root.iterdir(), key=lambda p: p.name):
        if not cao_folder.is_dir():
            continue
        cao_number = cao_folder.name
        md_files = sorted(cao_folder.glob("*.md"))
        if not md_files:
            print(f"  (empty folder) {cao_number}", flush=True)
            empty_dirs += 1
            continue
        for md_path in md_files:
            file_name = md_path.stem
            result = classify_file(cao_number, md_path, content_mode=content_mode)
            rows.append(
                {
                    "cao_number": str(cao_number),
                    "file_name": file_name,
                    "lifecycle_stage": result["lifecycle_stage"],
                    "is_signed": result["is_signed"],
                    "is_filed_szw": result["is_filed_szw"],
                    "is_avv_declared": result["is_avv_declared"],
                    "lifecycle_evidence": result["lifecycle_evidence"],
                    "lifecycle_source": result["lifecycle_source"],
                }
            )
            n_done += 1
            if n_done % 500 == 0:
                elapsed = time.perf_counter() - t0
                rate = n_done / elapsed if elapsed > 0 else 0.0
                print(f"  ... {n_done} files ({rate:.0f}/s)", flush=True)

    elapsed = time.perf_counter() - t0
    print(
        f"Classified {n_done} files in {elapsed:.1f}s ({n_done / elapsed if elapsed else 0:.0f}/s)",
        flush=True,
    )
    if content_mode == "filename_only":
        fn_only = sum(1 for r in rows if r["lifecycle_source"] == "filename")
        print(f"  filename-only (no file read): {fn_only} / {n_done}", flush=True)

    df = pd.DataFrame(rows)
    df.to_csv(out_csv, sep=";", index=False)
    print()
    print(f"Markdown files classified: {len(df)}")
    if empty_dirs:
        print(f"Empty CAO folders skipped: {empty_dirs}")

    print("\n--- lifecycle_stage ---")
    print(df["lifecycle_stage"].value_counts(dropna=False).to_string())
    print("\n--- booleans (True counts) ---")
    for col in ("is_signed", "is_filed_szw", "is_avv_declared"):
        n_true = int(df[col].astype(bool).sum())
        print(f"  {col}: {n_true} / {len(df)}")

    full_mask = df["lifecycle_stage"].isin(
        ("definitive_signed", "definitive_unsigned", "consolidated_amendments")
    )
    share_fullish = float(full_mask.mean()) if len(df) else 0.0
    unspecified_share = float((df["lifecycle_stage"] == "unspecified").mean()) if len(df) else 0.0
    print("\n--- acceptance-style shares (all classified files) ---")
    print(
        f"  definitive_signed|definitive_unsigned|consolidated_amendments: {share_fullish:.1%}"
    )
    print(f"  unspecified: {unspecified_share:.1%}")
    print(f"\nWrote: {out_csv}")


if __name__ == "__main__":
    main()
