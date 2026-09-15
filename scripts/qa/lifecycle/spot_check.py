"""
Sample classified markdown files for manual lifecycle QA.

Selects 20 rows from ``lifecycle_results.csv`` with ``random.seed(42)``, ensuring
at least one row per ``lifecycle_stage`` value when possible, then writes
``spot_check_sample.csv`` including a short raw-markdown prefix for context.

Usage::

    conda run -n caos-extract python scripts/qa/lifecycle/spot_check.py
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd


def _project_root() -> Path:
    """Return repository root path."""
    return _ROOT


def _markdown_prefix(md_path: Path, n: int = 200) -> str:
    """Return first ``n`` characters of the markdown file (UTF-8, replace errors)."""
    if not md_path.is_file():
        return ""
    try:
        text = md_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[:n].replace("\r\n", "\n")


def _pick_sample_rows(df: pd.DataFrame, k: int = 20) -> pd.DataFrame:
    """
    Pick up to ``k`` rows, oversampling so each lifecycle_stage appears once if possible.

    Parameters:
        df: Full ``lifecycle_results`` frame.
        k: Target sample size (default 20).

    Returns:
        Subset dataframe (at most ``k`` rows, fewer if corpus is smaller).
    """
    random.seed(42)
    if len(df) <= k:
        return df.copy()

    picked_idx: list[int] = []
    seen: set[int] = set()
    stages = sorted(df["lifecycle_stage"].dropna().unique())
    for st in stages:
        if len(picked_idx) >= k:
            break
        sub = df.index[df["lifecycle_stage"] == st].tolist()
        if not sub:
            continue
        i = random.choice(sub)
        if i not in seen:
            picked_idx.append(i)
            seen.add(i)

    remaining = [i for i in df.index if i not in seen]
    random.shuffle(remaining)
    for i in remaining:
        if len(picked_idx) >= k:
            break
        picked_idx.append(i)

    picked_idx = picked_idx[:k]
    return df.loc[picked_idx].copy()


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for spot_check."""
    parser = argparse.ArgumentParser(description="Sample lifecycle rows for manual QA.")
    parser.add_argument(
        "--lifecycle-csv",
        type=Path,
        default=None,
        help="Input lifecycle results CSV (default: outputs/qa/lifecycle/lifecycle_results.csv).",
    )
    return parser.parse_args()


def main() -> None:
    """
    Build ``spot_check_sample.csv`` and print a readable preview to stdout.

    Side effects:
        Writes ``outputs/qa/lifecycle/spot_check_sample.csv``.
    """
    args = _parse_args()
    root = _project_root()
    qa = root / "outputs" / "qa" / "lifecycle"
    lr = args.lifecycle_csv or (qa / "lifecycle_results.csv")
    md_root = root / "outputs" / "parsed_pdfs" / "parsed_pdfs_markdown"
    out_csv = qa / "spot_check_sample.csv"

    if not lr.is_file():
        print(f"ERROR: missing {lr} — run derive_lifecycle.py first.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(lr, sep=";")
    sample = _pick_sample_rows(df, 20)

    prefixes: list[str] = []
    for _, row in sample.iterrows():
        cao = str(row["cao_number"])
        fn = str(row["file_name"])
        md_path = md_root / cao / f"{fn}.md"
        prefixes.append(_markdown_prefix(md_path))
    sample = sample.copy()
    sample["markdown_prefix_200"] = prefixes

    qa.mkdir(parents=True, exist_ok=True)
    sample.to_csv(out_csv, sep=";", index=False)

    print(f"Wrote {len(sample)} rows to {out_csv}\n")
    print("--- preview ---")
    for _, row in sample.iterrows():
        print("-" * 72)
        print(f"cao={row['cao_number']} file={row['file_name']}")
        print(f"  stage={row['lifecycle_stage']}  source={row['lifecycle_source']}")
        print(f"  signed={row['is_signed']}  szw={row['is_filed_szw']}  avv={row['is_avv_declared']}")
        ev = str(row["lifecycle_evidence"])
        if len(ev) > 240:
            ev = ev[:239] + "…"
        print(f"  evidence: {ev}")


if __name__ == "__main__":
    main()
