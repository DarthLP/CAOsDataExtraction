"""
Run the lifecycle QA pipeline (derive → join → mismatches → spot-check).

Provides two presets matching ``derive_lifecycle.py`` content modes:

- **full**: canonical run (head/tail content + filename); use after markdown is local.
- **filename_only**: fast run when files are iCloud placeholders.

Usage::

    python scripts/qa/lifecycle/run_lifecycle_pipeline.py full
    python scripts/qa/lifecycle/run_lifecycle_pipeline.py filename_only
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_PY = sys.executable


def _run(script: str, *extra: str) -> None:
    """Run a lifecycle script under this package; exit on failure."""
    path = _ROOT / "scripts" / "qa" / "lifecycle" / script
    cmd = [_PY, str(path), *extra]
    print(f"\n>> {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=_ROOT, check=True)


def main() -> None:
    """
    Execute derive, join, report_mismatches, and spot_check for the chosen preset.

    Parameters:
        CLI: ``full`` or ``filename_only`` (positional).
    """
    parser = argparse.ArgumentParser(description="Run lifecycle QA pipeline end-to-end.")
    parser.add_argument(
        "preset",
        choices=("full", "filename_only"),
        help="full: content+filename (canonical). filename_only: skip read when filename matches.",
    )
    args = parser.parse_args()

    if args.preset == "full":
        derive_args = ["--content-mode", "full"]
        lifecycle_csv = _ROOT / "outputs" / "qa" / "lifecycle" / "lifecycle_results.csv"
    else:
        derive_args = ["--content-mode", "filename_only"]
        lifecycle_csv = (
            _ROOT / "outputs" / "qa" / "lifecycle" / "lifecycle_results_filename_only.csv"
        )

    _run("derive_lifecycle.py", *derive_args)
    _run("join_lifecycle.py", "--lifecycle-csv", str(lifecycle_csv))
    if args.preset == "full":
        _run("report_mismatches.py")
    else:
        print("\n(Skipping report_mismatches.py for filename_only preset.)", flush=True)
    _run("spot_check.py", "--lifecycle-csv", str(lifecycle_csv))

    print(f"\nDone. Lifecycle CSV: {lifecycle_csv}", flush=True)


if __name__ == "__main__":
    main()
