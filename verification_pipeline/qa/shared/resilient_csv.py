"""Tolerant CSV reader/writer.

The qa_leave/ input CSV uses `;` (semicolon) as delimiter and contains
multi-line free-text cells. Quoting may be inconsistent across source rows;
this reader skips malformed lines (logged via stderr) rather than raising.

Generic version (non-leave-aware). Used by every other shared module that
touches CSV data.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Iterable, Any, Optional

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore


def read_csv(path: Path,
             delimiter: str = ";",
             encoding: str = "utf-8",
             skip_malformed: bool = True):
    """Read a CSV file tolerantly; return a pandas DataFrame.

    Args:
        path: file to read
        delimiter: default ';' (the convention for qa_leave/ inputs)
        encoding: default utf-8
        skip_malformed: if True, lines that can't be parsed are skipped with
            a warning on stderr. If False, raises.

    Returns:
        pd.DataFrame
    """
    if pd is None:
        raise RuntimeError("pandas is required for read_csv")
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        df = pd.read_csv(
            path,
            sep=delimiter,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding=encoding,
            on_bad_lines="skip" if skip_malformed else "error",
            engine="python",
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            path,
            sep=delimiter,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="latin-1",
            on_bad_lines="skip" if skip_malformed else "error",
            engine="python",
        )
    return df


def write_csv(rows: Iterable[dict[str, Any]],
              path: Path,
              fieldnames: Optional[list[str]] = None,
              delimiter: str = ";",
              encoding: str = "utf-8") -> int:
    """Write an iterable of dict rows to CSV with consistent quoting.

    Args:
        rows: iterable of {column: value} dicts
        path: output file (parent dir created if missing)
        fieldnames: column order; if None, derived from first row
        delimiter: default ';'
        encoding: default utf-8

    Returns: number of rows written
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = list(rows)
    if not rows:
        # Write an empty file with just the header if fieldnames provided
        if fieldnames:
            with path.open("w", encoding=encoding, newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames,
                                        delimiter=delimiter,
                                        quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
        return 0

    if fieldnames is None:
        fieldnames = list(rows[0].keys())

    with path.open("w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames,
                                delimiter=delimiter,
                                quoting=csv.QUOTE_MINIMAL,
                                extrasaction="ignore")
        writer.writeheader()
        count = 0
        for row in rows:
            # Coerce non-string scalars to strings; None -> empty
            clean = {k: ("" if v is None else str(v)) for k, v in row.items()}
            writer.writerow(clean)
            count += 1
    return count


def read_csv_iter(path: Path,
                  delimiter: str = ";",
                  encoding: str = "utf-8"):
    """Generator that yields one dict per row. Lighter than read_csv when
    you don't need the whole DataFrame in memory."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            yield row
