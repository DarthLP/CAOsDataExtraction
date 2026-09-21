"""
Common paths, utilities, and LaTeX fragment emitters for the CAO Descriptive Summary report.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import matplotlib.pyplot as plt

# Pinned repository roots
ANALYSIS_DIR = Path(__file__).resolve().parent.parent
CAOS_REPO_ROOT = Path("/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction")
try:
    from repo_paths import EXTRACTION_ROOT, VERIFICATION_ROOT
    CAOS_REPO_ROOT = EXTRACTION_ROOT
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "verification_pipeline"))
    from repo_paths import EXTRACTION_ROOT, VERIFICATION_ROOT
    CAOS_REPO_ROOT = EXTRACTION_ROOT

# REPO_ROOT = the verification_pipeline root (where qa/ and indices/ live), NOT the
# CAOsDataExtraction repo root. Reports/Analysis was moved to CAOsDataExtraction/Reports/
# on 2026-09-16, so this can no longer be derived as ANALYSIS_DIR.parent.parent (which
# now lands on CAOsDataExtraction/ instead) — pinned via repo_paths.py instead.
REPO_ROOT = VERIFICATION_ROOT

# Output directories
FIGURES_DIR = ANALYSIS_DIR / "figures"
TABLES_DIR = ANALYSIS_DIR / "tables"
SCRIPTS_DIR = ANALYSIS_DIR / "scripts"
MACROS_FILE = TABLES_DIR / "macros.tex"

# Ensure subdirectories exist
for d in [
    FIGURES_DIR,
    FIGURES_DIR / "boolean" / "latest_cao_view",
    FIGURES_DIR / "boolean" / "new_cao_yearly",
    FIGURES_DIR / "numeric",
    FIGURES_DIR / "salary",
    FIGURES_DIR / "indices",
    TABLES_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)

# Add CAOsDataExtraction to sys.path so its excel_analysis helper modules can be imported
if str(CAOS_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(CAOS_REPO_ROOT))

# Shared Matplotlib style configuration
def setup_matplotlib():
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.size": 10,
        "axes.labelsize": 11,
        "axes.titlesize": 12,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "figure.titlesize": 13,
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "axes.grid": True,
        "grid.alpha": 0.3,
        "grid.linestyle": "--",
    })

import re

# Macro management
_MACROS: Dict[str, str] = {}
_MACRO_RE = re.compile(r"^\\(?:providecommand|newcommand|def)\{\\([A-Za-z]+)\}\{(.*)\}$")

def load_macros(filepath: Path = MACROS_FILE):
    """Load existing macros from file if it exists."""
    global _MACROS
    if not filepath.exists():
        return
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            m = _MACRO_RE.match(line.strip())
            if m:
                name, val = m.group(1), m.group(2)
                if name not in _MACROS:
                    _MACROS[name] = val

def set_macro(name: str, value: Any):
    """Register a macro name and value (will be written to macros.tex)."""
    load_macros()
    clean_name = "".join(c for c in name if c.isalpha())
    _MACROS[clean_name] = str(value)

def save_macros(filepath: Path = MACROS_FILE):
    """Write all registered macros to filepath using \\providecommand."""
    load_macros(filepath)
    lines = [
        "% Generated LaTeX macros for CAO Descriptive Summary",
        "% Auto-generated - do not edit manually",
        "",
    ]
    for name, value in sorted(_MACROS.items()):
        val_str = str(value)
        # Ensure % is escaped as \% in LaTeX
        val_str = re.sub(r"(?<!\\)%", r"\\%", val_str)
        lines.append(f"\\providecommand{{\\{name}}}{{{val_str}}}")
    lines.append("")
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def emit_macro(name: str, value: Any, filepath: Path = MACROS_FILE):
    """Set a macro and immediately write all macros to file."""
    set_macro(name, value)
    save_macros(filepath)

def emit_table(
    df: pd.DataFrame,
    filepath: Path,
    col_align: Optional[str] = None,
    headers: Optional[List[str]] = None,
    caption: Optional[str] = None,
    label: Optional[str] = None,
    escape: bool = True,
):
    """Emit a booktabs tabular fragment suitable for \\input in LaTeX."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    n_cols = df.shape[1]
    if col_align is None:
        col_align = "l" + "r" * (n_cols - 1)
    
    header_row = headers if headers is not None else [str(c) for c in df.columns]
    header_row = [re.sub(r"(?<!\\)&", r"\&", h) for h in header_row]
    if escape:
        header_row = [h.replace("_", "\\_").replace("%", "\\%") for h in header_row]
    
    lines = []
    lines.append(f"\\begin{{tabular}}{{{col_align}}}")
    lines.append("    \\hline\\hline")
    lines.append("    " + " & ".join(header_row) + " \\\\")
    lines.append("    \\hline")
    
    for _, row in df.iterrows():
        row_vals = []
        for v in row:
            if pd.isna(v):
                row_vals.append("")
            elif isinstance(v, (int, float)):
                if isinstance(v, int) or (isinstance(v, float) and v.is_integer()):
                    row_vals.append(f"{int(v):,}")
                else:
                    row_vals.append(f"{v:.2f}")
            else:
                s = str(v)
                # Safely escape & and _ if not already escaped
                s = re.sub(r"(?<!\\)&", r"\&", s)
                # If not inside math ($...$), escape _
                if "$" not in s:
                    s = re.sub(r"(?<!\\)_", r"\_", s)
                if escape:
                    s = re.sub(r"(?<!\\)%", r"\%", s)
                row_vals.append(s)
        lines.append("    " + " & ".join(row_vals) + " \\\\")
    
    lines.append("    \\hline\\hline")
    lines.append("\\end{tabular}")
    lines.append("")
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
