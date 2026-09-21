#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANALYSIS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== [1/5] Running 01_general.py ==="
python3 "$SCRIPT_DIR/01_general.py"

echo "=== [2/5] Running 02_non_salary.py ==="
python3 "$SCRIPT_DIR/02_non_salary.py"

echo "=== [3/5] Running 03_salary.py (regenerates all salary figures from parser v2, ~15 min) ==="
python3 "$SCRIPT_DIR/03_salary.py"

echo "=== [4/5] Running 04_indices.py ==="
python3 "$SCRIPT_DIR/04_indices.py"

echo "=== [5/5] Compiling LaTeX Document with xelatex ==="
cd "$ANALYSIS_DIR"
xelatex -interaction=nonstopmode main.tex
xelatex -interaction=nonstopmode main.tex

echo "=== Build Complete! ==="
echo "PDF: $ANALYSIS_DIR/main.pdf"

