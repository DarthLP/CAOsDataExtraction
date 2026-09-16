# Pipeline Replication Guide & Methodology Documentation

This directory contains the unified replication guide for the entire CAOs data extraction, deterministic salary parsing, multi-layer QA/correction, and derived indices pipeline.

## Structure

- `analysis/`: Fact base and audit logs supporting the updated guide.
  - `01-old-report-audit.md`: Claim-by-claim audit of the November 2025 report text (KEEP / FIX / STALE).
  - `02-new-material-map.md`: Mapping of codebase documentation to new guide sections.
  - `03-facts-and-figures.md`: Source-verified numbers, dimensions, and error metrics.
- `report/`: Modular LaTeX source and compiled documentation.
  - `CAOPipeline.tex`: Master LaTeX document.
  - `sections/*.tex`: Individual chapters covering introduction, extraction, salary parsing, correction, indices, replication, and appendices.
  - `CAO_Pipeline_Explanation.pdf`: Final compiled replication manual.

## How to Rebuild the PDF

From the `report/` directory, run:
```bash
latexmk -pdf CAOPipeline.tex
```
To clean temporary compilation artifacts:
```bash
latexmk -c
```

