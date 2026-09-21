# Original User Request

## Initial Request — 2026-09-16T09:58:31Z

Comprehensive audit, code verification, factual completion, and pipeline streamlining of the Dutch Collective Labour Agreements (CAOs) technical replication report ([`CAOPipeline.tex`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/Reports/Pipeline/report/CAOPipeline.tex) and [`CAOPipeline.pdf`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/Reports/Pipeline/report/CAOPipeline.pdf)) across all active pipeline stages, schemas, scripts, data artifacts, and automated verification suites.

Working directory: /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction
Integrity mode: development

## Requirements

### R1. Complete & Clean Pipeline Description (Active Code Only)
Ensure the pipeline architecture and replication workflow described in the report strictly documents the active, living codebase, stripping out references to dead or legacy scripts unless they represent key architectural design decisions (which should only be noted briefly):
- **Stage 1 (Text Extraction)**: Document the active 5-stage flow (`pipelines/p1_webscraping.py` through `p5_excel_creation.py`). Confirm active hyperparameters (`pipelines/p3_llmExtraction.py`: model `gemini-2.5-flash`, temperature 0.0, top_p 0.1, top_k 1, seed 42, max_tokens 65536).
- **Stage 1b (Deterministic Salary Parser)**: Document `salary_parser/deliver.py` as the active single-entrypoint pipeline. Briefly note the architectural pivot: replacing the stochastic 4-tier LLM salary ladder from `p4_analysis.py` because of ~20% error rates (hallucinations, token truncations). Document parser invariants (100% amount provenance, CAO 592 2-cell exception, header date grounding).
- **Stage 2 (Verification & QA)**: Accurately describe the 34-layer correction lineage (G0–G33) documented in `verification_pipeline/docs/DATA_LINEAGE.md`. Clarify that `verification_pipeline/qa/corrected_dataset.csv` is the canonical, frozen artifact. Do not present one-off historical scripts (e.g. `apply_corrections.py` which only applied Layer 1) as active regeneration commands. Highlight the active verification suite: 152 automated regression tests in `verification_pipeline/qa/` (`pytest qa -q`).
- **Stage 3 (Derived Indices & Panel)**: Fully specify the active rebuild pipeline orchestrated by `verification_pipeline/indices/rebuild.sh`: the 13 topic index scripts, `mw_indices.py`, `statutory_index.py`, `composite_index.py`, `build_panel_monthly.py`, `advanced_analysis.py`, `build_cao_level_export.py`, and `build_combined.py` producing `all_indices.xlsx`. Briefly note the v1 $\rightarrow$ v2 architectural transition (moving to pooled-z scores with term-group deduplicated yardstick, detailed in `INDICES_V2_PLAN.md`).
- **Stage 4 (Downstream Reporting)**: Reference the active reports layout: replication guide (`Reports/Pipeline/`) and descriptive summary report (`Reports/Analysis/`, built via `scripts/run_all.sh`).

### R2. Exact Data Artifact & Numerical Consistency Audit
Verify all numbers, table entries, percentages, and matrix dimensions against canonical files:
- Canonical artifacts table (`01_intro.tex`, Table 1):
  - `extracted_data_salary_v2.csv` (359,474 rows, 749 cols, delimiter `;`).
  - `corrected_dataset.csv` (2,739 rows, 318 cols, 242 unique CAOs, delimiter `;`).
  - `composite_index.csv` (2,698 rows, 112 cols).
  - `mw_indices.csv` (1,970 rows, 22 cols).
  - `scoring_params.csv` (89 parameter rows registering all 39 scored fields).
- Salary parser confidence tier distribution (`03_salary_parser.tex`, Table 2):
  - Tier A: 254,334 (70.75%), Tier B: 88,777 (24.70%), Tier C: 13,693 (3.81%), Tier D: 2,670 (0.74%), Total: 359,474.
  - Analytical sample (Tiers A+B): 343,111 rows (95.45%).
- QA and audit queue metrics (`04_correction_verification.tex`):
  - Suspect queue in `verification_pipeline/indices/out/statutory_fingerprint_suspects.csv`: 1,244 suspects (1,150 never agent-checked).
  - Verification error rates: ~97% snippet false absence, ~22% definition-less drift, 87% KEEP preservation.
- Statistical findings (`05_indices.tex`):
  - KMO measure of 0.64 in `advanced_analysis.py` / `out/factor_summary.txt`.
- Worked example correction (`07_appendices.tex`, Appendix D):
  - Correct CAO 1022 PDF filename, salary scales, and QA layer citation to reflect real data.

### R3. Test Suite Execution & Script Portability
- Execute the full automated regression test suite: `cd verification_pipeline && python3 -m pytest qa -q` (all 152 tests must pass).
- Execute the indices validation battery: `cd verification_pipeline/indices && python3 check_battery.py` (all 6 battery checks must pass).
- Make `verification_pipeline/indices/rebuild.sh` portable by replacing the hardcoded user directory with dynamic script directory resolution (`cd "$(dirname "$0")"`).

### R4. Report Correction and PDF Recompilation
- Update `Reports/Pipeline/report/sections/*.tex` with corrected paths, exact numerical descriptions, and clean active-pipeline prose.
- Compile the document using `latexmk -pdf CAOPipeline.tex` from `Reports/Pipeline/report/`. Ensure clean exit with 0 errors and an updated `CAOPipeline.pdf`.

## Acceptance Criteria

### Pipeline Currency & Legacy Pruning
- [ ] Every component and script in the architecture diagram and text corresponds to the active system.
- [ ] Superseded tools (LLM salary ladder, indices v1, p0 scraper, historical one-off layer scripts) are either removed or briefly contextualized as design motivations.
- [ ] Every file path points to an existing file relative to the repo root.

### Numerical & Factual Precision
- [ ] Every table entry, count, percentage, and delimiter matches the programmatic extraction from the active CSVs/code.
- [ ] Known discrepancies (`DATA_LINEAGE.md` path, `statutory_fingerprint_suspects.csv` path, `wage_check_full` path, CAO 1022 worked example, and scoring params row count) are completely resolved.

### Test Execution & Compilation
- [ ] `pytest qa -q` passes with 152/152 tests passing.
- [ ] `python3 check_battery.py` passes all 6 validation stages with 0 failures.
- [ ] `rebuild.sh` runs cleanly without hardcoded machine path dependencies.
- [ ] `latexmk -pdf CAOPipeline.tex` compiles cleanly without fatal errors.

