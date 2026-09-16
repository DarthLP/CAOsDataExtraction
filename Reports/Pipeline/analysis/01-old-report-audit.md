# Old Report Audit (November 2025 Text)

Claim-by-claim audit of `inputs/CAO_Pipeline_Explanation.pdf` (source: `inputs/CAOPipeline.tex`, 2025-11-27).

---

## 1. Section-by-Section Verdicts

| Section | Topic in Old Report | Verdict | Rationale & Action |
|---|---|---|---|
| **§1** | High-level goal & repo link | **FIX** | Repo link is still valid. Claims `pipelines/p0_webscraping.py` exists; live file is `pipelines/p1_webscraping.py`. Notes `p1_inputExcel.py` as legacy; neither `p1_inputExcel.py` nor `docs/fields_prompt*.md` exist in the repo. Scope expands from extraction-only to the entire extraction-to-indices pipeline. |
| **§2.1** | Top-level directory layout | **FIX** | Layout now includes `salary_parser/`, `verification_pipeline/` (QA + indices + salary validation), and `Reports/Pipeline/`. References to `inputs/excel/inputExcel/` removed as legacy artefacts. |
| **§2.2** | Core pipeline scripts | **FIX** | Replace `p0_webscraping` with `p1_webscraping`. Remove description of non-existent `p1_inputExcel.py`. Note addition of deterministic `salary_parser/`. |
| **§3.1** | Stage 1 Webscraping | **FIX** | Fix script name to `p1_webscraping.py`. Confirm even-link selection, multi-part document concatenation, and directory partitioning by CAO number. |
| **§3.2** | Legacy Excel stage | **FIX** | Delete claim that `p1_inputExcel.py` and `docs/fields_prompt*.md` exist on disk. Replaced with reference to `schema/non_salary_schema.py` and prompt export scripts. |
| **§3.3** | Stage 2 PDF text extraction | **KEEP** | Multi-method PDF extraction (PyPDF2 + pdfplumber + Tesseract OCR) and markdown/JSON output in `outputs/parsed_pdfs/` remain accurate. |
| **§3.4** | Stage 3 LLM extraction | **KEEP** | Gemini 2.5 Flash invocation, 13 topic categories, temperature 0.0, top_p 0.1, top_k 1, seed 42, 65,536 output tokens remain accurate. |
| **§3.5** | Stage 4 LLM analysis: Non-salary | **KEEP** | Schema enforcement, Pydantic validation, three topic groups (`gen_bon_wag_pen_ter`, `lea_ove_tra`, `hom_con_saf_chi_ai_fri`) remain accurate. |
| **§3.5** | Stage 4 LLM analysis: Salary ladder | **STALE** | Code is present in `p4_analysis.py`, but its outputs are superseded. The 4-tier LLM retry ladder had a ~20% major-issue rate and is replaced by `salary_parser/`. |
| **§3.6** | Stage 5 Excel creation | **STALE in role** | Non-salary creation is valid; salary slot extraction `salary_N_*` is superseded by `salary_parser/deliver.py`. |
| **§4** | Robustness & error handling | **KEEP** | Lock-file parallelization, retry logic, error classification, and JSON sanitization remain accurate. |
| **§5.2** | Pipeline orchestration | **KEEP** | Root `run_pipeline.py` importing `pipelines.p5_run` was a non-functional placeholder and is now superseded by explicit stage execution instructions. |
| **§5.3** | Environment requirements | **KEEP** | Python 3.13, `requirements.txt`, headless Chrome/Selenium dependencies remain accurate. |
| **§6** | Reproducibility & determinism | **FIX** | Expand non-determinism sources to document agent-assisted verification and the requirement that the Stage 3 indices build must be byte-identical. |

---

## 2. Completely Missing Areas in Old Report

The following major pipeline components built since November 2025 were entirely omitted from the old report:
1. **Deterministic Salary Parser (`salary_parser/`)**: P3 grid parsing, 100% amount provenance, confidence tiers A–D, replacing LLM ladder.
2. **Deterministic Lifecycle QA (`scripts/qa/lifecycle/`)**: Document classification and timeline join.
3. **P3/P4 Resume System**: State caching and lock-file recovery.
4. **Correction & Verification Pipeline (`verification_pipeline/qa/`)**: 34 audited layers (G0→G33), ~12,000 corrected cells, multi-agent arbitration protocol.
5. **Derived Generosity & Wage Indices (`verification_pipeline/indices/`)**: Pooled-z scoring, statutory floors, monthly panel, 26-tab workbook.

