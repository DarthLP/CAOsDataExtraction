# Dutch Collective Labour Agreements (CAOs) — Extraction & Verification Pipeline

Unified repository for end-to-end extraction, deterministic salary parsing, multi-layer auditing/correction, and derived generosity and wage indices for Dutch Collective Bargaining Agreements (Collectieve Arbeidsovereenkomsten / CAOs).

---

## 1. End-to-End Pipeline Architecture

```
Stage 1: Extraction (p1–p5)
  ├── p1_webscraping.py        -> Download PDFs from official portal
  ├── p2_extract.py            -> OCR / text extraction (pdfplumber / PyPDF2)
  ├── p3_llmExtraction.py      -> Gemini extraction to JSON (13 topics)
  ├── p4_analysis.py           -> Schema validation & structured analysis
  └── p5_excel_creation.py     -> Merged Excel / CSV tabular outputs

Stage 1b: Deterministic Salary Parsing (salary_parser/)
  └── deliver.py / run_all.py  -> Parses p3 wage tables deterministically
                                  (100% amount provenance, confidence tiers A–D)

Stage 2: Correction & Verification (verification_pipeline/qa/)
  └── corrected_dataset.csv    -> Canonical frozen dataset: 34 audited layers
                                  (G0→G33), ~12,000 corrected cells, grounded
                                  against English-translated source text.
                                  Verified by 152 regression tests (pytest qa -q)

Stage 3: Derived Indices (verification_pipeline/indices/)
  └── rebuild.sh               -> Pooled-z generosity scores, statutory floor,
                                  wage index, monthly in-force panel, 26-tab workbook

Stage 4: Replication Guide & Reports (Reports/Pipeline/)
  └── report/CAOPipeline.tex   -> Comprehensive LaTeX replication guide and methodology
```

---

## 2. Canonical Artifacts

| Stage | Artifact | Description | Grounding / Integrity |
|---|---|---|---|
| **Raw Inputs** | `inputs/pdfs/input_pdfs/` | Downloaded official CAO PDF documents | Upstream source |
| **Stage 1 (p3)** | `outputs/llm_extracted/new_flow/` | Per-document extract JSONs (13 topic keys) | English-translated text excerpts |
| **Stage 1 (p5)** | `outputs/excel/new_results/` | Initial tabular extractions (`extracted_data_non_salary.csv`) | Raw LLM output |
| **Stage 1b** | `outputs/parser_salary/extracted_data_salary_v2.csv` | Deterministic salary table (tiers A+B canonical) | 100% amount provenance against p3 |
| **Stage 2** | `verification_pipeline/qa/corrected_dataset.csv` | Canonical corrected non-salary dataset (34 layers) | Multi-agent arbitrated, source-backed |
| **Stage 3** | `verification_pipeline/indices/out/composite_index.csv` | Overall and topic-level generosity z-scores | Pooled standardisation, winsorised |
| **Stage 3** | `verification_pipeline/indices/out/mw_indices.csv` | Nominal wage ladder & monthly wage indices | Stratified by full-time hours & tier |
| **Stage 3** | `verification_pipeline/indices/out/scoring_params.csv` | Parameter registry for all scored fields | Fixed baselines & weights |

---

## 3. Standard Execution Order

To reproduce the pipeline outputs from a clean clone:

1. **Deterministic Salary Parsing:**
   ```bash
   python3 salary_parser/deliver.py
   ```
2. **QA & Correction Suite:**
   ```bash
   cd verification_pipeline
   python3 -m pytest qa -q
   ```
3. **Indices Generation & Battery Check:**
   ```bash
   cd verification_pipeline/indices
   ./rebuild.sh
   python3 check_battery.py
   ```
4. **Compile Replication Guide:**
   ```bash
   cd Reports/Pipeline/report
   latexmk -pdf CAOPipeline.tex
   ```

---

## 4. Repository Structure & Key Documentation

- `pipelines/` — Core extraction pipeline scripts (`p1` through `p5`). See [`pipelines/README.md`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/pipelines/README.md).
- `salary_parser/` — Deterministic wage table parser. See [`salary_parser/README.md`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/salary_parser/README.md).
- `verification_pipeline/` — Correction suite, audit layers, and derived indices. See [`verification_pipeline/README.md`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/verification_pipeline/README.md).
  - `verification_pipeline/qa/` — 34-layer correction framework.
  - `verification_pipeline/indices/` — Generosity scoring and panel construction.
  - `verification_pipeline/salary/` — Salary QA and validation checks.
- `Reports/Pipeline/` — Unified replication report and LaTeX documentation.
- `Reports/Analysis/` — CAO Descriptive Summary report (general/non-salary/salary/indices
  figures and tables, built from `verification_pipeline/qa` + `verification_pipeline/indices`
  + the salary parser output). See [`Reports/Analysis/README.md`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/Reports/Analysis/README.md).
- `docs/EXTRACTION_GUIDE.md` — Detailed extraction pipeline manual (former root README).
- `CLAUDE.md` — Unified developer instructions and hard rules.

