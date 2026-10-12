# Extraction Pipelines (Stages p1–p5)

Core sequential pipeline for extracting and structuring information from collective labor agreement PDFs.

## Pipeline Sequence

```
p1_webscraping.py -> p2_extract.py -> p3_llmExtraction.py -> p4_analysis.py -> p5_excel_creation.py
```

### 1. `p1_webscraping.py`
- Downloads CAO PDFs from `uitvoeringarbeidsvoorwaardenwetgeving.nl`.
- Filters even/odd document links and merges multi-part documents into unified files per CAO edition.
- Saves raw PDFs to `inputs/pdfs/input_pdfs/[CAO_NUMBER]/`.

### 2. `p2_extract.py`
- Text extraction pipeline using `pdfplumber`, `PyPDF2`, and OCR fallbacks.
- Emits parsed markdown and JSON per document into `outputs/parsed_pdfs/`.

### 3. `p3_llmExtraction.py`
- LLM extraction via Google Gemini API.
- Extracts raw excerpts across 13 topics (wage, leave, term, pension, bonus, contract, etc.).
- Saves document JSONs to `outputs/llm_extracted/new_flow/[CAO_NUMBER]/[FILENAME]_extract.json`.

### 4. `p4_analysis.py`
- Structured extraction using Pydantic schemas in `schema/` (`non_salary_schema.py`).
- Generates validated JSON structures under `outputs/llm_analysis/`.
- *Note:* LLM salary ladder output in p4 is superseded by the deterministic `salary_parser/`.

### 5. `p5_excel_creation.py`
- Merges topic-level extraction outputs and metadata from `extracted_cao_info.csv`.
- Generates final flat tabular CSVs and Excel files under `outputs/excel/new_results/` (`extracted_data_non_salary.csv`).


### 0. `p0_fetch_avv_pdfs.py` (side task, not part of the p1–p5 chain)
- Fetches **earlier** CAO PDFs (AVV decisions, Staatscourant) from `zoek.officielebekendmakingen.nl` for the
  CAO codes in `inputs/pdfs/input_pdfs_new/SectoralCAOs_Update.xlsx`, published before Overview col I.
- Subcommands `catalog` -> `candidates` -> (LLM classify) -> `select` -> `download` -> `verify-batches`
  (LLM verify) -> `write-excel`. Artifacts, prompts and the step table: `p0_avv_selection/README.md`.
- Rules and trade-offs: root `DECISIONS.md` (2026-10-12). Never overwrites or deletes files.
