"""
Centralized cross-repository filesystem paths for verification_pipeline.
Resolves paths relative to the unified CAOsDataExtraction repository root,
replacing hard-coded user-specific absolute paths.
"""
from pathlib import Path

# Repository root (CAOsDataExtraction/)
EXTRACTION_ROOT = Path(__file__).resolve().parents[1]

# Upstream extraction output paths
LLM_EXTRACTED_DIR = EXTRACTION_ROOT / "outputs" / "llm_extracted" / "new_flow"
SALARY_PARSER_CSV = EXTRACTION_ROOT / "outputs" / "parser_salary" / "extracted_data_salary_v2.csv"
EXCEL_RESULTS_DIR = EXTRACTION_ROOT / "outputs" / "excel" / "new_results"
PARSED_PDFS_DIR = EXTRACTION_ROOT / "outputs" / "parsed_pdfs"
LLM_ANALYSIS_SALARY_DIR = EXTRACTION_ROOT / "outputs" / "llm_analysis" / "salary"

# Verification pipeline root
VERIFICATION_ROOT = Path(__file__).resolve().parent

