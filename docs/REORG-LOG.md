# Repository Reorganization Log

**Started:** 2025-08-22  
**Branch:** reorg/file-structure-2025-08-22  
**Pre-reorg commit:** 21a2ea481099e84440e5464d1fae300b11a3f5d7  
**Pre-reorg tag:** pre-reorg-20250822T000000Z

## Actions Log

### 2025-08-22 00:00:00 - Initial setup
- Created backup branch: reorg/file-structure-2025-08-22
- Tagged pre-reorg state: pre-reorg-20250822T000000Z
- Created reorganization log file

### 2025-08-22 00:00:00 - File moves completed
- 0_webscrapping.py -> pipelines/p0_webscraping.py (corrected spelling)
- 1_inputExcel.py -> pipelines/p1_inputExcel.py
- 2_extract.py -> pipelines/p2_extract.py
- 3_llmExtraction.py -> pipelines/p3_llmExtraction.py
- 3_1_llmExtraction.py -> pipelines/p3_1_llmExtraction.py
- 4_analysis.py -> pipelines/p4_analysis.py
- 5_run.py -> pipelines/p5_run.py
- monitoring_3_1.py -> monitoring/monitoring_3_1.py
- performance_logs/ -> monitoring/performance_logs/
- input_pdfs/ -> inputs/pdfs/
- inputExcel/ -> inputs/excel/
- INPUT_compare_pdfs_csv_vs_disk.py -> inputs/INPUT_compare_pdfs_csv_vs_disk.py
- INPUT_rename_to_original_pdf_names.py -> inputs/INPUT_rename_to_original_pdf_names.py
- OUTPUT_analyze_empty_json_files.py -> utils/OUTPUT_analyze_empty_json_files.py
- OUTPUT_analyze_extracted_data.py -> utils/OUTPUT_analyze_extracted_data.py
- OUTPUT_compare_with_handAnalysis.py -> utils/OUTPUT_compare_with_handAnalysis.py
- OUTPUT_delete_cao_files.py -> utils/OUTPUT_delete_cao_files.py
- OUTPUT_merge_analysis_results.py -> utils/OUTPUT_merge_analysis_results.py
- OUTPUT_tracker.py -> utils/OUTPUT_tracker.py
- llmExtracted_json/ -> outputs/json/ (merged)
- output_json/ -> outputs/json/
- results/ -> outputs/excel/
- comparison_results/ -> outputs/comparison/
- analysis_output/ -> outputs/analysis/
- failed_files_llm_extraction.txt -> outputs/logs/
- failed_files_analysis.txt -> outputs/logs/
- empty_files_analysis.txt -> outputs/logs/
- extracted_data_analysis_report.txt -> outputs/logs/
- fields_prompt.md -> docs/
- fields_prompt_collapsed.md -> docs/
- fields_prompt_rest.md -> docs/
- fields_prompt_salary.md -> docs/
- gemini_info.txt -> docs/
- count_files.py -> scripts/
- test_*.py -> scripts/
- translate_*.py -> scripts/
- _EXTRA/ -> scripts/

### 2025-08-22 00:00:00 - Path configuration updates
- Created conf/config.yaml with centralized path configuration
- Updated pipelines/p0_webscraping.py: added yaml import and config loading, updated INPUT_EXCEL_PATH and OUTPUT_FOLDER
- Updated pipelines/p1_inputExcel.py: added yaml import and config loading, updated excel paths and output paths
- Updated pipelines/p2_extract.py: added yaml import and config loading, updated INPUT_FOLDER and OUTPUT_FOLDER
- Updated pipelines/p3_llmExtraction.py: added yaml import and config loading, updated INPUT_JSON_FOLDER and OUTPUT_JSON_FOLDER
- Updated pipelines/p3_1_llmExtraction.py: added yaml import and config loading, updated paths and PDF references
- Updated pipelines/p4_analysis.py: added yaml import and config loading, updated all input/output paths
- Updated pipelines/p5_run.py: added yaml import and config loading, updated script references and paths

### 2025-08-22 00:00:00 - Import rewrite completed
- Created scripts/rewrite_imports_ast.py for AST-based import rewriting
- Processed 35 Python files
- Updated 7 files with import changes:
  - pipelines/p3_1_llmExtraction.py: monitoring_3_1 -> monitoring.monitoring_3_1
  - pipelines/p3_llmExtraction.py: OUTPUT_tracker -> utils.OUTPUT_tracker
  - pipelines/p2_extract.py: OUTPUT_tracker -> utils.OUTPUT_tracker
  - pipelines/p4_analysis.py: OUTPUT_tracker -> utils.OUTPUT_tracker
  - pipelines/p0_webscraping.py: OUTPUT_tracker -> utils.OUTPUT_tracker
  - monitoring/performance_logs/cleanup_duplicates.py: monitoring_3_1 -> monitoring.monitoring_3_1
  - monitoring/performance_logs/update_summary.py: monitoring_3_1 -> monitoring.monitoring_3_1
- Summary written to docs/import_rewrite_summary.json

### 2025-08-22 00:00:00 - Entrypoints and package structure
- Added main() functions to all pipeline files:
  - pipelines/p1_inputExcel.py: added main() function
  - pipelines/p3_llmExtraction.py: added main() function  
  - pipelines/p3_1_llmExtraction.py: added main() function
  - pipelines/p4_analysis.py: added main() function
  - pipelines/p5_run.py: added main() function
- Created pipelines/__init__.py with minimal structure
- Created run_pipeline.py at repo root to call pipelines.p5_run.main()
- Separated LLM extraction flows:
  - p3_llmExtraction.py writes to outputs/json/old_flow/
  - p3_1_llmExtraction.py writes to outputs/json/new_flow/
- Installed missing dependencies: astor, PyYAML
- Created scripts/check_imports_and_syntax.py for sanity checking
- Created separate output directories for LLM extraction flows
