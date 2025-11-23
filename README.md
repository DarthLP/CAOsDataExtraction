# CAOsDataExtraction

An AI-powered pipeline for extracting structured data from Dutch Collective Labor Agreements (CAOs) using advanced PDF processing, OCR, and Large Language Models. Processes 1,580+ PDF documents from the official Dutch government website.

## Quick Start

1. **Activate environment:**
   ```bash
   conda activate caos-extract
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API keys:**
   Create a `.env` file with your Google Gemini API keys:
   ```
   GOOGLE_API_KEY1=your_key_here
   GOOGLE_API_KEY2=your_key_here
   # ... additional keys for parallel processing
   ```

4. **Run the complete pipeline:**
   ```bash
   python run_pipeline.py
   ```

## Pipeline Overview

```
Web Scraping → Excel Processing → PDF Extraction → LLM Extraction → Analysis → Excel Creation
     p0              p1              p2              p3              p4          p5
```

1. **p0_webscraping.py** - Downloads CAO PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl using Selenium
2. **p1_inputExcel.py** - Converts Excel field definitions to markdown prompt templates
3. **p2_extract.py** - Multi-method PDF text extraction (PyPDF2 + pdfplumber + Tesseract OCR)
4. **p3_llmExtraction.py** - Raw data extraction using Google Gemini API with context preservation
5. **p4_analysis.py** - Schema-driven structured extraction (salary + non-salary) using Pydantic models
6. **p5_excel_creation.py** - Merges results and creates final Excel outputs with proper formatting

## Folder Structure

```
CAOsDataExtraction/
├── conf/
│   └── config.yaml              # Centralized configuration (paths and settings)
├── docs/
│   ├── fields_prompt*.md        # LLM prompt templates
│   └── gemini_info.txt          # API documentation
├── inputs/
│   ├── excel/                   # Excel input files (field definitions)
│   └── pdfs/                    # PDF input files (CAO documents, organized by CAO number)
├── monitoring/
│   ├── monitoring_3_1.py        # Performance monitoring and cost tracking
│   └── performance_logs/         # Performance log files
├── outputs/
│   ├── llm_extracted/           # LLM extracted JSON files
│   ├── llm_analysis/            # Schema-validated extraction results
│   ├── parsed_pdfs/             # Parsed PDF JSON/Markdown files
│   ├── excel/                   # Final Excel output files
│   └── logs/                    # Processing logs and error reports
├── pipelines/
│   ├── p0_webscraping.py        # Web scraping
│   ├── p1_inputExcel.py         # Excel processing
│   ├── p2_extract.py            # PDF extraction
│   ├── p3_llmExtraction.py      # LLM extraction
│   ├── p4_analysis.py           # Data analysis
│   └── p5_excel_creation.py     # Excel creation
├── schema/
│   ├── salary_schema.py         # Salary data schema (Pydantic models) - regular schema
│   ├── salary_schema_compact.py # Compact salary schema (no table_label) - for attempts 6-8
│   ├── salary_schema_split.py   # Split salary schema - for attempts 9-10
│   ├── salary_prompt_split.py   # Split extraction prompts - for attempts 9-10
│   ├── non_salary_schema.py     # Non-salary data schema (Pydantic models)
│   └── excel_output_schema.py   # Excel output column definitions
├── scripts/                     # Utility and analysis scripts
├── utils/                       # Helper utilities (input/output management)
│   ├── input_utils/             # Input utilities including merge_split_salary.py
│   └── output_utils/            # Output utilities
└── run_pipeline.py              # Main entry point
```

**Note**: Files are organized by CAO number folders (e.g., `inputs/pdfs/input_pdfs/10/`, `inputs/pdfs/input_pdfs/1536/`). Multiple files can have the same filename but exist in different CAO folders. File identification requires both filename and CAO number.

## Configuration

All paths and settings are centralized in `conf/config.yaml`. Key paths include:
- Input PDFs: `inputs/pdfs/input_pdfs` and `inputs/pdfs/input_pdfs_extra`
- Input Excel: `inputs/excel/inputExcel`
- Output directories: `outputs/llm_extracted`, `outputs/llm_analysis`, `outputs/excel`
- Parsed PDFs: `outputs/parsed_pdfs/parsed_pdfs_json` and `outputs/parsed_pdfs/parsed_pdfs_markdown`

## Usage Examples

### Run Individual Stages
```bash
python -m pipelines.p0_webscraping    # Web scraping
python -m pipelines.p1_inputExcel     # Excel processing
python -m pipelines.p2_extract        # PDF extraction
python -m pipelines.p3_llmExtraction  # LLM extraction
python -m pipelines.p4_analysis       # Data analysis
python -m pipelines.p5_excel_creation # Excel creation
```

### Parallel Processing

Stages p2, p3, and p4 support parallel processing for large batches:

```bash
# PDF extraction with 4 processes
python pipelines/p2_extract.py --process_id 0 --total_processes 4
python pipelines/p2_extract.py --process_id 1 --total_processes 4
python pipelines/p2_extract.py --process_id 2 --total_processes 4
python pipelines/p2_extract.py --process_id 3 --total_processes 4
```

### With Logging and Power Management (macOS)
```bash
unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 6 2>&1 | tee p3_log1.txt &
unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 6 2>&1 | tee p3_log2.txt &
# ... continue for all processes
```

## Technical Highlights

- **Intelligent OCR**: Automatically detects when OCR is needed (image detection, vector graphics, minimal text) and compares results to choose the best extraction method
- **Multi-method PDF Extraction**: Combines PyPDF2, pdfplumber, and Tesseract OCR with intelligent method selection
- **Unicode Processing**: Automatic conversion of /uniXXXX and /GXXX patterns to readable text
- **Schema Validation**: Pydantic-based schemas ensure data quality and structure
- **Parallel Processing**: Multi-process support with file locking to prevent duplicate processing
- **Robust Error Handling**: Exponential backoff, adaptive retry strategies (attempts 1-5), compact schema retries (attempts 6-8), split extraction retries (attempts 9-10), and comprehensive error recovery
- **Performance Monitoring**: Real-time tracking of processing time, token usage, costs, and quality metrics
- **Scalable Architecture**: Designed for processing 1,580+ PDF documents efficiently

## Pipeline Stage Details

### Stage 0: Web Scraping (p0_webscraping.py)
- Downloads CAO PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl
- Uses Selenium with Chrome for robust scrolling and link discovery
- Supports primary and extra runs with duplicate prevention
- Generates metadata CSV files for tracking

### Stage 1: Excel Processing (p1_inputExcel.py)
- Converts Excel field definitions to markdown prompt templates
- Processes multiple worksheets for different extraction categories
- Creates structured prompts for LLM processing

### Stage 2: PDF Extraction (p2_extract.py)
- **Multi-method extraction**: PyPDF2 + pdfplumber + Tesseract OCR
- **Intelligent OCR triggering** based on image detection, vector graphics detection, and minimal text detection
- **Smart comparison**: Always chooses extraction method with more characters
- **Unicode handling**: Automatic conversion of /uniXXXX and /GXXX patterns
- **Parallel processing**: Multi-process support for large batches

### Stage 3: LLM Extraction (p3_llmExtraction.py)
- Uses Google Gemini API for raw data extraction
- Direct markdown upload for optimal accuracy
- Context-preserving extraction (keeps related information together)
- **Parallel processing**: Multi-process support with different API keys
- **Robust error handling**: 
  - **Attempts 1-5**: Unified extraction with adaptive retry (exponential backoff, parameter adjustments)
  - **Attempts 6-8**: Split extraction (salary and non-salary separately) with partial success caching
  - File locking to prevent duplicate processing
- **Split extraction strategy**: For files with very large outputs, splits extraction into salary-only and non-salary-only schemas, then merges results. Successful partial extractions are cached to avoid re-extraction on retries.

### Stage 4: Analysis (p4_analysis.py)
- Schema-driven structured extraction using Pydantic models
- Separates salary and non-salary information
- Non-salary schema split into 3 parts for better performance
- **Multi-process parallel processing** with independent error handling
- **Performance monitoring** and quality tracking
- **Advanced retry strategy for large files**:
  - **Attempts 1-5**: Regular extraction with adaptive parameter adjustment
  - **Attempts 6-8**: Compact schema extraction (reduced output size) - triggered if truncation occurs after attempt 4, or if file is in truncated folder
  - **Attempts 9-10**: Split extraction (first half/second half by jobgroup boundaries) - triggered if truncation occurs after attempt 7, or if file is in truncated_2 folder
  - **File handling**:
    - Files in truncated folder → retry with attempts 6-8 (compact), extend to 9-10 (split) if needed
    - Files in truncated_2 folder → retry with attempts 9-10 (split extraction)
    - Files in truncated_3 folder → skipped (all attempts exhausted)

### Stage 5: Excel Creation (p5_excel_creation.py)
- Merges salary and non-salary extraction results
- Adds CAO metadata and dates
- Creates final Excel files with proper formatting
- Handles Excel cell size limits (32,767 character limit)

## Troubleshooting

### Common Issues

**API Quota Errors**: Use multiple API keys and distribute processes across them. Check `monitoring/performance_logs/` for usage statistics.

**PDF Extraction Failures**: Check `outputs/logs/` for failed files. Re-run p2_extract.py with DEBUG=True for detailed logs.

**Unicode/Encoding Issues**: The pipeline automatically handles common patterns. For persistent issues, check `scripts/unicode_processing/` utilities.

**Parallel Processing Conflicts**: File locking prevents duplicates. If processes hang, check for stale lock files in output directories.

**Memory Issues**: Process large batches in smaller chunks using `--max_files` parameter.

### Performance Optimization

- Use parallel processing for stages p2, p3, and p4
- Monitor performance logs in `monitoring/performance_logs/`
- Adjust `total_processes` based on available resources and API quotas
- Use `caffeinate` (macOS) to prevent system sleep during long runs

## External Resources

- **CAO Source**: [uitvoeringarbeidsvoorwaardenwetgeving.nl](https://www.uitvoeringarbeidsvoorwaardenwetgeving.nl/mozard/!suite16.scherm1168?mGmr=66)
- **Google Gemini API**: See `docs/gemini_info.txt` for API documentation
- **Prompt Templates**: See `docs/fields_prompt*.md` for LLM prompt definitions
