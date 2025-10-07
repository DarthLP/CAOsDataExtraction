# CAOsDataExtraction

An AI-powered pipeline for extracting structured data from Dutch Collective Labor Agreements (CAOs) using advanced PDF processing, OCR, and Large Language Models.

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

1. **p0_webscraping.py** - Downloads CAO PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl
2. **p1_inputExcel.py** - Converts Excel field definitions to markdown prompts
3. **p2_extract.py** - Multi-method PDF text extraction with intelligent OCR
4. **p3_llmExtraction.py** - Raw data extraction using Google Gemini API
5. **p4_analysis.py** - Schema-driven structured extraction (salary + non-salary)
6. **p5_excel_creation.py** - Merges results and creates final Excel outputs

## Folder Structure

```
CAOsDataExtraction/
├── conf/
│   └── config.yaml              # Centralized configuration
├── docs/
│   ├── fields_prompt*.md        # LLM prompt templates
│   └── gemini_info.txt          # API documentation
├── inputs/
│   ├── excel/                   # Excel input files
│   └── pdfs/                    # PDF input files
├── monitoring/
│   ├── monitoring_3_1.py        # Performance monitoring
│   └── performance_logs/        # Log files
├── outputs/
│   ├── analysis/                # Analysis results
│   ├── comparison/              # Comparison results
│   ├── excel/                   # Excel output files
│   ├── llm_extracted/           # LLM extracted JSON files
│   ├── parsed_pdfs/             # Parsed PDF JSON/Markdown files
│   └── logs/                    # Log files
├── pipelines/
│   ├── p0_webscraping.py        # Web scraping
│   ├── p1_inputExcel.py         # Excel processing
│   ├── p2_extract.py            # PDF extraction
│   ├── p3_llmExtraction.py      # LLM extraction
│   ├── p4_analysis.py           # Data analysis
│   └── p5_excel_creation.py     # Excel creation
├── scripts/                     # Utility and analysis scripts
├── utils/                       # Helper utilities
└── run_pipeline.py              # Main entry point
```

## Pipeline Stages

### Stage 0: Web Scraping (p0_webscraping.py)
- Downloads CAO PDFs from the official Dutch government website
- Uses Selenium with Chrome for robust scrolling and link discovery
- Supports primary and extra runs with duplicate prevention
- Generates metadata CSV files for tracking

### Stage 1: Excel Processing (p1_inputExcel.py)
- Converts Excel field definitions to markdown prompt templates
- Processes multiple worksheets for different extraction categories
- Creates structured prompts for LLM processing

### Stage 2: PDF Extraction (p2_extract.py)
- **Multi-method extraction**: PyPDF2 + pdfplumber + Tesseract OCR
- **Intelligent OCR triggering** based on:
  - Image detection with coverage analysis
  - Vector graphics detection (embedded tables)
  - Minimal/corrupted text detection
- **Smart comparison**: Always chooses extraction method with more characters
- **Unicode handling**: Automatic conversion of /uniXXXX and /GXXX patterns
- **Parallel processing**: Multi-process support for large batches

### Stage 3: LLM Extraction (p3_llmExtraction.py)
- Uses Google Gemini API for raw data extraction
- Direct markdown upload for optimal accuracy
- Context-preserving extraction (keeps related information together)
- **Parallel processing**: Multi-process support with different API keys
- **Robust error handling**: Exponential backoff and file locking

### Stage 4: Analysis (p4_analysis.py)
- Schema-driven structured extraction using Pydantic models
- Separates salary and non-salary information
- **Advanced features**:
  - Multi-process parallel processing
  - Performance monitoring and quality tracking
  - Comprehensive error handling and retry logic

### Stage 5: Excel Creation (p5_excel_creation.py)
- Merges salary and non-salary extraction results
- Adds CAO metadata and dates
- Creates final Excel files with proper formatting
- Handles Excel cell size limits (32,767 character limit)

## Advanced Features

### Performance Monitoring
- Real-time performance tracking with `monitoring_3_1.py`
- Structured JSON logging for all extraction attempts
- Cost tracking and analysis
- Performance insights and optimization recommendations

### Parallel Processing
- Multi-process support across p2, p3, and p4 stages
- File locking prevents duplicate processing
- Process distribution for optimal resource utilization
- Example parallel execution:
  ```bash
  unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 6 2>&1 | tee log1.txt &
  unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 6 2>&1 | tee log2.txt &
  # ... continue for all processes
  ```

### Quality Tracking
- Extraction quality analysis and reporting
- Failed file identification and retry mechanisms
- Comprehensive logging and error tracking
- Performance metrics and optimization insights

## Technical Highlights

- **Intelligent OCR**: Automatically detects when OCR is needed and compares results
- **Unicode Processing**: Handles complex encoding issues in PDF documents
- **Robust Error Handling**: Exponential backoff, file locking, and comprehensive retry logic
- **Scalable Architecture**: Designed for processing 1,580+ PDF documents
- **Output Organization**: Clear separation of old/new flows and salary/non-salary data
- **Configuration Management**: Centralized YAML configuration for all paths and settings

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
```bash
# PDF extraction with 4 processes
python pipelines/p2_extract.py --process_id 0 --total_processes 4
python pipelines/p2_extract.py --process_id 1 --total_processes 4
python pipelines/p2_extract.py --process_id 2 --total_processes 4
python pipelines/p2_extract.py --process_id 3 --total_processes 4
```

### With Logging and Power Management
```bash
unbuffer caffeinate python pipelines/p2_extract.py --process_id 0 --total_processes 4 2>&1 | tee log1.txt
```

---

CAO Source: [uitvoeringarbeidsvoorwaardenwetgeving.nl](https://www.uitvoeringarbeidsvoorwaardenwetgeving.nl/mozard/!suite16.scherm1168?mGmr=66)