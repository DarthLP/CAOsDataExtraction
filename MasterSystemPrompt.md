# Master System Prompt - CAO Data Extraction Project

## Project Context

**Purpose**: Extract structured data from Dutch Collective Labor Agreements (CAOs) - legal documents that define employment terms, salaries, benefits, and working conditions.

**Domain**: Dutch labor law and employment contracts. CAOs are published as PDFs by the government.

**Scale**: Processes 1,580+ PDF documents from uitvoeringarbeidsvoorwaardenwetgeving.nl

**Output**: Structured Excel files with salary and non-salary information extracted using AI/LLM processing.

## Architecture Overview

### Pipeline Flow
```
p0_webscraping → p1_inputExcel → p2_extract → p3_llmExtraction → p4_analysis → p5_excel_creation
```

**Stage 0 (p0)**: Web scraping - Downloads PDFs using Selenium, organizes by CAO number
**Stage 1 (p1)**: Excel processing - Converts field definitions to markdown prompts
**Stage 2 (p2)**: PDF extraction - Multi-method text extraction (PyPDF2 + pdfplumber + OCR)
**Stage 3 (p3)**: LLM extraction - Raw data extraction using Google Gemini API
**Stage 4 (p4)**: Analysis - Schema-driven structured extraction (salary + non-salary)
**Stage 5 (p5)**: Excel creation - Merges results and creates final Excel outputs

### Data Flow
- **Input**: PDFs in `inputs/pdfs/input_pdfs/[CAO_NUMBER]/`
- **Intermediate**: Parsed markdown/JSON in `outputs/parsed_pdfs/`, LLM extracted JSON in `outputs/llm_extracted/`
- **Output**: Schema-validated JSON in `outputs/llm_analysis/`, Excel files in `outputs/excel/`

**Important**: Files are organized by CAO number folders. Multiple files can have the same filename but exist in different CAO folders (e.g., `10/file.pdf` and `1536/file.pdf`). File matching always requires both filename AND CAO number to uniquely identify a file.

### Key Components
- **Configuration**: Centralized in `conf/config.yaml` - all paths and settings
- **Schemas**: Pydantic models in `schema/` for validation (salary_schema.py, non_salary_schema.py, excel_output_schema.py)
- **Monitoring**: Performance tracking in `monitoring/monitoring_3_1.py`
- **Utilities**: Input/output management in `utils/`, analysis scripts in `scripts/`

## Code Structure

### Directory Organization
- **pipelines/**: Main pipeline stages (p0-p5)
- **schema/**: Pydantic data models and validation schemas
- **utils/**: Helper utilities (input_utils/, output_utils/)
- **scripts/**: Analysis and utility scripts
- **monitoring/**: Performance monitoring and logging
- **conf/**: Configuration files (config.yaml)
- **docs/**: Documentation and prompt templates

### Import Patterns
- All pipeline scripts add parent directory to sys.path: `sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))`
- Configuration loaded via: `yaml.safe_load(open('conf/config.yaml'))`
- Environment variables via: `load_dotenv()` from python-dotenv
- Schema imports: `from schema.salary_schema import ...`

### File Naming Conventions
- Pipeline stages: `p0_webscraping.py`, `p1_inputExcel.py`, etc.
- Utility scripts: `INPUT_*.py` (input utils), `OUTPUT_*.py` (output utils)
- Schema files: `salary_schema.py`, `non_salary_schema.py`, `excel_output_schema.py`

## Key Technical Decisions

### Multi-Method PDF Extraction (p2)
**Why**: PDFs vary widely - native text, scanned images, embedded tables, encoding issues
**Approach**: Try PyPDF2 → pdfplumber → OCR, compare results, choose method with most characters
**Rationale**: No single method works for all PDFs; comparison ensures best extraction quality

### Pydantic Schemas for Validation (p4)
**Why**: Need strict validation of extracted data structure and types
**Approach**: Define Pydantic models for salary and non-salary data, validate LLM outputs
**Rationale**: Ensures data quality, catches errors early, provides clear error messages

### Google Gemini API (p3, p4)
**Why**: High-quality extraction from complex legal documents, good markdown support
**Approach**: Direct markdown upload, structured prompts, adaptive retry strategies
**Rationale**: Better accuracy than alternatives, cost-effective for large batches

### Parallel Processing (p2, p3, p4)
**Why**: Process 1,580+ PDFs efficiently, utilize multiple API keys
**Approach**: Multi-process with file locking, process distribution by modulo
**Rationale**: Significantly faster processing, prevents duplicate work via file locks

### Non-Salary Schema Split (p4)
**Why**: Large schema causes API timeouts and JSON truncation
**Approach**: Split into 3 parts (Part1: General/Bonuses/WageScales/Pension/Termination, Part2: Leave/Overtime/Training, Part3: Homeoffice/Contract/Safety/Childcare/AI/Fringe)
**Rationale**: Smaller schemas = more reliable extraction, independent retry logic

### Error Handling Strategy
**Why**: API failures, timeouts, JSON parsing errors are common at scale
**Approach**: Exponential backoff, adaptive retry (adjust temp/top_p/top_k on attempts 4-5), file locking, comprehensive logging
**Rationale**: Robust recovery from transient failures, prevents data loss

### Unicode/Encoding Handling (p2)
**Why**: PDFs contain /uniXXXX and /GXXX patterns that break text extraction
**Approach**: Automatic pattern detection and conversion to readable text
**Rationale**: Critical for OCR quality and LLM processing accuracy

## Data Schemas

### Salary Schema (`schema/salary_schema.py`)
- **Amount**: `{value: float, unit: str}` - Single value with unit
- **AmountRange**: `{min: float, max: float, unit: str}` - Min/max range
- **SalaryPoint**: One effective salary value with start_date, end_date, amount, context
- **SalaryRow**: Collection of SalaryPoints for one job group/worker type
- **SalaryExtractionSchema**: Root schema containing list of SalaryRows

### Non-Salary Schema (`schema/non_salary_schema.py`)
Split into 3 parts for performance:
- **Part1**: GeneralInfo, BonusesInfo, WageScalesInfo, PensionInfo, TerminationInfo
- **Part2**: LeaveInfo, OvertimeInfo, TrainingInfo
- **Part3**: HomeofficeInfo, ContractTypeInfo, SafetyInfo, ChildcareInfo, AIInfo, FringeBenefitsInfo

Each info class contains structured fields (Amount, AmountRange, booleans, strings, lists) representing specific CAO provisions.

### Excel Output Schema (`schema/excel_output_schema.py`)
- Flattens nested Pydantic models into Excel columns
- Handles Amount/AmountRange flattening (value + unit columns)
- Generates column lists automatically from schema definitions
- Adds CAO metadata columns (cao_number, id, TTW, dates, file_name)

## API Integration

### Google Gemini API Usage
- **Library**: `google-genai` (newer library, not `google-generativeai`)
- **Model**: `gemini-1.5-pro` or `gemini-1.5-flash` (configurable)
- **Input**: Markdown files uploaded directly (not text strings)
- **Output**: JSON responses validated against Pydantic schemas

### API Key Management
- Multiple keys in `.env`: `GOOGLE_API_KEY1`, `GOOGLE_API_KEY2`, etc.
- Key rotation via `--key_number` parameter
- Parallel processes use different keys to distribute load

### Error Handling & Retry
- Exponential backoff: 2^attempt seconds
- Adaptive retry: Adjust temperature/top_p/top_k on attempts 4-5
- Failure-aware guidance: Detect LLM-controllable errors (truncated JSON, empty responses), provide retry instructions
- File locking: Prevents duplicate processing across parallel processes

### Cost Optimization
- Performance monitoring tracks token usage and costs
- Logs stored in `monitoring/performance_logs/`
- Direct markdown upload reduces token costs vs text strings

## Code Patterns

### Function Documentation
Every function has comprehensive docstring:
```python
"""
Brief description.

Detailed explanation of what the function does, its parameters, and return values.

Args:
    param1: Description
    param2: Description
    
Returns:
    Description of return value
"""
```

### Error Handling Pattern
```python
try:
    # Operation
except SpecificError as e:
    # Log error
    # Retry or handle gracefully
    # Update monitoring
```

### Logging Conventions
- Minimal console output: Only CAO number, filename, completion status
- Detailed logging: JSON Lines format in `monitoring/performance_logs/`
- Error logs: Text files in `outputs/logs/` (failed_files_*.txt)

### Configuration Management
- All paths in `conf/config.yaml`
- Load once at script start: `yaml.safe_load(open('conf/config.yaml'))`
- Use config dict throughout script

### File I/O Patterns
- Use `pathlib.Path` for path operations
- Check for existing files before processing (skip if exists)
- Create directories automatically: `Path.mkdir(parents=True, exist_ok=True)`
- File locking for parallel processing: `fcntl.flock()` on Unix
- **File identification**: Always use both filename AND CAO number to uniquely identify files, as same filename can exist in different CAO folders

### Parallel Processing Pattern
```python
# Distribute files across processes
files = sorted(all_files)
process_files = [f for i, f in enumerate(files) if i % total_processes == process_id]

# File locking to prevent duplicates
lock_file = output_path.with_suffix('.lock')
with open(lock_file, 'w') as lock:
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    # Process file
```

## Development Guidelines

### Adding a New Pipeline Stage
1. Create `pipelines/pX_stagename.py`
2. Follow existing stage patterns (config loading, error handling, logging)
3. Add to pipeline flow documentation
4. Update `run_pipeline.py` if needed

### Extending Schemas
1. Add new fields to existing Pydantic models in `schema/`
2. Update prompt templates in `docs/fields_prompt*.md` if needed
3. Update Excel output schema if new fields should appear in Excel
4. Test with sample files before full run

### Adding Utilities
- Input utilities: `utils/input_utils/INPUT_*.py`
- Output utilities: `utils/output_utils/OUTPUT_*.py`
- Analysis scripts: `scripts/`
- Include comprehensive docstring at top of file

### Code Style
- Follow existing patterns and conventions
- Always include function docstrings
- Use type hints where appropriate
- Handle errors gracefully with logging
- Support parallel processing where applicable

## Current Status

### Completed Features
- Full pipeline (p0-p5) operational
- Multi-method PDF extraction with intelligent OCR
- Schema-driven LLM extraction with validation
- Parallel processing support (p2, p3, p4)
- Performance monitoring and cost tracking
- Comprehensive error handling and retry logic
- Unicode/encoding issue handling
- Excel output generation with proper formatting

### Known Limitations
- Non-salary schema split into 3 parts (by design for performance)
- Excel cell size limit (32,767 chars) requires truncation
- API rate limits require multiple keys for large batches
- OCR quality varies by document quality

### Active Development Areas
- Performance optimization
- Quality improvement for edge cases
- Schema refinements based on extraction results

### Technical Debt
- Some legacy code paths (old_flow vs new_flow)
- Test coverage could be expanded
- Some hardcoded paths that should use config.yaml

## Common Tasks

### Processing New CAOs
1. Run p0_webscraping.py to download PDFs
2. Run p2_extract.py to extract text
3. Run p3_llmExtraction.py for raw extraction
4. Run p4_analysis.py for schema validation
5. Run p5_excel_creation.py for final Excel output

### Debugging Extraction Issues
- Check `outputs/logs/failed_files_*.txt` for failed files
- Review `monitoring/performance_logs/` for performance data
- Enable DEBUG mode in p2_extract.py for detailed logs
- Use `scripts/analyze_single_pdf.py` for single file analysis

### Handling API Errors
- Check API quota in monitoring logs
- Rotate to different API key
- Review retry logs for persistent failures
- Adjust retry parameters if needed

### Optimizing Performance
- Use parallel processing (p2, p3, p4)
- Monitor performance logs for bottlenecks
- Adjust `total_processes` based on resources
- Use multiple API keys to distribute load

