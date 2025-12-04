# Master System Prompt - CAO Data Extraction Project

## Project Context

**Purpose**: Extract structured data from Dutch Collective Labor Agreements (CAOs) - legal documents that define employment terms, salaries, benefits, and working conditions.

**Domain**: Dutch labor law and employment contracts. CAOs are published as PDFs by the government.

**Scale**: Processes 1,580+ PDF documents from uitvoeringarbeidsvoorwaardenwetgeving.nl

**Output**: Structured Excel files with salary and non-salary information extracted using AI/LLM processing.

## Architecture Overview

### Pipeline Flow
```
p1_webscraping → p2_extract → p3_llmExtraction → p4_analysis → p5_excel_creation
```

**Stage 1 (p1)**: Web scraping - Downloads PDFs using Selenium, organizes by CAO number; uses `inputs/excel/CAO_Frequencies_2014.xlsx` to skip already-handled CAOs and defaults to writing into `inputs/pdfs/input_pdfs_extra/`
**Stage 2 (p2)**: PDF extraction - Multi-method text extraction (PyPDF2 + pdfplumber + OCR)
**Stage 3 (p3)**: LLM extraction - Raw data extraction using Google Gemini API
**Stage 4 (p4)**: Analysis - Schema-driven structured extraction (salary + non-salary)
**Stage 5 (p5)**: Excel creation - Merges results and creates final Excel outputs; adds CAO metadata and dates from `extracted_cao_info.csv`; properly handles NaN values in date fields

### Data Flow
- **Input**: PDFs in `inputs/pdfs/input_pdfs/[CAO_NUMBER]/`
- **Intermediate**: Parsed markdown/JSON in `outputs/parsed_pdfs/`, LLM extracted JSON in `outputs/llm_extracted/`
- **Output**: Schema-validated JSON in `outputs/llm_analysis/` (salary + non-salary parts in `.../gen_bon_wag_pen_ter`, `.../lea_ove_tra`, `.../hom_con_saf_chi_ai_fri`), Excel files in `outputs/excel/`

**Important**: Files are organized by CAO number folders. Multiple files can have the same filename but exist in different CAO folders (e.g., `10/file.pdf` and `1536/file.pdf`). File matching always requires both filename AND CAO number to uniquely identify a file.

### Key Components
- **Configuration**: Centralized in `conf/config.yaml` - all paths and settings
- **Schemas**: Pydantic models in `schema/` for validation (salary_schema.py, non_salary_schema.py, excel_output_schema.py)
- **Monitoring**: Performance tracking in `monitoring/monitoring_3_1.py`
- **Utilities**: Input/output management in `utils/`, analysis scripts in `scripts/`

## Code Structure

### Directory Organization
- **pipelines/**: Main pipeline stages (p1-p5)
- **scripts_pipeline_helper/**: Helper scripts directly used by pipeline stages
  - **p1_p2/**: Helpers used by p1_webscraping.py and p2_extract.py (OUTPUT_tracker.py)
  - **p4/**: Stage 4 specific helpers (merge_split_salary.py)
- **schema/**: Pydantic data models and validation schemas
- **utils/**: Standalone utility scripts (not directly used by pipeline)
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
- Pipeline stages: `p1_webscraping.py`, `p2_extract.py`, `p3_llmExtraction.py`, `p4_analysis.py`, `p5_excel_creation.py`
- Pipeline helpers: `scripts_pipeline_helper/p1_p2/OUTPUT_tracker.py`, `scripts_pipeline_helper/p4/merge_split_salary.py`
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
**Approach**: Exponential backoff, adaptive retry (adjust temp/top_p/top_k on attempts 4-5), compact schema retries (p4 attempts 6-8), split extraction retries (p3 attempts 6-8, p4 attempts 9-10), super compact schema retries (p4 attempts 11-12), file locking, comprehensive logging
**Rationale**: Robust recovery from transient failures, prevents data loss

### Split Extraction Retry Strategy (p3, attempts 6-8)
**Why**: Some files produce outputs too long for single API call, causing truncation after all regular retries fail
**Approach**: 
- Attempts 1-5: Unified extraction with single schema (all fields together)
- Attempts 6-8: Split extraction into two separate calls:
  - Salary-only extraction (wage_information field only)
  - Non-salary extraction (all other fields)
  - Results merged back into unified format matching original schema
- Partial success caching: If salary succeeds but non-salary fails (or vice versa), successful part is cached and only failed part is retried on next attempt
**Parameters**: 
- Attempt 6: Same parameters as attempt 1 (original settings)
- Attempt 7: Same parameters as attempt 3 (original settings)
- Attempt 8: Same parameters as attempt 4 (+0.1 adjustment)
**Rationale**: Smaller schemas reduce output length, allowing extraction of large files that would otherwise fail. Caching prevents re-extracting successful parts.

### Salary Extraction Retry Strategy (p4, attempts 1-12)
**Why**: Large CAO files with extensive salary tables can exceed max_output_tokens (65536), causing truncation even with compact schema and split extraction
**Approach**:
- **Attempts 1-5**: Regular extraction with SalaryExtractionSchema (full schema with table_label)
  - Attempt 1-2: Original parameters (temp=0.0, top_p=0.1, top_k=1)
  - Attempt 3: Adjusted parameters (temp=0.1, top_p=0.2, top_k=0.9)
  - Attempt 4: Adjusted parameters (temp=0.2, top_p=0.3, top_k=0.8)
  - Attempt 5: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
  - If truncation occurs after attempt 4 → extends to attempts 6-8 (compact schema)
- **Attempts 6-8**: Compact schema extraction (SalaryExtractionSchemaCompact)
  - Removes table_label, uses abbreviated unit field, uses 2-letter field names to minimize JSON output size
  - Field names: sd (start_date), ed (end_date), am (amount), un (unit), ip (inc_pct), hp (holiday_incl in point), nt (note), jg (jobgroup), st (step), wr (worker), ie (is_entry), ag (age_group), eu (education), fh (ft_hours), pe (permanency), ht (hours_type), hi (holiday_incl in row), tl (timeline), rn (row_note), si (salary_information)
  - **IMPORTANT**: In compact schema, holiday_incl moved from SalaryPoint (hp) to SalaryRow (hi) - affects Excel output format
  - Uses SALARY_PROMPT_COMPACT (same as SALARY_PROMPT but with field abbreviation section and "si" instead of "salary_information" in JSON output)
  - Attempt 6: Original parameters (temp=0.0, top_p=0.1, top_k=1)
  - Attempt 7: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
  - Attempt 8: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
  - If truncation occurs after attempt 7 → extends to attempts 9-10 (split extraction)
  - Files in truncated folder → automatically start at attempt 6, extend to 9-10, 10-11 if needed
- **Attempts 9-10**: Split extraction by jobgroup boundaries
  - Attempt 9 (first half): Extract approximately first 50% of salary rows, completing any jobgroup that is started (jobgroups not split)
  - Attempt 10 (second half): Extract remaining jobgroups with first half results as context, merge results
  - Uses SalaryExtractionSchemaSplit (same as compact but optimized for split extraction)
  - Prompt for attempt 10 includes anti-repetition rules: notes shared across jobgroups use reference format instead of repetition
  - Files in truncated_2 folder → automatically start at attempt 9
  - If truncation occurs after attempt 9 → extends to attempts 10-11 (super compact schema)
- **Attempts 11-12**: Super compact schema extraction (SalaryExtractionSchemaSuperCompact)
  - Removes all optional metadata fields, keeping only essential salary data
  - Field names: sd (start_date), am (amount), un (unit), ip (inc_pct), jg (jobgroup), st (step), wr (worker), ag (age_group), eu (education), pe (permanency), tl (timeline), si (salary_information)
  - Removed fields: ed (end_date), nt (note), ie (is_entry), fh (ft_hours), ht (hours_type), hi (holiday_incl), rn (row_note)
  - Uses SALARY_PROMPT_SUPER_COMPACT (adapted from SALARY_PROMPT_COMPACT with only essential fields)
  - Attempt 11: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
  - Attempt 12: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
  - Files in truncated_3 folder → automatically start at attempts 10-11
- **File handling**:
  - Files in truncated folder → retry with attempts 6-8 (compact), automatically extend to 9-10 (split), 10-11 (super compact) if needed
  - Files in truncated_2 folder → retry with attempts 9-10 (split extraction) directly, may extend to 10-11
  - Files in truncated_3 folder → retry with attempts 10-11 (super compact schema) directly
  - Files in truncated_4 folder → skipped (all attempts exhausted including super compact schema)
**Parameters**:
- Attempt 9: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
- Attempt 10: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
- Attempt 11: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
- Attempt 12: Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
**Rationale**: Graduated approach - first try with full schema, then reduce schema size (compact), split extraction for extremely large files, finally super compact schema with minimal fields for the largest files. Jobgroup boundary preservation ensures data integrity when splitting.

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

**Schema Variants**:
- **Regular schema** (`salary_schema.py`): Full schema with table_label, full field names, used for attempts 1-5. Uses SALARY_PROMPT.
- **Compact schema** (`salary_schema_compact.py`): Reduced schema (no table_label, abbreviated units, 2-letter field names), used for attempts 6-8. **IMPORTANT**: holiday_incl moved from SalaryPoint to SalaryRow (hp in point, hi in row) - affects Excel format. Uses SALARY_PROMPT_COMPACT (identical to SALARY_PROMPT except: adds "FIELD NAME ABBREVIATIONS" section, uses "si" instead of "salary_information" in JSON output example).
- **Split schema** (`salary_schema_split.py`): Same as compact, optimized for split extraction, used for attempts 9-10. Uses SALARY_PROMPT_SPLIT_ATTEMPT_9/10 (includes field abbreviations section, split extraction rules, anti-repetition rules for attempt 10).
- **Super compact schema** (`salary_schema_super_compact.py`): Minimal schema with only essential fields (no end_date, notes, is_entry, ft_hours, hours_type, holiday_incl, row_note), used for attempts 11-12. Uses SALARY_PROMPT_SUPER_COMPACT (adapted from SALARY_PROMPT_COMPACT with only essential fields).

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

### Date Format Handling
**Critical**: Different date fields use different formats and require different parsing:
- **CAO metadata dates (DD/MM/YYYY format)**: `ingangsdatum`, `expiratiedatum`, `datum_kennisgeving`
  - Format: `'01/01/2014'`, `'31/12/2014'`
  - Parsing: Must use `pd.to_datetime(..., dayfirst=True)` to correctly parse DD/MM/YYYY format
  - Source: Extracted from website metadata CSV (`extracted_cao_info.csv`)
- **Contract dates (YYYY-MM-DD format)**: `general_start_date`, `general_expiry_date`, `general_retro_start_date`, `general_retro_end_date`, `general_avv_start_date`, `general_avv_end_date`, `general_signing_date`
  - Format: `'2014-01-01'`, `'2014-12-31'` (ISO format)
  - Parsing: Use default `pd.to_datetime()` (no `dayfirst` parameter needed)
  - Source: Extracted from PDF documents by LLM
- **Salary timeline dates (YYYY-MM-DD format)**: `salary_1_start_date`, `salary_1_end_date`, etc.
  - Format: `'2014-01-01'`, `'2014-12-31'` (ISO format)
  - Parsing: Use default `pd.to_datetime()` (no `dayfirst` parameter needed)
  - Source: Extracted from PDF documents by LLM

**All descriptives and analysis scripts** (`scripts/excel_analysis/*.py`) have been updated to correctly parse dates based on their format. Generic date parsing functions check if the column is a CAO metadata date and apply `dayfirst=True` accordingly.

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

**p3 (LLM Extraction)**:
- **Unified extraction (attempts 1-5)**: Single schema extraction with all fields
  - Exponential backoff: 2^attempt seconds
  - Adaptive retry: Adjust temperature/top_p/top_k on attempts 4-5
  - Failure-aware guidance: Detect LLM-controllable errors (truncated JSON, empty responses), provide retry instructions
- **Split extraction (attempts 6-8)**: Two separate extractions for salary and non-salary
  - Only triggered after attempts 1-5 fail
  - Partial success caching: Successful parts cached, only failed parts retried
  - Results merged back into unified format matching original schema

**p4 (Analysis)**:
- **Regular extraction (attempts 1-5)**: Full schema with all fields including table_label
  - Adaptive parameter adjustment on attempts 3-5
  - Extends to compact schema if truncation occurs after attempt 4
- **Compact schema (attempts 6-8)**: Reduced schema (no table_label, abbreviated units)
  - Files in truncated folder automatically start here
  - Extends to split extraction if truncation occurs after attempt 7
- **Split extraction (attempts 9-10)**: Extract in two halves by jobgroup boundaries
  - Attempt 9: First half (complete jobgroups, don't split them)
  - Attempt 10: Second half (skip already-extracted jobgroups, merge with first half)
  - Files in truncated_2 folder automatically start here
  - Files in truncated_4 folder are skipped (all attempts exhausted)
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
4. Update `run_pipeline.py` if needed (note: current file imports non-existent `pipelines.p5_run`; add that module or adjust import before using it as an orchestrator)

### Extending Schemas
1. Add new fields to existing Pydantic models in `schema/`
2. Update prompt templates in `docs/fields_prompt*.md` if needed (legacy only; runtime prompts live in code/schemas)
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
- Full pipeline (p1-p5) operational
- Multi-method PDF extraction with intelligent OCR
- Schema-driven LLM extraction with validation
- Parallel processing support (p2, p3, p4)
- Performance monitoring and cost tracking
- Comprehensive error handling and retry logic
  - **p3**: Unified extraction retries (attempts 1-5) with adaptive parameter adjustment, split extraction retries (attempts 6-8) with partial success caching
  - **p4**: Regular extraction (attempts 1-5), compact schema retries (attempts 6-8), split extraction retries (attempts 9-10), super compact schema retries (attempts 11-12) by jobgroup boundaries with merge
- Unicode/encoding issue handling
- Excel output generation with proper formatting
- Intelligent file handling: truncated folder files retry with compact schema, truncated_2 folder files retry with split extraction, truncated_3 folder files retry with super compact schema, truncated_4 folder files skipped
- **Date format handling**: Correct parsing of DD/MM/YYYY (CAO metadata dates) and YYYY-MM-DD (contract/salary timeline dates) in all descriptives and analysis scripts

### Known Limitations
- Non-salary schema split into 3 parts in p4 (by design for performance)
- p3 uses split extraction (salary vs non-salary) for attempts 6-8 when unified extraction fails
- p4 uses split extraction (first half/second half by jobgroups) for attempts 9-10 when compact schema fails, and super compact schema for attempts 11-12 when split extraction fails
- Files in truncated_4 folder are skipped (all retry attempts exhausted, including super compact schema)
- Excel cell size limit (32,767 chars) requires truncation
- API rate limits require multiple keys for large batches
- OCR quality varies by document quality
- Max output tokens (65536) for Gemini 2.5 Flash limits single-response extraction size

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
1. Run p1_webscraping.py to download PDFs
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
