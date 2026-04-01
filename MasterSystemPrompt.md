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
                                                     ↑
                                    [optional] scripts/validation/validate_extraction.py
```

**Stage 1 (p1)**: Web scraping - Downloads PDFs using Selenium, extracts every second PDF link (indices 0, 2, 4, 6...) and merges all PDFs from the same main link/page into a single file (saved with the first PDF's name), organizes by CAO number; uses `inputs/excel/CAO_Frequencies_2014.xlsx` to skip already-handled CAOs and defaults to writing into `inputs/pdfs/input_pdfs_extra/`
**Stage 2 (p2)**: PDF extraction - Multi-method text extraction (PyPDF2 + pdfplumber + OCR)
**Stage 3 (p3)**: LLM extraction - Raw data extraction using Google Gemini API
**Stage 4 (p4)**: Analysis - Schema-driven structured extraction (salary + non-salary)
**Stage 5 (p5)**: Excel creation - Merges results and creates final Excel outputs; adds CAO metadata and dates from `extracted_cao_info.csv`; properly handles NaN values in date fields

### Data Flow
- **Input**: PDFs in `inputs/pdfs/input_pdfs/[CAO_NUMBER]/`
- **Intermediate**: Parsed markdown/JSON in `outputs/parsed_pdfs/`, LLM extracted JSON in `outputs/llm_extracted/`
- **Output**: Schema-validated JSON in `outputs/llm_analysis/` (salary + non-salary parts in `.../gen_bon_wag_pen_ter`, `.../lea_ove_tra`, `.../hom_con_saf_chi_ai_fri`), Excel files in `outputs/excel/`

**Important**: Files are organized by CAO number folders. Multiple files can have the same filename but exist in different CAO folders (e.g., `10/file.pdf` and `1536/file.pdf`). File matching always requires both filename AND CAO number to uniquely identify a file.

### Excel Analysis Extensions
- Salary descriptives/plots now use dynamic slot detection from `salary_<k>_*` columns instead of a fixed slot cap.
- Salary long-table construction uses subset-first slot extraction and single concat to reduce peak RAM.
- Latest CAO forward-fill logic for analysis plots is file/version based (per-CAO active range) and avoids arbitrary one-row-per-CAO-year selection.
- Forward-fill is limited to state/context selection; event variables are not forward-filled.
- `convert_salary_to_monthly` (analysis_utils) normalizes **daily** pay, compact **`d`**, and **offshore day** phrasing (`offshore day`, trailing `offshore da`, `per offshore`…) via the same `amount × 5 workdays × 4.33` rule. **Hourly** detection excludes **`N-hour` duration** units (e.g. `3-hour activity`) so they are not treated as €/h. All normalized monthly values are **rounded to 2 decimals**; amounts pass through `coerce_salary_amount_scalar` (EU `2.230,91` vs US `2,230.91`). Salary diagnostics/regression CSVs under `outputs/analysis/` use `sep=';'` and **`decimal=','`** to reduce Excel locale misreads of long float strings.
- Salary increase analytics are standardized with three event-level series:
  - `increase_diff_only` (derived from consecutive normalized monthly amounts within a single original row; **both** consecutive events must satisfy `analysis_monthly_band_ok`),
  - `increase_csv_only` (reported in CSV),
  - `increase_merged_pref_csv` (CSV preferred, derived fallback, then NaN if `analysis_monthly_band_ok` is False).
- **Monthly band**: normalized `amount_monthly` must lie between the NL statutory gross monthly minimum for `salary_start_date` (`conf/nl_statutory_minimum_monthly_gross_eur.csv`, 1 Jan schedule; `scripts/excel_analysis/nl_minimum_wage_monthly.py`) and `SALARY_ANALYSIS_MONTHLY_CAP_EUR` in `analysis_utils.py`. Shared vectorized check: `compute_analysis_monthly_floor_and_band_ok` in `salary_increase_derivation.py` (also used for band-aligned salary **level** plots). Diagnostics append band failures; `derive_salary_increase_series` returns `band_summary`. `descriptives_salary.py` logs counts, writes `outputs/analysis/salary_monthly_band_summary.csv`, and adds section `salary_monthly_band` to `01_sample_overview`. **`descriptives_salary_plots.py`** refreshes the same `salary_monthly_band_summary.csv` and prints the band line when plots are run without the full descriptives workbook. Band summary shares use **`n_conversion_ok`** as denominator. Salary **level** trend PNG `salary_amount_monthly_eur_band_eligible_by_salary_year.png` uses the same normalization + band as increases (boxplot fliers hidden on that figure). **`outputs/analysis/salary_plot_years_dropped.csv`** logs years dropped from salary plots when `n < MIN_OBS_PER_YEAR` (default 10), with matching figure footnotes plus an additional low-n included-years note for variance interpretation. Spaghetti plot black line = mean over **all** dedup-valid band-eligible CAOs per year, not only highlighted trajectories.
- Increase trend/regression aggregation uses observed event rows only (event-time semantics), except `salary_increase_percent_by_salary_year` in `descriptives_salary_plots.py`: regular view uses contract-cohort mean-of-means from `increase_merged_pref_csv` by contract start year (contract = `cao_number + file_name`), while latest view forward-fills each CAO's file-mean state across years (excluding years before first observed file).
- Derived increase QA artifacts are persisted in `outputs/analysis/` as semicolon-separated CSVs: `salary_increase_events_derived.csv`, `salary_increase_conversion_diagnostics.csv` (always emitted by salary descriptives), and `salary_increase_csv_vs_diff_comparison.csv` (CSV vs diff comparison with `abs_diff_gt_0_1` among other columns).
- Non-salary analysis and plots use document-type filtering only: exclude `protocol`; keep `full_cao_original` and `full_cao_update`. Topic lists remain available on raw-metadata tabs (e.g. document type / updated topics) but do not gate the analysis sample.
- Non-salary **numeric** trend figures (`descriptives_non_salary_plots.py`, `outputs/analysis/figures/numeric/`): each variable is plotted on its own subplot with a canonical unit. `normalize_for_plot` in `scripts/excel_analysis/non_salary_unit_normalization.py` maps `*_value` + `*_unit` to that scale (contract/overtime in hours/week; vacation and training in hours/year; sick-pay duration in weeks; sick-pay continuation and pension employee contribution in 0–100%; retirement age in years). Rows that cannot be converted (missing/blank/`0` unit where required, EUR, ambiguous formula-like text, non-scalar tiered continuation prose, or implausible conversion outputs) are dropped from aggregation. Vacation/training apply clear-outlier caps post-conversion to remove obvious artifacts while keeping broad valid coverage. Numeric plots are emitted in both **mean** and **median** variants (`_median` suffix for median files), and latest-view pension/training charts annotate first/last points for the retirement-age series.
- Regression outputs: `scripts/excel_analysis/salary_increase_regression.py` writes `outputs/analysis/salary_regression_event_level.csv`, `salary_regression_transition_level.csv`, and **`salary_regression_fit_metrics.csv`** (per-model **`r2`**, **`r2_within`**, **`adj_r2`**, **`adj_r2_within`**, **`rmse`**, formula, ref year, n_obs, clusters). Plotting lives in **`scripts/excel_analysis/salary_regression_plotting.py`**; figures go to **`outputs/analysis/figures/salary_regression/`** (event: two-panel coef plot + interactions-only companion per outcome; transition: year coefficients for `delta_file_mean_increase`). **Shaded bands** = approximate **95% CI** (±1.96×SE) from **CRV1** vcov; legends label the fill **`95% CI`**; panel B uses SE for **Var(β_NF + β_NF×y)**. Bottom captions reserve margin so axis labels do not overlap notes. **`add_yearly_variance_layer`** in `descriptives_salary_plots.py`: **`hide_boxplot_fliers=True`** sets **`showfliers=False`**; otherwise fliers are shown. Salary increase axis conventions are now fixed in the plotting functions: single-series derived/merged/csv increase plots and `salary_increase_percent_by_salary_year` use **[-2, 10]**, and `salary_increase_series_comparison_by_year` uses **[0, 4]**. For `salary_increase_percent_by_salary_year`, regular view shows contract-cohort mean-of-means of `increase_merged_pref_csv` and latest view shows forward-filled CAO file-mean state by year; both use `Contract start year` on x-axis and `Average increase (%)` on y-axis. Coefficient tables use **`Coefficient`**, **`formula`**, **`ref_year`**, **`se_invalid`**, standard inference columns, **`outcome`** (event only), sample/cluster counts; sep `;`, `decimal=','`. Reference year is the minimum factor level kept after sparse-year drops. Wide salary CSV load uses header-derived **`usecols`**; scoped **`DtypeWarning`** suppression on the subset read when needed.
- A lightweight output validation script is available at `scripts/excel_analysis/validate_analysis_outputs.py`.

### Key Components
- **Configuration**: Centralized in `conf/config.yaml` - all paths and settings
- **Schemas**: Pydantic models in `schema/` for validation (salary_schema.py, non_salary_schema.py, excel_output_schema.py)
- **Monitoring**: Performance tracking in `monitoring/monitoring_3_1.py`
- **Utilities**: Input/output management in `utils/`, analysis scripts in `scripts/`
- **Validation**: `scripts/validation/validate_extraction.py` — LLM-based validation of extraction outputs (hallucination, completeness, accuracy) using `gemini-flash-latest` with per-CAO cached reports

## Code Structure

### Directory Organization
- **pipelines/**: Main pipeline stages (p1-p5)
- **scripts_pipeline_helper/**: Helper scripts directly used by pipeline stages
  - **p1_p2/**: Helpers used by p1_webscraping.py and p2_extract.py (OUTPUT_tracker.py)
  - **p3_p4/**: Shared helpers for p3 and p4 (retry_error_classification.py - error classification utilities)
  - **p3/**: Stage 3 specific helpers (retry_logic.py - p3 retry logic)
  - **p4/**: Stage 4 specific helpers (merge_split_salary.py, retry_logic.py - p4 retry logic)
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
- Pipeline helpers: 
  - Shared: `scripts_pipeline_helper/p3_p4/retry_error_classification.py`
  - p3-specific: `scripts_pipeline_helper/p3/retry_logic.py`
  - p4-specific: `scripts_pipeline_helper/p4/retry_logic.py`
  - p1_p2: `scripts_pipeline_helper/p1_p2/OUTPUT_tracker.py`
- Utility scripts: `INPUT_*.py` (input utils), `OUTPUT_*.py` (output utils)
- Schema files: `salary_schema.py`, `non_salary_schema.py`, `excel_output_schema.py`

## Key Technical Decisions

### PDF Link Filtering and Merging (p1)
**Why**: Website contains duplicate PDF links (2 links per PDF file), and multiple PDFs per main link/page should be combined
**Approach**: Extract every second PDF link (indices 0, 2, 4, 6...), group PDFs by main link URL, download to temporary files, merge using PyPDF2, save with first PDF's name
**Rationale**: Avoids duplicate downloads, combines related PDFs from same page into single file for easier processing

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
**Approach**: Intelligent error classification, synchronized retry logic with exponential backoff (2.1^attempt), adaptive retry (adjust temp/top_p/top_k on attempts 4-5), compact schema retries (p4 attempts 5-6), super compact schema retries (p4 attempt 10), file locking, comprehensive logging
**Error Classification**:
- **Request problems** (truncated, incomplete JSON, empty response): Increment to next attempt, potentially changing parameters
- **External errors** (503, 500, connection reset, per-minute quota 429): Retry with same attempt number, wait 15 minutes each time, unlimited retries until max_retries
- **Daily quota errors**: Exit gracefully without retrying
- **Schema complexity errors** (p4 only): Fatal error, don't retry (schema needs simplification)
**Synchronized Retry Parameters** (p3 and p4):
- Quota delay buffer: 2-6 minutes (2 + min(attempt, 4))
- Per-minute quota additional wait: 150 seconds
- Empty response additional wait: +120 seconds
- Debug logging: Enabled in calculate_quota_retry_delay
**Rationale**: Robust recovery from transient failures, prevents data loss, intelligent handling of different error types

### Split Extraction Retry Strategy (p3, attempts 5, 7)
**Why**: Some files produce outputs too long for single API call, causing truncation after all regular retries fail
**Approach**: 
- Attempts 0, 3, 4: Unified extraction with single schema (all fields together) - attempts 1, 2 removed as duplicates/user request
- Attempts 5, 7: Split extraction into two separate calls (attempt 6 removed as duplicate of 5):
  - Salary-only extraction (wage_information field only)
  - Non-salary extraction (all other fields)
  - Results merged back into unified format matching original schema
- Partial success caching: If salary succeeds but non-salary fails (or vice versa), successful part is cached and only failed part is retried on next attempt
**Parameters**: 
- Attempt 5: Same parameters as attempt 0 (original settings)
- Attempt 7: Same parameters as attempt 3 (+0.1 adjustment)
**Rationale**: Smaller schemas reduce output length, allowing extraction of large files that would otherwise fail. Caching prevents re-extracting successful parts.
**Failed CAO Saving**: Files that fail all retry attempts are saved to `performance_logs/llm_extraction/failed_cao_numbers/[cao_number]/[filename]_failed.txt` and automatically skipped in future runs.

### Salary Extraction Retry Strategy (p4, attempts 0, 2, 4, 5, 6, 10)
**Why**: Large CAO files with extensive salary tables can exceed max_output_tokens (65536), causing truncation even with compact schema.
**Approach** (no split extraction – compact → super compact directly):
- **Attempts 0, 2, 4**: Regular extraction with SalaryExtractionSchema (full schema with table_label)
  - Attempt 0: Original parameters (temp=0.0, top_p=0.1, top_k=1)
  - Attempt 2: Adjusted parameters (temp=0.1, top_p=0.2, top_k=0.9)
  - Attempt 4: Adjusted parameters (temp=0.2, top_p=0.3, top_k=0.8)
  - If truncation occurs after attempt 4 → extends to attempts 5-6 (compact schema)
- **Attempts 5-6**: Compact schema extraction (SalaryExtractionSchemaCompact)
  - Removes table_label, uses abbreviated unit field, uses 2-letter field names
  - Field names: sd, ed, am, un, ip, hp, nt, jg, st, wr, ie, ag, eu, fh, pe, ht, hi, tl, rn, si (salary_information)
  - **IMPORTANT**: In compact schema, holiday_incl moved from SalaryPoint (hp) to SalaryRow (hi) - affects Excel output format
  - Attempt 5: Original parameters; Attempt 6: Adjusted (temp=0.3, top_p=0.4, top_k=0.7)
  - If truncation occurs after attempt 6 → extends to attempt 10 (super compact schema)
  - Files in truncated folder → automatically start at attempts 5-6, extend to 10 if needed
- **Attempt 10**: Super compact schema extraction (SalaryExtractionSchemaSuperCompact)
  - Minimal fields: sd, am, un, ip, jg, st, wr, ag, eu, pe, tl, si (no ed, nt, ie, fh, ht, hi, rn)
  - Uses SALARY_PROMPT_SUPER_COMPACT
  - Adjusted parameters (temp=0.3, top_p=0.4, top_k=0.7)
  - Files in truncated_2 or truncated_3 folder → automatically start at attempt 10
- **File handling**:
  - Files in truncated folder → retry with attempts 5-6 (compact), extend to 10 (super compact) if needed
  - Files in truncated_2/3 folder → retry with attempt 10 (super compact) directly
  - Files in truncated_4 folder → skipped (all attempts exhausted including super compact)
**Rationale**: Graduated approach – full schema → compact → super compact. No split extraction.

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
- **Regular schema** (`salary_schema.py`): Full schema, used for attempts 0, 2, 4. Uses SALARY_PROMPT.
- **Compact schema** (`salary_schema_compact.py`): Reduced schema (no table_label, 2-letter field names), used for attempts 5-6. **IMPORTANT**: holiday_incl moved from SalaryPoint to SalaryRow - affects Excel format. Uses SALARY_PROMPT_COMPACT.
- **Super compact schema** (`salary_schema_super_compact.py`): Minimal fields only, used for attempt 10. Uses SALARY_PROMPT_SUPER_COMPACT.

### Non-Salary Schema (`schema/non_salary_schema.py`)
Split into 3 parts for performance:
- **Part1**: GeneralInfo, BonusesInfo, WageScalesInfo, PensionInfo, TerminationInfo
- **Part2**: LeaveInfo, OvertimeInfo, TrainingInfo
- **Part3**: HomeofficeInfo, ContractTypeInfo, SafetyInfo, ChildcareInfo, AIInfo, FringeBenefitsInfo

Each info class contains structured fields (Amount, AmountRange, booleans, strings, lists) representing specific CAO provisions.

**Parental and care leave (Work and Care Act)**:
- **Two booleans**: `parental_statutory_ref` (CAO mentions statutory law and is largely in line), `parental_exceptions` (CAO states exceptions). Same pattern for care: `care_statutory_ref`, `care_exceptions`. If statutory_ref true and exceptions false, omit eligibility/detail sub-fields; when exceptions true, fill them.
- **Eligibility**: `parental_eligibility_present`, `parental_min_tenure` (employment tenure with employer), `parental_min_contract_length` (contract-duration threshold, e.g. contracts ≤1 year). Do not conflate tenure with contract length.
- **Note**: `parental_note` captures other parental-leave text (pension breaks, consequences after resuming, ZW/insurance consequences); eligibility fields capture only who is eligible / from when.

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
- **Validation**: `scripts/validation/validate_extraction.py` invokes `gemini-flash-latest` for quality scoring
- **Input**: Markdown files uploaded directly (not text strings)
- **Output**: JSON responses validated against Pydantic schemas

### API Key Management
- Multiple keys in `.env`: `GOOGLE_API_KEY1`, `GOOGLE_API_KEY2`, etc.
- Key rotation via `--key_number` parameter
- Parallel processes use different keys to distribute load

### Error Handling & Retry

**Shared Error Classification** (`scripts_pipeline_helper/p3_p4/retry_error_classification.py`):
- **Request problems**: Timeout, truncation, incomplete JSON, empty response → Increment attempt, adjust parameters
- **External errors**: Service unavailable (503), internal error (500), connection reset, per-minute quota (429) → Retry same attempt, wait 15 minutes
- **Daily quota errors**: Per-day quota limit → Exit gracefully, no retry
- **Schema complexity errors** (p4 only): Too many states, constraint violations → Fatal error, don't retry

**p3 (LLM Extraction)** - `scripts_pipeline_helper/p3/retry_logic.py`:
- **Unified extraction (attempts 0, 3, 4)**: Single schema extraction with all fields - attempts 1, 2 removed as duplicates/user request
  - Exponential backoff: 2.1^attempt (synchronized with p4)
  - Adaptive retry: Adjust temperature/top_p/top_k on attempts 3-4
  - Failure-aware guidance: Detect LLM-controllable errors (truncated JSON, empty responses), provide retry instructions
  - Quota delay buffer: 2-6 minutes, per-minute quota wait: 150 seconds, empty response wait: +120 seconds
- **Split extraction (attempts 5, 7)**: Two separate extractions for salary and non-salary - attempt 6 removed as duplicate of 5
  - Only triggered after attempts 0, 3, 4 fail
  - Partial success caching: Successful parts cached, only failed parts retried
  - Results merged back into unified format matching original schema
- **Failed CAO saving**: Files that fail all retry attempts are saved to `performance_logs/llm_extraction/failed_cao_numbers/[cao_number]/[filename]_failed.txt` and automatically skipped in future runs

**p4 (Analysis)** - `scripts_pipeline_helper/p4/retry_logic.py`:
- **Regular extraction (attempts 0, 2, 4)**: Full schema with all fields including table_label - attempts 1, 3 removed as duplicates/user request
  - Adaptive parameter adjustment on attempts 2, 4
  - Extends to compact schema if truncation occurs after attempt 4
  - Quota delay buffer: 2-6 minutes, per-minute quota wait: 150 seconds, empty response wait: +120 seconds
- **Compact schema (attempts 5-6)**: Reduced schema (no table_label, abbreviated units) - attempt 7 removed as duplicate of 6
  - Files in truncated folder automatically start here
  - Extends to split extraction if truncation occurs after attempt 6
- **Split extraction (attempt 8)**: Extract in two halves by jobgroup boundaries - attempt 9 removed as duplicate of 8
  - First half: Extract approximately first 50% of salary rows, completing any jobgroup that is started
  - Second half: Extract remaining jobgroups with first half results as context, merge results
  - Files in truncated_2 folder automatically start here
  - Files in truncated_4 folder are skipped (all attempts exhausted)
- **Super compact schema (attempt 10)**: Minimal fields only - attempt 11 removed as duplicate of 10
  - Files in truncated_3 folder automatically start here
- File locking: Prevents duplicate processing across parallel processes
- **PAID_MODE / PAID_MAX_SECONDS** (top of p4_analysis.py): When PAID_MODE is True, most retry/split/lock delays are capped at PAID_MAX_SECONDS (e.g. 5 s); full waits are kept for service_unavailable, quota, timeout (not truncation), and wait_until_reset. Delay between extended attempts (5 to 6, 6 to 8, 8 to 10) is 180 s when not paid, capped when paid. Default PAID_MODE = False (free-tier pacing).

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

### Extraction Validation (scripts/validation/)
- **Purpose**: LLM-based validation of salary/non-salary extraction outputs against source markdown
- **Inputs**: Extracted JSON + parsed markdown; schema definitions (field names + descriptions)
- **Outputs**: Per-file validation report (hallucination, completeness, accuracy, temporal validity), summary CSV
- **Sampling**: One file per CAO number, chosen at random (--seed for reproducibility)
- **Model**: `gemini-flash-latest` with cached results saved under `outputs/validation/{salary|non_salary}/{cao_number}/validation_<filename>.json`
- **Cache control**: Cached validations are skipped automatically; pass `--force` to re-run and overwrite. Summary CSVs merge cached and newly generated results each execution.
- **Usage**: `python scripts/validation/validate_extraction.py --type salary|non_salary|both --seed 42`

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
- Comprehensive error handling and retry logic with intelligent error classification
  - **Shared error classification**: Request problems vs external errors vs daily quota vs schema complexity
  - **Synchronized retry parameters**: Quota delay buffer (2-6 minutes), per-minute quota wait (150 seconds), empty response wait (+120 seconds), debug logging
  - **p3**: Unified extraction retries (attempts 0, 3, 4) with adaptive parameter adjustment, split extraction retries (attempts 5, 7) with partial success caching, exponential backoff (2.1^attempt), failed CAO number saving for skipped files
  - **p4**: Regular extraction (attempts 0, 2, 4), compact schema retries (attempts 5-6), super compact schema retry (attempt 10), exponential backoff (2.1^attempt); PAID_MODE caps most delays at PAID_MAX_SECONDS (full wait for service_unavailable, quota, timeout, wait_until_reset)
- Unicode/encoding issue handling
- Excel output generation with proper formatting
- Intelligent file handling: truncated folder files retry with compact schema, truncated_2 folder files retry with split extraction, truncated_3 folder files retry with super compact schema, truncated_4 folder files skipped
- Failed CAO number saving (p3): Files that fail all retry attempts are saved to `performance_logs/llm_extraction/failed_cao_numbers/[cao_number]/` and automatically skipped in future runs
- **Date format handling**: Correct parsing of DD/MM/YYYY (CAO metadata dates) and YYYY-MM-DD (contract/salary timeline dates) in all descriptives and analysis scripts
- **Memory diagnostics**: Analysis scripts print checkpoint memory usage (`[MEM] ... MB`) for wide/long/latest DataFrames

### Known Limitations
- Non-salary schema split into 3 parts in p4 (by design for performance)
- p3 uses split extraction (salary vs non-salary) for attempts 5, 7 when unified extraction fails (attempts 1, 2, 6 removed as duplicates)
- p4 uses split extraction (first half/second half by jobgroups) for attempt 8 when compact schema fails, and super compact schema for attempt 10 when split extraction fails (attempts 1, 3, 7, 9, 11 removed as duplicates/user request)
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
