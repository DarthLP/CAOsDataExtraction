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

4. **Run the complete pipeline (if orchestrator is present):**
   ```bash
   python run_pipeline.py
   ```
   Note: `run_pipeline.py` currently imports a non-existent `pipelines.p5_run`; until that orchestrator exists, run stages individually as shown below.

## Excel Analysis Additions (Salary + Non-Salary)

- **Salary dynamic slots**: `scripts/excel_analysis/descriptives_salary.py` and `scripts/excel_analysis/descriptives_salary_plots.py` now detect salary slots dynamically from `salary_<k>_*` columns (no fixed 1-11 range).
- **Salary increase definitions** (event-level):
  - `increase_diff_only`: computed from consecutive normalized monthly salary points within one original wide row (both endpoints must pass the **monthly band** below).
  - `increase_csv_only`: direct `salary_k_increase_percent` from CSV.
  - `increase_merged_pref_csv`: CSV if present, else diff-based value (not masked on `analysis_monthly_band_ok`; band-only plots and regressions filter explicitly).
- **Monthly analysis band** (relaxed statutory floor + cap on normalized EUR/month):
  - Floor reference: `conf/nl_statutory_minimum_monthly_gross_eur.csv` — Dutch gross monthly minima on **1 January** per year (1990–2025; dates before 1990 clamp to 1990; after last row use 2025). Lookup uses `salary_start_date`. The full statutory value is stored in `analysis_monthly_floor_eur`; band eligibility requires normalized monthly ≥ `SALARY_ANALYSIS_MONTHLY_FLOOR_RELATIVE_MIN` × that floor (default **0.9**, i.e. up to 10% below the statutory minimum is allowed).
  - Cap: `SALARY_ANALYSIS_MONTHLY_CAP_EUR` in `scripts/excel_analysis/analysis_utils.py` (default 50,000).
  - Event columns: `analysis_monthly_floor_eur`, `analysis_monthly_band_ok`, `analysis_drop_reason_band`. Youth / sub-minimum steps more than the allowed margin below the statutory monthly minimum are excluded from the band.
- **Normalization rule**: diff derivation uses canonical `convert_salary_to_monthly` in `scripts/excel_analysis/analysis_utils.py` (monthly, 4-weekly, hourly, weekly, **daily** / compact **`d`** / **offshore day** variants, annual); **4-weekly** amounts use **× 13/12** (13 four-week pay periods per year ÷ 12 calendar months); daily-type rates use `amount × 5 × 4.33`. **Not** treated as hourly: **`N-hour` activity** slices (e.g. `3-hour activity`). Amount cells are parsed with EU/US decimal rules via `coerce_salary_amount_scalar`; monthly equivalents are **rounded to 2 decimals**; diagnostics CSVs use **`decimal=','`**. Events with invalid conversion inputs are excluded from diff derivation.
- **Salary diagnostics outputs** (semicolon-separated):
  - `outputs/analysis/salary_monthly_band_summary.csv` — single-row summary. **`n_dropped_above_cap`**: normalized monthly amount **>** `SALARY_ANALYSIS_MONTHLY_CAP_EUR` (50k). **`n_dropped_below_floor`**: below `SALARY_ANALYSIS_MONTHLY_FLOOR_RELATIVE_MIN` × NL statutory gross monthly minimum for `salary_start_date`. **`share_*` columns**: numerator over **`n_conversion_ok`** (successful unit→monthly conversion), not over all wide rows. Written by **`descriptives_salary.py`** and refreshed by **`descriptives_salary_plots.py`** when you run plots alone.
  - `outputs/analysis/salary_band_and_conversion_diagnostics.csv` — combined QA file from **`descriptives_salary_plots.py`**: `record_type` = `row_exclusion` (wide slots that never enter long + long rows failing conversion/band), `cao_summary` (CAOs with no band-eligible slot anywhere), `reason_aggregate` (global counts).
  - Other CSVs (conversion diagnostics, derived events, CSV vs diff) are emitted by **`descriptives_salary.py`** (including header-only files when empty):
  - `outputs/analysis/salary_increase_conversion_diagnostics.csv` (includes conversion failures, invalid diff pairs, and band exclusions)
  - `outputs/analysis/salary_increase_events_derived.csv`
  - `outputs/analysis/salary_increase_csv_vs_diff_comparison.csv` (CSV vs derived diff increases where both exist; includes `abs_diff_gt_0_1`)
- **Salary FE regression** (`scripts/excel_analysis/salary_increase_regression.py`, helpers in `scripts/excel_analysis/salary_regression_plotting.py`): writes coefficient tables `outputs/analysis/salary_regression_event_level.csv` and `outputs/analysis/salary_regression_transition_level.csv`, fit summary `outputs/analysis/salary_regression_fit_metrics.csv` (**`r2`**, **`r2_within`**, adjusted R² variants, **`rmse`**, one row per model), and PNGs under **`outputs/analysis/figures/salary_regression/`** — for each event outcome a **two-panel** coefficient plot (year path for `new_file_effect=0`; implied `new_file_effect` by year) plus a separate **`_nf_year_interactions_only`** figure; for transitions, **`salary_regression_transition_delta_file_mean_increase.png`**. **Shaded regions** are **approximate 95% CIs** (estimate ± 1.96×SE) from the **CRV1 cluster-robust** variance matrix (`cao_number`); legends use the short label **`95% CI`** (module docstring in `salary_regression_plotting.py` spells out the construction). Captions under figures reserve extra bottom margin so x-axis labels do not overlap notes. All use sep `;` and `decimal=','` for CSVs. Coefficient rows include **`Coefficient`**, **`formula`**, **`ref_year`**, **`se_invalid`**, inference columns, **`n_obs`**, **`n_clusters_cao`**, and **`outcome`** (event-level only). **`increase_merged_pref_csv`** event and transition models restrict to **`analysis_monthly_band_ok`** so estimates stay on the band-eligible sample. Wide salary load uses header-based **`usecols`** to limit RAM; remaining mixed-type reads on selected columns are silenced with a scoped **`DtypeWarning`** filter.
- **Salary descriptives workbook tabs added**:
  - `01_sample_overview` includes section `salary_monthly_band` (eligible / dropped below floor / above cap / missing date).
  - `09_increase_diff_only`
  - `10_increase_merge_vs_csv`
- **Salary figures** (under `outputs/analysis/figures/salary/`):
  - `salary_amount_monthly_eur_band_eligible_by_salary_year.png` — **EUR/month** after `convert_salary_to_monthly` (same rules as increase derivation), **band-eligible** rows only (relaxed statutory floor + analysis cap); boxplot **fliers hidden** so the axis tracks box/whisker spread.
  - `salary_amount_monthly_eur_band_eligible_by_contract_year.png` — same normalization and band; **x-axis** = contract start year (`ingangsdatum`); governing file per `(CAO, year)`; one row per **band-eligible salary slot** on that file (not a pre-aggregated mean). `salary_amount_monthly_eur_band_eligible_by_contract_year_latest_cao_view.png` uses **calendar year** on the x-axis and **every** band-eligible slot row on the **snapped** active file in `T`: after nominal forward-fill, `snap_active_table_to_band_eligible_salary_files` keeps the previous contract file until the new file has ≥1 governed band-eligible long row (same logic as `build_governed_band_eligible_slot_long` in `descriptives_salary_plots.py`).
  - `outputs/analysis/salary_plot_years_dropped.csv` — **header-only** placeholder; salary descriptive plots no longer drop years by minimum *n*.
  - `salary_ft_hours_by_contract_year.png`: boxplot outliers are hidden to reduce visual clutter.
  - **CAO-equal weighting** (descriptive salary plots): within each cohort year, each CAO has weight `1 / n_{c,y}` where `n_{c,y}` is that CAO’s row count in the final analytic frame; box stats use weighted quantiles + `matplotlib.pyplot.bxp` (not Seaborn weighted boxplots).
  - `salary_increase_percent_by_contract_year.png` (regular): merged increase by **contract start year** after **governing-file** cohort construction (`ingangsdatum` year + one governing file per `(CAO, year)`); weighted layer and cohort-local CAO count on twin axis.
  - `salary_increase_percent_by_contract_year_latest_cao_view.png`: forward-filled CAO contract-mean increase across calendar years; same y-axis family as the regular contract-year figure.
  - `salary_increase_*_by_salary_year.png` (diff / merged / CSV): **salary start year** on the x-axis; newest-file overlap; outliers hidden; fixed y-axis **[-4, 12]**.
  - `salary_increase_*_by_salary_year_latest_cao_view.png`: **Salary year** using the **same snapped** active file as latest salary level plots; **0%** imputed when there is no band-eligible non-null event in `T` on that file for that series; fixed y-axis **[-4, 12]**.
  - `salary_increase_percent_by_contract_year.png` / `_latest_cao_view.png`: fixed y-axis **[-4, 12]**.
  - `salary_increase_series_comparison_by_year.png` — three series of yearly **CAO-equal weighted** means; twin axis shows CAO counts for the merged series; fixed y-axis **[0, 6]**.
  - `salary_increase_shift_by_new_file_year.png` — **weighted** mean shifts between consecutive files (deduped at most one shift per `(CAO, new-file year)`); x-axis label **Contract start year** (same numeric values as before).
  - `salary_points_per_row_by_year.png` — mean/median **band-eligible** slot counts per wide row (not raw positive amount counts); no latest-view variant.
  - `salary_increase_spaghetti_selected_caos.png` — thin lines: highlighted top/bottom CAOs; **black line**: **CAO-equal weighted** mean of **`increase_merged_pref_csv`** on the overlap-resolved panel; **twin axis** = CAO count for the same grand-line sample per year.
- **Salary descriptive methodology (CAO-equal, paper-ready summary)**  
  Cohort construction and weights follow `scripts/excel_analysis/salary_plot_cohort_utils.py` and `descriptives_salary_plots.py`. In words:
  - **Four objects:** (1) **Nominal** active CAO **file** in calendar year `T` (forward-fill panel); for **latest salary level** (contract-calendar + salary-year) and **latest increase-by–Salary-year**, **(1b)** `snap_active_table_to_band_eligible_salary_files` replaces the nominal file with the last file that has ≥1 governed band-eligible long row for that CAO until the new contract file qualifies; (2) **Latest salary-by–Salary-year** effective slots: per `(row_id, salary_index)` on that **snapped** active file, the band-eligible observation with latest `salary_start_date` ≤ end(`T`); if that set is empty for `(CAO, T)` on a new file, **carry** the previous calendar year’s full effective slot set for that CAO until new steps start (**file-transition gap**); (3) **retained salary slots** for normal salary-start-year (newest-file overlap), normal contract cohort (governing file, all slots on that file), and **Latest contract-year salary** (calendar year on the x-axis, **all** band-eligible slots on the **snapped** active file in `T`); (4) **retained increase events** with the same overlap / governing-file rules; Latest increase-by–Salary-year uses the **snapped** file and adds **one synthetic 0%** when there is **no** band-eligible non-null event on that file in `T` (no carry-forward of past **increase** values—distinct from salary **level** gap carry).
  - **Weights:** For each figure and cohort year `y`, on the **final** row-level sample `S_y` for that figure, `n_{c,y}` = number of rows with CAO `c`, and `w_i = 1/n_{c,y}`. Then the plotted yearly mean equals the mean of per-CAO means on `S_y`. Box medians/quartiles/whiskers use **weighted quantiles** and precomputed stats passed to Matplotlib **`bxp`** (not library `boxplot(..., weights=...)`).
  - **Twin axis:** One secondary series only: count of **distinct CAOs** with ≥1 row in `S_y` at each x tick (not row counts, not cumulative unless a figure is explicitly documented that way). Latest increase plots **include** CAOs that appear only via imputed 0%.
  - **Minimum-n:** `MIN_OBS_PER_YEAR` does **not** gate salary descriptive plots; `salary_plot_years_dropped.csv` is header-only. (Non-salary plots and salary **regression** may still use their own sparsity rules.)
  - **Sanity checks:** (1) weighted mean ≡ mean of per-CAO means on `S_y`; (2) one frame + one weight column per cohort layer; (3) no duplicate active-slot keys per `(CAO, T)` for Latest salary; (4) Latest increase twin counts match CAOs in panel including 0% rows; (5) contract salary/increase use governing file only; (6) shift plot yearly value ≡ weighted mean of per-CAO shifts in that year’s `S_y`.
- **Non-salary filter semantics for analysis/plots**:
  - Exclude `protocol` always.
  - Always include `full_cao_original`, `full_cao_update`.
  - Other document types are included only when `general_updated_topics` intersects relevant topic keywords.
- **Non-salary metadata outputs**:
  - Descriptives sheets `01b_document_type`, `01c_updated_topics_top`, `01d_filter_diagnostics`
  - Plots `non_salary_document_type_distribution.png`, `non_salary_updated_topics_top10.png`
- **Non-salary numeric trend plots** (`scripts/excel_analysis/descriptives_non_salary_plots.py`): figures under `outputs/analysis/figures/numeric/` use **one subplot per variable** so each series has a correct y-axis label. Values are converted from `*_unit` strings to a canonical scale via `normalize_for_plot` in `scripts/excel_analysis/non_salary_unit_normalization.py` (contract/overtime → hours/week; vacation and training → hours/year; sick-pay duration → weeks; sick-pay continuation and pension employee contribution → %; normal retirement age → years). Rows with missing values, blank/placeholder units, EUR pension amounts, non-scalar/tiered continuation prose, and non-convertible or ambiguous unit strings are excluded from aggregation. Vacation/training normalization also applies clear-outlier caps after conversion to drop obvious extraction artifacts. Numeric outputs are now generated as both yearly **mean** and yearly **median** variants (median files use `_median` suffix), and latest-view pension/training plots annotate first/last points for the retirement-age series.

### Run analysis scripts

```bash
conda run -n caos-extract python scripts/excel_analysis/descriptives_salary.py
conda run -n caos-extract python scripts/excel_analysis/descriptives_salary_plots.py
conda run -n caos-extract python scripts/excel_analysis/descriptives_non_salary.py
conda run -n caos-extract python scripts/excel_analysis/descriptives_non_salary_plots.py
conda run -n caos-extract python scripts/excel_analysis/salary_increase_regression.py
conda run -n caos-extract python scripts/excel_analysis/validate_analysis_outputs.py
```

### Memory-safe execution

- Run heavy analysis scripts sequentially in a single process (do not start salary and non-salary analysis in parallel).
- Latest-view forward-fill selects the **nominal** active file in calendar year *T*. **Latest contract-year salary** and **latest salary-by–salary-year** apply **`snap_active_table_to_band_eligible_salary_files`** so the effective file always has ≥1 governed band-eligible long row (otherwise the prior qualifying file is carried). **Latest increase by salary year** uses the **same snapped** file. **Boolean shares** (`salary_boolean_shares_by_contract_year*.png`) use nominal forward-fill only (no snap) and **exclude wide rows with no positive coerced `salary_k_amount`** (same rule as long-build / `n_salary_points_per_row`). **FT hours** and **points-per-row** descriptive plots are **regular (contract-cohort) view only** in the salary plots script.
- Increase regression and most aggregations use observed event rows; descriptive plots use the weighted / cohort rules in `scripts/excel_analysis/salary_plot_cohort_utils.py` and the module docstring of `descriptives_salary_plots.py`.
- Salary long construction now uses subset-first slot extraction and a single concat to avoid per-slot full-frame copies.
- Non-salary plotting applies one document-type filter per plot function pass (no per-domain topic cache).
- Use runtime diagnostics in script logs (`[MEM] ... MB`) to spot memory spikes.

## Pipeline Overview

```
Web Scraping → PDF Extraction → LLM Extraction → Analysis → Excel Creation
     p1              p2              p3              p4          p5
```

1. **p1_webscraping.py** - Downloads CAO PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl using Selenium; extracts every second PDF link (indices 0, 2, 4, 6...) and merges all PDFs from the same main link/page into a single file, saved with the name of the first PDF; uses `inputs/excel/CAO_Frequencies_2014.xlsx` to decide skips and defaults to writing into `inputs/pdfs/input_pdfs_extra/`.
2. **p2_extract.py** - Multi-method PDF text extraction (PyPDF2 + pdfplumber + Tesseract OCR)
3. **p3_llmExtraction.py** - Raw data extraction using Google Gemini API with context preservation
4. **p4_analysis.py** - Schema-driven structured extraction (salary + non-salary) using Pydantic models; non-salary outputs go to `outputs/llm_analysis/non_salary/gen_bon_wag_pen_ter/`, `outputs/llm_analysis/non_salary/lea_ove_tra/`, and `outputs/llm_analysis/non_salary/hom_con_saf_chi_ai_fri/`. Supports a **PAID_MODE** flag at the top of the script: when `True`, most retry and pacing delays are capped at **PAID_MAX_SECONDS** (e.g. 5 s) for paid-tier API usage; full waits are kept for service-unavailable, per-minute quota, timeout, and quota reset. Default is `PAID_MODE = False` (free-tier pacing).
5. **p5_excel_creation.py** - Merges results and creates final Excel outputs with proper formatting

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
│       ├── llm_extraction/       # p3 performance logs
│       │   └── failed_cao_numbers/ # Failed CAO numbers (skipped in future runs)
│       └── llm_analysis/          # p4 performance logs
│           ├── max_tokens_truncated/      # First truncation (regular schema) → retry with compact
│           ├── max_tokens_truncated_2/    # Compact failed → retry with super compact
│           ├── max_tokens_truncated_3/    # (legacy) Compact/split failed → retry with super compact
│           └── max_tokens_truncated_4/    # Super compact failed → skip (all exhausted)
├── outputs/
│   ├── llm_extracted/           # LLM extracted JSON files
│   ├── llm_analysis/            # Schema-validated extraction results
│   ├── parsed_pdfs/             # Parsed PDF JSON/Markdown files
│   ├── excel/                   # Final Excel output files
│   ├── validation/              # Extraction validation reports (per-type CAO folders + summary CSV)
│   └── logs/                    # Processing logs and error reports
├── pipelines/
│   ├── p1_webscraping.py        # Web scraping
│   ├── p2_extract.py            # PDF extraction
│   ├── p3_llmExtraction.py      # LLM extraction
│   ├── p4_analysis.py           # Data analysis
│   └── p5_excel_creation.py     # Excel creation
├── scripts_pipeline_helper/     # Helper scripts directly used by pipeline stages
│   ├── p1_p2/                    # Helpers used by p1_webscraping.py and p2_extract.py
│   │   └── OUTPUT_tracker.py    # Progress tracking
│   ├── p3_p4/                    # Shared helpers for p3 and p4
│   │   └── retry_error_classification.py # Error classification utilities (request problems, external errors, daily quota)
│   ├── p3/                       # Stage 3 specific helpers
│   │   └── retry_logic.py       # p3 retry logic (error handling, quota delays, parameter adjustments)
│   └── p4/                      # Stage 4 specific helpers
│       └── retry_logic.py       # p4 retry logic (error handling, quota delays, parameter adjustments)
├── schema/
│   ├── salary_schema.py         # Salary data schema (Pydantic models) - regular schema with full field names, for attempts 0, 2, 4
│   ├── salary_schema_compact.py # Compact salary schema (no table_label, 2-letter field names) - for attempts 5-6. NOTE: holiday_incl moved from SalaryPoint to SalaryRow
│   ├── salary_schema_super_compact.py  # Super compact salary schema (minimal fields) - for attempt 10
│   ├── non_salary_schema.py     # Non-salary data schema (Pydantic models)
│   └── excel_output_schema.py   # Excel output column definitions
├── scripts/                     # Utility and analysis scripts
│   └── validation/              # Extraction validation (validate_extraction.py)
├── utils/                       # Standalone utility scripts (not directly used by pipeline)
│   ├── input_utils/             # Input utilities
│   └── output_utils/            # Output utilities
└── run_pipeline.py              # Main entry point
```

**Note**: Files are organized by CAO number folders (e.g., `inputs/pdfs/input_pdfs/10/`, `inputs/pdfs/input_pdfs/1536/`). Multiple files can have the same filename but exist in different CAO folders. File identification requires both filename and CAO number.

## Configuration

All paths and settings are centralized in `conf/config.yaml`. Key paths include:
- Input PDFs: `inputs/pdfs/input_pdfs` and `inputs/pdfs/input_pdfs_extra`
- Input Excel: `inputs/excel/inputExcel`
- Output directories: `outputs/llm_extracted`, `outputs/llm_analysis`, `outputs/excel`, `outputs/validation`
- Parsed PDFs: `outputs/parsed_pdfs/parsed_pdfs_json` and `outputs/parsed_pdfs/parsed_pdfs_markdown`

## Usage Examples

### Run Individual Stages
```bash
python -m pipelines.p1_webscraping    # Web scraping
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
- **Robust Error Handling**: Intelligent error classification (request problems vs external errors), synchronized retry logic with exponential backoff (2.1^attempt), adaptive retry strategies (p3: attempts 0,3,4,5,7; p4: attempts 0,2,4,5,6,8,10), compact schema retries (p4 attempts 5-6), split extraction retries (p3 attempts 5,7; p4 attempt 8), super compact schema retries (p4 attempt 10), and comprehensive error recovery
- **Performance Monitoring**: Real-time tracking of processing time, token usage, costs, and quality metrics
- **Scalable Architecture**: Designed for processing 1,580+ PDF documents efficiently

## Pipeline Stage Details

### Stage 1: Web Scraping (p1_webscraping.py)
- Downloads CAO PDFs from uitvoeringarbeidsvoorwaardenwetgeving.nl
- Uses Selenium with Chrome for robust scrolling and link discovery
- **PDF Link Filtering**: Extracts every second PDF link (indices 0, 2, 4, 6...) since there are always 2 links per PDF file
- **PDF Merging**: Merges all PDFs from the same main link/page into a single PDF file using PyPDF2
- **File Naming**: Saves merged PDFs with the filename of the first PDF (link at index 0)
- Supports primary and extra runs with duplicate prevention
- Generates metadata CSV files for tracking

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
- **Robust error handling with intelligent retry logic**: 
  - **Error classification**: Distinguishes between request problems (truncated, incomplete JSON, empty response) and external errors (503, 500, connection reset, per-minute quota)
  - **Request problems**: Increment to next attempt with parameter adjustments
  - **External errors**: Retry with same attempt number, wait 15 minutes (unlimited retries until max_retries)
  - **Daily quota errors**: Exit gracefully without retrying
  - **Attempts 0, 3, 4**: Unified extraction with adaptive retry (exponential backoff 2.1^attempt, parameter adjustments)
  - **Attempts 5, 7**: Split extraction (salary and non-salary separately) with partial success caching (attempts 1, 2, 6 removed as duplicates)
  - **Synchronized retry parameters**: Quota delay buffer (2-6 minutes), per-minute quota wait (150 seconds), empty response wait (+120 seconds), debug logging
  - File locking to prevent duplicate processing
  - **Failed CAO number saving**: Files that fail all retry attempts are saved to `performance_logs/llm_extraction/failed_cao_numbers/[cao_number]/` and automatically skipped in future runs
- **Split extraction strategy**: For files with very large outputs, splits extraction into salary-only and non-salary-only schemas, then merges results. Successful partial extractions are cached to avoid re-extraction on retries.

### Stage 4: Analysis (p4_analysis.py)
- Schema-driven structured extraction using Pydantic models
- Separates salary and non-salary information
- Non-salary schema split into 3 parts for better performance
- **Multi-process parallel processing** with independent error handling
- **Performance monitoring** and quality tracking
- **Advanced retry strategy with intelligent error handling**:
  - **Error classification**: Distinguishes between request problems (truncated, incomplete JSON, empty response), external errors (503, 500, connection reset, per-minute quota), and schema complexity errors
  - **Request problems**: Increment to next attempt with parameter adjustments
  - **External errors**: Retry with same attempt number, wait 15 minutes (unlimited retries until max_retries)
  - **Daily quota errors**: Exit gracefully without retrying
  - **Schema complexity errors**: Fatal error, don't retry (schema needs simplification)
  - **Attempts 0, 2, 4**: Regular extraction with adaptive parameter adjustment (attempts 1, 3 removed as duplicates/user request)
  - **Attempts 5-6**: Compact schema extraction (reduced output size) - triggered if truncation occurs after attempt 4, or if file is in truncated folder (attempt 7 removed as duplicate of 6)
  - **Attempt 8**: Split extraction (first half/second half by jobgroup boundaries) - triggered if truncation occurs after attempt 6, or if file is in truncated_2 folder (attempt 9 removed as duplicate of 8)
  - **Attempt 10**: Super compact schema extraction (minimal fields only) - triggered if truncation occurs after attempt 8, or if file is in truncated_3 folder (attempt 11 removed as duplicate of 10)
  - **Synchronized retry parameters**: Quota delay buffer (2-6 minutes), per-minute quota wait (150 seconds), empty response wait (+120 seconds), debug logging
  - **File handling**:
    - Files in truncated folder → retry with attempts 5-6 (compact), extend to 8 (split), 10 (super compact) if needed
    - Files in truncated_2 folder → retry with attempt 8 (split extraction), may extend to 10
    - Files in truncated_3 folder → retry with attempt 10 (super compact schema)
    - Files in truncated_4 folder → skipped (all attempts exhausted)

### Extraction Validation (scripts/validation/validate_extraction.py)
- Validates salary and/or non-salary extraction outputs against source parsed markdown
- Scores: hallucination, completeness, accuracy, temporal validity (salary only)
- Samples one file per CAO number (random, `--seed` for reproducibility)
- Leverages `gemini-flash-latest`; cached results are stored per CAO/type under `outputs/validation/{salary|non_salary}/{cao_number}/validation_<filename>.json`
- Pass `--force` to re-run validation even when cached files exist; the summary CSV merges cached and newly generated results
- **Usage**: `python scripts/validation/validate_extraction.py --type salary --seed 42`
- **Usage**: `python scripts/validation/validate_extraction.py --type both --max_files 10`

### Stage 5: Excel Creation (p5_excel_creation.py)
- Merges salary and non-salary extraction results
- Adds CAO metadata and dates from `extracted_cao_info.csv`
- Creates final Excel files with proper formatting
- Handles Excel cell size limits (32,767 character limit)
- Properly handles NaN values in date fields

## Date Format Handling

**Important**: Different date fields use different formats:

- **CAO metadata dates (DD/MM/YYYY)**: `ingangsdatum`, `expiratiedatum`, `datum_kennisgeving`
  - Format: `'01/01/2014'`, `'31/12/2014'`
  - Source: Website metadata CSV
  - Parsing: Requires `dayfirst=True` in `pd.to_datetime()`

- **Contract dates (YYYY-MM-DD)**: `general_start_date`, `general_expiry_date`, etc.
  - Format: `'2014-01-01'`, `'2014-12-31'` (ISO format)
  - Source: Extracted from PDFs by LLM
  - Parsing: Default `pd.to_datetime()` (no `dayfirst` needed)

- **Salary timeline dates (YYYY-MM-DD)**: `salary_1_start_date`, `salary_1_end_date`, etc.
  - Format: `'2014-01-01'`, `'2014-12-31'` (ISO format)
  - Source: Extracted from PDFs by LLM
  - Parsing: Default `pd.to_datetime()` (no `dayfirst` needed)

All descriptives and analysis scripts correctly handle these format differences.

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
