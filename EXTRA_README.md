# CAOsDataExtraction – Replication Guide (Extra README)

Purpose-built notes to reproduce the full CAO extraction pipeline with minimal variation across runs (low temperatures are already enforced). This file centralizes setup, execution order, prompts, schemas, and output expectations.

## Environment & Configuration
- Python environment: activate `conda activate caos-extract` and install `pip install -r requirements.txt`.
- System dependencies: Tesseract OCR (for p2), `poppler`/`pdf2image` helpers, Chrome + Selenium driver for p1 (if scraping).
- Secrets: `.env` with `GOOGLE_API_KEY1`, `GOOGLE_API_KEY2`, ... (one per parallel p3/p4 process). p4 examples use keys 7/8 for testing, but any numbered keys work.
- Config: `conf/config.yaml` centralizes paths. Key entries:  
  - Inputs: `inputs/pdfs/input_pdfs`, `inputs/pdfs/input_pdfs_extra`, `inputs/excel/inputExcel`  
  - Parsed text: `outputs/parsed_pdfs/parsed_pdfs_json`, `outputs/parsed_pdfs/parsed_pdfs_markdown`  
  - LLM outputs: `outputs/llm_extracted`, `outputs/llm_analysis`, `outputs/excel`
- Folder conventions: PDFs live under CAO-number subfolders (`10/file.pdf`, `1536/file.pdf`), so always pair filename + CAO number.

## Pipeline Execution (p1 → p5)
Run from repo root. Stages can be invoked individually (`python -m pipelines.pX_*`). The top-level `run_pipeline.py` currently imports a non-existent `pipelines.p5_run`; treat it as a placeholder until that orchestrator exists.

1) `p1_webscraping.py` (optional if PDFs already present) – downloads CAO PDFs with Selenium; uses `inputs/excel/CAO_Frequencies_2014.xlsx` to decide skips and defaults to writing into `inputs/pdfs/input_pdfs_extra/`.  
2) `p2_extract.py` – PDF → Markdown/JSON extraction. Key options:  
   - Parallel: `python pipelines/p2_extract.py --process_id 0 --total_processes 4` (modulo distribution).  
   - Flags in code: `OUTPUT_FORMAT` (`markdown`/`json`/`both`), `AUTO_FIX_UNICODE`, `AUTO_FIX_POSTSCRIPT`, `DEBUG`.  
   - Output: Markdown to `outputs/parsed_pdfs/parsed_pdfs_markdown/[CAO]/`.  
3) `p3_llmExtraction.py` – Gemini raw extraction from Markdown to JSON (`outputs/llm_extracted/new_flow/[CAO]/`).  
   - Parallel: `--key_number N --process_id i --total_processes P [--max_files K]`.  
   - Delay between files: 150s; file locking prevents duplicates.  
4) `p4_analysis.py` – schema-driven Gemini analysis/validation.  
   - Input: `outputs/llm_extracted/new_flow/[CAO]/`.  
   - Output: `outputs/llm_analysis/salary/[CAO]/` and `outputs/llm_analysis/non_salary/gen_bon_wag_pen_ter/[CAO]/`, `.../lea_ove_tra/[CAO]/`, `.../hom_con_saf_chi_ai_fri/[CAO]/`.  
   - Parallel options mirror p3.  
5) `p5_excel_creation.py` – merges analysis outputs + `inputs/pdfs/extracted_cao_info.csv` → CSVs in `outputs/excel/new_results/` (`extracted_data_salary.csv`, `extracted_data_non_salary.csv`).  
   - Optional `--max_files` to cap processing.

## LLM Settings & Retry Logic (for reproducibility)
- **Models:** p3 and p4 default to `gemini-2.5-flash`. p2 is deterministic extraction (no LLM).  
- **Base parameters (p3 ExtractionConfig):** `temperature=0.0`, `top_p=0.1`, `top_k=1`, `max_tokens=65536`, `candidate_count=1`, `seed=42`, `presence_penalty=0`, `frequency_penalty=0`, `thinking_budget=-1`, `max_retries=8`.  
- **p3 retries:**  
  - Attempts 1–3: base params.  
  - Attempt 4: +0.1 to temp/top_p, top_k reduced.  
  - Attempt 5: +0.2 adjustment.  
  - Attempts 6–8: split extraction (salary vs non-salary) with adjustments: attempt 6 = base, 7 = base, 8 = +0.1. Partial successes cached; outputs merged.  
  - Error-aware guidance for truncated/empty JSON; exponential backoff based on file size.  
- **p4 salary retries:**  
  - Attempts 1–5: regular `SalaryExtractionSchema` with temp progression 0.0 → 0.0 → 0.1 → 0.2 → 0.3.  
  - Attempts 6–8 (compact schema, no `table_label`, abbreviated units) triggered on truncation or presence in `truncated` folder.  
  - Attempts 9–10 (split schema) trigger after compact truncation or files in `truncated_2`; jobgroup boundaries preserved; anti-repetition rules on attempt 10. Files in `truncated_3` are skipped.  
- **p4 non-salary:** three independent parts (General+Bonuses+WageScales+Pension+Termination / Leave+Overtime+Training / Homeoffice+Contract+Safety+Childcare+AI+Fringe) each with their own retries; same base temperature logic.  
- **Output determinism:** Temperature is 0.0 by default; keep it to minimize replication variance. Seeds are set; prompts mandate JSON-only outputs.

## Prompts (authoritative text)
### p3 unified extraction (`create_extraction_prompt`)
```
Extract information from the Dutch CAO Markdown document parsed from a PDF. Pages are marked ## Page X _native_ (text-based) or ## Page X  _OCR_ (image-based); use these labels to interpret the layout correctly.

GOAL: Produce one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, each mapping to a List[List[str]]. If nothing is found for a key, return an empty list.

THINKING & OUTPUT: Think step by step INTERNALLY to locate, route, and clean the data, but OUTPUT ONLY the final JSON (no explanations, no notes, no chain-of-thought).
    
CRITICAL RULES:
    - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information. Always VERIFY the extracted information with the document.
    - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
    - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
    - IMPORTANT: Translate all Dutch text (clauses, tables, titles, etc.) into clear and precise English but keep names in Dutch. For legal clauses, preserve the exact legal meaning without simplification.
    - IMPORTANT: Check the appendix if it exists in the document. Salary tables and important information are sometimes located in appendices and should not be skipped.

INSIDE SECTION RULES: Order does not matter within a section - keep related items together (e.g., a table followed by its note/explanation).

ROUTING RULES (Use to avoid duplicates across sections):
    - Wage vs Overtime: atypical hours pay → overtime_information; structural bonuses → wage_information.  
    - Wage vs Fringe: cash → wage_information; non-cash perks/reimbursements → fringe_benefits_information.  
    - Homeoffice vs Fringe: WFH-specific (stipend, equipment, internet) → homeoffice_information; general perks → fringe_benefits_information.  
    - Childcare vs Leave: time off/pay during leave → leave_information; childcare services/subsidies/discounts → childcare_information.  
    - Training vs Safety vs AI: safety/Arbo training → safety_information; AI-related training → AI_information; all other training → training_information.  
    - Safety vs Homeoffice: safety/Arbo for home working → homeoffice_information; general safety/integrity → safety_information.  
    - Pension vs Wage vs Fringe: pension schemes/funds → pension_information; wages/bonuses → wage_information; non-pension perks → fringe_benefits_information.  
    - Contract vs Termination: contract forms/ketenregeling/conversion → contract_type_information; notice/dismissal/severance/WW supplements → termination_information.  
    - Holidays: pay/allowance for working on holidays → overtime_information; days off/policies → leave_information.  

WHAT TO INCLUDE:
    - Numbers, amounts, percentages, dates, periods, conditions, eligibility rules, procedures, entitlements, allowances.
    - Tables: include a compact structure (see TABLE FORMAT) with headers and all data rows and columns plus any short note that explains the table.

WAGE TABLE DEDUP (IMPORTANT):
    - Keep tables that differ by period, worker type, education level, job group/function scale, experience steps (periodieken/trede), age bands, or other substantive distinctions.
    - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present).

TABLE FORMAT:
    - Represent each table as a short list of strings like:
        [
            "Table title with context and units",
            "Columns: <Row label (if present)> | <Col 1 label> | <Col 2 label> | <Col 3 label> | ...",
            "<Row 1 label (if present)> | <v1> | <v2> | <v3> | ... ",
            "<Row 2 label (if present)> | <v1> | <v2> | <v3> | ... ",
            "... (one string per row)",
            "Additional notes or clarifying information if any"
        ]

EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT):
    1) Read & anchor: read all instructions and section descriptions.
    2) Sweep & mark: scan the whole document including any appendices; mark every clause/table/text that matches any section description; ignore text/sentences/clasues/passages that matches none.
    3) Route: apply the ROUTING RULES to decide the correct section when overlaps occur.
    4) Length pre-check: if the marked set, when extracted verbatim, would likely exceed ~262,144 characters, plan to trim narrative/boilerplate in step 5.
    5) Extract, translate & build — DO NOT HALLUCINATE:
        - Build one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, in this order: general_information → wage_information → pension_information → leave_information → termination_information → overtime_information → training_information → homeoffice_information → contract_type_information → safety_information → childcare_information → AI_information → fringe_benefits_information.
        - COPY numbers/dates/%/names literally; TRANSLATE all other Dutch text (clauses, part of tables that are not numbers or names, titles, etc.) to clear English; leave blank if not stated.
        - Tables: rebuild to TABLE FORMAT; apply WAGE TABLE DEDUP (remove pure unit-conversion duplicates; prefer monthly).
        - Consolidate: keep related items adjacent.
        - If trimming per Step 4 is needed, shorten only narrative notes or minor wording not directly tied to field content, without changing legal meaning.
    6) Verify: confirm that every extracted fact, number, table, or clause is explicitly present in the document (allowing for shortening, restructuring, and translation to English) and that no important information is missing. Correct or remove anything not grounded in the source. Do not infer or guess.
    7) Validate: output one JSON object only; UTF-8 only; valid JSON (balanced brackets, no trailing commas); all template keys present (empty list if none).

JSON OUTPUT REQUIREMENTS:
    - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
    - Ensure brackets/commas are correct; no trailing commas; all top-level keys present.

OUTPUT_JSON_TEMPLATE:
    {
        "general_information": [],
        "wage_information": [],
        "pension_information": [],
        "leave_information": [],
        "termination_information": [],
        "overtime_information": [],
        "training_information": [],
        "homeoffice_information": [],
        "contract_type_information": [],
        "safety_information": [],
        "childcare_information": [],
        "AI_information": [],
        "fringe_benefits_information": []
    }

Document: {filename}
```

### p3 salary-only prompt (`create_salary_extraction_prompt`)
```
Extract ONLY wage and salary information from the Dutch CAO Markdown document parsed from a PDF. Pages are marked ## Page X _native_ (text-based) or ## Page X  _OCR_ (image-based); use these labels to interpret the layout correctly.

GOAL: Produce one JSON object with ONLY the "wage_information" key mapping to a List[List[str]]. If nothing is found, return an empty list.

THINKING & OUTPUT: Think step by step INTERNALLY to locate and clean the data, but OUTPUT ONLY the final JSON (no explanations, no notes, no chain-of-thought).
    
CRITICAL RULES:
    - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information. Always VERIFY the extracted information with the document.
    - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
    - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
    - IMPORTANT: Translate all Dutch text (clauses, tables, titles, etc.) into clear and precise English but keep names in Dutch. For legal clauses, preserve the exact legal meaning without simplification.
    - IMPORTANT: Check the appendix if it exists in the document. Salary tables and important information are sometimes located in appendices and should not be skipped.

WHAT TO INCLUDE:
    - all wage tables (that are not identical except for unit conversion) and salary scales, 
    - job classifications, function groups, and pay groups/grades,
    - age-related or service-year/experience-based pay steps (trede/periodieken), including any transitions between age bands and experience steps,
    - rules governing progression within scales (e.g., annual increments, step frequency, performance-based step changes, freeze/unfreeze conditions),
    - entry-placement rules (e.g., starting above step 0 for prior relevant experience or competence),
    - personal allowances at the maximum of a scale ("persoonlijke toeslag") and their conditions (basis, %/amount, pensionability, duration, phase-out),
    - general wage increases (periodic percentage or nominal increases applied sector-wide or by group),
    - all bonuses and allowances, including sign-on, 13th month, fixed lump sums, profit-sharing, performance bonuses, seniority/loyalty or jubilee bonuses, job-specific allowances, retirement gratuities, and insurance/savings benefits,
    - notes explaining how the wage system or tables operate (e.g., "scale applies to 36-h week", "wages include 8% holiday allowance", "conversion rules for youth to adult wage scale").

WAGE TABLE DEDUP (IMPORTANT):
    - Keep tables that differ by period, worker type, education level, job group/function scale, experience steps (periodieken/trede), age bands, or other substantive distinctions.
    - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present).

TABLE FORMAT:
    - Represent each table as a short list of strings like:
        [
            "Table title with context and units",
            "Columns: <Row label (if present)> | <Col 1 label> | <Col 2 label> | <Col 3 label> | ...",
            "<Row 1 label (if present)> | <v1> | <v2> | <v3> | ... ",
            "<Row 2 label (if present)> | <v1> | <v2> | <v3> | ... ",
            "... (one string per row)",
            "Additional notes or clarifying information if any"
        ]

EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT):
    1) Read & anchor: read all instructions and focus ONLY on wage/salary information.
    2) Sweep & mark: scan the whole document including any appendices; mark every clause/table/text that relates to wages/salaries.
    3) Extract, translate & build — DO NOT HALLUCINATE:
        - Build one JSON object with ONLY the "wage_information" key.
        - COPY numbers/dates/%/names literally; TRANSLATE all other Dutch text (clauses, part of tables that are not numbers or names, titles, etc.) to clear English.
        - Tables: rebuild to TABLE FORMAT; apply WAGE TABLE DEDUP (remove pure unit-conversion duplicates; prefer monthly).
        - Consolidate: keep related items adjacent.
    4) Verify: confirm that every extracted fact, number, table, or clause is explicitly present in the document and that no important wage/salary information is missing. Correct or remove anything not grounded in the source.
    5) Validate: output one JSON object only; UTF-8 only; valid JSON (balanced brackets, no trailing commas); the "wage_information" key must be present (empty list if none).

JSON OUTPUT REQUIREMENTS:
    - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
    - Ensure brackets/commas are correct; no trailing commas.

OUTPUT_JSON_TEMPLATE:
    {
        "wage_information": []
    }

Document: {filename}
```

### p3 non-salary prompt (`create_nonsalary_extraction_prompt`)
```
Extract information from the Dutch CAO Markdown document parsed from a PDF. Pages are marked ## Page X _native_ (text-based) or ## Page X  _OCR_ (image-based); use these labels to interpret the layout correctly.

GOAL: Produce one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, each mapping to a List[List[str]]. If nothing is found for a key, return an empty list.
IMPORTANT: The field "wage_information" (Wage table and salary scales) is excluded and will be handled separately.

THINKING & OUTPUT: Think step by step INTERNALLY to locate, route, and clean the data, but OUTPUT ONLY the final JSON (no explanations, no notes, no chain-of-thought).
    
CRITICAL RULES:
    - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information. Always VERIFY the extracted information with the document.
    - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
    - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
    - IMPORTANT: Translate all Dutch text (clauses, tables, titles, etc.) into clear and precise English but keep names in Dutch. For legal clauses, preserve the exact legal meaning without simplification.
    - IMPORTANT: Check the appendix if it exists in the document. Salary tables and important information are sometimes located in appendices and should not be skipped.

INSIDE SECTION RULES: Order does not matter within a section - keep related items together (e.g., a table followed by its note/explanation).

ROUTING RULES (Use to avoid duplicates across sections):
    - Wage vs Overtime: atypical hours pay → overtime_information; structural bonuses → EXCLUDE (wage information, handled separately).
    - Wage vs Fringe: cash wages → EXCLUDE (wage information, handled separately); non-cash perks/reimbursements → fringe_benefits_information.
    - Homeoffice vs Fringe: WFH-specific (stipend, equipment, internet) → homeoffice_information; general perks → fringe_benefits_information.
    - Childcare vs Leave: time off/pay during leave → leave_information; childcare services/subsidies/discounts → childcare_information.
    - Training vs Safety vs AI: safety/Arbo training → safety_information; AI-related training → AI_information; all other training → training_information.
    - Safety vs Homeoffice: safety/Arbo for home working → homeoffice_information; general safety/integrity → safety_information.
    - Pension vs Wage vs Fringe: pension schemes/funds → pension_information; wages/bonuses → EXCLUDE (wage information, handled separately); non-pension perks → fringe_benefits_information.
    - Contract vs Termination: contract forms/ketenregeling/conversion → contract_type_information; notice/dismissal/severance/WW supplements → termination_information.
    - Holidays: pay/allowance for working on holidays → overtime_information; days off/policies → leave_information.

WHAT TO INCLUDE:
    - Numbers, amounts, percentages, dates, periods, conditions, eligibility rules, procedures, entitlements, allowances (but NOT wage/salary amounts).
    - Tables: include a compact structure (see TABLE FORMAT) with headers and all data rows and columns plus any short note that explains the table.

TABLE FORMAT:
    - Represent each table as a short list of strings like:
        [
            "Table title with context and units",
            "Columns: <Row label (if present)> | <Col 1 label> | <Col 2 label> | <Col 3 label> | ...",
            "<Row 1 label (if present)> | <v1> | <v2> | <v3> | ... ",
            "<Row 2 label (if present)> | <v1> | <v2> | <v3> | ... ",
            "... (one string per row)",
            "Additional notes or clarifying information if any"
        ]

EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT):
    1) Read & anchor: read all instructions and section descriptions.
    2) Sweep & mark: scan the whole document including any appendices; mark every clause/table/text that matches any section description EXCEPT wage/salary information; ignore text/sentences/clauses/passages that match none.
    3) Route: apply the ROUTING RULES to decide the correct section when overlaps occur. SKIP all wage/salary information.
    4) Length pre-check: if the marked set, when extracted verbatim, would likely exceed ~262,144 characters, plan to trim narrative/boilerplate in step 5.
    5) Extract, translate & build — DO NOT HALLUCINATE:
        - Build one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, in this order: general_information → pension_information → leave_information → termination_information → overtime_information → training_information → homeoffice_information → contract_type_information → safety_information → childcare_information → AI_information → fringe_benefits_information.
        - DO NOT include "wage_information" key.
        - COPY numbers/dates/%/names literally; TRANSLATE all other Dutch text (clauses, part of tables that are not numbers or names, titles, etc.) to clear English; leave blank if not stated.
        - Tables: rebuild to TABLE FORMAT.
        - Consolidate: keep related items adjacent.
        - If trimming per Step 4 is needed, shorten only narrative notes or minor wording not directly tied to field content, without changing legal meaning.
    6) Verify: confirm that every extracted fact, number, table, or clause is explicitly present in the document (allowing for shortening, restructuring, and translation to English) and that no important information is missing. Correct or remove anything not grounded in the source. Do not infer or guess. Ensure NO wage/salary information is included.
    7) Validate: output one JSON object only; UTF-8 only; valid JSON (balanced brackets, no trailing commas); all template keys present (empty list if none); "wage_information" must NOT be present.

JSON OUTPUT REQUIREMENTS:
    - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
    - Ensure brackets/commas are correct; no trailing commas; all top-level keys present.
    - DO NOT include "wage_information" in the output.

OUTPUT_JSON_TEMPLATE:
    {
        "general_information": [],
        "pension_information": [],
        "leave_information": [],
        "termination_information": [],
        "overtime_information": [],
        "training_information": [],
        "homeoffice_information": [],
        "contract_type_information": [],
        "safety_information": [],
        "childcare_information": [],
        "AI_information": [],
        "fringe_benefits_information": []
    }

Document: {filename}
```

### p4 salary prompt (`schema/salary_schema.py::SALARY_PROMPT`)
Full schema-aligned wage extraction prompt (includes table selection, age-group rules, jobgroup/step/education/contract handling, timeline construction, validation, and JSON requirements). Use the exact text from `schema/salary_schema.py` when calling Gemini; it requires output `{"salary_information": [SalaryRow, ...]}` matching the SalaryExtractionSchema with `table_label` and full unit strings.

### p4 salary split prompts (`schema/salary_prompt_split.py`)
- **Attempt 9 (`SALARY_PROMPT_SPLIT_ATTEMPT_9`)**: Extract ~first 50% of salary rows, completing any jobgroup started; same schema as compact/split, enforces jobgroup boundary completion.  
- **Attempt 10 (`SALARY_PROMPT_SPLIT_ATTEMPT_10`)**: Extract remaining jobgroups, skipping any already extracted; includes anti-repetition rules (single-note references, max 20 words per note) and requires passing `already_extracted_json` context.

### p4 non-salary prompt (`schema/non_salary_schema.py::NON_SALARY_PROMPT`)
Schema-enforced prompt for parts 1–3; parameters include `{sections}` (subset of parts), filename, and source JSON. Covers critical rules (no hallucination, literal values, YYYY-MM-DD dates), Amount/AmountRange handling, worker-focus/heterogeneity rules, extraction/validation steps, and JSON-only output.

## Pydantic Schemas (output shape)
- **Salary (schema/salary_schema.py):**  
  - `Amount` `{value: float|None, unit: str|None}`; `AmountRange` `{min, max, unit}`.  
  - `SalaryPoint`: `start_date`, optional `end_date`, `amount` (float), `unit` (full text), `table_label`, optional `inc_pct`, `holiday_incl`, `note`.  
  - `SalaryRow`: `jobgroup`, optional `step`, `worker`, `is_entry`, `age_group`, `education`, `ft_hours`, `permanency`, `hours_type`, `timeline` `[SalaryPoint]`, optional `row_note`.  
  - `SalaryExtractionSchema`: `salary_information: List[SalaryRow]`.  
- **Compact salary (schema/salary_schema_compact.py):** Same structure but `SalaryPointCompact` uses abbreviated unit (`m`, `4-w`, `w`, `h`, `a`) and removes `table_label` to save tokens; used attempts 6–8.  
- **Split salary (schema/salary_schema_split.py):** Same as compact but paired with split prompts for attempts 9–10 (jobgroup-aware merging).  
- **Non-salary (schema/non_salary_schema.py):**  
  - Shared scalars: frequent use of `Amount`/`AmountRange`; booleans gate presence; strings default to `""` or `unspecified`.  
  - Part 1 (`NonSalaryPart1`): `general_information` (dates, retroactivity, scope, AVV), `bonuses_info` (bonus flags/Amount fields incl. sign-on, 13th month, qualification bonuses, retirement gratuities), `wage_scales_info` (general increases, allowances, compensation practices), `pension_information` (scheme type, contributions, accrual, franchise, ages, eligibility, funds), `termination_information` (notice periods, severance, protections).  
  - Part 2 (`NonSalaryPart2`): `leave_information` (vacation, parental/maternity/partner/adoption/sick/care leave, senior days), `overtime_information` (triggers, allowances, caps), `training_information` (paid time, budgets, obligations, clawbacks).  
  - Part 3 (`NonSalaryPart3`): `homeoffice_information` (allowances, equipment, safety), `contract_type_information` (chain rules, forms, conversion), `safety_information` (integrity/ARBO policies), `childcare_information`, `ai_information`, `fringe_benefits_information` (perks, allowances).  
- **Excel output (schema/excel_output_schema.py):**  
  - `get_salary_columns(max_timeline_length)` builds metadata columns + mapped SalaryRow fields + timeline columns (`salary_1_*`, `salary_2_*`, ...).  
  - `get_non_salary_columns()` flattens all non-salary parts with prefixes (`general_`, `bonus_`, `wage_`, `pension_`, `term_`, `leave_`, `overtime_`, `training_`, `homeoffice_`, `contract_`, `safety_`, `childcare_`, `ai_`, `fringe_`).  
  - Flatten helpers convert Amount/AmountRange objects to value/unit columns.

## Data Flow & Outputs
- p2 writes Markdown (primary) and JSON per page with extraction method labels; always chooses longer text between OCR and native; fixes `/uniXXXX` and `/GXXX` patterns; keeps empty pages and notes missing pages.  
- p3 reads Markdown → LLM JSON (`outputs/llm_extracted/new_flow/[CAO]/`). Failed files logged to `outputs/logs/failed_files_llm_extraction.txt` and parsing errors to `outputs/logs/structured_output_parsing_errors.txt`.  
- p4 reads p3 JSON; outputs salary + three non-salary parts; performance logs in `monitoring/performance_logs/llm_analysis/` and combined `analysis_performance.jsonl`; failures logged to `outputs/logs/failed_files_analysis.txt`.  
- p5 merges p4 outputs + metadata CSV, truncates cells to 32,000 chars for Excel, writes CSVs to `outputs/excel/new_results/`.

## Replication Tips
- Keep `temperature=0.0` (default) for both p3 and p4 to reduce divergence; avoid changing top_p/top_k unless intentionally re-tuning.  
- Use consistent `.env` key assignments per process to prevent rate-limit variability.  
- When re-running partial stages, clear or target specific files; file locks prevent duplicate work.  
- For large/problematic files, observe `truncated`, `truncated_2`, `truncated_3` folders to understand which retry pathway will trigger.
