# Salary Extraction Flow in p4_analysis.py

## Overview

The salary extraction process uses a graduated retry strategy with 3 schema types and 4 different prompts to handle files of varying sizes and complexity.

## Attempt Mapping

| Attempt Index | Overall Attempt | Schema Type | Prompt Used | Field Names |
|--------------|----------------|-------------|------------|-------------|
| 0-4 | 1st-5th | Regular | SALARY_PROMPT | Full names (salary_information, start_date, etc.) |
| 5-7 | 6th-8th | Compact | SALARY_PROMPT_COMPACT | 2-letter (si, sd, am, etc.) |
| 8-9 | 9th-10th | Split | SALARY_PROMPT_SPLIT_ATTEMPT_9 (part 1)<br>SALARY_PROMPT_SPLIT_ATTEMPT_10 (part 2) | 2-letter (si, sd, am, etc.) |

## Prompt Usage Details

### 1. SALARY_PROMPT (Normal)
- **Used for**: Attempts 0-4 (1st-5th overall)
- **Schema**: `SalaryExtractionSchema` (regular)
- **Field names**: Full names
  - `salary_information`, `start_date`, `end_date`, `amount`, `unit`, `inc_pct`, `holiday_incl`, `note`
  - `jobgroup`, `step`, `worker`, `is_entry`, `age_group`, `education`, `ft_hours`, `permanency`, `hours_type`, `timeline`, `row_note`
- **JSON output**: `{"salary_information": [...]}`
- **When to use**: First attempts for all files

### 2. SALARY_PROMPT_COMPACT
- **Used for**: Attempts 5-7 (6th-8th overall)
- **Schema**: `SalaryExtractionSchemaCompact`
- **Field names**: 2-letter abbreviations
  - `si` (salary_information), `sd` (start_date), `ed` (end_date), `am` (amount), `un` (unit), `ip` (inc_pct), `hp` (holiday_incl in point), `nt` (note)
  - `jg` (jobgroup), `st` (step), `wr` (worker), `ie` (is_entry), `ag` (age_group), `eu` (education), `fh` (ft_hours), `pe` (permanency), `ht` (hours_type), `hi` (holiday_incl in row), `tl` (timeline), `rn` (row_note)
- **JSON output**: `{"si": [...]}`
- **Differences from normal prompt**:
  1. Adds "FIELD NAME ABBREVIATIONS" section
  2. Mentions "when the omit condition is met" in CRITICAL RULES
  3. Uses "si" instead of "salary_information" in JSON output example
- **When to use**: After truncation in attempts 1-4, or files in `truncated` folder
- **IMPORTANT**: `holiday_incl` moved from `SalaryPoint` (hp) to `SalaryRow` (hi) - affects Excel format

### 3. SALARY_PROMPT_SPLIT_ATTEMPT_9
- **Used for**: Part 1 of split extraction (attempts 8-9)
- **Schema**: `SalaryExtractionSchemaSplit` (same as compact)
- **Field names**: 2-letter abbreviations (same as compact)
- **JSON output**: `{"si": [...]}`
- **Purpose**: Extract approximately first 50% of salary rows
- **Key rule**: Must complete entire jobgroup if started (jobgroups not split)
- **When to use**: Part 1 of split extraction (always paired with attempt 10 for part 2)

### 4. SALARY_PROMPT_SPLIT_ATTEMPT_10
- **Used for**: Part 2 of split extraction (attempts 8-9)
- **Schema**: `SalaryExtractionSchemaSplit` (same as compact)
- **Field names**: 2-letter abbreviations (same as compact)
- **JSON output**: `{"si": [...]}`
- **Purpose**: Extract remaining salary rows (jobgroups NOT in part 1)
- **Key features**:
  - Skips jobgroups already extracted in part 1
  - Includes anti-repetition rules for notes (max 20 words, reference format for shared notes)
- **When to use**: Part 2 of split extraction (always after attempt 9 for part 1)

## Split Extraction Flow

**Important**: Both attempt 8 (9th overall) AND attempt 9 (10th overall) use BOTH prompts:

1. **Part 1** (always uses SALARY_PROMPT_SPLIT_ATTEMPT_9):
   - Extract first ~50% of salary rows
   - Complete any jobgroup that is started
   - Store result in `first_half_result`

2. **Delay**: 3 minutes (180 seconds) between parts

3. **Part 2** (always uses SALARY_PROMPT_SPLIT_ATTEMPT_10):
   - Extract remaining jobgroups (not in part 1)
   - Use `first_half_result` as context to avoid duplicates
   - Store result in `second_half_result`

4. **Merge**: Combine both parts using `merge_split_salary_results()`

The only difference between attempt 8 and attempt 9 is the attempt number and parameters (both use attempt 4 parameters: temp=0.3, top_p=0.4, top_k=0.7).

## Automatic Extensions

The retry logic automatically extends to more aggressive strategies:

1. **After attempt 4 (5th overall)**: If truncation error → extends to attempts 5-7 (compact schema)
2. **After attempt 7 (8th overall)**: If truncation error → extends to attempts 8-9 (split extraction)

## File Folder Logic

- **Normal files**: Start with attempts 0-4 (regular schema)
- **Files in `truncated` folder**: Start with attempts 5-7 (compact schema), may extend to 8-9
- **Files in `truncated_2` folder**: Start with attempts 8-9 (split extraction) directly
- **Files in `truncated_3` folder**: Skipped entirely (all attempts exhausted)

## Field Access in Code

All code correctly handles both field name formats:
- Regular schema: `response.parsed.salary_information`
- Compact/split schema: `parsed_dump.get('si', parsed_dump.get('salary_information', []))`
- Manual parsing: `salary_schema.si if hasattr(salary_schema, 'si') else salary_schema.salary_information`

## Key Differences Summary

| Aspect | Regular | Compact | Split |
|--------|---------|---------|-------|
| Field names | Full | 2-letter | 2-letter |
| JSON top-level | `salary_information` | `si` | `si` |
| `holiday_incl` location | SalaryPoint | SalaryRow (hi) | SalaryRow (hi) |
| `table_label` | Yes | No | No |
| Unit format | Full text | Abbreviated | Abbreviated |
| Extraction method | Single | Single | Two parts |

