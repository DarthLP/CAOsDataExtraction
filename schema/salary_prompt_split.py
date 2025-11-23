"""
Split Extraction Prompt Templates for Salary Data

This module contains prompt templates for split extraction attempts (9-10),
where data is extracted in two halves with jobgroup boundaries respected.

USAGE:
    from schema.salary_prompt_split import (
        SALARY_PROMPT_SPLIT_ATTEMPT_9,
        SALARY_PROMPT_SPLIT_ATTEMPT_10
    )
"""

# ---------------------------------------------------------------------
# SPLIT EXTRACTION PROMPT - ATTEMPT 9 (First Half)
# ---------------------------------------------------------------------

SALARY_PROMPT_SPLIT_ATTEMPT_9 = """Extract structured salary data from a JSON object derived from the Dutch CAO document.

    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no hallucination, no guessing, no markdown fences, no extra text.

    INPUTS
    Filename: {filename}
    Source text: {source_json}

    CRITICAL RULES
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
        - Missing values: Omit optional fields entirely. Only include optional fields with actual values.
        - Output ONLY valid JSON format matching the provided schema structure.
        - EXTRACT APPROXIMATELY FIRST 50% OF SALARY ROWS (by order encountered)
        - JOBGROUP BOUNDARY RULE: If you start extracting a jobgroup, you MUST complete ALL rows for that entire jobgroup (all steps, ages, education levels, worker types, contract types for that jobgroup). DO NOT split a jobgroup - finish it completely even if it means extracting slightly more than 50%.

    TABLE SELECTION
        - Include ONLY standard/regular wage tables. 
        - EXCLUDE allowances, bonuses, overtime, irregular hours, reimbursements, and non-standard worker roles like apprentices, interns, trainees, or foremen.
        - If multiple tables exist for different worker types, time periods, education levels, job groups, steps, or age bands under this standard wage type, include all of them (but only for jobgroups you're extracting).
        - Record the unit exactly as printed. If the same baseline is printed in multiple units for the SAME workers/period/step/education/age, choose ONE using this order: monthly > hourly > 4-week > weekly > annual.
        - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present). Keep tables that differ by time period, worker type, education level, job group/function scale, steps (periodieken/trede), age bands, or contract type.

    TABLE AGE GROUP SELECTION
        - Create distinct SalaryRow objects for each adult-eligible age band present:
            - Open-ended adult bands (e.g., "22+", "21 and older"), OR
            - Bands that intersect ages 23-65.
        - IGNORE age and job groups limited to workers under 23 (e.g., "16-20", "20") unless the group is open-ended ("20+") or spans older ages ("18-65").

    TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE
        - Extract job groups in order as encountered in the source document.
        - For each jobgroup you extract, extract ALL steps/trede, ages, education levels, worker types, and contract types for that jobgroup (complete jobgroup extraction).
        - If steps/trede (periodieken) are shown, create a separate SalaryRow per jobgroup × step × [worker type] (× [age] × [education] × [contract type]).
        - If education tiers (e.g., MBO/HBO) determine different wages, create separate rows per jobgroup × step × [worker type] × education (× [age] × [contract type]).
        - If contract permanency or contract hours (work arrangement) determine different wages, create separate rows per jobgroup × step × [worker type] × contract type (× [age] × [education]).
        - Worker type field: OMIT if the value is generic (e.g. 'employee', 'standard worker') AND there is only one worker type in the entire CAO. KEEP when it provides meaningful distinction between different worker categories.
        - EXTRACTION RULE: Extract approximately first 50% of rows, but always complete any jobgroup that you start extracting.

    TABLE AMOUNTS, PERCENTAGES, DATES
        - Salary amount: output as a number using a dot as the decimal separator (e.g., 2300.00). Do NOT use quotes, commas or thousands separators.
        - inc_pct: include only if the table or a relating clause explicitly states a general % for that version.
        - Dates: Use YYYY-MM-DD format (e.g., "2023-11-01"). Do NOT invent or infer dates.
    
    TABLE TIMELINE CONSTRUCTION
        - For each (jobgroup × step × [worker type] × [age] × [education] × [contract type]), build `timeline` with a SalaryPoint per table version that prints salary amounts.
        - Each SalaryPoint MUST have a printed amount. If only a % increase is announced but no new amounts are printed, DO NOT add a timeline point; instead mention the % in a note.
        - Use start_date exactly as the table heading or clause states, converting to YYYY-MM-DD format (e.g., "per 1 Nov 2023" → "2023-11-01"). If day is not printed, use the first day of the month, same for month.
        - Align timeline points for the SAME (jobgroup × step × [worker type] × [age] × [education] × [contract type]) across time periods / table versions. Do not impute missing values.

    WORKFLOW STEPS (INTERNAL - DO NOT OUTPUT)
        1) READ & ANCHOR
            - Review all general rules and field descriptions in the Pydantic output schema.
            - Read the input text to understand its structure, content, and table layout.
        2) LOCATE & MARK all standard wage tables according to the TABLE SELECTION rules.
        3) IDENTIFY JOBGROUPS: List all unique jobgroups found in the source document.
        4) EXTRACT FIRST HALF: Extract approximately first 50% of salary rows, but:
            - ALWAYS complete any jobgroup you start extracting (all steps, ages, education levels, worker types, contract types)
            - Stop after completing a jobgroup that brings you to approximately 50% of total rows
        5) DETECT AGE GROUPS: Within each selected jobgroup, extract all age groups meeting the TABLE AGE GROUP SELECTION criteria.
        6) DETECT STEPS, EDUCATION LEVELS & CONTRACT TYPES: For each jobgroup being extracted, identify steps, worker types, education levels, and contract types following the TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE rules.
        7) CONSTRUCT TIMELINE STRUCTURE:
            7.1) Apply TABLE TIMELINE CONSTRUCTION rules to align job groups, steps, worker types, education levels, age bands, and contract types across table versions.
            7.2) Build one SalaryRow for every unique detected combination of (jobgroup × step × [worker type] × [age] × [education] × [contract type]) within the jobgroups you're extracting.
            7.3) Build timeline: For each SalaryRow, create one SalaryPoint per version/time period where that combination appears, then normalize labels (jobgroup/step/worker type/age/education/contract type), deduplicate identical periods, and align all points that refer to the same combination across versions (no imputation).
        8) SORT & CLEAN each row's timeline chronologically by start_date. Omit or nullify any fields not explicitly printed in the source.
        9) VERIFY (SOURCE-GROUNDED) that every extracted number/date/percentage/unit/clause is explicitly present in the input. Remove or correct anything not grounded.
        10) VALIDATE (SCHEMA & JSON) that the output is a valid JSON object that conforms exactly to the Pydantic schema (keys, types, null/""/omit conventions).
        11) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY a single valid JSON. No comments, no trailing commas, no text before/after.
        - Do NOT include fields not defined above.
        - Schema summary (orientation only; responseSchema enforces structure):
            Output a single JSON object:
            {{
            "salary_information": [ SalaryRow, ... ]
            }}
        
    """


# ---------------------------------------------------------------------
# SPLIT EXTRACTION PROMPT - ATTEMPT 10 (Second Half with Anti-Repetition)
# ---------------------------------------------------------------------

SALARY_PROMPT_SPLIT_ATTEMPT_10 = """Extract structured salary data from a JSON object derived from the Dutch CAO document.

    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no hallucination, no guessing, no markdown fences, no extra text.

    INPUTS
    Filename: {filename}
    Source text: {source_json}
    Already extracted (first half): {already_extracted_json}

    CRITICAL RULES
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
        - Missing values: Omit optional fields entirely. Only include optional fields with actual values.
        - Output ONLY valid JSON format matching the provided schema structure.
        - EXTRACT ONLY REMAINING SALARY ROWS (jobgroups NOT in already_extracted)
        - SKIP all jobgroups that appear in the "already extracted" data - do not duplicate them
        - Extract starting from the first jobgroup NOT present in already_extracted

    NOTE ANTI-REPETITION RULES (CRITICAL):
        - If the same note applies to MULTIPLE rows with the SAME jobgroup/step/age pattern: Omit repetition, include the note ONCE per unique pattern
        - If the same note applies to DIFFERENT jobgroups/steps/ages: Use reference format "Note: [text] (applies to jobgroups A1-A10, steps 1-5)" instead of repeating the full note for each row
        - Keep all notes extremely concise (single sentence, max 20 words)
        - Individual row notes: max 20 words, single sentence
        - Prohibit repetition in notes EXCEPT when note applies to different fields/jobgroups/steps/ages - in that case use the reference format above

    TABLE SELECTION
        - Include ONLY standard/regular wage tables. 
        - EXCLUDE allowances, bonuses, overtime, irregular hours, reimbursements, and non-standard worker roles like apprentices, interns, trainees, or foremen.
        - Extract ONLY jobgroups NOT present in already_extracted
        - If multiple tables exist for different worker types, time periods, education levels, job groups, steps, or age bands under this standard wage type, include all of them (but only for jobgroups you're extracting).
        - Record the unit exactly as printed. If the same baseline is printed in multiple units for the SAME workers/period/step/education/age, choose ONE using this order: monthly > hourly > 4-week > weekly > annual.
        - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present). Keep tables that differ by time period, worker type, education level, job group/function scale, steps (periodieken/trede), age bands, or contract type.

    TABLE AGE GROUP SELECTION
        - Create distinct SalaryRow objects for each adult-eligible age band present:
            - Open-ended adult bands (e.g., "22+", "21 and older"), OR
            - Bands that intersect ages 23-65.
        - IGNORE age and job groups limited to workers under 23 (e.g., "16-20", "20") unless the group is open-ended ("20+") or spans older ages ("18-65").

    TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE
        - Extract job groups in order as encountered in the source document.
        - SKIP any jobgroups that appear in already_extracted data
        - Start extracting from the first jobgroup NOT in already_extracted
        - For each jobgroup you extract, extract ALL steps/trede, ages, education levels, worker types, and contract types for that jobgroup (complete jobgroup extraction).
        - If steps/trede (periodieken) are shown, create a separate SalaryRow per jobgroup × step × [worker type] (× [age] × [education] × [contract type]).
        - If education tiers (e.g., MBO/HBO) determine different wages, create separate rows per jobgroup × step × [worker type] × education (× [age] × [contract type]).
        - If contract permanency or contract hours (work arrangement) determine different wages, create separate rows per jobgroup × step × [worker type] × contract type (× [age] × [education]).
        - Worker type field: OMIT if the value is generic (e.g. 'employee', 'standard worker') AND only one worker type exists. KEEP when it provides meaningful distinction between different worker categories.

    TABLE AMOUNTS, PERCENTAGES, DATES
        - Salary amount: output as a number using a dot as the decimal separator (e.g., 2300.00). Do NOT use quotes, commas or thousands separators.
        - inc_pct: include only if the table or a relating clause explicitly states a general % for that version.
        - Dates: Use YYYY-MM-DD format (e.g., "2023-11-01"). Do NOT invent or infer dates.
    
    TABLE TIMELINE CONSTRUCTION
        - For each (jobgroup × step × [worker type] × [age] × [education] × [contract type]), build `timeline` with a SalaryPoint per table version that prints salary amounts.
        - Each SalaryPoint MUST have a printed amount. If only a % increase is announced but no new amounts are printed, DO NOT add a timeline point; instead mention the % in a note.
        - Use start_date exactly as the table heading or clause states, converting to YYYY-MM-DD format (e.g., "per 1 Nov 2023" → "2023-11-01"). If day is not printed, use the first day of the month, same for month.
        - Align timeline points for the SAME (jobgroup × step × [worker type] × [age] × [education] × [contract type]) across time periods / table versions. Do not impute missing values.

    WORKFLOW STEPS (INTERNAL - DO NOT OUTPUT)
        1) READ & ANCHOR
            - Review all general rules and field descriptions in the Pydantic output schema.
            - Read the input text to understand its structure, content, and table layout.
            - Review the already_extracted data to identify which jobgroups have been extracted
        2) LOCATE & MARK all standard wage tables according to the TABLE SELECTION rules.
        3) IDENTIFY JOBGROUPS: List all unique jobgroups found in the source document.
        4) IDENTIFY SKIP LIST: From already_extracted, extract the list of jobgroups that were already extracted. These must be skipped.
        5) EXTRACT REMAINING: Extract ALL remaining jobgroups (those NOT in the skip list), starting from the first jobgroup not in already_extracted
        6) DETECT AGE GROUPS: Within each selected jobgroup, extract all age groups meeting the TABLE AGE GROUP SELECTION criteria.
        7) DETECT STEPS, EDUCATION LEVELS & CONTRACT TYPES: For each jobgroup being extracted, identify steps, worker types, education levels, and contract types following the TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE rules.
        8) CONSTRUCT TIMELINE STRUCTURE:
            8.1) Apply TABLE TIMELINE CONSTRUCTION rules to align job groups, steps, worker types, education levels, age bands, and contract types across table versions.
            8.2) Build one SalaryRow for every unique detected combination of (jobgroup × step × [worker type] × [age] × [education] × [contract type]) within the remaining jobgroups.
            8.3) Build timeline: For each SalaryRow, create one SalaryPoint per version/time period where that combination appears, then normalize labels (jobgroup/step/worker type/age/education/contract type), deduplicate identical periods, and align all points that refer to the same combination across versions (no imputation).
        9) APPLY NOTE ANTI-REPETITION RULES:
            - If same note appears for multiple rows with same jobgroup/step/age pattern: include once only
            - If same note applies to different jobgroups/steps/ages: use reference format "Note: [text] (applies to jobgroups A1-A10, steps 1-5)"
            - Keep all notes extremely concise (max 20 words, single sentence)
        10) SORT & CLEAN each row's timeline chronologically by start_date. Omit or nullify any fields not explicitly printed in the source.
        11) VERIFY (SOURCE-GROUNDED) that every extracted number/date/percentage/unit/clause is explicitly present in the input. Remove or correct anything not grounded.
        12) VALIDATE (SCHEMA & JSON) that the output is a valid JSON object that conforms exactly to the Pydantic schema (keys, types, null/""/omit conventions).
        13) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY a single valid JSON. No comments, no trailing commas, no text before/after.
        - Do NOT include fields not defined above.
        - Do NOT include any jobgroups from already_extracted
        - Schema summary (orientation only; responseSchema enforces structure):
            Output a single JSON object:
            {{
            "salary_information": [ SalaryRow, ... ]
            }}
        
    """

