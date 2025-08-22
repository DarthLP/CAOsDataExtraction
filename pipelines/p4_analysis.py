import os
import json
import pandas as pd
import time
from pathlib import Path
from deep_translator import GoogleTranslator
import google.generativeai as genai
from dotenv import load_dotenv
from utils.OUTPUT_tracker import update_progress
import re
import sys
import fcntl


def acquire_file_lock(file_path):
    """Try to acquire a lock for processing a file. Returns True if lock acquired, False if already locked."""
    lock_file = file_path.with_suffix('.analysis_lock')
    try:
        with open(lock_file, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(f'Process {process_id + 1} using API key {key_number}\n')
            f.write(f'Timestamp: {time.time()}\n')
        return True
    except (IOError, OSError):
        return False


def release_file_lock(file_path):
    """Release the lock for a file."""
    lock_file = file_path.with_suffix('.analysis_lock')
    try:
        if lock_file.exists():
            lock_file.unlink()
    except:
        pass


def announce_cao_once(cao_number):
    """Announce a CAO number only once across all processes using a simple file lock."""
    announce_file = Path('results') / f'.cao_{cao_number}_analysis_announced'
    try:
        with open(announce_file, 'x') as f:
            f.write(f'Announced by process {process_id + 1}\n')
        print(f'--- CAO {cao_number} ---')
        return True
    except FileExistsError:
        return False


import yaml
with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
INPUT_JSON_FOLDER = config['paths']['outputs_json']
FIELDS_PROMPT_PATH = f"{config['paths']['docs']}/fields_prompt.md"
FIELDS_PROMPT_SALARY_PATH = (
    f"{config['paths']['docs']}/fields_prompt_salary.md")
FIELDS_PROMPT_REST_PATH = f"{config['paths']['docs']}/fields_prompt_rest.md"
CAO_INFO_PATH = f"{config['paths']['inputs_pdfs']}/extracted_cao_info.csv"
DEBUG_MODE = False
MAX_JSON_FILES = 1000
MAX_PROCESSING_TIME_HOURS = 1
SORTED_FILES = False
key_number = int(sys.argv[1]) if len(sys.argv) > 1 else 1
process_id = int(sys.argv[2]) if len(sys.argv) > 2 else 0
total_processes = int(sys.argv[3]) if len(sys.argv) > 3 else 1
if not SORTED_FILES:
    import random
    random.seed(43)
load_dotenv()
api_key = os.getenv(f'GOOGLE_API_KEY{key_number}')
if not api_key:
    api_key = os.getenv('GOOGLE_API_KEY1')
    if not api_key:
        raise ValueError(
            f'Neither GOOGLE_API_KEY{key_number} nor GOOGLE_API_KEY1 environment variable found. Please set at least GOOGLE_API_KEY1 before running this script.'
            )
    else:
        key_number = 1
        print(
            f'Warning: GOOGLE_API_KEY{key_number} not found, using GOOGLE_API_KEY1 instead'
            )
OUTPUT_EXCEL_PATH = (
    f"{config['paths']['outputs_excel']}/extracted_data_process_{process_id + 1}.xlsx"
    )
genai.configure(api_key=api_key)
GEMINI_MODEL = 'gemini-2.5-pro'
LLM_TEMPERATURE = 0.0
LLM_TOP_P = 0.1
LLM_TOP_K = 1
LLM_MAX_TOKENS = None
LLM_CANDIDATE_COUNT = 1
INFOTYPE_FIELD_MAPPINGS = {'Pension': ['pension_premium_basic',
    'pension_premium_plus', 'retire_age_basic', 'retire_age_plus',
    'pension_age_group'], 'Leave': ['maternity_leave', 'maternity_pay',
    'maternity_note', 'vacation_time', 'vacation_unit', 'vacation_note'],
    'Termination': ['term_period_employer', 'term_employer_note',
    'term_period_worker', 'term_worker_note', 'probation_period',
    'probation_note'], 'Overtime': ['overtime_compensation', 'max_hrs',
    'min_hrs', 'shift_compensation', 'overtime_allowance_min',
    'overtime_allowance_max'], 'Training': ['training'], 'Homeoffice': [
    'Homeoffice']}
with open(FIELDS_PROMPT_PATH, 'r', encoding='utf-8') as f:
    prompt_fields_markdown = f.read()
columns = [col.strip() for col in prompt_fields_markdown.splitlines()[0].
    strip('|').split('|')]
with open(FIELDS_PROMPT_SALARY_PATH, 'r', encoding='utf-8') as f:
    prompt_salary_markdown = f.read()
with open(FIELDS_PROMPT_REST_PATH, 'r', encoding='utf-8') as f:
    prompt_rest_markdown = f.read()
salary_columns = [col.strip() for col in prompt_salary_markdown.splitlines(
    )[0].strip('|').split('|')]
rest_columns = [col.strip() for col in prompt_rest_markdown.splitlines()[0]
    .strip('|').split('|')]
salary_fields = [col for col in salary_columns if col != 'File_name']
rest_fields = [col for col in rest_columns if col != 'File_name']
if DEBUG_MODE:
    print(
        f'DEBUG: Dynamically extracted {len(salary_fields)} salary fields: {salary_fields}'
        )
    print(
        f'DEBUG: Dynamically extracted {len(rest_fields)} rest fields: {rest_fields}'
        )
    print(f'DEBUG: Total columns from main prompt: {len(columns)}')
    print(
        f"DEBUG: Expected total after merge: {len(set(salary_fields + rest_fields + ['File_name', 'CAO', 'id']))}"
        )


def verify_field_coverage():
    """
    Verify field coverage and explain the merge strategy.
    
    New Strategy:
    1. Extract salary fields from fields_prompt_salary.md
    2. Extract rest fields from fields_prompt_rest.md  
    3. Create complete structure from fields_prompt.md (ALL fields)
    4. Populate structure with extracted data from both extractions
    """
    programmatic_fields = {'CAO', 'id'}
    main_fields = set(columns)
    split_fields = set(salary_fields + rest_fields + ['File_name'])
    split_fields.update(programmatic_fields)
    extractable_fields = main_fields & split_fields
    missing_from_split = main_fields - split_fields
    extra_in_split = split_fields - main_fields
    if DEBUG_MODE:
        print(
            f'✓ Merge Strategy: Complete structure from fields_prompt.md ({len(main_fields)} fields)'
            )
        print(
            f'✓ Extractable fields: {len(extractable_fields)}/{len(main_fields)}'
            )
        if missing_from_split:
            print(
                f'⚠️  Fields in main prompt but not extractable: {missing_from_split}'
                )
            print('   (These will remain empty in final output)')
        if extra_in_split:
            print(
                f'ℹ️  Fields extractable but not in main structure: {extra_in_split}'
                )
            print('   (These will be ignored during merge)')
        coverage_percent = len(extractable_fields) / len(main_fields) * 100
        print(
            f'✓ Coverage: {coverage_percent:.1f}% of main structure is extractable'
            )
    return len(missing_from_split) == 0


verify_field_coverage()


def load_cao_info():
    """
    Load CAO information from CSV and create a mapping dictionary.
    Returns:
        dict: Mapping from composite key (pdf_name + cao_number) to CAO metadata.
    """
    cao_info_df = pd.read_csv(CAO_INFO_PATH, sep=';')
    cao_mapping = {}
    for _, row in cao_info_df.iterrows():
        pdf_name = row['pdf_name']
        cao_number = row['cao_number']
        composite_key = f'{pdf_name}_{cao_number}'
        cao_mapping[composite_key] = {'cao_number': cao_number, 'id': row[
            'id'], 'ingangsdatum': row['ingangsdatum'], 'expiratiedatum':
            row['expiratiedatum'], 'datum_kennisgeving': row[
            'datum_kennisgeving']}
    return cao_mapping


cao_info_mapping = load_cao_info()
df_results = pd.DataFrame(columns=columns)


def query_gemini(prompt, model=GEMINI_MODEL, max_retries=5):
    """
    Query Gemini model with improved exponential backoff retry logic for 504 errors.
    Args:
        prompt (str): The prompt to send to Gemini.
        model (str): The Gemini model name.
        max_retries (int): Maximum number of retry attempts.
    Returns:
        str: The raw Gemini output.
    """
    for attempt in range(max_retries):
        try:
            model_obj = genai.GenerativeModel(model)
            generation_config = genai.types.GenerationConfig(temperature=
                LLM_TEMPERATURE, top_p=LLM_TOP_P, top_k=LLM_TOP_K,
                max_output_tokens=LLM_MAX_TOKENS, candidate_count=
                LLM_CANDIDATE_COUNT)
            response = model_obj.generate_content(prompt, generation_config
                =generation_config)
            if hasattr(response, 'text') and response.text.strip():
                return response.text
            raise ValueError('Empty or invalid model response')
        except Exception as e:
            error_str = str(e).lower()
            if 'deadlineexceeded' in error_str or '504' in error_str:
                if attempt < max_retries - 1:
                    wait_time = 120 * 2 ** attempt
                    print(
                        f'  Attempt {attempt + 1} failed (504 timeout), retrying in {wait_time // 60} minutes... [API {key_number}/{total_processes}]'
                        )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f'  All {max_retries} attempts failed with 504 errors - skipping file [API {key_number}/{total_processes}]'
                        )
                    return ''
            elif any(keyword in error_str for keyword in ['quota',
                'rate limit', 'too many requests', '429']):
                if attempt < max_retries - 1:
                    wait_time = 120 * 2 ** attempt
                    print(
                        f'  Attempt {attempt + 1} failed (rate limit), retrying in {wait_time // 60} minutes... [API {key_number}/{total_processes}]'
                        )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f'  All {max_retries} attempts failed with rate limiting - skipping file [API {key_number}/{total_processes}]'
                        )
                    return ''
            elif attempt < max_retries - 1:
                wait_time = 120 * 2 ** attempt
                print(
                    f'  Attempt {attempt + 1} failed ({type(e).__name__}), retrying in {wait_time // 60} minutes... [API {key_number}/{total_processes}]'
                    )
                time.sleep(wait_time)
                continue
            else:
                raise e
    raise ValueError(f'All {max_retries} retry attempts failed')


def clean_gemini_output(output):
    """
    Clean the Gemini model output by removing markdown and trailing commas.
    Args:
        output (str): The raw output from Gemini.
    Returns:
        str: Cleaned output string.
    """
    if output.strip().startswith('```'):
        lines = output.strip().splitlines()
        content = '\n'.join(line for line in lines if not line.strip().
            startswith('```'))
    else:
        content = output.strip()
    import re
    content = re.sub(',\\s*(?=[}\\]])', '', content)
    return content


def extract_fields_from_text(text, prompt_fields_markdown, filename=''):
    """
    Generate a prompt with the list of desired fields and extract structured data from text.
    Args:
        text (str): The full CAO text to extract from.
        prompt_fields_markdown (str): Markdown table of fields.
        filename (str): The filename for context.
    Returns:
        dict: Extracted fields as a dictionary.
    """
    prompt = f"""You are an AI assistant that extracts structured JSON data from Dutch collective labor agreements (CAOs). These CAOs were originally provided as PDF files, and are now given to you as structured JSON files derived from them.

=== Source Text ===
The input is a shortened and grouped JSON-like structure. Each section is titled according to its content (e.g., "Wage information", "Pension information"), and contains a list of paragraphs or table contents from the CAO PDF relevant to that topic.
From file: {filename}

{text}

=== Extraction Fields ===
Below is a table of fields to extract. The first row contains the field names. The rows below describe each field. They have the following format: Description (expected format). Help or further guidance. Ex: one or more examples
{prompt_fields_markdown}

=== Extraction Instructions ===
- For each field in the table, extract the data from the source text.
- Do NOT hallucinate, infer, or guess any information. Only extract what is explicitly present in the text.
- Do NOT fill in missing data unless it is directly found in the source text.
- Do NOT repeat the prompt, instructions, or any explanations in your output.
- Do NOT use markdown, code blocks, or comments. Output only pure JSON.
- If a field or cell is empty, blank, or marked "empty" in the structure, leave it empty in your output.
- Rows marked with "..." indicate a repeating pattern. Use the 3 rows above to understand the pattern and complete it for all job groups present.
- Output all field names, even if the value is empty.

=== Special Domain Instructions ===
- Wage: When multiple wage tables are present, focus only on tables that represent standard or regular wages (sometimes referred to as "basic" or "normal" even if not labeled explicitly). If multiple tables exist for different job groups or levels under this standard wage type, include all of them. Prefer hourly units when both hourly and monthly wage tables are available. Only extract salary-related data for workers aged 21 and older.
- Pension: For all pension-related fields, help the model by searching for Dutch keywords like “AOW”, “pensioen”, and “regeling”.
- Leave, Termination, Overtime, Training and Homeoffice: For all fields related to leave, contract termination, working hours, overtime, training or homeoffice, extract as much relevant information as possible - more is better, as long as it is factually present in the text.

=== Output Format ===
Return ONLY valid JSON, for example:
{{"field1": "value1", "field2": "value2", ...}}
Do NOT wrap the JSON in code blocks or markdown. Do NOT include any explanations or comments.
Reminder: Only output factual information stated in the source text. No assumptions, no guesses. If unsure, leave the field empty.
"""
    raw_output = query_gemini(prompt)
    cleaned_output = clean_gemini_output(raw_output)
    try:
        return json.loads(cleaned_output)
    except Exception as e:
        if DEBUG_MODE:
            print(
                f'Failed to parse JSON from model output: {e}\nRaw output:\n{cleaned_output}'
                )
        return {}


def extract_salary_fields_from_text(text, prompt_fields_markdown, filename=''):
    """
    Extract salary-related fields from CAO text using only wage information.
    Args:
        text (str): The wage information section from CAO JSON.
        prompt_fields_markdown (str): Markdown table of salary fields.
        filename (str): The filename for context.
    Returns:
        dict: Extracted salary fields as a dictionary.
    """
    prompt = f"""You are an AI assistant that extracts structured JSON data from Dutch collective labor agreements (CAOs). These CAOs were originally provided as PDF files, and are now given to you as structured JSON files derived from them.

=== Source Text ===
The input is wage information from a CAO document. This section contains salary tables, job classifications, and wage-related rules.
{text}

=== Extraction Fields ===
Below is a table of salary fields to extract. The first row contains the field names. The rows below describe each field. They have the following format: Description (expected format). Help or further guidance. Ex: one or more examples
{prompt_fields_markdown}

=== Extraction Instructions ===
- For each field in the table, extract the data from the source text.
- Do NOT hallucinate, infer, or guess any information. Only extract what is explicitly present in the text.
- Do NOT fill in missing data unless it is directly found in the source text.
- Do NOT repeat the prompt, instructions, or any explanations in your output.
- Do NOT use markdown, code blocks, or comments. Output only pure JSON.
- Rows marked with "..." indicate a repeating pattern. Use the 3 rows above to understand the pattern and complete it for all job groups present.
- Output all field names, even if the value is empty.

=== Special Domain Instructions ===
- Wage: When multiple wage tables are present, focus only on tables that represent standard or regular wages (sometimes referred to as "basic" or "normal" even if not labeled explicitly). If multiple tables exist for different job groups or levels under this standard wage type, include all of them. Prefer hourly units when both hourly and monthly wage tables are available. Only extract salary-related data for workers aged 21 and older.
- IMPORTANT: Translate all extracted values from Dutch to English before outputting them.

=== Output Format ===
Return ONLY valid JSON, for example:
{{"field1": "value1", "field2": "value2", ...}}
Do NOT wrap the JSON in code blocks or markdown. Do NOT include any explanations or comments.
Reminder: Only output factual information stated directly in the source text! No assumptions, no guesses!
"""
    raw_output = query_gemini(prompt)
    if not raw_output:
        return None
    cleaned_output = clean_gemini_output(raw_output)
    try:
        return json.loads(cleaned_output)
    except Exception as e:
        if DEBUG_MODE:
            print(
                f'Failed to parse JSON from salary extraction: {e}\nRaw output:\n{cleaned_output}'
                )
        return {}


def extract_rest_fields_from_text(text, prompt_fields_markdown, filename=''):
    """
    Extract non-salary fields from CAO text using general, pension, leave, termination, overtime, training, and homeoffice information.
    Args:
        text (str): The non-wage sections from CAO JSON.
        prompt_fields_markdown (str): Markdown table of non-salary fields.
        filename (str): The filename for context.
    Returns:
        dict: Extracted non-salary fields as a dictionary.
    """
    prompt = f"""You are an AI assistant that extracts structured JSON data from Dutch collective labor agreements (CAOs). These CAOs were originally provided as PDF files, and are now given to you as structured JSON files derived from them.

=== Source Text ===
The input contains information from a CAO document, including general contract information, pension details, leave policies, termination procedures, overtime rules, training provisions, and home office policies.
From file: {filename}

{text}

=== Extraction Fields ===
Below is a table of fields to extract. The first row contains the field names. The rows below describe each field. They have the following format: Description (expected format). Help or further guidance. Ex: one or more examples
{prompt_fields_markdown}

=== Extraction Instructions ===
- For each field in the table, extract the data from the source text.
- Do NOT hallucinate, infer, or guess any information. Only extract what is explicitly present in the text.
- Do NOT fill in missing data unless it is directly found in the source text.
- Do NOT repeat the prompt, instructions, or any explanations in your output.
- Do NOT use markdown, code blocks, or comments. Output only pure JSON.
- Output all field names, even if the value is empty.

=== Special Domain Instructions ===
- Pension: For all pension-related fields, help the model by searching for Dutch keywords like "AOW", "pensioen", and "regeling".
- Leave, Termination, Overtime, Training and Homeoffice: For all fields related to leave, contract termination, working hours, overtime, training or homeoffice, extract as much relevant information as possible - more is better, as long as it is factually present in the text.
- IMPORTANT: Translate all extracted values from Dutch to English before outputting them.

=== Output Format ===
Return ONLY valid JSON, for example:
{{"field1": "value1", "field2": "value2", ...}}
Do NOT wrap the JSON in code blocks or markdown. Do NOT include any explanations or comments.
Reminder: Only output factual information stated in the source text! No assumptions, no guesses!
"""
    raw_output = query_gemini(prompt)
    if not raw_output:
        return None
    cleaned_output = clean_gemini_output(raw_output)
    try:
        return json.loads(cleaned_output)
    except Exception as e:
        if DEBUG_MODE:
            print(
                f'Failed to parse JSON from rest extraction: {e}\nRaw output:\n{cleaned_output}'
                )
        return {}


def merge_extraction_results(salary_extracted, rest_extracted):
    """
    Merge results from salary and rest extractions into multiple rows with specific infotype labels.
    Creates complete structure from fields_prompt.md and populates with extracted data.
    Handles both single dictionaries and lists of dictionaries.
    Args:
        salary_extracted (dict or list): Results from salary extraction.
        rest_extracted (dict or list): Results from rest extraction.
    Returns:
        list: List of merged extraction results with complete field structure and infotype labels.
    """
    if isinstance(salary_extracted, dict):
        salary_items = [salary_extracted]
    elif isinstance(salary_extracted, list):
        salary_items = salary_extracted
    else:
        salary_items = [{}]
    if isinstance(rest_extracted, dict):
        rest_items = [rest_extracted]
    elif isinstance(rest_extracted, list):
        rest_items = rest_extracted
    else:
        rest_items = [{}]
    if DEBUG_MODE:
        print(
            f'    DEBUG: Processing {len(salary_items)} salary items and {len(rest_items)} rest items'
            )
        for i, salary_item in enumerate(salary_items):
            if isinstance(salary_item, dict):
                populated_fields = [field for field, value in salary_item.
                    items() if value]
                print(
                    f'    DEBUG: Salary item {i + 1}: {len(populated_fields)} populated fields'
                    )
            else:
                print(f'    DEBUG: Salary item {i + 1}: {type(salary_item)}')
    merged_results = []
    for salary_item in salary_items:
        wage_row = {field: '' for field in columns}
        wage_row['infotype'] = 'Wage'
        if isinstance(salary_item, dict):
            for field, value in salary_item.items():
                if field in wage_row and value:
                    wage_row[field] = value
        elif DEBUG_MODE:
            print(
                f'    DEBUG: Skipping non-dict salary item: {type(salary_item)}'
                )
        for rest_item in rest_items:
            if isinstance(rest_item, dict):
                if 'start_date_contract' in rest_item and rest_item[
                    'start_date_contract']:
                    wage_row['start_date_contract'] = rest_item[
                        'start_date_contract']
                if 'expiry_date_contract' in rest_item and rest_item[
                    'expiry_date_contract']:
                    wage_row['expiry_date_contract'] = rest_item[
                        'expiry_date_contract']
        merged_results.append(wage_row)
        if DEBUG_MODE:
            populated_wage_fields = [field for field, value in wage_row.
                items() if value]
            print(
                f'    DEBUG: Created wage row with {len(populated_wage_fields)} populated fields: {populated_wage_fields}'
                )
    for rest_item in rest_items:
        if not isinstance(rest_item, dict):
            if DEBUG_MODE:
                print(
                    f'    DEBUG: Skipping non-dict rest item: {type(rest_item)}'
                    )
            continue
        for infotype, fields in INFOTYPE_FIELD_MAPPINGS.items():
            rest_row = {field: '' for field in columns}
            rest_row['infotype'] = infotype
            for field in fields:
                if field in rest_item and rest_item[field]:
                    rest_row[field] = rest_item[field]
            if 'start_date_contract' in rest_item and rest_item[
                'start_date_contract']:
                rest_row['start_date_contract'] = rest_item[
                    'start_date_contract']
            if 'expiry_date_contract' in rest_item and rest_item[
                'expiry_date_contract']:
                rest_row['expiry_date_contract'] = rest_item[
                    'expiry_date_contract']
            merged_results.append(rest_row)
    if DEBUG_MODE:
        print(f'    DEBUG: Created {len(merged_results)} merged result(s)')
        for i, result in enumerate(merged_results):
            populated_fields = [field for field, value in result.items() if
                value]
            infotype = result.get('infotype', 'Unknown')
            print(
                f"    DEBUG: Result {i + 1}: {len(populated_fields)}/{len(columns)} fields populated, infotype='{infotype}'"
                )
            if result.get('jobgroup'):
                print(
                    f"    DEBUG: Result {i + 1}: jobgroup = '{result['jobgroup']}'"
                    )
            if infotype == 'Wage':
                salary_fields = [field for field in result.keys() if field.
                    startswith('salary_') and result[field]]
                print(
                    f'    DEBUG: Wage row {i + 1} has {len(salary_fields)} salary fields: {salary_fields}'
                    )
    return merged_results


def flatten_to_str_list(lst):
    """
    Recursively flatten a nested list into a list of strings, joining sublists with ' | '.
    Args:
        lst (list): The list to flatten.
    Returns:
        list: Flattened list of strings.
    """
    result = []
    for item in lst:
        if isinstance(item, list):
            result.append(' | '.join(str(subitem) for subitem in
                flatten_to_str_list(item)))
        else:
            result.append(str(item))
    return result


def normalize_filename(name):
    """
    Normalize filename for robust duplicate detection: lowercase, remove extension, and strip non-alphanumeric characters.
    """
    name = name.lower()
    name = re.sub('\\.[^.]+$', '', name)
    name = re.sub('[^a-z0-9]', '', name)
    return name


cao_analysis_tracking = {}
cao_folders = sorted([f for f in Path(INPUT_JSON_FOLDER).iterdir() if f.
    is_dir() and f.name.isdigit()], key=lambda f: int(f.name))
all_json_files = []
for cao_folder in cao_folders:
    cao_number = cao_folder.name
    json_files = sorted(cao_folder.glob('*.json'))
    for json_file in json_files:
        all_json_files.append((cao_folder, json_file))
if not SORTED_FILES:
    import random
    random.shuffle(all_json_files)
current_cao = None
processed_files = 0
successful_analyses = 0
failed_files = []
timed_out_files = []
for file_idx, (cao_folder, json_file) in enumerate(all_json_files):
    if file_idx % total_processes != process_id:
        continue
    if processed_files >= MAX_JSON_FILES:
        break
    cao_number = cao_folder.name
    current_cao = cao_number
    if not acquire_file_lock(json_file):
        print(
            f'  {cao_number}: Skipping {json_file.name} (being processed by another process)'
            )
        time.sleep(2)
        continue
    try:
        cao_id = None
        pdf_name_cleaned = json_file.stem + '.pdf'
        try:
            cao_number = int(json_file.parent.name)
        except (ValueError, AttributeError):
            cao_number = None
        if cao_number:
            composite_key = f'{pdf_name_cleaned}_{cao_number}'
            if composite_key in cao_info_mapping:
                cao_info = cao_info_mapping[composite_key]
                cao_number = cao_info['cao_number']
                cao_id = cao_info['id']
            else:

                def normalize_lookup(s):
                    return s.replace(' ', '').replace('-', '').replace('_', ''
                        ).lower()
                normalized_cleaned = normalize_lookup(pdf_name_cleaned)
                found = False
                for key in cao_info_mapping.keys():
                    key_pdf_name = key.rsplit('_', 1)[0]
                    if normalize_lookup(key_pdf_name) == normalized_cleaned:
                        cao_info = cao_info_mapping[key]
                        if cao_info['cao_number'] == cao_number:
                            cao_id = cao_info['id']
                            found = True
                            break
                if not found and DEBUG_MODE:
                    print(
                        f"[DEBUG] Could not find CAO id for {json_file.name} with CAO {cao_number} (tried composite key '{composite_key}' and fuzzy match)"
                        )
        elif DEBUG_MODE:
            print(
                f'[DEBUG] Could not extract CAO number from folder for {json_file.name}'
                )
        final_excel_path = (
            f"{config['paths']['outputs_excel']}/extracted_data.xlsx")
        already_processed = False
        if os.path.exists(final_excel_path):
            try:
                time.sleep(0.1)
                existing_df = pd.read_excel(final_excel_path)
                if ('File_name' in existing_df.columns and 'CAO' in
                    existing_df.columns):
                    file_exists = existing_df[(existing_df['File_name'] ==
                        json_file.name) & (existing_df['CAO'].astype(str) ==
                        str(cao_number))].shape[0] > 0
                    if file_exists:
                        already_processed = True
                        print(
                            f'  {cao_number}: Skipping {json_file.name} (already in final Excel file for CAO {cao_number})'
                            )
                        release_file_lock(json_file)
                        continue
            except Exception as e:
                if DEBUG_MODE:
                    print(f'  Could not check final Excel file: {e}')
        if already_processed:
            release_file_lock(json_file)
            continue
        with open(json_file, 'r', encoding='utf-8') as f:
            context_by_infotype = json.load(f)
        print(
            f'  {cao_number}: {json_file.name} [API {key_number}/{total_processes}]'
            )
        file_start = time.time()
        max_processing_time = MAX_PROCESSING_TIME_HOURS * 3600
        processed_files += 1
        salary_text = ''
        if 'Wage information' in context_by_infotype:
            value = context_by_infotype['Wage information']
            if isinstance(value, list):
                flat_value = flatten_to_str_list(value)
                salary_text = f'== Wage information ==\n' + '\n'.join(
                    flat_value)
            elif isinstance(value, str):
                salary_text = f'== Wage information ==\n{value}'
        rest_sections = ['General information', 'Pension information',
            'Leave information', 'Termination information',
            'Overtime information', 'Training information',
            'Homeoffice information']
        rest_text_parts = []
        for section in rest_sections:
            if section in context_by_infotype:
                value = context_by_infotype[section]
                if isinstance(value, list):
                    flat_value = flatten_to_str_list(value)
                    rest_text_parts.append(f'== {section} ==\n' + '\n'.join
                        (flat_value))
                elif isinstance(value, str):
                    rest_text_parts.append(f'== {section} ==\n{value}')
        rest_text = '\n\n'.join(rest_text_parts)
        if time.time() - file_start > max_processing_time:
            print(
                f'  {cao_number}: ⏰ Timeout after {MAX_PROCESSING_TIME_HOURS} hours for {json_file.name} [API {key_number}/{total_processes}]'
                )
            timed_out_files.append(json_file.name)
            continue
        salary_request_size = len(salary_text.encode('utf-8')) / 1024
        salary_request_chars = len(salary_text)
        print(
            f'  {cao_number}: Salary LLM extraction (Request size: {salary_request_size:.1f} KB, {salary_request_chars:,} characters) [API {key_number}/{total_processes}]'
            )
        salary_start = time.time()
        salary_extracted = extract_salary_fields_from_text(salary_text,
            prompt_salary_markdown, filename=json_file.name)
        salary_time = time.time() - salary_start
        print(
            f'  {cao_number}: Salary LLM extraction completed in {salary_time:.2f} seconds [API {key_number}/{total_processes}]'
            )
        if salary_extracted is None:
            print(
                f'  {cao_number}: ✗ Salary extraction failed for {json_file.name} [API {key_number}/{total_processes}]'
                )
            failed_files.append(json_file.name)
            continue
        if DEBUG_MODE:
            print(f'  DEBUG: Salary extracted data: {salary_extracted}')
            if salary_extracted:
                if isinstance(salary_extracted, dict):
                    populated_salary_fields = [field for field, value in
                        salary_extracted.items() if value]
                    print(
                        f'  DEBUG: Populated salary fields: {populated_salary_fields}'
                        )
                elif isinstance(salary_extracted, list):
                    print(
                        f'  DEBUG: Salary extracted is a list with {len(salary_extracted)} items'
                        )
                    for i, item in enumerate(salary_extracted):
                        if isinstance(item, dict):
                            populated_fields = [field for field, value in
                                item.items() if value]
                            print(
                                f'  DEBUG: Item {i + 1} has {len(populated_fields)} populated fields: {populated_fields}'
                                )
                        else:
                            print(f'  DEBUG: Item {i + 1} is {type(item)}')
                else:
                    print(
                        f'  DEBUG: Salary extracted is {type(salary_extracted)}'
                        )
            else:
                print(f'  DEBUG: No salary data extracted!')
        if time.time() - file_start > max_processing_time:
            print(
                f'  {cao_number}: ⏰ Timeout after {MAX_PROCESSING_TIME_HOURS} hours for {json_file.name} [API {key_number}/{total_processes}]'
                )
            timed_out_files.append(json_file.name)
            continue
        time.sleep(60)
        rest_request_size = len(rest_text.encode('utf-8')) / 1024
        rest_request_chars = len(rest_text)
        print(
            f'  {cao_number}: Rest LLM extraction (Request size: {rest_request_size:.1f} KB, {rest_request_chars:,} characters) [API {key_number}/{total_processes}]'
            )
        rest_start = time.time()
        rest_extracted = extract_rest_fields_from_text(rest_text,
            prompt_rest_markdown, filename=json_file.name)
        rest_time = time.time() - rest_start
        print(
            f'  {cao_number}: Rest LLM extraction completed in {rest_time:.2f} seconds [API {key_number}/{total_processes}]'
            )
        if rest_extracted is None:
            print(
                f'  {cao_number}: ✗ Rest extraction failed for {json_file.name} [API {key_number}/{total_processes}]'
                )
            failed_files.append(json_file.name)
            continue
        merge_start = time.time()
        extracted = merge_extraction_results(salary_extracted, rest_extracted)
        merge_time = time.time() - merge_start
        print(
            f'  {cao_number}: Merge operation completed in {merge_time:.2f} seconds [API {key_number}/{total_processes}]'
            )
        if not extracted:
            print(
                f'  {cao_number}: ✗ Failed to extract data from {json_file.name}'
                )
            failed_files.append(json_file.name)
            continue
        if isinstance(extracted, dict):
            extracted_items = [extracted]
        elif isinstance(extracted, list):
            extracted_items = extracted
        else:
            extracted_items = []
        for item in extracted_items:
            row = dict.fromkeys(columns, '')
            for key, value in item.items():
                if key in row:
                    row[key] = value
            row['CAO'] = str(cao_number) if cao_number else json_file.stem
            row['id'] = str(cao_id) if cao_id else ''
            row['TTW'] = 'yes' if 'TTW' in json_file.stem.upper() else 'no'
            row['File_name'] = json_file.name
            pdf_name = json_file.stem + '.pdf'
            if cao_number:
                composite_key = f'{pdf_name}_{cao_number}'
                if composite_key in cao_info_mapping:
                    cao_info = cao_info_mapping[composite_key]
                    row['CAO'] = cao_info['cao_number']
                    row['id'] = cao_info['id']
                    row['start_date'] = cao_info['ingangsdatum']
                    row['expiry_date'] = cao_info['expiratiedatum']
                    row['date_of_formal_notification'] = cao_info[
                        'datum_kennisgeving']
                elif DEBUG_MODE:
                    print(
                        f'  No CAO info found for composite key: {composite_key}'
                        )
            elif DEBUG_MODE:
                print(f'  No CAO number available for PDF: {pdf_name}')
            if DEBUG_MODE:
                print('Row content before appending:', row)
            row_df = pd.DataFrame([row])
            row_df_full = row_df.reindex(columns=df_results.columns)
            row_df_full_filled = row_df_full.fillna('Empty')
            if DEBUG_MODE:
                print('About to append row:')
                print(row_df_full_filled)
                print('All NA after replace check?', row_df_full_filled.
                    replace(['Empty', ''], pd.NA).isna().all(axis=1))
            row_to_append_check = row_df_full_filled.replace(['Empty', '',
                None], pd.NA)
            nonmeta_cols = [col for col in row_to_append_check.columns if 
                col not in ('CAO', 'TTW', 'File_name', 'id', 'infotype')]
            result = row_to_append_check[nonmeta_cols].isna().all(axis=1)
            if isinstance(result, bool):
                is_all_na = result
            else:
                is_all_na = result.iloc[0]
            if is_all_na:
                print('Skipped appending due to only Empty values.')
                continue
            if DEBUG_MODE:
                print('Appending row after check passed:')
                print(row_df_full_filled)
                print('ROW BEFORE CONCAT:')
                print(row_df_full_filled)
                print('ROW TYPES:')
                print(row_df_full_filled.dtypes)
            if df_results.empty:
                df_results = row_df_full_filled
            else:
                df_results = df_results.astype('object')
                row_df_full_filled = row_df_full_filled.astype('object')
                df_results = pd.concat([df_results, row_df_full_filled],
                    ignore_index=True, copy=False)
            df_results.replace('Empty', pd.NA, inplace=True)
        file_time = time.time() - file_start
        print(
            f'  {cao_number}: Total file processing time: {file_time:.2f} seconds [API {key_number}/{total_processes}]'
            )
        successful_analyses += 1
        if cao_number:
            if cao_number not in cao_analysis_tracking:
                cao_analysis_tracking[cao_number] = {'successful': 0,
                    'failed': 0}
            cao_analysis_tracking[cao_number]['successful'] += 1
            update_progress(cao_number, 'llm_analysis', successful=
                cao_analysis_tracking[cao_number]['successful'],
                failed_files=cao_analysis_tracking[cao_number].get(
                'failed_files', []))
        os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
        df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)
        if processed_files >= MAX_JSON_FILES:
            print(
                f'  {cao_number}: Reached MAX_JSON_FILES limit, exiting [API {key_number}/{total_processes}]'
                )
            break
        else:
            print(
                f'  {cao_number}: Starting 3-minute delay... [API {key_number}/{total_processes}]'
                )
            delay_start = time.time()
            time.sleep(180)
            delay_time = time.time() - delay_start
            print(
                f'  {cao_number}: 3-minute delay completed in {delay_time:.2f} seconds [API {key_number}/{total_processes}]'
                )
    except Exception as e:
        print(f'  {cao_number}: ✗ Error processing {json_file.name}: {e}')
        failed_files.append(json_file.name)
        release_file_lock(json_file)
    finally:
        release_file_lock(json_file)
os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)
if failed_files:
    failed_log_path = 'failed_files_analysis.txt'
    with open(failed_log_path, 'a', encoding='utf-8') as f:
        f.write(
            f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}\n"
            )
        for file in failed_files:
            f.write(f'API {key_number}: {file}\n')
    print(f'Failed files saved to: {failed_log_path}')
if timed_out_files:
    timeout_log_path = 'timed_out_files_analysis.txt'
    with open(timeout_log_path, 'a', encoding='utf-8') as f:
        f.write(
            f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}\n"
            )
        for file in timed_out_files:
            f.write(f'API {key_number}: {file}\n')
    print(f'Timed out files saved to: {timeout_log_path}')
if failed_files or timed_out_files:
    print(
        f'Process {process_id + 1} completed: {successful_analyses} successful, {len(failed_files)} failed, {len(timed_out_files)} timed out'
        )
    if failed_files:
        print(f'Failed files: {failed_files}')
    if timed_out_files:
        print(f'Timed out files: {timed_out_files}')
else:
    print(
        f'Process {process_id + 1} completed: {successful_analyses} successful'
        )

def main():
    """Main entry point for the analysis pipeline."""
    # The script runs automatically when imported or executed
    pass

if __name__ == "__main__":
    main()
