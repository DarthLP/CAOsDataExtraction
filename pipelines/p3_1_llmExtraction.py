"""
CAO Data Extraction Script with Structured Output
================================================

DESCRIPTION:
This script extracts raw text information from Dutch Collective Labor Agreement (CAO) PDF documents
using Google's Gemini AI with context-preserving extraction. It processes PDF files directly
and returns JSON data organized into broad thematic categories as complete text snippets.

FEATURES:
- Direct PDF upload to Gemini API for optimal accuracy
- Context-preserving extraction (keeps related information together)
- Multi-process support for parallel processing
- Robust error handling with exponential backoff
- PDF quality validation and best practices enforcement
- Dynamic timeouts based on file size
- File locking to prevent duplicate processing

USAGE:
    Single Process:
        python 3_1_llmExtraction.py [api_key_number]
        
    Multi-Process (4 parallel processes):
        python 3_1_llmExtraction.py 1 0 4  # Process 0 of 4 with API key 1
        python 3_1_llmExtraction.py 2 1 4  # Process 1 of 4 with API key 2

ARGUMENTS:
    api_key_number: Which API key to use (1, 2, 3, etc.) - defaults to 1
    process_id: Process ID for work distribution (0-based) - defaults to 0
    total_processes: Total number of parallel processes - defaults to 1

ENVIRONMENT VARIABLES:
    GOOGLE_API_KEY1, GOOGLE_API_KEY2, etc.: Google Gemini API keys

INPUT:
    - JSON files in {config['paths']['inputs_pdfs']}/[CAO_NUMBER]/ folders
- Corresponding PDF files in {config['paths']['inputs_pdfs']}/[CAO_NUMBER]/ folders

OUTPUT:
    - Extracted JSON data in output_json/[CAO_NUMBER]/ folders
    - Error logs: outputs/logs/failed_files_llm_extraction.txt, outputs/logs/structured_output_parsing_errors.txt
"""
import os
import sys
import json
import time
from pathlib import Path

# Add the parent directory to Python path so we can import monitoring
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import fcntl
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from monitoring.monitoring_3_1 import PerformanceMonitor


class CAOExtractionSchema(BaseModel):
    """Schema for extracting structured data from Dutch CAO documents."""
    general_information: List[List[str]] = Field(description=
        'Extract: Document title, contract period dates, validity dates, parties involved, scope of agreement. Be concise and focus on essential contract basics only.'
        , default_factory=list)
    wage_information: List[List[str]] = Field(description=
        'Extract: Wage information and wage tables with all columns and rows, salary scales, job classifications, hourly/monthly rates, ages, increases, allowances, bonuses and short descriptions. IMPORTANT: Extract ALL wage tables, including different worker types, different dates, different percentages, different job categories, and different time periods. Only skip if tables are identical except for the unit (e.g., hourly vs monthly rates for the same job/date).'
        , default_factory=list)
    pension_information: List[List[str]] = Field(description=
        'Extract: Pension scheme details, contribution percentages, employer/employee splits, retirement ages, eligibility requirements, pension fund information.'
        , default_factory=list)
    leave_information: List[List[str]] = Field(description=
        'Extract: Vacation days/weeks, holiday allowances, maternity leave, paternity leave, sick leave policies, special leave types.'
        , default_factory=list)
    termination_information: List[List[str]] = Field(description=
        'Extract: Notice periods, probation periods, termination procedures, dismissal rules, severance pay, exit requirements. Include complete termination notice period tables with all age/service year combinations.'
        , default_factory=list)
    overtime_information: List[List[str]] = Field(description=
        'Extract: Overtime rates, shift differentials, weekend/holiday pay, night work compensation, maximum hours, overtime conditions.'
        , default_factory=list)
    training_information: List[List[str]] = Field(description=
        'Extract: Training entitlements, education budgets, professional development programs, course allowances, study time, certification support.'
        , default_factory=list)
    homeoffice_information: List[List[str]] = Field(description=
        'Extract: Remote work policies, home office allowances, equipment provisions, internet/phone reimbursements, work-from-home conditions, hybrid work arrangements.'
        , default_factory=list)
    model_config = ConfigDict(title='CAO Extraction Schema',
        json_schema_extra={'propertyOrdering': ['general_information',
        'wage_information', 'pension_information', 'leave_information',
        'termination_information', 'overtime_information',
        'training_information', 'homeoffice_information']})


def acquire_file_lock(file_path):
    """Try to acquire a lock for processing a file. Returns True if lock acquired, False if already locked."""
    lock_file = file_path.with_suffix('.lock')
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
    lock_file = file_path.with_suffix('.lock')
    try:
        if lock_file.exists():
            lock_file.unlink()
    except:
        pass


def announce_cao_once(cao_number):
    """Announce a CAO number only once across all processes using a simple file lock."""
    announce_file = OUTPUT_JSON_FOLDER / f'.cao_{cao_number}_announced'
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
OUTPUT_JSON_FOLDER = Path(config['paths']['outputs_json']) / "new_flow"
MAX_JSON_FILES = 350
MAX_PROCESSING_TIME_HOURS = 1
SORTED_FILES = True
OUTPUT_JSON_FOLDER.mkdir(exist_ok=True)
key_number = int(sys.argv[1]) if len(sys.argv) > 1 else 1
process_id = int(sys.argv[2]) if len(sys.argv) > 2 else 0
total_processes = int(sys.argv[3]) if len(sys.argv) > 3 else 1
if not SORTED_FILES:
    import random
    random.seed(42)
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
from google import genai
from google.genai import types
client = genai.Client(api_key=api_key)
GEMINI_MODEL = 'gemini-2.5-flash'
LLM_TEMPERATURE = 0.0
LLM_TOP_P = 0.1
LLM_TOP_K = 1
LLM_MAX_TOKENS = 65536
LLM_CANDIDATE_COUNT = 1
LLM_SEED = 42
LLM_PRESENCE_PENALTY = 0
LLM_FREQUENCY_PENALTY = 0
LLM_THINKING_BUDGET = -1
performance_monitor = PerformanceMonitor(log_file=
    'performance_logs/extraction_performance.jsonl', summary_file=
    'performance_logs/extraction_summary.json')


def check_pdf_quality(pdf_path):
    """
    Comprehensive PDF quality check based on best practices.
    
    Checks:
    1. File exists and is readable
    2. File is not empty
    3. File has valid PDF header
    4. File size is reasonable (not too small, not too large)
    5. File can be opened and read
    """
    try:
        if not os.path.exists(pdf_path):
            return False, 'PDF file does not exist'
        file_size = os.path.getsize(pdf_path)
        if file_size == 0:
            return False, 'PDF file is empty'
        if file_size < 1024:
            return (False,
                f'PDF file too small ({file_size} bytes) - likely corrupted')
        if file_size > 50 * 1024 * 1024:
            return (False,
                f'PDF file too large ({file_size / (1024 * 1024):.1f}MB) - exceeds reasonable limit'
                )
        with open(pdf_path, 'rb') as f:
            header = f.read(8)
            if not header.startswith(b'%PDF'):
                return (False,
                    'File does not appear to be a valid PDF (missing %PDF header)'
                    )
            f.seek(0)
            sample_data = f.read(1024)
            if len(sample_data) < 100:
                return False, 'PDF file appears to be corrupted or truncated'
        return True, f'PDF appears valid ({file_size / (1024 * 1024):.1f}MB)'
    except PermissionError:
        return False, 'Permission denied - cannot read PDF file'
    except Exception as e:
        return False, f'Error checking PDF: {str(e)}'


def find_original_pdf(json_filename, cao_number):
    """
    Find the original PDF file that corresponds to the JSON file.
    The JSON files have exactly the same names as the PDF files but with .json extension.
    
    This function ensures we get the correct PDF by:
    1. Looking only in the specific CAO folder (cao_number)
    2. Using exact filename matching (just changing .json to .pdf)
    3. Validating that the PDF actually exists and is valid
    """
    base_name = json_filename.replace('.json', '')
    pdf_path = f"{config['paths']['inputs_pdfs']}/{cao_number}/{base_name}.pdf"
    if os.path.exists(pdf_path):
        is_valid, _ = check_pdf_quality(pdf_path)
        if is_valid:
            return pdf_path
        else:
            print(
                f"  WARNING: Found PDF at {pdf_path} but it's not a valid PDF file"
                )
    possible_paths = [
        f"{config['paths']['inputs_pdfs']}/{cao_number}/{base_name}.pdf",
        f"{config['paths']['inputs_pdfs']}/{cao_number}/{base_name.replace('.pdf', '')}.pdf"
        ]
    for path in possible_paths:
        if os.path.exists(path):
            is_valid, _ = check_pdf_quality(path)
            if is_valid:
                return path
            else:
                print(
                    f"  WARNING: Found PDF at {path} but it's not a valid PDF file"
                    )
    return None


def _sanitize_json_strings(possible_json: str) ->str:
    """Best-effort sanitizer for malformed JSON emitted by LLMs.
    - Escapes raw newlines and carriage returns inside strings
    - Normalizes stray backslashes to double backslashes when not a valid escape
    - Escapes likely-unescaped quotes inside strings that would otherwise break parsing
    """
    out_chars = []
    in_string = False
    escaped = False
    quote_char = '"'
    i = 0
    while i < len(possible_json):
        ch = possible_json[i]
        nxt = possible_json[i + 1] if i + 1 < len(possible_json) else ''
        if escaped:
            if nxt in ['\\', '"', '/', 'b', 'f', 'n', 'r', 't']:
                out_chars.append(ch)
            else:
                out_chars.append('\\')
                i -= 1
            escaped = False
            i += 1
            continue
        if ch == '\\':
            if in_string:
                escaped = True
                out_chars.append(ch)
                i += 1
                continue
            else:
                out_chars.append(ch)
                i += 1
                continue
        if ch in ('"', "'"):
            if in_string:
                if ch == quote_char:
                    if nxt and (nxt.isalnum() or nxt in [' ', ',', '.', ':',
                        ';', '-', '(', ')', '%', '+', '/']):
                        out_chars.append('\\"' if ch == '"' else "\\'")
                        i += 1
                        continue
                    out_chars.append(ch)
                    in_string = False
                    i += 1
                    continue
                else:
                    out_chars.append('\\"' if ch == '"' else "\\'")
                    i += 1
                    continue
            else:
                out_chars.append(ch)
                in_string = True
                quote_char = ch
                i += 1
                continue
        if in_string and ch == '\n':
            out_chars.append('\\n')
            i += 1
            continue
        if in_string and ch == '\r':
            out_chars.append('\\r')
            i += 1
            continue
        out_chars.append(ch)
        i += 1
    return ''.join(out_chars)


def _normalize_json_text(raw_text: str) ->str:
    """Normalize common LLM artifacts before JSON parsing.
    - Trim to the outermost {...}
    - Remove BOM and zero-width spaces
    - Replace curly quotes with straight quotes
    """
    start = raw_text.find('{')
    end = raw_text.rfind('}')
    if start != -1 and end != -1 and end > start:
        raw_text = raw_text[start:end + 1]
    raw_text = raw_text.replace('\ufeff', '').replace('\u200b', '').replace(
        '\u200c', '').replace('\u200d', '')
    translation_map = {ord('“'): '"', ord('”'): '"', ord('‟'): '"', ord('‟'
        ): '"', ord('‟'): '"', ord('’'): "'", ord('‘'): "'"}
    raw_text = raw_text.translate(translation_map)
    return raw_text


def extract_with_pdf_upload(pdf_path, filename, cao_number, max_retries=5):
    """
    Extract using PDF upload approach - always upload the whole PDF.
    """
    print(f'  INFO: Using PDF upload approach for {filename}')
    start_time = time.time()
    file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    if not pdf_path or not os.path.exists(pdf_path):
        print(f'  ERROR: No original PDF found for {filename}')
        performance_monitor.log_extraction(filename=filename, file_size_mb=
            0, processing_time=time.time() - start_time, usage_metadata=
            None, success=False, error_message='PDF not found',
            api_key_used=key_number, process_id=process_id, cao_number=
            cao_number)
        return None
    print(f'  INFO: Found original PDF: {pdf_path}')
    is_valid, quality_message = check_pdf_quality(pdf_path)
    if not is_valid:
        print(f'  ERROR: PDF quality check failed: {quality_message}')
        return None
    else:
        print(f'  INFO: {quality_message}')
    file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    if file_size_mb > 20.0:
        print(
            f'  ERROR: PDF file too large ({file_size_mb:.1f}MB) - exceeds 20MB limit. Skipping file.'
            )
        return None
    elif file_size_mb > 15.0:
        print(
            f'  WARNING: Very large PDF file ({file_size_mb:.1f}MB) - approaching 20MB limit'
            )
    elif file_size_mb > 10.0:
        print(
            f'  WARNING: Large PDF file ({file_size_mb:.1f}MB) - may cause timeout issues'
            )
    elif file_size_mb > 5.0:
        print(
            f'  INFO: Large PDF file ({file_size_mb:.1f}MB) - may take longer to process'
            )
    estimated_pages = int(file_size_mb * 1024 / 50)
    estimated_tokens = estimated_pages * 258
    if file_size_mb < 0.1:
        print(
            f'  WARNING: Very small PDF ({file_size_mb:.2f}MB) - may be low quality or empty'
            )
    elif file_size_mb < 0.5:
        print(f'  INFO: Small PDF ({file_size_mb:.2f}MB) - ensure good quality'
            )
    if estimated_pages > 800:
        print(
            f'  WARNING: Estimated {estimated_pages} pages ({estimated_tokens:,} tokens) - approaching 1,000 page limit'
            )
    elif estimated_pages > 500:
        print(
            f'  INFO: Estimated {estimated_pages} pages ({estimated_tokens:,} tokens) - large document'
            )
    for attempt in range(max_retries):
        try:
            if file_size_mb > 8.0:
                timeout_seconds = 1200
            elif file_size_mb > 5.0:
                timeout_seconds = 900
            else:
                timeout_seconds = 600
            print(f'  INFO: Uploading PDF file to Gemini...')
            uploaded_file = client.files.upload(file=pdf_path)
            max_wait_seconds = (300 if file_size_mb <= 5.0 else 600 if 
                file_size_mb <= 10.0 else 900)
            poll_interval_seconds = 2
            waited = 0
            while waited < max_wait_seconds:
                try:
                    file_resource = client.files.get(name=uploaded_file.name)
                    if file_resource.state.name == 'ACTIVE':
                        break
                    elif file_resource.state.name == 'FAILED':
                        raise ValueError(
                            f'Uploaded file processing FAILED for {filename}')
                except Exception:
                    time.sleep(poll_interval_seconds)
                    waited += poll_interval_seconds
                    continue
                time.sleep(poll_interval_seconds)
                waited += poll_interval_seconds
            if waited >= max_wait_seconds:
                raise TimeoutError(
                    f'Uploaded file not ACTIVE after {max_wait_seconds}s for {filename}'
                    )
            extraction_prompt = f"""
Extract information from this Dutch CAO (Collective Labor Agreement) PDF document.

TASK: Categorize and extract relevant information into the specified fields based on the document content.

CRITICAL RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally (dates, numbers, percentages, units)
- Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters

CONTENT INCLUSION RULES:
- Include relevant numerical values, percentages, amounts, and time periods
- Include conditions, requirements, procedural steps, entitlements, allowances, and eligibility criteria
- For tables, include short descriptions and table structure with headers and all data rows
- WAGE TABLES: Extract ALL wage tables. Skip only if tables are identical except for the unit (hourly vs monthly vs yearly rates for the same job/date).

TABLE FORMATTING:
- Preserve table structure: each table row should be a single array element containing all columns
- Keep table headers, descriptions, column names, and data rows together as complete units
- Maintain column alignment and spacing within each row

Document: {filename}
"""
            response = client.models.generate_content(model=GEMINI_MODEL,
                contents=[extraction_prompt, uploaded_file], config={
                'temperature': LLM_TEMPERATURE, 'top_p': LLM_TOP_P, 'top_k':
                LLM_TOP_K, 'max_output_tokens': LLM_MAX_TOKENS,
                'candidate_count': LLM_CANDIDATE_COUNT, 'seed': LLM_SEED,
                'presence_penalty': LLM_PRESENCE_PENALTY,
                'frequency_penalty': LLM_FREQUENCY_PENALTY,
                'response_mime_type': 'application/json', 'response_schema':
                CAOExtractionSchema, 'thinking_config': types.
                ThinkingConfig(thinking_budget=LLM_THINKING_BUDGET)})
            if response is None:
                raise ValueError('No response received from model')
            if hasattr(response, 'text'
                ) and response.text and response.text.strip():
                processing_time = time.time() - start_time
                print(
                    f'  INFO: Successfully extracted structured data from PDF (time: {processing_time:.1f}s)'
                    )
                performance_monitor.log_extraction(filename=filename,
                    file_size_mb=file_size_mb, processing_time=
                    processing_time, usage_metadata=response.usage_metadata,
                    success=True, api_key_used=key_number, process_id=
                    process_id, cao_number=cao_number)
                return response.text
            else:
                raise ValueError('No content returned by model')
        except Exception as e:
            error_str = str(e).lower()
            print(
                f'  DEBUG: PDF upload error type: {type(e).__name__}, Error message: {error_str}'
                )
            print(f'  DEBUG: Full error details: {e}')
            if hasattr(e, '__traceback__'):
                import traceback
                print(f'  DEBUG: Traceback: {traceback.format_exc()}')
            if ('deadlineexceeded' in error_str or '504' in error_str or 
                'timeout' in error_str):
                if attempt < max_retries - 1:
                    wait_time = 120 * 2 ** attempt
                    print(
                        f'  Attempt {attempt + 1} failed (timeout), retrying in {wait_time // 60} minutes...'
                        )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f'  All {max_retries} attempts failed with timeout errors'
                        )
                    return None
            elif 'serviceunavailable' in error_str or '503' in error_str or 'connection reset' in error_str:
                if attempt < max_retries - 1:
                    wait_time = 60 * 2 ** attempt
                    print(
                        f'  Attempt {attempt + 1} failed (service unavailable), retrying in {wait_time // 60} minutes...'
                        )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f'  All {max_retries} attempts failed with service unavailable errors'
                        )
                    return None
            elif 'quota' in error_str or '429' in error_str:
                if attempt < max_retries - 1:
                    wait_time = 60 * 2 ** attempt
                    print(
                        f'  Attempt {attempt + 1} failed (quota), retrying in {wait_time // 60} minutes...'
                        )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f'  All {max_retries} attempts failed with quota errors'
                        )
                    return None
            elif attempt < max_retries - 1:
                wait_time = 30 * 2 ** attempt
                print(
                    f'  Attempt {attempt + 1} failed ({type(e).__name__}), retrying in {wait_time} seconds...'
                    )
                time.sleep(wait_time)
                continue
            else:
                processing_time = time.time() - start_time
                print(f'  PDF upload failed after {max_retries} attempts')
                performance_monitor.log_extraction(filename=filename,
                    file_size_mb=file_size_mb, processing_time=
                    processing_time, usage_metadata=None, success=False,
                    error_message=
                    f'Failed after {max_retries} attempts: {str(e)}',
                    api_key_used=key_number, process_id=process_id,
                    cao_number=cao_number)
                return None
    return None


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
successful_extractions = 0
failed_files = []
timed_out_files = []
for file_idx, (cao_folder, json_file) in enumerate(all_json_files):
    if file_idx % total_processes != process_id:
        continue
    cao_number = cao_folder.name
    current_cao = cao_number
    output_cao_folder = OUTPUT_JSON_FOLDER / cao_number
    output_cao_folder.mkdir(exist_ok=True)
    output_file = output_cao_folder / json_file.name
    if output_file.exists():
        print(f'  {cao_number}: Skipping {json_file.name} (already processed)')
        time.sleep(5)
        successful_extractions += 1
        continue
    if processed_files >= MAX_JSON_FILES:
        break
    if not acquire_file_lock(output_file):
        print(
            f'  {cao_number}: Skipping {json_file.name} (being processed by another process)'
            )
        time.sleep(2)
        continue
    original_pdf_path = find_original_pdf(json_file.name, cao_number)
    if not original_pdf_path or not os.path.exists(original_pdf_path):
        print(
            f'  {cao_number}: ✗ No PDF found for {json_file.name} [API {key_number}/{total_processes}]'
            )
        failed_files.append(json_file.name)
        failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
        with open(failed_log_path, 'a', encoding='utf-8') as f:
            f.write(
                f"""{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name} (PDF not found)
"""
                )
        continue
    is_valid, quality_message = check_pdf_quality(original_pdf_path)
    if not is_valid:
        print(
            f'  {cao_number}: ✗ PDF quality check failed for {json_file.name}: {quality_message} [API {key_number}/{total_processes}]'
            )
        failed_files.append(json_file.name)
        failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
        with open(failed_log_path, 'a', encoding='utf-8') as f:
            f.write(
                f"""{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name} (PDF quality: {quality_message})
"""
                )
        continue
    file_size_mb = os.path.getsize(original_pdf_path) / (1024 * 1024)
    if file_size_mb > 20.0:
        print(
            f'  {cao_number}: ✗ PDF file too large ({file_size_mb:.1f}MB) - exceeds 20MB limit for {json_file.name} [API {key_number}/{total_processes}]'
            )
        failed_files.append(json_file.name)
        failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
        with open(failed_log_path, 'a', encoding='utf-8') as f:
            f.write(
                f"""{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name} (PDF too large: {file_size_mb:.1f}MB)
"""
                )
        continue
    print(
        f'  {cao_number}: {json_file.name} (PDF: {file_size_mb:.1f}MB) [API {key_number}/{total_processes}]'
        )
    processed_files += 1
    try:
        extraction_start = time.time()
        max_processing_time = MAX_PROCESSING_TIME_HOURS * 3600
        if time.time() - extraction_start > max_processing_time:
            print(
                f'  {cao_number}: ⏰ Timeout after {MAX_PROCESSING_TIME_HOURS} hours for {json_file.name} [API {key_number}/{total_processes}]'
                )
            timed_out_files.append(json_file.name)
            timeout_log_path = 'outputs/logs/timed_out_files_llm_extraction.txt'
            with open(timeout_log_path, 'a', encoding='utf-8') as f:
                f.write(
                    f"""{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name}
"""
                    )
            continue
        raw_output = extract_with_pdf_upload(original_pdf_path, json_file.
            name, cao_number)
        extraction_time = time.time() - extraction_start
        if not raw_output:
            print(
                f'  {cao_number}: ✗ LLM extraction failed for {json_file.name} [API {key_number}/{total_processes}]'
                )
            failed_files.append(json_file.name)
            failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
            with open(failed_log_path, 'a', encoding='utf-8') as f:
                f.write(
                    f"""{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name}
"""
                    )
            continue
        out_path = output_cao_folder / json_file.name
        print(
            f'  INFO: Saving structured output directly (length: {len(raw_output)} chars)'
            )
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(raw_output)
        print(
            f'  {cao_number}: LLM extraction completed in {extraction_time:.2f} seconds [API {key_number}/{total_processes}]'
            )
        successful_extractions += 1
        if processed_files % 10 == 0:
            performance_monitor.print_progress(len(all_json_files))
        if processed_files >= MAX_JSON_FILES:
            break
        else:
            time.sleep(180)
        continue
        pass
    except Exception as e:
        import traceback
        print(
            f'  {cao_number}: Error with {json_file.name}: {e} [API {key_number}/{total_processes}]'
            )
        traceback.print_exc()
        failed_files.append(json_file.name)
        failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
        with open(failed_log_path, 'a', encoding='utf-8') as f:
            f.write(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name}\n"
                )
        release_file_lock(output_file)
    finally:
        release_file_lock(output_file)
if failed_files or timed_out_files:
    print(
        f'Process {process_id + 1} completed: {processed_files} actually processed, {successful_extractions} total successful (including skipped), {len(failed_files)} failed, {len(timed_out_files)} timed out'
        )
    if failed_files:
        print(f'Failed files: {failed_files}')
    if timed_out_files:
        print(f'Timed out files: {timed_out_files}')
else:
    print(
        f'Process {process_id + 1} completed: {processed_files} actually processed, {successful_extractions} total successful (including skipped)'
        )
print('\n' + '=' * 60)
print('FINAL PERFORMANCE ANALYSIS')
print('=' * 60)
performance_monitor.analyze_performance()
performance_monitor.update_summary_file()
print(f"""
📁 Performance data saved to:""")
print(f'   Detailed logs: {performance_monitor.log_file}')
print(f'   Summary: {performance_monitor.summary_file}')

def main():
    """Main entry point for the LLM extraction pipeline (new version)."""
    # The script runs automatically when imported or executed
    pass

if __name__ == "__main__":
    main()
