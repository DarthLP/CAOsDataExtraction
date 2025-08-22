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
    - JSON files in input_pdfs/[CAO_NUMBER]/ folders
    - Corresponding PDF files in input_pdfs/[CAO_NUMBER]/ folders

OUTPUT:
    - Extracted JSON data in output_json/[CAO_NUMBER]/ folders
    - Error logs: failed_files_llm_extraction.txt, structured_output_parsing_errors.txt
"""

import os
import json
import time
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv
from OUTPUT_tracker import update_progress
import sys
import fcntl
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from monitoring_3_1 import PerformanceMonitor

# =========================
# Pydantic Schema for Structured Output
# =========================

class CAOExtractionSchema(BaseModel):
    """Schema for extracting structured data from Dutch CAO documents."""
    
    general_information: List[List[str]] = Field(
        description="Extract: Document title, contract period dates, validity dates, parties involved, scope of agreement, general contract terms.",
        default_factory=list
    )
    
    wage_information: List[List[str]] = Field(
        description="Extract: Complete wage tables with all columns and rows, salary scales, job classifications, hourly/monthly rates, ages, age-based increases, allowances, bonuses. RULE: From multiple wage tables with identical data but different units (monthly, hourly, yearly), extract a maximum of one table: prefer hourly, otherwise any one available.",
        default_factory=list
    )
    
    pension_information: List[List[str]] = Field(
        description="Extract: Pension scheme details, contribution percentages, employer/employee splits, retirement ages, eligibility requirements, pension fund information.",
        default_factory=list
    )
    
    leave_information: List[List[str]] = Field(
        description="Extract: Vacation days/weeks, holiday allowances, maternity leave, paternity leave, sick leave policies, special leave types.",
        default_factory=list
    )
    
    termination_information: List[List[str]] = Field(
        description="Extract: Notice periods, probation periods, termination procedures, dismissal rules, severance pay, exit requirements.",
        default_factory=list
    )
    
    overtime_information: List[List[str]] = Field(
        description="Extract: Overtime rates, shift differentials, weekend/holiday pay, night work compensation, maximum hours, overtime conditions.",
        default_factory=list
    )
    
    training_information: List[List[str]] = Field(
        description="Extract: Training entitlements, education budgets, professional development programs, course allowances, study time, certification support.",
        default_factory=list
    )
    
    homeoffice_information: List[List[str]] = Field(
        description="Extract: Remote work policies, home office allowances, equipment provisions, internet/phone reimbursements, work-from-home conditions, hybrid work arrangements.",
        default_factory=list
    )
    
    model_config = ConfigDict(
        title="CAO Extraction Schema",
        json_schema_extra={
            "propertyOrdering": [
                "general_information",
                "wage_information", 
                "pension_information",
                "leave_information",
                "termination_information",
                "overtime_information",
                "training_information",
                "homeoffice_information"
            ]
        }
    )

# =========================
# Configuration and Setup
# =========================

def acquire_file_lock(file_path):
    """Try to acquire a lock for processing a file. Returns True if lock acquired, False if already locked."""
    lock_file = file_path.with_suffix('.lock')
    try:
        # Try to create lock file
        with open(lock_file, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(f"Process {process_id + 1} using API key {key_number}\n")
            f.write(f"Timestamp: {time.time()}\n")
        return True
    except (IOError, OSError):
        # File is already locked by another process
        return False

def release_file_lock(file_path):
    """Release the lock for a file."""
    lock_file = file_path.with_suffix('.lock')
    try:
        if lock_file.exists():
            lock_file.unlink()
    except:
        pass  # Ignore errors when releasing lock

def announce_cao_once(cao_number):
    """Announce a CAO number only once across all processes using a simple file lock."""
    announce_file = OUTPUT_JSON_FOLDER / f".cao_{cao_number}_announced"
    try:
        # Try to create the announce file atomically
        with open(announce_file, 'x') as f:
            f.write(f"Announced by process {process_id + 1}\n")
        print(f"--- CAO {cao_number} ---")
        return True
    except FileExistsError:
        # Another process already announced this CAO (or from a previous run)
        return False

# Set paths and constants
INPUT_JSON_FOLDER = "output_json"
OUTPUT_JSON_FOLDER = Path("llmExtracted_json")
DEBUG_MODE = False
MAX_JSON_FILES = 350  # Limit how many JSON files to process
MAX_PROCESSING_TIME_HOURS = 1  # Maximum time to spend on a single file (hours)
SORTED_FILES = True  # True for sorted files, False for shuffled files within each CAO folder
OUTPUT_JSON_FOLDER.mkdir(exist_ok=True)

# Get key number from command line or default to 1
key_number = int(sys.argv[1]) if len(sys.argv) > 1 else 1
# Get process ID and total processes for work distribution
process_id = int(sys.argv[2]) if len(sys.argv) > 2 else 0
total_processes = int(sys.argv[3]) if len(sys.argv) > 3 else 1

# Set fixed seed for consistent shuffling across all processes
if not SORTED_FILES:
    import random
    random.seed(42)

# Load environment variables if not already loaded
load_dotenv()

# Try to get the specified API key, fallback to API key 1 if not found
api_key = os.getenv(f"GOOGLE_API_KEY{key_number}")
if not api_key:
    # Fallback to API key 1 if the specified key doesn't exist
    api_key = os.getenv("GOOGLE_API_KEY1")
    if not api_key:
        raise ValueError(f"Neither GOOGLE_API_KEY{key_number} nor GOOGLE_API_KEY1 environment variable found. Please set at least GOOGLE_API_KEY1 before running this script.")
    else:
        # Update key_number to 1 for consistency
        key_number = 1
        print(f"Warning: GOOGLE_API_KEY{key_number} not found, using GOOGLE_API_KEY1 instead")

# Initialize the new Gemini client using the recommended API
from google import genai
from google.genai import types
client = genai.Client(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-flash"  # Using flash for better structured output support

# LLM Generation Configuration - Optimized for Determinism + Performance
LLM_TEMPERATURE = 0.0  # Zero temperature for completely deterministic output
LLM_TOP_P = 0.1  # Keep low for determinism (restricts to most likely tokens)
LLM_TOP_K = 1  # Always pick the most likely token
LLM_MAX_TOKENS = 65536  # Maximum allowed for Gemini 2.5
LLM_CANDIDATE_COUNT = 1  # Single response only
LLM_SEED = 42  # Fixed seed for reproducible results
LLM_PRESENCE_PENALTY = 0  # No penalty for token reuse
LLM_FREQUENCY_PENALTY = 0  # No penalty for repeated tokens
LLM_THINKING_BUDGET = -1  # Dynamic thinking for adaptive reasoning

# =========================
# Performance Monitoring Setup
# =========================
performance_monitor = PerformanceMonitor(
    log_file="performance_logs/extraction_performance.jsonl",
    summary_file="performance_logs/extraction_summary.json"
)

# =========================
# Structured Output Configuration
# =========================

# Note: Using structured output with Pydantic schema instead of prompt-based JSON generation
# The schema is defined above in CAOExtractionSchema class

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
        # Check if file exists and is readable
        if not os.path.exists(pdf_path):
            return False, "PDF file does not exist"
        
        # Check file size
        file_size = os.path.getsize(pdf_path)
        if file_size == 0:
            return False, "PDF file is empty"
        
        # Check if file is too small (likely corrupted or not a real PDF)
        if file_size < 1024:  # Less than 1KB
            return False, f"PDF file too small ({file_size} bytes) - likely corrupted"
        
        # Check if file is unreasonably large (over 50MB)
        if file_size > 50 * 1024 * 1024:  # 50MB
            return False, f"PDF file too large ({file_size / (1024*1024):.1f}MB) - exceeds reasonable limit"
        
        # Try to read the first few bytes to check if it's a valid PDF
        with open(pdf_path, 'rb') as f:
            header = f.read(8)  # Read more bytes to be sure
            if not header.startswith(b'%PDF'):
                return False, "File does not appear to be a valid PDF (missing %PDF header)"
            
            # Try to read a bit more to ensure file is not corrupted
            f.seek(0)
            sample_data = f.read(1024)  # Read first 1KB
            if len(sample_data) < 100:  # If we can't read much, file might be corrupted
                return False, "PDF file appears to be corrupted or truncated"
        
        return True, f"PDF appears valid ({file_size / (1024*1024):.1f}MB)"
        
    except PermissionError:
        return False, "Permission denied - cannot read PDF file"
    except Exception as e:
        return False, f"Error checking PDF: {str(e)}"

def find_original_pdf(json_filename, cao_number):
    """
    Find the original PDF file that corresponds to the JSON file.
    The JSON files have exactly the same names as the PDF files but with .json extension.
    
    This function ensures we get the correct PDF by:
    1. Looking only in the specific CAO folder (cao_number)
    2. Using exact filename matching (just changing .json to .pdf)
    3. Validating that the PDF actually exists and is valid
    """
    # Remove .json extension to get the base name
    base_name = json_filename.replace('.json', '')
    
    # The PDF should be in the same CAO folder with exactly the same name but .pdf extension
    pdf_path = f"input_pdfs/{cao_number}/{base_name}.pdf"
    
    if os.path.exists(pdf_path):
        # Verify it's actually a valid PDF file
        is_valid, _ = check_pdf_quality(pdf_path)
        if is_valid:
            return pdf_path
        else:
            print(f"  WARNING: Found PDF at {pdf_path} but it's not a valid PDF file")
    
    # If exact match not found, try some common variations but be very specific
    possible_paths = [
        f"input_pdfs/{cao_number}/{base_name}.pdf",
        f"input_pdfs/{cao_number}/{base_name.replace('.pdf', '')}.pdf",  # Handle double .pdf extensions
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            # Verify it's actually a valid PDF file
            is_valid, _ = check_pdf_quality(path)
            if is_valid:
                return path
            else:
                print(f"  WARNING: Found PDF at {path} but it's not a valid PDF file")
    
    return None



def _sanitize_json_strings(possible_json: str) -> str:
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
            # Keep valid simple escapes; otherwise, normalize
            if nxt in ['\\', '"', '/', 'b', 'f', 'n', 'r', 't']:
                out_chars.append(ch)
            else:
                # Convert unknown escapes to backslash + char
                out_chars.append('\\')
                i -= 1  # reprocess current char as normal
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
                # Outside strings, keep as-is
                out_chars.append(ch)
                i += 1
                continue

        if ch in ('"', "'"):
            if in_string:
                if ch == quote_char:
                    # This could be an end-of-string or an inner quote. Heuristic:
                    # If next char is alnum or common text punctuation, assume it was an inner quote and escape it.
                    if nxt and (nxt.isalnum() or nxt in [' ', ',', '.', ':', ';', '-', '(', ')', '%', '+', '/']):
                        out_chars.append('\\"' if ch == '"' else "\\'")
                        i += 1
                        continue
                    # Otherwise treat as string terminator
                    out_chars.append(ch)
                    in_string = False
                    i += 1
                    continue
                else:
                    # Different quote inside string -> escape
                    out_chars.append('\\"' if ch == '"' else "\\'")
                    i += 1
                    continue
            else:
                # Start of string
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

def _normalize_json_text(raw_text: str) -> str:
    """Normalize common LLM artifacts before JSON parsing.
    - Trim to the outermost {...}
    - Remove BOM and zero-width spaces
    - Replace curly quotes with straight quotes
    """
    # Trim to outermost braces
    start = raw_text.find('{')
    end = raw_text.rfind('}')
    if start != -1 and end != -1 and end > start:
        raw_text = raw_text[start:end+1]
    # Remove BOM / zero-width
    raw_text = raw_text.replace('\ufeff', '').replace('\u200b', '').replace('\u200c', '').replace('\u200d', '')
    # Replace curly quotes
    translation_map = {
        ord('“'): '"', ord('”'): '"', ord('‟'): '"', ord('‟'): '"',
        ord('‟'): '"', ord('’'): "'", ord('‘'): "'",
    }
    raw_text = raw_text.translate(translation_map)
    return raw_text

def extract_with_pdf_upload(pdf_path, filename, cao_number, max_retries=5):
    """
    Extract using PDF upload approach - always upload the whole PDF.
    """
    print(f"  INFO: Using PDF upload approach for {filename}")
    
    start_time = time.time()
    file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    
    if not pdf_path or not os.path.exists(pdf_path):
        print(f"  ERROR: No original PDF found for {filename}")
        performance_monitor.log_extraction(
            filename=filename,
            file_size_mb=0,
            processing_time=time.time() - start_time,
            usage_metadata=None,
            success=False,
            error_message="PDF not found",
            api_key_used=key_number,
            process_id=process_id,
            cao_number=cao_number
        )
        return None
    
    print(f"  INFO: Found original PDF: {pdf_path}")
    
    # Check PDF quality before processing
    is_valid, quality_message = check_pdf_quality(pdf_path)
    if not is_valid:
        print(f"  ERROR: PDF quality check failed: {quality_message}")
        return None
    else:
        print(f"  INFO: {quality_message}")
    
    # Check file size and handle large files based on official Gemini API limits
    file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    if file_size_mb > 20.0:
        print(f"  ERROR: PDF file too large ({file_size_mb:.1f}MB) - exceeds 20MB limit. Skipping file.")
        return None
    elif file_size_mb > 15.0:
        print(f"  WARNING: Very large PDF file ({file_size_mb:.1f}MB) - approaching 20MB limit")
    elif file_size_mb > 10.0:
        print(f"  WARNING: Large PDF file ({file_size_mb:.1f}MB) - may cause timeout issues")
    elif file_size_mb > 5.0:
        print(f"  INFO: Large PDF file ({file_size_mb:.1f}MB) - may take longer to process")
    
    # Estimate page count and token usage (rough estimation)
    # Average PDF page is ~50KB, so estimate pages based on file size
    estimated_pages = int(file_size_mb * 1024 / 50)  # Rough estimation
    estimated_tokens = estimated_pages * 258  # 258 tokens per page
    
    # Basic quality check
    if file_size_mb < 0.1:
        print(f"  WARNING: Very small PDF ({file_size_mb:.2f}MB) - may be low quality or empty")
    elif file_size_mb < 0.5:
        print(f"  INFO: Small PDF ({file_size_mb:.2f}MB) - ensure good quality")
    
    if estimated_pages > 800:
        print(f"  WARNING: Estimated {estimated_pages} pages ({estimated_tokens:,} tokens) - approaching 1,000 page limit")
    elif estimated_pages > 500:
        print(f"  INFO: Estimated {estimated_pages} pages ({estimated_tokens:,} tokens) - large document")
    
    for attempt in range(max_retries):
        try:
            # Create the extraction prompt (concise, non-duplicative, context-preserving)
            extraction_prompt = f"""
You are an AI assistant extracting raw text from Dutch CAO PDFs.

Goal:
- Extract all relevant text into broad thematic categories as lists of complete snippets, where each snippet is an array of lines that preserves table rows and paragraph breaks.

Note:
- This is a user-provided CAO document; verbatim extraction is permitted for internal processing.

Rules:
- Preserve natural context; one snippet = one coherent unit (e.g., a wage table or a full paragraph).
- Do not micro-structure, split fields or tables; keep related data together. Include headers and descriptions of tables.
- Copy text literally (dates, numbers, percentages, units); no paraphrasing.
- Cover all relevant content for each category.
- Do not mix categories.
- NO HALLUCINATION - Only extract information that is explicitly present in the text.

Categories (with brief examples):
- general_information: contract period, validity. Example: "Contract period: 01/04/2019–31/03/2023".
- wage_information: wage tables with job groups, salaries, increases, age groups. Example: "Group I, 13,17 EUR/hour from 01/01/2018, age 21+".
    - From multiple wage tables with identical data but different units (monthly, hourly, yearly), extract a maximum of one table: prefer hourly, otherwise any one available.
- pension_information: schemes, premiums, retirement ages. Example: "Premium 21,4% in 2021, split evenly".
- leave_information: maternity/adoption/child leave, vacation/time/units/rules. Example: "8% holiday allowance".
- termination_information: notice periods, probation, rules. Example: "1 month notice if >6 months".
- overtime_information: overtime pay, hours limits, shift compensation. Example: "35% surcharge; 25% 20:00–22:00; 50% 22:00–06:00".
- training_information: training rights/budget/programs. Example: "2% payroll allocated; €175 POB".
- homeoffice_information: remote work rules/allowances. Example: "€3/day; max 8 days/month".

Formatting:
- Return only JSON matching CAOExtractionSchema keys.
- Each value is a list of snippets.
- Each snippet is an array of strings (lines). Use one array element per logical line of the source.

Document: {filename}
"""
            
            # Add dynamic timeout (in seconds) based on file size
            if file_size_mb > 8.0:
                timeout_seconds = 1200  # 20 minutes for very large files
            elif file_size_mb > 5.0:
                timeout_seconds = 900   # 15 minutes for large files
            else:
                timeout_seconds = 600   # 10 minutes for normal files
            
            # Upload the PDF file using the newer API
            print(f"  INFO: Uploading PDF file to Gemini...")
            uploaded_file = client.files.upload(file=pdf_path)

            # Wait for the uploaded PDF to be ready (ACTIVE) before generating content
            max_wait_seconds = 300 if file_size_mb <= 5.0 else (600 if file_size_mb <= 10.0 else 900)
            poll_interval_seconds = 2
            waited = 0
            
            while waited < max_wait_seconds:
                try:
                    file_resource = client.files.get(name=uploaded_file.name)
                    if file_resource.state.name == "ACTIVE":
                        break
                    elif file_resource.state.name == "FAILED":
                        raise ValueError(f"Uploaded file processing FAILED for {filename}")
                except Exception:
                    time.sleep(poll_interval_seconds)
                    waited += poll_interval_seconds
                    continue

                time.sleep(poll_interval_seconds)
                waited += poll_interval_seconds

            if waited >= max_wait_seconds:
                raise TimeoutError(f"Uploaded file not ACTIVE after {max_wait_seconds}s for {filename}")
            
            # Create the extraction prompt
            extraction_prompt = f"""
Extract information from this Dutch CAO (Collective Labor Agreement) PDF document.

TASK: Categorize and extract relevant information into the specified fields based on the document content.

TOPICS TO EXTRACT:
- General contract information (dates, parties, scope)
- Wage tables and salary information
- Pension schemes and contributions
- Leave policies and vacation entitlements
- Termination procedures and notice periods
- Overtime rates and shift compensation
- Training programs and budgets
- Remote work policies and allowances

CRITICAL RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally (dates, numbers, percentages, units)
- NO paraphrasing, NO interpretation, NO added explanations
- NO unnecessary separator lines or formatting characters, NO decorative elements
- Each snippet should be pure content from the document

CONTENT INCLUSION RULES:
- Include relevant headers, section titles, and table headers for each topic
- Include relevant numerical values, percentages, amounts, and time periods for each topic
- Include conditions, requirements, and procedural steps
- Include entitlements, allowances, and eligibility criteria

Document: {filename}
"""
            
            # Generate content with structured output using the newer API
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[extraction_prompt, uploaded_file],
                config={
                    "temperature": LLM_TEMPERATURE,
                    "top_p": LLM_TOP_P,
                    "top_k": LLM_TOP_K,
                    "max_output_tokens": LLM_MAX_TOKENS,
                    "candidate_count": LLM_CANDIDATE_COUNT,
                    "seed": LLM_SEED,
                    "presence_penalty": LLM_PRESENCE_PENALTY,
                    "frequency_penalty": LLM_FREQUENCY_PENALTY,
                    "response_mime_type": "application/json",
                    "response_schema": CAOExtractionSchema,
                    # Enable dynamic thinking for adaptive reasoning
                    "thinking_config": types.ThinkingConfig(thinking_budget=LLM_THINKING_BUDGET)
                }
            )

            # Access the structured output
            if hasattr(response, 'text') and response.text.strip():
                processing_time = time.time() - start_time
                print(f"  INFO: Successfully extracted structured data from PDF (time: {processing_time:.1f}s)")
                
                # Log successful extraction
                performance_monitor.log_extraction(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=response.usage_metadata,
                    success=True,
                    api_key_used=key_number,
                    process_id=process_id,
                    cao_number=cao_number
                )
                
                return response.text
            else:
                raise ValueError("No content returned by model")
                
        except Exception as e:
            error_str = str(e).lower()
            print(f"  DEBUG: PDF upload error type: {type(e).__name__}, Error message: {error_str}")
            
            # Handle timeout errors (504, DeadlineExceeded)
            if "deadlineexceeded" in error_str or "504" in error_str or "timeout" in error_str:
                if attempt < max_retries - 1:
                    wait_time = 120 * (2 ** attempt)  # 2min, 4min, 8min, 16min
                    print(f"  Attempt {attempt + 1} failed (timeout), retrying in {wait_time//60} minutes...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with timeout errors")
                    return None
            
            # Handle service unavailable errors (503)
            elif "serviceunavailable" in error_str or "503" in error_str or "connection reset" in error_str:
                if attempt < max_retries - 1:
                    wait_time = 60 * (2 ** attempt)  # 1min, 2min, 4min, 8min
                    print(f"  Attempt {attempt + 1} failed (service unavailable), retrying in {wait_time//60} minutes...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with service unavailable errors")
                    return None
            
            # Handle quota errors
            elif "quota" in error_str or "429" in error_str:
                if attempt < max_retries - 1:
                    wait_time = 60 * (2 ** attempt)  # 1min, 2min, 4min, 8min
                    print(f"  Attempt {attempt + 1} failed (quota), retrying in {wait_time//60} minutes...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with quota errors")
                    return None
            
            # Handle other errors
            elif attempt < max_retries - 1:
                wait_time = 30 * (2 ** attempt)
                print(f"  Attempt {attempt + 1} failed ({type(e).__name__}), retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                processing_time = time.time() - start_time
                print(f"  PDF upload failed after {max_retries} attempts")
                
                # Log failed extraction
                performance_monitor.log_extraction(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=None,
                    success=False,
                    error_message=f"Failed after {max_retries} attempts: {str(e)}",
                    api_key_used=key_number,
                    process_id=process_id,
                    cao_number=cao_number
                )
                return None
    
    return None

# =========================
# Main LLM Extraction Loop
# =========================

# Get all CAO number folders in input folder
cao_folders = sorted(
    [f for f in Path(INPUT_JSON_FOLDER).iterdir() if f.is_dir() and f.name.isdigit()],
    key=lambda f: int(f.name)
)

# Collect all JSON files from all CAO folders with their paths
all_json_files = []
for cao_folder in cao_folders:
    cao_number = cao_folder.name
    json_files = sorted(cao_folder.glob("*.json"))
    for json_file in json_files:
        all_json_files.append((cao_folder, json_file))

# Shuffle the entire file list once for consistent distribution across all processes
if not SORTED_FILES:
    import random
    random.shuffle(all_json_files)

# Process files using atomic file-level distribution
current_cao = None
processed_files = 0
successful_extractions = 0
failed_files = []
timed_out_files = []

for file_idx, (cao_folder, json_file) in enumerate(all_json_files):
    # Only process files that belong to this process
    if file_idx % total_processes != process_id:
        continue
        
    cao_number = cao_folder.name
    
    # Track current CAO for prefixing messages
    current_cao = cao_number
    
    # Create corresponding output folder
    output_cao_folder = OUTPUT_JSON_FOLDER / cao_number
    output_cao_folder.mkdir(exist_ok=True)
    
    # Check if output file already exists (skip if already processed)
    output_file = output_cao_folder / json_file.name
    if output_file.exists():
        print(f"  {cao_number}: Skipping {json_file.name} (already processed)")
        # Small delay when skipping to maintain flow
        time.sleep(5)
        successful_extractions += 1  # Count as successful since it was already processed
        continue
    
    # Check if we've reached the limit (only after skipping already processed files)
    if processed_files >= MAX_JSON_FILES:
        break
    
    # Try to acquire lock for this file to prevent double processing
    if not acquire_file_lock(output_file):
        print(f"  {cao_number}: Skipping {json_file.name} (being processed by another process)")
        time.sleep(2)
        continue
        
    # Find the corresponding PDF file
    original_pdf_path = find_original_pdf(json_file.name, cao_number)
    
    if not original_pdf_path or not os.path.exists(original_pdf_path):
        print(f"  {cao_number}: ✗ No PDF found for {json_file.name} [API {key_number}/{total_processes}]")
        failed_files.append(json_file.name)
        # Log failure immediately
        failed_log_path = "failed_files_llm_extraction.txt"
        with open(failed_log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name} (PDF not found)\n")
        continue
    
    # Check PDF quality before processing
    is_valid, quality_message = check_pdf_quality(original_pdf_path)
    if not is_valid:
        print(f"  {cao_number}: ✗ PDF quality check failed for {json_file.name}: {quality_message} [API {key_number}/{total_processes}]")
        failed_files.append(json_file.name)
        # Log failure immediately
        failed_log_path = "failed_files_llm_extraction.txt"
        with open(failed_log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name} (PDF quality: {quality_message})\n")
        continue
    

    
    # Check file size and handle large files based on official Gemini API limits
    file_size_mb = os.path.getsize(original_pdf_path) / (1024 * 1024)
    if file_size_mb > 20.0:
        print(f"  {cao_number}: ✗ PDF file too large ({file_size_mb:.1f}MB) - exceeds 20MB limit for {json_file.name} [API {key_number}/{total_processes}]")
        failed_files.append(json_file.name)
        # Log failure immediately
        failed_log_path = "failed_files_llm_extraction.txt"
        with open(failed_log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name} (PDF too large: {file_size_mb:.1f}MB)\n")
        continue
    
    print(f"  {cao_number}: {json_file.name} (PDF: {file_size_mb:.1f}MB) [API {key_number}/{total_processes}]")
    processed_files += 1

    try:
        extraction_start = time.time()
        max_processing_time = MAX_PROCESSING_TIME_HOURS * 3600  # Convert to seconds
        
        # Check if we've exceeded the maximum processing time
        if time.time() - extraction_start > max_processing_time:
            print(f"  {cao_number}: ⏰ Timeout after {MAX_PROCESSING_TIME_HOURS} hours for {json_file.name} [API {key_number}/{total_processes}]")
            timed_out_files.append(json_file.name)
            # Log timeout immediately
            timeout_log_path = "timed_out_files_llm_extraction.txt"
            with open(timeout_log_path, "a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name}\n")
            continue
            
        raw_output = extract_with_pdf_upload(original_pdf_path, json_file.name, cao_number)
        extraction_time = time.time() - extraction_start
        
        if not raw_output:
            print(f"  {cao_number}: ✗ LLM extraction failed for {json_file.name} [API {key_number}/{total_processes}]")
            failed_files.append(json_file.name)
            # Log failure immediately
            failed_log_path = "failed_files_llm_extraction.txt"
            with open(failed_log_path, "a", encoding="utf-8") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name}\n")
            continue

        # With proper structured output, we should get valid JSON directly
        out_path = output_cao_folder / json_file.name
        
        # Since we're using structured output, the raw_output should be valid JSON
        # Save it directly without parsing to avoid issues with large responses
        print(f"  INFO: Saving structured output directly (length: {len(raw_output)} chars)")
        
        # Save the raw structured output directly
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(raw_output)
        
        print(f"  {cao_number}: LLM extraction completed in {extraction_time:.2f} seconds [API {key_number}/{total_processes}]")
        successful_extractions += 1
        
        # Show progress every 10 files
        if processed_files % 10 == 0:
            performance_monitor.print_progress(len(all_json_files))
        
        # Check if we've reached the limit before waiting
        if processed_files >= MAX_JSON_FILES:
            break
        else:
            # Add 3-minute delay after successful request to prevent rate limiting
            time.sleep(180)
        continue
        
        # This section is no longer needed since we save the raw output directly above
        pass
    except Exception as e:
        import traceback
        print(f"  {cao_number}: Error with {json_file.name}: {e} [API {key_number}/{total_processes}]")
        traceback.print_exc()
        failed_files.append(json_file.name)
        # Log failure immediately
        failed_log_path = "failed_files_llm_extraction.txt"
        with open(failed_log_path, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {json_file.name}\n")
        # Release lock on error too
        release_file_lock(output_file)
    finally:
        # Always release the lock
        release_file_lock(output_file)

# Print completion message
if failed_files or timed_out_files:
    print(f"Process {process_id + 1} completed: {processed_files} actually processed, {successful_extractions} total successful (including skipped), {len(failed_files)} failed, {len(timed_out_files)} timed out")
    if failed_files:
        print(f"Failed files: {failed_files}")
    if timed_out_files:
        print(f"Timed out files: {timed_out_files}")
else:
    print(f"Process {process_id + 1} completed: {processed_files} actually processed, {successful_extractions} total successful (including skipped)")

# Final performance analysis
print("\n" + "="*60)
print("FINAL PERFORMANCE ANALYSIS")
print("="*60)
performance_monitor.analyze_performance()
performance_monitor.update_summary_file()
print(f"\n📁 Performance data saved to:")
print(f"   Detailed logs: {performance_monitor.log_file}")
print(f"   Summary: {performance_monitor.summary_file}")
