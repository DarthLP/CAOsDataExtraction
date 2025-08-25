"""
Single File CAO Data Extraction Script (Markdown Version)
=======================================================

DESCRIPTION:
This script extracts raw text information from a specific Dutch Collective Labor Agreement (CAO) 
markdown document (parsed text) using Google's Gemini AI with context-preserving extraction. It processes a single 
markdown file directly and returns JSON data organized into broad thematic categories.

FEATURES:
- Direct markdown upload to Gemini API for optimal accuracy
- Context-preserving extraction (keeps related information together)
- Target specific CAO files by number and filename
- Robust error handling with exponential backoff
- Markdown quality validation and best practices enforcement
- Dynamic timeouts based on file size
- File locking to prevent duplicate processing

USAGE:
    python scripts/single_file_extraction_json.py <cao_number> <markdown_filename> [api_key_number]
    
    Examples:
        python scripts/single_file_extraction_json.py 10 "Bouw CAO 2011 aanmelding TTW 1-2011.md"
        python scripts/single_file_extraction_json.py 1536 "Ned_Universiteiten_1536.md" 2

ARGUMENTS:
    cao_number: CAO number to process (e.g., 10, 1536)
    markdown_filename: Specific markdown filename to process
    api_key_number: Which API key to use (1, 2, 3, etc.) - defaults to 1

ENVIRONMENT VARIABLES:
    GOOGLE_API_KEY1, GOOGLE_API_KEY2, etc.: Google Gemini API keys

INPUT:
    - Markdown files in {config['paths']['parsed_pdfs_markdown']}/[CAO_NUMBER]/ folders

OUTPUT:
    - Extracted JSON data in outputs/llm_extracted/single_file/json_single/[CAO_NUMBER]/ folders
    - Error logs: outputs/logs/failed_files_llm_extraction.txt, outputs/logs/structured_output_parsing_errors.txt
"""
import os
import sys
import json
import time
from pathlib import Path
import yaml

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
        'Extract: Wage tables and salary information, including job classifications, ages, increases, bonuses and short descriptions. SKIP: Tables identical except unit conversion (hourly vs monthly vs weekly vs 4 weeks for same data). KEEP: Tables with different periods, worker types, job categories, or other differences.'
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
            f.write(f'Single file extraction using API key {key_number}\n')
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
    """Print CAO announcement only once per CAO number across all processes."""
    print(f"--- CAO {cao_number} ---")


with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
INPUT_JSON_FOLDER = config['paths']['parsed_pdfs_markdown']
# Parse command line arguments
if len(sys.argv) < 3:
    print("❌ Usage: python scripts/single_file_extraction_json.py <cao_number> <markdown_filename> [api_key_number]")
    print()
    print("Examples:")
    print("  python scripts/single_file_extraction_json.py 10 \"Bouw CAO 2011 aanmelding TTW 1-2011.md\"")
    print("  python scripts/single_file_extraction_json.py 1536 \"Ned_Universiteiten_1536.md\" 2")
    sys.exit(1)

target_cao_number = sys.argv[1]
target_json_filename = sys.argv[2]
key_number = int(sys.argv[3]) if len(sys.argv) > 3 else 1

OUTPUT_JSON_FOLDER = Path(config['paths']['outputs_json']) / "single_file" / "json_single"
OUTPUT_JSON_FOLDER.mkdir(exist_ok=True)

print(f"🎯 Single File CAO Extraction")
print(f"📊 CAO Number: {target_cao_number}")
print(f"📄 JSON File: {target_json_filename}")
print(f"🔑 API Key: {key_number}")
print(f"📁 Output: {OUTPUT_JSON_FOLDER}")
print()
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
GEMINI_MODEL = 'gemini-2.5-pro'
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


def extract_with_json_upload(markdown_path, filename, cao_number, max_retries=2):
    """
    Extract using Files API approach - upload markdown file to Gemini.
    """
    print(f'  INFO: Using Files API approach for {filename}')
    start_time = time.time()
    file_size_mb = os.path.getsize(markdown_path) / (1024 * 1024)
    if not markdown_path or not os.path.exists(markdown_path):
        print(f'  ERROR: Markdown file not found: {markdown_path}')
        performance_monitor.log_extraction(filename=filename, file_size_mb=
            0, processing_time=time.time() - start_time, usage_metadata=
            None, success=False, error_message='Markdown not found',
            api_key_used=key_number, process_id=0, cao_number=
            cao_number)
        return None
    
    print(f'  INFO: Markdown file size: {file_size_mb:.2f} MB')
    
    # Dynamic timeout based on file size
    if file_size_mb > 8.0:
        timeout_seconds = 1200
    elif file_size_mb > 5.0:
        timeout_seconds = 900
    else:
        timeout_seconds = 600
    
    for attempt in range(max_retries):
        try:
            print(f'  INFO: Uploading markdown file to Gemini...')
            try:
                uploaded_file = client.files.upload(
                    file=markdown_path,
                    config={"mime_type": "text/markdown"}
                )
                print(f'  INFO: File uploaded successfully: {uploaded_file.name}')
            except Exception as e:
                print(f'  ERROR: File upload failed: {e}')
                raise ValueError(f'Failed to upload file {filename}: {e}')
            
            # Check file state and wait for processing
            max_wait_seconds = (300 if file_size_mb <= 5.0 else 600 if 
                file_size_mb <= 10.0 else 900)
            poll_interval_seconds = 2
            waited = 0
            
            print(f'  INFO: Waiting for file processing (max {max_wait_seconds}s)...')
            while waited < max_wait_seconds:
                try:
                    file_resource = client.files.get(name=uploaded_file.name)
                    if file_resource.state.name == 'ACTIVE':
                        print(f'  INFO: File is ready for processing')
                        break
                    elif file_resource.state.name == 'FAILED':
                        raise ValueError(
                            f'Uploaded file processing FAILED for {filename}')
                    else:
                        print(f'  INFO: File state: {file_resource.state.name} (waited {waited}s)')
                except Exception as e:
                    print(f'  WARNING: Error checking file state: {e}')
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
Extract information from this Dutch CAO (Collective Labor Agreement) Markdown document, which is a parsed version of the original PDF.

TASK: Categorize and extract relevant information into the specified fields based on the document content.

CRITICAL RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally (dates, numbers, percentages, units)
- Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters

CONTENT INCLUSION RULES:
- Include relevant numerical values, percentages, amounts, and time periods
- Include conditions, requirements, procedural steps, entitlements, allowances, and eligibility criteria
- For tables, include short descriptions and table structure with headers and all data rows and columns
- WAGE TABLES: Extract wage tables that differ in time periods, worker types, job categories, age groups, or other meaningful differences. For unit conversions (hourly vs monthly vs weekly vs 4 weeks vs yearly for same workers/periods/jobs/ages/others), extract only one version.

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
                ThinkingConfig(thinking_budget=LLM_THINKING_BUDGET),
                'http_options': types.HttpOptions(timeout=timeout_seconds * 1000)})
            if response is None:
                raise ValueError('No response received from model')
            
            # Add debugging information
            print(f'  DEBUG: Response object type: {type(response)}')
            print(f'  DEBUG: Response.text available: {response.text is not None}')
            print(f'  DEBUG: Response.parsed available: {response.parsed is not None}')
            
            if hasattr(response, 'candidates') and response.candidates and len(response.candidates) > 0:
                if response.candidates[0] and hasattr(response.candidates[0], 'content') and response.candidates[0].content:
                    if hasattr(response.candidates[0].content, 'parts') and response.candidates[0].content.parts and len(response.candidates[0].content.parts) > 0:
                        raw_content = response.candidates[0].content.parts[0].text
                        print(f'  DEBUG: Raw response length: {len(raw_content)}')
                    else:
                        print(f'  DEBUG: No content parts found')
                else:
                    print(f'  DEBUG: No content in first candidate')
            else:
                print(f'  DEBUG: No candidates found')
            
            # Try structured output first, then fall back to raw response
            if hasattr(response, 'text') and response.text:
                content = response.text
                print(f'  DEBUG: Using structured output (response.text)')
                
                # Check if the JSON response is complete (ends with proper closing)
                if not content.strip().endswith('}'):
                    raise ValueError('Response appears to be truncated - JSON does not end properly')
                
                # Validate JSON structure
                try:
                    json.loads(content)
                except json.JSONDecodeError as e:
                    raise ValueError(f'Invalid JSON response: {str(e)}')
                    
            elif hasattr(response, 'candidates') and response.candidates and len(response.candidates) > 0:
                if response.candidates[0] and hasattr(response.candidates[0], 'content') and response.candidates[0].content:
                    if hasattr(response.candidates[0].content, 'parts') and response.candidates[0].content.parts and len(response.candidates[0].content.parts) > 0:
                        # Fall back to raw response when structured output fails
                        content = response.candidates[0].content.parts[0].text
                        print(f'  WARNING: Structured output failed, using raw response')
                        print(f'  DEBUG: Using raw response (candidates[0].content.parts[0].text)')
                        
                        # Check if the raw response looks like JSON
                        if content.strip().startswith('{') and content.strip().endswith('}'):
                            try:
                                json.loads(content)
                                print(f'  DEBUG: Raw response is valid JSON')
                            except json.JSONDecodeError as e:
                                print(f'  WARNING: Raw response is not valid JSON: {str(e)}')
                        else:
                            print(f'  WARNING: Raw response does not look like JSON')
                    else:
                        raise ValueError('No content parts found in response')
                else:
                    raise ValueError('No content in response candidates')
            else:
                raise ValueError('No candidates found in response')
            
            if content:
                processing_time = time.time() - start_time
                content_length = len(content)
                estimated_tokens = content_length // 4
                print(
                    f'  INFO: Successfully extracted structured data from markdown (time: {processing_time:.1f}s)'
                    )
                print(f'  INFO: Response size: {content_length:,} chars (~{estimated_tokens:,} tokens)')
                performance_monitor.log_extraction(filename=filename,
                    file_size_mb=file_size_mb, processing_time=
                    processing_time, usage_metadata=response.usage_metadata,
                    success=True, api_key_used=key_number, process_id=
                    0, cao_number=cao_number)
                
                # Clean up uploaded file
                try:
                    client.files.delete(name=uploaded_file.name)
                    print(f'  INFO: Cleaned up uploaded file: {uploaded_file.name}')
                except Exception as e:
                    print(f'  WARNING: Failed to clean up file {uploaded_file.name}: {e}')
                
                return content
            else:
                raise ValueError('No content returned by model')
        except Exception as e:
            # Clean up uploaded file on error
            try:
                if 'uploaded_file' in locals():
                    client.files.delete(name=uploaded_file.name)
                    print(f'  INFO: Cleaned up uploaded file after error: {uploaded_file.name}')
            except Exception as cleanup_error:
                print(f'  WARNING: Failed to clean up file after error: {cleanup_error}')
            
            error_str = str(e).lower()
            print(
                f'  DEBUG: PDF upload error type: {type(e).__name__}, Error message: {error_str}'
                )
            print(f'  DEBUG: Full error details: {e}')
            
            # Log additional error details for debugging
            if 'response' in locals() and hasattr(response, 'candidates') and response.candidates:
                finish_reason = response.candidates[0].finish_reason
                print(f'  DEBUG: Response finish reason: {finish_reason}')
                if hasattr(response.candidates[0], 'safety_ratings'):
                    print(f'  DEBUG: Safety ratings: {response.candidates[0].safety_ratings}')
            
            # Log token usage if available
            if 'response' in locals() and hasattr(response, 'usage_metadata') and response.usage_metadata:
                print(f'  DEBUG: Token usage - Input: {response.usage_metadata.prompt_token_count}, Output: {response.usage_metadata.candidates_token_count}')
            
            if hasattr(e, '__traceback__'):
                import traceback
                print(f'  DEBUG: Traceback: {traceback.format_exc()}')
            if ('deadlineexceeded' in error_str or '504' in error_str or 
                'timeout' in error_str or 'truncated' in error_str):
                if attempt < max_retries - 1:
                    wait_time = 120 * 2 ** attempt
                    print(
                        f'  Attempt {attempt + 1} failed (timeout/truncation), retrying in {wait_time // 60} minutes...'
                        )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f'  All {max_retries} attempts failed with timeout/truncation errors'
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
                    api_key_used=key_number, process_id=0,
                    cao_number=cao_number)
                return None
    return None


# Process the single specified file
markdown_path = f"outputs/parsed_pdfs/parsed_pdfs_markdown/{target_cao_number}/{target_json_filename}"

if not os.path.exists(markdown_path):
    print(f"❌ Markdown file not found: {markdown_path}")
    print(f"   Expected location: outputs/parsed_pdfs/parsed_pdfs_markdown/{target_cao_number}/")
    print(f"   Available files in {target_cao_number}/:")
    try:
        files = os.listdir(f"outputs/parsed_pdfs/parsed_pdfs_markdown/{target_cao_number}/")
        for file in sorted(files):
            if file.endswith('.md'):
                print(f"     - {file}")
    except FileNotFoundError:
        print(f"     (CAO {target_cao_number} folder not found)")
    sys.exit(1)

print(f"✅ Found Markdown: {markdown_path}")

# Create output directory
output_cao_folder = OUTPUT_JSON_FOLDER / target_cao_number
output_cao_folder.mkdir(exist_ok=True)

# Generate output filename (markdown name with .json extension for output)
output_filename = target_json_filename
if not output_filename.endswith('.json'):
    output_filename += '.json'

output_file = output_cao_folder / output_filename

# Check if already processed
if output_file.exists():
    print(f"⚠️  File already processed: {output_file}")
    print("Delete the existing file if you want to re-process it.")
    sys.exit(0)

# Try to acquire lock
if not acquire_file_lock(output_file):
    print(f"🔒 File is being processed by another process")
    sys.exit(1)

try:
    print(f"🤖 Starting extraction...")
    extraction_start = time.time()
    
    raw_output = extract_with_json_upload(markdown_path, target_json_filename, target_cao_number, max_retries=2)
    
    extraction_time = time.time() - extraction_start
    
    if not raw_output:
        print(f"❌ LLM extraction failed")
        sys.exit(1)
    
    # Save the output
    print(f"💾 Saving structured output (length: {len(raw_output)} chars)")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(raw_output)
    
    print(f"✅ Extraction completed successfully in {extraction_time:.2f} seconds")
    print(f"📁 Output saved to: {output_file}")
    
except Exception as e:
    import traceback
    print(f"❌ Error during extraction: {e}")
    traceback.print_exc()
    
    # Log the error
    failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
    with open(failed_log_path, 'a', encoding='utf-8') as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {key_number}: {target_json_filename} (Error: {str(e)})\n")
    
    sys.exit(1)
    
finally:
    release_file_lock(output_file)

def main():
    """Main entry point for the LLM extraction pipeline (new version)."""
    # The script runs automatically when imported or executed
    pass

if __name__ == "__main__":
    main()
