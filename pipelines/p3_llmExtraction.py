"""
CAO Data Extraction Script with Structured Output (Markdown Version)
================================================

DESCRIPTION:
This script extracts raw text information from Dutch Collective Labor Agreement (CAO) markdown documents
using Google's Gemini AI with context-preserving extraction. It processes markdown files directly
and returns JSON data organized into broad thematic categories as complete text snippets.

FEATURES:
- Direct markdown upload to Gemini API for optimal accuracy
- Context-preserving extraction (keeps related information together)
- Multi-process support for parallel processing
- Robust error handling with exponential backoff
- Markdown quality validation and best practices enforcement
- Dynamic timeouts based on file size
- File locking to prevent duplicate processing
- Enhanced JSON validation and fallback mechanisms

USAGE:
    Single Process:
        python p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 1
        
    Multi-Process (2 parallel processes):
        python p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 2
        python p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 2

    Bash script for parallel execution:
        unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 4 2>&1 | tee log1.txt &
        unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 4 2>&1 | tee log2.txt &
        unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 3 --process_id 2 --total_processes 4 2>&1 | tee log3.txt &
        unbuffer caffeinate python pipelines/p3_llmExtraction.py --key_number 4 --process_id 3 --total_processes 4 2>&1 | tee log4.txt &

    With file limit:
        python p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 1 --max_files 10

ARGUMENTS:
    --key_number: Which API key to use (1, 2, 3, etc.) - defaults to 1
    --process_id: Process ID for work distribution (0-based) - defaults to 0
    --total_processes: Total number of parallel processes - defaults to 1
    --max_files: Maximum number of files to process (optional)

ENVIRONMENT VARIABLES:
    GOOGLE_API_KEY1, GOOGLE_API_KEY2, etc.: Google Gemini API keys

INPUT:
    - Markdown files in {config['paths']['parsed_pdfs_markdown']}/[CAO_NUMBER]/ folders

OUTPUT:
    - Extracted JSON data in outputs_json/new_flow/[CAO_NUMBER]/ folders
    - Error logs: outputs/logs/failed_files_llm_extraction.txt, outputs/logs/structured_output_parsing_errors.txt
"""

# =============================================================================
# IMPORTS
# =============================================================================
# Standard library imports for file operations, system access, and data handling
import os
import sys
import json
import time
import argparse
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field

# Add the parent directory to Python path so we can import monitoring
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Third-party imports for environment variables, file locking, and data validation
from dotenv import load_dotenv
import fcntl
from pydantic import BaseModel, Field, ConfigDict
import yaml
from monitoring.monitoring_3_1 import PerformanceMonitor

# Google Gemini API imports
from google import genai
from google.genai import types


# =============================================================================
# DATA SCHEMAS
# =============================================================================
# Pydantic schema for structured extraction of CAO document information
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


# =============================================================================
# GLOBAL STATE
# =============================================================================
# Process-specific quota flags to stop individual processes when daily quota is hit
process_quota_flags = {}

# =============================================================================
# CONFIGURATION CLASSES
# =============================================================================
# Data classes for managing extraction settings, statistics, and processing context
@dataclass
class ExtractionConfig:
    """Configuration class for extraction settings."""
    input_folder: str
    output_folder: Path
    max_files: int = 1000000
    max_processing_hours: int = 1
    sorted_files: bool = True
    model: str = 'gemini-2.5-flash'
    temperature: float = 0.0
    top_p: float = 0.1
    top_k: int = 1
    max_tokens: int = 65536
    candidate_count: int = 1
    seed: int = 42
    presence_penalty: float = 0
    frequency_penalty: float = 0
    thinking_budget: int = -1
    max_retries: int = 5
    delay_between_files: int = 200  # about 5 minutes between files to avoid rate limits


@dataclass
class ExtractionStats:
    """Statistics tracking for the extraction process."""
    processed_files: int = 0
    successful_extractions: int = 0
    failed_files: List[str] = field(default_factory=list)
    timed_out_files: List[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    
    def add_success(self, filename: str):
        """Add a successful extraction."""
        self.processed_files += 1
        self.successful_extractions += 1
    
    def add_failure(self, filename: str):
        """Add a failed extraction."""
        self.processed_files += 1
        self.failed_files.append(filename)
    
    def add_timeout(self, filename: str):
        """Add a timed out extraction."""
        self.timed_out_files.append(filename)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        elapsed_time = time.time() - self.start_time
        return {
            'processed_files': self.processed_files,
            'successful_extractions': self.successful_extractions,
            'failed_files': len(self.failed_files),
            'timed_out_files': len(self.timed_out_files),
            'elapsed_time': elapsed_time,
            'success_rate': self.successful_extractions / max(self.processed_files, 1)
        }


@dataclass
class ProcessingContext:
    """Context for the processing session."""
    config: ExtractionConfig
    process_id: int
    total_processes: int
    api_key: str
    key_number: int
    client: Any  # Gemini client
    performance_monitor: PerformanceMonitor
    stats: ExtractionStats = field(default_factory=ExtractionStats)


# =============================================================================
# CONFIGURATION & SETUP FUNCTIONS
# =============================================================================
# Functions for loading configuration, setting up environment, and initializing components
def load_configuration() -> ExtractionConfig:
    """Load and validate configuration from config.yaml."""
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    return ExtractionConfig(
        input_folder=config_data['paths']['parsed_pdfs_markdown'],
        output_folder=Path(config_data['paths']['outputs_json']) / "new_flow"
    )


def setup_environment(key_number: int = 1) -> Tuple[str, int]:
    """Setup environment variables and API key."""
    load_dotenv()
    
    api_key = os.getenv(f'GOOGLE_API_KEY{key_number}')
    if not api_key:
        api_key = os.getenv('GOOGLE_API_KEY1')
        if not api_key:
            raise ValueError(
                f'Neither GOOGLE_API_KEY{key_number} nor GOOGLE_API_KEY1 environment variable found. '
                f'Please set at least GOOGLE_API_KEY1 before running this script.'
            )
        else:
            key_number = 1
            print(f'Warning: GOOGLE_API_KEY{key_number} not found, using GOOGLE_API_KEY1 instead')
    
    return api_key, key_number


def setup_gemini_client(api_key: str):
    """Setup Gemini client."""
    from google import genai
    return genai.Client(api_key=api_key)


def setup_performance_monitor() -> PerformanceMonitor:
    """Setup performance monitoring."""
    return PerformanceMonitor(
        log_file='performance_logs/extraction_performance.jsonl',
        summary_file='performance_logs/extraction_summary.json'
    )


def setup_processing_context(config: ExtractionConfig, process_id: int, 
                           total_processes: int, key_number: int) -> ProcessingContext:
    """Setup complete processing context."""
    api_key, actual_key_number = setup_environment(key_number)
    client = setup_gemini_client(api_key)
    performance_monitor = setup_performance_monitor()
    
    return ProcessingContext(
        config=config,
        process_id=process_id,
        total_processes=total_processes,
        api_key=api_key,
        key_number=actual_key_number,
        client=client,
        performance_monitor=performance_monitor
    )


def validate_input_paths(config: ExtractionConfig):
    """Validate that input/output paths exist and are accessible."""
    if not os.path.exists(config.input_folder):
        raise ValueError(f"Input folder does not exist: {config.input_folder}")
    
    config.output_folder.mkdir(exist_ok=True)
    
    # Check if we can write to output folder
    test_file = config.output_folder / ".test_write"
    try:
        test_file.write_text("test")
        test_file.unlink()
    except Exception as e:
        raise ValueError(f"Cannot write to output folder: {config.output_folder}, Error: {e}")


# =============================================================================
# FILE DISCOVERY & VALIDATION FUNCTIONS
# =============================================================================
# Functions for finding, filtering, and validating markdown files
def discover_markdown_files(input_folder: str) -> List[Tuple[Path, Path]]:
    """Discover all markdown files organized by CAO."""
    cao_folders = sorted([f for f in Path(input_folder).iterdir() 
                         if f.is_dir() and f.name.isdigit()], 
                        key=lambda f: int(f.name))
    
    all_files = []
    for cao_folder in cao_folders:
        markdown_files = sorted(cao_folder.glob('*.md'))
        for markdown_file in markdown_files:
            all_files.append((cao_folder, markdown_file))
    
    return all_files


def filter_files_for_processing(all_files: List[Tuple[Path, Path]], 
                              context: ProcessingContext) -> List[Tuple[Path, Path]]:
    """Filter files based on multi-process distribution."""
    filtered_files = []
    for file_idx, (cao_folder, markdown_file) in enumerate(all_files):
        if file_idx % context.total_processes == context.process_id:
            filtered_files.append((cao_folder, markdown_file))
    
    if not context.config.sorted_files:
        import random
        random.seed(42)
        random.shuffle(filtered_files)
    
    return filtered_files


def validate_markdown_file(markdown_path: str) -> Tuple[bool, str]:
    """Validate a single markdown file."""
    try:
        if not os.path.exists(markdown_path):
            return False, 'Markdown file does not exist'
        
        file_size = os.path.getsize(markdown_path)
        if file_size == 0:
            return False, 'Markdown file is empty'
        if file_size < 1024:
            return False, f'Markdown file too small ({file_size} bytes) - likely corrupted'
        if file_size > 50 * 1024 * 1024:
            return False, f'Markdown file too large ({file_size / (1024 * 1024):.1f}MB) - exceeds reasonable limit'
        if not markdown_path.endswith('.md'):
            return False, 'File does not have .md extension'
        
        return True, f'Markdown appears valid ({file_size / (1024 * 1024):.1f}MB)'
    except PermissionError:
        return False, 'Permission denied - cannot read markdown file'
    except Exception as e:
        return False, f'Error checking markdown: {str(e)}'


# =============================================================================
# FILE LOCKING & MANAGEMENT FUNCTIONS
# =============================================================================
# Functions for file locking, cleanup, and result saving
def acquire_file_lock(file_path: Path, context: ProcessingContext) -> bool:
    """Try to acquire a lock for processing a file."""
    lock_file = file_path.with_suffix('.lock')
    try:
        with open(lock_file, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(f'Process {context.process_id + 1} using API key {context.key_number}\n')
            f.write(f'Timestamp: {time.time()}\n')
        return True
    except (IOError, OSError):
        return False


def release_file_lock(file_path: Path):
    """Release the lock for a file."""
    lock_file = file_path.with_suffix('.lock')
    try:
        if lock_file.exists():
            lock_file.unlink()
    except:
        pass


def announce_cao_once(cao_number: str, context: ProcessingContext) -> bool:
    """Announce a CAO number only once across all processes."""
    announce_file = context.config.output_folder / f'.cao_{cao_number}_announced'
    try:
        with open(announce_file, 'x') as f:
            f.write(f'Announced by process {context.process_id + 1}\n')
        print(f'--- CAO {cao_number} ---')
        return True
    except FileExistsError:
        return False


def save_extraction_result(output_path: Path, content: str):
    """Save extraction result to file."""
    # print(f'  INFO: Saving structured output directly (length: {len(content)} chars)')
    
    # Parse and reformat JSON for better structure
    try:
        data = json.loads(content)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f'  INFO: JSON saved with proper formatting')
    except json.JSONDecodeError as e:
        print(f'  WARNING: JSON parsing failed, saving as raw content: {e}')
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)


def cleanup_uploaded_file(client, uploaded_file):
    """Clean up uploaded file from Gemini."""
    try:
        client.files.delete(name=uploaded_file.name)
        print(f'  INFO: Cleaned up uploaded file: {uploaded_file.name}')
    except Exception as e:
        print(f'  WARNING: Failed to clean up file {uploaded_file.name}: {e}')


# =============================================================================
# LLM RESPONSE VALIDATION & ERROR HANDLING
# =============================================================================
# Functions for validating LLM responses and handling various error types
def calculate_quota_retry_delay(file_size_mb: float, attempt: int) -> int:
    """
    Calculate quota retry delay based on file size and attempt number.
    
    Formula: (estimated_tokens / 125000) * 60 seconds * (2^attempt) + buffer
    - 125,000 tokens per minute limit
    - Exponential backoff: 2^attempt
    - Buffer time for safety
    """
    # Estimate tokens: roughly 4 chars per token, file_size_mb * 1024 * 1024 / 4
    estimated_tokens = int(file_size_mb * 1024 * 1024 / 4)
    
    # Calculate minutes needed to process this file
    minutes_needed = estimated_tokens / 125000
    
    # Add exponential backoff: 2^attempt
    backoff_multiplier = 2 ** attempt
    
    # Add buffer time (1-2 minutes for safety)
    buffer_minutes = 1 + attempt
    
    # Calculate total delay in seconds
    total_delay_seconds = int((minutes_needed * backoff_multiplier + buffer_minutes) * 60)
    
    print(f'  DEBUG: File size: {file_size_mb:.2f}MB, Estimated tokens: {estimated_tokens:,}')
    print(f'  DEBUG: Minutes needed: {minutes_needed:.1f}, Backoff: {backoff_multiplier}x, Buffer: {buffer_minutes}min')
    print(f'  DEBUG: Total delay: {total_delay_seconds // 60} minutes ({total_delay_seconds} seconds)')
    
    return total_delay_seconds


def get_model_parameters(config) -> Dict[str, Any]:
    """Get model parameters for logging."""
    return {
        "model": config.model,
        "temperature": config.temperature,
        "top_p": config.top_p,
        "top_k": config.top_k,
        "max_tokens": config.max_tokens,
        "candidate_count": config.candidate_count,
        "seed": config.seed,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "thinking_budget": config.thinking_budget,
        "max_retries": config.max_retries
    }


def create_extraction_prompt(filename: str) -> str:
    """Create the extraction prompt for CAO document processing."""
    return f"""
    Extract information from this Dutch CAO (Collective Labor Agreement) Markdown document, which is a parsed version of the original PDF.
    TASK: Categorize and extract relevant information into the specified fields based on the document content.
    CRITICAL RULES:
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information.
        - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
        - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
        - IMPORTANT: Translate all Dutch text to English.
    CONTENT INCLUSION RULES:
        - Include relevant numerical values, percentages, amounts, and time periods.
        - Include conditions, requirements, procedural steps, entitlements, allowances, and eligibility criteria.
        - For tables, include short descriptions and table structure with headers and all data rows and columns.
        - WAGE TABLES: Extract wage tables that differ in time periods, worker types, job categories, age groups, or other meaningful differences. For unit conversions (hourly vs monthly vs weekly vs 4 weeks vs yearly for same workers/periods/jobs/ages/others), extract only one version.
    TABLE FORMATTING:
        - Structure tables as below, including headers, descriptions, and footnotes: 
            [
                "Table title with context and units", 
                "column headers with descriptions", 
                "row data with values", 
                "additional rows as needed (for example for footnotes and further descriptions)"
            ]
        - If the original table structure is unclear or messy (missing headers, empty cells, broken formatting), reorganize it into a logical structure like the example above.
    JSON OUTPUT REQUIREMENTS:
        - Output ONLY valid JSON format.
        - Use ONLY standard ASCII characters (no special/control characters).
        - Replace any special characters with standard equivalents.
    Document: {filename}
    """


def validate_uploaded_file(client, uploaded_file, filename: str, original_size_mb: float):
    """Validate uploaded file with comprehensive checks."""
    try:
        file_resource = client.files.get(name=uploaded_file.name)
        
        # Check file state
        if file_resource.state.name != 'ACTIVE':
            raise ValueError(f'Uploaded file not ACTIVE: {file_resource.state.name}')
        
        # Check file size (using size_bytes attribute)
        if hasattr(file_resource, 'size_bytes') and file_resource.size_bytes == 0:
            raise ValueError(f'Uploaded file is empty for {filename}')
        
        # Check MIME type
        if hasattr(file_resource, 'mime_type') and file_resource.mime_type != "text/markdown":
            print(f'  WARNING: Unexpected MIME type: {file_resource.mime_type} (expected: text/markdown)')
        
        # Check if file size is reasonable (should be similar to original)
        if hasattr(file_resource, 'size_bytes'):
            uploaded_size_mb = file_resource.size_bytes / (1024 * 1024)
            size_diff_percent = abs(uploaded_size_mb - original_size_mb) / original_size_mb * 100
            
            if size_diff_percent > 50:  # More than 50% difference
                print(f'  WARNING: Large size difference: original={original_size_mb:.2f}MB, uploaded={uploaded_size_mb:.2f}MB ({size_diff_percent:.1f}% diff)')
            
            print(f'  DEBUG: File validation passed - size: {uploaded_size_mb:.2f}MB')
        else:
            print(f'  DEBUG: File validation passed - size information not available')
        
        return True
        
    except Exception as e:
        print(f'  ERROR: File validation failed: {e}')
        raise ValueError(f'File validation failed for {filename}: {e}')


def safe_contents(prompt: str, uploaded_file=None):
    """Safely construct contents array with validation for Gemini API call."""
    contents = []
    
    # Validate prompt
    if not prompt or not prompt.strip():
        raise ValueError("Empty prompt - refusing to call Gemini")
    contents.append(prompt.strip())
    
    # Validate file
    if uploaded_file:
        contents.append(uploaded_file)
    
    if not contents:
        raise ValueError("No valid content parts - refusing to call Gemini")
    
    print(f'  DEBUG: Constructed {len(contents)} content parts for API call')
    return contents


def extract_text_safely(response, filename: str):
    """Safely extract text from response with proper error handling and JSON cleanup."""
    if response is None:
        raise ValueError('No response received from model')
    
    print(f'  DEBUG: Response object type: {type(response)}')
    
    # Check if we have candidates
    if not getattr(response, "candidates", None):
        raise ValueError('No candidates in response')
    
    cand = response.candidates[0]
    
    # Check finish reason first - this is critical for understanding failures
    fr = getattr(cand, "finish_reason", None)
    print(f'  DEBUG: Finish reason: {fr}')
    
    # If filtered or blocked, don't try to access text
    if fr and fr not in ["STOP", "MAX_TOKENS"]:
        safety_info = []
        if hasattr(cand, "safety_ratings") and cand.safety_ratings is not None:
            safety_info = [(r.category, r.probability) for r in cand.safety_ratings]
        print(f'  DEBUG: Response blocked - Finish reason: {fr}, Safety ratings: {safety_info}')
        return "", {"finish": fr, "safety": safety_info, "filename": filename}
    
    # Check for structured output first (when using response_schema)
    print(f'  DEBUG: Checking response.parsed: hasattr={hasattr(response, "parsed")}, value={getattr(response, "parsed", "NOT_FOUND")}')
    if hasattr(response, 'parsed') and response.parsed:
        print(f'  DEBUG: Found structured output in response.parsed')
        print(f'  DEBUG: response.parsed type: {type(response.parsed)}')
        print(f'  DEBUG: response.parsed content: [STRUCTURED DATA - SUPPRESSED FOR CLARITY]')
        # Convert structured output to JSON string
        content = response.parsed.model_dump_json()
        print(f'  DEBUG: Converted structured output to JSON: {len(content)} chars')
        return content, {"finish": fr or "STOP", "filename": filename}
    
    # Check for direct text response (when not using response_schema)
    print(f'  DEBUG: Checking response.text: hasattr={hasattr(response, "text")}, value={getattr(response, "text", "NOT_FOUND")}')
    if hasattr(response, 'text') and response.text:
        print(f'  DEBUG: Found direct text response: {len(response.text)} chars')
        content = response.text
        return content, {"finish": fr or "STOP", "filename": filename}
    
    # Only proceed with text parts extraction if we don't have structured output
    # Fallback to text parts extraction
    text_chunks = []
    content = getattr(cand, "content", None)
    
    if not content:
        raise ValueError('No content in response candidate')
    
    parts = getattr(content, "parts", []) or []
    print(f'  DEBUG: Found {len(parts)} content parts')
    
    # Debug: Print all part types to understand what we're getting
    for i, part in enumerate(parts):
        print(f'  DEBUG: Part {i}: type={type(part).__name__}, attributes={dir(part)}')
        if hasattr(part, "text") and part.text:
            text_chunks.append(part.text)
            print(f'  DEBUG: Part {i}: text length = {len(part.text)}')
        elif hasattr(part, "function_call"):
            print(f'  DEBUG: Part {i}: function_call found')
        elif hasattr(part, "inline_data"):
            print(f'  DEBUG: Part {i}: inline_data found')
        else:
            print(f'  DEBUG: Part {i}: no text content, no function_call, no inline_data')
    
    if not text_chunks:
        # Try fallback: check if response has direct text attribute
        print(f'  DEBUG: No text parts found, trying fallback methods...')
        
        # Try direct response attributes (but we already checked for parsed and text above)
        print(f'  DEBUG: No fallback text found in response')
        print(f'  DEBUG: Response attributes: {dir(response)}')
        raise ValueError('No text parts found in response')
    else:
        content = "".join(text_chunks)
        print(f'  DEBUG: Total extracted text length: {len(content)}')
    
    # Apply JSON cleanup (integrated from original validate_llm_response)
    if content.strip().startswith('{') and content.strip().endswith('}'):
        try:
            json.loads(content)
            print(f'  DEBUG: JSON is valid without cleanup')
        except json.JSONDecodeError as e:
            print(f'  WARNING: JSON parsing failed, attempting cleanup: {str(e)}')
            
            # Remove problematic control characters (but keep \n, \t, \r)
            import re
            cleaned_content = re.sub(r'[\x00-\x09\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', content)
            
            try:
                json.loads(cleaned_content)
                print(f'  INFO: JSON cleanup successful, using cleaned content')
                content = cleaned_content
            except json.JSONDecodeError as e2:
                print(f'  WARNING: JSON cleanup failed: {str(e2)}')
                print(f'  INFO: Using raw text content as fallback')
    
    return content, {"finish": fr or "STOP", "filename": filename}


def handle_llm_errors(error: Exception, attempt: int, max_retries: int, file_size_mb: float = 0, context=None) -> bool:
    """Handle different types of LLM errors with appropriate retry logic."""
    error_str = str(error).lower()
    
    if ('deadlineexceeded' in error_str or '504' in error_str or 
        'timeout' in error_str or 'truncated' in error_str):
        if attempt < max_retries - 1:
            wait_time = 120 * 2 ** attempt
            print(f'  Attempt {attempt + 1} failed (timeout/truncation), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with timeout/truncation errors')
            return False
    elif 'serviceunavailable' in error_str or '503' in error_str or 'connection reset' in error_str or '500' in error_str or 'internal' in error_str:
        if attempt < max_retries - 1:
            wait_time = 60 * 2 ** attempt
            print(f'  Attempt {attempt + 1} failed (service unavailable/internal error), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with service unavailable/internal errors')
            return False
    elif 'no content parts found' in error_str or 'no content' in error_str:
        if attempt < max_retries - 1:
            wait_time = 60 * 2 ** attempt
            print(f'  Attempt {attempt + 1} failed (empty response), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with empty response errors')
            return False
    elif 'quota' in error_str or '429' in error_str:
        # Check if it's a daily quota limit (not retryable)
        if 'perday' in error_str or 'daily' in error_str or '3000000' in error_str:
            global process_quota_flags
            process_quota_flags[context.process_id] = True
            print(f'  ❌ DAILY QUOTA LIMIT REACHED for Process {context.process_id} - Cannot retry until tomorrow')
            print(f'  💡 Daily limit: 3,000,000 tokens per day')
            print(f'  💡 Quota resets at midnight (Google timezone)')
            print(f'  🛑 Stopping this process to avoid infinite retries')
            return False  # Stop immediately, don't retry
        
        # Regular per-minute quota limit (retryable)
        if attempt < max_retries - 1:
            if file_size_mb > 0:
                wait_time = calculate_quota_retry_delay(file_size_mb, attempt)
            else:
                wait_time = 60 * 2 ** attempt  # Fallback for unknown file size
            print(f'  Attempt {attempt + 1} failed (per-minute quota), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with per-minute quota errors')
            return False
    elif attempt < max_retries - 1:
        wait_time = 30 * 2 ** attempt
        print(f'  Attempt {attempt + 1} failed ({type(error).__name__}), retrying in {wait_time} seconds...')
        time.sleep(wait_time)
        return True
    else:
        return False


# =============================================================================
# LOGGING & MONITORING FUNCTIONS
# =============================================================================
# Functions for logging processing results and monitoring progress
def log_processing_result(filename: str, success: bool, context: ProcessingContext, 
                         error_message: str = None):
    """Log processing results to appropriate files."""
    if not success:
        context.stats.add_failure(filename)
        failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
        with open(failed_log_path, 'a', encoding='utf-8') as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {context.key_number}: {filename}")
            if error_message:
                f.write(f" (Error: {error_message})")
            f.write('\n')
    else:
        context.stats.add_success(filename)


def log_detailed_failure(response_info: dict, filename: str, attempt: int):
    """Log detailed failure information for debugging."""
    print(f'  🔍 DETAILED FAILURE ANALYSIS for {filename} (attempt {attempt + 1}):')
    print(f'    📊 Finish reason: {response_info.get("finish", "UNKNOWN")}')
    print(f'    🛡️  Safety ratings: {response_info.get("safety", [])}')
    print(f'    📄 Content length: {len(response_info.get("content", ""))}')
    print(f'    ⏱️  Processing time: {response_info.get("processing_time", "UNKNOWN")}s')
    print(f'    🔑 API key used: {response_info.get("api_key", "UNKNOWN")}')
    print(f'    🆔 Process ID: {response_info.get("process_id", "UNKNOWN")}')


def validate_response_schema(content: str, filename: str) -> bool:
    """Validate that response contains expected schema structure."""
    try:
        data = json.loads(content)
        required_fields = ['general_information', 'wage_information', 'pension_information']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            print(f'  WARNING: Missing required fields: {missing_fields}')
            return False
            
        return True
    except json.JSONDecodeError:
        print(f'  ERROR: Invalid JSON structure')
        return False


# =============================================================================
# CORE EXTRACTION FUNCTION
# =============================================================================
# Main function for extracting content from markdown files using Gemini API
def extract_with_markdown_upload(markdown_path: str, filename: str, cao_number: str, 
                               context: ProcessingContext) -> Optional[str]:
    """Extract using Files API approach - upload markdown file to Gemini."""
    print(f'  INFO: Using Files API approach for {filename}')
    start_time = time.time()
    file_size_mb = os.path.getsize(markdown_path) / (1024 * 1024)
    
    if not markdown_path or not os.path.exists(markdown_path):
        print(f'  ERROR: Markdown file not found: {markdown_path}')
        context.performance_monitor.log_extraction(
            filename=filename, file_size_mb=0, processing_time=time.time() - start_time,
            usage_metadata=None, success=False, error_message='Markdown not found',
            api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
            model=context.config.model, parameters=get_model_parameters(context.config)
        )
        return None
    
    print(f'  INFO: Markdown file size: {file_size_mb:.2f} MB')
    
    # Dynamic timeout based on file size
    if file_size_mb > 8.0:
        timeout_seconds = 1200
    elif file_size_mb > 5.0:
        timeout_seconds = 900
    else:
        timeout_seconds = 600
    
    for attempt in range(context.config.max_retries):
        uploaded_file = None
        try:
            print(f'  INFO: Uploading markdown file to Gemini...')
            try:
                uploaded_file = context.client.files.upload(
                    file=markdown_path,
                    config={"mime_type": "text/markdown"}
                )
                print(f'  INFO: File uploaded successfully: {uploaded_file.name}')
            except Exception as e:
                print(f'  ERROR: File upload failed: {e}')
                raise ValueError(f'Failed to upload file {filename}: {e}')
            
            # Check file state and wait for processing
            max_wait_seconds = (300 if file_size_mb <= 5.0 else 600 if file_size_mb <= 10.0 else 900)
            poll_interval_seconds = 2
            waited = 0
            
            # print(f'  INFO: Waiting for file processing (max {max_wait_seconds}s)...')
            while waited < max_wait_seconds:
                try:
                    file_resource = context.client.files.get(name=uploaded_file.name)
                    if file_resource.state.name == 'ACTIVE':
                        print(f'  INFO: File is ready for processing')
                        break
                    elif file_resource.state.name == 'FAILED':
                        raise ValueError(f'Uploaded file processing FAILED for {filename}')
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
                raise TimeoutError(f'Uploaded file not ACTIVE after {max_wait_seconds}s for {filename}')
            
            # Validate uploaded file with comprehensive checks
            validate_uploaded_file(context.client, uploaded_file, filename, file_size_mb)
            
            # Create and validate extraction prompt
            extraction_prompt = create_extraction_prompt(filename)
            
            # Disable safety settings to prevent content filtering
            safety_settings = [
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                )
            ]
            
            # Safely construct contents for API call
            safe_content = safe_contents(extraction_prompt, uploaded_file)
            
            # Use the original working approach with response_schema
            response = context.client.models.generate_content(
                model=context.config.model,
                contents=safe_content,
                config={
                    'temperature': context.config.temperature,
                    'top_p': context.config.top_p,
                    'top_k': context.config.top_k,
                    'max_output_tokens': context.config.max_tokens,
                    'candidate_count': context.config.candidate_count,
                    'seed': context.config.seed,
                    'presence_penalty': context.config.presence_penalty,
                    'frequency_penalty': context.config.frequency_penalty,
                    'response_mime_type': 'application/json',
                    'response_schema': CAOExtractionSchema,
                    'thinking_config': types.ThinkingConfig(thinking_budget=context.config.thinking_budget),
                    'http_options': types.HttpOptions(timeout=timeout_seconds * 1000),
                    'safety_settings': safety_settings
                }
            )
            
            content, extraction_info = extract_text_safely(response, filename)
            
            if content:
                processing_time = time.time() - start_time
                content_length = len(content)
                estimated_tokens = content_length // 4
                print(f'  INFO: Successfully extracted structured data from markdown (time: {processing_time:.1f}s)')
                print(f'  INFO: Response size: {content_length:,} chars (~{estimated_tokens:,} tokens)')
                
                # Validate response schema
                if not validate_response_schema(content, filename):
                    print(f'  WARNING: Response schema validation failed for {filename}')
                
                context.performance_monitor.log_extraction(
                    filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                    usage_metadata=response.usage_metadata, success=True, api_key_used=context.key_number,
                    process_id=context.process_id, cao_number=cao_number,
                    model=context.config.model, parameters=get_model_parameters(context.config)
                )
                
                cleanup_uploaded_file(context.client, uploaded_file)
                return content
            else:
                # Log detailed failure information
                processing_time = time.time() - start_time
                failure_info = {
                    "finish": extraction_info.get("finish", "UNKNOWN"),
                    "safety": extraction_info.get("safety", []),
                    "content": "",
                    "processing_time": processing_time,
                    "api_key": context.key_number,
                    "process_id": context.process_id
                }
                log_detailed_failure(failure_info, filename, attempt)
                raise ValueError(f'No content returned by model - Finish reason: {extraction_info.get("finish", "UNKNOWN")}')
                
        except Exception as e:
            # Clean up uploaded file on error
            if uploaded_file:
                cleanup_uploaded_file(context.client, uploaded_file)
            
            error_str = str(e).lower()
            print(f'  DEBUG: Markdown upload error type: {type(e).__name__}, Error message: {error_str}')
            print(f'  DEBUG: Full error details: {e}')
            
            # Log detailed failure information for all failures
            processing_time = time.time() - start_time
            failure_info = {
                "finish": "UNKNOWN",
                "safety": [],
                "content": "",
                "processing_time": processing_time,
                "api_key": context.key_number,
                "process_id": context.process_id
            }
            
            # Try to extract response information if available
            if 'response' in locals() and hasattr(response, 'candidates') and response.candidates:
                cand = response.candidates[0]
                failure_info["finish"] = getattr(cand, 'finish_reason', 'UNKNOWN')
                if hasattr(cand, 'safety_ratings') and cand.safety_ratings is not None:
                    failure_info["safety"] = [(r.category, r.probability) for r in cand.safety_ratings]
            
            # Log token usage if available
            if 'response' in locals() and hasattr(response, 'usage_metadata') and response.usage_metadata:
                print(f'  DEBUG: Token usage - Input: {response.usage_metadata.prompt_token_count}, Output: {response.usage_metadata.candidates_token_count}')
            
            # Log detailed failure analysis
            log_detailed_failure(failure_info, filename, attempt)
            
            if hasattr(e, '__traceback__'):
                import traceback
                print(f'  DEBUG: Traceback: {traceback.format_exc()}')
            
            # Handle retry logic
            if handle_llm_errors(e, attempt, context.config.max_retries, file_size_mb, context):
                        continue
            else:
                processing_time = time.time() - start_time
                print(f'  Markdown upload failed after {context.config.max_retries} attempts')
                context.performance_monitor.log_extraction(
                    filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                    usage_metadata=None, success=False, error_message=f'Failed after {context.config.max_retries} attempts: {str(e)}',
                    api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
                    model=context.config.model, parameters=get_model_parameters(context.config)
                )
                return None
    
    return None


# =============================================================================
# FILE PROCESSING FUNCTIONS
# =============================================================================
# Functions for processing individual files and managing the processing workflow
def process_single_file(markdown_file: Path, cao_number: str, output_folder: Path, 
                       context: ProcessingContext, total_files: int) -> bool:
    """Process a single markdown file end-to-end."""
    # Generate output filename
    output_filename = markdown_file.name
    if not output_filename.endswith('.json'):
        output_filename += '.json'
    
    output_file = output_folder / output_filename
    
    # Check if already processed
    if output_file.exists():
        print(f'  {cao_number}: Skipping {markdown_file.name} (already processed)')
        time.sleep(5)
        # Don't count already processed files toward the limit
        return True
    
    # Check file limit (only count successful extractions)
    if context.stats.successful_extractions >= context.config.max_files:
        return False
    
    # Try to acquire lock
    if not acquire_file_lock(output_file, context):
        print(f'  {cao_number}: Skipping {markdown_file.name} (being processed by another process)')
        time.sleep(2)
        return True
    
    try:
        # Validate markdown file
        is_valid, quality_message = validate_markdown_file(str(markdown_file))
        if not is_valid:
            print(f'  {cao_number}: ✗ Markdown quality check failed for {markdown_file.name}: {quality_message} [API {context.key_number}/{context.total_processes}]')
            log_processing_result(markdown_file.name, False, context, f"Markdown quality: {quality_message}")
            return True
        
        # Check file size
        file_size_mb = os.path.getsize(markdown_file) / (1024 * 1024)
        if file_size_mb > 50.0:
            print(f'  {cao_number}: ✗ Markdown file too large ({file_size_mb:.1f}MB) - exceeds 50MB limit for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
            log_processing_result(markdown_file.name, False, context, f"Markdown too large: {file_size_mb:.1f}MB")
            return True
        
        print(f'  {cao_number}: {markdown_file.name} (Markdown: {file_size_mb:.1f}MB) [API {context.key_number}/{context.total_processes}]')
        
        # Check timeout
        extraction_start = time.time()
        max_processing_time = context.config.max_processing_hours * 3600
        if time.time() - extraction_start > max_processing_time:
            print(f'  {cao_number}: ⏰ Timeout after {context.config.max_processing_hours} hours for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
            context.stats.add_timeout(markdown_file.name)
            timeout_log_path = 'outputs/logs/timed_out_files_llm_extraction.txt'
            with open(timeout_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {context.key_number}: {markdown_file.name}\n")
            return True
        
        # Extract content
        raw_output = extract_with_markdown_upload(str(markdown_file), markdown_file.name, cao_number, context)
        extraction_time = time.time() - extraction_start
        
        # Check if daily quota was hit during extraction
        if context.process_id in process_quota_flags and process_quota_flags[context.process_id]:
            print(f'  🛑 DAILY QUOTA LIMIT REACHED for Process {context.process_id} - Stopping this process')
            print(f'  💡 Wait until tomorrow to continue')
            return False  # Stop processing this process only
        
        if not raw_output:
            print(f'  {cao_number}: ✗ LLM extraction failed for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
            log_processing_result(markdown_file.name, False, context)
            return True
        
        # Save result
        save_extraction_result(output_file, raw_output)
        print(f'  {cao_number}: LLM extraction completed in {extraction_time:.2f} seconds [API {context.key_number}/{context.total_processes}]')
        
        # Mark as successful after saving
        context.stats.add_success(markdown_file.name)
        
        # Update progress
        if context.stats.processed_files % 10 == 0:
            context.performance_monitor.print_progress(total_files)
        
        # Add delay between files
        time.sleep(context.config.delay_between_files)
        
        return True
        
    except Exception as e:
        import traceback
        print(f'  {cao_number}: Error with {markdown_file.name}: {e} [API {context.key_number}/{context.total_processes}]')
        traceback.print_exc()
        log_processing_result(markdown_file.name, False, context, str(e))
        return True
    finally:
        release_file_lock(output_file)


# =============================================================================
# RESULTS & DISPLAY FUNCTIONS
# =============================================================================
# Functions for displaying final results and performance analysis
def cleanup_announce_files(context: ProcessingContext):
    """Clean up announce files and lock files created during processing."""
    try:
        # Clean up announce files
        announce_files = list(context.config.output_folder.glob('.cao_*_announced'))
        for announce_file in announce_files:
            announce_file.unlink()
            print(f'  🧹 Cleaned up announce file: {announce_file.name}')
        if announce_files:
            print(f'  🧹 Cleaned up {len(announce_files)} announce files')
        
        # Clean up lock files in all CAO folders
        lock_files_found = 0
        for cao_folder in context.config.output_folder.iterdir():
            if cao_folder.is_dir() and cao_folder.name.isdigit():
                lock_files = list(cao_folder.glob('.cao_*_processing'))
                for lock_file in lock_files:
                    lock_file.unlink()
                    print(f'  🧹 Cleaned up lock file: {cao_folder.name}/{lock_file.name}')
                    lock_files_found += 1
        
        if lock_files_found > 0:
            print(f'  🧹 Cleaned up {lock_files_found} lock files')
            
    except Exception as e:
        print(f'  ⚠️  Warning: Failed to clean up files: {e}')


def display_final_results(context: ProcessingContext):
    """Display final processing results."""
    print(f'Process {context.process_id + 1} completed:')
    print(f'  📊 Files processed: {context.stats.processed_files}')
    print(f'  ✅ Successful extractions: {context.stats.successful_extractions}')
    print(f'  ❌ Failed extractions: {len(context.stats.failed_files)}')
    print(f'  ⏰ Timed out: {len(context.stats.timed_out_files)}')
    
    if context.stats.failed_files:
        print(f'  📝 Failed files: {context.stats.failed_files}')
    if context.stats.timed_out_files:
        print(f'  📝 Timed out files: {context.stats.timed_out_files}')
    
    print('\n' + '=' * 60)
    print('FINAL PERFORMANCE ANALYSIS')
    print('=' * 60)
    context.performance_monitor.analyze_performance()
    context.performance_monitor.update_summary_file()
    print(f"""📁 Performance data saved to:""")
    print(f'   Detailed logs: {context.performance_monitor.log_file}')
    print(f'   Summary: {context.performance_monitor.summary_file}')
    
    # Clean up announce files
    cleanup_announce_files(context)


# =============================================================================
# MAIN PIPELINE ORCHESTRATION
# =============================================================================
# Main function that orchestrates the entire extraction pipeline
def run_extraction_pipeline():
    """Main pipeline orchestration."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='CAO LLM Extraction Pipeline')
    parser.add_argument('--key_number', type=int, default=1, help='API key number (1-3)')
    parser.add_argument('--process_id', type=int, default=0, help='Process ID for parallel processing')
    parser.add_argument('--total_processes', type=int, default=1, help='Total number of parallel processes')
    parser.add_argument('--max_files', type=int, help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    key_number = args.key_number
    process_id = args.process_id
    total_processes = args.total_processes
    
    # Load configuration
    config = load_configuration()
    
    # Override max_files if provided as argument
    if args.max_files is not None:
        config.max_files = args.max_files
    
    # Validate paths
    validate_input_paths(config)
    
    # Setup processing context
    context = setup_processing_context(config, process_id, total_processes, key_number)
    
    # Clean up announce files from previous runs at the beginning
    cleanup_announce_files(context)
    
    # Discover files
    all_markdown_files = discover_markdown_files(config.input_folder)
    filtered_files = filter_files_for_processing(all_markdown_files, context)
    
    print(f"🎯 CAO Markdown Extraction Pipeline")
    print(f"📊 Process: {process_id + 1}/{total_processes}")
    print(f"🔑 API Key: {context.key_number}")
    print(f"📁 Input: {config.input_folder}")
    print(f"📁 Output: {config.output_folder}")
    print(f"📄 Files to process: {len(filtered_files)}")
    print()
    
    # Process files
    for cao_folder, markdown_file in filtered_files:
        cao_number = cao_folder.name
        output_folder = config.output_folder / cao_number
        output_folder.mkdir(exist_ok=True)
        
        # Announce CAO once
        announce_cao_once(cao_number, context)
        
        # Process file
        should_continue = process_single_file(markdown_file, cao_number, output_folder, context, len(filtered_files))
        if not should_continue:
            break
    
    # Display final results
    display_final_results(context)


# =============================================================================
# ENTRY POINT
# =============================================================================
# Main entry point for the script
def main():
    """Main entry point for the LLM extraction pipeline (markdown version)."""
    run_extraction_pipeline()


if __name__ == "__main__":
    main()
