"""
CAO Data Analysis - LLM Extraction Pipeline (p4_analysis.py)

This script performs schema-driven LLM extraction on CAO JSON files with:
- Split non-salary schema into 3 independent parts for better performance and reliability
- Adaptive retry strategy with parameter adjustment (temp/top_p/top_k on attempts 4-5)
- Schema validation ensures each part returns expected sections
- Failure-aware retry guidance for LLM-controllable errors (truncated JSON, empty responses)
- Individual part retry logic with independent error handling
- Robust error handling and performance monitoring across 5 separate log files

Schema Structure:
- Salary: Single extraction using SalaryExtractionSchema
- Non-Salary Part 1: General, Bonuses, Wage Scales, Pension, Termination
- Non-Salary Part 2: Leave, Overtime, Training  
- Non-Salary Part 3: Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits

Output Structure:
- outputs/llm_analysis/salary/[cao_number]/
- outputs/llm_analysis/non_salary/gen_bon_wag_pen_ter/[cao_number]/
- outputs/llm_analysis/non_salary/lea_ove_tra/[cao_number]/
- outputs/llm_analysis/non_salary/hom_con_saf_chi_ai_fri/[cao_number]/

Logging Files:
- analysis_performance_salary.jsonl
- analysis_performance_non_salary1.jsonl
- analysis_performance_non_salary2.jsonl
- analysis_performance_non_salary3.jsonl
- analysis_performance.jsonl (combined success only)

It extracts salary and non-salary information using Google Gemini API.

USAGE:
    Single process:
        python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 1

    Multi-process (supports GOOGLE_API_KEY1 to GOOGLE_API_KEY20):
        caffeinate python -u pipelines/p4_analysis.py --key_number 1 --process_id 0 --total_processes 20 > p4_log1.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 2 --process_id 1 --total_processes 20 > p4_log2.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 3 --process_id 2 --total_processes 20 > p4_log3.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 4 --process_id 3 --total_processes 20 > p4_log4.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 5 --process_id 4 --total_processes 20 > p4_log5.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 6 --process_id 5 --total_processes 20 > p4_log6.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 7 --process_id 6 --total_processes 20 > p4_log7.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 8 --process_id 7 --total_processes 20 > p4_log8.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 9 --process_id 8 --total_processes 20 > p4_log9.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 10 --process_id 9 --total_processes 20 > p4_log10.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 11 --process_id 10 --total_processes 20 > p4_log11.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 12 --process_id 11 --total_processes 20 > p4_log12.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 13 --process_id 12 --total_processes 20 > p4_log13.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 14 --process_id 13 --total_processes 20 > p4_log14.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 15 --process_id 14 --total_processes 20 > p4_log15.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 16 --process_id 15 --total_processes 20 > p4_log16.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 17 --process_id 16 --total_processes 20 > p4_log17.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 18 --process_id 17 --total_processes 20 > p4_log18.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 19 --process_id 18 --total_processes 20 > p4_log19.txt 2>&1 &
        caffeinate python -u pipelines/p4_analysis.py --key_number 20 --process_id 19 --total_processes 20 > p4_log20.txt 2>&1 &

    With file limit:
        python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 1 --max_files 10

ARGUMENTS:
    --key_number: Which API key to use (1-20) - defaults to 7
    --process_id: Process ID for work distribution (0-based) - defaults to 0
    --total_processes: Total number of parallel processes - defaults to 1
    --max_files: Maximum number of files to process (optional)

ENVIRONMENT VARIABLES:
    GOOGLE_API_KEY1 to GOOGLE_API_KEY20: Google Gemini API keys for parallel processing

INPUT:
    - JSON files in {config['paths']['outputs_json']}/new_flow/[CAO_NUMBER]/ folders

OUTPUT:
    - Extracted JSON data in outputs/llm_analysis/salary/ and outputs/llm_analysis/non_salary/
    - Error logs: outputs/logs/failed_files_analysis.txt
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
import fcntl
import re
import signal
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any, Literal
from dataclasses import dataclass

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Third-party imports for environment variables, file locking, and data validation
import pandas as pd
import yaml
from dotenv import load_dotenv

# Google Gemini API imports
from google import genai
from google.genai import types

# Import performance monitoring
from monitoring.monitoring_3_1 import PerformanceMonitor

# Import schema definitions
from schema.salary_schema import (
    SalaryRow, SalaryExtractionSchema, SALARY_PROMPT
)
from schema.salary_schema_compact import (
    SalaryExtractionSchemaCompact, SALARY_PROMPT_COMPACT
)
from schema.salary_schema_split import (
    SalaryExtractionSchemaSplit
)
from schema.salary_prompt_split import (
    SALARY_PROMPT_SPLIT_ATTEMPT_9,
    SALARY_PROMPT_SPLIT_ATTEMPT_10
)
from schema.salary_schema_super_compact import (
    SalaryExtractionSchemaSuperCompact, SALARY_PROMPT_SUPER_COMPACT
)
from scripts_pipeline_helper.p4.merge_split_salary import merge_split_salary_results
from scripts_pipeline_helper.p4.retry_logic import (
    handle_llm_errors as handle_llm_errors_p4,
    calculate_quota_retry_delay as calculate_quota_retry_delay_p4,
    get_adjusted_parameters as get_adjusted_parameters_p4,
    get_retry_guidance as get_retry_guidance_p4,
    get_model_parameters as get_model_parameters_p4
)
from schema.non_salary_schema import (
    GeneralInfo, BonusesInfo, WageScalesInfo, PensionInfo, LeaveInfo, 
    TerminationInfo, OvertimeInfo, TrainingInfo, HomeofficeInfo, 
    ContractTypeInfo, SafetyInfo, ChildcareInfo, AIInfo, FringeBenefitsInfo,
    NonSalaryPart1, NonSalaryPart2, NonSalaryPart3, NON_SALARY_PROMPT
)

# =============================================================================
# SIGNAL HANDLING
# =============================================================================
# Handle signals gracefully to prevent unexpected exits when running with pipes/tee
def setup_signal_handlers():
    """Setup signal handlers to prevent unexpected process termination."""
    def handle_sigpipe(signum, frame):
        """Handle SIGPIPE (broken pipe) gracefully - ignore it to prevent exit code 1."""
        # When writing to a pipe (e.g., tee) that closes, we get SIGPIPE
        # Instead of crashing, we'll just ignore it and continue
        pass
    
    def handle_sigterm(signum, frame):
        """Handle SIGTERM gracefully - exit with code 0."""
        print('\n⚠️  Received SIGTERM, exiting gracefully...')
        sys.stdout.flush()
        sys.stderr.flush()
        sys.exit(0)
    
    # Register signal handlers
    # SIGPIPE: Ignore broken pipes (common when using tee/unbuffer)
    try:
        signal.signal(signal.SIGPIPE, handle_sigpipe)
    except (AttributeError, ValueError):
        # SIGPIPE may not be available on all platforms (Windows)
        pass
    
    # SIGTERM: Handle termination gracefully
    try:
        signal.signal(signal.SIGTERM, handle_sigterm)
    except (AttributeError, ValueError):
        pass


# =============================================================================
# GLOBAL FLAGS
# =============================================================================
# Process-specific quota flags to stop individual processes when quota is hit
process_quota_flags = {}

# =============================================================================
# CONSTANTS
# =============================================================================
# Global configuration constants
MODEL = 'gemini-2.5-flash'
SKIP_TRUNCATED_SALARY_FILES = True  # Skip salary extraction for files with MAX_TOKENS truncation

# =============================================================================
# CONFIGURATION & SETUP FUNCTIONS
# =============================================================================
# Functions for loading configuration, setting up environment, and initializing components

@dataclass
class AnalysisConfig:
    """Configuration for the analysis pipeline."""
    input_folder: str
    output_folder: Path
    cao_info_path: str
    max_processing_time_hours: int = 1
    max_json_files: int = 1000000  # Default value for max files to process
    token_limit: int = 900000  # 900K tokens safety limit


def load_configuration() -> AnalysisConfig:
    """Load and validate configuration from config.yaml."""
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    # Resolve project root (two levels up from this file: pipelines/ -> repo root)
    project_root = Path(__file__).resolve().parents[1]
    output_base = project_root / 'outputs' / 'llm_analysis'
    
    return AnalysisConfig(
        input_folder=config_data['paths']['outputs_json'] + "/new_flow",
        output_folder=output_base,
        cao_info_path=f"{config_data['paths']['inputs_pdfs']}/extracted_cao_info.csv"
    )


def setup_performance_monitor_p4() -> PerformanceMonitor:
    """Setup performance monitoring for p4 analysis pipeline."""
    return PerformanceMonitor(
        log_file='performance_logs/llm_analysis/analysis_performance.jsonl',
        summary_file='performance_logs/llm_analysis/analysis_summary.json'
    )


def setup_processing_context(config: AnalysisConfig, process_id: int, 
                           total_processes: int, key_number: int) -> Dict[str, Any]:
    """Setup complete processing context."""
    api_key, actual_key_number = setup_environment(key_number)
    client = setup_gemini_client(api_key)
    performance_monitor = setup_performance_monitor_p4()
    
    return {
        'config': config,
        'process_id': process_id,
        'total_processes': total_processes,
        'api_key': api_key,
        'key_number': actual_key_number,
        'client': client,
        'performance_monitor': performance_monitor
    }


def validate_input_paths(config: AnalysisConfig, process_id: int = 0):
    """Validate that input/output paths exist and are accessible."""
    if not os.path.exists(config.input_folder):
        raise ValueError(f"Input folder does not exist: {config.input_folder}")
    
    # Ensure full directory tree exists (avoid race conditions across processes)
    config.output_folder.mkdir(parents=True, exist_ok=True)
    
    # Check if we can write to output folder
    # Use process_id to avoid race conditions when running parallel processes
    test_file = config.output_folder / f".test_write_p{process_id}"
    try:
        test_file.write_text("test")
        if not test_file.exists():
            raise ValueError("Test file was not created")
    except Exception as e:
        raise ValueError(f"Cannot write to output folder: {config.output_folder}, Error: {e}")
    finally:
        # Clean up test file if it exists (do this separately from validation)
        if test_file.exists():
            try:
                test_file.unlink(missing_ok=True)
            except Exception:
                # Ignore cleanup errors - write permission was already validated
                pass

# =============================================================================
# LLM CLIENT FUNCTIONS
# =============================================================================

def get_safety_settings():
    """
    Get safety settings for Gemini API calls.
    Returns a list of SafetySetting objects with all categories set to BLOCK_NONE.
    """
    return [
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
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
        )
    ]
def setup_environment(key_number: int = 1) -> tuple[str, int]:
    """
    Setup environment variables and API key.
    
    Args:
        key_number: Which API key to use (1, 2, 3, etc.)
        
    Returns:
        tuple: (api_key, actual_key_number)
        
    Raises:
        ValueError: If no API key is found
    """
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
    """
    Setup Gemini client with the provided API key.
    
    Args:
        api_key: Google Gemini API key
        
    Returns:
        genai.Client: Configured Gemini client
    """
    return genai.Client(api_key=api_key)


# get_model_parameters moved to scripts_pipeline_helper.p4.retry_logic

# get_adjusted_parameters, get_retry_guidance, calculate_quota_retry_delay, handle_llm_errors
# moved to scripts_pipeline_helper.p4.retry_logic

# Temporary wrapper for backward compatibility during transition
def get_model_parameters() -> dict:
    """Wrapper to get model parameters using MODEL constant."""
    return get_model_parameters_p4(MODEL)


def log_api_response_details(response, filename: str, processing_time: float = 0) -> None:
    """
    Log detailed information about API response for debugging and monitoring.
    
    Args:
        response: The API response object
        filename: Name of the file being processed
        processing_time: Time taken for API call in seconds
    """
    try:
        # Extract input/output token counts if available
        input_tokens = "N/A"
        output_tokens = "N/A"
        if hasattr(response, 'usage_metadata'):
            if hasattr(response.usage_metadata, 'prompt_token_count'):
                input_tokens = response.usage_metadata.prompt_token_count
            if hasattr(response.usage_metadata, 'candidates_token_count'):
                output_tokens = response.usage_metadata.candidates_token_count
        
        # Extract response size
        response_size = 0
        if hasattr(response, 'text') and response.text:
            response_size = len(response.text)
        elif hasattr(response, 'parsed') and response.parsed:
            response_size = len(str(response.parsed))
        
        # Extract finish reason
        finish_reason = "UNKNOWN"
        if hasattr(response, 'candidates') and response.candidates:
            candidate = response.candidates[0]
            if hasattr(candidate, 'finish_reason'):
                finish_reason = str(candidate.finish_reason)
        
        # Log the comprehensive details
        # print(f"  📊 API Response Details for {filename}:")
        # print(f"    - Input tokens: {input_tokens} | Output tokens: {output_tokens} (max: 65536)")
        # print(f"    - Response size: {response_size} chars | Finish reason: {finish_reason}")
        print(f"Processing time: {processing_time:.1f}s | Status: SUCCESS")
        
    except Exception as e:
        print(f"  ⚠️  Failed to log response details for {filename}: {e}")


def validate_extraction_schema(extracted_data: dict, expected_sections: set, part_name: str) -> bool:
    """
    Validate that extracted data contains the expected top-level sections.
    
    Args:
        extracted_data: The extracted data dictionary
        expected_sections: Set of expected section names
        part_name: Name of the part being validated (for error messages)
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    if not extracted_data:
        print(f'  {part_name}: Validation failed - empty data')
        return False
    
    actual_sections = set(extracted_data.keys())
    
    if actual_sections == expected_sections:
        return True
    else:
        missing_sections = expected_sections - actual_sections
        extra_sections = actual_sections - expected_sections
        
        error_msg = f'{part_name}: Schema validation failed'
        if missing_sections:
            error_msg += f' - missing sections: {missing_sections}'
        if extra_sections:
            error_msg += f' - unexpected sections: {extra_sections}'
        
        print(f'  {error_msg}')
        return False


def check_response_truncation(response, filename: str, cao_number: str = None) -> bool:
    """
    Check if the LLM response was truncated.
    
    Args:
        response: The LLM response object
        filename: Filename for context
        cao_number: CAO number for filename prefix
        
    Returns:
        bool: True if response was truncated, False otherwise
    """
    # Check finish reason for truncation
    if hasattr(response, 'candidates') and response.candidates:
        candidate = response.candidates[0]
        if hasattr(candidate, 'finish_reason'):
            if candidate.finish_reason == 'MAX_TOKENS':
                print(f'  DEBUG: Response truncated due to MAX_TOKENS limit for {filename}')
                save_truncated_response(response.text, filename, cao_number)
                return True
    
    # Check if response text is empty or very short
    if hasattr(response, 'text') and response.text:
        text = response.text.strip()
        if len(text) < 10:  # Extremely short response (likely empty/invalid)
            print(f'  DEBUG: Response text is extremely short ({len(text)} chars) for {filename}')
            return True
        elif len(text) < 50:  # Short response - check if it's valid JSON
            print(f'  DEBUG: Response text is short ({len(text)} chars) for {filename}')
            # Check if it's valid JSON structure (starts with { and ends with })
            if text.startswith('{') and text.endswith('}'):
                try:
                    import json
                    json.loads(text)  # Try to parse it
                    print(f'  DEBUG: Short response is valid JSON, not truncated')
                    return False  # Valid short JSON, not truncated
                except json.JSONDecodeError:
                    print(f'  DEBUG: Short response is invalid JSON, likely truncated')
                    return True
            else:
                print(f'  DEBUG: Short response does not look like JSON, likely truncated')
                return True
    
    # Check if parsed attribute is empty when we expect structured output
    if hasattr(response, 'parsed') and response.parsed is None:
        print(f'  DEBUG: No parsed structured output for {filename}')
        return True
    
    
    return False




def extract_clean_filename(filename: str) -> str:
    """
    Extract a clean filename from the original filename for truncated file naming.
    
    Args:
        filename: Original filename (e.g., "CAO_GHZ_2019-2021_definitief_zonder_wijzigingen_20200107.docx_extract.json")
        
    Returns:
        Clean filename (e.g., "CAO_GHZ_2019-2021_definitief")
    """
    import re
    
    # Remove _extract.json suffix if present
    clean_name = filename
    if clean_name.endswith('_extract.json'):
        clean_name = clean_name[:-13]  # Remove '_extract.json'
    elif clean_name.endswith('.json'):
        clean_name = clean_name[:-5]   # Remove '.json'
    
    # Remove common file extensions
    extensions_to_remove = ['.docx', '.pdf', '.doc']
    for ext in extensions_to_remove:
        if clean_name.endswith(ext):
            clean_name = clean_name[:-len(ext)]
            break
    
    # Clean up the name - remove extra underscores and make it more readable
    clean_name = re.sub(r'_+', '_', clean_name)  # Replace multiple underscores with single
    clean_name = clean_name.strip('_')  # Remove leading/trailing underscores
    
    # Limit length to avoid overly long filenames
    if len(clean_name) > 100:
        clean_name = clean_name[:100].rstrip('_')
    
    return clean_name


def save_truncated_response(response_text: str, filename: str, cao_number: str = None):
    """
    Save truncated response to file for debugging analysis.
    
    Args:
        response_text: The truncated response text
        filename: Original filename for context
        cao_number: CAO number for filename prefix
    """
    try:
        import os
        from datetime import datetime
        
        # Create the truncated responses directory
        truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated")
        truncated_dir.mkdir(parents=True, exist_ok=True)
        
        # Create clean filename without timestamp
        clean_filename = extract_clean_filename(filename)
        
        # Add CAO number prefix if provided
        if cao_number:
            truncated_filename = f"{cao_number}_{clean_filename}_truncated.txt"
        else:
            truncated_filename = f"{clean_filename}_truncated.txt"
        
        truncated_file = truncated_dir / truncated_filename
        
        # Save the truncated response
        with open(truncated_file, 'w', encoding='utf-8') as f:
            f.write(f"TRUNCATED RESPONSE DEBUG INFO\n")
            f.write(f"Original filename: {filename}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Response length: {len(response_text)} characters\n")
            f.write(f"Finish reason: MAX_TOKENS\n")
            f.write(f"{'='*80}\n\n")
            f.write(response_text)
        
        print(f'  DEBUG: Truncated response saved to: {truncated_file}')
        
    except Exception as e:
        print(f'  DEBUG: Failed to save truncated response: {e}')


def is_file_in_truncated_folder(filename: str, cao_number: str) -> bool:
    """
    Check if a file exists in the max_tokens_truncated folder.
    
    Args:
        filename: Original filename (e.g., "CAO_file_extract.json")
        cao_number: CAO number for the file
        
    Returns:
        bool: True if the file exists in truncated folder, False otherwise
    """
    truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated")
    
    if not truncated_dir.exists():
        return False
    
    # Build expected truncated filename: {cao_number}_{clean_filename}_truncated.txt
    clean_filename = extract_clean_filename(filename)
    expected_truncated_file = truncated_dir / f"{cao_number}_{clean_filename}_truncated.txt"
    
    return expected_truncated_file.exists()


def is_file_in_truncated_2_folder(filename: str, cao_number: str) -> bool:
    """
    Check if a file exists in the max_tokens_truncated_2 folder.
    
    Args:
        filename: Original filename (e.g., "CAO_file_extract.json")
        cao_number: CAO number for the file
        
    Returns:
        bool: True if the file exists in truncated_2 folder, False otherwise
    """
    truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated_2")
    
    if not truncated_dir.exists():
        return False
    
    # Build expected truncated filename: {cao_number}_{clean_filename}_truncated.txt
    clean_filename = extract_clean_filename(filename)
    expected_truncated_file = truncated_dir / f"{cao_number}_{clean_filename}_truncated.txt"
    
    return expected_truncated_file.exists()


def is_file_in_truncated_3_folder(filename: str, cao_number: str) -> bool:
    """
    Check if a file exists in the max_tokens_truncated_3 folder.
    
    Args:
        filename: Original filename (e.g., "CAO_file_extract.json")
        cao_number: CAO number for the file
        
    Returns:
        bool: True if the file exists in truncated_3 folder, False otherwise
    """
    truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated_3")
    
    if not truncated_dir.exists():
        return False
    
    # Build expected truncated filename: {cao_number}_{clean_filename}_truncated.txt
    clean_filename = extract_clean_filename(filename)
    expected_truncated_file = truncated_dir / f"{cao_number}_{clean_filename}_truncated.txt"
    
    return expected_truncated_file.exists()


def is_file_in_truncated_4_folder(filename: str, cao_number: str) -> bool:
    """
    Check if a file exists in the max_tokens_truncated_4 folder.
    
    Args:
        filename: Original filename (e.g., "CAO_file_extract.json")
        cao_number: CAO number for the file
        
    Returns:
        bool: True if the file exists in truncated_4 folder, False otherwise
    """
    truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated_4")
    
    if not truncated_dir.exists():
        return False
    
    # Build expected truncated filename: {cao_number}_{clean_filename}_truncated.txt
    clean_filename = extract_clean_filename(filename)
    expected_truncated_file = truncated_dir / f"{cao_number}_{clean_filename}_truncated.txt"
    
    return expected_truncated_file.exists()


def save_failed_attempt_8(response_text: str, error_message: str, filename: str, cao_number: str = None):
    """
    Save failed 8th attempt response to max_tokens_truncated_2 folder for debugging.
    
    Args:
        response_text: The response text (if available)
        error_message: The error message from the failed attempt
        filename: Original filename for context
        cao_number: CAO number for filename prefix
    """
    try:
        import os
        from datetime import datetime
        
        # Create the truncated responses directory
        truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated_2")
        truncated_dir.mkdir(parents=True, exist_ok=True)
        
        # Create clean filename without timestamp
        clean_filename = extract_clean_filename(filename)
        
        # Add CAO number prefix if provided
        if cao_number:
            truncated_filename = f"{cao_number}_{clean_filename}_truncated.txt"
        else:
            truncated_filename = f"{clean_filename}_truncated.txt"
        
        truncated_file = truncated_dir / truncated_filename
        
        # Save the failed attempt response
        with open(truncated_file, 'w', encoding='utf-8') as f:
            f.write(f"FAILED 8TH ATTEMPT DEBUG INFO\n")
            f.write(f"Original filename: {filename}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Attempt: 8th (final attempt with compact schema)\n")
            f.write(f"Error: {error_message}\n")
            if response_text:
                f.write(f"Response length: {len(response_text)} characters\n")
            f.write(f"{'='*80}\n\n")
            if response_text:
                f.write(response_text)
            else:
                f.write("No response text available\n")
        
        print(f'  DEBUG: Failed 8th attempt saved to: {truncated_file}')
        
    except Exception as e:
        print(f'  DEBUG: Failed to save failed 8th attempt: {e}')


def save_failed_attempt_10(response_text: str, error_message: str, filename: str, cao_number: str = None, part: str = None):
    """
    Save failed 10th attempt response to max_tokens_truncated_3 folder for debugging.
    Can save both Part 1 and Part 2 separately.
    
    Args:
        response_text: The response text (if available)
        error_message: The error message from the failed attempt
        filename: Original filename for context
        cao_number: CAO number for filename prefix
        part: Optional part identifier ("Part1" or "Part2") to save separately
    """
    try:
        import os
        from datetime import datetime
        
        # Create the truncated responses directory
        truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated_3")
        truncated_dir.mkdir(parents=True, exist_ok=True)
        
        # Create clean filename without timestamp
        clean_filename = extract_clean_filename(filename)
        
        # Add part identifier to filename if provided
        part_suffix = f"_{part}" if part else ""
        
        # Add CAO number prefix if provided
        if cao_number:
            truncated_filename = f"{cao_number}_{clean_filename}{part_suffix}_truncated.txt"
        else:
            truncated_filename = f"{clean_filename}{part_suffix}_truncated.txt"
        
        truncated_file = truncated_dir / truncated_filename
        
        # Determine attempt number based on part
        attempt_info = f"10th (final attempt with split extraction, {part})" if part else "10th (final attempt with split extraction)"
        
        # Save the failed attempt response
        with open(truncated_file, 'w', encoding='utf-8') as f:
            f.write(f"FAILED 10TH ATTEMPT DEBUG INFO\n")
            f.write(f"Original filename: {filename}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Attempt: {attempt_info}\n")
            f.write(f"Error: {error_message}\n")
            if response_text:
                f.write(f"Response length: {len(response_text)} characters\n")
            f.write(f"{'='*80}\n\n")
            if response_text:
                f.write(response_text)
            else:
                f.write("No response text available\n")
        
        print(f'  DEBUG: Failed 10th attempt ({part if part else "both parts"}) saved to: {truncated_file}')
        
    except Exception as e:
        print(f'  DEBUG: Failed to save failed 10th attempt: {e}')


def save_failed_attempt_11(response_text: str, error_message: str, filename: str, cao_number: str = None):
    """
    Save failed 11th attempt response to max_tokens_truncated_4 folder for debugging.
    
    Args:
        response_text: The response text (if available)
        error_message: The error message from the failed attempt
        filename: Original filename for context
        cao_number: CAO number for filename prefix
    """
    try:
        import os
        from datetime import datetime
        
        # Create the truncated responses directory
        truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated_4")
        truncated_dir.mkdir(parents=True, exist_ok=True)
        
        # Create clean filename without timestamp
        clean_filename = extract_clean_filename(filename)
        
        # Add CAO number prefix if provided
        if cao_number:
            truncated_filename = f"{cao_number}_{clean_filename}_truncated.txt"
        else:
            truncated_filename = f"{clean_filename}_truncated.txt"
        
        truncated_file = truncated_dir / truncated_filename
        
        # Save the failed attempt response
        with open(truncated_file, 'w', encoding='utf-8') as f:
            f.write(f"FAILED 11TH ATTEMPT DEBUG INFO\n")
            f.write(f"Original filename: {filename}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Attempt: 11th (final attempt with super compact schema)\n")
            f.write(f"Error: {error_message}\n")
            if response_text:
                f.write(f"Response length: {len(response_text)} characters\n")
            f.write(f"{'='*80}\n\n")
            if response_text:
                f.write(response_text)
            else:
                f.write("No response text available\n")
        
        print(f'  DEBUG: Failed 11th attempt saved to: {truncated_file}')
        
    except Exception as e:
        print(f'  DEBUG: Failed to save failed 11th attempt: {e}')


# =============================================================================
# TOKEN SAFETY & VALIDATION FUNCTIONS
# =============================================================================
# Functions for checking token limits and validating input data

def check_token_limit(json_text: str, filename: str) -> bool:
    """
    Check if JSON text exceeds token limit.
    
    Args:
        json_text: JSON text to check
        filename: Filename for logging context
        
    Returns:
        bool: True if safe to process, False if should skip
    """
    # Estimate tokens: ~4 chars per token for Gemini models
    estimated_tokens = len(json_text) // 4
    
    if estimated_tokens > 900000:  # 900K token safety limit
        print(f'  {filename}: Skipping - estimated {estimated_tokens:,} tokens exceeds 800K limit')
        return False
    
    return True


def log_analysis_error(filename: str, error: str, raw_output: str = None):
    """
    Log analysis errors to the designated log file.
    
    Args:
        filename: Name of the file being processed
        error: Error message
        raw_output: Raw LLM output (optional)
    """
    log_path = 'outputs/logs/failed_files_analysis.txt'
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"File: {filename}\n")
        f.write(f"Error: {error}\n")
        if raw_output:
            f.write(f"Raw Output: {raw_output}\n")
        f.write("-" * 80 + "\n")


# =============================================================================
# LLM PROMPT TEMPLATES
# =============================================================================
# Exact prompt templates for salary and non-salary extraction

def extract_salary_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None, cao_number: str = None) -> dict:
    """Extract salary information from JSON using LLM."""
    global process_quota_flags
    
    # Check if file is in truncated_4 folder - if so, skip extraction entirely (all attempts exhausted)
    if cao_number and is_file_in_truncated_4_folder(filename, cao_number):
        print(f'  DEBUG: File found in truncated_4 folder - all attempts exhausted, skipping extraction')
        # Log that file is skipped due to truncation_4
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0.0,
                usage_metadata=None,
                success=False,
                analysis_type="salary",
                error_message="File in truncated_4 folder - all attempts exhausted (skipped)",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number,
                model="gemini-2.5-flash",
                parameters={"skipped": True, "reason": "truncated_4"},
                allow_duplicates=False
            )
        return None  # Skip extraction - file failed all attempts including super compact
    
    # Check if file is in truncated_3 folder - if so, skip directly to super compact attempts (10-11)
    use_super_compact_from_start = is_file_in_truncated_3_folder(filename, cao_number) if cao_number else False
    if use_super_compact_from_start:
        print(f'  DEBUG: File found in truncated_3 folder, skipping to super compact schema attempts (11th-12th attempts)')
    
    # Check if file is in truncated_2 folder - if so, skip directly to split extraction attempts (9-10)
    use_split_extraction_from_start = is_file_in_truncated_2_folder(filename, cao_number) if cao_number else False
    if use_split_extraction_from_start and not use_super_compact_from_start:
        print(f'  DEBUG: File found in truncated_2 folder, skipping to split extraction attempts (9th-10th attempts)')
    
    # Check if file is in truncated folder - if so, skip directly to compact schema attempts
    use_compact_schema_from_start = is_file_in_truncated_folder(filename, cao_number) if cao_number else False
    if use_compact_schema_from_start and not use_split_extraction_from_start and not use_super_compact_from_start:
        print(f'  DEBUG: File found in truncated folder, skipping to compact schema attempts (6th-8th attempts)')
    
    salary_text = ""
    wage_keys = ['wage_information', 'Wage information', 'wage information', 'WAGE_INFORMATION']
    
    for key in wage_keys:
        if key in json_obj:
            value = json_obj[key]
            
            if isinstance(value, list):
                flat_value = []
                for item in value:
                    if isinstance(item, list):
                        flat_value.extend(item)
                    elif isinstance(item, str):
                        if 'wage' in item.lower() or 'salary' in item.lower() or 'salaris' in item.lower():
                            flat_value.append(item)
                    else:
                        flat_value.append(str(item))
                salary_text = f'== Wage information ==\n' + '\n'.join(flat_value)
            elif isinstance(value, str):
                salary_text = f'== Wage information ==\n{value}'
            break
    
    # Only process general_information if no wage information was found
    if not salary_text.strip():
        general_keys = ['general_information', 'General information', 'general information', 'GENERAL_INFORMATION']
        for key in general_keys:
            if key in json_obj:
                value = json_obj[key]
                
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            try:
                                nested_data = json.loads(item)
                                wage_keys_nested = ['wage_information', 'Wage information', 'wage information', 'WAGE_INFORMATION']
                                for wage_key in wage_keys_nested:
                                    if wage_key in nested_data and nested_data[wage_key]:
                                        wage_data = nested_data[wage_key]
                                        if isinstance(wage_data, list):
                                            salary_text = f'== Wage information ==\n' + '\n'.join(wage_data)
                                        else:
                                            salary_text = f'== Wage information ==\n{wage_data}'
                                        break
                                if salary_text.strip():
                                    break
                            except json.JSONDecodeError:
                                if 'wage' in item.lower() or 'salary' in item.lower() or 'salaris' in item.lower():
                                    salary_text = f'== Wage information ==\n{item}'
                                    break
                elif isinstance(value, str):
                    if 'wage' in value.lower() or 'salary' in value.lower() or 'salaris' in value.lower():
                        salary_text = f'== Wage information ==\n{value}'
                        break
    
    if not salary_text.strip():
        print(f'  DEBUG: No salary text found!')
        # Log that no salary information was found in the source JSON
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0.0,  # No API call made
                usage_metadata=None,  # No API call made
                success=True,  # Not a failure, just no salary data available
                analysis_type="salary",
                error_message="No salary information found in source JSON",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number,
                model="gemini-2.5-flash",
                parameters={"no_salary_data": True},
                allow_duplicates=False
            )
        # Return empty dict structure for "no salary data" case - this is valid and should be saved
        return {"salary_information": []}
    
    if not check_token_limit(salary_text, filename):
        # Log token limit failure to performance monitor
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0.0,
                usage_metadata=None,
                success=False,
                analysis_type="salary",
                error_message="Token limit check failed - file too large",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number,
                model="gemini-2.5-flash",
                parameters={"token_limit_exceeded": True},
                allow_duplicates=False
            )
        # Return None for token limit failure - don't save this
        return None
    
    # Set base prompt - will be overridden in retry loop based on schema type
    try:
        base_prompt = SALARY_PROMPT.format(filename=filename, source_json=salary_text)
    except Exception as e:
        raise
    
    model_params = get_model_parameters()
    
    
    # Use proper safety settings format for newer google-genai API
    safety_settings = get_safety_settings()
    
    # Skip initial attempt if using compact schema from start
    last_error = None
    last_error_message = None
    truncation_error_after_attempt_4 = False
    first_half_result = None  # Store first half result for split extraction attempt 9
    
    if not use_compact_schema_from_start:
        # Normal flow: try initial attempt with regular schema
        config = {
            "temperature": model_params["temperature"],
            "top_p": model_params["top_p"],
            "top_k": model_params["top_k"],
            "max_output_tokens": 65536,  # Increased to maximum for Gemini 2.5 Flash
            "candidate_count": model_params["candidate_count"],
            "seed": model_params["seed"],
            "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
            "response_mime_type": "application/json",
            "response_schema": SalaryExtractionSchema,
            "safety_settings": safety_settings  # Include safety settings in config
        }
        
        try:
            print(f'  DEBUG: Making API call...')
            start_time = time.time()
            try:
                response = client.models.generate_content(
                    model=MODEL,
                    contents=base_prompt,
                    config=config
                )
            except Exception as api_error:
                # Defensive exception handling for API calls that escape retry loops
                import traceback
                error_type = type(api_error).__name__
                error_msg = str(api_error)
                print(f'  🚨 UNEXPECTED API ERROR during salary extraction (attempt {attempt_index + 1}): {error_type}: {error_msg}')
                print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
                # Log to file for debugging
                try:
                    error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                    Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(error_log_path, 'a', encoding='utf-8') as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (salary extraction)\n")
                        f.write(f"File: {filename}\n")
                        f.write(f"Error Type: {error_type}\n")
                        f.write(f"Error Message: {error_msg}\n")
                        f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
                except Exception:
                    pass  # Don't fail on logging failure
                # Re-raise so existing retry logic can handle it
                raise
            processing_time = time.time() - start_time
            
            
            # Log detailed response information
            log_api_response_details(response, filename, processing_time)
            
            
            # Check for truncation
            if check_response_truncation(response, filename, cao_number):
                raise Exception("Response truncated - incomplete JSON")
            
            # Check if response has parsed attribute (structured output)
            if hasattr(response, 'parsed') and response.parsed is not None:
                result = {"salary_information": [row.model_dump() for row in response.parsed.salary_information]}
                
                # Validate schema - salary_information can be empty if no salary data exists
                # Empty array is valid - some CAOs may not have salary information
                
                print(f'  Salary: Schema validation passed - {len(result["salary_information"])} salary entries')
                
                # Log successful salary extraction
                if context and 'performance_monitor' in context:
                    file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
                    log_params = model_params.copy()
                    log_params['schema_type'] = 'regular'
                    context['performance_monitor'].log_analysis(
                        filename=filename,
                        file_size_mb=file_size_mb,
                        processing_time=processing_time,
                        usage_metadata=getattr(response, 'usage_metadata', None),
                        success=True,
                        analysis_type="salary",
                        api_key_used=context.get('key_number', 1),
                        process_id=context.get('process_id', 0),
                        cao_number=cao_number,
                        model="gemini-2.5-flash",
                        parameters=log_params,
                        allow_duplicates=False
                    )
                
                return result
            else:
                if hasattr(response, 'text'):
                    # Try to parse the text manually with better error handling
                    try:
                        cleaned_text = response.text.strip()
                        
                        # Remove markdown code fences if present
                        if cleaned_text.startswith('```'):
                            lines = cleaned_text.split('\n')
                            cleaned_text = '\n'.join(lines[1:-1]).strip()
                        
                        parsed_json = json.loads(cleaned_text)
                        # Validate against schema
                        if 'salary_information' in parsed_json:
                            salary_schema = SalaryExtractionSchema(**parsed_json)
                            result = {"salary_information": [row.model_dump() for row in salary_schema.salary_information]}
                            return result
                        else:
                            log_analysis_error(filename, f"No salary_information key in parsed JSON. Available keys: {list(parsed_json.keys())}", str(parsed_json))
                    except json.JSONDecodeError as e:
                        # Try to extract partial data from the response
                        try:
                            # Look for salary_information array in the text
                            import re
                            salary_match = re.search(r'"salary_information":\s*\[(.*?)\]', response.text, re.DOTALL)
                            if salary_match:
                                # Partial salary data found but couldn't parse - log error
                                log_analysis_error(filename, f"Partial salary data found but couldn't parse: {e}", response.text[:1000])
                            else:
                                log_analysis_error(filename, f"JSON parsing failed and no salary data found: {e}", response.text[:1000])
                        except Exception as parse_error:
                            log_analysis_error(filename, f"Complete parsing failure: {parse_error}", response.text[:1000])
                    except Exception as e:
                        log_analysis_error(filename, f"Schema validation failed: {e}", response.text[:1000])
                else:
                    log_analysis_error(filename, "No structured output received from model", "")
                    # Log to performance monitor before returning
                    if context and 'performance_monitor' in context:
                        file_size_mb = len(str(json_obj)) / (1024 * 1024)
                        context['performance_monitor'].log_analysis(
                            filename=filename,
                            file_size_mb=file_size_mb,
                            processing_time=processing_time if 'processing_time' in locals() else 0.0,
                            usage_metadata=getattr(response, 'usage_metadata', None) if 'response' in locals() else None,
                            success=False,
                            analysis_type="salary",
                            error_message="No structured output received from model",
                            api_key_used=context.get('key_number', 1),
                            process_id=context.get('process_id', 0),
                            cao_number=cao_number,
                            model="gemini-2.5-flash",
                            parameters={"no_structured_output": True},
                            allow_duplicates=False
                        )
                    # Return None for no structured output - don't save this
                    return None
                
        except Exception as e:
            last_error = e  # Capture the initial error
            last_error_message = str(e)  # Track error message for retry guidance
    
    # Retry logic with proper attempt tracking
    # Determine attempt range based on whether we're starting with super compact, split extraction, or compact schema
    if use_super_compact_from_start:
        # Skip directly to super compact schema attempts (10, which is retry 11)
        # Removed attempt 11 (duplicate of 10)
        attempts_to_try = [10]
    elif use_split_extraction_from_start:
        # Skip directly to split extraction attempts (8, which is retry 9)
        # Removed attempt 9 (duplicate of 8)
        attempts_to_try = [8]
    elif use_compact_schema_from_start:
        # Skip to compact schema attempts (5-6), may extend to 8 if truncation error
        # Removed attempt 7 (duplicate of 6)
        attempts_to_try = [5, 6]
    else:
        # Normal retry attempts (0, 2, 4), may extend to 5-6 if truncation error
        # Removed attempts 1 (duplicate of 0) and 3 (user request)
        attempts_to_try = [0, 2, 4]
    
    # Track if we need to extend to compact schema attempts, split extraction attempts, or super compact attempts
    extended_to_compact = False
    extended_to_split = False
    extended_to_super_compact = False
    
    attempt_index = 0
    total_attempts = 0  # Total retry attempts (for max_retries limit)
    max_total_attempts = 7  # Maximum total attempts across all retry strategies (reduced from 12)
    
    while attempt_index < len(attempts_to_try) and total_attempts < max_total_attempts:
        # Check quota exhaustion at START of each retry attempt (before API call)
        process_id = context.get('process_id', 0) if context else 0
        if process_id in process_quota_flags and process_quota_flags[process_id]:
            print(f'  🛑 Quota exhausted detected at start of retry loop, stopping retries')
            break
        
        attempt = attempts_to_try[attempt_index]
        try:
            # Determine which schema to use: super compact for attempt 10, split for attempt 8, compact for attempts 5-6, regular for 0,2,4
            use_super_compact_schema = (attempt >= 10)
            use_split_schema = (attempt == 8)
            use_compact_schema = (attempt >= 5 and attempt < 8)
            schema_type = 'super_compact' if use_super_compact_schema else ('split' if use_split_schema else ('compact' if use_compact_schema else 'regular'))
            
            if use_super_compact_schema:
                response_schema = SalaryExtractionSchemaSuperCompact
            elif use_split_schema:
                response_schema = SalaryExtractionSchemaSplit
            elif use_compact_schema:
                response_schema = SalaryExtractionSchemaCompact
            else:
                response_schema = SalaryExtractionSchema
            
            # Get adjusted parameters for this attempt
            # For super compact schema attempts (10), use specific parameter adjustments
            # Note: Super compact is a single extraction (not split like attempt 8), so no delay between attempts
            # For attempts 5-10, there is NO delay between retries - they happen immediately
            # Files in truncated_3 folder go directly to attempt 10
            if use_super_compact_schema:
                # Attempt 10 (11th overall, super compact): temp=0.3, top_p=0.4, top_k=0.7 (same as attempt 4)
                # Removed attempt 11 (duplicate of 10)
                adjusted_params = get_adjusted_parameters_p4(4, MODEL)
            # For split schema attempts (8), use specific parameter adjustments
            elif use_split_schema:
                # Attempt 8 (9th overall, split): Use attempt 4 parameters (temp=0.3, top_p=0.4)
                # Removed attempt 9 (duplicate of 8)
                adjusted_params = get_adjusted_parameters_p4(4, MODEL)
            elif use_compact_schema:
                if attempt == 5:
                    # Attempt 5 (6th overall): Original parameters (temp=0.0, top_p=0.1)
                    base_params = get_model_parameters_p4(MODEL)
                    adjusted_params = {
                        "model": base_params["model"],
                        "temperature": base_params["temperature"],
                        "top_p": base_params["top_p"],
                        "top_k": base_params["top_k"],
                        "max_tokens": base_params["max_tokens"],
                        "candidate_count": base_params["candidate_count"],
                        "seed": base_params["seed"],
                        "thinking_budget": base_params["thinking_budget"],
                        "max_retries": base_params["max_retries"]
                    }
                else:  # attempt == 6
                    # Attempt 6 (7th overall): Like attempt 4 (temp=0.3, top_p=0.4)
                    # Removed attempt 7 (duplicate of 6)
                    adjusted_params = get_adjusted_parameters_p4(4, MODEL)
            else:
                adjusted_params = get_adjusted_parameters_p4(attempt, MODEL)
            
            # Handle split extraction separately - each attempt does both parts sequentially
            if use_split_schema:
                # Split extraction: attempt 8 (9th) - does part 1 then part 2
                # Removed attempt 9 (duplicate of 8)
                delay_seconds = 180  # 3 minutes delay between parts
                
                print(f'  ========================================')
                print(f'  Attempt {attempt + 1} (9th overall): Starting split extraction')
                print(f'  ========================================')
                
                # ========== PART 1: First Half Extraction ==========
                print(f'  Part 1: Extracting first half of salary data...')
                prompt_part1 = SALARY_PROMPT_SPLIT_ATTEMPT_9.format(filename=filename, source_json=salary_text)
                
                # Use proper safety settings format for newer google-genai API
                safety_settings = get_safety_settings()
                
                # Prepare API configuration for part 1
                config_part1 = {
                    "temperature": adjusted_params["temperature"],
                    "top_p": adjusted_params["top_p"],
                    "top_k": adjusted_params["top_k"],
                    "max_output_tokens": 65536,
                    "candidate_count": adjusted_params["candidate_count"],
                    "seed": adjusted_params["seed"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=adjusted_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": response_schema,
                    "safety_settings": safety_settings
                }
                
                print(f'  DEBUG: Making API call for attempt {attempt + 1}, Part 1 (first half)...')
                start_time_part1 = time.time()
                try:
                    response_part1 = client.models.generate_content(
                        model=MODEL,
                        contents=prompt_part1,
                        config=config_part1
                    )
                except Exception as api_error:
                    import traceback
                    error_type = type(api_error).__name__
                    error_msg = str(api_error)
                    print(f'  ERROR: Part 1 failed - {error_type}: {error_msg}')
                    print(f'  ERROR: Attempt {attempt + 1}, Part 1 (first half) extraction failed')
                    # Save part 1 error to max_tokens_truncated_3 before raising
                    save_failed_attempt_10("", f"Part 1 (first half) extraction failed: {error_msg}", filename, cao_number, part="Part1")
                    raise Exception(f"Part 1 (first half) extraction failed: {error_msg}")
                
                processing_time_part1 = time.time() - start_time_part1
                print(f'  DEBUG: Part 1 API response received (processing time: {processing_time_part1:.1f}s)')
                
                # Check for truncation in part 1
                if check_response_truncation(response_part1, filename, cao_number):
                    print(f'  ERROR: Part 1 response truncated - incomplete JSON')
                    # Save part 1 response to max_tokens_truncated_3 before raising
                    response_text_part1 = response_part1.text if hasattr(response_part1, 'text') else ""
                    save_failed_attempt_10(response_text_part1, "Part 1 (first half) response truncated - incomplete JSON", filename, cao_number, part="Part1")
                    raise Exception("Part 1 (first half) response truncated - incomplete JSON")
                
                # Parse part 1 result
                first_half_result = None
                if hasattr(response_part1, 'parsed') and response_part1.parsed is not None:
                    parsed_dump = response_part1.parsed.model_dump()
                    # Compact/split schema uses 'si', regular uses 'salary_information'
                    salary_data = parsed_dump.get('si', parsed_dump.get('salary_information', []))
                    first_half_result = {"salary_information": salary_data}
                    print(f'  SUCCESS: Part 1 extraction successful: {len(salary_data)} rows')
                elif hasattr(response_part1, 'text'):
                    try:
                        cleaned_text = response_part1.text.strip()
                        if cleaned_text.startswith('```'):
                            lines = cleaned_text.split('\n')
                            cleaned_text = '\n'.join(lines[1:-1]).strip()
                        parsed_json = json.loads(cleaned_text)
                        # Compact/split schema uses 'si', regular uses 'salary_information'
                        if 'si' in parsed_json or 'salary_information' in parsed_json:
                            salary_schema = SalaryExtractionSchemaSplit(**parsed_json)
                            # Access 'si' field (compact schema uses 2-letter field names)
                            salary_data = salary_schema.si if hasattr(salary_schema, 'si') else salary_schema.salary_information
                            first_half_result = {"salary_information": [row.model_dump() for row in salary_data]}
                            print(f'  SUCCESS: Part 1 extraction successful (manual parse): {len(first_half_result.get("salary_information", []))} rows')
                        else:
                            raise Exception("No 'si' or 'salary_information' key in Part 1 response")
                    except Exception as parse_error:
                        print(f'  ERROR: Part 1 parsing failed: {parse_error}')
                        # Save part 1 response to max_tokens_truncated_3 before raising
                        response_text_part1 = response_part1.text if hasattr(response_part1, 'text') else ""
                        save_failed_attempt_10(response_text_part1, f"Part 1 (first half) parsing failed: {parse_error}", filename, cao_number, part="Part1")
                        raise Exception(f"Part 1 (first half) parsing failed: {parse_error}")
                else:
                    # Save part 1 response to max_tokens_truncated_3 before raising
                    response_text_part1 = response_part1.text if hasattr(response_part1, 'text') else ""
                    save_failed_attempt_10(response_text_part1, "Part 1 (first half) - no structured output received", filename, cao_number, part="Part1")
                    raise Exception("Part 1 (first half) - no structured output received")
                
                # ========== DELAY BETWEEN PARTS ==========
                delay_minutes = delay_seconds // 60
                delay_remaining_seconds = delay_seconds % 60
                if delay_remaining_seconds > 0:
                    print(f'  INFO: Waiting {delay_minutes} minutes {delay_remaining_seconds} seconds ({delay_seconds} seconds total) before Part 2 extraction...')
                else:
                    print(f'  INFO: Waiting {delay_minutes} minutes ({delay_seconds} seconds total) before Part 2 extraction...')
                time.sleep(delay_seconds)
                print(f'  INFO: Delay completed ({delay_seconds} seconds), proceeding to Part 2')
                
                # ========== PART 2: Second Half Extraction ==========
                print(f'  Part 2: Extracting second half of salary data...')
                already_extracted_json = json.dumps(first_half_result, ensure_ascii=False, indent=2)
                prompt_part2 = SALARY_PROMPT_SPLIT_ATTEMPT_10.format(
                    filename=filename,
                    source_json=salary_text,
                    already_extracted_json=already_extracted_json
                )
                
                # Prepare API configuration for part 2
                config_part2 = {
                    "temperature": adjusted_params["temperature"],
                    "top_p": adjusted_params["top_p"],
                    "top_k": adjusted_params["top_k"],
                    "max_output_tokens": 65536,
                    "candidate_count": adjusted_params["candidate_count"],
                    "seed": adjusted_params["seed"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=adjusted_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": response_schema,
                    "safety_settings": safety_settings
                }
                
                print(f'  DEBUG: Making API call for attempt {attempt + 1}, Part 2 (second half)...')
                start_time_part2 = time.time()
                try:
                    response_part2 = client.models.generate_content(
                        model=MODEL,
                        contents=prompt_part2,
                        config=config_part2
                    )
                except Exception as api_error:
                    import traceback
                    error_type = type(api_error).__name__
                    error_msg = str(api_error)
                    print(f'  ERROR: Part 2 failed - {error_type}: {error_msg}')
                    print(f'  ERROR: Attempt {attempt + 1}, Part 2 (second half) extraction failed')
                    # Save part 2 error to max_tokens_truncated_3 before raising
                    save_failed_attempt_10("", f"Part 2 (second half) extraction failed: {error_msg}", filename, cao_number, part="Part2")
                    raise Exception(f"Part 2 (second half) extraction failed: {error_msg}")
                
                processing_time_part2 = time.time() - start_time_part2
                print(f'  DEBUG: Part 2 API response received (processing time: {processing_time_part2:.1f}s)')
                
                # Check for truncation in part 2
                if check_response_truncation(response_part2, filename, cao_number):
                    print(f'  ERROR: Part 2 response truncated - incomplete JSON')
                    # Save part 2 response to max_tokens_truncated_3 before raising
                    response_text_part2 = response_part2.text if hasattr(response_part2, 'text') else ""
                    save_failed_attempt_10(response_text_part2, "Part 2 (second half) response truncated - incomplete JSON", filename, cao_number, part="Part2")
                    raise Exception("Part 2 (second half) response truncated - incomplete JSON")
                
                # Parse part 2 result
                second_half_result = None
                if hasattr(response_part2, 'parsed') and response_part2.parsed is not None:
                    parsed_dump = response_part2.parsed.model_dump()
                    # Compact/split schema uses 'si', regular uses 'salary_information'
                    salary_data = parsed_dump.get('si', parsed_dump.get('salary_information', []))
                    second_half_result = {"salary_information": salary_data}
                    print(f'  SUCCESS: Part 2 extraction successful: {len(salary_data)} rows')
                elif hasattr(response_part2, 'text'):
                    try:
                        cleaned_text = response_part2.text.strip()
                        if cleaned_text.startswith('```'):
                            lines = cleaned_text.split('\n')
                            cleaned_text = '\n'.join(lines[1:-1]).strip()
                        parsed_json = json.loads(cleaned_text)
                        # Compact/split schema uses 'si', regular uses 'salary_information'
                        if 'si' in parsed_json or 'salary_information' in parsed_json:
                            salary_schema = SalaryExtractionSchemaSplit(**parsed_json)
                            # Access 'si' field (compact schema uses 2-letter field names)
                            salary_data = salary_schema.si if hasattr(salary_schema, 'si') else salary_schema.salary_information
                            second_half_result = {"salary_information": [row.model_dump() for row in salary_data]}
                            print(f'  SUCCESS: Part 2 extraction successful (manual parse): {len(second_half_result.get("salary_information", []))} rows')
                        else:
                            raise Exception("No 'si' or 'salary_information' key in Part 2 response")
                    except Exception as parse_error:
                        print(f'  ERROR: Part 2 parsing failed: {parse_error}')
                        # Save part 2 response to max_tokens_truncated_3 before raising
                        response_text_part2 = response_part2.text if hasattr(response_part2, 'text') else ""
                        save_failed_attempt_10(response_text_part2, f"Part 2 (second half) parsing failed: {parse_error}", filename, cao_number, part="Part2")
                        raise Exception(f"Part 2 (second half) parsing failed: {parse_error}")
                else:
                    # Save part 2 response to max_tokens_truncated_3 before raising
                    response_text_part2 = response_part2.text if hasattr(response_part2, 'text') else ""
                    save_failed_attempt_10(response_text_part2, "Part 2 (second half) - no structured output received", filename, cao_number, part="Part2")
                    raise Exception("Part 2 (second half) - no structured output received")
                
                # ========== MERGE BOTH PARTS ==========
                print(f'  DEBUG: Merging Part 1 ({len(first_half_result.get("salary_information", []))} rows) and Part 2 ({len(second_half_result.get("salary_information", []))} rows)...')
                merged_result = merge_split_salary_results(first_half_result, second_half_result, filename)
                
                if not merged_result:
                    raise Exception("Failed to merge split extraction results")
                
                print(f'  SUCCESS: Merge successful: {len(merged_result.get("salary_information", []))} total rows')
                print(f'  ========================================')
                print(f'  Attempt {attempt + 1} completed successfully (both parts)')
                print(f'  ========================================')
                
                result = merged_result
                total_processing_time = processing_time_part1 + processing_time_part2
                
                # Log successful split extraction
                if context and 'performance_monitor' in context:
                    file_size_mb = len(str(json_obj)) / (1024 * 1024)
                    log_params = adjusted_params.copy()
                    log_params['schema_type'] = schema_type
                    log_params['attempt'] = attempt + 1
                    log_params['merged'] = True
                    log_params['first_half_rows'] = len(first_half_result.get("salary_information", []))
                    log_params['second_half_rows'] = len(second_half_result.get("salary_information", []))
                    log_params['part1_processing_time'] = processing_time_part1
                    log_params['part2_processing_time'] = processing_time_part2
                    
                    # Use combined usage metadata if available
                    usage_metadata = None
                    if hasattr(response_part1, 'usage_metadata') and hasattr(response_part2, 'usage_metadata'):
                        # Combine usage metadata from both parts
                        usage_metadata = response_part1.usage_metadata
                        # Note: We can't easily combine usage metadata, so we'll use part 1's metadata
                    
                    context['performance_monitor'].log_analysis(
                        filename=filename,
                        file_size_mb=file_size_mb,
                        processing_time=total_processing_time,
                        usage_metadata=usage_metadata,
                        success=True,
                        analysis_type="salary",
                        api_key_used=context.get('key_number', 1),
                        process_id=context.get('process_id', 0),
                        cao_number=cao_number,
                        model="gemini-2.5-flash",
                        parameters=log_params,
                        allow_duplicates=False
                    )
                
                return result
            
            # Non-split extraction: regular flow
            # Generate retry guidance (only if attempt >= 2)
            retry_guidance = ""
            error_type = ""
            if attempt >= 2 and last_error_message:
                retry_guidance, error_type = get_retry_guidance_p4(last_error_message)
                if retry_guidance:
                    print(f'  INFO: Adding retry guidance for: {error_type}')
            
            # Select the correct prompt based on schema type
            if use_split_schema:
                # Split extraction uses its own prompts (handled above)
                prompt = base_prompt  # This shouldn't be reached for split schema
            elif use_super_compact_schema:
                print(f'  DEBUG: Using super compact schema for attempt {attempt + 1} (schema_type: {schema_type})')
                prompt = SALARY_PROMPT_SUPER_COMPACT.format(filename=filename, source_json=salary_text)
            elif use_compact_schema:
                print(f'  DEBUG: Using compact schema for attempt {attempt + 1} (schema_type: {schema_type})')
                prompt = SALARY_PROMPT_COMPACT.format(filename=filename, source_json=salary_text)
            else:
                prompt = SALARY_PROMPT.format(filename=filename, source_json=salary_text)
            
            # Add retry guidance if applicable
            if retry_guidance:
                prompt += f"\n\n{retry_guidance}"
            
            # print(f'  DEBUG: Model params: {adjusted_params}')
            
            # Use proper safety settings format for newer google-genai API
            safety_settings = get_safety_settings()
            
            # Prepare API configuration
            config = {
                "temperature": adjusted_params["temperature"],
                "top_p": adjusted_params["top_p"],
                "top_k": adjusted_params["top_k"],
                "max_output_tokens": 65536,  # Increased to maximum for Gemini 2.5 Flash
                "candidate_count": adjusted_params["candidate_count"],
                "seed": adjusted_params["seed"],
                "thinking_config": types.ThinkingConfig(thinking_budget=adjusted_params["thinking_budget"]),
                "response_mime_type": "application/json",
                "response_schema": response_schema,
                "safety_settings": safety_settings  # Include safety settings in config
            }
            
            print(f'  DEBUG: Making API call for attempt {attempt + 1}...')
            
            start_time = time.time()
            try:
                response = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config
                )
            except Exception as api_error:
                # Defensive exception handling for API calls that escape retry loops
                import traceback
                error_type = type(api_error).__name__
                error_msg = str(api_error)
                print(f'  🚨 UNEXPECTED API ERROR during salary extraction (attempt {attempt_index + 1}): {error_type}: {error_msg}')
                print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
                # Log to file for debugging
                try:
                    error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                    Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(error_log_path, 'a', encoding='utf-8') as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (salary extraction)\n")
                        f.write(f"File: {filename}\n")
                        f.write(f"Error Type: {error_type}\n")
                        f.write(f"Error Message: {error_msg}\n")
                        f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
                except Exception:
                    pass  # Don't fail on logging failure
                # Re-raise so existing retry logic can handle it
                raise
            processing_time = time.time() - start_time
            
            print(f'  DEBUG: API response received')
            print(f'  DEBUG: Response type: {type(response)}')
            # print(f'  DEBUG: Response attributes: {dir(response)}')
            
            # Log detailed response information
            log_api_response_details(response, filename, processing_time)
            
            # Check for truncation
            if check_response_truncation(response, filename, cao_number):
                print(f'  DEBUG: Response appears to be truncated, will retry with different parameters')
                raise Exception("Response truncated - incomplete JSON")
            
            # Check if response has parsed attribute (structured output)
            if hasattr(response, 'parsed') and response.parsed is not None:
                print(f'  DEBUG: Response has parsed attribute')
                parsed_dump = response.parsed.model_dump()
                # Compact/split/super compact schema uses 'si', regular uses 'salary_information'
                salary_data_raw = parsed_dump.get('si', parsed_dump.get('salary_information', []))
                
                # Convert to list of dicts for easier manipulation (in case they're Pydantic models)
                salary_data = []
                for row in salary_data_raw:
                    if isinstance(row, dict):
                        salary_data.append(row)
                    else:
                        # If it's a Pydantic model, convert to dict
                        salary_data.append(row.model_dump() if hasattr(row, 'model_dump') else dict(row))
                
                # Add row note to each row if super compact schema was used (AFTER extraction, not part of LLM extraction)
                if use_super_compact_schema:
                    super_compact_note = "Note: This data was extracted using the super compact schema (minimal fields only) due to file size constraints. Extracted fields: jobgroup, step, worker, age_group, education, permanency, timeline as parallel arrays (sd: start dates, am: amounts, un: unit(s) - single value if all same, array if they differ)."
                    for row in salary_data:
                        if isinstance(row, dict):
                            row['rn'] = super_compact_note
                
                result = {"salary_information": salary_data}
                print(f'  DEBUG: Parsed salary structure with {len(salary_data)} salary rows')
                
                # Log successful salary extraction
                if context and 'performance_monitor' in context:
                    file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
                    
                    # Add retry guidance info and schema type to parameters for logging
                    log_params = adjusted_params.copy()
                    log_params['schema_type'] = schema_type
                    log_params['attempt'] = attempt + 1  # Log attempt number (1-indexed)
                    # Note: Split extraction logging is handled in the split extraction block above
                    if 'retry_guidance' in locals() and retry_guidance and 'error_type' in locals():
                        log_params['retry_guidance_used'] = error_type
                    
                    context['performance_monitor'].log_analysis(
                        filename=filename,
                        file_size_mb=file_size_mb,
                        processing_time=processing_time,
                        usage_metadata=getattr(response, 'usage_metadata', None),
                        success=True,
                        analysis_type="salary",
                        api_key_used=context.get('key_number', 1),
                        process_id=context.get('process_id', 0),
                        cao_number=cao_number,
                        model="gemini-2.5-flash",
                        parameters=log_params,
                        allow_duplicates=False
                    )
                
                return result
            else:
                # Try to parse manually if parsed attribute not available
                if hasattr(response, 'text'):
                    try:
                        cleaned_text = response.text.strip()
                        if cleaned_text.startswith('```'):
                            lines = cleaned_text.split('\n')
                            cleaned_text = '\n'.join(lines[1:-1]).strip()
                        
                        parsed_json = json.loads(cleaned_text)
                        # Compact/split/super compact schema uses 'si', regular uses 'salary_information'
                        if 'si' in parsed_json or 'salary_information' in parsed_json:
                            if use_super_compact_schema:
                                salary_schema = SalaryExtractionSchemaSuperCompact(**parsed_json)
                                # Super compact schema uses 'si' field (2-letter field names)
                                salary_data = salary_schema.si if hasattr(salary_schema, 'si') else salary_schema.salary_information
                            elif use_split_schema:
                                salary_schema = SalaryExtractionSchemaSplit(**parsed_json)
                                # Compact/split schema uses 'si' field (2-letter field names)
                                salary_data = salary_schema.si if hasattr(salary_schema, 'si') else salary_schema.salary_information
                            elif use_compact_schema:
                                salary_schema = SalaryExtractionSchemaCompact(**parsed_json)
                                # Compact schema uses 'si' field (2-letter field names)
                                salary_data = salary_schema.si if hasattr(salary_schema, 'si') else salary_schema.salary_information
                            else:
                                salary_schema = SalaryExtractionSchema(**parsed_json)
                                salary_data = salary_schema.salary_information
                            
                            # Convert to list of dicts for easier manipulation
                            salary_rows = [row.model_dump() for row in salary_data]
                            
                            # Add row note to each row if super compact schema was used (AFTER extraction, not part of LLM extraction)
                            if use_super_compact_schema:
                                super_compact_note = "Note: This data was extracted using the super compact schema (minimal fields only) due to file size constraints. Extracted fields: jobgroup, step, worker, age_group, education, permanency, timeline as parallel arrays (sd: start dates, am: amounts, un: unit(s) - single value if all same, array if they differ)."
                                for row in salary_rows:
                                    if isinstance(row, dict):
                                        row['rn'] = super_compact_note
                            
                            result = {"salary_information": salary_rows}
                        else:
                            raise Exception("No 'si' or 'salary_information' key in parsed JSON")
                            
                            # Note: Split extraction is handled above, so we don't need to handle it here
                            
                            # Log successful extraction
                            if context and 'performance_monitor' in context:
                                file_size_mb = len(str(json_obj)) / (1024 * 1024)
                                log_params = adjusted_params.copy()
                                log_params['schema_type'] = schema_type
                                log_params['attempt'] = attempt + 1  # Log attempt number (1-indexed)
                                # Note: Split extraction logging is handled in the split extraction block above
                                if 'retry_guidance' in locals() and retry_guidance and 'error_type' in locals():
                                    log_params['retry_guidance_used'] = error_type
                                
                                context['performance_monitor'].log_analysis(
                                    filename=filename,
                                    file_size_mb=file_size_mb,
                                    processing_time=processing_time,
                                    usage_metadata=getattr(response, 'usage_metadata', None),
                                    success=True,
                                    analysis_type="salary",
                                    api_key_used=context.get('key_number', 1),
                                    process_id=context.get('process_id', 0),
                                    cao_number=cao_number,
                                    model="gemini-2.5-flash",
                                    parameters=log_params,
                                    allow_duplicates=False
                                )
                            
                            return result
                    except Exception as parse_error:
                        log_analysis_error(filename, f"Manual parsing failed ({schema_type} schema): {parse_error}", response.text[:1000] if hasattr(response, 'text') else "")
                        # Re-raise to trigger retry logic
                        raise
                    
        except Exception as e:
            last_error = e  # Update last error for each attempt
            last_error_message = str(e)  # Capture error message for retry guidance
            error_str = str(e)
            
            # Enhanced error reporting for split extraction
            if use_split_schema:
                if "Part 1" in error_str or "first half" in error_str.lower():
                    print(f'  ERROR: Attempt {attempt + 1} failed at Part 1 (first half): {type(e).__name__}: {e}')
                elif "Part 2" in error_str or "second half" in error_str.lower():
                    print(f'  ERROR: Attempt {attempt + 1} failed at Part 2 (second half): {type(e).__name__}: {e}')
                else:
                    print(f'  ERROR: Attempt {attempt + 1} failed (split extraction): {type(e).__name__}: {e}')
            else:
                print(f'  DEBUG: Attempt {attempt + 1} failed: {type(e).__name__}: {e}')
            
            # Check if quota was exhausted during this attempt (before calling handle_llm_errors)
            process_id = context.get('process_id', 0) if context else 0
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  🛑 Quota exhausted during salary extraction, stopping retries')
                break
            
            # Check if attempt 4 failed with truncation error - if so, add attempts 5-7
            if attempt == 4 and not use_compact_schema_from_start and not extended_to_compact:
                if "Response truncated - incomplete JSON" in error_str or "Max tokens" in error_str or "truncated" in error_str.lower():
                    truncation_error_after_attempt_4 = True
                    extended_to_compact = True
                    print(f'  DEBUG: Truncation error detected after attempt 4, adding compact schema attempts (5-6)')
                    # Extend attempts list to include 5-6 (removed 7 as duplicate of 6)
                    attempts_to_try.extend([5, 6])
                    total_attempts += 1  # Increment total attempts counter
                    attempt_index += 1
                    continue  # Continue to attempt 5
            
            # Check if attempt 6 failed with truncation error - if so, add attempt 8 (split extraction)
            if attempt == 6 and not use_split_extraction_from_start and not extended_to_split:
                if "Response truncated - incomplete JSON" in error_str or "Max tokens" in error_str or "truncated" in error_str.lower():
                    extended_to_split = True
                    print(f'  DEBUG: Truncation error detected after attempt 6, adding split extraction attempt (8, retry 9)')
                    # Extend attempts list to include 8 (removed 9 as duplicate of 8)
                    attempts_to_try.extend([8])
                    total_attempts += 1  # Increment total attempts counter
                    attempt_index += 1
                    continue  # Continue to attempt 8
            
            # Check if attempt 8 failed with truncation error - if so, add attempt 10 (super compact schema)
            if attempt == 8 and not use_super_compact_from_start and not extended_to_super_compact:
                if "Response truncated - incomplete JSON" in error_str or "Max tokens" in error_str or "truncated" in error_str.lower():
                    extended_to_super_compact = True
                    print(f'  DEBUG: Truncation error detected after attempt 8, adding super compact schema attempt (10, retry 11)')
                    # Extend attempts list to include 10 (removed 11 as duplicate of 10)
                    attempts_to_try.extend([10])
                    total_attempts += 1  # Increment total attempts counter
                    attempt_index += 1
                    continue  # Continue to attempt 10
            
            # Handle retry logic
            if attempt_index < len(attempts_to_try) - 1:
                if attempt < 4:
                    file_size_mb = len(str(json_obj)) / (1024 * 1024) if 'json_obj' in locals() else 0
                    should_retry, increment_attempt, wait_time = handle_llm_errors_p4(
                        e, attempt, 5, file_size_mb, context, process_quota_flags
                    )
                    # Check again after handle_llm_errors (it may have set the flag)
                    if process_id in process_quota_flags and process_quota_flags[process_id]:
                        print(f'  🛑 Quota exhausted detected by handle_llm_errors, stopping retries')
                        break
                    if should_retry:
                        # Wait before retrying
                        if wait_time > 0:
                            print(f'  INFO: Waiting {wait_time // 60 if wait_time >= 60 else wait_time} {"minutes" if wait_time >= 60 else "seconds"} before retry...')
                            time.sleep(wait_time)
                            if wait_time >= 60:
                                print(f'  INFO: Wait complete, continuing with retry...')
                        # Increment attempt counters
                        total_attempts += 1
                        if increment_attempt:
                            attempt_index += 1
                        # Continue to retry (with same attempt number if increment_attempt is False)
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # For attempts 5-11, also use handle_llm_errors to handle external errors properly
                    file_size_mb = len(str(json_obj)) / (1024 * 1024) if 'json_obj' in locals() else 0
                    should_retry, increment_attempt, wait_time = handle_llm_errors_p4(
                        e, attempt, 5, file_size_mb, context, process_quota_flags
                    )
                    # Check again after handle_llm_errors (it may have set the flag)
                    if process_id in process_quota_flags and process_quota_flags[process_id]:
                        print(f'  🛑 Quota exhausted detected by handle_llm_errors, stopping retries')
                        break
                    if should_retry:
                        # Wait before retrying
                        if wait_time > 0:
                            print(f'  INFO: Waiting {wait_time // 60 if wait_time >= 60 else wait_time} {"minutes" if wait_time >= 60 else "seconds"} before retry...')
                            time.sleep(wait_time)
                            if wait_time >= 60:
                                print(f'  INFO: Wait complete, continuing with retry...')
                        # Increment attempt counters
                        total_attempts += 1
                        if increment_attempt:
                            attempt_index += 1
                        # Continue to retry (with same attempt number if increment_attempt is False)
                        continue  # Retry
                    else:
                        break  # Don't retry
            else:
                # Last attempt failed
                process_id = context.get('process_id', 0) if context else 0
                quota_exhausted = process_id in process_quota_flags and process_quota_flags[process_id]
                
                # Check for truncation, 503, 429 errors
                is_truncation_error = (
                    "Response truncated - incomplete JSON" in error_str or 
                    "Max tokens" in error_str or
                    "truncated" in error_str.lower() or
                    "max_tokens" in error_str.lower()
                )
                is_503_error = (
                    "503" in error_str or 
                    "UNAVAILABLE" in error_str or 
                    "overloaded" in error_str.lower()
                )
                is_429_error = (
                    "429" in error_str or 
                    "RESOURCE_EXHAUSTED" in error_str or 
                    "resource_exhausted" in error_str.lower() or
                    "quota" in error_str.lower() and ("exceeded" in error_str.lower() or "limit" in error_str.lower())
                )
                
                if attempt == 7:
                    # Failed attempt 7 (8th overall) - save to truncated_2
                    if is_truncation_error and not is_503_error and not is_429_error and not quota_exhausted:
                        # Save failed 8th attempt to max_tokens_truncated_2 (only for truncation errors)
                        response_text = ""
                        if hasattr(e, 'response') and hasattr(e.response, 'text'):
                            response_text = e.response.text
                        elif 'response' in locals() and hasattr(response, 'text'):
                            response_text = response.text
                        
                        save_failed_attempt_8(response_text, error_str, filename, cao_number)
                        log_analysis_error(filename, f"All retry attempts failed (8th attempt with compact schema): {type(e).__name__}: {e} (saved to max_tokens_truncated_2)", response_text[:1000] if response_text else "")
                    else:
                        # Don't save 503 errors, 429 errors, quota exhaustion, or other non-truncation errors to max_tokens_truncated_2
                        if quota_exhausted or is_429_error:
                            log_analysis_error(filename, f"All retry attempts failed (8th attempt with compact schema) - QUOTA EXHAUSTED: {type(e).__name__}: {e}", "")
                        else:
                            log_analysis_error(filename, f"All retry attempts failed (8th attempt with compact schema): {type(e).__name__}: {e}", "")
                elif attempt == 9:
                    # Failed attempt 9 (10th overall) - save to truncated_3
                    if is_truncation_error and not is_503_error and not is_429_error and not quota_exhausted:
                        # Save failed 10th attempt to max_tokens_truncated_3 (only for truncation errors)
                        response_text = ""
                        if hasattr(e, 'response') and hasattr(e.response, 'text'):
                            response_text = e.response.text
                        elif 'response' in locals() and hasattr(response, 'text'):
                            response_text = response.text
                        
                        save_failed_attempt_10(response_text, error_str, filename, cao_number)
                        log_analysis_error(filename, f"All retry attempts failed (10th attempt with split extraction): {type(e).__name__}: {e} (saved to max_tokens_truncated_3)", response_text[:1000] if response_text else "")
                    else:
                        # Don't save 503 errors, 429 errors, quota exhaustion, or other non-truncation errors to max_tokens_truncated_3
                        if quota_exhausted or is_429_error:
                            log_analysis_error(filename, f"All retry attempts failed (10th attempt with split extraction) - QUOTA EXHAUSTED: {type(e).__name__}: {e}", "")
                        else:
                            log_analysis_error(filename, f"All retry attempts failed (10th attempt with split extraction): {type(e).__name__}: {e}", "")
                elif attempt == 11:
                    # Failed attempt 11 (12th overall) - save to truncated_4
                    if is_truncation_error and not is_503_error and not is_429_error and not quota_exhausted:
                        # Save failed 12th attempt to max_tokens_truncated_4 (only for truncation errors)
                        response_text = ""
                        if hasattr(e, 'response') and hasattr(e.response, 'text'):
                            response_text = e.response.text
                        elif 'response' in locals() and hasattr(response, 'text'):
                            response_text = response.text
                        
                        save_failed_attempt_11(response_text, error_str, filename, cao_number)
                        log_analysis_error(filename, f"All retry attempts failed (12th attempt with super compact schema): {type(e).__name__}: {e} (saved to max_tokens_truncated_4)", response_text[:1000] if response_text else "")
                    else:
                        # Don't save 503 errors, 429 errors, quota exhaustion, or other non-truncation errors to max_tokens_truncated_4
                        if quota_exhausted or is_429_error:
                            log_analysis_error(filename, f"All retry attempts failed (12th attempt with super compact schema) - QUOTA EXHAUSTED: {type(e).__name__}: {e}", "")
                        else:
                            log_analysis_error(filename, f"All retry attempts failed (12th attempt with super compact schema): {type(e).__name__}: {e}", "")
                
                print(f'  DEBUG: All attempts failed')
                break
        
        # Increment attempt index for next iteration (only if we didn't break/continue)
        attempt_index += 1
    
    # If we get here, all attempts failed
    if last_error:
        print(f'  ⚠️ All retry attempts failed, moving on to next part')
        if attempt == 7 or attempt == 9 or attempt == 11:
            # Already logged above
            pass
        else:
            log_analysis_error(filename, f"All retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed salary extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
            
            # Get final attempt parameters for logging
            # Determine which attempt was last
            if extended_to_super_compact or use_super_compact_from_start:
                # Last attempt was 10 (11th overall) with super compact schema
                final_params = get_adjusted_parameters_p4(4, MODEL)  # Use attempt 4 params (same as attempt 10)
                final_params['schema_type'] = 'super_compact'
                final_params['attempt'] = 11
            elif extended_to_split or use_split_extraction_from_start:
                # Last attempt was 8 (9th overall) with split schema
                final_params = get_adjusted_parameters_p4(4, MODEL)  # Use attempt 4 params (same as attempt 8)
                final_params['schema_type'] = 'split'
                final_params['attempt'] = 9
            elif extended_to_compact or use_compact_schema_from_start:
                # Last attempt was 6 (7th overall) with compact schema
                final_params = get_adjusted_parameters_p4(4, MODEL)  # Use attempt 4 params (same as attempt 6)
                final_params['schema_type'] = 'compact'
                final_params['attempt'] = 7
            else:
                # Last attempt was 4 (5th overall) with regular schema
                final_params = get_adjusted_parameters_p4(4, MODEL)
                final_params['schema_type'] = 'regular'
                final_params['attempt'] = 5
            
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance_p4(last_error_message)
                if final_guidance:
                    final_params['retry_guidance_used'] = final_error_type
            
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0,  # No processing time available for failures
                usage_metadata=None,
                success=False,
                analysis_type="salary",
                error_message=f"All retry attempts failed: {type(last_error).__name__}: {last_error}",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number or "",
                model="gemini-2.5-flash",
                parameters=final_params,
                allow_duplicates=False
            )
        
        return None




def extract_nonsalary_part1_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None, cao_number: str = None) -> dict:
    """Extract non-salary part 1 information (General, Bonuses, Wage Scales, Pension, Termination) from JSON using LLM."""
    global process_quota_flags
    
    print(f'  Part 1 extraction starting for {filename}')
    
    # Use full JSON input (no slicing)
    non_salary_text = json.dumps(json_obj, ensure_ascii=False, indent=2)
    
    if not non_salary_text.strip():
        print(f'  Part 1: No text found in input')
        return NonSalaryPart1().model_dump()
    
    if not check_token_limit(non_salary_text, filename):
        return NonSalaryPart1().model_dump()
    
    # Define sections for Part 1
    sections = "general_information, bonuses_info, wage_scales_info, pension_information, termination_information"
    base_prompt = NON_SALARY_PROMPT.format(filename=filename, source_json=non_salary_text, sections=sections)
    
    model_params = get_model_parameters()
    
    # Use proper safety settings format for newer google-genai API
    safety_settings = get_safety_settings()
    
    config = {
        "temperature": model_params["temperature"],
        "top_p": model_params["top_p"],
        "top_k": model_params["top_k"],
        "max_output_tokens": 65536,
        "candidate_count": model_params["candidate_count"],
        "seed": model_params["seed"],
        "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
        "response_mime_type": "application/json",
        "response_schema": NonSalaryPart1,
        "safety_settings": safety_settings
    }
    
    try:
        start_time = time.time()
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=base_prompt,
                config=config
            )
        except Exception as api_error:
            # Defensive exception handling for API calls that escape retry loops
            import traceback
            error_type = type(api_error).__name__
            error_msg = str(api_error)
            print(f'  🚨 UNEXPECTED API ERROR during part 1 extraction: {error_type}: {error_msg}')
            print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
            # Log to file for debugging
            try:
                error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                with open(error_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (part 1 extraction)\n")
                    f.write(f"File: {filename}\n")
                    f.write(f"Error Type: {error_type}\n")
                    f.write(f"Error Message: {error_msg}\n")
                    f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
            except Exception:
                pass  # Don't fail on logging failure
            # Re-raise so existing retry logic can handle it
            raise
        processing_time = time.time() - start_time
        
        # Check for truncation
        if check_response_truncation(response, filename, cao_number):
            raise Exception("Response truncated - incomplete JSON")
        
        # Check if response has parsed attribute (structured output)
        if hasattr(response, 'parsed') and response.parsed is not None:
            result = response.parsed.model_dump()
            
            # Validate schema
            expected_sections = {
                'general_information', 'bonuses_info', 'wage_scales_info', 
                'pension_information', 'termination_information'
            }
            if not validate_extraction_schema(result, expected_sections, "Part 1"):
                raise Exception("Schema validation failed")
            
            # Log successful part 1 extraction
            if context and 'performance_monitor' in context:
                file_size_mb = len(str(json_obj)) / (1024 * 1024)
                context['performance_monitor'].log_analysis(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=getattr(response, 'usage_metadata', None),
                    success=True,
                    analysis_type="non_salary_part1",
                    api_key_used=context.get('key_number', 1),
                    process_id=context.get('process_id', 0),
                    cao_number=cao_number,
                    model="gemini-2.5-flash",
                    parameters=model_params,
                    allow_duplicates=False
                )
            
            return result
        else:
            print(f'  Part 1: No structured output received from model')
            if hasattr(response, 'text') and response.text:
                # Try to parse the text manually
                try:
                    cleaned_text = response.text.strip()
                    
                    # Remove markdown code fences if present
                    if cleaned_text.startswith('```'):
                        lines = cleaned_text.split('\n')
                        cleaned_text = '\n'.join(lines[1:-1]).strip()
                    
                    parsed_json = json.loads(cleaned_text)
                    # Validate against schema
                    schema = NonSalaryPart1(**parsed_json)
                    result = schema.model_dump()
                    
                    # Validate schema
                    expected_sections = {
                        'general_information', 'bonuses_info', 'wage_scales_info', 
                        'pension_information', 'termination_information'
                    }
                    if validate_extraction_schema(result, expected_sections, "Part 1"):
                        print(f'  Part 1 extraction completed successfully (manual parse)')
                        return result
                    else:
                        raise Exception("Manual parse schema validation failed")
                        
                except json.JSONDecodeError as e:
                    print(f'  Part 1: Failed to parse response text as JSON: {e}')
                except Exception as e:
                    print(f'  Part 1: Failed to validate parsed JSON: {e}')
            
            log_analysis_error(filename, "No structured output received from model for non-salary part 1", "")
            return NonSalaryPart1().model_dump()
            
    except Exception as e:
        print(f'  Part 1: API call failed with error: {type(e).__name__}: {e}')
        last_error = e
        last_error_message = None
        
        # Retry logic with proper attempt tracking
        attempt = 0  # Current attempt number (for parameters)
        total_attempts = 0  # Total retry attempts (for max_retries limit)
        
        while total_attempts < model_params["max_retries"] + 1:
            # Check quota exhaustion at START of each retry attempt (before API call)
            process_id = context.get('process_id', 0) if context else 0
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  🛑 Quota exhausted detected at start of part 1 retry loop, stopping retries')
                break
            
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters_p4(attempt, MODEL)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance_p4(last_error_message)
                    if retry_guidance:
                        print(f'  Part 1: Adding retry guidance for: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = get_safety_settings()
                
                # Prepare API configuration
                config = {
                    "temperature": adjusted_params["temperature"],
                    "top_p": adjusted_params["top_p"],
                    "top_k": adjusted_params["top_k"],
                    "max_output_tokens": 65536,
                    "candidate_count": adjusted_params["candidate_count"],
                    "seed": adjusted_params["seed"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=adjusted_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": NonSalaryPart1,
                    "safety_settings": safety_settings
                }
                
                start_time = time.time()
                try:
                    response = client.models.generate_content(
                        model=MODEL,
                        contents=prompt,
                        config=config
                    )
                except Exception as api_error:
                    # Defensive exception handling for API calls that escape retry loops
                    import traceback
                    error_type = type(api_error).__name__
                    error_msg = str(api_error)
                    print(f'  🚨 UNEXPECTED API ERROR during part 1 extraction (attempt {attempt + 1}): {error_type}: {error_msg}')
                    print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
                    # Log to file for debugging
                    try:
                        error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                        Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                        with open(error_log_path, 'a', encoding='utf-8') as f:
                            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (part 1 extraction)\n")
                            f.write(f"File: {filename}\n")
                            f.write(f"Error Type: {error_type}\n")
                            f.write(f"Error Message: {error_msg}\n")
                            f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
                    except Exception:
                        pass  # Don't fail on logging failure
                    # Re-raise so existing retry logic can handle it
                    raise
                processing_time = time.time() - start_time
                
                # Check for truncation
                if check_response_truncation(response, filename, cao_number):
                    raise Exception("Response truncated - incomplete JSON")
                
                # Check if response has parsed attribute (structured output)
                if hasattr(response, 'parsed') and response.parsed is not None:
                    result = response.parsed.model_dump()
                    
                    # Validate schema
                    expected_sections = {
                        'general_information', 'bonuses_info', 'wage_scales_info', 
                        'pension_information', 'termination_information'
                    }
                    if not validate_extraction_schema(result, expected_sections, "Part 1"):
                        raise Exception("Schema validation failed")
                    
                    # Log successful part 1 extraction
                    if context and 'performance_monitor' in context:
                        file_size_mb = len(str(json_obj)) / (1024 * 1024)
                        
                        # Add retry guidance info to parameters for logging
                        log_params = adjusted_params.copy()
                        if retry_guidance:
                            log_params['retry_guidance_used'] = error_type
                        
                        context['performance_monitor'].log_analysis(
                            filename=filename,
                            file_size_mb=file_size_mb,
                            processing_time=processing_time,
                            usage_metadata=getattr(response, 'usage_metadata', None),
                            success=True,
                            analysis_type="non_salary_part1",
                            api_key_used=context.get('key_number', 1),
                            process_id=context.get('process_id', 0),
                            cao_number=cao_number,
                            model="gemini-2.5-flash",
                            parameters=log_params,
                            allow_duplicates=False
                        )
                    
                    print(f'  Part 1 extraction completed successfully (attempt {attempt + 1})')
                    return result
                    
            except Exception as e:
                last_error = e
                last_error_message = str(e)
                print(f'  Part 1: Attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt (before calling handle_llm_errors)
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  🛑 Quota exhausted during part 1 extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    file_size_mb = len(str(json_obj)) / (1024 * 1024)
                    should_retry, increment_attempt, wait_time = handle_llm_errors_p4(
                        e, attempt, 5, file_size_mb, context, process_quota_flags
                    )
                    # Check again after handle_llm_errors (it may have set the flag)
                    if process_id in process_quota_flags and process_quota_flags[process_id]:
                        print(f'  🛑 Quota exhausted detected by handle_llm_errors, stopping retries')
                        break
                    if should_retry:
                        # Wait before retrying
                        if wait_time > 0:
                            print(f'  INFO: Waiting {wait_time // 60 if wait_time >= 60 else wait_time} {"minutes" if wait_time >= 60 else "seconds"} before retry...')
                            time.sleep(wait_time)
                            if wait_time >= 60:
                                print(f'  INFO: Wait complete, continuing with retry...')
                        # Increment attempt counters
                        total_attempts += 1
                        if increment_attempt:
                            attempt += 1
                        # Continue to retry (with same attempt number if increment_attempt is False)
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  Part 1: All attempts failed')
                    break
            
            # If we get here, the attempt was successful, increment total_attempts
            total_attempts += 1
            break  # Exit loop on success
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All part 1 retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed part 1 extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters_p4(4, MODEL)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance_p4(last_error_message)
                if final_guidance:
                    final_params['retry_guidance_used'] = final_error_type
            
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0,
                usage_metadata=None,
                success=False,
                analysis_type="non_salary_part1",
                error_message=f"All retry attempts failed: {type(last_error).__name__}: {last_error}",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number or "",
                model="gemini-2.5-flash",
                parameters=final_params,
                allow_duplicates=False
            )
        
        print(f'  ⚠️ All {model_params["max_retries"]} retries failed for Part 1, moving on to next part')
        return None


def extract_nonsalary_part2_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None, cao_number: str = None) -> dict:
    """Extract non-salary part 2 information (Leave, Overtime, Training) from JSON using LLM."""
    global process_quota_flags
    
    print(f'  Part 2 extraction starting for {filename}')
    
    # Use full JSON input (no slicing)
    non_salary_text = json.dumps(json_obj, ensure_ascii=False, indent=2)
    
    if not non_salary_text.strip():
        print(f'  Part 2: No text found in input')
        return NonSalaryPart2().model_dump()
    
    if not check_token_limit(non_salary_text, filename):
        return NonSalaryPart2().model_dump()
    
    # Define sections for Part 2
    sections = "leave_information, overtime_information, training_information"
    base_prompt = NON_SALARY_PROMPT.format(filename=filename, source_json=non_salary_text, sections=sections)
    
    model_params = get_model_parameters()
    
    # Use proper safety settings format for newer google-genai API
    safety_settings = get_safety_settings()
    
    config = {
        "temperature": model_params["temperature"],
        "top_p": model_params["top_p"],
        "top_k": model_params["top_k"],
        "max_output_tokens": 65536,
        "candidate_count": model_params["candidate_count"],
        "seed": model_params["seed"],
        "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
        "response_mime_type": "application/json",
        "response_schema": NonSalaryPart2,
        "safety_settings": safety_settings
    }
    
    try:
        start_time = time.time()
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=base_prompt,
                config=config
            )
        except Exception as api_error:
            # Defensive exception handling for API calls that escape retry loops
            import traceback
            error_type = type(api_error).__name__
            error_msg = str(api_error)
            print(f'  🚨 UNEXPECTED API ERROR during part 2 extraction: {error_type}: {error_msg}')
            print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
            # Log to file for debugging
            try:
                error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                with open(error_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (part 2 extraction)\n")
                    f.write(f"File: {filename}\n")
                    f.write(f"Error Type: {error_type}\n")
                    f.write(f"Error Message: {error_msg}\n")
                    f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
            except Exception:
                pass  # Don't fail on logging failure
            # Re-raise so existing retry logic can handle it
            raise
        processing_time = time.time() - start_time
        
        # Log detailed response information
        log_api_response_details(response, f"{filename} (non-salary-part2)", processing_time)
        
        # Check for truncation
        if check_response_truncation(response, filename, cao_number):
            raise Exception("Response truncated - incomplete JSON")
        
        # Check if response has parsed attribute (structured output)
        if hasattr(response, 'parsed') and response.parsed is not None:
            result = response.parsed.model_dump()
            
            # Validate schema
            expected_sections = {
                'leave_information', 'overtime_information', 'training_information'
            }
            if not validate_extraction_schema(result, expected_sections, "Part 2"):
                raise Exception("Schema validation failed")
            
            # Log successful part 2 extraction
            if context and 'performance_monitor' in context:
                file_size_mb = len(str(json_obj)) / (1024 * 1024)
                context['performance_monitor'].log_analysis(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=getattr(response, 'usage_metadata', None),
                    success=True,
                    analysis_type="non_salary_part2",
                    api_key_used=context.get('key_number', 1),
                    process_id=context.get('process_id', 0),
                    cao_number=cao_number,
                    model="gemini-2.5-flash",
                    parameters=model_params,
                    allow_duplicates=False
                )
            
            return result
        else:
            print(f'  DEBUG: No parsed attribute in part 2 response')
            if hasattr(response, 'text'):
                print(f'  DEBUG: Part 2 response text length: {len(response.text) if response.text else 0}')
                if response.text:
                    print(f'  DEBUG: Part 2 response text preview: {response.text[:300]}...')
                    # Try to parse the text manually
                    try:
                        cleaned_text = response.text.strip()
                        
                        # Remove markdown code fences if present
                        if cleaned_text.startswith('```'):
                            lines = cleaned_text.split('\n')
                            cleaned_text = '\n'.join(lines[1:-1]).strip()
                        
                        parsed_json = json.loads(cleaned_text)
                        print(f'  DEBUG: Successfully parsed part 2 response text manually')
                        # Validate against schema
                        schema = NonSalaryPart2(**parsed_json)
                        result = schema.model_dump()
                        print(f'  DEBUG: Part 2 manual parse result keys: {list(result.keys())}')
                        return result
                    except json.JSONDecodeError as e:
                        print(f'  DEBUG: Failed to parse part 2 response text as JSON: {e}')
                    except Exception as e:
                        print(f'  DEBUG: Failed to validate parsed JSON against part 2 schema: {e}')
            log_analysis_error(filename, "No structured output received from model for non-salary part 2", "")
            return NonSalaryPart2().model_dump()
            
    except Exception as e:
        print(f'  DEBUG: Part 2 API call failed with error: {type(e).__name__}: {e}')
        last_error = e
        last_error_message = None
        
        # Retry logic with proper attempt tracking
        for attempt in range(model_params["max_retries"] + 1):
            # Check quota exhaustion at START of each retry attempt (before API call)
            process_id = context.get('process_id', 0) if context else 0
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  🛑 Quota exhausted detected at start of part 2 retry loop, stopping retries')
                break
            
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters_p4(attempt, MODEL)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance_p4(last_error_message)
                    if retry_guidance:
                        print(f'  INFO: Adding retry guidance for part 2: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = get_safety_settings()
                
                # Prepare API configuration
                config = {
                    "temperature": adjusted_params["temperature"],
                    "top_p": adjusted_params["top_p"],
                    "top_k": adjusted_params["top_k"],
                    "max_output_tokens": 65536,
                    "candidate_count": adjusted_params["candidate_count"],
                    "seed": adjusted_params["seed"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=adjusted_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": NonSalaryPart2,
                    "safety_settings": safety_settings
                }
                
                print(f'  DEBUG: Making part 2 API call...')
                
                start_time = time.time()
                try:
                    response = client.models.generate_content(
                        model=MODEL,
                        contents=prompt,
                        config=config
                    )
                except Exception as api_error:
                    # Defensive exception handling for API calls that escape retry loops
                    import traceback
                    error_type = type(api_error).__name__
                    error_msg = str(api_error)
                    print(f'  🚨 UNEXPECTED API ERROR during part 2 extraction (attempt {attempt + 1}): {error_type}: {error_msg}')
                    print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
                    # Log to file for debugging
                    try:
                        error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                        Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                        with open(error_log_path, 'a', encoding='utf-8') as f:
                            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (part 2 extraction)\n")
                            f.write(f"File: {filename}\n")
                            f.write(f"Error Type: {error_type}\n")
                            f.write(f"Error Message: {error_msg}\n")
                            f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
                    except Exception:
                        pass  # Don't fail on logging failure
                    # Re-raise so existing retry logic can handle it
                    raise
                processing_time = time.time() - start_time
                
                print(f'  DEBUG: Part 2 API response received')
                
                # Log detailed response information
                log_api_response_details(response, f"{filename} (non-salary-part2)", processing_time)
                
                # Check for truncation
                if check_response_truncation(response, filename, cao_number):
                    print(f'  DEBUG: Part 2 response appears to be truncated, will retry with different parameters')
                    raise Exception("Response truncated - incomplete JSON")
                
                # Check if response has parsed attribute (structured output)
                if hasattr(response, 'parsed') and response.parsed is not None:
                    print(f'  DEBUG: Part 2 response has parsed attribute')
                    result = response.parsed.model_dump()
                    print(f'  DEBUG: Part 2 parsed result keys: {list(result.keys())}')
                    
                    # Log successful part 2 extraction
                    if context and 'performance_monitor' in context:
                        file_size_mb = len(str(json_obj)) / (1024 * 1024)
                        
                        # Add retry guidance info to parameters for logging
                        log_params = adjusted_params.copy()
                        if retry_guidance:
                            log_params['retry_guidance_used'] = error_type
                        
                        context['performance_monitor'].log_analysis(
                            filename=filename,
                            file_size_mb=file_size_mb,
                            processing_time=processing_time,
                            usage_metadata=getattr(response, 'usage_metadata', None),
                            success=True,
                            analysis_type="non_salary_part2",
                            api_key_used=context.get('key_number', 1),
                            process_id=context.get('process_id', 0),
                            cao_number=cao_number,
                            model="gemini-2.5-flash",
                            parameters=log_params,
                            allow_duplicates=False
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e
                last_error_message = str(e)
                print(f'  DEBUG: Part 2 attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt (before calling handle_llm_errors)
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  🛑 Quota exhausted during part 2 extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    file_size_mb = len(str(json_obj)) / (1024 * 1024)
                    should_retry, increment_attempt, wait_time = handle_llm_errors_p4(
                        e, attempt, 5, file_size_mb, context, process_quota_flags
                    )
                    # Check again after handle_llm_errors (it may have set the flag)
                    if process_id in process_quota_flags and process_quota_flags[process_id]:
                        print(f'  🛑 Quota exhausted detected by handle_llm_errors, stopping retries')
                        break
                    if should_retry:
                        # Wait before retrying
                        if wait_time > 0:
                            print(f'  INFO: Waiting {wait_time // 60 if wait_time >= 60 else wait_time} {"minutes" if wait_time >= 60 else "seconds"} before retry...')
                            time.sleep(wait_time)
                            if wait_time >= 60:
                                print(f'  INFO: Wait complete, continuing with retry...')
                        # Increment attempt counters
                        total_attempts += 1
                        if increment_attempt:
                            attempt += 1
                        # Continue to retry (with same attempt number if increment_attempt is False)
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  DEBUG: All part 2 attempts failed')
                    break
            
            # If we get here, the attempt was successful, increment total_attempts
            total_attempts += 1
            break  # Exit loop on success
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All part 2 retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed part 2 extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters_p4(4, MODEL)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance_p4(last_error_message)
                if final_guidance:
                    final_params['retry_guidance_used'] = final_error_type
            
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0,
                usage_metadata=None,
                success=False,
                analysis_type="non_salary_part2",
                error_message=f"All retry attempts failed: {type(last_error).__name__}: {last_error}",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number or "",
                model="gemini-2.5-flash",
                parameters=final_params,
                allow_duplicates=False
            )
        
        print(f'  ⚠️ All {model_params["max_retries"]} retries failed for Part 2, moving on to next part')
        return None


def extract_nonsalary_part3_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None, cao_number: str = None) -> dict:
    """Extract non-salary part 3 information (Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits) from JSON using LLM."""
    global process_quota_flags
    
    print(f'  Part 3 extraction starting for {filename}')
    
    # Use full JSON input (no slicing)
    non_salary_text = json.dumps(json_obj, ensure_ascii=False, indent=2)
    
    if not non_salary_text.strip():
        print(f'  Part 3: No text found in input')
        return NonSalaryPart3().model_dump()
    
    if not check_token_limit(non_salary_text, filename):
        return NonSalaryPart3().model_dump()
    
    # Define sections for Part 3
    sections = "homeoffice_information, contract_type_information, safety_information, childcare_information, ai_information, fringe_benefits_information"
    base_prompt = NON_SALARY_PROMPT.format(filename=filename, source_json=non_salary_text, sections=sections)
    
    model_params = get_model_parameters()
    
    # Use proper safety settings format for newer google-genai API
    safety_settings = get_safety_settings()
    
    config = {
        "temperature": model_params["temperature"],
        "top_p": model_params["top_p"],
        "top_k": model_params["top_k"],
        "max_output_tokens": 65536,
        "candidate_count": model_params["candidate_count"],
        "seed": model_params["seed"],
        "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
        "response_mime_type": "application/json",
        "response_schema": NonSalaryPart3,
        "safety_settings": safety_settings
    }
    
    try:
        start_time = time.time()
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=base_prompt,
                config=config
            )
        except Exception as api_error:
            # Defensive exception handling for API calls that escape retry loops
            import traceback
            error_type = type(api_error).__name__
            error_msg = str(api_error)
            print(f'  🚨 UNEXPECTED API ERROR during part 3 extraction: {error_type}: {error_msg}')
            print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
            # Log to file for debugging
            try:
                error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                with open(error_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (part 3 extraction)\n")
                    f.write(f"File: {filename}\n")
                    f.write(f"Error Type: {error_type}\n")
                    f.write(f"Error Message: {error_msg}\n")
                    f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
            except Exception:
                pass  # Don't fail on logging failure
            # Re-raise so existing retry logic can handle it
            raise
        processing_time = time.time() - start_time
        
        # Log detailed response information
        log_api_response_details(response, f"{filename} (non-salary-part3)", processing_time)
        
        # Check for truncation
        if check_response_truncation(response, filename, cao_number):
            raise Exception("Response truncated - incomplete JSON")
        
        # Check if response has parsed attribute (structured output)
        if hasattr(response, 'parsed') and response.parsed is not None:
            result = response.parsed.model_dump()
            
            # Validate schema
            expected_sections = {
                'homeoffice_information', 'contract_type_information', 'safety_information',
                'childcare_information', 'ai_information', 'fringe_benefits_information'
            }
            if not validate_extraction_schema(result, expected_sections, "Part 3"):
                raise Exception("Schema validation failed")
            
            # Log successful part 3 extraction
            if context and 'performance_monitor' in context:
                file_size_mb = len(str(json_obj)) / (1024 * 1024)
                context['performance_monitor'].log_analysis(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=getattr(response, 'usage_metadata', None),
                    success=True,
                    analysis_type="non_salary_part3",
                    api_key_used=context.get('key_number', 1),
                    process_id=context.get('process_id', 0),
                    cao_number=cao_number,
                    model="gemini-2.5-flash",
                    parameters=model_params,
                    allow_duplicates=False
                )
            
            return result
        else:
            print(f'  Part 3: No structured output received from model')
            if hasattr(response, 'text') and response.text:
                # Try to parse the text manually
                try:
                    cleaned_text = response.text.strip()
                    
                    # Remove markdown code fences if present
                    if cleaned_text.startswith('```'):
                        lines = cleaned_text.split('\n')
                        cleaned_text = '\n'.join(lines[1:-1]).strip()
                    
                    parsed_json = json.loads(cleaned_text)
                    # Validate against schema
                    schema = NonSalaryPart3(**parsed_json)
                    result = schema.model_dump()
                    
                    # Validate schema
                    expected_sections = {
                        'homeoffice_information', 'contract_type_information', 'safety_information',
                        'childcare_information', 'ai_information', 'fringe_benefits_information'
                    }
                    if validate_extraction_schema(result, expected_sections, "Part 3"):
                        print(f'  Part 3 extraction completed successfully (manual parse)')
                        return result
                    else:
                        raise Exception("Manual parse schema validation failed")
                        
                except json.JSONDecodeError as e:
                    print(f'  Part 3: Failed to parse response text as JSON: {e}')
                except Exception as e:
                    print(f'  Part 3: Failed to validate parsed JSON: {e}')
            
            log_analysis_error(filename, "No structured output received from model for non-salary part 3", "")
            return NonSalaryPart3().model_dump()
            
    except Exception as e:
        print(f'  DEBUG: Part 3 API call failed with error: {type(e).__name__}: {e}')
        last_error = e
        last_error_message = None
        
        # Retry logic with proper attempt tracking
        for attempt in range(model_params["max_retries"] + 1):
            # Check quota exhaustion at START of each retry attempt (before API call)
            process_id = context.get('process_id', 0) if context else 0
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  🛑 Quota exhausted detected at start of part 3 retry loop, stopping retries')
                break
            
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters_p4(attempt, MODEL)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance_p4(last_error_message)
                    if retry_guidance:
                        print(f'  INFO: Adding retry guidance for part 3: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = get_safety_settings()
                
                # Prepare API configuration
                config = {
                    "temperature": adjusted_params["temperature"],
                    "top_p": adjusted_params["top_p"],
                    "top_k": adjusted_params["top_k"],
                    "max_output_tokens": 65536,
                    "candidate_count": adjusted_params["candidate_count"],
                    "seed": adjusted_params["seed"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=adjusted_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": NonSalaryPart3,
                    "safety_settings": safety_settings
                }
                
                print(f'  DEBUG: Making part 3 API call...')
                
                start_time = time.time()
                try:
                    response = client.models.generate_content(
                        model=MODEL,
                        contents=prompt,
                        config=config
                    )
                except Exception as api_error:
                    # Defensive exception handling for API calls that escape retry loops
                    import traceback
                    error_type = type(api_error).__name__
                    error_msg = str(api_error)
                    print(f'  🚨 UNEXPECTED API ERROR during part 3 extraction (attempt {attempt + 1}): {error_type}: {error_msg}')
                    print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
                    # Log to file for debugging
                    try:
                        error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                        Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                        with open(error_log_path, 'a', encoding='utf-8') as f:
                            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.get('process_id', 0) if context else 0} - API ERROR (part 3 extraction)\n")
                            f.write(f"File: {filename}\n")
                            f.write(f"Error Type: {error_type}\n")
                            f.write(f"Error Message: {error_msg}\n")
                            f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
                    except Exception:
                        pass  # Don't fail on logging failure
                    # Re-raise so existing retry logic can handle it
                    raise
                processing_time = time.time() - start_time
                
                print(f'  DEBUG: Part 3 API response received')
                
                # Log detailed response information
                log_api_response_details(response, f"{filename} (non-salary-part3)", processing_time)
                
                # Check for truncation
                if check_response_truncation(response, filename, cao_number):
                    print(f'  DEBUG: Part 3 response appears to be truncated, will retry with different parameters')
                    raise Exception("Response truncated - incomplete JSON")
                
                # Check if response has parsed attribute (structured output)
                if hasattr(response, 'parsed') and response.parsed is not None:
                    print(f'  DEBUG: Part 3 response has parsed attribute')
                    result = response.parsed.model_dump()
                    print(f'  DEBUG: Part 3 parsed result keys: {list(result.keys())}')
                    
                    # Log successful part 3 extraction
                    if context and 'performance_monitor' in context:
                        file_size_mb = len(str(json_obj)) / (1024 * 1024)
                        
                        # Add retry guidance info to parameters for logging
                        log_params = adjusted_params.copy()
                        if retry_guidance:
                            log_params['retry_guidance_used'] = error_type
                        
                        context['performance_monitor'].log_analysis(
                            filename=filename,
                            file_size_mb=file_size_mb,
                            processing_time=processing_time,
                            usage_metadata=getattr(response, 'usage_metadata', None),
                            success=True,
                            analysis_type="non_salary_part3",
                            api_key_used=context.get('key_number', 1),
                            process_id=context.get('process_id', 0),
                            cao_number=cao_number,
                            model="gemini-2.5-flash",
                            parameters=log_params,
                            allow_duplicates=False
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e
                last_error_message = str(e)
                print(f'  DEBUG: Part 3 attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt (before calling handle_llm_errors)
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  🛑 Quota exhausted during part 3 extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    file_size_mb = len(str(json_obj)) / (1024 * 1024)
                    should_retry, increment_attempt, wait_time = handle_llm_errors_p4(
                        e, attempt, 5, file_size_mb, context, process_quota_flags
                    )
                    # Check again after handle_llm_errors (it may have set the flag)
                    if process_id in process_quota_flags and process_quota_flags[process_id]:
                        print(f'  🛑 Quota exhausted detected by handle_llm_errors, stopping retries')
                        break
                    if should_retry:
                        # Wait before retrying
                        if wait_time > 0:
                            print(f'  INFO: Waiting {wait_time // 60 if wait_time >= 60 else wait_time} {"minutes" if wait_time >= 60 else "seconds"} before retry...')
                            time.sleep(wait_time)
                            if wait_time >= 60:
                                print(f'  INFO: Wait complete, continuing with retry...')
                        # Increment attempt counters
                        total_attempts += 1
                        if increment_attempt:
                            attempt += 1
                        # Continue to retry (with same attempt number if increment_attempt is False)
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  DEBUG: All part 3 attempts failed')
                    break
            
            # If we get here, the attempt was successful, increment total_attempts
            total_attempts += 1
            break  # Exit loop on success
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All part 3 retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed part 3 extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters_p4(4, MODEL)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance_p4(last_error_message)
                if final_guidance:
                    final_params['retry_guidance_used'] = final_error_type
            
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0,
                usage_metadata=None,
                success=False,
                analysis_type="non_salary_part3",
                error_message=f"All retry attempts failed: {type(last_error).__name__}: {last_error}",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number or "",
                model="gemini-2.5-flash",
                parameters=final_params,
                allow_duplicates=False
            )
        
        print(f'  ⚠️ All {model_params["max_retries"]} retries failed for Part 3, moving on to next part')
        return None


# =============================================================================
# FILE PROCESSING & MULTI-PROCESS SUPPORT
# =============================================================================
# Functions for file discovery, locking, and multi-process coordination

def acquire_file_lock(file_path: Path) -> bool:
    """
    Try to acquire a lock for processing a file.
    
    Args:
        file_path: Path to the file to lock
        
    Returns:
        bool: True if lock acquired, False if already locked
    """
    lock_file = file_path.with_suffix('.analysis_lock')
    try:
        with open(lock_file, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(f'Timestamp: {time.time()}\n')
        return True
    except (IOError, OSError):
        return False


def release_file_lock(file_path: Path):
    """Release the lock for a file."""
    lock_file = file_path.with_suffix('.analysis_lock')
    try:
        if lock_file.exists():
            lock_file.unlink(missing_ok=True)
    except Exception:
        pass  # Ignore errors (file might be deleted by another process)


def discover_json_files(input_folder: str) -> List[Tuple[Path, Path]]:
    """
    Discover all JSON files organized by CAO.
    
    Args:
        input_folder: Path to the input folder
        
    Returns:
        List[Tuple[Path, Path]]: List of (cao_folder, json_file) tuples
    """
    cao_folders = sorted([f for f in Path(input_folder).iterdir() 
                         if f.is_dir() and f.name.isdigit()], 
                        key=lambda f: int(f.name))
    
    all_files = []
    for cao_folder in cao_folders:
        json_files = sorted(cao_folder.glob('*.json'))
        for json_file in json_files:
            all_files.append((cao_folder, json_file))
    
    return all_files


def is_file_already_processed(filename: str, cao_number: str) -> bool:
    """Check if a file has already been processed by looking for existing LLM analysis files."""
    # Remove _extract from filename if present
    base_name = Path(filename).stem
    if base_name.endswith('_extract'):
        base_name = base_name[:-8]  # Remove '_extract'
    
    # Check for all 4 required files in the new split structure
    salary_file = Path('outputs/llm_analysis/salary') / cao_number / f"{base_name}_analysis.json"
    part1_file = Path('outputs/llm_analysis/non_salary/gen_bon_wag_pen_ter') / cao_number / f"{base_name}_analysis.json"
    part2_file = Path('outputs/llm_analysis/non_salary/lea_ove_tra') / cao_number / f"{base_name}_analysis.json"
    part3_file = Path('outputs/llm_analysis/non_salary/hom_con_saf_chi_ai_fri') / cao_number / f"{base_name}_analysis.json"
    
    # File is considered processed only if ALL 4 parts exist
    return (salary_file.exists() and 
            part1_file.exists() and 
            part2_file.exists() and 
            part3_file.exists())


# =============================================================================
# CAO INFO INTEGRATION
# =============================================================================
# Functions for loading and merging CAO metadata

def load_cao_info(cao_info_path: str) -> dict:
    """
    Load CAO information from CSV and create a mapping dictionary.
    
    Args:
        cao_info_path: Path to the CAO info CSV file
        
    Returns:
        dict: Mapping from composite key (pdf_name + cao_number) to CAO metadata
    """
    cao_info_df = pd.read_csv(cao_info_path, sep=';')
    cao_mapping = {}
    
    for _, row in cao_info_df.iterrows():
        pdf_name = row['pdf_name']
        cao_number = row['cao_number']
        composite_key = f'{pdf_name}_{cao_number}'
        cao_mapping[composite_key] = {
            'cao_number': cao_number,
            'id': row['id'],
            'ingangsdatum': row['ingangsdatum'],
            'expiratiedatum': row['expiratiedatum'],
            'datum_kennisgeving': row['datum_kennisgeving']
        }
    
    return cao_mapping


def normalize_lookup(s: str) -> str:
    """Normalize string for fuzzy matching."""
    return s.replace(' ', '').replace('-', '').replace('_', '').lower()


def find_cao_info(pdf_name: str, cao_number: int, cao_info_mapping: dict) -> Optional[dict]:
    """
    Find CAO info for a given PDF and CAO number.
    
    Args:
        pdf_name: Name of the PDF file
        cao_number: CAO number
        cao_info_mapping: Mapping dictionary from load_cao_info
        
    Returns:
        Optional[dict]: CAO info if found, None otherwise
    """
    # Try exact match first
    composite_key = f'{pdf_name}_{cao_number}'
    if composite_key in cao_info_mapping:
        return cao_info_mapping[composite_key]
    
    # Try fuzzy match
    normalized_pdf = normalize_lookup(pdf_name)
    for key in cao_info_mapping.keys():
        key_pdf_name = key.rsplit('_', 1)[0]
        if normalize_lookup(key_pdf_name) == normalized_pdf:
            cao_info = cao_info_mapping[key]
            if cao_info['cao_number'] == cao_number:
                return cao_info
    
    return None


# =============================================================================
# EXCEL OUTPUT & DATA MERGING
# =============================================================================
# Functions for creating Excel output and merging extracted data

def save_extraction_json(data: dict, filename: str, extraction_type: str, cao_number: str = None) -> bool:
    """
    Save extracted JSON data to appropriate folders for analysis.
    
    Args:
        data: Extracted data to save
        filename: Original filename
        extraction_type: 'salary', 'non_salary_part1', 'non_salary_part2', or 'non_salary_part3'
        cao_number: CAO number for folder organization
        
    Returns:
        bool: True if saved successfully, False if validation failed or save failed
    """
    try:
        # For salary extraction, check if salary_information is empty
        # Only allow empty arrays if it's a legitimate "no salary data" case
        # Error cases (token limit, no structured output) now return None, so they won't reach here
        if extraction_type == 'salary':
            if isinstance(data, dict) and 'salary_information' in data:
                if isinstance(data['salary_information'], list) and len(data['salary_information']) == 0:
                    # Empty array - this is allowed for legitimate "no salary data" cases
                    # Error cases now return None, so any empty array reaching here is legitimate
                    pass  # Allow empty arrays for salary (legitimate "no salary data" case)
            elif isinstance(data, list) and len(data) == 0:
                # Old format - empty list, don't save (should be dict format)
                print(f'  ❌ Skipping save: Empty list format not allowed (should be dict with salary_information key)')
                return False
            elif data is None:
                # None means error case - don't save
                print(f'  ❌ Skipping save: None data indicates error case')
                return False
        
        # Validate data before saving
        if not validate_extraction_data(data, extraction_type):
            print(f'  ❌ Validation failed for {extraction_type}: missing required fields')
            print(f'  Expected: {get_required_fields_for_extraction_type(extraction_type)}')
            print(f'  Found: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}')
            return False
        
        # Create base path
        base_path = Path('outputs/llm_analysis')
        if extraction_type == 'salary':
            save_path = base_path / 'salary'
        elif extraction_type == 'non_salary_part1':
            save_path = base_path / 'non_salary' / 'gen_bon_wag_pen_ter'
        elif extraction_type == 'non_salary_part2':
            save_path = base_path / 'non_salary' / 'lea_ove_tra'
        elif extraction_type == 'non_salary_part3':
            save_path = base_path / 'non_salary' / 'hom_con_saf_chi_ai_fri'
        else:
            save_path = base_path / 'non_salary'
        
        # Create CAO-specific subfolder if cao_number provided
        if cao_number:
            save_path = save_path / str(cao_number)
        
        save_path.mkdir(parents=True, exist_ok=True)
        
        # Create filename - remove _extract if present and use _analysis suffix
        base_name = Path(filename).stem
        if base_name.endswith('_extract'):
            base_name = base_name[:-8]  # Remove '_extract'
        json_filename = f"{base_name}_analysis.json"
        file_path = save_path / json_filename
        
        # Save JSON data
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return True
        
    except Exception as e:
        print(f'  DEBUG: Failed to save {extraction_type} extraction: {e}')
        return False


def get_required_fields_for_extraction_type(extraction_type: str) -> List[str]:
    """Dynamically get required fields from Pydantic schemas."""
    from schema.salary_schema import SalaryExtractionSchema
    from schema.non_salary_schema import NonSalaryPart1, NonSalaryPart2, NonSalaryPart3
    
    schema_map = {
        'salary': SalaryExtractionSchema,
        'non_salary_part1': NonSalaryPart1,
        'non_salary_part2': NonSalaryPart2,
        'non_salary_part3': NonSalaryPart3
    }
    
    schema = schema_map.get(extraction_type)
    if not schema:
        return []
    
    return list(schema.model_fields.keys())


def validate_extraction_data(data: dict, extraction_type: str) -> bool:
    """Validate that extraction data contains required fields."""
    if not data or not isinstance(data, dict):
        return False
    
    required_fields = get_required_fields_for_extraction_type(extraction_type)
    return all(field in data for field in required_fields)


def check_missing_extraction_parts(filename: str, cao_number: str, skip_truncated_salary: bool = False) -> dict:
    """
    Check which extraction parts are missing for a given file.
    
    Args:
        filename: The filename to check
        cao_number: CAO number
        skip_truncated_salary: If True, mark salary as not missing if file is in truncated folder
    
    Returns:
        dict with keys: 'salary', 'part1', 'part2', 'part3' 
        and boolean values indicating if that part is missing
    """
    base_dir = Path("outputs/llm_analysis")
    
    missing = {
        'salary': True,
        'part1': True, 
        'part2': True,
        'part3': True
    }
    
    # Check each output file (using correct folder structure from save_extraction_json)
    salary_file = base_dir / "salary" / cao_number / filename.replace("_extract.json", "_analysis.json")
    part1_file = base_dir / "non_salary" / "gen_bon_wag_pen_ter" / cao_number / filename.replace("_extract.json", "_analysis.json")
    part2_file = base_dir / "non_salary" / "lea_ove_tra" / cao_number / filename.replace("_extract.json", "_analysis.json")
    part3_file = base_dir / "non_salary" / "hom_con_saf_chi_ai_fri" / cao_number / filename.replace("_extract.json", "_analysis.json")
    
    missing['salary'] = not salary_file.exists()
    missing['part1'] = not part1_file.exists()
    missing['part2'] = not part2_file.exists()
    missing['part3'] = not part3_file.exists()
    
    # If skip_truncated_salary is enabled:
    # - Files in truncated_4 folder: skip (mark as not missing) - these failed all attempts including super compact schema
    # - Files in truncated_3 folder: do NOT skip - they will be retried with attempts 10-11 (super compact schema)
    # - Files in truncated_2 folder: do NOT skip - they will be retried with attempts 9-10 (split extraction)
    # - Files in truncated folder: do NOT skip - they will be retried with attempts 6-8 (compact schema)
    if skip_truncated_salary and missing['salary']:
        if is_file_in_truncated_4_folder(filename, cao_number):
            missing['salary'] = False
            print(f'  {cao_number}: Salary extraction skipped (file in max_tokens_truncated_4 folder - all attempts exhausted)')
        # Note: Files in truncated_3 will be retried with super compact schema (attempts 10-11)
        # Note: Files in truncated_2 will be retried with split extraction (attempts 9-10)
        # Note: Files in truncated will be retried with compact schema (attempts 6-8)
    
    return missing


def process_single_file(json_file: Path, cao_folder: Path, client, cao_info_mapping: dict, 
                       config: AnalysisConfig, context: Dict[str, Any]) -> bool:
    """Process a single JSON file and save LLM extraction results."""
    global process_quota_flags
    filename = json_file.name
    cao_number = cao_folder.name
    
    print(f'  {cao_number}: {filename}')
    
    try:
        # Read JSON file
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Check which parts are missing
        missing_parts = check_missing_extraction_parts(filename, cao_number, SKIP_TRUNCATED_SALARY_FILES)

        # If all parts exist, skip entirely
        if not any(missing_parts.values()):
            print(f'  {cao_number}: Skipping {filename} (all parts already processed)')
            return True
            
        # Print which parts need processing
        parts_to_process = [k for k, v in missing_parts.items() if v]
        print(f'  {cao_number}: Processing missing parts: {", ".join(parts_to_process)}')

        # Check if quota was exhausted for this process
        process_id = context.get('process_id', 0) if context else 0
        if process_id in process_quota_flags and process_quota_flags[process_id]:
            print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
            return False  # Stop processing this process only

        # Extract salary information (only if missing)
        if missing_parts['salary']:
            # Check quota before starting
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            salary_extracted = extract_salary_from_json(json_data, filename, client, context, cao_number)
            # Check quota after extraction
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            salary_success = salary_extracted is not None
            if salary_success:
                print(f'  {cao_number}: Salary extraction completed')
            else:
                print(f'  {cao_number}: ⚠️ Salary extraction failed, moving to next part')
        else:
            salary_extracted = None
            salary_success = None  # Not run
            print(f'  {cao_number}: Salary extraction skipped (already exists)')

        # Extract non-salary part 1 (only if missing)
        if missing_parts['part1']:
            # Check quota before starting
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            part1_extracted = extract_nonsalary_part1_from_json(json_data, filename, client, context, cao_number)
            # Check quota after extraction
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            part1_success = part1_extracted is not None
            if part1_success:
                print(f'  {cao_number}: Non-salary part 1 extraction completed')
            else:
                print(f'  {cao_number}: ⚠️ Non-salary part 1 extraction failed, moving to next part')
        else:
            part1_extracted = None
            part1_success = None
            print(f'  {cao_number}: Non-salary part 1 extraction skipped (already exists)')

        # Extract non-salary part 2 (only if missing)
        if missing_parts['part2']:
            # Check quota before starting
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            part2_extracted = extract_nonsalary_part2_from_json(json_data, filename, client, context, cao_number)
            # Check quota after extraction
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            part2_success = part2_extracted is not None
            if part2_success:
                print(f'  {cao_number}: Non-salary part 2 extraction completed')
            else:
                print(f'  {cao_number}: ⚠️ Non-salary part 2 extraction failed, moving to next part')
        else:
            part2_extracted = None
            part2_success = None
            print(f'  {cao_number}: Non-salary part 2 extraction skipped (already exists)')

        # Extract non-salary part 3 (only if missing)
        if missing_parts['part3']:
            # Check quota before starting
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            part3_extracted = extract_nonsalary_part3_from_json(json_data, filename, client, context, cao_number)
            # Check quota after extraction
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
                return False
            part3_success = part3_extracted is not None
            if part3_success:
                print(f'  {cao_number}: Non-salary part 3 extraction completed')
            else:
                print(f'  {cao_number}: ⚠️ Non-salary part 3 extraction failed, moving to next part')
        else:
            part3_extracted = None
            part3_success = None
            print(f'  {cao_number}: Non-salary part 3 extraction skipped (already exists)')
        
        # Save each part separately (only if extraction was attempted and successful)
        if salary_success is True:
            save_success = save_extraction_json(salary_extracted, filename, 'salary', cao_number)
            if not save_success:
                print(f'  {cao_number}: ⚠️ Salary file validation failed, not saved')
        elif salary_success is False:
            print(f'  {cao_number}: Skipping salary file save due to extraction failure')

        if part1_success is True:
            save_success = save_extraction_json(part1_extracted, filename, 'non_salary_part1', cao_number)
            if not save_success:
                print(f'  {cao_number}: ⚠️ Part 1 file validation failed, not saved')
        elif part1_success is False:
            print(f'  {cao_number}: Skipping part 1 file save due to extraction failure')

        if part2_success is True:
            save_success = save_extraction_json(part2_extracted, filename, 'non_salary_part2', cao_number)
            if not save_success:
                print(f'  {cao_number}: ⚠️ Part 2 file validation failed, not saved')
        elif part2_success is False:
            print(f'  {cao_number}: Skipping part 2 file save due to extraction failure')

        if part3_success is True:
            save_success = save_extraction_json(part3_extracted, filename, 'non_salary_part3', cao_number)
            if not save_success:
                print(f'  {cao_number}: ⚠️ Part 3 file validation failed, not saved')
        elif part3_success is False:
            print(f'  {cao_number}: Skipping part 3 file save due to extraction failure')
        
        # Log to analysis_performance.jsonl only when ALL 4 parts are complete
        # (either newly extracted successfully OR already existed)
        all_parts_complete = all([
            salary_success is not False,  # True (succeeded) or None (skipped/already exists)
            part1_success is not False,
            part2_success is not False,
            part3_success is not False
        ])

        # Only log if at least one part was newly extracted successfully
        any_newly_extracted = any([
            salary_success is True,
            part1_success is True,
            part2_success is True,
            part3_success is True
        ])

        if all_parts_complete and any_newly_extracted and context and 'performance_monitor' in context:
            # Log combined success
            file_size_mb = len(str(json_data)) / (1024 * 1024)
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0,  # Will be calculated from individual parts
                usage_metadata=None,  # Will be aggregated from individual parts
                success=True,
                analysis_type="combined",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number=cao_number,
                model="gemini-2.5-flash",
                allow_duplicates=False
            )
            print(f'  {cao_number}: Combined analysis logged successfully')
        else:
            failed_parts = []
            if salary_success is False:
                failed_parts.append("salary")
            if part1_success is False:
                failed_parts.append("part1")
            if part2_success is False:
                failed_parts.append("part2")
            if part3_success is False:
                failed_parts.append("part3")
            if failed_parts:
                print(f'  {cao_number}: Combined analysis not logged - failed parts: {failed_parts}')
            else:
                print(f'  {cao_number}: Combined analysis not logged - no new extractions performed')

        # Return success if all parts that were attempted succeeded or already existed
        print(f'  {cao_number}: Successfully processed {filename}')
        return all_parts_complete
        
    except Exception as e:
        print(f'  {cao_number}: Error processing {filename}: {e}')
        log_analysis_error(filename, f"Processing error: {e}", "")
        return False


# =============================================================================
# MAIN EXECUTION FUNCTIONS
# =============================================================================
# Functions for orchestrating the complete analysis pipeline

def main():
    """Main entry point for the analysis pipeline."""
    global process_quota_flags
    process_id = None  # Initialize for exception handling
    current_file = None  # Track current file being processed
    successful_analyses = 0
    failed_files = []
    total_files = 0
    
    try:
        parser = argparse.ArgumentParser(description='CAO Data Analysis with Schema-Driven Extraction')
        parser.add_argument('--key_number', type=int, default=7, help='API key number to use')
        parser.add_argument('--process_id', type=int, default=0, help='Process ID for work distribution')
        parser.add_argument('--total_processes', type=int, default=1, help='Total number of parallel processes')
        parser.add_argument('--max_files', type=int, help='Maximum number of files to process')
        
        args = parser.parse_args()
        process_id = args.process_id
        # Load configuration
        config = load_configuration()
        
        # Validate paths (pass process_id to avoid race conditions in parallel execution)
        validate_input_paths(config, args.process_id)
        
        # Setup processing context
        context = setup_processing_context(config, args.process_id, args.total_processes, args.key_number)
        
        # Load CAO info
        cao_info_mapping = load_cao_info(config.cao_info_path)
        
        # Discover files
        all_files = discover_json_files(config.input_folder)
        
        # Filter files for this process
        process_files = [f for i, f in enumerate(all_files) if i % args.total_processes == args.process_id]
        
        # Apply file limit from config or command line
        max_files = args.max_files if args.max_files is not None else config.max_json_files
        if max_files:
            process_files = process_files[:max_files]
        
        total_files = len(process_files)
        print(f'Process {args.process_id + 1}: Processing {total_files} files')
        
        # Process files
        successful_analyses = 0
        failed_files = []
        
        for cao_folder, json_file in process_files:
            current_file = f"{cao_folder.name}/{json_file.name}"  # Track current file
            # Check for quota exhaustion flag before processing each file
            if args.process_id in process_quota_flags and process_quota_flags[args.process_id]:
                print(f'🚨 QUOTA EXHAUSTED - Process {args.process_id + 1} (API key {args.key_number}) shutting down gracefully')
                print(f'📊 Partial results: {successful_analyses} successful, {len(failed_files)} failed before shutdown')
                print(f'🧹 Cleaning up and exiting...')
                break
            
            if not acquire_file_lock(json_file):
                print(f'  Skipping {json_file.name} (being processed by another process)')
                time.sleep(2)
                continue
            
            try:
                success = process_single_file(json_file, cao_folder, context['client'], 
                                            cao_info_mapping, config, context)
                if success:
                    successful_analyses += 1
                else:
                    # Check if this was due to quota exhaustion
                    if args.process_id in process_quota_flags and process_quota_flags[args.process_id]:
                        print(f'🚨 QUOTA EXHAUSTED during processing - Process {args.process_id + 1} shutting down gracefully')
                        break
                    else:
                        failed_files.append(json_file.name)
            finally:
                release_file_lock(json_file)
        
        # Final summary with quota exhaustion indication
        if args.process_id in process_quota_flags and process_quota_flags[args.process_id]:
            print(f'Process {args.process_id + 1} completed with QUOTA EXHAUSTION: {successful_analyses} successful, {len(failed_files)} failed')
        else:
            print(f'Process {args.process_id + 1} completed: {successful_analyses} successful, {len(failed_files)} failed')
        
    except KeyboardInterrupt:
        process_id_str = f"Process {process_id + 1}" if process_id is not None else "Process ?"
        print(f'\n⚠️  {process_id_str} interrupted by user')
        if current_file:
            print(f'   📄 Was processing: {current_file}')
        if total_files > 0:
            print(f'   📊 Progress: {successful_analyses}/{total_files} successful, {len(failed_files)} failed')
        sys.exit(0)
    except Exception as e:
        import traceback
        process_id_str = f"Process {process_id + 1}" if process_id is not None else "Process ?"
        error_str = str(e).lower()
        
        # Check if this is a quota exhaustion error - if so, exit gracefully
        if process_id is not None and process_id in process_quota_flags and process_quota_flags[process_id]:
            print(f'\n🚨 {process_id_str} completed with QUOTA EXHAUSTION due to error: {e}')
            if current_file:
                print(f'   📄 Was processing: {current_file}')
            if total_files > 0:
                print(f'   📊 Progress: {successful_analyses}/{total_files} successful, {len(failed_files)} failed')
            sys.exit(0)
        
        # Check if it's a known retryable error that should NOT stop the process
        # These errors are normally handled by retry logic, but if they escape, we should continue
        is_retryable_error = (
            ('429' in error_str and 'perday' not in error_str and 'daily' not in error_str and '3000000' not in error_str) or  # Per-minute quota (not daily)
            ('503' in error_str or 'unavailable' in error_str or 'overloaded' in error_str) or  # Service unavailable
            ('timeout' in error_str or 'deadline' in error_str) or  # Timeout
            ('truncated' in error_str or 'incomplete json' in error_str or 'json validation failed' in error_str) or  # JSON issues
            ('no content parts found' in error_str or 'no content' in error_str)  # Empty response
        )
        
        # Check if it's a daily quota (should stop process)
        is_daily_quota = (
            ('429' in error_str or 'quota' in error_str) and 
            ('perday' in error_str or 'daily' in error_str or '3000000' in error_str)
        )
        
        # Check if it's a fatal error (configuration, file system, etc.)
        is_fatal_error = (
            'valueerror' in error_str and ('input folder' in error_str or 'output folder' in error_str or 'api key' in error_str) or
            'filenotfounderror' in error_str or
            'permissionerror' in error_str
        )
        
        if is_retryable_error and not is_daily_quota:
            # This is a retryable error that occurred during setup/configuration (not file processing)
            # Since it's during setup, we can't continue - exit gracefully
            print(f'\n⚠️  RETRYABLE ERROR during setup in {process_id_str}')
            print(f'📋 Error Type: {type(e).__name__}')
            print(f'📋 Error Message: {str(e)[:200]}...' if len(str(e)) > 200 else f'📋 Error Message: {e}')
            
            print(f'\n💡 This is a retryable error (API quota/timeout/service unavailable) that occurred during setup.')
            print(f'   The process will exit, but you can restart it to continue processing.')
            print(f'   Other parallel processes will continue running.')
            
            # Log the error as retryable
            try:
                error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
                Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                with open(error_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {process_id_str} - RETRYABLE ERROR (during setup)\n")
                    f.write(f"Error Type: {type(e).__name__}\n")
                    f.write(f"Error Message: {e}\n")
                    f.write(f"Note: This is a retryable error - process can be restarted\n\n")
            except Exception:
                pass
            
            # Exit gracefully with code 0 (not fatal)
            sys.exit(0)
        
        # For daily quota or fatal errors, exit gracefully
        print(f'\n{"="*80}')
        print(f'❌ FATAL ERROR in {process_id_str}')
        print(f'{"="*80}')
        print(f'📋 Error Type: {type(e).__name__}')
        print(f'📋 Error Message: {e}')
        
        if current_file:
            print(f'\n📄 File being processed when error occurred: {current_file}')
        
        if total_files > 0:
            print(f'\n📊 Progress Summary:')
            print(f'   ✅ Successful: {successful_analyses}/{total_files} files')
            print(f'   ❌ Failed: {len(failed_files)} files')
            if failed_files:
                print(f'   📝 Failed files: {failed_files[-5:]}')  # Show last 5 failed files
            remaining = total_files - successful_analyses - len(failed_files)
            if remaining > 0:
                print(f'   ⏸️  Remaining: {remaining} files not processed')
        
        # Provide context about error type
        if is_daily_quota:
            print(f'\n💡 Error Type: Daily API Quota Limit Reached')
            print(f'   This process has hit its daily quota limit and will stop.')
            print(f'   Other parallel processes will continue running.')
            print(f'   You can restart this process tomorrow when the quota resets.')
        elif is_fatal_error:
            print(f'\n💡 Error Type: Fatal Configuration/System Error')
            print(f'   This is a system-level error that prevents processing.')
            print(f'   Check configuration, file permissions, or API key setup.')
        else:
            print(f'\n💡 This appears to be an unexpected fatal error. Check the traceback below for details.')
        
        print(f'\n📋 Full Traceback:')
        print(f'{"-"*80}')
        traceback.print_exc()
        print(f'{"-"*80}')
        
        # Try to log the error
        try:
            error_log_path = 'outputs/logs/fatal_errors_llm_analysis.txt'
            Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(error_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{'='*80}\n")
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {process_id_str} - FATAL ERROR\n")
                f.write(f"{'='*80}\n")
                f.write(f"Error Type: {type(e).__name__}\n")
                f.write(f"Error Message: {e}\n")
                if current_file:
                    f.write(f"File being processed: {current_file}\n")
                if total_files > 0:
                    f.write(f"Progress: {successful_analyses}/{total_files} successful, {len(failed_files)} failed\n")
                f.write(f"\nTraceback:\n")
                f.write(f"{traceback.format_exc()}\n\n")
        except Exception:
            pass  # Don't fail on logging failure
        
        # Exit with code 0 instead of 1 to prevent all processes from stopping
        # The error is logged, so we can investigate without crashing the entire pipeline
        print(f'\n⚠️  {process_id_str} exiting gracefully (error logged to outputs/logs/fatal_errors_llm_analysis.txt)')
        print(f'💡 Other parallel processes will continue running independently.')
        sys.exit(0)


if __name__ == "__main__":
    # Setup signal handlers first to prevent unexpected exits
    setup_signal_handlers()
    
    # Ensure stdout/stderr are line-buffered when piped (not a TTY)
    # This helps prevent issues when writing to tee/unbuffer
    if not sys.stdout.isatty():
        # When piped (e.g., to tee), set line buffering for better behavior
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            # Python < 3.7 or reconfigure not available - use flush() calls instead
            pass
        try:
            sys.stderr.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            pass
    
    main()
