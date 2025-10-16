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

    Multi-process:
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 6 2>&1 | tee p4_log1.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 2 --process_id 1 --total_processes 6 2>&1 | tee p4_log2.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 3 --process_id 2 --total_processes 6 2>&1 | tee p4_log3.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 4 --process_id 3 --total_processes 6 2>&1 | tee p4_log4.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 5 --process_id 4 --total_processes 6 2>&1 | tee p4_log5.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 6 --process_id 5 --total_processes 6 2>&1 | tee p4_log6.txt &

    With file limit:
        python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 1 --max_files 10

ARGUMENTS:
    --key_number: Which API key to use (7, 8 for testing) - defaults to 7
    --process_id: Process ID for work distribution (0-based) - defaults to 0
    --total_processes: Total number of parallel processes - defaults to 1
    --max_files: Maximum number of files to process (optional)

ENVIRONMENT VARIABLES:
    GOOGLE_API_KEY7, GOOGLE_API_KEY8: Google Gemini API keys for testing

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
from enum import Enum
from pathlib import Path
from types import NoneType
from typing import List, Optional, Tuple, Dict, Any, Literal
from dataclasses import dataclass, field

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Third-party imports for environment variables, file locking, and data validation
import pandas as pd
import yaml
from pydantic import BaseModel, Field, ConfigDict, conint, constr
from dotenv import load_dotenv

# Google Gemini API imports
from google import genai
from google.genai import types

# Import performance monitoring
from monitoring.monitoring_3_1 import PerformanceMonitor

# =============================================================================
# GLOBAL FLAGS
# =============================================================================
# Process-specific quota flags to stop individual processes when quota is hit
process_quota_flags = {}

# =============================================================================
# LLM CLIENT FUNCTIONS
# =============================================================================
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


def get_model_parameters() -> dict:
    """
    Get model parameters for LLM calls.
    
    Returns:
        dict: Model parameters
    """
    return {
    "model": MODEL,
    "temperature": 0.0,
    "top_p": 0.1,
    "top_k": 1,
    "max_tokens": 65536,
    "candidate_count": 1,
    "seed": 42,
    "thinking_budget": -1,  # Dynamic thinking (like p3)
    "max_retries": 0
    }


def get_adjusted_parameters(attempt: int) -> dict:
    """
    Get adjusted model parameters based on retry attempt.
    
    - Attempt 0 (1st try): original parameters
    - Attempt 1 (2nd try): original parameters  
    - Attempt 2 (3rd try): temperature +0.1, top_p +0.1, top_k -10%
    - Attempt 3 (4th try): temperature +0.2, top_p +0.2, top_k -20%
    - Attempt 4+ (5th+ try): temperature +0.3, top_p +0.3, top_k -30%
    
    Args:
        attempt: Current retry attempt number (0-based)
        
    Returns:
        dict: Model parameters with attempt-based adjustments
    """
    # Get base parameters
    base_params = get_model_parameters()
    
    # Calculate adjustment based on attempt (0.1 steps starting from attempt 2)
    if attempt <= 1:
        # First 2 attempts: use original parameters
        adjustment = 0.0
    else:
        # Starting from 3rd attempt: +0.1 per step
        adjustment = 0.1 * (attempt - 1)
    
    # Calculate adjusted values
    adjusted_temp = base_params["temperature"] + adjustment
    adjusted_top_p = min(1.0, base_params["top_p"] + adjustment)  # Cap at 1.0
    adjusted_top_k = max(1, int(base_params["top_k"] - adjustment * base_params["top_k"]))  # Reduce by percentage, min 1
    
    return {
        "model": base_params["model"],
        "temperature": adjusted_temp,
        "top_p": adjusted_top_p,
        "top_k": adjusted_top_k,
        "max_tokens": base_params["max_tokens"],
        "candidate_count": base_params["candidate_count"],
        "seed": base_params["seed"],
        "thinking_budget": base_params["thinking_budget"],
        "max_retries": base_params["max_retries"]
    }


def get_retry_guidance(error_message: str) -> tuple[str, str]:
    """
    Get retry guidance based on previous failure for LLM-controllable errors.
    
    Only provides guidance for the 2 most common LLM-controllable errors:
    1. Truncated JSON (incomplete response)
    2. Empty/no text response
    
    For all other errors (timeouts, 504, etc.), returns empty string.
    
    Args:
        error_message: The error message from the previous attempt
        
    Returns:
        tuple: (guidance_text, error_type) where error_type is "" if no guidance
    """
    if not error_message:
        return "", ""
    
    error_lower = error_message.lower()
    
    # Check for truncated JSON error
    if "does not end with }" in error_lower or "truncated" in error_lower:
        guidance = """
    PREVIOUS ATTEMPT FAILED: Response was TRUNCATED (incomplete JSON).
    CRITICAL: Ensure your response ENDS with the closing }  
        - Be more CONCISE in narrative descriptions while keeping all important data intact
        - Prioritize completing the JSON structure over verbose explanations
        - Keep all numbers, dates, tables - compress only explanatory text
    """
        return guidance, "truncated JSON"
    
    # Check for empty response error
    if "no text parts" in error_lower or "no content" in error_lower:
        guidance = """
    PREVIOUS ATTEMPT FAILED: No valid output was generated.
    CRITICAL: Output ONLY the JSON object
        - No markdown code fences (no ```json)
        - Include ALL required fields (use empty [] if no data)
        - Ensure final JSON output is generated, not just thinking tokens
    """
        return guidance, "empty response"
    
    # For all other errors, return empty string (no guidance)
    return "", ""


def calculate_quota_retry_delay(file_size_mb: float, attempt: int) -> int:
    """
    Calculate quota retry delay based on file size and attempt number.
    
    Formula: (estimated_tokens / 125000) * 60 seconds * (2^attempt) + buffer
    - 125,000 tokens per minute limit
    - Exponential backoff: 2^attempt
    - Buffer time for safety
    
    Args:
        file_size_mb: Size of file in MB
        attempt: Current attempt number (0-based)
        
    Returns:
        int: Delay in seconds
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


def handle_llm_errors(error: Exception, attempt: int, max_retries: int, 
                     file_size_mb: float = 0, context: Optional[str] = None) -> bool:
    """
    Handle different types of LLM errors with appropriate retry logic.
    
    Args:
        error: The exception that occurred
        attempt: Current attempt number (0-based)
        max_retries: Maximum number of retry attempts
        file_size_mb: Size of file in MB (for quota calculations)
        context: Optional context string for logging
        
    Returns:
        bool: True if should retry, False if should give up
    """
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
            # Custom wait times for 503 errors: 2, 4, 8, 12, 20 minutes
            wait_times = [2, 4, 8, 12, 20]  # minutes
            wait_time_minutes = wait_times[min(attempt, len(wait_times) - 1)]
            wait_time = wait_time_minutes * 60  # convert to seconds
            print(f'  Attempt {attempt + 1} failed (service unavailable/internal error), retrying in {wait_time_minutes} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with service errors')
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
    elif 'incomplete json' in error_str or 'json validation failed' in error_str or 'truncated' in error_str:
        if attempt < max_retries - 1:
            wait_time = 30 * 2 ** attempt
            print(f'  Attempt {attempt + 1} failed (incomplete JSON), retrying in {wait_time} seconds...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with incomplete JSON errors')
            return False
    elif any(keyword in error_str for keyword in ['quota', 'rate limit', 'too many requests', '429']):
        # Check specifically for RESOURCE_EXHAUSTED (quota exceeded)
        if 'resource_exhausted' in error_str and 'quota' in error_str:
            print(f'  🚨 API QUOTA EXHAUSTED detected - Process will shutdown gracefully')
            # Set per-process flag to trigger graceful shutdown
            global process_quota_flags
            # Extract process_id from context if available, otherwise use 0
            process_id = 0
            if context and hasattr(context, 'process_id'):
                process_id = context.process_id
            elif context and isinstance(context, dict) and 'process_id' in context:
                process_id = context['process_id']
            process_quota_flags[process_id] = True
            return False  # Don't retry, trigger shutdown
        elif attempt < max_retries - 1:
            wait_time = calculate_quota_retry_delay(file_size_mb, attempt)
            print(f'  Attempt {attempt + 1} failed (rate limit), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with rate limiting')
            return False
    elif attempt < max_retries - 1:
        wait_time = 60 * 2 ** attempt
        print(f'  Attempt {attempt + 1} failed ({type(error).__name__}), retrying in {wait_time // 60} minutes...')
        time.sleep(wait_time)
        return True
    else:
        print(f'  All {max_retries} attempts failed with {type(error).__name__}: {error}')
        return False


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


def check_response_truncation(response, filename: str) -> bool:
    """
    Check if the LLM response was truncated.
    
    Args:
        response: The LLM response object
        filename: Filename for context
        
    Returns:
        bool: True if response was truncated, False otherwise
    """
    # Check finish reason for truncation
    if hasattr(response, 'candidates') and response.candidates:
        candidate = response.candidates[0]
        if hasattr(candidate, 'finish_reason'):
            if candidate.finish_reason == 'MAX_TOKENS':
                print(f'  DEBUG: Response truncated due to MAX_TOKENS limit for {filename}')
                save_truncated_response(response.text, filename)
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


def save_truncated_response(response_text: str, filename: str):
    """
    Save truncated response to file for debugging analysis.
    
    Args:
        response_text: The truncated response text
        filename: Original filename for context
    """
    try:
        import os
        from datetime import datetime
        
        # Create the truncated responses directory
        truncated_dir = Path("performance_logs/llm_analysis/max_tokens_truncated")
        truncated_dir.mkdir(parents=True, exist_ok=True)
        
        # Create clean filename without timestamp
        clean_filename = extract_clean_filename(filename)
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


# =============================================================================
# CONSTANTS
# =============================================================================
# Global configuration constants
MODEL = 'gemini-2.5-flash'  


# =============================================================================
# DATA SCHEMAS
# =============================================================================
# Pydantic schemas for structured extraction of CAO document information

# ---------------------------------------------------------------------
# AMOUNT CLASSES
# ---------------------------------------------------------------------


class Amount(BaseModel):
    """Value-unit pair for amounts, durations, percentages, etc."""
    value: Optional[float] = None
    unit: Optional[str] = None


class AmountRange(BaseModel):
    """Compact min/max range with shared unit."""
    min: Optional[float] = None
    max: Optional[float] = None
    unit: Optional[str] = None


# ---------------------------------------------------------------------
# SALARY POINT: one effective value in time
# ---------------------------------------------------------------------
class SalaryPoint(BaseModel):
    """
    One effective salary value valid for a specific period.

    Each SalaryPoint represents a single salary entry from a wage table or 
    general increase clause — defined by its start date and (if stated) end date. 
    It records the raw printed amount, pay unit, and optional context such as 
    general increase, working hours basis, or inclusion of holiday allowance.
    """

    start_date: str = Field(
        ...,
        description="Date the salary amount becomes effective (YYYY-MM-DD format)."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="Date the salary amount ceases to apply, if explicitly stated (YYYY-MM-DD format). "
                    "Omit if not given or still in force at contract expiry."
    )

    amount: float = Field(
        ...,
        description="Gross salary amount as printed in the CAO (no conversions or derivations)."
    )


    unit: str = Field(
        ...,
        description="Pay period unit as stated (e.g., 'monthly', '4-week', 'weekly', 'hourly', 'annual')."
    )

    table_label: str = Field(
        default="",
        description="Concise identifier of the wage table or adjustment version (e.g., 'per 1 Nov 2023', 'Table A - 2024 rates')."
    )

    increase_percent: Optional[float] = Field(
        default=None,
        description="If the CAO specifies a general percentage increase for this table or time period (e.g., 3.00 for a +3% wage rise), record it here; otherwise omit."
    )

    holiday_in_amount: Optional[bool] = Field(
        default=None,
        description="True if the printed amount explicitly includes holiday allowance; "
                    "Omit if not stated or if false."
    )

    hours_basis_ft_week: Optional[float] = Field(
        default=None,
        description="Full-time weekly hours underlying this amount (e.g., 36, 37, 38, 40), "
                    "recorded only if explicitly for the table version; omit if not mentioned."
    )

    note: Optional[str] = Field(
        default=None,
        description="Footnotes, exceptions, or remarks directly tied to this wage entry. "
                    "Keep concise but faithful to the original text. Omit if not present."
    )


# ---------------------------------------------------------------------
# SALARY ROW: one job group × step × optional age/education
# ---------------------------------------------------------------------
class SalaryRow(BaseModel):
    """
    One complete wage-scale cell representing a combination of:
    (worker type) × (job group) × [optional step/trede] × [optional age band] × [optional education level] × [optional contract type] × timeline.

    The 'timeline' field contains the series of salary values (`SalaryPoint`) 
    over time, as published in successive CAO wage tables.
    """

    # ---- Identification / scoping ----
    jobgroup: str = Field(
        ...,
        description="Job group or salary scale label/code. If a descriptive subtitle is given, append it in parentheses (e.g., 'F-45-9 (workers with high school diploma)')."
    )

    step_label: Optional[str] = Field(
        default=None,
        description="Printed label of the step/trede (e.g., 'trede 0', 'periodiek 3', 'aanloopschaal C'); omit if not printed."
    )

    worker_type: Optional[str] = Field(
        default=None,
        description="Worker type or category. Omit if generic (e.g., 'employee', 'standard worker') AND only one worker type exists. Keep when meaningful (e.g., 'Construction worker', 'UTA employee')."
    )

    is_entry: Optional[bool] = Field(
        default=None,
        description="True if explicitly described as an entry/aanloop scale; "
                    "False if explicitly standard; Omit if not stated."
    )

    # ---- Filters (only when printed) ----
    age_group: Optional[str] = Field(
        default=None,
        description="Printed age band of the wage table (e.g., '23 jaar', '21+'). Consider only age bands capturing at least some workers aged between 23-65. Omit if not printed."
    )

    education: Optional[str] = Field(
        default=None,
        description="Printed education level qualifier (e.g., 'MBO-2', 'HBO-bachelor'), if stated; omit if not printed."
    )

    ft_hours: Optional[float] = Field(
        default=None,
        description="Full-time weekly hours baseline for this CAO (e.g., 37). Record only if explicitly stated, not derived. Omit if not printed."
    )

    # ---- Contract type information ----
    permanency: Optional[str] = Field(
        default=None,
        description="Contract permanency type as explicitly stated (e.g., 'permanent', 'temporary', 'fixed-term'). "
                    "Omit if not printed or not applicable to this salary row."
    )

    hours_type: Optional[str] = Field(
        default=None,
        description="Work arrangement type as explicitly stated (e.g., 'full-time', 'part-time'). "
                    "Omit if not printed or not applicable to this salary row."
    )

    # ---- Salary timeline ----
    timeline: List[SalaryPoint] = Field(
        default_factory=list,
        description="Chronological list of salary points (each from a wage table or increase clause)."
    )

    # ---- Meta / context ----
    row_note: Optional[str] = Field(
        default=None,
        description="Row-level remarks that apply across all timeline points "
                    "(e.g., 'All amounts exclude 8% holiday allowance unless noted', "
                    "'Scale F merges into G from 2026-01-01'). Keep concise but faithful to the original text. Omit if not present."
    )

class SalaryExtractionSchema(BaseModel):
    """Top-level container for all extracted wage-related information from one CAO."""
    salary_information: List[SalaryRow] = Field(
        default_factory=list,
        description=(
            "List of all wage-scale rows extracted from the CAO, "
            "each describing one (job group) × (step) × [optional worker type] × "
            "[optional age band] × [optional education level] × [optional contract type] × timeline."
        )
    )

#----------------------------
# Non-salary information
#----------------------------

# ----------------------------
# GENERAL INFORMATION
# ----------------------------
class GeneralInfo(BaseModel):
    """Schema for general contract information (record exactly as stated in the CAO)."""
    start_date_contract: str = Field(
        default="",
        description="CAO validity start date (YYYY-MM-DD)."
    )
    expiry_date_contract: str = Field(
        default="",
        description="CAO validity end date (YYYY-MM-DD)."
    )
    signing_date: str = Field(
        default="",
        description="Date the CAO was signed by the parties (YYYY-MM-DD)."
    )

    # Retroactivity — record only when explicitly stated
    retroactive_applies: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states that (some) terms apply retroactively."
    )
    retroactive_start_date: str = Field(
        default="",
        description="Start date of retroactive application (YYYY-MM-DD). Leave empty if retroactive_applies = false."
    )
    retroactive_end_date: str = Field(
        default="",
        description="End date of retroactive application (YYYY-MM-DD). Leave empty if retroactive_applies = false."
    )
    retroactive_scope_note: str = Field(
        default="",
        description="What is retroactive (e.g., wage scales, allowances). Leave empty if retroactive_applies = false."
    )
    retroactive_backpay_due: Optional[bool] = Field(
        default=None,
        description="Set true only if back-pay for the retro period is explicitly required, false if not explicitly stated — Omit if retroactive_applies = false."
    )
    retroactive_backpay_terms: str = Field(
        default="",
        description="Back-pay rules as stated. Leave empty if retroactive_applies = false OR retroactive_backpay_due = false."
    )
    retroactive_exclusions_note: str = Field(
        default="",
        description="Groups or items explicitly excluded from retroactivity. Leave empty if retroactive_applies = false."
    )
    retroactive_interest_or_surcharge: str = Field(
        default="",
        description="Interest/surcharge on late back-pay, if stated. Leave empty if retroactive_applies = false OR retroactive_backpay_due = false."
    )

    # Scope / classification
    sbi_code_primary: str = Field(
        default="",
        description="Primary SBI code (e.g., '41.20')."
    )
    sbi_code_secondary: str = Field(
        default="",
        description="Secondary SBI code(s), if any (comma-separated)."
    )
    sbi_code_version: str = Field(
        default="",
        description="Version of the SBI classification (e.g., 'SBI 2008')."
    )

    deviation_allowed_company_level: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly permits company-level deviations from CAO terms."
    )
    cao_scope_type: str = Field(
        default="unspecified",
        description="CAO scope type (e.g., 'sectoral', 'single_company', 'group', 'association_limited', 'occupational_niche', 'unspecified', 'other')."
    )
    firm_name: str = Field(
        default="",
        description="Company name — ONLY if cao_scope_type = 'single_company'."
    )
    firm_cao_scope_description: str = Field(
        default="",
        description="Brief description of firm-level scope, as stated."
    )

    # AVV (generally binding)
    avv_applies: bool = Field(
        default=False,
        description="Set true only if the CAO is/was declared generally binding (AVV)."
    )
    avv_start_date: str = Field(
        default="",
        description="AVV start date (YYYY-MM-DD) — ONLY if avv_applies = true."
    )
    avv_end_date: str = Field(
        default="",
        description="AVV end date (YYYY-MM-DD) — ONLY if avv_applies = true."
    )

# ----------------------------
# BONUSES (WAGE)
# ----------------------------
class BonusesInfo(BaseModel):
    has_bonus_schemes: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly provides any recurring or structural bonus/incentive beyond base salary."
    )

    sign_on_bonus_present: bool = Field(
        default=False,
        description="Set true only if the CAO includes a sign-on bonus for new hires."
    )
    sign_on_bonus: Optional[Amount] = Field(
        default=None,
        description="Sign-on bonus amount with unit (e.g., value=500, unit='EUR one-off') — Omit if sign_on_bonus_present = false."
    )

    thirteenth_month_present: bool = Field(
        default=False,
        description="Set true only if the CAO grants a 13th month of salary (or equivalent)."
    )
    thirteenth_month: Optional[Amount] = Field(
        default=None,
        description="13th-month value with unit (e.g., value=1.0, unit='monthly wage' or value=50, unit='% of annual salary') — Omit if thirteenth_month_present = false."
    )

    fixed_annual_lump: Optional[Amount] = Field(
        default=None,
        description="Fixed recurring lump sum per year with unit (e.g., value=1000, unit='EUR per year')."
    )

    profit_sharing_present: bool = Field(
        default=False,
        description="Set true only if a profit-sharing scheme is explicitly stated."
    )
    profit_sharing_note: str = Field(
        default="",
        description="Short description of how profit-sharing is calculated (e.g., '% of company profit'). Leave empty if profit_sharing_present = false."
    )

    performance_bonus_present: bool = Field(
        default=False,
        description="Set true only if a performance/target-based bonus beyond base pay is explicitly stated."
    )

    job_specific_allowances_present: bool = Field(
        default=False,
        description="Set true only if role-linked allowances are explicitly stated (e.g., cashier allowance, driver's license allowance)."
    )
    job_specific_allowances_note: str = Field(
        default="",
        description="Short description of role-linked allowances as stated. Leave empty if job_specific_allowances_present = false."
    )

    qualification_bonus_present: bool = Field(
        default=False,
        description="Set true only if a monetary bonus for obtaining specific diplomas/certifications is explicitly stated."
    )
    qualification_bonus_note: str = Field(
        default="",
        description=(
            "Short note exactly as stated describing qualification-related bonuses — include whether it is one-off or recurring (e.g., monthly), the amount or percentage, eligible diplomas/certifications, and any other stated conditions (e.g., job relevance, repayment if leaving early). Leave empty if qualification_bonus_present = false."
        )
    )

    seniority_or_loyalty_bonus_present: bool = Field(
        default=False,
        description="Set true only if a bonus/gratuity for long service or seniority is explicitly stated."
    )

    retirement_gratuity_present: bool = Field(
        default=False,
        description="Set true only if a lump sum at retirement or long-service exit is explicitly stated."
    )
    retirement_gratuity_note: str = Field(
        default="",
        description="Description/value of lump sum at retirement or long-service exit exactly as stated (e.g., '1 month salary after 25 years'). Leave empty if retirement_gratuity_present = false."
    )

# ----------------------------
# WAGE SCALES & PROGRESSION
# ----------------------------
class WageScalesInfo(BaseModel):
    entry_step_by_experience_present: bool = Field(
        default=False,
        description="Set true only if the CAO allows a higher initial step/trede based on relevant experience/competence."
    )
    entry_step_by_experience_rule: str = Field(
        default="",
        description="Short rule text exactly as stated (e.g., '≥3 yrs relevant exp → start ≥ Trede 3; manager discretion'). Leave empty if entry_step_by_experience_present = false."
    )

    personal_allowance_at_max_scale_present: bool = Field(
        default=False,
        description="Set true only if a personal pay supplement ('persoonlijke toeslag') is granted when an employee reaches the maximum of the wage scale or retains a higher wage after reclassification."
    )
    personal_allowance_rule_text: str = Field(
        default="",
        description="Basis/%/amount, duration, pensionability, and any phase-out or indexation exactly as stated. Leave empty if personal_allowance_at_max_scale_present = false."
    )

    performance_step_variation_present: bool = Field(
        default=False,
        description="Set true only if the employer may grant extra steps or withhold steps based on performance."
    )
    performance_step_variation_rule: str = Field(
        default="",
        description="Criteria/limits exactly as stated (e.g., 'max +2 steps after excellent rating; withholding requires PIP & OR notification'). Leave empty if performance_step_variation_present = false."
    )


# ----------------------------
# PENSION INFORMATION
# ----------------------------
class PensionInfo(BaseModel):
    """Schema for pension information (record values exactly as stated; do not infer statutory comparisons)."""
    has_pension_scheme: bool = Field(
        default=False,
        description="Set true only if any pension scheme beyond AOW is mentioned; false if none is mentioned."
    )
    pension_type: str = Field(
        default="unspecified",
        description="Scheme type (e.g., 'DB' = Defined Benefit, 'DC' = Defined Contribution, 'hybrid' = combination, 'unspecified', 'other')."
    )
    mandatory_participation: bool = Field(
        default=False,
        description="Set true only if participation in a (sector) pension fund is explicitly mandatory."
    )

    # Selection rule for 'typical' group (if needed for single values)
    selection_rule_pension: str = Field(
        default="unspecified",
        description=(
            "How the 'typical' group was chosen when multiple rates exist (e.g., 'majority_headcount', 'office_vs_field_rule', 'base_tier', 'latest_year', 'other', 'unspecified')."
        )
    )

    employee_contrib: Optional[Amount] = Field(
        default=None,
        description="Employee pension contribution for the chosen group with unit (e.g., value=5.5, unit='% of salary')."
    )
    accrual_rate: Optional[Amount] = Field(
        default=None,
        description="Annual accrual rate for the chosen group with unit (e.g., value=1.875, unit='% per year')."
    )
    franchise: Optional[Amount] = Field(
        default=None,
        description="Franchise amount for the CAO period with unit (e.g., value=14400, unit='EUR per year')."
    )

    retirement_age_normal: Optional[Amount] = Field(
        default=None,
        description="Normal retirement age (e.g., value=67, unit='years')."
    )
    retirement_age_early: Optional[Amount] = Field(
        default=None,
        description="Early retirement age (e.g., value=63, unit='years')."
    )
    retirement_age_deferred: Optional[Amount] = Field(
        default=None,
        description="Deferred/postponed retirement age (e.g., value=70, unit='years')."
    )

    accrual_during_statutory_leaves: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states accrual continues during statutory leaves."
    )
    accrual_during_illness_year2: bool = Field(
        default=False,
        description="Set true only if full accrual continues in the 2nd year of illness is explicitly stated."
    )
    excedentregeling_present: bool = Field(
        default=False,
        description="Set true if an 'excedentregeling' (accrual above wage cap) is explicitly offered."
    )
    premium_change_equal_split: bool = Field(
        default=False,
        description="Set true only if future premium changes are explicitly split equally between employer and employee."
    )

    # Heterogeneity (capture ranges; do not infer)
    heterogeneity_present_pension: bool = Field(
        default=False,
        description="Set true if different pension rates are shown for major groups."
    )
    employee_contrib_range: Optional[AmountRange] = Field(
        default=None,
        description="Employee contribution range among major groups (e.g., min=4.5, max=6.5, unit='% of salary') — Omit if heterogeneity_present_pension = false."
    )
    premium_total_range: Optional[AmountRange] = Field(
        default=None,
        description="Total pension premium range among major groups (e.g., min=15, max=25, unit='% of salary') — Omit if heterogeneity_present_pension = false."
    )


# ----------------------------
# LEAVE INFORMATION
# ----------------------------
class LeaveInfo(BaseModel):
    """
    Schema for leave information.
    Policy: record absolute CAO entitlements exactly as stated (durations, pay levels, units).
    Do NOT compare to statutory baselines in the prompt; only set '*_above_statutory' flags
    when the CAO explicitly says so. Any historical comparison to statute is a downstream analysis task.
    """
    has_leave_enhancements: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states any leave improvement or top-up relative to statute."
    )

    # Maternity
    has_above_statutory_maternity: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states an enhancement above statutory for maternity."
    )
    paid_maternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of fully paid maternity leave exactly as stated in the CAO (e.g., value=16, unit='weeks')."
    )
    partially_paid_maternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of partially paid maternity leave as stated (e.g., value=10, unit='weeks')."
    )
    partially_paid_maternity_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during partially paid maternity leave (e.g., value=70, unit='% of salary')."
    )
    unpaid_maternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of additional unpaid maternity leave, as stated (e.g., value=6, unit='weeks')."
    )
    maternity_note: str = Field(
        default="",
        description="Maternity notes exactly as stated."
    )

    # Paternity / partner
    paternity_explicitly_above_statutory: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states any improvement for paternity/partner leave."
    )
    paid_paternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of fully paid paternity/partner leave as stated (e.g., value=6, unit='weeks')."
    )
    partially_paid_paternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of partially paid paternity/partner leave as stated (e.g., value=4, unit='weeks')."
    )
    partially_paid_paternity_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during partially paid paternity/partner leave (e.g., value=70, unit='% of salary')."
    )
    unpaid_paternity_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of unpaid paternity/partner leave as stated (e.g., value=2, unit='weeks')."
    )

    # Adoption / foster
    adoption_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of adoption/foster leave as stated (e.g., value=10, unit='weeks')."
    )
    adoption_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during adoption/foster leave (e.g., value=100, unit='% of salary')."
    )

    # Parental
    parental_leave_topup_present: bool = Field(
        default=False,
        description="Set true only if an employer top-up for parental leave is explicitly stated."
    )
    parental_leave_topup_pay: Optional[Amount] = Field(
        default=None,
        description="Top-up pay level during parental leave, as stated (e.g., value=70, unit='% of salary')."
    )
    parental_leave_unpaid: Optional[Amount] = Field(
        default=None,
        description="Duration of unpaid parental leave as stated (e.g., value=26, unit='weeks')."
    )

    # Abortion
    abortion_leave_present: bool = Field(
        default=False,
        description="Set true only if a specific abortion leave provision is explicitly mentioned."
    )

    # Sickness
    sick_leave_topup_present: bool = Field(
        default=False,
        description="Set true only if an employer sick-pay top-up is explicitly stated."
    )
    sickpay_duration: Optional[Amount] = Field(
        default=None,
        description="Duration of stated sick-pay continuation/top-up (e.g., value=104, unit='weeks')."
    )
    sickpay_continuation: Optional[Amount] = Field(
        default=None,
        description="Sick-pay continuation rate as stated (e.g., value=70, unit='% of salary')."
    )
    sickpay_extra_insurance_present: bool = Field(
        default=False,
        description="Set true if extra disability/WGA-gap insurance is explicitly included."
    )

    # Care leave
    care_leave_topup_present: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly tops up short-/long-term care leave."
    )
    short_term_care_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of short-term care leave (e.g., value=10, unit='days per year')."
    )
    short_term_care_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during short-term care leave (e.g., value=100, unit='% of salary')."
    )
    long_term_care_leave: Optional[Amount] = Field(
        default=None,
        description="Duration of long-term care leave (e.g., value=6, unit='months')."
    )
    long_term_care_pay: Optional[Amount] = Field(
        default=None,
        description="Pay level during long-term care leave (e.g., value=70, unit='% of salary')."
    )

    # Vacation & holiday allowance
    vacation_time: Optional[Amount] = Field(
        default=None,
        description="Typical vacation entitlement for a standard worker (e.g., value=25, unit='days per year')."
    )
    vacation_bonus: Optional[Amount] = Field(
        default=None,
        description="Holiday allowance (vakantiegeld) amount or percentage (e.g., value=8, unit='% of annual salary')."
    )

    # Heterogeneity & notes
    heterogeneity_present_leave: bool = Field(
        default=False,
        description="Set true if major groups have different leave entitlements or pay levels."
    )
    liberation_day_annual: bool = Field(
        default=False,
        description="Set true if 5 May (Liberation Day) is a paid day off every year, as stated."
    )
    liberation_day_lustrum: bool = Field(
        default=False,
        description="Set true if 5 May is a paid day off only in lustrum years (every 5 years), as stated."
    )
    liberation_day_comp_note: str = Field(
        default="",
        description="Compensation note if Liberation Day is not a day off."
    )
    extra_leave_seniority_present: bool = Field(
        default=False,
        description=(
            "Set true only if the CAO explicitly grants extra vacation or leave entitlements based on years of service and/or age."
        )
    )
    extra_leave_seniority_schedule: str = Field(
        default="",
        description=(
            "Compact schedule or rule exactly as stated, showing seniority- or age-based leave increments. Leave empty if extra_leave_seniority_present = false."
        )
    )
    leave_note: str = Field(
        default="",
        description="Any special leaves."
    )

# ----------------------------
# TERMINATION INFORMATION
# ----------------------------
class TerminationInfo(BaseModel):
    """Schema for termination information (record exactly as stated; no statutory inference)."""

    has_termination_rules: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly contains termination/notice rules beyond statutory defaults."
    )

    selection_rule_notice: str = Field(
        default="unspecified",
        description=(
            "How the 'typical' group for notice was chosen when multiple rules exist (e.g., 'majority_headcount', 'base_tier', 'office_vs_field_rule', 'latest_year', 'unspecified', 'other')."
        )
    )

    employer_notice: Optional[Amount] = Field(
        default=None,
        description="Typical employer notice period (employer terminating) (e.g., value=2, unit='months')."
    )

    employee_notice: Optional[Amount] = Field(
        default=None,
        description="Typical employee notice period (employee resigning) (e.g., value=1, unit='months')."
    )

    heterogeneity_present_notice: bool = Field(
        default=False,
        description="Set true if major groups have different notice periods."
    )

    employer_notice_range: Optional[AmountRange] = Field(
        default=None,
        description="Employer notice duration range across main groups (e.g., min=1, max=3, unit='months') — Omit if heterogeneity_present_notice = false."
    )

    employee_notice_range: Optional[AmountRange] = Field(
        default=None,
        description="Employee notice duration range across main groups (e.g., min=1, max=2, unit='months') — Omit if heterogeneity_present_notice = false."
    )

    notice_period_by_tenure_present: bool = Field(
        default=False,
        description=(
            "Set true only if notice periods vary explicitly by years of service (for employer and/or employee)."
        )
    )

    notice_period_by_tenure_rule: str = Field(
        default="",
        description=(
            "Exact rule or schedule for tenure-based notice periods as stated. Leave empty if notice_period_by_tenure_present = false."
        )
    )

    can_shorten_notice_with_uwv_permit: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly allows notice to be shortened with a UWV permit."
    )
    notice_min_floor: Optional[Amount] = Field(
        default=None,
        description="Minimum notice duration that must remain after any shortening (e.g., value=1, unit='months') — Omit if can_shorten_notice_with_uwv_permit = false."
    )

    dismissal_approval: str = Field(
        default="unspecified",
        description="Which approval or authorization route the CAO states is required for a standard dismissal (e.g., 'UWV', 'Judge', 'Both', 'None', 'Conditional', 'unspecified', 'other'). Use 'Conditional' if approval applies only in specific cases or time periods."
    )

    sickness_dismissal_protection: bool = Field(
        default=False,
        description="Set true if the CAO reiterates or extends the dismissal ban/protection during sickness."
    )

    end_at_AOW_age_automatic: bool = Field(
        default=False,
        description="Set true only if the CAO states employment ends automatically at AOW (statutory pension) age."
    )

    probation_allowed: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly allows a probationary period in general."
    )
    probation_fixedterm: Optional[Amount] = Field(
        default=None,
        description="Maximum probation period for fixed-term contracts (e.g., value=2, unit='months'), exactly as stated."
    )
    probation_indefinite: Optional[Amount] = Field(
        default=None,
        description="Maximum probation period for indefinite-term contracts (e.g., value=1, unit='months'), exactly as stated."
    )

    severance_or_ww_supplement_present: bool = Field(
        default=False,
        description="Set true only if the CAO adds severance or WW (unemployment) supplements beyond statutory transition pay."
    )
    severance_extra: Optional[Amount] = Field(
        default=None,
        description="Quantified extra severance if stated (e.g., value=5000, unit='EUR') — Omit if severance_or_ww_supplement_present = false."
    )
    severance_extra_formula_note: str = Field(
        default="",
        description="Short formula or rule text for extra severance. Leave empty if severance_or_ww_supplement_present = false."
    )
    severance_by_tenure_rule_note: str = Field(
        default="",
        description=(
            "Brief formula or schedule exactly as stated if the CAO adds tenure-based severance beyond statutory transition pay. Leave empty if not applicable."
        )
    )

# ----------------------------
# OVERTIME INFORMATION
# ----------------------------
class OvertimeInfo(BaseModel):
    """Schema for overtime information (record exactly as stated; add units whenever a value is present)."""

    has_overtime_rules: bool = Field(
        default=False,
        description="Set true only if the CAO specifies overtime or allowance rules beyond statutory defaults."
    )

    selection_rule_overtime: str = Field(
        default="unspecified",
        description=(
            "How the 'typical' group for overtime was chosen when multiple worker groups exist (e.g., 'majority_headcount', 'base_tier', 'office_vs_field_rule', 'latest_year', 'unspecified', 'other'). Use the same selection logic as in notice and pension fields."
        )
    )

    overtime_trigger_daily: Optional[Amount] = Field(
        default=None,
        description="Daily threshold after which hours count as overtime, exactly as stated (e.g., value=8, unit='hours')."
    )
    overtime_trigger_weekly: Optional[Amount] = Field(
        default=None,
        description="Weekly threshold after which hours count as overtime, exactly as stated (e.g., value=40, unit='hours')."
    )

    # Surcharges
    overtime_compensation_mode: str = Field(
        default="unspecified",
        description="How overtime is compensated according to the CAO (e.g., 'monetary_pay', 'TOIL', 'both', 'unspecified', 'other')."
    )
    stacking_rule: str = Field(
        default="unspecified",
        description="How surcharges (e.g., overtime, night, weekend) interact (e.g., 'highest_only', 'cumulative', 'unclear', 'unspecified', 'other')."
    )
    overtime_allowance: Optional[Amount] = Field(
        default=None,
        description="Typical overtime surcharge (e.g., value=150, unit='% of hourly rate')."
    )
    heterogeneity_present_overtime: bool = Field(
        default=False,
        description="Set true if different overtime rates are shown for major groups."
    )
    overtime_allowance_range: Optional[AmountRange] = Field(
        default=None,
        description="Overtime surcharge range across main cases (e.g., min=125, max=175, unit='% of hourly rate') — Omit if heterogeneity_present_overtime = false."
    )

    # Shift / unfavourable hours
    shift_allowance_present: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly provides a separate shift allowance for working in regular shifts, distinct from overtime pay."
    )
    shift_allowance_range: Optional[AmountRange] = Field(
        default=None,
        description="Shift allowance range (e.g., min=10, max=25, unit='% of hourly rate') — Omit if shift_allowance_present = false."
    )

    unfavourable_hours_allowance: Optional[Amount] = Field(
        default=None,
        description="Maximum allowance for work during unfavourable hours (e.g., value=20, unit='% of hourly rate'). Distinct from overtime pay and from regular shift allowances."
    )

    # Working time bounds and rest
    min_rest_between_shifts: Optional[Amount] = Field(
        default=None,
        description="Minimum rest required between shifts, exactly as stated (e.g., value=11, unit='hours')."
    )

    max_hours_per_day: Optional[Amount] = Field(
        default=None,
        description="Maximum daily working time set by the CAO (e.g., value=10, unit='hours')."
    )

    max_hours_per_week: Optional[Amount] = Field(
        default=None,
        description="Maximum weekly working time set by the CAO (e.g., value=45, unit='hours')."
    )

    compulsory_overtime_annual: Optional[Amount] = Field(
        default=None,
        description="Maximum annual compulsory overtime if specified (e.g., value=200, unit='hours per year')."
    )

    guaranteed_weekends_off_rule_text: str = Field(
        default="",
        description="Verbatim summary if the CAO guarantees a minimum number of weekends off."
    )


# ----------------------------
# TRAINING INFORMATION
# ----------------------------
class TrainingInfo(BaseModel):
    """Schema for training information (record exactly as stated; add units when values are present)."""

    has_training_rights: bool = Field(
        default=False,
        description="Set true only if the CAO grants training/education rights."
    )

    training_time_yearly: Optional[Amount] = Field(
        default=None,
        description="Typical paid training time per year (e.g., value=40, unit='hours per year')."
    )

    training_budget: Optional[Amount] = Field(
        default=None,
        description="Annual monetary training budget (e.g., value=2000, unit='EUR per year')."
    )

    career_scan_freq: Optional[Amount] = Field(
        default=None,
        description="Frequency of employability/career scans (e.g., value=2, unit='times per year')."
    )

    cost_reimbursement: Optional[Amount] = Field(
        default=None,
        description="Percentage or amount of training/study costs reimbursed by the employer or sector fund (e.g., value=100, unit='% of costs')."
    )

    training_fund_present: bool = Field(
        default=False,
        description="Set true if a sectoral/CAO training fund finances training or subsidies."
    )
    reclaim_clause_present: bool = Field(
        default=False,
        description="Set true if the employer may reclaim training costs upon early departure."
    )
    mandatory_training_paid: bool = Field(
        default=False,
        description="Set true only if the CAO states employer pays 100% for mandatory/company-required training."
    )

    training_note: str = Field(
        default="",
        description="Concise verbatim summary of special rules."
    )


# ----------------------------
# HOMEOFFICE / TELEWORK INFORMATION
# ----------------------------
class HomeofficeInfo(BaseModel):
    """Schema for home office / telework information (record exactly as stated)."""

    has_homeoffice_rights: bool = Field(
        default=False,
        description="Set true only if the CAO includes home office / telework provisions."
    )

    homeoffice_entitlement: Optional[Amount] = Field(
        default=None,
        description="Entitled amount of remote work allowed, as explicitly stated in the CAO (e.g., value=2, unit='days per week')."
    )

    homeoffice_stipend_present: bool = Field(
        default=False,
        description="Set true only if a fixed home office allowance is stated."
    )
    homeoffice_stipend: Optional[Amount] = Field(
        default=None,
        description="Home office allowance amount (e.g., value=50, unit='EUR per month') — Omit if homeoffice_stipend_present = false."
    )

    homeoffice_discretion: str = Field(
        default="unspecified",
        description="Who decides on home office arrangements, as explicitly stated in the CAO (e.g., 'employer_only', 'joint_with_OR' = decision made jointly with the Works Council, 'employee_request' = employees may request or decide, 'unspecified', 'other')."
    )

    homeoffice_costs_reimbursed: bool = Field(
        default=False,
        description="Set true if the employer reimburses home office-related costs."
    )
    homeoffice_costs_note: str = Field(
        default="",
        description="Short note on reimbursed cost types. Leave empty if homeoffice_costs_reimbursed = false."
    )

    homeoffice_agreement_required: bool = Field(
        default=False,
        description="Set true if a formal telework agreement/protocol is required."
    )
    homeoffice_health_safety_guarantee: bool = Field(
        default=False,
        description="Set true if the employer commits to meet OSH/Arbo obligations at the home office."
    )
    homeoffice_travel_time_compensation: bool = Field(
        default=False,
        description="Set true only if the CAO explicitly states that additional commuting time arising from home-office or hybrid work is compensated."
    )

    homeoffice_note: str = Field(
        default="",
        description="Other remarks (e.g., equipment provision, campaigns, stimulation-only, only statutory minimum provisions)."
    )


# ----------------------------
# CONTRACT TYPE INFORMATION
# ----------------------------
class ContractTypeInfo(BaseModel):
    """Schema for contract type rules (record exactly as stated; units with values)."""

    has_contract_type_rules: bool = Field(
        default=False,
        description="Set true only if the CAO sets explicit rules on contract types of the workers beyond statutory defaults."
    )

    full_time_hours: Optional[Amount] = Field(
        default=None,
        description="Standard full-time working hours, exactly as stated (e.g., value=38, unit='hours per week')."
    )

    part_time_allowed: bool = Field(
        default=False,
        description="Set true only if part-time contracts are explicitly permitted for standard workers."
    )
    part_time_range: Optional[AmountRange] = Field(
        default=None,
        description="Part-time fraction/hours range (e.g., min=0.5, max=0.9, unit='FTE') — Omit if part_time_allowed = false."
    )

    minmax_hours_contract_allowed: bool = Field(
        default=False,
        description="Set true if 'min-max' (bandwidth) contracts are explicitly permitted."
    )
    minmax_hours_range: Optional[AmountRange] = Field(
        default=None,
        description="Min-max contract hours range (e.g., min=20, max=40, unit='hours per week') — Omit if minmax_hours_contract_allowed = false."
    )

    zero_hour_oncall_allowed: bool = Field(
        default=False,
        description="Set true if zero-hour or on-call contracts are explicitly allowed."
    )

    ketenregeling_deviation_present: bool = Field(
        default=False,
        description="Set true only if the CAO deviates from the statutory 'ketenregeling' (fixed-term chain rule)."
    )
    ketenregeling_max_contracts: Optional[Amount] = Field(
        default=None,
        description="Maximum number of successive fixed-term contracts (e.g., value=3, unit='contracts') — Omit if ketenregeling_deviation_present = false."
    )
    ketenregeling_max_duration: Optional[Amount] = Field(
        default=None,
        description="Maximum total duration of the fixed-term chain (e.g., value=24, unit='months') — Omit if ketenregeling_deviation_present = false."
    )

    conversion_rights_temp_to_perm_present: bool = Field(
        default=False,
        description="Set true if the CAO grants extra rights to convert fixed-term to indefinite contracts beyond the law."
    )
    conversion_rights_rule_text: str = Field(
        default="",
        description="Exact rule text on conversion from fixed-term to indefinite contracts, as stated. Leave empty if conversion_rights_temp_to_perm_present = false."    
    )


# ----------------------------
# FRINGE BENEFITS INFORMATION
# ----------------------------
class FringeBenefitsInfo(BaseModel):
    """Schema for fringe benefits (record exactly as stated; units with values)."""

    has_fringe_benefits: bool = Field(
        default=False,
        description="Set true only if the CAO mentions any fringe benefits beyond base pay."
    )

    commuting_allowance_present: bool = Field(
        default=False,
        description="Set true if commuting is reimbursed."
    )
    commuting_allowance: Optional[Amount] = Field(
        default=None,
        description="Commuting allowance amount (e.g., value=0.19, unit='EUR per km') — Omit if commuting_allowance_present = false."
    )

    bike_scheme_present: bool = Field(
        default=False,
        description="Set true if a bicycle/leasefiets scheme is present."
    )
    bike_scheme_note: str = Field(
        default="",
        description="Short note exactly as stated. Leave empty if bike_scheme_present = false."
    )

    internet_or_phone_reimbursement_present: bool = Field(
        default=False,
        description="Set true if internet and/or phone costs are reimbursed."
    )

    meal_benefit_present: bool = Field(
        default=False,
        description="Set true if a meal benefit is provided."
    )
    meal_benefit_type: str = Field(
        default="unspecified",
        description="Type of meal benefit (e.g., 'free_meals', 'subsidised_canteen', 'meal_vouchers', 'meal_allowance', 'other', 'unspecified')."
    )
    meal_benefit_amt: Optional[Amount] = Field(
        default=None,
        description="Meal benefit amount/percentage (e.g., value=5, unit='EUR per day') — Omit if meal_benefit_present = false."
    )

    health_insurance_support_present: bool = Field(
        default=False,
        description="Set true if there is an employer contribution or collective discount for health insurance."
    )
    health_insurance_support_note: str = Field(
        default="",
        description="Short note exactly as stated describing the health insurance support. Leave empty if health_insurance_support_present = false."
    )
    insurance_or_savings_benefit_present: bool = Field(
        default=False,
        description="Set true only if employer-paid financial benefits are explicitly stated."
    )
    insurance_or_savings_benefit_note: str = Field(
        default="",
        description="Short description exactly as stated. Leave empty if insurance_or_savings_benefit_present = false."
    )

    relocation_allowance_present: bool = Field(
        default=False,
        description="Set true if relocation/housing support is provided."
    )
    relocation_allowance: Optional[Amount] = Field(
        default=None,
        description="Relocation allowance value (e.g., value=5000, unit='EUR one-off') — Omit if relocation_allowance_present = false."
    )

    mandatory_certifications_paid: bool = Field(
        default=False,
        description="Set true if the employer covers costs of mandatory licenses/certifications."
    )

    other_fringe_benefits_note: str = Field(
        default="",
        description="Concise catch-all for other benefits."
    )


# ----------------------------
# SAFETY / INTEGRITY INFORMATION
# ----------------------------
class SafetyInfo(BaseModel):
    """Schema for safety and integrity provisions (record exactly as stated)."""

    harassment_protocol_present: bool = Field(
        default=False,
        description="Set true if a sexual harassment/integrity protocol is included."
    )
    harassment_protocol_note: str = Field(
        default="",
        description="Short description of the harassment protocol exactly as stated. Leave empty if harassment_protocol_present = false."
    )

    integrity_protocol_present: bool = Field(
        default=False,
        description="Set true if a broader integrity/behavior protocol is included."
    )

    confidential_counsellor_present: bool = Field(
        default=False,
        description="Set true if an internal or external confidential adviser is explicitly provided."
    )

    reporting_channel_external: bool = Field(
        default=False,
        description="Set true if an external reporting channel is guaranteed."
    )

    safety_training_present: bool = Field(
        default=False,
        description="Set true if the employer/sector funds mandatory safety or psychosocial risk training."
    )

    safety_committee_present: bool = Field(
        default=False,
        description="Set true if a joint safety/health committee is provided."
    )

    rie_psa_required: bool = Field(
        default=False,
        description="True if the CAO requires a Risk Inventory & Evaluation (RI&E) to cover psychosocial risks such as stress or burnout."
    )

    psa_prevention_measures_present: bool = Field(
        default=False,
        description="True if the CAO lists explicit PSA prevention or wellbeing measures."
    )
    psa_measures_note: str = Field(
        default="",
        description="Concise summary of psychosocial-risk or wellbeing measures."
    )

    arbodienst_access_provided: bool = Field(
        default=False,
        description="True if the CAO guarantees employee access to an occupational health service (arbodienst/bedrijfsarts) or sector-funded prevention service."
    )

    preventive_medical_checkup_present: bool = Field(
        default=False,
        description="True if the CAO mentions a Preventive Medical Examination (PMO/PAGO) or health-check entitlement."
    )

    workload_monitoring_present: bool = Field(
        default=False,
        description="True if the CAO includes workload or stress monitoring."
    )

    wellbeing_program_present: bool = Field(
        default=False,
        description="True if the CAO includes wellbeing or vitality programs."
    )

    safety_note: str = Field(
        default="",
        description="Catch-all for unusual obligations."
    )


# ----------------------------
# CHILDCARE INFORMATION
# ----------------------------
class ChildcareInfo(BaseModel):
    """Schema for childcare support (record exactly as stated; units with values)."""

    childcare_support_present: bool = Field(
        default=False,
        description="Set true if the employer provides any childcare benefit/support."
    )

    childcare_support: Optional[Amount] = Field(
        default=None,
        description="Monetary childcare support amount (e.g., value=200, unit='EUR per month')."
    )

    childcare_support_cap: Optional[Amount] = Field(
        default=None,
        description="Maximum employer/sector contribution if a cap is stated (e.g., value=400, unit='EUR per month')."
    )

    childcare_inhouse_present: bool = Field(
        default=False,
        description="Set true if on-site or company-arranged childcare is provided/financed."
    )
    childcare_discount_present: bool = Field(
        default=False,
        description="Set true if discounts at contracted childcare institutions are provided."
    )
    childcare_priority_access: bool = Field(
        default=False,
        description="Set true if priority access or reserved places are provided."
    )

    childcare_age_min: Optional[Amount] = Field(
        default=None,
        description="Minimum covered child age if stated (e.g., value=0, unit='years')."
    )
    childcare_age_max: Optional[Amount] = Field(
        default=None,
        description="Maximum covered child age if stated (e.g., value=12, unit='years')."
    )
    childcare_age_limit_note: str = Field(
        default="",
        description="Free-form age scope details."
    )

    childcare_provider_scope: str = Field(
        default="unspecified",
        description="Which childcare providers qualify for the employer/sector childcare support, as explicitly stated (e.g., 'any', 'contracted_only', 'sector_only', 'company_only', 'unspecified', 'other'): "
        "'any' = all registered providers; "
        "'contracted_only' = only providers with a contract/arrangement; "
        "'sector_only' = sector-specific facilities; "
        "'company_only' = company-run or on-site childcare; "
        "'unspecified' = not stated."
    )

    childcare_public_coord: str = Field(
        default="unspecified",
        description="How childcare benefits interact with public subsidies/fiscal rules (e.g., 'top_up_after_public_benefit', 'within_fiscal_max', 'gross_before_public_benefit', 'unspecified', 'other')."
    )

    childcare_funding_through_sector_fund: bool = Field(
        default=False,
        description="Set true if childcare support is financed via a sector fund."
    )

    childcare_min_tenure_months: Optional[float] = Field(
        default=None,
        description="Minimum tenure required to be eligible for the childcare benefit (months)."
    )
    childcare_min_fte: Optional[Amount] = Field(
        default=None,
        description="Minimum employment fraction (FTE) required for childcare benefit eligibility (e.g., value=0.5, unit='FTE')."
    )

    childcare_benefit_eligibility_note: str = Field(
        default="",
        description="Other eligibility limits or conditions exactly as stated."
    )


# ----------------------------
# AI / ALGORITHMIC MANAGEMENT
# ----------------------------
class AIInfo(BaseModel):
    """Schema for AI/ML/LLM provisions (record exactly as stated)."""

    ai_policy_exists: bool = Field(
        default=False,
        description="Set true only if the CAO contains any AI/algorithmic-management provisions."
    )

    ai_automated_decisions: str = Field(
        default="unspecified",
        description="Are automated AI decisions allowed (e.g., 'never', 'with_human_review', 'unspecified', 'other')."
    )

    ai_transparency_requirements: str = Field(
        default="",
        description="Required disclosures (purpose, data, vendor, logic, worker information). Leave empty if ai_policy_exists = false."
    )

    ai_bias_audit: str = Field(
        default="unspecified",
        description="Frequency/requirement of bias audits (e.g., 'annual', '≥annual', 'none', 'unspecified', 'other')."
    )

    ai_governance_body_present: bool = Field(
        default=False,
        description="Set true if a joint AI/Data/OR governance body/committee exists."
    )

    ai_dispute_rights_note: str = Field(
        default="",
        description="Summary of how workers can contest AI-based decisions. Leave empty if ai_policy_exists = false."
    )

    ai_training_rights_present: bool = Field(
        default=False,
        description="Set true if AI-literacy or upskilling provisions for affected roles are included."
    )
    ai_training_rights_note: str = Field(
        default="",
        description="Hours/budget or redeployment pathways exactly as stated. Leave empty if ai_policy_exists = false."
    )


class NonSalaryExtractionSchema(BaseModel):
    """Schema for non-salary extraction results."""
    pass  # Keep empty for reference


class NonSalaryPart1(BaseModel):
    """Part 1: General, Bonuses, Wage Scales, Pension, Termination"""
    general_information: GeneralInfo = Field(default_factory=GeneralInfo)
    bonuses_info: BonusesInfo = Field(default_factory=BonusesInfo)
    wage_scales_info: WageScalesInfo = Field(default_factory=WageScalesInfo)
    pension_information: PensionInfo = Field(default_factory=PensionInfo)
    termination_information: TerminationInfo = Field(default_factory=TerminationInfo)


class NonSalaryPart2(BaseModel):
    """Part 2: Leave, Overtime, Training"""
    leave_information: LeaveInfo = Field(default_factory=LeaveInfo)
    overtime_information: OvertimeInfo = Field(default_factory=OvertimeInfo)
    training_information: TrainingInfo = Field(default_factory=TrainingInfo)


class NonSalaryPart3(BaseModel):
    """Part 3: Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits"""
    homeoffice_information: HomeofficeInfo = Field(default_factory=HomeofficeInfo)
    contract_type_information: ContractTypeInfo = Field(default_factory=ContractTypeInfo)
    safety_information: SafetyInfo = Field(default_factory=SafetyInfo)
    childcare_information: ChildcareInfo = Field(default_factory=ChildcareInfo)
    ai_information: AIInfo = Field(default_factory=AIInfo)
    fringe_benefits_information: FringeBenefitsInfo = Field(default_factory=FringeBenefitsInfo)


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


class ModelOutputParseError(Exception):
    """Custom exception for model output parsing errors."""
    pass


# =============================================================================
# LLM PROMPT TEMPLATES
# =============================================================================
# Exact prompt templates for salary and non-salary extraction

SALARY_PROMPT = """Extract structured salary data from a JSON object derived from the Dutch CAO document.

    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no hallucination, no guessing, no markdown fences, no extra text.

    INPUTS
    Filename: {filename}
    Source text: {source_json}

    CRITICAL RULES
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
        - Missing values: Omit optional fields entirely. Only include optional fields with actual values.
        - Output ONLY valid JSON format matching the provided schema structure.

    TABLE SELECTION
        - Include ONLY standard/regular wage tables. 
        - EXCLUDE allowances, bonuses, overtime, irregular hours, reimbursements, and non-standard worker roles like apprentices, interns, trainees, or foremen.
        - If multiple tables exist for different worker types, time periods, education levels, job groups, steps, or age bands under this standard wage type, include all of them. 
        - Record the unit exactly as printed. If the same baseline is printed in multiple units for the SAME workers/period/step/education/age, choose ONE using this order: monthly > hourly > 4-week > weekly > annual.
        - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present). Keep tables that differ by time period, worker type, education level, job group/function scale, steps (periodieken/trede), age bands, or contract type.

    TABLE AGE GROUP SELECTION
        - Create distinct SalaryRow objects for each adult-eligible age band present:
            - Open-ended adult bands (e.g., “22+”, “21 and older”), OR
            - Bands that intersect ages 23-65.
        - IGNORE age and job groups limited to workers under 23 (e.g., "16-20", "20") unless the group is open-ended ("20+") or spans older ages ("18-65").

    TABLE JOB GROUPS, STEPS, EDUCATION, CONTRACT TYPE
        - Extract ALL job groups visible in the standard wage table.
        - If steps/trede (periodieken) are shown, create a separate SalaryRow per jobgroup × step × [worker type] (× [age] × [education] × [contract type]).
        - If education tiers (e.g., MBO/HBO) determine different wages, create separate rows per jobgroup × step × [worker type] × education (× [age] × [contract type]).
        - If contract permanency or contract hours (work arrangement) determine different wages, create separate rows per jobgroup × step × [worker type] × contract type (× [age] × [education]).
        - Worker type field: OMIT if the value is generic (e.g. 'employee', 'standard worker') AND there is only one worker type in the entire CAO. KEEP when it provides meaningful distinction between different worker categories.

    TABLE AMOUNTS, PERCENTAGES, DATES
        - Salary amount: output as a number using a dot as the decimal separator (e.g., 2300.00). Do NOT use quotes, commas or thousands separators.
        - increase_percent: include only if the table or a relating clause explicitly states a general % for that version.
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
        3) DETECT AGE GROUPS: Within each selected table, extract all age groups meeting the TABLE AGE GROUP SELECTION criteria.
        4) DETECT JOB GROUPS, STEPS, EDUCATION LEVELS & CONTRACT TYPES: For each table version, identify job groups, steps, worker types, education levels, and contract types following the TABLE JOB GROUP, STEP, EDUCATION, CONTRACT TYPE rules.
        5) CONSTRUCT TIMELINE STRUCTURE:
            5.1) Apply TABLE TIMELINE CONSTRUCTION rules to align job groups, steps, worker types, education levels, age bands, and contract types across table versions.
            5.2) Build one SalaryRow for every unique detected combination of (jobgroup × step × [worker type] × [age] × [education] × [contract type]).
            5.3) Build timeline: For each SalaryRow, create one SalaryPoint per version/time period where that combination appears, then normalize labels (jobgroup/step/worker type/age/education/contract type), deduplicate identical periods, and align all points that refer to the same combination across versions (no imputation).
        6) SORT & CLEAN each row's timeline chronologically by start_date. Omit or nullify any fields not explicitly printed in the source.
        7) VERIFY (SOURCE-GROUNDED) that every extracted number/date/percentage/unit/clause is explicitly present in the input. Remove or correct anything not grounded.
        8) VALIDATE (SCHEMA & JSON) that the output is a valid JSON object that conforms exactly to the Pydantic schema (keys, types, null/""/omit conventions).
        9) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY a single valid JSON. No comments, no trailing commas, no text before/after.
        - Do NOT include fields not defined above.
        - Schema summary (orientation only; responseSchema enforces structure):
            Output a single JSON object:
            {{
            "salary_information": [ SalaryRow, ... ]
            }}
        
    """


NON_SALARY_PROMPT = """Extract structured information from a JSON object derived from the Dutch CAO document.
    
    GOAL: Produce ONE JSON object that matches the exact field names, structure, and data types defined in the Pydantic schema. Output ONLY valid JSON (UTF-8), no explanations, no markdown fences, no extra text.

    INPUTS:
    Filename: {filename}
    Source text: {source_json}
    
    EXTRACT SECTIONS: {sections}

    CRITICAL RULES
        - Extract ONLY what is explicitly present in the CAO. Do NOT infer, guess, or hallucinate.
        - Copy numbers/dates/percentages/units EXACTLY as written. Preserve all values literally.
        - Dates MUST be formatted as YYYY-MM-DD (omit or "" if missing).
        - Be precise: no paraphrasing of quantitative terms; no decorative characters or separator lines.
        - Output ONLY valid JSON format matching the provided schema structure.

    EXTRACTION GUIDELINES
        - Extract factual information for each field based on the schema descriptions.
        - Include relevant conditions, exceptions, and legal references in note fields.
        - Missing values: Omit optional fields entirely. Only include optional fields with actual values.
        - Do NOT compare to statutory law or mark “above statutory” unless the CAO explicitly says so.

    AMOUNT & AMOUNT RANGE RULES
        - For Amount fields: record both value and unit as an object (e.g., {{"value": 500, "unit": "EUR one-off"}}).
        - For AmountRange fields: record min, max, and unit as an object (e.g., {{"min": 1, "max": 3, "unit": "months"}}).
        - If no value is present, omit the entire Amount/AmountRange object. Never output a unit without its value.
        - Value fields are numeric (float or null). Unit fields are strings.

    WORKER FOCUS & TYPICAL GROUP
        - Focus on “normal workers” (≈23-65 years, no small groups). If groups differ (e.g., Construction vs UTA) and a single typical cannot be clearly chosen, allow min/max ONLY for key metrics (e.g., notice periods, overtime allowances).
        - When present set heterogeneity_present_* = true when major worker groups have any different terms.
            - When heterogeneity_present_* = true: fill BOTH typical values AND min/max fields for key metrics. When false: fill typical values only; leave min/max as null. 
        - If present in {sections}: in pension_information, termination_information or overtime_information, first choose the typical worker/group using selection_rule_*. Preference order: majority_headcount (largest group) > office_vs_field_rule (core group) > base_tier (lowest service band for ages 23-65) > latest_year (most recent values) > other > unspecified (could not determine).
            - pension_information: From employee_contrib till premium_change_equal_split, populate data ONLY for this group.
            - overtime_information: From overtime_trigger_daily till overtime_allowance, populate data ONLY for this group.
            - termination_information: From employer_notice till heterogeneity_present_notice, populate data ONLY for this group.

    EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT)
        1) READ & ANCHOR
            - Read all general rules and field descriptions of the Pydantic schema.
            - Scan the input to understand its structure and identify corresponding sections.
        2) PROCESS sections in schema order of {sections} (Use a 1→1 mapping between input and output sections, except that wage_information feeds two outputs: bonuses_info and wage_scales_info):
            - Capture literals exactly (numbers, percentages, units, dates).
            - Apply WORKER FOCUS & TYPICAL GROUP rules (heterogeneity, selection rules, pension consistency, overtime consistency).
            - Apply EXTRACTION GUIDELINES, AMOUNT & AMOUNT RANGE RULES, DATA TYPES & MISSING VALUES, and string fields (exact tokens; else "other"/"unspecified").
        3) INCLUDE CROSS-REFERENCED CONTENT: Check if relevant information for any {sections} appears elsewhere in the input. Include it only if contextually consistent and clearly related.
        4) CROSS-FIELD CONSISTENCY
	        - Ensure amounts, ranges, and units are internally coherent (min ≤ typ ≤ max).
	        - Validate all dates (YYYY-MM-DD).
	    5) VERIFY (SOURCE-GROUNDED)
	        - Confirm every extracted number/date/percentage/unit/clause is explicitly present in the input.
	        - Remove or correct anything not grounded.
	    6) VALIDATE (SCHEMA & JSON)
	        - Build one JSON object that conforms exactly to the Pydantic schema (keys, types, null/””/omit conventions).
	        - JSON is UTF-8, syntactically valid (balanced brackets, no trailing commas).
	    7) OUTPUT only the final JSON.

    JSON OUTPUT REQUIREMENTS
        - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
        - Ensure brackets/commas are correct; no trailing commas; all fields present.
    """


# =============================================================================
# LLM EXTRACTION FUNCTIONS
# =============================================================================
# Functions for calling the LLM and processing responses

def extract_salary_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> List[dict]:
    """Extract salary information from JSON using LLM."""
    
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
        return []
    
    if not check_token_limit(salary_text, filename):
        return []
    
    try:
        base_prompt = SALARY_PROMPT.format(filename=filename, source_json=salary_text)
    except Exception as e:
        raise
    
    model_params = get_model_parameters()
    
    
    # Use proper safety settings format for newer google-genai API
    safety_settings = [
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
        response = client.models.generate_content(
            model=MODEL,
            contents=base_prompt,
            config=config
        )
        processing_time = time.time() - start_time
        
        
        # Log detailed response information
        log_api_response_details(response, filename, processing_time)
        
        
        # Check for truncation
        if check_response_truncation(response, filename):
            raise Exception("Response truncated - incomplete JSON")
        
        # Check if response has parsed attribute (structured output)
        if hasattr(response, 'parsed') and response.parsed is not None:
            result = [row.model_dump() for row in response.parsed.salary_information]
            
            # Validate schema - salary_information can be empty if no salary data exists
            # Empty array is valid - some CAOs may not have salary information
            
            print(f'  Salary: Schema validation passed - {len(result)} salary entries')
            
            # Log successful salary extraction
            if context and 'performance_monitor' in context:
                file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
                context['performance_monitor'].log_analysis(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=getattr(response, 'usage_metadata', None),
                    success=True,
                    analysis_type="salary",
                    api_key_used=context.get('key_number', 1),
                    process_id=context.get('process_id', 0),
                    cao_number="",  # Will be extracted from file path if needed
                    model="gemini-2.5-flash",
                    parameters=model_params
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
                        result = [row.model_dump() for row in salary_schema.salary_information]
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
                            salary_content = salary_match.group(1)
                            # Try to parse individual salary objects
                            # This is a simplified approach - in production you might want more sophisticated parsing
                            log_analysis_error(filename, f"Partial salary data found but couldn't parse: {e}", response.text[:1000])
                        else:
                            log_analysis_error(filename, f"JSON parsing failed and no salary data found: {e}", response.text[:1000])
                    except Exception as parse_error:
                        log_analysis_error(filename, f"Complete parsing failure: {e}", response.text[:1000])
                except Exception as e:
                    log_analysis_error(filename, f"Schema validation failed: {e}", response.text[:1000])
            else:
                log_analysis_error(filename, "No structured output received from model", "")
                return []
            
    except Exception as e:
        last_error = e  # Capture the initial error
        last_error_message = None  # Track error message for retry guidance
        
        # Retry logic with proper attempt tracking
        for attempt in range(model_params["max_retries"] + 1):
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters(attempt)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance(last_error_message)
                    if retry_guidance:
                        print(f'  INFO: Adding retry guidance for: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt  # Reset to original
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                
                # print(f'  DEBUG: Model params: {adjusted_params}')
                
                # Use proper safety settings format for newer google-genai API
                safety_settings = [
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
                    "response_schema": SalaryExtractionSchema,
                    "safety_settings": safety_settings  # Include safety settings in config
                }
                
                print(f'  DEBUG: Making API call...')
                
                start_time = time.time()
                response = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config
                )
                processing_time = time.time() - start_time
                
                print(f'  DEBUG: API response received')
                print(f'  DEBUG: Response type: {type(response)}')
                # print(f'  DEBUG: Response attributes: {dir(response)}')
                
                # Log detailed response information
                log_api_response_details(response, filename, processing_time)
                
                # Check for truncation
                if check_response_truncation(response, filename):
                    print(f'  DEBUG: Response appears to be truncated, will retry with different parameters')
                    raise Exception("Response truncated - incomplete JSON")
                
                # Check if response has parsed attribute (structured output)
                if hasattr(response, 'parsed') and response.parsed is not None:
                    print(f'  DEBUG: Response has parsed attribute')
                    result = [row.model_dump() for row in response.parsed.salary_information]
                    print(f'  DEBUG: Parsed {len(result)} salary rows')
                    
                    # Log successful salary extraction
                    if context and 'performance_monitor' in context:
                        file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
                        
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
                            analysis_type="salary",
                            api_key_used=context.get('key_number', 1),
                            process_id=context.get('process_id', 0),
                            cao_number="",  # Will be extracted from file path if needed
                            model="gemini-2.5-flash",
                            parameters=log_params
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e  # Update last error for each attempt
                last_error_message = str(e)  # Capture error message for retry guidance
                print(f'  DEBUG: Attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global process_quota_flags
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  DEBUG: Quota exhausted during salary extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    if handle_llm_errors(e, attempt, 5, context=filename):
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  DEBUG: All attempts failed')
                    break
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed salary extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters(4)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance(last_error_message)
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
                cao_number="",
                model="gemini-2.5-flash",
                parameters=final_params
            )
        
        return []


def extract_nonsalary_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> dict:
    """Extract non-salary information from JSON using LLM."""
    
    # Extract all non-wage sections (including wage_information for wage scales context)
    non_salary_text = ""
    sections_to_extract = [
        'general_information', 'General information', 'general information', 'GENERAL_INFORMATION',
        'wage_information', 'Wage information', 'wage information', 'WAGE_INFORMATION',
        'pension_information', 'Pension information', 'pension information', 'PENSION_INFORMATION',
        'leave_information', 'Leave information', 'leave information', 'LEAVE_INFORMATION',
        'termination_information', 'Termination information', 'termination information', 'TERMINATION_INFORMATION',
        'overtime_information', 'Overtime information', 'overtime information', 'OVERTIME_INFORMATION',
        'training_information', 'Training information', 'training information', 'TRAINING_INFORMATION',
        'homeoffice_information', 'Homeoffice information', 'homeoffice information', 'HOMEOFFICE_INFORMATION',
        'contract_type_information', 'Contract type information', 'contract type information', 'CONTRACT_TYPE_INFORMATION',
        'fringe_benefits_information', 'Fringe benefits information', 'fringe benefits information', 'FRINGE_BENEFITS_INFORMATION',
        'safety_information', 'Safety information', 'safety information', 'SAFETY_INFORMATION',
        'childcare_information', 'Childcare information', 'childcare information', 'CHILDCARE_INFORMATION',
        'AI_information', 'AI information', 'ai information', 'ai_INFORMATION',
    ]
    
    for key in sections_to_extract:
        if key in json_obj:
            value = json_obj[key]
            
            if isinstance(value, list):
                flat_value = []
                for item in value:
                    if isinstance(item, list):
                        flat_value.extend(item)
                    elif isinstance(item, str):
                        flat_value.append(item)
                    else:
                        flat_value.append(str(item))
                non_salary_text += f'== {key} ==\n' + '\n'.join(flat_value) + '\n\n'
            elif isinstance(value, str):
                non_salary_text += f'== {key} ==\n{value}\n\n'
                print(f'  DEBUG: Added section from string, total length now: {len(non_salary_text)}')
    
    print(f'  DEBUG: Final non-salary text length: {len(non_salary_text)}')
    if non_salary_text.strip():
        print(f'  DEBUG: Non-salary text preview: {non_salary_text[:200]}...')
    else:
        print(f'  DEBUG: No non-salary text found!')
        return NonSalaryExtractionSchema().model_dump()
    
    if not check_token_limit(non_salary_text, filename):
        return NonSalaryExtractionSchema().model_dump()
    
    base_prompt = NON_SALARY_PROMPT.format(filename=filename, source_json=non_salary_text)
    
    model_params = get_model_parameters()
    
    # Use proper safety settings format for newer google-genai API
    safety_settings = [
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
    
    config = {
        "temperature": model_params["temperature"],
        "top_p": model_params["top_p"],
        "top_k": model_params["top_k"],
        "max_output_tokens": 65536,  # Increased to maximum for Gemini 2.5 Flash
        "candidate_count": model_params["candidate_count"],
        "seed": model_params["seed"],
        "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
        "response_mime_type": "application/json",
        "response_schema": NonSalaryExtractionSchema,
        "safety_settings": safety_settings  # Include safety settings in config
    }
    
    try:
        start_time = time.time()
        response = client.models.generate_content(
            model=MODEL,
            contents=base_prompt,
            config=config
        )
        processing_time = time.time() - start_time
        
        # Log detailed response information
        log_api_response_details(response, f"{filename} (non-salary)", processing_time)
        
        # Check for truncation
        if check_response_truncation(response, filename):
            raise Exception("Response truncated - incomplete JSON")
        
        # Check if response has parsed attribute (structured output)
        if hasattr(response, 'parsed') and response.parsed is not None:
            result = response.parsed.model_dump()
            
            # Log successful non-salary extraction
            if context and 'performance_monitor' in context:
                file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
                context['performance_monitor'].log_analysis(
                    filename=filename,
                    file_size_mb=file_size_mb,
                    processing_time=processing_time,
                    usage_metadata=getattr(response, 'usage_metadata', None),
                    success=True,
                    analysis_type="non_salary",
                    api_key_used=context.get('key_number', 1),
                    process_id=context.get('process_id', 0),
                    cao_number="",  # Will be extracted from file path if needed
                    model="gemini-2.5-flash"
                )
            
            return result
        else:
            print(f'  DEBUG: No parsed attribute in non-salary response')
            if hasattr(response, 'text'):
                print(f'  DEBUG: Non-salary response text length: {len(response.text) if response.text else 0}')
                if response.text:
                    print(f'  DEBUG: Non-salary response text preview: {response.text[:300]}...')
                    # Try to parse the text manually
                    try:
                        cleaned_text = response.text.strip()
                        
                        # Remove markdown code fences if present
                        if cleaned_text.startswith('```'):
                            lines = cleaned_text.split('\n')
                            cleaned_text = '\n'.join(lines[1:-1]).strip()
                        
                        parsed_json = json.loads(cleaned_text)
                        print(f'  DEBUG: Successfully parsed response text manually')
                        # Validate against schema
                        schema = NonSalaryExtractionSchema(**parsed_json)
                        result = schema.model_dump()
                        print(f'  DEBUG: Non-salary manual parse result keys: {list(result.keys())}')
                        return result
                    except json.JSONDecodeError as e:
                        print(f'  DEBUG: Failed to parse response text as JSON: {e}')
                    except Exception as e:
                        print(f'  DEBUG: Failed to validate parsed JSON against schema: {e}')
            log_analysis_error(filename, "No structured output received from model for non-salary", "")
            return NonSalaryExtractionSchema().model_dump()
            
    except Exception as e:
        print(f'  DEBUG: Non-salary API call failed with error: {type(e).__name__}: {e}')
        last_error = e  # Capture the initial error
        last_error_message = None  # Track error message for retry guidance
        
        # Retry logic with proper attempt tracking
        for attempt in range(model_params["max_retries"] + 1):
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters(attempt)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance(last_error_message)
                    if retry_guidance:
                        print(f'  INFO: Adding retry guidance for: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt  # Reset to original
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = [
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
                    "response_schema": NonSalaryExtractionSchema,
                    "safety_settings": safety_settings  # Include safety settings in config
                }
                
                print(f'  DEBUG: Making non-salary API call...')
                
                start_time = time.time()
                response = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config
                )
                processing_time = time.time() - start_time
                
                print(f'  DEBUG: Non-salary API response received')
                print(f'  DEBUG: Non-salary response type: {type(response)}')
                print(f'  DEBUG: Non-salary response attributes: {dir(response)}')
                
                # Log detailed response information
                log_api_response_details(response, f"{filename} (non-salary)", processing_time)
                
                # Check for truncation
                if check_response_truncation(response, filename):
                    print(f'  DEBUG: Non-salary response appears to be truncated, will retry with different parameters')
                    raise Exception("Response truncated - incomplete JSON")
                
                # Check if response has parsed attribute (structured output)
                if hasattr(response, 'parsed') and response.parsed is not None:
                    print(f'  DEBUG: Non-salary response has parsed attribute')
                    result = response.parsed.model_dump()
                    print(f'  DEBUG: Non-salary parsed result keys: {list(result.keys())}')
                    
                    # Log successful non-salary extraction
                    if context and 'performance_monitor' in context:
                        file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
                        
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
                            analysis_type="non_salary",
                            api_key_used=context.get('key_number', 1),
                            process_id=context.get('process_id', 0),
                            cao_number="",  # Will be extracted from file path if needed
                            model="gemini-2.5-pro",
                            parameters=log_params
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e  # Update last error for each attempt
                last_error_message = str(e)  # Capture error message for retry guidance
                print(f'  DEBUG: Non-salary attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global process_quota_flags
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  DEBUG: Quota exhausted during non-salary extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    if handle_llm_errors(e, attempt, 5, context=filename):
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  DEBUG: All non-salary attempts failed')
                    break
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All non-salary retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed non-salary extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)  # Rough estimate
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters(4)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance(last_error_message)
                if final_guidance:
                    final_params['retry_guidance_used'] = final_error_type
            
            context['performance_monitor'].log_analysis(
                filename=filename,
                file_size_mb=file_size_mb,
                processing_time=0,  # No processing time available for failures
                usage_metadata=None,
                success=False,
                analysis_type="non_salary",
                error_message=f"All retry attempts failed: {type(last_error).__name__}: {last_error}",
                api_key_used=context.get('key_number', 1),
                process_id=context.get('process_id', 0),
                cao_number="",
                model="gemini-2.5-flash",
                parameters=final_params
            )
        
        return {}


def extract_nonsalary_part1_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> dict:
    """Extract non-salary part 1 information (General, Bonuses, Wage Scales, Pension, Termination) from JSON using LLM."""
    
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
    safety_settings = [
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
        response = client.models.generate_content(
            model=MODEL,
            contents=base_prompt,
            config=config
        )
        processing_time = time.time() - start_time
        
        # Check for truncation
        if check_response_truncation(response, filename):
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
                    cao_number="",
                    model="gemini-2.5-flash",
                    parameters=model_params
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
        for attempt in range(model_params["max_retries"] + 1):
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters(attempt)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance(last_error_message)
                    if retry_guidance:
                        print(f'  Part 1: Adding retry guidance for: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = [
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
                response = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config
                )
                processing_time = time.time() - start_time
                
                # Check for truncation
                if check_response_truncation(response, filename):
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
                            cao_number="",
                            model="gemini-2.5-flash",
                            parameters=log_params
                        )
                    
                    print(f'  Part 1 extraction completed successfully (attempt {attempt + 1})')
                    return result
                    
            except Exception as e:
                last_error = e
                last_error_message = str(e)
                print(f'  Part 1: Attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global process_quota_flags
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  Part 1: Quota exhausted, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    if handle_llm_errors(e, attempt, 5, context=filename):
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  Part 1: All attempts failed')
                    break
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All part 1 retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed part 1 extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters(4)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance(last_error_message)
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
                cao_number="",
                model="gemini-2.5-flash",
                parameters=final_params
            )
        
        print(f'  Part 1 extraction failed')
        return {}


def extract_nonsalary_part2_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> dict:
    """Extract non-salary part 2 information (Leave, Overtime, Training) from JSON using LLM."""
    
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
    safety_settings = [
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
        response = client.models.generate_content(
            model=MODEL,
            contents=base_prompt,
            config=config
        )
        processing_time = time.time() - start_time
        
        # Log detailed response information
        log_api_response_details(response, f"{filename} (non-salary-part2)", processing_time)
        
        # Check for truncation
        if check_response_truncation(response, filename):
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
                    cao_number="",
                    model="gemini-2.5-flash",
                    parameters=model_params
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
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters(attempt)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance(last_error_message)
                    if retry_guidance:
                        print(f'  INFO: Adding retry guidance for part 2: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = [
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
                response = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config
                )
                processing_time = time.time() - start_time
                
                print(f'  DEBUG: Part 2 API response received')
                
                # Log detailed response information
                log_api_response_details(response, f"{filename} (non-salary-part2)", processing_time)
                
                # Check for truncation
                if check_response_truncation(response, filename):
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
                            cao_number="",
                            model="gemini-2.5-flash",
                            parameters=log_params
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e
                last_error_message = str(e)
                print(f'  DEBUG: Part 2 attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global process_quota_flags
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  DEBUG: Quota exhausted during part 2 extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    if handle_llm_errors(e, attempt, 5, context=filename):
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  DEBUG: All part 2 attempts failed')
                    break
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All part 2 retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed part 2 extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters(4)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance(last_error_message)
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
                cao_number="",
                model="gemini-2.5-flash",
                parameters=final_params
            )
        
        return {}


def extract_nonsalary_part3_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> dict:
    """Extract non-salary part 3 information (Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits) from JSON using LLM."""
    
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
    safety_settings = [
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
        response = client.models.generate_content(
            model=MODEL,
            contents=base_prompt,
            config=config
        )
        processing_time = time.time() - start_time
        
        # Log detailed response information
        log_api_response_details(response, f"{filename} (non-salary-part3)", processing_time)
        
        # Check for truncation
        if check_response_truncation(response, filename):
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
                    cao_number="",
                    model="gemini-2.5-flash",
                    parameters=model_params
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
            try:
                # Get adjusted parameters for this attempt
                adjusted_params = get_adjusted_parameters(attempt)
                
                # Generate retry guidance (only if attempt >= 2)
                retry_guidance = ""
                error_type = ""
                if attempt >= 2 and last_error_message:
                    retry_guidance, error_type = get_retry_guidance(last_error_message)
                    if retry_guidance:
                        print(f'  INFO: Adding retry guidance for part 3: {error_type}')
                
                # Recreate prompt with guidance if applicable
                prompt = base_prompt
                if retry_guidance:
                    prompt += f"\n\n{retry_guidance}"
                                
                # Use proper safety settings format for newer google-genai API
                safety_settings = [
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
                response = client.models.generate_content(
                    model=MODEL,
                    contents=prompt,
                    config=config
                )
                processing_time = time.time() - start_time
                
                print(f'  DEBUG: Part 3 API response received')
                
                # Log detailed response information
                log_api_response_details(response, f"{filename} (non-salary-part3)", processing_time)
                
                # Check for truncation
                if check_response_truncation(response, filename):
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
                            cao_number="",
                            model="gemini-2.5-flash",
                            parameters=log_params
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e
                last_error_message = str(e)
                print(f'  DEBUG: Part 3 attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global process_quota_flags
                process_id = context.get('process_id', 0) if context else 0
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    print(f'  DEBUG: Quota exhausted during part 3 extraction, stopping retries')
                    break
                    
                if attempt < 4:  # Not the last attempt
                    if handle_llm_errors(e, attempt, 5, context=filename):
                        continue  # Retry
                    else:
                        break  # Don't retry
                else:
                    # Last attempt failed
                    print(f'  DEBUG: All part 3 attempts failed')
                    break
        
        # If we get here, all attempts failed
        log_analysis_error(filename, f"All part 3 retry attempts failed: {type(last_error).__name__}: {last_error}", "")
        
        # Log failed part 3 extraction
        if context and 'performance_monitor' in context:
            file_size_mb = len(str(json_obj)) / (1024 * 1024)
            
            # Get final attempt parameters for logging (attempt 4 = 5th try)
            final_params = get_adjusted_parameters(4)
            if last_error_message:
                final_guidance, final_error_type = get_retry_guidance(last_error_message)
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
                cao_number="",
                model="gemini-2.5-flash",
                parameters=final_params
            )
        
        return {}


def analyze_cao_json(json_text: str, filename: str = None) -> dict:
    """
    Main analysis function that processes JSON text and returns structured data.
    
    Args:
        json_text: Raw JSON string content from llm_Extracted/new_flow
        filename: Optional filename for context
        
    Returns:
        dict: Combined extraction results with structure:
              {"salary_extraction": List[dict], "non_salary_extraction": dict}
    """
    if not json_text or not json_text.strip():
        return {
            "salary_extraction": [],
            "non_salary_extraction": NonSalaryExtractionSchema().model_dump()
        }
    
    try:
        # Parse JSON text
        json_obj = json.loads(json_text)
        
        # Setup LLM client
        api_key, key_number = setup_environment()
        client = setup_gemini_client(api_key)
        
        # Extract salary information
        salary_extracted = extract_salary_from_json(json_obj, filename or "unknown", client, None)
        
        # Extract non-salary information
        nonsalary_extracted = extract_nonsalary_from_json(json_obj, filename or "unknown", client, None)
        
        return {
            "salary_extraction": salary_extracted,
            "non_salary_extraction": nonsalary_extracted
        }
        
    except json.JSONDecodeError as e:
        log_analysis_error(filename or "unknown", f"JSON parsing failed: {e}", json_text)
        raise ModelOutputParseError(f"Invalid JSON input: {e}")
    except Exception as e:
        log_analysis_error(filename or "unknown", f"Analysis failed: {e}", json_text)
        raise


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
    except:
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

def save_extraction_json(data: dict, filename: str, extraction_type: str, cao_number: str = None):
    """
    Save extracted JSON data to appropriate folders for analysis.
    
    Args:
        data: Extracted data to save
        filename: Original filename
        extraction_type: 'salary', 'non_salary_part1', 'non_salary_part2', or 'non_salary_part3'
        cao_number: CAO number for folder organization
    """
    try:
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
        
    except Exception as e:
        print(f'  DEBUG: Failed to save {extraction_type} extraction: {e}')


def check_missing_extraction_parts(filename: str, cao_number: str) -> dict:
    """
    Check which extraction parts are missing for a given file.
    
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
    
    return missing


def process_single_file(json_file: Path, cao_folder: Path, client, cao_info_mapping: dict, 
                       config: AnalysisConfig, context: Dict[str, Any]) -> bool:
    """Process a single JSON file and save LLM extraction results."""
    filename = json_file.name
    cao_number = cao_folder.name
    
    print(f'  {cao_number}: {filename}')
    
    try:
        # Read JSON file
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Check which parts are missing
        missing_parts = check_missing_extraction_parts(filename, cao_number)

        # If all parts exist, skip entirely
        if not any(missing_parts.values()):
            print(f'  {cao_number}: Skipping {filename} (all parts already processed)')
            return True
            
        # Print which parts need processing
        parts_to_process = [k for k, v in missing_parts.items() if v]
        print(f'  {cao_number}: Processing missing parts: {", ".join(parts_to_process)}')

        # Check if quota was exhausted for this process
        global process_quota_flags
        process_id = context.get('process_id', 0) if context else 0
        if process_id in process_quota_flags and process_quota_flags[process_id]:
            print(f'  {cao_number}: 🛑 QUOTA EXHAUSTED for Process {process_id} - Stopping this process')
            return False  # Stop processing this process only

        # Extract salary information (only if missing)
        if missing_parts['salary']:
            salary_extracted = extract_salary_from_json(json_data, filename, client, context)
            salary_success = salary_extracted is not None
            print(f'  {cao_number}: Salary extraction {"completed" if salary_success else "failed"}')
        else:
            salary_extracted = None
            salary_success = None  # Not run
            print(f'  {cao_number}: Salary extraction skipped (already exists)')

        # Extract non-salary part 1 (only if missing)
        if missing_parts['part1']:
            part1_extracted = extract_nonsalary_part1_from_json(json_data, filename, client, context)
            part1_success = part1_extracted is not None
            print(f'  {cao_number}: Non-salary part 1 extraction {"completed" if part1_success else "failed"}')
        else:
            part1_extracted = None
            part1_success = None
            print(f'  {cao_number}: Non-salary part 1 extraction skipped (already exists)')

        # Extract non-salary part 2 (only if missing)
        if missing_parts['part2']:
            part2_extracted = extract_nonsalary_part2_from_json(json_data, filename, client, context)
            part2_success = part2_extracted is not None
            print(f'  {cao_number}: Non-salary part 2 extraction {"completed" if part2_success else "failed"}')
        else:
            part2_extracted = None
            part2_success = None
            print(f'  {cao_number}: Non-salary part 2 extraction skipped (already exists)')

        # Extract non-salary part 3 (only if missing)
        if missing_parts['part3']:
            part3_extracted = extract_nonsalary_part3_from_json(json_data, filename, client, context)
            part3_success = part3_extracted is not None
            print(f'  {cao_number}: Non-salary part 3 extraction {"completed" if part3_success else "failed"}')
        else:
            part3_extracted = None
            part3_success = None
            print(f'  {cao_number}: Non-salary part 3 extraction skipped (already exists)')
        
        # Save each part separately (only if extraction was attempted and successful)
        if salary_success is True:
            save_extraction_json({'salary_information': salary_extracted}, filename, 'salary', cao_number)
        elif salary_success is False:
            print(f'  {cao_number}: Skipping salary file save due to extraction failure')

        if part1_success is True:
            save_extraction_json(part1_extracted, filename, 'non_salary_part1', cao_number)
        elif part1_success is False:
            print(f'  {cao_number}: Skipping part 1 file save due to extraction failure')

        if part2_success is True:
            save_extraction_json(part2_extracted, filename, 'non_salary_part2', cao_number)
        elif part2_success is False:
            print(f'  {cao_number}: Skipping part 2 file save due to extraction failure')

        if part3_success is True:
            save_extraction_json(part3_extracted, filename, 'non_salary_part3', cao_number)
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
                model="gemini-2.5-flash"
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
    parser = argparse.ArgumentParser(description='CAO Data Analysis with Schema-Driven Extraction')
    parser.add_argument('--key_number', type=int, default=7, help='API key number to use')
    parser.add_argument('--process_id', type=int, default=0, help='Process ID for work distribution')
    parser.add_argument('--total_processes', type=int, default=1, help='Total number of parallel processes')
    parser.add_argument('--max_files', type=int, help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    try:
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
        
        print(f'Process {args.process_id + 1}: Processing {len(process_files)} files')
        
        # Process files
        successful_analyses = 0
        failed_files = []
        
        for cao_folder, json_file in process_files:
            # Check for quota exhaustion flag before processing each file
            global process_quota_flags
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
        
    except Exception as e:
        # Check if this is a quota exhaustion error - if so, exit gracefully
        if args.process_id in process_quota_flags and process_quota_flags[args.process_id]:
            print(f'Process {args.process_id + 1} completed with QUOTA EXHAUSTION due to error: {e}')
        else:
            print(f'Fatal error: {e}')
            sys.exit(1)


def cli_test_fixture():
    """
    CLI helper for testing with fixture files.
    Usage: python -m p4_analysis --fixture tests/fixtures/example_salary.json
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Test p4_analysis with fixture files')
    parser.add_argument('--fixture', type=str, help='Path to fixture JSON file')
    parser.add_argument('--key_number', type=int, default=7, help='API key number to use')
    parser.add_argument('--process_id', type=int, default=0, help='Process ID')
    parser.add_argument('--total_processes', type=int, default=1, help='Total number of processes')
    parser.add_argument('--max_files', type=int, help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    if args.fixture:
        # Test with fixture file
        try:
            with open(args.fixture, 'r', encoding='utf-8') as f:
                json_text = f.read()
            
            print(f"Testing with fixture: {args.fixture}")
            result = analyze_cao_json(json_text, args.fixture)
            
            # Save extracted JSON data for analysis
            base_name = Path(args.fixture).stem
            save_extraction_json(result['salary_extraction'], base_name, 'salary')
            save_extraction_json(result['non_salary_extraction'], base_name, 'non_salary')
            
            print("\n=== EXTRACTION RESULTS ===")
            salary_count = len(result['salary_extraction'])
            # Count only non-empty values in non-salary extraction
            nonsalary_count = 0
            for key, value in result['non_salary_extraction'].items():
                if isinstance(value, dict):
                    for subkey, subvalue in value.items():
                        if subvalue and subvalue != "":  # Only count non-empty values
                            nonsalary_count += 1
                elif value and value != "":  # Only count non-empty values
                    nonsalary_count += 1
            print(f"Salary rows: {salary_count}")
            print(f"Non-salary fields with data: {nonsalary_count}")
            
            print("\n=== SALARY EXTRACTION ===")
            if salary_count > 0:
                for i, row in enumerate(result['salary_extraction']):
                    print(f"Row {i+1}: {row.get('jobgroup', 'N/A')} - {row.get('salary_1', 'N/A')} {row.get('salary_1_unit', '')}")
            else:
                print("No salary data extracted")
            
            print("\n=== NON-SALARY EXTRACTION ===")
            has_nonsalary_data = False
            for key, value in result['non_salary_extraction'].items():
                if isinstance(value, dict):
                    has_data = False
                    for subkey, subvalue in value.items():
                        if subvalue:  # Only show non-empty values
                            if not has_data:
                                print(f"{key}:")
                                has_data = True
                                has_nonsalary_data = True
                            print(f"  {subkey}: {subvalue}")
                elif value:  # Only show non-empty values
                    print(f"{key}: {value}")
                    has_nonsalary_data = True
            
            if not has_nonsalary_data:
                print("No non-salary data extracted")
            
            # Determine if extraction was successful
            total_extracted = salary_count + nonsalary_count
            if total_extracted > 0:
                print(f"\n✓ Fixture test completed successfully! Extracted {total_extracted} data points.")
            else:
                print(f"\n⚠ Fixture test completed but no data was extracted. Check API quota and retry.")
                return 1
            
        except Exception as e:
            print(f"✗ Fixture test failed: {e}")
            return 1
    else:
        # Run normal main function
        main()
    
    return 0


if __name__ == "__main__":
    exit(cli_test_fixture())
