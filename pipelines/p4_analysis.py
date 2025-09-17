"""
CAO Data Analysis - LLM Extraction Pipeline (p4_analysis.py)

This script performs schema-driven LLM extraction on CAO JSON files.
It extracts salary and non-salary information using Google Gemini API.

USAGE:
    Single process:
        python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 1

    Multi-process:
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 6 2>&1 | tee log1.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 2 --process_id 1 --total_processes 6 2>&1 | tee log2.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 3 --process_id 2 --total_processes 6 2>&1 | tee log3.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 4 --process_id 3 --total_processes 6 2>&1 | tee log4.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 5 --process_id 4 --total_processes 6 2>&1 | tee log5.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 6 --process_id 5 --total_processes 6 2>&1 | tee log6.txt &

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
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Third-party imports for environment variables, file locking, and data validation
import pandas as pd
import yaml
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv

# Google Gemini API imports
from google import genai
from google.genai import types

# Import performance monitoring
from monitoring.monitoring_3_1 import PerformanceMonitor

# =============================================================================
# GLOBAL FLAGS
# =============================================================================
# Global flag to signal quota exhaustion for graceful shutdown
quota_exhausted_flag = False

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
        "max_tokens": None,
        "candidate_count": 1,
        "seed": 42,
        "presence_penalty": 0.0,
        "frequency_penalty": 0.0,
        "thinking_budget": -1,  # Dynamic thinking (like p3)
        "max_retries": 5
    }



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
            # Set global flag to trigger graceful shutdown
            global quota_exhausted_flag
            quota_exhausted_flag = True
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
        print(f"  📊 API Response Details for {filename}:")
        print(f"    - Input tokens: {input_tokens} | Output tokens: {output_tokens} (max: 65536)")
        print(f"    - Response size: {response_size} chars | Finish reason: {finish_reason}")
        print(f"    - Processing time: {processing_time:.1f}s | Status: SUCCESS")
        
    except Exception as e:
        print(f"  ⚠️  Failed to log response details for {filename}: {e}")


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
                return True
    
    # Check if response text is empty or very short
    if hasattr(response, 'text') and response.text:
        if len(response.text.strip()) < 50:  # Very short response
            print(f'  DEBUG: Response text is very short ({len(response.text)} chars) for {filename}')
            return True
    
    # Check if parsed attribute is empty when we expect structured output
    if hasattr(response, 'parsed') and response.parsed is None:
        print(f'  DEBUG: No parsed structured output for {filename}')
        return True
    
    return False


def validate_llm_response_json(content: str, filename: str) -> dict:
    """
    Validate LLM response JSON for completeness and validity.
    
    Args:
        content: Raw response content from LLM
        filename: Filename for context in error messages
        
    Returns:
        dict: {'is_valid': bool, 'error': str or None}
    """
    # Check if content is empty
    if not content or not content.strip():
        return {'is_valid': False, 'error': 'Empty content'}
    
    # Check if content starts with {
    if not content.strip().startswith('{'):
        return {'is_valid': False, 'error': 'Content does not start with {'}
    
    # Check if content ends with }
    if not content.strip().endswith('}'):
        return {'is_valid': False, 'error': 'Content does not end with } - JSON appears to be truncated'}
    
    # Try to parse JSON to validate structure
    try:
        json.loads(content)
        return {'is_valid': True, 'error': None}
    except json.JSONDecodeError as e:
        return {'is_valid': False, 'error': f'JSON parsing error: {str(e)}'}

# =============================================================================
# CONSTANTS
# =============================================================================
# Global configuration constants
MODEL = 'gemini-2.5-flash'  


# =============================================================================
# DATA SCHEMAS
# =============================================================================
# Pydantic schemas for structured extraction of CAO document information

class SalaryRow(BaseModel):
    """Schema for a single salary row representing one job group."""
    jobgroup: str = Field(default="", description="Job group name - if mentioned put descriptions in parentheses (e.g., 'F-45-9 (workers with high school diploma)')")    
    salary_1: str = Field(default="", description="Salary of the first job group listed in the earliest wage table")
    salary_1_unit: str = Field(default="", description="Unit for first salary")
    salary_1_startdate: str = Field(default="", description="Start date for first salary")
    salary_increment_1: str = Field(default="", description="Percentage increase of first salary in the earliest wage table")
    
    salary_2: str = Field(default="", description="Salary of the first job group listed in the second earliest wage table")
    salary_2_unit: str = Field(default="", description="Unit for second salary")
    salary_2_startdate: str = Field(default="", description="Start date for second salary")
    salary_increment_2: str = Field(default="", description="Percentage increase of second salary in the second earliest wage table")
    
    salary_3: str = Field(default="", description="Salary of the first job group listed in the third earliest wage table")
    salary_3_unit: str = Field(default="", description="Unit for third salary")
    salary_3_startdate: str = Field(default="", description="Start date for third salary")
    salary_increment_3: str = Field(default="", description="Percentage increase of third salary in the third earliest wage table")
    
    salary_4: str = Field(default="", description="Salary of the first job group listed in the fourth earliest wage table")
    salary_4_unit: str = Field(default="", description="Unit for fourth salary")
    salary_4_startdate: str = Field(default="", description="Start date for fourth salary")
    salary_increment_4: str = Field(default="", description="Percentage increase of fourth salary in the fourth earliest wage table")
    
    salary_5: str = Field(default="", description="Salary of the first job group listed in the fifth earliest wage table")
    salary_5_unit: str = Field(default="", description="Unit for fifth salary")
    salary_5_startdate: str = Field(default="", description="Start date for fifth salary")
    salary_increment_5: str = Field(default="", description="Percentage increase of salaries in the fifth earliest wage table")
    
    salary_6: str = Field(default="", description="Salary of the first job group listed in the sixth earliest wage table")
    salary_6_unit: str = Field(default="", description="Unit for sixth salary")
    salary_6_startdate: str = Field(default="", description="Start date for sixth salary")
    salary_increment_6: str = Field(default="", description="Percentage increase of sixth salary in the sixth earliest wage table")
    
    salary_7: str = Field(default="", description="Salary of the first job group listed in the seventh earliest wage table")
    salary_7_unit: str = Field(default="", description="Unit for seventh salary")
    salary_7_startdate: str = Field(default="", description="Start date for seventh salary")
    salary_increment_7: str = Field(default="", description="Percentage increase of seventh salary in the seventh earliest wage table")
    
    more_salaries: bool = Field(default=False, description="True ONLY if the job group has more than 7 salary steps (i.e., salary_1 … salary_7 are all filled and at least one additional salary exists); otherwise False")
    salary_note: str = Field(default="", description="Table-level salary context including calculation methods, effective dates, conditions, and any additional regular/standard wage increments beyond salary_increment_1,...,salary_increment_7")
    salary_age_group: str = Field(default="", description="Age group the salary of the first job group applies to")


class SalaryExtractionSchema(BaseModel):
    """Schema for salary extraction results."""
    salary_information: List[SalaryRow] = Field(default_factory=list)


class ContractInfo(BaseModel):
    """Schema for contract information."""
    start_date_contract: str = Field(default="", description="Contract start date")
    expiry_date_contract: str = Field(default="", description="Contract expiry/end date")


class PensionInfo(BaseModel):
    """Schema for pension information."""
    pension_scheme_basic: str = Field(default="", description="Basic pension scheme rules, premiums, development and structure (e.g., '50% pension premium paid by employee', 'Employees 21-68 eligible')")
    pension_scheme_plus: str = Field(default="", description="Additional or 'plus' pension scheme rules, premiums, development and structure (e.g., '2% additional premium', 'Generation Policy for 60+')")
    retire_age_basic: str = Field(default="", description="Retirement age for the basic pension scheme")
    retire_age_plus: str = Field(default="", description="Retirement age for the additional or 'plus' pension scheme")
    pension_age_group: str = Field(default="", description="Age group eligible for pension schemes (e.g., '21+')")


class LeaveInfo(BaseModel):
    """Schema for leave information."""
    maternity_leave: str = Field(default="", description="Child-related (Maternity, adoption, etc.) leave duration (e.g., '5 days of paid maternity leave', 'Additional 4 weeks in multiple births')")
    maternity_pay: str = Field(default="", description="Salary and benefits during child-related leave (e.g., '100% paid by employer', '70% UWV benefit')")
    maternity_note: str = Field(default="", description="Additional child-related leave rules (e.g., 'Vacation accrues during leave', 'leave may be split between partners')")
    vacation_time: str = Field(default="", description="Annual vacation time")
    vacation_unit: str = Field(default="", description="Unit of vacation time (e.g., 'weeks')")
    vacation_note: str = Field(default="", description="Additional vacation rules (e.g., 'plus public holidays for part time workers', '8% holiday allowance')")


class TerminationInfo(BaseModel):
    """Schema for termination information."""
    term_period_employer: str = Field(default="", description="Notice period required from employer - include special rules based on age, start date, or contract length")
    term_employer_note: str = Field(default="", description="Additional notes about employer termination (e.g., 'plus 1 month per year of service', 'Civil Code provisions apply')")
    term_period_worker: str = Field(default="", description="Notice period required from worker - include special rules based on age, start date, or contract length")
    term_worker_note: str = Field(default="", description="Additional notes about worker termination (e.g., 'Starting date for notice is always a Saturday', 'can be extended')")
    probation_period: str = Field(default="", description="Probation period duration (e.g., '2 months for indefinite contracts', '60 days')")
    probation_note: str = Field(default="", description="Additional notes about probation (e.g., 'Article 7 of the Civil Code applies', 'shorter notice period during probation')")


class OvertimeInfo(BaseModel):
    """Schema for overtime information."""
    overtime_compensation: str = Field(default="", description="Overtime pay and compensation rate (e.g., '1:1 Time Off In Lieu', '€25/hour')")
    max_hrs: str = Field(default="", description="Maximum working and overtime time (e.g., '52-hour weekly average if salary exceeds IP number 74', '16 hours/month')")
    min_hrs: str = Field(default="", description="Minimum working and overtime time (e.g., '96 hours/month')")
    shift_compensation: str = Field(default="", description="Shift compensations (e.g., '25% surcharge 8pm–10pm', 'Max 20 shifts per 4-weeks')")
    overtime_allowance_min: str = Field(default="", description="Minimum overtime and shift allowance (e.g., 'min 4-hour shift to qualify for night compensation')")
    overtime_allowance_max: str = Field(default="", description="Maximum overtime and shift allowance (e.g., 'max 10 hours a day')")


class TrainingInfo(BaseModel):
    """Schema for training information."""
    training: str = Field(default="", description="Training rights, budgets and requirements (e.g., '€175 per year budget', '5 days of mandatory safety training')")


class HomeofficeInfo(BaseModel):
    """Schema for homeoffice information."""
    Homeoffice: str = Field(default="", description="Home office and remote work rules (e.g., 'up to 2 days/week', 'Home office allowance of €3 per day', 'equipment provided')")


class NonSalaryExtractionSchema(BaseModel):
    """Schema for non-salary extraction results."""
    contract_information: ContractInfo = Field(default_factory=ContractInfo)
    pension_information: PensionInfo = Field(default_factory=PensionInfo)
    leave_information: LeaveInfo = Field(default_factory=LeaveInfo)
    termination_information: TerminationInfo = Field(default_factory=TerminationInfo)
    overtime_information: OvertimeInfo = Field(default_factory=OvertimeInfo)
    training_information: TrainingInfo = Field(default_factory=TrainingInfo)
    homeoffice_information: HomeofficeInfo = Field(default_factory=HomeofficeInfo)


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


def validate_input_paths(config: AnalysisConfig):
    """Validate that input/output paths exist and are accessible."""
    if not os.path.exists(config.input_folder):
        raise ValueError(f"Input folder does not exist: {config.input_folder}")
    
    # Ensure full directory tree exists (avoid race conditions across processes)
    config.output_folder.mkdir(parents=True, exist_ok=True)
    
    # Check if we can write to output folder (retry once if parent disappears)
    test_file = config.output_folder / ".test_write"
    try:
        test_file.write_text("test")
        test_file.unlink()
    except FileNotFoundError:
        # Recreate and retry once
        config.output_folder.mkdir(parents=True, exist_ok=True)
        test_file.write_text("test")
        test_file.unlink()
    except Exception as e:
        raise ValueError(f"Cannot write to output folder: {config.output_folder}, Error: {e}")


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

SALARY_PROMPT = """

    You are an information-extraction assistant. Input: text derived from a Dutch CAO (collective labour agreement) that contains wage tables and related wage text.

    TASK: Extract structured salary data from the provided text:
    Filename: {filename}
    Source text: {source_json}

    CRITICAL RULES:
         - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
         - For missing values: Use empty string "" for string fields and false for boolean fields.
         - Output ONLY valid JSON format matching the provided schema structure.

    UNIT & TABLE SELECTION:
        - Include ONLY standard/regular wage tables - EXCLUDE allowances, bonuses, overtime, reimbursements, and non-standard worker roles like apprentices or foremen.
        - If multiple tables exist for different time periods under this standard wage type, include all of them. 
        - Within standard wage tables extract salary information for all job groups.
        - Unit choice: if the same table exists in multiple units for the same workers/groups/periods/ages, choose the hourly version. If hourly is absent, KEEP the original unit as-is.
        - If salaries are presented as a range (e.g., "€2,000 – €2,400"), always extract the minimum value as the salary and state this in salary_note field.

    AGE GROUP SELECTION:
        - Select EXACTLY ONE age group per wage table, using this order of preference:
            1. Open-ended groups (e.g., "22+"), preferring the lowest starting age if multiple exist.
            2. If none exist, select the group that covers the widest span (e.g. '20 - 65' preferred over '20 - 50').
            3. If tied, select the group with the highest maximum age (e.g. '20 - 65' preferred over '20 - 60' and '22' preferred over '21').
        - IGNORE age and job groups limited to workers under 21 (e.g., "16-20", "20") unless the group is open-ended ("20+") or spans older ages ("18-65").
        - Extract data only for the selected age group; do not output multiple age groups.
        - NEVER borrow values across age groups. If a job group has no data for the chosen age group, leave its fields empty/null.
        - Record the chosen age group in the age_group field. If the chosen group changes across different wage tables/periods, note this in salary_note.

    NOTES & CONSISTENCY:
         - Salary_note, salary_age_group and more_salaries fields should be consistent across all job groups since they usually apply to entire wage tables (if this is not the case mention it in the salary_note field). 
         - No Gaps Rule: When multiple salary values are present for the same jobgroup, map them sequentially into salary_1, salary_2, salary_3, etc. without skipping any index.
         - Save dates in DD/MM/YYYY format.
         - If no suitable salary information is found at all, return an empty salary_information array but include one entry with only salary_note field filled as "NO SALARY INFO FOUND".
         - Be very concise and precise in your output, especially in lengthy fields (like salary_note).

    PROCESS — FOLLOW IN THIS ORDER:
        1. Filter tables: keep only eligible standard wage tables (apply UNIT & TABLE rules).
        2. For each included table: extract all job groups present (do not omit).
        3. For each table: select EXACTLY ONE age group using AGE GROUP rules.
        4. Collect table versions for the same standard table and sort them EARLIEST→LATEST (per TIME-PERIOD ORDERING).
        5. For each job group (aligned across versions as described): assign salary_1, salary_2, ... by chronological order of versions (observe NO GAPS rule), and fill for each salary_x: salary_x_unit, salary_x_startdate and salary_increment_x.
        6. Fill fields salary_note, salary_age_group and more_salaries

    FINAL CHECK: Before producing output, verify that all above constraints are satisfied. Then output a single JSON object conforming exactly to the schema.
    """


NON_SALARY_PROMPT = """

    You are an information-extraction assistant. Input: text derived from a Dutch CAO (collective labour agreement) that contains general contract info, pension, leave, termination, overtime, training and homeoffice sections.

    TASK: Extract structured data from the provided text:
    Filename: {filename}
    Source text: {source_json}

    CRITICAL RULES:
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess.
        - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
        - Output ONLY valid JSON format matching the provided schema structure.

    EXTRACTION GUIDELINES:
        - Extract factual information for each field based on the schema descriptions. Be concise.
        - Include relevant conditions, exceptions, and legal references in note fields.
        - For missing values use empty string "".
        - Save dates in DD/MM/YYYY format.

    FINAL CHECK: Before producing output, verify that all above constraints are satisfied. Then output a single JSON object conforming exactly to the schema.
    """


# =============================================================================
# LLM EXTRACTION FUNCTIONS
# =============================================================================
# Functions for calling the LLM and processing responses

def query_gemini_with_retry(client, prompt: str, filename: str, max_retries: int = 5) -> str:
    """
    Query Gemini model with retry logic and error handling.
    
    Args:
        client: Gemini client instance
        prompt: Prompt to send to the model
        filename: Filename for context in error messages
        max_retries: Maximum number of retry attempts
        
    Returns:
        str: Raw model response text
        
    Raises:
        Exception: If all retry attempts fail
    """
    model_params = get_model_parameters()
    
    # Define safety settings (same as p3)
    safety_settings = [
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE
        )
    ]
    
    for attempt in range(max_retries):
        try:
            config = {
                'temperature': model_params["temperature"],
                'top_p': model_params["top_p"],
                'top_k': model_params["top_k"],
                'max_output_tokens': model_params["max_tokens"],
                'candidate_count': model_params["candidate_count"],
                'seed': model_params["seed"],
                'presence_penalty': model_params["presence_penalty"],
                'frequency_penalty': model_params["frequency_penalty"],
                'thinking_config': types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
                'http_options': types.HttpOptions(timeout=300000),  # 5 minutes timeout
                'safety_settings': safety_settings
            }
            
            response = client.models.generate_content(
                model=model_params["model"],
                contents=prompt,
                config=config
            )
            
            if hasattr(response, 'text') and response.text and response.text.strip():
                return response.text
            else:
                raise ValueError('Empty or invalid model response')
                
        except Exception as e:
            if not handle_llm_errors(e, attempt, max_retries, context=filename):
                raise e
    
    raise ValueError(f'All {max_retries} retry attempts failed')


def clean_gemini_output(output: str) -> str:
    """
    Clean the Gemini model output by removing markdown and trailing commas.
    
    Args:
        output: Raw output from Gemini
        
    Returns:
        str: Cleaned output string
    """
    if output.strip().startswith('```'):
        lines = output.strip().splitlines()
        content = '\n'.join(line for line in lines if not line.strip().startswith('```'))
    else:
        content = output.strip()
    
    # Remove trailing commas before closing braces/brackets
    content = re.sub(r',\s*(?=[}\]])', '', content)
    return content


def parse_llm_response(response_text: str, filename: str, schema_type: str):
    """
    Parse and validate LLM response with cleanup and retry logic.
    
    Args:
        response_text: Raw response from LLM
        filename: Filename for context
        schema_type: Type of schema ('salary' or 'nonsalary')
        
    Returns:
        dict: Parsed and validated data
        
    Raises:
        ModelOutputParseError: If parsing fails after cleanup attempts
    """
    # First validation attempt
    validation_result = validate_llm_response_json(response_text, filename)
    
    if validation_result['is_valid']:
        try:
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            # This shouldn't happen if validation passed, but handle it
            raise ModelOutputParseError(f"JSON parsing failed despite validation: {e}")
    
    # Cleanup attempt
    cleaned_output = clean_gemini_output(response_text)
    validation_result = validate_llm_response_json(cleaned_output, filename)
    
    if validation_result['is_valid']:
        try:
            return json.loads(cleaned_output)
        except json.JSONDecodeError as e:
            log_analysis_error(filename, f"JSON parsing failed after cleanup: {e}", cleaned_output)
            raise ModelOutputParseError(f"JSON parsing failed after cleanup: {e}")
    
    # Final attempt: strip everything before first { and after last }
    try:
        start_idx = cleaned_output.find('{')
        end_idx = cleaned_output.rfind('}') + 1
        if start_idx != -1 and end_idx > start_idx:
            final_attempt = cleaned_output[start_idx:end_idx]
            json.loads(final_attempt)  # Test if valid
            return json.loads(final_attempt)
    except (json.JSONDecodeError, ValueError):
        pass
    
    # All attempts failed
    log_analysis_error(filename, f"All parsing attempts failed for {schema_type} extraction", response_text)
    raise ModelOutputParseError(f"Failed to parse {schema_type} extraction response")


def extract_salary_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> List[dict]:
    """Extract salary information from JSON using LLM."""
    print(f'  DEBUG: Starting salary extraction for {filename}')
    print(f'  DEBUG: Input JSON keys: {list(json_obj.keys())}')
    
    salary_text = ""
    wage_keys = ['wage_information', 'Wage information', 'wage information', 'WAGE_INFORMATION']
    
    for key in wage_keys:
        if key in json_obj:
            value = json_obj[key]
            print(f'  DEBUG: Found wage key: {key}')
            print(f'  DEBUG: Wage value type: {type(value)}')
            print(f'  DEBUG: Wage value length: {len(str(value)) if value else 0}')
            
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
                print(f'  DEBUG: Created salary text from list, length: {len(salary_text)}')
            elif isinstance(value, str):
                salary_text = f'== Wage information ==\n{value}'
                print(f'  DEBUG: Created salary text from string, length: {len(salary_text)}')
            break
    
    general_keys = ['general_information', 'General information', 'general information', 'GENERAL_INFORMATION']
    for key in general_keys:
        if key in json_obj:
            value = json_obj[key]
            print(f'  DEBUG: Found general key: {key}')
            print(f'  DEBUG: General value type: {type(value)}')
            
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        try:
                            nested_data = json.loads(item)
                            print(f'  DEBUG: Successfully parsed nested JSON from general info')
                            wage_keys_nested = ['wage_information', 'Wage information', 'wage information', 'WAGE_INFORMATION']
                            for wage_key in wage_keys_nested:
                                if wage_key in nested_data and nested_data[wage_key]:
                                    wage_data = nested_data[wage_key]
                                    if isinstance(wage_data, list):
                                        salary_text = f'== Wage information ==\n' + '\n'.join(wage_data)
                                    else:
                                        salary_text = f'== Wage information ==\n{wage_data}'
                                    print(f'  DEBUG: Found wage data in nested JSON, length: {len(salary_text)}')
                                    break
                            if salary_text.strip():
                                break
                        except json.JSONDecodeError:
                            if 'wage' in item.lower() or 'salary' in item.lower() or 'salaris' in item.lower():
                                salary_text = f'== Wage information ==\n{item}'
                                print(f'  DEBUG: Found wage-related text in general info, length: {len(salary_text)}')
                                break
            elif isinstance(value, str):
                if 'wage' in value.lower() or 'salary' in value.lower() or 'salaris' in value.lower():
                    salary_text = f'== Wage information ==\n{value}'
                    print(f'  DEBUG: Found wage-related text in general string, length: {len(salary_text)}')
                    break
    
    print(f'  DEBUG: Final salary text length: {len(salary_text)}')
    if salary_text.strip():
        print(f'  DEBUG: Salary text preview: {salary_text[:200]}...')
    else:
        print(f'  DEBUG: No salary text found!')
        return []
    
    if not check_token_limit(salary_text, filename):
        return []
    
    prompt = SALARY_PROMPT.format(filename=filename, source_json=salary_text)
    print(f'  DEBUG: Prompt length: {len(prompt)}')
    print(f'  DEBUG: Prompt preview: {prompt[:300]}...')
    
    model_params = get_model_parameters()
    print(f'  DEBUG: Model params: {model_params}')
    
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
        "presence_penalty": model_params["presence_penalty"],
        "frequency_penalty": model_params["frequency_penalty"],
        "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
        "response_mime_type": "application/json",
        "response_schema": SalaryExtractionSchema,
        "safety_settings": safety_settings  # Include safety settings in config
    }
    
    print(f'  DEBUG: API config: {config}')
    
    try:
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
        print(f'  DEBUG: Response attributes: {dir(response)}')
        
        # Log detailed response information
        log_api_response_details(response, filename, processing_time)
        
        # Log actual response content for debugging
        if hasattr(response, 'text') and response.text:
            print(f'  DEBUG: Response text sample: "{response.text[:100]}..."')
        if hasattr(response, 'parsed'):
            print(f'  DEBUG: Parsed response: {response.parsed}')
        
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
                    model="gemini-2.5-flash"
                )
            
            return result
        else:
            print(f'  DEBUG: No parsed attribute in response')
            if hasattr(response, 'text'):
                print(f'  DEBUG: Response text length: {len(response.text) if response.text else 0}')
                if response.text:
                    print(f'  DEBUG: Response text preview: {response.text[:300]}...')
                    # Try to parse the text manually with better error handling
                    try:
                        # First, try to clean up the JSON if it's truncated
                        cleaned_text = response.text.strip()
                        if cleaned_text.endswith(','):
                            cleaned_text = cleaned_text[:-1]
                        if not cleaned_text.endswith('}'):
                            # Try to find the last complete object
                            last_brace = cleaned_text.rfind('}')
                            if last_brace > 0:
                                cleaned_text = cleaned_text[:last_brace+1]
                            else:
                                # If no closing brace, try to add one
                                cleaned_text += '}'
                        
                        parsed_json = json.loads(cleaned_text)
                        print(f'  DEBUG: Successfully parsed response text manually')
                        # Validate against schema
                        if 'salary_information' in parsed_json:
                            salary_schema = SalaryExtractionSchema(**parsed_json)
                            result = [row.model_dump() for row in salary_schema.salary_information]
                            print(f'  DEBUG: Salary manual parse result: {len(result)} rows')
                            return result
                        else:
                            print(f'  DEBUG: No salary_information key in parsed JSON')
                    except json.JSONDecodeError as e:
                        print(f'  DEBUG: Failed to parse response text as JSON: {e}')
                        # Try to extract partial data from the response
                        try:
                            # Look for salary_information array in the text
                            import re
                            salary_match = re.search(r'"salary_information":\s*\[(.*?)\]', response.text, re.DOTALL)
                            if salary_match:
                                salary_content = salary_match.group(1)
                                print(f'  DEBUG: Found partial salary content, length: {len(salary_content)}')
                                # Try to parse individual salary objects
                                # This is a simplified approach - in production you might want more sophisticated parsing
                                log_analysis_error(filename, f"Partial salary data found but couldn't parse: {e}", response.text[:1000])
                            else:
                                print(f'  DEBUG: No salary_information found in response text')
                                log_analysis_error(filename, f"JSON parsing failed and no salary data found: {e}", response.text[:1000])
                        except Exception as parse_error:
                            print(f'  DEBUG: Failed to extract partial data: {parse_error}')
                            log_analysis_error(filename, f"Complete parsing failure: {e}", response.text[:1000])
                    except Exception as e:
                        print(f'  DEBUG: Failed to validate parsed JSON against schema: {e}')
                        log_analysis_error(filename, f"Schema validation failed: {e}", response.text[:1000])
                log_analysis_error(filename, "No structured output received from model", "")
                return []
            
    except Exception as e:
        print(f'  DEBUG: API call failed with error: {type(e).__name__}: {e}')
        last_error = e  # Capture the initial error
        
        # Retry logic with proper attempt tracking
        for attempt in range(5):
            try:
                # Get model parameters (same for all attempts, like p3)
                model_params = get_model_parameters()
                
                print(f'  DEBUG: Model params: {model_params}')
                
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
                    "temperature": model_params["temperature"],
                    "top_p": model_params["top_p"],
                    "top_k": model_params["top_k"],
                    "max_output_tokens": 65536,  # Increased to maximum for Gemini 2.5 Flash
                    "candidate_count": model_params["candidate_count"],
                    "seed": model_params["seed"],
                    "presence_penalty": model_params["presence_penalty"],
                    "frequency_penalty": model_params["frequency_penalty"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": SalaryExtractionSchema,
                    "safety_settings": safety_settings  # Include safety settings in config
                }
                
                print(f'  DEBUG: API config: {config}')
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
                print(f'  DEBUG: Response attributes: {dir(response)}')
                
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
                            model="gemini-2.5-flash"
                        )
                    
                    return result
                    
            except Exception as e:
                last_error = e  # Update last error for each attempt
                print(f'  DEBUG: Attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global quota_exhausted_flag
                if quota_exhausted_flag:
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
                model="gemini-2.5-flash"
            )
        
        return []


def extract_nonsalary_from_json(json_obj: dict, filename: str, client, context: Dict[str, Any] = None) -> dict:
    """Extract non-salary information from JSON using LLM."""
    print(f'  DEBUG: Starting non-salary extraction for {filename}')
    print(f'  DEBUG: Input JSON keys: {list(json_obj.keys())}')
    
    # Extract all non-wage sections
    non_salary_text = ""
    sections_to_extract = [
        'general_information', 'General information', 'general information', 'GENERAL_INFORMATION',
        'pension_information', 'Pension information', 'pension information', 'PENSION_INFORMATION',
        'leave_information', 'Leave information', 'leave information', 'LEAVE_INFORMATION',
        'termination_information', 'Termination information', 'termination information', 'TERMINATION_INFORMATION',
        'overtime_information', 'Overtime information', 'overtime information', 'OVERTIME_INFORMATION',
        'training_information', 'Training information', 'training information', 'TRAINING_INFORMATION',
        'homeoffice_information', 'Homeoffice information', 'homeoffice information', 'HOMEOFFICE_INFORMATION'
    ]
    
    for key in sections_to_extract:
        if key in json_obj:
            value = json_obj[key]
            print(f'  DEBUG: Found non-salary key: {key}')
            print(f'  DEBUG: Value type: {type(value)}')
            print(f'  DEBUG: Value length: {len(str(value)) if value else 0}')
            
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
                print(f'  DEBUG: Added section from list, total length now: {len(non_salary_text)}')
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
    
    prompt = NON_SALARY_PROMPT.format(filename=filename, source_json=non_salary_text)
    print(f'  DEBUG: Non-salary prompt length: {len(prompt)}')
    print(f'  DEBUG: Non-salary prompt preview: {prompt[:300]}...')
    
    model_params = get_model_parameters()
    print(f'  DEBUG: Non-salary model params: {model_params}')
    
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
        "presence_penalty": model_params["presence_penalty"],
        "frequency_penalty": model_params["frequency_penalty"],
        "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
        "response_mime_type": "application/json",
        "response_schema": NonSalaryExtractionSchema,
        "safety_settings": safety_settings  # Include safety settings in config
    }
    
    print(f'  DEBUG: Non-salary API config: {config}')
    
    try:
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
        
        # Log actual response content for debugging
        if hasattr(response, 'text') and response.text:
            print(f'  DEBUG: Non-salary response text sample: "{response.text[:100]}..."')
        if hasattr(response, 'parsed'):
            print(f'  DEBUG: Non-salary parsed response: {response.parsed}')
        
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
                        parsed_json = json.loads(response.text)
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
        
        # Retry logic with proper attempt tracking
        for attempt in range(5):
            try:
                # Get model parameters (same for all attempts, like p3)
                model_params = get_model_parameters()
                
                print(f'  DEBUG: Non-salary model params: {model_params}')
                
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
                    "temperature": model_params["temperature"],
                    "top_p": model_params["top_p"],
                    "top_k": model_params["top_k"],
                    "max_output_tokens": 65536,  # Increased to maximum for Gemini 2.5 Flash
                    "candidate_count": model_params["candidate_count"],
                    "seed": model_params["seed"],
                    "presence_penalty": model_params["presence_penalty"],
                    "frequency_penalty": model_params["frequency_penalty"],
                    "thinking_config": types.ThinkingConfig(thinking_budget=model_params["thinking_budget"]),
                    "response_mime_type": "application/json",
                    "response_schema": NonSalaryExtractionSchema,
                    "safety_settings": safety_settings  # Include safety settings in config
                }
                
                print(f'  DEBUG: Non-salary API config: {config}')
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
                    
            except Exception as e:
                last_error = e  # Update last error for each attempt
                print(f'  DEBUG: Non-salary attempt {attempt + 1} failed: {type(e).__name__}: {e}')
                
                # Check if quota was exhausted during this attempt
                global quota_exhausted_flag
                if quota_exhausted_flag:
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
                model="gemini-2.5-flash"
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
            lock_file.unlink()
    except:
        pass


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
    salary_file = Path('outputs/llm_analysis/salary') / cao_number / f"{Path(filename).stem}_salary.json"
    non_salary_file = Path('outputs/llm_analysis/non_salary') / cao_number / f"{Path(filename).stem}_non_salary.json"
    
    return salary_file.exists() and non_salary_file.exists()


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
        extraction_type: 'salary' or 'non_salary'
        cao_number: CAO number for folder organization
    """
    try:
        # Create base path
        base_path = Path('outputs/llm_analysis')
        if extraction_type == 'salary':
            save_path = base_path / 'salary'
        else:
            save_path = base_path / 'non_salary'
        
        # Create CAO-specific subfolder if cao_number provided
        if cao_number:
            save_path = save_path / str(cao_number)
        
        save_path.mkdir(parents=True, exist_ok=True)
        
        # Create filename
        base_name = Path(filename).stem
        json_filename = f"{base_name}_{extraction_type}.json"
        file_path = save_path / json_filename
        
        # Save JSON data
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
    except Exception as e:
        print(f'  DEBUG: Failed to save {extraction_type} extraction: {e}')


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
        
        # Check if file is already processed
        if is_file_already_processed(filename, cao_number):
            print(f'  {cao_number}: Skipping {filename} (already processed)')
            return True  # Return True since we successfully skipped it

        # Extract salary information
        salary_extracted = extract_salary_from_json(json_data, filename, client, context)
        print(f'  {cao_number}: Salary extraction - {len(salary_extracted)} rows')
        
        # Extract non-salary information
        rest_extracted = extract_nonsalary_from_json(json_data, filename, client, context)
        
        # Count non-salary data
        non_salary_count = 0
        for key, value in rest_extracted.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    if subvalue and subvalue != "":
                        non_salary_count += 1
        print(f'  {cao_number}: Non-salary extraction - {non_salary_count} fields with data')
        
        # Save extracted JSON data
        save_extraction_json({'salary_information': salary_extracted}, filename, 'salary', cao_number)
        save_extraction_json(rest_extracted, filename, 'non_salary', cao_number)

        # Check if we got any data
        if not salary_extracted and not any(value for value in rest_extracted.values() if isinstance(value, dict) and any(v for v in value.values() if v)):
            print(f'  {cao_number}: No data extracted from {filename}')
            return False
        
        print(f'  {cao_number}: Successfully processed {filename}')
        return True
        
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
        validate_input_paths(config)
        
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
            global quota_exhausted_flag
            if quota_exhausted_flag:
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
                    failed_files.append(json_file.name)
                    
                # Check quota flag again after processing (in case it was set during processing)
                if quota_exhausted_flag:
                    print(f'🚨 QUOTA EXHAUSTED during processing - Process {args.process_id + 1} shutting down gracefully')
                    break
                    
            finally:
                release_file_lock(json_file)
        
        # Final summary with quota exhaustion indication
        if quota_exhausted_flag:
            print(f'Process {args.process_id + 1} completed with QUOTA EXHAUSTION: {successful_analyses} successful, {len(failed_files)} failed')
        else:
            print(f'Process {args.process_id + 1} completed: {successful_analyses} successful, {len(failed_files)} failed')
        
    except Exception as e:
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
