"""
CAO Data Analysis Script with Schema-Driven Extraction
====================================================

DESCRIPTION:
This script performs structured analysis of Dutch Collective Labor Agreement (CAO) JSON data
using Google's Gemini AI with schema-driven extraction. It processes JSON files from the
llm_Extracted/new_flow pipeline and extracts salary and non-salary information using
separate LLM calls with strict Pydantic validation.

FEATURES:
- Schema-driven extraction with Pydantic validation
- Separate salary and non-salary LLM extractions
- Token safety checks (800K limit)
- Multi-process support for parallel processing
- Robust error handling with exponential backoff
- File locking to prevent duplicate processing
- CAO info integration and Excel output generation

USAGE:
    Single Process:
        python p4_analysis.py --key_number 7 --process_id 0 --total_processes 1
        
    Multi-Process (2 parallel processes):
        python p4_analysis.py --key_number 7 --process_id 0 --total_processes 2
        python p4_analysis.py --key_number 8 --process_id 1 --total_processes 2

    Bash script for parallel execution:
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 7 --process_id 0 --total_processes 6 2>&1 | tee log1.txt &
        unbuffer caffeinate python pipelines/p4_analysis.py --key_number 8 --process_id 1 --total_processes 6 2>&1 | tee log2.txt &

    With file limit:
        python p4_analysis.py --key_number 7 --process_id 0 --total_processes 1 --max_files 10

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
    - Extracted Excel data in outputs/excel/new_results/ folders
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
from dotenv import load_dotenv
import pandas as pd
import yaml
from pydantic import BaseModel, Field, ConfigDict

# Google Gemini API imports
from google import genai
from google.genai import types

# Shared utility imports
from utils.llm_client import (
    setup_environment, setup_gemini_client, get_model_parameters,
    handle_llm_errors, validate_llm_response_json
)


# =============================================================================
# DATA SCHEMAS
# =============================================================================
# Pydantic schemas for structured extraction of CAO document information

class SalaryRow(BaseModel):
    """Schema for a single salary row representing one job group."""
    jobgroup: str = ""
    salary_1: str = ""
    salary_1_unit: str = ""
    salary_1_startdate: str = ""
    salary_increment_1: str = ""
    
    salary_2: str = ""
    salary_2_unit: str = ""
    salary_2_startdate: str = ""
    salary_increment_2: str = ""
    
    salary_3: str = ""
    salary_3_unit: str = ""
    salary_3_startdate: str = ""
    salary_increment_3: str = ""
    
    salary_4: str = ""
    salary_4_unit: str = ""
    salary_4_startdate: str = ""
    salary_increment_4: str = ""
    
    salary_5: str = ""
    salary_5_unit: str = ""
    salary_5_startdate: str = ""
    salary_increment_5: str = ""
    
    salary_6: str = ""
    salary_6_unit: str = ""
    salary_6_startdate: str = ""
    salary_increment_6: str = ""
    
    salary_7: str = ""
    salary_7_unit: str = ""
    salary_7_startdate: str = ""
    salary_increment_7: str = ""
    
    more_salaries: bool = False
    salary_note: str = ""
    salary_age_group: str = ""


class SalaryExtractionSchema(BaseModel):
    """Schema for salary extraction results."""
    salary_information: List[SalaryRow] = Field(default_factory=list)


class ContractInfo(BaseModel):
    """Schema for contract information."""
    start_date_contract: str = ""
    expiry_date_contract: str = ""


class PensionInfo(BaseModel):
    """Schema for pension information."""
    pension_premium_basic: str = ""
    pension_premium_plus: str = ""
    retire_age_basic: str = ""
    retire_age_plus: str = ""
    pension_age_group: str = ""


class LeaveInfo(BaseModel):
    """Schema for leave information."""
    maternity_leave: str = ""
    maternity_pay: str = ""
    maternity_note: str = ""
    vacation_time: str = ""
    vacation_unit: str = ""
    vacation_note: str = ""


class TerminationInfo(BaseModel):
    """Schema for termination information."""
    term_period_employer: str = ""
    term_employer_note: str = ""
    term_period_worker: str = ""
    term_worker_note: str = ""
    probation_period: str = ""
    probation_note: str = ""


class OvertimeInfo(BaseModel):
    """Schema for overtime information."""
    overtime_compensation: str = ""
    max_hrs: str = ""
    min_hrs: str = ""
    shift_compensation: str = ""
    overtime_allowance_min: str = ""
    overtime_allowance_max: str = ""


class TrainingInfo(BaseModel):
    """Schema for training information."""
    training: str = ""


class HomeofficeInfo(BaseModel):
    """Schema for homeoffice information."""
    Homeoffice: str = ""


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
    max_json_files: int = 1
    token_limit: int = 800000  # 800K tokens safety limit


def load_configuration() -> AnalysisConfig:
    """Load and validate configuration from config.yaml."""
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    return AnalysisConfig(
        input_folder=config_data['paths']['outputs_json'] + "/new_flow",
        output_folder=Path(config_data['paths']['outputs_excel']) / "new_results",
        cao_info_path=f"{config_data['paths']['inputs_pdfs']}/extracted_cao_info.csv"
    )


def setup_processing_context(config: AnalysisConfig, process_id: int, 
                           total_processes: int, key_number: int) -> Dict[str, Any]:
    """Setup complete processing context."""
    api_key, actual_key_number = setup_environment(key_number)
    client = setup_gemini_client(api_key)
    
    return {
        'config': config,
        'process_id': process_id,
        'total_processes': total_processes,
        'api_key': api_key,
        'key_number': actual_key_number,
        'client': client
    }


def validate_input_paths(config: AnalysisConfig):
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
    
    if estimated_tokens > 800000:  # 800K token safety limit
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

SALARY_PROMPT = """You are an information-extraction assistant. Input: JSON text derived from a Dutch CAO (collective labour agreement) that contains wage tables and related wage text.
TASK: Extract structured salary data and return PURE JSON matching the exact schema below.

Schema (conceptual):
{{
  "salary_information": [
    {{ SalaryRow }}, ...
  ]
}}
Each SalaryRow MUST include these keys exactly:
["jobgroup","salary_1","salary_1_unit","salary_1_startdate","salary_increment_1",
 "salary_2","salary_2_unit","salary_2_startdate","salary_increment_2",
 "salary_3","salary_3_unit","salary_3_startdate","salary_increment_3",
 "salary_4","salary_4_unit","salary_4_startdate","salary_increment_4",
 "salary_5","salary_5_unit","salary_5_startdate","salary_increment_5",
 "salary_6","salary_6_unit","salary_6_startdate","salary_increment_6",
 "salary_7","salary_7_unit","salary_7_startdate","salary_increment_7",
 "more_salaries","salary_note","salary_age_group"]

Instructions:
- Output MUST be valid JSON and nothing else (no markdown, no commentary).
- Output MUST be a single JSON object with top-level key "salary_information" whose value is a list of SalaryRow objects.
- Each SalaryRow represents exactly one job group (one Excel row). Return as many SalaryRow objects as necessary (0..N).
- If a job group lists more than 7 salary steps, fill salary_1..salary_7 and set more_salaries = true.
- If multiple tables are identical except unit (hourly vs monthly vs weekly), keep only one (prefer hourly).
- Extract salary info applicable to ages 21+; if the table is age-specific, ensure salary_age_group reflects it (e.g., "21+").
- Use empty string "" for missing values.
- Translate any Dutch text to English, except organization names or abbreviations (preserve them).
- Where possible add a short context in salary_note (e.g., "table p.12", "36h week").
- Do NOT invent or hallucinate values.

Filename: {filename}
Source JSON:
{source_json}

Return: valid JSON object, example:
{{
  "salary_information": [
    {{
      "jobgroup": "Helper A",
      "salary_1": "2200",
      "salary_1_unit": "monthly",
      "salary_1_startdate": "2023-01-01",
      "salary_increment_1": "2%",
      "salary_2": "",
      "salary_2_unit": "",
      "salary_2_startdate": "",
      "salary_increment_2": "",
      ...
      "more_salaries": false,
      "salary_note": "based on 36h week (table p.5)",
      "salary_age_group": "21+"
    }},
    ...
  ]
}}"""


NON_SALARY_PROMPT = """You are an information-extraction assistant. Input: JSON text derived from a Dutch CAO that contains general contract info, pension, leave, termination, overtime, training and homeoffice sections.
TASK: Extract structured non-salary data and return PURE JSON matching the exact structure below.

Return EXACT structure (keys and nested keys must exist; use "" for missing values):

{{
  "contract_information": {{
    "start_date_contract":"", "expiry_date_contract":""
  }},
  "pension_information": {{
    "pension_premium_basic":"", "pension_premium_plus":"",
    "retire_age_basic":"", "retire_age_plus":"", "pension_age_group":""
  }},
  "leave_information": {{
    "maternity_leave":"", "maternity_pay":"", "maternity_note":"",
    "vacation_time":"", "vacation_unit":"", "vacation_note":""
  }},
  "termination_information": {{
    "term_period_employer":"", "term_employer_note":"",
    "term_period_worker":"", "term_worker_note":"",
    "probation_period":"", "probation_note":""
  }},
  "overtime_information": {{
    "overtime_compensation":"", "max_hrs":"", "min_hrs":"",
    "shift_compensation":"", "overtime_allowance_min":"", "overtime_allowance_max":""
  }},
  "training_information": {{"training":""}},
  "homeoffice_information": {{"Homeoffice":""}}
}}

Rules:
- Output MUST be valid JSON and nothing else (no markdown, no commentary).
- Translate Dutch → English (preserve organization names / abbreviations).
- For pensions: look for "AOW", "pensioen", "premie", "fonds" and extract fund names, contribution percentages, eligibility, retirement ages.
- For termination: include complete notice period tables if present; include context (page/line) in notes where available.
- For leave/overtime/training/homeoffice: extract factual short strings; if multiple candidate values exist, include the most detailed factual one and log conflicts.
- Use "" for missing fields.
- Do NOT invent or hallucinate values.

Filename: {filename}
Source JSON:
{source_json}

Return: valid JSON object matching the structure above."""


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


def extract_salary_from_json(json_obj: dict, filename: str, client) -> List[dict]:
    """
    Extract salary information from JSON using LLM.
    
    Args:
        json_obj: JSON object containing CAO data
        filename: Filename for context
        client: Gemini client instance
        
    Returns:
        List[dict]: List of salary row dictionaries
    """
    # Extract wage information section
    salary_text = ""
    if 'wage_information' in json_obj:
        value = json_obj['wage_information']
        if isinstance(value, list):
            # Flatten nested lists
            flat_value = []
            for item in value:
                if isinstance(item, list):
                    flat_value.extend(item)
                else:
                    flat_value.append(str(item))
            salary_text = f'== Wage information ==\n' + '\n'.join(flat_value)
        elif isinstance(value, str):
            salary_text = f'== Wage information ==\n{value}'
    
    if not salary_text.strip():
        return []
    
    # Check token limit
    if not check_token_limit(salary_text, filename):
        return []
    
    # Create prompt
    prompt = SALARY_PROMPT.format(filename=filename, source_json=salary_text)
    
    # Try structured output first, fallback to text if it fails
    try:
        # Use structured output for better validation
        model_params = get_model_parameters()
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
            'http_options': types.HttpOptions(timeout=300000),
            'safety_settings': safety_settings,
            'response_mime_type': 'application/json',
            'response_schema': SalaryExtractionSchema
        }
        
        response = client.models.generate_content(
            model=model_params["model"],
            contents=prompt,
            config=config
        )
        
        # Check if we got structured output
        if hasattr(response, 'parsed') and response.parsed:
            return [row.model_dump() for row in response.parsed.salary_information]
        elif hasattr(response, 'text') and response.text and response.text.strip():
            # Fallback to text parsing
            parsed_data = parse_llm_response(response.text, filename, 'salary')
            salary_schema = SalaryExtractionSchema.parse_obj(parsed_data)
            return [row.model_dump() for row in salary_schema.salary_information]
        else:
            raise ValueError('Empty or invalid model response')
            
    except Exception as e:
        # Fallback to text-based extraction
        print(f'  WARNING: Structured output failed for {filename}, falling back to text extraction: {e}')
        raw_output = query_gemini_with_retry(client, prompt, filename)
        parsed_data = parse_llm_response(raw_output, filename, 'salary')
        
        # Validate with Pydantic schema
        try:
            salary_schema = SalaryExtractionSchema.parse_obj(parsed_data)
            return [row.model_dump() for row in salary_schema.salary_information]
        except Exception as e:
            log_analysis_error(filename, f"Salary schema validation failed: {e}", raw_output)
            raise ModelOutputParseError(f"Salary schema validation failed: {e}")


def extract_nonsalary_from_json(json_obj: dict, filename: str, client) -> dict:
    """
    Extract non-salary information from JSON using LLM.
    
    Args:
        json_obj: JSON object containing CAO data
        filename: Filename for context
        client: Gemini client instance
        
    Returns:
        dict: Non-salary extraction results
    """
    # Extract non-wage sections
    rest_sections = ['general_information', 'pension_information', 'leave_information', 
                    'termination_information', 'overtime_information', 'training_information', 
                    'homeoffice_information']
    
    rest_text_parts = []
    for section in rest_sections:
        if section in json_obj:
            value = json_obj[section]
            if isinstance(value, list):
                # Flatten nested lists
                flat_value = []
                for item in value:
                    if isinstance(item, list):
                        flat_value.extend(item)
                    else:
                        flat_value.append(str(item))
                rest_text_parts.append(f'== {section} ==\n' + '\n'.join(flat_value))
            elif isinstance(value, str):
                rest_text_parts.append(f'== {section} ==\n{value}')
    
    rest_text = '\n\n'.join(rest_text_parts)
    
    if not rest_text.strip():
        # Return empty structure
        return NonSalaryExtractionSchema().model_dump()
    
    # Check token limit
    if not check_token_limit(rest_text, filename):
        return NonSalaryExtractionSchema().model_dump()
    
    # Create prompt
    prompt = NON_SALARY_PROMPT.format(filename=filename, source_json=rest_text)
    
    # Try structured output first, fallback to text if it fails
    try:
        # Use structured output for better validation
        model_params = get_model_parameters()
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
            'http_options': types.HttpOptions(timeout=300000),
            'safety_settings': safety_settings,
            'response_mime_type': 'application/json',
            'response_schema': NonSalaryExtractionSchema
        }
        
        response = client.models.generate_content(
            model=model_params["model"],
            contents=prompt,
            config=config
        )
        
        # Check if we got structured output
        if hasattr(response, 'parsed') and response.parsed:
            return response.parsed.model_dump()
        elif hasattr(response, 'text') and response.text and response.text.strip():
            # Fallback to text parsing
            parsed_data = parse_llm_response(response.text, filename, 'nonsalary')
            nonsalary_schema = NonSalaryExtractionSchema.parse_obj(parsed_data)
            return nonsalary_schema.model_dump()
        else:
            raise ValueError('Empty or invalid model response')
            
    except Exception as e:
        # Fallback to text-based extraction
        print(f'  WARNING: Structured output failed for {filename}, falling back to text extraction: {e}')
        raw_output = query_gemini_with_retry(client, prompt, filename)
        parsed_data = parse_llm_response(raw_output, filename, 'nonsalary')
        
        # Validate with Pydantic schema
        try:
            nonsalary_schema = NonSalaryExtractionSchema.parse_obj(parsed_data)
            return nonsalary_schema.model_dump()
        except Exception as e:
            log_analysis_error(filename, f"Non-salary schema validation failed: {e}", raw_output)
            raise ModelOutputParseError(f"Non-salary schema validation failed: {e}")


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
        salary_extracted = extract_salary_from_json(json_obj, filename or "unknown", client)
        
        # Extract non-salary information
        nonsalary_extracted = extract_nonsalary_from_json(json_obj, filename or "unknown", client)
        
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

def merge_extraction_results(salary_extracted: List[dict], rest_extracted: dict) -> List[dict]:
    """
    Merge results from salary and rest extractions into multiple rows with specific infotype labels.
    
    Args:
        salary_extracted: List of salary extraction results
        rest_extracted: Non-salary extraction results
        
    Returns:
        List[dict]: List of merged extraction results with complete field structure
    """
    # Define the complete field structure (all fields from fields_prompt.md)
    # This should match the current system's column structure
    all_fields = [
        'File_name', 'CAO', 'id', 'TTW', 'infotype',
        'jobgroup', 'salary_1', 'salary_1_unit', 'salary_1_startdate', 'salary_increment_1',
        'salary_2', 'salary_2_unit', 'salary_2_startdate', 'salary_increment_2',
        'salary_3', 'salary_3_unit', 'salary_3_startdate', 'salary_increment_3',
        'salary_4', 'salary_4_unit', 'salary_4_startdate', 'salary_increment_4',
        'salary_5', 'salary_5_unit', 'salary_5_startdate', 'salary_increment_5',
        'salary_6', 'salary_6_unit', 'salary_6_startdate', 'salary_increment_6',
        'salary_7', 'salary_7_unit', 'salary_7_startdate', 'salary_increment_7',
        'more_salaries', 'salary_note', 'salary_age_group',
        'start_date_contract', 'expiry_date_contract',
        'pension_premium_basic', 'pension_premium_plus', 'retire_age_basic', 'retire_age_plus', 'pension_age_group',
        'maternity_leave', 'maternity_pay', 'maternity_note', 'vacation_time', 'vacation_unit', 'vacation_note',
        'term_period_employer', 'term_employer_note', 'term_period_worker', 'term_worker_note', 'probation_period', 'probation_note',
        'overtime_compensation', 'max_hrs', 'min_hrs', 'shift_compensation', 'overtime_allowance_min', 'overtime_allowance_max',
        'training', 'Homeoffice'
    ]
    
    # Define field mappings for different infotypes
    INFOTYPE_FIELD_MAPPINGS = {
        'Pension': ['pension_premium_basic', 'pension_premium_plus', 'retire_age_basic', 'retire_age_plus', 'pension_age_group'],
        'Leave': ['maternity_leave', 'maternity_pay', 'maternity_note', 'vacation_time', 'vacation_unit', 'vacation_note'],
        'Termination': ['term_period_employer', 'term_employer_note', 'term_period_worker', 'term_worker_note', 'probation_period', 'probation_note'],
        'Overtime': ['overtime_compensation', 'max_hrs', 'min_hrs', 'shift_compensation', 'overtime_allowance_min', 'overtime_allowance_max'],
        'Training': ['training'],
        'Homeoffice': ['Homeoffice']
    }
    
    merged_results = []
    
    # Process salary items
    for salary_item in salary_extracted:
        if not isinstance(salary_item, dict):
            continue
            
        # Create wage row
        wage_row = {field: '' for field in all_fields}
        wage_row['infotype'] = 'Wage'
        
        # Fill salary fields
        for field, value in salary_item.items():
            if field in wage_row and value:
                wage_row[field] = value
        
        # Add contract dates from rest_extracted if available
        if 'contract_information' in rest_extracted:
            contract_info = rest_extracted['contract_information']
            if contract_info.get('start_date_contract'):
                wage_row['start_date_contract'] = contract_info['start_date_contract']
            if contract_info.get('expiry_date_contract'):
                wage_row['expiry_date_contract'] = contract_info['expiry_date_contract']
        
        merged_results.append(wage_row)
    
    # Process non-salary items (create separate rows for each infotype)
    for infotype, fields in INFOTYPE_FIELD_MAPPINGS.items():
        rest_row = {field: '' for field in all_fields}
        rest_row['infotype'] = infotype
        
        # Map fields based on infotype
        if infotype == 'Pension' and 'pension_information' in rest_extracted:
            pension_info = rest_extracted['pension_information']
            for field in fields:
                if field in pension_info and pension_info[field]:
                    rest_row[field] = pension_info[field]
        elif infotype == 'Leave' and 'leave_information' in rest_extracted:
            leave_info = rest_extracted['leave_information']
            for field in fields:
                if field in leave_info and leave_info[field]:
                    rest_row[field] = leave_info[field]
        elif infotype == 'Termination' and 'termination_information' in rest_extracted:
            termination_info = rest_extracted['termination_information']
            for field in fields:
                if field in termination_info and termination_info[field]:
                    rest_row[field] = termination_info[field]
        elif infotype == 'Overtime' and 'overtime_information' in rest_extracted:
            overtime_info = rest_extracted['overtime_information']
            for field in fields:
                if field in overtime_info and overtime_info[field]:
                    rest_row[field] = overtime_info[field]
        elif infotype == 'Training' and 'training_information' in rest_extracted:
            training_info = rest_extracted['training_information']
            for field in fields:
                if field in training_info and training_info[field]:
                    rest_row[field] = training_info[field]
        elif infotype == 'Homeoffice' and 'homeoffice_information' in rest_extracted:
            homeoffice_info = rest_extracted['homeoffice_information']
            for field in fields:
                if field in homeoffice_info and homeoffice_info[field]:
                    rest_row[field] = homeoffice_info[field]
        
        # Add contract dates
        if 'contract_information' in rest_extracted:
            contract_info = rest_extracted['contract_information']
            if contract_info.get('start_date_contract'):
                rest_row['start_date_contract'] = contract_info['start_date_contract']
            if contract_info.get('expiry_date_contract'):
                rest_row['expiry_date_contract'] = contract_info['expiry_date_contract']
        
        merged_results.append(rest_row)
    
    return merged_results


def process_single_file(json_file: Path, cao_folder: Path, client, cao_info_mapping: dict, 
                       config: AnalysisConfig, context: dict, df_results: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """
    Process a single JSON file through the analysis pipeline.
    
    Args:
        json_file: Path to the JSON file
        cao_folder: CAO folder containing the file
        client: Gemini client instance
        cao_info_mapping: CAO info mapping dictionary
        config: Analysis configuration
        context: Processing context
        df_results: Current results DataFrame
        
    Returns:
        Tuple[bool, pd.DataFrame]: (success, updated_df_results)
    """
    cao_number = cao_folder.name
    filename = json_file.name
    
    print(f'  {cao_number}: {filename} [API {context["key_number"]}/{context["total_processes"]}]')
    
    try:
        # Read JSON file
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Extract salary information
        salary_extracted = extract_salary_from_json(json_data, filename, client)
        
        # Extract non-salary information
        rest_extracted = extract_nonsalary_from_json(json_data, filename, client)
        
        # Merge results
        merged_results = merge_extraction_results(salary_extracted, rest_extracted)
        
        if not merged_results:
            print(f'  {cao_number}: No data extracted from {filename}')
            return False, df_results
        
        # Process each merged result
        for item in merged_results:
            # Add metadata
            item['CAO'] = str(cao_number) if cao_number else json_file.stem
            item['TTW'] = 'yes' if 'TTW' in json_file.stem.upper() else 'no'
            item['File_name'] = json_file.name
            
            # Find CAO info
            cao_id = None
            pdf_name = json_file.stem + '.pdf'
            if cao_number:
                cao_info = find_cao_info(pdf_name, int(cao_number), cao_info_mapping)
                if cao_info:
                    item['CAO'] = cao_info['cao_number']
                    item['id'] = cao_info['id']
                    item['start_date'] = cao_info['ingangsdatum']
                    item['expiry_date'] = cao_info['expiratiedatum']
                    item['date_of_formal_notification'] = cao_info['datum_kennisgeving']
                    cao_id = cao_info['id']
                else:
                    item['id'] = ''
            else:
                item['id'] = ''
            
            # Create DataFrame row
            row_df = pd.DataFrame([item])
            
            # Ensure all columns exist
            if df_results.empty:
                df_results = row_df
            else:
                # Reindex to match existing columns
                row_df_full = row_df.reindex(columns=df_results.columns, fill_value='')
                df_results = pd.concat([df_results, row_df_full], ignore_index=True)
        
        # Save incremental Excel file
        output_path = config.output_folder / f"extracted_data_process_{context['process_id'] + 1}.xlsx"
        df_results.to_excel(output_path, index=False)
        
        print(f'  {cao_number}: Successfully processed {filename}')
        return True, df_results
        
    except Exception as e:
        print(f'  {cao_number}: ✗ Error processing {filename}: {e}')
        log_analysis_error(filename, str(e))
        return False, df_results


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
        
        if args.max_files:
            process_files = process_files[:args.max_files]
        
        print(f'Process {args.process_id + 1}: Processing {len(process_files)} files')
        
        # Process files
        successful_analyses = 0
        failed_files = []
        df_results = pd.DataFrame()  # Initialize empty DataFrame
        
        for cao_folder, json_file in process_files:
            if not acquire_file_lock(json_file):
                print(f'  Skipping {json_file.name} (being processed by another process)')
                time.sleep(2)
                continue
            
            try:
                success, df_results = process_single_file(json_file, cao_folder, context['client'], 
                                                        cao_info_mapping, config, context, df_results)
                if success:
                    successful_analyses += 1
                else:
                    failed_files.append(json_file.name)
            finally:
                release_file_lock(json_file)
        
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
            
            print("\n=== EXTRACTION RESULTS ===")
            print(f"Salary rows: {len(result['salary_extraction'])}")
            print(f"Non-salary fields: {len(result['non_salary_extraction'])}")
            
            print("\n=== SALARY EXTRACTION ===")
            for i, row in enumerate(result['salary_extraction']):
                print(f"Row {i+1}: {row.get('jobgroup', 'N/A')} - {row.get('salary_1', 'N/A')} {row.get('salary_1_unit', '')}")
            
            print("\n=== NON-SALARY EXTRACTION ===")
            for key, value in result['non_salary_extraction'].items():
                if isinstance(value, dict):
                    print(f"{key}:")
                    for subkey, subvalue in value.items():
                        if subvalue:  # Only show non-empty values
                            print(f"  {subkey}: {subvalue}")
                elif value:  # Only show non-empty values
                    print(f"{key}: {value}")
            
            print("\n✓ Fixture test completed successfully!")
            
        except Exception as e:
            print(f"✗ Fixture test failed: {e}")
            return 1
    else:
        # Run normal main function
        main()
    
    return 0


if __name__ == "__main__":
    exit(cli_test_fixture())
