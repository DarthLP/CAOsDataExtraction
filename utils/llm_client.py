"""
Shared LLM Client Utilities for CAO Data Extraction
==================================================

DESCRIPTION:
This module provides shared utility functions for LLM client setup, error handling,
and response processing that can be used across different extraction scripts.

FEATURES:
- Gemini client setup and configuration
- Environment variable management
- Error handling with exponential backoff
- Response validation and cleanup
- Rate limit calculations

USAGE:
    from utils.llm_client import setup_gemini_client, handle_llm_errors
    
    client = setup_gemini_client(api_key)
    # Use client for LLM operations
"""

# =============================================================================
# IMPORTS
# =============================================================================
import os
import time
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from google import genai


# =============================================================================
# CLIENT SETUP & CONFIGURATION FUNCTIONS
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


def get_model_parameters() -> Dict[str, Any]:
    """
    Get standard model parameters for consistent LLM configuration.
    
    Returns:
        dict: Model configuration parameters
    """
    return {
        "model": "gemini-2.5-pro",
        "temperature": 0.0,
        "top_p": 0.1,
        "top_k": 1,
        "max_tokens": None,
        "candidate_count": 1,
        "seed": 42,  # For deterministic output
        "presence_penalty": 0.0,
        "frequency_penalty": 0.0,
        "thinking_budget": 1024,  # Medium complexity tasks
        "max_retries": 5
    }


# =============================================================================
# ERROR HANDLING & RETRY LOGIC
# =============================================================================
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
            wait_time = 60 * 2 ** attempt
            print(f'  Attempt {attempt + 1} failed (service unavailable/internal error), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with service errors')
            return False
    elif any(keyword in error_str for keyword in ['quota', 'rate limit', 'too many requests', '429']):
        if attempt < max_retries - 1:
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


# =============================================================================
# RESPONSE PROCESSING & VALIDATION
# =============================================================================
def cleanup_uploaded_file(client, uploaded_file):
    """
    Clean up uploaded file from Gemini.
    
    Args:
        client: Gemini client instance
        uploaded_file: Uploaded file object to delete
    """
    try:
        client.files.delete(name=uploaded_file.name)
        print(f'  INFO: Cleaned up uploaded file: {uploaded_file.name}')
    except Exception as e:
        print(f'  WARNING: Failed to clean up file {uploaded_file.name}: {e}')


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
        import json
        json.loads(content)
        return {'is_valid': True, 'error': None}
    except json.JSONDecodeError as e:
        return {'is_valid': False, 'error': f'JSON parsing error: {str(e)}'}
