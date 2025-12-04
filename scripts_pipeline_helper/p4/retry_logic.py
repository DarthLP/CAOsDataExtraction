"""
p4-specific retry logic for LLM analysis pipeline.

This module contains retry logic functions specific to p4_analysis.py,
including error handling, quota delay calculations, parameter adjustments,
and retry guidance.
"""
import time
from typing import Dict, Any, Optional, Tuple
from ..p3_p4.retry_error_classification import (
    is_request_problem,
    is_external_error,
    is_daily_quota,
    is_schema_complexity_error
)


def get_model_parameters(model: str = "gemini-2.5-flash") -> dict:
    """
    Get model parameters for LLM calls.
    
    Args:
        model: Model name (defaults to gemini-2.0-flash-exp)
        
    Returns:
        dict: Model parameters
    """
    return {
        "model": model,
        "temperature": 0.0,
        "top_p": 0.1,
        "top_k": 1,
        "max_tokens": 65536,
        "candidate_count": 1,
        "seed": 42,
        "thinking_budget": -1,  # Dynamic thinking (like p3)
        "max_retries": 5
    }


def calculate_quota_retry_delay(file_size_mb: float, attempt: int) -> int:
    """
    Calculate quota retry delay based on file size and attempt number.
    
    Formula: (estimated_tokens / 125000) * 60 seconds * (2.1^attempt) + buffer
    - 125,000 tokens per minute limit
    - Exponential backoff: 2.1^attempt (synchronized with p3)
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
    
    # Add exponential backoff: 2.1^attempt (capped at attempt 4 - keep steady after retry 5)
    # Synchronized with p3
    backoff_multiplier = 2.1 ** min(attempt, 4)
    
    # Add buffer time (2-6 minutes for safety, capped at attempt 4)
    buffer_minutes = 2 + min(attempt, 4)
    
    # Calculate total delay in seconds
    total_delay_seconds = int((minutes_needed * backoff_multiplier + buffer_minutes) * 60)
    
    print(f'  DEBUG: File size: {file_size_mb:.2f}MB, Estimated tokens: {estimated_tokens:,}')
    print(f'  DEBUG: Minutes needed: {minutes_needed:.1f}, Backoff: {backoff_multiplier}x, Buffer: {buffer_minutes}min')
    print(f'  DEBUG: Total delay: {total_delay_seconds // 60} minutes ({total_delay_seconds} seconds)')
    
    return total_delay_seconds


def get_adjusted_parameters(attempt: int, model: str = "gemini-2.5-flash") -> dict:
    """
    Get adjusted model parameters based on retry attempt.
    
    - Attempt 0 (1st try): original parameters
    - Attempt 1 (2nd try): original parameters  
    - Attempt 2 (3rd try): temperature +0.1, top_p +0.1, top_k -10%
    - Attempt 3 (4th try): temperature +0.2, top_p +0.2, top_k -20%
    - Attempt 4+ (5th+ try): temperature +0.3, top_p +0.3, top_k -30%
    
    Args:
        attempt: Current retry attempt number (0-based)
        model: Model name (defaults to gemini-2.0-flash-exp)
        
    Returns:
        dict: Model parameters with attempt-based adjustments
    """
    # Get base parameters
    base_params = get_model_parameters(model)
    
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


def get_retry_guidance(error_message: str) -> Tuple[str, str]:
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


def handle_llm_errors(
    error: Exception,
    attempt: int,
    max_retries: int,
    file_size_mb: float = 0,
    context: Optional[Any] = None,
    process_quota_flags: Optional[dict] = None
) -> Tuple[bool, bool, int]:
    """
    Handle different types of LLM errors with appropriate retry logic.
    
    Returns a tuple indicating:
    - should_retry: Whether to retry the request
    - increment_attempt: Whether to increment the attempt counter (True for request problems, False for external errors)
    - wait_time_seconds: How long to wait before retrying
    
    Args:
        error: The exception that occurred
        attempt: Current attempt number (0-based)
        max_retries: Maximum number of retry attempts
        file_size_mb: Size of file in MB (for quota calculations)
        context: Processing context object (for quota flags)
        process_quota_flags: Dictionary to set quota flags (for daily quota)
        
    Returns:
        tuple: (should_retry: bool, increment_attempt: bool, wait_time_seconds: int)
    """
    error_str = str(error).lower()
    
    # Check for schema complexity error (fatal error, don't retry - p4 only)
    if is_schema_complexity_error(error_str):
        print(f'  ⚠️ Schema complexity error: The schema is too complex for Google\'s structured output API.')
        print(f'  ⚠️ This error is not retryable - the schema needs to be simplified or split further.')
        return (False, False, 0)  # Don't retry
    
    # Check for daily quota limit (fatal error, don't retry)
    if is_daily_quota(error_str):
        # Extract process_id from context if available
        process_id = 0
        if context and hasattr(context, 'process_id'):
            process_id = context.process_id
        elif context and isinstance(context, dict) and 'process_id' in context:
            process_id = context['process_id']
        
        if process_quota_flags is not None:
            process_quota_flags[process_id] = True
            print(f'  🚨 DAILY QUOTA LIMIT REACHED (429) - Process will shutdown gracefully')
            print(f'  🛑 Daily quota flag set for process {process_id} only - will stop this process after current attempt')
            print(f'  💡 Other parallel processes will continue running')
        return (False, False, 0)  # Don't retry
    
    # Check for request problems (increment attempt)
    if is_request_problem(error_str):
        if attempt < max_retries - 1:
            # Timeout/Truncation errors
            if any(keyword in error_str for keyword in ['deadlineexceeded', '504', 'timeout', 'truncated']):
                wait_time = 120 * 2 ** min(attempt, 4)
                print(f'  Attempt {attempt + 1} failed (timeout/truncation), retrying in {wait_time // 60} minutes...')
                return (True, True, wait_time)  # Increment attempt
            
            # Incomplete JSON errors
            elif any(keyword in error_str for keyword in ['incomplete json', 'json validation failed']):
                wait_time = 30 * 2 ** min(attempt, 4)
                print(f'  Attempt {attempt + 1} failed (incomplete JSON), retrying in {wait_time} seconds...')
                return (True, True, wait_time)  # Increment attempt
            
            # Empty response errors
            elif any(keyword in error_str for keyword in ['no content parts found', 'no content', 'no text parts']):
                wait_time = 60 * 2 ** min(attempt, 4)
                # Add 120 seconds to empty response errors
                wait_time += 120
                print(f'  Attempt {attempt + 1} failed (empty response), retrying in {wait_time // 60} minutes...')
                return (True, True, wait_time)  # Increment attempt
        else:
            print(f'  All {max_retries} attempts failed with request problem errors')
            return (False, False, 0)
    
    # Check for external errors (retry same attempt, wait 15 minutes)
    if is_external_error(error_str):
        if attempt < max_retries - 1:
            # Service unavailable errors (503, 500, connection reset, internal)
            # p4 uses custom wait times: [2, 4, 8, 12, 20] minutes
            if any(keyword in error_str for keyword in ['serviceunavailable', '503', 'connection reset', '500', 'internal']):
                wait_times = [2, 4, 8, 12, 20]  # minutes
                wait_time_minutes = wait_times[min(attempt, len(wait_times) - 1)]
                wait_time = wait_time_minutes * 60  # convert to seconds
                print(f'  Attempt {attempt + 1} failed (service unavailable/internal error), retrying in {wait_time_minutes} minutes...')
                # For external errors, always wait 15 minutes and retry same attempt
                return (True, False, 900)  # Don't increment attempt, wait 15 min
            
            # Per-minute quota errors
            elif any(keyword in error_str for keyword in ['quota', '429', 'rate limit', 'too many requests']):
                wait_time = calculate_quota_retry_delay(file_size_mb, attempt)
                # Always add 150 seconds to quota retry delay
                wait_time += 150
                print(f'  Attempt {attempt + 1} failed (per-minute rate limit), retrying in {wait_time // 60} minutes...')
                # For external errors, always wait 15 minutes and retry same attempt
                return (True, False, 900)  # Don't increment attempt, wait 15 min
        else:
            print(f'  All {max_retries} attempts failed with external errors')
            return (False, False, 0)
    
    # Generic errors (increment attempt)
    if attempt < max_retries - 1:
        wait_time = 60 * 2 ** min(attempt, 4)
        print(f'  Attempt {attempt + 1} failed ({type(error).__name__}), retrying in {wait_time // 60} minutes...')
        return (True, True, wait_time)  # Increment attempt
    else:
        print(f'  All {max_retries} attempts failed with {type(error).__name__}: {error}')
        return (False, False, 0)

