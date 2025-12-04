"""
p3-specific retry logic for LLM extraction pipeline.

This module contains retry logic functions specific to p3_llmExtraction.py,
including error handling, quota delay calculations, parameter adjustments,
and retry guidance.
"""
import time
from typing import Dict, Any, Optional, Tuple
from ..p3_p4.retry_error_classification import (
    is_request_problem,
    is_external_error,
    is_daily_quota,
    extract_api_retry_delay
)


def calculate_quota_retry_delay(file_size_mb: float, attempt: int) -> int:
    """
    Calculate quota retry delay based on file size and attempt number.
    
    Formula: (estimated_tokens / 125000) * 60 seconds * (2.1^attempt) + buffer
    - 125,000 tokens per minute limit
    - Exponential backoff: 2.1^attempt (capped at attempt 4 for steady delays after retry 5)
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
    backoff_multiplier = 2.1 ** min(attempt, 4)
    
    # Add buffer time (2-6 minutes for safety, capped at attempt 4)
    buffer_minutes = 2 + min(attempt, 4)
    
    # Calculate total delay in seconds
    total_delay_seconds = int((minutes_needed * backoff_multiplier + buffer_minutes) * 60)
    
    print(f'  DEBUG: File size: {file_size_mb:.2f}MB, Estimated tokens: {estimated_tokens:,}')
    print(f'  DEBUG: Minutes needed: {minutes_needed:.1f}, Backoff: {backoff_multiplier}x, Buffer: {buffer_minutes}min')
    print(f'  DEBUG: Total delay: {total_delay_seconds // 60} minutes ({total_delay_seconds} seconds)')
    
    return total_delay_seconds


def get_adjusted_parameters(config, attempt: int) -> Dict[str, Any]:
    """
    Get adjusted model parameters based on retry attempt.
    
    - Attempts 0-2 (1st-3rd tries): original parameters
    - Attempt 3 (4th try): temperature +0.1, top_p +0.1, top_k -0.1
    - Attempt 4 (5th try): temperature +0.2, top_p +0.2, top_k -0.2
    - Attempt 5 (6th try, split extraction): same as attempt 0 → original parameters
    - Attempt 6 (7th try, split extraction): same as attempt 2 → original parameters
    - Attempt 7 (8th try, split extraction): same as attempt 3 → +0.1 adjustment
    
    Args:
        config: Configuration object with model parameters
        attempt: Current retry attempt number (0-based)
        
    Returns:
        dict: Model parameters with attempt-based adjustments
    """
    if attempt <= 2:
        # First 3 attempts: use original parameters
        adjustment = 0.0
    elif attempt == 3:
        # 4th attempt: +0.1 adjustment
        adjustment = 0.1
    elif attempt == 4:
        # 5th attempt: +0.2 adjustment
        adjustment = 0.2
    elif attempt == 5:
        # 6th attempt (split extraction): same as attempt 0 → original parameters
        adjustment = 0.0
    elif attempt == 6:
        # 7th attempt (split extraction): same as attempt 2 → original parameters
        adjustment = 0.0
    elif attempt == 7:
        # 8th attempt (split extraction): same as attempt 3 → +0.1 adjustment
        adjustment = 0.1
    else:
        # Fallback for any higher attempts (shouldn't happen with max_retries=8)
        adjustment = 0.2
    
    # Calculate adjusted values
    adjusted_temp = config.temperature + adjustment
    adjusted_top_p = min(1.0, config.top_p + adjustment)  # Cap at 1.0
    adjusted_top_k = max(1, int(config.top_k - adjustment * config.top_k))  # Reduce by percentage, min 1
    
    return {
        "model": config.model,
        "temperature": adjusted_temp,
        "top_p": adjusted_top_p,
        "top_k": adjusted_top_k,
        "max_tokens": config.max_tokens,
        "candidate_count": config.candidate_count,
        "seed": config.seed,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "thinking_budget": config.thinking_budget,
        "max_retries": config.max_retries
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
        - Be more CONCISE in narrative descriptions while keeping all important data intact!
        - Prioritize completing the JSON structure over verbose explanations
        - Keep all numbers, dates, tables - compress only explanatory text!
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
    context=None,
    remaining_budget_s: Optional[int] = None,
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
        remaining_budget_s: Remaining budget in seconds (for timeout calculations)
        
    Returns:
        tuple: (should_retry: bool, increment_attempt: bool, wait_time_seconds: int)
    """
    error_str = str(error).lower()
    
    # Check for daily quota limit (fatal error, don't retry)
    if is_daily_quota(error_str):
        if context and hasattr(context, 'process_id') and process_quota_flags is not None:
            process_quota_flags[context.process_id] = True
            print(f'  ❌ DAILY QUOTA LIMIT REACHED for Process {context.process_id} - Cannot retry until tomorrow')
            print(f'  💡 Daily limit: 3,000,000 tokens per day')
            print(f'  💡 Quota resets at midnight (Google timezone)')
            print(f'  🛑 Stopping this process to avoid infinite retries')
        return (False, False, 0)  # Don't retry
    
    # Check for request problems (increment attempt)
    if is_request_problem(error_str):
        if attempt < max_retries - 1:
            # Timeout/Truncation errors
            if any(keyword in error_str for keyword in ['deadlineexceeded', '504', 'timeout', 'truncated']):
                wait_time = 120 * 2 ** min(attempt, 4)
                if remaining_budget_s is not None:
                    wait_time = min(wait_time, max(0, int(remaining_budget_s) - 5))
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
            if any(keyword in error_str for keyword in ['serviceunavailable', '503', 'connection reset', '500', 'internal']):
                wait_time = 60 * 2 ** min(attempt, 4)
                print(f'  Attempt {attempt + 1} failed (service unavailable/internal error), retrying in {wait_time // 60} minutes...')
                # For external errors, always wait 15 minutes and retry same attempt
                return (True, False, 900)  # Don't increment attempt, wait 15 min
            
            # Per-minute quota errors
            elif any(keyword in error_str for keyword in ['quota', '429', 'rate limit', 'too many requests']):
                # Try to extract API's suggested retry delay from error details
                api_retry_delay = extract_api_retry_delay(error)
                
                # Calculate our delay
                if file_size_mb > 0:
                    wait_time = calculate_quota_retry_delay(file_size_mb, attempt)
                else:
                    # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
                    wait_time = 90 * 2 ** min(attempt, 4)  # Fallback for unknown file size
                
                # Always add 150 seconds to quota retry delay
                wait_time += 150
                print(f'  DEBUG: Calculated wait time (before API delay check): {wait_time}s ({wait_time // 60} minutes)')
                
                # Use API's suggested delay if it's longer than our calculated delay (with 3 min buffer)
                if api_retry_delay is not None:
                    # Add buffer to API delay (at least 10 seconds, or 20% more, whichever is larger)
                    api_delay_with_buffer = max(api_retry_delay + 10, api_retry_delay * 1.2)
                    # Ensure we always have at least 3 minutes total
                    api_delay_with_buffer = max(api_delay_with_buffer, 180)
                    wait_time = max(wait_time, int(api_delay_with_buffer))
                    print(f'  INFO: API suggested retry delay: {api_retry_delay:.1f}s, using {wait_time}s (with 3 min minimum)')
                
                print(f'  Attempt {attempt + 1} failed (per-minute quota), retrying in {wait_time // 60} minutes ({wait_time}s)...')
                # For external errors, always wait 15 minutes and retry same attempt
                return (True, False, 900)  # Don't increment attempt, wait 15 min
        else:
            print(f'  All {max_retries} attempts failed with external errors')
            return (False, False, 0)
    
    # Generic errors (increment attempt)
    if attempt < max_retries - 1:
        wait_time = 30 * 2 ** min(attempt, 4)
        print(f'  Attempt {attempt + 1} failed ({type(error).__name__}), retrying in {wait_time} seconds...')
        return (True, True, wait_time)  # Increment attempt
    else:
        return (False, False, 0)



