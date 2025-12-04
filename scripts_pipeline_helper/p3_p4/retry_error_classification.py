"""
Shared error classification utilities for p3 and p4 retry logic.

This module provides functions to classify different types of errors
to determine appropriate retry behavior.
"""
from typing import Optional
import re


def is_request_problem(error_str: str) -> bool:
    """
    Check if error is a request problem that should increment attempt.
    
    Request problems are issues with the request itself (truncated response,
    incomplete JSON, empty response, timeout) that may be resolved by
    adjusting parameters or retrying with different settings.
    
    Args:
        error_str: Lowercase error message string
        
    Returns:
        bool: True if error is a request problem
    """
    error_lower = error_str.lower()
    
    # Timeout/Truncation errors
    if any(keyword in error_lower for keyword in ['deadlineexceeded', '504', 'timeout', 'truncated']):
        return True
    
    # Incomplete JSON errors
    if any(keyword in error_lower for keyword in ['incomplete json', 'json validation failed']):
        return True
    
    # Empty response errors
    if any(keyword in error_lower for keyword in ['no content parts found', 'no content', 'no text parts']):
        return True
    
    return False


def is_external_error(error_str: str) -> bool:
    """
    Check if error is an external error that should retry same attempt.
    
    External errors are issues with the service itself (server unavailable,
    connection issues, per-minute quota) that should be retried with the
    same attempt number and parameters, waiting 15 minutes.
    
    Args:
        error_str: Lowercase error message string
        
    Returns:
        bool: True if error is an external error
    """
    error_lower = error_str.lower()
    
    # Service unavailable errors
    if any(keyword in error_lower for keyword in ['serviceunavailable', '503', 'connection reset', '500', 'internal']):
        return True
    
    # Per-minute quota errors (not daily quota)
    if any(keyword in error_lower for keyword in ['429', 'quota', 'rate limit', 'too many requests']):
        # Check it's not a daily quota
        if not is_daily_quota(error_str):
            return True
    
    return False


def is_daily_quota(error_str: str) -> bool:
    """
    Check if error is a daily quota limit (fatal error, don't retry).
    
    Args:
        error_str: Lowercase error message string
        
    Returns:
        bool: True if error is a daily quota limit
    """
    error_lower = error_str.lower()
    
    # Daily quota indicators
    if any(keyword in error_lower for keyword in ['perday', 'daily', 'generaterequestsperday', '3000000']):
        return True
    
    # Free tier daily limit
    if 'free_tier_requests' in error_lower and ('limit: 250' in error_str or 'limit:250' in error_str):
        return True
    
    return False


def is_schema_complexity_error(error_str: str) -> bool:
    """
    Check if error is a schema complexity error (p4 only, fatal error).
    
    Args:
        error_str: Lowercase error message string
        
    Returns:
        bool: True if error is a schema complexity error
    """
    error_lower = error_str.lower()
    
    if 'too many states' in error_lower:
        return True
    
    if '400' in error_lower and 'invalid_argument' in error_lower and 'constraint' in error_lower:
        return True
    
    return False


def extract_api_retry_delay(error: Exception) -> Optional[float]:
    """
    Extract API suggested retry delay from error (p3 only).
    
    Tries multiple methods to extract the retry delay suggested by the API
    from the error object.
    
    Args:
        error: The exception object
        
    Returns:
        Optional[float]: Retry delay in seconds, or None if not found
    """
    try:
        # Method 1: Check if error has 'error' attribute (ClientError structure)
        if hasattr(error, 'error') and isinstance(error.error, dict):
            details = error.error.get('details', [])
            for detail in details:
                if isinstance(detail, dict) and detail.get('@type') == 'type.googleapis.com/google.rpc.RetryInfo':
                    retry_delay_str = detail.get('retryDelay', '')
                    # Parse duration string (e.g., "8s" or "8.666s")
                    if retry_delay_str.endswith('s'):
                        return float(retry_delay_str[:-1])
        
        # Method 2: Check error string representation for "Please retry in X.XXs"
        error_str = str(error).lower()
        match = re.search(r'please retry in ([\d.]+)s', error_str, re.IGNORECASE)
        if match:
            return float(match.group(1))
    except Exception:
        pass  # If extraction fails, return None
    
    return None

