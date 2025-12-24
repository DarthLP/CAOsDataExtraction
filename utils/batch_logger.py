"""
Aggregated Batch Logging

This module handles atomic updates to the batch summary log file that tracks
all API keys' status and statistics across batches.

USAGE:
    from utils.batch_logger import (
        initialize_batch_summary, update_batch_summary,
        get_batch_summary, mark_key_completed
    )
    
    # Initialize summary file
    initialize_batch_summary()
    
    # Update a key's status
    update_batch_summary(1, {"last_status": "running", "successful_files": 10})
    
    # Get current summary
    summary = get_batch_summary()
    
    # Mark key as completed
    mark_key_completed(1)
"""

import json
import fcntl
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import pytz


SUMMARY_FILE = Path("logs/batch_summary.json")


def _acquire_file_lock(file_handle):
    """Acquire exclusive lock on file for atomic operations."""
    try:
        fcntl.flock(file_handle, fcntl.LOCK_EX)
        return True
    except (AttributeError, IOError):
        # Windows or file locking not available - proceed without lock
        return False


def _release_file_lock(file_handle):
    """Release file lock."""
    try:
        fcntl.flock(file_handle, fcntl.LOCK_UN)
    except (AttributeError, IOError):
        pass


def initialize_batch_summary() -> None:
    """
    Creates initial batch_summary.json structure if it doesn't exist.
    
    Initializes the file with batch configuration and empty api_keys dictionary.
    """
    # Ensure logs directory exists
    SUMMARY_FILE.parent.mkdir(exist_ok=True)
    
    # Only initialize if file doesn't exist
    if SUMMARY_FILE.exists():
        return
    
    from utils.quota_resume import BATCH_CONFIG
    
    # Create initial structure
    summary = {
        "batches": {},
        "api_keys": {}
    }
    
    # Initialize batch configurations
    for batch_num, config in BATCH_CONFIG.items():
        hours, minutes = config["reset_offset"]
        reset_offset_str = f"{hours:02d}:{minutes:02d}:00"
        summary["batches"][str(batch_num)] = {
            "keys": config["keys"],
            "reset_offset": reset_offset_str
        }
    
    # Write initial file
    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        if _acquire_file_lock(f):
            try:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            finally:
                _release_file_lock(f)
        else:
            json.dump(summary, f, indent=2, ensure_ascii=False)


def get_batch_summary() -> Dict[str, Any]:
    """
    Reads current batch summary.
    
    Returns:
        Dictionary containing batch summary structure
        
    Raises:
        FileNotFoundError: If summary file doesn't exist
        json.JSONDecodeError: If file is corrupted
    """
    if not SUMMARY_FILE.exists():
        initialize_batch_summary()
    
    with open(SUMMARY_FILE, 'r', encoding='utf-8') as f:
        if _acquire_file_lock(f):
            try:
                return json.load(f)
            finally:
                _release_file_lock(f)
        else:
            return json.load(f)


def update_batch_summary(key_number: int, update_data: Dict[str, Any]) -> None:
    """
    Atomically updates ONLY the specific key's entry in batch_summary.json.
    
    Reads entire file, updates only api_keys.key_{N} section, writes back atomically.
    Never deletes or modifies other keys' data. Uses file locking to prevent
    race conditions when multiple processes update simultaneously.
    
    Args:
        key_number: API key number (1-22)
        update_data: Dictionary with fields to update for this key
    """
    # Ensure summary file exists
    initialize_batch_summary()
    
    # Use atomic write pattern: write to temp file, then rename
    temp_file = SUMMARY_FILE.with_suffix('.json.tmp')
    
    # Read current summary
    summary = get_batch_summary()
    
    # Ensure api_keys structure exists
    if "api_keys" not in summary:
        summary["api_keys"] = {}
    
    # Update ONLY this key's entry
    key_name = f"key_{key_number}"
    if key_name not in summary["api_keys"]:
        summary["api_keys"][key_name] = {}
    
    # Merge update_data into existing key data (preserve other fields)
    summary["api_keys"][key_name] = {
        **summary["api_keys"][key_name],
        **update_data,
        "last_updated": datetime.now(pytz.UTC).isoformat()
    }
    
    # Write to temp file first
    with open(temp_file, 'w', encoding='utf-8') as f:
        if _acquire_file_lock(f):
            try:
                json.dump(summary, f, indent=2, ensure_ascii=False)
                f.flush()
            finally:
                _release_file_lock(f)
        else:
            json.dump(summary, f, indent=2, ensure_ascii=False)
            f.flush()
    
    # Atomic rename (atomic on most filesystems)
    temp_file.replace(SUMMARY_FILE)


def mark_key_completed(key_number: int) -> None:
    """
    Updates key's status to "completed" and clears resume_file reference.
    
    Preserves all historical data (successful_files, failed_files, etc.).
    Only modifies that specific key's entry.
    
    Args:
        key_number: API key number (1-22)
    """
    update_data = {
        "last_status": "completed",
        "resume_file": None
    }
    update_batch_summary(key_number, update_data)

