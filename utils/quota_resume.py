"""
Quota Resume State Management

This module handles saving and loading resume state for API quota exhaustion scenarios.
It manages per-key resume state files and calculates batch-specific reset times.

USAGE:
    from utils.quota_resume import (
        get_batch_for_key, calculate_reset_time, save_resume_state,
        load_resume_state, clear_resume_state, wait_until_reset
    )
    
    # Get batch number for a key
    batch = get_batch_for_key(1)  # Returns 1
    
    # Calculate reset time
    reset_time = calculate_reset_time(batch)
    
    # Save state when quota exhausted
    save_resume_state(1, {...})
    
    # Load state on resume
    state = load_resume_state(1)
    
    # Wait until reset time
    wait_until_reset(reset_time)
    
    # Clear state when done
    clear_resume_state(1)
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import pytz


# Batch configuration: 11 batches of 2 keys each
BATCH_CONFIG = {
    1: {"keys": [1, 2], "reset_offset": (1, 1)},    # 1:01 AM PT
    2: {"keys": [3, 4], "reset_offset": (2, 31)},   # 2:31 AM PT
    3: {"keys": [5, 6], "reset_offset": (4, 1)},    # 4:01 AM PT
    4: {"keys": [7, 8], "reset_offset": (5, 31)},   # 5:31 AM PT
    5: {"keys": [9, 10], "reset_offset": (7, 1)},   # 7:01 AM PT
    6: {"keys": [11, 12], "reset_offset": (8, 31)}, # 8:31 AM PT
    7: {"keys": [13, 14], "reset_offset": (10, 1)}, # 10:01 AM PT
    8: {"keys": [15, 16], "reset_offset": (11, 31)}, # 11:31 AM PT
    9: {"keys": [17, 18], "reset_offset": (13, 1)}, # 1:01 PM PT
    10: {"keys": [19, 20], "reset_offset": (14, 31)}, # 2:31 PM PT
    11: {"keys": [21, 22], "reset_offset": (16, 1)}, # 4:01 PM PT
}

# Pacific Timezone
PT = pytz.timezone('America/Los_Angeles')


def get_batch_for_key(key_number: int) -> int:
    """
    Returns batch number (1-11) for a given API key (1-22).
    
    Args:
        key_number: API key number (1-22)
        
    Returns:
        Batch number (1-11)
        
    Raises:
        ValueError: If key_number is not in range 1-22
    """
    if key_number < 1 or key_number > 22:
        raise ValueError(f"Key number must be between 1 and 22, got {key_number}")
    
    for batch_num, config in BATCH_CONFIG.items():
        if key_number in config["keys"]:
            return batch_num
    
    raise ValueError(f"Could not find batch for key {key_number}")


def calculate_reset_time(batch_number: int) -> datetime:
    """
    Calculates next reset time based on batch offset from midnight PT.
    
    The reset time is calculated as midnight PT plus the batch's offset.
    If the calculated time has already passed today, it returns tomorrow's reset time.
    
    Args:
        batch_number: Batch number (1-11)
        
    Returns:
        datetime: Next reset time in Pacific Time (timezone-aware)
        
    Raises:
        ValueError: If batch_number is not in range 1-11
    """
    if batch_number < 1 or batch_number > 11:
        raise ValueError(f"Batch number must be between 1 and 11, got {batch_number}")
    
    config = BATCH_CONFIG[batch_number]
    hours, minutes = config["reset_offset"]
    
    # Get current time in PT
    now_pt = datetime.now(PT)
    
    # Calculate today's reset time
    today_reset = now_pt.replace(hour=hours, minute=minutes, second=0, microsecond=0)
    
    # If today's reset time has passed, use tomorrow
    if today_reset <= now_pt:
        today_reset = today_reset + timedelta(days=1)
    
    return today_reset


def save_resume_state(key_number: int, state: Dict[str, Any]) -> None:
    """
    Saves resume state to logs/resume_state_key{N}.json.
    
    Each API key has its own separate resume state file, so saving one key's
    state never affects other keys' files.
    
    Args:
        key_number: API key number (1-22)
        state: Dictionary containing resume state information
        
    Raises:
        IOError: If file cannot be written
    """
    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    state_file = logs_dir / f"resume_state_key{key_number}.json"
    
    # Add metadata
    state_with_meta = {
        "api_key": key_number,
        "saved_at": datetime.now(pytz.UTC).isoformat(),
        **state
    }
    
    # Write state file
    with open(state_file, 'w', encoding='utf-8') as f:
        json.dump(state_with_meta, f, indent=2, ensure_ascii=False)


def load_resume_state(key_number: int) -> Optional[Dict[str, Any]]:
    """
    Loads resume state if valid (checks if reset time has passed).
    
    State is considered valid if:
    - The resume state file exists
    - The reset time has not yet passed
    
    If the reset time has passed, the state is considered expired and None is returned.
    
    Args:
        key_number: API key number (1-22)
        
    Returns:
        Dictionary containing resume state, or None if state doesn't exist or is expired
    """
    logs_dir = Path("logs")
    state_file = logs_dir / f"resume_state_key{key_number}.json"
    
    if not state_file.exists():
        return None
    
    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            state = json.load(f)
        
        # Check if reset time has passed
        if "reset_time" in state:
            reset_time_str = state["reset_time"]
            # Parse reset time (should be in ISO format)
            reset_time = datetime.fromisoformat(reset_time_str.replace('Z', '+00:00'))
            # Make timezone-aware if needed
            if reset_time.tzinfo is None:
                # Assume UTC if no timezone info
                reset_time = pytz.UTC.localize(reset_time)
            
            # Convert to UTC for comparison
            reset_time_utc = reset_time.astimezone(pytz.UTC)
            now_utc = datetime.now(pytz.UTC)
            
            # If reset time has passed, state is expired
            if reset_time_utc <= now_utc:
                return None
        
        return state
    
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        # If file is corrupted or invalid, return None
        print(f"Warning: Could not load resume state for key {key_number}: {e}")
        return None


def clear_resume_state(key_number: int) -> None:
    """
    Deletes ONLY the resume state file for this specific key.
    
    Each key has its own separate file, so clearing one key's state never
    affects other keys' files.
    
    Args:
        key_number: API key number (1-22)
    """
    logs_dir = Path("logs")
    state_file = logs_dir / f"resume_state_key{key_number}.json"
    
    if state_file.exists():
        state_file.unlink()


def wait_until_reset(reset_time: datetime) -> None:
    """
    Waits until the specified reset time, displaying progress updates.
    
    Args:
        reset_time: datetime object (timezone-aware) representing when to resume
    """
    # Convert reset_time to UTC for comparison
    if reset_time.tzinfo is None:
        reset_time = PT.localize(reset_time)
    reset_time_utc = reset_time.astimezone(pytz.UTC)
    
    now_utc = datetime.now(pytz.UTC)
    
    if reset_time_utc <= now_utc:
        print(f"  Reset time {reset_time.strftime('%Y-%m-%d %H:%M:%S %Z')} has already passed")
        return
    
    # Calculate wait duration
    wait_seconds = (reset_time_utc - now_utc).total_seconds()
    wait_hours = wait_seconds / 3600
    
    print(f"\n{'='*70}")
    print(f"⏸️  QUOTA EXHAUSTED - Waiting for quota reset")
    print(f"{'='*70}")
    print(f"Current time: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Reset time:   {reset_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Wait duration: {wait_hours:.2f} hours ({wait_seconds:.0f} seconds)")
    print(f"{'='*70}\n")
    
    # Wait with periodic status updates
    update_interval = 3600  # Update every hour
    last_update = time.time()
    
    while True:
        now_utc = datetime.now(pytz.UTC)
        remaining = (reset_time_utc - now_utc).total_seconds()
        
        if remaining <= 0:
            break
        
        # Update status every hour
        if time.time() - last_update >= update_interval:
            remaining_hours = remaining / 3600
            print(f"  ⏳ Still waiting... {remaining_hours:.2f} hours remaining until reset")
            last_update = time.time()
        
        # Sleep for 5 minutes at a time
        sleep_duration = min(300, remaining)
        if sleep_duration > 0:
            time.sleep(sleep_duration)
    
    print(f"\n{'='*70}")
    print(f"✅ Quota reset time reached! Resuming processing...")
    print(f"{'='*70}\n")

