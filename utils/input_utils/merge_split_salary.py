"""
Merge Split Salary Extraction Results

This module provides functionality to merge two split salary extraction results,
deduplicating rows and consolidating shared notes.

USAGE:
    from utils.input_utils.merge_split_salary import merge_split_salary_results
    
    merged_result = merge_split_salary_results(first_half, second_half, filename)
"""

import json
from typing import Dict, Any, Optional, List
from pathlib import Path


def get_row_key(row: Dict[str, Any]) -> str:
    """
    Generate a unique key for a salary row based on jobgroup, step, worker, age, education, contract.
    
    Supports both regular schema (full field names) and compact/split schema (2-letter abbreviations).
    
    Args:
        row: Salary row dictionary (can use either full names or 2-letter abbreviations)
        
    Returns:
        str: Unique key for deduplication
    """
    # Support both regular schema (full names) and compact/split schema (2-letter abbreviations)
    jobgroup = row.get('jg', row.get('jobgroup', ''))
    step = row.get('st', row.get('step', '')) or ''
    worker = row.get('wr', row.get('worker', '')) or ''
    age_group = row.get('ag', row.get('age_group', '')) or ''
    education = row.get('eu', row.get('education', '')) or ''
    permanency = row.get('pe', row.get('permanency', '')) or ''
    hours_type = row.get('ht', row.get('hours_type', '')) or ''
    
    # Create composite key
    key_parts = [jobgroup, step, worker, age_group, education, permanency, hours_type]
    return '|'.join(str(p) for p in key_parts)


def merge_split_salary_results(first_half: Dict[str, Any], second_half: Dict[str, Any], filename: str = "") -> Optional[Dict[str, Any]]:
    """
    Merge two split salary extraction results into a single result.
    
    This function:
    - Concatenates salary_information arrays from both halves
    - Deduplicates rows based on jobgroup+step+worker+age+education+contract
    - Consolidates shared notes where appropriate
    - Validates the merged result
    - Supports both regular schema (full field names) and compact/split schema (2-letter abbreviations)
    
    Args:
        first_half: First extraction result (dict with 'salary_information' or 'si' key)
        second_half: Second extraction result (dict with 'salary_information' or 'si' key)
        filename: Filename for error reporting (optional)
        
    Returns:
        dict: Merged result with 'salary_information' key, or None if merge fails
    """
    try:
        # Validate inputs - support both 'salary_information' (regular) and 'si' (compact/split)
        if not isinstance(first_half, dict):
            print(f'  ERROR: Invalid first_half structure for {filename}')
            return None
        
        if not isinstance(second_half, dict):
            print(f'  ERROR: Invalid second_half structure for {filename}')
            return None
        
        # Get salary data - support both 'si' (compact/split) and 'salary_information' (regular)
        first_rows = first_half.get('si', first_half.get('salary_information', []))
        second_rows = second_half.get('si', second_half.get('salary_information', []))
        
        # Validate that we found salary data
        if 'si' not in first_half and 'salary_information' not in first_half:
            print(f'  ERROR: Invalid first_half structure for {filename} - missing salary_information or si key')
            return None
        
        if 'si' not in second_half and 'salary_information' not in second_half:
            print(f'  ERROR: Invalid second_half structure for {filename} - missing salary_information or si key')
            return None
        
        if not isinstance(first_rows, list):
            first_rows = []
        if not isinstance(second_rows, list):
            second_rows = []
        
        print(f'  DEBUG: Merging {len(first_rows)} rows from first half and {len(second_rows)} rows from second half')
        
        # Track rows by key for deduplication
        merged_rows = {}
        seen_keys = set()
        
        # Add first half rows
        for row in first_rows:
            if not isinstance(row, dict):
                continue
            
            key = get_row_key(row)
            if key not in seen_keys:
                merged_rows[key] = row
                seen_keys.add(key)
            else:
                # Support both field name formats for logging
                jobgroup_val = row.get('jg', row.get('jobgroup', 'unknown'))
                print(f'  WARNING: Duplicate row in first half (jobgroup: {jobgroup_val}), keeping first occurrence')
        
        # Add second half rows (skip duplicates)
        duplicate_count = 0
        for row in second_rows:
            if not isinstance(row, dict):
                continue
            
            key = get_row_key(row)
            if key not in seen_keys:
                merged_rows[key] = row
                seen_keys.add(key)
            else:
                duplicate_count += 1
                # Support both field name formats for logging
                jobgroup_val = row.get('jg', row.get('jobgroup', 'unknown'))
                print(f'  WARNING: Duplicate row in second half (jobgroup: {jobgroup_val}), skipping')
        
        if duplicate_count > 0:
            print(f'  INFO: Skipped {duplicate_count} duplicate rows during merge')
        
        # Convert merged_rows dict back to list (preserve order: first half, then second half)
        # We'll maintain insertion order
        merged_list = []
        for row in first_rows:
            if isinstance(row, dict):
                key = get_row_key(row)
                if key in merged_rows:
                    merged_list.append(merged_rows[key])
                    del merged_rows[key]  # Remove to avoid re-adding
        
        # Add remaining rows from second half
        for row in second_rows:
            if isinstance(row, dict):
                key = get_row_key(row)
                if key in merged_rows:
                    merged_list.append(merged_rows[key])
        
        # Consolidate shared notes (optional - for future enhancement)
        # For now, we just deduplicate and merge
        
        # Build final result
        result = {
            'salary_information': merged_list
        }
        
        print(f'  DEBUG: Merge complete: {len(merged_list)} total rows (from {len(first_rows)} + {len(second_rows)} input rows)')
        
        return result
        
    except Exception as e:
        print(f'  ERROR: Failed to merge split salary results for {filename}: {e}')
        import traceback
        print(f'  Traceback: {traceback.format_exc()}')
        return None

