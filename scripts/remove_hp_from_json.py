#!/usr/bin/env python3
"""
Remove hp field from salary JSON output files

This script removes the 'hp' (holiday_incl at SalaryPoint level) field from all
timeline points in salary JSON analysis files. The 'hi' field at SalaryRow level
is kept.

USAGE:
    python scripts/remove_hp_from_json.py

INPUT:
    - JSON files in outputs/llm_analysis/salary/

OUTPUT:
    - Modified JSON files (hp field removed from timeline points)
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List


def remove_hp_from_salary_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove 'hp' field from all timeline points in salary data.
    
    Args:
        data: JSON data dictionary (may have 'salary_information' or 'si' key)
        
    Returns:
        Modified data dictionary with 'hp' removed from timeline points
    """
    # Handle both 'salary_information' (regular) and 'si' (compact) keys
    salary_rows_key = None
    if 'salary_information' in data:
        salary_rows_key = 'salary_information'
    elif 'si' in data:
        salary_rows_key = 'si'
    else:
        # No salary data found, return as-is
        return data
    
    salary_rows = data[salary_rows_key]
    if not isinstance(salary_rows, list):
        return data
    
    # Process each salary row
    for row in salary_rows:
        if not isinstance(row, dict):
            continue
        
        # Handle both 'timeline' (regular) and 'tl' (compact) keys
        timeline_key = None
        if 'timeline' in row:
            timeline_key = 'timeline'
        elif 'tl' in row:
            timeline_key = 'tl'
        else:
            continue
        
        timeline = row.get(timeline_key, [])
        if not isinstance(timeline, list):
            continue
        
        # Remove 'hp' from each timeline point
        for point in timeline:
            if isinstance(point, dict) and 'hp' in point:
                del point['hp']
    
    return data


def process_json_file(file_path: Path, dry_run: bool = False) -> bool:
    """
    Process a single JSON file to remove 'hp' fields.
    
    Args:
        file_path: Path to JSON file
        dry_run: If True, only check if file needs processing without modifying
        
    Returns:
        True if file was modified (or would be modified in dry_run), False otherwise
    """
    try:
        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if file contains 'hp' field
        file_content = json.dumps(data)
        if '"hp"' not in file_content:
            return False  # File doesn't contain 'hp', no processing needed
        
        if dry_run:
            return True  # File contains 'hp' and needs processing
        
        # Remove 'hp' fields
        modified_data = remove_hp_from_salary_data(data)
        
        # Write back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(modified_data, f, indent=2, ensure_ascii=False)
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"  ❌ Error: Invalid JSON in {file_path}: {e}")
        return False
    except Exception as e:
        print(f"  ❌ Error processing {file_path}: {e}")
        return False


def main():
    """Main function to process all salary JSON files."""
    salary_dir = Path('outputs/llm_analysis/salary')
    
    if not salary_dir.exists():
        print(f"Error: Directory {salary_dir} does not exist")
        sys.exit(1)
    
    print(f"Scanning {salary_dir} for JSON files with 'hp' field...")
    
    # Find all JSON files
    json_files = list(salary_dir.rglob('*.json'))
    print(f"Found {len(json_files)} JSON files")
    
    # First pass: dry run to count files that need processing
    files_to_process = []
    for json_file in json_files:
        if process_json_file(json_file, dry_run=True):
            files_to_process.append(json_file)
    
    if not files_to_process:
        print("✅ No files contain 'hp' field. All files are up to date.")
        return
    
    print(f"\nFound {len(files_to_process)} files containing 'hp' field:")
    for f in files_to_process[:10]:  # Show first 10
        print(f"  - {f.relative_to(salary_dir)}")
    if len(files_to_process) > 10:
        print(f"  ... and {len(files_to_process) - 10} more")
    
    # Process files
    print(f"\nRemoving 'hp' field from {len(files_to_process)} files...")
    processed_count = 0
    for json_file in files_to_process:
        if process_json_file(json_file, dry_run=False):
            processed_count += 1
            if processed_count % 10 == 0:
                print(f"  Processed {processed_count}/{len(files_to_process)} files...")
    
    print(f"\n✅ Successfully removed 'hp' field from {processed_count} files")


if __name__ == '__main__':
    main()

