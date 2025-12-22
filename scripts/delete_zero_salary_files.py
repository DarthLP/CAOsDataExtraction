#!/usr/bin/env python3
"""
Delete files with zero salary entries from analysis and extraction folders.

This script:
- Scans all salary analysis files in outputs/llm_analysis/salary/[cao_number]/
- Identifies files with empty salary_information arrays (0 entries)
- Shows count of files to be deleted before deletion
- Deletes the analysis files and corresponding extracted files from outputs/llm_extracted/new_flow/[cao_number]/

USAGE:
    python scripts/delete_zero_salary_files.py [--dry-run] [--yes]

ARGUMENTS:
    --dry-run: Show what would be deleted without actually deleting
    --yes: Skip confirmation prompt and proceed with deletion
"""

import json
import argparse
from pathlib import Path
from typing import List, Tuple


def find_zero_salary_files() -> List[Tuple[Path, str, str]]:
    """
    Find all salary analysis files with zero salary entries.
    
    Returns:
        List of tuples: (analysis_file_path, cao_number, base_filename)
    """
    salary_analysis_dir = Path("outputs/llm_analysis/salary")
    zero_salary_files = []
    
    if not salary_analysis_dir.exists():
        print(f"Error: Directory {salary_analysis_dir} does not exist")
        return zero_salary_files
    
    # Scan all CAO number folders
    for cao_folder in sorted(salary_analysis_dir.iterdir()):
        if not cao_folder.is_dir():
            continue
        
        cao_number = cao_folder.name
        
        # Check all analysis files in this CAO folder
        for analysis_file in cao_folder.glob("*_analysis.json"):
            try:
                with open(analysis_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Check if salary_information is empty
                if 'salary_information' in data:
                    salary_info = data['salary_information']
                    if isinstance(salary_info, list) and len(salary_info) == 0:
                        # Extract base filename (remove _analysis.json, add _extract.json)
                        base_filename = analysis_file.stem.replace("_analysis", "")
                        zero_salary_files.append((analysis_file, cao_number, base_filename))
            except (json.JSONDecodeError, KeyError, Exception) as e:
                print(f"Warning: Could not process {analysis_file}: {e}")
                continue
    
    return zero_salary_files


def delete_files(zero_salary_files: List[Tuple[Path, str, str]], dry_run: bool = False) -> None:
    """
    Delete analysis files and corresponding extracted files.
    
    Args:
        zero_salary_files: List of (analysis_file_path, cao_number, base_filename) tuples
        dry_run: If True, only show what would be deleted without actually deleting
    """
    deleted_analysis = 0
    deleted_extracted = 0
    missing_extracted = 0
    
    for analysis_file, cao_number, base_filename in zero_salary_files:
        # Delete analysis file
        if dry_run:
            print(f"Would delete: {analysis_file}")
        else:
            try:
                analysis_file.unlink()
                deleted_analysis += 1
                print(f"Deleted: {analysis_file}")
            except Exception as e:
                print(f"Error deleting {analysis_file}: {e}")
        
        # Delete corresponding extracted file
        extracted_file = Path("outputs/llm_extracted/new_flow") / cao_number / f"{base_filename}_extract.json"
        
        if extracted_file.exists():
            if dry_run:
                print(f"Would delete: {extracted_file}")
            else:
                try:
                    extracted_file.unlink()
                    deleted_extracted += 1
                    print(f"Deleted: {extracted_file}")
                except Exception as e:
                    print(f"Error deleting {extracted_file}: {e}")
        else:
            missing_extracted += 1
            if not dry_run:
                print(f"Warning: Extracted file not found: {extracted_file}")
    
    # Summary
    if dry_run:
        print(f"\nDRY RUN SUMMARY:")
        print(f"  Analysis files to delete: {len(zero_salary_files)}")
        print(f"  Extracted files to delete: {len(zero_salary_files) - missing_extracted}")
        print(f"  Missing extracted files: {missing_extracted}")
    else:
        print(f"\nDELETION SUMMARY:")
        print(f"  Analysis files deleted: {deleted_analysis}")
        print(f"  Extracted files deleted: {deleted_extracted}")
        print(f"  Missing extracted files: {missing_extracted}")


def main():
    """Main function to run the deletion script."""
    parser = argparse.ArgumentParser(description="Delete files with zero salary entries")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without actually deleting")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()
    
    print("Scanning for files with zero salary entries...")
    zero_salary_files = find_zero_salary_files()
    
    if not zero_salary_files:
        print("No files with zero salary entries found.")
        return
    
    # Show summary
    print(f"\nFound {len(zero_salary_files)} files with zero salary entries:")
    print(f"  - Analysis files to delete: {len(zero_salary_files)}")
    
    # Count how many extracted files exist
    missing_count = 0
    for _, cao_number, base_filename in zero_salary_files:
        extracted_file = Path("outputs/llm_extracted/new_flow") / cao_number / f"{base_filename}_extract.json"
        if not extracted_file.exists():
            missing_count += 1
    
    print(f"  - Extracted files to delete: {len(zero_salary_files) - missing_count}")
    print(f"  - Missing extracted files: {missing_count}")
    
    if args.dry_run:
        print("\n=== DRY RUN MODE ===")
        delete_files(zero_salary_files, dry_run=True)
        return
    
    # Ask for confirmation unless --yes flag is used
    if not args.yes:
        response = input(f"\nProceed with deletion of {len(zero_salary_files)} analysis files? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Deletion cancelled.")
            return
    
    print("\nProceeding with deletion...")
    delete_files(zero_salary_files, dry_run=False)
    print("\nDeletion complete.")


if __name__ == "__main__":
    main()

