"""
Script to clean up failed_cao_numbers folder, keeping only files with 
"JSON incomplete after 8 attempts" errors and removing all other error types
(e.g., RESOURCE_EXHAUSTED, etc.).

Usage:
    python scripts/cleanup_failed_cao_numbers.py
"""

import os
from pathlib import Path


def cleanup_failed_cao_numbers():
    """
    Remove failed CAO number files that are NOT "JSON incomplete after 8 attempts" errors.
    Only keeps files with JSON incomplete errors.
    """
    failed_dir = Path("performance_logs/llm_extraction/failed_cao_numbers")
    
    if not failed_dir.exists():
        print(f"Failed CAO numbers directory not found: {failed_dir}")
        return
    
    # Track statistics
    kept_files = []
    removed_files = []
    removed_folders = []
    
    # Iterate through all CAO number folders
    for cao_folder in sorted(failed_dir.iterdir()):
        if not cao_folder.is_dir():
            continue
        
        cao_number = cao_folder.name
        failed_files = list(cao_folder.glob("*_failed.txt"))
        
        if not failed_files:
            continue
        
        # Check each failed file
        for failed_file in failed_files:
            try:
                # Read the error line (line 5, index 4)
                with open(failed_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    if len(lines) >= 5:
                        error_line = lines[4].strip()  # Line 5 (0-indexed: 4)
                        
                        # Check if it's a JSON incomplete error
                        if "JSON incomplete after 8 attempts" in error_line:
                            kept_files.append(failed_file)
                            print(f"✓ KEEP: {failed_file.name} (JSON incomplete error)")
                        else:
                            # Remove file - not a JSON incomplete error
                            failed_file.unlink()
                            removed_files.append(failed_file)
                            print(f"✗ REMOVE: {failed_file.name} (other error type)")
                    else:
                        # File format unexpected, remove it
                        failed_file.unlink()
                        removed_files.append(failed_file)
                        print(f"✗ REMOVE: {failed_file.name} (unexpected format)")
            except Exception as e:
                print(f"✗ ERROR reading {failed_file}: {e}")
        
        # Remove empty folders
        if cao_folder.exists() and not any(cao_folder.iterdir()):
            cao_folder.rmdir()
            removed_folders.append(cao_folder)
            print(f"  Removed empty folder: {cao_folder.name}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("CLEANUP SUMMARY")
    print("=" * 80)
    print(f"Files kept (JSON incomplete errors): {len(kept_files)}")
    print(f"Files removed (other errors): {len(removed_files)}")
    print(f"Empty folders removed: {len(removed_folders)}")
    print("=" * 80)
    
    if kept_files:
        print("\nKept files:")
        for f in kept_files:
            print(f"  - {f}")
    
    if removed_files:
        print(f"\nRemoved {len(removed_files)} files with non-JSON-incomplete errors")


if __name__ == "__main__":
    cleanup_failed_cao_numbers()

