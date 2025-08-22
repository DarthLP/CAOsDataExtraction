#!/usr/bin/env python3
"""
Script to delete specific CAO files and their data from the analysis results.

Usage:
    python delete_cao_files.py cao_name1 cao_name2 cao_name3
    or
    python delete_cao_files.py --file list_of_caos.txt
"""

import os
import sys
import pandas as pd
from pathlib import Path
import argparse
import tempfile
import platform
import subprocess
import time

def find_and_delete_json_files(cao_names, json_folder="llmExtracted_json"):
    """
    Find and delete JSON files matching the given CAO names.
    
    Args:
        cao_names (list): List of CAO names to delete (without .json extension)
        json_folder (str): Path to the JSON folder
    
    Returns:
        dict: Statistics about deleted files
    """
    deleted_files = []
    not_found_files = []
    
    json_path = Path(json_folder)
    if not json_path.exists():
        print(f"❌ JSON folder '{json_folder}' not found!")
        return {"deleted": deleted_files, "not_found": not_found_files}
    
    # Search through all CAO number folders
    for cao_folder in json_path.iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            for json_file in cao_folder.glob("*.json"):
                # Check if this file matches any of our target names
                file_stem = json_file.stem  # filename without extension
                
                for cao_name in cao_names:
                    if cao_name == file_stem:  # Exact match only
                        try:
                            json_file.unlink()  # Delete the file
                            deleted_files.append(str(json_file))
                            print(f"🗑️  Deleted: {json_file}")
                        except Exception as e:
                            print(f"❌ Error deleting {json_file}: {e}")
                        break  # Found and deleted, move to next file
    
    # Check which files were not found
    for cao_name in cao_names:
        found = False
        for deleted_file in deleted_files:
            if cao_name in Path(deleted_file).stem:
                found = True
                break
        if not found:
            not_found_files.append(cao_name)
            print(f"⚠️  Not found: {cao_name}.json")
    
    return {"deleted": deleted_files, "not_found": not_found_files}

def _atomic_save_excel_with_retries(df: pd.DataFrame, final_path: str, max_retries: int = 5, delay_seconds: int = 2) -> None:
    """Save DataFrame to Excel atomically with retries to avoid intermittent FS/openpyxl timeouts.

    Strategy:
    - Write to a temporary file in the same directory (no dot-prefix to avoid Finder hidden quirks)
    - On success, os.replace to the final path (atomic on POSIX)
    - Retry on exceptions (e.g., TimeoutError) with small backoff
    - On macOS, clear hidden flag if set
    """
    directory = os.path.dirname(final_path) or "."
    os.makedirs(directory, exist_ok=True)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        temp_file = None
        try:
            # Create temp file in same dir (avoid leading dot so Finder does not carry hidden attributes)
            fd, temp_file = tempfile.mkstemp(prefix="delete_tmp_", suffix=".xlsx", dir=directory)
            os.close(fd)

            with pd.ExcelWriter(temp_file, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)

            os.replace(temp_file, final_path)

            # Best-effort: ensure file not hidden on macOS
            if platform.system() == "Darwin":
                try:
                    subprocess.run(["chflags", "nohidden", final_path], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception:
                    pass
            return
        except Exception as e:  # noqa: BLE001
            last_error = e
            try:
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception:
                pass
            if attempt < max_retries:
                time.sleep(delay_seconds * attempt)
                continue
            break
    raise RuntimeError(f"Failed to save Excel after {max_retries} attempts: {last_error}")

def delete_excel_rows(cao_names, excel_path="results/extracted_data.xlsx"):
    """
    Delete rows from Excel file that match the given CAO names.
    
    Args:
        cao_names (list): List of CAO names to delete
        excel_path (str): Path to the Excel file
    
    Returns:
        dict: Statistics about deleted rows
    """
    if not os.path.exists(excel_path):
        print(f"❌ Excel file '{excel_path}' not found!")
        return {"deleted_rows": 0, "total_rows": 0}
    
    try:
        # Read the Excel file
        df = pd.read_excel(excel_path)
        original_rows = len(df)
        
        # Find rows to delete based on File_name column
        rows_to_delete = []
        for index, row in df.iterrows():
            if 'File_name' in df.columns:
                file_name = row['File_name']
                if file_name:
                    file_stem = Path(file_name).stem  # Remove .json extension
                    
                    # Check if this file matches any of our target names
                    for cao_name in cao_names:
                        if cao_name == file_stem:  # Exact match only
                            rows_to_delete.append(index)
                            print(f"📊 Will delete row {index}: {file_name}")
                            break
        
        # Delete the rows
        if rows_to_delete:
            df = df.drop(rows_to_delete)
            _atomic_save_excel_with_retries(df, excel_path)
            deleted_rows = len(rows_to_delete)
            print(f"✅ Deleted {deleted_rows} rows from Excel file")
        else:
            deleted_rows = 0
            print("ℹ️  No matching rows found in Excel file")
        
        return {"deleted_rows": deleted_rows, "total_rows": original_rows}
        
    except Exception as e:
        print(f"❌ Error processing Excel file: {e}")
        return {"deleted_rows": 0, "total_rows": 0}

def main():
    parser = argparse.ArgumentParser(description="Delete specific CAO files and their Excel data")
    parser.add_argument("cao_names", nargs="*", help="CAO names to delete (without .json extension)")
    parser.add_argument("--file", help="File containing CAO names (one per line)")
    parser.add_argument("--json-folder", default="llmExtracted_json", help="Path to JSON folder")
    parser.add_argument("--excel-file", default="results/extracted_data.xlsx", help="Path to Excel file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without actually deleting")
    
    args = parser.parse_args()
    
    # Get CAO names from arguments or file
    cao_names = []
    if args.file:
        try:
            with open(args.file, 'r') as f:
                cao_names = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"❌ Error reading file {args.file}: {e}")
            return
    else:
        cao_names = args.cao_names
    
    if not cao_names:
        print("❌ No CAO names provided!")
        print("Usage: python delete_cao_files.py cao_name1 cao_name2")
        print("   or: python delete_cao_files.py --file list.txt")
        return
    
    print(f"🎯 CAO names to delete: {cao_names}")
    print(f"📁 JSON folder: {args.json_folder}")
    print(f"📊 Excel file: {args.excel_file}")
    
    if args.dry_run:
        print("\n🔍 DRY RUN MODE - No files will be deleted")
        print("Files that would be deleted:")
        
        # Simulate JSON file search
        json_path = Path(args.json_folder)
        if json_path.exists():
            for cao_folder in json_path.iterdir():
                if cao_folder.is_dir() and cao_folder.name.isdigit():
                    for json_file in cao_folder.glob("*.json"):
                        file_stem = json_file.stem
                        for cao_name in cao_names:
                            if cao_name == file_stem:  # Exact match only
                                print(f"  📄 {json_file}")
                                break
        
        # Simulate Excel row search
        if os.path.exists(args.excel_file):
            try:
                df = pd.read_excel(args.excel_file)
                for index, row in df.iterrows():
                    if 'File_name' in df.columns:
                        file_name = row['File_name']
                        if file_name:
                            file_stem = Path(file_name).stem
                            for cao_name in cao_names:
                                if cao_name == file_stem:  # Exact match only
                                    print(f"  📊 Row {index}: {file_name}")
                                    break
            except Exception as e:
                print(f"  ❌ Error reading Excel: {e}")
        
        return
    
    # Confirm deletion
    print(f"\n⚠️  WARNING: This will permanently delete files and data!")
    response = input("Are you sure you want to continue? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ Operation cancelled")
        return
    
    print("\n" + "="*50)
    print("🗑️  DELETING JSON FILES")
    print("="*50)
    
    # Delete JSON files
    json_stats = find_and_delete_json_files(cao_names, args.json_folder)
    
    print(f"\n📊 JSON deletion summary:")
    print(f"  ✅ Deleted: {len(json_stats['deleted'])} files")
    print(f"  ⚠️  Not found: {len(json_stats['not_found'])} files")
    
    print("\n" + "="*50)
    print("🗑️  DELETING EXCEL ROWS")
    print("="*50)
    
    # Delete Excel rows
    excel_stats = delete_excel_rows(cao_names, args.excel_file)
    
    print(f"\n📊 Excel deletion summary:")
    print(f"  ✅ Deleted: {excel_stats['deleted_rows']} rows")
    print(f"  📊 Total rows remaining: {excel_stats['total_rows'] - excel_stats['deleted_rows']}")
    
    print("\n" + "="*50)
    print("✅ OPERATION COMPLETED")
    print("="*50)

if __name__ == "__main__":
    main() 