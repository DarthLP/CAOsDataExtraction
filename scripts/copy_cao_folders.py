#!/usr/bin/env python3
"""
Copy CAO folders from input_pdfs_extra to input_pdfs and update CSV metadata files.

This script:
- Reads CAO numbers from inputs/SectoralCAOs_Copy.xlsx (column "code")
- Identifies CAO numbers that are in the Excel file, exist in input_pdfs_extra but NOT in input_pdfs
- Copies those CAO folders from input_pdfs_extra to input_pdfs
- Updates extracted_cao_info.csv and main_links_log.csv in input_pdfs with data from input_pdfs_extra

USAGE:
    python scripts/copy_cao_folders.py
"""

import os
import shutil
import pandas as pd
from pathlib import Path
from typing import Set, List


# Paths
EXCEL_FILE = "inputs/SectoralCAOs_Copy.xlsx"
INPUT_PDFS_DIR = "inputs/pdfs/input_pdfs"
INPUT_PDFS_EXTRA_DIR = "inputs/pdfs/input_pdfs_extra"
EXTRACTED_CAO_INFO_MAIN = os.path.join(INPUT_PDFS_DIR, "extracted_cao_info.csv")
EXTRACTED_CAO_INFO_EXTRA = os.path.join(INPUT_PDFS_EXTRA_DIR, "extracted_cao_info_extra.csv")
MAIN_LINKS_LOG_MAIN = os.path.join(INPUT_PDFS_DIR, "main_links_log.csv")
MAIN_LINKS_LOG_EXTRA = os.path.join(INPUT_PDFS_EXTRA_DIR, "main_links_log_extra.csv")


def read_cao_numbers_from_excel(excel_path: str) -> Set[str]:
    """
    Read CAO numbers from Excel file.
    
    Args:
        excel_path: Path to the Excel file
        
    Returns:
        Set of CAO numbers as strings
    """
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excel file not found: {excel_path}")
    
    df = pd.read_excel(excel_path)
    
    if "code" not in df.columns:
        raise ValueError(f"Column 'code' not found in Excel file. Available columns: {list(df.columns)}")
    
    # Extract CAO numbers from "code" column
    cao_numbers = set()
    for value in df["code"]:
        if pd.notna(value):
            cao_str = str(value).strip()
            if cao_str and cao_str.isdigit():
                cao_numbers.add(cao_str)
    
    print(f"Read {len(cao_numbers)} CAO numbers from Excel file")
    return cao_numbers


def get_cao_folders(directory: str) -> Set[str]:
    """
    Get list of CAO folder names (numeric folder names) from a directory.
    
    Args:
        directory: Path to directory containing CAO folders
        
    Returns:
        Set of CAO folder names as strings
    """
    if not os.path.exists(directory):
        return set()
    
    cao_folders = set()
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isdir(item_path) and item.isdigit():
            cao_folders.add(item)
    
    return cao_folders


def find_caos_to_copy(excel_caos: Set[str], input_pdfs_caos: Set[str], 
                     input_pdfs_extra_caos: Set[str]) -> List[str]:
    """
    Find CAOs that are in Excel file, in input_pdfs_extra, but not in input_pdfs.
    
    Args:
        excel_caos: Set of CAO numbers from Excel file
        input_pdfs_caos: Set of CAO folder names in input_pdfs
        input_pdfs_extra_caos: Set of CAO folder names in input_pdfs_extra
        
    Returns:
        List of CAO numbers to copy (sorted)
    """
    # CAOs that are in Excel AND in input_pdfs_extra BUT NOT in input_pdfs
    caos_to_copy = (excel_caos & input_pdfs_extra_caos) - input_pdfs_caos
    
    result = sorted(list(caos_to_copy), key=int)
    print(f"Found {len(result)} CAOs to copy")
    return result


def copy_cao_folder(cao_number: str, source_dir: str, dest_dir: str) -> bool:
    """
    Copy a CAO folder from source to destination.
    
    Args:
        cao_number: CAO number (folder name)
        source_dir: Source directory (input_pdfs_extra)
        dest_dir: Destination directory (input_pdfs)
        
    Returns:
        True if copied successfully, False if skipped
    """
    source_path = os.path.join(source_dir, cao_number)
    dest_path = os.path.join(dest_dir, cao_number)
    
    if not os.path.exists(source_path):
        print(f"  Warning: Source folder does not exist: {source_path}")
        return False
    
    if os.path.exists(dest_path):
        print(f"  WARNING: Folder {cao_number} already exists in {dest_dir}. Skipping copy.")
        return False
    
    try:
        shutil.copytree(source_path, dest_path)
        print(f"  Copied CAO {cao_number}")
        return True
    except Exception as e:
        print(f"  Error copying CAO {cao_number}: {e}")
        return False


def update_csv_files(caos_to_copy: List[str], info_extra_path: str, info_main_path: str,
                    log_extra_path: str, log_main_path: str):
    """
    Update CSV files in input_pdfs with data from input_pdfs_extra for the specified CAOs.
    
    Args:
        caos_to_copy: List of CAO numbers to copy data for
        info_extra_path: Path to extracted_cao_info_extra.csv
        info_main_path: Path to extracted_cao_info.csv
        log_extra_path: Path to main_links_log_extra.csv
        log_main_path: Path to main_links_log.csv
    """
    if not caos_to_copy:
        print("No CAOs to copy, skipping CSV updates")
        return
    
    # Read extra CSV files
    print("\nReading CSV files from input_pdfs_extra...")
    
    if os.path.exists(info_extra_path):
        info_extra_df = pd.read_csv(info_extra_path, sep=';', encoding='utf-8')
        print(f"  Read {len(info_extra_df)} rows from extracted_cao_info_extra.csv")
    else:
        print(f"  Warning: {info_extra_path} not found")
        info_extra_df = pd.DataFrame()
    
    if os.path.exists(log_extra_path):
        log_extra_df = pd.read_csv(log_extra_path, sep=';', encoding='utf-8')
        print(f"  Read {len(log_extra_df)} rows from main_links_log_extra.csv")
    else:
        print(f"  Warning: {log_extra_path} not found")
        log_extra_df = pd.DataFrame()
    
    # Filter rows for CAOs being copied
    if not info_extra_df.empty:
        info_extra_df['cao_number'] = info_extra_df['cao_number'].astype(str)
        info_new = info_extra_df[info_extra_df['cao_number'].isin(caos_to_copy)].copy()
        print(f"  Filtered to {len(info_new)} rows for CAOs being copied")
    else:
        info_new = pd.DataFrame()
    
    if not log_extra_df.empty:
        log_extra_df['cao_number'] = log_extra_df['cao_number'].astype(str)
        log_new = log_extra_df[log_extra_df['cao_number'].isin(caos_to_copy)].copy()
        print(f"  Filtered to {len(log_new)} rows for CAOs being copied")
    else:
        log_new = pd.DataFrame()
    
    # Read existing main CSV files
    print("\nReading existing CSV files from input_pdfs...")
    
    if os.path.exists(info_main_path):
        info_main_df = pd.read_csv(info_main_path, sep=';', encoding='utf-8')
        print(f"  Read {len(info_main_df)} existing rows from extracted_cao_info.csv")
    else:
        print(f"  Creating new extracted_cao_info.csv")
        info_main_df = pd.DataFrame()
    
    if os.path.exists(log_main_path):
        log_main_df = pd.read_csv(log_main_path, sep=';', encoding='utf-8')
        print(f"  Read {len(log_main_df)} existing rows from main_links_log.csv")
    else:
        print(f"  Creating new main_links_log.csv")
        log_main_df = pd.DataFrame()
    
    # Append new rows, avoiding duplicates
    if not info_new.empty:
        if not info_main_df.empty:
            # Ensure cao_number is string type for comparison
            info_main_df['cao_number'] = info_main_df['cao_number'].astype(str)
            # Avoid duplicates based on cao_number + pdf_name + id
            info_combined = pd.concat([info_main_df, info_new], ignore_index=True)
            info_combined = info_combined.drop_duplicates(
                subset=['cao_number', 'pdf_name', 'id'], 
                keep='first'
            )
        else:
            info_combined = info_new
        
        # Write back to CSV
        info_combined.to_csv(info_main_path, index=False, sep=';', encoding='utf-8')
        print(f"  Updated extracted_cao_info.csv with {len(info_combined)} total rows "
              f"({len(info_new)} new rows added)")
    
    if not log_new.empty:
        if not log_main_df.empty:
            # Ensure cao_number is string type for comparison
            log_main_df['cao_number'] = log_main_df['cao_number'].astype(str)
            # Avoid duplicates based on cao_number + main_link_url + id
            log_combined = pd.concat([log_main_df, log_new], ignore_index=True)
            log_combined = log_combined.drop_duplicates(
                subset=['cao_number', 'main_link_url', 'id'], 
                keep='first'
            )
        else:
            log_combined = log_new
        
        # Write back to CSV
        log_combined.to_csv(log_main_path, index=False, sep=';', encoding='utf-8')
        print(f"  Updated main_links_log.csv with {len(log_combined)} total rows "
              f"({len(log_new)} new rows added)")


def main():
    """Main function to execute the CAO folder copying and CSV update process."""
    print("=" * 60)
    print("Copy CAO Folders from input_pdfs_extra to input_pdfs")
    print("=" * 60)
    
    # Step 1: Read CAO numbers from Excel
    print("\n[Step 1] Reading CAO numbers from Excel file...")
    try:
        excel_caos = read_cao_numbers_from_excel(EXCEL_FILE)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # Step 2: Identify CAO folders
    print("\n[Step 2] Identifying CAO folders...")
    input_pdfs_caos = get_cao_folders(INPUT_PDFS_DIR)
    input_pdfs_extra_caos = get_cao_folders(INPUT_PDFS_EXTRA_DIR)
    print(f"  Found {len(input_pdfs_caos)} CAO folders in input_pdfs")
    print(f"  Found {len(input_pdfs_extra_caos)} CAO folders in input_pdfs_extra")
    
    # Step 3: Find CAOs to copy
    print("\n[Step 3] Finding CAOs to copy...")
    caos_to_copy = find_caos_to_copy(excel_caos, input_pdfs_caos, input_pdfs_extra_caos)
    
    if not caos_to_copy:
        print("No CAOs to copy. Exiting.")
        return
    
    print(f"  CAOs to copy: {', '.join(caos_to_copy)}")
    
    # Step 4: Copy CAO folders
    print("\n[Step 4] Copying CAO folders...")
    copied_count = 0
    skipped_count = 0
    for cao_number in caos_to_copy:
        if copy_cao_folder(cao_number, INPUT_PDFS_EXTRA_DIR, INPUT_PDFS_DIR):
            copied_count += 1
        else:
            skipped_count += 1
    
    print(f"\n  Summary: {copied_count} folders copied, {skipped_count} skipped")
    
    # Step 5: Update CSV files
    print("\n[Step 5] Updating CSV files...")
    update_csv_files(
        caos_to_copy,
        EXTRACTED_CAO_INFO_EXTRA,
        EXTRACTED_CAO_INFO_MAIN,
        MAIN_LINKS_LOG_EXTRA,
        MAIN_LINKS_LOG_MAIN
    )
    
    print("\n" + "=" * 60)
    print("Process completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

