#!/usr/bin/env python3
"""
Cleanup duplicate CAO folders from input_pdfs_extra after verification.

This script:
- Verifies data integrity between input_pdfs and input_pdfs_extra for CAOs that were copied
- Checks that extracted_cao_info.csv and main_links_log.csv have all expected entries
- Checks that input_pdfs folders have all expected PDF files
- Reports any irregularities
- Asks for user confirmation before deleting CAO folders from input_pdfs_extra
- Note: Only deletes folders, CSV files in input_pdfs_extra remain unchanged

USAGE:
    python scripts/cleanup_duplicate_caos.py
"""

import os
import shutil
import pandas as pd
from pathlib import Path
from typing import Set, List, Dict, Tuple


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


def get_pdf_files_in_folder(cao_number: str, base_dir: str) -> Set[str]:
    """
    Get set of PDF file names in a CAO folder.
    
    Args:
        cao_number: CAO number (folder name)
        base_dir: Base directory (input_pdfs or input_pdfs_extra)
        
    Returns:
        Set of PDF file names (without path)
    """
    folder_path = os.path.join(base_dir, cao_number)
    if not os.path.exists(folder_path):
        return set()
    
    pdf_files = set()
    for item in os.listdir(folder_path):
        if item.lower().endswith('.pdf'):
            pdf_files.add(item)
    
    return pdf_files


def verify_csv_integrity(cao_numbers: List[str], info_extra_path: str, info_main_path: str,
                         log_extra_path: str, log_main_path: str) -> Dict[str, Dict]:
    """
    Verify that all CSV entries from input_pdfs_extra exist in input_pdfs CSVs.
    
    Args:
        cao_numbers: List of CAO numbers to verify
        info_extra_path: Path to extracted_cao_info_extra.csv
        info_main_path: Path to extracted_cao_info.csv
        log_extra_path: Path to main_links_log_extra.csv
        log_main_path: Path to main_links_log.csv
        
    Returns:
        Dictionary with verification results and irregularities
    """
    results = {
        'info_verified': True,
        'log_verified': True,
        'info_irregularities': [],
        'log_irregularities': [],
        'info_missing_count': 0,
        'log_missing_count': 0
    }
    
    # Read CSV files
    if not os.path.exists(info_extra_path):
        print(f"  Warning: {info_extra_path} not found")
        return results
    
    if not os.path.exists(info_main_path):
        results['info_verified'] = False
        results['info_irregularities'].append(f"Main CSV file not found: {info_main_path}")
        return results
    
    info_extra_df = pd.read_csv(info_extra_path, sep=';', encoding='utf-8')
    info_main_df = pd.read_csv(info_main_path, sep=';', encoding='utf-8')
    
    # Convert cao_number to string for comparison
    info_extra_df['cao_number'] = info_extra_df['cao_number'].astype(str)
    info_main_df['cao_number'] = info_main_df['cao_number'].astype(str)
    
    # Filter for CAOs being checked
    info_extra_filtered = info_extra_df[info_extra_df['cao_number'].isin(cao_numbers)].copy()
    
    # Check each row from extra exists in main
    for _, row in info_extra_filtered.iterrows():
        cao_num = str(row['cao_number'])
        pdf_name = str(row.get('pdf_name', ''))
        id_val = str(row.get('id', ''))
        
        # Check if this row exists in main (based on unique key: cao_number + pdf_name + id)
        exists = info_main_df[
            (info_main_df['cao_number'] == cao_num) &
            (info_main_df['pdf_name'].astype(str) == pdf_name) &
            (info_main_df['id'].astype(str) == id_val)
        ]
        
        if exists.empty:
            results['info_verified'] = False
            results['info_missing_count'] += 1
            results['info_irregularities'].append(
                f"CAO {cao_num}: Missing entry - pdf_name: {pdf_name}, id: {id_val}"
            )
    
    # Same for log file
    if os.path.exists(log_extra_path) and os.path.exists(log_main_path):
        log_extra_df = pd.read_csv(log_extra_path, sep=';', encoding='utf-8')
        log_main_df = pd.read_csv(log_main_path, sep=';', encoding='utf-8')
        
        log_extra_df['cao_number'] = log_extra_df['cao_number'].astype(str)
        log_main_df['cao_number'] = log_main_df['cao_number'].astype(str)
        
        log_extra_filtered = log_extra_df[log_extra_df['cao_number'].isin(cao_numbers)].copy()
        
        for _, row in log_extra_filtered.iterrows():
            cao_num = str(row['cao_number'])
            main_link_url = str(row.get('main_link_url', ''))
            id_val = str(row.get('id', ''))
            
            exists = log_main_df[
                (log_main_df['cao_number'] == cao_num) &
                (log_main_df['main_link_url'].astype(str) == main_link_url) &
                (log_main_df['id'].astype(str) == id_val)
            ]
            
            if exists.empty:
                results['log_verified'] = False
                results['log_missing_count'] += 1
                results['log_irregularities'].append(
                    f"CAO {cao_num}: Missing entry - main_link_url: {main_link_url}, id: {id_val}"
                )
    elif not os.path.exists(log_extra_path):
        print(f"  Warning: {log_extra_path} not found")
    elif not os.path.exists(log_main_path):
        results['log_verified'] = False
        results['log_irregularities'].append(f"Main CSV file not found: {log_main_path}")
    
    return results


def verify_folder_integrity(cao_numbers: List[str]) -> Dict[str, Dict]:
    """
    Verify that all PDF files from input_pdfs_extra folders exist in input_pdfs folders.
    
    Args:
        cao_numbers: List of CAO numbers to verify
        
    Returns:
        Dictionary with verification results and irregularities
    """
    results = {
        'verified': True,
        'irregularities': [],
        'missing_files': []
    }
    
    for cao_number in cao_numbers:
        extra_pdfs = get_pdf_files_in_folder(cao_number, INPUT_PDFS_EXTRA_DIR)
        main_pdfs = get_pdf_files_in_folder(cao_number, INPUT_PDFS_DIR)
        
        # Check that all PDFs from extra exist in main
        missing = extra_pdfs - main_pdfs
        
        if missing:
            results['verified'] = False
            for pdf in missing:
                results['missing_files'].append(f"CAO {cao_number}: Missing PDF - {pdf}")
                results['irregularities'].append(f"CAO {cao_number}: PDF '{pdf}' exists in input_pdfs_extra but not in input_pdfs")
        
        # Also check if main has PDFs that extra doesn't have (informational)
        extra_only = main_pdfs - extra_pdfs
        if extra_only:
            print(f"  Note: CAO {cao_number} has {len(extra_only)} PDF(s) in input_pdfs that are not in input_pdfs_extra")
    
    return results


def delete_cao_folders(cao_numbers: List[str]) -> int:
    """
    Delete CAO folders from input_pdfs_extra.
    
    Args:
        cao_numbers: List of CAO numbers to delete
        
    Returns:
        Number of folders deleted
    """
    deleted_count = 0
    
    for cao_number in cao_numbers:
        folder_path = os.path.join(INPUT_PDFS_EXTRA_DIR, cao_number)
        if os.path.exists(folder_path):
            try:
                shutil.rmtree(folder_path)
                print(f"  Deleted folder: {cao_number}")
                deleted_count += 1
            except Exception as e:
                print(f"  Error deleting folder {cao_number}: {e}")
        else:
            print(f"  Warning: Folder {cao_number} does not exist in input_pdfs_extra")
    
    return deleted_count


def main():
    """Main function to execute the cleanup process."""
    print("=" * 60)
    print("Cleanup Duplicate CAOs from input_pdfs_extra")
    print("=" * 60)
    
    # Step 1: Identify CAOs to check
    print("\n[Step 1] Identifying CAOs to check...")
    try:
        excel_caos = read_cao_numbers_from_excel(EXCEL_FILE)
        print(f"  Read {len(excel_caos)} CAO numbers from Excel file")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    input_pdfs_caos = get_cao_folders(INPUT_PDFS_DIR)
    input_pdfs_extra_caos = get_cao_folders(INPUT_PDFS_EXTRA_DIR)
    
    print(f"  Found {len(input_pdfs_caos)} CAO folders in input_pdfs")
    print(f"  Found {len(input_pdfs_extra_caos)} CAO folders in input_pdfs_extra")
    
    # Find CAOs that exist in both (these are the ones that were copied)
    caos_to_check = sorted(list(excel_caos & input_pdfs_caos & input_pdfs_extra_caos), key=int)
    
    if not caos_to_check:
        print("  No CAOs found in both input_pdfs and input_pdfs_extra. Nothing to clean up.")
        return
    
    print(f"  Found {len(caos_to_check)} CAOs to verify: {', '.join(caos_to_check)}")
    
    # Step 2: Verify CSV integrity
    print("\n[Step 2] Verifying CSV file integrity...")
    csv_results = verify_csv_integrity(
        caos_to_check,
        EXTRACTED_CAO_INFO_EXTRA,
        EXTRACTED_CAO_INFO_MAIN,
        MAIN_LINKS_LOG_EXTRA,
        MAIN_LINKS_LOG_MAIN
    )
    
    if csv_results['info_verified']:
        print("  ✓ extracted_cao_info.csv: All entries verified")
    else:
        print(f"  ✗ extracted_cao_info.csv: {csv_results['info_missing_count']} missing entries found")
    
    if csv_results['log_verified']:
        print("  ✓ main_links_log.csv: All entries verified")
    else:
        print(f"  ✗ main_links_log.csv: {csv_results['log_missing_count']} missing entries found")
    
    # Step 3: Verify folder integrity
    print("\n[Step 3] Verifying folder/file integrity...")
    folder_results = verify_folder_integrity(caos_to_check)
    
    if folder_results['verified']:
        print("  ✓ All PDF files verified")
    else:
        print(f"  ✗ {len(folder_results['missing_files'])} missing PDF files found")
    
    # Step 4: Report findings
    print("\n[Step 4] Verification Summary")
    print("=" * 60)
    
    all_verified = (csv_results['info_verified'] and 
                   csv_results['log_verified'] and 
                   folder_results['verified'])
    
    if all_verified:
        print("✓ All verifications passed!")
    else:
        print("✗ Some irregularities found:")
        
        if csv_results['info_irregularities']:
            print(f"\n  extracted_cao_info.csv irregularities ({len(csv_results['info_irregularities'])}):")
            for irreg in csv_results['info_irregularities'][:10]:  # Show first 10
                print(f"    - {irreg}")
            if len(csv_results['info_irregularities']) > 10:
                print(f"    ... and {len(csv_results['info_irregularities']) - 10} more")
        
        if csv_results['log_irregularities']:
            print(f"\n  main_links_log.csv irregularities ({len(csv_results['log_irregularities'])}):")
            for irreg in csv_results['log_irregularities'][:10]:  # Show first 10
                print(f"    - {irreg}")
            if len(csv_results['log_irregularities']) > 10:
                print(f"    ... and {len(csv_results['log_irregularities']) - 10} more")
        
        if folder_results['irregularities']:
            print(f"\n  Folder/file irregularities ({len(folder_results['irregularities'])}):")
            for irreg in folder_results['irregularities'][:10]:  # Show first 10
                print(f"    - {irreg}")
            if len(folder_results['irregularities']) > 10:
                print(f"    ... and {len(folder_results['irregularities']) - 10} more")
    
    print(f"\nCAOs to be deleted from input_pdfs_extra: {', '.join(caos_to_check)}")
    print(f"Total: {len(caos_to_check)} CAO(s)")
    print("Note: Only folders will be deleted, CSV files will remain unchanged")
    
    # Step 5: User confirmation
    print("\n" + "=" * 60)
    if not all_verified:
        print("WARNING: Irregularities were found during verification!")
        print("Please review the issues above before proceeding.")
        print("=" * 60)
    
    response = input(f"\nDo you want to delete {len(caos_to_check)} CAO folder(s) from input_pdfs_extra? (yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        print("Deletion cancelled.")
        return
    
    # Step 6: Delete duplicates
    print("\n[Step 6] Deleting duplicate folders...")
    deleted_folders = delete_cao_folders(caos_to_check)
    
    print("\n" + "=" * 60)
    print("Cleanup completed!")
    print(f"  Deleted {deleted_folders} CAO folder(s)")
    print("  Note: CSV files in input_pdfs_extra were not modified")
    print("=" * 60)


if __name__ == "__main__":
    main()

