#!/usr/bin/env python3
"""
Script to merge analysis process files into the main extracted_data.xlsx file.
This is needed when the analysis script is stopped early and the automatic merge doesn't run.
"""

import pandas as pd
import os
from pathlib import Path
import sys
import time
import tempfile
import platform
import subprocess


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
            fd, temp_file = tempfile.mkstemp(prefix="merge_tmp_", suffix=".xlsx", dir=directory)
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


def merge_analysis_results():
    """Merge all process-specific Excel files into the main extracted_data.xlsx"""
    
    # Configuration
    final_excel_path = "results/extracted_data.xlsx"
    available_keys = []
    
    # Detect how many API keys were used (check for process files)
    i = 1
    while os.path.exists(f"results/extracted_data_process_{i}.xlsx"):
        available_keys.append(i)
        i += 1
    
    if not available_keys:
        print("❌ No process files found in results/ directory")
        return
    
    print(f"🔍 Found {len(available_keys)} process files: {available_keys}")
    
    # Collect data from all process files
    all_dataframes = []
    total_new_rows = 0
    
    for key_num in available_keys:
        process_file = f"results/extracted_data_process_{key_num}.xlsx"
        print(f"📖 Reading {process_file}...")
        
        try:
            df = pd.read_excel(process_file)
            print(f"  - Found {len(df)} rows")
            
            if not df.empty:
                metadata_cols = ['CAO', 'TTW', 'File_name', 'id', 'start_date', 'expiry_date', 'date_of_formal_notification']
                content_cols = [col for col in df.columns if col not in metadata_cols]
                
                if content_cols:
                    df_filtered = df.dropna(subset=content_cols, how='all')
                    mask = df_filtered[content_cols].replace(['', ' ', 'Empty'], pd.NA).notna().any(axis=1)
                    df_filtered = df_filtered[mask]
                else:
                    df_filtered = df
                
                if not df_filtered.empty:
                    all_dataframes.append(df_filtered)
                    total_new_rows += len(df_filtered)
                    print(f"  - Kept {len(df_filtered)} non-empty rows")
                else:
                    print(f"  - All rows were empty, skipping")
            else:
                print(f"  - File is empty")
                
        except Exception as e:
            print(f"  ❌ Error reading {process_file}: {e}")
    
    if not all_dataframes:
        print("❌ No valid data found in any process file")
        return
    
    print(f"\n📊 Combining {len(all_dataframes)} dataframes...")
    new_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"✓ Combined {len(new_df)} total rows from process files")
    
    print(f"✓ Keeping all {len(new_df)} rows (multiple rows per file is normal)")
    
    if os.path.exists("results/extracted_data.xlsx"):
        print(f"\n📖 Reading existing results/extracted_data.xlsx...")
        try:
            existing_df = pd.read_excel("results/extracted_data.xlsx")
            print(f"  - Found {len(existing_df)} existing rows")
            
            if 'File_name' in existing_df.columns and 'File_name' in new_df.columns and 'CAO' in existing_df.columns and 'CAO' in new_df.columns:
                # Use both File_name AND CAO number to allow same filename in different CAOs
                # Create composite key for comparison
                existing_df['file_cao_key'] = existing_df['File_name'] + '_' + existing_df['CAO'].astype(str)
                new_df['file_cao_key'] = new_df['File_name'] + '_' + new_df['CAO'].astype(str)
                
                existing_keys = set(existing_df['file_cao_key'].dropna())
                new_keys = set(new_df['file_cao_key'].dropna())
                overlap = existing_keys & new_keys
                
                if overlap:
                    print(f"⚠️  Found {len(overlap)} file-CAO combinations that already exist in the main file")
                    print(f"  - Overlapping combinations: {list(overlap)[:5]}{'...' if len(overlap) > 5 else ''}")
                    new_df = new_df[~new_df['file_cao_key'].isin(overlap)]
                    print(f"✓ Removed overlapping file-CAO combinations, now have {len(new_df)} new unique combinations")
                else:
                    print(f"✓ No overlapping file-CAO combinations found")
                
                # Clean up temporary column
                existing_df = existing_df.drop('file_cao_key', axis=1)
                new_df = new_df.drop('file_cao_key', axis=1)
            
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"✓ Final result: {len(existing_df)} existing + {len(new_df)} new = {len(final_df)} total rows")
            
        except Exception as e:
            print(f"❌ Error reading existing Excel: {e}")
            final_df = new_df
            print(f"✓ Using only new data: {len(final_df)} rows")
    else:
        print(f"\n📝 No existing results/extracted_data.xlsx found, creating new file")
        final_df = new_df
        print(f"✓ Creating new file with {len(final_df)} rows")
    
    _atomic_save_excel_with_retries(final_df, "results/extracted_data.xlsx")
    print(f"\n✅ Final results saved to results/extracted_data.xlsx")
    print(f"📊 Summary: {len(final_df)} total rows")
    
    if 'infotype' in final_df.columns:
        print(f"\n📋 Breakdown by infotype:")
        infotype_counts = final_df['infotype'].value_counts()
        for infotype, count in infotype_counts.items():
            print(f"  {infotype}: {count} rows")
    
    return final_df

if __name__ == "__main__":
    print("🔄 Merging analysis process files...")
    result_df = merge_analysis_results()
    if result_df is not None:
        print("\n✅ Merge completed successfully!")
    else:
        print("\n❌ Merge failed!")
        sys.exit(1) 