#!/usr/bin/env python3
"""
Script to merge analysis process files into the main extracted_data.xlsx file.
This is needed when the analysis script is stopped early and the automatic merge doesn't run.
"""

import pandas as pd
import os
from pathlib import Path
import sys

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
                # Filter out completely empty rows
                metadata_cols = ['CAO', 'TTW', 'File_name', 'id', 'start_date', 'expiry_date', 'date_of_formal_notification']
                content_cols = [col for col in df.columns if col not in metadata_cols]
                
                if content_cols:
                    # Check if any content column has non-empty values
                    df_filtered = df.dropna(subset=content_cols, how='all')
                    # Also remove rows where all content columns are just empty strings
                    mask = df_filtered[content_cols].replace(['', ' ', 'Empty'], pd.NA).notna().any(axis=1)
                    df_filtered = df_filtered[mask]
                else:
                    # If no content columns, keep all rows
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
    
    # Combine new data from current processes
    print(f"\n📊 Combining {len(all_dataframes)} dataframes...")
    new_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"✓ Combined {len(new_df)} total rows from process files")
    
    # Note: Multiple rows per file is expected (one per infotype)
    # So we don't remove "duplicates" based on File_name
    print(f"✓ Keeping all {len(new_df)} rows (multiple rows per file is normal)")
    
    # Merge with existing Excel file if it exists
    if os.path.exists(final_excel_path):
        print(f"\n📖 Reading existing {final_excel_path}...")
        try:
            existing_df = pd.read_excel(final_excel_path)
            print(f"  - Found {len(existing_df)} existing rows")
            
            # Check for overlapping files between existing and new data
            if 'File_name' in existing_df.columns and 'File_name' in new_df.columns:
                existing_files = set(existing_df['File_name'].dropna())
                new_files = set(new_df['File_name'].dropna())
                overlap = existing_files & new_files
                
                if overlap:
                    print(f"⚠️  Found {len(overlap)} files that already exist in the main file")
                    print(f"  - Overlapping files: {list(overlap)[:5]}{'...' if len(overlap) > 5 else ''}")
                    
                    # Remove overlapping files from new data to avoid double-processing
                    new_df = new_df[~new_df['File_name'].isin(overlap)]
                    print(f"✓ Removed overlapping files, now have {len(new_df)} new unique files")
                else:
                    print(f"✓ No overlapping files found")
            
            # Combine existing and new data
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"✓ Final result: {len(existing_df)} existing + {len(new_df)} new = {len(final_df)} total rows")
            
        except Exception as e:
            print(f"❌ Error reading existing Excel: {e}")
            final_df = new_df
            print(f"✓ Using only new data: {len(final_df)} rows")
    else:
        print(f"\n📝 No existing {final_excel_path} found, creating new file")
        final_df = new_df
        print(f"✓ Creating new file with {len(final_df)} rows")
    
    # Save final merged file
    os.makedirs(os.path.dirname(final_excel_path), exist_ok=True)
    final_df.to_excel(final_excel_path, index=False)
    print(f"\n✅ Final results saved to {final_excel_path}")
    print(f"📊 Summary: {len(final_df)} total rows")
    
    # Show breakdown by infotype if available
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