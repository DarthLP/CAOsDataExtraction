"""
Script to sequentially run the main CAO extraction and analysis scripts.
Ensures correct order and adds delays between steps to avoid rate limiting.
"""

import subprocess
import time
import sys
import os
from dotenv import load_dotenv

# =========================
# Configuration and Setup
# =========================

# Load environment variables from .env file
load_dotenv()

# Define the scripts to run in order
scripts = ["4_analysis.py"]
# scripts = ["0_webscrapping.py", "2_extract.py", "3_llmExtraction.py", "4_analysis.py"]


# =========================
# Script Execution Sequence
# =========================

# Automatically detect number of API keys available
def count_api_keys():
    """Count the number of GOOGLE_API_KEY* environment variables available."""
    api_keys = []
    i = 1
    while os.getenv(f"GOOGLE_API_KEY{i}"):
        api_keys.append(i)
        i += 1
    return api_keys

# Get available API keys
available_keys = count_api_keys()
num_keys = len(available_keys)

if num_keys == 0:
    print("Error: No GOOGLE_API_KEY* environment variables found!")
    sys.exit(1)

print(f"Found {num_keys} API keys: {available_keys}")

# Clean up any leftover lock files from previous interrupted runs
from pathlib import Path
llm_extracted_folder = Path("llmExtracted_json")
results_folder = Path("results")

if llm_extracted_folder.exists():
    # Remove all .lock files (from LLM extraction)
    for lock_file in llm_extracted_folder.rglob("*.lock"):
        try:
            lock_file.unlink()
            print(f"Cleaned up orphaned lock file: {lock_file.name}")
        except:
            pass  # Ignore errors when cleaning up
    
    # Remove all analysis .lock files
    for lock_file in llm_extracted_folder.rglob("*.analysis_lock"):
        try:
            lock_file.unlink()
            print(f"Cleaned up orphaned analysis lock file: {lock_file.name}")
        except:
            pass  # Ignore errors when cleaning up
    
    # Remove all CAO announcement files from previous runs
    for announce_file in llm_extracted_folder.glob(".cao_*_announced"):
        try:
            announce_file.unlink()
        except:
            pass  # Ignore errors when cleaning up

if results_folder.exists():
    # Remove analysis announcement files from previous runs
    for announce_file in results_folder.glob(".cao_*_analysis_announced"):
        try:
            announce_file.unlink()
        except:
            pass  # Ignore errors when cleaning up
    


# Use the current Python executable to ensure we're in the right environment
python_executable = sys.executable

for idx, script in enumerate(scripts):
    if script == "3_llmExtraction.py":
        print(f"\n--- Launching {num_keys} parallel LLM extraction processes (one per API key) ---\n")
        processes = []
        for key_num in available_keys:
            process_id = key_num - 1  # 0, 1, 2, ...
            p = subprocess.Popen([python_executable, script, str(key_num), str(process_id), str(num_keys)])
            processes.append(p)
        for proc_idx, p in enumerate(processes, 1):
            ret = p.wait()
            print(f"Process {proc_idx} (GOOGLE_API_KEY{available_keys[proc_idx-1]}) exited with code {ret}")
        
        # Clean up CAO announcement files after all processes complete
        from pathlib import Path
        llm_extracted_folder = Path("llmExtracted_json")
        if llm_extracted_folder.exists():
            for announce_file in llm_extracted_folder.glob(".cao_*_announced"):
                try:
                    announce_file.unlink()
                except:
                    pass  # Ignore errors when cleaning up
    elif script == "4_analysis.py":
        print(f"\n--- Launching {num_keys} parallel analysis processes (one per API key) ---\n")
        processes = []
        for key_num in available_keys:
            process_id = key_num - 1  # 0, 1, 2, ...
            p = subprocess.Popen([python_executable, script, str(key_num), str(process_id), str(num_keys)])
            processes.append(p)
        for proc_idx, p in enumerate(processes, 1):
            ret = p.wait()
            print(f"Process {proc_idx} (GOOGLE_API_KEY{available_keys[proc_idx-1]}) exited with code {ret}")
        
        # Clean up analysis announcement files and lock files after all processes complete
        from pathlib import Path
        results_folder = Path("results")
        llm_extracted_folder = Path("llmExtracted_json")
        
        if results_folder.exists():
            for announce_file in results_folder.glob(".cao_*_analysis_announced"):
                try:
                    announce_file.unlink()
                except:
                    pass  # Ignore errors when cleaning up
            

        
        if llm_extracted_folder.exists():
            for lock_file in llm_extracted_folder.rglob("*.analysis_lock"):
                try:
                    lock_file.unlink()
                except:
                    pass  # Ignore errors when cleaning up
        
        # Merge all process-specific Excel files into one final file
        import pandas as pd
        final_excel_path = "results/extracted_data.xlsx"
        all_dataframes = []
        
        for key_num in available_keys:
            process_file = f"results/extracted_data_process_{key_num}.xlsx"
            if os.path.exists(process_file):
                try:
                    df = pd.read_excel(process_file)
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
                except Exception as e:
                    print(f"Warning: Could not read {process_file}: {e}")
        
        if all_dataframes:
            # Combine new data from current processes
            new_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Merge with existing Excel file if it exists
            if os.path.exists(final_excel_path):
                try:
                    existing_df = pd.read_excel(final_excel_path)
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
                    print(f"✓ Added {len(new_df)} new rows to existing {len(existing_df)} rows")
                except Exception as e:
                    print(f"Warning: Could not read existing Excel: {e}")
                    final_df = new_df
            else:
                final_df = new_df
            
            # Save final merged file
            os.makedirs(os.path.dirname(final_excel_path), exist_ok=True)
            final_df.to_excel(final_excel_path, index=False)
            print(f"✓ Final results saved: {len(final_df)} total rows")
        else:
            print("Warning: No data found from any analysis process")
    else:
        print(f"\n--- Running {script} ---\n")
        subprocess.run([python_executable, script], check=True)
    # Add 2-minute delay between scripts (except after the last one)
    if idx < len(scripts) - 1:
        print(f"\n--- Waiting 2 minutes before next script ---\n")
        time.sleep(120)