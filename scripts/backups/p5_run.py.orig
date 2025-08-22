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
    print(f"\n{'='*60}")
    print(f"PHASE {idx + 1}/{len(scripts)}: {script}")
    print(f"{'='*60}")
    
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
        
        print(f"\n✓ PHASE {idx + 1} COMPLETED: LLM Extraction")
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
        
        print(f"\n✓ PHASE {idx + 1} COMPLETED: Analysis")
        
        # Merge all process-specific Excel files into one final file using robust merge script
        print(f"\n🔄 Merging analysis results to final extracted_data.xlsx...")
        try:
            subprocess.run([python_executable, "OUTPUT_merge_analysis_results.py"], check=True)
            print(f"✓ Merge completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"❌ Merge failed with error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error during merge: {e}")
    else:
        print(f"\n--- Running {script} ---\n")
        subprocess.run([python_executable, script], check=True)
    # Add 2-minute delay between scripts (except after the last one)
    if idx < len(scripts) - 1:
        print(f"\n--- Waiting 5 minutes before next script ---\n")
        time.sleep(300)