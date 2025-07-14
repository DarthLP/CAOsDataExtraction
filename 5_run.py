"""
Script to sequentially run the main CAO extraction and analysis scripts.
Ensures correct order and adds delays between steps to avoid rate limiting.
"""

import subprocess
import time
import sys

# =========================
# Script Execution Sequence
# =========================

# Define the scripts to run in order
scripts = ["3_llmExtraction.py", "4_analysis.py"]
# scripts = ["0_webscrapping.py", "2_extract.py", "3_llmExtraction.py", "4_analysis.py"]

# Use the current Python executable to ensure we're in the right environment
python_executable = sys.executable

# Run each script in order
for i, script in enumerate(scripts):
    print(f"\n--- Running {script} ---\n")
    subprocess.run([python_executable, script], check=True)
    
    # Add 2-minute delay between scripts (except after the last one)
    if i < len(scripts) - 1:
        print(f"\n--- Waiting 2 minutes before next script ---\n")
        time.sleep(120)