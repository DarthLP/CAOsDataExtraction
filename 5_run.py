

import subprocess

# Define the scripts to run
scripts = ["2_extract.py", "3_llmExtraction.py", "4_analysis.py"]

# Run each script in order
for script in scripts:
    print(f"\n--- Running {script} ---\n")
    subprocess.run(["python", script], check=True)