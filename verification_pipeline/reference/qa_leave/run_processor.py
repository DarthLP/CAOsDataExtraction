#!/usr/bin/env python3
"""
Execute the correction processor and output results.
This script will be run externally to generate the CSV.
"""

import subprocess
import sys

script = '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/correction_minimal.py'

result = subprocess.run([sys.executable, script], capture_output=True, text=True)

# Write stderr (status output) to stdout so we can see it
print(result.stderr)
if result.returncode != 0:
    print(f"Error: {result.returncode}", file=sys.stderr)
    print(result.stdout, file=sys.stderr)
    sys.exit(result.returncode)
