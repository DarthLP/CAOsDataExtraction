#!/usr/bin/env python3
"""
Main entry point for the CAO Data Extraction Pipeline.
This script runs the complete pipeline from the reorganized structure.
"""

import sys
import os

# Add the current directory to Python path to ensure imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipelines.p5_run import main

if __name__ == "__main__":
    main()
