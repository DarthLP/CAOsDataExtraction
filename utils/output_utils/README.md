# Output Utils

This folder contains utility scripts for analyzing and managing extracted output data.

## Scripts Overview

### `OUTPUT_analyze_empty_json_files.py`
**Purpose**: Identifies and analyzes JSON files that are empty or contain no useful data
**Usage**: `python utils/output_utils/OUTPUT_analyze_empty_json_files.py`
**Function**: Quality control for extraction results

## Purpose

This script helps with:
- **Quality control**: Identifying empty or problematic extractions

## Typical Workflow

1. **Check for empty files**:
   ```bash
   python utils/output_utils/OUTPUT_analyze_empty_json_files.py
   ```

## Output Data Structure
```
outputs/
├── llm_extracted/    # Extracted JSON data
│   └── new_flow/     # New extraction method (p3_llmExtraction.py output)
├── analysis/         # Analysis results
├── logs/            # Processing logs
└── comparison/      # Comparison results
```
