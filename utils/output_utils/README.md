# Output Utils

This folder contains utility scripts for analyzing and managing extracted output data.

## Scripts Overview

### `OUTPUT_analyze_empty_json_files.py`
**Purpose**: Identifies and analyzes JSON files that are empty or contain no useful data
**Usage**: `python utils/output_utils/OUTPUT_analyze_empty_json_files.py`
**Function**: Quality control for extraction results

### `OUTPUT_analyze_extracted_data.py`
**Purpose**: Comprehensive analysis of extracted CAO data
**Usage**: `python utils/output_utils/OUTPUT_analyze_extracted_data.py`
**Function**: Provides detailed statistics and insights on extraction results

### `OUTPUT_compare_with_handAnalysis.py`
**Purpose**: Compares automated extraction results with manual analysis
**Usage**: `python utils/output_utils/OUTPUT_compare_with_handAnalysis.py`
**Function**: Validation and accuracy assessment of extraction

### `OUTPUT_delete_cao_files.py`
**Purpose**: Safely deletes specific CAO files from output folders
**Usage**: `python utils/output_utils/OUTPUT_delete_cao_files.py`
**Function**: File management and cleanup of output data

### `OUTPUT_merge_analysis_results.py`
**Purpose**: Merges multiple analysis results into consolidated reports
**Usage**: `python utils/output_utils/OUTPUT_merge_analysis_results.py`
**Function**: Combines analysis data for comprehensive reporting

### `OUTPUT_tracker.py`
**Purpose**: Tracks progress and status of output file processing
**Usage**: `python utils/output_utils/OUTPUT_tracker.py`
**Function**: Monitoring and progress reporting for batch operations

## Purpose

These scripts help with:
- **Quality control**: Identifying empty or problematic extractions
- **Analysis**: Understanding extraction results and data quality
- **Validation**: Comparing automated results with manual analysis
- **Management**: Organizing and cleaning up output files
- **Monitoring**: Tracking progress of processing operations

## Typical Workflow

1. **Check for empty files**:
   ```bash
   python utils/output_utils/OUTPUT_analyze_empty_json_files.py
   ```

2. **Analyze extracted data**:
   ```bash
   python utils/output_utils/OUTPUT_analyze_extracted_data.py
   ```

3. **Compare with manual analysis**:
   ```bash
   python utils/output_utils/OUTPUT_compare_with_handAnalysis.py
   ```

4. **Merge results**:
   ```bash
   python utils/output_utils/OUTPUT_merge_analysis_results.py
   ```

5. **Track progress**:
   ```bash
   python utils/output_utils/OUTPUT_tracker.py
   ```

## Output Data Structure
```
outputs/
├── llm_extracted/    # Extracted JSON data
│   ├── new_flow/     # New extraction method
│   ├── old_flow/     # Old extraction method
│   └── single_file/  # Single file extractions
├── analysis/         # Analysis results
├── logs/            # Processing logs
└── comparison/      # Comparison results
```
