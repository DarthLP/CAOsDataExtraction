# Input Utils

This folder contains utility scripts for working with input data (PDFs, CSV files, and data preparation).

## Scripts Overview

### `INPUT_compare_pdfs_csv_vs_disk.py`
**Purpose**: Compares PDF files listed in CSV files against what's actually on disk
**Usage**: `python utils/input_utils/INPUT_compare_pdfs_csv_vs_disk.py`
**Function**: Helps identify missing or extra PDF files in your input dataset

### `INPUT_rename_to_original_pdf_names.py`
**Purpose**: Renames PDF files to their original names for consistency
**Usage**: `python utils/input_utils/INPUT_rename_to_original_pdf_names.py`
**Function**: Ensures PDF filenames match their original names from the source

## Purpose

These scripts help with:
- **Data validation**: Ensuring your input PDFs are complete and correctly named
- **File management**: Organizing and preparing input files for processing
- **Quality control**: Identifying discrepancies between expected and actual files

## Typical Workflow

1. **Compare files against CSV**:
   ```bash
   python utils/input_utils/INPUT_compare_pdfs_csv_vs_disk.py
   ```

2. **Rename files to originals**:
   ```bash
   python utils/input_utils/INPUT_rename_to_original_pdf_names.py
   ```

## Input Data Structure
```
inputs/pdfs/input_pdfs/
├── 10/          # CAO number folders
│   ├── file1.pdf
│   └── file2.pdf
├── 1536/
│   └── file3.pdf
└── ...
```
