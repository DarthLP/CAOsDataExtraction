# Unicode Processing Scripts

This folder contains scripts for detecting and fixing encoding issues in extracted JSON files.

## Scripts Overview

### 1. `find_unicode_files.py`
**Purpose**: Analyzes JSON files to find those containing unicode patterns (`/uniXXXX/`)
**Usage**: `python scripts/unicode_processing/find_unicode_files.py`
**Output**: Creates `outputs/unicode_analysis.json` with analysis results

### 2. `copy_unicode_files.py`
**Purpose**: Copies files with unicode issues to a separate folder structure
**Usage**: `python scripts/unicode_processing/copy_unicode_files.py`
**Output**: Copies files to `outputs/unicode_pdfs/original/`

### 3. `transform_unicode_files.py`
**Purpose**: Converts unicode patterns (`/uniXXXX/`) to readable text
**Usage**: `python scripts/unicode_processing/transform_unicode_files.py`
**Output**: Saves transformed files to `outputs/unicode_pdfs/transformed/`

### 4. `transform_postscript_glyphs.py`
**Purpose**: Converts PostScript glyph patterns (`/GXXX/`) to readable text
**Usage**: `python scripts/unicode_processing/transform_postscript_glyphs.py`
**Output**: Saves transformed files to `outputs/unicode_pdfs/transformed/`

### 5. `unicode_json_processor.py`
**Purpose**: All-in-one script that finds, copies, and transforms unicode files
**Usage**: `python scripts/unicode_processing/unicode_json_processor.py`
**Output**: Both original and transformed files in the unicode_pdfs folder structure

## Typical Workflow

1. **Find problematic files**:
   ```bash
   python scripts/unicode_processing/find_unicode_files.py
   ```

2. **Copy files to safe location**:
   ```bash
   python scripts/unicode_processing/copy_unicode_files.py
   ```

3. **Transform unicode patterns**:
   ```bash
   python scripts/unicode_processing/transform_unicode_files.py
   ```

4. **Transform PostScript glyphs** (if needed):
   ```bash
   python scripts/unicode_processing/transform_postscript_glyphs.py
   ```

## Or use the all-in-one processor:
```bash
python scripts/unicode_processing/unicode_json_processor.py
```

## Output Structure
```
outputs/unicode_pdfs/
├── original/          # Original files with encoding issues
├── transformed/       # Clean, readable versions
└── unicode_analysis.json  # Analysis report
```
