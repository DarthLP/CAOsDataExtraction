"""
CAO Data Analysis - Excel Creation Pipeline (p5_excel_creation.py)

This script creates separate Excel outputs for salary and non-salary data from LLM extraction results.
It merges data from three non-salary folders, adds CAO info, and creates final Excel files.

USAGE:
    
    With file limit:
        python pipelines/p5_excel_creation.py --max_files 10

ARGUMENTS:
    --max_files: Maximum number of files to process (optional)

INPUT:
    - LLM extraction results in outputs/llm_analysis/salary/ and outputs/llm_analysis/non_salary/
    - CAO info from inputs/pdfs/extracted_cao_info.csv

OUTPUT:
    - CSV files in outputs/excel/new_results/
      - extracted_data_salary.csv
      - extracted_data_non_salary.csv
"""

# =============================================================================
# IMPORTS
# =============================================================================
import os
import sys
import json
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Third-party imports
import pandas as pd
import yaml

# Import Excel schema definitions
from schema.excel_output_schema import (
    get_salary_columns, get_non_salary_columns, 
    flatten_salary_row, flatten_non_salary_data
)

# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass
class ExcelConfig:
    """Configuration for Excel creation."""
    llm_analysis_folder: Path
    cao_info_path: str
    output_folder: Path
    max_json_files: int = 1000000  # Default value for max files to process

def load_configuration() -> ExcelConfig:
    """Load and validate configuration from config.yaml."""
    # Resolve project root (two levels up from this file: pipelines/ -> repo root)
    project_root = Path(__file__).resolve().parents[1]
    
    with open(project_root / 'conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    return ExcelConfig(
        llm_analysis_folder=project_root / 'outputs/llm_analysis',
        cao_info_path=str(project_root / f"{config_data['paths']['inputs_pdfs']}/extracted_cao_info.csv"),
        output_folder=project_root / config_data['paths']['outputs_excel'] / "new_results"
    )

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
def truncate_text_for_excel(text: str, max_length: int = 32000) -> str:
    """
    Truncate text to fit Excel cell limits.
    
    Args:
        text: Text to truncate
        max_length: Maximum length (default 32000 to stay under Excel's 32767 limit)
        
    Returns:
        str: Truncated text with ellipsis if needed
    """
    if not text or not isinstance(text, str):
        return text
    
    if len(text) <= max_length:
        return text
    
    # Truncate and add ellipsis
    return text[:max_length-3] + "..."

def load_cao_info(cao_info_path: str) -> Dict[str, Dict[str, str]]:
    """Load CAO information from CSV file."""
    cao_info_mapping = {}
    
    print(f"Loading CAO info from: {cao_info_path}")
    
    if os.path.exists(cao_info_path):
        try:
            df = pd.read_csv(cao_info_path, sep=';')  # Use semicolon separator
            print(f"  Loaded {len(df)} CAO info records")
            print(f"  Columns: {list(df.columns)}")
            
            # Helper function to safely get string value, handling NaN
            def safe_get_str(row, key, default=''):
                """Get string value from row, converting NaN to empty string."""
                value = row.get(key, default)
                if pd.isna(value):
                    return default
                return str(value) if value != '' else default
            
            for _, row in df.iterrows():
                cao_number = safe_get_str(row, 'cao_number', '')
                pdf_name = safe_get_str(row, 'pdf_name', '')
                if cao_number and pdf_name:
                    # Index by cao_number:pdf_name directly (no normalization)
                    # The base_filename from analysis files will be used to reconstruct the PDF name
                    key = f"{cao_number}:{pdf_name}"
                    cao_info_mapping[key] = {
                        'cao_number': cao_number,
                        'id': safe_get_str(row, 'id', ''),
                        'pdf_name': pdf_name,
                        'TTW': 'yes' if 'TTW' in pdf_name.upper() else 'no',
                        'ingangsdatum': safe_get_str(row, 'ingangsdatum', ''),
                        'expiratiedatum': safe_get_str(row, 'expiratiedatum', ''),
                        'datum_kennisgeving': safe_get_str(row, 'datum_kennisgeving', '')
                    }
            print(f"  Mapped {len(cao_info_mapping)} unique CAO entries")
        except Exception as e:
            print(f"Warning: Could not load CAO info from {cao_info_path}: {e}")
    else:
        print(f"Warning: CAO info file not found: {cao_info_path}")
    
    return cao_info_mapping

def match_cao_info(cao_number: str, filename: str, cao_info_mapping: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """
    Match CAO info using direct filename matching.
    
    The base_filename from analysis files preserves the original PDF structure:
    - If base_filename ends with .pdf, the original PDF was .pdf.pdf
    - If base_filename doesn't end with .pdf, the original PDF was .pdf
    
    We reconstruct the PDF name by adding .pdf to base_filename and match directly.
    
    Args:
        cao_number: CAO number from folder name
        filename: Base filename from analysis file (e.g., "Cao Bouw en Infra 2025 - 2027.pdf" or "Cao Bouw en Infra 2025 - 2027")
        cao_info_mapping: CAO info mapping from CSV (indexed by "cao_number:pdf_name")
        
    Returns:
        Dict with CAO metadata
    """
    cao_info = {}  # Initialize
    
    # Reconstruct the PDF name by adding .pdf to the base_filename
    # The base_filename already preserves the structure from the original PDF
    expected_pdf_name = filename + '.pdf'
    
    # Try exact match first: cao_number:expected_pdf_name
    cao_info_key = f"{cao_number}:{expected_pdf_name}"
    cao_info = cao_info_mapping.get(cao_info_key, {})
    
    if cao_info:
        print(f'    Found CAO info by direct match: {cao_info_key}')
    
    # If no exact match, try to find by PDF name only (in case CAO number folder is wrong)
    if not cao_info:
        for key, info in cao_info_mapping.items():
            if info.get('pdf_name', '') == expected_pdf_name:
                cao_info = info
                print(f'    Found CAO info by PDF name match: {key}')
                break
    
    # Create metadata dict
    metadata = {
        'cao_number': cao_number,
        'id': cao_info.get('id', ''),
        'TTW': cao_info.get('TTW', ''),
        'ingangsdatum': cao_info.get('ingangsdatum', ''),
        'expiratiedatum': cao_info.get('expiratiedatum', ''),
        'datum_kennisgeving': cao_info.get('datum_kennisgeving', ''),
        'file_name': filename
    }
    
    if cao_info:
        print(f'    Added CAO info: id={metadata["id"]}, TTW={metadata["TTW"]}')
    else:
        print(f'    No CAO info found for CAO {cao_number}, filename {filename} (expected PDF: {expected_pdf_name})')
    
    return metadata

def load_non_salary_data(cao_number: str, filename: str, llm_analysis_folder: Path) -> Dict[str, Any]:
    """
    Load and merge non-salary data from three folders.
    
    Args:
        cao_number: CAO number
        filename: Base filename (without extensions)
        llm_analysis_folder: Path to llm_analysis folder
        
    Returns:
        Dict with merged non-salary data
    """
    non_salary_folders = ['gen_bon_wag_pen_ter', 'lea_ove_tra', 'hom_con_saf_chi_ai_fri']
    merged_data = {}
    
    for folder in non_salary_folders:
        non_salary_file = llm_analysis_folder / 'non_salary' / folder / cao_number / f"{filename}_analysis.json"
        
        if non_salary_file.exists():
            try:
                with open(non_salary_file, 'r', encoding='utf-8') as f:
                    folder_data = json.load(f)
                    merged_data.update(folder_data)
                print(f'    Loaded non-salary data from {folder}')
            except Exception as e:
                print(f'    Warning: Could not load {non_salary_file}: {e}')
        else:
            print(f'    Warning: Non-salary file not found: {non_salary_file}')
    
    return merged_data

# =============================================================================
# FILE PROCESSING FUNCTIONS
# =============================================================================
def discover_llm_files(llm_analysis_folder: Path) -> List[tuple]:
    """
    Discover all LLM analysis files.
    
    Uses non-salary folders as source of truth to ensure all files are discovered,
    even if salary extraction failed for some files. For each file found in non-salary
    folders, looks for corresponding salary file (which may or may not exist).
    """
    files = []
    discovered_files = set()  # Track (cao_number, base_filename) to avoid duplicates
    
    # Use non-salary folders as source of truth (they have all 1580 files)
    # Check the first non-salary folder to discover all files
    non_salary_folder = llm_analysis_folder / 'non_salary' / 'gen_bon_wag_pen_ter'
    salary_folder = llm_analysis_folder / 'salary'
    
    if non_salary_folder.exists():
        for cao_folder in non_salary_folder.iterdir():
            if cao_folder.is_dir():
                cao_number = cao_folder.name
                for json_file in cao_folder.glob('*_analysis.json'):
                    # Extract base filename
                    base_filename = json_file.stem.replace('_analysis', '')
                    
                    # Avoid duplicates
                    file_key = (cao_number, base_filename)
                    if file_key in discovered_files:
                        continue
                    discovered_files.add(file_key)
                    
                    # Look for corresponding salary file (may not exist)
                    salary_file = salary_folder / cao_number / f"{base_filename}_analysis.json"
                    
                    # Use salary file if it exists, otherwise use non-salary file as placeholder
                    # (process_non_salary_file will handle missing salary gracefully)
                    if salary_file.exists():
                        files.append((cao_number, base_filename, salary_file))
                    else:
                        # File has non-salary but no salary - still process it
                        files.append((cao_number, base_filename, None))
    
    return files

def determine_max_timeline_length(files: List[tuple]) -> int:
    """
    Determine the maximum timeline length across all salary files.
    
    Handles all schema variants:
    - Regular schema: nested 'timeline' array
    - Compact/split schema: nested 'tl' array
    - Super compact schema: parallel 'sd' and 'am' arrays
    """
    max_timeline = 0
    
    print("Determining max timeline length...")
    for cao_number, base_filename, salary_file in files:
        if salary_file is None:
            continue  # Skip files without salary data
        try:
            with open(salary_file, 'r', encoding='utf-8') as f:
                salary_data = json.load(f)
                # Handle both 'salary_information' (regular) and 'si' (compact/split/super compact)
                salary_rows = salary_data.get('salary_information') or salary_data.get('si', [])
                for salary_row in salary_rows:
                    # Check for super compact schema with parallel arrays (sd/am)
                    if 'sd' in salary_row and 'am' in salary_row:
                        # Super compact schema: use length of sd or am array
                        sd_array = salary_row.get('sd', [])
                        am_array = salary_row.get('am', [])
                        timeline_length = max(len(sd_array), len(am_array))
                        max_timeline = max(max_timeline, timeline_length)
                    else:
                        # Regular/compact/split schema: nested timeline format
                        timeline = salary_row.get('timeline') or salary_row.get('tl', [])
                        max_timeline = max(max_timeline, len(timeline))
        except Exception as e:
            print(f"Warning: Could not read {salary_file} for timeline analysis: {e}")
    
    print(f"Max timeline length determined: {max_timeline}")
    return max_timeline

def process_salary_file(cao_number: str, base_filename: str, salary_file: Optional[Path], 
                       cao_info_mapping: Dict[str, Dict[str, str]], config: ExcelConfig, 
                       max_timeline_length: int) -> List[dict]:
    """
    Process a single salary file and return Excel rows.
    
    Args:
        salary_file: Path to salary file, or None if file doesn't exist (has non-salary but no salary)
    """
    excel_rows = []
    
    if salary_file is None or not salary_file.exists():
        # File has non-salary data but no salary data - return empty list
        return excel_rows
    
    # Load salary data
    with open(salary_file, 'r', encoding='utf-8') as f:
        salary_data = json.load(f)
    
    # Handle both 'salary_information' (regular) and 'si' (compact/split)
    salary_rows = salary_data.get('salary_information') or salary_data.get('si', [])
    
    # If no salary rows found, create a placeholder row to track that this file was processed
    if not salary_rows or len(salary_rows) == 0:
        # Get CAO metadata
        cao_metadata = match_cao_info(cao_number, base_filename, cao_info_mapping)
        # Create a minimal row with just metadata to track this file
        # This ensures files with empty salary_information arrays are still counted
        placeholder_row = {
            'cao_number': cao_number,
            'file_name': base_filename,
        }
        # Add all metadata fields
        if cao_metadata:
            placeholder_row.update(cao_metadata)
        # Note: Empty salary fields will be filled by DataFrame creation to maintain schema
        excel_rows.append(placeholder_row)
        print(f'  {cao_number}: No salary data found in {salary_file.name} (created placeholder row)')
        return excel_rows
    
    # Get CAO metadata
    cao_metadata = match_cao_info(cao_number, base_filename, cao_info_mapping)
    
    # Convert to Excel rows (wide format)
    for salary_row in salary_rows:
        row = flatten_salary_row(salary_row, cao_metadata, max_timeline_length)
        excel_rows.append(row)
    
    print(f'  {cao_number}: Processed {len(excel_rows)} salary rows from {salary_file.name}')
    return excel_rows

def process_non_salary_file(cao_number: str, base_filename: str, cao_info_mapping: Dict[str, Dict[str, str]], 
                           config: ExcelConfig) -> Dict[str, Any]:
    """Process non-salary data for a single file and return Excel row."""
    # Load and merge non-salary data
    non_salary_data = load_non_salary_data(cao_number, base_filename, config.llm_analysis_folder)
    
    # Get CAO metadata
    cao_metadata = match_cao_info(cao_number, base_filename, cao_info_mapping)
    
    # Convert to Excel row
    excel_row = flatten_non_salary_data(non_salary_data, cao_metadata)
    
    print(f'  {cao_number}: Processed non-salary data for {base_filename}')
    return excel_row

# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
    """Main entry point for Excel creation."""
    parser = argparse.ArgumentParser(description='CAO Data Excel Creation')
    parser.add_argument('--max_files', type=int, help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_configuration()
        config.output_folder.mkdir(parents=True, exist_ok=True)
        
        # Load CAO info
        cao_info_mapping = load_cao_info(config.cao_info_path)
        
        # Discover files
        all_files = discover_llm_files(config.llm_analysis_folder)
        print(f"  Discovered {len(all_files)} total files")
        
        # Apply file limit
        max_files = args.max_files if args.max_files is not None else config.max_json_files
        print(f"  Max files limit: {max_files}")
        if max_files and max_files < len(all_files):
            all_files = all_files[:max_files]
            print(f"  Limited to {len(all_files)} files due to max_files limit")
        
        # Determine max timeline length dynamically
        max_timeline_length = determine_max_timeline_length(all_files)
        
        print(f'Processing {len(all_files)} files')
        
        # Process files
        salary_results = []
        non_salary_results = []
        successful_files = 0
        failed_files = []
        
        for cao_number, base_filename, salary_file in all_files:
            try:
                # Process salary data (may be None if file doesn't exist)
                salary_rows = process_salary_file(cao_number, base_filename, salary_file, cao_info_mapping, config, max_timeline_length)
                salary_results.extend(salary_rows)
                
                # Process non-salary data
                non_salary_row = process_non_salary_file(cao_number, base_filename, cao_info_mapping, config)
                non_salary_results.append(non_salary_row)
                
                successful_files += 1
                file_name = salary_file.name if salary_file else f"{base_filename}_analysis.json (no salary)"
                print(f'  {cao_number}: Processed {file_name}')
            except Exception as e:
                file_name = salary_file.name if salary_file else f"{base_filename}_analysis.json (no salary)"
                print(f'  {cao_number}: Error processing {file_name}: {e}')
                failed_files.append(file_name)
        
        # Create DataFrames and save Excel files
        if salary_results:
            try:
                # Get column definitions with dynamic max timeline length
                salary_columns = get_salary_columns(max_timeline_length)
                
                # Create DataFrame
                df_salary = pd.DataFrame(salary_results)
                
                # Ensure all columns exist (fill missing with empty values)
                missing_cols = [col for col in salary_columns if col not in df_salary.columns]
                if missing_cols:
                    df_salary = pd.concat([df_salary, pd.DataFrame(columns=missing_cols)], axis=1)
                
                # Reorder columns
                df_salary = df_salary[salary_columns]
                
                print(f'  Created salary DataFrame with {len(df_salary)} rows and {len(df_salary.columns)} columns')
                print(f'  Max timeline length used: {max_timeline_length}')
                
                # Save salary CSV
                salary_output_path = config.output_folder / "extracted_data_salary.csv"
                df_salary.to_csv(salary_output_path, index=False, encoding='utf-8', sep=';')
                print(f'  Saved salary CSV file: {salary_output_path}')
                
            except Exception as e:
                print(f'  ERROR creating salary CSV file: {e}')
        
        if non_salary_results:
            try:
                # Get column definitions
                non_salary_columns = get_non_salary_columns()
                
                # Create DataFrame
                df_non_salary = pd.DataFrame(non_salary_results)
                
                # Ensure all columns exist (fill missing with empty values)
                missing_cols = [col for col in non_salary_columns if col not in df_non_salary.columns]
                if missing_cols:
                    df_non_salary = pd.concat([df_non_salary, pd.DataFrame(columns=missing_cols)], axis=1)
                
                # Add any additional columns that were created by flattening but not in the schema
                additional_cols = [col for col in df_non_salary.columns if col not in non_salary_columns]
                if additional_cols:
                    print(f'  Adding {len(additional_cols)} additional columns from flattening (Amount/AmountRange)')
                    non_salary_columns.extend(additional_cols)
                
                # Reorder columns
                df_non_salary = df_non_salary[non_salary_columns]
            
                print(f'  Created non-salary DataFrame with {len(df_non_salary)} rows and {len(df_non_salary.columns)} columns')
                
                # Save non-salary CSV
                non_salary_output_path = config.output_folder / "extracted_data_non_salary.csv"
                df_non_salary.to_csv(non_salary_output_path, index=False, encoding='utf-8', sep=';')
                print(f'  Saved non-salary CSV file: {non_salary_output_path}')
                
            except Exception as e:
                print(f'  ERROR creating non-salary CSV file: {e}')
        
        print(f'Completed: {successful_files} successful, {len(failed_files)} failed')
        
    except Exception as e:
        print(f'Fatal error: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()