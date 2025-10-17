"""
CAO Data Analysis - Excel Creation Pipeline (p5_excel_creation.py)

This script creates separate Excel outputs for salary and non-salary data from LLM extraction results.
It merges data from three non-salary folders, adds CAO info, and creates final Excel files.

USAGE:
    python pipelines/p5_excel_creation.py

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
import csv
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
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    return ExcelConfig(
        llm_analysis_folder=Path('outputs/llm_analysis'),
        cao_info_path="pdfs/input_pdfs/extracted_cao_info.csv",  # Fixed path
        output_folder=Path(config_data['paths']['outputs_excel']) / "new_results"
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

def normalize_filename(filename: str) -> str:
    """
    Normalize filename by removing common suffixes for matching.
    
    Args:
        filename: Original filename
        
    Returns:
        str: Normalized filename for matching
    """
    # Remove common suffixes
    suffixes_to_remove = ['.pdf', '.md', '_analysis', '_extract']
    
    normalized = filename
    for suffix in suffixes_to_remove:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
    
    return normalized

def load_cao_info(cao_info_path: str) -> Dict[str, Dict[str, str]]:
    """Load CAO information from CSV file."""
    cao_info_mapping = {}
    
    print(f"Loading CAO info from: {cao_info_path}")
    
    if os.path.exists(cao_info_path):
        try:
            df = pd.read_csv(cao_info_path, sep=';')  # Use semicolon separator
            print(f"  Loaded {len(df)} CAO info records")
            print(f"  Columns: {list(df.columns)}")
            
            for _, row in df.iterrows():
                cao_number = str(row.get('cao_number', ''))
                pdf_name = str(row.get('pdf_name', ''))
                if cao_number and pdf_name:
                    # Create key as "cao_number:pdf_name" for unique identification
                    key = f"{cao_number}:{pdf_name}"
                    cao_info_mapping[key] = {
                        'cao_number': cao_number,
                        'id': str(row.get('id', '')),
                        'TTW': 'yes' if 'TTW' in str(row.get('pdf_name', '')).upper() else 'no',
                        'ingangsdatum': str(row.get('ingangsdatum', '')),
                        'expiratiedatum': str(row.get('expiratiedatum', '')),
                        'datum_kennisgeving': str(row.get('datum_kennisgeving', ''))
                    }
            print(f"  Mapped {len(cao_info_mapping)} unique CAO entries")
        except Exception as e:
            print(f"Warning: Could not load CAO info from {cao_info_path}: {e}")
    else:
        print(f"Warning: CAO info file not found: {cao_info_path}")
    
    return cao_info_mapping

def match_cao_info(cao_number: str, filename: str, cao_info_mapping: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """
    Match CAO info using robust filename matching.
    
    Args:
        cao_number: CAO number from folder name
        filename: Original filename
        cao_info_mapping: CAO info mapping from CSV
        
    Returns:
        Dict with CAO metadata
    """
    # Normalize filename for matching
    normalized_filename = normalize_filename(filename)
    
    # Try exact match first (with .pdf extension)
    cao_info_key = f"{cao_number}:{normalized_filename}.pdf"
    cao_info = cao_info_mapping.get(cao_info_key, {})
    
    # If no exact match, try to find by filename only (in case CAO number folder is wrong)
    if not cao_info:
        for key, info in cao_info_mapping.items():
            # Compare normalized filenames
            csv_filename = normalize_filename(info.get('pdf_name', ''))
            if csv_filename == normalized_filename:
                cao_info = info
                print(f'    Found CAO info by filename match: {key}')
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
        print(f'    No CAO info found for CAO {cao_number}, filename {normalized_filename}')
    
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
    """Discover all LLM analysis files."""
    files = []
    
    # Look for salary files
    salary_folder = llm_analysis_folder / 'salary'
    if salary_folder.exists():
        for cao_folder in salary_folder.iterdir():
            if cao_folder.is_dir():
                cao_number = cao_folder.name
                for json_file in cao_folder.glob('*_analysis.json'):
                    # Extract base filename
                    base_filename = json_file.stem.replace('_analysis', '')
                    files.append((cao_number, base_filename, json_file))
    
    return files

def determine_max_timeline_length(files: List[tuple]) -> int:
    """Determine the maximum timeline length across all salary files."""
    max_timeline = 0
    
    print("Determining max timeline length...")
    for cao_number, base_filename, salary_file in files:
        try:
            with open(salary_file, 'r', encoding='utf-8') as f:
                salary_data = json.load(f)
                salary_rows = salary_data.get('salary_information', [])
                for salary_row in salary_rows:
                    timeline = salary_row.get('timeline', [])
                    max_timeline = max(max_timeline, len(timeline))
        except Exception as e:
            print(f"Warning: Could not read {salary_file} for timeline analysis: {e}")
    
    print(f"Max timeline length determined: {max_timeline}")
    return max_timeline

def process_salary_file(cao_number: str, base_filename: str, salary_file: Path, 
                       cao_info_mapping: Dict[str, Dict[str, str]], config: ExcelConfig, 
                       max_timeline_length: int) -> List[dict]:
    """Process a single salary file and return Excel rows."""
    # Load salary data
    with open(salary_file, 'r', encoding='utf-8') as f:
        salary_data = json.load(f)
    
    salary_rows = salary_data.get('salary_information', [])
    
    # Get CAO metadata
    cao_metadata = match_cao_info(cao_number, base_filename, cao_info_mapping)
    
    # Convert to Excel rows (wide format)
    excel_rows = []
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
                # Process salary data
                salary_rows = process_salary_file(cao_number, base_filename, salary_file, cao_info_mapping, config, max_timeline_length)
                salary_results.extend(salary_rows)
                
                # Process non-salary data
                non_salary_row = process_non_salary_file(cao_number, base_filename, cao_info_mapping, config)
                non_salary_results.append(non_salary_row)
                
                successful_files += 1
                print(f'  {cao_number}: Processed {salary_file.name}')
            except Exception as e:
                print(f'  {cao_number}: Error processing {salary_file.name}: {e}')
                failed_files.append(salary_file.name)
        
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