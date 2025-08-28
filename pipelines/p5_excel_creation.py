"""
CAO Data Analysis - Excel Creation Pipeline (p5_excel_creation.py)

This script creates Excel output from LLM extraction results.
It merges salary and non-salary data, adds CAO info, and creates final Excel files.

USAGE:
    Single process:
        python pipelines/p5_excel_creation.py --process_id 0 --total_processes 1

    Multi-process:
        python pipelines/p5_excel_creation.py --process_id 0 --total_processes 6

    With file limit:
        python pipelines/p5_excel_creation.py --process_id 0 --total_processes 1 --max_files 10

ARGUMENTS:
    --process_id: Process ID for work distribution (0-based) - defaults to 0
    --total_processes: Total number of parallel processes - defaults to 1
    --max_files: Maximum number of files to process (optional)

INPUT:
    - LLM extraction results in outputs/llm_analysis/salary/ and outputs/llm_analysis/non_salary/
    - CAO info from inputs/pdfs/extracted_cao_info.csv

OUTPUT:
    - Excel files in outputs/excel/new_results/
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
from dotenv import load_dotenv

# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass
class ExcelConfig:
    """Configuration for Excel creation."""
    llm_analysis_folder: Path
    cao_info_path: str
    output_folder: Path
    max_json_files: int = 1000000

def load_configuration() -> ExcelConfig:
    """Load and validate configuration from config.yaml."""
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    return ExcelConfig(
        llm_analysis_folder=Path('outputs/llm_analysis'),
        cao_info_path="pdfs/input_pdfs/extracted_cao_info.csv",  # Fixed path
        output_folder=Path(config_data['paths']['outputs_excel']) / "new_results",
        max_json_files=config_data.get('max_json_files', 1000000)
    )

# =============================================================================
# DATA MERGING FUNCTIONS
# =============================================================================
def merge_extraction_results(salary_extracted: List[dict], rest_extracted: dict, cao_dates: Dict[str, str]) -> List[dict]:
    """
    Merge results from salary and rest extractions into multiple rows with specific infotype labels.
    
    Args:
        salary_extracted: List of salary extraction results
        rest_extracted: Non-salary extraction results
        cao_dates: Dictionary containing CAO start date, expiry date, and date of formal notification
        
    Returns:
        List[dict]: List of merged extraction results with complete field structure
    """
    # Define the complete field structure in the exact order specified
    all_fields = [
        'File_name', 'CAO', 'id', 'infotype', 'start_date', 'expiry_date', 'start_date_contract', 'expiry_date_contract', 'date_of_formal_notification', 'TTW',
        'jobgroup', 'salary_1', 'salary_1_unit', 'salary_1_startdate', 'salary_increment_1',
        'salary_2', 'salary_2_unit', 'salary_2_startdate', 'salary_increment_2',
        'salary_3', 'salary_3_unit', 'salary_3_startdate', 'salary_increment_3',
        'salary_4', 'salary_4_unit', 'salary_4_startdate', 'salary_increment_4',
        'salary_5', 'salary_5_unit', 'salary_5_startdate', 'salary_increment_5',
        'salary_6', 'salary_6_unit', 'salary_6_startdate', 'salary_increment_6',
        'salary_7', 'salary_7_unit', 'salary_7_startdate', 'salary_increment_7',
        'more_salaries', 'salary_note', 'salary_age_group',
        'pension_premium_basic', 'pension_premium_plus', 'retire_age_basic', 'retire_age_plus', 'pension_age_group',
        'maternity_leave', 'maternity_pay', 'maternity_note', 'vacation_time', 'vacation_unit', 'vacation_note',
        'term_period_employer', 'term_employer_note', 'term_period_worker', 'term_worker_note', 'probation_period', 'probation_note',
        'overtime_compensation', 'max_hrs', 'min_hrs', 'shift_compensation', 'overtime_allowance_min', 'overtime_allowance_max',
        'training', 'Homeoffice'
    ]
    
    # Define field mappings for different infotypes
    INFOTYPE_FIELD_MAPPINGS = {
        'Pension': ['pension_premium_basic', 'pension_premium_plus', 'retire_age_basic', 'retire_age_plus', 'pension_age_group'],
        'Leave': ['maternity_leave', 'maternity_pay', 'maternity_note', 'vacation_time', 'vacation_unit', 'vacation_note'],
        'Termination': ['term_period_employer', 'term_employer_note', 'term_period_worker', 'term_worker_note', 'probation_period', 'probation_note'],
        'Overtime': ['overtime_compensation', 'max_hrs', 'min_hrs', 'shift_compensation', 'overtime_allowance_min', 'overtime_allowance_max'],
        'Training': ['training'],
        'Homeoffice': ['Homeoffice']
    }
    
    merged_results = []
    
    # Process salary items - always create at least one wage row
    if salary_extracted:
        # Create wage rows for each salary item
        for salary_item in salary_extracted:
            if not isinstance(salary_item, dict):
                continue
                
            # Create wage row
            wage_row = {field: '' for field in all_fields}
            wage_row['infotype'] = 'Wage'
            
            # Fill salary fields
            for field, value in salary_item.items():
                if field in wage_row and value:
                    wage_row[field] = value
            
            # Add contract dates from rest_extracted if available
            if 'contract_information' in rest_extracted:
                contract_info = rest_extracted['contract_information']
                if contract_info.get('start_date_contract'):
                    wage_row['start_date_contract'] = contract_info['start_date_contract']
                if contract_info.get('expiry_date_contract'):
                    wage_row['expiry_date_contract'] = contract_info['expiry_date_contract']
            
            # Add CAO dates
            wage_row['start_date'] = cao_dates.get('start_date', '')
            wage_row['expiry_date'] = cao_dates.get('expiry_date', '')
            wage_row['date_of_formal_notification'] = cao_dates.get('date_of_formal_notification', '')
            
            merged_results.append(wage_row)
    else:
        # Create empty wage row if no salary data
        wage_row = {field: '' for field in all_fields}
        wage_row['infotype'] = 'Wage'
        
        # Add contract dates from rest_extracted if available
        if 'contract_information' in rest_extracted:
            contract_info = rest_extracted['contract_information']
            if contract_info.get('start_date_contract'):
                wage_row['start_date_contract'] = contract_info['start_date_contract']
            if contract_info.get('expiry_date_contract'):
                wage_row['expiry_date_contract'] = contract_info['expiry_date_contract']
        
        # Add CAO dates
        wage_row['start_date'] = cao_dates.get('start_date', '')
        wage_row['expiry_date'] = cao_dates.get('expiry_date', '')
        wage_row['date_of_formal_notification'] = cao_dates.get('date_of_formal_notification', '')
        
        merged_results.append(wage_row)
    
    # Process non-salary items (create separate rows for each infotype)
    for infotype, fields in INFOTYPE_FIELD_MAPPINGS.items():
        rest_row = {field: '' for field in all_fields}
        rest_row['infotype'] = infotype
        
        # Map fields based on infotype
        if infotype == 'Pension' and 'pension_information' in rest_extracted:
            pension_info = rest_extracted['pension_information']
            # Map Pydantic field names to Excel column names
            field_mapping = {
                'pension_premium_basic': 'pension_scheme_basic',
                'pension_premium_plus': 'pension_scheme_plus',
                'retire_age_basic': 'retire_age_basic',
                'retire_age_plus': 'retire_age_plus',
                'pension_age_group': 'pension_age_group'
            }
            for excel_field, pydantic_field in field_mapping.items():
                if pydantic_field in pension_info and pension_info[pydantic_field]:
                    rest_row[excel_field] = pension_info[pydantic_field]
        elif infotype == 'Leave' and 'leave_information' in rest_extracted:
            leave_info = rest_extracted['leave_information']
            for field in fields:
                if field in leave_info and leave_info[field]:
                    rest_row[field] = leave_info[field]
        elif infotype == 'Termination' and 'termination_information' in rest_extracted:
            termination_info = rest_extracted['termination_information']
            for field in fields:
                if field in termination_info and termination_info[field]:
                    rest_row[field] = termination_info[field]
        elif infotype == 'Overtime' and 'overtime_information' in rest_extracted:
            overtime_info = rest_extracted['overtime_information']
            for field in fields:
                if field in overtime_info and overtime_info[field]:
                    rest_row[field] = overtime_info[field]
        elif infotype == 'Training' and 'training_information' in rest_extracted:
            training_info = rest_extracted['training_information']
            for field in fields:
                if field in training_info and training_info[field]:
                    rest_row[field] = training_info[field]
        elif infotype == 'Homeoffice' and 'homeoffice_information' in rest_extracted:
            homeoffice_info = rest_extracted['homeoffice_information']
            for field in fields:
                if field in homeoffice_info and homeoffice_info[field]:
                    rest_row[field] = homeoffice_info[field]
        
        # Add contract dates from p4_analysis.py
        if 'contract_information' in rest_extracted:
            contract_info = rest_extracted['contract_information']
            if contract_info.get('start_date_contract'):
                rest_row['start_date_contract'] = contract_info['start_date_contract']
            if contract_info.get('expiry_date_contract'):
                rest_row['expiry_date_contract'] = contract_info['expiry_date_contract']
        
        # Add CAO dates to the rest_row
        rest_row['start_date'] = cao_dates.get('start_date', '')
        rest_row['expiry_date'] = cao_dates.get('expiry_date', '')
        rest_row['date_of_formal_notification'] = cao_dates.get('date_of_formal_notification', '')
        
        merged_results.append(rest_row)
    
    return merged_results

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
                if cao_number:
                    cao_info_mapping[cao_number] = {
                        'id': str(row.get('id', '')),
                        'TTW': 'yes' if 'TTW' in str(row.get('pdf_name', '')).upper() else 'no',
                        'ingangsdatum': str(row.get('ingangsdatum', '')),
                        'expiratiedatum': str(row.get('expiratiedatum', '')),
                        'datum_kennisgeving': str(row.get('datum_kennisgeving', ''))
                    }
            print(f"  Mapped {len(cao_info_mapping)} CAO numbers")
        except Exception as e:
            print(f"Warning: Could not load CAO info from {cao_info_path}: {e}")
    else:
        print(f"Warning: CAO info file not found: {cao_info_path}")
    
    return cao_info_mapping

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
                for json_file in cao_folder.glob('*_salary.json'):
                    files.append((cao_number, json_file))
    
    return files

def process_llm_file(cao_number: str, salary_file: Path, cao_info_mapping: Dict[str, Dict[str, str]], 
                    config: ExcelConfig) -> List[dict]:
    """Process a single LLM file and return merged results."""
    filename = salary_file.stem.replace('_salary', '') + '.json'
    
    # Load salary data
    with open(salary_file, 'r', encoding='utf-8') as f:
        salary_data = json.load(f)
    salary_extracted = salary_data.get('salary_information', [])
    
    # Load non-salary data
    non_salary_file = config.llm_analysis_folder / 'non_salary' / cao_number / f"{salary_file.stem.replace('_salary', '')}_non_salary.json"
    
    rest_extracted = {}
    if non_salary_file.exists():
        with open(non_salary_file, 'r', encoding='utf-8') as f:
            rest_extracted = json.load(f)
    
    # Get CAO info dates
    cao_info = cao_info_mapping.get(cao_number, {})
    cao_dates = {
        'start_date': cao_info.get('ingangsdatum', ''),
        'expiry_date': cao_info.get('expiratiedatum', ''),
        'date_of_formal_notification': cao_info.get('datum_kennisgeving', '')
    }
    
    # Merge results
    merged_results = merge_extraction_results(salary_extracted, rest_extracted, cao_dates)
    
    # Add metadata
    for item in merged_results:
        item['File_name'] = filename
        item['CAO'] = cao_number
        
        # Add CAO info if available
        if cao_number in cao_info_mapping:
            cao_info = cao_info_mapping[cao_number]
            item['id'] = cao_info.get('id', '')
            item['TTW'] = cao_info.get('TTW', '')
            # Note: infotype is set by data processing logic, not from CAO info
            print(f'    Added CAO info: id={item["id"]}, TTW={item["TTW"]}')
        else:
            item['id'] = ''
            item['TTW'] = ''
            print(f'    No CAO info found for CAO {cao_number}')
    
    return merged_results

# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
    """Main entry point for Excel creation."""
    parser = argparse.ArgumentParser(description='CAO Data Excel Creation')
    parser.add_argument('--process_id', type=int, default=0, help='Process ID for work distribution')
    parser.add_argument('--total_processes', type=int, default=1, help='Total number of parallel processes')
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
        
        # Filter files for this process
        process_files = [f for i, f in enumerate(all_files) if i % args.total_processes == args.process_id]
        
        # Apply file limit
        max_files = args.max_files if args.max_files is not None else config.max_json_files
        if max_files:
            process_files = process_files[:max_files]
        
        print(f'Process {args.process_id + 1}: Processing {len(process_files)} files')
        
        # Process files
        all_results = []
        successful_files = 0
        failed_files = []
        
        for cao_number, salary_file in process_files:
            try:
                results = process_llm_file(cao_number, salary_file, cao_info_mapping, config)
                all_results.extend(results)
                successful_files += 1
                print(f'  {cao_number}: Processed {salary_file.name}')
            except Exception as e:
                print(f'  {cao_number}: Error processing {salary_file.name}: {e}')
                failed_files.append(salary_file.name)
        
        # Create DataFrame and save Excel
        if all_results:
            df_results = pd.DataFrame(all_results)
            output_path = config.output_folder / f"extracted_data.xlsx"
            df_results.to_excel(output_path, index=False)
            print(f'  Saved Excel file: {output_path}')
            print(f'  Total rows: {len(df_results)}')
        else:
            print('  No data to save')
        
        print(f'Process {args.process_id + 1} completed: {successful_files} successful, {len(failed_files)} failed')
        
    except Exception as e:
        print(f'Fatal error: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()
