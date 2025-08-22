#!/usr/bin/env python3
"""
Script to compare CAO data between manual dataset and extracted Excel file.

This script:
1. Loads both datasets
2. Matches CAOs by name/ID
3. Sends matched data to LLM for comparison
4. Handles column differences and data format variations
"""

import os
import sys
import pandas as pd
import json
import time
from pathlib import Path
import argparse
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables
load_dotenv()

# Configure Gemini API
api_key = os.getenv("GOOGLE_API_KEY1")
if not api_key:
    raise ValueError("GOOGLE_API_KEY1 environment variable not found")

genai.configure(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-pro"

def load_datasets():
    """
    Load both datasets and return them as DataFrames.
    
    Returns:
        tuple: (manual_df, extracted_df)
    """
    manual_file = "dataset_CAO_182_533_156_316_433.xlsx"
    extracted_file = "results/extracted_data.xlsx"
    
    if not os.path.exists(manual_file):
        raise FileNotFoundError(f"Manual dataset file not found: {manual_file}")
    if not os.path.exists(extracted_file):
        raise FileNotFoundError(f"Extracted dataset file not found: {extracted_file}")
    
    print("📊 Loading datasets...")
    manual_df = pd.read_excel(manual_file)
    extracted_df = pd.read_excel(extracted_file)
    
    print(f"📋 Manual dataset: {manual_df.shape[0]} rows, {manual_df.shape[1]} columns")
    print(f"📋 Extracted dataset: {extracted_df.shape[0]} rows, {extracted_df.shape[1]} columns")
    
    return manual_df, extracted_df

def find_matching_files(manual_df, extracted_df):
    """
    Find individual files that exist in both datasets by matching id and file information.
    
    Args:
        manual_df: Manual dataset DataFrame
        extracted_df: Extracted dataset DataFrame
    
    Returns:
        list: List of tuples (cao_number, id, file_name) for matching files
    """
    matching_files = []
    
    # Get unique file identifiers from manual dataset
    manual_files = manual_df[['CAO', 'id']].drop_duplicates()
    
    # Get unique file identifiers from extracted dataset
    extracted_files = extracted_df[['CAO', 'id', 'File_name']].drop_duplicates()
    
    print(f"📋 Manual dataset has {len(manual_files)} unique files")
    print(f"📋 Extracted dataset has {len(extracted_files)} unique files")
    
    # Match files by CAO and id
    for _, manual_row in manual_files.iterrows():
        manual_cao = manual_row['CAO']
        manual_id = manual_row['id']
        
        # Find matching files in extracted dataset
        matching_extracted = extracted_files[
            (extracted_files['CAO'] == manual_cao) & 
            (extracted_files['id'] == manual_id)
        ]
        
        if not matching_extracted.empty:
            for _, extracted_row in matching_extracted.iterrows():
                file_name = extracted_row['File_name']
                matching_files.append((manual_cao, manual_id, file_name))
                print(f"  ✅ Matched: CAO {manual_cao}, ID {manual_id} → {file_name}")
        else:
            print(f"  ❌ No match: CAO {manual_cao}, ID {manual_id}")
    
    print(f"\n🔍 Found {len(matching_files)} matching files between datasets")
    return matching_files

def get_file_data(manual_df, extracted_df, cao_number, file_id, file_name):
    """
    Get all data for a specific file from both datasets.
    
    Args:
        manual_df: Manual dataset DataFrame
        extracted_df: Extracted dataset DataFrame
        cao_number: CAO number
        file_id: File ID
        file_name: File name from extracted dataset
    
    Returns:
        tuple: (manual_data, extracted_data)
    """
    # Get manual data for this specific file
    manual_file_data = manual_df[
        (manual_df['CAO'] == cao_number) & 
        (manual_df['id'] == file_id)
    ]
    
    # Get extracted data for this specific file
    extracted_file_data = extracted_df[
        (extracted_df['CAO'] == cao_number) & 
        (extracted_df['id'] == file_id) &
        (extracted_df['File_name'] == file_name)
    ]
    
    return manual_file_data, extracted_file_data

def prepare_data_for_llm(manual_data, extracted_data, cao_number, file_id, file_name):
    """
    Prepare data for LLM comparison by organizing information into categories.
    
    Args:
        manual_data: Manual dataset rows for the specific file
        extracted_data: Extracted dataset rows for the specific file
        cao_number: CAO number
        file_id: File ID
        file_name: File name
    
    Returns:
        str: Formatted data for LLM
    """
    # Define information categories
    categories = {
        "wage_information": ["salary_1", "salary_2", "salary_3", "salary_4", "salary_5", "salary_6", "salary_7", 
                           "salary_1_unit", "salary_2_unit", "salary_3_unit", "salary_4_unit", "salary_5_unit", "salary_6_unit", "salary_7_unit",
                           "salary_1_startdate", "salary_2_startdate", "salary_3_startdate", "salary_4_startdate", "salary_5_startdate", "salary_6_startdate", "salary_7_startdate",
                           "salary_increment_1", "salary_increment_2", "salary_increment_3", "salary_increment_4", "salary_increment_5", "salary_increment_6", "salary_increment_7",
                           "jobgroup", "salary_note", "salary_age_group", "more_salaries"],
        
        "pension_information": ["pension_premium_basic", "pension_premium_plus", "retire_age_basic", "retire_age_plus", 
                              "pension_age_group", "age_group"],
        
        "leave_information": ["maternity_leave", "maternity_pay", "maternity_note", "vacation_time", "vacation_unit", 
                            "vacation_note", "maternity_leave_weeks"],
        
        "termination_information": ["term_period_employer", "term_employer_note", "term_period_worker", "term_worker_note", 
                                  "probation_period", "probation_note", "term_period_employer_unit", "term_period_worker_unit", 
                                  "duration_worked", "duration_worked_unit"],
        
        "overtime_information": ["overtime_compensation", "max_hrs", "min_hrs", "shift_compensation", 
                                "overtime_allowance_min", "overtime_allowance_max"],
        
        "training_information": ["training", "phase", "training_spendingobl"],
        
        "contract_dates": ["start_date", "expiry_date", "start_date_contract", "expiry_date_contract", 
                          "date_of_formal_notification", "temporary"]
    }
    
    def extract_category_data(df, category_name, category_fields):
        """Extract all data for a specific category from the dataset."""
        category_data = {}
        for field in category_fields:
            if field in df.columns:
                # Get all non-empty values for this field
                values = df[field].dropna().unique()
                if len(values) > 0:
                    category_data[field] = values.tolist()
        return category_data
    
    def compare_content_values(manual_data, extracted_data, category_fields):
        """Compare actual content values between manual and extracted datasets."""
        content_comparison = {}
        
        for field in category_fields:
            if field in manual_data.columns and field in extracted_data.columns:
                manual_values = manual_data[field].dropna().unique()
                extracted_values = extracted_data[field].dropna().unique()
                
                # Special handling for date fields
                if any(date_keyword in field.lower() for date_keyword in ['date', 'start', 'expiry', 'expirat']):
                    # For date fields, normalize the dates for comparison
                    manual_normalized = set()
                    extracted_normalized = set()
                    
                    for value in manual_values:
                        try:
                            # Try to parse and normalize the date
                            normalized = normalize_date(str(value))
                            if normalized:
                                manual_normalized.add(normalized)
                        except:
                            # If parsing fails, keep original
                            manual_normalized.add(str(value))
                    
                    for value in extracted_values:
                        try:
                            # Try to parse and normalize the date
                            normalized = normalize_date(str(value))
                            if normalized:
                                extracted_normalized.add(normalized)
                        except:
                            # If parsing fails, keep original
                            extracted_normalized.add(str(value))
                    
                    # Compare normalized dates
                    only_in_manual = manual_normalized - extracted_normalized
                    only_in_extracted = extracted_normalized - manual_normalized
                    
                else:
                    # For non-date fields, use regular string comparison
                    manual_set = set(str(v) for v in manual_values)
                    extracted_set = set(str(v) for v in extracted_values)
                    only_in_manual = manual_set - extracted_set
                    only_in_extracted = extracted_set - manual_set
                
                # Only include fields that have actual differences
                if only_in_manual or only_in_extracted:
                    content_comparison[field] = {
                        "manual_values": list(manual_values),
                        "extracted_values": list(extracted_values),
                        "only_in_manual": list(only_in_manual),
                        "only_in_extracted": list(only_in_extracted),
                        "has_differences": True
                    }
        
        return content_comparison
    
    def normalize_date(date_str):
        """Normalize date strings to a standard format for comparison."""
        import re
        from datetime import datetime
        
        if not date_str or pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # Common date patterns
        patterns = [
            # DD/MM/YYYY or DD-MM-YYYY
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
            # YYYY-MM-DD
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            # DD Month YYYY (e.g., "1 January 2024")
            r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',
            # Month DD, YYYY (e.g., "January 1, 2024")
            r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    if len(match.groups()) == 3:
                        if pattern == patterns[0]:  # DD/MM/YYYY or DD-MM-YYYY
                            day, month, year = match.groups()
                        elif pattern == patterns[1]:  # YYYY-MM-DD
                            year, month, day = match.groups()
                        elif pattern == patterns[2]:  # DD Month YYYY
                            day, month, year = match.groups()
                        elif pattern == patterns[3]:  # Month DD, YYYY
                            month, day, year = match.groups()
                        
                        # Try to create a datetime object
                        dt = datetime(int(year), int(month), int(day))
                        # Return normalized format (YYYY-MM-DD)
                        return dt.strftime('%Y-%m-%d')
                except:
                    continue
        
        # If no pattern matches, return original
        return date_str
    
    # Extract category data from both datasets
    manual_categories = {}
    extracted_categories = {}
    content_comparisons = {}
    
    for category_name, category_fields in categories.items():
        manual_categories[category_name] = extract_category_data(manual_data, category_name, category_fields)
        extracted_categories[category_name] = extract_category_data(extracted_data, category_name, category_fields)
        content_comparisons[category_name] = compare_content_values(manual_data, extracted_data, category_fields)
    
    # Create structured comparison format
    comparison_data = {
        "file_info": {
            "cao_number": cao_number,
            "file_id": file_id,
            "file_name": file_name
        },
        "manual_dataset": {
            "total_rows": len(manual_data),
            "categories": manual_categories
        },
        "extracted_dataset": {
            "total_rows": len(extracted_data),
            "categories": extracted_categories
        },
        "content_comparison": content_comparisons
    }
    
    return json.dumps(comparison_data, indent=2, default=str)

def query_llm_for_comparison(data_str, cao_number, file_id, file_name):
    """
    Send data to LLM for comparison analysis.
    
    Args:
        data_str: Formatted data string
        cao_number: CAO number being compared
        file_id: File ID
        file_name: File name
    
    Returns:
        str: LLM analysis response
    """
    prompt = f"""
You are an expert data quality analyst evaluating the accuracy and completeness of AI-extracted data against manually verified ground truth data.

=== TASK ===
Evaluate the quality of the EXTRACTED DATASET (AI-generated) against the MANUAL DATASET (human-verified ground truth) for:
- CAO Number: {cao_number}
- File ID: {file_id}
- File Name: {file_name}

The MANUAL DATASET is the source of truth - it contains data that was manually entered and verified by humans.
The EXTRACTED DATASET contains data that was automatically extracted from PDF documents using AI.

=== DATA ===
{data_str}

=== ANALYSIS INSTRUCTIONS ===
Your primary goal is to assess whether the AI-extracted data is CORRECT and COMPLETE compared to the manual data.

**IMPORTANT**: Focus on DIFFERENCES only. If information is the same or similar, don't mention it. Only report what's actually different.

Please analyze each INFORMATION CATEGORY and report only the differences:

1. **WAGE INFORMATION COMPARISON**:
   - **Missing Information**: What wage information is missing from the extracted dataset?
   - **Additional Information**: What additional wage information did the AI find?
   - **Content Differences**: Only report fields where values are different (e.g., salary amounts, dates)

2. **PENSION INFORMATION COMPARISON**:
   - **Missing Information**: What pension information is missing from the extracted dataset?
   - **Additional Information**: What additional pension information did the AI find?
   - **Content Differences**: Only report fields where values are different

3. **LEAVE INFORMATION COMPARISON**:
   - **Missing Information**: What leave information is missing from the extracted dataset?
   - **Additional Information**: What additional leave information did the AI find?
   - **Content Differences**: Only report fields where values are different

4. **TERMINATION INFORMATION COMPARISON**:
   - **Missing Information**: What termination information is missing from the extracted dataset?
   - **Additional Information**: What additional termination information did the AI find?
   - **Content Differences**: Only report fields where values are different

5. **OVERTIME INFORMATION COMPARISON**:
   - **Missing Information**: What overtime information is missing from the extracted dataset?
   - **Additional Information**: What additional overtime information did the AI find?
   - **Content Differences**: Only report fields where values are different

6. **TRAINING INFORMATION COMPARISON**:
   - **Missing Information**: What training information is missing from the extracted dataset?
   - **Additional Information**: What additional training information did the AI find?
   - **Content Differences**: Only report fields where values are different

7. **CONTRACT DATES COMPARISON**:
   - **Missing Information**: What date information is missing from the extracted dataset?
   - **Additional Information**: What additional date information did the AI find?
   - **Content Differences**: Only report fields where actual dates are different (ignoring format differences)

=== DETAILED ANALYSIS REQUIREMENTS ===
For WAGE INFORMATION specifically:
- Compare each salary field individually (salary_1, salary_2, etc.)
- Check units, start dates, and increments for each salary level
- **ONLY REPORT** missing salary levels or different values
- **DON'T REPORT** matching values

For ALL categories:
- **CRITICAL**: Compare the ACTUAL CONTENT VALUES, not just field presence
- **ONLY REPORT** fields where values are different
- **DON'T REPORT** fields where values are the same or similar
- If manual says "65" and extracted says "67", FLAG THIS DIFFERENCE
- If manual says "€2500" and extracted says "€2600", FLAG THIS DIFFERENCE
- For longer text fields, only flag if the meaning/content is significantly different

=== CONTENT COMPARISON REQUIREMENTS ===
The data includes detailed content comparisons. **ONLY REPORT** fields with differences:
- **Manual Value**: What the manual dataset says
- **Extracted Value**: What the AI extracted
- **Flag any differences** as potential errors
- **Examples to flag**:
  - Retirement age: Manual "65" vs Extracted "67"
  - Salary: Manual "€2500" vs Extracted "€2600"
  - Pension premium: Manual "5%" vs Extracted "6%"
  - Vacation days: Manual "25" vs Extracted "20"
  - Contract dates: Manual "01/01/2024" vs Extracted "2024-01-01" ✅ (Same date, different format - NOT flagged)
  - Contract dates: Manual "01/01/2024" vs Extracted "01/02/2024" ❌ (Different dates - FLAGGED)

**IMPORTANT**: If a field has the same or similar content, DO NOT report it.

=== OVERALL ASSESSMENT ===
For each category, provide:
- **Missing Information**: What should the AI have found but didn't?
- **Additional Information**: What did the AI find that wasn't in the manual dataset?
- **Content Differences**: Only the specific fields where values are different

**IMPORTANT**: Only report what's actually different. If information is complete and accurate, just state "No issues found" for that category.

=== OUTPUT FORMAT ===
Provide a structured analysis with clear sections for each information category.
For each category, state:
1. **Missing Information**: What's missing from the extracted dataset
2. **Additional Information**: What the AI found that wasn't in the manual dataset
3. **Content Differences**: Only the specific fields where values are different

**CRITICAL**: For each field with different values, clearly state:
- "Manual says: [value], Extracted says: [value] - FLAGGED AS DIFFERENCE"
- "Retirement age: Manual '65', Extracted '67' - FLAGGED AS DIFFERENCE"
- "Salary_1: Manual '€2500', Extracted '€2600' - FLAGGED AS DIFFERENCE"

**IMPORTANT**: If a category has no issues (all information present and correct), simply state "No issues found" for that category.

Focus on DIFFERENCES ONLY. Don't report matching or similar information.
"""

    try:
        model = genai.GenerativeModel(GEMINI_MODEL)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error querying LLM: {e}"

def save_comparison_results(cao_number, file_id, file_name, analysis, output_dir="comparison_results"):
    """
    Save comparison results to a file.
    
    Args:
        cao_number: CAO number
        file_id: File ID
        file_name: File name
        analysis: LLM analysis text
        output_dir: Output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a safe filename
    safe_file_name = file_name.replace('.json', '').replace('/', '_').replace('\\', '_')
    filename = f"{output_dir}/comparison_CAO_{cao_number}_ID_{file_id}_{safe_file_name}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"CAO {cao_number} - ID {file_id} - {file_name} COMPARISON ANALYSIS\n")
        f.write("=" * 70 + "\n\n")
        f.write(analysis)
    
    print(f"💾 Saved comparison to: {filename}")

def main():
    parser = argparse.ArgumentParser(description="Compare CAO data between manual and extracted datasets")
    parser.add_argument("--cao", type=int, help="Specific CAO number to compare")
    parser.add_argument("--file-id", type=int, help="Specific file ID to compare")
    parser.add_argument("--all", action="store_true", help="Compare all matching files")
    parser.add_argument("--output-dir", default="comparison_results", help="Output directory for results")
    parser.add_argument("--delay", type=int, default=60, help="Delay between LLM requests in seconds")
    
    args = parser.parse_args()
    
    if not args.cao and not args.all:
        print("❌ Please specify --cao <number> or --all")
        return
    
    try:
        # Load datasets
        manual_df, extracted_df = load_datasets()
        
        # Find matching files
        matching_files = find_matching_files(manual_df, extracted_df)
        
        if not matching_files:
            print("❌ No matching files found between datasets")
            return
        
        # Determine which files to process
        if args.cao:
            if args.file_id:
                # Specific file
                files_to_process = [f for f in matching_files if f[0] == args.cao and f[1] == args.file_id]
                if not files_to_process:
                    print(f"❌ No matching file found for CAO {args.cao}, ID {args.file_id}")
                    return
            else:
                # All files for this CAO
                files_to_process = [f for f in matching_files if f[0] == args.cao]
                if not files_to_process:
                    print(f"❌ No matching files found for CAO {args.cao}")
                    return
        else:
            files_to_process = matching_files
        
        print(f"\n🔄 Processing {len(files_to_process)} file(s)...")
        
        for i, (cao_number, file_id, file_name) in enumerate(files_to_process, 1):
            print(f"\n{'='*60}")
            print(f"📊 Processing CAO {cao_number}, ID {file_id}, File: {file_name}")
            print(f"📊 Progress: {i}/{len(files_to_process)}")
            print(f"{'='*60}")
            
            # Get data for this specific file
            manual_data, extracted_data = get_file_data(manual_df, extracted_df, cao_number, file_id, file_name)
            
            print(f"📋 Manual dataset: {len(manual_data)} rows")
            print(f"📋 Extracted dataset: {len(extracted_data)} rows")
            
            # Prepare data for LLM
            data_str = prepare_data_for_llm(manual_data, extracted_data, cao_number, file_id, file_name)
            
            # Query LLM
            print("🤖 Sending to LLM for analysis...")
            analysis = query_llm_for_comparison(data_str, cao_number, file_id, file_name)
            
            # Save results
            save_comparison_results(cao_number, file_id, file_name, analysis, args.output_dir)
            
            # Add delay between requests (except for last one)
            if i < len(files_to_process):
                print(f"⏳ Waiting {args.delay} seconds before next request...")
                time.sleep(args.delay)
        
        print(f"\n✅ Comparison completed! Results saved in '{args.output_dir}' directory")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return

if __name__ == "__main__":
    main() 