#!/usr/bin/env python3
"""
Script to translate Dutch CAO data from CAOData2024.xlsx to English
Usage: python translate_cao_data.py [api_key_number] [process_id] [total_processes]
"""

import os
import pandas as pd
import time
import json
import sys
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv
import re

# =========================
# Configuration and Setup
# =========================

# Load environment variables
load_dotenv()

# Get key number from command line or default to 1
key_number = int(sys.argv[1]) if len(sys.argv) > 1 else 1
# Get process ID and total processes for work distribution
process_id = int(sys.argv[2]) if len(sys.argv) > 2 else 0
total_processes = int(sys.argv[3]) if len(sys.argv) > 3 else 1

# Try to get the specified API key, fallback to API key 1 if not found
api_key = os.getenv(f"GOOGLE_API_KEY{key_number}")
if not api_key:
    # Fallback to API key 1 if the specified key doesn't exist
    api_key = os.getenv("GOOGLE_API_KEY1")
    if not api_key:
        raise ValueError(f"Neither GOOGLE_API_KEY{key_number} nor GOOGLE_API_KEY1 environment variable found. Please set at least GOOGLE_API_KEY1 before running this script.")
    else:
        # Update key_number to 1 for consistency
        key_number = 1
        print(f"Warning: GOOGLE_API_KEY{key_number} not found, using GOOGLE_API_KEY1 instead")

# Configure Gemini
genai.configure(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-pro"

# LLM Configuration
LLM_TEMPERATURE = 0.1
LLM_TOP_P = 0.8
LLM_TOP_K = 40
LLM_MAX_TOKENS = 8192
LLM_CANDIDATE_COUNT = 1
MAX_RETRIES = 3

# File paths
CAO_DATA_PATH = "inputs/_EXTRA/CAOData2024.xlsx"
TRANSLATION_HELPER_PATH = "inputs/_EXTRA/TranslationHelper.xlsx"
OUTPUT_TRANSLATED_PATH = "inputs/_EXTRA/CAOData2024_Translated.xlsx"

# =========================
# Gemini API Functions
# =========================

def query_gemini(prompt, model=GEMINI_MODEL, max_retries=MAX_RETRIES):
    """
    Query Gemini with retry logic and error handling.
    Args:
        prompt (str): The prompt to send to Gemini.
        model (str): The model to use.
        max_retries (int): Maximum number of retry attempts.
    Returns:
        str: The model response.
    """
    for attempt in range(max_retries):
        try:
            model_obj = genai.GenerativeModel(model)
            
            # Configure generation parameters for maximum consistency
            generation_config = genai.types.GenerationConfig(
                temperature=LLM_TEMPERATURE,
                top_p=LLM_TOP_P,
                top_k=LLM_TOP_K,
                max_output_tokens=LLM_MAX_TOKENS,
                candidate_count=LLM_CANDIDATE_COUNT
            )
            
            response = model_obj.generate_content(prompt, generation_config=generation_config)
            
            # Check if response has content and is not blocked
            if hasattr(response, 'candidates') and response.candidates:
                candidate = response.candidates[0]
                if hasattr(candidate, 'finish_reason'):
                    if candidate.finish_reason == 1:  # SAFETY
                        print(f"  Attempt {attempt + 1} blocked by safety filters, retrying with modified prompt... [API {key_number}/{total_processes}]")
                        # Try with a simpler prompt
                        simplified_prompt = "Translate these Dutch terms to English: " + str(list(df.columns))[:1000]
                        response = model_obj.generate_content(simplified_prompt, generation_config=generation_config)
                    elif candidate.finish_reason == 2:  # RECITATION
                        print(f"  Attempt {attempt + 1} failed (recitation), retrying... [API {key_number}/{total_processes}]")
                        if attempt < max_retries - 1:
                            time.sleep(30)
                            continue
                        else:
                            return ""
                    elif candidate.finish_reason == 3:  # OTHER
                        print(f"  Attempt {attempt + 1} failed (other reason), retrying... [API {key_number}/{total_processes}]")
                        if attempt < max_retries - 1:
                            time.sleep(30)
                            continue
                        else:
                            return ""
            
            # Check for valid text response
            if hasattr(response, "text") and response.text.strip():
                return response.text
            else:
                # Try to get text from parts
                if hasattr(response, 'candidates') and response.candidates:
                    candidate = response.candidates[0]
                    if hasattr(candidate, 'content') and candidate.content.parts:
                        text = candidate.content.parts[0].text
                        if text.strip():
                            return text
                
                raise ValueError("Empty or invalid model response")
                
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle 504 Deadline Exceeded errors with reasonable retry
            if "deadlineexceeded" in error_str or "504" in error_str:
                if attempt < max_retries - 1:
                    wait_time = 120 * (2 ** attempt)  # 120s, 240s, 480s
                    print(f"  Attempt {attempt + 1} failed (504 timeout), retrying in {wait_time//60} minutes... [API {key_number}/{total_processes}]")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with 504 errors - skipping [API {key_number}/{total_processes}]")
                    return ""
            
            # Handle other rate limiting errors
            elif any(keyword in error_str for keyword in ["quota", "rate limit", "too many requests", "429"]):
                if attempt < max_retries - 1:
                    wait_time = 120 * (2 ** attempt)
                    print(f"  Attempt {attempt + 1} failed (rate limit), retrying in {wait_time//60} minutes... [API {key_number}/{total_processes}]")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with rate limiting - skipping [API {key_number}/{total_processes}]")
                    return ""
            
            # Handle other errors with standard retry
            elif attempt < max_retries - 1:
                wait_time = 120 * (2 ** attempt)
                print(f"  Attempt {attempt + 1} failed ({type(e).__name__}), retrying in {wait_time//60} minutes... [API {key_number}/{total_processes}]")
                time.sleep(wait_time)
                continue
            else:
                raise e
    
    raise ValueError(f"All {max_retries} retry attempts failed")

def clean_gemini_output(output):
    """
    Clean the Gemini model output by removing markdown and trailing commas.
    Args:
        output (str): The raw output from Gemini.
    Returns:
        str: Cleaned output string.
    """
    if output.strip().startswith("```"):
        lines = output.strip().splitlines()
        content = "\n".join(line for line in lines if not line.strip().startswith("```"))
    else:
        content = output.strip()

    # Remove trailing commas before closing brackets/braces
    content = re.sub(r',\s*(?=[}\]])', '', content)
    return content

# =========================
# Translation Functions
# =========================

def translate_column_headers(df, sheet_name):
    """
    Translate column headers from Dutch to English using Gemini.
    Args:
        df (pd.DataFrame): The dataframe with Dutch column headers.
        sheet_name (str): Name of the sheet for context.
    Returns:
        dict: Mapping of Dutch to English column names.
    """
    # Get sample data for context
    sample_data = df.head(3).to_dict('records')
    
    prompt = f"""
Translate these Dutch column headers to English. This is for a labor agreement dataset.

Sheet: {sheet_name}
Column headers: {list(df.columns)}

Sample data context:
{json.dumps(sample_data, indent=2, default=str)}

Instructions:
- Translate each column header from Dutch to English
- Keep important codes like CAO, SBI unchanged
- For abbreviations, provide full meaning
- Use professional HR terminology
- If already in English, keep as is

Return JSON format:
{{
    "translations": {{
        "dutch_header": "english_translation"
    }},
    "abbreviations": {{
        "abbreviation": "full_meaning"
    }},
    "explanations": {{
        "term": "explanation"
    }}
}}

Only return valid JSON, no markdown.
"""
    
    try:
        raw_output = query_gemini(prompt)
        cleaned_output = clean_gemini_output(raw_output)
        result = json.loads(cleaned_output)
        return result
    except Exception as e:
        print(f"Error translating column headers for {sheet_name}: {e}")
        return {"translations": {}, "abbreviations": {}, "explanations": {}}

def translate_data_values(df, translations, sheet_name):
    """
    Translate data values from Dutch to English where appropriate.
    Args:
        df (pd.DataFrame): The dataframe with Dutch data.
        translations (dict): Column header translations.
        sheet_name (str): Name of the sheet for context.
    Returns:
        pd.DataFrame: Dataframe with translated values.
    """
    translated_df = df.copy()
    
    # Identify columns that likely contain text data (not codes, dates, numbers)
    text_columns = []
    for col in df.columns:
        if col in translations.get("translations", {}):
            # Check if column contains mostly text data
            sample_values = df[col].dropna().astype(str).head(10)
            if len(sample_values) > 0:
                # If more than 50% of values contain letters (not just numbers/symbols)
                text_ratio = sum(1 for val in sample_values if re.search(r'[a-zA-Z]', val)) / len(sample_values)
                if text_ratio > 0.5:
                    text_columns.append(col)
    
    if not text_columns:
        return translated_df
    
    # Translate text columns in batches
    batch_size = 5  # Process 5 columns at a time to avoid token limits
    for i in range(0, len(text_columns), batch_size):
        batch_cols = text_columns[i:i+batch_size]
        
        # Prepare sample data for the batch
        sample_data = {}
        for col in batch_cols:
            unique_values = df[col].dropna().unique()[:10]  # Get up to 10 unique values
            sample_data[col] = list(unique_values)
        
        prompt = f"""
You are an expert translator specializing in Dutch labor law and HR terminology. Translate the unique values from Dutch to English.

=== Context ===
Sheet: {sheet_name}
These are unique values from columns that likely contain text data.

=== Sample Data ===
{json.dumps(sample_data, indent=2, default=str)}

=== Translation Instructions ===
1. Translate each unique value from Dutch to English
2. Preserve codes, numbers, and technical identifiers
3. Maintain professional HR/labor law terminology
4. If a value is already in English, keep it as is
5. For "ja"/"nee" values, translate to "yes"/"no"
6. For empty/null values, keep them as is

=== Output Format ===
Return a JSON object with the following structure:
{{
    "translations": {{
        "column_name": {{
            "dutch_value": "english_value"
        }}
    }}
}}

Only return valid JSON. Do not include markdown or code blocks.
"""
        
        try:
            raw_output = query_gemini(prompt)
            cleaned_output = clean_gemini_output(raw_output)
            result = json.loads(cleaned_output)
            
            # Apply translations to the dataframe
            for col, value_translations in result.get("translations", {}).items():
                if col in translated_df.columns:
                    translated_df[col] = translated_df[col].replace(value_translations)
                    
        except Exception as e:
            print(f"Error translating data values for batch {i//batch_size + 1}: {e}")
            continue
    
    return translated_df

def update_translation_helper(translations, abbreviations, explanations, sheet_name):
    """
    Update the TranslationHelper.xlsx file with new translations.
    Args:
        translations (dict): Column header translations.
        abbreviations (dict): Abbreviation meanings.
        explanations (dict): Term explanations.
        sheet_name (str): Name of the sheet.
    """
    try:
        # Load existing translation helper
        if os.path.exists(TRANSLATION_HELPER_PATH):
            helper_df = pd.read_excel(TRANSLATION_HELPER_PATH)
        else:
            helper_df = pd.DataFrame(columns=['Dutch', 'Translation', 'Explanation', 'Confidence Level', 'Matching Topic', 'Source Sheet'])
        
        # Prepare new entries
        new_entries = []
        
        # Add column header translations
        for dutch, english in translations.items():
            new_entries.append({
                'Dutch': dutch,
                'Translation': english,
                'Explanation': explanations.get(dutch, ''),
                'Confidence Level': 0.95,
                'Matching Topic': 'column_header',
                'Source Sheet': sheet_name
            })
        
        # Add abbreviations
        for abbrev, meaning in abbreviations.items():
            new_entries.append({
                'Dutch': abbrev,
                'Translation': meaning,
                'Explanation': f'Abbreviation found in {sheet_name}',
                'Confidence Level': 0.85,
                'Matching Topic': 'abbreviation',
                'Source Sheet': sheet_name
            })
        
        # Add explanations
        for term, explanation in explanations.items():
            if term not in translations:  # Avoid duplicates
                new_entries.append({
                    'Dutch': term,
                    'Translation': '',
                    'Explanation': explanation,
                    'Confidence Level': 0.90,
                    'Matching Topic': 'explanation',
                    'Source Sheet': sheet_name
                })
        
        # Add new entries to helper
        if new_entries:
            new_df = pd.DataFrame(new_entries)
            helper_df = pd.concat([helper_df, new_df], ignore_index=True)
            
            # Remove duplicates
            helper_df = helper_df.drop_duplicates(subset=['Dutch', 'Source Sheet'], keep='last')
            
            # Save updated helper
            helper_df.to_excel(TRANSLATION_HELPER_PATH, index=False)
            print(f"Updated TranslationHelper.xlsx with {len(new_entries)} new entries from {sheet_name}")
    
    except Exception as e:
        print(f"Error updating TranslationHelper.xlsx: {e}")

# =========================
# Main Translation Logic
# =========================

def translate_sheet(df, sheet_name):
    """
    Translate a single sheet from Dutch to English.
    Args:
        df (pd.DataFrame): The dataframe to translate.
        sheet_name (str): Name of the sheet.
    Returns:
        pd.DataFrame: Translated dataframe.
    """
    print(f"Translating sheet: {sheet_name}")
    
    # Step 1: Translate column headers
    print(f"  Translating column headers...")
    translation_result = translate_column_headers(df, sheet_name)
    
    # Step 2: Update translation helper
    update_translation_helper(
        translation_result.get("translations", {}),
        translation_result.get("abbreviations", {}),
        translation_result.get("explanations", {}),
        sheet_name
    )
    
    # Step 3: Rename columns
    translated_df = df.copy()
    column_translations = translation_result.get("translations", {})
    translated_df = translated_df.rename(columns=column_translations)
    
    # Step 4: Translate data values
    print(f"  Translating data values...")
    translated_df = translate_data_values(translated_df, translation_result, sheet_name)
    
    print(f"  Completed translation of {sheet_name}")
    return translated_df

def main():
    """
    Main function to translate all sheets in CAOData2024.xlsx.
    """
    print(f"Starting CAO data translation with API key {key_number}")
    print(f"Process {process_id + 1} of {total_processes}")
    
    # Check if input file exists
    if not os.path.exists(CAO_DATA_PATH):
        print(f"Error: {CAO_DATA_PATH} not found!")
        return
    
    # Load the Excel file
    print(f"Loading {CAO_DATA_PATH}...")
    excel_file = pd.ExcelFile(CAO_DATA_PATH)
    
    # Get list of sheets to process - only from "Hoofdstuk 5 - BWA 2024" onwards
    all_sheets = excel_file.sheet_names
    
    # Find the starting index for "Hoofdstuk 5 - BWA 2024"
    start_sheet = "Hoofdstuk 5 - BWA 2024"
    try:
        start_index = all_sheets.index(start_sheet)
        sheets_to_translate = all_sheets[start_index:]
        print(f"First 5 sheets already translated. Starting from sheet {start_index + 1}: {start_sheet}")
    except ValueError:
        print(f"Warning: Could not find '{start_sheet}', processing all sheets")
        sheets_to_translate = all_sheets
    
    # Distribute work across processes
    sheets_per_process = len(sheets_to_translate) // total_processes
    start_idx = process_id * sheets_per_process
    end_idx = start_idx + sheets_per_process if process_id < total_processes - 1 else len(sheets_to_translate)
    sheets_to_process = sheets_to_translate[start_idx:end_idx]
    
    print(f"Processing sheets {start_idx + 1}-{end_idx} of {len(sheets_to_translate)}: {sheets_to_process}")
    
    # Track processed sheets
    processed_sheets = []
    
    # Create output Excel writer
    with pd.ExcelWriter(OUTPUT_TRANSLATED_PATH, engine='openpyxl') as writer:
        # First, copy the already translated sheets (first 5)
        if start_index > 0:
            print(f"Copying first {start_index} already translated sheets...")
            for i in range(start_index):
                sheet_name = all_sheets[i]
                try:
                    df = pd.read_excel(CAO_DATA_PATH, sheet_name=sheet_name)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"  Copied sheet: {sheet_name}")
                except Exception as e:
                    print(f"Error copying sheet {sheet_name}: {e}")
        
        # Now process the sheets that need translation
        for sheet_name in sheets_to_process:
            try:
                print(f"\n--- Processing {sheet_name} ---")
                
                # Read the sheet
                df = pd.read_excel(CAO_DATA_PATH, sheet_name=sheet_name)
                print(f"  Loaded {df.shape[0]} rows, {df.shape[1]} columns")
                
                # Translate the sheet
                translated_df = translate_sheet(df, sheet_name)
                
                # Write to output file
                translated_df.to_excel(writer, sheet_name=sheet_name, index=False)
                processed_sheets.append(sheet_name)
                print(f"  Saved translated sheet: {sheet_name}")
                
                # Small delay to avoid rate limiting
                time.sleep(2)
                
            except Exception as e:
                print(f"Error processing sheet {sheet_name}: {e}")
                # Write original data if translation fails
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                processed_sheets.append(sheet_name)
                continue
        
        # Ensure at least one sheet is written
        if not processed_sheets and start_index == 0:
            # Write a simple overview sheet if no sheets were processed
            overview_df = pd.DataFrame({
                'Status': ['Translation failed or no sheets processed'],
                'Timestamp': [pd.Timestamp.now()]
            })
            overview_df.to_excel(writer, sheet_name='Overview', index=False)
    
    print(f"\nTranslation completed! Output saved to: {OUTPUT_TRANSLATED_PATH}")
    print(f"Translation helper updated: {TRANSLATION_HELPER_PATH}")
    print(f"Processed {len(processed_sheets)} sheets: {processed_sheets}")
    if start_index > 0:
        print(f"Copied {start_index} already translated sheets")

if __name__ == "__main__":
    main()
