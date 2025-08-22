#!/usr/bin/env python3
"""
CAO translation script with disabled Gemini safety constraints
Usage: python translate_cao_unsafe.py [sheet_name]
"""

import os
import pandas as pd
import time
import json
import sys
import google.generativeai as genai
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Configure Gemini
api_key = os.getenv("GOOGLE_API_KEY1")
if not api_key:
    raise ValueError("GOOGLE_API_KEY1 environment variable not found")

genai.configure(api_key=api_key)

# File paths
CAO_DATA_PATH = "_EXTRA/CAOData2024.xlsx"
TRANSLATION_HELPER_PATH = "_EXTRA/TranslationHelper.xlsx"

# Common translations
COMMON_TRANSLATIONS = {
    'caonr': 'cao_number',
    'caonaam': 'cao_name', 
    'ingdat': 'start_date',
    'expdat': 'expiry_date',
    'kvo': 'collective_agreement_type',
    'sbi': 'sbi_code',
    'sbi_sector': 'sbi_sector',
    'cao': 'cao_employees',
    'avv': 'avv_employees',
    'totaal24': 'total_2024',
    'ja': 'yes',
    'nee': 'no',
    'NaN': 'NaN',
    'nan': 'NaN',
    'soort': 'type',
    'naam': 'name',
    'opmerkingen': 'remarks',
    'bedrijfstak': 'industry',
    'onderneming': 'company'
}

def query_gemini_unsafe(prompt, max_retries=3):
    """
    Query Gemini with safety constraints disabled.
    """
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel("gemini-2.5-pro")
            
            # Configure with safety settings disabled
            generation_config = genai.types.GenerationConfig(
                temperature=0.1,
                top_p=0.8,
                top_k=40,
                max_output_tokens=4096
            )
            
            # Disable safety filters
            safety_settings = [
                {
                    "category": "HARM_CATEGORY_HARASSMENT",
                    "threshold": "BLOCK_NONE"
                },
                {
                    "category": "HARM_CATEGORY_HATE_SPEECH", 
                    "threshold": "BLOCK_NONE"
                },
                {
                    "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                    "threshold": "BLOCK_NONE"
                },
                {
                    "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                    "threshold": "BLOCK_NONE"
                }
            ]
            
            response = model.generate_content(
                prompt, 
                generation_config=generation_config,
                safety_settings=safety_settings
            )
            
            if hasattr(response, "text") and response.text.strip():
                return response.text.strip()
            else:
                return None
                
        except Exception as e:
            error_str = str(e).lower()
            if "429" in error_str or "quota" in error_str:
                wait_time = 120 * (2 ** attempt)
                print(f"    Rate limit, waiting {wait_time//60} minutes...")
                time.sleep(wait_time)
                continue
            elif "500" in error_str or "internal" in error_str:
                wait_time = 60 * (2 ** attempt)
                print(f"    Server error, waiting {wait_time//60} minutes...")
                time.sleep(wait_time)
                continue
            else:
                print(f"    Error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(60)
                    continue
                else:
                    return None
    
    return None

def translate_columns_unsafe(columns, sheet_name):
    """
    Translate columns with safety constraints disabled.
    """
    print(f"  Translating {len(columns)} columns...")
    
    # Prepare the prompt
    columns_str = ", ".join(columns)
    
    prompt = f"""Translate these Dutch column headers to English for a labor agreement dataset.

Sheet: {sheet_name}
Columns: {columns_str}

Instructions:
- Translate each column header from Dutch to English
- Keep important codes like CAO, SBI unchanged
- Use professional HR/labor law terminology
- If already in English, keep as is
- For abbreviations, provide full meaning

Return JSON format:
{{
    "translations": {{
        "dutch_header": "english_translation"
    }},
    "abbreviations": {{
        "abbreviation": "full_meaning"
    }}
}}

Only return valid JSON, no markdown or explanations."""

    result = query_gemini_unsafe(prompt)
    
    if result:
        try:
            # Clean the response
            if result.startswith("```"):
                lines = result.split("\n")
                result = "\n".join(line for line in lines if not line.strip().startswith("```"))
            
            # Remove trailing commas
            result = re.sub(r',\s*(?=[}\]])', '', result)
            
            parsed = json.loads(result)
            return parsed
        except json.JSONDecodeError as e:
            print(f"    JSON parsing error: {e}")
            print(f"    Raw response: {result[:200]}...")
            return {"translations": {}, "abbreviations": {}}
    else:
        print(f"    No response from Gemini")
        return {"translations": {}, "abbreviations": {}}

def translate_values_unsafe(df, translations, sheet_name):
    """
    Translate data values with safety constraints disabled.
    """
    print(f"  Translating data values...")
    
    translated_df = df.copy()
    
    # Identify text columns
    text_columns = []
    for col in df.columns:
        if col in translations.get("translations", {}):
            # Check if column contains text data
            sample_values = df[col].dropna().astype(str).head(10)
            if len(sample_values) > 0:
                # If more than 20% contain letters
                text_ratio = sum(1 for val in sample_values if re.search(r'[a-zA-Z]', val)) / len(sample_values)
                if text_ratio > 0.2:
                    text_columns.append(col)
    
    print(f"    Found {len(text_columns)} text columns")
    
    # Process text columns in batches
    batch_size = 5
    for i in range(0, len(text_columns), batch_size):
        batch_cols = text_columns[i:i+batch_size]
        print(f"    Processing batch {i//batch_size + 1}: {batch_cols}")
        
        # Get unique values for each column in the batch
        batch_data = {}
        for col in batch_cols:
            unique_values = df[col].dropna().unique()
            if len(unique_values) > 15:
                value_counts = df[col].value_counts()
                unique_values = value_counts.head(15).index.tolist()
            batch_data[col] = list(unique_values)
        
        # Create prompt for batch translation
        batch_str = json.dumps(batch_data, indent=2, default=str)
        
        prompt = f"""Translate these unique values from Dutch to English for a labor agreement dataset.

Sheet: {sheet_name}
Columns and their unique values:
{batch_str}

Instructions:
- Translate each unique value from Dutch to English
- Keep codes, numbers, and technical identifiers unchanged
- Use professional HR/labor law terminology
- If already in English, keep as is
- For "ja"/"nee" values, translate to "yes"/"no"
- For empty/null values, keep them as is

Return JSON format:
{{
    "translations": {{
        "column_name": {{
            "dutch_value": "english_value"
        }}
    }}
}}

Only return valid JSON, no markdown or explanations."""

        result = query_gemini_unsafe(prompt)
        
        if result:
            try:
                # Clean the response
                if result.startswith("```"):
                    lines = result.split("\n")
                    result = "\n".join(line for line in lines if not line.strip().startswith("```"))
                
                # Remove trailing commas
                result = re.sub(r',\s*(?=[}\]])', '', result)
                
                parsed = json.loads(result)
                
                # Apply translations to dataframe
                for col, value_translations in parsed.get("translations", {}).items():
                    if col in translated_df.columns:
                        translated_df[col] = translated_df[col].replace(value_translations)
                        print(f"      Applied {len(value_translations)} translations to {col}")
                
            except json.JSONDecodeError as e:
                print(f"      JSON parsing error for batch: {e}")
                continue
        else:
            print(f"      No response for batch {i//batch_size + 1}")
        
        # Wait between batches
        print(f"    Waiting 2 minutes before next batch...")
        time.sleep(120)
    
    return translated_df

def translate_sheet_unsafe(sheet_name):
    """
    Translate a sheet with safety constraints disabled.
    """
    print(f"\n=== Unsafe Translation: {sheet_name} ===")
    
    try:
        # Read the sheet
        df = pd.read_excel(CAO_DATA_PATH, sheet_name=sheet_name)
        print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Step 1: Translate column headers
        translation_result = translate_columns_unsafe(list(df.columns), sheet_name)
        
        # Step 2: Apply column translations
        translated_df = df.copy()
        column_translations = translation_result.get("translations", {})
        translated_df = translated_df.rename(columns=column_translations)
        
        print(f"  Applied {len(column_translations)} column translations")
        
        # Step 3: Translate data values
        translated_df = translate_values_unsafe(translated_df, translation_result, sheet_name)
        
        # Step 4: Save translated sheet
        output_path = f"_EXTRA/{sheet_name.replace(' ', '_').replace('-', '_')}_unsafe_translated.xlsx"
        translated_df.to_excel(output_path, index=False)
        print(f"Saved unsafe translation to: {output_path}")
        
        # Step 5: Update translation helper
        update_helper_unsafe(translation_result, sheet_name)
        
        return True
        
    except Exception as e:
        print(f"Error processing {sheet_name}: {e}")
        return False

def update_helper_unsafe(translation_result, sheet_name):
    """
    Update translation helper with unsafe translations.
    """
    try:
        # Load existing helper or create new
        if os.path.exists(TRANSLATION_HELPER_PATH):
            helper_df = pd.read_excel(TRANSLATION_HELPER_PATH)
        else:
            helper_df = pd.DataFrame(columns=['Dutch', 'Translation', 'Explanation', 'Confidence Level', 'Matching Topic', 'Source Sheet'])
        
        # Add new entries
        new_entries = []
        
        # Add column header translations
        for dutch, english in translation_result.get("translations", {}).items():
            new_entries.append({
                'Dutch': dutch,
                'Translation': english,
                'Explanation': f'Column header from {sheet_name} (unsafe mode)',
                'Confidence Level': 0.90,
                'Matching Topic': 'column_header',
                'Source Sheet': sheet_name
            })
        
        # Add abbreviations
        for abbrev, meaning in translation_result.get("abbreviations", {}).items():
            new_entries.append({
                'Dutch': abbrev,
                'Translation': meaning,
                'Explanation': f'Abbreviation from {sheet_name} (unsafe mode)',
                'Confidence Level': 0.80,
                'Matching Topic': 'abbreviation',
                'Source Sheet': sheet_name
            })
        
        if new_entries:
            new_df = pd.DataFrame(new_entries)
            helper_df = pd.concat([helper_df, new_df], ignore_index=True)
            helper_df = helper_df.drop_duplicates(subset=['Dutch', 'Source Sheet'], keep='last')
            helper_df.to_excel(TRANSLATION_HELPER_PATH, index=False)
            print(f"Updated TranslationHelper.xlsx with {len(new_entries)} entries")
    
    except Exception as e:
        print(f"Error updating helper: {e}")

def main():
    # Get sheet name from command line or process all remaining sheets
    if len(sys.argv) > 1:
        sheet_to_process = sys.argv[1]
        sheets_to_process = [sheet_to_process]
    else:
        # Process all sheets from Hoofdstuk 5 onwards
        excel_file = pd.ExcelFile(CAO_DATA_PATH)
        all_sheets = excel_file.sheet_names
        
        start_sheet = "Hoofdstuk 5 - BWA 2024"
        try:
            start_index = all_sheets.index(start_sheet)
            sheets_to_process = all_sheets[start_index:]
        except ValueError:
            print(f"Could not find '{start_sheet}', processing all sheets")
            sheets_to_process = all_sheets
    
    print(f"CAO Data Translation with Disabled Safety Constraints")
    print(f"Sheets to process: {sheets_to_process}")
    print(f"Note: Safety filters are disabled to avoid content blocking")
    
    # Check if input file exists
    if not os.path.exists(CAO_DATA_PATH):
        print(f"Error: {CAO_DATA_PATH} not found!")
        return
    
    # Process each sheet
    for i, sheet_name in enumerate(sheets_to_process):
        print(f"\n{'='*60}")
        print(f"Processing sheet {i+1}/{len(sheets_to_process)}: {sheet_name}")
        print(f"{'='*60}")
        
        success = translate_sheet_unsafe(sheet_name)
        if not success:
            print(f"Failed to process {sheet_name}")
        
        # Wait between sheets
        if i < len(sheets_to_process) - 1:
            print(f"\nWaiting 3 minutes before next sheet...")
            time.sleep(180)
    
    print(f"\n{'='*60}")
    print("Unsafe translation process finished!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

