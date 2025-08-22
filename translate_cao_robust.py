#!/usr/bin/env python3
"""
Robust CAO data translation script that handles rate limits and uses simpler translation approach
Usage: python translate_cao_robust.py [sheet_name]
"""

import os
import pandas as pd
import time
import json
import sys
import google.generativeai as genai
from dotenv import load_dotenv

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

# Simple manual translations for common terms to reduce API calls
MANUAL_TRANSLATIONS = {
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
    'oudstkprf': 'oldest_profile',
    'soort': 'type',
    'soortplus': 'type_plus',
    'naam': 'name',
    'ja': 'yes',
    'nee': 'no',
    'Caonummer': 'CAO_Number',
    'caonaam': 'CAO_Name',
    'Aantal medewerkers CAO': 'Number_of_CAO_Employees',
    'Aantal medewerkers AVV': 'Number_of_AVV_Employees',
    'totaal aantal medewerkers 2024': 'Total_Number_of_Employees_2024',
    'soort-bedrijfstak-of-onderneming': 'Type_Industry_or_Company'
}

def get_gemini_translation(text, max_retries=2):
    """
    Get translation from Gemini with proper error handling and rate limiting.
    """
    for attempt in range(max_retries):
        try:
            # Wait between requests to avoid rate limiting
            if attempt > 0:
                wait_time = 60  # Wait 1 minute between retries
                print(f"    Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            
            model = genai.GenerativeModel("gemini-1.5-flash")  # Use faster, cheaper model
            
            # Very simple prompt
            prompt = f"Translate this Dutch term to English: {text}"
            
            response = model.generate_content(prompt)
            
            if hasattr(response, "text") and response.text.strip():
                translation = response.text.strip().lower()
                # Clean the response
                translation = translation.replace('"', '').replace("'", "")
                if " " in translation:
                    translation = translation.replace(" ", "_")
                return translation
            else:
                print(f"    No valid response for '{text}'")
                return None
                
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "quota" in error_str.lower():
                print(f"    Rate limit hit, waiting longer...")
                time.sleep(120)  # Wait 2 minutes for rate limit
                continue
            elif "500" in error_str:
                print(f"    Server error, retrying...")
                time.sleep(30)
                continue
            else:
                print(f"    Error translating '{text}': {e}")
                break
    
    return None

def translate_columns_hybrid(columns, sheet_name):
    """
    Hybrid translation using manual translations + minimal Gemini calls.
    """
    translations = {}
    
    print(f"Translating {len(columns)} columns for {sheet_name}")
    
    # First pass: use manual translations
    manual_count = 0
    for col in columns:
        col_lower = col.lower()
        if col_lower in MANUAL_TRANSLATIONS:
            translations[col] = MANUAL_TRANSLATIONS[col_lower]
            manual_count += 1
        elif col in MANUAL_TRANSLATIONS:
            translations[col] = MANUAL_TRANSLATIONS[col]
            manual_count += 1
    
    print(f"  Manual translations: {manual_count}/{len(columns)}")
    
    # Second pass: use Gemini for remaining columns (with limits)
    remaining = [col for col in columns if col not in translations]
    gemini_count = 0
    max_gemini_calls = 3  # Limit API calls due to quota
    
    for col in remaining[:max_gemini_calls]:
        print(f"  Translating '{col}' with Gemini...")
        translation = get_gemini_translation(col)
        if translation:
            translations[col] = translation
            gemini_count += 1
        else:
            # Fallback: simple transformation
            fallback = col.lower().replace(' ', '_').replace('-', '_')
            translations[col] = fallback
        
        # Wait between calls to respect rate limits
        time.sleep(30)
    
    # Third pass: fallback transformations for remaining columns
    for col in remaining[max_gemini_calls:]:
        fallback = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        translations[col] = fallback
    
    print(f"  Gemini translations: {gemini_count}")
    print(f"  Fallback translations: {len(remaining) - gemini_count}")
    
    return translations

def translate_sheet_robust(sheet_name):
    """
    Translate a sheet using the robust hybrid approach.
    """
    print(f"\n=== Translating {sheet_name} ===")
    
    try:
        # Read the sheet
        df = pd.read_excel(CAO_DATA_PATH, sheet_name=sheet_name)
        print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Translate column headers
        translations = translate_columns_hybrid(list(df.columns), sheet_name)
        
        # Apply translations
        translated_df = df.rename(columns=translations)
        
        # Show some examples
        print(f"\nSample translations:")
        for i, (orig, trans) in enumerate(translations.items()):
            if i < 5:  # Show first 5
                print(f"  {orig} -> {trans}")
        
        # Save translated sheet
        output_path = f"_EXTRA/{sheet_name.replace(' ', '_').replace('-', '_')}_translated.xlsx"
        translated_df.to_excel(output_path, index=False)
        print(f"\nSaved to: {output_path}")
        
        # Update translation helper
        update_helper(translations, sheet_name)
        
        return True
        
    except Exception as e:
        print(f"Error processing {sheet_name}: {e}")
        return False

def update_helper(translations, sheet_name):
    """
    Update translation helper with new translations.
    """
    try:
        # Load existing helper or create new
        if os.path.exists(TRANSLATION_HELPER_PATH):
            helper_df = pd.read_excel(TRANSLATION_HELPER_PATH)
        else:
            helper_df = pd.DataFrame(columns=['Dutch', 'Translation', 'Explanation', 'Confidence Level', 'Matching Topic', 'Source Sheet'])
        
        # Add new translations
        new_entries = []
        for dutch, english in translations.items():
            confidence = 0.95 if dutch.lower() in MANUAL_TRANSLATIONS else 0.75
            new_entries.append({
                'Dutch': dutch,
                'Translation': english,
                'Explanation': f'Column header from {sheet_name}',
                'Confidence Level': confidence,
                'Matching Topic': 'column_header',
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
    
    print(f"CAO Data Robust Translation")
    print(f"Sheets to process: {sheets_to_process}")
    print(f"Note: Using hybrid approach with manual translations + limited Gemini calls")
    
    # Check if input file exists
    if not os.path.exists(CAO_DATA_PATH):
        print(f"Error: {CAO_DATA_PATH} not found!")
        return
    
    # Process each sheet
    for sheet_name in sheets_to_process:
        success = translate_sheet_robust(sheet_name)
        if not success:
            print(f"Failed to process {sheet_name}")
        
        # Wait between sheets to avoid rate limits
        print("Waiting 2 minutes before next sheet...")
        time.sleep(120)
    
    print("\nTranslation completed!")

if __name__ == "__main__":
    main()

