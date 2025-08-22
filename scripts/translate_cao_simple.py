#!/usr/bin/env python3
"""
Simple CAO translation script that avoids Gemini safety filters
Usage: python translate_cao_simple.py [sheet_name]
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

# Common Dutch to English translations
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
    'Bedrijfstak': 'Industry',
    'Onderneming': 'Company',
    'Landbouw, bosbouw en visserij': 'Agriculture, forestry and fishing',
    'industrie en nutsbedrijven': 'industry and utilities',
    'financiele instellingen': 'financial institutions',
    'vervoer en opslag': 'transport and storage',
    'bouw': 'construction',
    'gezondheidszorg': 'healthcare',
    'onderwijs': 'education',
    'overheid': 'government',
    'diensten': 'services'
}

def simple_gemini_translate(text, max_retries=2):
    """
    Simple translation with minimal prompt to avoid safety filters.
    """
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel("gemini-2.5-pro")
            
            # Very simple, safe prompt
            prompt = f"Translate this Dutch term to English: {text}"
            
            response = model.generate_content(prompt)
            
            if hasattr(response, "text") and response.text.strip():
                translation = response.text.strip()
                # Clean the response
                translation = translation.replace('"', '').replace("'", "")
                return translation
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
                break
    
    return None

def translate_columns_simple(columns, sheet_name):
    """
    Translate columns using simple approach.
    """
    print(f"  Translating {len(columns)} columns...")
    
    translations = {}
    
    # First pass: use common translations
    common_count = 0
    for col in columns:
        col_lower = col.lower()
        if col_lower in COMMON_TRANSLATIONS:
            translations[col] = COMMON_TRANSLATIONS[col_lower]
            common_count += 1
        elif col in COMMON_TRANSLATIONS:
            translations[col] = COMMON_TRANSLATIONS[col]
            common_count += 1
    
    print(f"    Common translations: {common_count}")
    
    # Second pass: translate remaining columns one by one
    remaining = [col for col in columns if col not in translations]
    gemini_count = 0
    
    for i, col in enumerate(remaining):
        print(f"    Translating column {i+1}/{len(remaining)}: {col}")
        
        # Try Gemini translation
        translation = simple_gemini_translate(col)
        
        if translation:
            translations[col] = translation
            gemini_count += 1
            print(f"      -> {translation}")
        else:
            # Fallback: simple transformation
            fallback = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
            translations[col] = fallback
            print(f"      -> {fallback} (fallback)")
        
        # Wait between calls
        time.sleep(30)
    
    print(f"    Gemini translations: {gemini_count}")
    print(f"    Fallback translations: {len(remaining) - gemini_count}")
    
    return translations

def translate_values_simple(df, translations, sheet_name):
    """
    Translate data values using simple approach.
    """
    print(f"  Translating data values...")
    
    translated_df = df.copy()
    
    # Identify text columns
    text_columns = []
    for col in df.columns:
        if col in translations:
            # Check if column contains text data
            sample_values = df[col].dropna().astype(str).head(10)
            if len(sample_values) > 0:
                # If more than 20% contain letters
                text_ratio = sum(1 for val in sample_values if re.search(r'[a-zA-Z]', val)) / len(sample_values)
                if text_ratio > 0.2:
                    text_columns.append(col)
    
    print(f"    Found {len(text_columns)} text columns")
    
    # Process each text column
    for i, col in enumerate(text_columns):
        print(f"    Processing column {i+1}/{len(text_columns)}: {col}")
        
        # Get unique values
        unique_values = df[col].dropna().unique()
        if len(unique_values) > 10:
            # Take most common values
            value_counts = df[col].value_counts()
            unique_values = value_counts.head(10).index.tolist()
        
        # Translate each unique value
        value_translations = {}
        for value in unique_values:
            value_str = str(value)
            
            # Skip if already in common translations
            if value_str.lower() in COMMON_TRANSLATIONS:
                value_translations[value] = COMMON_TRANSLATIONS[value_str.lower()]
                continue
            
            # Skip if it's a number or code
            if value_str.replace('.', '').replace(',', '').isdigit():
                continue
            
            # Try Gemini translation
            translation = simple_gemini_translate(value_str)
            if translation and translation.lower() != value_str.lower():
                value_translations[value] = translation
                print(f"      {value_str} -> {translation}")
            
            # Wait between translations
            time.sleep(10)
        
        # Apply translations to column
        if value_translations:
            translated_df[col] = translated_df[col].replace(value_translations)
            print(f"      Applied {len(value_translations)} translations to {col}")
        
        # Wait between columns
        time.sleep(30)
    
    return translated_df

def translate_sheet_simple(sheet_name):
    """
    Simple translation of a sheet.
    """
    print(f"\n=== Simple Translation: {sheet_name} ===")
    
    try:
        # Read the sheet
        df = pd.read_excel(CAO_DATA_PATH, sheet_name=sheet_name)
        print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Step 1: Translate column headers
        translations = translate_columns_simple(list(df.columns), sheet_name)
        
        # Step 2: Apply column translations
        translated_df = df.rename(columns=translations)
        
        # Step 3: Translate data values
        translated_df = translate_values_simple(translated_df, translations, sheet_name)
        
        # Step 4: Save translated sheet
        output_path = f"_EXTRA/{sheet_name.replace(' ', '_').replace('-', '_')}_simple_translated.xlsx"
        translated_df.to_excel(output_path, index=False)
        print(f"Saved simple translation to: {output_path}")
        
        # Step 5: Update translation helper
        update_helper_simple(translations, sheet_name)
        
        return True
        
    except Exception as e:
        print(f"Error processing {sheet_name}: {e}")
        return False

def update_helper_simple(translations, sheet_name):
    """
    Update translation helper with simple translations.
    """
    try:
        # Load existing helper or create new
        if os.path.exists(TRANSLATION_HELPER_PATH):
            helper_df = pd.read_excel(TRANSLATION_HELPER_PATH)
        else:
            helper_df = pd.DataFrame(columns=['Dutch', 'Translation', 'Explanation', 'Confidence Level', 'Matching Topic', 'Source Sheet'])
        
        # Add new entries
        new_entries = []
        for dutch, english in translations.items():
            confidence = 0.95 if dutch.lower() in COMMON_TRANSLATIONS else 0.75
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
    
    print(f"Simple CAO Data Translation (Avoiding Safety Filters)")
    print(f"Sheets to process: {sheets_to_process}")
    print(f"Note: Using minimal prompts to avoid content filtering")
    
    # Check if input file exists
    if not os.path.exists(CAO_DATA_PATH):
        print(f"Error: {CAO_DATA_PATH} not found!")
        return
    
    # Process each sheet
    for i, sheet_name in enumerate(sheets_to_process):
        print(f"\n{'='*60}")
        print(f"Processing sheet {i+1}/{len(sheets_to_process)}: {sheet_name}")
        print(f"{'='*60}")
        
        success = translate_sheet_simple(sheet_name)
        if not success:
            print(f"Failed to process {sheet_name}")
        
        # Wait between sheets
        if i < len(sheets_to_process) - 1:
            print(f"\nWaiting 2 minutes before next sheet...")
            time.sleep(120)
    
    print(f"\n{'='*60}")
    print("Simple translation process finished!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()

