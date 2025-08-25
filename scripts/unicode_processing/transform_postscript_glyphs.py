"""
PostScript Glyph Transformation Script
=====================================

DESCRIPTION:
This script transforms JSON files containing PostScript glyph patterns (/GXXX/)
to normal readable text. It reads from outputs/unicode_pdfs/original/ and
saves transformed files to outputs/unicode_pdfs/transformed/.

USAGE:
    python scripts/unicode_processing/transform_postscript_glyphs.py

OUTPUT:
    - Transformed JSON files in outputs/unicode_pdfs/transformed/
"""

import os
import sys
import json
import re
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def convert_postscript_glyphs_to_text(text):
    """Convert PostScript glyph patterns like /GXXX/ to actual characters."""
    def replace_glyph(match):
        glyph_code = match.group(1)
        try:
            # Convert decimal to character (PostScript glyphs use decimal ASCII codes)
            char_code = int(glyph_code)
            return chr(char_code)
        except (ValueError, OverflowError):
            return match.group(0)

    # Replace all /GXXX and GXXX patterns with actual characters
    # First replace /GXXX patterns (with leading slash)
    converted_text = re.sub(r'/G([0-9]+)', replace_glyph, text)
    # Then replace remaining GXXX patterns (without slash)
    converted_text = re.sub(r'G([0-9]+)', replace_glyph, converted_text)
    
    # Convert literal \n to actual line breaks
    converted_text = converted_text.replace('\\n', '\n')
    
    # Clean up excessive line breaks - replace 3 or more \n with just 2
    converted_text = re.sub(r'\n{3,}', '\n\n', converted_text)
    
    # Clean up stray slashes that are likely artifacts from the glyph conversion
    # Remove slashes that appear at the end of lines or are isolated
    converted_text = re.sub(r'/\s*\n', '\n', converted_text)  # Remove trailing slashes before line breaks
    converted_text = re.sub(r'/\s*$', '', converted_text)     # Remove trailing slashes at end of text
    
    # Fix common patterns where slashes split words incorrectly
    converted_text = re.sub(r'(\w+)/(\w+)', r'\1\2', converted_text)  # Join words split by slashes
    converted_text = re.sub(r'(\d+)/(\d+)', r'\1\2', converted_text)  # Join numbers split by slashes
    
    # Clean up excessive spaces - replace multiple spaces with single space
    converted_text = re.sub(r' +', ' ', converted_text)
    
    # Remove leading/trailing spaces from each line, but keep empty lines
    lines = converted_text.split('\n')
    cleaned_lines = []
    for line in lines:
        cleaned_line = line.strip()
        cleaned_lines.append(cleaned_line)
    
    return '\n'.join(cleaned_lines)

def transform_json_file(json_path, transformed_folder):
    """Transform a single JSON file containing PostScript glyphs."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Transform text in each page
        for page in data:
            if 'text' in page:
                original_text = page['text']
                transformed_text = convert_postscript_glyphs_to_text(original_text)
                page['text'] = transformed_text
        
        # Save transformed file
        output_path = transformed_folder / json_path.name
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return True, output_path
        
    except Exception as e:
        return False, str(e)

def main():
    print("🔄 PostScript Glyph Transformation")
    print("=" * 40)
    
    original_folder = Path('outputs/unicode_pdfs/original')
    transformed_folder = Path('outputs/unicode_pdfs/transformed')
    
    if not original_folder.exists():
        print(f"❌ Original folder not found: {original_folder}")
        return
    
    # Find all JSON files in original folder
    json_files = list(original_folder.glob('*.json'))
    
    if not json_files:
        print("✅ No JSON files found in original folder.")
        return
    
    print(f"📁 Found {len(json_files)} JSON files to transform")
    
    success_count = 0
    for json_file in json_files:
        print(f"🔄 Processing: {json_file.name}")
        
        success, result = transform_json_file(json_file, transformed_folder)
        
        if success:
            print(f"   ✅ Transformed: {result}")
            success_count += 1
        else:
            print(f"   ❌ Error: {result}")
    
    print(f"\n📊 Transformation Complete")
    print(f"   Successfully transformed: {success_count}/{len(json_files)} files")

if __name__ == "__main__":
    main()
