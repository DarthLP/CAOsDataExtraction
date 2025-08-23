"""
Transform Unicode Files
=======================

DESCRIPTION:
This script transforms JSON files with unicode patterns (like /uni0032/uni0048/...)
to normal readable text. It reads from the original folder and saves transformed
versions to the transformed folder.

USAGE:
    python scripts/transform_unicode_files.py

OUTPUT:
    - outputs/unicode_pdfs/transformed/ - Contains transformed normal text JSON files
"""
import os
import sys
import json
import re
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def convert_unicode_to_text(text):
    """Convert unicode patterns like uniXXXX to actual characters."""
    def replace_unicode(match):
        unicode_hex = match.group(1)  # Extract the hex part
        try:
            # Convert hex to unicode character
            unicode_char = chr(int(unicode_hex, 16))
            return unicode_char
        except (ValueError, OverflowError):
            # If conversion fails, return the original pattern
            return match.group(0)
    
    # Replace all uniXXXX patterns with actual characters (both with and without slashes)
    # First try with slashes, then without
    converted_text = re.sub(r'/uni([0-9a-fA-F]{4})/', replace_unicode, text)
    converted_text = re.sub(r'uni([0-9a-fA-F]{4})', replace_unicode, converted_text)
    
    # Convert literal \n to actual line breaks
    converted_text = converted_text.replace('\\n', '\n')
    
    # Clean up stray slashes that are likely artifacts from the unicode conversion
    # Remove slashes that appear at the end of lines or are isolated
    converted_text = re.sub(r'/\s*\n', '\n', converted_text)  # Remove trailing slashes before line breaks
    converted_text = re.sub(r'/\s*$', '', converted_text)     # Remove trailing slashes at end of text
    
    # Fix common patterns where slashes split words incorrectly
    converted_text = re.sub(r'(\w+)/(\w+)', r'\1\2', converted_text)  # Join words split by slashes
    converted_text = re.sub(r'(\d+)/(\d+)', r'\1\2', converted_text)  # Join numbers split by slashes
    
    # Clean up excessive spaces - replace multiple spaces with single space
    converted_text = re.sub(r' +', ' ', converted_text)
    
    # Remove leading/trailing spaces from each line
    lines = converted_text.split('\n')
    cleaned_lines = []
    for line in lines:
        cleaned_line = line.strip()
        if cleaned_line:
            cleaned_lines.append(cleaned_line)
        else:
            cleaned_lines.append('')
    
    return '\n'.join(cleaned_lines)

def transform_json_file(json_path, transformed_folder):
    """Transform a single JSON file from unicode to normal text."""
    try:
        # Read the JSON file
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Transform each page
        transformed_data = []
        total_patterns_converted = 0
        
        for page in json_data:
            if isinstance(page, dict):
                transformed_page = page.copy()
                if 'text' in transformed_page:
                    original_text = transformed_page['text']
                    # Convert unicode patterns to normal text
                    transformed_text = convert_unicode_to_text(original_text)
                    transformed_page['text'] = transformed_text
                    
                    # Count converted patterns
                    unicode_pattern = r'/uni[0-9a-fA-F]{4}/'
                    patterns_found = len(re.findall(unicode_pattern, original_text))
                    total_patterns_converted += patterns_found
                
                transformed_data.append(transformed_page)
            else:
                transformed_data.append(page)
        
        # Save transformed version
        dest_path = transformed_folder / json_path.name
        with open(dest_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        return True, total_patterns_converted
        
    except Exception as e:
        print(f"  Error transforming {json_path.name}: {str(e)}")
        return False, 0

def main():
    print("🔄 Transform Unicode Files")
    print("=" * 35)
    
    # Setup paths
    unicode_pdfs_folder = Path('outputs/unicode_pdfs')
    original_folder = unicode_pdfs_folder / 'original'
    transformed_folder = unicode_pdfs_folder / 'transformed'
    
    if not original_folder.exists():
        print("❌ Original folder not found. Please run copy_unicode_files.py first.")
        return
    
    # Create transformed folder
    transformed_folder.mkdir(parents=True, exist_ok=True)
    
    # Find all JSON files in original folder
    json_files = list(original_folder.glob('*.json'))
    
    if not json_files:
        print("✅ No files to transform!")
        return
    
    print(f"📁 Transforming {len(json_files)} files to {transformed_folder}")
    
    # Transform each file
    transformed_files = []
    total_patterns_converted = 0
    
    for json_file in json_files:
        print(f"  Transforming {json_file.name}...")
        
        success, patterns_converted = transform_json_file(json_file, transformed_folder)
        
        if success:
            transformed_files.append(json_file.name)
            total_patterns_converted += patterns_converted
            print(f"    ✅ Transformed {patterns_converted:,} unicode patterns")
        else:
            print(f"    ❌ Failed to transform")
    
    # Summary
    print(f"\n📊 Transform Summary")
    print(f"   Files to transform: {len(json_files)}")
    print(f"   Successfully transformed: {len(transformed_files)}")
    print(f"   Total unicode patterns converted: {total_patterns_converted:,}")
    print(f"   Destination: {transformed_folder}")
    
    if transformed_files:
        print(f"\n✅ Transformation complete!")
        print(f"   Original files: {original_folder}")
        print(f"   Transformed files: {transformed_folder}")
        
        # Show file size comparison
        for filename in transformed_files:
            original_path = original_folder / filename
            transformed_path = transformed_folder / filename
            
            if original_path.exists() and transformed_path.exists():
                original_size = original_path.stat().st_size / (1024 * 1024)
                transformed_size = transformed_path.stat().st_size / (1024 * 1024)
                size_reduction = ((original_size - transformed_size) / original_size) * 100
                
                print(f"   {filename}:")
                print(f"     Original: {original_size:.2f} MB")
                print(f"     Transformed: {transformed_size:.2f} MB")
                print(f"     Size reduction: {size_reduction:.1f}%")
    
    return transformed_files

if __name__ == "__main__":
    main()
