"""
Unicode JSON Processor
======================

DESCRIPTION:
This script finds JSON files in the parsed_pdfs folder that contain unicode patterns
(like /uni0032/uni0048/...), copies them to a new folder structure, and transforms
them to normal readable text.

USAGE:
    python scripts/unicode_processing/unicode_json_processor.py

OUTPUT:
    - outputs/unicode_pdfs/original/ - Contains original unicode JSON files
    - outputs/unicode_pdfs/transformed/ - Contains transformed normal text JSON files
"""
import os
import sys
import json
import re
import shutil
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def detect_unicode_content(text):
    """Detect if text contains unicode patterns like /uniXXXX/."""
    # Look for patterns like /uniXXXX/ where XXXX are hex digits
    unicode_pattern = r'/uni[0-9a-fA-F]{4}/'
    matches = re.findall(unicode_pattern, text)
    return len(matches) > 0, len(matches)

def convert_unicode_to_text(text):
    """Convert unicode patterns like /uniXXXX/ to actual characters."""
    def replace_unicode(match):
        unicode_hex = match.group(1)  # Extract the hex part
        try:
            # Convert hex to unicode character
            unicode_char = chr(int(unicode_hex, 16))
            return unicode_char
        except (ValueError, OverflowError):
            # If conversion fails, return the original pattern
            return match.group(0)
    
    # Replace all /uniXXXX/ patterns with actual characters
    converted_text = re.sub(r'/uni([0-9a-fA-F]{4})/', replace_unicode, text)
    return converted_text

def process_json_file(json_path, original_folder, transformed_folder):
    """Process a single JSON file: copy original and create transformed version."""
    try:
        # Read the JSON file
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Check if any page contains unicode content
        has_unicode = False
        total_unicode_patterns = 0
        
        for page in json_data:
            if isinstance(page, dict) and 'text' in page:
                text = page['text']
                contains_unicode, pattern_count = detect_unicode_content(text)
                if contains_unicode:
                    has_unicode = True
                    total_unicode_patterns += pattern_count
        
        if not has_unicode:
            return False, 0
        
        # Copy original file to original folder
        original_dest = original_folder / json_path.name
        shutil.copy2(json_path, original_dest)
        
        # Create transformed version
        transformed_data = []
        for page in json_data:
            if isinstance(page, dict):
                transformed_page = page.copy()
                if 'text' in transformed_page:
                    # Convert unicode patterns to normal text
                    transformed_page['text'] = convert_unicode_to_text(transformed_page['text'])
                transformed_data.append(transformed_page)
            else:
                transformed_data.append(page)
        
        # Save transformed version
        transformed_dest = transformed_folder / json_path.name
        with open(transformed_dest, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        return True, total_unicode_patterns
        
    except Exception as e:
        print(f"  Error processing {json_path.name}: {str(e)}")
        return False, 0

def main():
    print("🔍 Unicode JSON Processor")
    print("=" * 40)
    
    # Setup paths
    parsed_pdfs_folder = Path('outputs/parsed_pdfs/parsed_pdfs_json')
    unicode_pdfs_folder = Path('outputs/unicode_pdfs')
    original_folder = unicode_pdfs_folder / 'original'
    transformed_folder = unicode_pdfs_folder / 'transformed'
    
    # Create output folders
    original_folder.mkdir(parents=True, exist_ok=True)
    transformed_folder.mkdir(parents=True, exist_ok=True)
    
    if not parsed_pdfs_folder.exists():
        print("❌ Parsed PDFs folder not found: outputs/parsed_pdfs/parsed_pdfs_json")
        return
    
    # Find all JSON files
    json_files = []
    for cao_folder in parsed_pdfs_folder.iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            for json_file in cao_folder.glob('*.json'):
                json_files.append(json_file)
    
    print(f"📁 Found {len(json_files)} JSON files to check")
    
    # Process files
    unicode_files = []
    total_unicode_patterns = 0
    
    for json_file in json_files:
        print(f"  Checking {json_file.name}...")
        
        is_unicode, pattern_count = process_json_file(json_file, original_folder, transformed_folder)
        
        if is_unicode:
            unicode_files.append({
                'file': json_file.name,
                'cao': json_file.parent.name,
                'patterns': pattern_count
            })
            total_unicode_patterns += pattern_count
            print(f"    ✅ Contains {pattern_count} unicode patterns")
        else:
            print(f"    ⏭️  No unicode patterns found")
    
    # Generate summary
    print(f"\n📊 Processing Summary")
    print(f"   Total JSON files checked: {len(json_files)}")
    print(f"   Files with unicode content: {len(unicode_files)}")
    print(f"   Total unicode patterns found: {total_unicode_patterns}")
    
    if unicode_files:
        print(f"\n📋 Files with unicode content:")
        for file_info in sorted(unicode_files, key=lambda x: x['patterns'], reverse=True):
            print(f"   - {file_info['file']} (CAO {file_info['cao']}): {file_info['patterns']} patterns")
        
        print(f"\n📁 Output folders:")
        print(f"   Original files: {original_folder}")
        print(f"   Transformed files: {transformed_folder}")
        
        # Save summary to file
        summary_file = unicode_pdfs_folder / 'unicode_summary.json'
        summary = {
            'total_files_checked': len(json_files),
            'files_with_unicode': len(unicode_files),
            'total_unicode_patterns': total_unicode_patterns,
            'unicode_files': unicode_files
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"   Summary saved to: {summary_file}")
    else:
        print(f"\n✅ No files with unicode content found!")

if __name__ == "__main__":
    main()
