"""
Copy Unicode Files
==================

DESCRIPTION:
This script copies JSON files with unicode content to the unicode_pdfs folder
structure. It reads the analysis from the previous step and copies files safely.

USAGE:
    python scripts/copy_unicode_files.py

OUTPUT:
    - outputs/unicode_pdfs/original/ - Contains copied unicode JSON files
"""
import os
import sys
import json
import shutil
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    print("📋 Copy Unicode Files")
    print("=" * 30)
    
    # Read the analysis file
    analysis_file = Path('outputs/unicode_analysis.json')
    
    if not analysis_file.exists():
        print("❌ Analysis file not found. Please run find_unicode_files.py first.")
        return
    
    with open(analysis_file, 'r', encoding='utf-8') as f:
        analysis = json.load(f)
    
    unicode_files = analysis.get('unicode_files', [])
    
    if not unicode_files:
        print("✅ No unicode files to copy!")
        return
    
    # Setup paths
    parsed_pdfs_folder = Path('outputs/parsed_pdfs')
    unicode_pdfs_folder = Path('outputs/unicode_pdfs')
    original_folder = unicode_pdfs_folder / 'original'
    
    # Create output folder
    original_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Copying {len(unicode_files)} unicode files to {original_folder}")
    
    # Copy each file
    copied_files = []
    
    for file_info in unicode_files:
        cao_number = file_info['cao']
        filename = file_info['file']
        
        # Source file path
        source_path = parsed_pdfs_folder / cao_number / filename
        
        if not source_path.exists():
            print(f"  ❌ Source file not found: {source_path}")
            continue
        
        # Destination file path
        dest_path = original_folder / filename
        
        try:
            # Copy the file
            shutil.copy2(source_path, dest_path)
            copied_files.append(filename)
            print(f"  ✅ Copied: {filename} ({file_info['file_size_mb']:.2f} MB)")
            
        except Exception as e:
            print(f"  ❌ Error copying {filename}: {str(e)}")
    
    # Summary
    print(f"\n📊 Copy Summary")
    print(f"   Files to copy: {len(unicode_files)}")
    print(f"   Successfully copied: {len(copied_files)}")
    print(f"   Destination: {original_folder}")
    
    if copied_files:
        print(f"\n💡 Next step: Run the transform script to convert unicode to normal text")
    
    return copied_files

if __name__ == "__main__":
    main()
