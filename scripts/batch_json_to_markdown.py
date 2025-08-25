"""
Batch JSON to Markdown Conversion Script
========================================

DESCRIPTION:
This script converts all JSON files from the parsed_pdfs_json folder to markdown files
and saves them in the parsed_pdfs_markdown folder, maintaining the same folder structure.

USAGE:
    python scripts/batch_json_to_markdown.py

INPUT:
    - JSON files in outputs/parsed_pdfs/parsed_pdfs_json/[CAO_NUMBER]/ folders

OUTPUT:
    - Markdown files in outputs/parsed_pdfs/parsed_pdfs_markdown/[CAO_NUMBER]/ folders
"""

import os
import sys
from pathlib import Path
import yaml

# Add the parent directory to Python path so we can import monitoring
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load configuration
with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

def convert_all_json_to_markdown():
    """Convert all JSON files to markdown files."""
    
    json_root = Path(config['paths']['parsed_pdfs'])
    markdown_root = Path(config['paths']['parsed_pdfs_markdown'])
    
    print(f"🔄 Starting batch JSON to Markdown conversion...")
    print(f"📁 JSON source: {json_root}")
    print(f"📁 Markdown destination: {markdown_root}")
    print()
    
    total_files = 0
    converted_files = 0
    skipped_files = 0
    error_files = 0
    
    # Get all CAO folders
    cao_folders = [f for f in json_root.iterdir() if f.is_dir()]
    print(f"📊 Found {len(cao_folders)} CAO folders to process")
    print()
    
    for cao_folder in sorted(cao_folders):
        cao_number = cao_folder.name
        print(f"📂 Processing CAO {cao_number}...")
        
        # Get all JSON files in this CAO folder
        json_files = list(cao_folder.glob("*.json"))
        total_files += len(json_files)
        
        if not json_files:
            print(f"   ⚠️  No JSON files found in CAO {cao_number}")
            continue
        
        print(f"   📄 Found {len(json_files)} JSON files")
        
        # Create markdown output folder
        markdown_folder = markdown_root / cao_number
        markdown_folder.mkdir(parents=True, exist_ok=True)
        
        cao_converted = 0
        cao_skipped = 0
        cao_errors = 0
        
        for json_file in json_files:
            json_filename = json_file.name
            markdown_filename = json_filename.replace('.json', '.md')
            markdown_path = markdown_folder / markdown_filename
            
            # Check if markdown already exists
            if markdown_path.exists():
                print(f"   ⏭️  Skipped {json_filename} (markdown already exists)")
                cao_skipped += 1
                skipped_files += 1
                continue
            
            try:
                # Import and run the conversion
                from scripts.json_to_markdown import convert_json_to_markdown
                
                # Convert the file
                success = convert_json_to_markdown(cao_number, json_filename)
                
                if success:
                    print(f"   ✅ Converted {json_filename}")
                    cao_converted += 1
                    converted_files += 1
                else:
                    print(f"   ❌ Failed to convert {json_filename}")
                    cao_errors += 1
                    error_files += 1
                    
            except Exception as e:
                print(f"   💥 Error converting {json_filename}: {str(e)}")
                cao_errors += 1
                error_files += 1
        
        print(f"   📊 CAO {cao_number}: {cao_converted} converted, {cao_skipped} skipped, {cao_errors} errors")
        print()
    
    # Final summary
    print("=" * 60)
    print("📊 BATCH CONVERSION SUMMARY")
    print("=" * 60)
    print(f"📁 Total JSON files found: {total_files}")
    print(f"✅ Successfully converted: {converted_files}")
    print(f"⏭️  Skipped (already exists): {skipped_files}")
    print(f"❌ Errors: {error_files}")
    print(f"📈 Success rate: {(converted_files / total_files * 100):.1f}%" if total_files > 0 else "N/A")
    print("=" * 60)

if __name__ == "__main__":
    convert_all_json_to_markdown()
