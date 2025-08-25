"""
JSON to Markdown Converter
==========================

DESCRIPTION:
This script converts parsed JSON files (from p2_extract.py) into markdown format
that can be uploaded to Gemini's document vision API. It extracts the text content
from each page and formats it as a clean markdown document.

USAGE:
python scripts/json_to_markdown.py <cao_number> <json_filename>

EXAMPLES:
python scripts/json_to_markdown.py 316 "levensmiddelensbedrijf_definitief.json"
"""

import json
import sys
import os
from pathlib import Path

def convert_json_to_markdown(cao_number, json_filename):
    """Convert a JSON file to Markdown format."""
    
    # Construct paths
    json_path = f"outputs/parsed_pdfs/parsed_pdfs_json/{cao_number}/{json_filename}"
    
    # Check if JSON file exists
    if not os.path.exists(json_path):
        print(f"   ❌ JSON file not found: {json_path}")
        return False
    
    # Create output filename and directory
    base_name = json_filename.replace('.json', '')
    output_filename = f"{base_name}.md"
    
    # Create new directory structure
    output_dir = f"outputs/parsed_pdfs/parsed_pdfs_markdown/{cao_number}"
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = f"{output_dir}/{output_filename}"
    
    try:
        # Read JSON file
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create markdown content
        markdown_content = []
        
        # Add header
        markdown_content.append("# CAO Document - Extracted Content\n")
        markdown_content.append(f"*Converted from: {os.path.basename(json_path)}*\n")
        markdown_content.append("---\n")
        
        # Process each page
        for page_data in data:
            page_num = page_data.get('page', 'Unknown')
            text = page_data.get('text', '').strip()
            ocr_used = page_data.get('ocr_used', False)
            
            if text:  # Only add pages with content
                # Add page header
                markdown_content.append(f"## Page {page_num}\n")
                
                # Add OCR indicator if used
                if ocr_used:
                    markdown_content.append("*[Text extracted using OCR]*\n")
                
                # Add the text content
                markdown_content.append(text)
                markdown_content.append("\n\n")
        
        # Write markdown file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(''.join(markdown_content))
        
        return True
        
    except Exception as e:
        print(f"   💥 Error converting {json_filename}: {str(e)}")
        return False

def json_to_markdown(json_path, output_path):
    """
    Convert JSON file to markdown format.
    
    Args:
        json_path: Path to the input JSON file
        output_path: Path to the output markdown file
    """
    print(f"📖 Converting JSON to Markdown...")
    print(f"   Input: {json_path}")
    print(f"   Output: {output_path}")
    
    # Read JSON file
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Create markdown content
    markdown_content = []
    
    # Add header
    markdown_content.append("# CAO Document - Extracted Content\n")
    markdown_content.append(f"*Converted from: {os.path.basename(json_path)}*\n")
    markdown_content.append("---\n")
    
    # Process each page
    for page_data in data:
        page_num = page_data.get('page', 'Unknown')
        text = page_data.get('text', '').strip()
        ocr_used = page_data.get('ocr_used', False)
        
        if text:  # Only add pages with content
            # Add page header
            markdown_content.append(f"## Page {page_num}\n")
            
            # Add OCR indicator if used
            if ocr_used:
                markdown_content.append("*[Text extracted using OCR]*\n")
            
            # Add the text content
            markdown_content.append(text)
            markdown_content.append("\n\n")
    
    # Write markdown file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(''.join(markdown_content))
    
    # Get file sizes
    json_size = os.path.getsize(json_path) / (1024 * 1024)
    md_size = os.path.getsize(output_path) / (1024 * 1024)
    
    print(f"✅ Conversion completed!")
    print(f"   JSON size: {json_size:.2f} MB")
    print(f"   Markdown size: {md_size:.2f} MB")
    print(f"   Pages processed: {len([p for p in data if p.get('text', '').strip()])}")
    
    return output_path

def main():
    """Main function to handle command line arguments and conversion."""
    if len(sys.argv) != 3:
        print("❌ Usage: python scripts/json_to_markdown.py <cao_number> <json_filename>")
        print()
        print("Examples:")
        print("  python scripts/json_to_markdown.py 316 \"levensmiddelensbedrijf_definitief.json\"")
        sys.exit(1)
    
    cao_number = sys.argv[1]
    json_filename = sys.argv[2]
    
    # Construct paths
    json_path = f"outputs/parsed_pdfs/parsed_pdfs_json/{cao_number}/{json_filename}"
    
    # Check if JSON file exists
    if not os.path.exists(json_path):
        print(f"❌ JSON file not found: {json_path}")
        print(f"   Expected location: outputs/parsed_pdfs/parsed_pdfs_json/{cao_number}/")
        print(f"   Available files in {cao_number}/:")
        try:
            files = os.listdir(f"outputs/parsed_pdfs/parsed_pdfs_json/{cao_number}/")
            for file in sorted(files):
                if file.endswith('.json'):
                    print(f"     - {file}")
        except FileNotFoundError:
            print(f"     (CAO {cao_number} folder not found)")
        sys.exit(1)
    
    # Create output filename and directory
    base_name = json_filename.replace('.json', '')
    output_filename = f"{base_name}.md"
    
    # Create new directory structure
    output_dir = f"outputs/parsed_pdfs/parsed_pdfs_markdown/{cao_number}"
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = f"{output_dir}/{output_filename}"
    
    # Convert JSON to Markdown
    try:
        json_to_markdown(json_path, output_path)
        print(f"\n🎯 Next steps:")
        print(f"   1. Test markdown upload: python scripts/single_file_extraction_md.py {cao_number} \"{output_filename}\" 1")
        print(f"   2. Or use the markdown file directly with Gemini API")
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
