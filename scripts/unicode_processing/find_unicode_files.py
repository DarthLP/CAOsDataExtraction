"""
Find Unicode Files Analyzer
===========================

DESCRIPTION:
This script analyzes JSON files in the parsed_pdfs folder to find those containing
unicode patterns (like /uni0032/uni0048/...). It only analyzes and reports - no
files are copied or modified.

USAGE:
    python scripts/unicode_processing/find_unicode_files.py

OUTPUT:
    - Analysis report of files with unicode content
    - Summary statistics
"""
import os
import sys
import json
import re
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def detect_unicode_content(text):
    """Detect if text contains unicode patterns like /uniXXXX/."""
    # Look for patterns like /uniXXXX/ where XXXX are hex digits
    unicode_pattern = r'/uni[0-9a-fA-F]{4}/'
    matches = re.findall(unicode_pattern, text)
    return len(matches) > 0, len(matches)

def analyze_json_file(json_path):
    """Analyze a single JSON file for unicode content."""
    try:
        # Read the JSON file
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Check if any page contains unicode content
        has_unicode = False
        total_unicode_patterns = 0
        pages_with_unicode = 0
        
        for page in json_data:
            if isinstance(page, dict) and 'text' in page:
                text = page['text']
                contains_unicode, pattern_count = detect_unicode_content(text)
                if contains_unicode:
                    has_unicode = True
                    total_unicode_patterns += pattern_count
                    pages_with_unicode += 1
        
        if has_unicode:
            return {
                'file': json_path.name,
                'cao': json_path.parent.name,
                'total_pages': len(json_data),
                'pages_with_unicode': pages_with_unicode,
                'total_unicode_patterns': total_unicode_patterns,
                'file_size_mb': json_path.stat().st_size / (1024 * 1024)
            }
        else:
            return None
        
    except Exception as e:
        print(f"  Error analyzing {json_path.name}: {str(e)}")
        return None

def main():
    print("🔍 Unicode Files Analyzer")
    print("=" * 40)
    
    # Setup paths
    parsed_pdfs_folder = Path('outputs/parsed_pdfs/parsed_pdfs_json')
    
    if not parsed_pdfs_folder.exists():
        print("❌ Parsed PDFs folder not found: outputs/parsed_pdfs/parsed_pdfs_json")
        return
    
    # Find all JSON files
    json_files = []
    for cao_folder in parsed_pdfs_folder.iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            for json_file in cao_folder.glob('*.json'):
                json_files.append(json_file)
    
    print(f"📁 Found {len(json_files)} JSON files to analyze")
    
    # Analyze files
    unicode_files = []
    
    for json_file in json_files:
        print(f"  Analyzing {json_file.name}...")
        
        result = analyze_json_file(json_file)
        
        if result:
            unicode_files.append(result)
            print(f"    ✅ Contains {result['total_unicode_patterns']} unicode patterns")
        else:
            print(f"    ⏭️  No unicode patterns found")
    
    # Generate summary
    print(f"\n📊 Analysis Summary")
    print(f"   Total JSON files analyzed: {len(json_files)}")
    print(f"   Files with unicode content: {len(unicode_files)}")
    
    if unicode_files:
        total_patterns = sum(f['total_unicode_patterns'] for f in unicode_files)
        total_size = sum(f['file_size_mb'] for f in unicode_files)
        
        print(f"   Total unicode patterns found: {total_patterns}")
        print(f"   Total size of unicode files: {total_size:.2f} MB")
        
        print(f"\n📋 Files with unicode content (sorted by pattern count):")
        for file_info in sorted(unicode_files, key=lambda x: x['total_unicode_patterns'], reverse=True):
            print(f"   - {file_info['file']} (CAO {file_info['cao']})")
            print(f"     Patterns: {file_info['total_unicode_patterns']}")
            print(f"     Pages with unicode: {file_info['pages_with_unicode']}/{file_info['total_pages']}")
            print(f"     File size: {file_info['file_size_mb']:.2f} MB")
        
        # Save analysis to file
        analysis_file = Path('outputs/unicode_analysis.json')
        analysis_file.parent.mkdir(exist_ok=True)
        
        analysis = {
            'total_files_analyzed': len(json_files),
            'files_with_unicode': len(unicode_files),
            'total_unicode_patterns': total_patterns,
            'total_size_mb': total_size,
            'unicode_files': unicode_files
        }
        
        with open(analysis_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        
        print(f"\n📁 Analysis saved to: {analysis_file}")
        print(f"\n💡 Next steps:")
        print(f"   1. Review the analysis above")
        print(f"   2. Run the copy script to copy unicode files")
        print(f"   3. Run the transform script to convert unicode to normal text")
        
    else:
        print(f"\n✅ No files with unicode content found!")

if __name__ == "__main__":
    main()
