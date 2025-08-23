"""
PDF vs JSON Size Comparison
===========================

DESCRIPTION:
This script compares file sizes between original PDFs and their parsed JSON files
to understand the size differences and identify potential issues.

USAGE:
    python scripts/compare_pdf_json_sizes.py

OUTPUT:
    - Comparison report showing PDF vs JSON sizes
    - Summary statistics
    - Files with unusual size ratios
"""
import os
import sys
from pathlib import Path
import json

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_file_size_mb(file_path):
    """Get file size in MB."""
    if file_path.exists():
        return file_path.stat().st_size / (1024 * 1024)
    return 0

def find_matching_files():
    """Find matching PDF and JSON files."""
    pdfs_folder = Path('pdfs/input_pdfs')
    parsed_folder = Path('outputs/parsed_pdfs')
    
    matches = []
    
    # Go through each CAO folder in parsed_pdfs
    for cao_folder in parsed_folder.iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            cao_number = cao_folder.name
            
            # Find JSON files in this CAO folder
            for json_file in cao_folder.glob('*.json'):
                json_name = json_file.stem  # filename without .json extension
                
                # Look for matching PDF in the pdfs folder
                # Try different possible PDF names
                possible_pdf_names = [
                    f"{json_name}.pdf",
                    f"{json_name}.PDF"
                ]
                
                pdf_file = None
                for pdf_name in possible_pdf_names:
                    potential_pdf = pdfs_folder / cao_number / pdf_name
                    if potential_pdf.exists():
                        pdf_file = potential_pdf
                        break
                
                # If we found a matching PDF, add to matches
                if pdf_file:
                    matches.append({
                        'cao': cao_number,
                        'name': json_name,
                        'pdf_path': pdf_file,
                        'json_path': json_file,
                        'pdf_size_mb': get_file_size_mb(pdf_file),
                        'json_size_mb': get_file_size_mb(json_file)
                    })
    
    return matches

def analyze_size_ratios(matches):
    """Analyze the size ratios and categorize files."""
    analysis = {
        'total_pairs': len(matches),
        'total_pdf_size': 0,
        'total_json_size': 0,
        'size_ratios': [],
        'large_json_files': [],  # JSON files significantly larger than PDF
        'small_json_files': [],  # JSON files much smaller than PDF
        'similar_sizes': []      # Similar sizes
    }
    
    for match in matches:
        pdf_size = match['pdf_size_mb']
        json_size = match['json_size_mb']
        
        analysis['total_pdf_size'] += pdf_size
        analysis['total_json_size'] += json_size
        
        # Calculate ratio (JSON size / PDF size)
        if pdf_size > 0:
            ratio = json_size / pdf_size
            analysis['size_ratios'].append(ratio)
            match['ratio'] = ratio
            
            # Categorize based on ratio
            if ratio > 2.0:  # JSON is more than 2x larger than PDF
                analysis['large_json_files'].append(match)
            elif ratio < 0.5:  # JSON is less than half the PDF size
                analysis['small_json_files'].append(match)
            else:
                analysis['similar_sizes'].append(match)
        else:
            match['ratio'] = 0
    
    return analysis

def main():
    print("📊 PDF vs JSON Size Comparison")
    print("=" * 40)
    
    # Find matching files
    print("🔍 Finding matching PDF and JSON files...")
    matches = find_matching_files()
    
    if not matches:
        print("❌ No matching PDF and JSON files found!")
        return
    
    print(f"📁 Found {len(matches)} matching file pairs")
    
    # Analyze size ratios
    analysis = analyze_size_ratios(matches)
    
    # Display summary statistics
    print(f"\n📊 Summary Statistics")
    print(f"   Total file pairs: {analysis['total_pairs']}")
    print(f"   Total PDF size: {analysis['total_pdf_size']:.2f} MB")
    print(f"   Total JSON size: {analysis['total_json_size']:.2f} MB")
    print(f"   Overall ratio (JSON/PDF): {analysis['total_json_size']/analysis['total_pdf_size']:.2f}x")
    
    if analysis['size_ratios']:
        avg_ratio = sum(analysis['size_ratios']) / len(analysis['size_ratios'])
        max_ratio = max(analysis['size_ratios'])
        min_ratio = min(analysis['size_ratios'])
        print(f"   Average ratio: {avg_ratio:.2f}x")
        print(f"   Max ratio: {max_ratio:.2f}x")
        print(f"   Min ratio: {min_ratio:.2f}x")
    
    # Show categorized results
    print(f"\n📈 Size Categories")
    print(f"   Large JSON files (>2x PDF): {len(analysis['large_json_files'])}")
    print(f"   Similar sizes (0.5x-2x): {len(analysis['similar_sizes'])}")
    print(f"   Small JSON files (<0.5x PDF): {len(analysis['small_json_files'])}")
    
    # Show top 10 largest ratios
    if analysis['large_json_files']:
        print(f"\n🔍 Top 10 files where JSON is much larger than PDF:")
        large_sorted = sorted(analysis['large_json_files'], key=lambda x: x['ratio'], reverse=True)[:10]
        for match in large_sorted:
            print(f"   CAO {match['cao']}: {match['name']}")
            print(f"     PDF: {match['pdf_size_mb']:.2f} MB → JSON: {match['json_size_mb']:.2f} MB (ratio: {match['ratio']:.1f}x)")
    
    # Show top 10 smallest ratios
    if analysis['small_json_files']:
        print(f"\n🔍 Top 10 files where JSON is much smaller than PDF:")
        small_sorted = sorted(analysis['small_json_files'], key=lambda x: x['ratio'])[:10]
        for match in small_sorted:
            print(f"   CAO {match['cao']}: {match['name']}")
            print(f"     PDF: {match['pdf_size_mb']:.2f} MB → JSON: {match['json_size_mb']:.2f} MB (ratio: {match['ratio']:.1f}x)")
    
    # Save detailed results
    output_file = Path('outputs/pdf_json_size_comparison.json')
    output_file.parent.mkdir(exist_ok=True)
    
    # Prepare data for JSON serialization
    serializable_matches = []
    for match in matches:
        serializable_matches.append({
            'cao': match['cao'],
            'name': match['name'],
            'pdf_size_mb': match['pdf_size_mb'],
            'json_size_mb': match['json_size_mb'],
            'ratio': match.get('ratio', 0)
        })
    
    results = {
        'summary': {
            'total_pairs': analysis['total_pairs'],
            'total_pdf_size_mb': analysis['total_pdf_size'],
            'total_json_size_mb': analysis['total_json_size'],
            'overall_ratio': analysis['total_json_size']/analysis['total_pdf_size'] if analysis['total_pdf_size'] > 0 else 0,
            'avg_ratio': sum(analysis['size_ratios']) / len(analysis['size_ratios']) if analysis['size_ratios'] else 0,
            'large_json_count': len(analysis['large_json_files']),
            'similar_size_count': len(analysis['similar_sizes']),
            'small_json_count': len(analysis['small_json_files'])
        },
        'all_files': serializable_matches
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Detailed results saved to: {output_file}")

if __name__ == "__main__":
    main()
