#!/usr/bin/env python3
"""
Script to analyze JSON files in llmExtracted_json folder and identify empty fields.

This script:
1. Scans all JSON files in llmExtracted_json folder
2. Analyzes which fields are empty in each file
3. Provides detailed statistics about missing data
4. Saves results to empty_files_analysis.txt
"""

import os
import json
from pathlib import Path
from collections import defaultdict

def is_empty_value(value):
    """
    Check if a value is considered empty.
    
    Args:
        value: The value to check
    
    Returns:
        bool: True if the value is considered empty
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.lower() in ["empty", "null", "none"]
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False

def analyze_json_file(file_path):
    """
    Analyze a single JSON file for empty fields.
    
    Args:
        file_path: Path to the JSON file
    
    Returns:
        dict: Analysis results for this file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        empty_fields = []
        non_empty_fields = []
        total_fields = 0
        
        # Analyze the JSON structure
        if isinstance(data, dict):
            for key, value in data.items():
                total_fields += 1
                if is_empty_value(value):
                    empty_fields.append(key)
                else:
                    non_empty_fields.append(key)
        elif isinstance(data, list):
            # If it's a list, check each item
            for item in data:
                if isinstance(item, dict):
                    for key, value in item.items():
                        total_fields += 1
                        if is_empty_value(value):
                            empty_fields.append(key)
                        else:
                            non_empty_fields.append(key)
        
        return {
            'file_path': str(file_path),
            'total_fields': total_fields,
            'empty_fields': empty_fields,
            'non_empty_fields': non_empty_fields,
            'empty_count': len(empty_fields),
            'completeness_percentage': ((total_fields - len(empty_fields)) / total_fields * 100) if total_fields > 0 else 0
        }
    
    except Exception as e:
        return {
            'file_path': str(file_path),
            'error': str(e),
            'total_fields': 0,
            'empty_fields': [],
            'non_empty_fields': [],
            'empty_count': 0,
            'completeness_percentage': 0
        }

def analyze_all_json_files():
    """
    Analyze all JSON files in the llmExtracted_json folder.
    
    Returns:
        list: List of analysis results for all files
    """
    json_folder = Path("llmExtracted_json")
    if not json_folder.exists():
        print(f"❌ JSON folder '{json_folder}' not found!")
        return []
    
    results = []
    total_files = 0
    
    print("🔍 Scanning JSON files...")
    
    # Walk through all CAO number folders
    for cao_folder in json_folder.iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            cao_number = cao_folder.name
            print(f"📁 Processing CAO {cao_number}...")
            
            for json_file in cao_folder.glob("*.json"):
                total_files += 1
                result = analyze_json_file(json_file)
                result['cao_number'] = cao_number
                result['file_name'] = json_file.name
                results.append(result)
                
                if total_files % 100 == 0:
                    print(f"  Processed {total_files} files...")
    
    print(f"✅ Completed analysis of {total_files} files")
    return results

def generate_statistics(results):
    """
    Generate statistics from the analysis results.
    
    Args:
        results: List of analysis results
    
    Returns:
        dict: Statistics about empty fields
    """
    stats = {
        'total_files': len(results),
        'files_with_errors': len([r for r in results if 'error' in r]),
        'files_with_empty_fields': len([r for r in results if r['empty_count'] > 0]),
        'total_empty_fields': sum(r['empty_count'] for r in results),
        'average_completeness': sum(r['completeness_percentage'] for r in results) / len(results) if results else 0,
        'field_empty_counts': defaultdict(int)
    }
    
    # Count empty fields by field name
    for result in results:
        if 'error' not in result:
            for field in result['empty_fields']:
                stats['field_empty_counts'][field] += 1
    
    return stats

def save_analysis_report(results, stats):
    """
    Save the analysis report to a file.
    
    Args:
        results: List of analysis results
        stats: Statistics dictionary
    """
    output_file = "empty_files_analysis.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("EMPTY JSON FILES ANALYSIS REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        # Overall statistics
        f.write("OVERALL STATISTICS:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Total files analyzed: {stats['total_files']}\n")
        f.write(f"Files with errors: {stats['files_with_errors']}\n")
        f.write(f"Files with empty fields: {stats['files_with_empty_fields']}\n")
        f.write(f"Total empty fields: {stats['total_empty_fields']}\n")
        f.write(f"Average completeness: {stats['average_completeness']:.1f}%\n\n")
        
        # Most common empty fields
        f.write("MOST COMMON EMPTY FIELDS:\n")
        f.write("-" * 20 + "\n")
        sorted_fields = sorted(stats['field_empty_counts'].items(), key=lambda x: x[1], reverse=True)
        for field, count in sorted_fields[:20]:  # Top 20
            f.write(f"{field}: {count} files\n")
        f.write("\n")
        
        # Files with most empty fields
        f.write("Files with most empty fields (sorted by empty count):\n")
        f.write("-" * 50 + "\n")
        sorted_results = sorted(results, key=lambda x: x['empty_count'], reverse=True)
        
        for i, result in enumerate(sorted_results):
            if result['empty_count'] > 0:
                f.write(f"  {i+1}. {result['file_path']}\n")
                f.write(f"     Empty fields ({result['empty_count']}): {', '.join(result['empty_fields'])}\n")
                f.write(f"     Non-empty fields ({len(result['non_empty_fields'])}): {', '.join(result['non_empty_fields'])}\n")
                f.write("\n")
        
        # Files with errors
        if stats['files_with_errors'] > 0:
            f.write("FILES WITH ERRORS:\n")
            f.write("-" * 20 + "\n")
            for result in results:
                if 'error' in result:
                    f.write(f"{result['file_path']} - Error: {result['error']}\n")
    
    print(f"💾 Analysis report saved to: {output_file}")

def main():
    """Main function to run the analysis."""
    print("🔍 Starting JSON files empty field analysis...")
    
    # Analyze all JSON files
    results = analyze_all_json_files()
    
    if not results:
        print("❌ No JSON files found to analyze")
        return
    
    # Generate statistics
    stats = generate_statistics(results)
    
    # Save report
    save_analysis_report(results, stats)
    
    # Print summary
    print(f"\n📊 ANALYSIS SUMMARY:")
    print(f"  Total files: {stats['total_files']}")
    print(f"  Files with empty fields: {stats['files_with_empty_fields']}")
    print(f"  Total empty fields: {stats['total_empty_fields']}")
    print(f"  Average completeness: {stats['average_completeness']:.1f}%")
    
    if stats['files_with_errors'] > 0:
        print(f"  Files with errors: {stats['files_with_errors']}")
    
    print(f"\n✅ Analysis complete! Check 'empty_files_analysis.txt' for detailed results.")

if __name__ == "__main__":
    main() 