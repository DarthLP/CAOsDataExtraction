#!/usr/bin/env python3
"""
Analyze extraction quality and identify files with minimal output.

This script analyzes JSON extraction files to identify:
- Files with empty sections (wage_information, pension_information, etc.)
- Files with very small content
- Files that might need re-extraction

Usage:
    python scripts/analyze_extraction_quality.py
"""

import json
import os
from pathlib import Path
import sys
from collections import defaultdict

def analyze_json_file(file_path: Path) -> dict:
    """
    Analyze a JSON extraction file for quality and completeness.
    
    Returns:
        dict: Analysis results including section counts, content length, etc.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        analysis = {
            'file': file_path.name,
            'file_size': file_path.stat().st_size,
            'sections': {},
            'empty_sections': [],
            'total_content_length': 0,
            'has_wage_info': False,
            'has_pension_info': False,
            'has_leave_info': False,
            'has_termination_info': False,
            'has_overtime_info': False,
            'has_training_info': False,
            'has_homeoffice_info': False
        }
        
        # Analyze each section
        expected_sections = [
            'general_information', 'wage_information', 'pension_information',
            'leave_information', 'termination_information', 'overtime_information',
            'training_information', 'homeoffice_information'
        ]
        
        for section in expected_sections:
            if section in data:
                section_data = data[section]
                if isinstance(section_data, list):
                    content_length = sum(len(str(item)) for item in section_data)
                    item_count = len(section_data)
                else:
                    content_length = len(str(section_data))
                    item_count = 1
                
                analysis['sections'][section] = {
                    'content_length': content_length,
                    'item_count': item_count,
                    'is_empty': content_length == 0 or item_count == 0
                }
                
                analysis['total_content_length'] += content_length
                
                # Check for specific important sections
                if section == 'wage_information' and content_length > 0:
                    analysis['has_wage_info'] = True
                elif section == 'pension_information' and content_length > 0:
                    analysis['has_pension_info'] = True
                elif section == 'leave_information' and content_length > 0:
                    analysis['has_leave_info'] = True
                elif section == 'termination_information' and content_length > 0:
                    analysis['has_termination_info'] = True
                elif section == 'overtime_information' and content_length > 0:
                    analysis['has_overtime_info'] = True
                elif section == 'training_information' and content_length > 0:
                    analysis['has_training_info'] = True
                elif section == 'homeoffice_information' and content_length > 0:
                    analysis['has_homeoffice_info'] = True
                
                if analysis['sections'][section]['is_empty']:
                    analysis['empty_sections'].append(section)
            else:
                analysis['sections'][section] = {
                    'content_length': 0,
                    'item_count': 0,
                    'is_empty': True
                }
                analysis['empty_sections'].append(section)
        
        return analysis
        
    except Exception as e:
        return {
            'file': file_path.name,
            'error': str(e),
            'file_size': file_path.stat().st_size if file_path.exists() else 0
        }

def analyze_extraction_quality(output_dir: str = "outputs/llm_extracted/new_flow"):
    """
    Analyze all JSON extraction files for quality and completeness.
    """
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Output directory not found: {output_dir}")
        return
    
    print(f"🔍 Analyzing extraction quality in: {output_dir}")
    print("=" * 80)
    
    all_analyses = []
    total_files = 0
    files_with_errors = 0
    
    # Analyze all JSON files
    for json_file in output_path.rglob("*.json"):
        total_files += 1
        analysis = analyze_json_file(json_file)
        all_analyses.append(analysis)
        
        if 'error' in analysis:
            files_with_errors += 1
    
    # Sort by total content length (ascending) to find files with minimal output
    all_analyses.sort(key=lambda x: x.get('total_content_length', 0))
    
    # Summary statistics
    print(f"📊 SUMMARY STATISTICS:")
    print(f"   Total files analyzed: {total_files}")
    print(f"   Files with errors: {files_with_errors}")
    print(f"   Average content length: {sum(a.get('total_content_length', 0) for a in all_analyses) / max(1, total_files):.0f} chars")
    print()
    
    # Files with minimal output (bottom 10)
    print(f"🔴 FILES WITH MINIMAL OUTPUT (Bottom 10):")
    print("-" * 80)
    for i, analysis in enumerate(all_analyses[:10]):
        if 'error' in analysis:
            print(f"{i+1:2d}. {analysis['file']} - ERROR: {analysis['error']}")
        else:
            empty_count = len(analysis['empty_sections'])
            print(f"{i+1:2d}. {analysis['file']}")
            print(f"     Content: {analysis['total_content_length']} chars, Empty sections: {empty_count}/8")
            print(f"     Missing: {', '.join(analysis['empty_sections'][:3])}{'...' if len(analysis['empty_sections']) > 3 else ''}")
    print()
    
    # Files with good output (top 10)
    print(f"🟢 FILES WITH GOOD OUTPUT (Top 10):")
    print("-" * 80)
    for i, analysis in enumerate(all_analyses[-10:]):
        if 'error' not in analysis:
            empty_count = len(analysis['empty_sections'])
            print(f"{i+1:2d}. {analysis['file']}")
            print(f"     Content: {analysis['total_content_length']} chars, Empty sections: {empty_count}/8")
            if analysis['has_wage_info']:
                print(f"     ✅ Has wage info")
            if analysis['has_pension_info']:
                print(f"     ✅ Has pension info")
    print()
    
    # Section analysis
    print(f"📋 SECTION ANALYSIS:")
    print("-" * 80)
    section_stats = defaultdict(lambda: {'present': 0, 'empty': 0, 'missing': 0})
    
    for analysis in all_analyses:
        if 'error' not in analysis:
            for section_name, section_data in analysis['sections'].items():
                if section_data['is_empty']:
                    if section_name in analysis['empty_sections']:
                        section_stats[section_name]['empty'] += 1
                    else:
                        section_stats[section_name]['missing'] += 1
                else:
                    section_stats[section_name]['present'] += 1
    
    for section in ['general_information', 'wage_information', 'pension_information', 
                   'leave_information', 'termination_information', 'overtime_information',
                   'training_information', 'homeoffice_information']:
        stats = section_stats[section]
        total = stats['present'] + stats['empty'] + stats['missing']
        if total > 0:
            present_pct = (stats['present'] / total) * 100
            empty_pct = (stats['empty'] / total) * 100
            missing_pct = (stats['missing'] / total) * 100
            print(f"{section:25s}: {stats['present']:3d} present ({present_pct:5.1f}%), "
                  f"{stats['empty']:3d} empty ({empty_pct:5.1f}%), "
                  f"{stats['missing']:3d} missing ({missing_pct:5.1f}%)")
    
    print()
    
    # Recommendations
    print(f"💡 RECOMMENDATIONS:")
    print("-" * 80)
    
    # Count files that need re-extraction
    files_needing_reextraction = []
    for analysis in all_analyses:
        if 'error' not in analysis:
            reextraction_reason = None
            
            # Check if wage information is empty
            if not analysis['has_wage_info']:
                reextraction_reason = "No wage information"
            
            # Check if general information is empty
            elif analysis['sections']['general_information']['is_empty']:
                reextraction_reason = "No general information"
            
            # Check if more than 2 sections are empty (including homeoffice)
            else:
                if len(analysis['empty_sections']) > 2:
                    reextraction_reason = f"Too many empty sections: {len(analysis['empty_sections'])} empty (including homeoffice)"
            
            if reextraction_reason:
                files_needing_reextraction.append({
                    'file': analysis['file'],
                    'reason': reextraction_reason,
                    'empty_sections': analysis['empty_sections'],
                    'content_length': analysis['total_content_length']
                })
    
    print(f"   Files that may need re-extraction: {len(files_needing_reextraction)}")
    
    if len(files_needing_reextraction) > 0:
        print(f"\n   📋 DETAILED RE-EXTRACTION RECOMMENDATIONS:")
        print(f"   {'='*60}")
        for i, file_info in enumerate(files_needing_reextraction, 1):
            print(f"   {i:2d}. {file_info['file']}")
            print(f"       Reason: {file_info['reason']}")
            print(f"       Content: {file_info['content_length']} chars")
            print(f"       Empty sections: {len(file_info['empty_sections'])}/8")
            if len(file_info['empty_sections']) > 0:
                empty_list = ', '.join(file_info['empty_sections'][:3])
                if len(file_info['empty_sections']) > 3:
                    empty_list += f" (+{len(file_info['empty_sections']) - 3} more)"
                print(f"       Missing: {empty_list}")
            print()
        
            print(f"   💡 Consider re-extracting these files with an improved prompt.")
    print(f"   The new prompt should include specific Dutch terms and comprehensive extraction guidelines.")
    
    # Option to delete files needing re-extraction
    if len(files_needing_reextraction) > 0:
        print(f"\n🗑️  DELETE FILES NEEDING RE-EXTRACTION:")
        print(f"   {'='*60}")
        print(f"   Would you like to delete the files that need re-extraction?")
        print(f"   This will remove the incomplete JSON files so they can be re-extracted.")
        
        delete_response = input(f"\n❓ Delete {len(files_needing_reextraction)} files needing re-extraction? (y/N): ")
        
        if delete_response.lower() in ['y', 'yes']:
            deleted_count = 0
            print(f"\n🗑️  DELETING FILES:")
            print(f"   {'='*60}")
            
            for file_info in files_needing_reextraction:
                # Find the actual file path
                file_path = None
                for json_file in output_path.rglob("*.json"):
                    if json_file.name == file_info['file']:
                        file_path = json_file
                        break
                
                if file_path and file_path.exists():
                    print(f"\n📄 File: {file_info['file']}")
                    print(f"   Reason: {file_info['reason']}")
                    print(f"   Content: {file_info['content_length']} chars")
                    print(f"   Empty sections: {len(file_info['empty_sections'])}/8")
                    
                    individual_response = input(f"   ❓ Delete this file? (y/N): ")
                    
                    if individual_response.lower() in ['y', 'yes']:
                        try:
                            file_path.unlink()
                            print(f"   ✅ DELETED: {file_info['file']}")
                            deleted_count += 1
                        except Exception as e:
                            print(f"   ❌ Failed to delete {file_info['file']}: {e}")
                    else:
                        print(f"   ⏭️  SKIPPED: {file_info['file']}")
                else:
                    print(f"   ⚠️  File not found: {file_info['file']}")
            
            print(f"\n📊 DELETION SUMMARY:")
            print(f"   {'='*60}")
            print(f"   Files deleted: {deleted_count}")
            print(f"   Files skipped: {len(files_needing_reextraction) - deleted_count}")
            
            if deleted_count > 0:
                print(f"\n💡 Next steps:")
                print(f"   - Run the extraction pipeline again to re-extract the deleted files")
                print(f"   - The improved prompt should produce better results")
        else:
            print(f"   ⏭️  Deletion cancelled")
    else:
        print(f"   ✅ All files appear to have adequate extraction quality!")
    
    # Additional statistics
    print(f"\n   📊 ADDITIONAL STATISTICS:")
    print(f"   {'='*60}")
    
    # Count files by empty section criteria
    files_no_wage = sum(1 for a in all_analyses if 'error' not in a and not a['has_wage_info'])
    files_no_general = sum(1 for a in all_analyses if 'error' not in a and a['sections']['general_information']['is_empty'])
    files_many_empty = sum(1 for a in all_analyses if 'error' not in a and len(a['empty_sections']) > 2)
    
    print(f"   Files with no wage info: {files_no_wage}")
    print(f"   Files with no general info: {files_no_general}")
    print(f"   Files with >2 empty sections (including homeoffice): {files_many_empty}")
    
    # Save detailed report
    report_file = "performance_logs/llm_extraction/extraction_quality_report.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'total_files': total_files,
                'files_with_errors': files_with_errors,
                'files_needing_reextraction': len(files_needing_reextraction),
                'average_content_length': sum(a.get('total_content_length', 0) for a in all_analyses) / max(1, total_files)
            },
            'section_stats': dict(section_stats),
            'files_with_minimal_output': [a['file'] for a in all_analyses[:10] if 'error' not in a],
            'reextraction_recommendations': files_needing_reextraction,
            'detailed_analyses': all_analyses
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Detailed report saved to: {report_file}")

if __name__ == "__main__":
    # Allow custom output directory as command line argument
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "outputs/llm_extracted/new_flow"
    analyze_extraction_quality(output_dir)
