#!/usr/bin/env python3
"""
Analyze LLM analysis quality and identify files with minimal output.

This script analyzes JSON analysis files from p4_analysis.py to identify:
- Files with empty sections in salary and non-salary analysis
- Connection between salary and non-salary folders (missing files)
- Files with very small content
- Files that might need re-analysis

Usage:
    python scripts/analyze_llm_analysis_quality.py
"""

import json
import os
from pathlib import Path
import sys
from collections import defaultdict

def analyze_salary_file(file_path: Path) -> dict:
    """
    Analyze a salary JSON analysis file for quality and completeness.
    
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
            'salary_entries': 0,
            'has_salary_info': False,
            'has_jobgroups': False
        }
        
        # Analyze salary_information section
        if 'salary_information' in data:
            salary_data = data['salary_information']
            if isinstance(salary_data, list):
                analysis['salary_entries'] = len(salary_data)
                content_length = 0
                jobgroups = set()
                
                for entry in salary_data:
                    if isinstance(entry, dict):
                        entry_content = json.dumps(entry, ensure_ascii=False)
                        content_length += len(entry_content)
                        
                        # Check for jobgroup
                        if 'jobgroup' in entry and entry['jobgroup']:
                            jobgroups.add(entry['jobgroup'])
                
                analysis['sections']['salary_information'] = {
                    'content_length': content_length,
                    'item_count': len(salary_data),
                    'is_empty': content_length == 0 or len(salary_data) == 0,
                    'jobgroups': list(jobgroups)
                }
                
                analysis['total_content_length'] = content_length
                analysis['has_salary_info'] = content_length > 0
                analysis['has_jobgroups'] = len(jobgroups) > 0
                
                if analysis['sections']['salary_information']['is_empty']:
                    analysis['empty_sections'].append('salary_information')
            else:
                analysis['sections']['salary_information'] = {
                    'content_length': 0,
                    'item_count': 0,
                    'is_empty': True,
                    'jobgroups': []
                }
                analysis['empty_sections'].append('salary_information')
        else:
            analysis['sections']['salary_information'] = {
                'content_length': 0,
                'item_count': 0,
                'is_empty': True,
                'jobgroups': []
            }
            analysis['empty_sections'].append('salary_information')
        
        return analysis
        
    except Exception as e:
        return {
            'file': file_path.name,
            'error': str(e),
            'file_size': file_path.stat().st_size if file_path.exists() else 0
        }

def analyze_non_salary_file(file_path: Path) -> dict:
    """
    Analyze a non-salary JSON analysis file for quality and completeness.
    
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
            'has_contract_info': False,
            'has_pension_info': False,
            'has_leave_info': False,
            'has_termination_info': False,
            'has_overtime_info': False,
            'has_training_info': False,
            'has_homeoffice_info': False
        }
        
        # Analyze each section
        expected_sections = [
            'contract_information', 'pension_information', 'leave_information',
            'termination_information', 'overtime_information', 'training_information',
            'homeoffice_information'
        ]
        
        for section in expected_sections:
            if section in data:
                section_data = data[section]
                if isinstance(section_data, dict):
                    content_length = len(json.dumps(section_data, ensure_ascii=False))
                    item_count = len(section_data)
                elif isinstance(section_data, list):
                    content_length = sum(len(json.dumps(item, ensure_ascii=False)) for item in section_data)
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
                if section == 'contract_information' and content_length > 0:
                    analysis['has_contract_info'] = True
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

def analyze_llm_analysis_quality(base_dir: str = "outputs/llm_analysis"):
    """
    Analyze all JSON analysis files for quality and completeness.
    """
    base_path = Path(base_dir)
    
    if not base_path.exists():
        print(f"❌ Analysis directory not found: {base_dir}")
        return
    
    print(f"🔍 Analyzing LLM analysis quality in: {base_dir}")
    print("=" * 80)
    
    salary_path = base_path / "salary"
    non_salary_path = base_path / "non_salary"
    
    if not salary_path.exists() or not non_salary_path.exists():
        print(f"❌ Salary or non-salary directory not found")
        return
    
    # Get all CAO numbers from both directories
    salary_caos = {d.name for d in salary_path.iterdir() if d.is_dir()}
    non_salary_caos = {d.name for d in non_salary_path.iterdir() if d.is_dir()}
    
    all_caos = salary_caos.union(non_salary_caos)
    missing_salary_caos = non_salary_caos - salary_caos
    missing_non_salary_caos = salary_caos - non_salary_caos
    
    print(f"📊 CAO DIRECTORY ANALYSIS:")
    print(f"   Total unique CAO numbers: {len(all_caos)}")
    print(f"   CAO numbers with salary analysis: {len(salary_caos)}")
    print(f"   CAO numbers with non-salary analysis: {len(non_salary_caos)}")
    print(f"   CAO numbers missing salary analysis: {len(missing_salary_caos)}")
    print(f"   CAO numbers missing non-salary analysis: {len(missing_non_salary_caos)}")
    
    print(f"   All CAO numbers: {', '.join(sorted(all_caos))}")
    
    if missing_salary_caos:
        print(f"   Missing salary CAO numbers: {', '.join(sorted(missing_salary_caos))}")
    if missing_non_salary_caos:
        print(f"   Missing non-salary CAO numbers: {', '.join(sorted(missing_non_salary_caos))}")
    print()
    
    # Analyze files in each CAO directory
    all_salary_analyses = []
    all_non_salary_analyses = []
    total_salary_files = 0
    total_non_salary_files = 0
    salary_files_with_errors = 0
    non_salary_files_with_errors = 0
    
    # Analyze salary files
    print(f"🔍 Analyzing salary files...")
    for cao_dir in salary_path.iterdir():
        if cao_dir.is_dir():
            for json_file in cao_dir.glob("*.json"):
                total_salary_files += 1
                analysis = analyze_salary_file(json_file)
                analysis['cao_number'] = cao_dir.name
                all_salary_analyses.append(analysis)
                
                if 'error' in analysis:
                    salary_files_with_errors += 1
    
    # Analyze non-salary files
    print(f"🔍 Analyzing non-salary files...")
    for cao_dir in non_salary_path.iterdir():
        if cao_dir.is_dir():
            for json_file in cao_dir.glob("*.json"):
                total_non_salary_files += 1
                analysis = analyze_non_salary_file(json_file)
                analysis['cao_number'] = cao_dir.name
                all_non_salary_analyses.append(analysis)
                
                if 'error' in analysis:
                    non_salary_files_with_errors += 1
    
    # File connection analysis
    print(f"📋 PDF FILE CONNECTION ANALYSIS:")
    print("-" * 80)
    
    # Get all file names (without _salary.json or _non_salary.json suffix)
    salary_files = {Path(f['file']).stem.replace('_salary', '') for f in all_salary_analyses if 'error' not in f}
    non_salary_files = {Path(f['file']).stem.replace('_non_salary', '') for f in all_non_salary_analyses if 'error' not in f}
    
    files_in_both = salary_files.intersection(non_salary_files)
    files_only_salary = salary_files - non_salary_files
    files_only_non_salary = non_salary_files - salary_files
    
    print(f"   PDF files processed: {len(files_in_both)}")
    print(f"   PDF files with both salary and non-salary analysis: {len(files_in_both)}")
    print(f"   PDF files with only salary analysis: {len(files_only_salary)}")
    print(f"   PDF files with only non-salary analysis: {len(files_only_non_salary)}")
    print(f"   Total JSON files created: {len(files_in_both) * 2} (salary + non-salary)")
    
    if files_only_salary:
        print(f"   PDF files missing non-salary analysis: {', '.join(sorted(list(files_only_salary)[:10]))}{'...' if len(files_only_salary) > 10 else ''}")
    if files_only_non_salary:
        print(f"   PDF files missing salary analysis: {', '.join(sorted(list(files_only_non_salary)[:10]))}{'...' if len(files_only_non_salary) > 10 else ''}")
    print()
    
    # Summary statistics
    print(f"📊 SUMMARY STATISTICS:")
    print(f"   Total PDF files processed: {len(files_in_both)}")
    print(f"   Total salary JSON files analyzed: {total_salary_files}")
    print(f"   Total non-salary JSON files analyzed: {total_non_salary_files}")
    print(f"   Total JSON files created: {total_salary_files + total_non_salary_files}")
    print(f"   Salary JSON files with errors: {salary_files_with_errors}")
    print(f"   Non-salary JSON files with errors: {non_salary_files_with_errors}")
    
    if all_salary_analyses:
        avg_salary_content = sum(a.get('total_content_length', 0) for a in all_salary_analyses if 'error' not in a) / max(1, total_salary_files - salary_files_with_errors)
        print(f"   Average salary content length: {avg_salary_content:.0f} chars")
    
    if all_non_salary_analyses:
        avg_non_salary_content = sum(a.get('total_content_length', 0) for a in all_non_salary_analyses if 'error' not in a) / max(1, total_non_salary_files - non_salary_files_with_errors)
        print(f"   Average non-salary content length: {avg_non_salary_content:.0f} chars")
    print()
    
    # Files with minimal output (salary)
    if all_salary_analyses:
        salary_analyses_sorted = sorted([a for a in all_salary_analyses if 'error' not in a], key=lambda x: x.get('total_content_length', 0))
        
        print(f"🔴 SALARY FILES WITH MINIMAL OUTPUT (Bottom 10):")
        print("-" * 80)
        for i, analysis in enumerate(salary_analyses_sorted[:10]):
            empty_count = len(analysis['empty_sections'])
            print(f"{i+1:2d}. {analysis['file']} (CAO {analysis['cao_number']})")
            print(f"     Content: {analysis['total_content_length']} chars, Salary entries: {analysis['salary_entries']}")
            print(f"     Empty sections: {empty_count}/1, Has jobgroups: {analysis['has_jobgroups']}")
        print()
        
        # Files with good output (salary)
        print(f"🟢 SALARY FILES WITH GOOD OUTPUT (Top 10):")
        print("-" * 80)
        for i, analysis in enumerate(salary_analyses_sorted[-10:]):
            empty_count = len(analysis['empty_sections'])
            print(f"{i+1:2d}. {analysis['file']} (CAO {analysis['cao_number']})")
            print(f"     Content: {analysis['total_content_length']} chars, Salary entries: {analysis['salary_entries']}")
            print(f"     Empty sections: {empty_count}/1, Has jobgroups: {analysis['has_jobgroups']}")
        print()
    
    # Files with minimal output (non-salary)
    if all_non_salary_analyses:
        non_salary_analyses_sorted = sorted([a for a in all_non_salary_analyses if 'error' not in a], key=lambda x: x.get('total_content_length', 0))
        
        print(f"🔴 NON-SALARY FILES WITH MINIMAL OUTPUT (Bottom 10):")
        print("-" * 80)
        for i, analysis in enumerate(non_salary_analyses_sorted[:10]):
            empty_count = len(analysis['empty_sections'])
            print(f"{i+1:2d}. {analysis['file']} (CAO {analysis['cao_number']})")
            print(f"     Content: {analysis['total_content_length']} chars, Empty sections: {empty_count}/7")
            print(f"     Missing: {', '.join(analysis['empty_sections'][:3])}{'...' if len(analysis['empty_sections']) > 3 else ''}")
        print()
        
        # Files with good output (non-salary)
        print(f"🟢 NON-SALARY FILES WITH GOOD OUTPUT (Top 10):")
        print("-" * 80)
        for i, analysis in enumerate(non_salary_analyses_sorted[-10:]):
            empty_count = len(analysis['empty_sections'])
            print(f"{i+1:2d}. {analysis['file']} (CAO {analysis['cao_number']})")
            print(f"     Content: {analysis['total_content_length']} chars, Empty sections: {empty_count}/7")
            if analysis['has_contract_info']:
                print(f"     ✅ Has contract info")
            if analysis['has_pension_info']:
                print(f"     ✅ Has pension info")
        print()
    
    # Section analysis (non-salary)
    if all_non_salary_analyses:
        print(f"📋 NON-SALARY SECTION ANALYSIS:")
        print("-" * 80)
        section_stats = defaultdict(lambda: {'present': 0, 'empty': 0, 'missing': 0})
        
        for analysis in all_non_salary_analyses:
            if 'error' not in analysis:
                for section_name, section_data in analysis['sections'].items():
                    if section_data['is_empty']:
                        if section_name in analysis['empty_sections']:
                            section_stats[section_name]['empty'] += 1
                        else:
                            section_stats[section_name]['missing'] += 1
                    else:
                        section_stats[section_name]['present'] += 1
        
        for section in ['contract_information', 'pension_information', 'leave_information',
                       'termination_information', 'overtime_information', 'training_information',
                       'homeoffice_information']:
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
    
    # Count files that need re-analysis
    salary_files_needing_reanalysis = []
    non_salary_files_needing_reanalysis = []
    
    for analysis in all_salary_analyses:
        if 'error' not in analysis:
            reanalysis_reason = None
            
            # Check if salary information is empty
            if not analysis['has_salary_info']:
                reanalysis_reason = "No salary information"
            
            # Check if no jobgroups found
            elif not analysis['has_jobgroups']:
                reanalysis_reason = "No jobgroups found"
            
            # Check if very low content (less than 1000 chars)
            elif analysis['total_content_length'] < 1000:
                reanalysis_reason = f"Very low content: {analysis['total_content_length']} chars"
            
            # Check if very few salary entries (1 or 2 for large CAOs)
            elif analysis['salary_entries'] <= 2 and analysis['total_content_length'] < 5000:
                reanalysis_reason = f"Very few salary entries: {analysis['salary_entries']} entries"
            
            if reanalysis_reason:
                salary_files_needing_reanalysis.append({
                    'file': analysis['file'],
                    'cao_number': analysis['cao_number'],
                    'reason': reanalysis_reason,
                    'salary_entries': analysis['salary_entries'],
                    'content_length': analysis['total_content_length']
                })
    
    for analysis in all_non_salary_analyses:
        if 'error' not in analysis:
            reanalysis_reason = None
            
            # Check if contract information is empty
            if not analysis['has_contract_info']:
                reanalysis_reason = "No contract information"
            
            # Check if pension information is empty (critical section)
            elif not analysis['has_pension_info']:
                reanalysis_reason = "No pension information"
            
            # Check if more than 3 sections are empty
            elif len(analysis['empty_sections']) > 3:
                reanalysis_reason = f"Too many empty sections: {len(analysis['empty_sections'])} empty"
            
            # Check if very low content (less than 2000 chars)
            elif analysis['total_content_length'] < 2000:
                reanalysis_reason = f"Very low content: {analysis['total_content_length']} chars"
            
            # Check if critical sections are empty (contract + pension)
            elif not analysis['has_contract_info'] and not analysis['has_pension_info']:
                reanalysis_reason = "Missing critical sections: contract and pension"
            
            if reanalysis_reason:
                non_salary_files_needing_reanalysis.append({
                    'file': analysis['file'],
                    'cao_number': analysis['cao_number'],
                    'reason': reanalysis_reason,
                    'empty_sections': analysis['empty_sections'],
                    'content_length': analysis['total_content_length']
                })
    
    print(f"   Salary JSON files that may need re-analysis: {len(salary_files_needing_reanalysis)}")
    print(f"   Non-salary JSON files that may need re-analysis: {len(non_salary_files_needing_reanalysis)}")
    print(f"   PDF files missing counterpart analysis: {len(files_only_salary) + len(files_only_non_salary)}")
    
    if len(salary_files_needing_reanalysis) > 0 or len(non_salary_files_needing_reanalysis) > 0:
        print(f"\n   📋 DETAILED RE-ANALYSIS RECOMMENDATIONS:")
        print(f"   {'='*60}")
        
        if salary_files_needing_reanalysis:
            print(f"\n   🔴 SALARY FILES NEEDING RE-ANALYSIS:")
            for i, file_info in enumerate(salary_files_needing_reanalysis, 1):
                print(f"   {i:2d}. {file_info['file']} (CAO {file_info['cao_number']})")
                print(f"       Reason: {file_info['reason']}")
                print(f"       Content: {file_info['content_length']} chars")
                print(f"       Salary entries: {file_info['salary_entries']}")
        
        if non_salary_files_needing_reanalysis:
            print(f"\n   🔴 NON-SALARY FILES NEEDING RE-ANALYSIS:")
            for i, file_info in enumerate(non_salary_files_needing_reanalysis, 1):
                print(f"   {i:2d}. {file_info['file']} (CAO {file_info['cao_number']})")
                print(f"       Reason: {file_info['reason']}")
                print(f"       Content: {file_info['content_length']} chars")
                print(f"       Empty sections: {len(file_info['empty_sections'])}/7")
        
        print(f"\n   💡 Consider re-analyzing these files with an improved prompt.")
    else:
        print(f"   ✅ All files appear to have adequate analysis quality!")
    
    # Save detailed report
    report_file = "llm_analysis_quality_report.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'total_caos': len(all_caos),
                'caos_with_salary': len(salary_caos),
                'caos_with_non_salary': len(non_salary_caos),
                'missing_salary_caos': list(missing_salary_caos),
                'missing_non_salary_caos': list(missing_non_salary_caos),
                'total_salary_files': total_salary_files,
                'total_non_salary_files': total_non_salary_files,
                'salary_files_with_errors': salary_files_with_errors,
                'non_salary_files_with_errors': non_salary_files_with_errors,
                'files_in_both': len(files_in_both),
                'files_only_salary': len(files_only_salary),
                'files_only_non_salary': len(files_only_non_salary),
                'salary_files_needing_reanalysis': len(salary_files_needing_reanalysis),
                'non_salary_files_needing_reanalysis': len(non_salary_files_needing_reanalysis)
            },
            'section_stats': dict(section_stats) if 'section_stats' in locals() else {},
            'salary_files_with_minimal_output': [a['file'] for a in salary_analyses_sorted[:10] if 'error' not in a] if 'salary_analyses_sorted' in locals() else [],
            'non_salary_files_with_minimal_output': [a['file'] for a in non_salary_analyses_sorted[:10] if 'error' not in a] if 'non_salary_analyses_sorted' in locals() else [],
            'salary_reanalysis_recommendations': salary_files_needing_reanalysis,
            'non_salary_reanalysis_recommendations': non_salary_files_needing_reanalysis,
            'detailed_salary_analyses': all_salary_analyses,
            'detailed_non_salary_analyses': all_non_salary_analyses
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Detailed report saved to: {report_file}")

if __name__ == "__main__":
    # Allow custom base directory as command line argument
    base_dir = sys.argv[1] if len(sys.argv) > 1 else "outputs/llm_analysis"
    analyze_llm_analysis_quality(base_dir)
