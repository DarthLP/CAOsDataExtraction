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
import re
import yaml

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
        
        # Analyze each section that is actually present in the file
        # Different categories have different sections, so we only analyze what's there
        for section in data.keys():
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
            if section == 'contract_type_information' and content_length > 0:
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
        
        return analysis
        
    except Exception as e:
        return {
            'file': file_path.name,
            'error': str(e),
            'file_size': file_path.stat().st_size if file_path.exists() else 0
        }

def find_corresponding_extracted_file(analysis_filename: str, cao_number: str) -> Path:
    """
    Find the corresponding LLM extracted file for a given analysis file.
    
    Args:
        analysis_filename: Name of the analysis file (e.g., "file_analysis.json")
        cao_number: CAO number as string
        
    Returns:
        Path to the extracted file if found, None otherwise
    """
    # Remove _analysis.json or _salary.json suffix to get base name
    base_name = Path(analysis_filename).stem
    if base_name.endswith('_analysis'):
        base_name = base_name[:-9]  # Remove '_analysis'
    elif base_name.endswith('_salary'):
        base_name = base_name[:-7]  # Remove '_salary'
    
    # Try to load config for the path, otherwise use default
    extracted_base_path = None
    try:
        with open('conf/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            extracted_base_path = Path(config['paths']['outputs_json']) / 'new_flow'
    except:
        # Fallback to hardcoded path
        extracted_base_path = Path('outputs/llm_extracted/new_flow')
    
    # Look for the extracted file
    cao_dir = extracted_base_path / cao_number
    if not cao_dir.exists():
        return None
    
    # Try both naming patterns: {base_name}_extract.json and {base_name}.json
    possible_files = [
        cao_dir / f"{base_name}_extract.json",
        cao_dir / f"{base_name}.json"
    ]
    
    for file_path in possible_files:
        if file_path.exists():
            return file_path
    
    return None

def check_extracted_file_has_salary_tables(extracted_file_path: Path) -> bool:
    """
    Check if an extracted file contains actual salary tables (not just descriptive text).
    
    Args:
        extracted_file_path: Path to the extracted JSON file
        
    Returns:
        True if actual salary tables are detected, False otherwise
    """
    try:
        with open(extracted_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Look for wage_information key
        wage_keys = ['wage_information', 'Wage information', 'wage information', 'WAGE_INFORMATION']
        wage_content = None
        
        for key in wage_keys:
            if key in data:
                wage_content = data[key]
                break
        
        # Also check nested in general_information if present
        if not wage_content:
            general_keys = ['general_information', 'General information', 'general information', 'GENERAL_INFORMATION']
            for key in general_keys:
                if key in data:
                    general_data = data[key]
                    # Check if it's a list of lists containing wage info
                    if isinstance(general_data, list):
                        for item in general_data:
                            if isinstance(item, (list, str)):
                                item_str = ' '.join(item) if isinstance(item, list) else item
                                if any(wk.lower() in item_str.lower() for wk in ['wage', 'salary', 'salaris', 'loon']):
                                    wage_content = general_data
                                    break
                    if wage_content:
                        break
        
        if not wage_content:
            return False
        
        # Check for actual table structures first (pipe-delimited format with "Columns:")
        # This is the format used by the extraction pipeline for actual tables
        has_table_format = False
        if isinstance(wage_content, list):
            for item in wage_content:
                if isinstance(item, list):
                    item_text = " ".join(str(x) for x in item).lower()
                    # Look for table format indicators
                    if "columns:" in item_text:
                        # Found explicit table format
                        has_table_format = True
                        break
                    elif "|" in item_text:
                        # Check if it has multiple pipe-separated values (indicating table rows)
                        pipe_count = item_text.count("|")
                        if pipe_count >= 3:  # At least 3 pipes suggests a table structure
                            has_table_format = True
                            break
                elif isinstance(item, str):
                    item_lower = item.lower()
                    if "columns:" in item_lower:
                        has_table_format = True
                        break
                    elif item_lower.count("|") >= 3:
                        has_table_format = True
                        break
        
        # If we found table format, that's strong evidence - check for salary amounts in entire content
        if has_table_format:
            # Flatten entire content to check for salary amounts
            content_text = ""
            if isinstance(wage_content, list):
                for item in wage_content:
                    if isinstance(item, list):
                        content_text += " ".join(str(x) for x in item) + " "
                    elif isinstance(item, str):
                        content_text += item + " "
                    else:
                        content_text += str(item) + " "
            elif isinstance(wage_content, str):
                content_text = wage_content
            else:
                content_text = str(wage_content)
            
            content_text = content_text.lower()
            
            # Look for salary-like amounts in the content
            # More flexible patterns to catch amounts in various formats
            salary_amount_patterns = [
                r'\b[€]\s*\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{2})?\b',  # €2,500.00
                r'\b\d{4,6}(?:[.,]\d{2})?\s*(?:€|eur|euro)\b',  # 2500.00 €
                r'\b\d{4,6}(?:[.,]\d{2})?\s*(?:per\s+maand|maandelijks|monthly)\b',  # 2500.00 per maand
                r'\b\d{4,6}(?:[.,]\d{2})?\b',  # Just 4-6 digit numbers (in table context)
            ]
            
            found_amounts = []
            for pattern in salary_amount_patterns:
                matches = re.findall(pattern, content_text, re.IGNORECASE)
                for match in matches:
                    # Extract just the number part
                    num_part = match.split()[0] if ' ' in match else match
                    num_str = re.sub(r'[€$,\s]', '', num_part)
                    
                    # Handle decimal separators
                    if '.' in num_str and ',' in num_str:
                        # Has both - assume last is decimal
                        parts = num_str.split(',')
                        if len(parts[-1]) <= 2:
                            num_str = num_str.replace('.', '').replace(',', '.')
                        else:
                            num_str = num_str.replace(',', '')
                    elif ',' in num_str:
                        parts = num_str.split(',')
                        if len(parts[-1]) <= 2:
                            num_str = num_str.replace(',', '.')
                        else:
                            num_str = num_str.replace(',', '')
                    else:
                        # No decimal or dot as thousands separator
                        num_str = num_str.replace('.', '')
                    
                    try:
                        num_value = float(num_str)
                        if 2000 <= num_value <= 100000:
                            found_amounts.append(num_value)
                    except:
                        pass
            
            # If we found table format with at least one salary amount, return True
            if len(found_amounts) > 0:
                return True
        
        # If no table format found, use stricter detection for unstructured content
        # Flatten the content to a single string for analysis
        content_text = ""
        if isinstance(wage_content, list):
            for item in wage_content:
                if isinstance(item, list):
                    content_text += " ".join(str(x) for x in item) + " "
                elif isinstance(item, str):
                    content_text += item + " "
                else:
                    content_text += str(item) + " "
        elif isinstance(wage_content, str):
            content_text = wage_content
        else:
            content_text = str(wage_content)
        
        content_text = content_text.lower()
        
        # Check for indicators of actual salary tables
        # We need STRONG evidence of actual tabular data, not just mentions
        
        # 1. Find numeric salary amounts (2000-100000 range)
        # Exclude percentages, dates, and small amounts that are likely not salaries
        salary_amount_patterns = [
            r'\b[€]\s*\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{2})?\b',  # Currency with thousands separators: €2,500.00
            r'\b\d{4,6}(?:[.,]\d{2})?\s*(?:€|eur|euro|per\s+maand|per\s+maand|maandelijks)\b',  # 4-6 digits with currency/unit
        ]
        
        salary_amounts = []
        for pattern in salary_amount_patterns:
            matches = re.findall(pattern, content_text, re.IGNORECASE)
            for match in matches:
                # Extract numeric value, excluding currency symbols and units
                num_str = re.sub(r'[€$,\s]', '', match.split()[0] if ' ' in match else match)
                # Handle both . and , as decimal separators
                if '.' in num_str and ',' in num_str:
                    # Has both - assume last is decimal
                    num_str = num_str.replace('.', '').replace(',', '.')
                elif ',' in num_str and len(num_str.split(',')[-1]) <= 2:
                    # Comma as decimal separator
                    num_str = num_str.replace(',', '.')
                else:
                    # No decimal or dot as thousands separator
                    num_str = num_str.replace('.', '').replace(',', '')
                
                try:
                    num_value = float(num_str)
                    # Strict range: 2000-100000 (exclude percentages, small amounts, dates)
                    if 2000 <= num_value <= 100000:
                        salary_amounts.append(num_value)
                except:
                    pass
        
        # Need at least 3 distinct salary amounts to indicate a table (not just one or two mentions)
        has_salary_amounts = len(set(salary_amounts)) >= 3
        
        # 2. Job groups/scales references - be more specific
        # Look for explicit scale mentions with identifiers, not just any letter-number combo
        scale_patterns = [
            r'\b(schaal|niveau|functiegroep)\s*[a-z]\s*[-]?\s*\d+\b',  # "schaal A-1", "niveau B2"
            r'\b(schaal|niveau)\s*\d+\s*(?:[-]|en|tot)\s*\d+\b',  # "schaal 1-10", "niveau 5 tot 8"
            r'\bfunctiegroep\s*[ivxlcdm]+\b',  # Roman numerals: "functiegroep VI"
            r'\b(schaal|niveau)\s*[a-z]\b',  # "schaal A", "niveau B" (standalone)
        ]
        
        # Count scale references - need multiple to indicate a table
        scale_matches = []
        for pattern in scale_patterns:
            matches = re.findall(pattern, content_text, re.IGNORECASE)
            scale_matches.extend(matches)
        has_scales = len(scale_matches) >= 2  # Need at least 2 different scale references
        
        # 3. Steps/periodieken references - be more specific
        step_patterns = [
            r'\b(trede|periodiek)\s*\d+\b',  # "trede 1", "periodiek 3"
            r'\b(trede|periodiek)\s*[a-z]\s*\d+\b',  # "trede A-1"
            r'\baanloopschaal\s*[a-z]?\b',  # "aanloopschaal A"
        ]
        
        # Count step references
        step_matches = []
        for pattern in step_patterns:
            matches = re.findall(pattern, content_text, re.IGNORECASE)
            step_matches.extend(matches)
        has_steps = len(step_matches) >= 2  # Need at least 2 different step references
        
        # 4. Look for table-like structures: multiple amounts in structured context
        # Check if amounts appear near scale/step references (indicating tabular data)
        has_table_structure = False
        if len(salary_amounts) >= 3:
            # Check if amounts appear in proximity to scale/step mentions
            # This suggests they're in a table format, not just descriptive text
            amount_positions = []
            for amount in set(salary_amounts)[:10]:  # Check first 10 unique amounts
                # Find position of amount in text
                amount_str = str(int(amount))
                pos = content_text.find(amount_str)
                if pos != -1:
                    amount_positions.append(pos)
            
            # If we have multiple amounts and scale/step references, likely a table
            if len(amount_positions) >= 3 and (has_scales or has_steps):
                has_table_structure = True
        
        # Return True ONLY if we have STRONG evidence of actual salary tables:
        # - Multiple salary amounts (at least 3) AND
        # - Multiple scale references (at least 2) OR multiple step references (at least 2) OR structured table format
        if has_salary_amounts and (has_scales or has_steps or has_table_structure):
            return True
        
        # Very strict alternative: both scales AND steps with at least some amounts
        if has_scales and has_steps and len(salary_amounts) >= 2:
            return True
        
        # If we found table format earlier but no amounts, return False (might be a non-salary table)
        if has_table_format:
            return False
        
        return False
        
    except FileNotFoundError:
        return False
    except json.JSONDecodeError:
        return False
    except Exception as e:
        # On any error, assume no tables (conservative approach)
        return False

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
    # Salary: direct CAO directories
    salary_caos = {d.name for d in salary_path.iterdir() if d.is_dir()}
    # Non-salary: CAO directories are nested under category subdirectories
    non_salary_caos = set()
    for category_dir in non_salary_path.iterdir():
        if category_dir.is_dir():
            for cao_dir in category_dir.iterdir():
                if cao_dir.is_dir():
                    non_salary_caos.add(cao_dir.name)
    
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
    # Non-salary files are nested under category subdirectories
    # Categories: gen_bon_wag_pen_ter, lea_ove_tra, hom_con_saf_chi_ai_fri
    print(f"🔍 Analyzing non-salary files...")
    for category_dir in non_salary_path.iterdir():
        if category_dir.is_dir():
            category_name = category_dir.name
            for cao_dir in category_dir.iterdir():
                if cao_dir.is_dir():
                    for json_file in cao_dir.glob("*.json"):
                        total_non_salary_files += 1
                        analysis = analyze_non_salary_file(json_file)
                        analysis['cao_number'] = cao_dir.name
                        analysis['category'] = category_name  # Track which category this file belongs to
                        all_non_salary_analyses.append(analysis)
                        
                        if 'error' in analysis:
                            non_salary_files_with_errors += 1
    
    # File connection analysis
    print(f"📋 PDF FILE CONNECTION ANALYSIS:")
    print("-" * 80)
    
    # Get all file names (both end with _analysis.json, so remove _analysis suffix)
    salary_files = {Path(f['file']).stem.replace('_analysis', '') for f in all_salary_analyses if 'error' not in f}
    non_salary_files = {Path(f['file']).stem.replace('_analysis', '') for f in all_non_salary_analyses if 'error' not in f}
    
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
    
    # Additional informational counts (no re-analysis triggers)
    salary_no_jobgroups_count = sum(1 for a in all_salary_analyses if 'error' not in a and not a.get('has_jobgroups', False))
    salary_low_content_count = sum(1 for a in all_salary_analyses if 'error' not in a and a.get('total_content_length', 0) < 1000)
    non_salary_low_content_count = sum(1 for a in all_non_salary_analyses if 'error' not in a and a.get('total_content_length', 0) < 2000)
    print(f"   Salary files with no jobgroups: {salary_no_jobgroups_count}")
    print(f"   Salary files with very low content (<1000 chars): {salary_low_content_count}")
    print(f"   Non-salary files with very low content (<2000 chars): {non_salary_low_content_count}")
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
            present_count = len(analysis.get('sections', {}))
            category = analysis.get('category', 'unknown')
            print(f"{i+1:2d}. {analysis['file']} (CAO {analysis['cao_number']}, category: {category})")
            print(f"     Content: {analysis['total_content_length']} chars, Empty sections: {empty_count}/{present_count} present")
            print(f"     Missing: {', '.join(analysis['empty_sections'][:3])}{'...' if len(analysis['empty_sections']) > 3 else ''}")
        print()
        
        # Files with good output (non-salary)
        print(f"🟢 NON-SALARY FILES WITH GOOD OUTPUT (Top 10):")
        print("-" * 80)
        for i, analysis in enumerate(non_salary_analyses_sorted[-10:]):
            empty_count = len(analysis['empty_sections'])
            present_count = len(analysis.get('sections', {}))
            category = analysis.get('category', 'unknown')
            print(f"{i+1:2d}. {analysis['file']} (CAO {analysis['cao_number']}, category: {category})")
            print(f"     Content: {analysis['total_content_length']} chars, Empty sections: {empty_count}/{present_count} present")
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
                # Check if the corresponding extracted file has actual salary tables
                extracted_file = find_corresponding_extracted_file(analysis['file'], analysis['cao_number'])
                if extracted_file:
                    has_tables = check_extracted_file_has_salary_tables(extracted_file)
                    if has_tables:
                        # Extracted file has tables, so analysis should have found them
                        reanalysis_reason = "No salary information (but extracted file has salary tables)"
                    # If extracted file has no tables, don't recommend re-analysis
                else:
                    # Can't find extracted file, recommend re-analysis to be safe
                    reanalysis_reason = "No salary information (extracted file not found)"
            
            if reanalysis_reason:
                salary_files_needing_reanalysis.append({
                    'file': analysis['file'],
                    'cao_number': analysis['cao_number'],
                    'reason': reanalysis_reason,
                    'salary_entries': analysis['salary_entries'],
                    'content_length': analysis['total_content_length']
                })
    
    # Define expected sections for each category
    category_expected_sections = {
        'gen_bon_wag_pen_ter': ['general_information', 'bonuses_info', 'wage_scales_info', 'pension_information', 'termination_information'],
        'lea_ove_tra': ['leave_information', 'overtime_information', 'training_information'],
        'hom_con_saf_chi_ai_fri': ['homeoffice_information', 'contract_type_information', 'safety_information', 'childcare_information', 'ai_information', 'fringe_benefits_information']
    }
    
    for analysis in all_non_salary_analyses:
        if 'error' not in analysis:
            reanalysis_reason = None
            category = analysis.get('category', 'unknown')
            
            # Get expected sections for this category
            expected_sections = category_expected_sections.get(category, [])
            
            # Get sections that are actually present in the file
            present_sections = list(analysis.get('sections', {}).keys())
            
            # Check if file has very low content (likely empty or minimal)
            if analysis.get('total_content_length', 0) < 500:
                reanalysis_reason = f"Very low content: {analysis.get('total_content_length', 0)} chars"
            
            # Check if file has no sections at all (shouldn't happen)
            elif not present_sections:
                reanalysis_reason = "No sections found in file"
            
            # Check if all present sections are empty
            elif len(analysis.get('empty_sections', [])) == len(present_sections):
                reanalysis_reason = f"All present sections are empty ({len(present_sections)} sections)"
            
            # For expected sections: check if most are missing or empty
            elif expected_sections:
                # Count how many expected sections are present
                present_expected = [s for s in expected_sections if s in present_sections]
                empty_expected = [s for s in expected_sections if s in analysis.get('empty_sections', [])]
                
                # If less than 30% of expected sections are present, flag for re-analysis
                if len(present_expected) < len(expected_sections) * 0.3:
                    reanalysis_reason = f"Too few expected sections present ({len(present_expected)}/{len(expected_sections)}) for category {category}"
                # If more than 70% of expected sections are empty, flag for re-analysis
                elif len(empty_expected) > len(expected_sections) * 0.7:
                    reanalysis_reason = f"Most expected sections empty ({len(empty_expected)}/{len(expected_sections)}) for category {category}"
            
            if reanalysis_reason:
                non_salary_files_needing_reanalysis.append({
                    'file': analysis['file'],
                    'cao_number': analysis['cao_number'],
                    'category': category,
                    'reason': reanalysis_reason,
                    'empty_sections': analysis['empty_sections'],
                    'present_sections': present_sections,
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
                category = file_info.get('category', 'unknown')
                expected_count = len(category_expected_sections.get(category, []))
                present_count = len(file_info.get('present_sections', []))
                print(f"   {i:2d}. {file_info['file']} (CAO {file_info['cao_number']}, category: {category})")
                print(f"       Reason: {file_info['reason']}")
                print(f"       Content: {file_info['content_length']} chars")
                print(f"       Empty sections: {len(file_info['empty_sections'])}/{present_count} present")
        
        print(f"\n   💡 Consider re-analyzing these files with an improved prompt.")
    
    # Option to delete files needing re-analysis
    if len(salary_files_needing_reanalysis) > 0 or len(non_salary_files_needing_reanalysis) > 0:
        print(f"\n🗑️  DELETE FILES NEEDING RE-ANALYSIS:")
        print(f"   {'='*60}")
        print(f"   Would you like to delete the files that need re-analysis?")
        print(f"   This will remove the incomplete analysis files so they can be re-analyzed.")
        
        total_files_to_delete = len(salary_files_needing_reanalysis) + len(non_salary_files_needing_reanalysis)
        delete_response = input(f"\n❓ Delete {total_files_to_delete} files needing re-analysis? (y/N): ")
        
        if delete_response.lower() in ['y', 'yes']:
            deleted_count = 0
            print(f"\n🗑️  DELETING FILES:")
            print(f"   {'='*60}")
            
            # Delete salary files
            if salary_files_needing_reanalysis:
                print(f"\n🔴 SALARY FILES:")
                for file_info in salary_files_needing_reanalysis:
                    # Find the actual file path
                    file_path = None
                    cao_salary_dir = salary_path / file_info['cao_number']
                    if cao_salary_dir.exists():
                        for json_file in cao_salary_dir.glob("*.json"):
                            if json_file.name == file_info['file']:
                                file_path = json_file
                                break
                    
                    if file_path and file_path.exists():
                        print(f"\n📄 File: {file_info['file']} (CAO {file_info['cao_number']})")
                        print(f"   Reason: {file_info['reason']}")
                        print(f"   Content: {file_info['content_length']} chars")
                        print(f"   Salary entries: {file_info['salary_entries']}")
                        
                        individual_response = input(f"   ❓ Delete this salary file? (y/N): ")
                        
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
            
            # Delete non-salary files
            if non_salary_files_needing_reanalysis:
                print(f"\n🔵 NON-SALARY FILES:")
                for file_info in non_salary_files_needing_reanalysis:
                    # Find the actual file path (search through category subdirectories)
                    file_path = None
                    for category_dir in non_salary_path.iterdir():
                        if category_dir.is_dir():
                            cao_non_salary_dir = category_dir / file_info['cao_number']
                            if cao_non_salary_dir.exists():
                                for json_file in cao_non_salary_dir.glob("*.json"):
                                    if json_file.name == file_info['file']:
                                        file_path = json_file
                                        break
                            if file_path:
                                break
                    
                    if file_path and file_path.exists():
                        print(f"\n📄 File: {file_info['file']} (CAO {file_info['cao_number']})")
                        print(f"   Reason: {file_info['reason']}")
                        category = file_info.get('category', 'unknown')
                        present_count = len(file_info.get('present_sections', []))
                        print(f"   Content: {file_info['content_length']} chars")
                        print(f"   Empty sections: {len(file_info['empty_sections'])}/{present_count} present (category: {category})")
                        if len(file_info['empty_sections']) > 0:
                            empty_list = ', '.join(file_info['empty_sections'][:3])
                            if len(file_info['empty_sections']) > 3:
                                empty_list += f" (+{len(file_info['empty_sections']) - 3} more)"
                            print(f"   Missing: {empty_list}")
                        
                        individual_response = input(f"   ❓ Delete this non-salary file? (y/N): ")
                        
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
            print(f"   Files skipped: {total_files_to_delete - deleted_count}")
            
            if deleted_count > 0:
                print(f"\n💡 Next steps:")
                print(f"   - Run the analysis pipeline (p4_analysis.py) again to re-analyze the deleted files")
                print(f"   - The improved prompt should produce better analysis results")
        else:
            print(f"   ⏭️  Deletion cancelled")
    else:
        print(f"   ✅ All files appear to have adequate analysis quality!")
    
    # Save detailed report
    report_file = "performance_logs/llm_analysis/llm_analysis_quality_report.json"
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
