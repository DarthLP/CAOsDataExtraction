#!/usr/bin/env python3
"""
Script to compare JSON files between new_flow and single_flow folders for CAO number 10.
Tests determinism by comparing extracted data from both flows.

Usage: python scripts/compare_determinism_cao_10.py
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any
import hashlib

def load_json_file(file_path: Path) -> Dict[str, Any]:
    """Load and return JSON content from file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return {}

def get_file_hash(file_path: Path) -> str:
    """Get MD5 hash of file content."""
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception as e:
        print(f"Error getting hash for {file_path}: {e}")
        return ""

def normalize_for_flexible_comparison(data: Any) -> Any:
    """Normalize data for flexible comparison by sorting and removing whitespace."""
    if isinstance(data, dict):
        return {k: normalize_for_flexible_comparison(v) for k, v in sorted(data.items())}
    elif isinstance(data, list):
        return sorted([normalize_for_flexible_comparison(item) for item in data])
    elif isinstance(data, str):
        return data.strip()
    elif isinstance(data, float):
        # Round to 6 decimal places to handle floating point precision issues
        return round(data, 6)
    else:
        return data

def normalize_for_very_flexible_comparison(data: Any) -> Any:
    """Normalize data for very flexible comparison by focusing on content structure."""
    if isinstance(data, dict):
        # Remove empty strings, None values, and normalize remaining content
        normalized = {}
        for k, v in data.items():
            if v is not None and v != "":
                normalized[k] = normalize_for_very_flexible_comparison(v)
        return {k: normalized[k] for k in sorted(normalized.keys())}
    elif isinstance(data, list):
        # Remove empty items and normalize remaining
        normalized = [normalize_for_very_flexible_comparison(item) for item in data if item not in [None, "", []]]
        return sorted(normalized) if normalized else []
    elif isinstance(data, str):
        # Remove extra whitespace and normalize
        return " ".join(data.split())
    elif isinstance(data, float):
        # Round to 4 decimal places for very flexible comparison
        return round(data, 4)
    else:
        return data

def normalize_for_fuzzy_comparison(data: Any) -> Any:
    """Normalize data for fuzzy comparison to handle small mistakes and typos."""
    if isinstance(data, dict):
        normalized = {}
        for k, v in data.items():
            if v is not None and v != "":
                normalized[k] = normalize_for_fuzzy_comparison(v)
        return {k: normalized[k] for k in sorted(normalized.keys())}
    elif isinstance(data, list):
        normalized = [normalize_for_fuzzy_comparison(item) for item in data if item not in [None, "", []]]
        return sorted(normalized) if normalized else []
    elif isinstance(data, str):
        # Aggressive normalization for fuzzy comparison
        # Remove all punctuation and convert to lowercase
        import re
        cleaned = re.sub(r'[^\w\s]', '', data.lower())
        # Remove extra whitespace and normalize
        normalized = " ".join(cleaned.split())
        # Remove common filler words that might vary
        filler_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = normalized.split()
        filtered_words = [word for word in words if word not in filler_words]
        return " ".join(filtered_words)
    elif isinstance(data, float):
        # Round to 2 decimal places for fuzzy comparison
        return round(data, 2)
    else:
        return data

def calculate_similarity_score(str1: str, str2: str) -> float:
    """Calculate similarity score between two strings using simple character-based comparison."""
    if not str1 and not str2:
        return 1.0
    if not str1 or not str2:
        return 0.0
    
    # Convert to sets of characters for simple similarity
    set1 = set(str1.lower())
    set2 = set(str2.lower())
    
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    
    return intersection / union if union > 0 else 0.0

def compare_strings_fuzzy(str1: str, str2: str, threshold: float = 0.8) -> bool:
    """Compare two strings with fuzzy matching."""
    if str1 == str2:
        return True
    
    similarity = calculate_similarity_score(str1, str2)
    return similarity >= threshold

def normalize_for_semantic_comparison(data: Any) -> Any:
    """Normalize data for semantic comparison focusing on core meaning and key terms."""
    if isinstance(data, dict):
        normalized = {}
        for k, v in data.items():
            if v is not None and v != "":
                normalized[k] = normalize_for_semantic_comparison(v)
        return {k: normalized[k] for k in sorted(normalized.keys())}
    elif isinstance(data, list):
        normalized = [normalize_for_semantic_comparison(item) for item in data if item not in [None, "", []]]
        return sorted(normalized) if normalized else []
    elif isinstance(data, str):
        # Very aggressive normalization for semantic comparison
        import re
        
        # Convert to lowercase and remove punctuation
        cleaned = re.sub(r'[^\w\s]', ' ', data.lower())
        
        # Remove extra whitespace
        normalized = " ".join(cleaned.split())
        
        # Remove common words that don't carry semantic meaning
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'could', 'should', 'may', 'might', 'can', 'must', 'shall',
            'this', 'that', 'these', 'those', 'it', 'its', 'they', 'them', 'their',
            'from', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
            'up', 'down', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once',
            'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few',
            'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
            'so', 'than', 'too', 'very', 'just', 'now', 'also', 'well', 'even', 'still', 'back',
            'like', 'way', 'year', 'day', 'time', 'work', 'people', 'man', 'woman', 'child',
            'number', 'part', 'thing', 'case', 'point', 'place', 'group', 'company', 'business',
            'information', 'data', 'system', 'program', 'question', 'problem', 'fact', 'government',
            'hand', 'eye', 'life', 'world', 'head', 'face', 'month', 'week', 'hour', 'minute',
            'second', 'morning', 'afternoon', 'evening', 'night', 'today', 'tomorrow', 'yesterday'
        }
        
        words = normalized.split()
        # Keep only meaningful words (longer than 2 characters and not stop words)
        meaningful_words = [word for word in words if len(word) > 2 and word not in stop_words]
        
        return " ".join(meaningful_words)
    elif isinstance(data, float):
        # Round to 1 decimal place for semantic comparison
        return round(data, 1)
    else:
        return data

def extract_key_terms(text: str) -> set:
    """Extract key terms from text for semantic comparison."""
    if not text:
        return set()
    
    # Normalize the text
    normalized = normalize_for_semantic_comparison(text)
    
    # Split into words and create a set of unique terms
    words = normalized.split()
    
    # Filter out very short words and common terms
    key_terms = {word for word in words if len(word) > 3}
    
    return key_terms

def calculate_semantic_similarity(text1: str, text2: str) -> float:
    """Calculate semantic similarity between two texts based on key terms."""
    if not text1 and not text2:
        return 1.0
    if not text1 or not text2:
        return 0.0
    
    terms1 = extract_key_terms(text1)
    terms2 = extract_key_terms(text2)
    
    if not terms1 and not terms2:
        return 1.0
    if not terms1 or not terms2:
        return 0.0
    
    intersection = len(terms1.intersection(terms2))
    union = len(terms1.union(terms2))
    
    return intersection / union if union > 0 else 0.0

def compare_strings_semantic(str1: str, str2: str, threshold: float = 0.6) -> bool:
    """Compare two strings with semantic similarity."""
    if str1 == str2:
        return True
    
    similarity = calculate_semantic_similarity(str1, str2)
    return similarity >= threshold

def extract_numbers_and_structured_data(data: Any) -> Dict[str, Any]:
    """Extract numbers and structured data from JSON for comparison."""
    extracted = {
        'numbers': [],
        'percentages': [],
        'dates': [],
        'currencies': [],
        'table_data': [],
        'key_values': {}
    }
    
    def extract_recursive(obj, path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                extract_recursive(value, current_path)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                current_path = f"{path}[{i}]"
                extract_recursive(item, current_path)
        elif isinstance(obj, (int, float)):
            extracted['numbers'].append({
                'value': obj,
                'path': path,
                'type': 'number'
            })
        elif isinstance(obj, str):
            # Extract numbers from strings
            import re
            
            # Find percentages (e.g., "3%", "5.5%")
            percentages = re.findall(r'(\d+(?:\.\d+)?)\s*%', obj)
            for pct in percentages:
                extracted['percentages'].append({
                    'value': float(pct),
                    'path': path,
                    'original': obj
                })
            
            # Find currency amounts (e.g., "€5000", "$1000")
            currencies = re.findall(r'[€$£]\s*(\d+(?:,\d{3})*(?:\.\d{2})?)', obj)
            for curr in currencies:
                # Remove commas and convert to float
                clean_value = float(curr.replace(',', ''))
                extracted['currencies'].append({
                    'value': clean_value,
                    'path': path,
                    'original': obj
                })
            
            # Find dates (various formats)
            date_patterns = [
                r'(\d{1,2}-\d{1,2}-\d{4})',  # DD-MM-YYYY
                r'(\d{4}-\d{1,2}-\d{1,2})',  # YYYY-MM-DD
                r'(\d{1,2}/\d{1,2}/\d{4})',  # DD/MM/YYYY
                r'(\d{4}/\d{1,2}/\d{1,2})',  # YYYY/MM/DD
            ]
            for pattern in date_patterns:
                dates = re.findall(pattern, obj)
                for date in dates:
                    extracted['dates'].append({
                        'value': date,
                        'path': path,
                        'original': obj
                    })
            
            # Find standalone numbers in text
            numbers = re.findall(r'\b(\d+(?:\.\d+)?)\b', obj)
            for num in numbers:
                extracted['numbers'].append({
                    'value': float(num),
                    'path': path,
                    'original': obj
                })
    
    extract_recursive(data)
    return extracted

def normalize_for_data_focused_comparison(data: Any) -> Any:
    """Normalize data focusing on numbers, tables, and structured content."""
    if isinstance(data, dict):
        # For dictionaries, focus on key-value pairs with numbers or structured data
        normalized = {}
        for k, v in data.items():
            if v is not None and v != "":
                # Keep the key but normalize the value
                normalized[k] = normalize_for_data_focused_comparison(v)
        return {k: normalized[k] for k in sorted(normalized.keys())}
    elif isinstance(data, list):
        # For lists, keep only non-empty items and normalize them
        normalized = [normalize_for_data_focused_comparison(item) for item in data if item not in [None, "", []]]
        return sorted(normalized) if normalized else []
    elif isinstance(data, str):
        # For strings, extract and normalize numbers and key data
        import re
        
        # Extract all numbers and keep them as the main content
        numbers = re.findall(r'\d+(?:\.\d+)?', data)
        
        # Extract key terms that might indicate important data
        key_indicators = [
            'salary', 'wage', 'payment', 'amount', 'cost', 'price', 'fee',
            'percentage', 'percent', '%', 'rate', 'increase', 'decrease',
            'duration', 'period', 'time', 'month', 'year', 'week', 'day',
            'date', 'deadline', 'expiry', 'valid', 'contract', 'agreement',
            'employee', 'worker', 'staff', 'personnel', 'member',
            'benefit', 'allowance', 'bonus', 'compensation', 'remuneration',
            'hour', 'overtime', 'shift', 'schedule', 'roster',
            'leave', 'vacation', 'holiday', 'sick', 'maternity', 'paternity'
        ]
        
        # Check if string contains any key indicators
        has_key_content = any(indicator in data.lower() for indicator in key_indicators)
        
        if numbers or has_key_content:
            # Keep the string if it contains numbers or key indicators
            return data.lower().strip()
        else:
            # For other strings, return empty to focus on data
            return ""
    elif isinstance(data, (int, float)):
        # Keep numbers as they are
        return data
    else:
        return data

def find_key_differences(data1: Any, data2: Any, max_differences: int = 5) -> List[str]:
    """Find key differences between two data structures."""
    differences = []
    
    def compare_recursive(obj1, obj2, path=""):
        if len(differences) >= max_differences:
            return
        
        if type(obj1) != type(obj2):
            differences.append(f"Type mismatch at {path}: {type(obj1).__name__} vs {type(obj2).__name__}")
            return
        
        if isinstance(obj1, dict):
            keys1 = set(obj1.keys())
            keys2 = set(obj2.keys())
            
            # Missing keys
            missing_in_2 = keys1 - keys2
            if missing_in_2:
                differences.append(f"Missing keys in second file at {path}: {list(missing_in_2)[:3]}")
            
            # Extra keys
            extra_in_2 = keys2 - keys1
            if extra_in_2:
                differences.append(f"Extra keys in second file at {path}: {list(extra_in_2)[:3]}")
            
            # Compare common keys
            common_keys = keys1 & keys2
            for key in list(common_keys)[:3]:  # Limit to first 3 keys
                if len(differences) >= max_differences:
                    break
                compare_recursive(obj1[key], obj2[key], f"{path}.{key}" if path else key)
        
        elif isinstance(obj1, list):
            if len(obj1) != len(obj2):
                differences.append(f"List length differs at {path}: {len(obj1)} vs {len(obj2)}")
                return
            
            # Compare first few items
            for i in range(min(3, len(obj1))):
                if len(differences) >= max_differences:
                    break
                compare_recursive(obj1[i], obj2[i], f"{path}[{i}]")
        
        elif isinstance(obj1, str):
            if obj1 != obj2:
                # Show first 50 characters of difference
                diff_preview = f"'{obj1[:50]}...' vs '{obj2[:50]}...'" if len(obj1) > 50 or len(obj2) > 50 else f"'{obj1}' vs '{obj2}'"
                differences.append(f"String differs at {path}: {diff_preview}")
        
        elif isinstance(obj1, (int, float)):
            if obj1 != obj2:
                differences.append(f"Number differs at {path}: {obj1} vs {obj2}")
    
    compare_recursive(data1, data2)
    return differences

def compare_data_focused(data1: Any, data2: Any) -> bool:
    """Compare two data structures focusing on numbers and structured content."""
    # Extract numbers and structured data from both
    extracted1 = extract_numbers_and_structured_data(data1)
    extracted2 = extract_numbers_and_structured_data(data2)
    
    # Compare numbers
    numbers1 = {item['value'] for item in extracted1['numbers']}
    numbers2 = {item['value'] for item in extracted2['numbers']}
    numbers_match = numbers1 == numbers2
    
    # Compare percentages
    percentages1 = {item['value'] for item in extracted1['percentages']}
    percentages2 = {item['value'] for item in extracted2['percentages']}
    percentages_match = percentages1 == percentages2
    
    # Compare currencies
    currencies1 = {item['value'] for item in extracted1['currencies']}
    currencies2 = {item['value'] for item in extracted2['currencies']}
    currencies_match = currencies1 == currencies2
    
    # Compare dates
    dates1 = {item['value'] for item in extracted1['dates']}
    dates2 = {item['value'] for item in extracted2['dates']}
    dates_match = dates1 == dates2
    
    # Normalize both structures for comparison
    normalized1 = normalize_for_data_focused_comparison(data1)
    normalized2 = normalize_for_data_focused_comparison(data2)
    structure_match = normalized1 == normalized2
    
    # Return True if any of the key data types match
    return numbers_match or percentages_match or currencies_match or dates_match or structure_match

def compare_json_files(new_flow_file: Path, single_flow_file: Path) -> Dict[str, Any]:
    """Compare two JSON files and return comparison results with four levels of strictness."""
    new_flow_data = load_json_file(new_flow_file)
    single_flow_data = load_json_file(single_flow_file)
    
    new_flow_hash = get_file_hash(new_flow_file)
    single_flow_hash = get_file_hash(single_flow_file)
    
    # Level 1: Strict comparison (exact match)
    strict_match = new_flow_data == single_flow_data
    
    # Level 2: Flexible comparison (normalized)
    new_flow_normalized = normalize_for_flexible_comparison(new_flow_data)
    single_flow_normalized = normalize_for_flexible_comparison(single_flow_data)
    flexible_match = new_flow_normalized == single_flow_normalized
    
    # Level 3: Very flexible comparison (content-focused)
    new_flow_very_flexible = normalize_for_very_flexible_comparison(new_flow_data)
    single_flow_very_flexible = normalize_for_very_flexible_comparison(single_flow_data)
    very_flexible_match = new_flow_very_flexible == single_flow_very_flexible
    
    # Level 4: Fuzzy comparison (handles small mistakes and typos)
    new_flow_fuzzy = normalize_for_fuzzy_comparison(new_flow_data)
    single_flow_fuzzy = normalize_for_fuzzy_comparison(single_flow_data)
    fuzzy_match = new_flow_fuzzy == single_flow_fuzzy
    
    # Level 5: Semantic comparison (focuses on core meaning)
    new_flow_semantic = normalize_for_semantic_comparison(new_flow_data)
    single_flow_semantic = normalize_for_semantic_comparison(single_flow_data)
    semantic_match = new_flow_semantic == single_flow_semantic
    
    # Level 6: Data-focused comparison (numbers, tables, structured content)
    data_focused_match = compare_data_focused(new_flow_data, single_flow_data)
    
    # Calculate differences for each level
    strict_differences = [] if strict_match else find_key_differences(new_flow_data, single_flow_data)
    flexible_differences = [] if flexible_match else find_key_differences(new_flow_normalized, single_flow_normalized)
    very_flexible_differences = [] if very_flexible_match else find_key_differences(new_flow_very_flexible, single_flow_very_flexible)
    fuzzy_differences = [] if fuzzy_match else find_key_differences(new_flow_fuzzy, single_flow_fuzzy)
    semantic_differences = [] if semantic_match else find_key_differences(new_flow_semantic, single_flow_semantic)
    
    comparison = {
        'new_flow_file': str(new_flow_file),
        'single_flow_file': str(single_flow_file),
        'new_flow_hash': new_flow_hash,
        'single_flow_hash': single_flow_hash,
        'hashes_match': new_flow_hash == single_flow_hash,
        'new_flow_size': len(new_flow_data),
        'single_flow_size': len(single_flow_data),
        'sizes_match': len(new_flow_data) == len(single_flow_data),
        'strict_match': strict_match,
        'strict_differences': strict_differences,
        'flexible_match': flexible_match,
        'flexible_differences': flexible_differences,
        'very_flexible_match': very_flexible_match,
        'very_flexible_differences': very_flexible_differences,
        'fuzzy_match': fuzzy_match,
        'fuzzy_differences': fuzzy_differences,
        'semantic_match': semantic_match,
        'semantic_differences': semantic_differences,
        'data_focused_match': data_focused_match
    }
    
    return comparison

def find_matching_files(new_flow_dir: Path, single_flow_dir: Path) -> List[Tuple[Path, Path]]:
    """Find matching JSON files between the two directories."""
    new_flow_files = list(new_flow_dir.glob("*.json"))
    single_flow_files = list(single_flow_dir.glob("*.json"))
    
    # Create mapping of base names to full paths
    new_flow_map = {f.stem: f for f in new_flow_files}
    single_flow_map = {f.stem: f for f in single_flow_files}
    
    # Find common files
    common_stems = set(new_flow_map.keys()) & set(single_flow_map.keys())
    
    return [(new_flow_map[stem], single_flow_map[stem]) for stem in common_stems]

def main():
    """Main function to compare determinism for CAO 10."""
    # Define paths
    base_dir = Path("outputs/llm_extracted")
    new_flow_dir = base_dir / "new_flow" / "10"
    single_flow_dir = base_dir / "single_file" / "json_single" / "10"
    
    print(f"Comparing determinism for CAO 10")
    print(f"New flow directory: {new_flow_dir}")
    print(f"Single flow directory: {single_flow_dir}")
    print("-" * 60)
    
    # Check if directories exist
    if not new_flow_dir.exists():
        print(f"Error: New flow directory does not exist: {new_flow_dir}")
        return
    
    if not single_flow_dir.exists():
        print(f"Error: Single flow directory does not exist: {single_flow_dir}")
        print("Please copy the files to the single_flow directory first.")
        return
    
    # Find matching files
    matching_files = find_matching_files(new_flow_dir, single_flow_dir)
    
    if not matching_files:
        print("No matching JSON files found between the two directories.")
        print(f"Files in new_flow: {[f.name for f in new_flow_dir.glob('*.json')]}")
        print(f"Files in single_flow: {[f.name for f in single_flow_dir.glob('*.json')]}")
        return
    
    print(f"Found {len(matching_files)} matching files to compare:")
    for new_file, single_file in matching_files:
        print(f"  - {new_file.name}")
    print()
    
    # Compare each pair of files
    results = []
    all_match = True
    
    for new_file, single_file in matching_files:
        print(f"Comparing: {new_file.name}")
        comparison = compare_json_files(new_file, single_file)
        results.append(comparison)
        
        if comparison['hashes_match']:
            print(f"  ✓ Hashes match")
        else:
            print(f"  ✗ Hashes differ")
            all_match = False
        
        # Level 1: Strict comparison
        if comparison['strict_match']:
            print(f"  ✓ Strict match (exact)")
        else:
            print(f"  ✗ Strict match failed")
            if comparison['strict_differences']:
                print(f"    Main differences: {comparison['strict_differences'][:2]}")
            all_match = False
        
        # Level 2: Flexible comparison
        if comparison['flexible_match']:
            print(f"  ✓ Flexible match (normalized)")
        else:
            print(f"  ✗ Flexible match failed")
            if comparison['flexible_differences']:
                print(f"    Main differences: {comparison['flexible_differences'][:2]}")
            all_match = False
        
        # Level 3: Very flexible comparison
        if comparison['very_flexible_match']:
            print(f"  ✓ Very flexible match (content-focused)")
        else:
            print(f"  ✗ Very flexible match failed")
            if comparison['very_flexible_differences']:
                print(f"    Main differences: {comparison['very_flexible_differences'][:2]}")
            all_match = False
        
        # Level 4: Fuzzy comparison (handles small mistakes)
        if comparison['fuzzy_match']:
            print(f"  ✓ Fuzzy match (typo-tolerant)")
        else:
            print(f"  ✗ Fuzzy match failed")
            if comparison['fuzzy_differences']:
                print(f"    Main differences: {comparison['fuzzy_differences'][:2]}")
            all_match = False
        
        # Level 5: Semantic comparison (core meaning)
        if comparison['semantic_match']:
            print(f"  ✓ Semantic match (meaning-focused)")
        else:
            print(f"  ✗ Semantic match failed")
            if comparison['semantic_differences']:
                print(f"    Main differences: {comparison['semantic_differences'][:2]}")
            all_match = False
        
        # Level 6: Data-focused comparison (numbers & tables)
        if comparison['data_focused_match']:
            print(f"  ✓ Data-focused match (numbers/tables)")
        else:
            print(f"  ✗ Data-focused match failed")
            all_match = False
        
        print(f"  New flow size: {comparison['new_flow_size']}")
        print(f"  Single flow size: {comparison['single_flow_size']}")
        print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if all_match:
        print("🎉 ALL FILES MATCH - Determinism test PASSED!")
    else:
        print("❌ SOME FILES DIFFER - Determinism test FAILED!")
    
    print(f"\nDetailed results:")
    for i, result in enumerate(results, 1):
        filename = Path(result['new_flow_file']).name
        print(f"{i}. {filename}:")
        print(f"   Hashes match: {result['hashes_match']}")
        print(f"   Strict match: {result['strict_match']}")
        print(f"   Flexible match: {result['flexible_match']}")
        print(f"   Very flexible match: {result['very_flexible_match']}")
        print(f"   Fuzzy match: {result['fuzzy_match']}")
        print(f"   Semantic match: {result['semantic_match']}")
        print(f"   Data-focused match: {result['data_focused_match']}")
        print(f"   Sizes match: {result['sizes_match']}")
    
    # Save detailed summary to JSON file
    summary_data = {
        'timestamp': str(Path().cwd()),
        'cao_number': '10',
        'new_flow_directory': str(new_flow_dir),
        'single_flow_directory': str(single_flow_dir),
        'total_files_compared': len(results),
        'all_match': all_match,
        'summary': {
            'hashes_match': all(result['hashes_match'] for result in results),
            'strict_match': all(result['strict_match'] for result in results),
            'flexible_match': all(result['flexible_match'] for result in results),
            'very_flexible_match': all(result['very_flexible_match'] for result in results),
            'fuzzy_match': all(result['fuzzy_match'] for result in results),
            'semantic_match': all(result['semantic_match'] for result in results),
            'data_focused_match': all(result['data_focused_match'] for result in results),
            'sizes_match': all(result['sizes_match'] for result in results)
        },
        'detailed_results': results
    }
    
    # Create output directory if it doesn't exist
    output_dir = Path("outputs/comparison_results")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save to JSON file
    output_file = output_dir / f"determinism_comparison_cao_10_{Path().cwd().name}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Detailed summary saved to: {output_file}")

if __name__ == "__main__":
    main()
