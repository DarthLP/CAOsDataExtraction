#!/usr/bin/env python3
"""
Script to analyze list content differences between new_flow and single_flow JSON files.
Checks if lists with different lengths contain the same information.

Usage: python scripts/analyze_list_content_differences.py
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Set

def load_json_file(file_path: Path) -> Dict[str, Any]:
    """Load and return JSON content from file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return {}

def extract_list_content(data: Any, path: str = "") -> Dict[str, Any]:
    """Extract all list content from JSON data with their paths."""
    lists = {}
    
    def extract_recursive(obj, current_path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}.{key}" if current_path else key
                extract_recursive(value, new_path)
        elif isinstance(obj, list):
            lists[current_path] = {
                'length': len(obj),
                'items': obj,
                'content_summary': summarize_list_content(obj)
            }
        elif isinstance(obj, (dict, list)):
            extract_recursive(obj, current_path)
    
    extract_recursive(data)
    return lists

def summarize_list_content(items: List[Any]) -> Dict[str, Any]:
    """Create a summary of list content for comparison."""
    if not items:
        return {'empty': True}
    
    # Get unique items (for deduplication)
    unique_items = []
    seen = set()
    
    for item in items:
        if isinstance(item, dict):
            # For dictionaries, create a hash-like representation
            item_str = json.dumps(item, sort_keys=True)
        elif isinstance(item, (list, tuple)):
            # For lists/tuples, convert to string
            item_str = json.dumps(item, sort_keys=True)
        else:
            # For other types, convert to string
            item_str = str(item)
        
        if item_str not in seen:
            seen.add(item_str)
            unique_items.append(item)
    
    # Analyze content types
    content_types = {}
    for item in items:
        item_type = type(item).__name__
        content_types[item_type] = content_types.get(item_type, 0) + 1
    
    # Extract key information from items
    key_info = []
    for item in items[:5]:  # Look at first 5 items
        if isinstance(item, dict):
            # Extract key-value pairs
            key_info.append({k: str(v)[:50] for k, v in list(item.items())[:3]})
        elif isinstance(item, str):
            key_info.append(item[:100])
        else:
            key_info.append(str(item)[:100])
    
    return {
        'total_items': len(items),
        'unique_items': len(unique_items),
        'duplicates': len(items) - len(unique_items),
        'content_types': content_types,
        'sample_items': key_info,
        'has_duplicates': len(items) != len(unique_items)
    }

def compare_list_content(new_flow_lists: Dict[str, Any], single_flow_lists: Dict[str, Any]) -> Dict[str, Any]:
    """Compare list content between two flows."""
    comparison = {}
    
    # Find lists that exist in both flows
    common_paths = set(new_flow_lists.keys()) & set(single_flow_lists.keys())
    
    for path in common_paths:
        new_list = new_flow_lists[path]
        single_list = single_flow_lists[path]
        
        # Check if lengths differ
        length_diff = new_list['length'] != single_list['length']
        
        # Compare content summaries
        content_similar = new_list['content_summary'] == single_list['content_summary']
        
        # Compare actual items (normalized)
        new_items_normalized = normalize_list_items(new_list['items'])
        single_items_normalized = normalize_list_items(single_list['items'])
        
        items_match = new_items_normalized == single_items_normalized
        
        comparison[path] = {
            'new_flow_length': new_list['length'],
            'single_flow_length': single_list['length'],
            'length_difference': abs(new_list['length'] - single_list['length']),
            'lengths_differ': length_diff,
            'content_summary_match': content_similar,
            'items_match': items_match,
            'new_flow_summary': new_list['content_summary'],
            'single_flow_summary': single_list['content_summary']
        }
    
    return comparison

def normalize_list_items(items: List[Any]) -> List[str]:
    """Normalize list items for comparison."""
    normalized = []
    for item in items:
        if isinstance(item, dict):
            # Sort dictionary keys and convert to string
            normalized.append(json.dumps(item, sort_keys=True))
        elif isinstance(item, (list, tuple)):
            # Convert lists/tuples to string
            normalized.append(json.dumps(item, sort_keys=True))
        else:
            # Convert other types to string
            normalized.append(str(item))
    
    # Sort and return unique items
    return sorted(set(normalized))

def main():
    """Main function to analyze list content differences."""
    # Define paths
    base_dir = Path("outputs/llm_extracted")
    new_flow_dir = base_dir / "new_flow" / "10"
    single_flow_dir = base_dir / "single_file" / "json_single" / "10"
    
    print("Analyzing list content differences for CAO 10")
    print("=" * 60)
    
    # Find matching files
    new_flow_files = list(new_flow_dir.glob("*.json"))
    single_flow_files = list(single_flow_dir.glob("*.json"))
    
    new_flow_map = {f.stem: f for f in new_flow_files}
    single_flow_map = {f.stem: f for f in single_flow_files}
    common_stems = set(new_flow_map.keys()) & set(single_flow_map.keys())
    
    if not common_stems:
        print("No matching files found!")
        return
    
    all_results = {}
    
    for stem in common_stems:
        print(f"\n📄 Analyzing file: {stem}")
        print("-" * 40)
        
        new_flow_file = new_flow_map[stem]
        single_flow_file = single_flow_map[stem]
        
        # Load data
        new_flow_data = load_json_file(new_flow_file)
        single_flow_data = load_json_file(single_flow_file)
        
        # Extract list content
        new_flow_lists = extract_list_content(new_flow_data)
        single_flow_lists = extract_list_content(single_flow_data)
        
        # Compare lists
        comparison = compare_list_content(new_flow_lists, single_flow_lists)
        
        # Display results
        for path, result in comparison.items():
            print(f"\n🔍 List: {path}")
            print(f"   Lengths: {result['new_flow_length']} vs {result['single_flow_length']} (diff: {result['length_difference']})")
            print(f"   Lengths differ: {'❌' if result['lengths_differ'] else '✅'}")
            print(f"   Content summary matches: {'✅' if result['content_summary_match'] else '❌'}")
            print(f"   Items match: {'✅' if result['items_match'] else '❌'}")
            
            if result['lengths_differ']:
                print(f"   📊 New flow summary: {result['new_flow_summary']}")
                print(f"   📊 Single flow summary: {result['single_flow_summary']}")
        
        all_results[stem] = comparison
    
    # Save detailed analysis
    output_dir = Path("outputs/comparison_results")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"list_content_analysis_cao_10.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Detailed analysis saved to: {output_file}")

if __name__ == "__main__":
    main()
