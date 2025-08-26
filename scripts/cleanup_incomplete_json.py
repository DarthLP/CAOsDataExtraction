#!/usr/bin/env python3
"""
Cleanup script to find and delete incomplete JSON files.

This script searches for JSON files that are incomplete (don't end with '}')
or are invalid JSON, and deletes them.

Usage:
    python scripts/cleanup_incomplete_json.py
"""

import json
import os
from pathlib import Path
import sys

def validate_json_file(file_path: Path) -> dict:
    """
    Validate a JSON file for completeness and validity.
    
    Returns:
        dict: {'is_valid': bool, 'error': str or None}
    """
    try:
        # Check if file exists and is not empty
        if not file_path.exists():
            return {'is_valid': False, 'error': 'File does not exist'}
        
        if file_path.stat().st_size == 0:
            return {'is_valid': False, 'error': 'File is empty'}
        
        # Read file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if content is empty
        if not content or not content.strip():
            return {'is_valid': False, 'error': 'Content is empty'}
        
        # Check if content starts with {
        if not content.strip().startswith('{'):
            return {'is_valid': False, 'error': 'Content does not start with {'}
        
        # Check if content ends with }
        if not content.strip().endswith('}'):
            return {'is_valid': False, 'error': 'Content does not end with } - JSON appears to be truncated'}
        
        # Try to parse JSON to validate structure
        json.loads(content)
        return {'is_valid': True, 'error': None}
        
    except json.JSONDecodeError as e:
        return {'is_valid': False, 'error': f'JSON parsing error: {str(e)}'}
    except Exception as e:
        return {'is_valid': False, 'error': f'File reading error: {str(e)}'}

def find_and_cleanup_incomplete_json(output_dir: str = "outputs/llm_extracted/new_flow"):
    """
    Find and delete incomplete JSON files in the specified directory.
    """
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Output directory not found: {output_dir}")
        return
    
    print(f"🔍 Scanning for incomplete JSON files in: {output_dir}")
    print("=" * 60)
    
    incomplete_files = []
    total_files = 0
    
    # Find all JSON files
    for json_file in output_path.rglob("*.json"):
        total_files += 1
        validation_result = validate_json_file(json_file)
        
        if not validation_result['is_valid']:
            incomplete_files.append({
                'file': json_file,
                'error': validation_result['error']
            })
            print(f"❌ Invalid JSON: {json_file.relative_to(output_path)}")
            print(f"   Error: {validation_result['error']}")
    
    print("=" * 60)
    print(f"📊 Summary:")
    print(f"   Total JSON files found: {total_files}")
    print(f"   Invalid/incomplete files: {len(incomplete_files)}")
    
    if not incomplete_files:
        print("✅ No incomplete JSON files found!")
        return
    
    # Ask for confirmation before deletion
    print(f"\n🗑️  Found {len(incomplete_files)} incomplete JSON files to delete:")
    for file_info in incomplete_files:
        print(f"   - {file_info['file'].relative_to(output_path)}")
    
    response = input(f"\n❓ Do you want to delete these {len(incomplete_files)} files? (y/N): ")
    
    if response.lower() in ['y', 'yes']:
        deleted_count = 0
        for file_info in incomplete_files:
            try:
                file_info['file'].unlink()
                print(f"🗑️  Deleted: {file_info['file'].relative_to(output_path)}")
                deleted_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {file_info['file'].relative_to(output_path)}: {e}")
        
        print(f"\n✅ Successfully deleted {deleted_count} incomplete JSON files")
    else:
        print("❌ Deletion cancelled")

if __name__ == "__main__":
    # Allow custom output directory as command line argument
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "outputs/llm_extracted/new_flow"
    find_and_cleanup_incomplete_json(output_dir)
