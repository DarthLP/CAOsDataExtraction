#!/usr/bin/env python3
"""
Count Files Script
==================

This script counts the actual number of files in the input folders
to give you the real total for progress calculations.
"""

from pathlib import Path

def count_input_files():
    """Count all JSON files in input folders"""
    
    input_folder = Path("output_json")
    
    if not input_folder.exists():
        print("❌ Input folder 'output_json' not found!")
        return 0
    
    total_files = 0
    cao_folders = []
    
    # Count files in each CAO folder
    for cao_folder in sorted(input_folder.iterdir()):
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            cao_number = cao_folder.name
            json_files = list(cao_folder.glob("*.json"))
            file_count = len(json_files)
            total_files += file_count
            cao_folders.append((cao_number, file_count))
    
    print(f"📊 FILE COUNT SUMMARY:")
    print(f"   Total files found: {total_files:,}")
    print(f"   CAO folders: {len(cao_folders)}")
    print(f"\n📁 Files per CAO folder:")
    
    for cao_number, file_count in cao_folders:
        print(f"   CAO {cao_number}: {file_count} files")
    
    print(f"\n💡 Use this number ({total_files}) for progress calculations!")
    return total_files

if __name__ == "__main__":
    count_input_files()
