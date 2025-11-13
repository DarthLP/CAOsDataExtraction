"""
Script to identify missing files between parsed markdown and extracted JSON outputs.

This script compares:
- Input: outputs/parsed_pdfs/parsed_pdfs_markdown/[CAO_NUMBER]/*.md
- Output: outputs/llm_extracted/new_flow/[CAO_NUMBER]/*_extract.json

It identifies:
1. Markdown files that don't have corresponding JSON outputs
2. Files that might have failed extraction
3. Total counts and discrepancies

USAGE:
    python scripts/find_missing_files.py
"""

import os
from pathlib import Path
import yaml

def load_config():
    """Load configuration from config.yaml."""
    with open('conf/config.yaml', 'r') as f:
        return yaml.safe_load(f)

def find_missing_files():
    """Find markdown files without corresponding JSON outputs."""
    config = load_config()
    
    input_folder = Path(config['paths']['parsed_pdfs_markdown'])
    output_folder = Path(config['paths']['outputs_json']) / "new_flow"
    
    print("=" * 80)
    print("MISSING FILES ANALYSIS")
    print("=" * 80)
    print(f"Input folder: {input_folder}")
    print(f"Output folder: {output_folder}")
    print()
    
    # Collect all markdown files
    markdown_files = {}
    cao_folders = sorted([f for f in input_folder.iterdir() 
                         if f.is_dir() and f.name.isdigit()], 
                        key=lambda f: int(f.name))
    
    for cao_folder in cao_folders:
        cao_number = cao_folder.name
        md_files = list(cao_folder.glob('*.md'))
        markdown_files[cao_number] = md_files
    
    total_markdown = sum(len(files) for files in markdown_files.values())
    print(f"Total markdown files found: {total_markdown}")
    
    # Collect all JSON output files
    json_files = {}
    if output_folder.exists():
        for cao_folder in output_folder.iterdir():
            if cao_folder.is_dir() and cao_folder.name.isdigit():
                cao_number = cao_folder.name
                json_files[cao_number] = list(cao_folder.glob('*_extract.json'))
    else:
        print(f"WARNING: Output folder does not exist: {output_folder}")
    
    total_json = sum(len(files) for files in json_files.values())
    print(f"Total JSON files found: {total_json}")
    print()
    
    # Find missing files
    missing_files = []
    missing_by_cao = {}
    
    for cao_number, md_files in markdown_files.items():
        cao_missing = []
        
        for md_file in md_files:
            # Expected JSON filename: {md_file.stem}_extract.json
            expected_json = md_file.stem + "_extract.json"
            expected_json_path = output_folder / cao_number / expected_json
            
            if not expected_json_path.exists():
                missing_files.append((cao_number, md_file.name, md_file))
                cao_missing.append(md_file.name)
        
        if cao_missing:
            missing_by_cao[cao_number] = cao_missing
    
    print("=" * 80)
    print(f"MISSING FILES: {len(missing_files)} files without JSON output")
    print("=" * 80)
    
    if missing_files:
        print("\nMissing files by CAO number:")
        for cao_number in sorted(missing_by_cao.keys(), key=int):
            files = missing_by_cao[cao_number]
            print(f"\n  CAO {cao_number}: {len(files)} missing files")
            for filename in sorted(files):
                print(f"    - {filename}")
        
        # Check if files are in failed logs
        failed_log = Path('outputs/logs/failed_files_llm_extraction.txt')
        failed_files = set()
        if failed_log.exists():
            with open(failed_log, 'r', encoding='utf-8') as f:
                for line in f:
                    if ':' in line:
                        # Extract filename from log line
                        parts = line.strip().split(' - ')
                        if len(parts) > 1:
                            filename = parts[1].split(' (Error:')[0].strip()
                            failed_files.add(filename)
        
        print("\n" + "=" * 80)
        print("ANALYSIS:")
        print("=" * 80)
        
        in_failed_log = []
        not_in_failed_log = []
        
        for cao_number, filename, filepath in missing_files:
            if filename in failed_files:
                in_failed_log.append((cao_number, filename))
            else:
                not_in_failed_log.append((cao_number, filename))
        
        print(f"\nFiles in failed log: {len(in_failed_log)}")
        if in_failed_log:
            print("  (These files were logged as failed)")
            for cao_number, filename in in_failed_log:
                print(f"    - CAO {cao_number}: {filename}")
        
        print(f"\nFiles NOT in failed log: {len(not_in_failed_log)}")
        if not_in_failed_log:
            print("  (These files were never attempted or failed silently)")
            for cao_number, filename in not_in_failed_log:
                print(f"    - CAO {cao_number}: {filename}")
        
        # Save to file
        output_file = Path('outputs/logs/missing_files_analysis.txt')
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("MISSING FILES ANALYSIS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total markdown files: {total_markdown}\n")
            f.write(f"Total JSON files: {total_json}\n")
            f.write(f"Missing files: {len(missing_files)}\n\n")
            
            f.write("Missing files by CAO:\n")
            for cao_number in sorted(missing_by_cao.keys(), key=int):
                f.write(f"\nCAO {cao_number}:\n")
                for filename in sorted(missing_by_cao[cao_number]):
                    f.write(f"  - {filename}\n")
        
        print(f"\n📁 Detailed analysis saved to: {output_file}")
    else:
        print("✅ All markdown files have corresponding JSON outputs!")
    
    print("\n" + "=" * 80)
    print(f"SUMMARY: {total_markdown} markdown files, {total_json} JSON files, {len(missing_files)} missing")
    print("=" * 80)

if __name__ == "__main__":
    find_missing_files()

