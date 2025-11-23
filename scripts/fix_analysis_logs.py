"""
Fix Analysis Performance Logs
=============================

This script cleans up stale failed entries in analysis performance logs.
It removes failed log entries for files that have since succeeded, ensuring
the logs accurately reflect the current state.

USAGE:
    python scripts/fix_analysis_logs.py [--log-file LOG_FILE] [--dry-run]

ARGUMENTS:
    --log-file: Specific log file to clean (default: all analysis log files)
    --dry-run: Show what would be deleted without actually deleting

EXAMPLES:
    # Clean all analysis logs
    python scripts/fix_analysis_logs.py

    # Clean only salary log
    python scripts/fix_analysis_logs.py --log-file performance_logs/llm_analysis/analysis_performance_salary.jsonl

    # Dry run to see what would be cleaned
    python scripts/fix_analysis_logs.py --dry-run
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional


def load_log_entries(log_file: Path) -> List[Dict]:
    """
    Load all log entries from a JSONL file.
    
    Args:
        log_file: Path to the JSONL log file
        
    Returns:
        List of log entry dictionaries
    """
    entries = []
    if not log_file.exists():
        return entries
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Invalid JSON on line {line_num} of {log_file}: {e}")
    
    return entries


def check_output_file_exists(entry: Dict, log_file: Path) -> bool:
    """
    Check if an output file exists for a given log entry.
    
    Args:
        entry: Log entry dictionary
        log_file: Path to the log file (to determine analysis type)
        
    Returns:
        True if output file exists, False otherwise
    """
    cao_number = entry.get('cao_number', '')
    filename = entry.get('filename', '')
    analysis_type = entry.get('analysis_type', '')
    
    # Determine output path based on analysis type
    base_dir = Path('outputs/llm_analysis')
    
    if analysis_type == 'salary':
        output_file = base_dir / 'salary' / cao_number / filename.replace('_extract.json', '_analysis.json')
    elif analysis_type == 'non_salary_part1':
        output_file = base_dir / 'non_salary' / 'gen_bon_wag_pen_ter' / cao_number / filename.replace('_extract.json', '_analysis.json')
    elif analysis_type == 'non_salary_part2':
        output_file = base_dir / 'non_salary' / 'lea_ove_tra' / cao_number / filename.replace('_extract.json', '_analysis.json')
    elif analysis_type == 'non_salary_part3':
        output_file = base_dir / 'non_salary' / 'hom_con_saf_chi_ai_fri' / cao_number / filename.replace('_extract.json', '_analysis.json')
    else:
        return False
    
    return output_file.exists()


def clean_log_file(log_file: Path, dry_run: bool = False) -> Tuple[int, int]:
    """
    Clean stale failed entries from a log file.
    
    Removes failed entries for files that have successful entries or output files.
    
    Args:
        log_file: Path to the log file to clean
        dry_run: If True, only report what would be deleted without deleting
        
    Returns:
        Tuple of (entries_removed, entries_kept)
    """
    entries = load_log_entries(log_file)
    
    if not entries:
        print(f"No entries found in {log_file}")
        return 0, 0
    
    # Group entries by (filename, cao_number, analysis_type)
    entry_groups = defaultdict(list)
    for entry in entries:
        key = (
            entry.get('filename', ''),
            entry.get('cao_number', ''),
            entry.get('analysis_type', '')
        )
        entry_groups[key].append(entry)
    
    entries_to_keep = []
    entries_to_remove = []
    
    for key, group_entries in entry_groups.items():
        # Check if any entry in the group is successful
        has_success = any(e.get('success', False) for e in group_entries)
        
        # Check if output file exists
        has_output = check_output_file_exists(group_entries[0], log_file)
        
        if has_success:
            # Keep only the most recent successful entry
            successful_entries = [e for e in group_entries if e.get('success', False)]
            if successful_entries:
                # Sort by timestamp, keep most recent
                successful_entries.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
                entries_to_keep.append(successful_entries[0])
                # Remove all failed entries and older successful entries
                for entry in group_entries:
                    if entry != successful_entries[0]:
                        entries_to_remove.append(entry)
        elif has_output:
            # File exists but no successful log entry - keep the most recent entry
            group_entries.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            entries_to_keep.append(group_entries[0])
            entries_to_remove.extend(group_entries[1:])
        else:
            # No success and no output - keep all entries (they're legitimate failures)
            entries_to_keep.extend(group_entries)
    
    if dry_run:
        print(f"\nDRY RUN - {log_file}:")
        print(f"  Would remove: {len(entries_to_remove)} entries")
        print(f"  Would keep: {len(entries_to_keep)} entries")
        if entries_to_remove:
            print(f"\n  Entries that would be removed:")
            for entry in entries_to_remove[:10]:
                print(f"    {entry.get('cao_number')}/{entry.get('filename')}: {entry.get('error_message', 'No error')[:60]}")
        return len(entries_to_remove), len(entries_to_keep)
    
    # Write back cleaned entries
    if entries_to_remove:
        # Sort kept entries by timestamp to maintain chronological order
        entries_to_keep.sort(key=lambda x: x.get('timestamp', ''))
        
        with open(log_file, 'w', encoding='utf-8') as f:
            for entry in entries_to_keep:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        print(f"Cleaned {log_file}:")
        print(f"  Removed: {len(entries_to_remove)} stale entries")
        print(f"  Kept: {len(entries_to_keep)} entries")
    
    return len(entries_to_remove), len(entries_to_keep)


def main():
    """Main entry point for the cleanup script."""
    parser = argparse.ArgumentParser(description='Clean up stale failed entries in analysis performance logs')
    parser.add_argument('--log-file', type=str, help='Specific log file to clean (default: all analysis logs)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    if args.log_file:
        log_files = [Path(args.log_file)]
    else:
        # Default: clean all analysis log files
        log_dir = Path('performance_logs/llm_analysis')
        log_files = [
            log_dir / 'analysis_performance_salary.jsonl',
            log_dir / 'analysis_performance_non_salary1.jsonl',
            log_dir / 'analysis_performance_non_salary2.jsonl',
            log_dir / 'analysis_performance_non_salary3.jsonl',
        ]
    
    print("Cleaning analysis performance logs...")
    if args.dry_run:
        print("(DRY RUN - no files will be modified)")
    
    total_removed = 0
    total_kept = 0
    
    for log_file in log_files:
        if not log_file.exists():
            print(f"Skipping {log_file} (does not exist)")
            continue
        
        removed, kept = clean_log_file(log_file, dry_run=args.dry_run)
        total_removed += removed
        total_kept += kept
    
    print(f"\nSummary:")
    print(f"  Total entries removed: {total_removed}")
    print(f"  Total entries kept: {total_kept}")
    
    if args.dry_run:
        print("\nRun without --dry-run to apply changes")


if __name__ == '__main__':
    main()

