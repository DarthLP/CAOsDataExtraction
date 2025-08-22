"""
Cleanup Duplicate Performance Logs
==================================

This script removes duplicate entries from the performance logs.
Use this if you've re-run files and got duplicate log entries.

USAGE:
    python performance_logs/cleanup_duplicates.py
"""
import json
import os
from datetime import datetime
from pathlib import Path


def cleanup_duplicates():
    """Remove duplicate entries from performance logs"""
    log_file = 'performance_logs/extraction_performance.jsonl'
    backup_file = (
        f"performance_logs/extraction_performance_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        )
    if not os.path.exists(log_file):
        print(f'❌ No log file found at: {log_file}')
        return
    print(f'🧹 Cleaning up duplicate entries in: {log_file}')
    with open(log_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    if not lines:
        print('✅ Log file is empty, nothing to clean.')
        return
    print(f'📊 Found {len(lines)} total entries')
    entries = []
    for line_num, line in enumerate(lines, 1):
        if line.strip():
            try:
                entry = json.loads(line)
                entries.append(entry)
            except json.JSONDecodeError as e:
                print(f'⚠️  Warning: Invalid JSON on line {line_num}: {e}')
    print(f'💾 Creating backup: {backup_file}')
    with open(backup_file, 'w', encoding='utf-8') as f:
        for entry in entries:
            f.write(json.dumps(entry) + '\n')
    unique_entries = {}
    for entry in entries:
        filename = entry.get('filename', '')
        cao_number = entry.get('cao_number', '')
        timestamp = entry.get('timestamp', '')
        key = filename, cao_number
        if key not in unique_entries:
            unique_entries[key] = entry
        else:
            existing_timestamp = unique_entries[key].get('timestamp', '')
            if timestamp > existing_timestamp:
                unique_entries[key] = entry
    unique_list = list(unique_entries.values())
    duplicates_removed = len(entries) - len(unique_list)
    print(f'🔍 Analysis:')
    print(f'   Original entries: {len(entries)}')
    print(f'   Unique entries: {len(unique_list)}')
    print(f'   Duplicates removed: {duplicates_removed}')
    if duplicates_removed == 0:
        print('✅ No duplicates found! Log file is clean.')
        os.remove(backup_file)
        return
    with open(log_file, 'w', encoding='utf-8') as f:
        for entry in sorted(unique_list, key=lambda x: x.get('timestamp', '')):
            f.write(json.dumps(entry) + '\n')
    print(f'✅ Cleanup completed!')
    print(f'   Duplicates removed: {duplicates_removed}')
    print(f'   Clean entries: {len(unique_list)}')
    print(f'   Backup saved: {backup_file}')
    print(f'🔄 Updating summary file...')
    try:
        import sys
        sys.path.append(str(Path(__file__).parent.parent))
        from monitoring.monitoring_3_1 import PerformanceMonitor
        monitor = PerformanceMonitor(log_file=log_file, summary_file=
            'performance_logs/extraction_summary.json')
        monitor.update_summary_file()
        print(f'✅ Summary updated!')
    except ImportError:
        print(f'⚠️  Could not update summary - run update_summary.py manually')


if __name__ == '__main__':
    cleanup_duplicates()
