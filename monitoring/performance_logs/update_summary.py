"""
Update Extraction Summary Script
================================

This script updates the extraction summary from existing performance logs.
Use this when you stop the extraction process and want to see the current status.

USAGE:
    python performance_logs/update_summary.py
"""
import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from monitoring.monitoring_3_1 import PerformanceMonitor


def main():
    """Update the extraction summary from existing logs"""
    print('🔄 Updating extraction summary from performance logs...')
    monitor = PerformanceMonitor(log_file=
        'performance_logs/extraction_performance.jsonl', summary_file=
        'performance_logs/extraction_summary.json')
    if not os.path.exists(monitor.log_file):
        print(f'❌ No performance log found at: {monitor.log_file}')
        print('   Run the extraction script first to generate logs.')
        return
    data = monitor.get_performance_data()
    if not data:
        print('❌ No performance data found in log file.')
        return
    print(f'✅ Found {len(data)} extraction records in log file.')
    monitor.update_summary_file()
    monitor.print_summary()
    print('\n' + '=' * 50)
    print('PROGRESS TOWARDS 1,580 FILES')
    print('=' * 50)
    monitor.print_progress(1580)
    print('\n' + '=' * 50)
    print('DETAILED PERFORMANCE ANALYSIS')
    print('=' * 50)
    monitor.analyze_performance()
    print(f'\n✅ Summary updated successfully!')
    print(f'📁 Files:')
    print(f'   - Logs: {monitor.log_file}')
    print(f'   - Summary: {monitor.summary_file}')


if __name__ == '__main__':
    main()
