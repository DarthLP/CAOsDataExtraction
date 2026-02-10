"""
P4 Token Usage Analysis Script
===============================

DESCRIPTION:
This script analyzes token usage from p4 analysis performance logs to calculate:
- Average input tokens per API call
- Average output tokens per API call
- Average total tokens per file (sum of all 4 parts: salary + 3 non-salary)
- Total token usage statistics
- Cost estimation for paid API access

The p4 pipeline makes 4 API calls per file:
1. Salary extraction
2. Non-salary part 1 (General, Bonuses, Wage Scales, Pension, Termination)
3. Non-salary part 2 (Leave, Overtime, Training)
4. Non-salary part 3 (Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits)

USAGE:
    python scripts/analyze_p4_token_usage.py
    
    # Analyze specific log directory
    python scripts/analyze_p4_token_usage.py --log-dir performance_logs/llm_analysis/_old
"""

import json
import statistics
from pathlib import Path
from typing import Dict, List, Tuple
import argparse


def load_log_file(log_path: Path) -> List[Dict]:
    """
    Load and parse a JSONL log file.
    
    Args:
        log_path: Path to the JSONL log file
        
    Returns:
        List of parsed JSON objects
    """
    data = []
    if not log_path.exists():
        print(f"  ⚠️  Log file not found: {log_path}")
        return data
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"  ⚠️  Skipping invalid JSON line: {e}")
    except Exception as e:
        print(f"  ⚠️  Error reading {log_path}: {e}")
    
    return data


def analyze_token_usage(log_dir: Path) -> Dict:
    """
    Analyze token usage from all p4 analysis log files.
    
    Args:
        log_dir: Directory containing the log files
        
    Returns:
        Dictionary with analysis results
    """
    # Define log files
    log_files = {
        'salary': log_dir / 'analysis_performance_salary.jsonl',
        'non_salary1': log_dir / 'analysis_performance_non_salary1.jsonl',
        'non_salary2': log_dir / 'analysis_performance_non_salary2.jsonl',
        'non_salary3': log_dir / 'analysis_performance_non_salary3.jsonl',
    }
    
    # Load all log files
    all_data = {}
    for part_name, log_path in log_files.items():
        print(f"📂 Loading {part_name} log...")
        data = load_log_file(log_path)
        # Filter to only successful entries with token data
        data = [d for d in data if d.get('success', False) and d.get('input_tokens', 0) > 0]
        all_data[part_name] = data
        print(f"  ✓ Loaded {len(data)} entries with token data")
    
    # Calculate per-part statistics
    part_stats = {}
    for part_name, data in all_data.items():
        if not data:
            continue
        
        input_tokens = [d.get('input_tokens', 0) for d in data]
        output_tokens = [d.get('output_tokens', 0) for d in data]
        total_tokens = [d.get('total_tokens', 0) for d in data]
        
        part_stats[part_name] = {
            'count': len(data),
            'avg_input_tokens': statistics.mean(input_tokens),
            'median_input_tokens': statistics.median(input_tokens),
            'min_input_tokens': min(input_tokens),
            'max_input_tokens': max(input_tokens),
            'avg_output_tokens': statistics.mean(output_tokens),
            'median_output_tokens': statistics.median(output_tokens),
            'min_output_tokens': min(output_tokens),
            'max_output_tokens': max(output_tokens),
            'avg_total_tokens': statistics.mean(total_tokens),
            'median_total_tokens': statistics.median(total_tokens),
            'total_input_tokens': sum(input_tokens),
            'total_output_tokens': sum(output_tokens),
            'total_tokens': sum(total_tokens),
        }
    
    # Calculate overall statistics (all API calls combined)
    all_input_tokens = []
    all_output_tokens = []
    all_total_tokens = []
    
    for data in all_data.values():
        for entry in data:
            all_input_tokens.append(entry.get('input_tokens', 0))
            all_output_tokens.append(entry.get('output_tokens', 0))
            all_total_tokens.append(entry.get('total_tokens', 0))
    
    overall_stats = {
        'total_api_calls': len(all_input_tokens),
        'avg_input_tokens': statistics.mean(all_input_tokens) if all_input_tokens else 0,
        'median_input_tokens': statistics.median(all_input_tokens) if all_input_tokens else 0,
        'min_input_tokens': min(all_input_tokens) if all_input_tokens else 0,
        'max_input_tokens': max(all_input_tokens) if all_input_tokens else 0,
        'avg_output_tokens': statistics.mean(all_output_tokens) if all_output_tokens else 0,
        'median_output_tokens': statistics.median(all_output_tokens) if all_output_tokens else 0,
        'min_output_tokens': min(all_output_tokens) if all_output_tokens else 0,
        'max_output_tokens': max(all_output_tokens) if all_output_tokens else 0,
        'avg_total_tokens': statistics.mean(all_total_tokens) if all_total_tokens else 0,
        'median_total_tokens': statistics.median(all_total_tokens) if all_total_tokens else 0,
        'total_input_tokens': sum(all_input_tokens),
        'total_output_tokens': sum(all_output_tokens),
        'total_tokens': sum(all_total_tokens),
    }
    
    # Calculate per-file statistics (group by filename + cao_number)
    # This gives us the total tokens used per file (sum of all 4 parts)
    file_totals = {}
    for part_name, data in all_data.items():
        for entry in data:
            key = (entry.get('filename', ''), entry.get('cao_number', ''))
            if key not in file_totals:
                file_totals[key] = {
                    'input_tokens': 0,
                    'output_tokens': 0,
                    'total_tokens': 0,
                    'parts': []
                }
            file_totals[key]['input_tokens'] += entry.get('input_tokens', 0)
            file_totals[key]['output_tokens'] += entry.get('output_tokens', 0)
            file_totals[key]['total_tokens'] += entry.get('total_tokens', 0)
            file_totals[key]['parts'].append(part_name)
    
    per_file_stats = {
        'total_files': len(file_totals),
        'avg_input_tokens_per_file': statistics.mean([f['input_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'median_input_tokens_per_file': statistics.median([f['input_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'avg_output_tokens_per_file': statistics.mean([f['output_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'median_output_tokens_per_file': statistics.median([f['output_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'avg_total_tokens_per_file': statistics.mean([f['total_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'median_total_tokens_per_file': statistics.median([f['total_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'min_total_tokens_per_file': min([f['total_tokens'] for f in file_totals.values()]) if file_totals else 0,
        'max_total_tokens_per_file': max([f['total_tokens'] for f in file_totals.values()]) if file_totals else 0,
    }
    
    return {
        'part_stats': part_stats,
        'overall_stats': overall_stats,
        'per_file_stats': per_file_stats,
    }


def print_analysis(results: Dict, input_price: float = 0.30, output_price: float = 2.50, num_files: int = None):
    """
    Print formatted analysis results.
    
    Args:
        results: Analysis results dictionary
        input_price: Price per 1M input tokens (default: $0.30)
        output_price: Price per 1M output tokens (default: $2.50)
        num_files: Number of files to estimate cost for (default: None, uses actual file count)
    """
    print("\n" + "="*80)
    print("📊 P4 TOKEN USAGE ANALYSIS")
    print("="*80)
    
    # Overall statistics (per API call)
    overall = results['overall_stats']
    print(f"\n🔹 OVERALL STATISTICS (Per API Call):")
    print(f"   Total API calls: {overall['total_api_calls']:,}")
    print(f"\n   Input Tokens:")
    print(f"     Average: {overall['avg_input_tokens']:,.0f}")
    print(f"     Median:  {overall['median_input_tokens']:,.0f}")
    print(f"     Range:   {overall['min_input_tokens']:,} - {overall['max_input_tokens']:,}")
    print(f"     Total:   {overall['total_input_tokens']:,}")
    print(f"\n   Output Tokens:")
    print(f"     Average: {overall['avg_output_tokens']:,.0f}")
    print(f"     Median:  {overall['median_output_tokens']:,.0f}")
    print(f"     Range:   {overall['min_output_tokens']:,} - {overall['max_output_tokens']:,}")
    print(f"     Total:   {overall['total_output_tokens']:,}")
    print(f"\n   Total Tokens (Input + Output):")
    print(f"     Average: {overall['avg_total_tokens']:,.0f}")
    print(f"     Median:  {overall['median_total_tokens']:,.0f}")
    print(f"     Total:   {overall['total_tokens']:,}")
    
    # Per-file statistics
    per_file = results['per_file_stats']
    print(f"\n🔹 PER-FILE STATISTICS (Sum of all 4 parts per file):")
    print(f"   Total files analyzed: {per_file['total_files']:,}")
    print(f"\n   Input Tokens per File:")
    print(f"     Average: {per_file['avg_input_tokens_per_file']:,.0f}")
    print(f"     Median:  {per_file['median_input_tokens_per_file']:,.0f}")
    print(f"\n   Output Tokens per File:")
    print(f"     Average: {per_file['avg_output_tokens_per_file']:,.0f}")
    print(f"     Median:  {per_file['median_output_tokens_per_file']:,.0f}")
    print(f"\n   Total Tokens per File:")
    print(f"     Average: {per_file['avg_total_tokens_per_file']:,.0f}")
    print(f"     Median:  {per_file['median_total_tokens_per_file']:,.0f}")
    print(f"     Range:   {per_file['min_total_tokens_per_file']:,} - {per_file['max_total_tokens_per_file']:,}")
    
    # Per-part breakdown
    print(f"\n🔹 BREAKDOWN BY PART:")
    part_names = {
        'salary': 'Salary',
        'non_salary1': 'Non-Salary Part 1',
        'non_salary2': 'Non-Salary Part 2',
        'non_salary3': 'Non-Salary Part 3',
    }
    
    for part_key, part_name in part_names.items():
        if part_key in results['part_stats']:
            stats = results['part_stats'][part_key]
            print(f"\n   {part_name}:")
            print(f"     API calls: {stats['count']:,}")
            print(f"     Avg input:  {stats['avg_input_tokens']:,.0f} tokens")
            print(f"     Avg output: {stats['avg_output_tokens']:,.0f} tokens")
            print(f"     Avg total:  {stats['avg_total_tokens']:,.0f} tokens")
    
    # Cost estimation
    per_file = results['per_file_stats']
    estimate_files = num_files if num_files else per_file['total_files']
    
    print(f"\n🔹 COST ESTIMATION:")
    print(f"   Pricing:")
    print(f"     Input:  ${input_price:.2f} per 1M tokens")
    print(f"     Output: ${output_price:.2f} per 1M tokens")
    print(f"\n   Estimated cost per file (average):")
    avg_input_cost = (per_file['avg_input_tokens_per_file'] / 1_000_000) * input_price
    avg_output_cost = (per_file['avg_output_tokens_per_file'] / 1_000_000) * output_price
    avg_total_cost = avg_input_cost + avg_output_cost
    print(f"     Input cost:  ${avg_input_cost:.4f}")
    print(f"     Output cost: ${avg_output_cost:.4f}")
    print(f"     Total cost:  ${avg_total_cost:.4f}")
    
    if estimate_files != per_file['total_files']:
        print(f"\n   Estimated cost for {estimate_files:,} files:")
        total_input_tokens = per_file['avg_input_tokens_per_file'] * estimate_files
        total_output_tokens = per_file['avg_output_tokens_per_file'] * estimate_files
        total_input_cost = (total_input_tokens / 1_000_000) * input_price
        total_output_cost = (total_output_tokens / 1_000_000) * output_price
        total_cost = total_input_cost + total_output_cost
        print(f"     Total input tokens:  {total_input_tokens:,}")
        print(f"     Total output tokens: {total_output_tokens:,}")
        print(f"     Input cost:  ${total_input_cost:,.2f}")
        print(f"     Output cost: ${total_output_cost:,.2f}")
        print(f"     Total cost:  ${total_cost:,.2f}")
    
    print(f"\n   Actual cost for analyzed files ({per_file['total_files']:,} files):")
    total_input_cost = (overall['total_input_tokens'] / 1_000_000) * input_price
    total_output_cost = (overall['total_output_tokens'] / 1_000_000) * output_price
    total_cost = total_input_cost + total_output_cost
    print(f"     Input cost:  ${total_input_cost:,.2f}")
    print(f"     Output cost: ${total_output_cost:,.2f}")
    print(f"     Total cost:  ${total_cost:,.2f}")
    
    print("\n" + "="*80)


def main():
    """Main function to run the analysis."""
    parser = argparse.ArgumentParser(description='Analyze p4 token usage from performance logs')
    parser.add_argument(
        '--log-dir',
        type=str,
        default='performance_logs/llm_analysis/_old',
        help='Directory containing the log files (default: performance_logs/llm_analysis/_old)'
    )
    parser.add_argument(
        '--input-price',
        type=float,
        default=0.30,
        help='Price per 1M input tokens (default: 0.30)'
    )
    parser.add_argument(
        '--output-price',
        type=float,
        default=2.50,
        help='Price per 1M output tokens (default: 2.50)'
    )
    parser.add_argument(
        '--num-files',
        type=int,
        default=None,
        help='Number of files to estimate cost for (default: None, uses actual file count)'
    )
    
    args = parser.parse_args()
    
    log_dir = Path(args.log_dir)
    
    if not log_dir.exists():
        print(f"❌ Error: Log directory not found: {log_dir}")
        print(f"   Please specify the correct path with --log-dir")
        return
    
    print(f"📁 Analyzing logs in: {log_dir}")
    
    try:
        results = analyze_token_usage(log_dir)
        print_analysis(results, args.input_price, args.output_price, args.num_files)
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
