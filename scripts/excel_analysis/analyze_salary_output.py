"""
CAO Salary Data Analysis Script

This script performs comprehensive analysis of the salary Excel output data,
including cross-sectional statistics, longitudinal trends, temporal analysis,
boolean cross-tabulations, amount normalization, and salary-specific insights.

USAGE:
    python scripts/excel_analysis/analyze_salary_output.py
    
    With file limit:
        python scripts/excel_analysis/analyze_salary_output.py --max_files 1000

ARGUMENTS:
    --max_files: Maximum number of rows to process (optional)

INPUT:
    - outputs/excel/new_results/extracted_data_salary.csv
    - pdfs/input_pdfs/extracted_cao_info.csv (for CAO metadata)

OUTPUT:
    - Console summary with key findings
    - CSV reports in outputs/comparison_results/
    - JSON summary with key insights
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import analysis utilities
from scripts.excel_analysis.analysis_utils import (
    convert_salary_to_monthly, calculate_descriptive_stats, create_boolean_summary,
    create_categorical_summary, group_cao_timeline, calculate_longitudinal_changes,
    create_crosstab_summary, extract_year_from_date, identify_salary_columns
)


def load_salary_data(salary_csv_path: str, cao_info_path: str, max_files: Optional[int] = None) -> pd.DataFrame:
    """
    Load and prepare salary data for analysis.
    
    Args:
        salary_csv_path: Path to salary CSV file
        cao_info_path: Path to CAO info CSV file
        max_files: Maximum number of rows to process
        
    Returns:
        Prepared DataFrame with salary data
    """
    print(f"Loading salary data from: {salary_csv_path}")
    
    # Load salary data
    df = pd.read_csv(salary_csv_path, sep=';', encoding='utf-8')
    print(f"  Loaded {len(df)} salary rows")
    
    # Apply file limit if specified
    if max_files and max_files < len(df):
        df = df.head(max_files)
        print(f"  Limited to {len(df)} rows due to max_files limit")
    
    # Load CAO info for additional metadata
    cao_info = {}
    if os.path.exists(cao_info_path):
        try:
            cao_df = pd.read_csv(cao_info_path, sep=';', encoding='utf-8')
            for _, row in cao_df.iterrows():
                cao_number = str(row.get('cao_number', ''))
                pdf_name = str(row.get('pdf_name', ''))
                if cao_number and pdf_name:
                    key = f"{cao_number}:{pdf_name}"
                    cao_info[key] = {
                        'sbi_code': str(row.get('sbi_code', '')),
                        'sector': str(row.get('sector', ''))
                    }
            print(f"  Loaded CAO info for {len(cao_info)} entries")
        except Exception as e:
            print(f"  Warning: Could not load CAO info: {e}")
    
    return df, cao_info


def perform_cross_sectional_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform cross-sectional analysis on salary data.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary with cross-sectional analysis results
    """
    print("Performing cross-sectional analysis...")
    
    # Identify column types
    col_types = identify_salary_columns(df)
    
    results = {
        'dataset_overview': {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'unique_caos': df['cao_number'].nunique(),
            'date_range': {
                'earliest': df['ingangsdatum'].min(),
                'latest': df['ingangsdatum'].max()
            }
        },
        'column_analysis': {}
    }
    
    # Analyze each column type
    for col_type, columns in col_types.items():
        if not columns:
            continue
            
        results['column_analysis'][col_type] = {}
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if col_type == 'boolean':
                results['column_analysis'][col_type][col] = create_boolean_summary(df[col])
            elif col_type in ['numeric', 'salary_amounts']:
                results['column_analysis'][col_type][col] = calculate_descriptive_stats(df[col])
            elif col_type == 'categorical':
                results['column_analysis'][col_type][col] = create_categorical_summary(df[col])
            else:
                # For other types, just basic info
                results['column_analysis'][col_type][col] = {
                    'count': df[col].count(),
                    'missing_count': df[col].isna().sum(),
                    'unique_count': df[col].nunique()
                }
    
    return results


def perform_longitudinal_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform longitudinal analysis on salary data.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary with longitudinal analysis results
    """
    print("Performing longitudinal analysis...")
    
    # Group CAOs by timeline
    cao_groups = group_cao_timeline(df)
    
    # Calculate timeline statistics
    timeline_stats = {}
    for cao_num, group in cao_groups.items():
        timeline_stats[cao_num] = {
            'periods_count': len(group),
            'date_range': {
                'start': group['ingangsdatum'].min(),
                'end': group['ingangsdatum'].max()
            },
            'unique_files': group['file_name'].nunique()
        }
    
    # Identify numeric columns for change analysis
    col_types = identify_salary_columns(df)
    numeric_cols = col_types['salary_amounts'] + col_types['numeric']
    
    # Calculate longitudinal changes
    longitudinal_changes = calculate_longitudinal_changes(cao_groups, numeric_cols)
    
    # Summary statistics
    periods_distribution = {}
    for cao_num, stats in timeline_stats.items():
        periods = stats['periods_count']
        periods_distribution[periods] = periods_distribution.get(periods, 0) + 1
    
    return {
        'cao_timeline_stats': timeline_stats,
        'longitudinal_changes': longitudinal_changes,
        'periods_distribution': periods_distribution,
        'caos_with_multiple_periods': len([s for s in timeline_stats.values() if s['periods_count'] > 1])
    }


def perform_temporal_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform temporal trend analysis on salary data.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary with temporal analysis results
    """
    print("Performing temporal analysis...")
    
    # Extract year from ingangsdatum
    df_copy = df.copy()
    df_copy['year'] = extract_year_from_date(df_copy['ingangsdatum'])
    
    # Group by year
    yearly_stats = {}
    for year, year_group in df_copy.groupby('year'):
        if pd.isna(year):
            continue
            
        yearly_stats[int(year)] = {
            'cao_count': year_group['cao_number'].nunique(),
            'row_count': len(year_group),
            'unique_files': year_group['file_name'].nunique()
        }
    
    # Analyze salary amounts by year (if available)
    col_types = identify_salary_columns(df)
    salary_amount_cols = col_types['salary_amounts']
    
    yearly_salary_stats = {}
    for year, year_group in df_copy.groupby('year'):
        if pd.isna(year) or len(salary_amount_cols) == 0:
            continue
            
        year_salaries = {}
        for col in salary_amount_cols:
            if col in year_group.columns:
                clean_values = year_group[col].dropna()
                if len(clean_values) > 0:
                    year_salaries[col] = {
                        'count': len(clean_values),
                        'mean': clean_values.mean(),
                        'median': clean_values.median(),
                        'min': clean_values.min(),
                        'max': clean_values.max()
                    }
        
        if year_salaries:
            yearly_salary_stats[int(year)] = year_salaries
    
    return {
        'yearly_overview': yearly_stats,
        'yearly_salary_stats': yearly_salary_stats,
        'year_range': {
            'earliest': df_copy['year'].min(),
            'latest': df_copy['year'].max()
        }
    }


def perform_boolean_crosstabs(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform cross-tabulation analysis for boolean fields.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary with cross-tabulation results
    """
    print("Performing boolean cross-tabulation analysis...")
    
    # Identify boolean columns
    col_types = identify_salary_columns(df)
    bool_cols = col_types['boolean']
    
    if not bool_cols:
        return {'message': 'No boolean columns found'}
    
    # Prepare grouping variables
    df_copy = df.copy()
    df_copy['year'] = extract_year_from_date(df_copy['ingangsdatum'])
    df_copy['ttw_status'] = df_copy['TTW'].fillna('unknown')
    
    results = {}
    
    # Cross-tabulate each boolean variable
    for bool_col in bool_cols:
        if bool_col not in df.columns:
            continue
            
        results[bool_col] = {}
        
        # By year
        if 'year' in df_copy.columns:
            results[bool_col]['by_year'] = create_crosstab_summary(df_copy, bool_col, 'year')
        
        # By TTW status
        if 'ttw_status' in df_copy.columns:
            results[bool_col]['by_ttw'] = create_crosstab_summary(df_copy, bool_col, 'ttw_status')
    
    return results


def perform_amount_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform analysis of salary amounts with unit normalization.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary with amount analysis results
    """
    print("Performing amount analysis with unit normalization...")
    
    col_types = identify_salary_columns(df)
    salary_amount_cols = col_types['salary_amounts']
    salary_unit_cols = col_types['salary_units']
    
    if not salary_amount_cols:
        return {'message': 'No salary amount columns found'}
    
    results = {
        'raw_analysis': {},
        'normalized_analysis': {},
        'unit_distribution': {}
    }
    
    # Analyze each salary amount column
    for amount_col in salary_amount_cols:
        # Find corresponding unit column
        unit_col = amount_col.replace('_amount', '_unit')
        
        if unit_col not in df.columns:
            continue
            
        # Raw analysis
        results['raw_analysis'][amount_col] = calculate_descriptive_stats(df[amount_col])
        
        # Unit distribution
        unit_counts = df[unit_col].value_counts()
        results['unit_distribution'][amount_col] = unit_counts.to_dict()
        
        # Normalized analysis (convert to monthly)
        normalized_amounts = []
        ft_hours_col = 'ft_hours' if 'ft_hours' in df.columns else None
        
        for idx, row in df.iterrows():
            amount = row[amount_col]
            unit = row[unit_col]
            ft_hours = row[ft_hours_col] if ft_hours_col else None
            
            if pd.notna(amount) and pd.notna(unit):
                normalized = convert_salary_to_monthly(amount, unit, ft_hours)
                if normalized is not None:
                    normalized_amounts.append(normalized)
        
        if normalized_amounts:
            normalized_series = pd.Series(normalized_amounts)
            results['normalized_analysis'][amount_col] = calculate_descriptive_stats(normalized_series)
    
    return results


def perform_salary_specific_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform salary-specific analysis.
    
    Args:
        df: DataFrame with salary data
        
    Returns:
        Dictionary with salary-specific analysis results
    """
    print("Performing salary-specific analysis...")
    
    results = {}
    
    # Job group distribution
    if 'jobgroup' in df.columns:
        # Handle missing values explicitly
        jobgroup_series = df['jobgroup'].fillna('MISSING_JOBGROUP')
        jobgroup_dist = jobgroup_series.value_counts()
        
        # Count actual missing values
        missing_count = df['jobgroup'].isna().sum()
        
        results['jobgroup_distribution'] = {
            'all_groups': jobgroup_dist.to_dict(),  # Capture ALL job groups including missing
            'top_10': jobgroup_dist.head(10).to_dict(),
            'total_unique': jobgroup_dist.nunique(),
            'total_entries': len(jobgroup_dist),
            'missing_count': missing_count,
            'missing_pct': (missing_count / len(df)) * 100 if len(df) > 0 else 0
        }
    
    # Worker type distribution
    if 'worker_type' in df.columns:
        # Handle missing values explicitly
        worker_type_series = df['worker_type'].fillna('MISSING_WORKER_TYPE')
        worker_type_dist = worker_type_series.value_counts()
        
        # Count actual missing values
        missing_count = df['worker_type'].isna().sum()
        
        results['worker_type_distribution'] = {
            'all_types': worker_type_dist.to_dict(),  # Capture ALL worker types including missing
            'top_10': worker_type_dist.head(10).to_dict(),
            'total_unique': worker_type_dist.nunique(),
            'total_entries': len(worker_type_dist),
            'missing_count': missing_count,
            'missing_pct': (missing_count / len(df)) * 100 if len(df) > 0 else 0
        }
    
    # Timeline length analysis
    col_types = identify_salary_columns(df)
    salary_amount_cols = col_types['salary_amounts']
    
    if salary_amount_cols:
        # Count how many salary points each row has
        timeline_lengths = []
        for idx, row in df.iterrows():
            count = 0
            for col in salary_amount_cols:
                if pd.notna(row[col]):
                    count += 1
            timeline_lengths.append(count)
        
        timeline_series = pd.Series(timeline_lengths)
        results['timeline_length_analysis'] = {
            'distribution': timeline_series.value_counts().to_dict(),
            'stats': calculate_descriptive_stats(timeline_series)
        }
    
    # Age group coverage
    if 'age_group' in df.columns:
        age_group_dist = df['age_group'].value_counts()
        results['age_group_coverage'] = {
            'distribution': age_group_dist.to_dict(),
            'total_unique': age_group_dist.nunique(),
            'coverage_pct': (df['age_group'].notna().sum() / len(df)) * 100
        }
    
    # Contract type patterns
    if 'permanency' in df.columns:
        permanency_dist = df['permanency'].value_counts()
        results['contract_type_patterns'] = {
            'permanency_distribution': permanency_dist.to_dict(),
            'coverage_pct': (df['permanency'].notna().sum() / len(df)) * 100
        }
    
    if 'hours_type' in df.columns:
        hours_type_dist = df['hours_type'].value_counts()
        results['hours_type_patterns'] = {
            'distribution': hours_type_dist.to_dict(),
            'coverage_pct': (df['hours_type'].notna().sum() / len(df)) * 100
        }
    
    return results


def save_results(results: Dict[str, Any], output_dir: Path) -> None:
    """
    Save analysis results to consolidated CSV files.
    
    Args:
        results: Analysis results dictionary
        output_dir: Output directory path
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. SUMMARY FILE - Key metrics and overview
    summary_data = {
        'metric': ['analysis_timestamp', 'total_caos', 'total_rows', 'caos_with_multiple_periods', 
                  'earliest_year', 'latest_year', 'unique_job_groups', 'avg_timeline_length'],
        'value': [
            datetime.now().isoformat(),
            results.get('cross_sectional', {}).get('dataset_overview', {}).get('unique_caos', 0),
            results.get('cross_sectional', {}).get('dataset_overview', {}).get('total_rows', 0),
            results.get('longitudinal', {}).get('caos_with_multiple_periods', 0),
            results.get('temporal', {}).get('year_range', {}).get('earliest', 'N/A'),
            results.get('temporal', {}).get('year_range', {}).get('latest', 'N/A'),
            results.get('salary_specific', {}).get('jobgroup_distribution', {}).get('total_unique', 0),
            results.get('salary_specific', {}).get('timeline_length_analysis', {}).get('stats', {}).get('mean', 0)
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_dir / 'salary_analysis_summary.csv', index=False, sep=';')
    
    # 2. DESCRIPTIVE STATS - All variables with basic statistics
    if 'cross_sectional' in results:
        # Flatten the nested column analysis structure
        all_stats = []
        for col_type, columns in results['cross_sectional']['column_analysis'].items():
            for col_name, stats in columns.items():
                if isinstance(stats, dict):
                    row = {'variable': col_name, 'type': col_type}
                    # Clean up the stats dictionary to avoid mixed data types
                    for key, value in stats.items():
                        if pd.isna(value) or value is None:
                            row[key] = ''
                        elif isinstance(value, (int, float)):
                            row[key] = value
                        else:
                            row[key] = str(value)
                    all_stats.append(row)
        
        if all_stats:
            stats_df = pd.DataFrame(all_stats)
            stats_df.to_csv(output_dir / 'salary_descriptive_stats.csv', index=False, sep=';')
    
    # 3. LONGITUDINAL ANALYSIS - CAO changes over time
    if 'longitudinal' in results:
        longitudinal_data = results['longitudinal']
        
        # Periods distribution
        if 'periods_distribution' in longitudinal_data:
            periods_df = pd.DataFrame(list(longitudinal_data['periods_distribution'].items()), 
                                    columns=['periods_count', 'cao_count'])
            periods_df.to_csv(output_dir / 'salary_longitudinal_analysis.csv', index=False, sep=';')
        
        # Longitudinal changes (separate file)
        if 'longitudinal_changes' in longitudinal_data:
            changes_data = []
            for cao_num, cao_changes in longitudinal_data['longitudinal_changes'].items():
                for variable, stats in cao_changes.items():
                    # Only include key salary variables
                    if 'amount' in variable.lower() or 'salary' in variable.lower():
                        row = {
                            'cao_number': cao_num, 
                            'variable': variable,
                            'periods_count': stats.get('periods_count', 0),
                            'changes_count': stats.get('changes_count', 0),
                            'avg_change': stats.get('avg_change', 0),
                            'median_change': stats.get('median_change', 0),
                            'std_change': stats.get('std_change', 0),
                            'min_change': stats.get('min_change', 0),
                            'max_change': stats.get('max_change', 0),
                            'positive_changes': stats.get('positive_changes', 0),
                            'negative_changes': stats.get('negative_changes', 0),
                            'zero_changes': stats.get('zero_changes', 0),
                            'unit_normalized': stats.get('unit_normalized', False)
                        }
                        changes_data.append(row)
            
            if changes_data:
                changes_df = pd.DataFrame(changes_data)
                changes_df.to_csv(output_dir / 'salary_longitudinal_changes.csv', index=False, sep=';')
    
    # 4. TEMPORAL TRENDS - Year-over-year analysis
    if 'temporal' in results:
        temporal_data = results['temporal']
        
        # Yearly overview
        if 'yearly_overview' in temporal_data:
            yearly_df = pd.DataFrame(temporal_data['yearly_overview']).T
            yearly_df.index.name = 'year'
            yearly_df.to_csv(output_dir / 'salary_temporal_trends.csv', sep=';')
    
    # 5. SALARY-SPECIFIC INSIGHTS - Job groups, worker types, etc.
    if 'salary_specific' in results:
        specific_data = results['salary_specific']
        insights_data = []
        
        # Job group distribution (top 10 for insights file)
        if 'jobgroup_distribution' in specific_data and 'top_10' in specific_data['jobgroup_distribution']:
            for jobgroup, count in specific_data['jobgroup_distribution']['top_10'].items():
                insights_data.append({'category': 'jobgroup', 'name': jobgroup, 'count': count})
        
        # Save ALL job groups to separate file
        if 'jobgroup_distribution' in specific_data and 'all_groups' in specific_data['jobgroup_distribution']:
            all_jobgroups_data = []
            for jobgroup, count in specific_data['jobgroup_distribution']['all_groups'].items():
                all_jobgroups_data.append({'jobgroup': jobgroup, 'count': count})
            
            if all_jobgroups_data:
                all_jobgroups_df = pd.DataFrame(all_jobgroups_data)
                all_jobgroups_df.to_csv(output_dir / 'salary_all_jobgroups.csv', index=False, sep=';')
        
        # Worker type distribution (top 10 for insights file)
        if 'worker_type_distribution' in specific_data and 'top_10' in specific_data['worker_type_distribution']:
            for worker_type, count in specific_data['worker_type_distribution']['top_10'].items():
                insights_data.append({'category': 'worker_type', 'name': worker_type, 'count': count})
        
        # Save ALL worker types to separate file
        if 'worker_type_distribution' in specific_data and 'all_types' in specific_data['worker_type_distribution']:
            all_worker_types_data = []
            for worker_type, count in specific_data['worker_type_distribution']['all_types'].items():
                all_worker_types_data.append({'worker_type': worker_type, 'count': count})
            
            if all_worker_types_data:
                all_worker_types_df = pd.DataFrame(all_worker_types_data)
                all_worker_types_df.to_csv(output_dir / 'salary_all_worker_types.csv', index=False, sep=';')
        
        # Timeline length distribution
        if 'timeline_length_analysis' in specific_data and 'distribution' in specific_data['timeline_length_analysis']:
            for length, count in specific_data['timeline_length_analysis']['distribution'].items():
                insights_data.append({'category': 'timeline_length', 'name': f'{length}_points', 'count': count})
        
        if insights_data:
            insights_df = pd.DataFrame(insights_data)
            insights_df.to_csv(output_dir / 'salary_specific_insights.csv', index=False, sep=';')


def print_summary(results: Dict[str, Any]) -> None:
    """
    Print summary of analysis results to console.
    
    Args:
        results: Analysis results dictionary
    """
    print("\n" + "="*80)
    print("SALARY DATA ANALYSIS SUMMARY")
    print("="*80)
    
    # Dataset overview
    overview = results.get('cross_sectional', {}).get('dataset_overview', {})
    print(f"\nDataset Overview:")
    print(f"  Total rows: {overview.get('total_rows', 0):,}")
    print(f"  Total columns: {overview.get('total_columns', 0)}")
    print(f"  Unique CAOs: {overview.get('unique_caos', 0)}")
    print(f"  Date range: {overview.get('date_range', {}).get('earliest', 'N/A')} to {overview.get('date_range', {}).get('latest', 'N/A')}")
    
    # Longitudinal insights
    longitudinal = results.get('longitudinal', {})
    print(f"\nLongitudinal Analysis:")
    print(f"  CAOs with multiple periods: {longitudinal.get('caos_with_multiple_periods', 0)}")
    
    periods_dist = longitudinal.get('periods_distribution', {})
    if periods_dist:
        print(f"  Periods distribution:")
        for periods, count in sorted(periods_dist.items()):
            print(f"    {periods} period(s): {count} CAOs")
    
    # Temporal insights
    temporal = results.get('temporal', {})
    year_range = temporal.get('year_range', {})
    print(f"\nTemporal Analysis:")
    print(f"  Year range: {year_range.get('earliest', 'N/A')} to {year_range.get('latest', 'N/A')}")
    
    yearly_overview = temporal.get('yearly_overview', {})
    if yearly_overview:
        print(f"  Years with data: {len(yearly_overview)}")
        recent_years = sorted(yearly_overview.keys())[-3:]
        print(f"  Recent years: {', '.join(map(str, recent_years))}")
    
    # Salary-specific insights
    salary_specific = results.get('salary_specific', {})
    if 'jobgroup_distribution' in salary_specific:
        jobgroup_dist = salary_specific['jobgroup_distribution']
        print(f"\nSalary-Specific Analysis:")
        print(f"  Unique job groups: {jobgroup_dist.get('total_unique', 0)}")
        print(f"  Missing job groups: {jobgroup_dist.get('missing_count', 0)} ({jobgroup_dist.get('missing_pct', 0):.1f}%)")
        print(f"  Top job groups:")
        for jobgroup, count in list(jobgroup_dist.get('top_10', {}).items())[:3]:
            print(f"    {jobgroup}: {count}")
    
    if 'worker_type_distribution' in salary_specific:
        worker_type_dist = salary_specific['worker_type_distribution']
        print(f"  Unique worker types: {worker_type_dist.get('total_unique', 0)}")
        print(f"  Missing worker types: {worker_type_dist.get('missing_count', 0)} ({worker_type_dist.get('missing_pct', 0):.1f}%)")
        print(f"  Top worker types:")
        for worker_type, count in list(worker_type_dist.get('top_10', {}).items())[:3]:
            print(f"    {worker_type}: {count}")
    
    if 'timeline_length_analysis' in salary_specific:
        timeline_stats = salary_specific['timeline_length_analysis']['stats']
        print(f"  Average timeline length: {timeline_stats.get('mean', 0):.1f} salary points")
    
    print("\n" + "="*80)


def main():
    """Main entry point for salary analysis."""
    parser = argparse.ArgumentParser(description='CAO Salary Data Analysis')
    parser.add_argument('--max_files', type=int, help='Maximum number of rows to process')
    
    args = parser.parse_args()
    
    # Set up paths
    salary_csv_path = 'outputs/excel/new_results/extracted_data_salary.csv'
    cao_info_path = 'pdfs/input_pdfs/extracted_cao_info.csv'
    output_dir = Path('outputs/comparison_results')
    
    # Load data
    df, cao_info = load_salary_data(salary_csv_path, cao_info_path, args.max_files)
    
    # Perform analyses
    results = {}
    
    try:
        print("Performing cross-sectional analysis...")
        results['cross_sectional'] = perform_cross_sectional_analysis(df)
    except Exception as e:
        print(f"Error in cross-sectional analysis: {e}")
        # Create basic cross-sectional results with just overview
        try:
            df_dates = pd.to_datetime(df['ingangsdatum'], errors='coerce')
            earliest = df_dates.min()
            latest = df_dates.max()
        except Exception:
            earliest = 'N/A'
            latest = 'N/A'
        
        results['cross_sectional'] = {
            'dataset_overview': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'unique_caos': df['cao_number'].nunique(),
                'date_range': {
                    'earliest': earliest,
                    'latest': latest
                }
            },
            'column_analysis': {}
        }
    
    try:
        print("Performing longitudinal analysis...")
        results['longitudinal'] = perform_longitudinal_analysis(df)
    except Exception as e:
        print(f"Error in longitudinal analysis: {e}")
        results['longitudinal'] = {}
    
    try:
        print("Performing temporal analysis...")
        results['temporal'] = perform_temporal_analysis(df)
    except Exception as e:
        print(f"Error in temporal analysis: {e}")
        results['temporal'] = {}
    
    try:
        print("Performing boolean cross-tabulation analysis...")
        results['boolean_crosstabs'] = perform_boolean_crosstabs(df)
    except Exception as e:
        print(f"Error in boolean crosstabs analysis: {e}")
        results['boolean_crosstabs'] = {}
    
    try:
        print("Performing amount analysis with unit normalization...")
        results['amount_analysis'] = perform_amount_analysis(df)
    except Exception as e:
        print(f"Error in amount analysis: {e}")
        results['amount_analysis'] = {}
    
    try:
        print("Performing salary-specific analysis...")
        results['salary_specific'] = perform_salary_specific_analysis(df)
    except Exception as e:
        print(f"Error in salary-specific analysis: {e}")
        results['salary_specific'] = {}
    
    # Save results
    try:
        save_results(results, output_dir)
    except Exception as e:
        print(f"Error saving results: {e}")
    
    # Print summary
    try:
        print_summary(results)
    except Exception as e:
        print(f"Error printing summary: {e}")
    
    print(f"\nAnalysis complete! Results saved to: {output_dir}")


if __name__ == "__main__":
    main()
