"""
CAO Non-Salary Data Analysis Script

This script performs comprehensive analysis of the non-salary Excel output data,
including cross-sectional statistics, longitudinal trends, temporal analysis,
boolean cross-tabulations, amount normalization, and non-salary-specific insights.

USAGE:
    python scripts/excel_analysis/analyze_non_salary_output.py
    
    With file limit:
        python scripts/excel_analysis/analyze_non_salary_output.py --max_files 500

ARGUMENTS:
    --max_files: Maximum number of rows to process (optional)

INPUT:
    - outputs/excel/new_results/extracted_data_non_salary.csv
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
    calculate_descriptive_stats, create_boolean_summary,
    create_categorical_summary, group_cao_timeline, calculate_longitudinal_changes,
    create_crosstab_summary, extract_year_from_date, analyze_amount_ranges,
    identify_non_salary_columns
)


def load_non_salary_data(non_salary_csv_path: str, cao_info_path: str, max_files: Optional[int] = None) -> pd.DataFrame:
    """
    Load and prepare non-salary data for analysis.
    
    Args:
        non_salary_csv_path: Path to non-salary CSV file
        cao_info_path: Path to CAO info CSV file
        max_files: Maximum number of rows to process
        
    Returns:
        Prepared DataFrame with non-salary data
    """
    print(f"Loading non-salary data from: {non_salary_csv_path}")
    
    # Load non-salary data
    df = pd.read_csv(non_salary_csv_path, sep=';', encoding='utf-8')
    print(f"  Loaded {len(df)} non-salary rows")
    
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
    Perform cross-sectional analysis on non-salary data.
    
    Args:
        df: DataFrame with non-salary data
        
    Returns:
        Dictionary with cross-sectional analysis results
    """
    print("Performing cross-sectional analysis...")
    
    # Identify column types
    try:
        col_types = identify_non_salary_columns(df)
    except Exception as e:
        print(f"Error identifying column types: {e}")
        # Fallback to basic column identification
        col_types = {
            'numeric': [],
            'boolean': [],
            'categorical': list(df.columns),
            'amounts': [],
            'units': [],
            'ranges': []
        }
    
    results = {
        'dataset_overview': {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'unique_caos': df['cao_number'].nunique(),
            'date_range': {
                'earliest': 'N/A',
                'latest': 'N/A'
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
                
            try:
                if col_type == 'boolean':
                    results['column_analysis'][col_type][col] = create_boolean_summary(df[col])
                elif col_type in ['numeric', 'amounts']:
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
            except Exception as e:
                # Skip problematic columns
                print(f"  Warning: Skipping column {col} (type: {col_type}) due to error: {e}")
                # Add basic info for failed columns
                results['column_analysis'][col_type][col] = {
                    'count': 0,
                    'missing_count': len(df),
                    'missing_pct': 100.0,
                    'error': str(e)
                }
                continue
    
    return results


def merge_cross_sectional_results(chunk_results: List[Dict[str, Any]], full_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Merge cross-sectional analysis results from multiple chunks.
    
    Args:
        chunk_results: List of cross-sectional analysis results from chunks
        full_df: Full DataFrame to get correct overview statistics
        
    Returns:
        Merged cross-sectional analysis results
    """
    if not chunk_results:
        return {}
    
    # Use the first chunk as base
    merged = chunk_results[0].copy()
    
    # Update dataset overview with full dataset statistics
    merged['dataset_overview'] = {
        'total_rows': len(full_df),
        'total_columns': len(full_df.columns),
        'unique_caos': full_df['cao_number'].nunique(),
        'date_range': {
            'earliest': full_df['ingangsdatum'].min(),
            'latest': full_df['ingangsdatum'].max()
        }
    }
    
    # Merge column analysis from all chunks
    for chunk_result in chunk_results[1:]:
        if 'column_analysis' in chunk_result:
            for col_type, columns in chunk_result['column_analysis'].items():
                if col_type not in merged['column_analysis']:
                    merged['column_analysis'][col_type] = {}
                
                for col_name, stats in columns.items():
                    if col_name not in merged['column_analysis'][col_type]:
                        merged['column_analysis'][col_type][col_name] = stats
                    else:
                        # Merge statistics (sum counts, average percentages, etc.)
                        existing = merged['column_analysis'][col_type][col_name]
                        if isinstance(stats, dict) and isinstance(existing, dict):
                            # Sum counts
                            for key in ['count', 'total_count', 'missing_count', 'true_count', 'false_count']:
                                if key in stats and key in existing:
                                    existing[key] = existing[key] + stats[key]
                            
                            # Average percentages
                            for key in ['missing_pct', 'true_pct', 'false_pct']:
                                if key in stats and key in existing:
                                    existing[key] = (existing[key] + stats[key]) / 2
    
    return merged


def perform_longitudinal_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform longitudinal analysis on non-salary data.
    
    Args:
        df: DataFrame with non-salary data
        
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
    col_types = identify_non_salary_columns(df)
    numeric_cols = col_types['amounts'] + col_types['numeric']
    
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
    Perform temporal trend analysis on non-salary data.
    
    Args:
        df: DataFrame with non-salary data
        
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
    
    # Analyze key boolean variables by year
    col_types = identify_non_salary_columns(df)
    bool_cols = col_types['boolean']
    
    yearly_boolean_stats = {}
    for year, year_group in df_copy.groupby('year'):
        if pd.isna(year) or len(bool_cols) == 0:
            continue
            
        year_bools = {}
        for col in bool_cols[:10]:  # Limit to first 10 boolean columns
            if col in year_group.columns:
                true_count = (year_group[col] == True).sum()
                false_count = (year_group[col] == False).sum()
                total_count = true_count + false_count
                
                if total_count > 0:
                    year_bools[col] = {
                        'true_count': true_count,
                        'true_pct': (true_count / total_count) * 100,
                        'total_count': total_count
                    }
        
        if year_bools:
            yearly_boolean_stats[int(year)] = year_bools
    
    return {
        'yearly_overview': yearly_stats,
        'yearly_boolean_stats': yearly_boolean_stats,
        'year_range': {
            'earliest': df_copy['year'].min(),
            'latest': df_copy['year'].max()
        }
    }


def perform_boolean_crosstabs(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform cross-tabulation analysis for boolean fields.
    
    Args:
        df: DataFrame with non-salary data
        
    Returns:
        Dictionary with cross-tabulation results
    """
    print("Performing boolean cross-tabulation analysis...")
    
    # Identify boolean columns
    col_types = identify_non_salary_columns(df)
    bool_cols = col_types['boolean']
    
    if not bool_cols:
        return {'message': 'No boolean columns found'}
    
    # Prepare grouping variables
    df_copy = df.copy()
    df_copy['year'] = extract_year_from_date(df_copy['ingangsdatum'])
    df_copy['ttw_status'] = df_copy['TTW'].fillna('unknown')
    
    # Add CAO scope type if available
    if 'general_cao_scope_type' in df.columns:
        df_copy['scope_type'] = df_copy['general_cao_scope_type'].fillna('unknown')
    
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
        
        # By scope type (if available)
        if 'scope_type' in df_copy.columns:
            results[bool_col]['by_scope_type'] = create_crosstab_summary(df_copy, bool_col, 'scope_type')
    
    return results


def perform_amount_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform analysis of amount fields with unit analysis.
    
    Args:
        df: DataFrame with non-salary data
        
    Returns:
        Dictionary with amount analysis results
    """
    print("Performing amount analysis...")
    
    col_types = identify_non_salary_columns(df)
    amount_cols = col_types['amounts']
    unit_cols = col_types['units']
    range_min_cols = col_types['range_mins']
    range_max_cols = col_types['range_maxs']
    
    results = {
        'amount_analysis': {},
        'unit_distribution': {},
        'range_analysis': {}
    }
    
    # Analyze amount columns
    for amount_col in amount_cols:
        if amount_col not in df.columns:
            continue
            
        # Find corresponding unit column
        unit_col = amount_col.replace('_value', '_unit')
        
        results['amount_analysis'][amount_col] = calculate_descriptive_stats(df[amount_col])
        
        if unit_col in df.columns:
            unit_counts = df[unit_col].value_counts()
            results['unit_distribution'][amount_col] = unit_counts.to_dict()
    
    # Analyze range columns
    for min_col in range_min_cols:
        max_col = min_col.replace('_min', '_max')
        unit_col = min_col.replace('_min', '_unit')
        
        if max_col in df.columns and unit_col in df.columns:
            range_analysis = analyze_amount_ranges(df, min_col, max_col, unit_col)
            if range_analysis:
                results['range_analysis'][min_col.replace('_min', '')] = range_analysis
    
    return results


def perform_non_salary_specific_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform non-salary-specific analysis.
    
    Args:
        df: DataFrame with non-salary data
        
    Returns:
        Dictionary with non-salary-specific analysis results
    """
    print("Performing non-salary-specific analysis...")
    
    results = {}
    
    # Bonus scheme analysis
    bonus_cols = [col for col in df.columns if 'bonus' in col.lower() and col.endswith('_present')]
    if bonus_cols:
        results['bonus_analysis'] = {}
        for col in bonus_cols:
            results['bonus_analysis'][col] = create_boolean_summary(df[col])
    
    # Pension scheme analysis
    pension_cols = [col for col in df.columns if 'pension' in col.lower()]
    if pension_cols:
        results['pension_analysis'] = {}
        for col in pension_cols:
            if col.endswith('_present') or col == 'pension_type':
                if df[col].dtype == 'bool' or df[col].isin([True, False, np.nan]).all():
                    results['pension_analysis'][col] = create_boolean_summary(df[col])
                else:
                    results['pension_analysis'][col] = create_categorical_summary(df[col])
    
    # Leave analysis
    leave_cols = [col for col in df.columns if 'leave' in col.lower() and col.endswith('_present')]
    if leave_cols:
        results['leave_analysis'] = {}
        for col in leave_cols:
            results['leave_analysis'][col] = create_boolean_summary(df[col])
    
    # Work arrangement analysis
    work_cols = [col for col in df.columns if any(term in col.lower() for term in ['overtime', 'homeoffice', 'training']) and col.endswith('_present')]
    if work_cols:
        results['work_arrangement_analysis'] = {}
        for col in work_cols:
            results['work_arrangement_analysis'][col] = create_boolean_summary(df[col])
    
    # Safety and wellbeing analysis
    safety_cols = [col for col in df.columns if any(term in col.lower() for term in ['safety', 'harassment', 'integrity', 'wellbeing']) and col.endswith('_present')]
    if safety_cols:
        results['safety_wellbeing_analysis'] = {}
        for col in safety_cols:
            results['safety_wellbeing_analysis'][col] = create_boolean_summary(df[col])
    
    # AI/algorithmic management analysis
    ai_cols = [col for col in df.columns if 'ai' in col.lower() and col.endswith('_present')]
    if ai_cols:
        results['ai_analysis'] = {}
        for col in ai_cols:
            results['ai_analysis'][col] = create_boolean_summary(df[col])
    
    # Fringe benefits analysis
    fringe_cols = [col for col in df.columns if 'fringe' in col.lower() and col.endswith('_present')]
    if fringe_cols:
        results['fringe_benefits_analysis'] = {}
        for col in fringe_cols:
            results['fringe_benefits_analysis'][col] = create_boolean_summary(df[col])
    
    # Childcare analysis
    childcare_cols = [col for col in df.columns if 'childcare' in col.lower() and col.endswith('_present')]
    if childcare_cols:
        results['childcare_analysis'] = {}
        for col in childcare_cols:
            results['childcare_analysis'][col] = create_boolean_summary(df[col])
    
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
                  'earliest_year', 'latest_year'],
        'value': [
            datetime.now().isoformat(),
            results.get('cross_sectional', {}).get('dataset_overview', {}).get('unique_caos', 0),
            results.get('cross_sectional', {}).get('dataset_overview', {}).get('total_rows', 0),
            results.get('longitudinal', {}).get('caos_with_multiple_periods', 0),
            results.get('temporal', {}).get('year_range', {}).get('earliest', 'N/A'),
            results.get('temporal', {}).get('year_range', {}).get('latest', 'N/A')
        ]
    }
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_dir / 'non_salary_analysis_summary.csv', index=False, sep=';')
    
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
            stats_df.to_csv(output_dir / 'non_salary_descriptive_stats.csv', index=False, sep=';')
    
    # 3. LONGITUDINAL ANALYSIS - CAO changes over time
    if 'longitudinal' in results:
        longitudinal_data = results['longitudinal']
        
        # Periods distribution
        if 'periods_distribution' in longitudinal_data:
            periods_df = pd.DataFrame(list(longitudinal_data['periods_distribution'].items()), 
                                    columns=['periods_count', 'cao_count'])
            periods_df.to_csv(output_dir / 'non_salary_longitudinal_analysis.csv', index=False, sep=';')
    
    # 4. TEMPORAL TRENDS - Year-over-year analysis
    if 'temporal' in results:
        temporal_data = results['temporal']
        
        # Yearly overview
        if 'yearly_overview' in temporal_data:
            yearly_df = pd.DataFrame(temporal_data['yearly_overview']).T
            yearly_df.index.name = 'year'
            yearly_df.to_csv(output_dir / 'non_salary_temporal_trends.csv', sep=';')
    
    # 5. NON-SALARY-SPECIFIC INSIGHTS - Consolidated analysis
    if 'non_salary_specific' in results:
        specific_data = results['non_salary_specific']
        insights_data = []
        
        # Bonus schemes (top 5 most common)
        if 'bonus_analysis' in specific_data:
            bonus_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['bonus_analysis'].items()]
            bonus_stats.sort(key=lambda x: x[1], reverse=True)
            for bonus_type, pct in bonus_stats[:5]:
                insights_data.append({'category': 'bonus', 'name': bonus_type, 'percentage': pct})
        
        # Pension schemes
        if 'pension_analysis' in specific_data:
            pension_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['pension_analysis'].items()]
            pension_stats.sort(key=lambda x: x[1], reverse=True)
            for pension_type, pct in pension_stats[:5]:
                insights_data.append({'category': 'pension', 'name': pension_type, 'percentage': pct})
        
        # Leave enhancements
        if 'leave_analysis' in specific_data:
            leave_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['leave_analysis'].items()]
            leave_stats.sort(key=lambda x: x[1], reverse=True)
            for leave_type, pct in leave_stats[:5]:
                insights_data.append({'category': 'leave', 'name': leave_type, 'percentage': pct})
        
        # Work arrangements
        if 'work_arrangement_analysis' in specific_data:
            work_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['work_arrangement_analysis'].items()]
            work_stats.sort(key=lambda x: x[1], reverse=True)
            for work_type, pct in work_stats[:5]:
                insights_data.append({'category': 'work_arrangement', 'name': work_type, 'percentage': pct})
        
        # Safety and wellbeing
        if 'safety_wellbeing_analysis' in specific_data:
            safety_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['safety_wellbeing_analysis'].items()]
            safety_stats.sort(key=lambda x: x[1], reverse=True)
            for safety_type, pct in safety_stats[:5]:
                insights_data.append({'category': 'safety_wellbeing', 'name': safety_type, 'percentage': pct})
        
        # AI/algorithmic management
        if 'ai_analysis' in specific_data:
            ai_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['ai_analysis'].items()]
            ai_stats.sort(key=lambda x: x[1], reverse=True)
            for ai_type, pct in ai_stats[:5]:
                insights_data.append({'category': 'ai', 'name': ai_type, 'percentage': pct})
        
        # Fringe benefits
        if 'fringe_benefits_analysis' in specific_data:
            fringe_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['fringe_benefits_analysis'].items()]
            fringe_stats.sort(key=lambda x: x[1], reverse=True)
            for fringe_type, pct in fringe_stats[:5]:
                insights_data.append({'category': 'fringe_benefits', 'name': fringe_type, 'percentage': pct})
        
        # Childcare
        if 'childcare_analysis' in specific_data:
            childcare_stats = [(k, v.get('true_pct', 0)) for k, v in specific_data['childcare_analysis'].items()]
            childcare_stats.sort(key=lambda x: x[1], reverse=True)
            for childcare_type, pct in childcare_stats[:5]:
                insights_data.append({'category': 'childcare', 'name': childcare_type, 'percentage': pct})
        
        if insights_data:
            insights_df = pd.DataFrame(insights_data)
            insights_df.to_csv(output_dir / 'non_salary_specific_insights.csv', index=False, sep=';')


def print_summary(results: Dict[str, Any]) -> None:
    """
    Print summary of analysis results to console.
    
    Args:
        results: Analysis results dictionary
    """
    print("\n" + "="*80)
    print("NON-SALARY DATA ANALYSIS SUMMARY")
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
    
    # Non-salary-specific insights
    non_salary_specific = results.get('non_salary_specific', {})
    
    # Bonus analysis
    if 'bonus_analysis' in non_salary_specific:
        bonus_analysis = non_salary_specific['bonus_analysis']
        print(f"\nBonus Schemes:")
        for bonus_type, stats in list(bonus_analysis.items())[:3]:
            true_pct = stats.get('true_pct', 0)
            print(f"  {bonus_type}: {true_pct:.1f}% of CAOs")
    
    # Pension analysis
    if 'pension_analysis' in non_salary_specific:
        pension_analysis = non_salary_specific['pension_analysis']
        if 'pension_has_pension_scheme' in pension_analysis:
            pension_pct = pension_analysis['pension_has_pension_scheme'].get('true_pct', 0)
            print(f"\nPension Schemes:")
            print(f"  Has pension scheme: {pension_pct:.1f}% of CAOs")
    
    # Leave analysis
    if 'leave_analysis' in non_salary_specific:
        leave_analysis = non_salary_specific['leave_analysis']
        print(f"\nLeave Enhancements:")
        for leave_type, stats in list(leave_analysis.items())[:3]:
            true_pct = stats.get('true_pct', 0)
            print(f"  {leave_type}: {true_pct:.1f}% of CAOs")
    
    # AI analysis
    if 'ai_analysis' in non_salary_specific:
        ai_analysis = non_salary_specific['ai_analysis']
        if 'ai_ai_policy_exists' in ai_analysis:
            ai_pct = ai_analysis['ai_ai_policy_exists'].get('true_pct', 0)
            print(f"\nAI/Algorithmic Management:")
            print(f"  Has AI policy: {ai_pct:.1f}% of CAOs")
    
    print("\n" + "="*80)


def main():
    """Main entry point for non-salary analysis."""
    parser = argparse.ArgumentParser(description='CAO Non-Salary Data Analysis')
    parser.add_argument('--max_files', type=int, help='Maximum number of rows to process')
    
    args = parser.parse_args()
    
    # Set up paths
    non_salary_csv_path = 'outputs/excel/new_results/extracted_data_non_salary.csv'
    cao_info_path = 'pdfs/input_pdfs/extracted_cao_info.csv'
    output_dir = Path('outputs/comparison_results')
    
    # Load data
    df, cao_info = load_non_salary_data(non_salary_csv_path, cao_info_path, args.max_files)
    
    # Perform analyses
    results = {}
    
    # Perform cross-sectional analysis with error handling
    try:
        cross_sectional_results = perform_cross_sectional_analysis(df)
        results['cross_sectional'] = cross_sectional_results
        print("Cross-sectional analysis completed successfully")
    except Exception as e:
        print(f"Error in cross-sectional analysis: {e}")
        # Create minimal cross-sectional results for dataset overview
        results['cross_sectional'] = {
            'dataset_overview': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'unique_caos': df['cao_number'].nunique() if 'cao_number' in df.columns else 0,
                'date_range': {
                    'earliest': 'N/A',
                    'latest': 'N/A'
                }
            },
            'column_analysis': {}
        }
    
    # Convert dates to datetime to avoid comparison errors
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
        print("Performing amount analysis...")
        results['amount_analysis'] = perform_amount_analysis(df)
    except Exception as e:
        print(f"Error in amount analysis: {e}")
        results['amount_analysis'] = {}
    
    try:
        print("Performing non-salary-specific analysis...")
        results['non_salary_specific'] = perform_non_salary_specific_analysis(df)
    except Exception as e:
        print(f"Error in non-salary-specific analysis: {e}")
        results['non_salary_specific'] = {}
    
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
