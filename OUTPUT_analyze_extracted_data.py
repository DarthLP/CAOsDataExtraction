#!/usr/bin/env python3
"""
Script to analyze extracted_data.xlsx for:
1. Duplicates (multiple rows with same CAO and infotype)
2. Missing infotypes (check if all 7 infotypes exist for each CAO)
3. Empty data (CAOs with no data beyond first 10 columns)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Any, Set
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

class ExtractedDataAnalyzer:
    def __init__(self, excel_path: str = "results/extracted_data.xlsx"):
        self.excel_path = excel_path
        self.df = None
        self.load_data()
        
    def load_data(self):
        """Load the Excel file"""
        try:
            self.df = pd.read_excel(self.excel_path)
            print(f"✅ Loaded {len(self.df)} rows from {self.excel_path}")
            print(f"📊 Columns: {list(self.df.columns)}")
        except Exception as e:
            print(f"❌ Error loading Excel file: {e}")
            raise
    
    def get_column_info(self):
        """Get information about the columns"""
        print(f"\n📋 Column Information:")
        print(f"Total columns: {len(self.df.columns)}")
        print(f"First 10 columns: {list(self.df.columns[:10])}")
        
        # Find infotype column
        infotype_col = None
        for col in self.df.columns:
            if 'infotype' in col.lower() or 'info_type' in col.lower():
                infotype_col = col
                break
        
        if infotype_col:
            print(f"Infotype column: {infotype_col}")
            unique_infotypes = self.df[infotype_col].unique()
            print(f"Unique infotypes: {list(unique_infotypes)}")
        else:
            print("⚠️  No infotype column found")
        
        # Find CAO/ID columns
        cao_cols = []
        for col in self.df.columns:
            if 'cao' in col.lower() or 'id' in col.lower() or 'file_name' in col.lower():
                cao_cols.append(col)
        
        print(f"Potential CAO/ID columns: {cao_cols}")
        
        return infotype_col, cao_cols
    
    def find_duplicates(self, infotype_col: str, id_col: str = 'id'):
        """Find duplicates - multiple rows with same ID and infotype (excluding Wage)"""
        print(f"\n🔍 Checking for duplicates (excluding Wage infotype)...")
        
        # Group by ID and infotype, but exclude Wage infotype
        duplicates = []
        duplicate_groups = self.df.groupby([id_col, infotype_col])
        
        for (file_id, infotype), group in duplicate_groups:
            # Skip Wage infotype as multiple rows are expected
            if infotype.lower() == 'wage':
                continue
                
            if len(group) > 1:
                duplicates.append({
                    'id': file_id,
                    'file_name': group['File_name'].iloc[0] if 'File_name' in group.columns else 'Unknown',
                    'infotype': infotype,
                    'count': len(group),
                    'rows': group.index.tolist()
                })
        
        print(f"Found {len(duplicates)} duplicate groups (excluding Wage)")
        
        # Show duplicates by infotype
        duplicate_by_infotype = defaultdict(list)
        for dup in duplicates:
            duplicate_by_infotype[dup['infotype']].append(dup)
        
        print(f"\n📊 Duplicates by infotype:")
        for infotype, dups in duplicate_by_infotype.items():
            print(f"  {infotype}: {len(dups)} duplicate groups")
            for dup in dups[:5]:  # Show first 5
                print(f"    - ID {dup['id']} ({dup['file_name']}): {dup['count']} rows")
            if len(dups) > 5:
                print(f"    ... and {len(dups) - 5} more")
        
        return duplicates
    
    def check_missing_infotypes(self, infotype_col: str, cao_col: str = 'CAO'):
        """Check if all 7 infotypes exist for each CAO"""
        print(f"\n🔍 Checking for missing infotypes...")
        
        # Define expected infotypes
        expected_infotypes = ['wage', 'pension', 'leave', 'termination', 'overtime', 'training', 'homeoffice']
        
        # Get unique CAOs and their infotypes
        cao_infotypes = defaultdict(set)
        for _, row in self.df.iterrows():
            cao = row[cao_col]
            infotype = row[infotype_col]
            cao_infotypes[cao].add(infotype.lower())
        
        missing_infotypes = []
        complete_caos = []
        
        for cao, infotypes in cao_infotypes.items():
            missing = set(expected_infotypes) - infotypes
            if missing:
                missing_infotypes.append({
                    'cao': cao,
                    'missing': list(missing),
                    'present': list(infotypes)
                })
            else:
                complete_caos.append(cao)
        
        print(f"CAOs with all infotypes: {len(complete_caos)}")
        print(f"CAOs with missing infotypes: {len(missing_infotypes)}")
        
        # Show missing infotypes by type
        missing_by_type = defaultdict(list)
        for item in missing_infotypes:
            for missing_type in item['missing']:
                missing_by_type[missing_type].append(item['cao'])
        
        print(f"\n📊 Missing infotypes by type:")
        for infotype, caos in missing_by_type.items():
            print(f"  {infotype}: {len(caos)} CAOs missing")
            print(f"    Examples: {caos[:5]}")
            if len(caos) > 5:
                print(f"    ... and {len(caos) - 5} more")
        
        return missing_infotypes, complete_caos
    
    def find_empty_data(self, infotype_col: str, cao_col: str = 'CAO'):
        """Find CAOs with no data beyond first 10 columns"""
        print(f"\n🔍 Checking for empty data...")
        
        # Get columns beyond first 10
        data_columns = self.df.columns[10:]
        print(f"Data columns (beyond first 10): {list(data_columns)}")
        
        # Check for empty data by infotype
        wage_empty = []
        other_empty = []
        
        for _, row in self.df.iterrows():
            cao = row[cao_col]
            infotype = row[infotype_col]
            
            # Check if all data columns are empty for this row
            data_values = row[data_columns]
            is_empty = data_values.isna().all() or (data_values == '').all()
            
            if is_empty:
                if infotype.lower() == 'wage':
                    wage_empty.append({
                        'cao': cao,
                        'infotype': infotype,
                        'row_index': row.name
                    })
                else:
                    other_empty.append({
                        'cao': cao,
                        'infotype': infotype,
                        'row_index': row.name
                    })
        
        print(f"Wage rows with empty data: {len(wage_empty)}")
        print(f"Other infotype rows with empty data: {len(other_empty)}")
        
        # Group by CAO
        wage_empty_by_cao = defaultdict(list)
        other_empty_by_cao = defaultdict(list)
        
        for item in wage_empty:
            wage_empty_by_cao[item['cao']].append(item)
        
        for item in other_empty:
            other_empty_by_cao[item['cao']].append(item)
        
        # Find CAOs where ALL non-wage infotypes are empty
        cao_all_other_empty = []
        for cao in self.df[cao_col].unique():
            cao_rows = self.df[self.df[cao_col] == cao]
            non_wage_rows = cao_rows[cao_rows[infotype_col].str.lower() != 'wage']
            
            if len(non_wage_rows) > 0:
                # Check if all non-wage rows have empty data
                all_empty = True
                for _, row in non_wage_rows.iterrows():
                    data_values = row[data_columns]
                    if not (data_values.isna().all() or (data_values == '').all()):
                        all_empty = False
                        break
                
                if all_empty:
                    cao_all_other_empty.append(cao)
        
        print(f"\n📊 CAOs with empty wage data: {len(wage_empty_by_cao)}")
        for cao, items in list(wage_empty_by_cao.items())[:10]:
            print(f"  - CAO {cao}: {len(items)} empty wage rows")
        
        print(f"\n📊 CAOs where ALL non-wage infotypes are empty: {len(cao_all_other_empty)}")
        for cao in cao_all_other_empty[:10]:
            print(f"  - CAO {cao}")
        
        # Count CAOs with each specific infotype empty
        print(f"\n📊 CAOs with specific infotypes empty:")
        infotype_empty_counts = defaultdict(int)
        
        for cao in self.df[cao_col].unique():
            cao_rows = self.df[self.df[cao_col] == cao]
            
            for _, row in cao_rows.iterrows():
                infotype = row[infotype_col]
                data_values = row[data_columns]
                is_empty = data_values.isna().all() or (data_values == '').all()
                
                if is_empty:
                    infotype_empty_counts[infotype] += 1
        
        for infotype, count in infotype_empty_counts.items():
            print(f"  {infotype}: {count} CAOs")
        
        return wage_empty_by_cao, cao_all_other_empty, infotype_empty_counts
    
    def generate_report(self):
        """Generate comprehensive analysis report"""
        print("=" * 60)
        print("EXTRACTED DATA ANALYSIS REPORT")
        print("=" * 60)
        
        # Get column information
        infotype_col, cao_cols = self.get_column_info()
        
        if not infotype_col:
            print("❌ No infotype column found. Cannot proceed with analysis.")
            return
        
        # Use first potential CAO column
        cao_col = cao_cols[0] if cao_cols else 'CAO'
        print(f"Using CAO column: {cao_col}")
        
        # 1. Find duplicates
        duplicates = self.find_duplicates(infotype_col, 'id')
        
        # 2. Check missing infotypes
        missing_infotypes, complete_caos = self.check_missing_infotypes(infotype_col, cao_col)
        
        # 3. Find empty data
        wage_empty_by_cao, cao_all_other_empty, infotype_empty_counts = self.find_empty_data(infotype_col, cao_col)
        
        # Save detailed report
        self.save_detailed_report(duplicates, missing_infotypes, wage_empty_by_cao, cao_all_other_empty, infotype_empty_counts)
        
        return {
            'duplicates': duplicates,
            'missing_infotypes': missing_infotypes,
            'complete_caos': complete_caos,
            'wage_empty_by_cao': wage_empty_by_cao,
            'cao_all_other_empty': cao_all_other_empty,
            'infotype_empty_counts': infotype_empty_counts
        }
    
    def save_detailed_report(self, duplicates, missing_infotypes, wage_empty_by_cao, cao_all_other_empty, infotype_empty_counts):
        """Save detailed analysis to file"""
        report_file = "extracted_data_analysis_report.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("EXTRACTED DATA ANALYSIS REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Analysis of: {self.excel_path}\n")
            f.write(f"Total rows: {len(self.df)}\n")
            f.write(f"Total columns: {len(self.df.columns)}\n\n")
            
            # 1. Duplicates
            f.write("1. DUPLICATES ANALYSIS\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total duplicate groups: {len(duplicates)}\n\n")
            
            for dup in duplicates:
                f.write(f"ID {dup['id']} ({dup['file_name']}) - {dup['infotype']}: {dup['count']} rows\n")
                f.write(f"  Row indices: {dup['rows']}\n\n")
            
            # 2. Missing infotypes
            f.write("2. MISSING INFOTYPES ANALYSIS\n")
            f.write("-" * 30 + "\n")
            f.write(f"CAOs with missing infotypes: {len(missing_infotypes)}\n\n")
            
            for item in missing_infotypes:
                f.write(f"CAO {item['cao']}:\n")
                f.write(f"  Missing: {', '.join(item['missing'])}\n")
                f.write(f"  Present: {', '.join(item['present'])}\n\n")
            
            # 3. Empty data
            f.write("3. EMPTY DATA ANALYSIS\n")
            f.write("-" * 30 + "\n")
            f.write(f"CAOs with empty wage data: {len(wage_empty_by_cao)}\n")
            f.write(f"CAOs where ALL non-wage infotypes are empty: {len(cao_all_other_empty)}\n\n")
            
            f.write("CAOs with empty wage data:\n")
            for cao, items in wage_empty_by_cao.items():
                f.write(f"  CAO {cao}: {len(items)} empty wage rows\n")
            
            f.write("\nCAOs where ALL non-wage infotypes are empty:\n")
            for cao in cao_all_other_empty:
                f.write(f"  CAO {cao}\n")
            
            # 4. Empty infotypes by type
            f.write("\n4. EMPTY INFOTYPES BY TYPE\n")
            f.write("-" * 30 + "\n")
            for infotype, count in infotype_empty_counts.items():
                f.write(f"{infotype}: {count} CAOs\n")
        
        print(f"✅ Detailed report saved to: {report_file}")

def main():
    """Main function"""
    analyzer = ExtractedDataAnalyzer()
    results = analyzer.generate_report()
    
    print(f"\n✅ Analysis complete!")
    print(f"📊 Summary:")
    print(f"  - Duplicate groups: {len(results['duplicates'])}")
    print(f"  - CAOs with missing infotypes: {len(results['missing_infotypes'])}")
    print(f"  - CAOs with empty wage data: {len(results['wage_empty_by_cao'])}")
    print(f"  - CAOs where ALL non-wage infotypes are empty: {len(results['cao_all_other_empty'])}")
    print(f"  - Empty infotypes by type:")
    for infotype, count in results['infotype_empty_counts'].items():
        print(f"    {infotype}: {count} CAOs")
    
    return results

if __name__ == "__main__":
    results = main() 