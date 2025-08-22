#!/usr/bin/env python3
"""
CAO Processing Tracker
Tracks progress of each CAO through the pipeline and updates the Excel file
"""

import pandas as pd
import os
from pathlib import Path
import json

class CAOTracker:
    def __init__(self, excel_path="inputExcel/CAO_Frequencies_2014.xlsx"):
        self.excel_path = excel_path
        self.tracker_columns = [
            "Number of Pdfs found",
            "Number of succesfull PDF Parsing", 
            "Failed PDF Parsing",
            "Number of succesfull LLM Extraction",
            "Failed LLM Extraction", 
            "Number of Successful LLM Analyisis",
            "Failed LLM Analysis"
        ]
        self.load_excel()
    
    def load_excel(self):
        """Load the Excel file and ensure tracking columns exist"""
        try:
            self.df = pd.read_excel(self.excel_path)
            
            # Add tracking columns if they don't exist
            for col in self.tracker_columns:
                if col not in self.df.columns:
                    if "Failed" in col:
                        self.df[col] = ""  # Empty string for file name lists
                    else:
                        self.df[col] = 0   # 0 for counts
            
            # Ensure all "Failed" columns are string type
            for col in self.tracker_columns:
                if "Failed" in col:
                    self.df[col] = self.df[col].astype(str)
            
            # Ensure CAO column exists
            if 'CAO' not in self.df.columns:
                raise ValueError("CAO column not found in Excel file")
                
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            raise
    
    def save_excel(self):
        """Save the updated Excel file"""
        try:
            self.df.to_excel(self.excel_path, index=False)
        except Exception as e:
            print(f"❌ Error saving Excel file: {e}")
    
    def get_cao_row(self, cao_number):
        """Get the row index for a specific CAO number"""
        cao_str = str(cao_number)
        mask = self.df['CAO'].astype(str) == cao_str
        if mask.any():
            return self.df[mask].index[0]
        else:
            print(f"⚠️  CAO {cao_number} not found in Excel file")
            return None
    
    def update_pdf_count(self, cao_number, count):
        """Update the number of PDFs found for a CAO"""
        row_idx = self.get_cao_row(cao_number)
        if row_idx is not None:
            self.df.loc[row_idx, "Number of Pdfs found"] = count
    
    def update_pdf_parsing(self, cao_number, successful, failed_files=None):
        """Update PDF parsing results for a CAO"""
        row_idx = self.get_cao_row(cao_number)
        if row_idx is not None:
            self.df.loc[row_idx, "Number of succesfull PDF Parsing"] = successful
            
            # Store failed file names as comma-separated list
            if failed_files:
                failed_list = ", ".join(failed_files)
                self.df.loc[row_idx, "Failed PDF Parsing"] = failed_list
            else:
                self.df.loc[row_idx, "Failed PDF Parsing"] = ""
    
    def update_llm_extraction(self, cao_number, successful, failed_files=None):
        """Update LLM extraction results for a CAO"""
        row_idx = self.get_cao_row(cao_number)
        if row_idx is not None:
            self.df.loc[row_idx, "Number of succesfull LLM Extraction"] = successful
            
            # Store failed file names as comma-separated list
            if failed_files:
                failed_list = ", ".join(failed_files)
                self.df.loc[row_idx, "Failed LLM Extraction"] = failed_list
            else:
                self.df.loc[row_idx, "Failed LLM Extraction"] = ""
    
    def update_llm_analysis(self, cao_number, successful, failed_files=None):
        """Update LLM analysis results for a CAO"""
        row_idx = self.get_cao_row(cao_number)
        if row_idx is not None:
            self.df.loc[row_idx, "Number of Successful LLM Analyisis"] = successful
            
            # Store failed file names as comma-separated list
            if failed_files:
                failed_list = ", ".join(failed_files)
                self.df.loc[row_idx, "Failed LLM Analysis"] = failed_list
            else:
                self.df.loc[row_idx, "Failed LLM Analysis"] = ""
    
    def count_pdfs_in_folder(self, cao_number):
        """Count PDFs in the input_pdfs folder for a CAO"""
        pdf_folder = Path(f"input_pdfs/{cao_number}")
        if pdf_folder.exists():
            pdf_count = len(list(pdf_folder.glob("*.pdf")))
            return pdf_count
        return 0
    
    def count_json_files(self, folder_path, cao_number):
        """Count JSON files in a specific folder for a CAO"""
        folder = Path(folder_path) / str(cao_number)
        if folder.exists():
            json_count = len(list(folder.glob("*.json")))
            return json_count
        return 0
    
    def get_failed_pdf_files(self, cao_number):
        """Get list of PDF files that failed to parse"""
        pdf_folder = Path(f"input_pdfs/{cao_number}")
        output_folder = Path("outputs/parsed_pdfs") / str(cao_number)
        
        if not pdf_folder.exists():
            return []
        
        failed_files = []
        for pdf_file in pdf_folder.glob("*.pdf"):
            expected_json = output_folder / f"{pdf_file.stem}.json"
            if not expected_json.exists():
                failed_files.append(pdf_file.name)
        
        return failed_files
    
    def get_failed_llm_files(self, cao_number):
        """Get list of JSON files that failed LLM extraction"""
        input_folder = Path("outputs/parsed_pdfs") / str(cao_number)
        output_folder = Path("outputs/llm_extracted") / str(cao_number)
        
        if not input_folder.exists():
            return []
        
        failed_files = []
        for json_file in input_folder.glob("*.json"):
            expected_output = output_folder / json_file.name
            if not expected_output.exists():
                failed_files.append(json_file.name)
        
        return failed_files
    
    def get_failed_llm_analysis_files(self, cao_number):
        """Get list of JSON files that failed LLM analysis"""
        input_folder = Path("llmExtracted_json") / str(cao_number)
        results_excel = Path("results/extracted_data.xlsx")
        
        if not input_folder.exists():
            return []
        
        if not results_excel.exists():
            # If no results Excel exists, all files failed
            return [json_file.name for json_file in input_folder.glob("*.json")]
        
        # Read the results Excel file
        try:
            df_results = pd.read_excel(results_excel)
            if 'File_name' not in df_results.columns:
                # If no File_name column, all files failed
                return [json_file.name for json_file in input_folder.glob("*.json")]
            
            # Get all filenames that exist in the results Excel for this CAO
            cao_str = str(cao_number)
            cao_results = df_results[df_results['CAO'].astype(str) == cao_str]
            successful_files = set(cao_results['File_name'].values)
            
            # Find files that don't have entries in the results
            failed_files = []
            for json_file in input_folder.glob("*.json"):
                if json_file.name not in successful_files:
                    failed_files.append(json_file.name)
            
            return failed_files
            
        except Exception as e:
            print(f"❌ Error reading results Excel file: {e}")
            # If error reading Excel, consider all files failed
            return [json_file.name for json_file in input_folder.glob("*.json")]
    
    def count_analysis_results(self, cao_number):
        """Count successful analysis results for a CAO in the results Excel"""
        results_excel = Path("results/extracted_data.xlsx")
        
        if not results_excel.exists():
            return 0
        
        try:
            df_results = pd.read_excel(results_excel)
            if 'CAO' not in df_results.columns:
                return 0
            
            cao_str = str(cao_number)
            cao_results = df_results[df_results['CAO'].astype(str) == cao_str]
            return len(cao_results)
            
        except Exception as e:
            print(f"❌ Error reading results Excel file: {e}")
            return 0

    def auto_update_from_files(self):
        """Automatically update tracking based on existing files"""
        print("🔄 Auto-updating progress from existing files...")
        
        for _, row in self.df.iterrows():
            cao_number = row['CAO']
            if cao_number is None or (isinstance(cao_number, float) and pd.isna(cao_number)):
                continue
                
            # Count PDFs
            pdf_count = self.count_pdfs_in_folder(cao_number)
            self.update_pdf_count(cao_number, pdf_count)
            
            # Count extracted JSONs (outputs/parsed_pdfs folder)
extracted_count = self.count_json_files("outputs/parsed_pdfs", cao_number)
            
            # Count LLM processed JSONs (outputs/llm_extracted folder)
llm_count = self.count_json_files("outputs/llm_extracted", cao_number)
            
            # Update parsing results with failed file names (even if 0 PDFs)
            if pdf_count > 0:
                successful_parsing = extracted_count
                failed_pdf_files = self.get_failed_pdf_files(cao_number)
                self.update_pdf_parsing(cao_number, successful_parsing, failed_pdf_files)
            else:
                # No PDFs found - set parsing to 0 successful, 0 failed
                self.update_pdf_parsing(cao_number, 0, [])
            
            # Update LLM extraction results with failed file names (even if 0 extracted)
            if extracted_count > 0:
                successful_llm = llm_count
                failed_llm_files = self.get_failed_llm_files(cao_number)
                self.update_llm_extraction(cao_number, successful_llm, failed_llm_files)
            else:
                # No extracted files - set LLM extraction to 0 successful, 0 failed
                self.update_llm_extraction(cao_number, 0, [])
            
            # Update LLM analysis results with failed file names (even if 0 analyzed)
            if llm_count > 0:
                successful_analysis = self.count_analysis_results(cao_number)
                failed_analysis_files = self.get_failed_llm_analysis_files(cao_number)
                self.update_llm_analysis(cao_number, successful_analysis, failed_analysis_files)
            else:
                # No analyzed files - set LLM analysis to 0 successful, 0 failed
                self.update_llm_analysis(cao_number, 0, [])
        
        self.save_excel()
        print("✅ Progress updated and saved")

# Global tracker instance
tracker = None

def get_tracker():
    """Get or create the global tracker instance"""
    global tracker
    if tracker is None:
        tracker = CAOTracker()
    return tracker

def update_progress(cao_number, step, successful=0, failed=0, failed_files=None, total=None):
    """Update progress for a specific CAO and step"""
    t = get_tracker()
    
    if step == "pdfs_found":
        t.update_pdf_count(cao_number, successful)
    elif step == "pdf_parsing":
        t.update_pdf_parsing(cao_number, successful, failed_files)
    elif step == "llm_extraction":
        t.update_llm_extraction(cao_number, successful, failed_files)
    elif step == "llm_analysis":
        t.update_llm_analysis(cao_number, successful, failed_files)
    
    t.save_excel()

def flatten_to_str_list(lst):
    result = []
    for item in lst:
        if isinstance(item, list):
            # Recursively flatten and join nested lists
            result.append(" | ".join(str(subitem) for subitem in flatten_to_str_list(item)))
        else:
            result.append(str(item))
    return result

if __name__ == "__main__":
    # Test the tracker
    tracker = CAOTracker()
    tracker.auto_update_from_files() 