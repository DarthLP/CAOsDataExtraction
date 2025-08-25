"""
Quick OCR Screening Script
==========================

DESCRIPTION:
This script quickly screens PDFs to identify those where normal text extraction
is more powerful than OCR, without doing full OCR analysis. It uses a fast
heuristic approach to detect potential issues.

USAGE:
    python scripts/quick_ocr_screening.py [CAO_NUMBER]
    
    If no CAO number is provided, screens all CAOs.

ARGUMENTS:
    CAO_NUMBER: Optional specific CAO number to screen

OUTPUT:
    - Console output with screening results
    - List of PDFs that need full analysis
"""

import os
import sys
from pathlib import Path
import yaml
from PyPDF2 import PdfReader
import pdfplumber
import re

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load configuration
with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

INPUT_FOLDER = config['paths']['inputs_pdfs']

def quick_screen_pdf(pdf_path):
    """Quick screen a PDF for potential OCR issues without full OCR"""
    filename = pdf_path.name
    
    try:
        # Quick PyPDF2 extraction
        reader = PdfReader(pdf_path)
        pypdf2_text = ""
        for page in reader.pages:
            pypdf2_text += page.extract_text() or ""
        pypdf2_text = pypdf2_text.strip()
        pypdf2_len = len(pypdf2_text)
        
        # Quick pdfplumber extraction
        with pdfplumber.open(pdf_path) as pdf:
            pdfplumber_text = ""
            for page in pdf.pages:
                pdfplumber_text += page.extract_text() or ""
        pdfplumber_text = pdfplumber_text.strip()
        pdfplumber_len = len(pdfplumber_text)
        
        # Quick checks for potential issues
        issues = []
        
        # Check 1: Large difference between PyPDF2 and pdfplumber
        if pypdf2_len > 0 and pdfplumber_len > 0:
            diff_ratio = abs(pdfplumber_len - pypdf2_len) / max(pypdf2_len, pdfplumber_len)
            if diff_ratio > 0.15:  # 15% difference
                issues.append(f"Large text difference: {diff_ratio:.2f}")
        
        # Check 2: Very long lines (potential content outside boundaries)
        if pdfplumber_text:
            lines = pdfplumber_text.split('\n')
            long_lines = [line for line in lines if len(line.strip()) > 150]
            if len(long_lines) > 0:
                issues.append(f"Very long lines detected: {len(long_lines)}")
        
        # Check 3: Specific patterns that might indicate hidden content
        annotation_keywords = ['comment', 'annotation', 'note', 'remark', 'genoten', 'bedongen']
        found_keywords = []
        for keyword in annotation_keywords:
            if keyword in pdfplumber_text.lower():
                found_keywords.append(keyword)
        
        if found_keywords:
            issues.append(f"Annotation patterns: {', '.join(found_keywords)}")
        
        # Check 4: Check for specific legal terms that might be in hidden content
        legal_terms = ['brutoloon', 'arbeidsvoorwaarden', 'uitkering', 'vakantie', 'pensioen']
        legal_term_count = sum(1 for term in legal_terms if term in pdfplumber_text.lower())
        if legal_term_count > 3:
            issues.append(f"High legal term density: {legal_term_count} terms")
        
        return {
            'filename': filename,
            'pypdf2_length': pypdf2_len,
            'pdfplumber_length': pdfplumber_len,
            'issues': issues,
            'needs_full_analysis': len(issues) > 0
        }
        
    except Exception as e:
        return {
            'filename': filename,
            'pypdf2_length': 0,
            'pdfplumber_length': 0,
            'issues': [f"Error: {str(e)}"],
            'needs_full_analysis': True
        }

def screen_cao_pdfs(cao_folder):
    """Screen all PDFs in a CAO folder"""
    cao_number = cao_folder.name
    pdf_files = list(cao_folder.glob('*.pdf'))
    
    if not pdf_files:
        return []
    
    print(f"\n=== Screening CAO {cao_number} ===")
    print(f"Found {len(pdf_files)} PDF files")
    
    results = []
    needs_analysis = []
    
    for pdf_file in pdf_files:
        result = quick_screen_pdf(pdf_file)
        results.append(result)
        
        if result['needs_full_analysis']:
            needs_analysis.append(result)
            print(f"  ⚠️  {pdf_file.name}: {len(result['issues'])} issues")
        else:
            print(f"  ✅ {pdf_file.name}: No issues detected")
    
    return needs_analysis

def main():
    """Main function to screen PDFs"""
    cao_number = sys.argv[1] if len(sys.argv) > 1 else None
    
    print("Quick OCR Screening Analysis")
    print("============================")
    print(f"Input folder: {INPUT_FOLDER}")
    
    all_problematic_pdfs = []
    
    if cao_number:
        # Screen specific CAO
        cao_folder = Path(INPUT_FOLDER) / cao_number
        if not cao_folder.exists():
            print(f"Error: CAO folder {cao_folder} does not exist")
            return
        
        problematic_pdfs = screen_cao_pdfs(cao_folder)
        all_problematic_pdfs.extend(problematic_pdfs)
    else:
        # Screen all CAOs
        cao_folders = [f for f in Path(INPUT_FOLDER).iterdir() if f.is_dir() and f.name.isdigit()]
        cao_folders.sort(key=lambda x: int(x.name))
        
        print(f"\nFound {len(cao_folders)} CAO folders to screen")
        
        for cao_folder in cao_folders:
            problematic_pdfs = screen_cao_pdfs(cao_folder)
            all_problematic_pdfs.extend(problematic_pdfs)
    
    # Generate summary
    print(f"\n{'='*60}")
    print("SCREENING SUMMARY")
    print(f"{'='*60}")
    
    if all_problematic_pdfs:
        print(f"\n⚠️  FOUND {len(all_problematic_pdfs)} PDFS NEEDING FULL ANALYSIS:")
        print("These PDFs should be analyzed with the full OCR comparison script.")
        print("\nFiles requiring full analysis:")
        
        for pdf_info in all_problematic_pdfs:
            print(f"\n📄 {pdf_info['filename']}")
            print(f"   PyPDF2 length: {pdf_info['pypdf2_length']}")
            print(f"   pdfplumber length: {pdf_info['pdfplumber_length']}")
            print("   Issues:")
            for issue in pdf_info['issues']:
                print(f"     - {issue}")
    else:
        print("\n✅ NO PROBLEMATIC PDFS FOUND")
        print("All PDFs appear to be compatible with direct PDF upload to Gemini.")
    
    print(f"\nScreening completed. {len(all_problematic_pdfs)} PDFs need full analysis.")

if __name__ == "__main__":
    main()
