"""
PDF Hidden Text Detection Script
================================

DESCRIPTION:
This script identifies PDFs that might have hidden text or content outside page boundaries,
which could be problematic when using direct PDF upload to Gemini instead of the parsing step.
It compares text extraction results between different methods to detect potential issues.

FEATURES:
- Compares PyPDF2, pdfplumber, and OCR text extraction methods
- Detects content outside page boundaries
- Identifies PDFs with hidden text or annotations
- Reports potential data loss when skipping parsing step
- Provides detailed analysis for each problematic PDF

USAGE:
    python scripts/identify_hidden_text_pdfs.py [CAO_NUMBER]
    
    If no CAO number is provided, analyzes all CAOs in the input folder.

ARGUMENTS:
    CAO_NUMBER: Optional specific CAO number to analyze

OUTPUT:
    - Console output with analysis results
    - Detailed report saved to outputs/logs/hidden_text_analysis.txt
"""

import os
import sys
import json
from pathlib import Path
import yaml
from PyPDF2 import PdfReader
import pdfplumber
from pdf2image import convert_from_path
import pytesseract
import re

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load configuration
with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

INPUT_FOLDER = config['paths']['inputs_pdfs']
OUTPUT_LOG = Path(config['paths']['outputs_json']).parent / 'logs' / 'hidden_text_analysis.txt'

def extract_text_pypdf2(pdf_path):
    """Extract text using PyPDF2 (basic text extraction)"""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text.strip()
    except Exception as e:
        return f"ERROR: {str(e)}"

def extract_text_pdfplumber(pdf_path):
    """Extract text using pdfplumber (more comprehensive)"""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                text += page_text
        return text.strip()
    except Exception as e:
        return f"ERROR: {str(e)}"

def extract_text_ocr(pdf_path):
    """Extract text using OCR (captures text in images)"""
    try:
        images = convert_from_path(pdf_path)
        text = ""
        for i, image in enumerate(images):
            page_text = pytesseract.image_to_string(image, lang='nld+eng')
            text += page_text
        return text.strip()
    except Exception as e:
        return f"ERROR: {str(e)}"

def analyze_pdf_for_hidden_content(pdf_path):
    """Analyze a PDF for potential hidden content issues"""
    filename = pdf_path.name
    
    # Extract text using different methods
    pypdf2_text = extract_text_pypdf2(pdf_path)
    pdfplumber_text = extract_text_pdfplumber(pdf_path)
    ocr_text = extract_text_ocr(pdf_path)
    
    # Calculate text lengths
    pypdf2_len = len(pypdf2_text) if not pypdf2_text.startswith("ERROR") else 0
    pdfplumber_len = len(pdfplumber_text) if not pdfplumber_text.startswith("ERROR") else 0
    ocr_len = len(ocr_text) if not ocr_text.startswith("ERROR") else 0
    
    # Check for potential issues
    issues = []
    
    # Issue 1: Significant difference between PyPDF2 and pdfplumber
    if pypdf2_len > 0 and pdfplumber_len > 0:
        diff_ratio = abs(pdfplumber_len - pypdf2_len) / max(pypdf2_len, pdfplumber_len)
        if diff_ratio > 0.2:  # 20% difference
            issues.append(f"Large text difference between methods: PyPDF2={pypdf2_len}, pdfplumber={pdfplumber_len} (ratio: {diff_ratio:.2f})")
    
    # Issue 2: OCR finds significantly more text than other methods
    if ocr_len > 0 and pypdf2_len > 0:
        ocr_ratio = ocr_len / pypdf2_len if pypdf2_len > 0 else 0
        if ocr_ratio > 1.5:  # OCR finds 50% more text
            issues.append(f"OCR finds significantly more text: OCR={ocr_len}, PyPDF2={pypdf2_len} (ratio: {ocr_ratio:.2f})")
    
    # Issue 3: pdfplumber finds more text than PyPDF2 (potential hidden content)
    if pdfplumber_len > pypdf2_len * 1.3:  # 30% more
        issues.append(f"pdfplumber finds more text than PyPDF2: pdfplumber={pdfplumber_len}, PyPDF2={pypdf2_len}")
    
    # Issue 4: Check for specific patterns that might indicate hidden content
    if pdfplumber_text and not pdfplumber_text.startswith("ERROR"):
        # Look for text that might be outside page boundaries
        lines = pdfplumber_text.split('\n')
        for line in lines:
            # Check for very long lines that might be outside boundaries
            if len(line.strip()) > 200:
                issues.append(f"Very long line detected: {line[:100]}...")
                break
            
            # Check for text that might be annotations or comments
            if any(keyword in line.lower() for keyword in ['comment', 'annotation', 'note', 'remark']):
                issues.append(f"Potential annotation/comment detected: {line[:100]}...")
    
    # Issue 5: Check if OCR finds text that other methods miss
    if ocr_text and not ocr_text.startswith("ERROR"):
        ocr_words = set(re.findall(r'\b\w+\b', ocr_text.lower()))
        pypdf2_words = set(re.findall(r'\b\w+\b', pypdf2_text.lower())) if not pypdf2_text.startswith("ERROR") else set()
        pdfplumber_words = set(re.findall(r'\b\w+\b', pdfplumber_text.lower())) if not pdfplumber_text.startswith("ERROR") else set()
        
        # Find words unique to OCR
        unique_ocr_words = ocr_words - pypdf2_words - pdfplumber_words
        if len(unique_ocr_words) > 10:  # More than 10 unique words
            issues.append(f"OCR finds {len(unique_ocr_words)} unique words not found by other methods")
    
    return {
        'filename': filename,
        'pypdf2_length': pypdf2_len,
        'pdfplumber_length': pdfplumber_len,
        'ocr_length': ocr_len,
        'issues': issues,
        'has_problems': len(issues) > 0
    }

def analyze_cao_pdfs(cao_folder):
    """Analyze all PDFs in a CAO folder"""
    cao_number = cao_folder.name
    pdf_files = list(cao_folder.glob('*.pdf'))
    
    if not pdf_files:
        return []
    
    print(f"\n=== Analyzing CAO {cao_number} ===")
    print(f"Found {len(pdf_files)} PDF files")
    
    results = []
    problematic_pdfs = []
    
    for pdf_file in pdf_files:
        print(f"  Analyzing: {pdf_file.name}")
        result = analyze_pdf_for_hidden_content(pdf_file)
        results.append(result)
        
        if result['has_problems']:
            problematic_pdfs.append(result)
            print(f"    ⚠️  PROBLEMS DETECTED:")
            for issue in result['issues']:
                print(f"      - {issue}")
        else:
            print(f"    ✅ No issues detected")
    
    return problematic_pdfs

def main():
    """Main function to analyze PDFs for hidden content issues"""
    cao_number = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Create output directory
    OUTPUT_LOG.parent.mkdir(parents=True, exist_ok=True)
    
    print("PDF Hidden Text Detection Analysis")
    print("==================================")
    print(f"Input folder: {INPUT_FOLDER}")
    print(f"Output log: {OUTPUT_LOG}")
    
    all_problematic_pdfs = []
    
    if cao_number:
        # Analyze specific CAO
        cao_folder = Path(INPUT_FOLDER) / cao_number
        if not cao_folder.exists():
            print(f"Error: CAO folder {cao_folder} does not exist")
            return
        
        problematic_pdfs = analyze_cao_pdfs(cao_folder)
        all_problematic_pdfs.extend(problematic_pdfs)
    else:
        # Analyze all CAOs
        cao_folders = [f for f in Path(INPUT_FOLDER).iterdir() if f.is_dir() and f.name.isdigit()]
        cao_folders.sort(key=lambda x: int(x.name))
        
        print(f"\nFound {len(cao_folders)} CAO folders to analyze")
        
        for cao_folder in cao_folders:
            problematic_pdfs = analyze_cao_pdfs(cao_folder)
            all_problematic_pdfs.extend(problematic_pdfs)
    
    # Generate summary report
    print(f"\n{'='*60}")
    print("SUMMARY REPORT")
    print(f"{'='*60}")
    
    if all_problematic_pdfs:
        print(f"\n⚠️  FOUND {len(all_problematic_pdfs)} PROBLEMATIC PDFS:")
        print("These PDFs may have hidden text or content outside page boundaries")
        print("that could be missed when using direct PDF upload to Gemini.")
        print("\nDetailed list:")
        
        for pdf_info in all_problematic_pdfs:
            print(f"\n📄 {pdf_info['filename']}")
            print(f"   PyPDF2 length: {pdf_info['pypdf2_length']}")
            print(f"   pdfplumber length: {pdf_info['pdfplumber_length']}")
            print(f"   OCR length: {pdf_info['ocr_length']}")
            print("   Issues:")
            for issue in pdf_info['issues']:
                print(f"     - {issue}")
    else:
        print("\n✅ NO PROBLEMATIC PDFS FOUND")
        print("All PDFs appear to be compatible with direct PDF upload to Gemini.")
    
    # Save detailed report to file
    with open(OUTPUT_LOG, 'w', encoding='utf-8') as f:
        f.write("PDF Hidden Text Detection Analysis Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Analysis date: {__import__('datetime').datetime.now()}\n")
        f.write(f"Input folder: {INPUT_FOLDER}\n\n")
        
        if all_problematic_pdfs:
            f.write(f"FOUND {len(all_problematic_pdfs)} PROBLEMATIC PDFS:\n\n")
            for pdf_info in all_problematic_pdfs:
                f.write(f"File: {pdf_info['filename']}\n")
                f.write(f"PyPDF2 length: {pdf_info['pypdf2_length']}\n")
                f.write(f"pdfplumber length: {pdf_info['pdfplumber_length']}\n")
                f.write(f"OCR length: {pdf_info['ocr_length']}\n")
                f.write("Issues:\n")
                for issue in pdf_info['issues']:
                    f.write(f"  - {issue}\n")
                f.write("\n" + "-" * 40 + "\n\n")
        else:
            f.write("NO PROBLEMATIC PDFS FOUND\n")
            f.write("All PDFs appear to be compatible with direct PDF upload to Gemini.\n")
    
    print(f"\nDetailed report saved to: {OUTPUT_LOG}")

if __name__ == "__main__":
    main()
