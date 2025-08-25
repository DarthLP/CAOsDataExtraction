"""
Single PDF Hidden Text Detection Script
======================================

DESCRIPTION:
This script analyzes a single specific PDF file for hidden text or content outside page boundaries,
with enhanced detection including checking if normal extraction finds text that OCR doesn't.

USAGE:
    python scripts/analyze_single_pdf.py [CAO_NUMBER] [PDF_FILENAME]
    
    Example:
    python scripts/analyze_single_pdf.py 316 levensmiddelensbedrijf_definitief.pdf

ARGUMENTS:
    CAO_NUMBER: CAO number containing the PDF
    PDF_FILENAME: Specific PDF filename to analyze
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

def analyze_single_pdf_for_hidden_content(cao_number, pdf_filename):
    """Analyze a single PDF for potential hidden content issues"""
    pdf_path = Path(INPUT_FOLDER) / cao_number / pdf_filename
    
    if not pdf_path.exists():
        print(f"Error: PDF file {pdf_path} does not exist")
        return None
    
    print(f"Analyzing: {pdf_filename}")
    print(f"Path: {pdf_path}")
    print("=" * 60)
    
    # Extract text using different methods
    print("Extracting text using different methods...")
    pypdf2_text = extract_text_pypdf2(pdf_path)
    pdfplumber_text = extract_text_pdfplumber(pdf_path)
    ocr_text = extract_text_ocr(pdf_path)
    
    # Calculate text lengths
    pypdf2_len = len(pypdf2_text) if not pypdf2_text.startswith("ERROR") else 0
    pdfplumber_len = len(pdfplumber_text) if not pdfplumber_text.startswith("ERROR") else 0
    ocr_len = len(ocr_text) if not ocr_text.startswith("ERROR") else 0
    
    print(f"\nText extraction results:")
    print(f"PyPDF2 length: {pypdf2_len}")
    print(f"pdfplumber length: {pdfplumber_len}")
    print(f"OCR length: {ocr_len}")
    
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
    
    # NEW: Issue 6: Check if normal extraction finds text that OCR doesn't
    if pypdf2_text and not pypdf2_text.startswith("ERROR") and ocr_text and not ocr_text.startswith("ERROR"):
        pypdf2_words = set(re.findall(r'\b\w+\b', pypdf2_text.lower()))
        pdfplumber_words = set(re.findall(r'\b\w+\b', pdfplumber_text.lower())) if not pdfplumber_text.startswith("ERROR") else set()
        ocr_words = set(re.findall(r'\b\w+\b', ocr_text.lower()))
        
        # Find words unique to PyPDF2 (not in OCR)
        unique_pypdf2_words = pypdf2_words - ocr_words
        if len(unique_pypdf2_words) > 10:
            issues.append(f"PyPDF2 finds {len(unique_pypdf2_words)} unique words not found by OCR")
        
        # Find words unique to pdfplumber (not in OCR)
        unique_pdfplumber_words = pdfplumber_words - ocr_words
        if len(unique_pdfplumber_words) > 10:
            issues.append(f"pdfplumber finds {len(unique_pdfplumber_words)} unique words not found by OCR")
    
    # NEW: Issue 7: Check for significant differences in word counts
    if pypdf2_len > 0 and ocr_len > 0:
        pypdf2_word_count = len(re.findall(r'\b\w+\b', pypdf2_text.lower())) if not pypdf2_text.startswith("ERROR") else 0
        ocr_word_count = len(re.findall(r'\b\w+\b', ocr_text.lower())) if not ocr_text.startswith("ERROR") else 0
        
        if pypdf2_word_count > 0 and ocr_word_count > 0:
            word_ratio = pypdf2_word_count / ocr_word_count
            if word_ratio > 1.3:  # Normal extraction finds 30% more words
                issues.append(f"Normal extraction finds significantly more words: PyPDF2={pypdf2_word_count}, OCR={ocr_word_count} (ratio: {word_ratio:.2f})")
            elif word_ratio < 0.7:  # OCR finds 30% more words
                issues.append(f"OCR finds significantly more words: PyPDF2={pypdf2_word_count}, OCR={ocr_word_count} (ratio: {word_ratio:.2f})")
    
    # Display results
    print(f"\n{'='*60}")
    print("ANALYSIS RESULTS")
    print(f"{'='*60}")
    
    if issues:
        print(f"\n⚠️  FOUND {len(issues)} POTENTIAL ISSUES:")
        print("This PDF may have hidden text or content outside page boundaries")
        print("that could be missed when using direct PDF upload to Gemini.")
        print("\nIssues detected:")
        
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
    else:
        print("\n✅ NO ISSUES DETECTED")
        print("This PDF appears to be compatible with direct PDF upload to Gemini.")
    
    # Show sample text differences
    print(f"\n{'='*60}")
    print("TEXT SAMPLE COMPARISON")
    print(f"{'='*60}")
    
    if pypdf2_text and not pypdf2_text.startswith("ERROR"):
        print(f"\nPyPDF2 sample (first 200 chars):")
        print(f"'{pypdf2_text[:200]}...'")
    
    if pdfplumber_text and not pdfplumber_text.startswith("ERROR"):
        print(f"\npdfplumber sample (first 200 chars):")
        print(f"'{pdfplumber_text[:200]}...'")
    
    if ocr_text and not ocr_text.startswith("ERROR"):
        print(f"\nOCR sample (first 200 chars):")
        print(f"'{ocr_text[:200]}...'")
    
    return {
        'filename': pdf_filename,
        'pypdf2_length': pypdf2_len,
        'pdfplumber_length': pdfplumber_len,
        'ocr_length': ocr_len,
        'issues': issues,
        'has_problems': len(issues) > 0
    }

def main():
    """Main function to analyze a single PDF"""
    if len(sys.argv) != 3:
        print("Usage: python scripts/analyze_single_pdf.py [CAO_NUMBER] [PDF_FILENAME]")
        print("Example: python scripts/analyze_single_pdf.py 316 levensmiddelensbedrijf_definitief.pdf")
        return
    
    cao_number = sys.argv[1]
    pdf_filename = sys.argv[2]
    
    print("Single PDF Hidden Text Detection Analysis")
    print("=========================================")
    print(f"CAO Number: {cao_number}")
    print(f"PDF Filename: {pdf_filename}")
    print(f"Input folder: {INPUT_FOLDER}")
    
    result = analyze_single_pdf_for_hidden_content(cao_number, pdf_filename)
    
    if result:
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"File: {result['filename']}")
        print(f"PyPDF2 length: {result['pypdf2_length']}")
        print(f"pdfplumber length: {result['pdfplumber_length']}")
        print(f"OCR length: {result['ocr_length']}")
        print(f"Has problems: {result['has_problems']}")
        print(f"Number of issues: {len(result['issues'])}")

if __name__ == "__main__":
    main()
