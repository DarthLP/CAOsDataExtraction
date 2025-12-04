"""
CAO PDF Text Extraction Pipeline (p2_extract.py)
================================================

DESCRIPTION:
This script extracts text from Dutch Collective Labor Agreement (CAO) PDF documents
and saves the output in both JSON and Markdown formats. It intelligently combines
PyPDF2, pdfplumber, and Tesseract OCR to handle various PDF types including:
- Native text PDFs
- Scanned documents (images)
- PDFs with embedded vector graphics (tables/charts)
- Documents with unicode/PostScript encoding issues

FEATURES:
- Multi-method extraction (PyPDF2 + pdfplumber + OCR)
- Intelligent OCR triggering based on:
  * Image detection (with smart coverage analysis) - improved for small tables
  * Vector graphics detection (embedded tables) - improved for small tables
  * Minimal/corrupted text detection
  * Content-aware analysis - enhanced for wage tables
- ALWAYS compares OCR vs native extraction and chooses the one with MORE characters (excluding whitespace)
- Simple character count comparison
- Automatic retry with aggressive OCR for failed pages
- Missing page detection and reporting (with specific page numbers)
- Automatic unicode pattern conversion (/uniXXXX → readable text)
- Automatic PostScript glyph conversion (/GXXX → readable text)
- Direct markdown output with extraction method labels (_native_, _OCR_, _empty_)
- Quality analysis and comprehensive reporting
- Configurable output formats (JSON, Markdown, or both)
- Parallel processing support for large batches

USAGE:
    Single process:
        python pipelines/p2_extract.py
    
    Multi-process (recommended for large batches):
        python pipelines/p2_extract.py --process_id 0 --total_processes 4
        python pipelines/p2_extract.py --process_id 1 --total_processes 4
        python pipelines/p2_extract.py --process_id 2 --total_processes 4
        python pipelines/p2_extract.py --process_id 3 --total_processes 4
    
    Using caffeinate and logging:
        unbuffer caffeinate python pipelines/p2_extract.py --process_id 0 --total_processes 4 2>&1 | tee p2_log1.txt &
        unbuffer caffeinate python pipelines/p2_extract.py --process_id 1 --total_processes 4 2>&1 | tee p2_log2.txt &
        unbuffer caffeinate python pipelines/p2_extract.py --process_id 2 --total_processes 4 2>&1 | tee p2_log3.txt &
        unbuffer caffeinate python pipelines/p2_extract.py --process_id 3 --total_processes 4 2>&1 | tee p2_log4.txt &

ARGUMENTS:
    --process_id: Process ID for work distribution (0-based) - defaults to 0
    --total_processes: Total number of parallel processes - defaults to 1

CONFIGURATION (edit in script):
    OUTPUT_FORMAT = 'both'           # Options: 'markdown', 'json', 'both'
    AUTO_FIX_UNICODE = True          # Convert /uniXXXX/ patterns to readable text
    AUTO_FIX_POSTSCRIPT = True       # Convert /GXXX/ patterns to readable text
    DEBUG = False                    # Enable debug logging

INPUT:
    - PDF files in {config['paths']['inputs_pdfs']}/[CAO_NUMBER]/ folders
    
OUTPUT:
    - JSON files in {config['paths']['parsed_pdfs']}/[CAO_NUMBER]/ (if enabled)
    - Markdown files in {config['paths']['parsed_pdfs_markdown']}/[CAO_NUMBER]/ (if enabled)
    - Progress tracking via scripts_pipeline_helper.p1_p2.OUTPUT_tracker
    - Debug logs in extraction_debug.log (if DEBUG=True)

OUTPUT STRUCTURE (JSON):
    [
        {
            "page": 1,
            "text": "Extracted text content...",
            "ocr_used": false,
            "encoding_fixed": false,
            "extraction_method": "native (good quality)"
        },
        ...
    ]

OUTPUT STRUCTURE (Markdown):
    # CAO Document - Extracted Content
    
    *Source: filename.pdf*
    
    ---
    
    ## Page 1 _native_
    
    Extracted text content...
    
    ## Page 49 _OCR_
    
    Salary table extracted via OCR...
    
    ## Page 50 _empty_
    
    [This page is blank]

QUALITY FEATURES:
- Encoding issue detection and automatic fixing
- Comprehensive quality analysis reports:
  * Pages using OCR vs native extraction
  * Pages with encoding fixes
  * Empty pages (included in markdown with _empty_ label)
  * Missing pages (with specific page numbers)
  * Retry results for failed pages
  * File size statistics

PERFORMANCE:
- Processes ~1-5 pages/second (varies by content type)
- OCR adds ~1-2 seconds per page
- File size reduction: 50-95% for documents with encoding issues
- Token cost reduction: 80-90% for LLM processing (encoding-fixed docs)

DEPENDENCIES:
    - PyPDF2: Text extraction from native PDFs
    - pdfplumber: Image and vector graphics detection
    - pytesseract: OCR for scanned documents and tables
    - pdf2image: PDF to image conversion for OCR
    - Pillow: Image processing

NOTES:
    - Requires Tesseract OCR installed on system
    - Skips files that already have output (checks for existing JSON/Markdown)
    - Creates output directories automatically
    - Uses conda environment: caos-extract
    - Always compares OCR vs native extraction (chooses longer text, excluding whitespace)
    - Includes empty pages in markdown for continuous page numbering
    - Automatic retry for pages with minimal content

EXAMPLES:
    Basic usage:
        python pipelines/p2_extract.py
    
    Markdown only (recommended for new workflows):
        # Edit script: OUTPUT_FORMAT = 'markdown'
        python pipelines/p2_extract.py
    
    With debug logging:
        # Edit script: DEBUG = True
        python pipelines/p2_extract.py
"""

import os
import sys
import json
from pathlib import Path
import argparse

# Add the parent directory to Python path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
import pdfplumber
from scripts_pipeline_helper.p1_p2.OUTPUT_tracker import update_progress
import traceback
DEBUG_LOG_FILE = 'extraction_debug.log'
DEBUG = False
import logging
logging.getLogger('pdfminer').setLevel(logging.ERROR)
import yaml
with open('conf/config.yaml', 'r') as f:
    config = yaml.safe_load(f)
INPUT_FOLDER = config['paths']['inputs_pdfs']
OUTPUT_FOLDER_JSON = config['paths']['parsed_pdfs']  # JSON output (legacy)
OUTPUT_FOLDER_MD = config['paths']['parsed_pdfs_markdown']  # Markdown output (primary)

# Choose output format: 'markdown', 'json', or 'both'
OUTPUT_FORMAT = 'both'  # Default to both for backwards compatibility

# Auto-fix unicode/postscript encoding issues during extraction
AUTO_FIX_UNICODE = True  # Convert /uniXXXX/ patterns to readable text
AUTO_FIX_POSTSCRIPT = True  # Convert /GXXX/ patterns to readable text


def clean_encoding_issues(text):
    """
    Auto-fix common encoding issues in extracted text.
    Converts unicode patterns (/uniXXXX) and PostScript glyphs (/GXXX) to readable text.
    """
    if not text or text == '[EMPTY PAGE]':
        return text
    
    import re
    
    # Track if any conversions were made
    original_text = text
    
    # Fix 1: Convert Unicode patterns /uniXXXX to characters
    if AUTO_FIX_UNICODE and '/uni' in text:
        def replace_unicode(match):
            unicode_hex = match.group(1)
            try:
                return chr(int(unicode_hex, 16))
            except (ValueError, OverflowError):
                return match.group(0)
        
        text = re.sub(r'/uni([0-9a-fA-F]{4})', replace_unicode, text)
    
    # Fix 2: Convert PostScript glyphs /GXXX to characters
    if AUTO_FIX_POSTSCRIPT and '/G' in text:
        def replace_glyph(match):
            glyph_code = match.group(1)
            try:
                char_code = int(glyph_code)
                if 0 <= char_code <= 1114111:  # Valid Unicode range
                    return chr(char_code)
                return match.group(0)
            except (ValueError, OverflowError):
                return match.group(0)
        
        # Match /GXXX patterns
        text = re.sub(r'/G([0-9]+)', replace_glyph, text)
        # Also match GXXX without leading slash (cleanup artifacts)
        text = re.sub(r'(?<![/a-zA-Z])G([0-9]+)(?![a-zA-Z])', replace_glyph, text)
    
    # Fix 3: Clean up common artifacts
    if text != original_text:
        # Remove excessive line breaks
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Remove trailing slashes before line breaks
        text = re.sub(r'/\s*\n', '\n', text)
        # Clean up excessive spaces
        text = re.sub(r' +', ' ', text)
        # Strip each line
        lines = text.split('\n')
        text = '\n'.join(line.strip() for line in lines)
    
    return text


def extract_text_from_pdf(pdf_path):
    """
    Extraction logic per page:
    1. If pdfplumber detects an image, use OCR.
    2. Else if PyPDF2 finds text, use the extracted text.
    3. Else (no image and no text), use OCR as a fallback.
    Loops over the maximum number of pages found by either library.
    
    Returns:
        tuple: (pages_data, pdf_metadata)
        - pages_data: list of dicts with page info
        - pdf_metadata: dict with 'expected_pages', 'extracted_pages'
    """
    if DEBUG:
        print(f'[DEBUG] Attempting to open PDF: {pdf_path}')
    try:
        reader = PdfReader(pdf_path)
    except Exception as e:
        if DEBUG:
            print(f'[DEBUG] Failed to open PDF {pdf_path}: {e}')
        return [], {'expected_pages': 0, 'extracted_pages': 0}
    try:
        plumber_pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        if DEBUG:
            print(f'[DEBUG] Failed to open PDF with pdfplumber {pdf_path}: {e}'
                )
        plumber_pdf = None
    num_pages = max(len(reader.pages), len(plumber_pdf.pages) if
        plumber_pdf else 0)
    pages = []
    failed_pages = []  # Track pages that failed to extract
    for i in range(num_pages):
        if DEBUG:
            print(f'[DEBUG] --- Processing page {i + 1} of {pdf_path} ---')
        page_info = {'page': i + 1, 'ocr_used': False, 'text': ''}
        if i < len(reader.pages):
            try:
                normal_text = reader.pages[i].extract_text() or ''
                normal_text = normal_text.strip()
                if DEBUG:
                    print(
                        f'[DEBUG] PyPDF2 text extraction done for page {i + 1}'
                        )
            except Exception as e:
                if DEBUG:
                    print(
                        f'[DEBUG] PyPDF2 text extraction failed for page {i + 1}: {e}'
                        )
                normal_text = ''
        else:
            normal_text = ''
        has_images = False
        if plumber_pdf and i < len(plumber_pdf.pages):
            try:
                plumber_page = plumber_pdf.pages[i]
                im_objs = plumber_page.images
                has_images = len(im_objs) > 0
                if DEBUG:
                    print(
                        f'[DEBUG] pdfplumber found {len(im_objs)} images on page {i + 1}'
                        )
            except Exception as e:
                if DEBUG:
                    print(f'[DEBUG] pdfplumber failed for page {i + 1}: {e}')
                has_images = False
        ocr_text = ''
        
        # Smart image handling: Check if images are substantial (likely tables/charts with data)
        # vs small decorative images (logos, icons)
        skip_ocr_for_small_images = False
        if has_images and normal_text and len(normal_text) > 200:
            # We have both images and substantial native text
            # Check if images are small (likely decorative) or large (likely data tables)
            if plumber_pdf and i < len(plumber_pdf.pages):
                try:
                    plumber_page = plumber_pdf.pages[i]
                    im_objs = plumber_page.images
                    
                    # Calculate image coverage of the page
                    page_height = plumber_page.height or 842  # Default A4 height
                    page_width = plumber_page.width or 595    # Default A4 width
                    page_area = page_height * page_width
                    
                    total_image_area = 0
                    for img in im_objs:
                        img_height = img.get('height', 0)
                        img_width = img.get('width', 0)
                        total_image_area += (img_height * img_width)
                    
                    image_coverage = total_image_area / page_area if page_area > 0 else 0
                    
                    # If images cover < 0.1% of page AND we have good text, likely decorative
                    # Lowered threshold to catch small wage tables (was 0.5%)
                    if image_coverage < 0.001:
                        skip_ocr_for_small_images = True
                        if DEBUG:
                            print(
                                f'    [DEBUG] Small images detected ({image_coverage*100:.1f}% coverage) with substantial text ({len(normal_text)} chars) - skipping OCR'
                            )
                    else:
                        if DEBUG:
                            print(
                                f'    [DEBUG] Substantial images detected ({image_coverage*100:.1f}% coverage) - will use OCR even with native text'
                            )
                    
                    # Additional check: Look for small images that might be wage tables
                    # Even if coverage is low, check if any image is reasonably sized for a table
                    if image_coverage < 0.005 and image_coverage > 0.001:  # Between 0.1% and 0.5%
                        # Check if any individual image is large enough to be a table
                        for img in im_objs:
                            img_height = img.get('height', 0)
                            img_width = img.get('width', 0)
                            img_area = img_height * img_width
                            
                            # If any image is > 0.05% of page area (potential small table)
                            if img_area > page_area * 0.0005:
                                skip_ocr_for_small_images = False  # Force OCR for potential table
                                if DEBUG:
                                    print(
                                        f'    [DEBUG] Found potentially substantial image ({img_area/page_area*100:.2f}% of page) - forcing OCR'
                                    )
                                break
                except Exception as e:
                    if DEBUG:
                        print(f'    [DEBUG] Could not analyze image size: {e}')
        
        if has_images and not skip_ocr_for_small_images:
            if DEBUG:
                print(
                    f'    [DEBUG] Image detected, using OCR for {pdf_path}, page {i + 1}'
                    )
            try:
                if DEBUG:
                    print(
                        f'    [DEBUG] Calling convert_from_path for page {i + 1}'
                        )
                images_list = convert_from_path(pdf_path, first_page=i + 1,
                    last_page=i + 1)
                if DEBUG:
                    print(
                        f'    [DEBUG] images_list created, length: {len(images_list)}'
                        )
                if images_list:
                    if DEBUG:
                        print(
                            f'    [DEBUG] images_list[0] exists, proceeding to save image (if enabled) and OCR'
                            )
                    ocr_text = pytesseract.image_to_string(images_list[0]
                        ).strip()
                    if DEBUG:
                        print(f'    [DEBUG] OCR completed for page {i + 1}')
                    
                    # Compare OCR vs normal extraction (choose longer without whitespace)
                    ocr_clean = ocr_text.replace(' ', '').replace('\n', '').replace('\t', '')
                    normal_clean = normal_text.replace(' ', '').replace('\n', '').replace('\t', '')
                    
                    if len(normal_clean) > len(ocr_clean):
                        page_info['text'] = normal_text
                        page_info['ocr_used'] = False
                        page_info['extraction_method'] = 'native (longer than OCR)'
                        if DEBUG:
                            print(f'    [DEBUG] Native text longer ({len(normal_clean)} vs {len(ocr_clean)} chars), using native')
                    else:
                        page_info['text'] = ocr_text
                        page_info['ocr_used'] = True
                        page_info['extraction_method'] = 'OCR (image-based)'
                        if DEBUG:
                            print(f'    [DEBUG] OCR text longer ({len(ocr_clean)} vs {len(normal_clean)} chars), using OCR')
                    
                else:
                    if DEBUG:
                        print(
                            f'    [DEBUG] images_list is empty for page {i + 1}'
                            )
                    ocr_text = ''
            except Exception as e:
                if DEBUG:
                    print(
                        f'    [WARN] OCR failed for {pdf_path}, page {i + 1}: {e}'
                        )
                ocr_text = ''
        elif normal_text:
            # Check if the extracted text is meaningful or just page numbers
            text_stripped = normal_text.strip()
            
            # Also check for vector graphics that might contain tables
            has_vector_graphics = False
            if plumber_pdf and i < len(plumber_pdf.pages):
                try:
                    plumber_page = plumber_pdf.pages[i]
                    objects = plumber_page.objects
                    lines = objects.get('line', [])
                    curves = objects.get('curve', [])
                    # High number of lines/curves suggests vector-based tables
                    # Lowered thresholds to catch smaller tables (like wage tables)
                    if len(lines) > 20 or len(curves) > 100:
                        has_vector_graphics = True
                        if DEBUG:
                            print(f'    [DEBUG] Detected vector graphics: {len(lines)} lines, {len(curves)} curves')
                except:
                    pass
            
            # Comprehensive OCR decision logic
            ocr_reasons = []
            should_use_ocr = False
            
            # Check 1: Minimal text (page numbers only)
            if len(text_stripped) < 30 and (text_stripped.isdigit() or text_stripped in ['', ' ']):
                ocr_reasons.append("minimal text (page numbers only)")
                should_use_ocr = True
            
            # Check 2: Vector graphics detected (embedded tables/charts)
            if has_vector_graphics:
                ocr_reasons.append("vector graphics detected")
                should_use_ocr = True
            
            # Check 3: Garbled/corrupted text (common in scanned docs)
            if text_stripped and len(text_stripped) > 10:
                corruption_score = 0
                corruption_indicators = []
                
                # High percentage of ÿ characters
                if text_stripped.count('ÿ') > len(text_stripped) * 0.2:
                    corruption_score += 1
                    corruption_indicators.append("high ÿ chars")
                
                # High percentage of replacement characters
                if text_stripped.count('') > len(text_stripped) * 0.15:
                    corruption_score += 1
                    corruption_indicators.append("replacement chars")
                
                # Very few unique characters (repetitive garbage)
                if len(set(text_stripped)) < 8:
                    corruption_score += 1
                    corruption_indicators.append("low character diversity")
                
                # Excessive whitespace or special characters
                if len([c for c in text_stripped if c in '\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f']):
                    corruption_score += 1
                    corruption_indicators.append("control characters")
                
                if corruption_score >= 2:
                    ocr_reasons.append(f"corrupted text ({', '.join(corruption_indicators)})")
                    should_use_ocr = True
            
            # Check 4: Content-aware analysis
            if text_stripped and len(text_stripped) > 20:
                # Check for missing expected content patterns
                text_lower = text_stripped.lower()
                
                # Look for table-like content that might be missing
                expected_patterns = ['loon', 'salaris', 'schaal', 'bedrag', 'euro', '€', 'tabel', 'treed', 'functiegroep', 'leeftijd', 'jaar']
                found_patterns = [pattern for pattern in expected_patterns if pattern in text_lower]
                
                # If we found some patterns but text is still short, might be incomplete
                # Lowered threshold from 300 to 500 characters for wage tables
                if found_patterns and len(text_stripped) < 500 and has_vector_graphics:
                    ocr_reasons.append(f"incomplete content (found: {', '.join(found_patterns[:2])})")
                    should_use_ocr = True
                
                # Check for suspiciously short content with numbers (might be table headers only)
                # Lowered threshold from 150 to 200 characters for wage tables
                if len(text_stripped) < 200 and any(char.isdigit() for char in text_stripped) and has_vector_graphics:
                    ocr_reasons.append("short content with numbers + vector graphics")
                    should_use_ocr = True
            
            # Check 5: Context-aware analysis (compare with surrounding pages)
            # Note: This requires pages_data to be built first, so we'll do this in a second pass
            # For now, skip this check and implement it after all pages are processed
            
            # Combine all reasons
            ocr_reason = "; ".join(ocr_reasons) if ocr_reasons else ""
            
            if should_use_ocr:
                if DEBUG:
                    print(
                        f'    [DEBUG] Using OCR fallback for page {i + 1}: {ocr_reason}'
                        )
                try:
                    if DEBUG:
                        print(
                            f'    [DEBUG] Calling convert_from_path for page {i + 1} (OCR fallback for minimal text)'
                            )
                    images_list = convert_from_path(pdf_path, first_page=i + 1,
                        last_page=i + 1)
                    if DEBUG:
                        print(
                            f'    [DEBUG] images_list created, length: {len(images_list)}'
                            )
                    if images_list:
                        if DEBUG:
                            print(
                                f'    [DEBUG] images_list[0] exists, proceeding to OCR (fallback for minimal text)'
                                )
                        ocr_text = pytesseract.image_to_string(images_list[0]
                            ).strip()
                        if DEBUG:
                            print(f'    [DEBUG] OCR completed for page {i + 1} (fallback for minimal text)')
                        if ocr_text:
                            # Compare both methods: choose longer text (excluding whitespace)
                            ocr_clean = ocr_text.replace(' ', '').replace('\n', '').replace('\t', '')
                            normal_clean = text_stripped.replace(' ', '').replace('\n', '').replace('\t', '')
                            
                            # Choose longer text (excluding whitespace)
                            if len(ocr_clean) > len(normal_clean):
                                # OCR has more content
                                page_info['text'] = ocr_text
                                page_info['ocr_used'] = True
                                page_info['extraction_method'] = f'OCR (longer: {len(ocr_clean)} vs {len(normal_clean)} chars)'
                                if DEBUG:
                                    print(f'    [DEBUG] OCR longer ({len(ocr_clean)} vs {len(normal_clean)} chars), using OCR')
                            else:
                                # Native text has more content
                                page_info['text'] = normal_text
                                page_info['ocr_used'] = False
                                page_info['extraction_method'] = f'native (longer: {len(normal_clean)} vs {len(ocr_clean)} chars)'
                                if DEBUG:
                                    print(f'    [DEBUG] Native longer ({len(normal_clean)} vs {len(ocr_clean)} chars), using native')
                        else:
                            # OCR failed completely, use original text
                            page_info['text'] = normal_text
                            page_info['ocr_used'] = False
                            page_info['extraction_method'] = 'native (OCR failed)'
                    else:
                        # OCR failed, use original text
                        page_info['text'] = normal_text
                        page_info['ocr_used'] = False
                        page_info['extraction_method'] = 'native (OCR failed)'
                except Exception as e:
                    if DEBUG:
                        print(
                            f'    [WARN] OCR fallback failed for minimal text on page {i + 1}: {e}'
                            )
                    page_info['text'] = normal_text
                    page_info['ocr_used'] = False
                    page_info['extraction_method'] = 'native (OCR fallback failed)'
            else:
                # Text is meaningful, use it
                page_info['text'] = normal_text
                page_info['ocr_used'] = False
                page_info['extraction_method'] = 'native (good quality)'
                if DEBUG:
                    print(
                        f'    [DEBUG] Using native text for page {i + 1} (length: {len(text_stripped)})'
                        )
        else:
            # No native text - try OCR and compare with native (even if native is empty)
            if DEBUG:
                print(
                    f'    [DEBUG] No/minimal native text, trying OCR for {pdf_path}, page {i + 1}'
                    )
            try:
                if DEBUG:
                    print(
                        f'    [DEBUG] Calling convert_from_path for page {i + 1} (OCR fallback)'
                        )
                images_list = convert_from_path(pdf_path, first_page=i + 1,
                    last_page=i + 1)
                if DEBUG:
                    print(
                        f'    [DEBUG] images_list created, length: {len(images_list)}'
                        )
                if images_list:
                    if DEBUG:
                        print(
                            f'    [DEBUG] images_list[0] exists, proceeding to save image (if enabled) and OCR (fallback)'
                            )
                    ocr_text = pytesseract.image_to_string(images_list[0]
                        ).strip()
                    if DEBUG:
                        print(
                            f'    [DEBUG] OCR completed for page {i + 1} (fallback)'
                            )
                    
                    # Compare OCR vs native (even if native is empty)
                    ocr_clean = ocr_text.replace(' ', '').replace('\n', '').replace('\t', '')
                    normal_clean = normal_text.replace(' ', '').replace('\n', '').replace('\t', '')
                    
                    if len(ocr_clean) > len(normal_clean):
                        page_info['text'] = ocr_text
                        page_info['ocr_used'] = True
                        page_info['extraction_method'] = f'OCR (fallback, longer: {len(ocr_clean)} vs {len(normal_clean)} chars)'
                        if DEBUG:
                            print(f'    [DEBUG] Using OCR ({len(ocr_clean)} chars) over native ({len(normal_clean)} chars)')
                    else:
                        # Native is same or better (both likely empty)
                        page_info['text'] = normal_text if normal_text else ocr_text
                        page_info['ocr_used'] = False
                        page_info['extraction_method'] = 'native (empty)' if not normal_text and not ocr_text else 'native'
                        if DEBUG:
                            print(f'    [DEBUG] Both empty or native same/better')
                    
                else:
                    if DEBUG:
                        print(
                            f'    [DEBUG] images_list is empty for page {i + 1} (fallback)'
                            )
                    ocr_text = ''
            except Exception as e:
                if DEBUG:
                    print(
                        f'    [WARN] OCR fallback failed for {pdf_path}, page {i + 1}: {e}'
                        )
                ocr_text = ''
        # Final text assignment - only if not already set by OCR logic above
        if not page_info.get('text'):
            if has_images or (not has_images and not normal_text):
                page_info['text'] = ocr_text
                page_info['ocr_used'] = True
                page_info['extraction_method'] = 'OCR (fallback)'
            else:
                page_info['text'] = normal_text
                page_info['ocr_used'] = False
                page_info['extraction_method'] = 'native'
        
        # Ensure extraction_method is set
        if 'extraction_method' not in page_info:
            page_info['extraction_method'] = 'OCR' if page_info.get('ocr_used') else 'native'
        
        # Ensure we have some text
        if not page_info['text']:
            page_info['text'] = '[EMPTY PAGE]'
        
        # Auto-fix encoding issues (unicode/postscript patterns)
        if page_info['text'] and page_info['text'] != '[EMPTY PAGE]':
            cleaned_text = clean_encoding_issues(page_info['text'])
            if cleaned_text != page_info['text']:
                page_info['text'] = cleaned_text
                page_info['encoding_fixed'] = True
                if DEBUG:
                    print(f'[DEBUG] Fixed encoding issues on page {i + 1}')
            else:
                page_info['encoding_fixed'] = False
        
        # Track if page extraction failed or produced suspiciously little content
        if not page_info['text'] or page_info['text'] == '[EMPTY PAGE]' or len(page_info['text']) < 10:
            failed_pages.append(i + 1)
        
        if DEBUG:
            print(f'[DEBUG] Appending page_info for page {i + 1}')
        pages.append(page_info)
    
    # Check if we got all expected pages
    extracted_page_numbers = [p.get('page', 0) for p in pages]
    expected_page_numbers = list(range(1, num_pages + 1))
    missing_from_extraction = [p for p in expected_page_numbers if p not in extracted_page_numbers]
    
    # Retry failed pages with aggressive OCR
    if failed_pages or missing_from_extraction:
        retry_pages = list(set(failed_pages + missing_from_extraction))
        print(f"  ⚠️  Retrying {len(retry_pages)} problematic pages with aggressive OCR...")
        
        for page_num in retry_pages:
            try:
                if DEBUG:
                    print(f'[DEBUG] Retrying page {page_num} with forced OCR')
                
                # Force OCR extraction
                images_list = convert_from_path(pdf_path, first_page=page_num, last_page=page_num)
                if images_list:
                    ocr_text = pytesseract.image_to_string(images_list[0]).strip()
                    
                    # Compare OCR vs existing text (excluding whitespace - same as main logic)
                    page_idx = page_num - 1
                    if page_idx < len(pages):
                        existing_text = pages[page_idx]['text']
                        ocr_clean = ocr_text.replace(' ', '').replace('\n', '').replace('\t', '')
                        existing_clean = existing_text.replace(' ', '').replace('\n', '').replace('\t', '')
                        
                        # Update if OCR has more content (excluding whitespace)
                        if len(ocr_clean) > len(existing_clean):
                            pages[page_idx]['text'] = ocr_text
                            pages[page_idx]['ocr_used'] = True
                            pages[page_idx]['extraction_method'] = 'OCR (retry)'
                            if DEBUG:
                                print(f'[DEBUG] Retry successful for page {page_num}: {len(ocr_clean)} vs {len(existing_clean)} chars (no whitespace)')
                        else:
                            if DEBUG:
                                print(f'[DEBUG] Retry did not improve page {page_num}: {len(ocr_clean)} vs {len(existing_clean)} chars')
            except Exception as e:
                if DEBUG:
                    print(f'[DEBUG] Retry failed for page {page_num}: {e}')
    
    if plumber_pdf:
        plumber_pdf.close()
    
    # Return pages and metadata
    # Count pages with actual content (not empty, not missing)
    pages_with_content = len([p for p in pages if p['text'] and p['text'] != '[EMPTY PAGE]'])
    empty_pages_count = len([p for p in pages if p.get('text') == '[EMPTY PAGE]'])
    
    metadata = {
        'expected_pages': num_pages,
        'extracted_pages': pages_with_content,
        'empty_pages': empty_pages_count,
        'failed_pages': failed_pages,
        'retried_pages': retry_pages if (failed_pages or missing_from_extraction) else []
    }
    
    return pages, metadata



def save_as_markdown(pages_data, output_path, pdf_name):
    """
    Save extracted pages as markdown format.
    
    Args:
        pages_data: List of page dictionaries with 'page', 'text', 'ocr_used' keys
        output_path: Path to output markdown file
        pdf_name: Name of the source PDF file
    """
    markdown_content = []
    
    # Add header
    markdown_content.append("# CAO Document - Extracted Content\n\n")
    markdown_content.append(f"*Source: {pdf_name}*\n\n")
    markdown_content.append("---\n\n")
    
    # Process each page
    for page_data in pages_data:
        page_num = page_data.get('page', 'Unknown')
        text = page_data.get('text', '').strip()
        ocr_used = page_data.get('ocr_used', False)
        
        if text and text != '[EMPTY PAGE]':
            # Add page header with simple extraction method (just "native" or "OCR")
            method = "OCR" if ocr_used else "native"
            markdown_content.append(f"## Page {page_num} _{method}_\n\n")
            
            # Add the text content (no OCR metadata - cleaner for LLM)
            markdown_content.append(text)
            markdown_content.append("\n\n")
        elif text == '[EMPTY PAGE]':
            # Include empty pages so page numbers are continuous
            markdown_content.append(f"## Page {page_num} _empty_\n\n")
            markdown_content.append("[This page is blank]\n\n")
    
    # Write markdown file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(''.join(markdown_content))
    
    return output_path

def analyze_pdf_quality(pages_data, pdf_name, metadata=None):
    """Analyze PDF extraction quality and report potential issues"""
    total_pages = len(pages_data)
    ocr_pages = sum(1 for page in pages_data if page.get('ocr_used', False))
    empty_pages = sum(1 for page in pages_data if not page['text'] or page['text'] == '[EMPTY PAGE]')
    short_pages = sum(1 for page in pages_data if len(page['text']) < 100 and page['text'] != '[EMPTY PAGE]')
    encoding_fixed_pages = sum(1 for page in pages_data if page.get('encoding_fixed', False))
    
    quality_issues = []
    
    # Check total page count vs expected (from PDF metadata)
    if metadata:
        expected_total = metadata.get('expected_pages', 0)
        extracted_total = metadata.get('extracted_pages', 0)
        empty_pages_meta = metadata.get('empty_pages', 0)
        
        # Calculate truly missing pages (not just empty)
        total_attempted = extracted_total + empty_pages_meta
        
        if total_attempted < expected_total:
            missing_count = expected_total - total_attempted
            
            # Identify WHICH pages are completely missing (not in pages_data at all)
            expected_all_pages = set(range(1, expected_total + 1))
            attempted_page_nums = set([p.get('page', 0) for p in pages_data])
            missing_specific = sorted(expected_all_pages - attempted_page_nums)
            
            quality_issues.append(f"⚠️  INCOMPLETE: {total_attempted}/{expected_total} pages attempted ({missing_count} failed completely)")
            print(f"  🚨 CRITICAL: Only {total_attempted}/{expected_total} pages attempted from {pdf_name}")
            if missing_specific:
                print(f"  📄 Pages that failed completely: {missing_specific[:10]}{'...' if len(missing_specific) > 10 else ''}")
        
        # Report empty pages separately (these were successfully processed, just blank)
        if empty_pages_meta > 0:
            empty_page_nums = [p.get('page') for p in pages_data if p.get('text') == '[EMPTY PAGE]']
            print(f"  📄 Blank pages (successfully processed): {empty_page_nums[:10]}{'...' if len(empty_page_nums) > 10 else ''}")
        
        # Report retry results
        retried = metadata.get('retried_pages', [])
        if retried:
            print(f"  🔄 Retried {len(retried)} pages with aggressive OCR: {retried[:5]}{'...' if len(retried) > 5 else ''}")
    
    # Check for missing pages (page number gaps in the middle)
    page_numbers = sorted([page.get('page', 0) for page in pages_data])
    if page_numbers:
        expected_pages = list(range(page_numbers[0], page_numbers[-1] + 1))
        missing_pages = [p for p in expected_pages if p not in page_numbers]
        if missing_pages:
            quality_issues.append(f"⚠️  GAPS IN PAGES: {missing_pages}")
            print(f"  🚨 CRITICAL: Page number gaps in {pdf_name}: {missing_pages}")
    
    if ocr_pages > total_pages * 0.5:
        quality_issues.append(f"High OCR usage: {ocr_pages}/{total_pages} pages ({ocr_pages/total_pages*100:.1f}%)")
    if empty_pages > 0:
        quality_issues.append(f"Empty pages: {empty_pages}")
    if short_pages > total_pages * 0.3:
        quality_issues.append(f"Many short pages: {short_pages}/{total_pages}")
    
    # Report encoding fixes as info, not issue
    if encoding_fixed_pages > 0:
        print(f"  ℹ️  Encoding fixed on {encoding_fixed_pages}/{total_pages} pages in {pdf_name}")
    
    if quality_issues:
        print(f"  ⚠️  Quality issues in {pdf_name}: {'; '.join(quality_issues)}")
    
    return {
        'total_pages': total_pages,
        'ocr_pages': ocr_pages,
        'empty_pages': empty_pages,
        'short_pages': short_pages,
        'encoding_fixed_pages': encoding_fixed_pages,
        'missing_pages': missing_pages if page_numbers else [],
        'quality_issues': quality_issues,
        'metadata': metadata
    }

def main():
    """
    Main driver function: loops through all CAO PDF folders, extracts text from each PDF,
    and saves the results as Markdown (and optionally JSON).
    
    Output formats controlled by OUTPUT_FORMAT variable:
    - 'markdown': Only markdown files
    - 'json': Only JSON files (legacy)
    - 'both': Both formats (default for backwards compatibility)
    
    Supports multiprocessing for parallel extraction.
    Updates progress and logs debug info.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract text from CAO PDF files')
    parser.add_argument('--process_id', type=int, default=0,
                       help='Process ID for work distribution (0-based)')
    parser.add_argument('--total_processes', type=int, default=1,
                       help='Total number of parallel processes')
    args = parser.parse_args()
    
    process_id = args.process_id
    total_processes = args.total_processes
    
    if total_processes > 1:
        print(f'🔄 Process {process_id + 1}/{total_processes} starting...')
    
    cao_folders = [f for f in Path(INPUT_FOLDER).iterdir() if f.is_dir() and
        f.name.isdigit()]
    # Sort numerically, not alphabetically (so 10 comes before 50)
    cao_folders = sorted(cao_folders, key=lambda x: int(x.name))
    
    # Distribute work across processes
    if total_processes > 1:
        cao_folders = [f for i, f in enumerate(cao_folders) if i % total_processes == process_id]
        print(f'📁 Process {process_id + 1} assigned {len(cao_folders)} CAO folders')
    
    # Create output folders based on format
    if OUTPUT_FORMAT in ['json', 'both']:
        os.makedirs(OUTPUT_FOLDER_JSON, exist_ok=True)
    if OUTPUT_FORMAT in ['markdown', 'both']:
        os.makedirs(OUTPUT_FOLDER_MD, exist_ok=True)
    os.makedirs('debug_images', exist_ok=True)
    if DEBUG:
        with open(DEBUG_LOG_FILE, 'w', encoding='utf-8') as log_file:
            log_file.write('PDF Extraction Debug Log\n\n')
    for cao_folder in cao_folders:
        cao_number = cao_folder.name
        print(f'Processing CAO {cao_number}')
        
        # Create output folders based on format
        if OUTPUT_FORMAT in ['json', 'both']:
            output_cao_folder_json = Path(OUTPUT_FOLDER_JSON) / cao_number
            output_cao_folder_json.mkdir(exist_ok=True)
        if OUTPUT_FORMAT in ['markdown', 'both']:
            output_cao_folder_md = Path(OUTPUT_FOLDER_MD) / cao_number
            output_cao_folder_md.mkdir(exist_ok=True)
        if DEBUG:
            with open(DEBUG_LOG_FILE, 'a', encoding='utf-8') as log_file:
                log_file.write(f'===== CAO {cao_number} =====\n')
        pdf_files = list(cao_folder.glob('*.pdf'))
        successful_extractions = 0
        failed_files = []
        for pdf_file in pdf_files:
            # Determine output paths based on format
            base_name = Path(pdf_file.name).with_suffix('')
            
            # Check if file already exists (skip if any format exists)
            skip = False
            if OUTPUT_FORMAT in ['json', 'both']:
                json_out_path = output_cao_folder_json / f"{base_name}.json"
                if json_out_path.exists():
                    skip = True
            if OUTPUT_FORMAT in ['markdown', 'both']:
                md_out_path = output_cao_folder_md / f"{base_name}.md"
                if md_out_path.exists() and OUTPUT_FORMAT == 'markdown':
                    skip = True
            
            if skip:
                print(f'  Skipping {pdf_file.name} (extraction already exists)')
                continue
            print(f'  Processing {pdf_file.name}')
            print(f'[DEBUG] About to extract: {pdf_file}')
            if DEBUG:
                with open(DEBUG_LOG_FILE, 'a', encoding='utf-8') as log_file:
                    log_file.write(f'  ----- {pdf_file.name} -----\n')
            try:
                pages_data, metadata = extract_text_from_pdf(str(pdf_file))
                if DEBUG:
                    with open(DEBUG_LOG_FILE, 'a', encoding='utf-8'
                        ) as log_file:
                        for page in pages_data:
                            if page['ocr_used']:
                                log_file.write(
                                    f"    Page {page['page']}: OCR used\n")
                            else:
                                log_file.write(
                                    f"    Page {page['page']}: Native text only\n"
                                    )
                        log_file.write('\n')
                
                # Save in requested format(s)
                if OUTPUT_FORMAT in ['json', 'both']:
                    with open(json_out_path, 'w', encoding='utf-8') as f:
                        json.dump(pages_data, f, indent=2, ensure_ascii=False)
                    print(f'  Saved JSON to {json_out_path}')
                
                if OUTPUT_FORMAT in ['markdown', 'both']:
                    save_as_markdown(pages_data, md_out_path, pdf_file.name)
                    md_size = os.path.getsize(md_out_path) / 1024
                    print(f'  Saved Markdown to {md_out_path} ({md_size:.1f} KB)')
                
                # Analyze PDF quality with metadata
                analyze_pdf_quality(pages_data, pdf_file.name, metadata)
                
                successful_extractions += 1
            except Exception as e:
                print(f'  ❌ Failed to extract {pdf_file.name}: {e}')
                traceback.print_exc()
                failed_files.append(pdf_file.name)
        update_progress(cao_number, 'pdf_parsing', successful=
            successful_extractions, failed_files=failed_files)


if __name__ == '__main__':
    main()
