import os
import json
from pathlib import Path
from PyPDF2 import PdfReader  # For reading text-based PDFs
from pdf2image import convert_from_path  # For converting PDF pages to images (used for OCR fallback or supplement)
import pytesseract  # For image-based text extraction
import pdfplumber  # For detecting image-based regions within PDFs
from tracker import update_progress

# =========================
# Configuration and Logging
# =========================

# Debug log file path
DEBUG_LOG_FILE = "extraction_debug.log"
DEBUG = False  # Set to False to disable debug logging

# Suppress pdfminer warnings (used by pdfplumber) to avoid cluttering ouput through color warnings
import logging
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Define input and output folder paths
INPUT_FOLDER = "input_pdfs"
OUTPUT_FOLDER = "output_json"

# =========================
# PDF Text Extraction Logic
# =========================

def extract_text_from_pdf(pdf_path):
    """
    Extract text from a PDF file using native text extraction and OCR fallback for image-based pages.
    For each page, use PyPDF2 for text, pdfplumber to detect images, and pytesseract for OCR if needed.
    Args:
        pdf_path (str): Path to the PDF file.
    Returns:
        list: List of dicts with page number, text, and OCR usage info for each page.
    """
    # Load PDFs and intialize page date list.
    reader = PdfReader(pdf_path)
    pages = []

    for i, page in enumerate(reader.pages):
        # print(f"--- Processing page {i + 1} ---")
        page_info = {
            "page": i + 1,
            "ocr_used": False,
            "text": ""
        }

        # Step 1: Native text extraction
        normal_text = page.extract_text() or ""
        normal_text = normal_text.strip()

        # Step 2: Check for image-based regions using pdfplumber
        with pdfplumber.open(pdf_path) as plumber_pdf:
            plumber_page = plumber_pdf.pages[i]
            im_objs = plumber_page.images
            has_images = len(im_objs) > 0

        # Step 3: Only run OCR if image regions are found
        ocr_text = ""
        if has_images:
            images = convert_from_path(pdf_path, first_page=i + 1, last_page=i + 1)
            images[0].save(f"debug_images/{Path(pdf_path).stem}_page_{i + 1}.png")
            ocr_text = pytesseract.image_to_string(images[0]).strip()

        # Step 4: Prefer native text; fallback to OCR if native is empty
        if ocr_text == "":
            page_info["text"] = normal_text
            page_info["ocr_used"] = False
        elif ocr_text:
            page_info["text"] = ocr_text
            page_info["ocr_used"] = True
        else:
            page_info["text"] = "[EMPTY PAGE]"
            page_info["ocr_used"] = has_images

        pages.append(page_info)

    # Return combined results for all pages
    return pages

# =========================
# Main Extraction Driver
# =========================

def main():
    """
    Main driver function: loops through all CAO PDF folders, extracts text from each PDF,
    and saves the results as JSON. Updates progress and logs debug info.
    """
    # Get all CAO number folders in input folder
    cao_folders = [f for f in Path(INPUT_FOLDER).iterdir() if f.is_dir() and f.name.isdigit()]
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs("debug_images", exist_ok=True)

    # Only create log file if DEBUG is True
    if DEBUG:
        # =========================
        # Create debug log file if debugging is enabled
        # =========================
        with open(DEBUG_LOG_FILE, "w", encoding="utf-8") as log_file:
            log_file.write("PDF Extraction Debug Log\n\n")

    # Loop over each CAO folder
    for cao_folder in cao_folders:
        cao_number = cao_folder.name
        print(f"Processing CAO {cao_number}")
        
        # Create corresponding output folder
        output_cao_folder = Path(OUTPUT_FOLDER) / cao_number
        output_cao_folder.mkdir(exist_ok=True)
        
        # =========================
        # Log CAO section if debugging is enabled
        # =========================
        if DEBUG:
            with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as log_file:
                log_file.write(f"===== CAO {cao_number} =====\n")

        # Get all PDF files in this CAO folder
        pdf_files = list(cao_folder.glob("*.pdf"))
        
        # Track successful and failed extractions
        successful_extractions = 0
        failed_files = []
        
        # Loop over each PDF file in the CAO folder
        for pdf_file in pdf_files:
            json_out_path = output_cao_folder / (Path(pdf_file.name).with_suffix('.json').name)
            # =========================
            # Skip PDFs if extraction already exists
            # =========================
            if json_out_path.exists():
                print(f"  Skipping {pdf_file.name} (extraction already exists)")
                continue
            print(f"  Processing {pdf_file.name}")
            # =========================
            # Log PDF section if debugging is enabled
            # =========================
            if DEBUG:
                with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as log_file:
                    log_file.write(f"  ----- {pdf_file.name} -----\n")

            try:
                # Extract page-wise text and OCR info
                pages_data = extract_text_from_pdf(str(pdf_file))

                # Debug: log OCR usage per page if DEBUG is set
                if DEBUG:
                    with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as log_file:
                        for page in pages_data:
                            if page["ocr_used"]:
                                log_file.write(f"    Page {page['page']}: OCR used\n")
                            else:
                                log_file.write(f"    Page {page['page']}: Native text only\n")
                        log_file.write("\n")

                # Save results to JSON in the corresponding CAO folder
                with open(json_out_path, "w", encoding="utf-8") as f:
                    json.dump(pages_data, f, indent=2, ensure_ascii=False)

                print(f"  Saved to {json_out_path}")
                successful_extractions += 1
                
            except Exception as e:
                print(f"  ❌ Failed to extract {pdf_file.name}: {e}")
                failed_files.append(pdf_file.name)
        
        # Update tracker for this CAO
        update_progress(cao_number, "pdf_parsing", successful=successful_extractions, failed_files=failed_files)

# =========================
# Script Entry Point
# =========================

# Run the extraction if script is executed directly
if __name__ == "__main__":
    main()
