import os
import json
from pathlib import Path
from PyPDF2 import PdfReader  # For reading text-based PDFs
from pdf2image import convert_from_path  # For converting PDF pages to images (used for OCR fallback or supplement)
import pytesseract  # For image-based text extraction
import pdfplumber  # For detecting image-based regions within PDFs

# Debug log file path
DEBUG_LOG_FILE = "extraction_debug.log"
DEBUG = True  # Set to False to disable debug logging

# Suppress pdfminer warnings (used by pdfplumber) to avoid cluttering ouput through color warnings
import logging
logging.getLogger("pdfminer").setLevel(logging.ERROR)

# Define input and output folder paths
INPUT_FOLDER = "input_pdfs"
OUTPUT_FOLDER = "output_json"

# Extract text from a PDF file using first native text parsing and if pdfplumber finds image on page, use image OCR parsing.
def extract_text_from_pdf(pdf_path):
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

# Main driver function: loops through all PDFs and saves extracted text as JSON
def main():
    # Get all PDF files in input folder
    pdf_files = list(Path(INPUT_FOLDER).glob("*.pdf"))
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs("debug_images", exist_ok=True)

    # Write log file header
    with open(DEBUG_LOG_FILE, "w", encoding="utf-8") as log_file:
        log_file.write("PDF Extraction Debug Log\n\n")

    # Loop over each file
    for pdf_file in pdf_files:
        print(f"Processing {pdf_file.name}")
        with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as log_file:
            log_file.write(f"===== {pdf_file.name} =====\n")

        # Extract page-wise text and OCR info
        pages_data = extract_text_from_pdf(str(pdf_file))

        # Debug: log OCR usage per page if DEBUG is set
        if DEBUG:
            with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as log_file:
                for page in pages_data:
                    if page["ocr_used"]:
                        log_file.write(f"Page {page['page']}: OCR used\n")
                    else:
                        log_file.write(f"Page {page['page']}: Native text only\n")
                log_file.write("\n")

        # Save results to JSON
        json_out_path = Path(OUTPUT_FOLDER) / (pdf_file.stem + ".json")
        with open(json_out_path, "w", encoding="utf-8") as f:
            json.dump(pages_data, f, indent=2, ensure_ascii=False)

        print(f"Saved to {json_out_path}")

# Run the extraction if script is executed directly
if __name__ == "__main__":
    main()
