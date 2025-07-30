import os
import json
import pandas as pd
import time
from pathlib import Path
from deep_translator import GoogleTranslator
import google.generativeai as genai
from dotenv import load_dotenv
from tracker import update_progress
import re
import sys
import fcntl

# =========================
# Configuration and Setup
# =========================

def acquire_file_lock(file_path):
    """Try to acquire a lock for processing a file. Returns True if lock acquired, False if already locked."""
    lock_file = file_path.with_suffix('.analysis_lock')
    try:
        # Try to create lock file
        with open(lock_file, 'w') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(f"Process {process_id + 1} using API key {key_number}\n")
            f.write(f"Timestamp: {time.time()}\n")
        return True
    except (IOError, OSError):
        # File is already locked by another process
        return False

def release_file_lock(file_path):
    """Release the lock for a file."""
    lock_file = file_path.with_suffix('.analysis_lock')
    try:
        if lock_file.exists():
            lock_file.unlink()
    except:
        pass  # Ignore errors when releasing lock

def announce_cao_once(cao_number):
    """Announce a CAO number only once across all processes using a simple file lock."""
    announce_file = Path("results") / f".cao_{cao_number}_analysis_announced"
    try:
        # Try to create the announce file atomically
        with open(announce_file, 'x') as f:
            f.write(f"Announced by process {process_id + 1}\n")
        print(f"--- CAO {cao_number} ---")
        return True
    except FileExistsError:
        # Another process already announced this CAO (or from a previous run)
        return False

# Paths
INPUT_JSON_FOLDER = "llmExtracted_json"
FIELDS_PROMPT_PATH = "fields_prompt.md"
CAO_INFO_PATH = "input_pdfs/extracted_cao_info.csv"
DEBUG_MODE = False
MAX_JSON_FILES = 2  # Limit how many JSON files to process

# Get key number from command line or default to 1
key_number = int(sys.argv[1]) if len(sys.argv) > 1 else 1
# Get process ID and total processes for work distribution
process_id = int(sys.argv[2]) if len(sys.argv) > 2 else 0
total_processes = int(sys.argv[3]) if len(sys.argv) > 3 else 1

# Process-specific output path (defined after process_id is available)
OUTPUT_EXCEL_PATH = f"results/extracted_data_process_{process_id + 1}.xlsx"

# =========================
# Gemini API Setup
# =========================

load_dotenv()
api_key = os.getenv(f"GOOGLE_API_KEY{key_number}")
if not api_key:
    raise ValueError(f"GOOGLE_API_KEY{key_number} environment variable not found. Please set it before running this script.")
genai.configure(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-pro" 

# =========================
# Load Prompt Fields and CAO Info
# =========================

with open(FIELDS_PROMPT_PATH, "r", encoding="utf-8") as f:
    prompt_fields_markdown = f.read()
columns = [col.strip() for col in prompt_fields_markdown.splitlines()[0].strip("|").split("|")]

# === Load CAO info from CSV ===
def load_cao_info():
    """
    Load CAO information from CSV and create a mapping dictionary.
    Returns:
        dict: Mapping from pdf_name to CAO metadata.
    """
    cao_info_df = pd.read_csv(CAO_INFO_PATH, sep=';')
    
    # Create mapping dictionary: pdf_name -> {cao_number, id, ingangsdatum, expiratiedatum, datum_kennisgeving}
    cao_mapping = {}
    for _, row in cao_info_df.iterrows():
        pdf_name = row['pdf_name']
        cao_mapping[pdf_name] = {
            'cao_number': row['cao_number'],
            'id': row['id'],
            'ingangsdatum': row['ingangsdatum'],
            'expiratiedatum': row['expiratiedatum'],
            'datum_kennisgeving': row['datum_kennisgeving']
        }
    
    return cao_mapping

# =========================
# DataFrame Initialization
# =========================

# Load CAO info mapping
cao_info_mapping = load_cao_info()
# Initialize the result DataFrame with accurate column names
# type: ignore[arg-type]
df_results = pd.DataFrame(columns=columns)

# =========================
# Gemini Query and Output Cleaning
# =========================

def query_gemini(prompt, model=GEMINI_MODEL, max_retries=5):
    """
    Query Gemini model with improved exponential backoff retry logic for 504 errors.
    Args:
        prompt (str): The prompt to send to Gemini.
        model (str): The Gemini model name.
        max_retries (int): Maximum number of retry attempts.
    Returns:
        str: The raw Gemini output.
    """
    for attempt in range(max_retries):
        try:
            # type: ignore[attr-defined]
            model_obj = genai.GenerativeModel(model)
            response = model_obj.generate_content(prompt)
            if hasattr(response, "text") and response.text.strip():
                return response.text
            raise ValueError("Empty or invalid model response")
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle 504 Deadline Exceeded errors with reasonable retry
            if "deadlineexceeded" in error_str or "504" in error_str:
                if attempt < max_retries - 1:
                    # Reasonable backoff: 2 minutes, 4 minutes, 8 minutes
                    wait_time = 120 * (2 ** attempt)  # 120s, 240s, 480s
                    print(f"  Attempt {attempt + 1} failed (504 timeout), retrying in {wait_time//60} minutes...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with 504 errors - skipping file")
                    return ""  # Return empty string to skip this file
            
            # Handle other rate limiting errors
            elif any(keyword in error_str for keyword in ["quota", "rate limit", "too many requests", "429"]):
                if attempt < max_retries - 1:
                    wait_time = 300 * (2 ** attempt)  # 5 minutes, 10 minutes, 20 minutes
                    print(f"  Attempt {attempt + 1} failed (rate limit), retrying in {wait_time//60} minutes...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with rate limiting - skipping file")
                    return ""  # Return empty string to skip this file
            
            # Handle other errors with standard retry
            elif attempt < max_retries - 1:
                wait_time = 60 * (2 ** attempt)  # 60s, 120s, 240s, 480s
                print(f"  Attempt {attempt + 1} failed ({type(e).__name__}), retrying in {wait_time//60} minutes...")
                time.sleep(wait_time)
                continue
            else:
                # Final attempt failed
                raise e
    
    raise ValueError(f"All {max_retries} retry attempts failed")

def clean_gemini_output(output):
    """
    Clean the Gemini model output by removing markdown and trailing commas.
    Args:
        output (str): The raw output from Gemini.
    Returns:
        str: Cleaned output string.
    """
    if output.strip().startswith("```"):
        lines = output.strip().splitlines()
        content = "\n".join(line for line in lines if not line.strip().startswith("```"))
    else:
        content = output.strip()

    # Remove trailing commas before closing brackets/braces
    import re
    content = re.sub(r',\s*(?=[}\]])', '', content)
    return content

# =========================
# Extraction and Flattening Logic
# =========================

def extract_fields_from_text(text, prompt_fields_markdown, filename=""):
    """
    Generate a prompt with the list of desired fields and extract structured data from text.
    Args:
        text (str): The full CAO text to extract from.
        prompt_fields_markdown (str): Markdown table of fields.
        filename (str): The filename for context.
    Returns:
        dict: Extracted fields as a dictionary.
    """
    prompt = (
        "You are an AI assistant that extracts structured JSON data from Dutch collective labor agreements (CAOs). "
        "These CAOs were originally provided as PDF files, and are now given to you as structured JSON files derived from them.\n\n"
        
        "=== Source Text ===\n"
        "The input is a shortened and grouped JSON-like structure. Each section is titled according to its content (e.g., \"Wage information\", \"Pension information\"), and contains a list of paragraphs or table contents from the CAO PDF relevant to that topic.\n"
        f"From file: {filename}\n\n"
        f"{text}\n\n"

        "=== Extraction Fields ===\n"
        "Below is a table of fields to extract. The first row contains the field names. The rows below describe each field. They have the following format: Description (expected format). Help or further guidance. Ex: one or more examples\n"
        f"{prompt_fields_markdown}\n\n"

        "=== Extraction Instructions ===\n"
        "- For each field in the table, extract the data from the source text.\n"
        "- Do NOT hallucinate, infer, or guess any information. Only extract what is explicitly present in the text.\n"
        "- Do NOT fill in missing data unless it is directly found in the source text.\n"
        "- Do NOT repeat the prompt, instructions, or any explanations in your output.\n"
        "- Do NOT use markdown, code blocks, or comments. Output only pure JSON.\n"
        "- If a field or cell is empty, blank, or marked \"empty\" in the structure, leave it empty in your output.\n"
        "- Rows marked with \"...\" indicate a repeating pattern. Use the 3 rows above to understand the pattern and complete it for all job groups present.\n"
        "- Output all field names, even if the value is empty.\n\n"

        "=== Special Domain Instructions ===\n"
        "- Wage: When multiple wage tables are present, focus only on tables that represent standard or regular wages (sometimes referred to as \"basic\" or \"normal\" even if not labeled explicitly). If multiple tables exist for different job groups or levels under this standard wage type, include all of them. Prefer hourly units when both hourly and monthly wage tables are available. Only extract salary-related data for workers aged 21 and older.\n"
        "- Pension: For all pension-related fields, help the model by searching for Dutch keywords like “AOW”, “pensioen”, and “regeling”.\n"
        "- Leave, Termination, Overtime, Training and Homeoffice: For all fields related to leave, contract termination, working hours, overtime, training or homeoffice, extract as much relevant information as possible - more is better, as long as it is factually present in the text.\n\n"

        "=== Output Format ===\n"
        "Return ONLY valid JSON, for example:\n"
        "{\"field1\": \"value1\", \"field2\": \"value2\", ...}\n"
        "Do NOT wrap the JSON in code blocks or markdown. Do NOT include any explanations or comments.\n"
        "Reminder: Only output factual information stated in the source text. No assumptions, no guesses. If unsure, leave the field empty.\n"
    )
    raw_output = query_gemini(prompt)
    cleaned_output = clean_gemini_output(raw_output)
    try:
        return json.loads(cleaned_output)
    except Exception as e:
        if DEBUG_MODE:
            print(f"Failed to parse JSON from model output: {e}\nRaw output:\n{cleaned_output}")
        return {}

# === Add flatten_to_str_list function ===
def flatten_to_str_list(lst):
    """
    Recursively flatten a nested list into a list of strings, joining sublists with ' | '.
    Args:
        lst (list): The list to flatten.
    Returns:
        list: Flattened list of strings.
    """
    result = []
    for item in lst:
        if isinstance(item, list):
            # Recursively flatten and join nested lists
            result.append(" | ".join(str(subitem) for subitem in flatten_to_str_list(item)))
        else:
            result.append(str(item))
    return result

# Robust duplicate check: normalize filename for deduplication

def normalize_filename(name):
    """
    Normalize filename for robust duplicate detection: lowercase, remove extension, and strip non-alphanumeric characters.
    """
    name = name.lower()
    name = re.sub(r'\.[^.]+$', '', name)  # Remove extension
    name = re.sub(r'[^a-z0-9]', '', name)  # Remove non-alphanumeric
    return name

# =========================
# Main Analysis Loop
# =========================

# Track CAO numbers for analysis
cao_analysis_tracking = {}

# Get all CAO number folders in input folder
cao_folders = sorted(
    [f for f in Path(INPUT_JSON_FOLDER).iterdir() if f.is_dir() and f.name.isdigit()],
    key=lambda f: int(f.name)
)

# Collect all JSON files from all CAO folders with their paths, ordered by CAO number then filename
all_json_files = []
for cao_folder in cao_folders:
    cao_number = cao_folder.name
    json_files = sorted(cao_folder.glob("*.json"))
    for json_file in json_files:
        all_json_files.append((cao_folder, json_file))

# Process files using atomic file-level distribution
current_cao = None
processed_files = 0
successful_analyses = 0
failed_files = []

for file_idx, (cao_folder, json_file) in enumerate(all_json_files):
    # Only process files that belong to this process
    if file_idx % total_processes != process_id:
        continue
        
    # Check if we've reached the limit
    if processed_files >= MAX_JSON_FILES:
        break
        
    cao_number = cao_folder.name
    
    # Print CAO number only when it changes and hasn't been announced yet
    if current_cao != cao_number:
        announce_cao_once(cao_number)
        current_cao = cao_number
    
    # Try to acquire lock for this file to prevent double processing
    if not acquire_file_lock(json_file):
        print(f"  Skipping {json_file.name} (being processed by another process)")
        time.sleep(2)
        continue
    
    try:
        # === Get CAO number and ID lookup first (needed for duplicate checking) ===
        cao_id = None
        pdf_name_cleaned = json_file.stem + ".pdf"
        # Try direct match (cleaned/encoded)
        if pdf_name_cleaned in cao_info_mapping:
            cao_number = cao_info_mapping[pdf_name_cleaned]['cao_number']
            cao_id = cao_info_mapping[pdf_name_cleaned]['id']
        else:
            # Try fuzzy match: compare ignoring spaces, dashes, and case
            def normalize_lookup(s):
                return s.replace(" ", "").replace("-", "").replace("_", "").lower()
            normalized_cleaned = normalize_lookup(pdf_name_cleaned)
            found = False
            for original_pdf_name in cao_info_mapping.keys():
                if normalize_lookup(original_pdf_name) == normalized_cleaned:
                    cao_number = cao_info_mapping[original_pdf_name]['cao_number']
                    cao_id = cao_info_mapping[original_pdf_name]['id']
                    found = True
                    break
            if not found:
                # Fall back to folder-based CAO number extraction
                try:
                    cao_number = int(json_file.parent.name)
                except (ValueError, AttributeError):
                    pass
            if not cao_id:
                if DEBUG_MODE:
                    print(f"[DEBUG] Could not find CAO id for {json_file.name} (tried '{pdf_name_cleaned}' and fuzzy match)")
        
        # Check if this file was already processed by checking the final Excel file
        final_excel_path = "results/extracted_data.xlsx"
        already_processed = False
        
        if os.path.exists(final_excel_path):
            try:
                # Add small delay to avoid race conditions when reading Excel
                time.sleep(0.1)
                existing_df = pd.read_excel(final_excel_path)
                if 'File_name' in existing_df.columns:
                    # Check if this specific file is already in the Excel
                    if json_file.name in existing_df['File_name'].values:
                        already_processed = True
                        print(f"  Skipping {json_file.name} (already in final Excel file)")
                        continue
            except Exception as e:
                # If we can't read the Excel file, continue processing
                if DEBUG_MODE:
                    print(f"  Could not check existing Excel file: {e}")
        
        if already_processed:
            continue

        with open(json_file, "r", encoding="utf-8") as f:
            context_by_infotype = json.load(f)

        print(f"  {json_file.name}")
        processed_files += 1

        full_text_parts = []
        for key, value in context_by_infotype.items():
            if isinstance(value, list):
                flat_value = flatten_to_str_list(value)
                full_text_parts.append(f"== {key} ==\n" + "\n".join(flat_value))
            elif isinstance(value, str):
                full_text_parts.append(f"== {key} ==\n{value}")
        full_text = "\n\n".join(full_text_parts)
        # full_text = GoogleTranslator(source='nl', target='en').translate(full_text)
        combined_fields = dict.fromkeys(columns, "")

        extracted = extract_fields_from_text(full_text, prompt_fields_markdown, filename=json_file.name)
        if not extracted:
            print(f"  ✗ Failed to extract data from {json_file.name}")
            failed_files.append(json_file.name)
            continue

        # Handle extracted as dict or list of dicts
        if isinstance(extracted, dict):
            extracted_items = [extracted]
        elif isinstance(extracted, list):
            extracted_items = extracted
        else:
            extracted_items = []

        for item in extracted_items:
            row = dict.fromkeys(columns, "")
            for key, value in item.items():
                if key in row:
                    row[key] = value

            # Translate extracted field values from Dutch to English
            for key, value in row.items():
                if isinstance(value, str) and value.strip():
                    try:
                        translated = GoogleTranslator(source='nl', target='en').translate(value)
                        row[key] = translated
                    except Exception as e:
                        if DEBUG_MODE:
                            print(f"Translation error for key '{key}': {e}")

            row["CAO"] = str(cao_number) if cao_number else json_file.stem
            row["id"] = str(cao_id) if cao_id else ""
            row["TTW"] = "yes" if "TTW" in json_file.stem.upper() else "no"
            row["File_name"] = json_file.name # Changed from file_basename to json_file.name

            # === Merge CAO info from CSV ===
            # Try to find matching CAO info by PDF name
            pdf_name = json_file.stem + ".pdf"  # Reconstruct PDF name from JSON filename
            
            if pdf_name in cao_info_mapping:
                cao_info = cao_info_mapping[pdf_name]
                # Map the fields as specified
                row["CAO"] = cao_info['cao_number']
                row["id"] = cao_info['id']
                row["start_date"] = cao_info['ingangsdatum']
                row["expiry_date"] = cao_info['expiratiedatum']
                row["date_of_formal_notification"] = cao_info['datum_kennisgeving']
            else:
                if DEBUG_MODE:
                    print(f"  No CAO info found for PDF: {pdf_name}")

            if DEBUG_MODE:
                print("Row content before appending:", row)

            row_df = pd.DataFrame([row])

            row_df_full = row_df.reindex(columns=df_results.columns)

            # Fill NaNs with a placeholder to avoid FutureWarning during concat
            row_df_full_filled = row_df_full.fillna("Empty")

            if DEBUG_MODE:
                print("About to append row:")
                print(row_df_full_filled)
                print("All NA after replace check?", row_df_full_filled.replace(["Empty", ""], pd.NA).isna().all(axis=1))

            # Final robust content check
            row_to_append_check = row_df_full_filled.replace(["Empty", "", None], pd.NA)
            nonmeta_cols = [col for col in row_to_append_check.columns if col not in ("CAO", "TTW", "File_name", "id")]
            result = row_to_append_check[nonmeta_cols].isna().all(axis=1)
            if isinstance(result, bool):
                is_all_na = result
            else:
                is_all_na = result.iloc[0]
            if is_all_na:
                print("Skipped appending due to only Empty values.")
                continue
            if DEBUG_MODE:
                print("Appending row after check passed:")
                print(row_df_full_filled)
                print("ROW BEFORE CONCAT:")
                print(row_df_full_filled)
                print("ROW TYPES:")
                print(row_df_full_filled.dtypes)
            df_results = pd.concat([df_results, row_df_full_filled], ignore_index=True)
            df_results.replace("Empty", pd.NA, inplace=True)

        # Track successful analysis
        successful_analyses += 1
        print(f"  ✓ {json_file.name}")
        
        # Track CAO number for this file
        if cao_number:
            if cao_number not in cao_analysis_tracking:
                cao_analysis_tracking[cao_number] = {'successful': 0, 'failed': 0}
            cao_analysis_tracking[cao_number]['successful'] += 1
            
            # Update tracker for this CAO immediately
            update_progress(cao_number, "llm_analysis", 
                           successful=cao_analysis_tracking[cao_number]['successful'], 
                           failed_files=cao_analysis_tracking[cao_number].get('failed_files', []))

        # Save progress after each file to prevent data loss
        os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
        df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)

        # Check if we've reached the limit before waiting
        if processed_files >= MAX_JSON_FILES:
            break
        else:
            # Add delay after successful request to prevent rate limiting
            time.sleep(120)

    except Exception as e:
        print(f"  ✗ Error processing {json_file.name}: {e}")
        failed_files.append(json_file.name)
        # Release lock on error too
        release_file_lock(json_file)
    finally:
        # Always release the lock
        release_file_lock(json_file)

# === Save final results to Excel ===
os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)

# Print completion message
if failed_files:
    print(f"Process {process_id + 1} completed: {successful_analyses} successful, {len(failed_files)} failed")
else:
    print(f"Process {process_id + 1} completed: {successful_analyses} successful")