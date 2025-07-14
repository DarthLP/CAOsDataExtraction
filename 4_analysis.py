import os
import json
import pandas as pd
import time
from pathlib import Path
from deep_translator import GoogleTranslator
import google.generativeai as genai
from dotenv import load_dotenv
from tracker import update_progress

# =========================
# Configuration and Setup
# =========================

# Paths
INPUT_JSON_FOLDER = "llmExtracted_json"
FIELDS_PROMPT_PATH = "fields_prompt.md"
OUTPUT_EXCEL_PATH = "results/extracted_data.xlsx"
CAO_INFO_PATH = "input_pdfs/extracted_cao_info.csv"
DEBUG_MODE = False
MAX_JSON_FILES = 40  # Limit how many JSON files to process

# =========================
# Gemini API Setup
# =========================

load_dotenv()
# type: ignore[attr-defined]
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
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
        f"You are an AI assistant that extracts structured JSON data from Dutch collective labor agreements (CAOs). These CAOs were originally provided as PDF files, and are now given to you as structured JSON files derived from them.\n\n"
        
        f"=== Source Text ===\n"
        f"The input is a shortened and grouped JSON-like structure. Each section is titled according to its content (e.g., \"Wage information\", \"Pension information\"), and contains a list of paragraphs or table contents from the CAO PDF relevant to that topic.\n"
        f"From file: {filename}\n\n"
        f"{text}\n\n"

        f"=== Context ===\n"
        f"The following table is a structured list of fields to extract. The first row contains the field names. The rows below describe each field using the following format:\n"
        f"Description (expected format). Help or further guidance. Ex: one or more examples\n"
        f"{prompt_fields_markdown}\n\n"

        f"=== General Instructions ===\n"
        f"- Extract the requested information as **strictly valid JSON**.\n"
        f"- If a field or cell is empty, blank or marked \"empty\" in the structure, **leave it empty** in your output.\n"
        f"- Do NOT hallucinate or infer information not explicitly mentioned in the text.\n"
        f"- Never guess or fabricate values. Do not fill in missing data unless it is directly found in the source text.\n"
        f"- Rows marked with \"...\" indicate a repeating pattern. Use the 3 rows above to understand the pattern and complete it for all job groups present.\n"
        f"- Do not return explanations, comments, or Markdown — return only pure JSON.\n\n"

        f"= Wage Instructions =\n"
        f"- When multiple wage tables are present, focus only on tables that represent standard or regular wages (sometimes referred to as \"basic\" or \"normal\" even if not labeled explicitly).\n"
        f"- If multiple tables exist for different job groups or levels under this standard wage type, include all of them.\n"
        f"- Prefer hourly units when both hourly and monthly wage tables are available.\n"
        f"- Only extract salary-related data for workers aged 21 and older.\n\n"

        f"= Pension Instructions =\n"
        f"- For all pension-related fields, help the model by searching for Dutch keywords like “AOW”, “pensioen”, and “regeling”.\n\n"

        f"= Leave, Termination, Overtime, and Training Instructions =\n"
        f"- For all fields related to leave, contract termination, working hours, overtime, or training, extract as much relevant information as possible - more is better, as long as it is factually present in the text.\n"

        f"=== Output Format ===\n"
        f"Return ONLY valid JSON, for example:\n"
        f"{{\"field1\": \"value1\", \"field2\": \"value2\", ...}}\n"
        f"⚠️ Reminder: Only output factual information stated in the source text. No assumptions, no guesses.\n"
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

# =========================
# Main Analysis Loop
# =========================

# Track CAO numbers for analysis
cao_analysis_tracking = {}

# Counter for number of successfully analyzed files
successful_analyses = 0  # Increments for each file successfully analyzed and appended to results
failed_files = []

# Check if output file already exists to load existing progress
if os.path.exists(OUTPUT_EXCEL_PATH):
    print(f"Loading existing progress from {OUTPUT_EXCEL_PATH}")
    df_results = pd.read_excel(OUTPUT_EXCEL_PATH)
    # Count already processed files (unique CAOs, not rows)
    if 'CAO' in df_results.columns:
        unique_caos = df_results['CAO'].nunique()
        processed_files = unique_caos
        print(f"Found {processed_files} already processed CAOs (unique files)")
    else:
        processed_files = 0
        print(f"Found existing file but no CAO column, starting fresh")
else:
    print(f"Starting fresh analysis")
    processed_files = 0

# Get all JSON files to process
json_files = list(Path(INPUT_JSON_FOLDER).rglob("*.json"))

for json_file in json_files:
    # Check if we've reached the limit
    if processed_files >= MAX_JSON_FILES:
        print(f"Reached limit of {MAX_JSON_FILES} files. Stopping processing.")
        break
    
    # === Robust CAO number and ID lookup ===
    cao_number = None
    cao_id = None
    pdf_name_cleaned = json_file.stem + ".pdf"
    # Try direct match (cleaned/encoded)
    if pdf_name_cleaned in cao_info_mapping:
        cao_number = cao_info_mapping[pdf_name_cleaned]['cao_number']
        cao_id = cao_info_mapping[pdf_name_cleaned]['id']
    else:
        # Try fuzzy match: compare ignoring spaces, dashes, and case
        def normalize(s):
            return s.replace(" ", "").replace("-", "").replace("_", "").lower()
        normalized_cleaned = normalize(pdf_name_cleaned)
        found = False
        for original_pdf_name in cao_info_mapping.keys():
            if normalize(original_pdf_name) == normalized_cleaned:
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
            print(f"[DEBUG] Could not find CAO id for {json_file.name} (tried '{pdf_name_cleaned}' and fuzzy match)")

    # Check if this PDF (by id) was already processed
    if cao_id and 'id' in df_results.columns and cao_id in df_results['id'].astype(str).values:
        print(f"  Skipping {json_file.name} (id {cao_id} already processed)")
        continue
        
    # Increment processed_files for each new file being processed
    processed_files += 1
    print(f"Processing {json_file.name} ({processed_files}/{MAX_JSON_FILES})")
    
    with open(json_file, "r", encoding="utf-8") as f:
        context_by_infotype = json.load(f)

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

    try:
        extracted = extract_fields_from_text(full_text, prompt_fields_markdown, filename=json_file.name)
        if not extracted:
            print("Gemini failed to extract data. Skipping file.")
            failed_files.append(json_file.name)
            continue
    except Exception as e:
        print(f"  Error processing {json_file.name}: {e}")
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
    
    # Track CAO number for this file
    cao_number = None
    if json_file.stem + ".pdf" in cao_info_mapping:
        cao_number = cao_info_mapping[json_file.stem + ".pdf"]['cao_number']
    else:
        # Try to extract CAO number from folder structure
        try:
            cao_number = int(json_file.parent.name)
        except (ValueError, AttributeError):
            pass
    
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
    print(f"  Progress saved to {OUTPUT_EXCEL_PATH}")

    # Add 2-minute delay after processing each file to prevent rate limiting
    print(f"  Completed processing {json_file.name}. Waiting 120 seconds before next file...")
    time.sleep(120)

# === Save final results to Excel ===
os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)

# Print confirmation of completion
print(f"Analysis complete. Saved to {OUTPUT_EXCEL_PATH}")
print(f"📊 Analysis Summary: {successful_analyses} successful, {len(failed_files)} failed")
if failed_files:
    print(f"   Failed files: {', '.join(failed_files)}")