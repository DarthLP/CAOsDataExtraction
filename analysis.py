import os
import json
import yaml
import pandas as pd
from pathlib import Path
from deep_translator import GoogleTranslator
import google.generativeai as genai
import time

# === Configuration: Set paths and constants ===
# Paths
INPUT_JSON_FOLDER = "output_json"
FIELDS_PROMPT_PATH = "fields_prompt.md"
OUTPUT_EXCEL_PATH = "results/extracted_data.xlsx"
DEBUG_MODE = False
MAX_JSON_FILES = 1  # Limit how many JSON files to process

# === Load prompt fields from fields_prompt.md ===
with open(FIELDS_PROMPT_PATH, "r", encoding="utf-8") as f:
    prompt_fields_markdown = f.read()
columns = [col.strip() for col in prompt_fields_markdown.splitlines()[0].strip("|").split("|")]

# Initialize the result DataFrame with accurate column names
df_results = pd.DataFrame(columns=columns)

# === Configure Google Generative AI (Gemini) API ===
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
GEMINI_MODEL = "gemini-2.5-pro" 

# Function to query Gemini model with retries
def query_gemini(prompt, model=GEMINI_MODEL, retries=3, delay=2):
    for attempt in range(retries):
        try:
            model_obj = genai.GenerativeModel(model)
            response = model_obj.generate_content(prompt)
            if hasattr(response, "text") and response.text.strip():
                return response.text
        except Exception as e:
            if DEBUG_MODE:
                print(f"Gemini error on attempt {attempt+1}: {e}")
            time.sleep(delay)
    return ""

def clean_gemini_output(output):
    if output.strip().startswith("```"):
        lines = output.strip().splitlines()
        content = "\n".join(line for line in lines if not line.strip().startswith("```"))
    else:
        content = output.strip()

    # Remove trailing commas before closing brackets/braces
    import re
    content = re.sub(r',\s*(?=[}\]])', '', content)
    return content

# Generate a prompt with the list of desired fields and extract structured data from text
def extract_fields_from_text(text, prompt_fields_markdown, filename=""):
    prompt = (
        f"You are an AI assistant that extracts structured JSON data from Dutch collective labor agreements (CAOs).\n\n"
        
        f"=== Context ===\n"
        f"- The source text comes from: {filename}\n"
        f"- The following table is a structured list of fields to extract. The first row contains the field names. The rows below provide a short description of each field and clarify the type of information that should be extracted.\n"
        f"{prompt_fields_markdown}\n\n"

        f"=== Instructions ===\n"
        f"- Extract the requested information as **strictly valid JSON**.\n"
        f"- If a field is empty, blank or marked \"empty\" in the structure, **leave it empty** in your output.\n"
        f"- Do NOT hallucinate or infer information not explicitly mentioned in the text.\n"
        f"- Rows marked with '...' indicate a repeating pattern. Use the 3 rows above and 1 row below to understand the pattern and complete it for all job groups present.\n"
        f"- Only extract salary-related data for workers aged 21 and older.\n"
        f"- Do not return explanations, comments, or Markdown — return only pure JSON.\n\n"

        f"=== Source Text ===\n{text}\n\n"

        f"=== Output Format ===\n"
        f"Return ONLY valid JSON, for example:\n"
        f"{{\"field1\": \"value1\", \"field2\": \"value2\", ...}}\n"
    )
    raw_output = query_gemini(prompt)
    cleaned_output = clean_gemini_output(raw_output)
    try:
        return json.loads(cleaned_output)
    except Exception as e:
        if DEBUG_MODE:
            print(f"Failed to parse JSON from model output: {e}\nRaw output:\n{cleaned_output}")
        return {}

# === Process each JSON file generated from PDF extraction ===
processed_files = 0
for json_file in Path(INPUT_JSON_FOLDER).glob("*.json"):
    with open(json_file, "r", encoding="utf-8") as f:
        if processed_files >= MAX_JSON_FILES:
            break
        processed_files += 1
        pages = json.load(f)

    full_text = "\n".join(page["text"] for page in pages)
    # full_text = GoogleTranslator(source='nl', target='en').translate(full_text)
    combined_fields = dict.fromkeys(columns, "")

    extracted = extract_fields_from_text(full_text, prompt_fields_markdown, filename=json_file.name)
    if not extracted:
        print("Gemini failed to extract data. Skipping file.")
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

        row["CAO"] = json_file.stem
        row["TTW"] = "yes" if "TTW" in json_file.stem.upper() else "no"

        if DEBUG_MODE:
            print("Row content before appending:", row)

        row_df = pd.DataFrame([row])
        # Drop columns that are entirely empty across all rows (excluding "CAO" and "TTW")
        non_empty_columns = [
            col for col in row_df.columns if col in ("CAO", "TTW") or
            any((val is not None and (not isinstance(val, str) or val.strip() != "")) for val in row_df[col])
        ]
        row_df_cleaned = row_df[non_empty_columns]

        row_df_full = row_df.reindex(columns=df_results.columns)

        # Fill NaNs with a placeholder to avoid FutureWarning during concat
        row_df_full_filled = row_df_full.fillna("Empty")

        if DEBUG_MODE:
            print("About to append row:")
            print(row_df_full_filled)
            print("All NA after replace check?", row_df_full_filled.replace(["Empty", ""], pd.NA).isna().all(axis=1).iloc[0])

        # Final robust content check
        row_to_append_check = row_df_full_filled.replace(["Empty", "", None], pd.NA)
        nonmeta_cols = [col for col in row_to_append_check.columns if col not in ("CAO", "TTW", "File_name")]
        if row_to_append_check[nonmeta_cols].isna().all(axis=1).iloc[0]:
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

# === Save final results to Excel ===
os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)

# Print confirmation of completion
print(f"Analysis complete. Saved to {OUTPUT_EXCEL_PATH}")