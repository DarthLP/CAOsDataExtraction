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
FIELDS_YAML_PATH = "fields.yaml"
OUTPUT_EXCEL_PATH = "results/extracted_data.xlsx"
DEBUG_MODE = True
MAX_JSON_FILES = 1  # Limit how many JSON files to process

# === Load field definitions from fields.yaml ===
# Load the extraction fields from fields.yaml
with open(FIELDS_YAML_PATH, "r", encoding="utf-8") as f:
    fields = yaml.safe_load(f)

# Prepare the list of column names for the output Excel
columns = [field["name"] for field in fields]
# Initialize the result DataFrame with predefined columns
df_results = pd.DataFrame(columns=columns)

# === Configure Google Generative AI (Gemini) API ===
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
GEMINI_MODEL = "gemini-2.5-pro"

# Function to query Gemini model with retries
def query_gemini(prompt, model=GEMINI_MODEL, retries=3, delay=2):
    for attempt in range(retries):
        try:
            model = genai.GenerativeModel(model)
            response = model.generate_content(prompt)
            if response.text.strip():
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
        return content.strip()
    return output.strip()

# Generate a prompt with the list of desired fields and extract structured data from text
def extract_fields_from_text(text, fields, filename=""):
    field_descriptions = []
    for field in fields:
        if "description" in field and field["description"]:
            field_descriptions.append(f"{field['name']}: {field['description']}")
        else:
            field_descriptions.append(field["name"])

    prompt = (
        f"You are an AI assistant that extracts structured JSON data from Dutch labor agreements (CAOs).\n"
        f"Text comes from file: {filename}\n\n"
        "Here are the fields to extract (skip any not found):\n" +
        "\n".join(field_descriptions) +
        "\n\n---\nTEXT:\n" + text +
        "\n\nReturn ONLY valid JSON as your answer, like this: {\"field1\": \"value\", ...}. "
        "DO NOT include any explanation or internal thoughts."
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

    extracted = extract_fields_from_text(full_text, fields, filename=json_file.name)
    if not extracted:
        print("Gemini failed to extract data. Skipping file.")
        continue
    for key, value in extracted.items():
        if key in combined_fields:
            combined_fields[key] = value

    # Translate extracted field values from Dutch to English
    for key, value in combined_fields.items():
        if isinstance(value, str) and value.strip():
            try:
                translated = GoogleTranslator(source='nl', target='en').translate(value)
                combined_fields[key] = translated
            except Exception as e:
                if DEBUG_MODE:
                    print(f"Translation error for key '{key}': {e}")

    combined_fields["CAO"] = json_file.stem
    if "TTW" in json_file.stem.upper():
        combined_fields["TTW"] = "yes"
    else:
        combined_fields["TTW"] = "no"

    df_results.loc[len(df_results)] = combined_fields

# === Save final results to Excel ===
os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)

# Print confirmation of completion
print(f"Analysis complete. Saved to {OUTPUT_EXCEL_PATH}")