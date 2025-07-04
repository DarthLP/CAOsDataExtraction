import os
import json
import yaml
import pandas as pd
from pathlib import Path
from deep_translator import GoogleTranslator

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

# === Ollama model configuration ===
import requests

OLLAMA_URL = "http://localhost:11434/v1"
OLLAMA_MODEL = "qwen3:4b"
MAX_CHARS = 1000  # Reduced chunk size to avoid timeouts

# Split long text into smaller chunks to fit within the model's input context
def chunk_text(text, max_chars=MAX_CHARS):
    paragraphs = text.split("\n")
    # Loop through paragraphs and group them into chunks without exceeding max_chars
    chunks = []
    current_chunk = ""
    for para in paragraphs:
        if len(current_chunk) + len(para) + 1 <= max_chars:
            current_chunk += para + "\n"
        else:
            chunks.append(current_chunk.strip())
            current_chunk = para + "\n"
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

# Send a prompt to the local Ollama model and return the response
import time

def query_ollama(prompt, retries=3, delay=5):
    headers = {"Content-Type": "application/json"}
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        "stream": False
    }
    for attempt in range(retries):
        try:
            response = requests.post(
                f"{OLLAMA_URL}/chat/completions",
                headers=headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            if not response.text.strip():
                raise ValueError("Empty response from model.")
            return response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        except Exception as e:
            if DEBUG_MODE:
                print(f"Ollama error on attempt {attempt+1}: {e}")
            time.sleep(delay)
    return ""

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

    raw_output = query_ollama(prompt)
    try:
        return json.loads(raw_output)
    except Exception as e:
        if DEBUG_MODE:
            print(f"Failed to parse JSON from model output: {e}\nRaw output:\n{raw_output}")
        return {}

# === Process each JSON file generated from PDF extraction ===
processed_files = 0
failed_chunks = 0
for json_file in Path(INPUT_JSON_FOLDER).glob("*.json"):
    # Merge all page texts into one string
    with open(json_file, "r", encoding="utf-8") as f:
        if processed_files >= MAX_JSON_FILES:
            break
        processed_files += 1
        pages = json.load(f)

    full_text = "\n".join(page["text"] for page in pages)
    # Split full text into chunks to fit model context window
    chunks = chunk_text(full_text)
    # Initialize a dictionary with all fields set to empty
    combined_fields = dict.fromkeys(columns, "")

    # Run field extraction for each chunk
    for chunk in chunks:
        if not chunk.strip():
            continue
        extracted = extract_fields_from_text(chunk, fields, filename=json_file.name)
        if not extracted:
            failed_chunks += 1
            if failed_chunks > 2:
                print("Too many Ollama failures. Aborting.")
                break
        time.sleep(2)  # Small delay between chunk queries
        for key, value in extracted.items():
            if key in combined_fields and not combined_fields[key]:
                combined_fields[key] = value

    combined_fields["CAO"] = json_file.stem
    if "TTW" in json_file.stem.upper():
        combined_fields["TTW"] = "yes"
    else:
        combined_fields["TTW"] = "no"

    # Append extracted row to results DataFrame
    df_results.loc[len(df_results)] = combined_fields

# === Save final results to Excel ===
os.makedirs(os.path.dirname(OUTPUT_EXCEL_PATH), exist_ok=True)
df_results.to_excel(OUTPUT_EXCEL_PATH, index=False)

# Print confirmation of completion
print(f"Analysis complete. Saved to {OUTPUT_EXCEL_PATH}")