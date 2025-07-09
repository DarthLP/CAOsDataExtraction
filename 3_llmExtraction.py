import os
import json
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv

# === Configuration: Set paths and constants ===
INPUT_JSON_FOLDER = "output_json"
OUTPUT_JSON_FOLDER = Path("llmExtracted_json")
FIELDS_PROMPT_PATH = "fields_prompt_collapsed.md"
DEBUG_MODE = False
MAX_JSON_FILES = 2  # Limit how many JSON files to process
OUTPUT_JSON_FOLDER.mkdir(exist_ok=True)

# === Configure Google Generative AI (Gemini) API ===
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
GEMINI_MODEL = "gemini-2.5-pro" 

# === Load collapsed field definitions ===
with open(FIELDS_PROMPT_PATH, "r", encoding="utf-8") as f:
    prompt_fields_markdown = f.read()

# === Define Prompt and Gemini Query ===
SYSTEM_PROMPT = (
    "You are an AI assistant that helps extract **raw context** from Dutch collective labor agreements (CAOs).\n"
    "These CAOs were originally PDF files, now represented as JSON — each page contains plain text from the source.\n\n"

    "=== Source Text ===\n"
    "From file: {filename}\n"
    "Below is the full text extracted from the CAO document:\n"
    "{text}\n\n"

    "=== Task ===\n"
    "Your task is to **identify and copy** all paragraphs, tables, descriptions, or sections that are even **remotely relevant** to the following categories:\n"
    "- General information: start_date_contract and expiry_date_contract\n"
    "- Wage information\n"
    "- Pension information\n"
    "- Leave information\n"
    "- Termination information\n"
    "- Overtime information\n"
    "- Training information\n\n"

    "=== Important Copying Rules ===\n"
    "- DO NOT hallucinate or generate content that isn't in the text.\n"
    "- Only copy content that is **explicitly present** in the source.\n"
    "- Always copy tables **in full**, including titles and headers. Do not split wage tables across sections — if they relate to job groups, keep them under 'Wage information'.\n"
    "- Copy entire paragraphs or full sentences. No truncation or paraphrasing.\n"
    "- If in doubt, include more — not less.\n"
    "- Preserve context between related fields (e.g., salaries linked to job groups).\n\n"

    "=== Guidance Using Field Definitions ===\n"
    "- Below is a field table to guide what might be relevant. You don’t need to extract structured values — this is for context.\n"
    "- Each field has a name in the first row, with a short description and example(s) in the second row.\n"
    "- Examples are illustrative only (e.g., 'F-21-5' as a job group) — your task is to capture **all** values of that type from the text.\n"
    "- Repeating fields like salary_1, salary_2 represent repeating patterns (e.g., multiple job groups) — copy any structure that supports this.\n\n"
    "- Salary and job group data are usually intertwined. Do not split them across sections unless they are clearly separated in the document.\n\n"
    f"{prompt_fields_markdown}\n\n"

    "=== Output Format ===\n"
    "Return a valid JSON object with the following keys:\n\n"
    "{{\n"
    "  \"General information\": [\"...\"],\n"
    "  \"Wage information\": [\"...\"],\n"
    "  \"Pension information\": [\"...\"],\n"
    "  \"Leave information\": [\"...\"],\n"
    "  \"Termination information\": [\"...\"],\n"
    "  \"Overtime information\": [\"...\"],\n"
    "  \"Training information\": [\"...\"]\n"
    "}}\n\n"
    "- Each key must contain a list of full, copied blocks of text (paragraphs, tables, etc.).\n"
    "- Include connector phrases only if they preserve critical context, and clearly mark them as such. Nver invent content.\n"
    "- If a section has no content, return an empty list for that key.\n"
    "- Return only valid JSON. No markdown or extra formatting.\n"
)

def extract_broad_context(text, filename):
    prompt = SYSTEM_PROMPT.format(filename=filename, text=text[:120000])
    response = genai.GenerativeModel(GEMINI_MODEL).generate_content(prompt)
    if hasattr(response, "text") and response.text.strip():
        return response.text
    raise ValueError("Empty or invalid model response")

# === Load and Process JSON Files ===
for json_file in Path(INPUT_JSON_FOLDER).glob("*.json"):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    full_text = "\n".join(page.get("text", "") for page in data if isinstance(page, dict))
    if not full_text:
        continue

    print(f"Extracting context from: {json_file.name}")
    try:
        raw_output = extract_broad_context(full_text, filename=json_file.name)

        # Clean LLM output
        cleaned_output = raw_output.strip()
        if cleaned_output.startswith("```"):
            cleaned_output = "\n".join(
                line for line in cleaned_output.splitlines() if not line.strip().startswith("```")
            )
        import re
        cleaned_output = re.sub(r',\s*(?=[}\]])', '', cleaned_output)

        # Try parsing JSON
        out_path = OUTPUT_JSON_FOLDER / json_file.name
        try:
            parsed_json = json.loads(cleaned_output)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(parsed_json, f, ensure_ascii=False, indent=2)
        except Exception as parse_error:
            print(f"Failed to parse cleaned output for {json_file.name}: {parse_error}")
            print("Raw model output was:")
            print(cleaned_output)

    except Exception as e:
        import traceback
        print(f"Error with {json_file.name}: {e}")
        traceback.print_exc()