import os
import json
import time
from pathlib import Path
import google.generativeai as genai
from dotenv import load_dotenv
from tracker import update_progress

# =========================
# Configuration and Setup
# =========================

# Set paths and constants
INPUT_JSON_FOLDER = "output_json"
OUTPUT_JSON_FOLDER = Path("llmExtracted_json")
# FIELDS_PROMPT_PATH = "fields_prompt_collapsed.md"  # No longer needed - embedded in prompt
DEBUG_MODE = False
MAX_JSON_FILES = 40  # Limit how many JSON files to process
OUTPUT_JSON_FOLDER.mkdir(exist_ok=True)

# Configure Google Generative AI (Gemini) API
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("GOOGLE_API_KEY environment variable not found. Please set it before running this script.")
genai.configure(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-pro"

# =========================
# Prompt and Extraction Logic
# =========================

# Detailed field definitions embedded directly in the prompt
SYSTEM_PROMPT = (
    "You are an AI assistant specialized in extracting **raw context** from Dutch collective labor agreements (CAOs).\n"
    "Your task is to identify and extract ALL relevant information from the CAO text according to specific field categories.\n\n"

    "=== Source Document ===\n"
    "File: {filename}\n"
    "Document text:\n"
    "{text}\n\n"

    "=== EXTRACTION TASK ===\n"
    "Extract ALL information that matches ANY of the following field categories. Be comprehensive - if information could be relevant, include it.\n\n"

    "=== FIELD CATEGORIES AND DEFINITIONS ===\n\n"

    "1. GENERAL INFORMATION\n"
    "- start date contract: Start date of contract usually on front page (dates). Ex: 01/04/2019; 01/01/2017; 01/07/2018\n"
    "- expiry date contract: End date of contract usually on front page (dates). Ex: 31/01/2023; 31/03/2019; 28/02/2014\n"

    "2. WAGE INFORMATION\n"
    "- All information about jobgroups. Often mentioned in wage tables (numbers or text). Ex: I=Statutory minimum wage (WML); F-21-5; 65-12-57\n"
    "- Salaries of the job groups. Usually listed in wage tables. (numbers). Ex: 13,17; 21,59; 9,58\n"
    "- Units of the salary values in the wage tables. (texts). Ex: hourly; monthly\n"
    "- Start dates of wage tables (dates in dd/mm/yyyy format). Ex: 01/01/2018; 01/01/2017; 01/07/2010\n"
    "- Percentage increases of salaries in wage tables. (numbers with % - may appear outside the tables, often near the Dutch words for increase such as \"verhoging\" or \"loonsverhoging\"). Ex: 1%; 3%; 10,5%\n"
    "- Age group the salary tables apply to, considering only workers aged 21 and older (text). Ex: 21 years and older; 22 years and olders\n\n"
    "- All additional context related to salary interpretation (text). Ex: Youth salary scales phased out from 2014; Hourly wage = monthly salary / 156; Classification via FWG® system; Introductory salary scales abolished as of 2013\n"

    "3. PENSION INFORMATION\n"
    "- pension premium basic: All information related to the basic pension premium and scheme (text). Ex: 50% of the pension premium will be paid by the employee; The pension scheme follows the rules of Stichting Bedrijfstakpensioenfonds; Employees aged 21 to 68 are registered with the Food Industry Pension Fund; The RVU allows early retirement up to AOW age if certain conditions are met\n"
    "- pension premium plus: All information related to additional or \"plus\" pension premiums, including age-related schemes like the Generation Policy and changes in contribution percentages (text). Ex: Pension premium increased to 21,4% in 2021, split evenly between employer and employee; Generation Policy applies to workers aged 60+ between 2018 and 2023; 0,2% premium increase for employees offset by wage increase on 1-6-2021\n"
    "- retire age basic: Retirement age for the basic pension scheme (text or number). Ex: 67; 68; 67–68\n"
    "- retire age plus: Retirement age for the additional or \"plus\" pension premium scheme (text or number). Ex: 65; 68; 66–68\n"
    "- pension age group: Age group eligible for the pension scheme (text). Ex: 21 years and older; 22 years and older\n\n"

    "4. LEAVE INFORMATION\n"
    "- maternity leave: All information related to the duration of maternity, adoption, or child-related leave (text). Ex: 5 days of paid maternity leave; At least 16 weeks; Additional 4 weeks in case of multiple births; Up to 5 weeks extra within 6 months after birth\n"
    "- maternity pay: All available information about salary, benefits, or compensation during maternity, adoption, or child-related leave (text). Include both employer and UWV contributions. Ex: 100% paid by employer; 70% UWV benefit; 100% of maximum daily wage\n"
    "- maternity note: All additional context related to maternity/ adoption/ child-related leave rules not covered in other fields (text). Include among other things rights, accruals, flexibility, partner substitution, and legal protections. Ex: Vacation accrues during leave; Leave may be split; Partner receives remaining leave if employee dies; 1 hour weekly reduction after birth\n"
    "- vacation time: All available information about the amount of vacation or holiday time employees are entitled to (number or text). Include base and extra entitlements. Ex: 0.0769; 192; 20\n"
    "- vacation unit: Unit or accrual structure of the vacation time reported in the previous column \"vacation_time\" (text). Be precise about whether it's hours, days, or a formula-based accrual. Ex: hours per vacation year; days per full-time contract\n"
    "- vacation note: All additional vacation/ holiday-related information not covered in other fields (text). Include accrual rules, holiday years, age/tenure-based bonuses, payout terms, and holiday allowance rules. Ex: Holiday year runs June–May; 8% holiday allowance; 3 weeks of consecutive vacation; 5 extra days after 40 years of service\n\n"

    "5. TERMINATION INFORMATION\n"
    "- termination period employer: All information about the notice period duration or unit for employer-initiated contract termination (text). Include special rules based on age, start date, or contract length. Ex: 1 month for contracts longer than 6 months; 4 weeks for employees with 10–15 years of service; Statutory period applies if longer than agreed term\n"
    "- termination employer note: All other information about employer-initiated contract termination not covered in the previous column \"term_period_employer\" (text). Include legal references, conditions, exceptions, or case-specific rules. Ex: Civil Code provisions apply; Prior employment counts toward service years; Periods may be defined in months or 4-week cycles\n"
    "- termination period worker: All information about the notice period duration or unit for worker-initiated contract termination (text). Include special rules based on age, start date, or contract length. Ex: 1 week if less than 2 years employed; 10 days; 6 weeks max based on age and service duration\n"
    "- termination worker note: All other information about worker-initiated contract termination not covered in the previous column \"term_period_worker\" (text). Include conditions, exceptions, legal references, or case-specific clauses. Ex: Old rule applies for employees aged 45+ as of Jan 1, 1999; Starting date for notice is always a Saturday\n"
    "- probation period: All information about the length or conditions of the probation period for new workers (text). Include all relevant rules based on contract length or type. Ex: 2 months for indefinite contracts; No trial period if contract ≤ 6 months; 1 month max for fixed-term contracts between 6 and 24 months\n"
    "- probation note: All other information about the probation period not covered in the previous column \"probation_period\" (text). Include references to conditions, exceptions, legal references, case-specific clauses or when probation is disallowed. Ex: Trial period only allowed if new role involves substantially different responsibilities; Article 7:652 of the Civil Code applies\n\n"

    "6. OVERTIME INFORMATION\n"
    "- overtime compensation: All information about overtime pay or compensation, including units, percentages, and whether compensation is given in time or money (text). Include legal rules, employer-specific clauses, and time limits. Ex: 35% surcharge on basic hourly wage; Paid time off within 4 weeks; 100% of hourly wage plus overtime premium; Overtime after 152 hours per period\n"
    "- max hrs: All information about the maximum allowed working hours or overtime hours, including what type of time it applies to and for which worker categories (text). Ex: 12 hours per day; Max 10 hours of overtime per week; 36 hours max overtime per 3 pay periods; 52-hour weekly average if salary exceeds IP number 74\n"
    "- min hrs: All information about the minimum required number of hours, days, weeks, or months to be worked, including units and reference periods (text). Ex: 24 hours per week; 8 hours per day minimum; 20 working days per month\n"
    "- shift compensation: All information about shift-based work and related compensation, including night, evening, early morning, and weekend shifts (text). Include hours, pay surcharges, limitations, and scheduling rules. Ex: 25% surcharge from 8pm–10pm; 50% surcharge between 10pm–6am; Night shift defined as work between 00:00 and 06:00; Max 20 shifts per 4-week period\n"
    "- overtime allowance min: All information about the minimum allowance for overtime or night shift work, including duration, compensation, and applicable legal limits (text). Ex: At least 4.5-hour shift to qualify for night compensation; Minimum 1 paid break for shifts covering 00:00–06:00; 16 night shifts over 16 weeks triggers lower working hour threshold\n"
    "- overtime allowance max: All information about the maximum allowance for overtime or night shift work, including duration, compensation, and applicable legal limits (text). Ex: Max 12 hours per day; Max 43 night shifts in 16 weeks; Max 36 overtime hours per 3 pay periods; Working time averaged over 13 weeks not to exceed 48 hours\n\n"

    "7. TRAINING INFORMATION\n"
    "- training: All information related to training, development, or education for employees or employers (text). Include training rights, budgets, mandatory recognition, funding percentages, and types of courses. Ex: Minimum 2% of annual payroll must be used for training; POB budget of €175 per year; Dutch language course and vocational training; Only recognized training companies may provide internships\n\n"

    "=== EXTRACTION RULES ===\n"
    "1. COPY EXACTLY - Do not paraphrase, summarize, or modify the original text\n"
    "2. BE COMPREHENSIVE - Include ALL relevant information, even if it seems minor\n"
    "3. PRESERVE CONTEXT - Keep related information together (e.g., salary tables with job groups)\n"
    "4. INCLUDE TABLES - Copy wage tables and structured data completely, including headers and descriptions\n"
    "5. CAPTURE DETAILS - Include specific numbers, percentages, dates, and conditions\n"
    "6. MAINTAIN STRUCTURE - Preserve the original document's organization where relevant\n"
    "7. NO HALLUCINATION - Only extract information that is explicitly present in the text\n\n"

    "=== OUTPUT FORMAT ===\n"
    "You MUST return ONLY a valid JSON object with these exact keys. Do not include any explanations, markdown formatting, or additional text.\n\n"
    "{{\n"
    "  \"General information\": [\"...\"],\n"
    "  \"Wage information\": [\"...\"],\n"
    "  \"Pension information\": [\"...\"],\n"
    "  \"Leave information\": [\"...\"],\n"
    "  \"Termination information\": [\"...\"],\n"
    "  \"Overtime information\": [\"...\"],\n"
    "  \"Training information\": [\"...\"]\n"
    "}}\n\n"
    "CRITICAL REQUIREMENTS:\n"
    "- Return ONLY the JSON object - no markdown, no explanations, no extra text\n"
    "- Each key must contain a list of full, copied blocks of text (paragraphs, tables, etc.)\n"
    "- Each text block should be a complete, coherent piece of information\n"
    "- If no relevant information exists for a category, use an empty array []\n"
    "- Ensure all extracted information is accurate and complete\n"
    "- The response must be parseable as valid JSON\n"
    "- IMPORTANT: Escape any quotes within the text content using backslash (\\)\n"
    "- IMPORTANT: Do not include any text before or after the JSON object\n"
    "- IMPORTANT: Start your response with {{ and end with }}\n"
)

def extract_broad_context(text, filename, max_retries=5):
    """
    Extract context from CAO text using Gemini LLM with exponential backoff retry logic for errors.
    Args:
        text (str): The full CAO document text to extract from.
        filename (str): The filename for context in the prompt.
        max_retries (int): Maximum number of retry attempts for API errors.
    Returns:
        str: The raw LLM output (should be JSON string or similar).
    Raises:
        ValueError: If all retry attempts fail.
    """
    for attempt in range(max_retries):
        try:
            prompt = SYSTEM_PROMPT.format(filename=filename, text=text[:120000])
            model = genai.GenerativeModel(GEMINI_MODEL)
            response = model.generate_content(prompt)
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
                    print(f"  All {max_retries} attempts failed with 504 errors for {filename} - skipping file")
                    return ""  # Return empty string to skip this file
            
            # Handle other rate limiting errors
            elif any(keyword in error_str for keyword in ["quota", "rate limit", "too many requests", "429"]):
                if attempt < max_retries - 1:
                    wait_time = 300 * (2 ** attempt)  # 5 minutes, 10 minutes, 20 minutes
                    print(f"  Attempt {attempt + 1} failed (rate limit), retrying in {wait_time//60} minutes...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  All {max_retries} attempts failed with rate limiting for {filename} - skipping file")
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
    
    raise ValueError(f"All {max_retries} retry attempts failed for {filename}")

# =========================
# Main LLM Extraction Loop
# =========================

# Get all CAO number folders in input folder
cao_folders = [f for f in Path(INPUT_JSON_FOLDER).iterdir() if f.is_dir() and f.name.isdigit()]

# Track processed files for limit
processed_files = 0

# Loop over each CAO folder
for cao_folder in cao_folders:
    cao_number = cao_folder.name
    print(f"Processing CAO {cao_number}")
    
    # Create corresponding output folder
    output_cao_folder = OUTPUT_JSON_FOLDER / cao_number
    output_cao_folder.mkdir(exist_ok=True)
    
    # Get all JSON files in this CAO folder
    json_files = list(cao_folder.glob("*.json"))
    
    # Track successful and failed LLM extractions
    successful_extractions = 0
    failed_files = []
    
    # Loop over each JSON file in the CAO folder
    for json_file in json_files:
        # Check if we've reached the limit
        if processed_files >= MAX_JSON_FILES:
            print(f"Reached limit of {MAX_JSON_FILES} files. Stopping processing.")
            break
        
        # Check if output file already exists (skip if already processed)
        output_file = output_cao_folder / json_file.name
        if output_file.exists():
            print(f"  Skipping {json_file.name} (already processed)")
            # Small delay when skipping to maintain flow
            time.sleep(5)
            successful_extractions += 1  # Count as successful since it was already processed
            continue
            
        print(f"  Processing {json_file.name}")
        processed_files += 1
        
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        full_text = "\n".join(page.get("text", "") for page in data if isinstance(page, dict))
        if not full_text:
            continue

        try:
            raw_output = extract_broad_context(full_text, filename=json_file.name)

            # Debug: Print the first 500 characters of raw output
            print(f"  Raw LLM output (first 500 chars): {raw_output[:500]}...")

            # Clean LLM output to ensure valid JSON
            cleaned_output = raw_output.strip()
            
            # Remove markdown code blocks
            if cleaned_output.startswith("```"):
                lines = cleaned_output.splitlines()
                # Find the start and end of code blocks
                start_idx = None
                end_idx = None
                for i, line in enumerate(lines):
                    if line.strip().startswith("```") and start_idx is None:
                        start_idx = i
                    elif line.strip().startswith("```") and start_idx is not None:
                        end_idx = i
                        break
                
                if start_idx is not None and end_idx is not None:
                    cleaned_output = "\n".join(lines[start_idx + 1:end_idx])
                else:
                    # If we can't find proper code blocks, remove all ``` lines
                    cleaned_output = "\n".join(
                        line for line in lines if not line.strip().startswith("```")
                    )
            
            # Remove any text before the first { and after the last }
            first_brace = cleaned_output.find('{')
            last_brace = cleaned_output.rfind('}')
            if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                cleaned_output = cleaned_output[first_brace:last_brace + 1]
            
            # Fix common JSON issues - more conservative approach
            import re
            # Remove trailing commas before closing brackets/braces
            cleaned_output = re.sub(r',\s*(?=[}\]])', '', cleaned_output)
            # Fix newlines in string values (but preserve them in the actual content)
            cleaned_output = re.sub(r'(?<!\\)\n(?=.*":\s*")', '\\n', cleaned_output)
            # Fix carriage returns
            cleaned_output = re.sub(r'(?<!\\)\r(?=.*":\s*")', '\\r', cleaned_output)

            # Try parsing JSON with fallback attempts
            out_path = output_cao_folder / json_file.name
            parsed_json = None
            
            # First attempt: try the cleaned output as-is
            try:
                parsed_json = json.loads(cleaned_output)
            except Exception as parse_error:
                # Second attempt: try to fix common JSON issues
                try:
                    # Remove any non-JSON text and try again
                    import re
                    # Find JSON object boundaries
                    json_match = re.search(r'\{.*\}', cleaned_output, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                        # Fix trailing commas
                        json_str = re.sub(r',\s*(?=[}\]])', '', json_str)
                        # Try to parse the extracted JSON
                        parsed_json = json.loads(json_str)
                    else:
                        # Check if the response is a JSON string (double-encoded)
                        if cleaned_output.startswith('"') and cleaned_output.endswith('"'):
                            # Try to unescape the JSON string
                            try:
                                unescaped = json.loads(cleaned_output)
                                # Now try to parse the unescaped content as JSON
                                parsed_json = json.loads(unescaped)
                            except:
                                raise ValueError("No valid JSON object found in response")
                        else:
                            raise ValueError("No JSON object found in response")
                except Exception as second_error:
                    # Third attempt: try to manually construct JSON from the response
                    try:
                        print(f"  Attempting manual JSON construction for {json_file.name}")
                        # Look for the expected keys in the response
                        expected_keys = [
                            "General information", "Wage information", "Pension information",
                            "Leave information", "Termination information", "Overtime information", "Training information"
                        ]
                        
                        parsed_json = {}
                        for key in expected_keys:
                            # Look for the key in the response
                            key_pattern = rf'"{re.escape(key)}"\s*:\s*\[(.*?)\]'
                            match = re.search(key_pattern, cleaned_output, re.DOTALL | re.IGNORECASE)
                            if match:
                                # Extract the content between the brackets
                                content = match.group(1).strip()
                                # Split by commas and clean up
                                items = []
                                if content:
                                    # Simple split by comma, but be careful with nested structures
                                    parts = content.split('","')
                                    for part in parts:
                                        part = part.strip().strip('"').strip()
                                        if part:
                                            items.append(part)
                                parsed_json[key] = items
                            else:
                                parsed_json[key] = []
                        
                        # If we found any content, use it
                        if any(parsed_json.values()):
                            print(f"  Successfully constructed JSON manually for {json_file.name}")
                        else:
                            raise ValueError("No content found in manual construction")
                            
                    except Exception as manual_error:
                        # Fourth attempt: create a minimal valid JSON structure
                        try:
                            print(f"  Failed to parse JSON for {json_file.name}, creating fallback structure")
                            parsed_json = {
                                "General information": [],
                                "Wage information": [],
                                "Pension information": [],
                                "Leave information": [],
                                "Termination information": [],
                                "Overtime information": [],
                                "Training information": []
                            }
                            # Try to extract any useful text and put it in a general category
                            if cleaned_output.strip():
                                # Remove markdown and extract plain text
                                text_content = re.sub(r'[#*`]', '', cleaned_output)
                                if text_content.strip():
                                    parsed_json["General information"] = [text_content[:1000] + "..."]
                        except Exception as fallback_error:
                            print(f"  All JSON parsing attempts failed for {json_file.name}")
                            print(f"  Original error: {parse_error}")
                            print(f"  Second attempt error: {second_error}")
                            print(f"  Manual construction error: {manual_error}")
                            print(f"  Fallback error: {fallback_error}")
                            print("  Raw model output was:")
                            print(cleaned_output[:500] + "..." if len(cleaned_output) > 500 else cleaned_output)
                            failed_files.append(json_file.name)
                            continue
            
            # Save the parsed JSON (either valid or fallback)
            if parsed_json:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(parsed_json, f, ensure_ascii=False, indent=2)
                print(f"  Saved to {out_path}")
                successful_extractions += 1
                
                # Add 2-minute delay after successful request to prevent rate limiting
                print("  Waiting 120 seconds before next request...")
                time.sleep(120)

        except Exception as e:
            import traceback
            print(f"  Error with {json_file.name}: {e}")
            traceback.print_exc()
            failed_files.append(json_file.name)
    
    # Update tracker for this CAO
    update_progress(cao_number, "llm_extraction", successful=successful_extractions, failed_files=failed_files)
    
    # Check if we've reached the limit (break out of outer loop too)
    if processed_files >= MAX_JSON_FILES:
        break