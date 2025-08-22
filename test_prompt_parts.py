#!/usr/bin/env python3
"""
Test different parts of our complex prompt to identify the problematic section.
"""

import os
import time
from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional

# Load environment variables
load_dotenv()

# Get API key
api_key = os.getenv("GOOGLE_API_KEY1")
if not api_key:
    raise ValueError("GOOGLE_API_KEY1 environment variable not found")

# Initialize client
client = genai.Client(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-flash"

# Full CAO Schema
class CAOExtractionSchema(BaseModel):
    """Schema for extracting structured data from Dutch CAO documents."""
    
    general_information: List[List[str]] = Field(
        description="Extract: Document title, contract period dates, validity dates, parties involved, scope of agreement, general contract terms.",
        default_factory=list
    )
    
    wage_information: List[List[str]] = Field(
        description="Extract: Complete wage tables with all columns and rows, salary scales, job classifications, hourly/monthly rates, ages, age-based increases, allowances, bonuses. IMPORTANT: Extract ALL wage tables, including different worker types (e.g., LONEN vs SALARISSEN), different dates, different percentages, different job categories, and different time periods. Only skip if tables are identical except for the unit (e.g., hourly vs monthly rates for the same job/date). Include tables for: regular employees, apprentices, trainees, different salary scales, different effective dates, different percentage increases.",
        default_factory=list
    )
    
    pension_information: List[List[str]] = Field(
        description="Extract: Pension scheme details, contribution percentages, employer/employee splits, retirement ages, eligibility requirements, pension fund information.",
        default_factory=list
    )
    
    leave_information: List[List[str]] = Field(
        description="Extract: Vacation days/weeks, holiday allowances, maternity leave, paternity leave, sick leave policies, special leave types.",
        default_factory=list
    )
    
    termination_information: List[List[str]] = Field(
        description="Extract: Notice periods, probation periods, termination procedures, dismissal rules, severance pay, exit requirements. Include complete termination notice period tables with all age/service year combinations.",
        default_factory=list
    )
    
    overtime_information: List[List[str]] = Field(
        description="Extract: Overtime rates, shift differentials, weekend/holiday pay, night work compensation, maximum hours, overtime conditions.",
        default_factory=list
    )
    
    training_information: List[List[str]] = Field(
        description="Extract: Training entitlements, education budgets, professional development programs, course allowances, study time, certification support.",
        default_factory=list
    )
    
    homeoffice_information: List[List[str]] = Field(
        description="Extract: Remote work policies, home office allowances, equipment provisions, internet/phone reimbursements, work-from-home conditions, hybrid work arrangements.",
        default_factory=list
    )
    
    model_config = ConfigDict(
        title="CAO Extraction Schema",
        json_schema_extra={
            "propertyOrdering": [
                "general_information",
                "wage_information", 
                "pension_information",
                "leave_information",
                "termination_information",
                "overtime_information",
                "training_information",
                "homeoffice_information"
            ]
        }
    )

def test_prompt(prompt_name, prompt_text, uploaded_file):
    """Test a specific prompt and return success status."""
    print(f"\n🧪 Testing: {prompt_name}")
    print(f"Prompt length: {len(prompt_text)} characters")
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt_text, uploaded_file],
            config={
                "temperature": 0.0,
                "top_p": 0.1,
                "top_k": 1,
                "max_output_tokens": 65536,
                "candidate_count": 1,
                "seed": 42,
                "presence_penalty": 0,
                "frequency_penalty": 0,
                "response_mime_type": "application/json",
                "response_schema": CAOExtractionSchema,
                "thinking_config": types.ThinkingConfig(thinking_budget=-1)
            }
        )
        
        if response and hasattr(response, 'text') and response.text:
            print(f"✅ SUCCESS: {len(response.text)} characters")
            return True
        else:
            print(f"❌ FAILED: No response")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

print("🧪 Testing different prompt parts...")

try:
    # Test with our actual PDF
    pdf_path = "input_pdfs/10/Bouw CAO 2011 aanmelding TTW 1-2011.pdf"
    
    if os.path.exists(pdf_path):
        print("📄 Uploading PDF...")
        
        # Upload file
        uploaded_file = client.files.upload(file=pdf_path)
        print(f"File uploaded: {uploaded_file.name}")
        
        # Wait for file to be ready
        max_wait = 60
        waited = 0
        while waited < max_wait:
            file_resource = client.files.get(name=uploaded_file.name)
            if file_resource.state.name == "ACTIVE":
                print("File is ready!")
                break
            elif file_resource.state.name == "FAILED":
                raise ValueError("File upload failed")
            time.sleep(2)
            waited += 2
        
        if waited >= max_wait:
            print("Timeout waiting for file")
        else:
            # Test different prompt variations
            
            # Test 1: Basic prompt
            basic_prompt = """
Extract information from this Dutch CAO document.

TASK: Extract information into the specified fields.

RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally
- NO paraphrasing or interpretation

Document: Bouw CAO 2011 aanmelding TTW 1-2011.pdf
"""
            test_prompt("Basic Prompt", basic_prompt, uploaded_file)
            
            # Test 2: Basic + Topics
            topics_prompt = """
Extract information from this Dutch CAO document.

TASK: Extract information into the specified fields.

TOPICS TO EXTRACT:
- General contract information (dates, parties, scope)
- Wage tables and salary information
- Pension schemes and contributions
- Leave policies and vacation entitlements
- Termination procedures and notice periods
- Overtime rates and shift compensation
- Training programs and budgets
- Remote work policies and allowances

RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally
- NO paraphrasing or interpretation

Document: Bouw CAO 2011 aanmelding TTW 1-2011.pdf
"""
            test_prompt("Basic + Topics", topics_prompt, uploaded_file)
            
            # Test 3: Basic + Topics + Critical Rules
            critical_prompt = """
Extract information from this Dutch CAO document.

TASK: Extract information into the specified fields.

TOPICS TO EXTRACT:
- General contract information (dates, parties, scope)
- Wage tables and salary information
- Pension schemes and contributions
- Leave policies and vacation entitlements
- Termination procedures and notice periods
- Overtime rates and shift compensation
- Training programs and budgets
- Remote work policies and allowances

CRITICAL RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally (dates, numbers, percentages, units)
- NO paraphrasing, NO interpretation, NO added explanations
- NO unnecessary separator lines or formatting characters, NO decorative elements
- Each snippet should be pure content from the document

Document: Bouw CAO 2011 aanmelding TTW 1-2011.pdf
"""
            test_prompt("Basic + Topics + Critical Rules", critical_prompt, uploaded_file)
            
            # Test 4: Full prompt (our current one)
            full_prompt = """
Extract information from this Dutch CAO (Collective Labor Agreement) PDF document.

TASK: Categorize and extract relevant information into the specified fields based on the document content.

TOPICS TO EXTRACT:
- General contract information (dates, parties, scope)
- Wage tables and salary information 
- Pension schemes and contributions
- Leave policies and vacation entitlements
- Termination procedures and notice periods
- Overtime rates and shift compensation
- Training programs and budgets
- Remote work policies and allowances

CRITICAL RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally (dates, numbers, percentages, units)
- NO paraphrasing, NO interpretation, NO added explanations
- NO unnecessary separator lines or formatting characters, NO decorative elements
- Each snippet should be pure content from the document

CONTENT INCLUSION RULES:
- Include relevant headers, section titles, and table headers for each topic
- Include relevant numerical values, percentages, amounts, and time periods for each topic
- Include conditions, requirements, and procedural steps
- Include entitlements, allowances, and eligibility criteria
- For articles and legal text, include complete paragraphs with article numbers and full content
- For tables, include complete table structure with all headers and data rows and also include descriptions
- WAGE TABLES: Extract ALL wage tables found in the document, including different worker types, different effective dates, different percentage increases, and different job categories. Only skip if tables are identical except for the unit (hourly vs monthly vs yearly rates for the same job/date).

TABLE FORMATTING:
- Preserve table structure: each table row should be a single array element containing all columns
- Keep table headers, descriptions, column names, and data rows together as complete units
- Maintain column alignment and spacing within each row
- Identify tables by their grid-like structure with multiple columns and rows
- For regular text (articles, paragraphs), keep each logical unit as a separate array element

Document: Bouw CAO 2011 aanmelding TTW 1-2011.pdf
"""
            test_prompt("Full Prompt", full_prompt, uploaded_file)
                    
    else:
        print(f"❌ Test PDF not found: {pdf_path}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

print("\n🏁 Testing complete!")
