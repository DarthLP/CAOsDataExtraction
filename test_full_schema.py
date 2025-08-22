#!/usr/bin/env python3
"""
Test with our full CAO schema but simplified prompt.
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

# Full CAO Schema (copy from 3_1_llmExtraction.py)
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

print("🧪 Testing full CAO schema with simplified prompt...")

try:
    # Test with our actual PDF
    pdf_path = "input_pdfs/10/Bouw CAO 2011 aanmelding TTW 1-2011.pdf"
    
    if os.path.exists(pdf_path):
        print("📄 Testing with actual PDF...")
        
        # Upload file
        print("  Uploading PDF...")
        uploaded_file = client.files.upload(file=pdf_path)
        print(f"  File uploaded: {uploaded_file.name}")
        
        # Wait for file to be ready
        print("  Waiting for file to be ready...")
        max_wait = 60
        waited = 0
        while waited < max_wait:
            file_resource = client.files.get(name=uploaded_file.name)
            if file_resource.state.name == "ACTIVE":
                print("  File is ready!")
                break
            elif file_resource.state.name == "FAILED":
                raise ValueError("File upload failed")
            time.sleep(2)
            waited += 2
        
        if waited >= max_wait:
            print("  Timeout waiting for file")
        else:
            # Test with simplified prompt but full schema
            print("  Testing extraction with full schema...")
            
            simple_prompt = """
Extract information from this Dutch CAO document.

TASK: Extract information into the specified fields.

RULES:
- Extract ONLY information explicitly present in the document
- Copy text literally
- NO paraphrasing or interpretation

Document: Bouw CAO 2011 aanmelding TTW 1-2011.pdf
"""
            
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[simple_prompt, uploaded_file],
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
                print(f"✅ Success! Response length: {len(response.text)} characters")
                print(f"First 500 chars: {response.text[:500]}...")
            else:
                print("❌ No response received")
                print(f"Response object: {response}")
                print(f"Response type: {type(response)}")
                if hasattr(response, 'candidates'):
                    print(f"Candidates: {response.candidates}")
                if hasattr(response, 'prompt_feedback'):
                    print(f"Prompt feedback: {response.prompt_feedback}")
                    
    else:
        print(f"❌ Test PDF not found: {pdf_path}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

print("\n🏁 Testing complete!")
