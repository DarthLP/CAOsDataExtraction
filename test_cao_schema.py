#!/usr/bin/env python3
"""
Test our CAO schema to identify why it's returning empty responses.
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

# Simplified CAO Schema for testing
class SimpleCAOSchema(BaseModel):
    """Simplified schema for testing."""
    
    general_information: List[List[str]] = Field(
        description="Extract: Document title, contract period dates, parties involved.",
        default_factory=list
    )
    
    wage_information: List[List[str]] = Field(
        description="Extract: Wage tables with all columns and rows. Include different worker types (LONEN vs SALARISSEN), different dates, different percentages.",
        default_factory=list
    )
    
    model_config = ConfigDict(
        title="Simple CAO Schema",
        json_schema_extra={
            "propertyOrdering": [
                "general_information",
                "wage_information"
            ]
        }
    )

print("🧪 Testing CAO schema...")

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
            # Test with simplified prompt and schema
            print("  Testing extraction with simplified schema...")
            
            simple_prompt = """
Extract information from this Dutch CAO document.

TASK: Extract general information and wage tables.

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
                    "response_schema": SimpleCAOSchema,
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
