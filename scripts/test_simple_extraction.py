#!/usr/bin/env python3
"""
Simple test to check if the Gemini API is working with basic text generation.
"""

import os
import time
from dotenv import load_dotenv
from google import genai

# Load environment variables
load_dotenv()

# Get API key
api_key = os.getenv("GOOGLE_API_KEY1")
if not api_key:
    raise ValueError("GOOGLE_API_KEY1 environment variable not found")

# Initialize client
client = genai.Client(api_key=api_key)
GEMINI_MODEL = "gemini-2.5-flash"

print("🧪 Testing basic Gemini API functionality...")

try:
    # Test 1: Simple text generation
    print("📝 Test 1: Simple text generation...")
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents="Say 'Hello World' in Dutch"
    )
    
    if response and hasattr(response, 'text') and response.text:
        print(f"✅ Success: {response.text}")
    else:
        print("❌ No response received")
        print(f"Response object: {response}")
        print(f"Response type: {type(response)}")
        
except Exception as e:
    print(f"❌ Error in simple test: {e}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

print("\n" + "="*50 + "\n")

try:
    # Test 2: File upload (if PDF exists)
    pdf_path = "input_pdfs/10/Bouw CAO 2011 aanmelding TTW 1-2011.pdf"
    if os.path.exists(pdf_path):
        print("📄 Test 2: File upload test...")
        
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
            print(f"  Still waiting... ({waited}s)")
        
        if waited >= max_wait:
            print("  Timeout waiting for file")
        else:
            # Test simple extraction
            print("  Testing simple extraction...")
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=["Extract the title of this document", uploaded_file]
            )
            
            if response and hasattr(response, 'text') and response.text:
                print(f"✅ Success: {response.text[:200]}...")
            else:
                print("❌ No response received")
                print(f"Response object: {response}")
                
    else:
        print(f"❌ Test PDF not found: {pdf_path}")
        
except Exception as e:
    print(f"❌ Error in file upload test: {e}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

print("\n" + "="*50 + "\n")

try:
    # Test 3: Structured output with simple schema
    print("🔧 Test 3: Simple structured output...")
    
    from pydantic import BaseModel, Field
    from typing import List
    
    class SimpleSchema(BaseModel):
        title: str = Field(description="Document title")
        pages: int = Field(description="Number of pages")
    
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents="Extract the title and number of pages from this document: 'Annual Report 2023 - 45 pages'",
        config={
            "response_mime_type": "application/json",
            "response_schema": SimpleSchema,
            "temperature": 0.0
        }
    )
    
    if response and hasattr(response, 'text') and response.text:
        print(f"✅ Success: {response.text}")
    else:
        print("❌ No response received")
        print(f"Response object: {response}")
        
except Exception as e:
    print(f"❌ Error in structured output test: {e}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")

print("\n🏁 Testing complete!")
