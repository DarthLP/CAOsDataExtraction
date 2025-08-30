#!/usr/bin/env python3
"""
Retry Failed Files Script

This script retries the 5 files that failed during the main extraction process.
It's based on the p3_llmExtraction.py pipeline but modified to handle specific files.

Usage:
    unbuffer caffeinate python scripts/retry_failed_files.py 2>&1 | tee logRETRY.txt
"""

import os
import sys
import time
import yaml
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Import the same modules as p3_llmExtraction.py
from pipelines.p3_llmExtraction import (
    ExtractionConfig, ProcessingContext, setup_environment, 
    setup_gemini_client, extract_with_markdown_upload,
    validate_markdown_file, log_processing_result, save_extraction_result,
    acquire_file_lock, release_file_lock, announce_cao_once
)

# List of files that failed during the main extraction
FAILED_FILES = [
    "CAO Gehandicaptenzorg 2021-2024 tweede ronde.md",
    "cao_2014_2016_aanmelding.md", 
    "Werkversie_CAO_GGZ_2011.md",
    "CAO_VVT_2010_2012_2_.md",
    "CAO VVT 2022-2024 verlengde cao ttw.md"
]

def find_failed_files(input_folder: Path) -> List[Tuple[Path, str]]:
    """Find the failed files in the input folder and return (file_path, cao_number) tuples."""
    found_files = []
    
    for cao_folder in input_folder.iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            for markdown_file in cao_folder.glob("*.md"):
                if markdown_file.name in FAILED_FILES:
                    found_files.append((markdown_file, cao_folder.name))
                    print(f"Found failed file: {markdown_file} (CAO: {cao_folder.name})")
    
    return found_files

def process_single_failed_file(markdown_file: Path, cao_number: str, output_folder: Path, 
                              context: ProcessingContext) -> bool:
    """Process a single failed file with the same logic as p3_llmExtraction.py."""
    # Generate output filename
    output_filename = markdown_file.name
    if not output_filename.endswith('.json'):
        output_filename += '.json'
    
    output_file = output_folder / output_filename
    
    # Check if already processed
    if output_file.exists():
        print(f'  {cao_number}: Skipping {markdown_file.name} (already processed)')
        return True
    
    # Try to acquire lock
    if not acquire_file_lock(output_file, context):
        print(f'  {cao_number}: Skipping {markdown_file.name} (being processed by another process)')
        return True
    
    try:
        # Validate markdown file
        is_valid, quality_message = validate_markdown_file(str(markdown_file))
        if not is_valid:
            print(f'  {cao_number}: ✗ Markdown quality check failed for {markdown_file.name}: {quality_message}')
            return True
        
        # Check file size
        file_size_mb = os.path.getsize(markdown_file) / (1024 * 1024)
        if file_size_mb > 50.0:
            print(f'  {cao_number}: ✗ Markdown file too large ({file_size_mb:.1f}MB) - exceeds 50MB limit for {markdown_file.name}')
            return True
        
        print(f'  {cao_number}: {markdown_file.name} (Markdown: {file_size_mb:.1f}MB) - RETRYING WITH INCREASED TOKENS')
        
        # Extract content with increased token limits
        raw_output = extract_with_markdown_upload(str(markdown_file), markdown_file.name, cao_number, context)
        
        if not raw_output:
            print(f'  {cao_number}: ✗ LLM extraction failed for {markdown_file.name}')
            return True
        
        # Save result with validation
        save_success = save_extraction_result(output_file, raw_output)
        if save_success:
            print(f'  {cao_number}: ✅ RETRY SUCCESSFUL - LLM extraction completed for {markdown_file.name}')
        else:
            print(f'  {cao_number}: ✗ JSON validation failed, extraction not saved for {markdown_file.name}')
            return True
        
        return True
        
    except Exception as e:
        import traceback
        print(f'  {cao_number}: Error with {markdown_file.name}: {e}')
        traceback.print_exc()
        return True
    finally:
        release_file_lock(output_file)

def retry_failed_files():
    """Retry the failed files with increased token limits."""
    print("🔄 Retrying Failed Files with Increased Token Limits")
    print("=" * 60)
    
    # Load configuration (same as p3)
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    config = ExtractionConfig(
        input_folder=Path(config_data['paths']['parsed_pdfs_markdown']),
        output_folder=Path(config_data['paths']['outputs_json']) / "new_flow"
    )
    
    # Use maximum allowed tokens for retry
    config.max_tokens = 65536  # Maximum allowed by Gemini API
    
    # Setup environment
    api_key, key_number = setup_environment(1)
    client = setup_gemini_client(api_key)
    
    # Create processing context (same as p3)
    context = ProcessingContext(
        config=config,
        process_id=999,  # Special process ID for retry
        total_processes=1,
        api_key=api_key,
        key_number=key_number,
        client=client,
        performance_monitor=None  # Skip performance monitoring for retry
    )
    
    # Find failed files
    failed_files = find_failed_files(config.input_folder)
    
    if not failed_files:
        print("❌ No failed files found to retry")
        return
    
    print(f"📁 Found {len(failed_files)} failed files to retry")
    print(f"🔧 Using {config.max_tokens:,} max tokens (maximum allowed)")
    print()
    
    # Process each failed file
    for markdown_file, cao_number in failed_files:
        output_folder = config.output_folder / cao_number
        output_folder.mkdir(exist_ok=True)
        
        # Announce CAO once (same as p3)
        announce_cao_once(cao_number, context)
        
        print(f"🔄 Retrying: {markdown_file.name}")
        print(f"📁 CAO: {cao_number}")
        
        # Process file with same logic as p3
        success = process_single_failed_file(markdown_file, cao_number, output_folder, context)
        
        if success:
            print(f"✅ Retry completed for: {markdown_file.name}")
        else:
            print(f"❌ Retry failed for: {markdown_file.name}")
        
        print("-" * 40)
        time.sleep(5)  # Delay between retries
    
    print("🎉 Retry process completed!")

if __name__ == "__main__":
    retry_failed_files()
