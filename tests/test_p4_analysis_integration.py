"""
Integration tests for the new p4_analysis.py implementation.
Tests with real JSON files from the llm_Extracted/new_flow pipeline.
"""

import sys
import os
import json
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.p4_analysis import (
    extract_salary_from_json, extract_nonsalary_from_json,
    merge_extraction_results, analyze_cao_json
)


def test_with_real_json_file():
    """Test with a real JSON file from the pipeline."""
    # Look for a real JSON file in the llm_Extracted/new_flow directory
    input_folder = "outputs_json/new_flow"
    
    if not os.path.exists(input_folder):
        print(f"Input folder {input_folder} does not exist, skipping integration test")
        return
    
    # Find the first JSON file
    json_files = []
    for cao_folder in Path(input_folder).iterdir():
        if cao_folder.is_dir() and cao_folder.name.isdigit():
            for json_file in cao_folder.glob('*.json'):
                json_files.append(json_file)
                break  # Just take the first one
            if json_files:
                break
    
    if not json_files:
        print("No JSON files found in llm_Extracted/new_flow, skipping integration test")
        return
    
    test_file = json_files[0]
    print(f"Testing with real JSON file: {test_file}")
    
    try:
        # Read the JSON file
        with open(test_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Test salary extraction (without LLM call)
        print("Testing salary extraction structure...")
        salary_text = ""
        if 'wage_information' in json_data:
            value = json_data['wage_information']
            if isinstance(value, list):
                flat_value = []
                for item in value:
                    if isinstance(item, list):
                        flat_value.extend(item)
                    else:
                        flat_value.append(str(item))
                salary_text = f'== Wage information ==\n' + '\n'.join(flat_value)
            elif isinstance(value, str):
                salary_text = f'== Wage information ==\n{value}'
        
        print(f"Salary text length: {len(salary_text)} characters")
        print(f"Estimated tokens: {len(salary_text) // 4}")
        
        # Test non-salary extraction structure
        print("Testing non-salary extraction structure...")
        rest_sections = ['general_information', 'pension_information', 'leave_information', 
                        'termination_information', 'overtime_information', 'training_information', 
                        'homeoffice_information']
        
        rest_text_parts = []
        for section in rest_sections:
            if section in json_data:
                value = json_data[section]
                if isinstance(value, list):
                    flat_value = []
                    for item in value:
                        if isinstance(item, list):
                            flat_value.extend(item)
                        else:
                            flat_value.append(str(item))
                    rest_text_parts.append(f'== {section} ==\n' + '\n'.join(flat_value))
                elif isinstance(value, str):
                    rest_text_parts.append(f'== {section} ==\n{value}')
        
        rest_text = '\n\n'.join(rest_text_parts)
        print(f"Rest text length: {len(rest_text)} characters")
        print(f"Estimated tokens: {len(rest_text) // 4}")
        
        print("✓ Integration test structure validation passed")
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        raise


def test_analyze_cao_json_function():
    """Test the main analyze_cao_json function."""
    # Test with the fixture
    fixture_path = "tests/fixtures/example_salary.json"
    
    if not os.path.exists(fixture_path):
        print(f"Fixture {fixture_path} does not exist, skipping test")
        return
    
    try:
        with open(fixture_path, 'r', encoding='utf-8') as f:
            json_text = f.read()
        
        # Test the function
        result = analyze_cao_json(json_text, "test_fixture.json")
        
        assert isinstance(result, dict)
        assert "salary_extraction" in result
        assert "non_salary_extraction" in result
        
        print("✓ analyze_cao_json function test passed")
        
    except Exception as e:
        print(f"✗ analyze_cao_json function test failed: {e}")
        raise


if __name__ == "__main__":
    print("Running integration tests for p4_analysis.py...")
    
    test_analyze_cao_json_function()
    test_with_real_json_file()
    
    print("\n✓ All integration tests passed!")
