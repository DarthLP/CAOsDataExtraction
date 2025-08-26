"""
Basic tests for the new p4_analysis.py implementation.
"""

import sys
import os
import json
from pathlib import Path

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.p4_analysis import (
    SalaryRow, SalaryExtractionSchema,
    ContractInfo, PensionInfo, LeaveInfo, TerminationInfo, 
    OvertimeInfo, TrainingInfo, HomeofficeInfo, NonSalaryExtractionSchema,
    merge_extraction_results
)


def test_salary_schema():
    """Test that salary schema works correctly."""
    # Create a sample salary row
    salary_row = SalaryRow(
        jobgroup="Helper A",
        salary_1="2200",
        salary_1_unit="monthly",
        salary_1_startdate="2023-01-01",
        salary_increment_1="2%",
        more_salaries=False,
        salary_note="based on 36h week",
        salary_age_group="21+"
    )
    
    # Create schema with the row
    schema = SalaryExtractionSchema(salary_information=[salary_row])
    
    # Convert to dict
    result = schema.model_dump()
    
    assert "salary_information" in result
    assert len(result["salary_information"]) == 1
    assert result["salary_information"][0]["jobgroup"] == "Helper A"
    assert result["salary_information"][0]["salary_1"] == "2200"
    
    print("✓ Salary schema test passed")


def test_nonsalary_schema():
    """Test that non-salary schema works correctly."""
    # Create sample data
    contract_info = ContractInfo(
        start_date_contract="2023-01-01",
        expiry_date_contract="2024-12-31"
    )
    
    pension_info = PensionInfo(
        pension_premium_basic="50% employee, 50% employer",
        retire_age_basic="67"
    )
    
    # Create schema
    schema = NonSalaryExtractionSchema(
        contract_information=contract_info,
        pension_information=pension_info
    )
    
    # Convert to dict
    result = schema.model_dump()
    
    assert "contract_information" in result
    assert "pension_information" in result
    assert result["contract_information"]["start_date_contract"] == "2023-01-01"
    assert result["pension_information"]["pension_premium_basic"] == "50% employee, 50% employer"
    
    print("✓ Non-salary schema test passed")


def test_merge_extraction_results():
    """Test that merge function works correctly."""
    # Sample salary data
    salary_extracted = [
        {
            "jobgroup": "Helper A",
            "salary_1": "2200",
            "salary_1_unit": "monthly",
            "salary_1_startdate": "2023-01-01",
            "salary_increment_1": "2%"
        }
    ]
    
    # Sample non-salary data
    rest_extracted = {
        "contract_information": {
            "start_date_contract": "2023-01-01",
            "expiry_date_contract": "2024-12-31"
        },
        "pension_information": {
            "pension_premium_basic": "50% employee, 50% employer",
            "retire_age_basic": "67"
        }
    }
    
    # Merge results
    merged_results = merge_extraction_results(salary_extracted, rest_extracted)
    
    assert len(merged_results) > 0
    
    # Check that we have a wage row
    wage_rows = [r for r in merged_results if r.get('infotype') == 'Wage']
    assert len(wage_rows) == 1
    assert wage_rows[0]['jobgroup'] == 'Helper A'
    assert wage_rows[0]['salary_1'] == '2200'
    
    # Check that we have pension row
    pension_rows = [r for r in merged_results if r.get('infotype') == 'Pension']
    assert len(pension_rows) == 1
    assert pension_rows[0]['pension_premium_basic'] == '50% employee, 50% employer'
    
    print("✓ Merge extraction results test passed")


def test_token_limit_check():
    """Test token limit checking function."""
    from pipelines.p4_analysis import check_token_limit
    
    # Test with small text (should pass)
    small_text = "This is a small text"
    assert check_token_limit(small_text, "test.txt") == True
    
    # Test with large text (should fail)
    large_text = "x" * 4000000  # ~1M tokens
    assert check_token_limit(large_text, "test.txt") == False
    
    print("✓ Token limit check test passed")


if __name__ == "__main__":
    print("Running basic tests for p4_analysis.py...")
    
    test_salary_schema()
    test_nonsalary_schema()
    test_merge_extraction_results()
    test_token_limit_check()
    
    print("\n✓ All basic tests passed!")
