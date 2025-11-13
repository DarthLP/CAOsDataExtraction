"""
Excel Output Schema Definitions for CAO Data Extraction

This module contains the comprehensive column definitions and field mappings
used in the Excel creation pipeline. It automatically generates column lists
based on the Pydantic schemas to ensure schema changes are reflected in Excel output.

USAGE:
    from schema.excel_output_schema import (
        get_salary_columns, get_non_salary_columns, 
        flatten_amount_object, flatten_amount_range_object
    )
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import inspect

# Import the schemas
from .salary_schema import SalaryExtractionSchema, SalaryRow, SalaryPoint
from .non_salary_schema import (
    NonSalaryPart1, NonSalaryPart2, NonSalaryPart3,
    GeneralInfo, BonusesInfo, WageScalesInfo, PensionInfo, TerminationInfo,
    LeaveInfo, OvertimeInfo, TrainingInfo, HomeofficeInfo, ContractTypeInfo,
    SafetyInfo, ChildcareInfo, AIInfo, FringeBenefitsInfo
)

# CAO metadata columns that appear in both files
CAO_METADATA_COLUMNS = [
    'cao_number',
    'id', 
    'TTW',
    'ingangsdatum',
    'expiratiedatum',
    'datum_kennisgeving',
    'file_name'
]

def flatten_amount_object(amount_obj: Optional[Dict[str, Any]], prefix: str) -> Dict[str, Any]:
    """
    Flatten an Amount object into separate value and unit columns.
    
    Args:
        amount_obj: Amount object with 'value' and 'unit' keys
        prefix: Column name prefix (e.g., 'employee_contrib')
        
    Returns:
        Dict with flattened columns: {prefix + '_value': value, prefix + '_unit': unit}
    """
    if not amount_obj:
        return {f"{prefix}_value": None, f"{prefix}_unit": None}
    
    return {
        f"{prefix}_value": amount_obj.get('value'),
        f"{prefix}_unit": amount_obj.get('unit')
    }

def flatten_amount_range_object(amount_range_obj: Optional[Dict[str, Any]], prefix: str) -> Dict[str, Any]:
    """
    Flatten an AmountRange object into separate min, max, and unit columns.
    
    Args:
        amount_range_obj: AmountRange object with 'min', 'max', and 'unit' keys
        prefix: Column name prefix (e.g., 'employee_contrib')
        
    Returns:
        Dict with flattened columns: {prefix + '_min': min, prefix + '_max': max, prefix + '_unit': unit}
    """
    if not amount_range_obj:
        return {f"{prefix}_min": None, f"{prefix}_max": None, f"{prefix}_unit": None}
    
    return {
        f"{prefix}_min": amount_range_obj.get('min'),
        f"{prefix}_max": amount_range_obj.get('max'),
        f"{prefix}_unit": amount_range_obj.get('unit')
    }

def get_pydantic_fields(model_class: BaseModel) -> List[str]:
    """
    Extract field names from a Pydantic model class.
    
    Args:
        model_class: Pydantic model class
        
    Returns:
        List of field names
    """
    if hasattr(model_class, 'model_fields'):
        return list(model_class.model_fields.keys())
    elif hasattr(model_class, '__fields__'):
        return list(model_class.__fields__.keys())
    else:
        return []

def get_salary_columns(max_timeline_length: int = 3) -> List[str]:
    """
    Generate comprehensive column list for salary Excel output.
    
    Maps new schema field names to old Excel column names for backward compatibility.
    
    Args:
        max_timeline_length: Maximum number of timeline points across all salary data
        
    Returns:
        List of column names for salary Excel file (using old field names for backward compatibility)
    """
    columns = CAO_METADATA_COLUMNS.copy()
    
    # Add SalaryRow metadata fields - map new names to old names
    salary_row_fields = get_pydantic_fields(SalaryRow)
    # Remove 'timeline' as it will be handled separately
    salary_row_fields = [f for f in salary_row_fields if f != 'timeline']
    
    # Map new field names to old names for Excel columns
    field_mapping = {
        'step': 'step_label',
        'worker': 'worker_type'
    }
    
    mapped_fields = []
    for field in salary_row_fields:
        mapped_fields.append(field_mapping.get(field, field))
    
    columns.extend(mapped_fields)
    
    # Add timeline columns for each point (up to max_timeline_length)
    # Map new field names to old names, and include hours_basis_ft_week even though removed from schema
    salary_point_fields = get_pydantic_fields(SalaryPoint)
    
    # Map new field names to old names
    timeline_field_mapping = {
        'inc_pct': 'increase_percent',
        'holiday_incl': 'holiday_in_amount'
    }
    
    # Create list of timeline fields with old names
    timeline_fields = []
    for field in salary_point_fields:
        mapped_field = timeline_field_mapping.get(field, field)
        timeline_fields.append(mapped_field)
    
    # Add hours_basis_ft_week even though removed from schema (for backward compatibility)
    if 'hours_basis_ft_week' not in timeline_fields:
        timeline_fields.append('hours_basis_ft_week')
    
    for i in range(1, max_timeline_length + 1):
        for field in timeline_fields:
            columns.append(f"salary_{i}_{field}")
    
    return columns

def get_non_salary_columns() -> List[str]:
    """
    Generate comprehensive column list for non-salary Excel output.
    
    Returns:
        List of column names for non-salary Excel file
    """
    columns = CAO_METADATA_COLUMNS.copy()
    
    # Get all fields from the three non-salary parts
    part1_fields = get_pydantic_fields(NonSalaryPart1)
    part2_fields = get_pydantic_fields(NonSalaryPart2) 
    part3_fields = get_pydantic_fields(NonSalaryPart3)
    
    # Flatten nested structure - each part contains sub-models
    all_fields = []
    
    # Part 1: General, Bonuses, Wage Scales, Pension, Termination
    for field_name in part1_fields:
        if field_name == 'general_information':
            general_fields = get_pydantic_fields(GeneralInfo)
            all_fields.extend([f"general_{f}" for f in general_fields])
        elif field_name == 'bonuses_info':
            bonus_fields = get_pydantic_fields(BonusesInfo)
            all_fields.extend([f"bonus_{f}" for f in bonus_fields])
        elif field_name == 'wage_scales_info':
            wage_fields = get_pydantic_fields(WageScalesInfo)
            all_fields.extend([f"wage_{f}" for f in wage_fields])
        elif field_name == 'pension_information':
            pension_fields = get_pydantic_fields(PensionInfo)
            all_fields.extend([f"pension_{f}" for f in pension_fields])
        elif field_name == 'termination_information':
            term_fields = get_pydantic_fields(TerminationInfo)
            all_fields.extend([f"term_{f}" for f in term_fields])
    
    # Part 2: Leave, Overtime, Training
    for field_name in part2_fields:
        if field_name == 'leave_information':
            leave_fields = get_pydantic_fields(LeaveInfo)
            all_fields.extend([f"leave_{f}" for f in leave_fields])
        elif field_name == 'overtime_information':
            overtime_fields = get_pydantic_fields(OvertimeInfo)
            all_fields.extend([f"overtime_{f}" for f in overtime_fields])
        elif field_name == 'training_information':
            training_fields = get_pydantic_fields(TrainingInfo)
            all_fields.extend([f"training_{f}" for f in training_fields])
    
    # Part 3: Homeoffice, Contract Type, Safety, Childcare, AI, Fringe Benefits
    for field_name in part3_fields:
        if field_name == 'homeoffice_information':
            homeoffice_fields = get_pydantic_fields(HomeofficeInfo)
            all_fields.extend([f"homeoffice_{f}" for f in homeoffice_fields])
        elif field_name == 'contract_type_information':
            contract_fields = get_pydantic_fields(ContractTypeInfo)
            all_fields.extend([f"contract_{f}" for f in contract_fields])
        elif field_name == 'safety_information':
            safety_fields = get_pydantic_fields(SafetyInfo)
            all_fields.extend([f"safety_{f}" for f in safety_fields])
        elif field_name == 'childcare_information':
            childcare_fields = get_pydantic_fields(ChildcareInfo)
            all_fields.extend([f"childcare_{f}" for f in childcare_fields])
        elif field_name == 'ai_information':
            ai_fields = get_pydantic_fields(AIInfo)
            all_fields.extend([f"ai_{f}" for f in ai_fields])
        elif field_name == 'fringe_benefits_information':
            fringe_fields = get_pydantic_fields(FringeBenefitsInfo)
            all_fields.extend([f"fringe_{f}" for f in fringe_fields])
    
    columns.extend(all_fields)
    return columns

def flatten_salary_row(salary_row: Dict[str, Any], cao_metadata: Dict[str, str], max_timeline_length: int = 3) -> Dict[str, Any]:
    """
    Convert a SalaryRow to a single Excel row with timeline data spread across columns.
    
    Maps new schema field names to old Excel column names for backward compatibility:
    - step → step_label
    - worker → worker_type
    - inc_pct → increase_percent
    - holiday_incl → holiday_in_amount
    
    Args:
        salary_row: SalaryRow data from JSON (may use new or old field names)
        cao_metadata: CAO metadata (cao_number, id, TTW, etc.)
        max_timeline_length: Maximum number of timeline points to create columns for
        
    Returns:
        Excel row dictionary with timeline data in columns (using old field names)
    """
    row = cao_metadata.copy()
    timeline = salary_row.get('timeline', [])
    
    # Map new field names to old names for Excel output (backward compatibility)
    field_mapping = {
        'step': 'step_label',
        'worker': 'worker_type',
        'inc_pct': 'increase_percent',
        'holiday_incl': 'holiday_in_amount'
    }
    
    # Add SalaryRow metadata fields - map new names to old names for Excel
    # Check for both new and old field names for compatibility
    row['worker_type'] = salary_row.get('worker') or salary_row.get('worker_type')
    row['step_label'] = salary_row.get('step') or salary_row.get('step_label')
    row['jobgroup'] = salary_row.get('jobgroup')
    row['is_entry'] = salary_row.get('is_entry')
    row['age_group'] = salary_row.get('age_group')
    row['education'] = salary_row.get('education')
    row['ft_hours'] = salary_row.get('ft_hours')
    row['permanency'] = salary_row.get('permanency')
    row['hours_type'] = salary_row.get('hours_type')
    row['row_note'] = salary_row.get('row_note')
    
    # Add timeline data as numbered columns
    # Map new field names to old names, keep hours_basis_ft_week even though removed from schema
    timeline_field_mapping = {
        'inc_pct': 'increase_percent',
        'holiday_incl': 'holiday_in_amount'
    }
    
    # Timeline fields in Excel (old names for backward compatibility)
    timeline_fields = ['start_date', 'end_date', 'amount', 'unit', 'table_label', 
                      'increase_percent', 'holiday_in_amount', 'hours_basis_ft_week', 'note']
    
    for i in range(1, max_timeline_length + 1):
        for field in timeline_fields:
            col_name = f"salary_{i}_{field}"
            if i <= len(timeline):
                timeline_point = timeline[i-1]
                # Map new field names to old names
                if field == 'increase_percent':
                    value = timeline_point.get('inc_pct') or timeline_point.get('increase_percent')
                elif field == 'holiday_in_amount':
                    value = timeline_point.get('holiday_incl') or timeline_point.get('holiday_in_amount')
                elif field == 'hours_basis_ft_week':
                    # This field was removed from schema, always use None
                    value = None
                else:
                    value = timeline_point.get(field)
                row[col_name] = value
            else:
                row[col_name] = None  # Empty for timeline points beyond available data
    
    return row

def flatten_non_salary_data(non_salary_data: Dict[str, Any], cao_metadata: Dict[str, str]) -> Dict[str, Any]:
    """
    Convert merged non-salary data to Excel row format.
    
    Args:
        non_salary_data: Merged data from three non-salary folders
        cao_metadata: CAO metadata (cao_number, id, TTW, etc.)
        
    Returns:
        Excel row dictionary
    """
    row = cao_metadata.copy()
    
    # Helper function to flatten nested objects
    def flatten_nested(obj, prefix=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, dict):
                    # Handle Amount and AmountRange objects
                    if 'value' in value and 'unit' in value:
                        # Amount object: {value: X, unit: Y}
                        row[f"{new_key}_value"] = value.get('value')
                        row[f"{new_key}_unit"] = value.get('unit')
                    elif 'min' in value and 'max' in value and 'unit' in value:
                        # AmountRange object: {min: X, max: Y, unit: Z}
                        row[f"{new_key}_min"] = value.get('min')
                        row[f"{new_key}_max"] = value.get('max')
                        row[f"{new_key}_unit"] = value.get('unit')
                    elif 'value' in value:
                        # Amount object with only value (no unit)
                        row[f"{new_key}_value"] = value.get('value')
                        row[f"{new_key}_unit"] = None
                    elif 'min' in value and 'max' in value:
                        # AmountRange object with only min/max (no unit)
                        row[f"{new_key}_min"] = value.get('min')
                        row[f"{new_key}_max"] = value.get('max')
                        row[f"{new_key}_unit"] = None
                    else:
                        # Regular nested dict, continue flattening
                        flatten_nested(value, new_key)
                else:
                    row[new_key] = value
        else:
            row[prefix] = obj
    
    # Flatten all non-salary data
    for part_name, part_data in non_salary_data.items():
        if isinstance(part_data, dict):
            # Map part names to prefixes used in column generation
            prefix_map = {
                'general_information': 'general',
                'bonuses_info': 'bonus', 
                'wage_scales_info': 'wage',
                'pension_information': 'pension',
                'termination_information': 'term',
                'leave_information': 'leave',
                'overtime_information': 'overtime',
                'training_information': 'training',
                'homeoffice_information': 'homeoffice',
                'contract_type_information': 'contract',
                'safety_information': 'safety',
                'childcare_information': 'childcare',
                'ai_information': 'ai',
                'fringe_benefits_information': 'fringe'
            }
            prefix = prefix_map.get(part_name, part_name)
            flatten_nested(part_data, prefix)
        else:
            row[part_name] = part_data
    
    return row
