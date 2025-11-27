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

# Field abbreviation mappings for compact/split schemas
# Row level abbreviations: compact/split → regular schema
ROW_ABBREVIATION_MAP = {
    'jg': 'jobgroup',
    'st': 'step',
    'wr': 'worker',
    'ie': 'is_entry',
    'ag': 'age_group',
    'eu': 'education',
    'fh': 'ft_hours',
    'pe': 'permanency',
    'ht': 'hours_type',
    'hi': 'holiday_incl',
    'rn': 'row_note',
    'tl': 'timeline'
}

# Point level abbreviations: compact/split → regular schema
POINT_ABBREVIATION_MAP = {
    'sd': 'start_date',
    'ed': 'end_date',
    'am': 'amount',
    'un': 'unit',
    'ip': 'inc_pct',
    'hp': 'holiday_incl',
    'nt': 'note'
}

# Unit abbreviation translation: compact/split → full names
UNIT_ABBREVIATION_MAP = {
    'm': 'monthly',
    '4-w': '4-week',
    'w': 'weekly',
    'h': 'hourly',
    'a': 'annual'
}

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

def get_field_with_abbreviation(data: Dict[str, Any], field_name: str, abbreviation_map: Dict[str, str]) -> Any:
    """
    Get field value from data, checking both full name and abbreviation.
    
    Args:
        data: Dictionary to get value from
        field_name: Full field name (regular schema)
        abbreviation_map: Mapping from abbreviations to full names
        
    Returns:
        Field value or None if not found
    """
    # First try full name
    if field_name in data:
        return data[field_name]
    
    # Then try abbreviation (reverse lookup)
    for abbrev, full_name in abbreviation_map.items():
        if full_name == field_name and abbrev in data:
            return data[abbrev]
    
    return None

def translate_unit(unit: Optional[str]) -> Optional[str]:
    """
    Translate unit abbreviation to full name.
    
    Args:
        unit: Unit value (may be abbreviation or full name)
        
    Returns:
        Translated unit name, or original if not an abbreviation
    """
    if not unit:
        return unit
    
    return UNIT_ABBREVIATION_MAP.get(unit, unit)

def get_holiday_incl_value(timeline_point: Dict[str, Any], salary_row: Dict[str, Any]) -> Optional[bool]:
    """
    Get holiday_incl value with point level precedence, fallback to row level.
    
    Point level takes precedence. If point level value doesn't exist (None or missing),
    fallback to row level. Handles both compact/split abbreviations (hp, hi) and 
    regular schema (holiday_incl).
    
    Args:
        timeline_point: Timeline point dictionary (may have hp or holiday_incl)
        salary_row: Salary row dictionary (may have hi or holiday_incl)
        
    Returns:
        holiday_incl value (bool or None)
    """
    # Check point level first (point takes precedence)
    # Try compact abbreviation 'hp' first, then regular 'holiday_incl'
    point_value = timeline_point.get('hp')
    if point_value is None:
        point_value = timeline_point.get('holiday_incl')
    
    # If point level has a value (including False), return it
    if point_value is not None:
        return point_value
    
    # Fallback to row level (point level was None or missing)
    # Try compact abbreviation 'hi' first, then regular 'holiday_incl'
    row_value = salary_row.get('hi')
    if row_value is None:
        row_value = salary_row.get('holiday_incl')
    
    return row_value

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
    
    Handles both compact/split schema abbreviations and regular schema field names.
    Translates unit abbreviations to full names.
    Implements holiday_incl merging with point level precedence, fallback to row level.
    
    Args:
        salary_row: SalaryRow data from JSON (may use abbreviations or full field names)
        cao_metadata: CAO metadata (cao_number, id, TTW, etc.)
        max_timeline_length: Maximum number of timeline points to create columns for
        
    Returns:
        Excel row dictionary with timeline data in columns (using old field names)
    """
    row = cao_metadata.copy()
    
    # Get timeline - handle both nested timeline format and parallel arrays format (super compact schema)
    # Check if this is super compact schema format with parallel arrays
    if 'sd' in salary_row and 'am' in salary_row and 'un' in salary_row:
        # Super compact schema: convert parallel arrays to timeline structure
        sd_array = salary_row.get('sd', [])
        am_array = salary_row.get('am', [])
        un_value = salary_row.get('un')
        ip_array = salary_row.get('ip', [])  # Optional increase_percent array
        
        # Handle un field: can be single string or array
        if isinstance(un_value, str):
            # Single unit for all timeline points
            un_array = [un_value] * len(sd_array) if sd_array else []
        else:
            # Array of units
            un_array = un_value if isinstance(un_value, list) else []
        
        # Build timeline from parallel arrays
        timeline = []
        max_len = max(len(sd_array), len(am_array), len(un_array), len(ip_array) if ip_array else 0)
        for i in range(max_len):
            timeline_point = {
                'sd': sd_array[i] if i < len(sd_array) else None,
                'am': am_array[i] if i < len(am_array) else None,
                'un': un_array[i] if i < len(un_array) else None,
                'ip': ip_array[i] if ip_array and i < len(ip_array) else None  # Include increase_percent if present
            }
            timeline.append(timeline_point)
    else:
        # Regular/compact/split schema: nested timeline format
        timeline = salary_row.get('timeline') or salary_row.get('tl', [])
    
    # Add SalaryRow metadata fields - handle both abbreviations and full names
    # Map to old Excel column names for backward compatibility
    row['worker_type'] = get_field_with_abbreviation(salary_row, 'worker', ROW_ABBREVIATION_MAP) or salary_row.get('worker_type')
    row['step_label'] = get_field_with_abbreviation(salary_row, 'step', ROW_ABBREVIATION_MAP) or salary_row.get('step_label')
    row['jobgroup'] = get_field_with_abbreviation(salary_row, 'jobgroup', ROW_ABBREVIATION_MAP) or salary_row.get('jobgroup')
    row['is_entry'] = get_field_with_abbreviation(salary_row, 'is_entry', ROW_ABBREVIATION_MAP) or salary_row.get('is_entry')
    row['age_group'] = get_field_with_abbreviation(salary_row, 'age_group', ROW_ABBREVIATION_MAP) or salary_row.get('age_group')
    row['education'] = get_field_with_abbreviation(salary_row, 'education', ROW_ABBREVIATION_MAP) or salary_row.get('education')
    row['ft_hours'] = get_field_with_abbreviation(salary_row, 'ft_hours', ROW_ABBREVIATION_MAP) or salary_row.get('ft_hours')
    row['permanency'] = get_field_with_abbreviation(salary_row, 'permanency', ROW_ABBREVIATION_MAP) or salary_row.get('permanency')
    row['hours_type'] = get_field_with_abbreviation(salary_row, 'hours_type', ROW_ABBREVIATION_MAP) or salary_row.get('hours_type')
    row['row_note'] = get_field_with_abbreviation(salary_row, 'row_note', ROW_ABBREVIATION_MAP) or salary_row.get('row_note')
    
    # Timeline fields in Excel (old names for backward compatibility)
    timeline_fields = ['start_date', 'end_date', 'amount', 'unit', 'table_label', 
                      'increase_percent', 'holiday_in_amount', 'hours_basis_ft_week', 'note']
    
    for i in range(1, max_timeline_length + 1):
        for field in timeline_fields:
            col_name = f"salary_{i}_{field}"
            if i <= len(timeline):
                timeline_point = timeline[i-1]
                
                # Handle each field type
                if field == 'increase_percent':
                    # Handle inc_pct with abbreviations
                    # Check for 'ip' (super compact parallel array), 'inc_pct' (compact/split), or 'increase_percent' (regular)
                    value = timeline_point.get('ip')  # Super compact schema uses 'ip' in parallel arrays
                    if value is None:
                        value = get_field_with_abbreviation(timeline_point, 'inc_pct', POINT_ABBREVIATION_MAP)
                    if value is None:
                        value = timeline_point.get('increase_percent')
                        
                elif field == 'holiday_in_amount':
                    # Use helper function for holiday_incl with point/row precedence
                    value = get_holiday_incl_value(timeline_point, salary_row)
                    
                elif field == 'unit':
                    # Get unit value and translate abbreviation
                    unit_value = get_field_with_abbreviation(timeline_point, 'unit', POINT_ABBREVIATION_MAP)
                    if unit_value is None:
                        unit_value = timeline_point.get('unit')
                    value = translate_unit(unit_value)
                    
                elif field == 'hours_basis_ft_week':
                    # This field was removed from schema, always use None
                    value = None
                    
                else:
                    # Handle other fields with abbreviations
                    value = get_field_with_abbreviation(timeline_point, field, POINT_ABBREVIATION_MAP)
                    if value is None:
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
