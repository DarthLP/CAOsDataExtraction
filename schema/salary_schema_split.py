"""
Split Salary Schema Definitions for CAO Data Extraction

This module contains split extraction schema definitions used for salary data extraction
when compact schema attempts (attempts 5-8) fail due to token limits. The split schema
uses the compact schema as base and is designed for extracting data in two halves,
ensuring jobgroups are not split across attempts.

USAGE:
    from schema.salary_schema_split import (
        SalaryPointSplit, SalaryRowSplit, SalaryExtractionSchemaSplit
    )
"""

from typing import List, Optional
from pydantic import BaseModel, Field

# Import compact schema components as base
from schema.salary_schema_compact import (
    SalaryPointCompact, SalaryRowCompact, SalaryExtractionSchemaCompact
)

# Use the same schema classes as compact (no changes needed for split)
# The splitting logic is handled in the prompts and extraction logic
SalaryPointSplit = SalaryPointCompact
SalaryRowSplit = SalaryRowCompact
SalaryExtractionSchemaSplit = SalaryExtractionSchemaCompact

