"""
CAO Data Extraction Script with Structured Output (Markdown Version)
================================================

DESCRIPTION:
This script extracts raw text information from Dutch Collective Labor Agreement (CAO) markdown documents
using Google's Gemini AI with context-preserving extraction. It processes markdown files directly
and returns JSON data organized into broad thematic categories as complete text snippets.

FEATURES:
- Direct markdown upload to Gemini API for optimal accuracy
- Context-preserving extraction (keeps related information together)
- Multi-process support for parallel processing
- Robust error handling with exponential backoff
- Adaptive retry strategy with parameter adjustment (temp/top_p/top_k on attempts 4-5)
- Failure-aware retry guidance for LLM-controllable errors (truncated JSON, empty responses)
- Markdown quality validation and best practices enforcement
- Dynamic timeouts based on file size
- File locking to prevent duplicate processing
- Enhanced JSON validation and fallback mechanisms

USAGE:
    Single Process:
        python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 1
        
    Multi-Process (2 parallel processes):
        python pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 2
        python pipelines/p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 2

    Bash script for parallel execution:
        caffeinate python -u pipelines/p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 6 > p3_log1.txt 2>&1 &
        caffeinate python -u pipelines/p3_llmExtraction.py --key_number 2 --process_id 1 --total_processes 6 > p3_log2.txt 2>&1 &
        caffeinate python -u pipelines/p3_llmExtraction.py --key_number 3 --process_id 2 --total_processes 6 > p3_log3.txt 2>&1 &
        caffeinate python -u pipelines/p3_llmExtraction.py --key_number 4 --process_id 3 --total_processes 6 > p3_log4.txt 2>&1 &
        caffeinate python -u pipelines/p3_llmExtraction.py --key_number 5 --process_id 4 --total_processes 6 > p3_log5.txt 2>&1 &
        caffeinate python -u pipelines/p3_llmExtraction.py --key_number 6 --process_id 5 --total_processes 6 > p3_log6.txt 2>&1 &

    With file limit:
        python p3_llmExtraction.py --key_number 1 --process_id 0 --total_processes 1 --max_files 10

ARGUMENTS:
    --key_number: Which API key to use (1, 2, 3, etc.) - defaults to 1
    --process_id: Process ID for work distribution (0-based) - defaults to 0
    --total_processes: Total number of parallel processes - defaults to 1
    --max_files: Maximum number of files to process (optional)

ENVIRONMENT VARIABLES:
    GOOGLE_API_KEY1, GOOGLE_API_KEY2, etc.: Google Gemini API keys

INPUT:
    - Markdown files in {config['paths']['parsed_pdfs_markdown']}/[CAO_NUMBER]/ folders

OUTPUT:
    - Extracted JSON data in outputs_json/new_flow/[CAO_NUMBER]/ folders
    - Error logs: outputs/logs/failed_files_llm_extraction.txt, outputs/logs/structured_output_parsing_errors.txt
"""

# =============================================================================
# IMPORTS
# =============================================================================
# Standard library imports for file operations, system access, and data handling
import os
import sys
import json
import time
import argparse
import threading
import signal
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field

# Add the parent directory to Python path so we can import monitoring
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Third-party imports for environment variables, file locking, and data validation
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ConfigDict
import yaml
from monitoring.monitoring_3_1 import PerformanceMonitor

# Google Gemini API imports
from google import genai
from google.genai import types


# =============================================================================
# SIGNAL HANDLING
# =============================================================================
# Handle signals gracefully to prevent unexpected exits when running with pipes/tee
def setup_signal_handlers():
    """Setup signal handlers to prevent unexpected process termination."""
    def handle_sigpipe(signum, frame):
        """Handle SIGPIPE (broken pipe) gracefully - ignore it to prevent exit code 1."""
        # When writing to a pipe (e.g., tee) that closes, we get SIGPIPE
        # Instead of crashing, we'll just ignore it and continue
        pass
    
    def handle_sigterm(signum, frame):
        """Handle SIGTERM gracefully - exit with code 0."""
        print('\n⚠️  Received SIGTERM, exiting gracefully...')
        sys.stdout.flush()
        sys.stderr.flush()
        sys.exit(0)
    
    # Register signal handlers
    # SIGPIPE: Ignore broken pipes (common when using tee/unbuffer)
    try:
        signal.signal(signal.SIGPIPE, handle_sigpipe)
    except (AttributeError, ValueError):
        # SIGPIPE may not be available on all platforms (Windows)
        pass
    
    # SIGTERM: Handle termination gracefully
    try:
        signal.signal(signal.SIGTERM, handle_sigterm)
    except (AttributeError, ValueError):
        pass


# =============================================================================
# DATA SCHEMAS
# =============================================================================
# Pydantic schema for structured extraction of CAO document information
class CAOExtractionSchema(BaseModel):
    """Schema for extracting structured data from Dutch CAO documents."""
    general_information: List[List[str]] = Field(description=
        """Extract the following basic CAO contract information if present in the CAO: 
        - start, end and signing date, 
        - retroactive application (applies?, period, scope, exclusions, back-pay terms, interest/surcharge), 
        - scope type of the CAO itself (sectoral, firm, group, niche),
        - company name and scope if single-firm,  
        - SBI codes and version that define the scope of this CAO, 
        - whether deviations are explicitly allowed at company-agreement level (yes/no, with topics if mentioned), 
        - AVV status (algemeen verbindend verklaard) with start and end dates if specified."""
        , default_factory=list)
    wage_information: List[List[str]] = Field(description=
        """Extract wage and salary information explicitly stated in the CAO: 
        - all wage tables (that are not identical except for unit conversion) and salary scales - make sure to check the entire document, also the appendices, 
        - job classifications, function groups, and pay groups/grades,
        - age-related or service-year/experience-based pay steps (trede/periodieken), including any transitions between age bands and experience steps,
        - rules governing progression within scales (e.g., annual increments, step frequency, performance-based step changes, freeze/unfreeze conditions),
        - entry-placement rules (e.g., starting above step 0 for prior relevant experience or competence),
        - personal allowances at the maximum of a scale (“persoonlijke toeslag”) and their conditions (basis, %/amount, pensionability, duration, phase-out),
        - general wage increases (periodic percentage or nominal increases applied sector-wide or by group),
        - all bonuses and allowances, including sign-on, 13th month, fixed lump sums, profit-sharing, performance bonuses, seniority/loyalty or jubilee bonuses, job-specific allowances, retirement gratuities, and insurance/savings benefits,
        - notes explaining how the wage system or tables operate (e.g., “scale applies to 36-h week”, “wages include 8% holiday allowance”, “conversion rules for youth to adult wage scale”).
        SKIP: tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4 weeks for same data); 
        KEEP: tables or rules that differ by period, worker type, education level, job group/function scale, experience steps (periodieken/trede), age bands, or other substantive distinctions."""
        , default_factory=list)
    pension_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated pension scheme information, including when present: 
        - type of scheme (DB, DC, hybrid), 
        - contribution percentages, employer/employee splits and premiums, 
        - accrual rules and franchise values, 
        - retirement ages, 
        - eligibility rules and accrual during leave/illness, 
        - special provisions (e.g. excedentregeling, premium change rules, group differences), 
        - pension fund name/abbreviation,
        - any other pension-related information mentioned.
        If the CAO explicitly states there is no occupational pension beyond AOW, note: 'no occupational pension (AOW only)'."""
        , default_factory=list)
    leave_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated leave information and conditions, including when present:
        - vacation entitlement and holiday allowance, 
        - maternity and paternity/partner leave, 
        - adoption and parental leave, 
        - sick leave and care leave (short- and long-term), 
        - special leaves including Liberation Day policy and senior days,
        - any other leave-related information mentioned and conditions."""
        , default_factory=list)
    termination_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated termination rules, including when present: 
        - notice periods for employers and employees (include full tables by age/service year if provided), 
        - rules on shortening, floors, and approval paths (UWV/judge), 
        - dismissal protections and conditions (e.g. during sickness), 
        - automatic end of employment at AOW age or other exit conditions, 
        - probation periods and maximum durations, 
        - severance pay and WW supplements beyond statutory transition pay,
        - any other termination-related information mentioned."""        
        , default_factory=list)
    overtime_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about overtime, shift and atypical hours, including when present:
        - overtime thresholds and compensation rules,
        - overtime rates and surcharges (including how they are applied or stacked),
        - shift, night, weekend, and holiday allowances,
        - rest periods and weekends-off guarantees,
        - maximum working hours or limits on compulsory overtime,
        - any other overtime/shift/atypical hours-related information mentioned (e.g. TOIL)."""
        , default_factory=list)
    training_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about training, including when present:
        - paid study and training time, budgets and reimbursements,
        - career scans or employability assessments,
        - sectoral/CAO training funds,
        - employer obligations for mandatory training,
        - reclaim clauses if employees leave,
        - any other training-related information mentioned."""
        , default_factory=list)
    homeoffice_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about home office and remote work, including when present:
        - entitlement to work from home (time and conditions),
        - fixed allowances or reimbursements (e.g. stipend, internet/phone, equipment),
        - decision rules or agreements required,
        - health and safety obligations at home,
        - travel time compensation for home-worksite arrangements,
        - any other home office/remote work-related information mentioned."""
        , default_factory=list)
    contract_type_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information and conditions about contract types, including when present:
        - rules on full-time and part-time work (standard hours, ranges, conditions),
        - provisions on min-max or zero-hour/on-call contracts,
        - deviations from the statutory fixed-term chain (ketenregeling),
        - rights to convert temporary to permanent contracts,
        - rights to adjust working hours,
        - any other information and rules on contract forms (e.g., freelance, internships)."""
        , default_factory=list)
    childcare_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated childcare information in the CAO, including when present:
        - allowances or subsidies,
        - in-house or employer/sector-arranged childcare,
        - discounts or priority access,
        - age limits and scope (e.g., after-school care),
        - provider rules and interaction with public benefits,
        - eligibility conditions (e.g. tenure, FTE),
        - sector fund financing,
        - other childcare-related provisions."""
        , default_factory=list)
    safety_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated safety and integrity provisions, including when present:
        - harassment, bullying, discrimination, aggression, or integrity protocols,
        - prevention of psychosocial risks (PSA) such as stress, burnout, or workload pressure,
        - RI&E requirements covering PSA,
        - confidential counsellors or internal/external contact points,
        - training or awareness on wellbeing and respectful behaviour,
        - reporting procedures and follow-up in PSA/integrity cases,
        - joint safety/health committees or sectoral Arbo arrangements,
        - mandatory safety or risk-prevention training,
        - other safety, health, or integrity-related measures or obligations."""
        , default_factory=list)
    AI_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about AI and algorithmic management, including when present:
        - rules on automated decisions and human review,
        - transparency, disclosure and audit obligations,
        - governance bodies or committees,
        - worker rights to contest AI-based decisions,
        - training or upskilling provisions related to AI,
        - any other AI and algorithmic management-related information mentioned."""
        , default_factory=list)
    fringe_benefits_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated fringe benefits, including when present:
        - commuting or travel allowances,
        - bicycle/leasefiets or mobility schemes,
        - meal benefits (meals, vouchers, allowances, canteen subsidies),
        - health insurance contributions or discounts,
        - relocation or housing allowances,
        - costs of mandatory certifications,
        - other non-cash benefits (e.g., wellbeing, gym, ergonomics)."""
        , default_factory=list)
    model_config = ConfigDict(title='CAO Extraction Schema',
        json_schema_extra={'propertyOrdering': ['general_information',
        'wage_information', 'pension_information', 'leave_information',
        'termination_information', 'overtime_information',
        'training_information', 'homeoffice_information',
        'contract_type_information', 'safety_information',
        'childcare_information', 'AI_information', 'fringe_benefits_information']})


# Split schemas for retries 6-8 (salary and non-salary extraction)
class CAOSalaryOnlySchema(BaseModel):
    """Schema for extracting only wage/salary information from Dutch CAO documents."""
    wage_information: List[List[str]] = Field(description=
        """Extract wage and salary information explicitly stated in the CAO: 
        - all wage tables (that are not identical except for unit conversion) and salary scales, 
        - job classifications, function groups, and pay groups/grades,
        - age-related or service-year/experience-based pay steps (trede/periodieken), including any transitions between age bands and experience steps,
        - rules governing progression within scales (e.g., annual increments, step frequency, performance-based step changes, freeze/unfreeze conditions),
        - entry-placement rules (e.g., starting above step 0 for prior relevant experience or competence),
        - personal allowances at the maximum of a scale ("persoonlijke toeslag") and their conditions (basis, %/amount, pensionability, duration, phase-out),
        - general wage increases (periodic percentage or nominal increases applied sector-wide or by group),
        - all bonuses and allowances, including sign-on, 13th month, fixed lump sums, profit-sharing, performance bonuses, seniority/loyalty or jubilee bonuses, job-specific allowances, retirement gratuities, and insurance/savings benefits,
        - notes explaining how the wage system or tables operate (e.g., "scale applies to 36-h week", "wages include 8% holiday allowance", "conversion rules for youth to adult wage scale").
        SKIP: tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4 weeks for same data); 
        KEEP: tables or rules that differ by period, worker type, education level, job group/function scale, experience steps (periodieken/trede), age bands, or other substantive distinctions."""
        , default_factory=list)
    model_config = ConfigDict(title='CAO Salary Only Schema')


class CAONonSalarySchema(BaseModel):
    """Schema for extracting all non-salary information from Dutch CAO documents."""
    general_information: List[List[str]] = Field(description=
        """Extract the following basic CAO contract information if present in the CAO: 
        - start, end and signing date, 
        - retroactive application (applies?, period, scope, exclusions, back-pay terms, interest/surcharge), 
        - scope type of the CAO itself (sectoral, firm, group, niche),
        - company name and scope if single-firm,  
        - SBI codes and version that define the scope of this CAO, 
        - whether deviations are explicitly allowed at company-agreement level (yes/no, with topics if mentioned), 
        - AVV status (algemeen verbindend verklaard) with start and end dates if specified."""
        , default_factory=list)
    pension_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated pension scheme information, including when present: 
        - type of scheme (DB, DC, hybrid), 
        - contribution percentages, employer/employee splits and premiums, 
        - accrual rules and franchise values, 
        - retirement ages, 
        - eligibility rules and accrual during leave/illness, 
        - special provisions (e.g. excedentregeling, premium change rules, group differences), 
        - pension fund name/abbreviation,
        - any other pension-related information mentioned.
        If the CAO explicitly states there is no occupational pension beyond AOW, note: 'no occupational pension (AOW only)'."""
        , default_factory=list)
    leave_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated leave information and conditions, including when present:
        - vacation entitlement and holiday allowance, 
        - maternity and paternity/partner leave, 
        - adoption and parental leave, 
        - sick leave and care leave (short- and long-term), 
        - special leaves including Liberation Day policy and senior days,
        - any other leave-related information mentioned and conditions."""
        , default_factory=list)
    termination_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated termination rules, including when present: 
        - notice periods for employers and employees (include full tables by age/service year if provided), 
        - rules on shortening, floors, and approval paths (UWV/judge), 
        - dismissal protections and conditions (e.g. during sickness), 
        - automatic end of employment at AOW age or other exit conditions, 
        - probation periods and maximum durations, 
        - severance pay and WW supplements beyond statutory transition pay,
        - any other termination-related information mentioned."""        
        , default_factory=list)
    overtime_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about overtime, shift and atypical hours, including when present:
        - overtime thresholds and compensation rules,
        - overtime rates and surcharges (including how they are applied or stacked),
        - shift, night, weekend, and holiday allowances,
        - rest periods and weekends-off guarantees,
        - maximum working hours or limits on compulsory overtime,
        - any other overtime/shift/atypical hours-related information mentioned (e.g. TOIL)."""
        , default_factory=list)
    training_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about training, including when present:
        - paid study and training time, budgets and reimbursements,
        - career scans or employability assessments,
        - sectoral/CAO training funds,
        - employer obligations for mandatory training,
        - reclaim clauses if employees leave,
        - any other training-related information mentioned."""
        , default_factory=list)
    homeoffice_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about home office and remote work, including when present:
        - entitlement to work from home (time and conditions),
        - fixed allowances or reimbursements (e.g. stipend, internet/phone, equipment),
        - decision rules or agreements required,
        - health and safety obligations at home,
        - travel time compensation for home-worksite arrangements,
        - any other home office/remote work-related information mentioned."""
        , default_factory=list)
    contract_type_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information and conditions about contract types, including when present:
        - rules on full-time and part-time work (standard hours, ranges, conditions),
        - provisions on min-max or zero-hour/on-call contracts,
        - deviations from the statutory fixed-term chain (ketenregeling),
        - rights to convert temporary to permanent contracts,
        - rights to adjust working hours,
        - any other information and rules on contract forms (e.g., freelance, internships)."""
        , default_factory=list)
    childcare_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated childcare information in the CAO, including when present:
        - allowances or subsidies,
        - in-house or employer/sector-arranged childcare,
        - discounts or priority access,
        - age limits and scope (e.g., after-school care),
        - provider rules and interaction with public benefits,
        - eligibility conditions (e.g. tenure, FTE),
        - sector fund financing,
        - other childcare-related provisions."""
        , default_factory=list)
    safety_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated safety and integrity provisions, including when present:
        - harassment, bullying, discrimination, aggression, or integrity protocols,
        - prevention of psychosocial risks (PSA) such as stress, burnout, or workload pressure,
        - RI&E requirements covering PSA,
        - confidential counsellors or internal/external contact points,
        - training or awareness on wellbeing and respectful behaviour,
        - reporting procedures and follow-up in PSA/integrity cases,
        - joint safety/health committees or sectoral Arbo arrangements,
        - mandatory safety or risk-prevention training,
        - other safety, health, or integrity-related measures or obligations."""
        , default_factory=list)
    AI_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated information about AI and algorithmic management, including when present:
        - rules on automated decisions and human review,
        - transparency, disclosure and audit obligations,
        - governance bodies or committees,
        - worker rights to contest AI-based decisions,
        - training or upskilling provisions related to AI,
        - any other AI and algorithmic management-related information mentioned."""
        , default_factory=list)
    fringe_benefits_information: List[List[str]] = Field(description=
        """Extract ALL explicitly stated fringe benefits, including when present:
        - commuting or travel allowances,
        - bicycle/leasefiets or mobility schemes,
        - meal benefits (meals, vouchers, allowances, canteen subsidies),
        - health insurance contributions or discounts,
        - relocation or housing allowances,
        - costs of mandatory certifications,
        - other non-cash benefits (e.g., wellbeing, gym, ergonomics)."""
        , default_factory=list)
    model_config = ConfigDict(title='CAO Non-Salary Schema',
        json_schema_extra={'propertyOrdering': ['general_information',
        'pension_information', 'leave_information',
        'termination_information', 'overtime_information',
        'training_information', 'homeoffice_information',
        'contract_type_information', 'safety_information',
        'childcare_information', 'AI_information', 'fringe_benefits_information']})


# =============================================================================
# GLOBAL STATE
# =============================================================================
# Process-specific quota flags to stop individual processes when daily quota is hit
process_quota_flags = {}

# Lock TTL for cleanup
LOCK_TTL_HOURS = 24


# =============================================================================
# DEBUG BUFFER CLASS
# =============================================================================
class DebugBuffer:
    """Buffer for debug messages that can be flushed on failure or streamed live."""
    def __init__(self, live: bool = False, max_lines: int = 1000):
        self.live = live
        self.max_lines = max_lines
        self.lines: list[str] = []
    
    def log(self, msg: str):
        if self.live:
            print(msg)
        elif len(self.lines) < self.max_lines:
            self.lines.append(msg)
    
    def flush(self):
        for line in self.lines:
            print(line)
        self.lines.clear()
    
    def clear(self):
        self.lines.clear()
    
    def enable_live(self):
        self.live = True
    
    def snapshot(self, last_n: int = 200) -> list[str]:
        return self.lines[-last_n:]

# =============================================================================
# CONFIGURATION CLASSES
# =============================================================================
# Data classes for managing extraction settings, statistics, and processing context
@dataclass
class ExtractionConfig:
    """Configuration class for extraction settings."""
    input_folder: str
    output_folder: Path
    max_files: int = 1000000
    max_processing_hours: int = 1
    sorted_files: bool = True
    model: str = 'gemini-2.5-flash'
    temperature: float = 0.0
    top_p: float = 0.1
    top_k: int = 1
    max_tokens: int = 65536
    candidate_count: int = 1
    seed: int = 42
    presence_penalty: float = 0
    frequency_penalty: float = 0
    thinking_budget: int = -1
    max_retries: int = 8
    delay_between_files: int = 150  # about 150 seconds between files to avoid rate limits


@dataclass
class ExtractionStats:
    """Statistics tracking for the extraction process."""
    processed_files: int = 0
    successful_extractions: int = 0
    failed_files: List[str] = field(default_factory=list)
    timed_out_files: List[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    
    def add_success(self, filename: str):
        """Add a successful extraction."""
        self.processed_files += 1
        self.successful_extractions += 1
    
    def add_failure(self, filename: str):
        """Add a failed extraction."""
        self.processed_files += 1
        self.failed_files.append(filename)
    
    def add_timeout(self, filename: str):
        """Add a timed out extraction."""
        self.timed_out_files.append(filename)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        elapsed_time = time.time() - self.start_time
        return {
            'processed_files': self.processed_files,
            'successful_extractions': self.successful_extractions,
            'failed_files': len(self.failed_files),
            'timed_out_files': len(self.timed_out_files),
            'elapsed_time': elapsed_time,
            'success_rate': self.successful_extractions / max(self.processed_files, 1)
        }


@dataclass
class ProcessingContext:
    """Context for the processing session."""
    config: ExtractionConfig
    process_id: int
    total_processes: int
    api_key: str
    key_number: int
    client: Any  # Gemini client
    performance_monitor: PerformanceMonitor
    stats: ExtractionStats = field(default_factory=ExtractionStats)
    debug: Any = None  # DebugBuffer
    current_stage: str = ""
    stage_start_ts: float = 0.0
    file_start_ts: float = 0.0
    live_escalated: bool = False


# =============================================================================
# CONFIGURATION & SETUP FUNCTIONS
# =============================================================================
# Functions for loading configuration, setting up environment, and initializing components
def load_configuration() -> ExtractionConfig:
    """Load and validate configuration from config.yaml."""
    with open('conf/config.yaml', 'r') as f:
        config_data = yaml.safe_load(f)
    
    return ExtractionConfig(
        input_folder=config_data['paths']['parsed_pdfs_markdown'],
        output_folder=Path(config_data['paths']['outputs_json']) / "new_flow"
    )


def setup_environment(key_number: int = 1) -> Tuple[str, int]:
    """Setup environment variables and API key."""
    load_dotenv()
    
    api_key = os.getenv(f'GOOGLE_API_KEY{key_number}')
    if not api_key:
        api_key = os.getenv('GOOGLE_API_KEY1')
        if not api_key:
            raise ValueError(
                f'Neither GOOGLE_API_KEY{key_number} nor GOOGLE_API_KEY1 environment variable found. '
                f'Please set at least GOOGLE_API_KEY1 before running this script.'
            )
        else:
            key_number = 1
            print(f'Warning: GOOGLE_API_KEY{key_number} not found, using GOOGLE_API_KEY1 instead')
    
    return api_key, key_number


def setup_gemini_client(api_key: str):
    """Setup Gemini client."""
    from google import genai
    return genai.Client(api_key=api_key)


def setup_performance_monitor() -> PerformanceMonitor:
    """Setup performance monitoring."""
    # Use a high limit (10000) since we're using paid API keys with token-based quota (3M tokens/day)
    # The actual quota is token-based, not request-based, so this is just for display purposes
    return PerformanceMonitor(
        log_file='performance_logs/llm_extraction/extraction_performance.jsonl',
        summary_file='performance_logs/llm_extraction/extraction_summary.json',
        free_tier_daily_limit=10000  # High limit for paid tier (actual limit is 3M tokens/day, not requests)
    )


def setup_processing_context(config: ExtractionConfig, process_id: int, 
                           total_processes: int, key_number: int, verbose: bool = False) -> ProcessingContext:
    """Setup complete processing context."""
    api_key, actual_key_number = setup_environment(key_number)
    client = setup_gemini_client(api_key)
    performance_monitor = setup_performance_monitor()
    debug_buffer = DebugBuffer(live=verbose)
    
    return ProcessingContext(
        config=config,
        process_id=process_id,
        total_processes=total_processes,
        api_key=api_key,
        key_number=actual_key_number,
        client=client,
        performance_monitor=performance_monitor,
        debug=debug_buffer
    )


def validate_input_paths(config: ExtractionConfig, process_id: int = 0):
    """Validate that input/output paths exist and are accessible."""
    if not os.path.exists(config.input_folder):
        raise ValueError(f"Input folder does not exist: {config.input_folder}")
    
    config.output_folder.mkdir(parents=True, exist_ok=True)
    
    # Check if we can write to output folder
    # Use process_id to avoid race conditions when running parallel processes
    test_file = config.output_folder / f".test_write_p{process_id}"
    try:
        test_file.write_text("test")
        if not test_file.exists():
            raise ValueError("Test file was not created")
    except Exception as e:
        raise ValueError(f"Cannot write to output folder: {config.output_folder}, Error: {e}")
    finally:
        # Clean up test file if it exists (do this separately from validation)
        if test_file.exists():
            try:
                test_file.unlink()
            except Exception:
                # Ignore cleanup errors - write permission was already validated
                pass


# =============================================================================
# FILE DISCOVERY & VALIDATION FUNCTIONS
# =============================================================================
# Functions for finding, filtering, and validating markdown files
def discover_markdown_files(input_folder: str) -> List[Tuple[Path, Path]]:
    """Discover all markdown files organized by CAO."""
    cao_folders = sorted([f for f in Path(input_folder).iterdir() 
                         if f.is_dir() and f.name.isdigit()], 
                        key=lambda f: int(f.name))
    
    all_files = []
    for cao_folder in cao_folders:
        markdown_files = sorted(cao_folder.glob('*.md'))
        for markdown_file in markdown_files:
            all_files.append((cao_folder, markdown_file))
    
    return all_files


def filter_files_for_processing(all_files: List[Tuple[Path, Path]], 
                              context: ProcessingContext) -> List[Tuple[Path, Path]]:
    """Filter files based on multi-process distribution."""
    filtered_files = []
    for file_idx, (cao_folder, markdown_file) in enumerate(all_files):
        if file_idx % context.total_processes == context.process_id:
            filtered_files.append((cao_folder, markdown_file))
    
    if not context.config.sorted_files:
        import random
        random.seed(42)
        random.shuffle(filtered_files)
    
    return filtered_files


def validate_markdown_file(markdown_path: str) -> Tuple[bool, str]:
    """Validate a single markdown file."""
    try:
        if not os.path.exists(markdown_path):
            return False, 'Markdown file does not exist'
        
        file_size = os.path.getsize(markdown_path)
        if file_size == 0:
            return False, 'Markdown file is empty'
        if file_size < 1024:
            return False, f'Markdown file too small ({file_size} bytes) - likely corrupted'
        if file_size > 50 * 1024 * 1024:
            return False, f'Markdown file too large ({file_size / (1024 * 1024):.1f}MB) - exceeds reasonable limit'
        if not markdown_path.endswith('.md'):
            return False, 'File does not have .md extension'
        
        return True, f'Markdown appears valid ({file_size / (1024 * 1024):.1f}MB)'
    except PermissionError:
        return False, 'Permission denied - cannot read markdown file'
    except Exception as e:
        return False, f'Error checking markdown: {str(e)}'


# =============================================================================
# HEARTBEAT WATCHDOG FUNCTIONS
# =============================================================================
# Functions for monitoring long-running stages and escalating debug output
def heartbeat_watchdog(context: ProcessingContext, hang_threshold: int, heartbeat_interval: int, stop_event: threading.Event):
    """Watchdog thread that monitors stage duration and escalates debug output on long stages."""
    while not stop_event.is_set():
        try:
            if context.current_stage and context.stage_start_ts > 0:
                elapsed = time.time() - context.stage_start_ts
                
                # Check if we should escalate to live debug
                if elapsed >= hang_threshold and not context.live_escalated:
                    print(f'[HEARTBEAT] long-running stage={context.current_stage} elapsed={elapsed:.0f}s — enabling live debug')
                    # Flush buffered debug once
                    for line in context.debug.snapshot():
                        print(line)
                    context.debug.enable_live()
                    context.live_escalated = True
                
                # Continue heartbeats if already escalated
                elif context.live_escalated:
                    file_elapsed = time.time() - context.file_start_ts
                    print(f'[HEARTBEAT] stage={context.current_stage} elapsed={elapsed:.0f}s (file {file_elapsed:.0f}s)')
            
            # Wait for next heartbeat or stop signal
            if stop_event.wait(heartbeat_interval):
                break
                
        except Exception as e:
            # Don't let watchdog errors crash the main process
            print(f'[HEARTBEAT] Watchdog error: {e}')
            break


# =============================================================================
# FILE LOCKING & MANAGEMENT FUNCTIONS
# =============================================================================
# Functions for file locking, cleanup, and result saving
def acquire_file_lock(file_path: Path, context: ProcessingContext) -> bool:
    """Try to acquire a lock for processing a file using atomic file creation."""
    lock_path = Path(str(file_path) + '.lock')
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, 'w') as f:
            f.write(f'pid={context.process_id} key={context.key_number} ts={time.time()}\n')
        return True
    except FileExistsError:
        return False


def release_file_lock(file_path: Path):
    """Release the lock for a file."""
    lock_path = Path(str(file_path) + '.lock')
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def announce_cao_once(cao_number: str, context: ProcessingContext) -> bool:
    """Announce a CAO number only once across all processes."""
    announce_file = context.config.output_folder / f'.cao_{cao_number}_announced'
    try:
        with open(announce_file, 'x') as f:
            f.write(f'Announced by process {context.process_id + 1}\n')
        print(f'--- CAO {cao_number} ---')
        return True
    except FileExistsError:
        return False


def save_extraction_result(output_path: Path, content: str):
    """Save extraction result to file with comprehensive JSON validation."""
    # print(f'  INFO: Saving structured output directly (length: {len(content)} chars)')
    
    # Comprehensive JSON validation before saving
    validation_result = validate_json_completeness(content, output_path.name)
    if not validation_result['is_valid']:
        print(f'  ❌ JSON validation failed for {output_path.name}: {validation_result["error"]}')
        print(f'  🗑️  Skipping save of incomplete JSON file')
        return False
    
    # Parse and reformat JSON for better structure
    try:
        data = json.loads(content)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f'  ✅ JSON saved with proper formatting')
        return True
    except json.JSONDecodeError as e:
        print(f'  ❌ JSON parsing failed, skipping save: {e}')
        return False


def validate_json_completeness(content: str, filename: str) -> dict:
    """
    Comprehensive JSON validation to ensure completeness and validity.
    
    Returns:
        dict: {'is_valid': bool, 'error': str or None}
    """
    # Check if content is empty
    if not content or not content.strip():
        return {'is_valid': False, 'error': 'Empty content'}
    
    # Check if content starts with {
    if not content.strip().startswith('{'):
        return {'is_valid': False, 'error': 'Content does not start with {'}
    
    # Check if content ends with }
    if not content.strip().endswith('}'):
        return {'is_valid': False, 'error': 'Content does not end with } - JSON appears to be truncated'}
    
    # Try to parse JSON to validate structure
    try:
        json.loads(content)
        return {'is_valid': True, 'error': None}
    except json.JSONDecodeError as e:
        return {'is_valid': False, 'error': f'JSON parsing error: {str(e)}'}


def cleanup_uploaded_file(client, uploaded_file):
    """Clean up uploaded file from Gemini."""
    try:
        client.files.delete(name=uploaded_file.name)
        print(f'  INFO: Cleaned up uploaded file: {uploaded_file.name}')
    except Exception as e:
        print(f'  WARNING: Failed to clean up file {uploaded_file.name}: {e}')


# =============================================================================
# LLM RESPONSE VALIDATION & ERROR HANDLING
# =============================================================================
# Functions for validating LLM responses and handling various error types
def calculate_quota_retry_delay(file_size_mb: float, attempt: int) -> int:
    """
    Calculate quota retry delay based on file size and attempt number.
    
    Formula: (estimated_tokens / 125000) * 60 seconds * (2^attempt) + buffer
    - 125,000 tokens per minute limit
    - Exponential backoff: 2.1^attempt (capped at attempt 4 for steady delays after retry 5)
    - Buffer time for safety
    """
    # Estimate tokens: roughly 4 chars per token, file_size_mb * 1024 * 1024 / 4
    estimated_tokens = int(file_size_mb * 1024 * 1024 / 4)
    
    # Calculate minutes needed to process this file
    minutes_needed = estimated_tokens / 125000
    
    # Add exponential backoff: 2.1^attempt (capped at attempt 4 - keep steady after retry 5)
    backoff_multiplier = 2.1 ** min(attempt, 4)
    
    # Add buffer time (2-3 minutes for safety)
    buffer_minutes = 2 + min(attempt, 4)
    
    # Calculate total delay in seconds
    total_delay_seconds = int((minutes_needed * backoff_multiplier + buffer_minutes) * 60)
    
    # Debug logging moved to context.debug.log() calls in calling functions
    
    return total_delay_seconds


def get_model_parameters(config) -> Dict[str, Any]:
    """Get model parameters for logging."""
    return {
        "model": config.model,
        "temperature": config.temperature,
        "top_p": config.top_p,
        "top_k": config.top_k,
        "max_tokens": config.max_tokens,
        "candidate_count": config.candidate_count,
        "seed": config.seed,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "thinking_budget": config.thinking_budget,
        "max_retries": config.max_retries
    }


def get_adjusted_parameters(config, attempt: int) -> Dict[str, Any]:
    """
    Get adjusted model parameters based on retry attempt.
    
    - Attempts 0-2 (1st-3rd tries): original parameters
    - Attempt 3 (4th try): temperature +0.1, top_p +0.1, top_k -0.1
    - Attempt 4 (5th try): temperature +0.2, top_p +0.2, top_k -0.2
    - Attempt 5 (6th try, split extraction): same as attempt 0 → original parameters
    - Attempt 6 (7th try, split extraction): same as attempt 2 → original parameters
    - Attempt 7 (8th try, split extraction): same as attempt 3 → +0.1 adjustment
    
    Returns:
        dict: Model parameters with attempt-based adjustments
    """
    if attempt <= 2:
        # First 3 attempts: use original parameters
        adjustment = 0.0
    elif attempt == 3:
        # 4th attempt: +0.1 adjustment
        adjustment = 0.1
    elif attempt == 4:
        # 5th attempt: +0.2 adjustment
        adjustment = 0.2
    elif attempt == 5:
        # 6th attempt (split extraction): same as attempt 0 → original parameters
        adjustment = 0.0
    elif attempt == 6:
        # 7th attempt (split extraction): same as attempt 2 → original parameters
        adjustment = 0.0
    elif attempt == 7:
        # 8th attempt (split extraction): same as attempt 3 → +0.1 adjustment
        adjustment = 0.1
    else:
        # Fallback for any higher attempts (shouldn't happen with max_retries=8)
        adjustment = 0.2
    
    # Calculate adjusted values
    adjusted_temp = config.temperature + adjustment
    adjusted_top_p = min(1.0, config.top_p + adjustment)  # Cap at 1.0
    adjusted_top_k = max(1, int(config.top_k - adjustment * config.top_k))  # Reduce by percentage, min 1
    
    return {
        "model": config.model,
        "temperature": adjusted_temp,
        "top_p": adjusted_top_p,
        "top_k": adjusted_top_k,
        "max_tokens": config.max_tokens,
        "candidate_count": config.candidate_count,
        "seed": config.seed,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "thinking_budget": config.thinking_budget,
        "max_retries": config.max_retries
    }


def get_retry_guidance(error_message: str) -> tuple[str, str]:
    """
    Get retry guidance based on previous failure for LLM-controllable errors.
    
    Only provides guidance for the 2 most common LLM-controllable errors:
    1. Truncated JSON (incomplete response)
    2. Empty/no text response
    
    For all other errors (timeouts, 504, etc.), returns empty string.
    
    Args:
        error_message: The error message from the previous attempt
        
    Returns:
        tuple: (guidance_text, error_type) where error_type is "" if no guidance
    """
    if not error_message:
        return "", ""
    
    error_lower = error_message.lower()
    
    # Check for truncated JSON error
    if "does not end with }" in error_lower or "truncated" in error_lower:
        guidance = """
    PREVIOUS ATTEMPT FAILED: Response was TRUNCATED (incomplete JSON).
    CRITICAL: Ensure your response ENDS with the closing }  
        - Be more CONCISE in narrative descriptions while keeping all important data intact!
        - Prioritize completing the JSON structure over verbose explanations
        - Keep all numbers, dates, tables - compress only explanatory text!
    """
        return guidance, "truncated JSON"
    
    # Check for empty response error
    if "no text parts" in error_lower or "no content" in error_lower:
        guidance = """
    PREVIOUS ATTEMPT FAILED: No valid output was generated.
    CRITICAL: Output ONLY the JSON object
        - No markdown code fences (no ```json)!
        - Include ALL required fields (use empty [] if no data)!
        - Ensure final JSON output is generated, not just thinking tokens!
    """
        return guidance, "empty response"
    
    # For all other errors, return empty string (no guidance)
    return "", ""


def create_extraction_prompt(filename: str) -> str:
    """Create the extraction prompt for CAO document processing."""
    return f"""
    Extract information from the Dutch CAO Markdown document parsed from a PDF. Pages are marked ## Page X _native_ (text-based) or ## Page X  _OCR_ (image-based); use these labels to interpret the layout correctly.

    GOAL: Produce one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, each mapping to a List[List[str]]. If nothing is found for a key, return an empty list.

    THINKING & OUTPUT: Think step by step INTERNALLY to locate, route, and clean the data, but OUTPUT ONLY the final JSON (no explanations, no notes, no chain-of-thought).
        
    CRITICAL RULES:
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information. Always VERIFY the extracted information with the document.
        - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
        - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
        - IMPORTANT: Translate all Dutch text (clauses, tables, titles, etc.) into clear and precise English but keep names in Dutch. For legal clauses, preserve the exact legal meaning without simplification.
        - IMPORTANT: Check the appendix if it exists in the document. Salary tables and important information are sometimes located in appendices and should not be skipped.
    
    INSIDE SECTION RULES: Order does not matter within a section - keep related items together (e.g., a table followed by its note/explanation).
    
    ROUTING RULES (Use to avoid duplicates across sections):
        - Wage vs Overtime: atypical hours pay → overtime_information; structural bonuses → wage_information.  
        - Wage vs Fringe: cash → wage_information; non-cash perks/reimbursements → fringe_benefits_information.  
        - Homeoffice vs Fringe: WFH-specific (stipend, equipment, internet) → homeoffice_information; general perks → fringe_benefits_information.  
        - Childcare vs Leave: time off/pay during leave → leave_information; childcare services/subsidies/discounts → childcare_information.  
        - Training vs Safety vs AI: safety/Arbo training → safety_information; AI-related training → AI_information; all other training → training_information.  
        - Safety vs Homeoffice: safety/Arbo for home working → homeoffice_information; general safety/integrity → safety_information.  
        - Pension vs Wage vs Fringe: pension schemes/funds → pension_information; wages/bonuses → wage_information; non-pension perks → fringe_benefits_information.  
        - Contract vs Termination: contract forms/ketenregeling/conversion → contract_type_information; notice/dismissal/severance/WW supplements → termination_information.  
        - Holidays: pay/allowance for working on holidays → overtime_information; days off/policies → leave_information.  
    
    WHAT TO INCLUDE:
        - Numbers, amounts, percentages, dates, periods, conditions, eligibility rules, procedures, entitlements, allowances.
        - Tables: include a compact structure (see TABLE FORMAT) with headers and all data rows and columns plus any short note that explains the table.

    WAGE TABLE DEDUP (IMPORTANT):
        - Keep tables that differ by period, worker type, education level, job group/function scale, experience steps (periodieken/trede), age bands, or other substantive distinctions.
        - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present).

    TABLE FORMAT:
        - Represent each table as a short list of strings like:
            [
                "Table title with context and units",
                "Columns: <Row label (if present)> | <Col 1 label> | <Col 2 label> | <Col 3 label> | ...",
                "<Row 1 label (if present)> | <v1> | <v2> | <v3> | ... ",
                "<Row 2 label (if present)> | <v1> | <v2> | <v3> | ... ",
                "... (one string per row)",
                "Additional notes or clarifying information if any"
            ]

    EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT):
        1) Read & anchor: read all instructions and section descriptions.
        2) Sweep & mark: scan the whole document including any appendices; mark every clause/table/text that matches any section description; ignore text/sentences/clasues/passages that matches none.
        3) Route: apply the ROUTING RULES to decide the correct section when overlaps occur.
        4) Length pre-check: if the marked set, when extracted verbatim, would likely exceed ~262,144 characters, plan to trim narrative/boilerplate in step 5.
        5) Extract, translate & build — DO NOT HALLUCINATE:
            - Build one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, in this order: general_information → wage_information → pension_information → leave_information → termination_information → overtime_information → training_information → homeoffice_information → contract_type_information → safety_information → childcare_information → AI_information → fringe_benefits_information.
            - COPY numbers/dates/%/names literally; TRANSLATE all other Dutch text (clauses, part of tables that are not numbers or names, titles, etc.) to clear English; leave blank if not stated.
            - Tables: rebuild to TABLE FORMAT; apply WAGE TABLE DEDUP (remove pure unit-conversion duplicates; prefer monthly).
            - Consolidate: keep related items adjacent.
            - If trimming per Step 4 is needed, shorten only narrative notes or minor wording not directly tied to field content, without changing legal meaning.
        6) Verify: confirm that every extracted fact, number, table, or clause is explicitly present in the document (allowing for shortening, restructuring, and translation to English) and that no important information is missing. Correct or remove anything not grounded in the source. Do not infer or guess.
        7) Validate: output one JSON object only; UTF-8 only; valid JSON (balanced brackets, no trailing commas); all template keys present (empty list if none).

    JSON OUTPUT REQUIREMENTS:
        - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
        - Ensure brackets/commas are correct; no trailing commas; all top-level keys present.

    OUTPUT_JSON_TEMPLATE:
        {{
            "general_information": [],
            "wage_information": [],
            "pension_information": [],
            "leave_information": [],
            "termination_information": [],
            "overtime_information": [],
            "training_information": [],
            "homeoffice_information": [],
            "contract_type_information": [],
            "safety_information": [],
            "childcare_information": [],
            "AI_information": [],
            "fringe_benefits_information": []
        }}

    Document: {filename}
    """


def create_salary_extraction_prompt(filename: str) -> str:
    """Create the extraction prompt for salary/wage information only."""
    return f"""
    Extract ONLY wage and salary information from the Dutch CAO Markdown document parsed from a PDF. Pages are marked ## Page X _native_ (text-based) or ## Page X  _OCR_ (image-based); use these labels to interpret the layout correctly.

    GOAL: Produce one JSON object with ONLY the "wage_information" key mapping to a List[List[str]]. If nothing is found, return an empty list.

    THINKING & OUTPUT: Think step by step INTERNALLY to locate and clean the data, but OUTPUT ONLY the final JSON (no explanations, no notes, no chain-of-thought).
        
    CRITICAL RULES:
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information. Always VERIFY the extracted information with the document.
        - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
        - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
        - IMPORTANT: Translate all Dutch text (clauses, tables, titles, etc.) into clear and precise English but keep names in Dutch. For legal clauses, preserve the exact legal meaning without simplification.
        - IMPORTANT: Check the appendix if it exists in the document. Salary tables and important information are sometimes located in appendices and should not be skipped.
    
    WHAT TO INCLUDE:
        - all wage tables (that are not identical except for unit conversion) and salary scales, 
        - job classifications, function groups, and pay groups/grades,
        - age-related or service-year/experience-based pay steps (trede/periodieken), including any transitions between age bands and experience steps,
        - rules governing progression within scales (e.g., annual increments, step frequency, performance-based step changes, freeze/unfreeze conditions),
        - entry-placement rules (e.g., starting above step 0 for prior relevant experience or competence),
        - personal allowances at the maximum of a scale ("persoonlijke toeslag") and their conditions (basis, %/amount, pensionability, duration, phase-out),
        - general wage increases (periodic percentage or nominal increases applied sector-wide or by group),
        - all bonuses and allowances, including sign-on, 13th month, fixed lump sums, profit-sharing, performance bonuses, seniority/loyalty or jubilee bonuses, job-specific allowances, retirement gratuities, and insurance/savings benefits,
        - notes explaining how the wage system or tables operate (e.g., "scale applies to 36-h week", "wages include 8% holiday allowance", "conversion rules for youth to adult wage scale").
    
    WAGE TABLE DEDUP (IMPORTANT):
        - Keep tables that differ by period, worker type, education level, job group/function scale, experience steps (periodieken/trede), age bands, or other substantive distinctions.
        - SKIP tables that are identical except for unit conversion (monthly vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer monthly if present).

    TABLE FORMAT:
        - Represent each table as a short list of strings like:
            [
                "Table title with context and units",
                "Columns: <Row label (if present)> | <Col 1 label> | <Col 2 label> | <Col 3 label> | ...",
                "<Row 1 label (if present)> | <v1> | <v2> | <v3> | ... ",
                "<Row 2 label (if present)> | <v1> | <v2> | <v3> | ... ",
                "... (one string per row)",
                "Additional notes or clarifying information if any"
            ]

    EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT):
        1) Read & anchor: read all instructions and focus ONLY on wage/salary information.
        2) Sweep & mark: scan the whole document including any appendices; mark every clause/table/text that relates to wages/salaries.
        3) Extract, translate & build — DO NOT HALLUCINATE:
            - Build one JSON object with ONLY the "wage_information" key.
            - COPY numbers/dates/%/names literally; TRANSLATE all other Dutch text (clauses, part of tables that are not numbers or names, titles, etc.) to clear English.
            - Tables: rebuild to TABLE FORMAT; apply WAGE TABLE DEDUP (remove pure unit-conversion duplicates; prefer monthly).
            - Consolidate: keep related items adjacent.
        4) Verify: confirm that every extracted fact, number, table, or clause is explicitly present in the document and that no important wage/salary information is missing. Correct or remove anything not grounded in the source.
        5) Validate: output one JSON object only; UTF-8 only; valid JSON (balanced brackets, no trailing commas); the "wage_information" key must be present (empty list if none).

    JSON OUTPUT REQUIREMENTS:
        - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
        - Ensure brackets/commas are correct; no trailing commas.

    OUTPUT_JSON_TEMPLATE:
        {{
            "wage_information": []
        }}

    Document: {filename}
    """


def create_nonsalary_extraction_prompt(filename: str) -> str:
    """Create the extraction prompt for all non-salary information."""
    return f"""
    Extract information from the Dutch CAO Markdown document parsed from a PDF. Pages are marked ## Page X _native_ (text-based) or ## Page X  _OCR_ (image-based); use these labels to interpret the layout correctly.

    GOAL: Produce one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, each mapping to a List[List[str]]. If nothing is found for a key, return an empty list.
    IMPORTANT: The field "wage_information" (Wage table and salary scales) is excluded and will be handled separately.

    THINKING & OUTPUT: Think step by step INTERNALLY to locate, route, and clean the data, but OUTPUT ONLY the final JSON (no explanations, no notes, no chain-of-thought).
        
    CRITICAL RULES:
        - Extract ONLY information explicitly present in the document. Do NOT hallucinate, infer, or guess any information. Always VERIFY the extracted information with the document.
        - Copy text literally (dates, numbers, percentages, units) - preserve exact values.
        - Be precise: NO paraphrasing, NO interpretation, NO added explanations, NO decorative elements, NO unnecessary separator lines or formatting characters.
        - IMPORTANT: Translate all Dutch text (clauses, tables, titles, etc.) into clear and precise English but keep names in Dutch. For legal clauses, preserve the exact legal meaning without simplification.
        - IMPORTANT: Check the appendix if it exists in the document. Salary tables and important information are sometimes located in appendices and should not be skipped.
    
    INSIDE SECTION RULES: Order does not matter within a section - keep related items together (e.g., a table followed by its note/explanation).
    
    ROUTING RULES (Use to avoid duplicates across sections):
        - Wage vs Overtime: atypical hours pay → overtime_information; structural bonuses → EXCLUDE (wage information, handled separately).
        - Wage vs Fringe: cash wages → EXCLUDE (wage information, handled separately); non-cash perks/reimbursements → fringe_benefits_information.
        - Homeoffice vs Fringe: WFH-specific (stipend, equipment, internet) → homeoffice_information; general perks → fringe_benefits_information.
        - Childcare vs Leave: time off/pay during leave → leave_information; childcare services/subsidies/discounts → childcare_information.
        - Training vs Safety vs AI: safety/Arbo training → safety_information; AI-related training → AI_information; all other training → training_information.
        - Safety vs Homeoffice: safety/Arbo for home working → homeoffice_information; general safety/integrity → safety_information.
        - Pension vs Wage vs Fringe: pension schemes/funds → pension_information; wages/bonuses → EXCLUDE (wage information, handled separately); non-pension perks → fringe_benefits_information.
        - Contract vs Termination: contract forms/ketenregeling/conversion → contract_type_information; notice/dismissal/severance/WW supplements → termination_information.
        - Holidays: pay/allowance for working on holidays → overtime_information; days off/policies → leave_information.
    
    WHAT TO INCLUDE:
        - Numbers, amounts, percentages, dates, periods, conditions, eligibility rules, procedures, entitlements, allowances (but NOT wage/salary amounts).
        - Tables: include a compact structure (see TABLE FORMAT) with headers and all data rows and columns plus any short note that explains the table.

    TABLE FORMAT:
        - Represent each table as a short list of strings like:
            [
                "Table title with context and units",
                "Columns: <Row label (if present)> | <Col 1 label> | <Col 2 label> | <Col 3 label> | ...",
                "<Row 1 label (if present)> | <v1> | <v2> | <v3> | ... ",
                "<Row 2 label (if present)> | <v1> | <v2> | <v3> | ... ",
                "... (one string per row)",
                "Additional notes or clarifying information if any"
            ]

    EXTRACTION STEPS (INTERNAL - DO NOT OUTPUT):
        1) Read & anchor: read all instructions and section descriptions.
        2) Sweep & mark: scan the whole document including any appendices; mark every clause/table/text that matches any section description EXCEPT wage/salary information; ignore text/sentences/clauses/passages that match none.
        3) Route: apply the ROUTING RULES to decide the correct section when overlaps occur. SKIP all wage/salary information.
        4) Length pre-check: if the marked set, when extracted verbatim, would likely exceed ~262,144 characters, plan to trim narrative/boilerplate in step 5.
        5) Extract, translate & build — DO NOT HALLUCINATE:
            - Build one JSON object with the exact keys in OUTPUT_JSON_TEMPLATE, in this order: general_information → pension_information → leave_information → termination_information → overtime_information → training_information → homeoffice_information → contract_type_information → safety_information → childcare_information → AI_information → fringe_benefits_information.
            - DO NOT include "wage_information" key.
            - COPY numbers/dates/%/names literally; TRANSLATE all other Dutch text (clauses, part of tables that are not numbers or names, titles, etc.) to clear English; leave blank if not stated.
            - Tables: rebuild to TABLE FORMAT.
            - Consolidate: keep related items adjacent.
            - If trimming per Step 4 is needed, shorten only narrative notes or minor wording not directly tied to field content, without changing legal meaning.
        6) Verify: confirm that every extracted fact, number, table, or clause is explicitly present in the document (allowing for shortening, restructuring, and translation to English) and that no important information is missing. Correct or remove anything not grounded in the source. Do not infer or guess. Ensure NO wage/salary information is included.
        7) Validate: output one JSON object only; UTF-8 only; valid JSON (balanced brackets, no trailing commas); all template keys present (empty list if none); "wage_information" must NOT be present.

    JSON OUTPUT REQUIREMENTS:
        - Output ONLY valid JSON (no markdown fences, no extra text). JSON must be UTF-8.
        - Ensure brackets/commas are correct; no trailing commas; all top-level keys present.
        - DO NOT include "wage_information" in the output.

    OUTPUT_JSON_TEMPLATE:
        {{
            "general_information": [],
            "pension_information": [],
            "leave_information": [],
            "termination_information": [],
            "overtime_information": [],
            "training_information": [],
            "homeoffice_information": [],
            "contract_type_information": [],
            "safety_information": [],
            "childcare_information": [],
            "AI_information": [],
            "fringe_benefits_information": []
        }}

    Document: {filename}
    """


def merge_split_extractions(salary_json: str, nonsalary_json: str, filename: str) -> Optional[str]:
    """
    Merge salary and non-salary extraction results into unified format.
    
    Args:
        salary_json: JSON string from salary-only extraction (wage_information)
        nonsalary_json: JSON string from non-salary extraction (all other fields)
        filename: Filename for error reporting
        
    Returns:
        Merged JSON string matching CAOExtractionSchema format, or None if merge fails
    """
    try:
        # Parse both JSON strings
        salary_data = json.loads(salary_json) if isinstance(salary_json, str) else salary_json
        nonsalary_data = json.loads(nonsalary_json) if isinstance(nonsalary_json, str) else nonsalary_json
        
        # Build unified structure matching CAOExtractionSchema
        merged = {
            "general_information": nonsalary_data.get("general_information", []),
            "wage_information": salary_data.get("wage_information", []),
            "pension_information": nonsalary_data.get("pension_information", []),
            "leave_information": nonsalary_data.get("leave_information", []),
            "termination_information": nonsalary_data.get("termination_information", []),
            "overtime_information": nonsalary_data.get("overtime_information", []),
            "training_information": nonsalary_data.get("training_information", []),
            "homeoffice_information": nonsalary_data.get("homeoffice_information", []),
            "contract_type_information": nonsalary_data.get("contract_type_information", []),
            "safety_information": nonsalary_data.get("safety_information", []),
            "childcare_information": nonsalary_data.get("childcare_information", []),
            "AI_information": nonsalary_data.get("AI_information", []),
            "fringe_benefits_information": nonsalary_data.get("fringe_benefits_information", [])
        }
        
        # Ensure all fields are lists of lists (List[List[str]])
        for key, value in merged.items():
            if not isinstance(value, list):
                merged[key] = []
            else:
                # Ensure nested lists are properly formatted
                normalized = []
                for item in value:
                    if isinstance(item, list):
                        normalized.append(item)
                    elif isinstance(item, str):
                        normalized.append([item])
                    else:
                        normalized.append([str(item)])
                merged[key] = normalized
        
        # Convert back to JSON string
        merged_json = json.dumps(merged, ensure_ascii=False, indent=2)
        
        # Validate the merged JSON matches the expected schema structure
        validation_result = validate_json_completeness(merged_json, filename)
        if not validation_result['is_valid']:
            print(f'  WARNING: Merged JSON validation failed: {validation_result["error"]}')
            return None
        
        return merged_json
        
    except json.JSONDecodeError as e:
        print(f'  ERROR: Failed to parse JSON during merge for {filename}: {e}')
        return None
    except Exception as e:
        print(f'  ERROR: Failed to merge split extractions for {filename}: {e}')
        return None


def validate_uploaded_file(client, uploaded_file, filename: str, original_size_mb: float):
    """Validate uploaded file with comprehensive checks."""
    try:
        file_resource = client.files.get(name=uploaded_file.name)
        
        # Check file state
        if file_resource.state.name != 'ACTIVE':
            raise ValueError(f'Uploaded file not ACTIVE: {file_resource.state.name}')
        
        # Check file size (using size_bytes attribute)
        if hasattr(file_resource, 'size_bytes') and file_resource.size_bytes == 0:
            raise ValueError(f'Uploaded file is empty for {filename}')
        
        # Check MIME type
        if hasattr(file_resource, 'mime_type') and file_resource.mime_type != "text/markdown":
            print(f'  WARNING: Unexpected MIME type: {file_resource.mime_type} (expected: text/markdown)')
        
        # Check if file size is reasonable (should be similar to original)
        if hasattr(file_resource, 'size_bytes'):
            uploaded_size_mb = file_resource.size_bytes / (1024 * 1024)
            size_diff_percent = abs(uploaded_size_mb - original_size_mb) / original_size_mb * 100
            
            if size_diff_percent > 50:  # More than 50% difference
                print(f'  WARNING: Large size difference: original={original_size_mb:.2f}MB, uploaded={uploaded_size_mb:.2f}MB ({size_diff_percent:.1f}% diff)')
            
            print(f'  DEBUG: File validation passed - size: {uploaded_size_mb:.2f}MB')
        else:
            print(f'  DEBUG: File validation passed - size information not available')
        
        return True
        
    except Exception as e:
        print(f'  ERROR: File validation failed: {e}')
        raise ValueError(f'File validation failed for {filename}: {e}')


def safe_contents(prompt: str, uploaded_file=None):
    """Safely construct contents array with validation for Gemini API call."""
    contents = []
    
    # Validate prompt
    if not prompt or not prompt.strip():
        raise ValueError("Empty prompt - refusing to call Gemini")
    contents.append(prompt.strip())
    
    # Validate file
    if uploaded_file:
        contents.append(uploaded_file)
    
    if not contents:
        raise ValueError("No valid content parts - refusing to call Gemini")
    
    print(f'  DEBUG: Constructed {len(contents)} content parts for API call')
    return contents


def extract_text_safely(response, filename: str, context: ProcessingContext = None):
    """Safely extract text from response with proper error handling and JSON cleanup."""
    if response is None:
        raise ValueError('No response received from model')
    
    if context and context.debug:
        context.debug.log(f'  DEBUG: Response object type: {type(response)}')
    
    # Check if we have candidates
    if not getattr(response, "candidates", None):
        raise ValueError('No candidates in response')
    
    cand = response.candidates[0]
    
    # Check finish reason first - this is critical for understanding failures
    fr = getattr(cand, "finish_reason", None)
    if context and context.debug:
        context.debug.log(f'  DEBUG: Finish reason: {fr}')
    
    # If filtered or blocked, don't try to access text
    if fr and fr not in ["STOP", "MAX_TOKENS"]:
        safety_info = []
        if hasattr(cand, "safety_ratings") and cand.safety_ratings is not None:
            safety_info = [(r.category, r.probability) for r in cand.safety_ratings]
        if context and context.debug:
            context.debug.log(f'  DEBUG: Response blocked - Finish reason: {fr}, Safety ratings: {safety_info}')
        return "", {"finish": fr, "safety": safety_info, "filename": filename}
    
    # Check for structured output first (when using response_schema)
    if context and context.debug:
        context.debug.log(f'  DEBUG: Checking response.parsed: hasattr={hasattr(response, "parsed")}, value={"FOUND" if hasattr(response, "parsed") and response.parsed else "NOT_FOUND"}')
    if hasattr(response, 'parsed') and response.parsed:
        if context and context.debug:
            context.debug.log(f'  DEBUG: Found structured output in response.parsed')
            context.debug.log(f'  DEBUG: response.parsed type: {type(response.parsed)}')
            context.debug.log(f'  DEBUG: response.parsed content: [STRUCTURED DATA - SUPPRESSED FOR CLARITY]')
            # Check if response.text exists to see raw JSON from Gemini
            if hasattr(response, 'text') and response.text:
                # Sample first 500 chars to check for nulls in raw JSON
                sample = response.text[:500]
                null_count = sample.count(': null')
                context.debug.log(f'  DEBUG: Raw JSON sample (first 500 chars) contains {null_count} null values')
                if null_count > 0:
                    context.debug.log(f'  DEBUG: Raw JSON sample: {sample}')
        # Convert structured output to JSON string
        content = response.parsed.model_dump_json()
        if context and context.debug:
            context.debug.log(f'  DEBUG: Converted structured output to JSON: {len(content)} chars')
        return content, {"finish": fr or "STOP", "filename": filename}
    
    # Check for direct text response (when not using response_schema)
    if context and context.debug:
        context.debug.log(f'  DEBUG: Checking response.text: hasattr={hasattr(response, "text")}, value={getattr(response, "text", "NOT_FOUND")}')
    if hasattr(response, 'text') and response.text:
        if context and context.debug:
            context.debug.log(f'  DEBUG: Found direct text response: {len(response.text)} chars')
        content = response.text
        return content, {"finish": fr or "STOP", "filename": filename}
    
    # Only proceed with text parts extraction if we don't have structured output
    # Fallback to text parts extraction
    text_chunks = []
    content = getattr(cand, "content", None)
    
    if not content:
        raise ValueError('No content in response candidate')
    
    parts = getattr(content, "parts", []) or []
    if context and context.debug:
        context.debug.log(f'  DEBUG: Found {len(parts)} content parts')
    
    # Debug: Print all part types to understand what we're getting
    for i, part in enumerate(parts):
        if context and context.debug:
            context.debug.log(f'  DEBUG: Part {i}: type={type(part).__name__}, attributes={dir(part)}')
        if hasattr(part, "text") and part.text:
            text_chunks.append(part.text)
            if context and context.debug:
                context.debug.log(f'  DEBUG: Part {i}: text length = {len(part.text)}')
        elif hasattr(part, "function_call"):
            if context and context.debug:
                context.debug.log(f'  DEBUG: Part {i}: function_call found')
        elif hasattr(part, "inline_data"):
            if context and context.debug:
                context.debug.log(f'  DEBUG: Part {i}: inline_data found')
        else:
            if context and context.debug:
                context.debug.log(f'  DEBUG: Part {i}: no text content, no function_call, no inline_data')
    
    if not text_chunks:
        # Try fallback: check if response has direct text attribute
        if context and context.debug:
            context.debug.log(f'  DEBUG: No text parts found, trying fallback methods...')
        
        # Try direct response attributes (but we already checked for parsed and text above)
        if context and context.debug:
            context.debug.log(f'  DEBUG: No fallback text found in response')
        raise ValueError('No text parts found in response')
    else:
        content = "".join(text_chunks)
        if context and context.debug:
            context.debug.log(f'  DEBUG: Total extracted text length: {len(content)}')
    
    # Apply JSON cleanup (integrated from original validate_llm_response)
    if content.strip().startswith('{') and content.strip().endswith('}'):
        try:
            json.loads(content)
            if context and context.debug:
                context.debug.log(f'  DEBUG: JSON is valid without cleanup')
        except json.JSONDecodeError as e:
            if context and context.debug:
                context.debug.log(f'  WARNING: JSON parsing failed, attempting cleanup: {str(e)}')
            
            # Remove problematic control characters (but keep \n, \t, \r)
            import re
            cleaned_content = re.sub(r'[\x00-\x09\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', content)
            
            try:
                json.loads(cleaned_content)
                if context and context.debug:
                    context.debug.log(f'  INFO: JSON cleanup successful, using cleaned content')
                content = cleaned_content
            except json.JSONDecodeError as e2:
                if context and context.debug:
                    context.debug.log(f'  WARNING: JSON cleanup failed: {str(e2)}')
                    context.debug.log(f'  INFO: Using raw text content as fallback')
    
    return content, {"finish": fr or "STOP", "filename": filename}


def handle_llm_errors(error: Exception, attempt: int, max_retries: int, file_size_mb: float = 0, context=None, remaining_budget_s: Optional[int] = None) -> bool:
    """Handle different types of LLM errors with appropriate retry logic."""
    error_str = str(error).lower()
    
    if ('deadlineexceeded' in error_str or '504' in error_str or 
        'timeout' in error_str or 'truncated' in error_str):
        if attempt < max_retries - 1:
            # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
            wait_time = 120 * 2 ** min(attempt, 4)
            if remaining_budget_s is not None:
                wait_time = min(wait_time, max(0, int(remaining_budget_s) - 5))
            print(f'  Attempt {attempt + 1} failed (timeout/truncation), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with timeout/truncation errors')
            return False
    elif 'serviceunavailable' in error_str or '503' in error_str or 'connection reset' in error_str or '500' in error_str or 'internal' in error_str:
        if attempt < max_retries - 1:
            # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
            wait_time = 60 * 2 ** min(attempt, 4)
            print(f'  Attempt {attempt + 1} failed (service unavailable/internal error), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with service unavailable/internal errors')
            return False
    elif 'no content parts found' in error_str or 'no content' in error_str:
        if attempt < max_retries - 1:
            # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
            wait_time = 60 * 2 ** min(attempt, 4)
            print(f'  Attempt {attempt + 1} failed (empty response), retrying in {wait_time // 60} minutes...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with empty response errors')
            return False
    elif 'incomplete json' in error_str or 'json validation failed' in error_str or 'truncated' in error_str:
        if attempt < max_retries - 1:
            # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
            wait_time = 30 * 2 ** min(attempt, 4)
            print(f'  Attempt {attempt + 1} failed (incomplete JSON), retrying in {wait_time} seconds...')
            time.sleep(wait_time)
            return True
        else:
            print(f'  All {max_retries} attempts failed with incomplete JSON errors')
            return False
    elif 'quota' in error_str or '429' in error_str:
        # Check if it's a daily quota limit (not retryable)
        if 'perday' in error_str or 'daily' in error_str or '3000000' in error_str:
            global process_quota_flags
            process_quota_flags[context.process_id] = True
            print(f'  ❌ DAILY QUOTA LIMIT REACHED for Process {context.process_id} - Cannot retry until tomorrow')
            print(f'  💡 Daily limit: 3,000,000 tokens per day')
            print(f'  💡 Quota resets at midnight (Google timezone)')
            print(f'  🛑 Stopping this process to avoid infinite retries')
            return False  # Stop immediately, don't retry
        
        # Regular per-minute quota limit (retryable)
        if attempt < max_retries - 1:
            # Try to extract API's suggested retry delay from error details
            api_retry_delay = None
            try:
                # Check if error has details with RetryInfo - try multiple ways to access it
                # Method 1: Check if error has 'error' attribute (ClientError structure)
                if hasattr(error, 'error') and isinstance(error.error, dict):
                    details = error.error.get('details', [])
                    for detail in details:
                        if isinstance(detail, dict) and detail.get('@type') == 'type.googleapis.com/google.rpc.RetryInfo':
                            retry_delay_str = detail.get('retryDelay', '')
                            # Parse duration string (e.g., "8s" or "8.666s")
                            if retry_delay_str.endswith('s'):
                                api_retry_delay = float(retry_delay_str[:-1])
                                break
                # Method 2: Check error string representation for "Please retry in X.XXs"
                if api_retry_delay is None:
                    import re
                    match = re.search(r'please retry in ([\d.]+)s', error_str, re.IGNORECASE)
                    if match:
                        api_retry_delay = float(match.group(1))
                        print(f'  DEBUG: Extracted API retry delay from message: {api_retry_delay:.1f}s')
            except Exception as e:
                print(f'  DEBUG: Error extracting API retry delay: {e}')
                pass  # If extraction fails, fall back to calculated delay
            
            # Calculate our delay
            if file_size_mb > 0:
                wait_time = calculate_quota_retry_delay(file_size_mb, attempt)
            else:
                # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
                wait_time = 90 * 2 ** min(attempt, 4)  # Fallback for unknown file size
            
            # Always add 3 minutes (180 seconds) to quota retry delay
            wait_time += 180
            print(f'  DEBUG: Calculated wait time (before API delay check): {wait_time}s ({wait_time // 60} minutes)')
            
            # Use API's suggested delay if it's longer than our calculated delay (with 3 min buffer)
            if api_retry_delay is not None:
                # Add buffer to API delay (at least 10 seconds, or 20% more, whichever is larger)
                api_delay_with_buffer = max(api_retry_delay + 10, api_retry_delay * 1.2)
                # Ensure we always have at least 3 minutes total
                api_delay_with_buffer = max(api_delay_with_buffer, 180)
                wait_time = max(wait_time, int(api_delay_with_buffer))
                print(f'  INFO: API suggested retry delay: {api_retry_delay:.1f}s, using {wait_time}s (with 3 min minimum)')
            
            print(f'  Attempt {attempt + 1} failed (per-minute quota), retrying in {wait_time // 60} minutes ({wait_time}s)...')
            print(f'  INFO: Waiting {wait_time}s before retry...')
            time.sleep(wait_time)
            print(f'  INFO: Wait complete, continuing with retry...')
            return True
        else:
            print(f'  All {max_retries} attempts failed with per-minute quota errors')
            return False
    elif attempt < max_retries - 1:
        # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
        wait_time = 30 * 2 ** min(attempt, 4)
        print(f'  Attempt {attempt + 1} failed ({type(error).__name__}), retrying in {wait_time} seconds...')
        time.sleep(wait_time)
        return True
    else:
        return False


# =============================================================================
# LOGGING & MONITORING FUNCTIONS
# =============================================================================
# Functions for logging processing results and monitoring progress
def log_processing_result(filename: str, success: bool, context: ProcessingContext, 
                         error_message: str = None):
    """Log processing results to appropriate files."""
    if not success:
        context.stats.add_failure(filename)
        failed_log_path = 'outputs/logs/failed_files_llm_extraction.txt'
        with open(failed_log_path, 'a', encoding='utf-8') as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {context.key_number}: {filename}")
            if error_message:
                f.write(f" (Error: {error_message})")
            f.write('\n')
    else:
        context.stats.add_success(filename)


def log_detailed_failure(response_info: dict, filename: str, attempt: int):
    """Log detailed failure information for debugging."""
    print(f'  🔍 DETAILED FAILURE ANALYSIS for {filename} (attempt {attempt + 1}):')
    print(f'    📊 Finish reason: {response_info.get("finish", "UNKNOWN")}')
    print(f'    🛡️  Safety ratings: {response_info.get("safety", [])}')
    print(f'    📄 Content length: {len(response_info.get("content", ""))} chars')
    print(f'    ⏱️  Processing time: {response_info.get("processing_time", "UNKNOWN")}s')
    print(f'    🔑 API key used: {response_info.get("api_key", "UNKNOWN")}')
    print(f'    🆔 Process ID: {response_info.get("process_id", "UNKNOWN")}')


def validate_response_schema(content: str, filename: str) -> bool:
    """Validate that response contains expected schema structure."""
    try:
        data = json.loads(content)
        required_fields = ['general_information', 'wage_information', 'pension_information', 'leave_information']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            print(f'  WARNING: Missing required fields: {missing_fields}')
            return False
            
        return True
    except json.JSONDecodeError:
        print(f'  ERROR: Invalid JSON structure')
        return False


# =============================================================================
# CORE EXTRACTION FUNCTION
# =============================================================================
# Main function for extracting content from markdown files using Gemini API
def extract_split_extraction(markdown_path: str, filename: str, cao_number: str,
                            uploaded_file, context: ProcessingContext, attempt: int,
                            timeout_seconds: int, adjusted_params: Dict[str, Any],
                            cached_salary: Optional[str] = None,
                            cached_nonsalary: Optional[str] = None,
                            remaining_budget_s: Optional[int] = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Perform split extraction (salary and non-salary) for attempts 5-7.
    
    Args:
        markdown_path: Path to markdown file
        filename: Name of file being processed
        cao_number: CAO number
        uploaded_file: Already uploaded file resource
        context: Processing context
        attempt: Current attempt number (5, 6, or 7)
        timeout_seconds: Timeout in seconds
        adjusted_params: Adjusted parameters for this attempt
        cached_salary: Previously successful salary extraction (optional)
        cached_nonsalary: Previously successful non-salary extraction (optional)
        remaining_budget_s: Remaining time budget in seconds
        
    Returns:
        Tuple of (merged_content, salary_content, nonsalary_content):
        - merged_content: Merged JSON string if both succeeded, None otherwise
        - salary_content: Salary JSON string (new or cached)
        - nonsalary_content: Non-salary JSON string (new or cached)
    """
    print(f'  INFO: Attempt {attempt + 1} - Using split extraction (salary + non-salary)')
    
    # Use cached results if available
    salary_content = cached_salary
    nonsalary_content = cached_nonsalary
    
    if cached_salary:
        print(f'  INFO: Using cached salary extraction from previous attempt')
    if cached_nonsalary:
        print(f'  INFO: Using cached non-salary extraction from previous attempt')
    
    # Safety settings
    safety_settings = [
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE
        )
    ]
    
    salary_response_info = None
    nonsalary_response_info = None
    
    # Extract salary information (only if not cached)
    if not salary_content:
        print(f'  INFO: Extracting salary information (attempt {attempt + 1})...')
        try:
            context.current_stage = "generating_content_salary"
            context.stage_start_ts = time.time()
            
            salary_prompt = create_salary_extraction_prompt(filename)
            salary_safe_content = safe_contents(salary_prompt, uploaded_file)
            
            salary_response = context.client.models.generate_content(
                model=context.config.model,
                contents=salary_safe_content,
                config={
                    'temperature': adjusted_params['temperature'],
                    'top_p': adjusted_params['top_p'],
                    'top_k': adjusted_params['top_k'],
                    'max_output_tokens': context.config.max_tokens,
                    'candidate_count': context.config.candidate_count,
                    'seed': context.config.seed,
                    'presence_penalty': context.config.presence_penalty,
                    'frequency_penalty': context.config.frequency_penalty,
                    'response_mime_type': 'application/json',
                    'response_schema': CAOSalaryOnlySchema,
                    'thinking_config': types.ThinkingConfig(thinking_budget=context.config.thinking_budget),
                    'http_options': types.HttpOptions(timeout=timeout_seconds * 1000),
                    'safety_settings': safety_settings
                }
            )
            
            salary_content, salary_response_info = extract_text_safely(salary_response, filename, context)
            print(f'  INFO: Salary extraction completed: {len(salary_content) if salary_content else 0} chars')
            
        except Exception as e:
            print(f'  ERROR: Salary extraction failed: {e}')
            salary_response_info = {"finish": "ERROR", "error": str(e)}
    else:
        print(f'  INFO: Skipping salary extraction (using cached result)')
    
    # Add 3-minute delay between salary and non-salary extraction for retries 6-8
    if not nonsalary_content:  # Only delay if we need to extract non-salary
        delay_seconds = 180  # 3 minutes
        print(f'  INFO: Waiting {delay_seconds // 60} minutes before non-salary extraction...')
        time.sleep(delay_seconds)
    
    # Extract non-salary information (only if not cached)
    if not nonsalary_content:
        print(f'  INFO: Extracting non-salary information (attempt {attempt + 1})...')
        try:
            # Re-fetch and validate file resource before second API call
            # The file resource may become stale after the first API call and 3-minute delay
            try:
                file_resource = context.client.files.get(name=uploaded_file.name)
                if file_resource.state.name != 'ACTIVE':
                    raise ValueError(f'Uploaded file not ACTIVE before non-salary extraction: {file_resource.state.name}')
                # Update uploaded_file reference to ensure we have a fresh resource
                uploaded_file = file_resource
                print(f'  DEBUG: Re-validated file resource before non-salary extraction - state: {file_resource.state.name}')
            except Exception as e:
                print(f'  WARNING: Failed to re-validate file resource: {e}')
                # Continue anyway - the original uploaded_file might still work
            
            context.current_stage = "generating_content_nonsalary"
            context.stage_start_ts = time.time()
            
            nonsalary_prompt = create_nonsalary_extraction_prompt(filename)
            nonsalary_safe_content = safe_contents(nonsalary_prompt, uploaded_file)
            
            nonsalary_response = context.client.models.generate_content(
                model=context.config.model,
                contents=nonsalary_safe_content,
                config={
                    'temperature': adjusted_params['temperature'],
                    'top_p': adjusted_params['top_p'],
                    'top_k': adjusted_params['top_k'],
                    'max_output_tokens': context.config.max_tokens,
                    'candidate_count': context.config.candidate_count,
                    'seed': context.config.seed,
                    'presence_penalty': context.config.presence_penalty,
                    'frequency_penalty': context.config.frequency_penalty,
                    'response_mime_type': 'application/json',
                    'response_schema': CAONonSalarySchema,
                    'thinking_config': types.ThinkingConfig(thinking_budget=context.config.thinking_budget),
                    'http_options': types.HttpOptions(timeout=timeout_seconds * 1000),
                    'safety_settings': safety_settings
                }
            )
            
            nonsalary_content, nonsalary_response_info = extract_text_safely(nonsalary_response, filename, context)
            print(f'  INFO: Non-salary extraction completed: {len(nonsalary_content) if nonsalary_content else 0} chars')
            
        except Exception as e:
            # Enhanced error logging for non-salary extraction failures
            error_msg = str(e)
            print(f'  ERROR: Non-salary extraction failed: {error_msg}')
            
            # Log additional diagnostic information if available
            # Note: nonsalary_response might not be defined if error occurred before API call
            try:
                if 'nonsalary_response' in locals() and hasattr(nonsalary_response, 'candidates') and nonsalary_response.candidates:
                    cand = nonsalary_response.candidates[0]
                    finish_reason = getattr(cand, "finish_reason", None)
                    print(f'  DEBUG: Non-salary response finish_reason: {finish_reason}')
                    if hasattr(cand, "safety_ratings") and cand.safety_ratings:
                        safety_info = [(r.category, r.probability) for r in cand.safety_ratings]
                        print(f'  DEBUG: Non-salary safety ratings: {safety_info}')
            except:
                pass  # Ignore errors in diagnostic logging
            
            nonsalary_response_info = {"finish": "ERROR", "error": error_msg}
    else:
        print(f'  INFO: Skipping non-salary extraction (using cached result)')
    
    # Check if both extractions succeeded (either from this attempt or cached)
    if not salary_content or not nonsalary_content:
        error_parts = []
        if not salary_content:
            error_parts.append("salary")
        if not nonsalary_content:
            error_parts.append("non-salary")
        error_msg = f"Split extraction incomplete: {' and '.join(error_parts)} extraction not available"
        print(f'  INFO: {error_msg} - will retry failed part on next attempt')
        # Return None for merged content, but return individual results for caching
        return None, salary_content, nonsalary_content
    
    # Validate individual extractions
    if salary_content:
        salary_validation = validate_json_completeness(salary_content, filename)
        if not salary_validation['is_valid']:
            print(f'  ERROR: Salary JSON validation failed: {salary_validation.get("error", "Unknown error")}')
            return None, None, nonsalary_content  # Clear invalid salary, keep valid nonsalary
    
    if nonsalary_content:
        nonsalary_validation = validate_json_completeness(nonsalary_content, filename)
        if not nonsalary_validation['is_valid']:
            print(f'  ERROR: Non-salary JSON validation failed: {nonsalary_validation.get("error", "Unknown error")}')
            return None, salary_content, None  # Keep valid salary, clear invalid nonsalary
    
    # If we don't have both, return partial results for next attempt
    if not salary_content or not nonsalary_content:
        return None, salary_content, nonsalary_content
    
    # Merge the results
    print(f'  INFO: Merging salary and non-salary results...')
    merged_content = merge_split_extractions(salary_content, nonsalary_content, filename)
    
    if not merged_content:
        print(f'  ERROR: Failed to merge split extractions')
        return None, salary_content, nonsalary_content
    
    # Validate merged result
    merged_validation = validate_json_completeness(merged_content, filename)
    if not merged_validation['is_valid']:
        print(f'  ERROR: Merged JSON validation failed: {merged_validation.get("error", "Unknown error")}')
        return None, salary_content, nonsalary_content
    
    # Validate merged schema
    if not validate_response_schema(merged_content, filename):
        print(f'  WARNING: Merged response schema validation failed for {filename}')
    
    print(f'  INFO: Split extraction and merge completed successfully')
    return merged_content, salary_content, nonsalary_content


def extract_with_markdown_upload(markdown_path: str, filename: str, cao_number: str, 
                               context: ProcessingContext, remaining_budget_s: Optional[int] = None) -> Optional[str]:
    """Extract using Files API approach - upload markdown file to Gemini."""
    global process_quota_flags
    print(f'  INFO: Using Files API approach for {filename}')
    start_time = time.time()
    file_size_mb = os.path.getsize(markdown_path) / (1024 * 1024)
    
    if not markdown_path or not os.path.exists(markdown_path):
        print(f'  ERROR: Markdown file not found: {markdown_path}')
        context.performance_monitor.log_extraction(
            filename=filename, file_size_mb=0, processing_time=time.time() - start_time,
            usage_metadata=None, success=False, error_message='Markdown not found',
            api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
            model=context.config.model, parameters=get_model_parameters(context.config)
        )
        return None
    
    print(f'  INFO: Markdown file size: {file_size_mb:.2f} MB')
    
    # Dynamic timeout based on file size
    if file_size_mb > 8.0:
        timeout_seconds = 1200
    elif file_size_mb > 5.0:
        timeout_seconds = 900
    else:
        timeout_seconds = 600
    
    # Cap timeout by remaining budget if provided
    if remaining_budget_s is not None:
        per_call_cap = max(15, int(remaining_budget_s) - 5)
        timeout_seconds = min(timeout_seconds, per_call_cap)
        if remaining_budget_s <= 0:
            raise TimeoutError(f'No remaining budget for {filename}')
    
    # Track last error message for retry guidance
    last_error_message = None
    
    # Cache for partial split extraction results (attempts 5-7)
    cached_salary = None
    cached_nonsalary = None
    
    for attempt in range(context.config.max_retries):
        # Check quota exhaustion at START of each retry attempt (before API call)
        if context.process_id in process_quota_flags and process_quota_flags[context.process_id]:
            print(f'  🛑 Quota exhausted detected at start of retry loop, stopping retries')
            break
        
        # Get adjusted parameters for this attempt (needed for logging even on errors)
        adjusted_params = get_adjusted_parameters(context.config, attempt)
        
        # Generate retry guidance for LLM-controllable errors (only after 2nd attempt)
        retry_guidance = ""
        error_type = ""
        if attempt >= 2 and last_error_message:
            retry_guidance, error_type = get_retry_guidance(last_error_message)
            if retry_guidance:
                print(f'  INFO: Adding retry guidance for: {error_type}')
        
        uploaded_file = None
        try:
            # Stage marker: uploading
            context.current_stage = "uploading"
            context.stage_start_ts = time.time()
            
            # Log parameter adjustments if this is attempt 3, 4, or 5-7 (4th, 5th, or 6th-8th try)
            if attempt >= 3:
                if attempt <= 4:
                    boost = 0.1 if attempt == 3 else 0.2
                    print(f'  INFO: Attempt {attempt + 1} - Adjusting parameters: temp={adjusted_params["temperature"]:.1f}, top_p={adjusted_params["top_p"]:.1f}, top_k={adjusted_params["top_k"]} (boost +{boost})')
                elif attempt >= 5:
                    # Attempts 5-7 use split extraction
                    if attempt == 5:
                        print(f'  INFO: Attempt {attempt + 1} - Split extraction with original parameters (temp={adjusted_params["temperature"]:.1f}, top_p={adjusted_params["top_p"]:.1f}, top_k={adjusted_params["top_k"]})')
                    elif attempt == 6:
                        print(f'  INFO: Attempt {attempt + 1} - Split extraction with original parameters (temp={adjusted_params["temperature"]:.1f}, top_p={adjusted_params["top_p"]:.1f}, top_k={adjusted_params["top_k"]})')
                    else:  # attempt == 7
                        print(f'  INFO: Attempt {attempt + 1} - Split extraction with +0.1 adjustment (temp={adjusted_params["temperature"]:.1f}, top_p={adjusted_params["top_p"]:.1f}, top_k={adjusted_params["top_k"]})')
            
            print(f'  INFO: Uploading markdown file to Gemini...')
            try:
                uploaded_file = context.client.files.upload(
                    file=markdown_path,
                    config={"mime_type": "text/markdown"}
                )
                print(f'  INFO: File uploaded successfully: {uploaded_file.name}')
            except Exception as e:
                print(f'  ERROR: File upload failed: {e}')
                raise ValueError(f'Failed to upload file {filename}: {e}')
            
            # Check file state and wait for processing
            context.current_stage = "file_state_polling"
            max_wait_seconds = (300 if file_size_mb <= 5.0 else 600 if file_size_mb <= 10.0 else 900)
            poll_interval_seconds = 2
            waited = 0
            
            # print(f'  INFO: Waiting for file processing (max {max_wait_seconds}s)...')
            while waited < max_wait_seconds:
                try:
                    file_resource = context.client.files.get(name=uploaded_file.name)
                    if file_resource.state.name == 'ACTIVE':
                        print(f'  INFO: File is ready for processing')
                        break
                    elif file_resource.state.name == 'FAILED':
                        raise ValueError(f'Uploaded file processing FAILED for {filename}')
                    else:
                        print(f'  INFO: File state: {file_resource.state.name} (waited {waited}s)')
                except Exception as e:
                    print(f'  WARNING: Error checking file state: {e}')
                    time.sleep(poll_interval_seconds)
                    waited += poll_interval_seconds
                    continue
                time.sleep(poll_interval_seconds)
                waited += poll_interval_seconds
            
            if waited >= max_wait_seconds:
                raise TimeoutError(f'Uploaded file not ACTIVE after {max_wait_seconds}s for {filename}')
            
            # Validate uploaded file with comprehensive checks
            validate_uploaded_file(context.client, uploaded_file, filename, file_size_mb)
            
            # Check if we should use split extraction (attempts 5-7)
            if attempt >= 5:
                # Use split extraction for attempts 6-8
                merged_content, salary_result, nonsalary_result = extract_split_extraction(
                    markdown_path, filename, cao_number, uploaded_file,
                    context, attempt, timeout_seconds, adjusted_params,
                    cached_salary, cached_nonsalary, remaining_budget_s
                )
                
                # Update cache with successful results (keep valid cached values if new attempt failed)
                if salary_result:
                    cached_salary = salary_result
                    print(f'  INFO: Cached salary extraction for future attempts')
                if nonsalary_result:
                    cached_nonsalary = nonsalary_result
                    print(f'  INFO: Cached non-salary extraction for future attempts')
                
                content = merged_content
                
                if content:
                    # Split extraction succeeded
                    processing_time = time.time() - start_time
                    content_length = len(content)
                    estimated_tokens = content_length // 4
                    print(f'  INFO: Successfully completed split extraction (time: {processing_time:.1f}s)')
                    print(f'  INFO: Merged response size: {content_length:,} chars (~{estimated_tokens:,} tokens)')
                    
                    # Validate merged result
                    validation_result = validate_json_completeness(content, filename)
                    if not validation_result['is_valid']:
                        error_msg = f"Merged JSON validation failed: {validation_result.get('error', 'Unknown error')}"
                        print(f'  WARNING: {error_msg} - retrying...')
                        last_error_message = error_msg
                        cleanup_uploaded_file(context.client, uploaded_file)
                        if attempt == context.config.max_retries - 1:
                            error_msg = f"Merged JSON validation failed after {context.config.max_retries} attempts"
                            print(f'  ERROR: {error_msg}')
                            context.performance_monitor.log_extraction(
                                filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                                usage_metadata=None, success=False, error_message=error_msg,
                                api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
                                model=context.config.model, parameters=adjusted_params
                            )
                            return None
                        continue
                    
                    # Validate response schema
                    if not validate_response_schema(content, filename):
                        print(f'  WARNING: Merged response schema validation failed for {filename}')
                    
                    # Log successful split extraction
                    log_params = adjusted_params.copy()
                    log_params['split_extraction'] = True
                    log_params['attempt'] = attempt + 1
                    
                    context.performance_monitor.log_extraction(
                        filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                        usage_metadata=None, success=True, api_key_used=context.key_number,
                        process_id=context.process_id, cao_number=cao_number,
                        model=context.config.model, parameters=log_params
                    )
                    
                    cleanup_uploaded_file(context.client, uploaded_file)
                    return content
                else:
                    # Split extraction incomplete - check which parts failed
                    missing_parts = []
                    if not cached_salary:
                        missing_parts.append("salary")
                    if not cached_nonsalary:
                        missing_parts.append("non-salary")
                    
                    if missing_parts:
                        error_msg = f"Split extraction incomplete on attempt {attempt + 1}: {' and '.join(missing_parts)} extraction not available"
                        print(f'  INFO: {error_msg} - will retry failed part(s) on next attempt')
                    else:
                        error_msg = f"Split extraction failed on attempt {attempt + 1}"
                        print(f'  WARNING: {error_msg} - retrying...')
                    
                    last_error_message = error_msg
                    cleanup_uploaded_file(context.client, uploaded_file)
                    if attempt == context.config.max_retries - 1:
                        final_error_msg = f"Split extraction incomplete after {context.config.max_retries} attempts"
                        if missing_parts:
                            final_error_msg += f" - missing: {', '.join(missing_parts)}"
                        print(f'  ERROR: {final_error_msg}')
                        processing_time = time.time() - start_time
                        log_params = adjusted_params.copy()
                        log_params['split_extraction'] = True
                        if missing_parts:
                            log_params['missing_parts'] = missing_parts
                        context.performance_monitor.log_extraction(
                            filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                            usage_metadata=None, success=False, error_message=final_error_msg,
                            api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
                            model=context.config.model, parameters=log_params
                        )
                        return None
                    continue
            
            # Regular unified extraction for attempts 0-4
            # Create and validate extraction prompt
            extraction_prompt = create_extraction_prompt(filename)
            
            # Add retry guidance if applicable (only for LLM-controllable errors after 2nd attempt)
            if retry_guidance:
                extraction_prompt += f"\n\n{retry_guidance}"
            
            # Disable safety settings to prevent content filtering
            safety_settings = [
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                )
            ]
            
            # Safely construct contents for API call
            safe_content = safe_contents(extraction_prompt, uploaded_file)
            
            # Stage marker: generating content
            context.current_stage = "generating_content"
            context.stage_start_ts = time.time()
            
            # Use the original working approach with response_schema
            try:
                response = context.client.models.generate_content(
                    model=context.config.model,
                    contents=safe_content,
                    config={
                        'temperature': adjusted_params['temperature'],
                        'top_p': adjusted_params['top_p'],
                        'top_k': adjusted_params['top_k'],
                        'max_output_tokens': context.config.max_tokens,
                        'candidate_count': context.config.candidate_count,
                        'seed': context.config.seed,
                        'presence_penalty': context.config.presence_penalty,
                        'frequency_penalty': context.config.frequency_penalty,
                        'response_mime_type': 'application/json',
                        'response_schema': CAOExtractionSchema,
                        'thinking_config': types.ThinkingConfig(thinking_budget=context.config.thinking_budget),
                        'http_options': types.HttpOptions(timeout=timeout_seconds * 1000),
                        'safety_settings': safety_settings
                    }
                )
            except Exception as api_error:
                # Defensive exception handling for API calls that escape retry loops
                import traceback
                error_type = type(api_error).__name__
                error_msg = str(api_error)
                print(f'  🚨 UNEXPECTED API ERROR during markdown extraction (attempt {attempt + 1}): {error_type}: {error_msg}')
                print(f'  📋 This error occurred during the API call itself and will be handled by retry logic')
                # Log to file for debugging
                try:
                    error_log_path = 'outputs/logs/fatal_errors_llm_extraction.txt'
                    Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(error_log_path, 'a', encoding='utf-8') as f:
                        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Process {context.process_id} - API ERROR (markdown extraction)\n")
                        f.write(f"File: {filename}\n")
                        f.write(f"Error Type: {error_type}\n")
                        f.write(f"Error Message: {error_msg}\n")
                        f.write(f"Traceback:\n{traceback.format_exc()}\n\n")
                except Exception:
                    pass  # Don't fail on logging failure
                # Re-raise so existing retry logic can handle it
                raise
            
            content, extraction_info = extract_text_safely(response, filename, context)
            
            # Stage marker: validating and saving
            context.current_stage = "validating_saving"
            context.stage_start_ts = time.time()
            
            if content:
                processing_time = time.time() - start_time
                content_length = len(content)
                estimated_tokens = content_length // 4
                print(f'  INFO: Successfully extracted structured data from markdown (time: {processing_time:.1f}s)')
                print(f'  INFO: Response size: {content_length:,} chars (~{estimated_tokens:,} tokens)')
                
                # Check for truncation indicators
                finish_reason = extraction_info.get("finish", "UNKNOWN")
                is_truncated = finish_reason == "MAX_TOKENS"
                
                # Validate JSON completeness
                validation_result = validate_json_completeness(content, filename)
                is_json_complete = validation_result['is_valid']
                
                # If JSON is incomplete or response was truncated, retry
                if not is_json_complete or is_truncated:
                    error_msg = f"JSON incomplete or truncated: {validation_result.get('error', 'Unknown error')}"
                    print(f'  WARNING: {error_msg} - retrying...')
                    
                    # Store error message for retry guidance
                    last_error_message = error_msg
                    
                    cleanup_uploaded_file(context.client, uploaded_file)
                    
                    # If this is the last attempt, log the failure and return None
                    if attempt == context.config.max_retries - 1:
                        error_msg = f"JSON incomplete after {context.config.max_retries} attempts: {validation_result.get('error', 'Unknown error')}"
                        print(f'  ERROR: {error_msg}')
                        
                        # Add retry guidance info to parameters for logging
                        log_params = adjusted_params.copy()
                        if retry_guidance:
                            log_params['retry_guidance_used'] = error_type
                        
                        context.performance_monitor.log_extraction(
                            filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                            usage_metadata=response.usage_metadata, success=False, error_message=error_msg,
                            api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
                            model=context.config.model, parameters=log_params
                        )
                        return None
                    
                    # Add delay before retrying incomplete JSON (same as handle_llm_errors logic)
                    # Cap delay at attempt 4 (retry 5) - keep steady after retry 5
                    wait_time = 30 * 2 ** min(attempt, 4)
                    print(f'  Attempt {attempt + 1} failed (incomplete JSON), retrying in {wait_time} seconds...')
                    time.sleep(wait_time)
                    
                    # Continue to next retry attempt
                    continue
                
                # Validate response schema
                if not validate_response_schema(content, filename):
                    print(f'  WARNING: Response schema validation failed for {filename}')
                
                # Add retry guidance info to parameters for logging
                log_params = adjusted_params.copy()
                if retry_guidance:
                    log_params['retry_guidance_used'] = error_type
                
                context.performance_monitor.log_extraction(
                    filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                    usage_metadata=response.usage_metadata, success=True, api_key_used=context.key_number,
                    process_id=context.process_id, cao_number=cao_number,
                    model=context.config.model, parameters=log_params
                )
                
                cleanup_uploaded_file(context.client, uploaded_file)
                return content
            else:
                # Log detailed failure information
                processing_time = time.time() - start_time
                failure_info = {
                    "finish": extraction_info.get("finish", "UNKNOWN"),
                    "safety": extraction_info.get("safety", []),
                    "content": "",
                    "processing_time": processing_time,
                    "api_key": context.key_number,
                    "process_id": context.process_id
                }
                log_detailed_failure(failure_info, filename, attempt)
                raise ValueError(f'No content returned by model - Finish reason: {extraction_info.get("finish", "UNKNOWN")}')
                
        except Exception as e:
            # Clean up uploaded file on error
            if uploaded_file:
                cleanup_uploaded_file(context.client, uploaded_file)
            
            error_str = str(e).lower()
            print(f'  DEBUG: Markdown upload error type: {type(e).__name__}, Error message: {error_str}')
            print(f'  DEBUG: Full error details: {e}')
            
            # Log detailed failure information for all failures
            processing_time = time.time() - start_time
            failure_info = {
                "finish": "UNKNOWN",
                "safety": [],
                "content": "",
                "processing_time": processing_time,
                "api_key": context.key_number,
                "process_id": context.process_id
            }
            
            # Try to extract response information if available
            if 'response' in locals() and hasattr(response, 'candidates') and response.candidates:
                cand = response.candidates[0]
                failure_info["finish"] = getattr(cand, 'finish_reason', 'UNKNOWN')
                if hasattr(cand, 'safety_ratings') and cand.safety_ratings is not None:
                    failure_info["safety"] = [(r.category, r.probability) for r in cand.safety_ratings]
            
            # Log token usage if available
            if 'response' in locals() and hasattr(response, 'usage_metadata') and response.usage_metadata:
                print(f'  DEBUG: Token usage - Input: {response.usage_metadata.prompt_token_count}, Output: {response.usage_metadata.candidates_token_count}')
            
            # Log detailed failure analysis
            log_detailed_failure(failure_info, filename, attempt)
            
            if hasattr(e, '__traceback__'):
                import traceback
                print(f'  DEBUG: Traceback: {traceback.format_exc()}')
            
            # Store error message for retry guidance
            last_error_message = str(e)
            
            # Handle retry logic
            if handle_llm_errors(e, attempt, context.config.max_retries, file_size_mb, context, remaining_budget_s):
                        continue
            else:
                processing_time = time.time() - start_time
                print(f'  Markdown upload failed after {context.config.max_retries} attempts')
                
                # Add retry guidance info to parameters for logging
                log_params = adjusted_params.copy()
                if retry_guidance:
                    log_params['retry_guidance_used'] = error_type
                
                context.performance_monitor.log_extraction(
                    filename=filename, file_size_mb=file_size_mb, processing_time=processing_time,
                    usage_metadata=None, success=False, error_message=f'Failed after {context.config.max_retries} attempts: {str(e)}',
                    api_key_used=context.key_number, process_id=context.process_id, cao_number=cao_number,
                    model=context.config.model, parameters=log_params
                )
                return None
    
    return None


# =============================================================================
# FILE PROCESSING FUNCTIONS
# =============================================================================
# Functions for processing individual files and managing the processing workflow
def process_single_file(markdown_file: Path, cao_number: str, output_folder: Path, 
                       context: ProcessingContext, total_files: int, hang_threshold: int = 1500, heartbeat_interval: int = 300) -> bool:
    """Process a single markdown file end-to-end."""
    # Generate output filename
    output_filename = f"{markdown_file.stem}_extract.json"
    output_file = output_folder / output_filename
    
    # Check if already processed (do this BEFORE creating watchdog thread to avoid overhead)
    if output_file.exists():
        print(f'  {cao_number}: Skipping {markdown_file.name} (already processed)')
        # Don't count already processed files toward the limit
        return True
    
    # Initialize file timing and debug
    file_start = time.time()
    context.file_start_ts = file_start
    context.debug.clear()
    context.live_escalated = False
    
    # Start heartbeat watchdog (only for files that will be processed)
    stop_event = threading.Event()
    watchdog_thread = threading.Thread(
        target=heartbeat_watchdog,
        args=(context, hang_threshold, heartbeat_interval, stop_event),
        daemon=True
    )
    watchdog_thread.start()
    
    # Check file limit (only count successful extractions)
    if context.stats.successful_extractions >= context.config.max_files:
        return False
    
    # Try to acquire lock
    if not acquire_file_lock(output_file, context):
        print(f'  {cao_number}: Skipping {markdown_file.name} (being processed by another process)')
        time.sleep(2)
        return True
    
    try:
        # Validate markdown file
        is_valid, quality_message = validate_markdown_file(str(markdown_file))
        if not is_valid:
            print(f'  {cao_number}: ✗ Markdown quality check failed for {markdown_file.name}: {quality_message} [API {context.key_number}/{context.total_processes}]')
            log_processing_result(markdown_file.name, False, context, f"Markdown quality: {quality_message}")
            return True
        
        # Check file size
        file_size_mb = os.path.getsize(markdown_file) / (1024 * 1024)
        if file_size_mb > 50.0:
            print(f'  {cao_number}: ✗ Markdown file too large ({file_size_mb:.1f}MB) - exceeds 50MB limit for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
            log_processing_result(markdown_file.name, False, context, f"Markdown too large: {file_size_mb:.1f}MB")
            return True
        
        print(f'  {cao_number}: {markdown_file.name} (Markdown: {file_size_mb:.1f}MB) [API {context.key_number}/{context.total_processes}]')
        
        # Check per-file deadline (skip for first processed file)
        deadline_s = int(context.config.max_processing_hours * 3600)
        enforce_deadline = (context.stats.processed_files > 0)
        
        if enforce_deadline:
            remaining = deadline_s - (time.time() - file_start)
            if remaining <= 0:
                print(f'  {cao_number}: ⏰ Timeout after {context.config.max_processing_hours} hours for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
                context.stats.add_timeout(markdown_file.name)
                timeout_log_path = 'outputs/logs/timed_out_files_llm_extraction.txt'
                with open(timeout_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - API {context.key_number}: {markdown_file.name}\n")
                return True
        else:
            remaining = None
        
        # Extract content
        extraction_start = time.time()
        raw_output = extract_with_markdown_upload(str(markdown_file), markdown_file.name, cao_number, context, remaining)
        extraction_time = time.time() - extraction_start
        
        # Check if daily quota was hit during extraction
        if context.process_id in process_quota_flags and process_quota_flags[context.process_id]:
            print(f'  🛑 DAILY QUOTA LIMIT REACHED for Process {context.process_id} - Stopping this process')
            print(f'  💡 Wait until tomorrow to continue')
            return False  # Stop processing this process only
        
        if not raw_output:
            print(f'  {cao_number}: ✗ LLM extraction failed for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
            context.debug.flush()  # Show debug info on failure
            log_processing_result(markdown_file.name, False, context)
            return True
        
        # Save result with validation
        save_success = save_extraction_result(output_file, raw_output)
        if save_success:
            print(f'  {cao_number}: LLM extraction completed in {extraction_time:.2f} seconds [API {context.key_number}/{context.total_processes}]')
            # Mark as successful after saving
            context.stats.add_success(markdown_file.name)
        else:
            print(f'  {cao_number}: ✗ JSON validation failed, extraction not saved for {markdown_file.name} [API {context.key_number}/{context.total_processes}]')
            context.debug.flush()  # Show debug info on failure
            log_processing_result(markdown_file.name, False, context, "JSON validation failed - incomplete or invalid JSON")
            return True
        
        # Update progress
        if context.stats.processed_files % 10 == 0:
            context.performance_monitor.print_progress(total_files)
        
        # Add delay between files
        time.sleep(context.config.delay_between_files)
        
        return True
        
    except Exception as e:
        import traceback
        print(f'  {cao_number}: Error with {markdown_file.name}: {e} [API {context.key_number}/{context.total_processes}]')
        context.debug.flush()  # Show debug info on failure
        traceback.print_exc()
        log_processing_result(markdown_file.name, False, context, str(e))
        return True
    finally:
        # Stop the heartbeat watchdog
        stop_event.set()
        watchdog_thread.join(timeout=1)  # Give it 1 second to stop gracefully
        release_file_lock(output_file)


# =============================================================================
# RESULTS & DISPLAY FUNCTIONS
# =============================================================================
# Functions for displaying final results and performance analysis
def cleanup_announce_files(context: ProcessingContext):
    """Clean up announce files and lock files created during processing."""
    try:
        # Clean up announce files
        announce_files = list(context.config.output_folder.glob('.cao_*_announced'))
        cleaned_count = 0
        for announce_file in announce_files:
            try:
                announce_file.unlink(missing_ok=True)
                cleaned_count += 1
                print(f'  🧹 Cleaned up announce file: {announce_file.name}')
            except Exception as e:
                # Ignore errors (file might have been deleted by another process)
                pass
        if cleaned_count > 0:
            print(f'  🧹 Cleaned up {cleaned_count} announce files')
        
        # Clean up stale lock files
        lock_files_found = 0
        current_time = time.time()
        ttl_seconds = LOCK_TTL_HOURS * 3600
        
        for lock_file in context.config.output_folder.rglob('*.json.lock'):
            try:
                if current_time - lock_file.stat().st_mtime > ttl_seconds:
                    lock_file.unlink(missing_ok=True)
                    lock_files_found += 1
            except Exception:
                pass  # Ignore errors on individual files (file might be deleted by another process)
        
        if lock_files_found > 0:
            print(f'  🧹 Cleaned up {lock_files_found} stale lock files')
            
    except Exception as e:
        print(f'  ⚠️  Warning: Failed to clean up files: {e}')


def display_final_results(context: ProcessingContext, quota_exhausted: bool = False):
    """Display final processing results."""
    if quota_exhausted:
        print(f'\n⚠️  Process {context.process_id + 1} STOPPED due to DAILY QUOTA EXHAUSTION:')
        print(f'   💡 Daily limit: 3,000,000 tokens per day')
        print(f'   💡 Quota resets at midnight (Google timezone)')
        print(f'   💡 Process will need to be restarted tomorrow to continue')
    else:
        print(f'Process {context.process_id + 1} completed:')
    
    print(f'  📊 Files processed: {context.stats.processed_files}')
    print(f'  ✅ Successful extractions: {context.stats.successful_extractions}')
    print(f'  ❌ Failed extractions: {len(context.stats.failed_files)}')
    print(f'  ⏰ Timed out: {len(context.stats.timed_out_files)}')
    
    if context.stats.failed_files:
        print(f'  📝 Failed files: {context.stats.failed_files}')
    if context.stats.timed_out_files:
        print(f'  📝 Timed out files: {context.stats.timed_out_files}')
    
    print('\n' + '=' * 60)
    if quota_exhausted:
        print('FINAL PERFORMANCE ANALYSIS (QUOTA EXHAUSTED - INCOMPLETE)')
    else:
        print('FINAL PERFORMANCE ANALYSIS')
    print('=' * 60)
    # Pass total input files count for comparison
    all_markdown_files = discover_markdown_files(context.config.input_folder)
    total_input_files = len(all_markdown_files)
    context.performance_monitor.analyze_performance(total_input_files=total_input_files)
    context.performance_monitor.update_summary_file()
    print(f"""📁 Performance data saved to:""")
    print(f'   Detailed logs: {context.performance_monitor.log_file}')
    print(f'   Summary: {context.performance_monitor.summary_file}')
    
    # Clean up announce files
    cleanup_announce_files(context)


# =============================================================================
# MAIN PIPELINE ORCHESTRATION
# =============================================================================
# Main function that orchestrates the entire extraction pipeline
def run_extraction_pipeline():
    """Main pipeline orchestration."""
    process_id = None  # Initialize for exception handling
    current_file = None  # Track current file being processed
    total_files = 0
    context = None  # Track context for stats
    
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='CAO LLM Extraction Pipeline')
        parser.add_argument('--key_number', type=int, default=1, help='API key number (1-3)')
        parser.add_argument('--process_id', type=int, default=0, help='Process ID for parallel processing')
        parser.add_argument('--total_processes', type=int, default=1, help='Total number of parallel processes')
        parser.add_argument('--max_files', type=int, help='Maximum number of files to process')
        parser.add_argument('--verbose', action='store_true', help='Stream debug output live')
        parser.add_argument('--debug_on_failure', action='store_true', default=True, help='Flush debug buffer on failures')
        parser.add_argument('--hang_threshold', type=int, default=600, help='Seconds before enabling live debug on long stages')
        parser.add_argument('--heartbeat', type=int, default=90, help='Heartbeat interval in seconds for long stages')
        
        args = parser.parse_args()
        
        key_number = args.key_number
        process_id = args.process_id
        total_processes = args.total_processes
        
        # Load configuration
        config = load_configuration()
        
        # Override max_files if provided as argument
        if args.max_files is not None:
            config.max_files = args.max_files
        
        # Validate paths (pass process_id to avoid race conditions in parallel execution)
        validate_input_paths(config, process_id)
        
        # Setup processing context
        context = setup_processing_context(config, process_id, total_processes, key_number, args.verbose)
        
        # Clean up announce files from previous runs at the beginning
        cleanup_announce_files(context)
        
        # Discover files
        all_markdown_files = discover_markdown_files(config.input_folder)
        filtered_files = filter_files_for_processing(all_markdown_files, context)
        total_files = len(filtered_files)
        
        print(f"🎯 CAO Markdown Extraction Pipeline")
        print(f"📊 Process: {process_id + 1}/{total_processes}")
        print(f"🔑 API Key: {context.key_number}")
        print(f"📁 Input: {config.input_folder}")
        print(f"📁 Output: {config.output_folder}")
        print(f"📄 Files to process: {total_files}")
        print()
        
        # Process files
        quota_exhausted = False
        for cao_folder, markdown_file in filtered_files:
            cao_number = cao_folder.name
            current_file = f"{cao_number}/{markdown_file.name}"  # Track current file
            output_folder = config.output_folder / cao_number
            output_folder.mkdir(exist_ok=True)
            
            # Check if quota was exhausted before processing this file
            if process_id in process_quota_flags and process_quota_flags[process_id]:
                quota_exhausted = True
                print(f'\n🛑 DAILY QUOTA LIMIT REACHED for Process {process_id + 1} - Stopping before processing remaining files')
                print(f'   📄 Next file would have been: {current_file}')
                break
            
            # Announce CAO once
            announce_cao_once(cao_number, context)
            
            # Process file (retry logic is handled inside process_single_file/extract_with_markdown_upload)
            should_continue = process_single_file(markdown_file, cao_number, output_folder, context, total_files, args.hang_threshold, args.heartbeat)
            if not should_continue:
                # Check if it stopped due to quota exhaustion
                if process_id in process_quota_flags and process_quota_flags[process_id]:
                    quota_exhausted = True
                break
        
        # Display final results
        display_final_results(context, quota_exhausted=quota_exhausted)
        
    except KeyboardInterrupt:
        process_id_str = f"Process {process_id + 1}" if process_id is not None else "Process ?"
        print(f'\n⚠️  {process_id_str} interrupted by user')
        if current_file:
            print(f'   📄 Was processing: {current_file}')
        if context and hasattr(context, 'stats'):
            print(f'   📊 Progress: {context.stats.successful_extractions} successful, {len(context.stats.failed_files)} failed')
        sys.exit(0)
    except Exception as e:
        import traceback
        process_id_str = f"Process {process_id + 1}" if process_id is not None else "Process ?"
        error_str = str(e).lower()
        
        # Check if it's a known retryable error that should NOT stop the process
        # These errors are normally handled by retry logic, but if they escape, we should continue
        is_retryable_error = (
            ('429' in error_str and 'perday' not in error_str and 'daily' not in error_str and '3000000' not in error_str) or  # Per-minute quota (not daily)
            ('503' in error_str or 'unavailable' in error_str or 'overloaded' in error_str) or  # Service unavailable
            ('timeout' in error_str or 'deadline' in error_str) or  # Timeout
            ('truncated' in error_str or 'incomplete json' in error_str or 'json validation failed' in error_str) or  # JSON issues
            ('no content parts found' in error_str or 'no content' in error_str)  # Empty response
        )
        
        # Check if it's a daily quota (should stop process)
        is_daily_quota = (
            ('429' in error_str or 'quota' in error_str) and 
            ('perday' in error_str or 'daily' in error_str or '3000000' in error_str)
        )
        
        # Check if it's a fatal error (configuration, file system, etc.)
        is_fatal_error = (
            'valueerror' in error_str and ('input folder' in error_str or 'output folder' in error_str or 'api key' in error_str) or
            'filenotfounderror' in error_str or
            'permissionerror' in error_str or
            'keyboardinterrupt' in error_str
        )
        
        if is_retryable_error and not is_daily_quota:
            # This is a retryable error that occurred during setup/configuration (not file processing)
            # Since it's during setup, we can't continue - exit gracefully
            print(f'\n⚠️  RETRYABLE ERROR during setup in {process_id_str}')
            print(f'📋 Error Type: {type(e).__name__}')
            print(f'📋 Error Message: {str(e)[:200]}...' if len(str(e)) > 200 else f'📋 Error Message: {e}')
            
            print(f'\n💡 This is a retryable error (API quota/timeout/service unavailable) that occurred during setup.')
            print(f'   The process will exit, but you can restart it to continue processing.')
            print(f'   Other parallel processes will continue running.')
            
            # Log the error as retryable
            try:
                error_log_path = 'outputs/logs/fatal_errors_llm_extraction.txt'
                Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
                with open(error_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {process_id_str} - RETRYABLE ERROR (during setup)\n")
                    f.write(f"Error Type: {type(e).__name__}\n")
                    f.write(f"Error Message: {e}\n")
                    f.write(f"Note: This is a retryable error - process can be restarted\n\n")
            except Exception:
                pass
            
            # Exit gracefully with code 0 (not fatal)
            sys.exit(0)
        
        # For daily quota or fatal errors, exit gracefully
        print(f'\n{"="*80}')
        print(f'❌ FATAL ERROR in {process_id_str}')
        print(f'{"="*80}')
        print(f'📋 Error Type: {type(e).__name__}')
        print(f'📋 Error Message: {e}')
        
        if current_file:
            print(f'\n📄 File being processed when error occurred: {current_file}')
        
        if context and hasattr(context, 'stats'):
            print(f'\n📊 Progress Summary:')
            print(f'   ✅ Successful: {context.stats.successful_extractions} files')
            print(f'   ❌ Failed: {len(context.stats.failed_files)} files')
            if context.stats.failed_files:
                print(f'   📝 Failed files: {context.stats.failed_files[-5:]}')  # Show last 5 failed files
            if total_files > 0:
                remaining = total_files - context.stats.processed_files
                if remaining > 0:
                    print(f'   ⏸️  Remaining: {remaining} files not processed')
        
        # Provide context about error type
        if is_daily_quota:
            print(f'\n💡 Error Type: Daily API Quota Limit Reached')
            print(f'   This process has hit its daily quota limit and will stop.')
            print(f'   Other parallel processes will continue running.')
            print(f'   You can restart this process tomorrow when the quota resets.')
        elif is_fatal_error:
            print(f'\n💡 Error Type: Fatal Configuration/System Error')
            print(f'   This is a system-level error that prevents processing.')
            print(f'   Check configuration, file permissions, or API key setup.')
        else:
            print(f'\n💡 This appears to be an unexpected fatal error. Check the traceback below for details.')
        
        print(f'\n📋 Full Traceback:')
        print(f"{'-'*80}")
        traceback.print_exc()
        print(f"{'-'*80}")
        
        # Try to log the error
        try:
            error_log_path = 'outputs/logs/fatal_errors_llm_extraction.txt'
            Path(error_log_path).parent.mkdir(parents=True, exist_ok=True)
            with open(error_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{'='*80}\n")
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {process_id_str} - FATAL ERROR\n")
                f.write(f"{'='*80}\n")
                f.write(f"Error Type: {type(e).__name__}\n")
                f.write(f"Error Message: {e}\n")
                if current_file:
                    f.write(f"File being processed: {current_file}\n")
                if context and hasattr(context, 'stats'):
                    f.write(f"Progress: {context.stats.successful_extractions} successful, {len(context.stats.failed_files)} failed\n")
                f.write(f"\nTraceback:\n")
                f.write(f"{traceback.format_exc()}\n\n")
        except Exception:
            pass  # Don't fail on logging failure
        
        # Exit with code 0 instead of 1 to prevent all processes from stopping
        # The error is logged, so we can investigate without crashing the entire pipeline
        print(f'\n⚠️  {process_id_str} exiting gracefully (error logged to outputs/logs/fatal_errors_llm_extraction.txt)')
        print(f'💡 Other parallel processes will continue running independently.')
        sys.exit(0)


# =============================================================================
# ENTRY POINT
# =============================================================================
# Main entry point for the script
def main():
    """Main entry point for the LLM extraction pipeline (markdown version)."""
    # Setup signal handlers first to prevent unexpected exits
    setup_signal_handlers()
    
    # Ensure stdout/stderr are line-buffered when piped (not a TTY)
    # This helps prevent issues when writing to tee/unbuffer
    if not sys.stdout.isatty():
        # When piped (e.g., to tee), set line buffering for better behavior
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            # Python < 3.7 or reconfigure not available - use flush() calls instead
            pass
        try:
            sys.stderr.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            pass
    
    run_extraction_pipeline()


if __name__ == "__main__":
    main()
