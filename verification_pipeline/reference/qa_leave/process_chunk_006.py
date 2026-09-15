#!/usr/bin/env python3
"""
Topic-presence check for Dutch CAO QA - chunk_006
Implements the rubric from leave_qa_presence_prompt.md
"""

import json
import csv
import re
from pathlib import Path
from collections import defaultdict

# Topic phrase mappings from rubric
TOPIC_PHRASES = {
    'general': [
        'general leave enhancements',
        'minimum-cao',
        'minimum regulation',
        'minimum provision'
    ],
    'maternity': [
        'maternity leave',
        'zwangerschap',
        'bevallingsverlof',
        'pregnancy leave',
        'pregnancy and maternity'
    ],
    'paternity': [
        'paternity',
        'partner leave',
        'geboorteverlof',
        'birth leave',
        'wieg',
        'aanvullend geboorteverlof',
        'birth of'
    ],
    'adoption': [
        'adoption',
        'foster',
        'adoptieverlof',
        'pleegzorgverlof'
    ],
    'parental': [
        'parental leave',
        'ouderschapsverlof'
    ],
    'sick': [
        'sickness',
        'sick pay',
        'incapacity for work',
        'loondoorbetaling',
        'wga',
        'iva',
        'first year of sickness',
        'second year of'
    ],
    'care': [
        'care leave',
        'zorgverlof',
        'short-term care',
        'long-term care',
        'kortdurend',
        'langdurend',
        'terminal care',
        'end-of-life',
        'calamiteit',
        'kort verzuim'
    ],
    'vacation_holidays': [
        'vacation',
        'holiday allowance',
        'vakantie',
        'vakantiegeld',
        'vakantietoeslag',
        'liberation day',
        'public holiday',
        'feestdag'
    ],
    'seniority_special': [
        'special leave',
        'seniorendagen',
        'senior days',
        'extra leave for older',
        'extra vacation for older',
        'functioneel leeftijdsontslag',
        'age-based',
        'jubilee'
    ]
}

# CSV field mappings per topic_group
TOPIC_CSV_FIELDS = {
    'general': [
        'has_leave_enhancements',
        'general_statutory_ref',
        'general_schedule',
        'general_comp_note'
    ],
    'maternity': [
        'maternity_present',
        'maternity_value',
        'maternity_unit',
        'maternity_note',
        'has_above_statutory_maternity',
        'maternity_explicitly_above_statutory',
        'maternity_statutory_ref'
    ],
    'paternity': [
        'paternity_present',
        'paternity_value',
        'paternity_unit',
        'paternity_note',
        'paternity_explicitly_above_statutory',
        'paternity_statutory_ref'
    ],
    'adoption': [
        'adoption_present',
        'adoption_value',
        'adoption_unit',
        'adoption_note',
        'adoption_explicitly_above_statutory',
        'adoption_statutory_ref'
    ],
    'parental': [
        'parental_present',
        'parental_value',
        'parental_unit',
        'parental_note',
        'parental_explicitly_above_statutory',
        'parental_statutory_ref'
    ],
    'sick': [
        'sick_present',
        'sick_value',
        'sick_unit',
        'sick_note',
        'sick_explicitly_above_statutory',
        'sick_statutory_ref'
    ],
    'care': [
        'care_present',
        'care_value',
        'care_unit',
        'care_note',
        'care_explicitly_above_statutory',
        'care_statutory_ref'
    ],
    'vacation_holidays': [
        'vacation_days',
        'vacation_note',
        'holiday_allowance_percent',
        'holiday_allowance_note',
        'liberation_day_present',
        'liberation_day_schedule',
        'extra_seniority_present'
    ],
    'seniority_special': [
        'extra_seniority_present',
        'seniority_value',
        'seniority_unit',
        'seniority_note'
    ]
}

ALL_TOPIC_GROUPS = [
    'general', 'maternity', 'paternity', 'adoption', 'parental',
    'sick', 'care', 'vacation_holidays', 'seniority_special'
]


def check_topic_in_source(source_text, topic_group):
    """
    Check if any phrase for this topic_group appears in source_text (case-insensitive).
    Returns (True/False, list of matched phrases)
    """
    if not source_text:
        return False, []

    source_lower = source_text.lower()
    phrases = TOPIC_PHRASES.get(topic_group, [])
    matched = []

    for phrase in phrases:
        if phrase.lower() in source_lower:
            matched.append(phrase)

    return len(matched) > 0, matched


def check_csv_has_data(csv_data, topic_group):
    """
    Check if any field in this topic_group is populated above default.
    Returns (True/False, list of populated field names)
    """
    if not csv_data:
        return False, []

    fields = TOPIC_CSV_FIELDS.get(topic_group, [])
    populated = []

    for field_name in fields:
        value = csv_data.get(field_name)

        # Boolean fields - True means populated
        if field_name.endswith(('_present', '_explicitly_above_statutory', '_statutory_ref',
                                '_exceptions', '_eligibility_present', '_topup_present',
                                '_extra_insurance_present', '_annual', '_lustrum',
                                '_annual_lustrum', '_has_date_bonus', '_annual_seniority_increase')):
            if isinstance(value, bool) and value is True:
                populated.append(field_name)
        # Fields with boolean-like values
        elif field_name in ['liberation_day_present', 'extra_seniority_present',
                           'has_leave_enhancements', 'has_above_statutory_maternity',
                           'paternity_explicitly_above_statutory', 'abortion_present']:
            if isinstance(value, bool) and value is True:
                populated.append(field_name)
        # Numeric or string fields - non-null and non-empty
        else:
            if value is not None and value != '' and str(value).strip():
                populated.append(field_name)

    return len(populated) > 0, populated


def determine_presence_status(topic_in_source, csv_has_data):
    """
    Determine presence_status from two booleans.
    """
    if topic_in_source and csv_has_data:
        return 'discussed'
    elif topic_in_source and not csv_has_data:
        return 'discussed_csv_empty'
    elif not topic_in_source and csv_has_data:
        return 'unsupported_csv'
    else:
        return 'not_applicable'


def process_chunk(input_path, output_path):
    """
    Process chunk_006.jsonl and write presence analysis to CSV.
    """
    records_processed = 0
    rows_written = 0
    presence_counts = defaultdict(int)

    # Read input JSONL
    records = []
    with open(input_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    records_processed = len(records)
    print(f"Loaded {records_processed} records from {input_path}")

    # Process and write CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'record_id', 'cao_number', 'file_name', 'ingangsdatum',
                'general_document_type', 'topic_group', 'topic_in_source',
                'csv_has_data', 'presence_status', 'matched_phrases', 'populated_fields'
            ],
            delimiter=';',
            quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()

        # Process each record
        for record in records:
            # Extract common fields
            record_id = record.get('record_id', '')
            cao_number = record.get('cao_number', '')
            file_name = record.get('file_name', '')
            ingangsdatum = record.get('ingangsdatum', '')
            general_document_type = record.get('general_document_type', '')
            source_text = record.get('source_text', '')
            csv_data = record.get('csv_leave_fields', {})

            # Process each topic_group
            for topic_group in ALL_TOPIC_GROUPS:
                topic_in_source, matched = check_topic_in_source(source_text, topic_group)
                csv_has_data, populated = check_csv_has_data(csv_data, topic_group)
                presence_status = determine_presence_status(topic_in_source, csv_has_data)

                # Track distribution
                presence_counts[presence_status] += 1

                row = {
                    'record_id': record_id,
                    'cao_number': cao_number,
                    'file_name': file_name,
                    'ingangsdatum': ingangsdatum,
                    'general_document_type': general_document_type,
                    'topic_group': topic_group,
                    'topic_in_source': 'yes' if topic_in_source else 'no',
                    'csv_has_data': 'yes' if csv_has_data else 'no',
                    'presence_status': presence_status,
                    'matched_phrases': '|'.join(matched) if matched else '',
                    'populated_fields': '|'.join(populated) if populated else ''
                }

                writer.writerow(row)
                rows_written += 1

    return rows_written, presence_counts


if __name__ == '__main__':
    input_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_chunks/chunk_006.jsonl')
    output_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_chunks/chunk_006_presence.csv')

    rows, dist = process_chunk(input_file, output_file)

    print(f"\nResults:")
    print(f"  rows_written: {rows}")
    print(f"  output_path: {output_file}")
    print(f"\npresence_status distribution:")
    for status in ['discussed', 'discussed_csv_empty', 'unsupported_csv', 'not_applicable']:
        count = dist.get(status, 0)
        print(f"  {status}: {count}")
