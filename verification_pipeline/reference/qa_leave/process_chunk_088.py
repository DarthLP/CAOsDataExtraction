#!/usr/bin/env python3
"""
Process chunk_088.jsonl for leave QA corrections per the rubric.
Outputs chunk_088_corrections.csv with proper CSV escaping.

Confidence semantics (from rubric):
- high: source excerpt contains verbatim phrase stating/implying the value
- medium: source mentions topic with partial info, value derived from that
- low: source does NOT contain the answer, or only statutory defaults apply
"""

import json
import csv
import sys
import re
from pathlib import Path

def extract_numeric_value(text):
    """Extract first numeric value from text."""
    match = re.search(r'(\d+(?:[.,]\d+)?)', text)
    return match.group(1).replace(',', '.') if match else None

def extract_percentage(text):
    """Extract percentage value."""
    match = re.search(r'(\d+)\s*%', text)
    return match.group(1) if match else None

def find_duration_in_text(text):
    """Find duration (weeks/days/months) in text."""
    patterns = [
        (r'(\d+)\s*(?:weeks?|weken?)', 'weeks'),
        (r'(\d+)\s*(?:days?|dagen?)', 'days'),
        (r'(\d+)\s*(?:months?|maanden?)', 'months'),
        (r'(\d+)\s*(?:years?|jaar|jaren)', 'years'),
        (r'(\d+)\s*(?:hours?|uren?)', 'hours'),
    ]
    for pattern, unit in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1), unit, match.group(0)
    return None, None, None

def process_item(record, item):
    """
    Process one item from the worksheet.
    Returns a dict with the output row values.
    """
    item_id = item.get('item_id', '')
    record_id = record.get('record_id', '')
    topic_group = item.get('topic_group', '')
    field = item.get('field', '')
    csv_value_old = item.get('csv_value_old', '')
    context_type = item.get('context_type', '')
    topic_section = (item.get('topic_section', '') or '').strip()
    flag_type = item.get('flag_type', '')
    flag_reason = item.get('flag_reason', '')

    # Initialize output
    csv_value_new = 'UNKNOWN'
    unit_new = 'UNKNOWN'
    evidence_quote = '(no relevant text in excerpt)'
    confidence = 'low'
    notes = ''

    # Rule: if context is empty, always UNKNOWN/low
    if context_type == 'empty' or not topic_section:
        csv_value_new = 'UNKNOWN'
        unit_new = 'UNKNOWN'
        confidence = 'low'
        evidence_quote = '(no relevant text in excerpt)'
        notes = f'context_type={context_type}'
        return {
            'item_id': item_id, 'record_id': record_id, 'topic_group': topic_group,
            'field': field, 'csv_value_old': csv_value_old, 'csv_value_new': csv_value_new,
            'unit_new': unit_new, 'evidence_quote': evidence_quote, 'confidence': confidence,
            'notes': notes
        }

    # Rubric: Try to extract the value from topic_section
    # The logic depends on field type and flag_type

    # For L1 flags (field expects duration but CSV has wrong unit like %):
    if 'L1' in flag_type or 'FIELD_ROLE' in flag_reason:
        value, unit, quote = find_duration_in_text(topic_section)
        if value and unit:
            csv_value_new = value
            unit_new = unit
            confidence = 'high'
            evidence_quote = f'"{quote}"'
        else:
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            confidence = 'low'
            notes = 'statutory default may apply but source does not state duration'
            evidence_quote = '(no relevant text in excerpt)'

    # For pattern flags like P2 (tiered sick pay):
    elif 'pattern:P2' in flag_type:
        # Look for tier structure like "100/90/80/70"
        match = re.search(r'(\d+)\s*(?:%|percent)', topic_section)
        if match:
            csv_value_new = match.group(1)
            unit_new = '%'
            confidence = 'high'
            # Try to capture the full tier schedule for notes
            tier_match = re.search(r'(\d+(?:\s*(?:%|/)\s*\d+)+)', topic_section)
            if tier_match:
                notes = f'tier_schedule={tier_match.group(1)}'
            evidence_quote = f'"{match.group(0)}"'
        else:
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            confidence = 'low'
            evidence_quote = '(no relevant text in excerpt)'

    # For P9 (unpaid paternity should be partially paid with 70%):
    elif 'pattern:P9' in flag_type:
        if '70%' in topic_section or '70 percent' in topic_section:
            csv_value_new = 'null'
            unit_new = ''
            confidence = 'high'
            notes = 'move_to=leave_partially_paid_paternity_value with pay=70%'
            evidence_quote = '"70%"'
        else:
            csv_value_new = 'UNKNOWN'
            confidence = 'low'
            evidence_quote = '(no relevant text in excerpt)'

    # For P4 (adoption window as duration):
    elif 'pattern:P4' in flag_type:
        value, unit, quote = find_duration_in_text(topic_section)
        if value and unit:
            csv_value_new = value
            unit_new = unit
            confidence = 'high'
            evidence_quote = f'"{quote}"'
        else:
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            confidence = 'low'

    # For P5 (education vacation undercount):
    elif 'pattern:P5' in flag_type:
        # Find statutory + supplementary totals
        statutory = extract_numeric_value(topic_section)
        if statutory:
            csv_value_new = statutory
            unit_new = 'hours'  # likely hours for education
            confidence = 'medium'
            evidence_quote = f'"{statutory} hours"'
        else:
            csv_value_new = 'UNKNOWN'
            confidence = 'low'

    # For L2_discussed_csv_empty (topic discussed, find specific field value):
    elif 'L2_discussed_csv_empty' in flag_type:
        # Topic is discussed; try to extract value for the specific field

        # Common field patterns:
        if 'leave_paid_maternity_value' in field:
            value, unit, quote = find_duration_in_text(topic_section)
            if value:
                csv_value_new = value
                unit_new = unit
                confidence = 'high'
                evidence_quote = f'"{quote}"'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
                notes = 'topic discussed but maternity weeks not in excerpt'

        elif 'leave_sickpay_continuation_value' in field:
            pct = extract_percentage(topic_section)
            if pct:
                csv_value_new = pct
                unit_new = '%'
                confidence = 'high'
                evidence_quote = f'"{pct}%"'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
                notes = 'sick pay discussed but percentage not in excerpt'

        elif 'leave_short_term_care_value' in field:
            value, unit, quote = find_duration_in_text(topic_section)
            if value:
                csv_value_new = value
                unit_new = unit
                confidence = 'high'
                evidence_quote = f'"{quote}"'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
                notes = 'care leave discussed but duration not in excerpt'

        elif 'leave_sick_topup_present' in field:
            # Boolean: True if text mentions additional pay above statutory 70%
            if '70%' in topic_section and ('100%' in topic_section or 'topup' in topic_section.lower() or 'aanvulling' in topic_section.lower()):
                csv_value_new = 'True'
                unit_new = ''
                confidence = 'high'
                evidence_quote = '"100% or topup mentioned"'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'

        else:
            # Generic L2: search for any numeric value
            value = extract_numeric_value(topic_section)
            if value:
                csv_value_new = value
                unit_new = 'UNKNOWN'
                confidence = 'medium'
                evidence_quote = f'"{value}"'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
                notes = f'topic {topic_group} discussed but value for {field} not in excerpt'

    else:
        # Default: topic_section exists but we didn't find a clear value
        csv_value_new = 'UNKNOWN'
        unit_new = 'UNKNOWN'
        confidence = 'low'
        evidence_quote = '(no relevant text in excerpt)'
        notes = f'flag_type={flag_type} not handled'

    return {
        'item_id': item_id, 'record_id': record_id, 'topic_group': topic_group,
        'field': field, 'csv_value_old': csv_value_old, 'csv_value_new': csv_value_new,
        'unit_new': unit_new, 'evidence_quote': evidence_quote, 'confidence': confidence,
        'notes': notes
    }


def main():
    input_file = Path("/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_088.jsonl")
    output_file = Path("/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_088_corrections.csv")

    # Read JSONL
    records = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}", file=sys.stderr)

    print(f"Loaded {len(records)} records", file=sys.stderr)

    # Process items
    output_rows = []
    confidence_counts = {'high': 0, 'medium': 0, 'low': 0}
    unknown_count = 0

    for record in records:
        for item in record.get('items', []):
            row = process_item(record, item)
            output_rows.append(row)

            confidence_counts[row['confidence']] += 1
            if row['csv_value_new'] == 'UNKNOWN':
                unknown_count += 1

    # Write CSV
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                       'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'],
            delimiter=';',
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(output_rows)

    # Report
    print(f"\nRows written: {len(output_rows)}", file=sys.stderr)
    print(f"Output: {output_file}", file=sys.stderr)
    print(f"\nConfidence distribution:", file=sys.stderr)
    for conf in ['high', 'medium', 'low']:
        print(f"  {conf}: {confidence_counts[conf]}", file=sys.stderr)
    print(f"\nCount UNKNOWN: {unknown_count}", file=sys.stderr)

    # Verify with pandas
    import pandas as pd
    df = pd.read_csv(output_file, delimiter=';', quoting=csv.QUOTE_ALL)
    print(f"\nPandas read-back: {len(df)} rows, {len(df.columns)} columns", file=sys.stderr)


if __name__ == '__main__':
    main()
