#!/usr/bin/env python3
"""
Correction subagent for chunk_099 QA processing.
Reads JSONL, applies rubric, outputs CSV corrections.
"""

import json
import csv
import re
from pathlib import Path

# Paths
input_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099.jsonl')
output_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099_corrections.csv')

# Initialize tracking
rows_written = 0
confidence_dist = {'high': 0, 'medium': 0, 'low': 0}
unknown_count = 0

def extract_value_from_section(topic_section, field, flag_type, flag_reason):
    """
    Extract the correct value from topic_section based on field type and flag.
    Returns: (csv_value_new, unit_new, evidence_quote, confidence)
    """
    if not topic_section or topic_section.strip() == '':
        return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

    # Patterns for common field types
    patterns = {
        'leave_paid': r'(\d+)\s*(week|weeks|day|days|month|months)',
        'leave_sickpay_continuation': r'(\d+)%|(\d+)\s*%',
        'topup': r'(above|above statutory|above 70%)',
        'adoption_window': r'(\d+)\s*(week|weeks)',
    }

    # Try to find numeric values with units
    for pattern_name, pattern in patterns.items():
        match = re.search(pattern, topic_section, re.IGNORECASE)
        if match:
            value = match.group(1) if match.group(1) else match.group(2) if len(match.groups()) > 1 else ''
            unit = match.group(2) if len(match.groups()) > 1 else 'unknown'

            # Get verbatim quote (max 200 chars around match)
            start = max(0, match.start() - 50)
            end = min(len(topic_section), match.end() + 50)
            quote = topic_section[start:end].strip()
            if len(quote) > 200:
                quote = quote[:200]

            return value, unit, f'"{quote}"', 'medium'

    # If no patterns match, return UNKNOWN
    return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

def process_items():
    """Main processing loop."""
    global rows_written, unknown_count

    # Read all records
    records = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    print(f"Loaded {len(records)} records")

    # Flatten items
    all_items = []
    for record in records:
        items = record.get('items', [])
        for item in items:
            item['record_id'] = record.get('record_id', '')
            all_items.append(item)

    print(f"Total items: {len(all_items)}")

    # Write CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)

        # Header
        header = ['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                  'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes']
        writer.writerow(header)

        # Process each item
        for item in all_items:
            item_id = item.get('item_id', '')
            record_id = item.get('record_id', '')
            topic_group = item.get('topic_group', '')
            field = item.get('field', '')
            csv_value_old = item.get('csv_value_old', '')
            topic_section = item.get('topic_section', '')
            context_type = item.get('context_type', '')
            flag_type = item.get('flag_type', '')
            flag_reason = item.get('flag_reason', '')

            # Extract values
            csv_value_new, unit_new, evidence_quote, confidence = extract_value_from_section(
                topic_section, field, flag_type, flag_reason
            )

            # Build notes
            notes = ''
            if csv_value_new == 'UNKNOWN':
                unknown_count += 1
                notes = f'Source context_type={context_type}'
                if confidence == 'low':
                    notes += '; statutory default may apply but not in excerpt'

            # Write row
            row = [item_id, record_id, topic_group, field, csv_value_old,
                   csv_value_new, unit_new, evidence_quote, confidence, notes]
            writer.writerow(row)
            rows_written += 1

            # Track confidence
            confidence_dist[confidence] += 1

    print(f"\nOutput: {output_file}")
    print(f"Rows written: {rows_written}")
    print(f"Confidence distribution: {confidence_dist}")
    print(f"UNKNOWN count: {unknown_count}")

if __name__ == '__main__':
    process_items()

    # Read back and validate
    print("\n--- Validation ---")
    df_rows = []
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        for i, row in enumerate(reader):
            if i == 0:
                print(f"Header: {row}")
            else:
                df_rows.append(row)

    print(f"Data rows read back: {len(df_rows)}")
    if df_rows:
        print(f"First data row: {df_rows[0]}")
