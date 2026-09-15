#!/usr/bin/env python3
"""
QA Correction subagent for chunk_099.
Applies rubric from leave_qa_correction_prompt.md to extract values.
"""

import json
import csv
import re
from pathlib import Path

input_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099.jsonl')
output_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099_corrections.csv')

confidence_dist = {'high': 0, 'medium': 0, 'low': 0}
unknown_count = 0
rows_written = 0

def extract_evidence(topic_section, field, flag_type):
    """
    Extract value, unit, quote, and confidence from topic_section.
    Applies rubric rules:
    - high: verbatim quote from source that explicitly states or directly implies
    - medium: partial info that required derivation
    - low: not in source, UNKNOWN

    Returns: (csv_value_new, unit_new, evidence_quote, confidence)
    """

    if not topic_section or topic_section.strip() == '':
        return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

    # Handle common patterns by flag_type

    # P2_tiered_sick_collapse: keep first tier, note full schedule
    if 'P2_tiered' in flag_type:
        # Look for percentage values (100%, 70%, etc.) and "first year"/"second year"
        match_100 = re.search(r'(\d+)%\s+(of the last earned salary|of last earned salary)', topic_section)
        match_70 = re.search(r'second year.*?(\d+)%', topic_section)

        if match_100:
            value = match_100.group(1)
            # Get context around match
            start = max(0, match_100.start() - 30)
            end = min(len(topic_section), match_100.end() + 80)
            quote = topic_section[start:end].strip()
            return value, '%', f'"{quote}"', 'high'

    # L1 with care leave: looking for duration
    if 'CARE_02' in flag_type or (flag_type == 'L1' and 'care' in topic_section.lower()):
        # "short-term care leave" + "continued salary" = paid leave
        # But need duration value
        match_duration = re.search(r'(\d+)\s*(days?|weeks?|hours?)', topic_section, re.IGNORECASE)
        if match_duration:
            value = match_duration.group(1)
            unit = match_duration.group(2).lower()
            start = max(0, match_duration.start() - 40)
            end = min(len(topic_section), match_duration.end() + 40)
            quote = topic_section[start:end].strip()
            return value, unit, f'"{quote}"', 'high'
        else:
            # Topic mentions care leave but no duration in excerpt
            if 'continued salary payment' in topic_section:
                return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

    # L2_discussed_csv_empty with maternity
    if 'L2_discussed' in flag_type and 'maternity' in topic_section.lower():
        # Look for maternity-related values
        # Maternity typically: weeks of paid leave, pay percentages
        match_maternity = re.search(r'maternity.*?(\d+)\s*(weeks?|days?|months?)', topic_section, re.IGNORECASE)
        if not match_maternity:
            # Not in excerpt
            return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

        value = match_maternity.group(1)
        unit = match_maternity.group(2).lower()
        start = max(0, match_maternity.start() - 30)
        end = min(len(topic_section), match_maternity.end() + 60)
        quote = topic_section[start:end].strip()
        return value, unit, f'"{quote}"', 'high'

    # Adoption: look for duration in adoption context
    if 'adoption' in topic_section.lower():
        match = re.search(r'adoption.*?(\d+)\s*(weeks?|days?)', topic_section, re.IGNORECASE)
        if match:
            value = match.group(1)
            unit = match.group(2).lower()
            start = max(0, match.start() - 20)
            end = min(len(topic_section), match.end() + 50)
            quote = topic_section[start:end].strip()
            return value, unit, f'"{quote}"', 'high'

    # Generic numeric + unit pattern (fallback)
    match = re.search(r'(\d+)\s*(weeks?|days?|months?|years?|%|hours?)', topic_section, re.IGNORECASE)
    if match:
        value = match.group(1)
        unit = match.group(2).lower() if match.lastindex > 1 else ''
        start = max(0, match.start() - 40)
        end = min(len(topic_section), match.end() + 40)
        quote = topic_section[start:end].strip()
        if len(quote) > 200:
            quote = quote[:200]
        return value, unit, f'"{quote}"', 'medium'

    # No value found in excerpt
    return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

def process():
    global rows_written, unknown_count

    # Read JSONL
    records = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    print(f"Loaded {len(records)} records from chunk_099.jsonl")

    # Flatten items
    all_items = []
    for record in records:
        for item in record.get('items', []):
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

            # Extract values based on rubric
            csv_value_new, unit_new, evidence_quote, confidence = extract_evidence(
                topic_section, field, flag_type
            )

            # Build notes
            notes = ''
            if csv_value_new == 'UNKNOWN':
                unknown_count += 1
                notes = f'context_type={context_type}'
                if confidence == 'low':
                    notes += '; value not in excerpt'

            # Write row
            row = [item_id, record_id, topic_group, field, csv_value_old,
                   csv_value_new, unit_new, evidence_quote, confidence, notes]
            writer.writerow(row)
            rows_written += 1

            # Track confidence
            confidence_dist[confidence] += 1

    print(f"\n=== RESULTS ===")
    print(f"Output file: {output_file}")
    print(f"Rows written: {rows_written}")
    print(f"Confidence distribution: {confidence_dist}")
    print(f"UNKNOWN count: {unknown_count}")

    # Validate by reading back
    print(f"\n=== VALIDATION (pandas read-back) ===")
    try:
        import pandas as pd
        df = pd.read_csv(output_file, delimiter=';', quoting=1)
        print(f"Rows in CSV: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print(f"UNKNOWN values: {(df['csv_value_new'] == 'UNKNOWN').sum()}")
        print(f"Confidence breakdown:")
        print(df['confidence'].value_counts().to_dict())
    except ImportError:
        print("pandas not available; using csv reader instead")
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';', quoting=1)
            rows = list(reader)
            print(f"CSV rows (including header): {len(rows)}")
            if rows:
                print(f"Header: {rows[0]}")
                print(f"First data row: {rows[1] if len(rows) > 1 else 'none'}")

if __name__ == '__main__':
    process()
