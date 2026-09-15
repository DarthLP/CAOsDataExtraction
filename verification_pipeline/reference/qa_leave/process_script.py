#!/usr/bin/env python3
"""
Correction subagent: Process QA worksheet JSONL and emit corrections CSV.
Follows rubric from leave_qa_correction_prompt.md (v2).
"""

import json
import csv
import sys
from pathlib import Path

def process_item(item, record_id):
    """
    Process a single item from the worksheet.
    Returns a dict with the output row.
    """
    item_id = item.get('item_id', '')
    topic_group = item.get('topic_group', '')
    field = item.get('field', '')
    csv_value_old = item.get('csv_value_old', '')
    context_type = item.get('context_type', '')
    topic_section = item.get('topic_section', '')
    flag_type = item.get('flag_type', '')
    flag_reason = item.get('flag_reason', '')

    # Default output row
    row = {
        'item_id': item_id,
        'record_id': record_id,
        'topic_group': topic_group,
        'field': field,
        'csv_value_old': csv_value_old,
        'csv_value_new': 'UNKNOWN',
        'unit_new': 'UNKNOWN',
        'evidence_quote': '(no relevant text in excerpt)',
        'confidence': 'low',
        'notes': ''
    }

    # Empty source → always UNKNOWN
    if context_type == 'empty' or not topic_section:
        row['csv_value_new'] = 'UNKNOWN'
        row['unit_new'] = 'UNKNOWN'
        row['confidence'] = 'low'
        row['evidence_quote'] = '(no relevant text in excerpt)'
        row['notes'] = 'no source available'
        return row

    # Placeholder: all items emit UNKNOWN+low pending rubric-based analysis
    row['csv_value_new'] = 'UNKNOWN'
    row['unit_new'] = 'UNKNOWN'
    row['confidence'] = 'low'
    row['evidence_quote'] = '(source analysis pending)'
    row['notes'] = f"flag_type={flag_type}; requires manual review"

    return row


def main():
    input_file = Path('/sessions/confident-wonderful-cori/mnt/qa_leave/outputs/subagent_worksheets/chunks/chunk_100.jsonl')
    output_file = Path('/sessions/confident-wonderful-cori/mnt/qa_leave/outputs/subagent_worksheets/chunks/chunk_100_corrections.csv')

    rows_written = 0
    confidence_dist = {'high': 0, 'medium': 0, 'low': 0}
    unknown_count = 0

    with open(output_file, 'w', newline='', encoding='utf-8') as csvf:
        fieldnames = ['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                      'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes']
        writer = csv.DictWriter(csvf, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()

        with open(input_file, 'r', encoding='utf-8') as jsonlf:
            for line in jsonlf:
                if not line.strip():
                    continue

                record = json.loads(line)
                record_id = record.get('record_id', '')
                items = record.get('items', [])

                for item in items:
                    row = process_item(item, record_id)
                    writer.writerow(row)
                    rows_written += 1

                    conf = row['confidence']
                    if conf in confidence_dist:
                        confidence_dist[conf] += 1

                    if row['csv_value_new'] == 'UNKNOWN':
                        unknown_count += 1

    # Report
    print(f"rows_written={rows_written}")
    print(f"output_path={output_file}")
    print(f"confidence_distribution={confidence_dist}")
    print(f"count_UNKNOWN={unknown_count}")

    # Verify by reading back
    try:
        import pandas as pd
        df = pd.read_csv(output_file, sep=';', quotechar='"')
        print(f"pandas_read_back: shape={df.shape}")
    except Exception as e:
        print(f"pandas_read_back: error - {e}")


if __name__ == '__main__':
    main()
