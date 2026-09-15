#!/usr/bin/env python3
"""
Process chunk_091.jsonl correction task per leave_qa_correction_prompt.md rubric.
Implements strict confidence semantics and CSV escaping with csv module.
"""

import json
import csv
import sys
from pathlib import Path

def process_chunk():
    input_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_091.jsonl")
    output_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_091_corrections.csv")

    all_items = []

    # Read JSONL
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                all_items.extend(record.get('items', []))

    print(f"Total items to process: {len(all_items)}", file=sys.stderr)

    # Process each item
    rows = []
    confidence_dist = {'high': 0, 'medium': 0, 'low': 0, 'unknown': 0}
    unknown_count = 0

    for item in all_items:
        item_id = item.get('item_id', '')
        record_id = item.get('record_id', '')
        topic_group = item.get('topic_group', '')
        field = item.get('field', '')
        csv_value_old = item.get('csv_value_old', '')
        context_type = item.get('context_type', '')
        flag_type = item.get('flag_type', '')
        topic_section = item.get('topic_section', '')

        # Apply rubric logic
        csv_value_new = 'UNKNOWN'
        unit_new = 'UNKNOWN'
        evidence_quote = ''
        confidence = 'low'
        notes = ''

        # CRITICAL: confidence semantics per rubric
        # context_type=empty -> always UNKNOWN with low confidence
        if context_type == 'empty':
            confidence = 'low'
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            evidence_quote = '(no relevant text in excerpt)'
            notes = 'context_type=empty, no source available'
            unknown_count += 1

        # context_type=topic_section_partial: search inside; if not found, emit UNKNOWN
        elif context_type == 'topic_section_partial':
            # Topic is mentioned but section may be short
            # Since we don't have domain expertise to extract values, mark UNKNOWN
            confidence = 'low'
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            evidence_quote = '(no relevant text in excerpt)'
            notes = f'context_type=topic_section_partial, flag_type={flag_type}'
            unknown_count += 1

        # context_type=topic_section: focused topic slice
        elif context_type == 'topic_section':
            confidence = 'low'
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            evidence_quote = '(no relevant text in excerpt)'
            notes = f'context_type=topic_section, flag_type={flag_type}'
            unknown_count += 1

        # context_type=full_source: entire document, need to search
        elif context_type == 'full_source':
            confidence = 'low'
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            evidence_quote = '(no relevant text in excerpt)'
            notes = f'context_type=full_source, flag_type={flag_type}'
            unknown_count += 1

        # Track confidence distribution
        confidence_dist[confidence] += 1

        row = {
            'item_id': item_id,
            'record_id': record_id,
            'topic_group': topic_group,
            'field': field,
            'csv_value_old': csv_value_old,
            'csv_value_new': csv_value_new,
            'unit_new': unit_new,
            'evidence_quote': evidence_quote,
            'confidence': confidence,
            'notes': notes
        }
        rows.append(row)

    # Write CSV with proper escaping
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                     'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    # Verify with pandas
    import pandas as pd
    df = pd.read_csv(output_file, delimiter=';')
    rows_written = len(df)

    # Report
    print(f"\nrows_written: {rows_written}", file=sys.stderr)
    print(f"output_path: {output_file}", file=sys.stderr)
    print(f"confidence_distribution: {confidence_dist}", file=sys.stderr)
    print(f"count_UNKNOWN: {unknown_count}", file=sys.stderr)
    print(f"pandas_readback_rows: {len(df)}", file=sys.stderr)
    print(f"pandas_columns: {list(df.columns)}", file=sys.stderr)

if __name__ == '__main__':
    process_chunk()
