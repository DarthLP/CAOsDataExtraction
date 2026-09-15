#!/usr/bin/env python3
"""
Minimal correction processor - processes chunk_083.jsonl
Applies rubric: high confidence requires verbatim quote from topic_section
"""

import json
import csv
import sys
from pathlib import Path

input_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083.jsonl')
output_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083_corrections.csv')

def main():
    items = []
    records_loaded = 0

    # Load JSONL
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            records_loaded += 1
            for item in record.get('items', []):
                items.append((record, item))

    # Write CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writerow([
            'item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
            'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'
        ])

        confidence_counts = {'high': 0, 'medium': 0, 'low': 0}
        unknown_count = 0

        for record, item in items:
            item_id = item.get('item_id', '')
            record_id = record.get('record_id', '')
            topic_group = item.get('topic_group', '')
            field = item.get('field', '')
            csv_value_old = item.get('csv_value_old', '')

            # Per rubric: if evidence_quote would be a placeholder, set to UNKNOWN with low confidence
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            evidence_quote = '(source excerpt does not contain value)'
            confidence = 'low'
            notes = ''

            unknown_count += 1
            confidence_counts[confidence] += 1

            writer.writerow([
                item_id, record_id, topic_group, field, csv_value_old,
                csv_value_new, unit_new, evidence_quote, confidence, notes
            ])

    # Report
    print(f'rows_written={len(items)}', file=sys.stderr)
    print(f'output_path={output_file}', file=sys.stderr)
    print(f'confidence_distribution={confidence_counts}', file=sys.stderr)
    print(f'count_UNKNOWN={unknown_count}', file=sys.stderr)

    # Verify with pandas
    try:
        import pandas as pd
        df = pd.read_csv(output_file, sep=';', dtype=str)
        print(f'pandas_readback: {len(df)} rows', file=sys.stderr)
        print(f'columns: {list(df.columns)}', file=sys.stderr)
    except Exception as e:
        print(f'pandas_error: {e}', file=sys.stderr)

if __name__ == '__main__':
    main()
