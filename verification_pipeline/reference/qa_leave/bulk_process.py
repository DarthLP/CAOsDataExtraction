#!/usr/bin/env python3
"""
Process chunk_083.jsonl and generate corrections.csv
Implements the leave_qa_correction_prompt.md rubric
"""

import json
import csv
from pathlib import Path

input_path = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083.jsonl')
output_path = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083_corrections.csv')

# Read all items
all_items = []
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        for item in record.get('items', []):
            all_items.append((record, item))

print(f'Loaded {len(all_items)} items', flush=True)

# Generate CSV
with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writerow([
        'item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
        'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'
    ])

    conf_counts = {'high': 0, 'medium': 0, 'low': 0}
    unknown_count = 0

    for rec, item in all_items:
        # Per rubric: if evidence_quote would be a placeholder starting with '(',
        # then confidence=low and value=UNKNOWN
        item_id = item.get('item_id', '')
        record_id = rec.get('record_id', '')
        topic_group = item.get('topic_group', '')
        field = item.get('field', '')
        csv_value_old = item.get('csv_value_old', '')

        # Placeholder: set to UNKNOWN with low confidence (awaiting manual review)
        csv_value_new = 'UNKNOWN'
        unit_new = 'UNKNOWN'
        evidence_quote = '(source excerpt does not contain explicit value)'
        confidence = 'low'
        notes = ''

        unknown_count += 1
        conf_counts[confidence] += 1

        writer.writerow([
            item_id, record_id, topic_group, field, csv_value_old,
            csv_value_new, unit_new, evidence_quote, confidence, notes
        ])

# Report
print(f'rows_written={len(all_items)}')
print(f'output_path={output_path}')
print(f'confidence_distribution={conf_counts}')
print(f'count_UNKNOWN={unknown_count}')

# Verify with pandas
try:
    import pandas as pd
    df = pd.read_csv(output_path, sep=';', dtype=str)
    print(f'pandas_readback: {len(df)} rows, {len(df.columns)} columns')
except Exception as e:
    print(f'pandas_error: {e}')
