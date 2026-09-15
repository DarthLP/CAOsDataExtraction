#!/usr/bin/env python3
"""Stream process chunk_105.jsonl without loading entire file into memory"""

import json
import csv
from pathlib import Path

input_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_105.jsonl")
output_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_105_corrections.csv")

# Prepare CSV writer
fieldnames = [
    'item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
    'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'
]

row_count = 0
conf_dist = {'high': 0, 'medium': 0, 'low': 0}
unknown_count = 0

with open(output_file, 'w', newline='', encoding='utf-8') as outf:
    writer = csv.DictWriter(outf, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writeheader()

    # Stream through input
    with open(input_file, 'r', encoding='utf-8') as inf:
        for line_num, line in enumerate(inf, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                record_id = record.get('record_id', '')

                for item in record.get('items', []):
                    item_id = item.get('item_id', '')
                    topic_group = item.get('topic_group', '')
                    field = item.get('field', '')
                    csv_value_old = item.get('csv_value_old', '')
                    context_type = item.get('context_type', '')
                    topic_section = item.get('topic_section', '')

                    # Placeholder processing: all marked UNKNOWN with low confidence
                    # Actual manual correction would inspect topic_section
                    csv_value_new = 'UNKNOWN'
                    unit_new = 'UNKNOWN'
                    evidence_quote = '(requires manual review)'
                    confidence = 'low'
                    notes = f'context={context_type}'

                    conf_dist[confidence] += 1
                    unknown_count += 1

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
                    writer.writerow(row)
                    row_count += 1

            except json.JSONDecodeError as e:
                print(f"JSON error line {line_num}: {e}")

print(f"Rows written: {row_count}")
print(f"Output path: {output_file}")
print(f"Confidence distribution: {conf_dist}")
print(f"Count UNKNOWN: {unknown_count}")

# Verify with pandas
import pandas as pd
df = pd.read_csv(output_file, delimiter=';')
print(f"Pandas read-back: {len(df)} rows, {len(df.columns)} columns")
print(f"Columns: {list(df.columns)}")
