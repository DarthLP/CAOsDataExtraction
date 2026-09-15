#!/usr/bin/env python3
import json
import csv
import sys
from pathlib import Path

# Input and output paths
input_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_105.jsonl")
output_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_105_corrections.csv")

# Read rubric from file for reference
rubric_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/docs/leave_qa_correction_prompt.md")

records = []
item_count = 0

# Load JSONL
print(f"Loading JSONL from {input_file}", file=sys.stderr)
with open(input_file, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            records.append(record)
            item_count += len(record.get('items', []))
        except json.JSONDecodeError as e:
            print(f"JSON error line {line_num}: {e}", file=sys.stderr)

print(f"Loaded {len(records)} records with {item_count} total items", file=sys.stderr)

# Process each item - simplified placeholder processing
# This will be replaced by Claude's manual review
rows = []
confidence_dist = {'high': 0, 'medium': 0, 'low': 0, 'UNKNOWN': 0}

for record in records:
    record_id = record.get('record_id', '')
    for item in record.get('items', []):
        item_id = item.get('item_id', '')
        topic_group = item.get('topic_group', '')
        field = item.get('field', '')
        csv_value_old = item.get('csv_value_old', '')
        topic_section = item.get('topic_section', '')
        context_type = item.get('context_type', '')

        # Placeholder: UNKNOWN with confidence=low until manually reviewed
        csv_value_new = 'UNKNOWN'
        unit_new = 'UNKNOWN'
        evidence_quote = '(no relevant text found or requires manual review)'
        confidence = 'low'
        notes = f'context={context_type}; requires manual review'

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

print(f"Processing {len(rows)} items", file=sys.stderr)
print(f"Confidence distribution (before manual correction): {confidence_dist}", file=sys.stderr)

# Write CSV with proper escaping
fieldnames = ['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
              'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes']

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {output_file}", file=sys.stderr)

# Read back and verify
import pandas as pd
df = pd.read_csv(output_file, delimiter=';')
print(f"Verification: pandas read {len(df)} rows", file=sys.stderr)
print(f"Columns: {list(df.columns)}", file=sys.stderr)
