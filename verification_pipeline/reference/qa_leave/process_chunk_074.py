import json
import csv
import re
from pathlib import Path

chunk_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_074.jsonl"
output_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_074_corrections.csv"

records = []
with open(chunk_path, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            records.append(json.loads(line))

# Collect all items
all_items = []
for record in records:
    for item in record.get('items', []):
        item['record_id'] = record['record_id']
        all_items.append(item)

print(f"Processing {len(all_items)} items...")

# Process each item - follow rubric strictly
corrections = []

for item in all_items:
    row = {
        'item_id': item['item_id'],
        'record_id': item['record_id'],
        'topic_group': item['topic_group'],
        'field': item['field'],
        'csv_value_old': item['csv_value_old'],
        'csv_value_new': 'UNKNOWN',
        'unit_new': 'UNKNOWN',
        'evidence_quote': '',
        'confidence': 'low',
        'notes': ''
    }

    flag_type = item.get('flag_type', '')
    context_type = item.get('context_type', '')
    topic_section = item.get('topic_section', '')
    csv_unit_old = item.get('csv_unit_old', '')

    # Context type empty - always UNKNOWN
    if context_type == 'empty':
        row['csv_value_new'] = 'UNKNOWN'
        row['unit_new'] = 'UNKNOWN'
        row['confidence'] = 'low'
        row['evidence_quote'] = '(no source available)'
        row['notes'] = 'context_type=empty'

    # Pattern P2: tiered sick collapse - keep first tier, preserve full schedule in notes
    elif flag_type == 'pattern:P2_tiered_sick_collapse':
        row['csv_value_new'] = item['csv_value_old']  # Keep first tier value
        row['unit_new'] = csv_unit_old if csv_unit_old else '%'

        percentages = re.findall(r'(\d+)%', topic_section)

        if percentages:
            tier_match = re.search(r'[^.!?\n]*(\d+%[^.!?\n]*(?:(?:\d+%|week)[^.!?\n]*)*)[.!?\n]', topic_section)
            if tier_match:
                quote = tier_match.group(0).strip()[:200]
                row['evidence_quote'] = quote
                row['confidence'] = 'high'
                row['notes'] = f"tier_schedule_preserved={'/'.join(percentages)}"
            else:
                row['evidence_quote'] = f"{percentages[0]}%"
                row['confidence'] = 'medium'
                row['notes'] = f"tiered={'/'.join(percentages)}"
        else:
            row['evidence_quote'] = '(tiered structure mentioned but percentages not found)'
            row['confidence'] = 'low'

    # L2_discussed_csv_empty - topic discussed but value empty
    elif flag_type == 'L2_discussed_csv_empty':
        # Look for numeric or percentage values
        numeric_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(weeks|days|hours|%|per year|per month)?', topic_section, re.IGNORECASE)
        percent_match = re.search(r'(\d+)\s*%', topic_section)

        if numeric_match:
            value = numeric_match.group(1)
            unit = numeric_match.group(2) or ''
            row['csv_value_new'] = value
            row['unit_new'] = unit
            row['evidence_quote'] = numeric_match.group(0)[:200]
            row['confidence'] = 'high'
        elif percent_match:
            value = percent_match.group(1)
            row['csv_value_new'] = value
            row['unit_new'] = '%'
            row['evidence_quote'] = f"{value}%"
            row['confidence'] = 'high'
        else:
            row['csv_value_new'] = 'UNKNOWN'
            row['unit_new'] = 'UNKNOWN'
            row['confidence'] = 'low'
            row['notes'] = 'topic discussed but value for field not in excerpt'

    # Default: look for any value in topic_section
    else:
        if topic_section and topic_section.strip() and context_type != 'empty':
            numeric_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(weeks|days|hours|%|per year|per month)?', topic_section, re.IGNORECASE)
            if numeric_match:
                value = numeric_match.group(1)
                unit = numeric_match.group(2) or ''
                row['csv_value_new'] = value
                row['unit_new'] = unit
                row['evidence_quote'] = numeric_match.group(0)[:200]
                row['confidence'] = 'medium'
            else:
                row['csv_value_new'] = 'UNKNOWN'
                row['unit_new'] = 'UNKNOWN'
                row['confidence'] = 'low'
                row['evidence_quote'] = '(no relevant value found in excerpt)'
        else:
            row['csv_value_new'] = 'UNKNOWN'
            row['unit_new'] = 'UNKNOWN'
            row['confidence'] = 'low'
            row['evidence_quote'] = '(no relevant text in excerpt)'

    # CRITICAL RULE: if evidence_quote starts with '(' then confidence must be 'low' and value must be 'UNKNOWN'
    if row['evidence_quote'].startswith('('):
        row['confidence'] = 'low'
        row['csv_value_new'] = 'UNKNOWN'
        row['unit_new'] = 'UNKNOWN'

    corrections.append(row)

# Write CSV with proper escaping
with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(
        f,
        fieldnames=['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old', 'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'],
        delimiter=';',
        quoting=csv.QUOTE_ALL
    )
    writer.writeheader()
    writer.writerows(corrections)

print(f"\nCSV written: {output_path}")
print(f"Total rows: {len(corrections) + 1} (including header)")

# Verify with pandas
import pandas as pd
df = pd.read_csv(output_path, sep=';', quoting=csv.QUOTE_ALL)
print(f"\nPandas read-back: {len(df)} rows")
print(f"Columns: {list(df.columns)}")

# Confidence distribution
conf_dist = df['confidence'].value_counts().to_dict()
print(f"\nConfidence distribution: {conf_dist}")
print(f"Count UNKNOWN: {(df['csv_value_new'] == 'UNKNOWN').sum()}")
