#!/usr/bin/env python3
import json, csv, re
from pathlib import Path

input_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099.jsonl')
output_file = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099_corrections.csv')

confidence_dist = {'high': 0, 'medium': 0, 'low': 0}
unknown_count = 0
rows_written = 0

def extract_evidence(topic_section, field, flag_type):
    if not topic_section or not topic_section.strip():
        return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

    # P2_tiered_sick_collapse: first year = 100%
    if 'P2_tiered' in flag_type:
        m = re.search(r'(\d+)%\s+(of the last earned salary|of last earned salary)', topic_section)
        if m:
            value = m.group(1)
            start = max(0, m.start() - 30)
            end = min(len(topic_section), m.end() + 80)
            quote = topic_section[start:end].strip()
            if len(quote) > 200:
                quote = quote[:200]
            return value, '%', f'"{quote}"', 'high'

    # Care leave looking for duration
    if 'CARE' in flag_type or ('care' in topic_section.lower() and 'care_value' in field):
        m = re.search(r'(\d+)\s*(days?|weeks?|hours?)', topic_section, re.IGNORECASE)
        if m:
            value, unit = m.group(1), m.group(2).lower()
            start = max(0, m.start() - 40)
            end = min(len(topic_section), m.end() + 40)
            quote = topic_section[start:end].strip()
            if len(quote) > 200:
                quote = quote[:200]
            return value, unit, f'"{quote}"', 'high'

    # Sickness second year 70%
    if 'sick' in field.lower() and '70%' in topic_section:
        m = re.search(r'second year.*?(\d+)%', topic_section, re.IGNORECASE)
        if m:
            start = max(0, m.start() - 40)
            end = min(len(topic_section), m.end() + 40)
            quote = topic_section[start:end].strip()
            if len(quote) > 200:
                quote = quote[:200]
            return '70', '%', f'"{quote}"', 'high'

    # Generic fallback: any number + unit
    m = re.search(r'(\d+)\s*(weeks?|days?|months?|years?|%|hours?)', topic_section, re.IGNORECASE)
    if m and m.lastindex and m.lastindex > 0 and m.group(2):
        value, unit = m.group(1), m.group(2).lower()
        start = max(0, m.start() - 40)
        end = min(len(topic_section), m.end() + 40)
        quote = topic_section[start:end].strip()
        if len(quote) > 200:
            quote = quote[:200]
        return value, unit, f'"{quote}"', 'medium'

    return 'UNKNOWN', 'UNKNOWN', '(no relevant text in excerpt)', 'low'

# Process
records = []
with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            records.append(json.loads(line))

print(f"Loaded {len(records)} records")

all_items = []
for record in records:
    for item in record.get('items', []):
        item['record_id'] = record.get('record_id', '')
        all_items.append(item)

print(f"Total items: {len(all_items)}")

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writerow(['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                     'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'])

    for item in all_items:
        topic_section = item.get('topic_section', '')
        field = item.get('field', '')
        flag_type = item.get('flag_type', '')

        csv_value_new, unit_new, evidence_quote, confidence = extract_evidence(
            topic_section, field, flag_type
        )

        notes = ''
        if csv_value_new == 'UNKNOWN':
            unknown_count += 1
            context_type = item.get('context_type', '')
            notes = f"context_type={context_type}; value not in excerpt"

        row = [item.get('item_id',''), item.get('record_id',''), item.get('topic_group',''),
               field, item.get('csv_value_old',''), csv_value_new, unit_new,
               evidence_quote, confidence, notes]
        writer.writerow(row)
        rows_written += 1
        confidence_dist[confidence] += 1

print(f"\n=== RESULTS ===")
print(f"rows_written: {rows_written}")
print(f"output_path: {output_file}")
print(f"confidence_distribution: {confidence_dist}")
print(f"count_UNKNOWN: {unknown_count}")

# Read back with pandas
try:
    import pandas as pd
    df = pd.read_csv(output_file, delimiter=';', quoting=1)
    print(f"\n=== VALIDATION ===")
    print(f"pandas_rows_read: {len(df)}")
    conf_counts = df['confidence'].value_counts().to_dict()
    print(f"confidence_breakdown: {conf_counts}")
    unknown_from_csv = (df['csv_value_new'] == 'UNKNOWN').sum()
    print(f"UNKNOWN_values_in_csv: {unknown_from_csv}")
except Exception as e:
    print(f"Pandas validation failed: {e}")
