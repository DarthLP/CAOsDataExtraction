#!/usr/bin/env python3
import json
import csv
import sys
import re

# Paths
input_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_037.jsonl"
output_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_037_corrections.csv"

# Read JSONL
items_list = []
with open(input_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                record = json.loads(line)
                items_list.append(record)
            except:
                pass

# Flatten items
all_items = []
for record in items_list:
    if 'items' in record and isinstance(record['items'], list):
        for item in record['items']:
            item['record_id'] = record.get('record_id', '')
            all_items.append(item)

print(f"Processing {len(all_items)} items", file=sys.stderr)

# Process each item
corrections = []
confidence_dist = {'high': 0, 'medium': 0, 'low': 0}
unknown_count = 0

for item in all_items:
    item_id = item.get('item_id', '')
    record_id = item.get('record_id', '')
    topic_group = item.get('topic_group', '')
    field = item.get('field', '')
    csv_value_old = item.get('csv_value_old', '')
    flag_type = item.get('flag_type', '')
    flag_reason = item.get('flag_reason', '')
    topic_section = item.get('topic_section', '')

    # Initialize
    csv_value_new = 'UNKNOWN'
    unit_new = 'UNKNOWN'
    evidence_quote = '(no relevant text in excerpt)'
    confidence = 'low'
    notes = ''

    # Pattern matching based on flag type
    if flag_type == 'pattern:P2_tiered_sick_collapse':
        percentages = re.findall(r'(\d+)%', topic_section)
        if percentages:
            csv_value_new = percentages[0]
            unit_new = '%'
            sentences = [s.strip() for s in topic_section.split('.') if '%' in s]
            if sentences:
                evidence_quote = sentences[0][:200]
                confidence = 'high'
            if len(percentages) > 1:
                notes = f"tier_schedule={'/'.join(percentages[:4])}"

    elif flag_type == 'pattern:P4_adoption_window_as_duration':
        match = re.search(r'(\d+)\s*weeks?', topic_section)
        if match:
            csv_value_new = match.group(1)
            unit_new = 'weeks'
            evidence_quote = match.group(0)
            confidence = 'high'

    elif flag_type == 'pattern:P5_education_vacation_undercount':
        stat = re.search(r'Statutory[^0-9]*(\d+)\s*(hours|days|weeks)', topic_section, re.I)
        supp = re.search(r'Supplementary[^0-9]*(\d+)\s*(hours|days|weeks)', topic_section, re.I)
        if stat and supp:
            csv_value_new = str(int(stat.group(1)) + int(supp.group(1)))
            unit_new = stat.group(2)
            evidence_quote = f"{stat.group(1)} + {supp.group(1)}"
            confidence = 'high'
        elif stat:
            csv_value_new = stat.group(1)
            unit_new = stat.group(2)
            evidence_quote = stat.group(0)[:100]
            confidence = 'medium'

    elif flag_type == 'pattern:P9_unpaid_paternity_should_be_partially_paid':
        if ('5 weeks' in topic_section or '5 weken' in topic_section) and ('70%' in topic_section or 'UWV' in topic_section):
            csv_value_new = 'null'
            unit_new = ''
            match = re.search(r'[\w\s,%-]*5\s*weeks[\w\s,%-]*', topic_section, re.I)
            if match:
                evidence_quote = match.group(0)[:150]
            confidence = 'high'
            notes = 'move_to=leave_partially_paid_paternity_value with pay=70%'

    elif 'FIELD_ROLE' in flag_reason:
        match = re.search(r'(\d+)\s*(weeks?|days?|months?)', topic_section, re.I)
        if match:
            csv_value_new = match.group(1)
            unit_new = match.group(2).lower()
            evidence_quote = match.group(0)[:100]
            confidence = 'high'
        else:
            csv_value_new = 'UNKNOWN'
            unit_new = 'UNKNOWN'
            confidence = 'low'
            notes = 'statutory NL default may apply but source excerpt does not state'

    elif flag_type == 'L2_discussed_csv_empty':
        if 'maternity_value' in field and 'pay_value' not in field:
            match = re.search(r'(\d+)\s*weeks?', topic_section, re.I)
            if match:
                csv_value_new = match.group(1)
                unit_new = 'weeks'
                evidence_quote = match.group(0)[:100]
                confidence = 'high'
        elif 'pay_value' in field:
            match = re.search(r'(\d+)%', topic_section)
            if match:
                csv_value_new = match.group(1)
                unit_new = '%'
                evidence_quote = match.group(0)[:100]
                confidence = 'high'
        elif 'sick_topup_present' in field:
            if '100%' in topic_section or 'above' in topic_section.lower():
                csv_value_new = 'True'
                unit_new = ''
                confidence = 'high'
            else:
                csv_value_new = 'False'
                unit_new = ''
                confidence = 'medium'
        elif 'sickpay_continuation' in field:
            match = re.search(r'(\d+)%', topic_section)
            if match:
                csv_value_new = match.group(1)
                unit_new = '%'
                evidence_quote = match.group(0)[:100]
                confidence = 'high'
        elif 'short_term_care' in field:
            match = re.search(r'(\d+)\s*(days?|hours?|weeks?)', topic_section, re.I)
            if match:
                csv_value_new = match.group(1)
                unit_new = match.group(2).lower()
                evidence_quote = match.group(0)[:100]
                confidence = 'high'
        else:
            match = re.search(r'(\d+)\s*(weeks?|days?|%|hours?)', topic_section, re.I)
            if match:
                csv_value_new = match.group(1)
                unit_new = match.group(2) if match.group(2) else ''
                evidence_quote = match.group(0)[:100]
                confidence = 'medium'
            else:
                csv_value_new = 'UNKNOWN'
                unit_new = 'UNKNOWN'
                confidence = 'low'
                notes = 'topic discussed but value for this field not in excerpt'

    confidence_dist[confidence] += 1
    if csv_value_new == 'UNKNOWN':
        unknown_count += 1

    corrections.append({
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
    })

# Write CSV
with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(
        f,
        fieldnames=['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                   'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'],
        delimiter=';',
        quoting=csv.QUOTE_ALL
    )
    writer.writeheader()
    writer.writerows(corrections)

print(f"\nWritten {len(corrections)} rows", file=sys.stderr)
print(f"Confidence: {confidence_dist}", file=sys.stderr)
print(f"UNKNOWN: {unknown_count}", file=sys.stderr)

# Verify
import pandas as pd
df = pd.read_csv(output_path, delimiter=';')
print(f"Pandas: {len(df)} rows, {len(df.columns)} cols", file=sys.stderr)
print("Success", file=sys.stderr)
