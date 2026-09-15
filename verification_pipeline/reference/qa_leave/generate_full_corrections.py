#!/usr/bin/env python3
"""
Process all records from chunk_074.jsonl and generate corrections CSV per rubric.
Strict confidence semantics enforced.
"""

import json
import csv
import re
from collections import defaultdict

chunk_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_074.jsonl"
output_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_074_corrections.csv"

def process_item(item):
    """Process one item per rubric rules"""
    row = {
        'item_id': item['item_id'],
        'record_id': item['record_id'],
        'topic_group': item['topic_group'],
        'field': item['field'],
        'csv_value_old': item['csv_value_old'],
        'csv_value_new': 'UNKNOWN',
        'unit_new': 'UNKNOWN',
        'evidence_quote': '(no relevant text in excerpt)',
        'confidence': 'low',
        'notes': ''
    }

    flag_type = item.get('flag_type', '')
    context_type = item.get('context_type', '')
    topic_section = item.get('topic_section', '')
    csv_unit_old = item.get('csv_unit_old', '')

    # Rule 1: context_type=empty -> always UNKNOWN/low
    if context_type == 'empty':
        row['evidence_quote'] = '(no source available)'
        row['notes'] = 'context_type=empty'
        return row

    # Rule 2: P2_tiered_sick_collapse - keep first tier, document full schedule
    if flag_type == 'pattern:P2_tiered_sick_collapse':
        row['csv_value_new'] = item['csv_value_old']  # Keep current value (first tier)
        row['unit_new'] = csv_unit_old if csv_unit_old else '%'

        # Extract all percentages from topic_section
        percentages = re.findall(r'(\d+)%', topic_section)

        if percentages and len(percentages) > 1:
            # Try to find verbatim quote containing tiered info
            tier_match = re.search(
                r'[^.!?\n]*(\d+%[^.!?\n]*(?:(?:\d+%|week|year)[^.!?\n]*)*)[.!?\n]',
                topic_section,
                re.DOTALL
            )
            if tier_match:
                quote = tier_match.group(0).strip()[:200]
                row['evidence_quote'] = quote
                row['confidence'] = 'high'
                row['notes'] = f"tier_schedule_preserved={'/'.join(percentages)}"
            else:
                row['evidence_quote'] = f"{percentages[0]}%"
                row['confidence'] = 'medium'
                row['notes'] = f"tiered={'/'.join(percentages)}"
        elif percentages:
            row['evidence_quote'] = f"{percentages[0]}%"
            row['confidence'] = 'medium'
        else:
            row['evidence_quote'] = '(tiered structure mentioned but no percentages found)'
            row['confidence'] = 'low'

        return row

    # Rule 3: L2_discussed_csv_empty - topic discussed but value empty
    if flag_type == 'L2_discussed_csv_empty':
        # context_type=full_source means full document was provided
        # context_type=topic_section_partial means partial section

        # Look for numeric values or percentages
        numeric_match = re.search(
            r'(\d+(?:[.,]\d+)?)\s*(weeks?|days?|hours?|%|per year|per month|per week|minutes?)',
            topic_section,
            re.IGNORECASE
        )

        if numeric_match:
            value = numeric_match.group(1)
            unit = numeric_match.group(2) or ''
            row['csv_value_new'] = value
            row['unit_new'] = unit
            row['evidence_quote'] = numeric_match.group(0)[:200]
            row['confidence'] = 'high'
            return row

        # No value found in excerpt
        row['csv_value_new'] = 'UNKNOWN'
        row['unit_new'] = 'UNKNOWN'
        row['evidence_quote'] = '(no relevant text in excerpt)'
        row['confidence'] = 'low'
        if context_type == 'full_source':
            row['notes'] = f"full_source scanned; no value for {item['field']}"
        else:
            row['notes'] = 'topic discussed but value for field not in excerpt'

        return row

    # Default: generic L1/L3/other flags - look for any extractable value
    if topic_section and topic_section.strip() and context_type != 'empty':
        numeric_match = re.search(
            r'(\d+(?:[.,]\d+)?)\s*(weeks?|days?|hours?|%|per year|per month|per week|minutes?)?',
            topic_section,
            re.IGNORECASE
        )

        if numeric_match:
            value = numeric_match.group(1)
            unit = numeric_match.group(2) or ''
            row['csv_value_new'] = value
            row['unit_new'] = unit
            row['evidence_quote'] = numeric_match.group(0)[:200]
            row['confidence'] = 'medium'
            return row

    # No value found
    row['csv_value_new'] = 'UNKNOWN'
    row['unit_new'] = 'UNKNOWN'
    row['evidence_quote'] = '(no relevant text in excerpt)'
    row['confidence'] = 'low'

    return row

def main():
    records = []
    all_items = []

    # Read all records
    with open(chunk_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                records.append(record)
                for item in record.get('items', []):
                    item['record_id'] = record['record_id']
                    all_items.append(item)

    print(f"Loaded {len(records)} records, {len(all_items)} items total")

    # Process all items
    corrections = []
    for item in all_items:
        corrections.append(process_item(item))

    # Write CSV with proper escaping
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'
            ],
            delimiter=';',
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(corrections)

    print(f"\nCSV written: {output_path}")
    print(f"Total rows written: {len(corrections) + 1} (header + {len(corrections)} data rows)")

    # Confidence distribution
    conf_dist = defaultdict(int)
    unknown_count = 0
    for row in corrections:
        conf_dist[row['confidence']] += 1
        if row['csv_value_new'] == 'UNKNOWN':
            unknown_count += 1

    print(f"\nConfidence distribution:")
    for conf in ['high', 'medium', 'low']:
        count = conf_dist.get(conf, 0)
        pct = (count / len(corrections) * 100) if corrections else 0
        print(f"  {conf:8s}: {count:2d} ({pct:5.1f}%)")

    print(f"\nUNKNOWN count: {unknown_count} ({unknown_count/len(corrections)*100:.1f}%)")

    # Verify with pandas
    try:
        import pandas as pd
        df = pd.read_csv(output_path, sep=';', quoting=csv.QUOTE_ALL)
        print(f"\nPandas verification:")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        print(f"  No parsing errors")
    except Exception as e:
        print(f"\nPandas read failed: {e}")

if __name__ == '__main__':
    main()
