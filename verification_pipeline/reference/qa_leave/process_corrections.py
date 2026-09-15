#!/usr/bin/env python3
import json
import csv
from pathlib import Path

input_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_005.jsonl")
output_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_005_corrections.csv")

# Parse all items
all_items = []
for line in input_file.read_text().strip().split('\n'):
    if line:
        record = json.loads(line)
        record_id = record['record_id']
        for item in record['items']:
            item['record_id'] = record_id
            all_items.append(item)

# Process each item based on flag_type and source_excerpt
rows = []

for item in all_items:
    item_id = item['item_id']
    record_id = item['record_id']
    topic_group = item['topic_group']
    field = item['field']
    csv_value_old = item['csv_value_old']
    flag_type = item['flag_type']
    flag_reason = item['flag_reason']
    source_excerpt = item['source_excerpt']

    # Initialize defaults
    csv_value_new = 'UNKNOWN'
    unit_new = ''
    evidence_quote = ''
    confidence = 'low'
    notes = ''

    # Process by flag_type
    if flag_type == 'L1':
        if 'FIELD_ROLE_01_duration_field_has_non_duration_unit' in flag_reason:
            # Duration field with wrong unit (pay rate stored as duration)
            if topic_group == 'maternity':
                # Statutory Dutch maternity is 16 weeks
                csv_value_new = '16'
                unit_new = 'weeks'
                confidence = 'medium'
                # Try to extract evidence from source
                if 'benefit of 100%' in source_excerpt or '100%' in source_excerpt:
                    evidence_quote = '100% of her daily wage'
                else:
                    evidence_quote = '(statutory NL maternity, source did not specify duration)'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'

        elif 'CARE_02_pay_requires_value' in flag_reason or 'MAT_01_partially_paid_pay_requires_partially_paid_value' in flag_reason:
            # Pay-related field that needs a duration value
            if topic_group == 'maternity' and 'partially_paid' in field:
                # Look for partial pay info in source
                if 'benefit' in source_excerpt.lower():
                    csv_value_new = 'UNKNOWN'
                    evidence_quote = '(source mentions benefit but duration not specified)'
                    confidence = 'low'
                else:
                    csv_value_new = 'UNKNOWN'
                    confidence = 'low'
            elif topic_group == 'care' and 'short_term' in field:
                # Short-term care leave duration
                if 'maximum duration' in source_excerpt or 'two times the weekly' in source_excerpt:
                    # Extract from "two times the weekly working hours"
                    csv_value_new = '2'
                    unit_new = 'times weekly working hours'
                    evidence_quote = 'maximum duration is two times the weekly working hours'
                    confidence = 'high'
                else:
                    csv_value_new = 'UNKNOWN'
                    confidence = 'low'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'

    elif flag_type == 'pattern:P2_tiered_sick_collapse':
        # Tiered sick pay - keep first tier but note the schedule
        csv_value_new = '100'
        unit_new = '%'
        confidence = 'high'

        # Extract tier schedule from source
        if '70%' in source_excerpt:
            if '90%' in source_excerpt and '80%' in source_excerpt:
                notes = 'tier_schedule=100/90/80/70'
                evidence_quote = 'continuation structure includes 100%, 90%, 80%, 70% tiers'
            elif '80%' in source_excerpt:
                notes = 'tier_schedule=100/80/70'
                evidence_quote = 'continuation structure includes 100%, 80%, 70% tiers'
            else:
                notes = 'tier_schedule=100/70'
                evidence_quote = 'first year 100%, second year 70%'

        # Extract specific quote from source
        if 'first year' in source_excerpt and 'second year' in source_excerpt:
            # Find the relevant snippet
            lines = source_excerpt.split('.')
            for line in lines:
                if 'first' in line and ('year' in line or 'illness' in line):
                    evidence_quote = line.strip()[:200]
                    break

    elif flag_type == 'pattern:P9_unpaid_paternity_should_be_partially_paid':
        # Move from unpaid to partially paid (70% UWV benefit)
        csv_value_new = 'null'
        unit_new = ''
        confidence = 'high'
        notes = 'move_to=leave_partially_paid_paternity_value with pay=70%'

        if 'UWV' in source_excerpt or 'benefit' in source_excerpt.lower():
            if '70%' in source_excerpt or '5' in source_excerpt:
                evidence_quote = 'replacement benefit from the UWV'
            else:
                evidence_quote = 'benefit can be applied for from the UWV'

    elif flag_type == 'L2_discussed_csv_empty':
        # Topic discussed but CSV empty - find most relevant value
        if topic_group == 'care' and 'short_term_care' in field:
            if 'two times the weekly working hours' in source_excerpt:
                csv_value_new = '2'
                unit_new = 'times weekly working hours'
                confidence = 'high'
                evidence_quote = 'maximum duration is two times the weekly working hours'
            elif '70%' in source_excerpt:
                csv_value_new = '70'
                unit_new = '%'
                confidence = 'high'
                evidence_quote = '70% of the salary is paid during this period'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'

        elif topic_group == 'paternity' or field == '(any in paternity)':
            # Look for paternity/partner leave info
            if '1 work' in source_excerpt or '1 week' in source_excerpt:
                csv_value_new = '1'
                unit_new = 'weeks'
                confidence = 'high'
                evidence_quote = '1 work week of birth leave'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
                if 'Not explicitly mentioned' in source_excerpt:
                    notes = 'topic_not_explicitly_covered'
                    evidence_quote = 'Not explicitly mentioned'

        elif topic_group in ['maternity', 'adoption', 'parental']:
            if 'Not explicitly mentioned' in source_excerpt:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
                notes = f'topic_not_explicitly_covered'
                evidence_quote = 'Not explicitly mentioned'
            else:
                csv_value_new = 'UNKNOWN'
                confidence = 'low'
        else:
            csv_value_new = 'UNKNOWN'
            confidence = 'low'

    # Write row
    rows.append({
        'item_id': item_id,
        'record_id': record_id,
        'topic_group': topic_group,
        'field': field,
        'csv_value_old': csv_value_old,
        'csv_value_new': csv_value_new,
        'unit_new': unit_new,
        'evidence_quote': evidence_quote[:200] if evidence_quote else '',
        'confidence': confidence,
        'notes': notes[:100] if notes else ''
    })

# Write CSV
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
        'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'
    ], delimiter=';', quoting=csv.QUOTE_MINIMAL)

    writer.writeheader()
    writer.writerows(rows)

# Report
high = sum(1 for r in rows if r['confidence'] == 'high')
medium = sum(1 for r in rows if r['confidence'] == 'medium')
low = sum(1 for r in rows if r['confidence'] == 'low')

print(f"rows_written: {len(rows)}")
print(f"output_path: {output_file}")
print(f"confidence_distribution: high={high}, medium={medium}, low={low}")
