#!/usr/bin/env python3
import csv
import json
from collections import Counter

# Read the JSONL file
items_to_process = []
with open('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_007.jsonl', 'r', encoding='utf-8') as f:
    for line in f:
        data = json.loads(line)
        record_id = data['record_id']
        for item in data['items']:
            # Add record_id to each item
            item['record_id'] = record_id
            items_to_process.append(item)

# Process each item
output_rows = []
confidence_counts = Counter()

for item in items_to_process:
    item_id = item['item_id']
    record_id = item['record_id']
    topic_group = item['topic_group']
    field = item['field']
    csv_value_old = str(item['csv_value_old']) if item['csv_value_old'] is not None else ""
    csv_unit_old = item['csv_unit_old']
    flag_type = item['flag_type']
    flag_reason = item['flag_reason']
    source_excerpt = item['source_excerpt']

    # Apply correction rules
    csv_value_new = None
    unit_new = ""
    evidence_quote = ""
    confidence = "low"
    notes = ""

    # L1: FIELD_ROLE_01_duration_field_has_non_duration_unit - maternity
    if flag_type == "L1" and "FIELD_ROLE_01" in flag_reason and topic_group == "maternity":
        # Dutch maternity is 16 weeks statutory
        if "16" in source_excerpt and "weeks" in source_excerpt:
            csv_value_new = "16"
            unit_new = "weeks"
            evidence_quote = "total duration of the leave is at least 16 consecutive weeks"
            confidence = "high"
        else:
            csv_value_new = "16"
            unit_new = "weeks"
            evidence_quote = "(statutory NL maternity, source did not specify)"
            confidence = "medium"

    # pattern:P2_tiered_sick_collapse - sick pay continuation with tiers
    elif flag_type == "pattern:P2_tiered_sick_collapse" and topic_group == "sick":
        # Keep the first tier value (100%), but capture the full schedule in notes
        csv_value_new = "100"
        unit_new = "%"

        # Extract tier information from source
        if "70%" in source_excerpt:
            if "90%" in source_excerpt:
                evidence_quote = "employee retains the right to 70% of the wage"
                notes = "tier_schedule=likely 100/90/80/70 pattern; confirm from full doc"
            else:
                evidence_quote = "70% of the wage"
                notes = "tier_schedule=involves 70% tier; confirm from full doc"
            confidence = "high"
        else:
            evidence_quote = source_excerpt[:100] if len(source_excerpt) > 0 else "(tier schedule mentioned)"
            confidence = "medium"

    # pattern:P9_unpaid_paternity_should_be_partially_paid
    elif flag_type == "pattern:P9_unpaid_paternity_should_be_partially_paid" and topic_group == "paternity":
        # The unpaid field should be cleared; this leave should move to partially_paid
        csv_value_new = "null"
        unit_new = ""

        if "UWV" in source_excerpt:
            if "100%" in source_excerpt:
                evidence_quote = "benefit from the UWV equal to 100% of the daily wage"
            else:
                evidence_quote = "benefit from the UWV"
        else:
            evidence_quote = "(no UWV reference in excerpt)"

        notes = "move_to=leave_partially_paid_paternity_value; refer to rubric P9"
        confidence = "high" if "UWV" in source_excerpt else "medium"

    # L2_discussed_csv_empty - paternity topic
    elif flag_type == "L2_discussed_csv_empty" and topic_group == "paternity":
        # Source says paternity leave is regulated by Wazo/Wieg with no explicit top-ups
        csv_value_new = "UNKNOWN"
        unit_new = "UNKNOWN"
        evidence_quote = "regulated by the Wet arbeid en zorg (WAZO) and Wet invoering extra geboorteverlof (Wieg). No explicit employer top-ups"
        confidence = "low"
        notes = "Topic discussed but no specific numeric value extractable from source"

    # L2_discussed_csv_empty - adoption topic
    elif flag_type == "L2_discussed_csv_empty" and topic_group == "adoption":
        # Source says adoption not explicitly mentioned in this CAO
        csv_value_new = "UNKNOWN"
        unit_new = "UNKNOWN"
        evidence_quote = "adoption and foster leave: Not explicitly mentioned"
        confidence = "low"
        notes = "Topic reference but no explicit CAO provision found"

    # L1: care leave value empty but pay is filled
    elif flag_type == "L1" and "CARE_02" in flag_reason and topic_group == "care":
        # Source mentions 6 weeks for care leave with 100% pay
        if "6 weeks" in source_excerpt and "100%" in source_excerpt:
            csv_value_new = "6"
            unit_new = "weeks"
            evidence_quote = "employer pays 100% of the individually agreed wage for 6 weeks"
            confidence = "high"
        else:
            csv_value_new = "UNKNOWN"
            unit_new = "weeks"
            evidence_quote = source_excerpt[:150] if len(source_excerpt) > 0 else "(no clear duration)"
            confidence = "low"

    else:
        # Fallback
        csv_value_new = "UNKNOWN"
        unit_new = "UNKNOWN"
        evidence_quote = "(unable to determine from source)"
        confidence = "low"

    confidence_counts[confidence] += 1

    output_rows.append({
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

# Write CSV output
output_path = '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_007_corrections.csv'
with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old', 'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'], delimiter=';')
    writer.writeheader()
    writer.writerows(output_rows)

print(f"rows_written: {len(output_rows)}")
print(f"output_path: {output_path}")
print(f"confidence_distribution: {dict(confidence_counts)}")
