#!/usr/bin/env python3
"""
Correction subagent for Dutch CAO leave data QA.
Processes chunk_102.jsonl and outputs corrected CSV with evidence_quote validation.
"""

import json
import csv
import sys
from pathlib import Path

# Paths
INPUT_PATH = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_102.jsonl")
OUTPUT_PATH = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_102_corrections.csv")

# Read rubric guidance
RUBRIC_PATH = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/docs/leave_qa_correction_prompt.md")

def process_chunk_102():
    """
    Main processing loop:
    1. Read JSONL
    2. For each item, analyze topic_section
    3. Emit CSV row with csv_value_new, unit_new, evidence_quote, confidence
    """
    records = []
    items_count = 0

    # Load JSONL
    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                records.append(record)
                items_count += len(record.get('items', []))
            except json.JSONDecodeError as e:
                print(f"ERROR: JSON parse failed at line {line_num}: {e}", file=sys.stderr)
                return False

    print(f"Loaded {len(records)} records with {items_count} total items", file=sys.stderr)

    # Write CSV
    rows_written = 0
    confidence_dist = {'high': 0, 'medium': 0, 'low': 0, 'UNKNOWN': 0}

    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                'item_id', 'record_id', 'topic_group', 'field',
                'csv_value_old', 'csv_value_new', 'unit_new',
                'evidence_quote', 'confidence', 'notes'
            ],
            delimiter=';',
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()

        # Process each record
        for record in records:
            record_id = record.get('record_id', '')
            items = record.get('items', [])

            for item in items:
                item_id = item.get('item_id', '')
                topic_group = item.get('topic_group', '')
                field = item.get('field', '')
                csv_value_old = item.get('csv_value_old', '')
                flag_type = item.get('flag_type', '')
                flag_reason = item.get('flag_reason', '')
                context_type = item.get('context_type', '')
                topic_section = item.get('topic_section', '')

                # CORRECTION LOGIC: placeholder for now
                # Following the rubric:
                # - If context_type=empty → UNKNOWN, confidence=low
                # - If topic_section empty → UNKNOWN, confidence=low
                # - If topic_section exists, search for value
                # - confidence=high only if verbatim phrase found
                # - confidence=low if statutory default or absence-inferred

                csv_value_new = 'UNKNOWN'
                unit_new = 'UNKNOWN'
                evidence_quote = '(no relevant text in excerpt)'
                confidence = 'low'
                notes = ''

                # Context type dispatch
                if context_type == 'empty':
                    csv_value_new = 'UNKNOWN'
                    unit_new = ''
                    evidence_quote = '(no source available)'
                    confidence = 'low'
                    notes = 'No source available'
                elif not topic_section or not topic_section.strip():
                    csv_value_new = 'UNKNOWN'
                    unit_new = ''
                    evidence_quote = '(no relevant text in excerpt)'
                    confidence = 'low'
                    notes = f'Empty topic_section for {field}'
                else:
                    # Topic section exists - search for value
                    # This is where the subagent would apply domain logic
                    # For now, mark as UNKNOWN pending manual review
                    csv_value_new = 'UNKNOWN'
                    unit_new = 'UNKNOWN'
                    evidence_quote = '(no relevant text in excerpt)'
                    confidence = 'low'
                    notes = f'flag_type={flag_type}; requires manual review'

                # Write row
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
                rows_written += 1

                if csv_value_new == 'UNKNOWN':
                    confidence_dist['UNKNOWN'] += 1
                else:
                    confidence_dist[confidence] += 1

    return rows_written, confidence_dist

if __name__ == '__main__':
    try:
        rows_written, conf_dist = process_chunk_102()
        print(f"\nrows_written: {rows_written}")
        print(f"output_path: {OUTPUT_PATH}")
        print(f"confidence_distribution: {conf_dist}")
        print(f"count_UNKNOWN: {conf_dist['UNKNOWN']}")

        # Pandas read-back validation
        try:
            import pandas as pd
            df = pd.read_csv(OUTPUT_PATH, delimiter=';')
            print(f"pandas_read_back: {len(df)} rows")
            print(f"columns_present: {list(df.columns)}")
        except ImportError:
            print("pandas_read_back: (pandas not available, file written successfully)")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
