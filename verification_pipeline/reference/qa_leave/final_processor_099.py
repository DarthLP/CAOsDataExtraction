#!/usr/bin/env python3
"""
Correction processor for chunk_099.jsonl
Reads from /Users/.../chunk_099.jsonl
Outputs to /Users/.../chunk_099_corrections.csv

Per rubric v2:
- confidence=high: source has verbatim phrase explicitly stating/implying answer
- confidence=medium: source mentions topic with partial info
- confidence=low: source does NOT contain answer, value=UNKNOWN, no placeholders in quote

HARD RULE: if evidence_quote starts with '(', confidence=low and value=UNKNOWN
"""

import json, csv, re, sys
from pathlib import Path
from typing import Tuple

INPUT_FILE = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099.jsonl')
OUTPUT_FILE = Path('/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_099_corrections.csv')

def sanitize_quote(text, max_len=200):
    """Ensure quote doesn't start with ( and is verbatim from source."""
    if not text or not isinstance(text, str):
        return "(no relevant text in excerpt)"
    text = str(text).strip()
    if text.startswith('('):
        return "(no relevant text in excerpt)"
    if len(text) > max_len:
        text = text[:max_len].rstrip()
    return text

def extract_value(topic_section: str, field: str, flag_type: str) -> Tuple[str, str, str, str, str]:
    """Extract (value, unit, quote, confidence, notes) from topic_section."""

    if not topic_section or not isinstance(topic_section, str):
        return "UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", "no source"

    text = topic_section.strip()
    if not text:
        return "UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", "empty source"

    # P2_tiered_sick_collapse: preserve 100% as first tier
    if "P2_tiered" in flag_type:
        # Look for "100% of the last earned salary" or "100% of last earned salary"
        m = re.search(r'(\d+)%\s+of\s+(?:the\s+)?last earned salary', text, re.I)
        if m:
            value = m.group(1)
            # Extract context: 40 chars before, 120 after
            start = max(0, m.start() - 40)
            end = min(len(text), m.end() + 120)
            quote_text = text[start:end].strip()
            quote = sanitize_quote(quote_text)

            # Check for second-year rate to document tier
            m2 = re.search(r'second year.*?(\d+)%', text, re.I)
            tier_note = ""
            if m2:
                tier_note = f"; tier_schedule={value}/{m2.group(1)}"

            return value, "%", f'"{quote}"', "high", f"P2_tiered{tier_note}"

    # Care leave: looking for duration
    if "CARE" in flag_type or ("care" in text.lower() and "care" in field.lower()):
        # Look for "X days", "X weeks", etc.
        m = re.search(r'(\d+)\s+(days?|weeks?|hours?|months?)', text, re.I)
        if m:
            value = m.group(1)
            unit = m.group(2).lower()
            # Normalize
            if "day" in unit:
                unit = "days"
            elif "week" in unit:
                unit = "weeks"
            elif "hour" in unit:
                unit = "hours"
            elif "month" in unit:
                unit = "months"

            start = max(0, m.start() - 40)
            end = min(len(text), m.end() + 60)
            quote_text = text[start:end].strip()
            quote = sanitize_quote(quote_text)
            return value, unit, f'"{quote}"', "high", "found in source"

    # L2_discussed_csv_empty with maternity: look for maternity-specific values
    if "L2_discussed" in flag_type and "maternity" in field.lower():
        # Look for maternity weeks/months/duration
        m = re.search(r'maternity.*?(\d+)\s+(weeks?|days?|months?)', text, re.I)
        if m:
            value = m.group(1)
            unit = m.group(2).lower()
            start = max(0, m.start() - 30)
            end = min(len(text), m.end() + 50)
            quote_text = text[start:end].strip()
            quote = sanitize_quote(quote_text)
            return value, unit, f'"{quote}"', "high", "maternity value found"

    # L2_discussed_csv_empty with adoption
    if "L2_discussed" in flag_type and "adoption" in field.lower():
        m = re.search(r'adoption.*?(\d+)\s+(weeks?|days?)', text, re.I)
        if m:
            value = m.group(1)
            unit = m.group(2).lower()
            start = max(0, m.start() - 30)
            end = min(len(text), m.end() + 50)
            quote_text = text[start:end].strip()
            quote = sanitize_quote(quote_text)
            return value, unit, f'"{quote}"', "high", "adoption value found"

    # Generic fallback: any percentage or duration
    m = re.search(r'(\d+)%|(\d+)\s+(weeks?|days?|months?|hours?)', text, re.I)
    if m:
        if m.group(1):  # Percentage match
            value = m.group(1)
            unit = "%"
        else:  # Duration match
            value = m.group(2)
            unit = m.group(3).lower()
            if "day" in unit:
                unit = "days"
            elif "week" in unit:
                unit = "weeks"
            elif "hour" in unit:
                unit = "hours"
            elif "month" in unit:
                unit = "months"

        start = max(0, m.start() - 30)
        end = min(len(text), m.end() + 40)
        quote_text = text[start:end].strip()
        quote = sanitize_quote(quote_text)
        return value, unit, f'"{quote}"', "medium", "generic pattern match"

    # No value found
    return "UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", "value not in source"

# Main processing
records = []
with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            records.append(json.loads(line))

print(f"Loaded {len(records)} records from chunk_099.jsonl", file=sys.stderr)

all_items = []
for rec in records:
    for item in rec.get('items', []):
        item['record_id'] = rec.get('record_id', '')
        all_items.append(item)

print(f"Total items to process: {len(all_items)}", file=sys.stderr)

# Write output CSV
conf_dist = {'high': 0, 'medium': 0, 'low': 0}
unknown_count = 0
rows_written = 0

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writerow(['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
                     'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes'])

    for item in all_items:
        topic_section = item.get('topic_section', '')
        field = item.get('field', '')
        flag_type = item.get('flag_type', '')

        value, unit, quote, conf, notes = extract_value(topic_section, field, flag_type)

        if value == 'UNKNOWN':
            unknown_count += 1

        row = [
            item.get('item_id', ''),
            item.get('record_id', ''),
            item.get('topic_group', ''),
            field,
            item.get('csv_value_old', ''),
            value,
            unit,
            quote,
            conf,
            notes
        ]
        writer.writerow(row)
        rows_written += 1
        conf_dist[conf] += 1

print(f"\n=== SUMMARY ===", file=sys.stderr)
print(f"rows_written: {rows_written}", file=sys.stderr)
print(f"output_path: {OUTPUT_FILE}", file=sys.stderr)
print(f"confidence_distribution: {conf_dist}", file=sys.stderr)
print(f"count_UNKNOWN: {unknown_count}", file=sys.stderr)

# Validate with pandas
try:
    import pandas as pd
    df = pd.read_csv(OUTPUT_FILE, delimiter=';', quoting=1)
    print(f"\npandas_read_back: {len(df)} rows", file=sys.stderr)
    print(f"confidence_breakdown: {df['confidence'].value_counts().to_dict()}", file=sys.stderr)
    unknown_in_csv = (df['csv_value_new'] == 'UNKNOWN').sum()
    print(f"UNKNOWN_values: {unknown_in_csv}", file=sys.stderr)
except Exception as e:
    print(f"Pandas validation failed: {e}", file=sys.stderr)
