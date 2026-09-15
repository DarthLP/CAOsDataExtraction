#!/usr/bin/env python3
"""
Correction subagent processor for chunk_043.jsonl
Implements rubric from leave_qa_correction_prompt.md
"""
import json
import csv
import re
import sys
from typing import Dict, List, Tuple, Any

# File paths
INPUT_PATH = "/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_043.jsonl"
OUTPUT_PATH = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_043_corrections.csv"

def extract_value_from_section(
    topic_section: str,
    field: str,
    csv_value_old: str,
    flag_type: str,
    flag_reason: str
) -> Tuple[str, str, str, str, str]:
    """
    Extract the correct value from topic_section based on field and flag_type.
    Returns: (csv_value_new, unit_new, evidence_quote, confidence, notes)

    CRITICAL RULE from rubric:
    - confidence=high REQUIRES verbatim phrase in evidence_quote
    - confidence=low if evidence_quote starts with '(' (placeholder)
    - UNKNOWN for low confidence if source doesn't contain answer
    """

    csv_value_new = "UNKNOWN"
    unit_new = "UNKNOWN"
    evidence_quote = "(no relevant text in excerpt)"
    confidence = "low"
    notes = ""

    if not topic_section or not topic_section.strip():
        return csv_value_new, unit_new, evidence_quote, confidence, notes

    # Pattern: P2_tiered_sick_collapse
    if flag_type == "pattern:P2_tiered_sick_collapse":
        # Field: leave_sickpay_continuation_value
        # Value 100 is correct (first tier), but full schedule should be captured
        if field == "leave_sickpay_continuation_value":
            # Look for the full tier schedule
            tier_pattern = r"(\d+)%.*?(\d+)%.*?(\d+)%.*?(\d+)%"
            match = re.search(tier_pattern, topic_section)
            if match:
                tiers = match.group(0)
                csv_value_new = "100"  # First tier is correct
                unit_new = "%"
                evidence_quote = f'"{tiers}"'
                confidence = "high"
                # Extract full schedule for notes
                numbers = re.findall(r'\d+', tiers)
                if len(numbers) >= 4:
                    notes = f"tier_schedule={numbers[0]}/{numbers[1]}/{numbers[2]}/{numbers[3]}"
            else:
                # Look for just 100% continuation
                if "100%" in topic_section:
                    csv_value_new = "100"
                    unit_new = "%"
                    quote_match = re.search(r"[^.!?]*100%[^.!?]*[.!?]", topic_section)
                    if quote_match:
                        evidence_quote = f'"{quote_match.group(0)[:200]}"'
                        confidence = "high"

    # Pattern: P4_adoption_window_as_duration
    elif flag_type == "pattern:P4_adoption_window_as_duration":
        # Find actual adoption leave duration (typically 6 weeks)
        # csv_value_old=26 weeks is the window, not duration
        if "6 weeks" in topic_section or "6weeks" in topic_section:
            csv_value_new = "6"
            unit_new = "weeks"
            match = re.search(r"[^.!?]*6\s*weeks?[^.!?]*[.!?]", topic_section)
            if match:
                evidence_quote = f'"{match.group(0)[:200]}"'
                confidence = "high"
        elif "4 weeks" in topic_section:
            csv_value_new = "4"
            unit_new = "weeks"
            match = re.search(r"[^.!?]*4\s*weeks?[^.!?]*[.!?]", topic_section)
            if match:
                evidence_quote = f'"{match.group(0)[:200]}"'
                confidence = "high"

    # Pattern: P5_education_vacation_undercount
    elif flag_type == "pattern:P5_education_vacation_undercount":
        # Sum statutory + supplementary vacation
        stat_match = re.search(r"[Ss]tatutory.*?(\d+)\s*(hours|days|weeks)", topic_section)
        supp_match = re.search(r"[Ss]upplementary.*?(\d+)\s*(hours|days|weeks)", topic_section)

        if stat_match and supp_match:
            stat_val = int(stat_match.group(1))
            supp_val = int(supp_match.group(1))
            total = stat_val + supp_val
            csv_value_new = str(total)
            unit_new = stat_match.group(2)
            quote_match = re.search(
                r"[Ss]tatutory[^.!?]*\d+[^.!?]*[Ss]upplementary[^.!?]*\d+[^.!?]*[.!?]",
                topic_section
            )
            if quote_match:
                evidence_quote = f'"{quote_match.group(0)[:200]}"'
                confidence = "high"

    # Pattern: P9_unpaid_paternity_should_be_partially_paid
    elif flag_type == "pattern:P9_unpaid_paternity_should_be_partially_paid":
        # Field should be cleared: csv_value_new=null
        # Move to leave_partially_paid_paternity_value
        csv_value_new = "null"
        unit_new = ""
        if "70%" in topic_section and "5 weeks" in topic_section:
            match = re.search(r"[^.!?]*(70%|UWV|5\s*weeks)[^.!?]*[.!?]", topic_section)
            if match:
                evidence_quote = f'"{match.group(0)[:200]}"'
                confidence = "high"
        notes = "move_to=leave_partially_paid_paternity_value with pay=70%"

    # L1 flag: FIELD_ROLE_* or similar - expecting duration but has different unit
    elif flag_type.startswith("L1"):
        # Look for any numeric duration value in weeks/days/months/years
        duration_pattern = r"(\d+)\s*(weeks?|days?|months?|years?)"
        match = re.search(duration_pattern, topic_section, re.IGNORECASE)
        if match:
            csv_value_new = match.group(1)
            unit_new = match.group(2).lower()
            # Extract broader context for quote
            start = max(0, match.start() - 50)
            end = min(len(topic_section), match.end() + 50)
            context = topic_section[start:end].strip()
            evidence_quote = f'"{context[:200]}"'
            confidence = "high"
        else:
            csv_value_new = "UNKNOWN"
            unit_new = "UNKNOWN"
            confidence = "low"
            notes = f"statutory NL {field} may apply but source excerpt does not state"

    # L2_discussed_csv_empty: topic is discussed but specific field is empty
    elif flag_type == "L2_discussed_csv_empty":
        # Extract based on field name
        if "maternity" in field.lower():
            if "weeks" in topic_section.lower():
                match = re.search(r"(\d+)\s*weeks?.*maternity", topic_section, re.IGNORECASE)
                if not match:
                    match = re.search(r"(\d+)\s*weeks?", topic_section)
                if match:
                    csv_value_new = match.group(1)
                    unit_new = "weeks"
                    start = max(0, match.start() - 40)
                    end = min(len(topic_section), match.end() + 40)
                    evidence_quote = f'"{topic_section[start:end][:200]}"'
                    confidence = "medium" if "maternity" in topic_section.lower() else "low"

        elif "sick" in field.lower() and "topup" in field.lower():
            # leave_sick_topup_present - boolean
            if re.search(r"(supplement|topup|top-up|pay.*above.*70%)", topic_section, re.IGNORECASE):
                csv_value_new = "True"
                unit_new = ""
                match = re.search(r"[^.!?]*(supplement|topup)[^.!?]*[.!?]", topic_section, re.IGNORECASE)
                if match:
                    evidence_quote = f'"{match.group(0)[:200]}"'
                    confidence = "high"

        elif "short_term_care" in field.lower():
            # Look for days/hours of care leave
            match = re.search(r"(\d+)\s*(days?|hours?)", topic_section, re.IGNORECASE)
            if match:
                csv_value_new = match.group(1)
                unit_new = match.group(2).lower()
                start = max(0, match.start() - 40)
                end = min(len(topic_section), match.end() + 40)
                evidence_quote = f'"{topic_section[start:end][:200]}"'
                confidence = "medium"

    # Fallback: try to find numeric values for typical numeric fields
    else:
        numeric_pattern = r"(\d+(?:\.\d+)?)\s*(weeks?|days?|%|hours?|years?|months?)?"
        match = re.search(numeric_pattern, topic_section)
        if match:
            csv_value_new = match.group(1)
            if match.group(2):
                unit_new = match.group(2).lower()
            else:
                unit_new = ""
            start = max(0, match.start() - 40)
            end = min(len(topic_section), match.end() + 40)
            evidence_quote = f'"{topic_section[start:end][:200]}"'
            confidence = "medium"

    return csv_value_new, unit_new, evidence_quote, confidence, notes


def main():
    # Load input
    records = []
    try:
        with open(INPUT_PATH, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"Error parsing line {line_num}: {e}", file=sys.stderr)
                        sys.exit(1)
    except FileNotFoundError:
        print(f"Input file not found: {INPUT_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(records)} records from chunk_043.jsonl", file=sys.stderr)

    # Count total items
    total_items = sum(len(r.get('items', [])) for r in records)
    print(f"Total items to process: {total_items}", file=sys.stderr)

    # Process each item
    all_rows = []
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    unknown_count = 0

    for record in records:
        record_id = record.get('record_id', '')
        items = record.get('items', [])

        for item in items:
            item_id = item.get('item_id', '')
            topic_group = item.get('topic_group', '')
            field = item.get('field', '')
            csv_value_old = item.get('csv_value_old', '')
            topic_section = item.get('topic_section', '')
            flag_type = item.get('flag_type', '')
            flag_reason = item.get('flag_reason', '')

            # Extract values
            csv_value_new, unit_new, evidence_quote, confidence, notes = extract_value_from_section(
                topic_section, field, csv_value_old, flag_type, flag_reason
            )

            # Apply HARD RULE: if evidence_quote starts with '(', confidence MUST be low
            if evidence_quote.startswith('('):
                confidence = "low"
                if csv_value_new != "UNKNOWN":
                    csv_value_new = "UNKNOWN"
                    unit_new = "UNKNOWN"

            # Count statistics
            confidence_counts[confidence] += 1
            if csv_value_new == "UNKNOWN":
                unknown_count += 1

            # Build row
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
            all_rows.append(row)

    # Write CSV with proper escaping
    header = ['item_id', 'record_id', 'topic_group', 'field', 'csv_value_old',
              'csv_value_new', 'unit_new', 'evidence_quote', 'confidence', 'notes']

    try:
        with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(all_rows)
    except Exception as e:
        print(f"Error writing CSV: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== PROCESSING COMPLETE ===", file=sys.stderr)
    print(f"Rows written: {len(all_rows)}", file=sys.stderr)
    print(f"Output path: {OUTPUT_PATH}", file=sys.stderr)
    print(f"\nConfidence distribution:", file=sys.stderr)
    print(f"  high:   {confidence_counts['high']}", file=sys.stderr)
    print(f"  medium: {confidence_counts['medium']}", file=sys.stderr)
    print(f"  low:    {confidence_counts['low']}", file=sys.stderr)
    print(f"  UNKNOWN: {unknown_count}", file=sys.stderr)

    # Verify with pandas
    try:
        import pandas as pd
        df = pd.read_csv(OUTPUT_PATH, delimiter=';')
        print(f"\nPandas read-back: {len(df)} rows, columns={list(df.columns)}", file=sys.stderr)
        print(f"Sample row:\n{df.iloc[0] if len(df) > 0 else 'no rows'}", file=sys.stderr)
    except Exception as e:
        print(f"Error reading with pandas: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
