#!/usr/bin/env python3
"""
Direct processor for chunk_064.jsonl - runs without bash
"""

import json
import csv
import sys
import re
from pathlib import Path

def sanitize_quote(text, max_len=200):
    """Extract and sanitize a verbatim quote."""
    if not text:
        return "(no relevant text in excerpt)"
    text = str(text).strip()
    if len(text) > max_len:
        text = text[:max_len].rstrip() + "…"
    return text


def extract_value_from_topic(topic_section, field, flag_type, flag_reason):
    """
    Extract value from topic_section based on field and flag_type.
    Returns: (csv_value_new, unit_new, evidence_quote, confidence, notes)
    """

    if not topic_section or not isinstance(topic_section, str):
        return ("UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", "empty topic_section")

    text = topic_section.strip()
    if not text:
        return ("UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", "empty topic_section")

    notes = ""

    # Pattern: L1 with FIELD_ROLE_* (duration field with wrong unit)
    if flag_type == "L1" and "FIELD_ROLE" in str(flag_reason):
        duration_match = re.search(r'(\d+)\s*(weeks?|months?|days?|hours?)', text, re.IGNORECASE)
        if duration_match:
            value = duration_match.group(1)
            unit = duration_match.group(2).lower()
            if "week" in unit:
                unit = "weeks"
            elif "month" in unit:
                unit = "months"
            elif "day" in unit:
                unit = "days"
            elif "hour" in unit:
                unit = "hours"
            quote = sanitize_quote(duration_match.group(0))
            return (value, unit, f'"{quote}"', "high", notes)
        else:
            notes = "statutory NL duration may apply but source excerpt does not state"
            return ("UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", notes)

    # Pattern: P2 tiered sick leave
    if flag_type == "pattern:P2_tiered_sick_collapse":
        sent_match = re.search(r'([^.!?]*\d+\s*%[^.!?]*(?:weeks?|wks?)[^.!?]*)', text, re.IGNORECASE)
        if sent_match:
            full_sent = sent_match.group(1)
            first_val = re.search(r'(\d+)\s*%', full_sent)
            first_value = first_val.group(1) if first_val else "UNKNOWN"
            tier_pattern = re.findall(r'(\d+)\s*%', full_sent)
            tier_str = "/".join(tier_pattern) if tier_pattern else "UNKNOWN"
            quote = sanitize_quote(full_sent)
            notes = f"tier_schedule={tier_str}"
            return (first_value, "%", f'"{quote}"', "high", notes)

    # Pattern: P4 adoption window as duration
    if flag_type == "pattern:P4_adoption_window_as_duration":
        adoption_match = re.search(r'adoptie|adoption|adopted', text, re.IGNORECASE)
        if adoption_match:
            durations = re.findall(r'(\d+)\s*(weeks?|days?)', text, re.IGNORECASE)
            if durations:
                value, unit = durations[0]
                unit = "weeks" if "week" in unit.lower() else unit.lower()
                quote = sanitize_quote(f"{value} {unit}")
                return (value, unit, f'"{quote}"', "medium", notes)
        notes = "adoption duration pattern not found in excerpt"
        return ("UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", notes)

    # Pattern: P5 education vacation undercount
    if flag_type == "pattern:P5_education_vacation_undercount":
        values = re.findall(r'(\d+)\s*(hours?|days?)', text, re.IGNORECASE)
        if len(values) >= 2:
            v1, u1 = values[0]
            v2, u2 = values[1]
            total = int(v1) + int(v2)
            unit = "hours" if "hour" in u1.lower() else u1.lower()
            quote = sanitize_quote(text[:150])
            notes = f"sum={v1}+{v2}={total}"
            return (str(total), unit, f'"{quote}"', "medium", notes)

    # Pattern: L2_discussed_csv_empty
    if flag_type == "L2_discussed_csv_empty":
        numeric_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(weeks?|days?|hours?|%|percent)?', text, re.IGNORECASE)
        if numeric_match:
            value = numeric_match.group(1).replace(',', '.')
            unit = numeric_match.group(2) if numeric_match.group(2) else ""
            quote = sanitize_quote(numeric_match.group(0))
            return (value, unit, f'"{quote}"', "high", notes)

        if re.search(r'\b(yes|true|is provided|has|includes?)\b', text, re.IGNORECASE):
            quote = sanitize_quote(text[:150])
            return ("True", "", f'"{quote}"', "medium", notes)
        if re.search(r'\b(no|false|not provided|lacks?|without)\b', text, re.IGNORECASE):
            quote = sanitize_quote(text[:150])
            return ("False", "", f'"{quote}"', "medium", notes)

        notes = "topic discussed but value for field not in excerpt"
        return ("UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", notes)

    # Default fallback
    numeric_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:weeks?|days?|hours?|%|percent)?', text)
    if numeric_match:
        value = numeric_match.group(1).replace(',', '.')
        quote = sanitize_quote(numeric_match.group(0))
        return (value, "", f'"{quote}"', "low", "generic numeric match")

    return ("UNKNOWN", "UNKNOWN", "(no relevant text in excerpt)", "low", "no matching value in topic_section")


def process_chunk_064():
    """Main processing function."""

    # Try mounted path first (for bash execution), then fall back to actual path
    input_path_mounted = Path("/sessions/confident-wonderful-cori/mnt/qa_leave/outputs/subagent_worksheets/chunks/chunk_064.jsonl")
    input_path_actual = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_064.jsonl")
    output_path_mounted = Path("/sessions/confident-wonderful-cori/mnt/qa_leave/outputs/subagent_worksheets/chunks/chunk_064_corrections.csv")
    output_path_actual = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_064_corrections.csv")

    input_path = input_path_mounted if input_path_mounted.exists() else input_path_actual
    output_path = output_path_mounted if input_path_mounted.exists() else output_path_actual

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        return 1

    header = [
        "item_id", "record_id", "topic_group", "field",
        "csv_value_old", "csv_value_new", "unit_new", "evidence_quote",
        "confidence", "notes"
    ]

    rows = []
    conf_dist = {"high": 0, "medium": 0, "low": 0}
    count_unknown = 0
    error_count = 0

    print(f"Processing {input_path}...", file=sys.stderr)

    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    error_count += 1
                    print(f"ERROR line {line_num}: {e}", file=sys.stderr)
                    continue

                record_id = record.get("record_id", "UNKNOWN")
                items = record.get("items", [])

                for item in items:
                    item_id = item.get("item_id", "")
                    topic_group = item.get("topic_group", "")
                    field = item.get("field", "")
                    csv_value_old = item.get("csv_value_old", "")
                    context_type = item.get("context_type", "")
                    topic_section = item.get("topic_section", "")
                    flag_type = item.get("flag_type", "")
                    flag_reason = item.get("flag_reason", "")

                    csv_value_new, unit_new, evidence_quote, confidence, notes = extract_value_from_topic(
                        topic_section, field, flag_type, flag_reason
                    )

                    # HARD RULE: if evidence_quote starts with '(', confidence must be 'low' and value must be UNKNOWN
                    if evidence_quote.startswith("("):
                        confidence = "low"
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"

                    row = {
                        "item_id": item_id,
                        "record_id": record_id,
                        "topic_group": topic_group,
                        "field": field,
                        "csv_value_old": csv_value_old,
                        "csv_value_new": csv_value_new,
                        "unit_new": unit_new,
                        "evidence_quote": evidence_quote,
                        "confidence": confidence,
                        "notes": notes
                    }

                    rows.append(row)

                    if csv_value_new == "UNKNOWN":
                        count_unknown += 1
                    conf_dist[confidence] += 1

    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        return 1

    # Write CSV with proper escaping
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=header,
                delimiter=';',
                quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(f"ERROR writing CSV: {e}", file=sys.stderr)
        return 1

    # Verify with pandas
    rows_written = len(rows)
    pandas_ok = "unavailable"
    try:
        import pandas as pd
        df = pd.read_csv(output_path, delimiter=';', quoting=csv.QUOTE_ALL)
        rows_written = len(df)
        pandas_ok = "OK"
    except ImportError:
        pandas_ok = "pandas not available"
    except Exception as e:
        print(f"ERROR: pandas read-back failed: {e}", file=sys.stderr)
        pandas_ok = f"ERROR: {e}"

    # Report (per spec: ONLY rows_written, output_path, confidence distribution, count UNKNOWN, pandas-read-back)
    print(f"\nrows_written: {rows_written}")
    print(f"output_path: {output_path}")
    print(f"confidence_distribution: high={conf_dist['high']} medium={conf_dist['medium']} low={conf_dist['low']}")
    print(f"count_UNKNOWN: {count_unknown}")
    print(f"pandas_read_back: {rows_written} rows × {len(header)} columns, {pandas_ok}")
    if error_count > 0:
        print(f"json_parse_errors: {error_count}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(process_chunk_064())
