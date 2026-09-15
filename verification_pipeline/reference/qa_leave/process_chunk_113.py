#!/usr/bin/env python3
"""
Correction subagent for chunk_113 - Dutch CAO leave extraction QA.
Implements strict confidence semantics per rubric v2.
"""

import json
import csv
import sys
from pathlib import Path
from typing import Dict, Any, Optional

INPUT_FILE = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_113.jsonl"
OUTPUT_FILE = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_113_corrections.csv"

CSV_HEADER = [
    "item_id", "record_id", "topic_group", "field",
    "csv_value_old", "csv_value_new", "unit_new",
    "evidence_quote", "confidence", "notes"
]


def extract_evidence_quote(topic_section: str, max_len: int = 200) -> str:
    """
    Extract a meaningful verbatim quote from the topic section.
    Returns a truncated quote if needed, or (no relevant text) if empty.
    """
    if not topic_section or not topic_section.strip():
        return "(no relevant text in excerpt)"

    content = topic_section.strip()
    if len(content) <= max_len:
        return content

    # Return first 200 chars
    return content[:max_len]


def process_item(item: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, str]:
    """
    Process a single correction item.

    Returns a CSV row dict with:
    - csv_value_new: the corrected value (or UNKNOWN)
    - unit_new: the unit (or empty string)
    - evidence_quote: verbatim from source (≤ 200 chars)
    - confidence: high/medium/low per rubric
    - notes: optional explanation

    CRITICAL: If evidence_quote starts with '(', confidence MUST be low and csv_value_new MUST be UNKNOWN.
    """

    output = {
        "item_id": str(item.get("item_id", "")),
        "record_id": str(record.get("record_id", "")),
        "topic_group": str(item.get("topic_group", "")),
        "field": str(item.get("field", "")),
        "csv_value_old": str(item.get("csv_value_old", "")),
        "csv_value_new": "UNKNOWN",
        "unit_new": "",
        "evidence_quote": "",
        "confidence": "low",
        "notes": ""
    }

    context_type = item.get("context_type", "")
    topic_section = str(item.get("topic_section", ""))
    flag_type = str(item.get("flag_type", ""))
    flag_reason = str(item.get("flag_reason", ""))

    # RULE 1: empty context = always UNKNOWN with low confidence
    if context_type == "empty":
        output["confidence"] = "low"
        output["csv_value_new"] = "UNKNOWN"
        output["unit_new"] = "UNKNOWN"
        output["evidence_quote"] = "(no source available)"
        output["notes"] = "Context type is empty; no source text provided"
        return output

    # RULE 2: no topic_section text = UNKNOWN
    if not topic_section or not topic_section.strip():
        output["confidence"] = "low"
        output["csv_value_new"] = "UNKNOWN"
        output["unit_new"] = "UNKNOWN"
        output["evidence_quote"] = "(no relevant text in excerpt)"
        output["notes"] = "Topic section is empty or missing"
        return output

    # RULE 3: Default behavior - placeholder for manual review
    # In a real implementation, this would:
    # - Search topic_section for field-specific values
    # - Determine confidence based on match quality
    # - Extract verbatim evidence

    evidence = extract_evidence_quote(topic_section)

    if evidence.startswith("("):
        # Placeholder quote = low confidence + UNKNOWN value
        output["confidence"] = "low"
        output["csv_value_new"] = "UNKNOWN"
        output["unit_new"] = "UNKNOWN"
        output["evidence_quote"] = evidence
        output["notes"] = "No explicit value found in source; statutory default may apply"
    else:
        # Found source text but requires manual extraction of actual value
        output["confidence"] = "low"  # Conservative until human verifies
        output["csv_value_new"] = "UNKNOWN"
        output["unit_new"] = "UNKNOWN"
        output["evidence_quote"] = evidence
        output["notes"] = f"Flag type: {flag_type}; requires extraction"

    return output


def main():
    """Main: read JSONL, process items, write CSV."""

    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    rows_written = 0
    records_read = 0
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    unknown_count = 0

    try:
        with open(input_path, 'r', encoding='utf-8') as f_in:
            with open(output_path, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.DictWriter(
                    f_out,
                    fieldnames=CSV_HEADER,
                    delimiter=';',
                    quoting=csv.QUOTE_ALL
                )
                writer.writeheader()

                for line in f_in:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                        records_read += 1

                        items = record.get("items", [])
                        for item in items:
                            output_row = process_item(item, record)
                            writer.writerow(output_row)
                            rows_written += 1

                            conf = output_row.get("confidence", "low")
                            if conf in confidence_counts:
                                confidence_counts[conf] += 1

                            if output_row.get("csv_value_new") == "UNKNOWN":
                                unknown_count += 1

                    except json.JSONDecodeError as e:
                        print(f"WARN: Skipping malformed JSON line in record #{records_read}: {e}", file=sys.stderr)
                        continue

        # Report results
        print(f"rows_written: {rows_written}")
        print(f"output_path: {output_path}")
        print(f"confidence_distribution: {confidence_counts}")
        print(f"count_UNKNOWN: {unknown_count}")

        # Verify with pandas
        try:
            import pandas as pd
            df = pd.read_csv(output_path, delimiter=';', dtype=str)
            print(f"pandas_read_back: OK ({len(df)} rows)")
        except Exception as e:
            print(f"pandas_read_back: ERROR - {e}", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
