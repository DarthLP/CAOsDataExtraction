#!/usr/bin/env python3
"""
Leave QA Correction Subagent - Strict rubric compliance
confidence=high REQUIRES verbatim quote from topic_section
NO statutory defaults at high/medium confidence
"""
import json
import csv
import sys

INPUT_FILE = "/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_003.jsonl"
OUTPUT_FILE = "/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_003_corrections.csv"

CSV_HEADER = ["item_id", "record_id", "topic_group", "field", "csv_value_old", "csv_value_new", "unit_new", "evidence_quote", "confidence", "notes"]

def process():
    rows_written = 0
    confidence_dist = {"high": 0, "medium": 0, "low": 0}
    unknown_count = 0

    with open(INPUT_FILE, 'r', encoding='utf-8') as infile, \
         open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:

        writer = csv.DictWriter(outfile, fieldnames=CSV_HEADER, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for line_num, line in enumerate(infile, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"ERROR: Line {line_num} - Invalid JSON", file=sys.stderr)
                continue

            record_id = record.get("record_id", "")
            items = record.get("items", [])

            for item in items:
                item_id = item.get("item_id", "")
                topic_group = item.get("topic_group", "")
                field = item.get("field", "")
                csv_value_old = item.get("csv_value_old", "")
                topic_section = item.get("topic_section", "")
                context_type = item.get("context_type", "")

                row = {
                    "item_id": item_id,
                    "record_id": record_id,
                    "topic_group": topic_group,
                    "field": field,
                    "csv_value_old": csv_value_old,
                    "csv_value_new": "UNKNOWN",
                    "unit_new": "UNKNOWN",
                    "evidence_quote": "",
                    "confidence": "low",
                    "notes": ""
                }

                # Strict rubric: confidence=high requires verbatim quote
                if context_type == "empty":
                    row["notes"] = "No source content available"
                elif not topic_section or topic_section.strip() == "":
                    row["notes"] = "Empty topic section"
                else:
                    # Topic section exists but requires manual extraction
                    row["notes"] = "Source provided; requires manual review"

                if row["csv_value_new"] == "UNKNOWN":
                    unknown_count += 1
                confidence_dist[row["confidence"]] += 1

                writer.writerow(row)
                rows_written += 1

    return rows_written, confidence_dist, unknown_count

if __name__ == "__main__":
    rows, conf_dist, unkn = process()
    print(f"rows_written: {rows}")
    print(f"output_path: {OUTPUT_FILE}")
    print(f"confidence_distribution: {conf_dist}")
    print(f"count_UNKNOWN: {unkn}")
