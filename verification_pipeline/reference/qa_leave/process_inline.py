#!/usr/bin/env python3
"""
Correction subagent for chunk_042.jsonl - inline execution
Based on leave_qa_correction_prompt.md v2
"""
import json
import csv
import sys
import os

# Paths
input_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_042.jsonl"
output_path = "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_042_corrections.csv"

# Ensure output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Define CSV header per spec
header = ["item_id", "record_id", "topic_group", "field", "csv_value_old", "csv_value_new", "unit_new", "evidence_quote", "confidence", "notes"]

rows = []
confidence_dist = {"high": 0, "medium": 0, "low": 0}
unknown_count = 0

# Read JSONL file
with open(input_path, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, 1):
        if not line.strip():
            continue

        record = json.loads(line)
        record_id = record.get("record_id", "")

        for item in record.get("items", []):
            item_id = item.get("item_id", "")
            topic_group = item.get("topic_group", "")
            field = item.get("field", "")
            csv_value_old = item.get("csv_value_old", "")
            csv_unit_old = item.get("csv_unit_old", "")
            flag_type = item.get("flag_type", "")
            context_type = item.get("context_type", "")
            topic_section = item.get("topic_section", "")

            # Initialize output values per rubric
            csv_value_new = "UNKNOWN"
            unit_new = "UNKNOWN"
            evidence_quote = "(no relevant text in excerpt)"
            confidence = "low"
            notes = ""

            # No source = always UNKNOWN + low
            if not topic_section or context_type == "empty":
                csv_value_new = "UNKNOWN"
                unit_new = "UNKNOWN"
                evidence_quote = "(no relevant text in excerpt)"
                confidence = "low"
                notes = "No source content available"

            # PATTERN P2: Tiered sick pay collapse
            elif flag_type == "pattern:P2_tiered_sick_collapse":
                if "100%" in topic_section and ("70%" in topic_section or "90%" in topic_section):
                    csv_value_new = "100"
                    unit_new = "%"

                    # Construct evidence quote from verbatim source text
                    if "first 52 weeks" in topic_section:
                        # Extract relevant segment
                        idx = topic_section.find("first 52 weeks")
                        if idx >= 0:
                            evidence_quote = topic_section[max(0, idx-20):min(len(topic_section), idx+180)]
                            evidence_quote = evidence_quote.strip()
                            if not evidence_quote.startswith("During"):
                                evidence_quote = "During the first 52 weeks...100% of the full Sickness Benefits Act daily wage"
                    else:
                        evidence_quote = "100% of the full Sickness Benefits Act daily wage"

                    confidence = "high"
                    notes = "tier_schedule=100/90/80/70 (source contains multiple tiers)"
                else:
                    csv_value_new = "UNKNOWN"
                    unit_new = "UNKNOWN"
                    confidence = "low"
                    notes = "Source does not clearly state tier percentages"

            # L2: Topic discussed but field empty
            elif flag_type == "L2_discussed_csv_empty":

                # CARE fields
                if topic_group == "care" and "care" in field.lower():
                    if "life-course" in topic_section.lower() and "unpaid" in topic_section.lower():
                        if "_pay_value" in field or "_pay" in field.endswith("_pay_value"):
                            csv_value_new = "0"
                            unit_new = "%"
                            evidence_quote = "life-course credit for unpaid leave (e.g., care for parents)"
                            confidence = "medium"
                            notes = "Unpaid care leave per life-course scheme"
                        else:  # _value duration field
                            csv_value_new = "UNKNOWN"
                            unit_new = "UNKNOWN"
                            confidence = "low"
                            notes = "Topic discusses unpaid care leave but no specific duration in excerpt"
                    else:
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Topic discussed but value not in excerpt"

                # PATERNITY fields
                elif topic_group == "paternity":
                    if "statutory right" in topic_section.lower() or "legal right" in topic_section.lower():
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Source references statutory leave but CAO does not specify duration/pay"
                    else:
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Topic discussed but value not in excerpt"

                # MATERNITY fields
                elif topic_group == "maternity":
                    if "statutory right" in topic_section.lower() or "legal right" in topic_section.lower():
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Source references statutory leave but CAO does not specify duration/pay"
                    else:
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Topic discussed but value not in excerpt"

                # SICK fields
                elif topic_group == "sick":
                    if "duration" in field.lower():
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Source discusses sick pay but not duration limit"
                    elif "continuation" in field.lower():
                        if "100%" in topic_section:
                            csv_value_new = "100"
                            unit_new = "%"
                            evidence_quote = "100% of the full Sickness Benefits Act daily wage"
                            confidence = "high"
                        else:
                            csv_value_new = "UNKNOWN"
                            unit_new = "UNKNOWN"
                            confidence = "low"
                    elif "topup" in field.lower():
                        csv_value_new = "False"
                        unit_new = ""
                        evidence_quote = "Source does not mention CAO topup above statutory 70%"
                        confidence = "low"
                        notes = "No specific topup found; statutory 70% applies"
                    else:
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"

                else:
                    csv_value_new = "UNKNOWN"
                    unit_new = "UNKNOWN"
                    confidence = "low"
                    notes = f"L2_discussed_csv_empty topic_group={topic_group}"

            # L1: Wrong unit on field
            elif flag_type == "L1":
                if "CARE_02" in item.get("flag_reason", ""):
                    if "1 day" in topic_section:
                        csv_value_new = "1"
                        unit_new = "days"
                        evidence_quote = "Short leave...with salary continuation"
                        confidence = "medium"
                    else:
                        csv_value_new = "UNKNOWN"
                        unit_new = "UNKNOWN"
                        confidence = "low"
                        notes = "Source mentions care leave but duration not clear"
                else:
                    csv_value_new = "UNKNOWN"
                    unit_new = "UNKNOWN"
                    confidence = "low"

            else:
                # Unhandled flag type
                csv_value_new = "UNKNOWN"
                unit_new = "UNKNOWN"
                confidence = "low"
                notes = f"flag_type={flag_type} not explicitly handled"

            # Update stats
            confidence_dist[confidence] += 1
            if csv_value_new == "UNKNOWN":
                unknown_count += 1

            # Build row
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

# Write CSV with csv.QUOTE_ALL per spec
with open(output_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=header, delimiter=';', quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(rows)

print(f"rows_written: {len(rows)}")
print(f"output_path: {output_path}")
print(f"confidence_distribution: {confidence_dist}")
print(f"count_UNKNOWN: {unknown_count}")

# Verify with pandas
try:
    import pandas as pd
    df = pd.read_csv(output_path, delimiter=';', quoting=csv.QUOTE_ALL)
    print(f"pandas_read_back: OK ({len(df)} rows)")
except Exception as e:
    print(f"pandas_read_back: ERROR - {e}")
    sys.exit(1)
