#!/usr/bin/env python3
"""
Topic-presence check for Dutch CAO QA chunk_003.
Processes chunk_003.jsonl and outputs presence_status CSV.
"""

import json
import csv
from pathlib import Path

# Topic group source phrase mappings (from rubric)
TOPIC_PHRASES = {
    "general": [
        "general leave enhancements",
        "minimum-cao",
        "minimum regulation",
        "minimum provision",
    ],
    "maternity": [
        "maternity leave",
        "zwangerschap",
        "bevallingsverlof",
        "pregnancy leave",
        "pregnancy and maternity",
    ],
    "paternity": [
        "paternity",
        "partner leave",
        "geboorteverlof",
        "birth leave",
        "WIEG",
        "aanvullend geboorteverlof",
        "birth of",
    ],
    "adoption": [
        "adoption",
        "foster",
        "adoptieverlof",
        "pleegzorgverlof",
    ],
    "parental": [
        "parental leave",
        "ouderschapsverlof",
    ],
    "sick": [
        "sickness",
        "sick pay",
        "incapacity for work",
        "loondoorbetaling",
        "WGA",
        "IVA",
        "first year of sickness",
        "second year of",
    ],
    "care": [
        "care leave",
        "zorgverlof",
        "short-term care",
        "long-term care",
        "kortdurend",
        "langdurend",
        "terminal care",
        "end-of-life",
        "calamiteit",
        "kort verzuim",
    ],
    "vacation_holidays": [
        "vacation",
        "holiday allowance",
        "vakantie",
        "vakantiegeld",
        "vakantietoeslag",
        "Liberation Day",
        "public holiday",
        "feestdag",
    ],
    "seniority_special": [
        "special leave",
        "seniorendagen",
        "senior days",
        "extra leave for older",
        "extra vacation for older",
        "functioneel leeftijdsontslag",
        "age-based",
        "jubilee",
    ],
}

# CSV field mappings by topic group (inferred from rubric)
CSV_FIELDS_BY_TOPIC = {
    "general": [
        "has_leave_enhancements",
    ],
    "maternity": [
        "has_above_statutory_maternity",
        "maternity_explicitly_above_statutory",
        "maternity_value",
        "maternity_unit",
        "maternity_note",
    ],
    "paternity": [
        "paternity_explicitly_above_statutory",
        "paternity_value",
        "paternity_unit",
        "paternity_note",
    ],
    "adoption": [
        "adoption_present",
        "adoption_value",
        "adoption_unit",
        "adoption_note",
    ],
    "parental": [
        "parental_present",
        "parental_value",
        "parental_unit",
        "parental_note",
    ],
    "sick": [
        "sick_present",
        "sick_explicitly_above_statutory",
        "sick_statutory_ref",
        "sick_exceptions",
        "sick_value",
        "sick_unit",
        "sick_note",
    ],
    "care": [
        "care_present",
        "care_eligibility_present",
        "care_topup_present",
        "care_value",
        "care_unit",
        "care_note",
        "care_schedule",
        "care_comp_note",
    ],
    "vacation_holidays": [
        "vacation_present",
        "vacation_value",
        "vacation_unit",
        "vacation_note",
        "holiday_allowance_present",
        "holiday_allowance_value",
        "holiday_allowance_unit",
        "liberation_day_present",
        "liberation_day_value",
        "extra_seniority_present",
    ],
    "seniority_special": [
        "seniority_annual",
        "seniority_lustrum",
        "extra_seniority_present",
        "hetero_present",
        "extra_insurance_present",
    ],
}


def check_topic_in_source(source_text, topic_group):
    """
    Check if any phrase for the topic_group appears in source_text (case-insensitive).
    Returns tuple: (found: bool, matched_phrases: list of matched phrases)
    """
    phrases = TOPIC_PHRASES.get(topic_group, [])
    source_lower = source_text.lower()
    matched = []

    for phrase in phrases:
        if phrase.lower() in source_lower:
            matched.append(phrase)

    return len(matched) > 0, matched


def is_field_populated(value):
    """
    Check if a field is populated above default.
    Boolean fields: True (case-insensitive)
    Numeric/_value/percent/unit/string fields: non-null and non-empty after strip
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value is True
    if isinstance(value, str):
        return len(value.strip()) > 0
    if isinstance(value, (int, float)):
        return value != 0  # non-zero numeric values
    return False


def check_csv_has_data(record, topic_group):
    """
    Check if any CSV field in topic_group is populated above default.
    Returns tuple: (has_data: bool, populated_fields: list of field names)
    """
    csv_fields = CSV_FIELDS_BY_TOPIC.get(topic_group, [])
    populated = []

    for field_name in csv_fields:
        # Try to find the field in record's leave_enhancements
        leave_enh = record.get("leave_enhancements", {})
        topic_data = leave_enh.get(topic_group, {})
        value = topic_data.get(field_name)

        if is_field_populated(value):
            populated.append(field_name)

    return len(populated) > 0, populated


def get_presence_status(topic_in_source, csv_has_data):
    """Derive presence_status from two booleans."""
    if topic_in_source and csv_has_data:
        return "discussed"
    elif topic_in_source and not csv_has_data:
        return "discussed_csv_empty"
    elif not topic_in_source and csv_has_data:
        return "unsupported_csv"
    else:
        return "not_applicable"


def process_chunk(input_file, output_file):
    """Process chunk file and write presence status CSV."""
    topic_groups = list(TOPIC_PHRASES.keys())

    rows = []

    with open(input_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse line {line_num}: {e}")
                continue

            record_id = record.get("record_id")
            cao_number = record.get("cao_number")
            file_name = record.get("file_name")
            ingangsdatum = record.get("ingangsdatum")
            general_document_type = record.get("general_document_type")
            source_text = record.get("source_text", "")

            # Process each topic group (9 rows per record)
            for topic_group in topic_groups:
                topic_in_source, matched_phrases = check_topic_in_source(
                    source_text, topic_group
                )
                csv_has_data, populated_fields = check_csv_has_data(record, topic_group)

                presence_status = get_presence_status(topic_in_source, csv_has_data)

                row = {
                    "record_id": record_id,
                    "cao_number": cao_number,
                    "file_name": file_name,
                    "ingangsdatum": ingangsdatum,
                    "general_document_type": general_document_type,
                    "topic_group": topic_group,
                    "topic_in_source": "yes" if topic_in_source else "no",
                    "csv_has_data": "yes" if csv_has_data else "no",
                    "presence_status": presence_status,
                    "matched_phrases": "|".join(matched_phrases) if matched_phrases else "",
                    "populated_fields": "|".join(populated_fields) if populated_fields else "",
                }
                rows.append(row)

    # Write CSV with semicolon delimiter
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "record_id",
            "cao_number",
            "file_name",
            "ingangsdatum",
            "general_document_type",
            "topic_group",
            "topic_in_source",
            "csv_has_data",
            "presence_status",
            "matched_phrases",
            "populated_fields",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


if __name__ == "__main__":
    input_path = Path(
        "/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_chunks/chunk_003.jsonl"
    )
    output_path = Path(
        "/sessions/confident-wonderful-cori/mnt/Dutch Bargaining Agreements/qa_leave/outputs/subagent_chunks/chunk_003_presence.csv"
    )

    print(f"Processing {input_path}")
    rows_written = process_chunk(input_path, output_path)
    print(f"Rows written: {rows_written}")
    print(f"Output: {output_path}")

    # Read CSV to compute status distribution
    status_counts = {}
    with open(output_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            status = row["presence_status"]
            status_counts[status] = status_counts.get(status, 0) + 1

    print("\nPresence status distribution:")
    for status in sorted(status_counts.keys()):
        print(f"  {status}: {status_counts[status]}")
