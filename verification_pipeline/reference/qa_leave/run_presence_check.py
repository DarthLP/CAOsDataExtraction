#!/usr/bin/env python3
"""
Topic-presence check for Dutch CAO QA - chunk_002.
Processes chunk_002.jsonl and outputs presence_status for each record × topic_group.
"""

import json
import csv
from pathlib import Path

# Topic group mappings for source phrase detection (case-insensitive)
TOPIC_PHRASES = {
    "general": [
        "general leave enhancements",
        "minimum-cao",
        "minimum regulation",
        "minimum provision"
    ],
    "maternity": [
        "maternity leave",
        "zwangerschap",
        "bevallingsverlof",
        "pregnancy leave",
        "pregnancy and maternity"
    ],
    "paternity": [
        "paternity",
        "partner leave",
        "geboorteverlof",
        "birth leave",
        "WIEG",
        "aanvullend geboorteverlof",
        "birth of"
    ],
    "adoption": [
        "adoption",
        "foster",
        "adoptieverlof",
        "pleegzorgverlof"
    ],
    "parental": [
        "parental leave",
        "ouderschapsverlof"
    ],
    "sick": [
        "sickness",
        "sick pay",
        "incapacity for work",
        "loondoorbetaling",
        "WGA",
        "IVA",
        "first year of sickness",
        "second year of"
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
        "kort verzuim"
    ],
    "vacation_holidays": [
        "vacation",
        "holiday allowance",
        "vakantie",
        "vakantiegeld",
        "vakantietoeslag",
        "Liberation Day",
        "public holiday",
        "feestdag"
    ],
    "seniority_special": [
        "special leave",
        "seniorendagen",
        "senior days",
        "extra leave for older",
        "extra vacation for older",
        "functioneel leeftijdsontslag",
        "age-based",
        "jubilee"
    ]
}

# CSV field mappings for each topic group
CSV_FIELD_MAPPING = {
    "general": [
        "leave_has_leave_enhancements",
        "leave_hetero_present",
        "leave_note"
    ],
    "maternity": [
        "leave_has_above_statutory_maternity",
        "leave_paid_maternity_value",
        "leave_paid_maternity_unit",
        "leave_partially_paid_maternity_value",
        "leave_partially_paid_maternity_unit",
        "leave_partially_paid_maternity_pay_value",
        "leave_partially_paid_maternity_pay_unit",
        "leave_unpaid_maternity_value",
        "leave_unpaid_maternity_unit",
        "leave_maternity_note"
    ],
    "paternity": [
        "leave_paternity_explicitly_above_statutory",
        "leave_paid_paternity_value",
        "leave_paid_paternity_unit",
        "leave_partially_paid_paternity_value",
        "leave_partially_paid_paternity_unit",
        "leave_partially_paid_paternity_pay_value",
        "leave_partially_paid_paternity_pay_unit",
        "leave_unpaid_paternity_value",
        "leave_unpaid_paternity_unit"
    ],
    "adoption": [
        "leave_adoption_value",
        "leave_adoption_unit",
        "leave_adoption_pay_value",
        "leave_adoption_pay_unit"
    ],
    "parental": [
        "leave_parental_statutory_ref",
        "leave_parental_exceptions",
        "leave_parental_eligibility_present",
        "leave_parental_min_contract_length_value",
        "leave_parental_min_contract_length_unit",
        "leave_parental_min_tenure_value",
        "leave_parental_min_tenure_unit",
        "leave_parental_note",
        "leave_parental_topup_present",
        "leave_parental_topup_pay_value",
        "leave_parental_topup_pay_unit",
        "leave_parental_unpaid_value",
        "leave_parental_unpaid_unit",
        "leave_abortion_present"
    ],
    "sick": [
        "leave_sick_topup_present",
        "leave_sickpay_duration_value",
        "leave_sickpay_duration_unit",
        "leave_sickpay_continuation_value",
        "leave_sickpay_continuation_unit",
        "leave_sickpay_extra_insurance_present"
    ],
    "care": [
        "leave_care_statutory_ref",
        "leave_care_exceptions",
        "leave_care_topup_present",
        "leave_short_term_care_value",
        "leave_short_term_care_unit",
        "leave_short_term_care_pay_value",
        "leave_short_term_care_pay_unit",
        "leave_long_term_care_value",
        "leave_long_term_care_unit",
        "leave_long_term_care_pay_value",
        "leave_long_term_care_pay_unit"
    ],
    "vacation_holidays": [
        "leave_vacation_time_value",
        "leave_vacation_time_unit",
        "leave_vacation_bonus_value",
        "leave_vacation_bonus_unit",
        "leave_liberation_day_annual",
        "leave_liberation_day_lustrum",
        "leave_liberation_day_comp_note"
    ],
    "seniority_special": [
        "leave_extra_seniority_present",
        "leave_extra_seniority_schedule"
    ]
}

# Boolean flag fields that need to be True
BOOLEAN_FIELDS = {
    "leave_present",
    "leave_explicitly_above_statutory",
    "leave_statutory_ref",
    "leave_exceptions",
    "leave_eligibility_present",
    "leave_topup_present",
    "leave_extra_insurance_present",
    "leave_annual",
    "leave_lustrum",
    "liberation_day_annual",
    "liberation_day_lustrum",
    "leave_extra_seniority_present",
    "leave_hetero_present",
    "leave_has_leave_enhancements",
    "leave_has_above_statutory_maternity",
    "leave_paternity_explicitly_above_statutory",
    "leave_abortion_present",
    "leave_sick_topup_present",
    "leave_care_statutory_ref",
    "leave_care_exceptions",
    "leave_care_topup_present",
    "leave_parental_statutory_ref",
    "leave_parental_exceptions",
    "leave_parental_eligibility_present",
    "leave_parental_topup_present"
}


def check_topic_in_source(source_text, topic_group):
    """
    Check if any phrase for the topic_group appears in source_text (case-insensitive).
    Returns (True/False, list of matched phrases)
    """
    source_lower = source_text.lower()
    phrases = TOPIC_PHRASES.get(topic_group, [])
    matched = []

    for phrase in phrases:
        if phrase.lower() in source_lower:
            matched.append(phrase)

    return (len(matched) > 0, matched)


def check_csv_has_data(csv_fields, topic_group):
    """
    Check if any field in the topic_group is populated above default.
    Returns (True/False, list of populated field names)
    """
    field_names = CSV_FIELD_MAPPING.get(topic_group, [])
    populated = []

    for field_name in field_names:
        value = csv_fields.get(field_name)

        # Check boolean fields
        if field_name in BOOLEAN_FIELDS:
            if value is True or (isinstance(value, str) and value.lower() == "true"):
                populated.append(field_name)
        else:
            # Check string/numeric fields
            if value is not None and value != "":
                populated.append(field_name)

    return (len(populated) > 0, populated)


def get_presence_status(topic_in_source, csv_has_data):
    """
    Derive presence_status from the two booleans.
    """
    if topic_in_source and csv_has_data:
        return "discussed"
    elif topic_in_source and not csv_has_data:
        return "discussed_csv_empty"
    elif not topic_in_source and csv_has_data:
        return "unsupported_csv"
    else:
        return "not_applicable"


def process_chunk(input_path, output_path):
    """
    Process the chunk JSONL file and write presence check CSV.
    """
    rows_written = 0
    presence_distribution = {
        "discussed": 0,
        "discussed_csv_empty": 0,
        "unsupported_csv": 0,
        "not_applicable": 0
    }

    with open(input_path, 'r', encoding='utf-8') as infile:
        with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)

            # Write header
            header = [
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
                "populated_fields"
            ]
            writer.writerow(header)
            rows_written += 1

            # Process each line
            for line in infile:
                record = json.loads(line.strip())

                record_id = record.get("record_id")
                cao_number = record.get("cao_number")
                file_name = record.get("file_name")
                ingangsdatum = record.get("ingangsdatum")
                general_document_type = record.get("general_document_type")
                source_text = record.get("source_text", "")
                csv_leave_fields = record.get("csv_leave_fields", {})

                # Process each topic group
                topic_groups = [
                    "general", "maternity", "paternity", "adoption",
                    "parental", "sick", "care", "vacation_holidays", "seniority_special"
                ]

                for topic_group in topic_groups:
                    # Check topic in source
                    topic_in_source_bool, matched_phrases = check_topic_in_source(
                        source_text, topic_group
                    )
                    topic_in_source_str = "yes" if topic_in_source_bool else "no"

                    # Check CSV has data
                    csv_has_data_bool, populated_fields = check_csv_has_data(
                        csv_leave_fields.get(topic_group, {}), topic_group
                    )
                    csv_has_data_str = "yes" if csv_has_data_bool else "no"

                    # Get presence status
                    presence_status = get_presence_status(topic_in_source_bool, csv_has_data_bool)
                    presence_distribution[presence_status] += 1

                    # Format matched_phrases and populated_fields
                    matched_phrases_str = "|".join(matched_phrases)
                    populated_fields_str = "|".join(populated_fields)

                    # Write row
                    row = [
                        record_id,
                        cao_number,
                        file_name,
                        ingangsdatum,
                        general_document_type,
                        topic_group,
                        topic_in_source_str,
                        csv_has_data_str,
                        presence_status,
                        matched_phrases_str,
                        populated_fields_str
                    ]
                    writer.writerow(row)
                    rows_written += 1

    return rows_written, presence_distribution


if __name__ == "__main__":
    input_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_chunks/chunk_002.jsonl")
    output_file = Path("/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_chunks/chunk_002_presence.csv")

    try:
        rows_written, presence_distribution = process_chunk(str(input_file), str(output_file))

        # Report
        print(f"rows_written: {rows_written}")
        print(f"output_path: {output_file}")
        print(f"presence_status distribution:")
        for status, count in sorted(presence_distribution.items()):
            print(f"  {status}: {count}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
