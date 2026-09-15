"""
qa_leave_presence.py — DETERMINISTIC topic-presence + CSV-data check.

Replaces the subagent presence-check (which was inconsistent across runs).
Pure Python: substring search for source phrases + boolean/null evaluation of CSV.

Reads:  qa_leave/outputs/leave_qa_payloads.jsonl  (matched records only)
Writes: qa_leave/outputs/leave_topic_presence.csv

Output schema (same as the subagent rubric):
  record_id;cao_number;file_name;ingangsdatum;general_document_type;topic_group;
  topic_in_source;csv_has_data;presence_status;matched_phrases;populated_fields

presence_status derivation (matches docs/leave_qa_presence_prompt.md):
  topic_in_source=yes, csv_has_data=yes -> discussed
  topic_in_source=yes, csv_has_data=no  -> discussed_csv_empty
  topic_in_source=no,  csv_has_data=yes -> unsupported_csv
  topic_in_source=no,  csv_has_data=no  -> not_applicable
"""

from __future__ import annotations
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAYLOADS_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
OUT_PATH = ROOT / "outputs" / "leave_topic_presence.csv"

# ---------------------------------------------------------------------------
# Source-phrase mapping (case-insensitive substring match)
# Mirrors docs/leave_qa_presence_prompt.md
# ---------------------------------------------------------------------------
PHRASE_MAP: dict[str, list[str]] = {
    "general": [
        "general leave enhancements", "minimum-cao", "minimum regulation",
        "minimum provision",
    ],
    "maternity": [
        "maternity leave", "zwangerschap", "bevallingsverlof",
        "pregnancy leave", "pregnancy and maternity",
    ],
    "paternity": [
        "paternity", "partner leave", "geboorteverlof", "birth leave",
        "wieg", "aanvullend geboorteverlof",
    ],
    "adoption": [
        "adoption", "foster leave", "foster care leave", "adoptieverlof",
        "pleegzorgverlof", "adoption and foster",
    ],
    "parental": [
        "parental leave", "ouderschapsverlof",
    ],
    "sick": [
        "sickness", "sick pay", "incapacity for work", "loondoorbetaling",
        " wga", " iva", "first year of sickness", "second year of sickness",
        "first year of illness", "second year of illness",
    ],
    "care": [
        "care leave", "zorgverlof", "short-term care", "long-term care",
        "kortdurend zorgverlof", "langdurend zorgverlof", "terminal care",
        "end-of-life", "end of life", "calamiteit", "kort verzuim",
    ],
    "vacation_holidays": [
        "vacation", "holiday allowance", "vakantie", "vakantiegeld",
        "vakantietoeslag", "liberation day", "public holiday", "feestdag",
    ],
    "seniority_special": [
        "special leave", "seniorendagen", "senior days",
        "extra leave for older", "extra vacation for older",
        "functioneel leeftijdsontslag", "service anniversary", "jubilee",
    ],
}

TOPIC_ORDER = [
    "general", "maternity", "paternity", "adoption", "parental",
    "sick", "care", "vacation_holidays", "seniority_special",
]


def truthy_string(v) -> bool:
    """Treat 'True'/'true'/'1'/'yes' as True; everything else False."""
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y")


def is_populated(field_name: str, value) -> bool:
    """Return True if the CSV value should count as 'populated above default'."""
    if value is None:
        return False
    s = str(value).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return False
    # Boolean fields default to 'False' — only 'True' counts as populated.
    boolean_field_markers = (
        "_present", "_explicitly_above_statutory", "_statutory_ref",
        "_exceptions", "_eligibility_present",
        "liberation_day_annual", "liberation_day_lustrum",
        "extra_seniority_present", "hetero_present",
        "has_leave_enhancements", "has_above_statutory_maternity",
        "abortion_present",
    )
    if any(m in field_name for m in boolean_field_markers):
        return truthy_string(value)
    # Other fields (numeric values, units, notes, schedules) — any non-empty string counts.
    return True


def topic_in_source(topic: str, source_text: str) -> tuple[bool, list[str]]:
    """Return (matched_any, list_of_matched_phrases)."""
    if not source_text:
        return False, []
    haystack = source_text.lower()
    matches: list[str] = []
    for phrase in PHRASE_MAP.get(topic, []):
        if phrase.lower() in haystack:
            matches.append(phrase)
    return (len(matches) > 0), matches


def populated_fields_for_topic(topic_csv: dict) -> list[str]:
    return [k for k, v in topic_csv.items() if is_populated(k, v)]


def derive_status(topic_in_src: bool, csv_has_data: bool) -> str:
    if topic_in_src and csv_has_data:
        return "discussed"
    if topic_in_src and not csv_has_data:
        return "discussed_csv_empty"
    if (not topic_in_src) and csv_has_data:
        return "unsupported_csv"
    return "not_applicable"


def main() -> None:
    n_records = 0
    n_rows = 0
    status_counts: dict[str, int] = {
        "discussed": 0, "discussed_csv_empty": 0,
        "unsupported_csv": 0, "not_applicable": 0,
    }

    with PAYLOADS_PATH.open("r", encoding="utf-8") as fin, OUT_PATH.open(
        "w", newline="", encoding="utf-8"
    ) as fout:
        w = csv.writer(fout, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow([
            "record_id", "cao_number", "file_name", "ingangsdatum",
            "general_document_type", "topic_group",
            "topic_in_source", "csv_has_data", "presence_status",
            "matched_phrases", "populated_fields",
        ])

        for line in fin:
            o = json.loads(line)
            if o.get("source_status") != "matched":
                continue
            n_records += 1
            src = o.get("source_text") or ""
            csv_fields = o.get("csv_leave_fields") or {}
            for topic in TOPIC_ORDER:
                topic_data = csv_fields.get(topic, {}) or {}
                in_src, phrases = topic_in_source(topic, src)
                pop = populated_fields_for_topic(topic_data)
                csv_has = len(pop) > 0
                status = derive_status(in_src, csv_has)
                status_counts[status] += 1
                w.writerow([
                    o.get("record_id", ""),
                    o.get("cao_number", ""),
                    o.get("file_name", ""),
                    o.get("ingangsdatum", ""),
                    o.get("general_document_type", ""),
                    topic,
                    "yes" if in_src else "no",
                    "yes" if csv_has else "no",
                    status,
                    "|".join(phrases),
                    "|".join(pop),
                ])
                n_rows += 1

    print(f"Records processed (matched): {n_records}")
    print(f"Verdict rows written:        {n_rows}")
    print(f"Output:                      {OUT_PATH}")
    print()
    print("Presence status distribution:")
    for k in ("discussed", "discussed_csv_empty", "unsupported_csv", "not_applicable"):
        print(f"  {k:<22s}: {status_counts[k]:>5d}  ({100*status_counts[k]/n_rows:5.1f}%)")


if __name__ == "__main__":
    main()
