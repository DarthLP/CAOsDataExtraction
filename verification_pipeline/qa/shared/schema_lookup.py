"""Per-field schema description lookup.

Parses `inputs/NON_SALARY_PROMPTS_AND_SCHEMA.md` once into a dict
{csv_field_name -> description}. The CSV uses `<topic>_<field>_value` /
`<topic>_<field>_unit` / `<topic>_<field>_range_min` style; the schema uses
`<topic>_information.<field>` style. This module bridges them.

Used by `worksheet_builder.build_chunk_items` to inject per-item field
descriptions into the chunk JSONL — avoids dumping the whole topic schema
into every subagent's system prompt.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

_SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "inputs" / "NON_SALARY_PROMPTS_AND_SCHEMA.md"

# Topic -> the dot-path prefix used in the schema doc
# (most are "<topic>_information" but a few aren't)
_TOPIC_TO_SCHEMA_PREFIX = {
    "leave":      "leave_information",
    "overtime":   "overtime_information",
    "homeoffice": "homeoffice_information",
    "training":   "training_information",
    "contract":   "contract_type_information",
    "bonus":      "bonuses_info",
    "fringe":     "fringe_benefits_information",
    "safety":     "safety_information",
    "childcare":  "childcare_information",
    "ai":         "ai_information",
    "term":       "termination_information",
    "pension":    "pension_information",
    "wage":       "wage_scales_info",
}

# Topic -> the CSV column prefix (most are `<topic>_`)
_TOPIC_TO_CSV_PREFIX = {
    "leave":      "leave_",
    "overtime":   "overtime_",
    "homeoffice": "homeoffice_",
    "training":   "training_",
    "contract":   "contract_",
    "bonus":      "bonus_",
    "fringe":     "fringe_",
    "safety":     "safety_",
    "childcare":  "childcare_",
    "ai":         "ai_",
    "term":       "term_",
    "pension":    "pension_",
    "wage":       "wage_",
}


@lru_cache(maxsize=1)
def _parse_schema_text() -> dict[tuple[str, str], str]:
    """Parse the schema MD and return {(topic, base_field): full_description}.
    base_field is the schema-side field name (e.g. 'allowance', 'trigger_daily').
    """
    if not _SCHEMA_PATH.exists():
        return {}
    text = _SCHEMA_PATH.read_text(encoding="utf-8")

    # Per-topic block extraction. Each `### <prefix>` starts a topic.
    out: dict[tuple[str, str], str] = {}
    for topic, schema_prefix in _TOPIC_TO_SCHEMA_PREFIX.items():
        header = f"### {schema_prefix}"
        if header not in text:
            continue
        start = text.index(header)
        next_match = re.search(r"\n### \w+", text[start + len(header):])
        end = start + len(header) + (next_match.start() if next_match else len(text) - start)
        section = text[start:end]

        # Field entries: `<schema_prefix>.<field>` | `<type>`\n<description>
        # The description ends at the next `<schema_prefix>.` line or end of section.
        field_pat = re.compile(
            rf"`{re.escape(schema_prefix)}\.([\w]+)`\s*\|\s*`([^`]+)`\s*\n(.*?)(?=`{re.escape(schema_prefix)}\.|\Z)",
            re.DOTALL,
        )
        for m in field_pat.finditer(section):
            base = m.group(1).strip()
            typ = m.group(2).strip()
            desc = m.group(3).strip()
            full = f"`{schema_prefix}.{base}` | `{typ}`\n{desc}"
            out[(topic, base)] = full
    return out


def _csv_field_to_base(topic: str, csv_field: str) -> str | None:
    """Map a CSV column name to its schema base-field.
    e.g. ('overtime', 'overtime_trigger_daily_value') -> 'trigger_daily'
         ('overtime', 'overtime_allowance_range_min') -> 'allowance_range'
    """
    prefix = _TOPIC_TO_CSV_PREFIX.get(topic, f"{topic}_")
    if not csv_field.startswith(prefix):
        return None
    rest = csv_field[len(prefix):]
    # Strip common suffixes
    for suffix in ("_value", "_unit", "_range_min", "_range_max", "_min", "_max", "_amt"):
        if rest.endswith(suffix):
            return rest[: -len(suffix)]
    return rest


def describe_field(topic: str, csv_field: str) -> str:
    """Return the schema description for a CSV field name.
    Empty string if no match (logged but non-fatal).
    """
    base = _csv_field_to_base(topic, csv_field)
    if base is None:
        return ""
    schema = _parse_schema_text()
    # Try exact match first
    desc = schema.get((topic, base))
    if desc:
        return desc
    # Fallback: search for partial base matches (e.g. 'allowance_range' might
    # share desc with 'allowance' if the schema entry covers both)
    for (t, b), d in schema.items():
        if t == topic and (base.startswith(b) or b.startswith(base)):
            return d
    return ""


def get_topic_enum_fields(topic: str) -> dict[str, set[str]]:
    """For a topic, return {csv_field_name: {canonical_enum_values}} for fields
    whose description mentions enum-style values in quotes.

    Matches quoted identifiers (mixed-case allowed) like:
      'monetary_pay', 'TOIL', 'joint_with_OR', 'employee_request',
      'unspecified', 'other', 'highest_only'.

    Used by enum-validity rule (catches non-canonical values).
    """
    schema = _parse_schema_text()
    prefix = _TOPIC_TO_CSV_PREFIX.get(topic, f"{topic}_")
    out: dict[str, set[str]] = {}
    # Free-text fields are NEVER enums even if their description happens to
    # contain >=2 quoted words (e.g. ai_policy_note's description mentions
    # 'annual'/'none'). Excluding them prevents flagging long note text as a
    # "non-canonical enum value".
    _FREETEXT_SUFFIXES = ("_note", "_text", "_summary", "_rule_text",
                          "_measures_note", "_information")
    # Allow mixed-case identifiers; must start with a letter, can contain
    # underscores and uppercase (joint_with_OR, RV_VPL_member, etc.).
    enum_pat = re.compile(r"'([A-Za-z][A-Za-z0-9_]*)'")
    for (t, base), desc in schema.items():
        if t != topic:
            continue
        if base.endswith(_FREETEXT_SUFFIXES):
            continue
        quoted = enum_pat.findall(desc)
        # Filter out plain English words that aren't enum identifiers
        # (no underscore + all-lowercase + common English word). Keep all
        # identifiers with underscores and known acronyms.
        common_english = {"the", "a", "an", "of", "is", "to", "for",
                          "in", "on", "with", "by", "or", "and",
                          "if", "true", "false", "set", "use"}
        filtered = [q for q in quoted if q.lower() not in common_english]
        if filtered and len(filtered) >= 2:
            csv_field = f"{prefix}{base}"
            out[csv_field] = set(filtered)
    return out
