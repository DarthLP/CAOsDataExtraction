"""L2 trigger — detect (record_id, field) pairs where the topic is discussed
in source but the CSV field is empty.

Generalized from qa_leave/scripts/qa_leave_presence.py. Uses
topic_keywords.TOPIC_KEYWORDS to detect topic discussion (replaces the
hardcoded PHRASE_MAP from the leave version).
"""

from __future__ import annotations

from typing import Optional

from qa.shared import topic_keywords, source_text_loader, field_keywords


_BOOLEAN_SUFFIXES = (
    "_present", "_exceptions", "_statutory_ref", "_above_statutory",
    "_annual", "_lustrum", "_required", "_allowed",
)
_TRUTHY = {"yes", "y", "true", "1", "ja"}
_FALSY = {"no", "n", "false", "0", "nee"}
_EMPTY_MARKERS = {"", "unknown", "nan", "none", "null", "n/a"}


def _is_boolean_field(field: str) -> bool:
    return any(field.endswith(s) for s in _BOOLEAN_SUFFIXES)


def _is_populated(value, field: str) -> bool:
    """Returns True if `value` represents real content for this field type.
    Boolean fields require a truthy value to count as populated; non-boolean
    fields require any non-empty / non-marker value.
    """
    if value is None:
        return False
    s = str(value).strip().lower()
    if s in _EMPTY_MARKERS:
        return False
    if _is_boolean_field(field):
        return s in _TRUTHY
    return True


def _topic_in_source(topic: str, source_text: str) -> tuple[bool, list[str]]:
    """Case-insensitive substring match using TOPIC_KEYWORDS[topic]."""
    if not source_text:
        return (False, [])
    haystack = source_text.lower()
    matches: list[str] = []
    for term in topic_keywords.TOPIC_KEYWORDS.get(topic, []):
        if term.lower() in haystack:
            matches.append(term)
    return (len(matches) > 0, matches)


def _topic_fields(topic: str, columns: list[str]) -> list[str]:
    """Filter columns to those belonging to the given topic.
    Convention: columns are prefixed with `<topic>_` (e.g. `pension_*`).
    """
    return [c for c in columns if c.startswith(f"{topic}_")]


def scan_topic_presence(topic: str, scoped_records, field_specific: bool = True):
    """For each (record_id, field) in scoped_records, determine whether the
    topic-specific concept is discussed in source AND the CSV field is empty
    (the L2 trigger).

    Two-stage check:
      1. Topic-level: source must mention the topic at all (any TOPIC_KEYWORDS
         match). Records where source doesn't mention the topic skip entirely.
      2. Field-level (when field_specific=True): for each empty field, also
         check field_keywords.has_field_mention. Only trigger L2 if a
         field-specific term matches in source — much more selective than
         the original topic-only trigger.
         If the field has no defined field-keyword set, fall back to the
         topic-level check (preserves coverage for unmaintained fields).

    Returns:
        DataFrame with columns:
            record_id, field, presence_flag (bool), evidence_excerpt,
            matched_phrases (comma-separated)
    """
    import pandas as pd

    rows = []
    columns = list(scoped_records.columns)
    topic_fields = _topic_fields(topic, columns)

    for _, record in scoped_records.iterrows():
        rid = str(record.get("id", "")).strip()
        if not rid:
            continue

        source = source_text_loader.load_source_text(topic, rid)
        if not source:
            continue
        in_src, topic_matched = _topic_in_source(topic, source)
        if not in_src:
            continue

        for f in topic_fields:
            v = record.get(f, "")
            if _is_populated(v, f):
                continue

            # Field-specific filter
            if field_specific:
                field_kws = field_keywords.get_field_keywords(topic, f)
                if field_kws:
                    f_hit, f_matched = field_keywords.has_field_mention(topic, f, source)
                    if not f_hit:
                        continue  # field-specific keyword not in source → skip
                    matched_for_excerpt = f_matched
                else:
                    # no field-specific keywords defined; fall back to topic-level
                    matched_for_excerpt = topic_matched
            else:
                matched_for_excerpt = topic_matched

            # Excerpt: 200 chars around the first match (prefer field-specific
            # match for relevance)
            excerpt = ""
            if matched_for_excerpt:
                first = matched_for_excerpt[0].lower()
                idx = source.lower().find(first)
                if idx >= 0:
                    excerpt = source[max(0, idx - 50):idx + 150].replace("\n", " ")
            rows.append({
                "record_id": rid,
                "field": f,
                "presence_flag": True,
                "evidence_excerpt": excerpt,
                "matched_phrases": ",".join(matched_for_excerpt[:5]),
            })

    if not rows:
        return pd.DataFrame(columns=["record_id", "field", "presence_flag",
                                     "evidence_excerpt", "matched_phrases"])
    return pd.DataFrame(rows)
