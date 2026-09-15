"""Subagent prompt + chunk builder with hard token-budget enforcement.

Context budgets (PLAN.md §4.5):
  - Static system prompt: <= 3,000 tokens
  - Per-item context: soft 2,000 / hard 4,000 tokens
  - Total chunk context: <= 50,000 tokens
  - Chunk size: 15-20 items (computed dynamically per per-item size)

Per-topic overrides for bonus/wage (3,500 / 6,000) — passed by the per-topic
aggregate script via kwargs.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from qa.shared import source_text_loader, schema_lookup


CHARS_PER_TOKEN = 4


class BudgetExceeded(Exception):
    """Raised when the static system prompt exceeds its cap."""


@dataclass
class ChunkItem:
    record_id: str
    cao_number: str
    file_name: str
    ingangsdatum: str
    era: str
    field: str
    field_description: str          # schema description for THIS field
    csv_value_old: str
    csv_unit_old: str
    flag_type: str
    flag_reason: str
    topic_section: str
    context_type: str
    topic_section_was_truncated: bool
    dropped_passages: list[dict]
    value_not_in_source: bool
    worksheet_mode: str
    enum_values: Optional[list[str]] = None  # canonical enum values if applicable
    proposed_correction: Optional[dict] = None


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _conventions_root() -> Path:
    here = Path(__file__).resolve()
    return here.parent.parent / "conventions"


def _count_tokens(text: str) -> int:
    return len(text) // CHARS_PER_TOKEN


def select_relevant_failure_modes(topic: str, max_count: int = 12) -> str:
    """Returns the concatenated general + per-topic failure-modes content
    (verbatim from MD files). Missing per_topic/<topic>.md is graceful.
    """
    cr = _conventions_root()
    general = _read_text(cr / "failure_modes" / "general.md")
    per_topic = _read_text(cr / "failure_modes" / "per_topic" / f"{topic}.md")
    parts = []
    if general:
        parts.append(general)
    if per_topic:
        parts.append(per_topic)
    return "\n\n".join(parts)


_SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "inputs" / "NON_SALARY_PROMPTS_AND_SCHEMA.md"

# Topic -> the heading used in the schema doc (sometimes diverges from short name)
_SCHEMA_SECTION_HEADER = {
    "leave":      "### leave_information",
    "overtime":   "### overtime_information",
    "homeoffice": "### homeoffice_information",
    "training":   "### training_information",
    "contract":   "### contract_type_information",
    "bonus":      "### bonuses_info",
    "fringe":     "### fringe_benefits_information",
    "safety":     "### safety_information",
    "childcare":  "### childcare_information",
    "ai":         "### ai_information",
    "term":       "### termination_information",
    "pension":    "### pension_information",
    "wage":       "### wage_scales_info",
}


def get_schema_section(topic: str) -> str:
    """Return the schema-doc section for `topic` (its field descriptions +
    enum vocabularies). Returns empty string if the schema file is missing
    or the topic header isn't found."""
    if not _SCHEMA_PATH.exists():
        return ""
    text = _SCHEMA_PATH.read_text(encoding="utf-8")
    header = _SCHEMA_SECTION_HEADER.get(topic)
    if not header or header not in text:
        return ""
    start = text.index(header)
    # Find next top-level ### that isn't this one
    rest = text[start + len(header):]
    next_hdr = re.search(r"\n### \w+", rest)
    end = start + len(header) + (next_hdr.start() if next_hdr else len(rest))
    return text[start:end].strip()


def build_subagent_prompt(topic: str, max_tokens: int = 6000) -> str:
    """Assemble static system prompt by reading conventions files.

    Raises BudgetExceeded if total > max_tokens.
    Missing per-topic files are handled gracefully.

    NOTE on cap: PLAN.md §4.5 originally estimated 3,000 tokens. Actuals
    (with schema field descriptions now included for subagent semantic
    grounding) range:
      - leave (6 FM + ~36 schema fields):     ~5,600 tokens
      - overtime (6 FM + ~17 schema fields):  ~5,000 tokens
      - bonus / pension / wage:               ~2,800-3,200 tokens
    Cap raised to 6,000 to accommodate fully-populated per-topic content
    plus schema field descriptions. If a topic approaches this, prune
    oldest-not-triggered FM entries per PLAN.md §3.2.5 or trim the schema
    section to only fields likely to appear in worksheets.
    """
    cr = _conventions_root()
    sections = []

    sections.append(f"You are a correction subagent for a Dutch CAO {topic.upper()} QA pipeline.\n")

    sections.append("═══ CONVENTIONS (READ FIRST) ═══")
    sections.append(_read_text(cr / "general_conventions.md"))

    fm_content = select_relevant_failure_modes(topic)
    if fm_content:
        sections.append("═══ FAILURE MODES ═══")
        sections.append(fm_content)

    per_topic_conv = _read_text(cr / "per_topic" / f"{topic}_conventions.md")
    if per_topic_conv:
        sections.append("═══ PER-TOPIC CONVENTIONS ═══")
        sections.append(per_topic_conv)

    sections.append("""═══ TASK ═══
Each input JSONL line is one worksheet item with fields:
  record_id, cao_number, file_name, ingangsdatum, era,
  field, field_description, csv_value_old, csv_unit_old,
  flag_type, flag_reason,
  topic_section (verbatim source text — already keyword-anchored, see §4.5),
  context_type ∈ {topic_section, topic_section_partial, full_source},
  topic_section_was_truncated (bool),
  dropped_passages (list — descriptors of less-relevant passages dropped),
  value_not_in_source (bool — CSV had a value but no source match),
  worksheet_mode ∈ {extract, blind, informed},
  enum_values (list of canonical strings — present ONLY when field is
    enum-typed; if present, new_value MUST be one of these literally),
  proposed_correction (only when worksheet_mode == "informed").

The `field_description` is the schema definition for THIS specific field
(copied from NON_SALARY_PROMPTS_AND_SCHEMA.md). Read it before extracting —
it specifies the semantic meaning, type (Amount/bool/str), and enum
vocabulary (if any). If `enum_values` is present, new_value MUST be one
of those literal strings — never paraphrase.

Emit one CSV row per item with these semicolon-delimited columns (12 cols):
  record_id ; original_field ; verdict ; target_field ; new_value ; new_unit ;
  evidence_quote ; confidence ; failure_modes_referenced ;
  topic_section_was_truncated ; value_not_in_source ; notes

PASS THROUGH the `topic_section_was_truncated` and `value_not_in_source`
booleans from the input JSONL item directly into the output row. The audit
checks A14/A15/A16 rely on these flags being preserved.

═══ VERDICT VOCABULARY ═══
  confirm            : CSV value matches source; no change
  clear              : value is wrong/garbage; new_value=""
  correct_in_place   : same field, fix value/unit
  move               : value belongs in different field; set target_field
  set_boolean        : boolean field; new_value ∈ {True, False}
  unable_to_verify   : source doesn't allow a verdict; routes to human

═══ STRICT RULES ═══
  - UNKNOWN MUST be confidence=low
  - evidence_quote must be verbatim from topic_section AND must literally
    contain the proposed new_value (or unit, for unit-only changes).
    If you cannot find a verbatim quote that includes the value, the
    correct verdict is unable_to_verify with confidence=low — never
    cite the source-block metadata header (`sgeving:`, `ingangsdatum:`)
    as evidence for a numerical extraction.
  - Never extrapolate across CAOs
  - DO NOT trust patterns from sibling records
  - Below-statutory readings = extraction error, not deviation
  - Process EVERY item — do not emit placeholder rows
  - Respect worksheet_mode:
      * extract  → CSV field is empty; extract from source if possible
      * blind    → verify the field against source independently
      * informed → proposed_correction is a hypothesis to test, not a fact
  - List failure_modes_referenced as comma-separated FM IDs you applied
    (empty if none)
  - When topic_section_was_truncated=true: whole less-relevant passages
    were dropped to fit. Look at dropped_passages to judge whether any
    dropped section was likely relevant; if so, prefer
    verdict=unable_to_verify or downgrade confidence.
  - When value_not_in_source=true: CSV has a value but no variant of it
    appeared in source. Treat with caution: if your reading agrees with
    the CSV value despite no anchor, set confidence=medium not high. If
    your reading contradicts, that's evidence of an extraction error.
""")

    full = "\n\n".join(sections)
    if _count_tokens(full) > max_tokens:
        raise BudgetExceeded(
            f"static system prompt is {_count_tokens(full)} tokens "
            f"(cap: {max_tokens})")
    return full


def build_chunk_items(records,
                      topic: str,
                      *,
                      per_item_soft_target: int = 2000,
                      per_item_hard_cap: int = 4000,
                      chunk_size_target: int = 20,
                      total_chunk_cap_tokens: int = 50000) -> list[list[ChunkItem]]:
    """Builds chunk JSONL items. Returns list of chunks, each a list of ChunkItems.

    Per-topic overrides for bonus/wage:
      per_item_soft_target=3500, per_item_hard_cap=6000

    Args:
        records: iterable of dicts with at minimum:
            record_id, cao_number, file_name, ingangsdatum, era, field,
            csv_value_old, csv_unit_old, flag_type, flag_reason, worksheet_mode,
            (optional) proposed_correction
        topic: short topic name
        chunk_size_target: target items per chunk; reduced dynamically if
            per-item content is large
        total_chunk_cap_tokens: total chunk-context budget (50K default)

    Per-item-budget overflow → record routed to needs_human_review
    (caller's responsibility; this function just skips it).
    """
    items: list[ChunkItem] = []
    skipped: list[tuple[str, str, str]] = []  # (record_id, field, reason)

    for rec in records:
        rid = str(rec.get("record_id", "")).strip()
        field = str(rec.get("field", "")).strip()
        try:
            result = source_text_loader.slice_for_item(
                topic, rid, field,
                csv_value_old=rec.get("csv_value_old"),
                csv_unit_old=rec.get("csv_unit_old"),
                soft_target_tokens=per_item_soft_target,
                hard_cap_tokens=per_item_hard_cap,
            )
        except source_text_loader.RouteToHumanReview as e:
            skipped.append((rid, field, e.reason))
            continue

        if not result.text:
            skipped.append((rid, field, "no_source_text"))
            continue

        # Per-item schema description + enum values (if applicable)
        field_desc = schema_lookup.describe_field(topic, field)
        enum_fields = schema_lookup.get_topic_enum_fields(topic)
        enum_values = sorted(enum_fields[field]) if field in enum_fields else None

        item = ChunkItem(
            record_id=rid,
            cao_number=str(rec.get("cao_number", "")),
            file_name=str(rec.get("file_name", "")),
            ingangsdatum=str(rec.get("ingangsdatum", "")),
            era=str(rec.get("era", "")),
            field=field,
            field_description=field_desc,
            csv_value_old=str(rec.get("csv_value_old", "")),
            csv_unit_old=str(rec.get("csv_unit_old", "")),
            flag_type=str(rec.get("flag_type", "")),
            flag_reason=str(rec.get("flag_reason", "")),
            topic_section=result.text,
            context_type=result.context_type,
            topic_section_was_truncated=result.was_truncated,
            dropped_passages=[{
                "section": dp.section_header,
                "first_100_chars": dp.first_100_chars,
                "anchors_matched": dp.anchors_matched,
            } for dp in result.dropped_passages],
            value_not_in_source=result.value_not_in_source,
            worksheet_mode=str(rec.get("worksheet_mode", "blind")),
            enum_values=enum_values,
            proposed_correction=rec.get("proposed_correction"),
        )
        items.append(item)

    # Partition into chunks. Dynamic sizing: aim for chunk_size_target items,
    # but reduce if total per-chunk tokens would exceed the cap.
    chunks: list[list[ChunkItem]] = []
    current: list[ChunkItem] = []
    current_tokens = 0
    item_tokens = lambda it: _count_tokens(it.topic_section) + 200  # +metadata est
    for it in items:
        it_tok = item_tokens(it)
        if (len(current) >= chunk_size_target or
                current_tokens + it_tok > total_chunk_cap_tokens):
            if current:
                chunks.append(current)
            current = [it]
            current_tokens = it_tok
        else:
            current.append(it)
            current_tokens += it_tok
    if current:
        chunks.append(current)
    return chunks


def write_chunk_jsonl(chunk: list[ChunkItem], out_path: Path) -> int:
    """Write one chunk as JSONL. Returns line count."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out_path.open("w", encoding="utf-8") as f:
        for item in chunk:
            obj = {
                "record_id": item.record_id,
                "cao_number": item.cao_number,
                "file_name": item.file_name,
                "ingangsdatum": item.ingangsdatum,
                "era": item.era,
                "field": item.field,
                "field_description": item.field_description,
                "csv_value_old": item.csv_value_old,
                "csv_unit_old": item.csv_unit_old,
                "flag_type": item.flag_type,
                "flag_reason": item.flag_reason,
                "topic_section": item.topic_section,
                "context_type": item.context_type,
                "topic_section_was_truncated": item.topic_section_was_truncated,
                "dropped_passages": item.dropped_passages,
                "value_not_in_source": item.value_not_in_source,
                "worksheet_mode": item.worksheet_mode,
            }
            if item.enum_values is not None:
                obj["enum_values"] = item.enum_values
            if item.proposed_correction is not None:
                obj["proposed_correction"] = item.proposed_correction
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            n += 1
    return n
