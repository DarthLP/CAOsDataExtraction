"""Per-record source-text loading + keyword + value-anchored slicing.

See PLAN.md §4.5 for the full algorithm. This module implements:
  - load_source_text(topic, record_id): full per-CAO source block
  - slice_for_item(topic, record_id, field, csv_value, csv_unit): anchored slice

Atomicity: anchored passages are never cut mid-content. Truncation = whole
less-relevant passages dropped, recorded in SliceResult.dropped_passages.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from qa.shared import topic_keywords, value_variants, field_keywords


# === Topic -> filename map (PLAN.md §0.6a) ===
TOPIC_TO_FILENAME: dict[str, str] = {
    "leave":      "leave_information.md",
    "overtime":   "overtime_information.md",
    "homeoffice": "homeoffice_information.md",
    "training":   "training_information.md",
    "contract":   "contract_type_information.md",
    "bonus":      "wage_information.md",          # shared with wage
    "fringe":     "fringe_benefits_information.md",
    "safety":     "safety_information.md",
    "childcare":  "childcare_information.md",
    "ai":         "AI_information.md",
    "term":       "termination_information.md",
    "pension":    "pension_information.md",
    "wage":       "wage_information.md",          # shared with bonus
}


# === Config (PLAN.md §4.5) ===
DEFAULT_SOFT_TARGET_TOKENS = 2000
DEFAULT_HARD_CAP_TOKENS = 4000
DEFAULT_CONTEXT_WINDOW_CHARS = 500
CHARS_PER_TOKEN = 4  # rough estimate for Dutch/English mixed text


# === Data classes ===
@dataclass
class DroppedPassage:
    section_header: str
    first_100_chars: str
    anchors_matched: list[str]
    byte_offset: int


@dataclass
class SliceResult:
    text: str
    context_type: str  # 'topic_section' | 'topic_section_partial' | 'full_source'
    was_truncated: bool
    keyword_hits: list[tuple[str, int]] = field(default_factory=list)
    value_hits: list[tuple[str, int]] = field(default_factory=list)
    value_not_in_source: bool = False
    dropped_passages: list[DroppedPassage] = field(default_factory=list)


class RouteToHumanReview(Exception):
    """Raised when a single passage exceeds the hard cap."""
    def __init__(self, reason: str, **details):
        super().__init__(reason)
        self.reason = reason
        self.details = details


# === Markdown block parsing (ported from qa_leave_prep.py) ===
_HEADER_RE = re.compile(r"^##\s+(\d+)\.\s+CAO\s+(\d+)\s+-\s+(.+?)\s*$", re.MULTILINE)
_META_RE = re.compile(
    r"-\s+cao_number:\s*`([^`]*)`.*?"
    r"-\s+source_file_name:\s*`([^`]*)`.*?"
    r"-\s+ingangsdatum:\s*`([^`]*)`",
    re.DOTALL,
)


def _normalize_filename(fn: str) -> str:
    """Lowercase + strip extension(s) + collapse whitespace.
    Matches qa_leave_prep.py logic for CSV <-> markdown filename matching.
    """
    s = fn.strip().lower()
    # Strip trailing extensions (e.g. .pdf, .docx)
    s = re.sub(r"\.[a-z]{2,5}$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


# === Caches ===
# (topic) -> {(cao_number, normalized_filename): source_block_text}
_BLOCKS_BY_TOPIC: dict[str, dict[tuple[str, str], str]] = {}
# (topic) -> {record_id: (cao_number, normalized_filename)}  derived from CSV
_RECORDID_BY_TOPIC: dict[str, dict[str, tuple[str, str]]] = {}


def _get_inputs_root() -> Path:
    """Return absolute path to inputs/ directory (works regardless of cwd)."""
    here = Path(__file__).resolve()
    # qa/shared/source_text_loader.py -> ../../../inputs/
    return here.parent.parent.parent / "inputs"


def _load_blocks(topic: str) -> dict[tuple[str, str], str]:
    """Load and cache all CAO blocks for a topic's by_topic markdown file."""
    if topic in _BLOCKS_BY_TOPIC:
        return _BLOCKS_BY_TOPIC[topic]

    filename = TOPIC_TO_FILENAME.get(topic)
    if filename is None:
        raise ValueError(f"Unknown topic: {topic!r}")

    by_topic = _get_inputs_root() / "by_topic"
    md_path = by_topic / filename
    if not md_path.exists():
        # Case-insensitive fallback (handles AI_information.md on case-sensitive FS)
        for candidate in by_topic.iterdir():
            if candidate.name.lower() == filename.lower():
                md_path = candidate
                break
        else:
            raise FileNotFoundError(f"Topic source not found: {md_path}")

    text = md_path.read_text(encoding="utf-8")
    blocks: dict[tuple[str, str], str] = {}

    matches = list(_HEADER_RE.finditer(text))
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[start:end]

        # Extract cao_number + file_name from metadata block
        meta = _META_RE.search(block)
        if meta:
            cao_num = meta.group(1).strip()
            file_name = meta.group(2).strip()
        else:
            # Fall back to the header pattern (less precise — may miss .pdf in filename)
            cao_num = m.group(2)
            file_name = m.group(3)

        key = (cao_num, _normalize_filename(file_name))
        blocks[key] = block

    _BLOCKS_BY_TOPIC[topic] = blocks
    return blocks


def _load_recordid_index(topic: str) -> dict[str, tuple[str, str]]:
    """Build {record_id -> (cao_number, normalized_filename)} from the main CSV."""
    if topic in _RECORDID_BY_TOPIC:
        return _RECORDID_BY_TOPIC[topic]

    from qa.shared import resilient_csv
    csv_path = _get_inputs_root() / "extracted_data_non_salary.csv"
    df = resilient_csv.read_csv(csv_path, delimiter=";")
    idx: dict[str, tuple[str, str]] = {}
    for _, row in df.iterrows():
        rid = str(row.get("id", "")).strip()
        cao = str(row.get("cao_number", "")).strip()
        fn = str(row.get("file_name", "")).strip()
        if rid:
            idx[rid] = (cao, _normalize_filename(fn))
    _RECORDID_BY_TOPIC[topic] = idx
    return idx


def load_source_text(topic: str, record_id: str) -> str:
    """Look up per-record source text for a given topic.

    Returns the full markdown block for the matching `## N. CAO X - filename`
    section. Empty string if not found.
    """
    recordid_idx = _load_recordid_index(topic)
    key = recordid_idx.get(str(record_id))
    if key is None:
        return ""
    blocks = _load_blocks(topic)
    return blocks.get(key, "")


# === Slicing helpers ===
def _find_anchors(text: str, terms: list[str]) -> list[tuple[str, int]]:
    """Find all case-insensitive matches of any term in terms.
    Returns [(matched_term, byte_offset)]."""
    hits: list[tuple[str, int]] = []
    lower = text.lower()
    for term in terms:
        if not term:
            continue
        term_lower = term.lower()
        start = 0
        while True:
            idx = lower.find(term_lower, start)
            if idx == -1:
                break
            hits.append((term, idx))
            start = idx + 1
    return hits


# Detection regexes for structural expansion
_LIST_ITEM_LINE_RE = re.compile(
    r"^\s*(?:"
    r"[-*•–—]\s+"                              # bullets
    r"|\([a-zA-Z0-9]+\)\s+"                                   # (a), (1), (i)
    r"|[a-z]\)\s+"                                            # a) b)
    r"|[A-Z]\)\s+"                                            # A) B)
    r"|\d+\)\s+"                                              # 1) 2)
    r"|[a-z]\.\s+"                                            # a. b. (rare)
    r"|[A-Z]\.\s+"                                            # A. B.
    r"|\d+\.\s+"                                              # 1. 2.
    r"|[ivx]+\)\s+"                                           # i) ii) iii)
    r"|[IVX]+\.\s+"                                           # I. II. III.
    r"|ten\s+(?:eerste|tweede|derde|vierde|vijfde)\b"         # Dutch ordinals
    r"|onder\s+[a-z]\b"                                       # onder a, onder b
    r")",
    re.MULTILINE,
)

_CONDITIONAL_RE = re.compile(
    r"\b(?:indien|wanneer|bij(?:\s+|$)|als|in\s+geval\s+van|mocht|tenzij|"
    r"if|when|should|in\s+case\s+of|in\s+the\s+event\s+of|unless)\b",
    re.IGNORECASE,
)

_SECTION_HEADER_RE = re.compile(r"^#{1,4}\s+.+$", re.MULTILINE)

# JSON-array-element boundary: a line that's `],` immediately followed by `[`
# (the per-question separator in our by_topic/*.md files). Match either the
# `],` line or the `[` line as a boundary.
_JSON_ITEM_BOUNDARY_RE = re.compile(r"\n\s*\],\s*\n\s*\[\s*\n")


def _find_paragraph_boundary(text: str, position: int, direction: str) -> int:
    """Find the nearest paragraph boundary in `direction` ('before' or 'after')
    of `position`. A paragraph boundary is either:
      - a blank line (`\\n\\n`)
      - a JSON-array-element boundary (`\\n  ],\\n  [\\n` — between top-level
        questions in by_topic/*.md files)
      - the start/end of `text`

    Returns the byte offset of the boundary.
    """
    n = len(text)
    if direction == "before":
        # Search backward for the nearest boundary at or before position
        blank = text.rfind("\n\n", 0, position + 1)
        json_b = -1
        for m in _JSON_ITEM_BOUNDARY_RE.finditer(text, 0, position + 1):
            json_b = m.end()
        # Pick whichever is closer (largest offset, since both <= position)
        candidates = [b for b in [blank + 2 if blank >= 0 else 0, json_b] if b >= 0]
        if not candidates:
            return 0
        return max(candidates)
    else:  # 'after'
        blank = text.find("\n\n", position)
        json_b = -1
        m = _JSON_ITEM_BOUNDARY_RE.search(text, position)
        if m:
            json_b = m.start()
        candidates = [b for b in [blank if blank >= 0 else n, json_b if json_b >= 0 else n]]
        return min(candidates)


def _expand_passage(text: str, anchor_offset: int,
                    window_chars: int = DEFAULT_CONTEXT_WINDOW_CHARS) -> tuple[int, int]:
    """Given an anchor position, return (start, end) of the atomic passage
    containing it. Captures: list parent + siblings, conditional + consequents,
    nearest section header. Default ±window_chars to paragraph boundaries.
    """
    n = len(text)
    # Initial window
    start = max(0, anchor_offset - window_chars)
    end = min(n, anchor_offset + window_chars)

    # Snap to paragraph boundaries (blank line OR JSON-item boundary)
    start = _find_paragraph_boundary(text, start, "before")
    end = _find_paragraph_boundary(text, max(anchor_offset, end), "after")

    # Structural expansion 1: if the anchor is inside a list, include the list
    # header (line ending with `:` immediately before list items) and all
    # preceding sibling list items.
    list_items = list(_LIST_ITEM_LINE_RE.finditer(text, start, end))
    if list_items:
        # Find first list item at or before the anchor
        first_list_start = list_items[0].start()
        # Look upward from first_list_start for the list header line (ends with ":")
        # within the same paragraph or the line immediately above.
        header_search_start = max(start, first_list_start - 300)
        # Find the most recent line ending with ":" before first_list_start
        header_end = text.rfind(":", header_search_start, first_list_start)
        if header_end >= 0:
            # Snap back to the beginning of that line
            line_start = text.rfind("\n", header_search_start, header_end) + 1
            if line_start <= 0:
                line_start = header_search_start
            start = min(start, line_start)

    # Structural expansion 2: conditional clause — if a conditional opener
    # appears within ~500 chars before the anchor and it has consequents
    # after the anchor, include them.
    cond_window_start = max(0, anchor_offset - 500)
    cond_match = None
    for m in _CONDITIONAL_RE.finditer(text, cond_window_start, anchor_offset):
        cond_match = m
    if cond_match:
        # Extend start to include the conditional opener (snap to line start)
        line_start = text.rfind("\n", 0, cond_match.start()) + 1
        start = min(start, line_start)
        # Extend end to include all list items / consequents up to the next
        # blank line. If there's a list following, end at the end of that list.
        # We've already extended to the next blank line; that should cover it.

    # Structural expansion 3: include nearest preceding section header (## or ###)
    headers_before = list(_SECTION_HEADER_RE.finditer(text, 0, start + 1))
    if headers_before:
        last_header = headers_before[-1]
        # Only include if it's reasonably close (within 1500 chars) to avoid
        # pulling in entire chapters
        if start - last_header.start() < 1500:
            start = min(start, last_header.start())

    # Clamp
    start = max(0, start)
    end = min(n, end)
    return (start, end)


def _merge_overlapping(passages: list[tuple[int, int, list[str]]]
                       ) -> list[tuple[int, int, list[str]]]:
    """Merge overlapping or adjacent passages. Each input is (start, end, anchors_list)."""
    if not passages:
        return []
    # Sort by start
    sorted_p = sorted(passages, key=lambda x: x[0])
    merged: list[tuple[int, int, list[str]]] = [sorted_p[0]]
    for start, end, anchors in sorted_p[1:]:
        cur_start, cur_end, cur_anchors = merged[-1]
        if start <= cur_end:
            # Overlapping or adjacent - merge
            merged[-1] = (cur_start, max(cur_end, end), cur_anchors + anchors)
        else:
            merged.append((start, end, anchors))
    return merged


def _passage_token_estimate(text: str, start: int, end: int) -> int:
    return (end - start) // CHARS_PER_TOKEN


def _natural_passages(text: str) -> list[tuple[int, int]]:
    """Pre-segment source into natural passages.

    For by_topic/*.md files, the structure is:
      ```json
      [
        ["question_1: answer..."],
        ["question_2: answer..."],
        ...
      ]
      ```
    Each `[..."..."]` array element is a natural passage. Returns
    [(start, end), ...] in source order.

    Falls back to blank-line paragraphs for non-JSON layouts, and to
    the whole text as a single passage if neither boundary is detected.
    """
    n = len(text)
    if n == 0:
        return []

    # First try JSON-array-element segmentation. Find all `\n  [\n` opens
    # and `\n  ],?\n` closes; each pair is a passage.
    # Conservative: find each `[` start by looking for `\n\s*\[\s*\n`,
    # and `]` end by `\n\s*\],?\s*\n`.
    item_open_re = re.compile(r"\n\s*\[\s*\n")
    item_close_re = re.compile(r"\n\s*\],?\s*\n")

    opens = [m.end() for m in item_open_re.finditer(text)]
    closes = [m.start() for m in item_close_re.finditer(text)]

    if opens and closes and len(opens) == len(closes):
        # Pair them up: each open with the next close
        passages: list[tuple[int, int]] = []
        for o, c in zip(opens, closes):
            if c > o:
                passages.append((o, c))
        if passages:
            return passages

    # Fallback: blank-line paragraphs
    paragraphs: list[tuple[int, int]] = []
    start = 0
    for m in re.finditer(r"\n\n+", text):
        if m.start() > start:
            paragraphs.append((start, m.start()))
        start = m.end()
    if start < n:
        paragraphs.append((start, n))
    if paragraphs:
        return paragraphs

    # Final fallback: whole text is one passage
    return [(0, n)]


def _section_header_for(text: str, offset: int) -> str:
    """Return the nearest preceding section header text, or empty."""
    headers = list(_SECTION_HEADER_RE.finditer(text, 0, offset + 1))
    if not headers:
        return ""
    last = headers[-1]
    line_end = text.find("\n", last.start())
    if line_end < 0:
        line_end = len(text)
    return text[last.start():line_end].strip()


# === Main entry: slice_for_item ===
def slice_for_item(topic: str,
                   record_id: str,
                   field_name: str,
                   csv_value_old=None,
                   csv_unit_old=None,
                   soft_target_tokens: int = DEFAULT_SOFT_TARGET_TOKENS,
                   hard_cap_tokens: int = DEFAULT_HARD_CAP_TOKENS) -> SliceResult:
    """Returns the keyword + value-anchored slice for one worksheet item.
    See PLAN.md §4.5.
    """
    source = load_source_text(topic, record_id)
    if not source:
        return SliceResult(text="", context_type="full_source",
                           was_truncated=False, value_not_in_source=False)

    # --- Pass 1: TOPIC keyword anchors (broad — overtime/overwerk/etc.) ---
    topic_kws = topic_keywords.TOPIC_KEYWORDS.get(topic, [])
    keyword_hits = _find_anchors(source, topic_kws)

    # --- Pass 1b: FIELD-SPECIFIC keyword anchors (narrow — for THIS field) ---
    field_kws = field_keywords.get_field_keywords(topic, field_name)
    field_hits = _find_anchors(source, field_kws) if field_kws else []

    # --- Pass 2: value anchors (only if csv_value_old non-empty) ---
    value_hits: list[tuple[str, int]] = []
    value_not_in_source = False
    if csv_value_old not in (None, "", "UNKNOWN"):
        variants = value_variants.generate(csv_value_old, csv_unit_old)
        if variants:
            value_hits = _find_anchors(source, variants)
            if not value_hits:
                value_not_in_source = True

    # --- Pre-segment source into natural passages (JSON elements or paragraphs) ---
    passages = _natural_passages(source)
    # For each natural passage: count anchors inside, flag value-bearing,
    # flag field-keyword-bearing (highest relevance for THIS specific field)
    passage_info: list[dict] = []
    for s, e in passages:
        kw_anchors = [t for (t, off) in keyword_hits if s <= off < e]
        field_anchors = [t for (t, off) in field_hits if s <= off < e]
        val_anchors = [t for (t, off) in value_hits if s <= off < e]
        if kw_anchors or field_anchors or val_anchors:
            passage_info.append({
                "start": s,
                "end": e,
                "anchors": kw_anchors + field_anchors + val_anchors,
                "value_bearing": bool(val_anchors),
                "field_specific": bool(field_anchors),
                "field_anchor_count": len(field_anchors),
            })

    # If no natural passage contains any anchor, fall back to full_source (capped)
    if not passage_info:
        cap_chars = hard_cap_tokens * CHARS_PER_TOKEN
        if len(source) <= cap_chars:
            return SliceResult(text=source, context_type="full_source",
                               was_truncated=False,
                               keyword_hits=keyword_hits,
                               value_hits=value_hits,
                               value_not_in_source=value_not_in_source)
        return SliceResult(text=source[:cap_chars], context_type="full_source",
                           was_truncated=True,
                           keyword_hits=keyword_hits,
                           value_hits=value_hits,
                           value_not_in_source=value_not_in_source)

    # --- Rank passages by relevance ---
    # Score = anchor_density + 1.5 per field-keyword hit (boost field-specific
    # passages so they're kept under truncation) + 2.0 if value-bearing.
    def _rank(p):
        length = max(1, p["end"] - p["start"])
        density = len(p["anchors"]) / (length / 1000.0)
        return (density
                + 1.5 * p.get("field_anchor_count", 0)
                + (2.0 if p["value_bearing"] else 0.0))

    ranked = sorted(passage_info, key=_rank, reverse=True)

    # --- Size-fit ---
    soft_chars = soft_target_tokens * CHARS_PER_TOKEN
    hard_chars = hard_cap_tokens * CHARS_PER_TOKEN

    # Single-passage oversize check
    for p in ranked:
        if (p["end"] - p["start"]) > hard_chars:
            raise RouteToHumanReview(
                "oversized_passage",
                passage_chars=p["end"] - p["start"],
                anchors=p["anchors"][:5],
            )

    # Accumulate passages by rank until cap
    kept: list[dict] = []
    dropped: list[dict] = []
    total = 0
    for p in ranked:
        plen = p["end"] - p["start"]
        if total + plen <= hard_chars:
            kept.append(p)
            total += plen
        else:
            dropped.append(p)

    was_truncated = len(dropped) > 0

    # Re-sort kept passages by source position so the assembled text is
    # in document order
    kept.sort(key=lambda p: p["start"])
    parts = [source[p["start"]:p["end"]] for p in kept]
    slice_text = "\n\n".join(parts)

    if total <= soft_chars:
        ctx = "topic_section"
    else:
        ctx = "topic_section_partial"

    # Build dropped descriptors
    dropped_descriptors: list[DroppedPassage] = []
    for p in dropped:
        s = p["start"]
        first_100 = source[s:s + 100].replace("\n", " ")
        dropped_descriptors.append(DroppedPassage(
            section_header=_section_header_for(source, s),
            first_100_chars=first_100,
            anchors_matched=list(set(p["anchors"]))[:10],
            byte_offset=s,
        ))

    return SliceResult(
        text=slice_text,
        context_type=ctx,
        was_truncated=was_truncated,
        keyword_hits=keyword_hits,
        value_hits=value_hits,
        value_not_in_source=value_not_in_source,
        dropped_passages=dropped_descriptors,
    )


def clear_cache() -> None:
    """Reset module caches. Useful for tests."""
    global _BLOCKS_BY_TOPIC, _RECORDID_BY_TOPIC
    _BLOCKS_BY_TOPIC = {}
    _RECORDID_BY_TOPIC = {}
