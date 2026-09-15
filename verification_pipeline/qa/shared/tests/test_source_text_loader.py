"""Tests for source_text_loader.slice_for_item.

Covers the slicer's anchor passes (keyword + value), atomicity rules,
truncation behavior, and structural-context handling.
"""

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from qa.shared.source_text_loader import (
    slice_for_item, _natural_passages, _expand_passage, _find_anchors,
    _find_paragraph_boundary, _load_blocks, _load_recordid_index,
    clear_cache, RouteToHumanReview, SliceResult, DroppedPassage,
    TOPIC_TO_FILENAME,
)


# ---------- Unit tests on the helper functions (no source files needed) ----------

def test_topic_to_filename_has_all_13_topics():
    assert len(TOPIC_TO_FILENAME) == 13
    expected = {"leave", "overtime", "homeoffice", "training", "contract",
                "bonus", "fringe", "safety", "childcare", "ai", "term",
                "pension", "wage"}
    assert set(TOPIC_TO_FILENAME.keys()) == expected


def test_bonus_and_wage_share_source():
    assert TOPIC_TO_FILENAME["bonus"] == TOPIC_TO_FILENAME["wage"]


def test_natural_passages_json_array_format():
    """JSON-array-element segmentation."""
    text = (
        '## 1. CAO 10 - x.pdf\n\n```json\n[\n'
        '  [\n    "question A: answer A"\n  ],\n'
        '  [\n    "question B: answer B"\n  ]\n'
        ']\n```\n'
    )
    passages = _natural_passages(text)
    assert len(passages) >= 2, f"expected ≥2 JSON elements, got {len(passages)}"


def test_natural_passages_fallback_blank_lines():
    """When no JSON structure, falls back to blank-line paragraphs."""
    text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
    passages = _natural_passages(text)
    assert len(passages) == 3


def test_find_anchors_case_insensitive():
    text = "The cao mentions PENSIOEN and pension and Pensioenpremie."
    hits = _find_anchors(text, ["pensioen", "pension"])
    # 3 hits for "pensioen" (PENSIOEN, pension matches start, Pensioenpremie)
    # Plus "pension" matches as substring of "pension" and "Pensioenpremie".
    assert len(hits) >= 3


def test_find_paragraph_boundary_blank_lines():
    text = "AAA\n\nBBB\n\nCCC"
    # Position inside BBB
    before = _find_paragraph_boundary(text, 6, "before")
    after = _find_paragraph_boundary(text, 6, "after")
    assert text[before:after].strip() == "BBB"


# ---------- Integration tests against real source files ----------

def test_load_block_for_known_cao():
    """homeoffice_information.md has 1505 blocks; CAO 633 is one of them."""
    clear_cache()
    blocks = _load_blocks("homeoffice")
    assert len(blocks) == 1505
    # CAO 633 'cao uzk januari 2011 integraal' was confirmed earlier
    assert ("633", "cao uzk januari 2011 integraal") in blocks


def test_slice_returns_full_source_when_keywords_miss():
    """When no anchors match but source is short, return full_source."""
    clear_cache()
    idx = _load_recordid_index("homeoffice")
    # Pick a record with mostly-empty homeoffice content (CAO 50 we saw earlier)
    rid_50 = next((rid for rid, key in idx.items()
                   if key == ("50", "cao_apotheken_2014_plat")), None)
    assert rid_50 is not None
    result = slice_for_item("homeoffice", rid_50, "homeoffice_has_homeoffice_rights")
    assert result.context_type == "full_source"


def test_slice_topic_section_for_anchored_block():
    """A homeoffice CAO with real content returns ctx=topic_section."""
    clear_cache()
    idx = _load_recordid_index("homeoffice")
    rid = next((r for r, k in idx.items()
                if k == ("633", "cao uzk januari 2011 integraal")), None)
    result = slice_for_item("homeoffice", rid, "homeoffice_has_homeoffice_rights")
    assert result.context_type in {"topic_section", "topic_section_partial"}
    assert len(result.keyword_hits) > 0
    assert result.text


def test_truncation_on_large_wage_block():
    """Wage CAOs with many anchors trigger truncation at default caps."""
    clear_cache()
    idx = _load_recordid_index("wage")
    blocks = _load_blocks("wage")
    # Pick the largest block
    test_key = max(blocks.keys(), key=lambda k: len(blocks[k]))
    test_rid = next(r for r, k in idx.items() if k == test_key)
    result = slice_for_item("wage", test_rid, "wage_entry_step_exp_rule")
    assert result.was_truncated, "expected truncation on large wage block"
    assert len(result.dropped_passages) > 0
    assert result.context_type == "topic_section_partial"


def test_truncation_flag_carries_dropped_descriptors():
    """Dropped passages include section + first_100_chars + anchors_matched."""
    clear_cache()
    idx = _load_recordid_index("wage")
    blocks = _load_blocks("wage")
    test_key = max(blocks.keys(), key=lambda k: len(blocks[k]))
    test_rid = next(r for r, k in idx.items() if k == test_key)
    result = slice_for_item("wage", test_rid, "wage_entry_step_exp_rule")
    dp = result.dropped_passages[0]
    assert isinstance(dp, DroppedPassage)
    assert dp.first_100_chars
    assert dp.anchors_matched


def test_value_not_in_source_flag_set_for_bogus_value():
    """CSV value with no plausible source match -> value_not_in_source=True."""
    clear_cache()
    idx = _load_recordid_index("pension")
    blocks = _load_blocks("pension")
    test_key = next(iter(blocks))
    test_rid = next(r for r, k in idx.items() if k == test_key)
    result = slice_for_item(
        "pension", test_rid, "pension_accrual_rate_value",
        csv_value_old="99.999", csv_unit_old="%",
    )
    assert result.value_not_in_source


def test_out_of_scope_record_returns_empty():
    """Record outside 95-CAO scope has no block; returns empty text."""
    clear_cache()
    result = slice_for_item("homeoffice", "999999999",
                            "homeoffice_has_homeoffice_rights")
    assert result.text == ""
