"""Tests for worksheet_builder."""

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from qa.shared.worksheet_builder import (
    build_subagent_prompt, BudgetExceeded, select_relevant_failure_modes,
)


def test_prompt_fits_budget_for_all_topics():
    """Static system prompt stays under the 6000-token cap for every topic.

    Now includes schema field descriptions (~500-1500 tokens per topic).
    """
    topics = ["overtime", "pension", "wage", "ai", "bonus", "term",
              "homeoffice", "training", "fringe", "safety", "childcare",
              "contract", "leave"]
    for topic in topics:
        p = build_subagent_prompt(topic)
        assert (len(p) // 4) <= 6000, f"topic {topic} exceeds 6K tokens: got {len(p)//4}"


def test_prompt_references_per_item_field_description():
    """Schema field descriptions are no longer dumped into the system prompt;
    instead each chunk item carries a `field_description` field. Verify the
    prompt instructs the subagent to read it."""
    p = build_subagent_prompt("overtime")
    assert "field_description" in p
    assert "enum_values" in p


def test_schema_lookup_describes_fields():
    """The schema_lookup module returns field descriptions and enum values."""
    from qa.shared.schema_lookup import describe_field, get_topic_enum_fields
    desc = describe_field("overtime", "overtime_compensation_mode")
    assert "monetary_pay" in desc
    assert "TOIL" in desc
    enums = get_topic_enum_fields("overtime")
    assert "overtime_compensation_mode" in enums
    assert "TOIL" in enums["overtime_compensation_mode"]


def test_budget_exceeded_raises_at_tight_cap():
    try:
        build_subagent_prompt("overtime", max_tokens=100)
        assert False, "expected BudgetExceeded"
    except BudgetExceeded:
        pass


def test_missing_per_topic_fm_is_graceful():
    """Topics without per_topic/<topic>.md (e.g. all non-leave initially)
    should not raise — select_relevant_failure_modes returns only general."""
    # ai_information per_topic doesn't exist; build should still succeed
    p = build_subagent_prompt("ai")
    assert p  # non-empty
    assert "GENERAL_FM_01" in p  # general FMs always included
