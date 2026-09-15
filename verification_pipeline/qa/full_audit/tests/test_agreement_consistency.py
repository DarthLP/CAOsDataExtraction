"""Tests for the agreement-consistency QA layer's comparison + triage logic."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from qa.full_audit.agreement_consistency import _canon, _triage


# ── _canon: float-normalize numerics, lower-strip else, None for blank ──────────
def test_canon_numeric_equates_4_and_4point0():
    assert _canon("numeric", "4") == _canon("numeric", "4.0")


def test_canon_numeric_blank_and_nonnumeric_are_none():
    assert _canon("numeric", "") is None
    assert _canon("numeric", "  ") is None
    assert _canon("numeric", "abc") is None


def test_canon_string_kinds_lower_strip():
    assert _canon("enum", "  Both ") == "both"
    assert _canon("unit", "Days") == "days"
    assert _canon("boolean", "True") == "true"
    assert _canon("unit", "") is None


# ── _triage: routing of within-agreement conflicts ─────────────────────────────
def test_unit_wording_collapses_under_period_normalization():
    label, prio, _ = _triage("unit", ["days", "day"], ["days", "day"], 2)
    assert label == "UNIT_WORDING" and prio == "low"


def test_real_unit_period_mismatch_is_not_wording():
    # weekly vs annual hours share dimension but differ in period -> real conflict
    label, prio, _ = _triage("unit", ["hours per week", "hours per year"],
                             ["hours per week", "hours per year"], 2)
    assert label == "AMBIGUOUS_2REC" and prio == "high"


def test_boolean_disagreement():
    label, prio, _ = _triage("boolean", ["true", "false"], [], 2)
    assert label == "BOOLEAN_DISAGREEMENT" and prio == "medium"


def test_two_record_numeric_diff_is_ambiguous():
    label, prio, _ = _triage("numeric", ["3.5", "4"], [], 2)
    assert label == "AMBIGUOUS_2REC" and prio == "high"


def test_clean_step_over_time_is_temporal_change():
    # values, in filing-date order, change once and never revert -> keep both
    label, prio, _ = _triage("numeric", ["2", "2", "5"], [], 3)
    assert label == "LIKELY_TEMPORAL_CHANGE" and prio == "low"


def test_recurrence_is_extraction_error():
    # non-monotonic (2 -> 5 -> 2) cannot be a real one-way change -> error
    label, prio, lone = _triage("numeric", ["2", "5", "2"], [], 3)
    assert label == "LIKELY_EXTRACTION_ERROR" and prio == "high"


def test_lone_minority_flagged_on_three_plus():
    _, _, lone = _triage("numeric", ["2", "5", "2"], [], 3)
    assert lone is True
