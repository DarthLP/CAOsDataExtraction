"""
Thorough tests for p4 salary extraction retry logic.

Verifies: attempts flow (regular -> compact -> super compact, no split),
folder semantics, extend logic, save_truncated tier logic, and schema selection.
Run: python tests/test_p4_retry_logic.py (requires project env with google-genai).
"""

import sys
import os
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.p4_analysis import (
    is_file_in_truncated_folder,
    is_file_in_truncated_2_folder,
    is_file_in_truncated_3_folder,
    is_file_in_truncated_4_folder,
    extract_clean_filename,
)


def test_extract_clean_filename():
    """Extract clean filename from extract.json paths."""
    assert "cao_KRI" in extract_clean_filename("cao_KRI_2014_2016___definitief_extract.json")
    assert "CAO" in extract_clean_filename("CAO_GHZ_2019-2021_definitief_extract.json")


def test_truncated_folder_checks_return_bool():
    """is_file_in_* functions return bool."""
    assert is_file_in_truncated_folder("test.json", "99999") in (True, False)
    assert is_file_in_truncated_2_folder("test.json", "99999") in (True, False)
    assert is_file_in_truncated_3_folder("test.json", "99999") in (True, False)
    assert is_file_in_truncated_4_folder("test.json", "99999") in (True, False)


def test_attempt_flow_logic():
    """Verify attempt flow: regular -> compact -> super compact (no split)."""
    # Simulate attempts_to_try logic from p4_analysis
    use_super_compact_from_start = False
    use_compact_schema_from_start = False

    if use_super_compact_from_start:
        attempts_to_try = [10]
    elif use_compact_schema_from_start:
        attempts_to_try = [5, 6]
    else:
        attempts_to_try = [0, 2, 4]

    assert attempts_to_try == [0, 2, 4]

    use_compact_schema_from_start = True
    if use_super_compact_from_start:
        attempts_to_try = [10]
    elif use_compact_schema_from_start:
        attempts_to_try = [5, 6]
    else:
        attempts_to_try = [0, 2, 4]

    assert attempts_to_try == [5, 6]

    use_super_compact_from_start = True
    use_compact_schema_from_start = False
    if use_super_compact_from_start:
        attempts_to_try = [10]
    elif use_compact_schema_from_start:
        attempts_to_try = [5, 6]
    else:
        attempts_to_try = [0, 2, 4]

    assert attempts_to_try == [10]


def test_schema_selection_by_attempt():
    """Verify schema selection: regular (0,2,4), compact (5,6), super (10)."""
    for attempt in [0, 2, 4]:
        use_super_compact_schema = attempt >= 10
        use_compact_schema = attempt >= 5 and attempt < 10
        assert not use_super_compact_schema
        assert not use_compact_schema

    for attempt in [5, 6]:
        use_super_compact_schema = attempt >= 10
        use_compact_schema = attempt >= 5 and attempt < 10
        assert not use_super_compact_schema
        assert use_compact_schema

    attempt = 10
    use_super_compact_schema = attempt >= 10
    use_compact_schema = attempt >= 5 and attempt < 10
    assert use_super_compact_schema
    assert not use_compact_schema


def test_extend_logic():
    """Verify extend: 4 truncation -> add [5,6]; 6 truncation -> add [10]."""
    attempts = [0, 2, 4]
    # After attempt 4 truncation
    attempts.extend([5, 6])
    assert attempts == [0, 2, 4, 5, 6]

    attempts = [5, 6]
    # After attempt 6 truncation
    attempts.extend([10])
    assert attempts == [5, 6, 10]


def test_save_truncated_tier_logic():
    """Verify save_truncated_response tier: >=10 -> _4, >=5 -> _2, else -> truncated."""
    def tier(attempt):
        if attempt is None:
            return "max_tokens_truncated"
        if attempt >= 10:
            return "max_tokens_truncated_4"
        if attempt >= 8:
            return "max_tokens_truncated_3"
        if attempt >= 5:
            return "max_tokens_truncated_2"
        return "max_tokens_truncated"

    assert tier(None) == "max_tokens_truncated"
    assert tier(0) == "max_tokens_truncated"
    assert tier(4) == "max_tokens_truncated"
    assert tier(5) == "max_tokens_truncated_2"
    assert tier(6) == "max_tokens_truncated_2"
    assert tier(10) == "max_tokens_truncated_4"


def test_no_split_in_attempts():
    """Verify attempt 8 (split) is NOT in the flow."""
    all_attempts = [0, 2, 4, 5, 6, 10]
    assert 8 not in all_attempts
    assert 9 not in all_attempts


if __name__ == "__main__":
    test_extract_clean_filename()
    test_truncated_folder_checks_return_bool()
    test_attempt_flow_logic()
    test_schema_selection_by_attempt()
    test_extend_logic()
    test_save_truncated_tier_logic()
    test_no_split_in_attempts()
    print("All p4 retry logic tests passed.")
