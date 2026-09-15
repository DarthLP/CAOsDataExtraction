"""Tests for audit_lib — one or more tests per A1-A16, plus the A13 floor."""

import sys, pathlib, tempfile, csv
from pathlib import Path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from qa.shared.aggregator_lib import OUTPUT_COLUMNS
from qa.shared import audit_lib


def _write_corrections(rows: list[dict]) -> Path:
    """Helper: write a temp corrections.csv with the given rows."""
    tmp = Path(tempfile.mkdtemp()) / "corrections.csv"
    # Fill in defaults
    fixed = []
    for r in rows:
        full = {col: "" for col in OUTPUT_COLUMNS}
        full.update(r)
        fixed.append(full)
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=";")
        w.writeheader()
        for r in fixed:
            w.writerow(r)
    return tmp


def _run(rows):
    return audit_lib.run_audit(_write_corrections(rows))


def _count(report, check_id: str) -> int:
    for c in report.checks:
        if c.id == check_id:
            return c.count
    raise KeyError(check_id)


def test_a1_missing_record_id():
    r = _run([{"record_id": "", "original_field": "x"}])
    assert _count(r, "A1") == 1


def test_a2_whitespace_in_value():
    r = _run([{"record_id": "1", "original_field": "x", "csv_value_new": "  5  "}])
    assert _count(r, "A2") == 1


def test_a3_high_conf_placeholder_evidence():
    r = _run([{"record_id": "1", "original_field": "x",
               "confidence": "high", "evidence_quote": "(no relevant text)"}])
    assert _count(r, "A3") == 1


def test_a4_unknown_high_conf_violation():
    r = _run([{"record_id": "1", "original_field": "x",
               "csv_value_new": "UNKNOWN", "confidence": "high"}])
    assert _count(r, "A4") == 1


def test_a5_number_with_unknown_unit():
    r = _run([{"record_id": "1", "original_field": "x_value",
               "csv_value_new": "5", "csv_unit_new": "UNKNOWN"}])
    assert _count(r, "A5") == 1


def test_a6_boolean_with_non_boolean_value():
    r = _run([{"record_id": "1", "original_field": "x_present",
               "csv_value_new": "Maybe"}])
    assert _count(r, "A6") == 1


def test_a7_pay_rate_over_100():
    # A7 fires for leave/sick pay-continuation fields where >100% is impossible
    r = _run([{"record_id": "1", "original_field": "leave_paid_pay_value",
               "csv_value_new": "150"}])
    assert _count(r, "A7") == 1


def test_a7_excludes_overtime_surcharge():
    """Overtime surcharges of 125%, 150%, 200% are valid time-and-a-half / double-time rates."""
    r = _run([
        {"record_id": "1", "original_field": "overtime_allowance_value",
         "csv_value_new": "150"},
        {"record_id": "2", "original_field": "overtime_unfavourable_hours_allowance_value",
         "csv_value_new": "200"},
        {"record_id": "3", "original_field": "overtime_shift_allowance_range_max",
         "csv_value_new": "175"},
    ])
    assert _count(r, "A7") == 0


def test_a8_year_like_in_duration_field():
    r = _run([{"record_id": "1", "original_field": "leave_paid_maternity_value",
               "csv_value_new": "2014"}])
    assert _count(r, "A8") == 1


def test_a9_pay_rate_with_duration_unit():
    r = _run([{"record_id": "1", "original_field": "x_pay_value",
               "csv_value_new": "50", "csv_unit_new": "weeks"}])
    assert _count(r, "A9") == 1


def test_a10_conflicting_pairs():
    r = _run([
        {"record_id": "1", "original_field": "x", "csv_value_new": "5"},
        {"record_id": "1", "original_field": "x", "csv_value_new": "10"},
    ])
    assert _count(r, "A10") == 2


def test_a11_topic_group_field_mismatch():
    r = _run([{"record_id": "1", "original_field": "leave_paid_maternity_value",
               "topic_group": "pension"}])
    assert _count(r, "A11") == 1


def test_a12_demoted_post_state_not_low_unknown():
    """A12: confidence != low with placeholder evidence."""
    r = _run([{"record_id": "1", "original_field": "x",
               "confidence": "medium", "evidence_quote": "(section missing)"}])
    assert _count(r, "A12") == 1


def test_a13_floor_below_10_does_not_trip():
    """Rule with 5 firings at 40% disagreement should NOT trip A13."""
    rows = []
    for i in range(3):
        rows.append({"record_id": str(i), "original_field": "x",
                     "topic_group": "leave", "verdict": "clear",
                     "fix_method": "det_only"})
    for i in range(3, 5):
        rows.append({"record_id": str(i), "original_field": "x",
                     "topic_group": "leave", "verdict": "clear",
                     "fix_method": "det_sub_conflict"})
    r = _run(rows)
    assert _count(r, "A13") == 0, "below-10-firings should not surface"


def test_a13_above_floor_trips():
    """Rule with 12 firings at >20% disagreement SHOULD trip A13."""
    rows = []
    for i in range(9):
        rows.append({"record_id": str(i), "original_field": "x",
                     "topic_group": "leave", "verdict": "clear",
                     "fix_method": "det_only"})
    for i in range(9, 12):
        rows.append({"record_id": str(i), "original_field": "x",
                     "topic_group": "leave", "verdict": "clear",
                     "fix_method": "det_sub_conflict"})
    r = _run(rows)
    assert _count(r, "A13") == 1


def test_a14_high_conf_on_truncated():
    r = _run([{"record_id": "1", "original_field": "x",
               "topic_section_was_truncated": "True",
               "confidence": "high", "verdict": "clear"}])
    assert _count(r, "A14") == 1


def test_a15_negative_verdict_on_truncated():
    r = _run([{"record_id": "1", "original_field": "x",
               "topic_section_was_truncated": "True",
               "verdict": "unable_to_verify", "confidence": "low"}])
    assert _count(r, "A15") == 1


def test_a16_value_not_in_source_with_confirm():
    r = _run([{"record_id": "1", "original_field": "x",
               "value_not_in_source": "True", "verdict": "confirm"}])
    assert _count(r, "A16") == 1


def test_a17_evidence_missing_value():
    r = _run([{"record_id": "1", "original_field": "x",
               "verdict": "correct_in_place", "csv_value_new": "25",
               "evidence_quote": "sgeving: 2018-02-28 some text without the value"}])
    assert _count(r, "A17") == 1


def test_a17_evidence_containing_value_ok():
    r = _run([{"record_id": "1", "original_field": "x",
               "verdict": "correct_in_place", "csv_value_new": "25",
               "evidence_quote": "Overtime hourly surcharge is 25%"}])
    assert _count(r, "A17") == 0


def test_a17_decimal_comma_variant_ok():
    """0.78 in CSV should match '0,78' in Dutch-formatted source quote."""
    r = _run([{"record_id": "1", "original_field": "x",
               "verdict": "correct_in_place", "csv_value_new": "0.78",
               "evidence_quote": "0,78% van het maandsalaris per uur"}])
    assert _count(r, "A17") == 0


def test_a17_skips_booleans_and_unknown():
    r = _run([
        {"record_id": "1", "original_field": "x", "verdict": "correct_in_place",
         "csv_value_new": "True", "evidence_quote": "header only"},
        {"record_id": "2", "original_field": "x", "verdict": "correct_in_place",
         "csv_value_new": "UNKNOWN", "evidence_quote": "header only"},
    ])
    assert _count(r, "A17") == 0


def test_A18_unable_to_verify_with_change(tmp_path):
    """A18: a row with verdict=unable_to_verify but is_noop=False (a change was
    applied) must be flagged for human review."""
    import csv as _csv
    from qa.shared.audit_lib import run_audit
    from qa.shared.aggregator_lib import OUTPUT_COLUMNS

    f = tmp_path / "corrections.csv"
    def row(rid, field, vnew, verdict, noop):
        d = {c: "" for c in OUTPUT_COLUMNS}
        d.update(record_id=rid, original_field=field, csv_value_new=vnew,
                 verdict=verdict, is_noop=noop, confidence="low")
        return [d[c] for c in OUTPUT_COLUMNS]
    with f.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(OUTPUT_COLUMNS)
        # change applied + unable_to_verify  -> A18
        w.writerow(row("A", "training_career_scan_freq_unit", "0.333", "unable_to_verify", "False"))
        # unable_to_verify but no change (noop) -> NOT A18
        w.writerow(row("B", "training_budget_value", "", "unable_to_verify", "True"))
        # a real correction -> NOT A18
        w.writerow(row("C", "training_budget_value", "750", "correct_in_place", "False"))
    audit = run_audit(f)
    a18 = audit.summary().get("A18", 0)
    assert a18 == 1
    assert ("A", "training_career_scan_freq_unit") in audit.flagged_keys
    assert ("B", "training_budget_value") not in audit.flagged_keys


# === A17/A18 normalization refinements (2026-05-24) ===

def test_a17_unit_field_real_evidence_not_flagged():
    """A _unit field holds a canonical token ('EUR per km') that is not a
    verbatim substring of real source ('€0.23 per km') — that's normalization,
    not fabrication, so A17 must NOT fire."""
    r = _run([{"record_id": "1", "original_field": "fringe_commuting_allowance_unit",
               "verdict": "correct_in_place", "csv_value_new": "EUR per km",
               "evidence_quote": "€0.23 per km for own car"}])
    assert _count(r, "A17") == 0


def test_a17_unit_field_placeholder_evidence_still_flagged():
    """Same field but with NO real evidence: that's fabrication, A17 still fires."""
    r = _run([{"record_id": "1", "original_field": "fringe_commuting_allowance_unit",
               "verdict": "correct_in_place", "csv_value_new": "EUR per km",
               "evidence_quote": "(no relevant text in excerpt)"}])
    assert _count(r, "A17") == 1


def test_a17_enum_field_real_evidence_not_flagged():
    """Topic enum fields are also controlled-vocabulary; with real evidence and
    topic_group set, A17 must not fire on the non-verbatim canonical token."""
    r = _run([{"record_id": "1", "original_field": "overtime_compensation_mode",
               "topic_group": "overtime", "verdict": "correct_in_place",
               "csv_value_new": "time_off_in_lieu",
               "evidence_quote": "overtime is compensated with time off rather than pay"}])
    assert _count(r, "A17") == 0


def test_a17_nonnormalized_field_still_strict():
    """A numeric _value field is NOT normalized: the strict literal-substring
    check is preserved (regression guard for the refinement's scope)."""
    r = _run([{"record_id": "1", "original_field": "term_employer_notice_value",
               "verdict": "correct_in_place", "csv_value_new": "3",
               "evidence_quote": "the employer must observe a notice period"}])
    assert _count(r, "A17") == 1


def test_a18_empty_value_not_flagged():
    """unable_to_verify with an EMPTY new_value applies no change, so there is
    nothing to review — A18 must not fire even when is_noop is False."""
    r = _run([{"record_id": "1", "original_field": "training_cost_reimbursement_unit",
               "verdict": "unable_to_verify", "csv_value_new": "",
               "is_noop": "False", "confidence": "low"}])
    assert _count(r, "A18") == 0


def test_a18_nonempty_value_still_flagged():
    """unable_to_verify WITH a non-empty value still ships a change → A18 fires."""
    r = _run([{"record_id": "1", "original_field": "training_cost_reimbursement_unit",
               "verdict": "unable_to_verify", "csv_value_new": "% of costs",
               "is_noop": "False", "confidence": "low"}])
    assert _count(r, "A18") == 1
