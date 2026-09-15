"""Tests for the §1.4 reconciliation logic in aggregator_lib._reconcile."""

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from qa.shared.aggregator_lib import (
    _reconcile, classify_change, _values_equiv, _units_equiv,
    _is_placeholder_evidence,
)


def test_rule_1_both_layers_agree():
    det = {"verdict": "clear", "csv_value_new": "", "csv_unit_new": "",
           "worksheet_mode": "blind"}
    sub = {"verdict": "clear", "csv_value_new": "", "csv_unit_new": "",
           "confidence": "high", "evidence_quote": "real quote"}
    method, _ = _reconcile(det, sub)
    assert method == "det+sub_agree"


def test_rule_a1_trusted_rule_subagent_uv_det_wins():
    det = {"verdict": "clear", "csv_value_new": "", "worksheet_mode": "none"}
    sub = {"verdict": "unable_to_verify", "confidence": "low",
           "evidence_quote": "(no relevant text)"}
    method, _ = _reconcile(det, sub)
    assert method == "det_only"


def test_rule_a2_risky_rule_subagent_uv_conflict():
    det = {"verdict": "clear", "csv_value_new": "", "worksheet_mode": "blind"}
    sub = {"verdict": "unable_to_verify", "confidence": "low",
           "evidence_quote": "(no relevant text)"}
    method, _ = _reconcile(det, sub)
    assert method == "det_sub_conflict"


def test_rule_b_subagent_high_conf_wins():
    det = {"verdict": "clear", "csv_value_new": "", "worksheet_mode": "blind"}
    sub = {"verdict": "correct_in_place", "csv_value_new": "5",
           "csv_unit_new": "weeks", "confidence": "high",
           "evidence_quote": "5 weken kraamverlof"}
    method, _ = _reconcile(det, sub)
    assert method == "sub_only"


def test_rule_c_low_confidence_conflict():
    det = {"verdict": "clear", "csv_value_new": "", "worksheet_mode": "blind"}
    sub = {"verdict": "correct_in_place", "csv_value_new": "5",
           "confidence": "low", "evidence_quote": "unclear"}
    method, _ = _reconcile(det, sub)
    assert method == "det_sub_conflict"


def test_rule_5_det_clear_sub_confirm_conflict():
    """Edge case: rule says garbage but source confirms — rule miscalibration."""
    det = {"verdict": "clear", "csv_value_new": "", "worksheet_mode": "blind"}
    sub = {"verdict": "confirm", "csv_value_new": "5", "confidence": "high",
           "evidence_quote": "real evidence"}
    method, _ = _reconcile(det, sub)
    assert method == "det_sub_conflict"


def test_classify_change_branches():
    assert classify_change("100", "100", "weeks", "weeks") == "none"
    assert classify_change("100", "101", "weeks", "weeks") == "value"
    assert classify_change("100", "100", "weeks", "days") == "unit"
    assert classify_change("100", "101", "weeks", "days") == "both"
    # Pluralization equivalence
    assert classify_change("5", "5", "week", "weeks") == "none"
    assert classify_change("5", "5", "uur", "uren") == "none"


def test_placeholder_evidence_detection():
    assert _is_placeholder_evidence("")
    assert _is_placeholder_evidence("(no relevant text)")
    assert _is_placeholder_evidence("(section missing)")
    assert not _is_placeholder_evidence("Article 3:1 grants 16 weeks")


def test_values_equiv_numeric_tolerance():
    assert _values_equiv("100", "100.0")
    assert _values_equiv("4.5", "4,5")  # comma-decimal — handled by float()
    assert not _values_equiv("100", "101")
    assert _values_equiv("UNKNOWN", "unknown")
    assert _values_equiv("", "n/a")


def test_suppress_unit_without_value(tmp_path):
    """A clean *_unit correction is suppressed when its paired value is empty,
    kept when the paired value is present (in CSV or via a clean sibling)."""
    import csv as _csv
    from qa.shared.aggregator_lib import suppress_unit_without_value, OUTPUT_COLUMNS

    scoped = tmp_path / "scoped_records.csv"
    with scoped.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(["id", "overtime_allowance_value", "overtime_allowance_unit",
                    "overtime_shift_allowance_value", "overtime_shift_allowance_unit"])
        # rec A: allowance_value empty; rec B: shift value populated
        w.writerow(["A", "", "", "", ""])
        w.writerow(["B", "", "", "150", ""])

    corr = tmp_path / "corrections.csv"

    def row(rid, field, vnew, unew, fix="sub_only", noop="False", changed="unit"):
        d = {c: "" for c in OUTPUT_COLUMNS}
        d.update(record_id=rid, original_field=field, csv_value_new=vnew,
                 csv_unit_new=unew, fix_method=fix, is_noop=noop, changed=changed)
        return [d[c] for c in OUTPUT_COLUMNS]

    with corr.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(OUTPUT_COLUMNS)
        # A: unit set, paired value empty -> SUPPRESS
        w.writerow(row("A", "overtime_allowance_unit", "", "% of hourly rate"))
        # B: unit set, paired value present in scoped -> KEEP
        w.writerow(row("B", "overtime_shift_allowance_unit", "", "% of hourly rate"))

    n = suppress_unit_without_value(corr, scoped)
    assert n == 1
    out = list(_csv.DictReader(corr.open(), delimiter=";"))
    by = {r["record_id"]: r for r in out}
    assert by["A"]["is_noop"] == "True" and by["A"]["changed"] == "none"
    assert "suppressed" in by["A"]["notes"]
    assert by["B"]["is_noop"] == "False"  # kept


def test_suppress_skips_nhr_rows(tmp_path):
    """NHR-flagged unit rows are left in the review queue, not suppressed."""
    import csv as _csv
    from qa.shared.aggregator_lib import suppress_unit_without_value, OUTPUT_COLUMNS

    scoped = tmp_path / "scoped_records.csv"
    with scoped.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(["id", "overtime_allowance_value", "overtime_allowance_unit"])
        w.writerow(["A", "", ""])
    corr = tmp_path / "corrections.csv"
    d = {c: "" for c in OUTPUT_COLUMNS}
    d.update(record_id="A", original_field="overtime_allowance_unit",
             csv_unit_new="% of hourly rate", fix_method="needs_human_review",
             is_noop="False", changed="unit")
    with corr.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(OUTPUT_COLUMNS)
        w.writerow([d[c] for c in OUTPUT_COLUMNS])
    n = suppress_unit_without_value(corr, scoped)
    assert n == 0  # NHR row untouched


def test_suppress_noop_vs_actual_csv(tmp_path):
    """A correction whose new value equals the existing CSV value is a
    confirmation, not a change, and must be reclassified is_noop."""
    import csv as _csv
    from qa.shared.aggregator_lib import suppress_noop_vs_actual_csv, OUTPUT_COLUMNS

    scoped = tmp_path / "scoped_records.csv"
    with scoped.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(["id", "bonus_sign_on_bonus_present", "bonus_profit_sharing_present"])
        w.writerow(["A", "False", "False"])
    corr = tmp_path / "corrections.csv"

    def row(rid, field, vnew, noop="False"):
        d = {c: "" for c in OUTPUT_COLUMNS}
        d.update(record_id=rid, original_field=field, csv_value_new=vnew,
                 verdict="set_boolean", is_noop=noop, changed="value")
        return [d[c] for c in OUTPUT_COLUMNS]

    with corr.open("w", newline="") as fh:
        w = _csv.writer(fh, delimiter=";")
        w.writerow(OUTPUT_COLUMNS)
        # confirms existing False -> SUPPRESS
        w.writerow(row("A", "bonus_sign_on_bonus_present", "False"))
        # genuine flip False -> True -> KEEP
        w.writerow(row("A", "bonus_profit_sharing_present", "True"))

    n = suppress_noop_vs_actual_csv(corr, scoped)
    assert n == 1
    out = {r["original_field"]: r for r in _csv.DictReader(corr.open(), delimiter=";")}
    assert out["bonus_sign_on_bonus_present"]["is_noop"] == "True"
    assert out["bonus_profit_sharing_present"]["is_noop"] == "False"  # genuine change kept


def test_make_output_row_uses_winner_value_not_det():
    """Regression: on a sub_only row, csv_value_new must come from the subagent
    (winner), not the det rule's proposal. Previously _g preferred det-first and
    silently overwrote the subagent's answer (e.g. R1 proposing True when the
    subagent answered False)."""
    from qa.shared.aggregator_lib import _make_output_row
    det = {"record_id": "A", "field": "fringe_meal_benefit_present",
           "csv_value_new": "True", "verdict": "set_boolean",
           "csv_value_old": "", "cao_number": "999", "topic_group": "fringe"}
    sub = {"record_id": "A", "original_field": "fringe_meal_benefit_present",
           "new_value": "False", "verdict": "set_boolean", "confidence": "high",
           "evidence_quote": "no meal benefit is provided"}
    # sub_only: winner is sub
    row = _make_output_row({"record_id": "A", **sub}, "sub_only",
                           det_row=det, sub_row=sub)
    assert row["csv_value_new"] == "False"   # subagent's answer, NOT det's "True"
    assert row["verdict"] == "set_boolean"
    # metadata still backfills from det when the winner lacks it
    assert row["cao_number"] == "999"
    assert row["topic_group"] == "fringe"


def test_make_output_row_det_only_uses_det_value():
    """det_only rows still take the det value."""
    from qa.shared.aggregator_lib import _make_output_row
    det = {"record_id": "B", "field": "overtime_allowance_value",
           "csv_value_new": "150", "csv_unit_new": "% of hourly rate",
           "verdict": "correct_in_place", "csv_value_old": ""}
    row = _make_output_row({"record_id": "B", **det}, "det_only", det_row=det)
    assert row["csv_value_new"] == "150"
    assert row["csv_unit_new"] == "% of hourly rate"
