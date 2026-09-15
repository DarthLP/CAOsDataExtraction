"""Tests for Check 5 — unit-kind semantic mismatch.

Two things must hold:
  1. The unit_family classifier uses EARLIEST-dimension-token-wins, so a
     duration annotated with a pay rate ("weeks at 100% pay") stays time while a
     rate-of-time ("percent of working hours") is pay.
  2. The end-to-end check flags pay-units inside a TIME-canonical field (the
     maternity=100 rate-bleed) but NEVER policies a PAY-canonical field (where
     months-of-salary etc. are legitimate), and stays silent on near-50/50
     ambiguous fields.
"""
from qa.full_audit import unit_semantics as c5
from qa.full_audit.tests.conftest import subchecks

# a real numeric value field with a unit partner; its canonical family is
# whatever the served rows make it, so we drive it per-test.
VAL = "leave_paid_maternity_value"
UNIT = "leave_paid_maternity_unit"


# ── classifier ──────────────────────────────────────────────────────────────
def test_unit_family_earliest_token_wins():
    # duration annotated with a pay rate -> the duration head wins
    assert c5.unit_family("weeks at 100% pay") == "time"
    assert c5.unit_family("working days at 100% pay") == "time"
    # rate OF a time base -> the percent head wins
    assert c5.unit_family("percent of annual working hours") == "pay"
    assert c5.unit_family("percent pay") == "pay"


def test_unit_family_qualifier_words_are_not_pay():
    # "pay/wage/salary" are qualifiers, not dimension markers
    assert c5.unit_family("2 pay periods") == "time"
    assert c5.unit_family("salary payment period") == "time"
    # a multiplier's dimension is the noun that follows "times"
    assert c5.unit_family("times weekly working hours") == "time"


def test_unit_family_pay_and_count_and_blank():
    assert c5.unit_family("EUR per month") == "pay"      # € leads -> money
    assert c5.unit_family("0.5 fte") == "count"
    assert c5.unit_family("") == ""
    assert c5.unit_family("a vague phrase") == ""


# ── directional severity rule ────────────────────────────────────────────────
def test_mismatch_severity_directional():
    assert c5.mismatch_severity("time", "pay") == "high"      # rate in duration
    assert c5.mismatch_severity("time", "count") == "medium"  # fte in hours
    assert c5.mismatch_severity("time", "time") is None
    # PAY-canonical fields are not policed (months-of-salary is legit)
    assert c5.mismatch_severity("pay", "time") is None
    assert c5.mismatch_severity("pay", "count") is None
    assert c5.mismatch_severity("count", "pay") is None


# ── end-to-end ───────────────────────────────────────────────────────────────
def _rows(unit_for_time_n, *extra):
    """n rows of a clean time unit + the extra (id, unit) rows under test."""
    rows = [{"id": f"W{i}", VAL: "16", UNIT: "weeks"} for i in range(unit_for_time_n)]
    for rid, unit in extra:
        rows.append({"id": rid, VAL: "100", UNIT: unit})
    return rows


def test_pay_unit_in_time_field_flagged_high(make_dataset):
    # 10 weeks rows make the field time-canonical; the two % rows are rate-bleed.
    make_dataset(_rows(10, ("P1", "percent pay"), ("P2", "% of salary")))
    flags = c5.run()
    sc = subchecks(flags)
    assert sc[("P1", "unit_kind_mismatch")] == 1
    assert sc[("P2", "unit_kind_mismatch")] == 1
    sev = {f["record_id"]: f["severity"] for f in flags}
    assert sev["P1"] == "high" and sev["P2"] == "high"


def test_duration_with_pay_annotation_not_flagged(make_dataset):
    # "weeks at 100% pay" is a duration; must NOT be flagged in a time field.
    make_dataset(_rows(10) + [{"id": "D1", VAL: "16", UNIT: "weeks at 100% pay"}])
    flags = c5.run()
    assert ("D1", "unit_kind_mismatch") not in subchecks(flags)


def test_pay_canonical_field_not_policed(make_dataset):
    # make the SAME field pay-canonical (€ rows); a time cell must stay silent
    # (months-of-salary is a legitimate severance/pay encoding).
    rows = [{"id": f"E{i}", VAL: "1500", UNIT: "EUR"} for i in range(10)]
    rows.append({"id": "T1", VAL: "3", UNIT: "months"})
    make_dataset(rows)
    flags = c5.run()
    assert ("T1", "unit_kind_mismatch") not in subchecks(flags)


def test_ambiguous_field_not_flagged(make_dataset):
    # ~50/50 time vs pay -> no clear canonical -> per-cell silent (surfaced as
    # an ambiguous dual-use field instead).
    rows = ([{"id": f"W{i}", VAL: "16", UNIT: "weeks"} for i in range(6)]
            + [{"id": f"P{i}", VAL: "100", UNIT: "percent"} for i in range(6)])
    make_dataset(rows)
    flags, ambiguous = c5.run(return_ambiguous=True)
    assert flags == []
    assert any(a[0] == UNIT for a in ambiguous)


def test_below_min_sample_silent(make_dataset):
    # too few classified cells to establish a canonical family -> silent.
    make_dataset(_rows(3, ("P1", "percent pay")))
    assert c5.run() == []
