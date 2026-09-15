"""Tests for Check 3 — enum / format / date / cross-field validity."""
from qa.full_audit import enum_format as c3
from qa.full_audit.tests.conftest import subchecks

ENUM = "pension_pension_type"          # allowed: DB DC hybrid other unspecified
RMIN = "pension_employee_contrib_range_min"
RMAX = "pension_employee_contrib_range_max"
PRESENT = "bonus_sign_on_bonus_present"
PVAL = "bonus_sign_on_bonus_value"
UNIT = "bonus_sign_on_bonus_unit"


def _run(make_dataset, rows):
    make_dataset(rows)
    return c3.run()


def test_enum_invalid_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "E1", ENUM: "frobnicate"}])
    assert subchecks(flags)[("E1", "enum_invalid")] == 1


def test_enum_valid_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "E2", ENUM: "DB"}])
    assert subchecks(flags)[("E2", "enum_invalid")] == 0


def test_malformed_number_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "M1", PVAL: "abc"}])
    assert subchecks(flags)[("M1", "malformed_number")] == 1


def test_range_inverted_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "R1", RMIN: "50", RMAX: "10"}])
    assert subchecks(flags)[("R1", "range_inverted")] == 1


def test_range_ordered_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "R2", RMIN: "10", RMAX: "50"}])
    assert subchecks(flags)[("R2", "range_inverted")] == 0


def test_date_malformed_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "D1", "ingangsdatum": "not-a-date"}])
    assert subchecks(flags)[("D1", "date_malformed")] == 1


def test_date_implausible_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "D2", "ingangsdatum": "01-01-1850"}])
    assert subchecks(flags)[("D2", "date_implausible")] == 1


def test_date_order_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "D3", "ingangsdatum": "01-01-2020",
                                 "expiratiedatum": "01-01-2019"}])
    assert subchecks(flags)[("D3", "date_order")] == 1


def test_date_order_ok_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "D4", "ingangsdatum": "01-01-2019",
                                 "expiratiedatum": "01-01-2020"}])
    assert subchecks(flags)[("D4", "date_order")] == 0


def test_date_mismatch_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "D5", "ingangsdatum": "01-01-2020",
                                 "general_start_date": "2019-01-01"}])
    assert subchecks(flags)[("D5", "date_mismatch")] == 1


def test_date_mismatch_equal_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "D6", "ingangsdatum": "01-01-2020",
                                 "general_start_date": "2020-01-01"}])
    assert subchecks(flags)[("D6", "date_mismatch")] == 0


def test_detail_no_present_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "P1", PRESENT: "False", PVAL: "100"}])
    assert subchecks(flags)[("P1", "detail_no_present")] == 1


def test_detail_present_true_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "P2", PRESENT: "True", PVAL: "100"}])
    assert subchecks(flags)[("P2", "detail_no_present")] == 0


def test_unit_no_value_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "U1", UNIT: "EUR"}])
    assert subchecks(flags)[("U1", "unit_no_value")] == 1


def test_unit_with_value_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "U2", UNIT: "EUR", PVAL: "100"}])
    assert subchecks(flags)[("U2", "unit_no_value")] == 0
