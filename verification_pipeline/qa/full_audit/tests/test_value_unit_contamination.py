"""Tests for Check 4 — value↔unit contamination & swaps.

Focus: the three precise contamination detectors fire on genuine slips, and the
legitimate descriptive-numeric units that make up the ~2,100 "number in a unit
cell" cells stay SILENT (the calibration that avoids the false-positive flood).
"""
from qa.full_audit import value_unit_contamination as c4
from qa.full_audit.tests.conftest import subchecks

VAL = "bonus_sign_on_bonus_value"
UNIT = "bonus_sign_on_bonus_unit"


def _run(make_dataset, rows):
    make_dataset(rows)
    return c4.run()


def test_unit_is_number_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "T1", UNIT: "38"}])
    assert subchecks(flags)[("T1", "unit_is_number")] == 1


def test_unit_is_number_accepts_decimal_and_sign(make_dataset):
    flags = _run(make_dataset, [{"id": "A", UNIT: "2.5"},
                                {"id": "B", UNIT: "-3"}])
    sc = subchecks(flags)
    assert sc[("A", "unit_is_number")] == 1
    assert sc[("B", "unit_is_number")] == 1


def test_value_in_unit_fires_when_value_blank(make_dataset):
    flags = _run(make_dataset, [{"id": "T2", UNIT: "2 weeks"}])
    assert subchecks(flags)[("T2", "value_in_unit")] == 1


def test_value_in_unit_silent_when_value_present(make_dataset):
    # value populated → not a mashed-and-orphaned pair; must not fire.
    flags = _run(make_dataset, [{"id": "T2b", VAL: "2", UNIT: "2 weeks"}])
    assert subchecks(flags)[("T2b", "value_in_unit")] == 0


def test_value_unit_swap_fires(make_dataset):
    flags = _run(make_dataset, [{"id": "T3", VAL: "weeks", UNIT: "20"}])
    sc = subchecks(flags)
    assert sc[("T3", "value_unit_swap")] == 1


def test_descriptive_numeric_unit_silent(make_dataset):
    # The hallmark legitimate units that merely START with / contain a digit.
    rows = [
        {"id": "D1", "leave_sickpay_continuation_unit":
            "100 percent for first 52 weeks, then 70 percent"},
        {"id": "D2", "contract_full_time_hours_unit":
            "36, 37, or 38 hours per week"},
        {"id": "D3", "pension_franchise_unit": "10/7 times the AOW"},
        {"id": "D4", "fringe_commuting_allowance_unit":
            "2nd class public transport"},
        {"id": "D5", "overtime_compulsory_annual_unit": "12-hour shifts"},
    ]
    flags = _run(make_dataset, rows)
    assert flags == []


def test_clean_value_unit_pair_silent(make_dataset):
    flags = _run(make_dataset, [{"id": "C", VAL: "1500", UNIT: "EUR per month"}])
    assert flags == []
