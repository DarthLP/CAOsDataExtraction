"""Tests for era_baselines — post-hoc floor/cap/informational outlier checks.

Baselines are NEVER fed to subagents; they only flag readings outside statutory
bounds for human review. These tests pin the flag behaviour and era boundaries.
"""

import datetime
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from qa.shared.era_baselines import EraBaseline

D = datetime.date


# ── floors (leave) ──────────────────────────────────────────────────────────
def test_floor_below_flagged():
    eb = EraBaseline("leave")
    outside, spec, reason = eb.check_outside(
        "leave_paid_maternity_value", 10, "weeks", D(2022, 1, 1))
    assert outside and spec is not None and "BELOW" in reason


def test_floor_at_or_above_not_flagged():
    eb = EraBaseline("leave")
    assert eb.check_outside("leave_paid_maternity_value", 16, "weeks", D(2022, 1, 1))[0] is False
    assert eb.check_outside("leave_paid_maternity_value", 20, "weeks", D(2022, 1, 1))[0] is False


def test_floor_era_not_yet_in_effect():
    # paid paternity floor starts 2019; a 2017 record has no baseline → no flag
    eb = EraBaseline("leave")
    assert eb.check_outside("leave_paid_paternity_value", 0, "weeks", D(2017, 1, 1))[0] is False


# ── caps (term probation, pension accrual) ──────────────────────────────────
def test_probation_cap_above_flagged():
    eb = EraBaseline("term")
    outside, _, reason = eb.check_outside(
        "term_probation_indef_value", 3, "months", D(2021, 1, 1))
    assert outside and "ABOVE" in reason


def test_probation_cap_at_limit_not_flagged():
    eb = EraBaseline("term")
    assert eb.check_outside("term_probation_indef_value", 2, "months", D(2021, 1, 1))[0] is False
    assert eb.check_outside("term_probation_fixedterm_value", 1, "months", D(2021, 1, 1))[0] is False


def test_accrual_cap_is_era_sensitive():
    eb = EraBaseline("pension")
    # 2.1% is legal in 2013 (cap 2.25) but not in 2016 (cap 1.875)
    assert eb.check_outside("pension_accrual_rate_value", 2.1, "%", D(2013, 6, 1))[0] is False
    assert eb.check_outside("pension_accrual_rate_value", 2.1, "%", D(2016, 6, 1))[0] is True


def test_accrual_post_wtp_note():
    eb = EraBaseline("pension")
    outside, _, reason = eb.check_outside(
        "pension_accrual_rate_value", 2.0, "%", D(2024, 1, 1))
    assert outside and "WTP" in reason


# ── severance cap by year ───────────────────────────────────────────────────
def test_severance_cap_by_year():
    eb = EraBaseline("term")
    # €90k exceeds the 2020 cap (€83k) but not the 2024 cap (€94k)
    assert eb.check_outside("term_severance_extra_value", 90000, "EUR", D(2020, 6, 1))[0] is True
    assert eb.check_outside("term_severance_extra_value", 90000, "EUR", D(2024, 6, 1))[0] is False


# ── franchise floor ─────────────────────────────────────────────────────────
def test_franchise_floor():
    eb = EraBaseline("pension")
    assert eb.check_outside("pension_franchise_value", 5000, "EUR", D(2023, 1, 1))[0] is True
    assert eb.check_outside("pension_franchise_value", 18000, "EUR", D(2023, 1, 1))[0] is False


def test_franchise_monthly_annualized_and_low_minimums_not_flagged():
    """Regression: real franchises were false-flagged by an over-high floor that
    ignored monthly units and year-specific minimums. After the fix, a monthly
    figure is annualized and the floor is conservative."""
    eb = EraBaseline("pension")
    # €1,329.62/month ≈ €15,955/yr → annualized, above floor → NOT flagged
    assert eb.check_outside("pension_franchise_value", 1329.62, "EUR per month", D(2021, 7, 1))[0] is False
    # genuine low year-specific annual minimums (stated verbatim in source) → NOT flagged
    assert eb.check_outside("pension_franchise_value", 10479, "EUR per year", D(2017, 1, 1))[0] is False
    assert eb.check_outside("pension_franchise_value", 12953, "EUR", D(2016, 10, 1))[0] is False
    # but clear garbage still flags: an article-number-sized figure, or a monthly
    # value that's too low even annualized (€100/mo = €1,200/yr)
    assert eb.check_outside("pension_franchise_value", 50, "EUR", D(2021, 1, 1))[0] is True
    assert eb.check_outside("pension_franchise_value", 100, "EUR per month", D(2021, 1, 1))[0] is True


# ── unit gating ─────────────────────────────────────────────────────────────
def test_unit_mismatch_skips_check():
    eb = EraBaseline("term")
    # a severance expressed in MONTHS must not be compared to the EUR cap
    assert eb.check_outside("term_severance_extra_value", 90000, "months", D(2020, 6, 1))[0] is False


def test_blank_unit_still_checked():
    eb = EraBaseline("pension")
    assert eb.check_outside("pension_accrual_rate_value", 2.5, "", D(2018, 1, 1))[0] is True


# ── informational / unknown fields never flag ───────────────────────────────
def test_informational_field_never_flagged():
    eb = EraBaseline("term")
    # notice periods are informational (not in the checked baseline list)
    assert eb.check_outside("term_employer_notice_value", 0, "months", D(2021, 1, 1))[0] is False


def test_unknown_topic_no_baselines():
    eb = EraBaseline("overtime")
    assert eb.check_outside("overtime_allowance_value", 5, "%", D(2021, 1, 1))[0] is False


# ── backward-compatible floor helper ────────────────────────────────────────
def test_is_below_statutory_backcompat():
    eb = EraBaseline("leave")
    below, spec = eb.is_below_statutory("leave_paid_maternity_value", 10, "weeks", D(2022, 1, 1))
    assert below and spec is not None
    # a cap field is not a floor → is_below_statutory returns False
    ebt = EraBaseline("term")
    assert ebt.is_below_statutory("term_probation_indef_value", 3, "months", D(2021, 1, 1))[0] is False


# ── flag_topic_outliers (post-hoc topic-wide surfacing) ─────────────────────
from qa.shared.era_baselines import flag_topic_outliers


def test_flag_outliers_existing_csv():
    scoped = [
        {"id": "1", "ingangsdatum": "2018-01-01",
         "pension_accrual_rate_value": "2.5", "pension_accrual_rate_unit": "%"},   # > 1.875 cap
        {"id": "2", "ingangsdatum": "2018-01-01",
         "pension_accrual_rate_value": "1.8", "pension_accrual_rate_unit": "%"},   # ok
    ]
    out = flag_topic_outliers("pension", scoped)
    ids = {o["record_id"] for o in out}
    assert ids == {"1"} and out[0]["value_source"] == "existing_csv"


def test_flag_outliers_uses_correction_override():
    scoped = [{"id": "1", "ingangsdatum": "2018-01-01",
               "pension_accrual_rate_value": "1.8", "pension_accrual_rate_unit": "%"}]
    # a clean-win correction pushes it above the cap → flagged from the correction
    corr = [{"record_id": "1", "original_field": "pension_accrual_rate_value",
             "csv_value_new": "2.9", "csv_unit_new": "%", "is_noop": "False"}]
    out = flag_topic_outliers("pension", scoped, corr)
    assert len(out) == 1 and out[0]["value_source"] == "correction"


def test_flag_outliers_skips_noop_correction():
    scoped = [{"id": "1", "ingangsdatum": "2018-01-01",
               "pension_accrual_rate_value": "1.8", "pension_accrual_rate_unit": "%"}]
    corr = [{"record_id": "1", "original_field": "pension_accrual_rate_value",
             "csv_value_new": "2.9", "csv_unit_new": "%", "is_noop": "True"}]  # noop ignored
    out = flag_topic_outliers("pension", scoped, corr)
    assert out == []


def test_flag_outliers_none_for_uncovered_topic():
    scoped = [{"id": "1", "ingangsdatum": "2021-01-01", "overtime_allowance_value": "5"}]
    assert flag_topic_outliers("overtime", scoped) == []


def test_flag_outliers_parses_dutch_slash_dates():
    """Regression: the scoped CSV uses DD/MM/YYYY (e.g. 01/04/2018). If _parse_date
    can't read it, era flagging silently no-ops. A post-2015 accrual above the
    Witteveen cap with a slash date MUST flag."""
    scoped = [
        {"id": "1", "ingangsdatum": "01/04/2018",
         "pension_accrual_rate_value": "2.5", "pension_accrual_rate_unit": "%"},   # >1.875 in 2018
        {"id": "2", "ingangsdatum": "01/01/2011",
         "pension_accrual_rate_value": "2.1", "pension_accrual_rate_unit": "%"},   # ok in 2011 (cap 2.25)
    ]
    out = flag_topic_outliers("pension", scoped)
    ids = {o["record_id"] for o in out}
    assert ids == {"1"}, f"expected only record 1 flagged, got {ids}"


def test_parse_date_formats():
    from qa.shared.era_baselines import _parse_date
    import datetime as _dt
    assert _parse_date("01/04/2018") == _dt.date(2018, 4, 1)   # DD/MM/YYYY (Dutch)
    assert _parse_date("2018-04-01") == _dt.date(2018, 4, 1)   # ISO
    assert _parse_date("01-04-2018") == _dt.date(2018, 4, 1)   # DD-MM-YYYY
    assert _parse_date("") is None and _parse_date("garbage") is None
