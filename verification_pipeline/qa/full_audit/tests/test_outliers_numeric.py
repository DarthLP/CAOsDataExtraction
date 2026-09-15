"""Tests for Check 1 — statistical outliers on numeric fields.

Covers the gated detectors (negative, scale-error, robust double-fence, rare-
value guard, unexpected/meaningful zero) and the n>=MIN_N_FENCE gate. Groups are
built large enough (n>=20) to exercise the fences; units are left blank (one
unit-signature group) since these tests target the numeric logic, not grouping.
"""
from qa.full_audit import outliers_numeric as c1
from qa.full_audit.tests.conftest import subchecks

VAL = "bonus_sign_on_bonus_value"             # 0 is NOT meaningful here
RMIN = "pension_employee_contrib_range_min"   # 0 IS meaningful (range floor)


def _grp(prefix, values, col=VAL):
    return [{"id": f"{prefix}-{i}", col: str(v)} for i, v in enumerate(values)]


def _run(make_dataset, rows):
    make_dataset(rows)
    return c1.run()


def test_negative_fires_regardless_of_n(make_dataset):
    flags = _run(make_dataset, [{"id": "N1", VAL: "-5"}])
    assert subchecks(flags)[("N1", "negative")] == 1


def test_scale_error_100x_fires(make_dataset):
    rows = _grp("S", ["38"] * 24 + ["3800"])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("S-24", "scale_error_100x")] == 1


def test_robust_fence_fires_on_lone_outlier(make_dataset):
    rows = _grp("F", ["37", "38", "39"] * 8 + ["500"])      # n=25, MAD>0
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("F-24", "robust_fence")] == 1


def test_robust_fence_rare_value_guard(make_dataset):
    # 500 recurs 5×: a recognized 2nd mode, not an idiosyncratic error → the
    # guard must suppress the fence even though 500 clears the z/IQR fences.
    rows = _grp("G", (["37", "38", "39"] * 7)[:20] + ["500"] * 5)   # n=25
    flags = _run(make_dataset, rows)
    assert not any(f["subcheck"] == "robust_fence" for f in flags)


def test_zero_unexpected_fires(make_dataset):
    rows = _grp("Z", ["38"] * 24 + ["0"])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("Z-24", "zero_unexpected")] == 1


def test_zero_meaningful_silent_on_range_min(make_dataset):
    # 0 is a natural floor of a *_range_min field → must NOT be flagged.
    rows = _grp("ZM", ["38"] * 24 + ["0"], col=RMIN)
    flags = _run(make_dataset, rows)
    assert not any(f["subcheck"] == "zero_unexpected" for f in flags)


def test_small_group_no_fence(make_dataset):
    # n < MIN_N_FENCE (20): no z/IQR fence, no scale, no zero — only structural
    # checks (negatives) would fire, and there are none here.
    rows = _grp("SM", ["38", "38", "38", "38", "500"])
    flags = _run(make_dataset, rows)
    assert flags == []
