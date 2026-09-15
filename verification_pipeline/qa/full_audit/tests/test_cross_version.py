"""Tests for Check 2 — same-CAO cross-version consensus / odd-one-out.

Each test builds one cao lineage (same cao_number, increasing ingangsdatum) and
checks that a consensus-breaking minority is flagged while genuine splits and
sub-threshold consensuses stay silent.
"""
from qa.full_audit import cross_version as c2
from qa.full_audit.tests.conftest import subchecks

VAL = "bonus_sign_on_bonus_value"
UNIT = "bonus_sign_on_bonus_unit"
ENUM = "pension_pension_type"
BOOL = "general_retro_applies"


def _versions(cao, cells_per_version):
    """cells_per_version: list of dicts (one per dated version)."""
    rows = []
    for i, cells in enumerate(cells_per_version):
        r = {"id": f"{cao}-{i}", "cao_number": cao,
             "ingangsdatum": f"01-01-{2000 + i:04d}",
             "file_name": f"{cao}_{i}.pdf"}
        r.update(cells)
        rows.append(r)
    return rows


def _run(make_dataset, rows):
    make_dataset(rows)
    return c2.run()


def test_numeric_odd_fires(make_dataset):
    rows = _versions("100", [{VAL: v} for v in
                             ["38", "38", "38", "38", "38", "200"]])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("100-5", "numeric_odd")] == 1


def test_numeric_two_odd_both_fire(make_dataset):
    rows = _versions("101", [{VAL: v} for v in
                             ["38", "38", "38", "38", "38", "38", "200", "201"]])
    flags = _run(make_dataset, rows)
    sc = subchecks(flags)
    assert sc[("101-6", "numeric_odd")] == 1
    assert sc[("101-7", "numeric_odd")] == 1


def test_numeric_split_silent(make_dataset):
    # 50/50 → no consensus → genuine evolution, not flagged.
    rows = _versions("102", [{VAL: v} for v in
                             ["38", "38", "38", "200", "200", "200"]])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("102-0", "numeric_odd")] == 0
    assert not any(f["subcheck"] == "numeric_odd" for f in flags)


def test_enum_odd_fires(make_dataset):
    rows = _versions("200", [{ENUM: v} for v in
                             ["DB", "DB", "DB", "DB", "DB", "DB", "DC"]])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("200-6", "enum_odd")] == 1


def test_enum_consensus_too_small_silent(make_dataset):
    # mode count 5 < ENUM_MIN_CONSENSUS (6) → silent.
    rows = _versions("201", [{ENUM: v} for v in
                             ["DB", "DB", "DB", "DB", "DB", "DC"]])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("201-5", "enum_odd")] == 0


def test_bool_odd_fires(make_dataset):
    vals = ["True"] * 12 + ["False"]
    rows = _versions("300", [{BOOL: v} for v in vals])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("300-12", "bool_odd")] == 1


def test_bool_consensus_too_small_silent(make_dataset):
    # 10 < BOOL_MIN_CONSENSUS (12) → silent even though it's a lone dissenter.
    vals = ["True"] * 10 + ["False"]
    rows = _versions("301", [{BOOL: v} for v in vals])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("301-10", "bool_odd")] == 0


def test_dropout_fires(make_dataset):
    rows = _versions("400", [{VAL: "38"}, {VAL: "38"}, {VAL: ""},
                             {VAL: "38"}, {VAL: "38"}])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("400-2", "dropout")] == 1


def test_dropout_differing_neighbours_silent(make_dataset):
    # neighbours differ → a real change straddling a gap, not a clean miss.
    rows = _versions("401", [{VAL: "38"}, {VAL: "38"}, {VAL: ""},
                             {VAL: "50"}, {VAL: "50"}])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("401-2", "dropout")] == 0


def test_unit_conflict_fires(make_dataset):
    units = ["EUR per month"] * 5 + ["EUR per year"]
    rows = _versions("500", [{UNIT: u} for u in units])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("500-5", "unit_conflict")] == 1


def test_unit_cosmetic_difference_silent(make_dataset):
    # bare "hours" vs qualified "hours per week": same dimension, one carries no
    # period → cosmetic, NOT a conflict.
    units = ["hours"] * 5 + ["hours per week"]
    rows = _versions("501", [{UNIT: u} for u in units])
    flags = _run(make_dataset, rows)
    assert subchecks(flags)[("501-5", "unit_conflict")] == 0
