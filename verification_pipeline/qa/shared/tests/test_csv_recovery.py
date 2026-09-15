"""Tests for csv_recovery — repair of subagent rows with unquoted ';'."""

from __future__ import annotations

import csv
from pathlib import Path

from qa.shared import csv_recovery


def test_clean_row_passes_through():
    row = ["123", "overtime_allowance_value", "correct_in_place", "", "150",
           "% of hourly rate", "the surcharge is 150%", "high", "", "False",
           "False", "tier 1"]
    out, status = csv_recovery.recover_row(row)
    assert status == "ok"
    assert out == row


def test_evidence_oversplit_recovered():
    # evidence_quote contains an unquoted ';' -> 13 fields
    row = ["123", "overtime_allowance_value", "correct_in_place", "", "150",
           "% of hourly rate", "first part; second part", "high", "", "False",
           "False", "note"]
    # simulate the over-split (what csv.reader would produce)
    split = ["123", "overtime_allowance_value", "correct_in_place", "", "150",
             "% of hourly rate", "first part", " second part", "high", "",
             "False", "False", "note"]
    out, status = csv_recovery.recover_row(split)
    assert status == "recovered"
    assert len(out) == 12
    assert out[6] == "first part; second part"
    assert out[7] == "high"
    assert out[9] == "False" and out[10] == "False"
    assert out[11] == "note"


def test_notes_oversplit_recovered():
    # csv.reader splitting "note part a; note part b" on ';' yields the
    # leading space on the second fragment; recovery rejoins with ';'.
    split = ["123", "training_budget_unit", "correct_in_place", "", "", "EUR",
             "evidence here", "low", "", "False", "False",
             "note part a", " note part b"]
    out, status = csv_recovery.recover_row(split)
    assert status == "recovered"
    assert out[11] == "note part a; note part b"
    assert out[5] == "EUR"
    assert out[6] == "evidence here"


def test_missing_unit_11col_recovered():
    # new_unit dropped entirely -> 11 fields; lone middle is prose -> evidence
    row = ["21013", "contract_conversion_rights_rule_text", "clear", "", "",
           "source does not mention extra conversion rights beyond law", "low",
           "", "false", "false", "omit per conventions"]
    out, status = csv_recovery.recover_row(row)
    assert status == "recovered"
    assert len(out) == 12
    assert out[5] == ""  # unit inferred empty
    assert out[6].startswith("source does not mention")
    assert out[7] == "low"
    assert out[11] == "omit per conventions"


def test_unrecoverable_short():
    out, status = csv_recovery.recover_row(["123", "field", "verdict"])
    assert out is None
    assert status.startswith("unrecoverable")


def test_recover_file_drops_junk(tmp_path: Path):
    f = tmp_path / "chunk_001_corrections.csv"
    with f.open("w", newline="") as fh:
        w = csv.writer(fh, delimiter=";")
        w.writerow(csv_recovery.CANONICAL_HEADER)
        w.writerow(["123", "overtime_allowance_value", "confirm", "", "", "",
                    "ev", "high", "", "False", "False", "ok"])
        fh.write("</content>\n")   # leaked tool-call junk
        fh.write("</invoke>\n")
    stats = csv_recovery.recover_file(f, write=True)
    assert stats["dropped_junk"] == 2
    assert stats["ok"] == 1
    # File now has header + 1 data row
    rows = list(csv.reader(f.open(), delimiter=";"))
    assert len(rows) == 2


def test_idempotent(tmp_path: Path):
    f = tmp_path / "chunk_002_corrections.csv"
    with f.open("w", newline="") as fh:
        fh.write(";".join(csv_recovery.CANONICAL_HEADER) + "\n")
        # an over-split row written raw (unquoted ';')
        fh.write("123;training_career_scan_freq_unit;correct_in_place;;0.2;"
                 "times per year;every 5 years; per employee;medium;;False;False;inv\n")
    s1 = csv_recovery.recover_file(f, write=True)
    assert s1["recovered"] == 1
    # second pass: now properly quoted -> parses as 12 cols -> ok
    s2 = csv_recovery.recover_file(f, write=True)
    assert s2["recovered"] == 0
    assert s2["ok"] == 1
