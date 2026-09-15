"""Tests for value_variants.generate."""

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent.parent))

from qa.shared.value_variants import generate


def test_decimal_comma_swap():
    out = generate(0.43, "%")
    assert "0.43%" in out
    assert "0,43%" in out
    assert "0.43 percent" in [v for v in out]
    assert "0.43 procent" in out


def test_spelled_out_low_integers():
    out = generate(5, "weeks")
    # Spelled-out Dutch + English
    assert "vijf" in out
    assert "five" in out
    # And digit-form
    assert "5 weken" in out
    assert "5 wk" in out


def test_unit_synonyms_pension_premium():
    out = generate(4, "%")
    assert "4%" in out
    assert "4 %" in out
    assert "4 procent" in out


def test_empty_value_returns_empty():
    assert generate(None, "%") == []
    assert generate("", "%") == []
    # Unrecognized number string also returns empty
    assert generate("notanumber", "%") == []
