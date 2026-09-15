"""Shared pytest fixtures for the full_audit check tests.

Every check reads `common.load_dataset()`. We patch that to serve small,
synthetic, FULL-WIDTH records (a blank cell for each of the dataset's real 317
columns, then the few cells a test populates). Building over the real column set
means `common.classify_columns()` / `numeric_columns()` / enum sets stay valid —
the synthetic rows exercise the exact same classification the checks use in
production, only the row VALUES are controlled.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from qa.full_audit import common  # noqa: E402


@pytest.fixture
def cols():
    return list(common.dataset_columns())


@pytest.fixture
def make_dataset(monkeypatch, cols):
    """Factory: given partial row dicts, patch common.load_dataset to serve them
    as a full-width synthetic dataset. Returns the DataFrame."""
    def _make(rows):
        full = []
        for r in rows:
            d = {c: "" for c in cols}
            d.update(r)
            full.append(d)
        df = pd.DataFrame(full, columns=cols).astype(str)
        monkeypatch.setattr(common, "load_dataset", lambda: df)
        return df
    return _make


def subchecks(flags):
    """Multiset of (record_id, subcheck) -> handy for assertions."""
    from collections import Counter
    return Counter((f["record_id"], f["subcheck"]) for f in flags)


def fields_with(flags, subcheck):
    return {(f["record_id"], f["field"]) for f in flags if f["subcheck"] == subcheck}
