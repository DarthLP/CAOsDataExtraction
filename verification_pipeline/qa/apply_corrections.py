"""apply_corrections.py — write the accepted corrections into a COPY of the dataset.

Reads qa/apply_list.csv (produced by consolidate_review.py) and applies each
(record_id, field) -> new_value into a copy of inputs/extracted_data_non_salary.csv.

SAFETY (this is the whole point — see Hanna's warning "same CAO, different row"):
  1. Match ONLY on the unique row `id`. Never on cao_number (one CAO has many
     versioned rows). Require EXACTLY ONE matching row, else skip + log.
  2. Verify the base cell currently equals `expected_current` (lenient: blank/
     numeric-normalised). If it doesn't, the row is NOT what we think it is →
     skip + log, never overwrite.
  3. For an ADD (expected_current blank) the target cell must be empty, else skip.
  4. Only the single named field of the single matched row is ever written.

inputs/ is never modified. Outputs go to qa/:
  corrected_dataset.csv  — full dataset (2,739 rows) with the accepted edits applied
  apply_changelog.csv    — every cell actually changed (old -> new)
  apply_skipped.csv      — every row skipped, with the reason
"""
from __future__ import annotations
import os
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(ROOT)
BASE = os.path.join(PROJECT, "inputs", "extracted_data_non_salary.csv")
_BLANK = {"", "nan", "none", "null", "n/a"}


def norm(x) -> str:
    return str(x).replace("\n", " ").replace("\r", " ").strip()


def is_blank(x) -> bool:
    return norm(x).lower() in _BLANK


def num_eq(a, b) -> bool:
    try:
        return float(norm(a).replace(",", ".")) == float(norm(b).replace(",", "."))
    except (ValueError, TypeError):
        return False


def same(a, b) -> bool:
    return norm(a).lower() == norm(b).lower() or num_eq(a, b) or (is_blank(a) and is_blank(b))


def unset_for_add(cur, nv) -> bool:
    """An ADD target counts as 'unset' if it's blank, OR it's the boolean default
    'False' being flipped to a boolean (so a genuine False->True presence win applies,
    while a real non-boolean value is still protected from being overwritten)."""
    if is_blank(cur):
        return True
    return norm(cur).lower() == "false" and norm(nv).lower() in ("true", "false")


def main():
    df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    assert df["id"].is_unique, "FATAL: base id column is not unique — aborting"
    ap = pd.read_csv(os.path.join(ROOT, "apply_list.csv"), sep=";", dtype=str, keep_default_na=False)

    changes, skipped = [], []
    for _, r in ap.iterrows():
        rid, f = norm(r["record_id"]), r["field"]
        exp, nv = r["expected_current"], r["new_value"]
        mask = df["id"] == rid
        nmatch = int(mask.sum())
        if nmatch != 1:                                   # gate 1: exactly one row
            skipped.append((rid, f, r["source"], f"id_match_count={nmatch}"))
            continue
        if f not in df.columns:
            skipped.append((rid, f, r["source"], "no_such_column"))
            continue
        cur = df.loc[mask, f].iloc[0]
        if is_blank(exp):                                 # gate 3: ADD target must be unset
            if not unset_for_add(cur, nv) and not same(cur, nv):
                skipped.append((rid, f, r["source"], f"add_target_not_empty(base={cur!r})"))
                continue
        elif not same(cur, exp):                          # gate 2: current must match expected
            skipped.append((rid, f, r["source"], f"current_mismatch(base={cur!r},expected={exp!r})"))
            continue
        if same(cur, nv):                                 # already correct -> nothing to do
            continue
        df.loc[mask, f] = nv                              # gate 4: write only this one cell
        changes.append((rid, f, cur, nv, r["source"], r["action"]))

    df.to_csv(os.path.join(ROOT, "corrected_dataset.csv"), sep=";", index=False, encoding="utf-8-sig")
    pd.DataFrame(changes, columns=["record_id", "field", "old_value", "new_value", "source", "action"]) \
        .to_csv(os.path.join(ROOT, "apply_changelog.csv"), sep=";", index=False, encoding="utf-8-sig")
    pd.DataFrame(skipped, columns=["record_id", "field", "source", "reason"]) \
        .to_csv(os.path.join(ROOT, "apply_skipped.csv"), sep=";", index=False, encoding="utf-8-sig")

    print(f"apply_list rows           : {len(ap)}")
    print(f"cells changed             : {len(changes)}")
    print(f"  from review edits       : {sum(1 for c in changes if c[4]=='review')}")
    print(f"  from clean wins         : {sum(1 for c in changes if c[4]=='cleanwin')}")
    print(f"skipped (logged)          : {len(skipped)}")
    if skipped:
        from collections import Counter
        for reason, n in Counter(s[3].split('(')[0] for s in skipped).most_common():
            print(f"    {reason}: {n}")
    print(f"distinct records touched  : {len({c[0] for c in changes})}")
    print(f"\nwrote corrected_dataset.csv ({df.shape[0]} rows x {df.shape[1]} cols), "
          f"apply_changelog.csv, apply_skipped.csv  (inputs/ untouched)")


if __name__ == "__main__":
    main()
