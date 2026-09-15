"""Layer 13 apply — blank systemic pension %-of-premium mis-extractions.

`pension_employee_contrib_value` is frequently the employee's share OF THE PENSION PREMIUM
(employer/employee split), not % of salary as the schema requires. Where the unit's denominator
is the premium itself (not a salary/wage/pension-base), the value is the wrong quantity and cannot
be converted without inventing a number -> blank (available-case, aligned with the pension
available-case-only rule). Salary/base-rate units and 'premium-bearing salary'-type adjectival
units are KEPT. Spot-verified (Sonnet source calibration: 6/6 decidable = premium-share, 0 salary).

Applies qa/pension_premium_apply_list.csv onto a COPY of the current canonical corrected_dataset.csv,
gated (unique id match + expected_current), blanking value + unit. Verify -> promote.
Run: python3.13 qa/apply_pension_premium_l13.py
"""
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
BASE = HERE / "corrected_dataset.csv"
APPLY = HERE / "pension_premium_apply_list.csv"
OUT = HERE / "corrected_dataset.l13_applied.csv"
LOG = HERE / "l13_pension_premium_changelog.csv"
_BLANK = {"", "nan", "none", "null", "n/a"}
VAL = "pension_employee_contrib_value"
UNIT = "pension_employee_contrib_unit"


def norm(x): return str(x).replace("\n", " ").replace("\r", " ").strip()
def is_blank(x): return norm(x).lower() in _BLANK
def num_eq(a, b):
    try: return float(norm(a).replace(",", ".")) == float(norm(b).replace(",", "."))
    except (ValueError, TypeError): return False
def same(a, b): return norm(a).lower() == norm(b).lower() or num_eq(a, b) or (is_blank(a) and is_blank(b))


def main():
    df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
    assert df["id"].is_unique
    idx = {r: i for i, r in enumerate(df["id"])}
    ap = pd.read_csv(APPLY, sep=";", dtype=str, keep_default_na=False)
    changes, skipped = [], []
    for _, r in ap.iterrows():
        rid = norm(r["record_id"]); exp = r["expected_current"]
        if rid not in idx:
            skipped.append((rid, "no_id")); continue
        i = idx[rid]
        if not same(df.at[i, VAL], exp):
            skipped.append((rid, f"mismatch(base={df.at[i, VAL]!r},exp={exp!r})")); continue
        for f in (VAL, UNIT):
            old = df.at[i, f]
            if not is_blank(old):
                df.at[i, f] = ""
                changes.append({"record_id": rid, "field": f, "old_value": old, "new_value": "",
                                "source": "L13:pension_pct_of_premium", "why": "employee share is % OF PREMIUM, not % of salary"})
    df.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")
    pd.DataFrame(changes).to_csv(LOG, sep=";", index=False, encoding="utf-8-sig")
    a = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
    b = pd.read_csv(OUT, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
    ne = int((a != b).values.sum())
    print(f"apply rows {len(ap)} | cells blanked {len(changes)} | verify diff {ne} MATCH={ne == len(changes)} | skipped {len(skipped)} {skipped[:6]}")
    print(f"records touched {len({c['record_id'] for c in changes})} | rows x cols {b.shape[0]+1} x {df.shape[1]}")


if __name__ == "__main__":
    main()
