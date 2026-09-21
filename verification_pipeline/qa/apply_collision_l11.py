"""Layer 11 apply — date-collision two-pass corrections (picked records only).

Applies the 27 CONFIRMED (high+medium) picked-record corrections from the collision-diff
two-pass campaign onto a COPY of the current canonical corrected_dataset.csv (G9/L1..L10),
with the same safety gates as _archive/apply_corrections.py:
  1. match ONLY on the unique row `id`; require EXACTLY ONE matching row.
  2. verify the base cell currently equals expected_current (lenient blank/numeric); else skip.
     (for an ADD — expected blank — the target cell must be unset.)
  3. write only the named field (+ its paired _unit per unit_mode); log old->new + why + quote.
Writes qa/corrected_dataset.l11_applied.csv + qa/l11_collision_changelog.csv. Verify, then promote.
Run: python3.13 qa/apply_collision_l11.py
"""
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BASE = HERE / "corrected_dataset.csv"
SCRATCH = Path("/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/6a042e07-7cbe-418d-8203-04dc71e197c0/scratchpad")
APPLY = SCRATCH / "collision_apply_list.csv"
OUT = HERE / "corrected_dataset.l11_applied.csv"
LOG = HERE / "l11_collision_changelog.csv"
_BLANK = {"", "nan", "none", "null", "n/a"}


def norm(x): return str(x).replace("\n", " ").replace("\r", " ").strip()
def is_blank(x): return norm(x).lower() in _BLANK
def num_eq(a, b):
    try: return float(norm(a).replace(",", ".")) == float(norm(b).replace(",", "."))
    except (ValueError, TypeError): return False
def same(a, b): return norm(a).lower() == norm(b).lower() or num_eq(a, b) or (is_blank(a) and is_blank(b))


def main():
    df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
    assert df["id"].is_unique, "FATAL: id not unique"
    idx = {rid: i for i, rid in enumerate(df["id"])}
    ap = pd.read_csv(APPLY, sep=";", dtype=str, keep_default_na=False)
    changes, skipped = [], []

    def write(i, field, newv, why, quote):
        old = df.at[i, field]
        if old != newv:
            df.at[i, field] = newv
            changes.append({"record_id": df.at[i, "id"], "field": field, "old_value": old,
                            "new_value": newv, "source": "L11:collision_2pass", "why": why,
                            "quote": str(quote)[:300]})

    for _, r in ap.iterrows():
        rid, f = norm(r["record_id"]), r["field"]
        exp, nv, um, nu = r["expected_current"], r["new_value"], r["unit_mode"], r["new_unit"]
        if rid not in idx:
            skipped.append((rid, f, "no_id")); continue
        if f not in df.columns:
            skipped.append((rid, f, "no_col")); continue
        i = idx[rid]; cur = df.at[i, f]
        if is_blank(exp):
            if not is_blank(cur) and not same(cur, nv):
                skipped.append((rid, f, f"add_target_not_empty({cur!r})")); continue
        elif not same(cur, exp):
            skipped.append((rid, f, f"current_mismatch(base={cur!r},exp={exp!r})")); continue
        write(i, f, nv, r["why"], r["quote"])
        # paired unit
        uf = f[:-6] + "_unit" if f.endswith("_value") else None
        if uf and uf in df.columns:
            if um == "blank":
                write(i, uf, "", r["why"], r["quote"])
            elif um == "set":
                write(i, uf, nu, r["why"], r["quote"])

    df.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")
    pd.DataFrame(changes).to_csv(LOG, sep=";", index=False, encoding="utf-8-sig")
    a = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
    b = pd.read_csv(OUT, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
    ne = int((a != b).values.sum())
    print(f"apply_list rows : {len(ap)}")
    print(f"cells changed   : {len(changes)}  | verify diff={ne}  MATCH={ne == len(changes)}")
    print(f"records touched : {len({c['record_id'] for c in changes})}  | skipped={len(skipped)} {skipped[:8]}")
    print(f"rows x cols     : {b.shape[0]+1} x {df.shape[1]} (base {a.shape[0]+1})")
    print(f"wrote {OUT.name} + {LOG.name}  (BASE untouched until promote)")


if __name__ == "__main__":
    main()
