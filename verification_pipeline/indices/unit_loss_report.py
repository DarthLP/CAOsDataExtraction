"""
UNIT-LOSS REPORT — for every scored field: how many non-empty values exist, how many
survive recomputation into the field's canonical unit (the same unit its statutory
anchor is stored in), how many are dropped by the plausibility clamp, and WHICH unit
strings fail to convert (the "doesn't fit any recomputation" bucket — dropped, never
guessed; incompatible-unit buckets like EUR-severance or %-budget live on as
descriptive columns instead).

Output: unit_loss_report.csv  (field, canonical, n_nonempty, n_converted,
        n_unit_dropped, n_clamp_dropped, share_lost, top_dropped_units)
Run: python3.13 indices/unit_loss_report.py
"""
import os, sys
from collections import Counter
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import importlib

DRIVERS = ["absence", "term", "contract", "overtime", "training", "bonus", "fringe",
           "homeoffice", "pension"]


def main():
    df = il.load_full_cao()
    rows = []
    for t in DRIVERS:
        m = importlib.import_module(f"{t}_index")
        clamp = getattr(m, "CLAMP", {})
        for col, canon, _sign in m.FIELDS:
            vals = df[col].astype(str).str.strip()
            units = df.get(il.unit_col(col), pd.Series("", index=df.index)).astype(str)
            nonempty = (vals != "") & (vals.str.lower() != "nan")
            conv = pd.Series([il.normalize(canon, v, u) for v, u in zip(df[col], units)],
                             index=df.index)
            conv = pd.to_numeric(conv, errors="coerce")
            unit_ok = conv.notna() & nonempty
            clamped = pd.Series(False, index=df.index)
            if col in clamp:
                lo, hi = clamp[col]
                clamped = unit_ok & ~conv.between(lo, hi)
            kept = unit_ok & ~clamped
            dropped_units = Counter(u.strip().lower() or "(blank)"
                                    for u, ne, ok in zip(units, nonempty, conv.notna())
                                    if ne and not ok)
            top = "; ".join(f"{u}×{n}" for u, n in dropped_units.most_common(4))
            n_ne = int(nonempty.sum())
            rows.append({
                "topic": t, "field": col, "canonical": canon,
                "n_nonempty": n_ne, "n_converted": int(kept.sum()),
                "n_unit_dropped": int((nonempty & conv.isna()).sum()),
                "n_clamp_dropped": int(clamped.sum()),
                "share_lost": round(1 - kept.sum() / n_ne, 3) if n_ne else None,
                "top_dropped_units": top,
            })
    R = pd.DataFrame(rows).sort_values("share_lost", ascending=False)
    R.to_csv(os.path.join(il.OUT, "unit_loss_report.csv"), sep=";", index=False)
    print(f"wrote unit_loss_report.csv ({len(R)} fields)")
    pd.set_option("display.width", 220); pd.set_option("display.max_colwidth", 60)
    print(R.to_string(index=False))


if __name__ == "__main__":
    main()
