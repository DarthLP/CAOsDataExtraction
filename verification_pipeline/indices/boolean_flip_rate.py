"""Same-term boolean flip-rate monitor.

A 'flip' = a coverage boolean changing True<->False between editions of the SAME
agreement term (same cao_number + ingangsdatum) — republications should agree unless
a genuine draft-vs-final amendment exists. Pre-campaign baseline (2026-07-07,
`boolean_flip_summary.csv`): mean per-boolean flip rate ~9% (max 30%). The jump
campaign (L20-L28) adjudicated every scoring-visible flip; the residual measured here
is the adjudicated-genuine share plus any sub-threshold noise the campaign's z>=0.5
event gate never surfaced. NOTE the residual is NOT a to-do list — term-level
consensus overriding it would clobber adjudicated genuine amendments (see
METHODOLOGY §12, 2026-07-13). Monitor for REGRESSION: a new extraction generation
or edit layer pushing rates back toward baseline is the alarm.

Writes boolean_flip_summary_post_campaign.csv. Run: python3 boolean_flip_rate.py
"""
import os
import sys
import importlib
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il

TOPICS = ["absence", "term", "contract", "overtime", "training", "bonus", "fringe",
          "homeoffice", "pension", "safety_coverage", "childcare_coverage", "leave"]

def _driver(t):
    name = {"safety_coverage": "safety_index", "childcare_coverage": "childcare_index",
            "leave": "parental_leave_index"}.get(t, f"{t}_index")
    return importlib.import_module(name)

def run():
    df = il.load_full_cao()
    bools = []
    for t in TOPICS:
        for b in getattr(_driver(t), "BOOLEANS", []):
            if b in df.columns:
                bools.append((t, b))
    df["_term"] = df["cao_number"].astype(str) + "|" + df["ingangsdatum"].astype(str)
    multi = df.groupby("_term").filter(lambda g: len(g) > 1)
    n_terms = multi["_term"].nunique()
    rows = []
    for t, b in bools:
        v = multi[b].astype(str).str.strip().str.lower()
        flagged = multi.assign(_v=v).groupby("_term")["_v"].nunique(dropna=False)
        n_flip = int((flagged > 1).sum())
        rows.append({"topic": t, "boolean": b, "n_multiedition_terms": n_terms,
                     "n_flipped": n_flip, "flip_rate": round(n_flip / n_terms, 3)})
    out = pd.DataFrame(rows).sort_values("flip_rate", ascending=False)
    out.to_csv(os.path.join(il.OUT, "boolean_flip_summary_post_campaign.csv"), sep=";", index=False)
    base_p = il.locate("boolean_flip_summary.csv")
    base_mean = base_max = None
    if os.path.exists(base_p):
        base = pd.read_csv(base_p, sep=";")
        base_mean, base_max = base["flip_rate"].mean(), base["flip_rate"].max()
    return out, base_mean, base_max

if __name__ == "__main__":
    out, bm, bx = run()
    print(f"post-campaign flip rate: mean {out['flip_rate'].mean():.3f} / max {out['flip_rate'].max():.3f}"
          + (f"  (pre-campaign baseline: mean {bm:.3f} / max {bx:.3f})" if bm is not None else ""))
    print(out.head(10).to_string(index=False))
