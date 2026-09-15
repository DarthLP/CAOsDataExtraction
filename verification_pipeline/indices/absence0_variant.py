"""
ABSENCE-0 VARIANT — "should a MISSING extra benefit count as 0 rather than be excluded?"

The published <topic>_z is AVAILABLE-CASE for EXTRA fields: a CAO that states no 13th month
is simply not scored on it (its bonus_z reflects only what it DOES state). The presence/
absence signal lives in COVERAGE instead. Hanna's question: for a generosity index, an
absent benefit is arguably 0, not "unknown".

This diagnostic builds a presence-gated zero-fill variant and measures its impact, WITHOUT
touching the primary index. The rule (principled, avoids scoring extraction misses as 0):

  an EXTRA field's blank becomes 0 ONLY when the topic's PRESENCE boolean is explicitly False
  (genuine absence). Blank WITH the boolean True (stated-but-unquantified) stays excluded.

Only applied to EXTRA fields whose presence boolean actually DISCRIMINATES (present-rate not
~1.0): bonus 13th-month, fringe meal, fringe relocation, homeoffice stipend/entitlement.
Fields with no boolean (severance_extra, pension early age) or a near-universal boolean
(training) are left available-case — absence there is genuinely ambiguous.

Output: absence0_impact.csv + prints. Run: python3.13 indices/absence0_variant.py
"""
import os, sys
import numpy as np
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import importlib

# topic -> list of (field, canonical, sign, presence_boolean)
GATED = {
    "bonus": [("bonus_thirteenth_month_amt_value", "pct_annual", +1, "bonus_thirteenth_month")],
    "fringe": [("fringe_meal_benefit_amt_value", "eur", +1, "fringe_meal_benefit_present"),
               ("fringe_relocation_allowance_value", "eur", +1, "fringe_relocation_allowance_present")],
    "homeoffice": [("homeoffice_stipend_value", "eur_per_month", +1, "homeoffice_stipend_present"),
                   ("homeoffice_entitlement_value", "count", +1, "homeoffice_has_homeoffice_rights")],
}


def main():
    df = il.load_full_cao(); df = il.add_newest(df)
    dmask = il.term_dedup_mask(df)
    rows = []
    for topic, fields in GATED.items():
        m = importlib.import_module(f"{topic}_index")
        clamp = getattr(m, "CLAMP", {})
        # normalise + forward-fill the topic's FULL field set (to reproduce the published z)
        allfields = m.FIELDS
        ncols = il.normalize_fields(df, allfields, clamp)
        d2 = il.forward_fill(df, ncols)
        dm = dmask.reindex(d2.index)
        # available-case per-field signed z (matches the published construction)
        def field_z(col, sign, zerofill_bool=None):
            x = d2[col + "__norm_ff"].copy()
            if zerofill_bool is not None:
                absent = d2[zerofill_bool].astype(str).str.strip().str.lower().eq("false")
                x = x.where(~(x.isna() & absent), 0.0)     # genuine absence -> 0
            return sign * il.pooled_z(x, il.pooled_params(x, dm))
        gated_cols = {f[0]: f[3] for f in fields}
        parts_av, parts_z0 = [], []
        for col, canon, sign in allfields:
            parts_av.append(field_z(col, sign))
            parts_z0.append(field_z(col, sign, gated_cols.get(col)))
        z_av = pd.concat(parts_av, axis=1).mean(axis=1)
        z_z0 = pd.concat(parts_z0, axis=1).mean(axis=1)
        new = d2["doc_is_newest"]
        a, b = z_av[new], z_z0[new]
        ok = a.notna() | b.notna()
        # how many newest CAOs gain a score they didn't have (were excluded, now 0-scored)
        gained = (a.isna() & b.notna()).sum()
        both = a.notna() & b.notna()
        rows.append({"topic": topic,
                     "newest_scored_available": int(a.notna().sum()),
                     "newest_scored_absence0": int(b.notna().sum()),
                     "newly_scored_as_absence": int(gained),
                     "rank_corr_where_both": round(a[both].rank().corr(b[both].rank()), 3) if both.sum() > 10 else None,
                     "mean_available": round(a.mean(), 3), "mean_absence0": round(b.mean(), 3)})
        # attach for composite comparison
        df[f"{topic}_z_av"] = z_av.reindex(df.index)
        df[f"{topic}_z_z0"] = z_z0.reindex(df.index)
    imp = pd.DataFrame(rows)
    imp.to_csv(os.path.join(il.OUT, "absence0_impact.csv"), sep=";", index=False)
    pd.set_option("display.width", 200)
    print("Per-topic impact of presence-gated zero-fill (newest doc per CAO):")
    print(imp.to_string(index=False))

    # composite impact: swap these 3 topics' z for the absence0 version, others unchanged
    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    base = comp.set_index("id")
    GEN = ["leave", "absence", "term", "contract", "overtime", "training", "bonus",
           "fringe", "homeoffice", "pension", "wage"]
    # overall_numeric_z is the NUMERIC (magnitude) roll-up (NAMING.md); wage = its single track
    zcols = [f"{t}_numeric_z" if t != "wage" else "wage_z" for t in GEN]
    Zav = base[zcols].apply(pd.to_numeric, errors="coerce")
    Zz0 = Zav.copy()
    idmap = dict(zip(df["id"], df.index))
    for topic in GATED:
        z0 = df.set_index("id")[f"{topic}_z_z0"]
        Zz0[f"{topic}_numeric_z"] = pd.to_numeric(z0.reindex(Zz0.index), errors="coerce")
    ov_av = Zav.mean(axis=1); ov_z0 = Zz0.mean(axis=1)
    newest = base["doc_is_newest"].astype(str) == "True"
    a, b = ov_av[newest], ov_z0[newest]
    both = a.notna() & b.notna()
    print(f"\nCOMPOSITE overall_numeric_z, newest {int(newest.sum())} CAOs:")
    print(f"  rank corr(available, absence0) = {a[both].rank().corr(b[both].rank()):.3f}")
    print(f"  mean shift {a.mean():.3f} -> {b.mean():.3f};  "
          f"CAOs dropping >0.25 in overall = {int(((a-b)>0.25).sum())}")
    mv = (a - b).sort_values(ascending=False).head(6)
    print("  biggest drops (CAOs that look generous available-case but sparse on extras):")
    for cid, dd in mv.items():
        print(f"    id {cid}: {a[cid]:+.2f} -> {b[cid]:+.2f}  (Δ {dd:+.2f})")


if __name__ == "__main__":
    main()
