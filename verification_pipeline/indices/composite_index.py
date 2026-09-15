"""CROSS-TOPIC COMPOSITE v2 — one row per full-CAO DOCUMENT (not just the newest).

Joins every topic's pooled z (comparable across years by construction) plus the
wage dimension (nominal wage_median_z from mw_indices, joined by cao x file-year) and rolls up:

  overall_numeric_z : equal-weight available-case mean over the 10 generosity
                   dimensions (leave term contract overtime training bonus fringe
                   homeoffice pension wage) — req 3a.
  overall_z_var  : variance (ddof=0) across those same topic z's = how UNEVEN the
                   package is across topics (lopsidedness), NaN when <2 topics.
  coverage_overall / coverage_overall_z : provision breadth over the 9 coverage
                   topics (v1's 8 + leave, new).

Equal weights are ONE transparent default; every component is exposed to reweight.
Output: composite_index.csv (doc grain — the monthly panel explodes this over time).
Run: python3.13 indices/composite_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import pandas as pd

MAG = ["term", "contract", "overtime", "training", "bonus", "fringe", "homeoffice", "pension",
       "absence"]                          # absence added 2026-07-05 (Tier 1)
# ai DROPPED from the overall coverage roll-up 2026-07-07: 99.3% of CAOs have no AI clause yet
# (era-artifact, not a bargaining choice), so it added noise. Its ai_coverage column is still
# emitted per-topic (ai_index.csv) for reference; childcare stays (its zeros are genuine
# "offers nothing" declines = real signal).
COV = ["safety", "training", "bonus", "fringe", "homeoffice", "pension", "childcare", "absence"]
GEN = ["leave"] + MAG + ["wage"]          # the 11 generosity dimensions


def apply_thin_doc_carry(comp):
    """THIN-DOCUMENT rule (2026-07-08, METHODOLOGY §12): some editions typed full_cao_* are not
    substantive full CAOs (mis-ingested appendix, mantel/umbrella doc deferring to companion
    regulations, deviations-only delta, procedure-only doc, failed extraction). Scoring them reads
    as "all provisions abolished" and produces spurious 0.5-1.2 dips that rebound at the next real
    edition. Rule (calibrated in thin_doc_simulation.csv, 6/6 known cases, 0 misses of genuine
    scope changes):
      CANDIDATE  = vs the CAO's last NON-THIN edition (chained baseline; a CAO's first doc never
                   flags): populated cells < 80% AND True-booleans < 85% (vacuous if baseline 0).
      REMEDY     = only for candidates SOURCE-VERIFIED as thin (indices/review/thin_docs_reviewed.csv,
                   verdict THIN_CONFIRMED): the previous non-thin edition's SCORE columns are
                   carried forward (the old agreement's conditions remain in force); identity
                   columns stay the doc's own; the row is MARKED thin_doc=True +
                   scores_carried_from=<baseline id>. Candidates NOT yet in the registry get NO
                   remedy — they are written to indices/out/thin_doc_candidates_unreviewed.csv and
                   printed, so every new flag is source-checked before it ever changes a score.
    GENUINE_CHANGE / COMPLETE_DOC verdicts are never carried (real narrower agreements stay)."""
    reg_path = il.locate("thin_docs_reviewed.csv")
    reg, carry_from = {}, {}
    if os.path.exists(reg_path):
        r = il.read_csv_safe(reg_path)
        reg = dict(zip(r["id"].astype(str), r["verdict"].astype(str)))
        if "carry_from_id" in r.columns:   # CROSS-CAO deferral (e.g. RPO -> national CAO PO):
            carry_from = {i: c for i, c in zip(r["id"].astype(str), r["carry_from_id"].astype(str))
                          if c.strip()}    # carry THAT doc's scores instead of the own-CAO baseline
    dfc = il.load_full_cao()
    dfc["_n_pop"] = (dfc.astype(str) != "").sum(axis=1)
    boolcols = [c for c in dfc.columns if not c.startswith("general_")
                and set(dfc[c].astype(str).str.strip().unique()) <= {"True", "False", ""}
                and (dfc[c].astype(str).str.strip() != "").any()]
    dfc["_n_true"] = (dfc[boolcols].astype(str) == "True").sum(axis=1)
    counts = dfc.set_index(dfc["id"].astype(str))[["_n_pop", "_n_true"]]

    comp = comp.reset_index(drop=True)
    comp["thin_doc"] = "False"
    comp["scores_carried_from"] = ""
    META = set(il.ID_COLS) | {"thin_doc", "scores_carried_from"}
    score_cols = [c for c in comp.columns if c not in META]
    comp["_fd"] = comp["file_date"].map(il.parse_date)
    comp["_idn"] = pd.to_numeric(comp["id"], errors="coerce")
    unreviewed, n_carried = [], 0
    for cao, g in comp.groupby("cao_number"):
        g = g.sort_values(["_fd", "_idn"])
        base = None                                    # index of the last non-thin edition
        for i in g.index:
            did = str(comp.at[i, "id"])
            if base is None or did not in counts.index or str(comp.at[base, "id"]) not in counts.index:
                base = i
                continue
            pop_r = counts.at[did, "_n_pop"] / max(counts.at[str(comp.at[base, "id"]), "_n_pop"], 1)
            bt = counts.at[str(comp.at[base, "id"]), "_n_true"]
            bool_r = (counts.at[did, "_n_true"] / bt) if bt > 0 else 1.0
            candidate = pop_r < 0.80 and (bt == 0 or bool_r < 0.85)
            if candidate and reg.get(did) == "THIN_CONFIRMED":
                src = base                              # default: own-CAO last full edition
                if did in carry_from:                  # cross-CAO deferral: the doc names ANOTHER
                    hits = comp.index[comp["id"].astype(str) == carry_from[did]]
                    if len(hits) == 1:                 # CAO as its base (source-verified in registry)
                        src = hits[0]
                    else:
                        print(f"  ⚠ thin-doc rule: carry_from_id {carry_from[did]} for {did} not "
                              f"found uniquely — falling back to own-CAO baseline")
                comp.loc[i, score_cols] = comp.loc[src, score_cols].values
                comp.at[i, "thin_doc"] = "True"
                comp.at[i, "scores_carried_from"] = str(comp.at[src, "id"])
                n_carried += 1                          # baseline unchanged -> chained carry
            else:
                if candidate and did not in reg:
                    unreviewed.append({"id": did, "cao_number": cao,
                                       "file_name": comp.at[i, "file_name"],
                                       "pop_ratio_vs_baseline": round(pop_r, 3),
                                       "bool_ratio_vs_baseline": round(bool_r, 3)})
                base = i
    comp = comp.drop(columns=["_fd", "_idn"])
    up = os.path.join(il.OUT, "thin_doc_candidates_unreviewed.csv")
    if unreviewed:
        pd.DataFrame(unreviewed).to_csv(up, sep=";", index=False)
        print(f"  ⚠ thin-doc rule: {len(unreviewed)} NEW candidate(s) need SOURCE REVIEW before any "
              f"remedy -> {up} (no scores were changed for them)")
    elif os.path.exists(up):
        os.remove(up)                                  # stale candidate list from a previous run
    print(f"  thin-doc rule: {n_carried} edition(s) carried forward from their last full edition "
          f"(marked thin_doc=True, scores_carried_from)")
    return comp


def main():
    # doc spine: term_index carries the full ID block for all 2,698 full-CAO docs
    spine_cols = il.ID_COLS
    # NOTE: topic CSVs use the NAMING.md taxonomy ({t}_numeric_z); the composite computes with the
    # internal short name {t}_z (=magnitude) and republishes the taxonomy via il.apply_scheme at the end.
    comp = (il.read_csv_safe(il.locate("term_index.csv"))[spine_cols + ["term_numeric_z"]]
            .rename(columns={"term_numeric_z": "term_z"}).copy())

    for t in MAG:
        if t == "term": continue
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        comp = comp.merge(d[["id", f"{t}_numeric_z"]].rename(columns={f"{t}_numeric_z": f"{t}_z"}),
                          on="id", how="left")

    leave = il.read_csv_safe(il.locate("parental_leave_index.csv"))
    comp = comp.merge(leave[["id", "leave_numeric_z", "leave_coverage", "leave_coverage_z"]]
                      .rename(columns={"leave_numeric_z": "leave_z"}), on="id", how="left")

    # wage: the NOMINAL median-wage z (z of mw_median EUR) is the HEADLINE wage score; the nominal
    # mean-wage z is exposed alongside. WML normalisation lives ONLY in the ratio_*_wml value columns.
    # Internally we drive the overall on wage_z (=median), then relabel wage_z -> wage_median_z and
    # add wage_mean_z/_pctile after apply_scheme.
    mw = il.read_csv_safe(il.locate("mw_indices.csv"))
    # 2026-07-15 (Hanna): Composite mirrors the PanelMonthly wage block — full mw_* level
    # quartet + all four WML ratios + wml_month, not just the z's and median/mean ratios.
    mw = mw[["cao_number", "year", "wage_median_z", "wage_mean_z",
             "mw_low", "mw_median", "mw_mean", "mw_high",
             "ratio_low_wml", "ratio_median_wml", "ratio_mean_wml", "ratio_high_wml",
             "wml_month"]].copy()   # NOMINAL z's; levels + ratios kept as values
    # AS-OF (forward-fill) wage join, 2026-07-08: a wage scale stays in force until a new one is
    # agreed, so a doc filed in a GAP year (no parsed wage table that calendar year) carries the
    # last known year's wage instead of silently losing the wage input from overall_z (the old
    # exact-year join made wage drop out of the available-case mean in gap years — e.g. CAO 1029's
    # 2018 doc — one of the spurious downward-jump mechanisms). Mirrors the panel's as-of fill
    # (build_panel_monthly). `wage_src_year` records the wage row actually used (staleness visible).
    WCOLS = ["wage_z", "wage_mean_z", "mw_low", "mw_median", "mw_mean", "mw_high",
             "ratio_low_wml", "ratio_median_wml", "ratio_mean_wml", "ratio_high_wml", "wml_month"]
    mw = mw.rename(columns={"wage_median_z": "wage_z"})
    mw["_yr"] = pd.to_numeric(mw["year"], errors="coerce")
    mw = mw.dropna(subset=["_yr"]).copy()
    mw["_yr"] = mw["_yr"].astype("int64")
    mw["_cao"] = mw["cao_number"].astype(str)
    comp["year"] = comp["year"].astype(str)
    lft = pd.DataFrame({"_cao": comp["cao_number"].astype(str),
                        "_yr": pd.to_numeric(comp["year"], errors="coerce"),
                        "_row": range(len(comp))}).dropna(subset=["_yr"])
    lft["_yr"] = lft["_yr"].astype("int64")
    m = pd.merge_asof(lft.sort_values("_yr"),
                      mw[["_cao", "_yr"] + WCOLS].rename(columns={"_yr": "_wyr"})
                        .sort_values("_wyr").assign(_yr=lambda d: d["_wyr"]),
                      on="_yr", by="_cao", direction="backward").set_index("_row")
    for c_ in WCOLS + ["_wyr"]:
        comp[c_ if c_ != "_wyr" else "wage_src_year"] = m[c_].reindex(range(len(comp))).values

    zcols = [f"{t}_z" for t in GEN]
    Z = comp[zcols].apply(pd.to_numeric, errors="coerce")
    comp["overall_numeric_z"] = Z.mean(axis=1).round(4)   # numeric-only roll-up (companion)
    comp["overall_numeric_z_var"] = Z.var(axis=1, ddof=0).where(Z.notna().sum(axis=1) >= 2).round(4)  # numeric spread (companion to overall_z_var)
    comp["n_topics_scored"] = Z.notna().sum(axis=1)

    for t in COV:
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        comp = comp.merge(d[["id", f"{t}_coverage"]], on="id", how="left")
    covcols = [f"{t}_coverage" for t in COV] + ["leave_coverage"]
    C = comp[covcols].apply(pd.to_numeric, errors="coerce")
    comp["coverage_overall"] = C.mean(axis=1).round(4)

    # pooled z of overall coverage (yardstick = one doc per term_group, latest file_date)
    s = comp.sort_values(["term_group", "file_date", "id"])
    dmask = pd.Series(comp.index.isin(s.groupby("term_group").tail(1).index), index=comp.index)
    co = pd.to_numeric(comp["coverage_overall"], errors="coerce")
    comp["coverage_overall_z"] = il.pooled_z(co, il.pooled_params(co, dmask)).round(4)

    # ---- EQUAL-RANGE PERCENTILE TRACK (2026-07-07) ----
    # Every field/topic on ONE symmetric [0,1] scale (mean of per-field ECDF ranks), so being
    # best in a bunched field fully offsets being worst in a spread field — cures the z-range
    # asymmetry (reimb z max +0.33 vs budget +3). overall_gen01 = equal-weight available-case
    # mean of the 11 topic gen01's; coverage_overall_pctile = same for the 9 coverage topics.
    for t in MAG:                                  # topic CSVs carry {t}_numeric_pctile (equal-range)
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        if f"{t}_numeric_pctile" in d.columns:
            comp = comp.merge(d[["id", f"{t}_numeric_pctile"]].rename(columns={f"{t}_numeric_pctile": f"{t}_gen01"}),
                              on="id", how="left")
    lv = il.read_csv_safe(il.locate("parental_leave_index.csv"))
    comp = comp.merge(lv[["id", "leave_numeric_pctile", "leave_coverage_pctile"]]
                      .rename(columns={"leave_numeric_pctile": "leave_gen01"}), on="id", how="left")
    # wage generosity percentile = yardstick ECDF of the (nominal) wage z — the SAME pooled,
    # term-deduped ECDF (il.pooled_pctile) every other topic pctile uses. (Was a reprint-weighted
    # rank(pct=True) over all docs, which distorted the wage scale toward reprint-happy CAOs.)
    wz = pd.to_numeric(comp["wage_z"], errors="coerce")
    comp["wage_gen01"] = il.pooled_pctile(wz, dmask).round(4)
    gen_cols = [f"{t}_gen01" for t in GEN if f"{t}_gen01" in comp.columns]
    G = comp[gen_cols].apply(pd.to_numeric, errors="coerce")
    comp["overall_gen01"] = G.mean(axis=1).round(4)
    comp["n_topics_gen01"] = G.notna().sum(axis=1)

    for t in COV:
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        if f"{t}_coverage_pctile" in d.columns:
            comp = comp.merge(d[["id", f"{t}_coverage_pctile"]], on="id", how="left")
    covp_cols = [f"{t}_coverage_pctile" for t in COV if f"{t}_coverage_pctile" in comp.columns] \
                + (["leave_coverage_pctile"] if "leave_coverage_pctile" in comp.columns else [])
    CP = comp[covp_cols].apply(pd.to_numeric, errors="coerce")
    comp["coverage_overall_pctile"] = CP.mean(axis=1).round(4)

    # ---- COMBINED per-topic generosity = mean of [magnitude z, coverage z] ----
    # Two-part model (2026-07-06 revised): coverage tracks PRESENCE, magnitude tracks AMOUNT
    # (absent -> 0). The 5 zero-filled EXTRA benefits (bonus 13th, fringe meal/reloc/commuting,
    # homeoffice stipend) appear in BOTH their topic's magnitude and coverage, so this combined
    # score modestly OVER-WEIGHTS their presence (counted in both parts). Accepted as the price
    # of losing NO present-but-unquantified CAOs (the DISJOINT alternative counted each benefit
    # once but dropped the present-but-unquantified group entirely). A topic with only magnitude
    # (absence, wage) or only coverage (safety, childcare, ai) contributes that single score.
    MAGZ = ["leave", "absence", "term", "contract", "overtime", "training", "bonus",
            "fringe", "homeoffice", "pension", "wage"]
    COVZ = ["leave", "absence", "term", "contract", "overtime", "training", "bonus", "fringe",
            "homeoffice", "pension", "safety", "childcare"]   # absence added 2026-07-08; ai dropped
    for t in COVZ:
        cz = f"{t}_coverage_z"
        if cz in comp.columns:
            continue
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        if cz in d.columns:
            comp = comp.merge(d[["id", cz]], on="id", how="left")
    combined_cols = []
    for t in sorted(set(MAGZ) | set(COVZ)):
        parts = [c for c in ([f"{t}_z"] if t in MAGZ else []) + ([f"{t}_coverage_z"] if t in COVZ else [])
                 if c in comp.columns]
        if not parts:
            continue
        comp[f"{t}_combined_z"] = comp[parts].apply(pd.to_numeric, errors="coerce").mean(axis=1).round(4)
        combined_cols.append(f"{t}_combined_z")
    CB = comp[combined_cols].apply(pd.to_numeric, errors="coerce")
    comp["overall_z"] = CB.mean(axis=1).round(4)          # PRIMARY overall (combined = numeric + coverage)
    comp["overall_z_var"] = CB.var(axis=1, ddof=0).where(CB.notna().sum(axis=1) >= 2).round(4)  # spread of overall_z
    comp["n_topics_combined"] = CB.notna().sum(axis=1)

    # ---- COMBINED per-topic on the [0,1] percentile scale = mean of [gen01, coverage_pctile] ----
    for t in COVZ:                                 # ensure every topic's coverage_pctile is present
        cp = f"{t}_coverage_pctile"
        if cp in comp.columns:
            continue
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        if cp in d.columns:
            comp = comp.merge(d[["id", cp]], on="id", how="left")
    combined01_cols = []
    for t in sorted(set(MAGZ) | set(COVZ)):
        parts = [c for c in ([f"{t}_gen01"] if t in MAGZ else []) + ([f"{t}_coverage_pctile"] if t in COVZ else [])
                 if c in comp.columns]
        if not parts:
            continue
        comp[f"{t}_combined01"] = comp[parts].apply(pd.to_numeric, errors="coerce").mean(axis=1).round(4)
        combined01_cols.append(f"{t}_combined01")
    CB01 = comp[combined01_cols].apply(pd.to_numeric, errors="coerce")
    comp["overall_combined01"] = CB01.mean(axis=1).round(4)

    comp = comp.sort_values(["cao_number", "file_date", "id"])
    comp = apply_thin_doc_carry(comp)     # thin editions: carry last full edition's scores, MARKED
    # score_src_id (PanelMonthly parity): the document whose scores this row carries —
    # itself unless a thin-doc carry redirected it.
    comp["score_src_id"] = comp["scores_carried_from"].astype(str).str.strip()
    comp.loc[comp["score_src_id"].isin(["", "nan"]), "score_src_id"] = comp["id"].astype(str)
    comp = il.apply_scheme(comp)          # republish internal names in the NAMING.md taxonomy
    # WAGE variants (no numeric/coverage split; instead median vs mean relative-to-WML). The overall
    # was driven on wage_z (=median); relabel it wage_median_z and add the mean-relative companion.
    comp = comp.rename(columns={"wage_z": "wage_median_z", "wage_pctile": "wage_median_pctile"})
    if "wage_mean_z" in comp.columns:
        comp["wage_mean_pctile"] = pd.to_numeric(comp["wage_mean_z"], errors="coerce").rank(pct=True).round(4)
    out = os.path.join(il.OUT, "composite_index.csv")
    comp.to_csv(out, sep=";", index=False)
    print(f"wrote {out} ({len(comp)} docs, {len(comp.columns)} cols)")

    new = comp[comp["doc_is_newest"] == "True"] if comp["doc_is_newest"].dtype == object \
        else comp[comp["doc_is_newest"]]
    m = lambda s: pd.to_numeric(s, errors="coerce")
    print(f"newest docs: {len(new)} | overall_numeric_z mean {m(new['overall_numeric_z']).mean():.3f} "
          f"| median topics scored {m(new['n_topics_scored']).median():.0f}/{len(GEN)}")
    print("most generous newest docs (top 8):")
    top = new.sort_values("overall_numeric_z", key=lambda s: pd.to_numeric(s, errors="coerce"),
                          ascending=False)
    print(top[["cao_number", "id", "overall_numeric_z", "overall_z_var",
               "n_topics_scored", "coverage_overall"]].head(8).to_string(index=False))
    print(f"corr(overall_numeric_z, coverage_overall) = "
          f"{m(comp['overall_numeric_z']).corr(m(comp['coverage_overall'])):.3f} "
          f"(two-part model: presence in coverage AND magnitude for the 5 zero-filled benefits, "
          f"so they share signal — this is expected, not double-counting a single number)")
    print(f"overall_z (combined): newest mean {m(new['overall_z']).mean():.3f}, "
          f"median topics combined {m(new['n_topics_combined']).median():.0f}")
    print(f"[percentile track] overall_numeric_pctile newest mean {m(new['overall_numeric_pctile']).mean():.3f} "
          f"(0-1, 0.5=median) | coverage_overall_pctile mean {m(new['coverage_overall_pctile']).mean():.3f} "
          f"| overall_pctile mean {m(new['overall_pctile']).mean():.3f}")
    print(f"corr(overall_numeric_z, overall_numeric_pctile) = "
          f"{m(comp['overall_numeric_z']).corr(m(comp['overall_numeric_pctile'])):.3f} "
          f"(z vs equal-range percentile aggregation of the SAME fields — high but <1 because "
          f"gen01 gives symmetric range to skewed fields the z under/over-weights)")


if __name__ == "__main__":
    main()
