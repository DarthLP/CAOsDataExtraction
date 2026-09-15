"""CAO-agreement-level export (concise import for the admin-data environment).

DESIGN (Hanna 2026-07-15, v2): one row per DOCUMENT, organized by agreement term — NOT one
row per term. Rationale: within one agreement term (same cao_number + ingangsdatum) multiple
files arrive over time (wage-table updates mostly; occasionally non-salary changes too). All
of them share the ingangsdatum, so the only way to order them — and to rebuild the exact
in-force panel — is the file_date (datum_kennisgeving, the SZW upload/receipt date). Keeping
one row per file with term bookkeeping preserves the full panel: in month m the in-force row
is the one with the latest file_date <= end of m. Collapsing to one row per term would lose
every within-term update.

Term bookkeeping per row: term_edition_seq (1,2,... by file_date within the term),
n_editions_in_term, is_last_filed_in_term. Both date axes plus retro/AVV fields are carried
so the panel can be rebuilt on either convention (kennisgeving = knowable/enforceable;
ingangsdatum = retroactive coverage). Two derived date axes sit next to the raw dates
(Hanna 2026-07-15): Date_first_is_Ingangsdatum = the first-file rule (term's first
file starts at its ingangsdatum, later files at their file_date) and Date_retro_datum =
the same but overridden by the explicit general_retro_start_date where the corrected
dataset records one. thin_doc/scores_carried_from/score_src_id mark carried
partial captures (see ReadMe_for_Hanna.md for the per-document carry list).

Output: indices/out/cao_agreement_level.csv (`;`-separated).
Run: python3 build_cao_level_export.py
"""
import os
import sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il


def main():
    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    comp["_fd"] = comp["file_date"].map(il.parse_date)
    comp["_idnum"] = pd.to_numeric(comp["id"], errors="coerce")
    comp["_ing"] = comp["ingangsdatum"].astype(str).str.strip()
    comp = comp[comp["_ing"].ne("") & comp["_ing"].str.lower().ne("nan")].copy()

    raw = pd.read_csv(os.path.join(HERE, "..", "qa", "corrected_dataset.csv"),
                      sep=";", dtype=str, low_memory=False).fillna("")
    raw = raw.set_index(raw["id"].astype(str))

    # term bookkeeping: order files within (cao, ingangsdatum) by file_date (tie: id)
    comp = comp.sort_values(["cao_number", "_ing", "_fd", "_idnum"])
    grp = comp.groupby(["cao_number", "_ing"])
    comp["term_edition_seq"] = grp.cumcount() + 1
    comp["n_editions_in_term"] = grp["id"].transform("size")
    comp["is_last_filed_in_term"] = comp["term_edition_seq"] == comp["n_editions_in_term"]

    # Date_first_is_Ingangsdatum (Hanna 2026-07-15): the first-file rule —
    # the term's FIRST file starts at its ingangsdatum (the initial text governs the whole term
    # from its start); every LATER file in the term keeps its own file_date. ISO format.
    def _iso(d):
        return d.isoformat() if d is not None and pd.notna(d) else ""
    comp["_ing_d"] = comp["_ing"].map(il.parse_date)
    _first = comp["term_edition_seq"].eq(1) & comp["_ing_d"].notna()
    comp["Date_first_is_Ingangsdatum"] = [
        _iso(i if f else d) for f, i, d in zip(_first, comp["_ing_d"], comp["_fd"])]

    meta = ["cao_number", "id", "file_name", "document_type", "file_date", "ingangsdatum",
            "Date_first_is_Ingangsdatum", "expiratiedatum",
            "term_group", "term_edition_seq", "n_editions_in_term", "is_last_filed_in_term",
            "pub_lag_months", "doc_is_newest", "thin_doc", "scores_carried_from", "score_src_id"]
    meta = [c for c in meta if c in comp.columns]
    # score block: all per-topic z/pctile/coverage + roll-ups + counts (wage columns are picked
    # up here too but dropped again below, after the without-wage roll-ups are verified)
    zcols = [c for c in comp.columns if c.endswith(("_z", "_pctile", "_coverage", "_z_var"))
             or c.startswith(("mw_", "ratio_", "wml_", "n_topics_"))
             or c in ("coverage_overall", "coverage_overall_pctile", "wage_src_year")]
    zcols = [c for c in zcols if c not in meta]

    out = comp[meta + zcols].copy()
    # retro + AVV + signing dates from the raw dataset
    for c_raw, c_out in [("general_retro_start_date", "retro_start_date"),
                         ("general_retro_end_date", "retro_end_date"),
                         ("general_avv_start_date", "avv_start_date"),
                         ("general_avv_end_date", "avv_end_date"),
                         ("general_signing_date", "signing_date")]:
        out[c_out] = [raw.at[str(i), c_raw] if str(i) in raw.index and c_raw in raw.columns else ""
                      for i in out["id"]]
    out["retro_applies"] = out["retro_start_date"].astype(str).str.strip().ne("")

    # Date_retro_datum (Hanna 2026-07-15): where the corrected dataset records an EXPLICIT
    # retroactivity start date (general_retro_start_date), that date is the first date the
    # file is active; every other row keeps Date_first_is_Ingangsdatum. ISO format.
    _retro_d = out["retro_start_date"].map(il.parse_date)
    out["Date_retro_datum"] = [
        _iso(r) if r is not None else b
        for r, b in zip(_retro_d, out["Date_first_is_Ingangsdatum"])]
    cols = list(out.columns)
    cols.insert(cols.index("Date_first_is_Ingangsdatum") + 1,
                cols.pop(cols.index("Date_retro_datum")))
    out = out[cols]

    # ---- WITHOUT-WAGE overall scores (Hanna 2026-07-15) -------------------------------------
    # At document grain the wage block is a borrowed CAO-year fact (pooled across files, mild
    # look-ahead), so the export's overall scores EXCLUDE wage and say so in their names
    # (*_without_wage); the wage block itself is dropped — wage lives in the Wage companion
    # table (cao x year) and joins back at panel-build. PanelMonthly keeps wage-inclusive
    # overall_z under the plain names.
    MAG10 = ["leave", "absence", "term", "contract", "overtime", "training", "bonus",
             "fringe", "homeoffice", "pension"]
    def _blk(cols):
        return out[[c for c in cols if c in out.columns]].apply(pd.to_numeric, errors="coerce")
    CB = _blk([f"{t}_z" for t in MAG10] + ["safety_coverage_z", "childcare_coverage_z"])
    CP = _blk([f"{t}_pctile" for t in MAG10] + ["safety_coverage_pctile", "childcare_coverage_pctile"])
    ZN = _blk([f"{t}_numeric_z" for t in MAG10])
    PN = _blk([f"{t}_numeric_pctile" for t in MAG10])
    # verification: adding wage back must reproduce the composite's wage-inclusive roll-ups
    for base, extra, orig in [(CB, "wage_median_z", "overall_z"), (CP, "wage_median_pctile", "overall_pctile"),
                              (ZN, "wage_median_z", "overall_numeric_z"), (PN, "wage_median_pctile", "overall_numeric_pctile")]:
        chk = pd.concat([base, pd.to_numeric(out[extra], errors="coerce")], axis=1).mean(axis=1)
        d = (chk - pd.to_numeric(out[orig], errors="coerce")).abs().max()
        assert d < 5e-4, f"without-wage recompute mismatch on {orig}: {d}"
    out["overall_z_without_wage"] = CB.mean(axis=1).round(4)
    out["overall_z_var_without_wage"] = CB.var(axis=1, ddof=0).where(CB.notna().sum(axis=1) >= 2).round(4)
    out["n_topics_combined_without_wage"] = CB.notna().sum(axis=1)
    out["overall_pctile_without_wage"] = CP.mean(axis=1).round(4)
    out["overall_numeric_z_without_wage"] = ZN.mean(axis=1).round(4)
    out["overall_numeric_z_var_without_wage"] = ZN.var(axis=1, ddof=0).where(ZN.notna().sum(axis=1) >= 2).round(4)
    out["n_topics_scored_without_wage"] = ZN.notna().sum(axis=1)
    out["overall_numeric_pctile_without_wage"] = PN.mean(axis=1).round(4)
    out["n_topics_pctile_without_wage"] = PN.notna().sum(axis=1)
    DROP = (["overall_z", "overall_pctile", "overall_z_var", "overall_numeric_z",
             "overall_numeric_z_var", "overall_numeric_pctile", "n_topics_scored",
             "n_topics_combined", "n_topics_pctile",
             "wage_median_z", "wage_mean_z", "wage_median_pctile", "wage_mean_pctile",
             "wage_span_z", "mw_low", "mw_median", "mw_mean", "mw_high",
             "ratio_low_wml", "ratio_median_wml", "ratio_mean_wml", "ratio_high_wml",
             "wml_month", "wage_src_year"])
    out = out.drop(columns=[c for c in DROP if c in out.columns])

    out = out.sort_values(["cao_number", "ingangsdatum", "term_edition_seq"])
    p = os.path.join(il.OUT, "cao_agreement_level.csv")
    out.to_csv(p, sep=";", index=False)
    print(f"wrote {p}: {len(out)} document rows in {out.groupby(['cao_number','ingangsdatum']).ngroups} "
          f"agreement terms, {out['cao_number'].nunique()} CAOs, {len(out.columns)} cols")
    return out


if __name__ == "__main__":
    main()
