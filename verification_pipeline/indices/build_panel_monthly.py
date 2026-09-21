"""
BUILD_PANEL_MONTHLY — the calendar-month in-force panel (v2 plan §4/§5b).

Grain: one row per cao_number x month, from the CAO's first file month through the
build month, plus a STATUTORY pseudo-CAO row per month.

IN-FORCE RULE ("round up"): the file in force in month m = the file with the latest
file_date (datum_kennisgeving) <= the END of m. A file published anywhere inside a
month owns that whole month; two files in one month -> the later one owns it
(same-day tie: higher id).

Scores are the file's POOLED z's — constant while a file is in force, EXCEPT the
statutory-anchored components (leave FRE, absence floors, ketenregeling defaults,
ATW caps/floor), which are re-scored at the PANEL MONTH's legal era: a law change
hits every in-force CAO the month it takes effect, not at the CAO's next file
(LAW-MONTH re-scoring, 2026-07-06). Wage columns are the CAO's (cao, calendar-year)
wage row broadcast over its 12 months.

Outputs:
  all_indices_panel_monthly.csv   cao x month (+ STATUTORY rows)
  all_indices_panel_yearly.csv    the December slice per year (convenience)
  panel_aggregates_monthly.csv    per month: cross-CAO mean/variance of each z
                                  (+ n, p10/p50/p90 of the overall) — req 3b
Run: python3.13 indices/build_panel_monthly.py
"""
import os, sys
from datetime import date
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

MAG = ["leave", "term", "contract", "overtime", "training", "bonus", "fringe",
       "homeoffice", "pension", "absence"]
COVC = ["safety_coverage", "training_coverage", "bonus_coverage", "fringe_coverage",
        "homeoffice_coverage", "pension_coverage", "childcare_coverage",   # ai dropped 2026-07-07
        "leave_coverage", "absence_coverage"]   # absence gained coverage 2026-07-08
ZCOLS = [f"{t}_numeric_z" for t in MAG] + ["wage_median_z"]   # magnitude track (NAMING.md)
STALE_MONTHS = 48


def ym(d): return d.year * 12 + (d.month - 1)          # month serial
def ym_str(s): return f"{s // 12:04d}-{s % 12 + 1:02d}"


def main(axis="file", suffix=""):
    """axis='file'  : in-force by datum_kennisgeving (what was filed & enforceable) — default.
       axis='first' : Hanna 2026-07-15 — the TERM'S FIRST file (lowest file_date within the
       cao x ingangsdatum term) is backdated to start at month(ingangsdatum), because the
       initial text is what retroactively governs from the term start (terugwerkende kracht);
       every LATER file in the term keeps its own file-month start (mid-term wage/condition
       updates apply going forward, not retroactively). Explicit general_retro_* clauses exist
       for only ~25%% of records, so the ingangsdatum rule is the robust default anchor."""
    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    comp["_fd"] = comp["file_date"].map(il.parse_date)
    comp["_idnum"] = pd.to_numeric(comp["id"], errors="coerce")
    comp = comp.dropna(subset=["_fd"]).reset_index(drop=True)
    build_month = ym(date.today())
    print(f"{len(comp)} dated full-CAO docs, {comp['cao_number'].nunique()} CAOs")

    # per CAO: docs sorted by (file_date, id); doc i owns months
    # [month(fd_i) .. month(fd_{i+1}) - 1], last doc through the build month.
    rows = []
    if axis == "first":
        comp["_ing_d"] = comp["ingangsdatum"].map(il.parse_date)
        # first file of each (cao, term): backdate its start to month(ingangsdatum)
        comp["_start_d"] = comp["_fd"]
        first_idx = (comp.sort_values(["_fd", "_idnum"])
                         .groupby(["cao_number", "ingangsdatum"], dropna=False).head(1).index)
        ok = comp.loc[first_idx, "_ing_d"].notna()
        comp.loc[first_idx[ok], "_start_d"] = comp.loc[first_idx[ok], "_ing_d"]
    elif axis == "retro":
        # Date_retro_datum rule (per-document, from the agreement-level export): the explicit
        # general_retro_start_date where the text states one, else the first-file rule; docs
        # not in the export (blank ingangsdatum) fall back to their file_date.
        ag = il.read_csv_safe(os.path.join(il.OUT, "cao_agreement_level.csv"))
        rmap = dict(zip(ag["id"].astype(str), ag["Date_retro_datum"].astype(str)))
        comp["_start_d"] = [il.parse_date(rmap.get(str(i), "")) or f
                            for i, f in zip(comp["id"], comp["_fd"])]
    else:
        comp["_start_d"] = comp["_fd"]
    for cao, g in comp.groupby("cao_number"):
        g = g.sort_values(["_start_d", "_fd", "_idnum"])
        months = [ym(d) for d in g["_start_d"]]
        idxs = list(g.index)
        for k, (m0, idx) in enumerate(zip(months, idxs)):
            m1 = build_month if k == len(idxs) - 1 else months[k + 1] - 1
            for mm in range(m0, m1 + 1):                 # empty when superseded same month
                rows.append((cao, mm, idx))
    P = pd.DataFrame(rows, columns=["cao_number", "_ym", "src"])
    P["month"] = P["_ym"].map(ym_str)
    P["months_since_file"] = P["_ym"] - P["src"].map(dict(zip(comp.index, [ym(d) for d in comp["_fd"]])))
    P["is_stale"] = P["months_since_file"] > STALE_MONTHS

    # carry per-doc summaries incl. the equal-range RANK columns (2026-07-07). NB: the panel
    # re-scores anchored topic *z's* at each law-month; these gen01/pctile ranks are the DOC's
    # own (not law-month re-ranked) — convenience columns, labelled as such.
    carry = (["id", "file_date", "ingangsdatum", "pub_lag_months", "doc_is_newest",
              "term_group", "overall_numeric_z", "overall_z_var", "n_topics_scored",
              "coverage_overall", "overall_numeric_pctile", "coverage_overall_pctile",
              "overall_z", "overall_pctile", "thin_doc", "scores_carried_from"]
             + [f"{t}_numeric_z" for t in MAG] + COVC)
    for c in carry:
        if c in comp.columns:
            P[c] = P["src"].map(comp[c])
    P = P.rename(columns={"id": "in_force_id"})
    # THIN-DOC editions (composite.apply_thin_doc_carry): their composite scores are carried from
    # the last full edition; the law-month re-scoring below must read the SAME source document's
    # per-topic values, not the thin doc's own (near-empty) row — score_src_id points there.
    P["score_src_id"] = P["scores_carried_from"].astype(str).where(
        P["scores_carried_from"].astype(str).str.strip().ne(""), P["in_force_id"])

    # wage: AS-OF (forward-fill) the (cao, calendar-year) wage row — a wage scale stays in force
    # until a new one is agreed, so carry the last known year forward. Fixes the ~half of panel
    # wage nulls that were merely gap-years (126/204 CAOs have internal year-gaps). (2026-07-07)
    mw = il.read_csv_safe(il.locate("mw_indices.csv"))
    WAGE_SRC = ["wage_median_z", "wage_mean_z", "mw_low", "mw_median", "mw_mean", "mw_high",
                "ratio_low_wml", "ratio_median_wml", "ratio_mean_wml", "ratio_high_wml", "wml_month"]
    mw["year"] = pd.to_numeric(mw["year"], errors="coerce")
    mws = mw.dropna(subset=["year"]).copy(); mws["cao_number"] = mws["cao_number"].astype(str)
    mws["year"] = mws["year"].astype("int64")            # align merge_asof key dtype (was float64
    mws = mws.sort_values("year")                        # after to_numeric -> crashes vs int64 left)
    tmp = pd.DataFrame({"cao_number": P["cao_number"].astype(str),
                        "year": (P["_ym"] // 12).astype("int64"),
                        "_row": range(len(P))}).sort_values("year")
    m = pd.merge_asof(tmp, mws[["cao_number", "year"] + WAGE_SRC], on="year",
                      by="cao_number", direction="backward")   # backward = forward-fill in time
    m = m.set_index("_row").sort_index()
    for c in WAGE_SRC:
        P[c] = m[c].values
    P["n_caos_in_month"] = P.groupby("month")["cao_number"].transform("size")

    # ---- LAW-MONTH RE-SCORING (2026-07-06): a statutory change hits EVERY in-force CAO
    # the month the law changes, not only when the CAO's next file appears. For the
    # statutory-anchored topics, the anchored fields are re-scored at the PANEL month's
    # era (same pooled params); unanchored fields keep their per-doc z. ----
    from datetime import date as _date
    from parental_leave_index import statutory_floor, fre_from_segments
    params = il.load_params()
    bounds = il.load_bounds()
    months = sorted(P["_ym"].unique())
    mid = {m: _date(m // 12, m % 12 + 1, 15) for m in months}

    ANCH = {  # topic -> (index csv, anchored (field, short, sign), unanchored signed-z cols)
        "absence": ("absence_index.csv", [
            ("leave_vacation_time_value", "vacation_days_yr", 1),
            ("leave_vacation_bonus_value", "vacation_bonus_pct", 1),
            ("leave_sickpay_continuation_value", "sickpay_pct", 1),
            ("leave_sickpay_duration_value", "sickpay_wks", 1),
            ("leave_short_term_care_value", "stcare_days_yr", 1),
            ("leave_short_term_care_pay_value", "stcare_pay_pct", 1),
            ("leave_long_term_care_value", "ltcare_wks", 1),
            ("leave_long_term_care_pay_value", "ltcare_pay_pct", 1)], []),
        "contract": ("contract_index.csv", [
            ("contract_ketenregeling_max_contracts_value", "keten_max_contracts", -1),
            ("contract_ketenregeling_max_duration_value", "keten_max_duration_mo", -1)],
            ["fulltime_hours_wk_z", "wh_adjust_tenure_mo_z"]),
        "overtime": ("overtime_index.csv", [
            ("overtime_min_rest_between_shifts_value", "min_rest_h", 1),
            ("overtime_max_hours_per_week_value", "max_hours_wk", -1),
            ("overtime_max_hours_per_day_value", "max_hours_day", -1)],
            ["ot_allowance_pct_z", "ot_allowance_max_pct_z", "shift_allowance_pct_z",
             "unfav_allowance_pct_z", "trigger_daily_h_z", "trigger_weekly_h_z"]),
    }
    for topic, (csvf, fields, other_z) in ANCH.items():
        d = il.read_csv_safe(il.locate(csvf)).set_index("id")
        parts = []
        for field, short, sign in fields:
            x = pd.to_numeric(P["score_src_id"].map(d[short]), errors="coerce")
            reg = {m: il.bound_for(bounds, field, mid[m]) for m in months}
            role = P["_ym"].map(lambda m: reg[m][0])
            val = pd.to_numeric(P["_ym"].map(lambda m: reg[m][1]), errors="coerce")
            fill = role.isin(["floor", "default", "floor_lift"]) & x.isna()
            x = x.where(~fill, val)
            lift = (role == "floor_lift") & x.notna() & (x < val)
            x = x.where(~lift, val)
            x = x.mask((role == "cap") & (x > val))
            parts.append(sign * il.pooled_z(x, params.get((field, "full"))))
        for zc in other_z:
            parts.append(pd.to_numeric(P["score_src_id"].map(d[zc]), errors="coerce"))
        P[f"{topic}_numeric_z"] = pd.concat(parts, axis=1).mean(axis=1).round(4)
    # leave: per-type z (paternity/adoption/parental), each re-scored against this
    # month's statutory floor for that type — matches the composite's per-type method
    # (parental_leave_index.py, 2026-07-07). Maternity excluded (no params: ~zero variance).
    lv = il.read_csv_safe(il.locate("parental_leave_index.csv")).set_index("id")
    LEAVE_TYPES = ("paternity", "adoption", "parental")
    type_z = []
    for t in LEAVE_TYPES:
        extr_t = pd.to_numeric(P["score_src_id"].map(lv[f"{t}_fre_extr"]), errors="coerce")
        stat_t = {m: fre_from_segments(statutory_floor(mid[m])[t]) for m in months}
        x_t = pd.concat([extr_t, P["_ym"].map(stat_t)], axis=1).max(axis=1)
        type_z.append(il.pooled_z(x_t, params.get((f"{t}_fre_stat", "full"))))
    P["leave_numeric_z"] = pd.concat(type_z, axis=1).mean(axis=1).round(4)

    # raw reference only — NOT fed into any z-score/composite computation
    fre_extr_total = pd.to_numeric(P["score_src_id"].map(lv["fre_total_extracted"]), errors="coerce")
    stat_fre_total = {m: sum(fre_from_segments(s) for s in statutory_floor(mid[m]).values()) for m in months}
    P["leave_fre_total_monthly"] = pd.concat([fre_extr_total, P["_ym"].map(stat_fre_total)], axis=1).max(axis=1).round(3)

    # ---- COMBINED roll-ups (2026-07-08) -----------------------------------------------------
    # PRIMARY overall_z / overall_z_var are the COMBINED (numeric+coverage) roll-ups, matching
    # composite.apply_scheme: overall_z = available-case mean of the 10 dual topics' combined
    # {t}_z + wage_median_z + safety/childcare coverage_z. The numeric-only companions get their
    # OWN matching variance (overall_numeric_z_var). Coverage z's don't change with the legal era,
    # so they're carried per in-force doc; the numeric part is the law-month re-scored z above.
    CBEXTRA = ["wage_median_z", "safety_coverage_z", "childcare_coverage_z"]
    COVZ = [f"{t}_coverage_z" for t in MAG] + ["safety_coverage_z", "childcare_coverage_z"]

    def combine(fr):
        for t in MAG:                                # combined {t}_z = mean(numeric_z, coverage_z)
            parts = [c for c in (f"{t}_numeric_z", f"{t}_coverage_z") if c in fr.columns]
            fr[f"{t}_z"] = fr[parts].apply(pd.to_numeric, errors="coerce").mean(axis=1).round(4)
        cb = [c for c in ([f"{t}_z" for t in MAG] + CBEXTRA) if c in fr.columns]
        CB = fr[cb].apply(pd.to_numeric, errors="coerce")
        fr["overall_z"] = CB.mean(axis=1).round(4)                                   # PRIMARY (combined)
        fr["overall_z_var"] = CB.var(axis=1, ddof=0).where(CB.notna().sum(axis=1) >= 2).round(4)
        zn = [c for c in ([f"{t}_numeric_z" for t in MAG] + ["wage_median_z"]) if c in fr.columns]
        ZN = fr[zn].apply(pd.to_numeric, errors="coerce")
        fr["overall_numeric_z"] = ZN.mean(axis=1).round(4)                           # numeric companion
        fr["overall_numeric_z_var"] = ZN.var(axis=1, ddof=0).where(ZN.notna().sum(axis=1) >= 2).round(4)
        fr["n_topics_scored"] = ZN.notna().sum(axis=1)

    for c in COVZ:                                    # graft coverage z from the in-force doc
        if c in comp.columns:
            P[c] = pd.to_numeric(P["src"].map(comp[c]), errors="coerce")
    combine(P)

    # STATUTORY pseudo-CAO rows (same yardstick by construction)
    st = il.read_csv_safe(il.locate("statutory_index.csv"))
    S = pd.DataFrame({"cao_number": "STATUTORY", "month": st["month"]})
    for t in MAG:                                    # full magnitude floor (extras=0, 2026-07-07)
        if f"{t}_numeric_z" in st.columns:
            S[f"{t}_numeric_z"] = st[f"{t}_numeric_z"]
    for c in COVZ:                                    # statutory coverage floor (share 0 -> deeply -z);
        if c in st.columns:                          # gives the statutory line a COMBINED score too,
            S[c] = pd.to_numeric(st[c], errors="coerce")   # so it no longer overstates the floor
    for wc in ("wage_median_z", "wage_mean_z"):
        if wc in st.columns:
            S[wc] = st[wc]                            # statutory = WML scored on the nominal wage pool
    combine(S)                                        # -> statutory overall_z / overall_z_var (combined)
    for cov in COVC:                                 # statutory holds every boolean False -> share 0
        S[cov] = 0.0
    S["coverage_overall"] = 0.0
    S["_ym"] = [int(m[:4]) * 12 + int(m[5:7]) - 1 for m in S["month"]]

    keep = (["cao_number", "month", "in_force_id", "thin_doc", "scores_carried_from", "score_src_id",
             "file_date", "ingangsdatum",
             "pub_lag_months", "months_since_file", "is_stale", "doc_is_newest",
             "n_caos_in_month", "overall_z", "overall_pctile", "overall_z_var", "n_topics_scored",
             "coverage_overall", "coverage_overall_pctile",
             "overall_numeric_z", "overall_numeric_z_var", "overall_numeric_pctile"]
            + [f"{t}_z" for t in MAG]                          # combined headline per dual topic
            + ZCOLS + ["wage_mean_z"] + COVZ + COVC +          # numeric z, coverage z, coverage shares
            ["mw_low", "mw_median", "mw_mean", "mw_high", "ratio_low_wml", "ratio_median_wml",
             "ratio_mean_wml", "ratio_high_wml", "wml_month", "leave_fre_total_monthly"])
    out = pd.concat([P, S], ignore_index=True)
    out = out[["_ym"] + [c for c in keep if c in out.columns]]
    out = out.sort_values(["cao_number", "_ym"]).drop(columns=["_ym"])
    p_out = os.path.join(il.OUT, "all_indices_panel_monthly" + suffix + ".csv")
    out.to_csv(p_out, sep=";", index=False)
    print(f"wrote {p_out} ({len(out)} rows: {len(P)} cao-months + {len(S)} statutory months), "
          f"window {out['month'].min()}..{out['month'].max()}")

    # yearly convenience slice: December (or the last available month of the final year)
    ye = out[out["month"].str.endswith("-12") | (out["month"] == out["month"].max())].copy()
    ye["year"] = ye["month"].str[:4]
    y_out = os.path.join(il.OUT, "all_indices_panel_yearly" + suffix + ".csv")
    ye.to_csv(y_out, sep=";", index=False)
    print(f"wrote {y_out} ({len(ye)} rows)")

    # per-month cross-CAO aggregates (STATUTORY excluded from the pool, appended as ref)
    cao_rows = out[out["cao_number"] != "STATUTORY"].copy()
    aggcols = ["overall_numeric_z", "coverage_overall"] + ZCOLS
    num = cao_rows[aggcols].apply(pd.to_numeric, errors="coerce")
    num["month"] = cao_rows["month"]
    gr = num.groupby("month")
    A = pd.DataFrame({"n_caos": gr["overall_numeric_z"].size()})
    for c in aggcols:
        A[f"{c}_mean"] = gr[c].mean().round(4)
        A[f"{c}_var"] = gr[c].var(ddof=0).round(4)
    q = gr["overall_numeric_z"].quantile([0.1, 0.5, 0.9]).unstack()
    A["overall_p10"] = q[0.1].round(4); A["overall_p50"] = q[0.5].round(4)
    A["overall_p90"] = q[0.9].round(4)
    stat_ref = pd.to_numeric(st.set_index("month")["overall_numeric_z"], errors="coerce")
    A["statutory_overall_z"] = stat_ref.reindex(A.index).round(4)
    A = A.reset_index().sort_values("month")
    a_out = os.path.join(il.OUT, "panel_aggregates_monthly" + suffix + ".csv")
    A.to_csv(a_out, sep=";", index=False)
    print(f"wrote {a_out} ({len(A)} months)")

    # sanity prints
    last = A.iloc[-1]
    print(f"\nlatest month {last['month']}: n={int(last['n_caos'])}, "
          f"overall mean {last['overall_numeric_z_mean']}, var {last['overall_numeric_z_var']}, "
          f"statutory {last['statutory_overall_z']}")
    dec = A[A["month"].str.endswith("-12")]
    print("December series (overall mean / var / statutory):")
    print(dec[["month", "n_caos", "overall_numeric_z_mean", "overall_numeric_z_var",
               "statutory_overall_z"]].tail(12).to_string(index=False))
    stale = cao_rows["is_stale"].astype(str).eq("True")
    print(f"\nis_stale (> {STALE_MONTHS} months): {int(stale.sum())}/{len(cao_rows)} cao-month rows "
          f"({stale.mean()*100:.1f}%)")


if __name__ == "__main__":
    main()   # file/kennisgeving axis (enforceable view). The first-file axis (axis="first":
             # term's first file starts at ingangsdatum) is no longer built by default
             # (tab retired 2026-07-15) — the AgreementLevel export carries the same rule as
             # the Date_first_is_Ingangsdatum column (+ Date_retro_datum for explicit-retro).
