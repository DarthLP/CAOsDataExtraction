"""
TERM — employment-security index (v2: pooled z per file, file_date axis).

Per full-CAO document: term_z = pooled, signed, available-case mean of
{employer_notice(+), probation_fixedterm(-), probation_indef(-),
 notice_min_floor(+), severance_extra months-of-salary(+)} pooled z's.
Severance EUR / % buckets surfaced descriptively. Coverage = generosity booleans.
Run:  python3.13 indices/term_index.py
"""
import os, sys
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

# field: (value_col, canonical_unit, sign)   employee_notice intentionally dropped
FIELDS = [
    ("term_employer_notice_value",     "months",        +1),
    ("term_employer_notice_range_max", "months",        +1),  # Tier-3: top-tenure notice tier
    ("term_probation_fixedterm_value", "months",        -1),
    ("term_probation_indef_value",     "months",        -1),
    ("term_notice_min_floor_value",    "months",        +1),
    ("term_severance_extra_value",     "months_salary", +1),
]
# Tier-2 2026-07-05. CAVEAT: sick_dismissal_prot partly RESTATES law (BW 7:670 = 104-week
# dismissal ban during illness, statutory_all informational row) — like the Arbowet caveat.
# probation_allowed / end_at_AOW_auto excluded: presence is anti-worker, wrong coverage sign.
BOOLEANS = ["term_notice_tenure_present", "term_sick_dismissal_prot", "term_severance_ww_supplement"]
SHORT = {
    "term_employer_notice_value": "employer_notice_mo",
    "term_employer_notice_range_max": "employer_notice_max_mo",
    "term_probation_fixedterm_value": "probation_fixedterm_mo",
    "term_probation_indef_value": "probation_indef_mo",
    "term_notice_min_floor_value": "notice_floor_mo",
    "term_severance_extra_value": "severance_mo",
}
CLAMP = {"term_severance_extra_value": (0, 60), "term_employer_notice_value": (0, 12),
         "term_employer_notice_range_max": (0, 12),
         "term_probation_fixedterm_value": (0, 6), "term_probation_indef_value": (0, 6),
         "term_notice_min_floor_value": (0, 12)}
TOPIC = "term"
OUT = os.path.join(il.OUT, f"{TOPIC}_index.csv"); DIAG = os.path.join(il.OUT, f"{TOPIC}_index_diagnostics.csv")


def main():
    bounds = il.load_bounds()
    df = il.load_full_cao(); df = il.add_newest(df)
    dmask = il.term_dedup_mask(df)
    print(f"{len(df)} full-CAO docs, {df['cao_number'].nunique()} CAOs, "
          f"yardstick {int(dmask.sum())} term-deduped docs")

    norm_cols = il.normalize_fields(df, FIELDS, CLAMP)
    # severance descriptive split (EUR / %)
    sev_split = [il.split_severance(il.parse_float(v), u)
                 for v, u in zip(df["term_severance_extra_value"], df["term_severance_extra_unit"])]
    df["term_severance_eur"] = [s["eur"] for s in sev_split]
    df["term_severance_pct"] = [s["pct"] for s in sev_split]

    df = il.forward_fill(df, norm_cols)
    dmask = dmask.reindex(df.index)
    role_s, capv_s = il.role_caps(df, FIELDS, bounds)

    # severance-extra zero-fill (Hanna 2026-07-06): a blank = no severance above the statutory
    # transitievergoeding = 0 (least generous). There is no "has extra severance" boolean, so
    # this is UNCONDITIONAL (boolean=None); a few extraction misses are accepted for the gain
    # of scoring the many CAOs that genuinely grant no top-up. Pre-fill score kept as
    # term_z_availcase.
    z_av, _ = il.magnitude_pooled(df, FIELDS, role_s, capv_s, "full", dmask)
    df["term_numeric_z_availcase"] = z_av.round(4)
    il.apply_zerofill(df, [("term_severance_extra_value", None)])

    params = []
    z_full, Z = il.magnitude_pooled(df, FIELDS, role_s, capv_s, "full", dmask, params)
    z_raw, _ = il.magnitude_pooled(df, FIELDS, role_s, capv_s, "raw", dmask, params)
    df["term_numeric_z"] = z_full.round(4); df["term_numeric_z_raw"] = z_raw.round(4)
    df["term_numeric_rankpct"] = il.pooled_pctile(z_full, dmask).round(4)
    fieldz = []
    for col, _, _ in FIELDS:
        fz = SHORT[col] + "_z"; df[fz] = Z[col].round(4); fieldz.append(fz)
    # equal-range percentile track (2026-07-07)
    gen01, P = il.magnitude_pctile(df, FIELDS, role_s, capv_s, dmask)
    df["term_numeric_pctile"] = gen01.round(4)
    fieldpct = []
    for col, _, _ in FIELDS:
        fp = SHORT[col] + "_prank"; df[fp] = P[col].round(4); fieldpct.append(fp)
    il.save_params(TOPIC, params)

    cov, covn = il.coverage(df, BOOLEANS)
    df["term_coverage"] = cov.round(4); df["term_coverage_n"] = covn
    df["term_coverage_z"] = il.coverage_pooled_z(df["term_coverage"], dmask).round(4)
    df["term_coverage_pctile"] = il.pooled_pctile(df["term_coverage"], dmask).round(4)
    fullparts = pd.concat([il.field_variant("full", c, df, role_s, capv_s) for c, _, _ in FIELDS], axis=1)
    df["n_fields_pop"] = fullparts.notna().sum(axis=1)
    df["low_support"] = df["n_fields_pop"] < 2
    over = pd.Series(False, index=df.index)
    for col in ["term_probation_fixedterm_value", "term_probation_indef_value"]:
        over = over | ((role_s[col] == "cap") & (df[col + "__norm"] > capv_s[col]))
    df["flag_probation_over_cap"] = over.fillna(False)

    inputs = {col + "__norm_ff": SHORT[col] for col, _, _ in FIELDS}
    out = df.rename(columns=inputs)
    keep = il.ID_COLS + list(inputs.values()) + ["term_severance_eur", "term_severance_pct",
            "term_dismissal_approval",
            "term_numeric_z", "term_numeric_z_availcase", "term_numeric_z_raw", "term_numeric_rankpct", "term_numeric_pctile"] + fieldz + fieldpct + \
           ["term_coverage", "term_coverage_z", "term_coverage_pctile", "term_coverage_n",
            "n_fields_pop", "low_support", "flag_probation_over_cap"]
    out = out[[c for c in keep if c in out.columns]].sort_values(["cao_number", "file_date", "id"])
    out.to_csv(OUT, sep=";", index=False)
    print(f"wrote {OUT}  ({len(out)} rows, {len(out.columns)} cols)")

    new = out[out["doc_is_newest"]]
    def m(s): return pd.to_numeric(s, errors="coerce")
    diag = pd.DataFrame({"metric": [
        "n_records", "n_caos", "n_newest", "share_low_support",
        "z_all_mean", "z_newest_mean", "z_newest_scored",
        "corr_z_raw", "corr_z_pctile", "flag_probation_over_cap", "coverage_newest_mean"],
        "value": [
        len(out), out["cao_number"].nunique(), int(out["doc_is_newest"].sum()),
        round(out["low_support"].mean(), 3),
        round(m(out["term_numeric_z"]).mean(), 4), round(m(new["term_numeric_z"]).mean(), 4),
        int(m(new["term_numeric_z"]).notna().sum()),
        round(m(out["term_numeric_z"]).corr(m(out["term_numeric_z_raw"])), 3),
        round(m(out["term_numeric_z"]).corr(m(out["term_numeric_rankpct"])), 3),
        int(out["flag_probation_over_cap"].sum()),
        round(m(new["term_coverage"]).mean(), 3)]})
    diag.to_csv(DIAG, sep=";", index=False)
    print(diag.to_string(index=False))


if __name__ == "__main__":
    main()
