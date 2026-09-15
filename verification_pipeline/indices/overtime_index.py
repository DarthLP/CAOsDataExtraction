"""
OVERTIME — v2: pooled z per file, file_date axis; two sub-scores kept.
  pay        : allowance %(+), unfavourable_allowance %(+), trigger_daily h(-), trigger_weekly h(-)
  protection : min_rest h(+, floor 11), max_hours_week h(-, cap 60), max_hours_day h(-, cap 12)
unfavourable_allowance %-bucket scores; EUR bucket descriptive. compulsory_annual EXCLUDED
(heterogeneous units). overtime_z = all 7 fields; sub-scores = subset means of the same
per-field pooled z's.
Run:  python3.13 indices/overtime_index.py
"""
import os, sys
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

PAY = [
    ("overtime_allowance_value",                  "percent", +1),
    ("overtime_allowance_range_max",              "percent", +1),  # Tier-3: top overtime tier
    ("overtime_shift_allowance_range_min",        "percent", +1),  # Tier-3: shift premium, base tier (C3)
    ("overtime_unfavourable_hours_allowance_value","percent", +1),
    ("overtime_trigger_daily_value",              "hours",   -1),
    ("overtime_trigger_weekly_value",             "hours",   -1),
]
PROT = [
    ("overtime_min_rest_between_shifts_value",    "hours",   +1),
    ("overtime_max_hours_per_week_value",         "hours",   -1),
    ("overtime_max_hours_per_day_value",          "hours",   -1),
]
FIELDS = PAY + PROT
# DISJOINT (2026-07-06): shift_allowance_present REMOVED from coverage — the shift-premium
# benefit is now carried by the zero-filled magnitude field shift_allowance_range_min. Only
# the derived comp_choice boolean remains as a presence-only coverage signal.
BOOLEANS = []   # + derived overtime_comp_choice below
SHORT = {
    "overtime_allowance_value": "ot_allowance_pct",
    "overtime_allowance_range_max": "ot_allowance_max_pct",
    "overtime_shift_allowance_range_min": "shift_allowance_pct",
    "overtime_unfavourable_hours_allowance_value": "unfav_allowance_pct",
    "overtime_trigger_daily_value": "trigger_daily_h",
    "overtime_trigger_weekly_value": "trigger_weekly_h",
    "overtime_min_rest_between_shifts_value": "min_rest_h",
    "overtime_max_hours_per_week_value": "max_hours_wk",
    "overtime_max_hours_per_day_value": "max_hours_day",
}
CLAMP = {
    "overtime_allowance_value": (0, 300),
    "overtime_allowance_range_max": (0, 300),
    "overtime_shift_allowance_range_min": (0, 100),
    "overtime_unfavourable_hours_allowance_value": (0, 300),
    "overtime_trigger_daily_value": (2, 16),
    "overtime_trigger_weekly_value": (20, 60),
    "overtime_min_rest_between_shifts_value": (8, 24),
    "overtime_max_hours_per_week_value": (24, 80),
    "overtime_max_hours_per_day_value": (4, 16),
}
TOPIC = "overtime"
OUT = os.path.join(il.OUT, f"{TOPIC}_index.csv"); DIAG = os.path.join(il.OUT, f"{TOPIC}_index_diagnostics.csv")


def main():
    bounds = il.load_bounds()
    df = il.load_full_cao(); df = il.add_newest(df)
    dmask = il.term_dedup_mask(df)
    print(f"{len(df)} full-CAO docs, {df['cao_number'].nunique()} CAOs, "
          f"yardstick {int(dmask.sum())} term-deduped docs")
    norm_cols = il.normalize_fields(df, FIELDS, CLAMP)
    # descriptive EUR bucket of the unfavourable-hours allowance (not scored)
    df["unfav_eur"] = [il.parse_float(v) if il._EUR.search((u or "").lower()) else None
                       for v, u in zip(df["overtime_unfavourable_hours_allowance_value"],
                                       df["overtime_unfavourable_hours_allowance_unit"])]
    # Tier-3: top shift-premium tier, descriptive (base tier is scored per C3)
    df["shift_allowance_max_pct"] = [il.normalize("percent", v, u) for v, u in
        zip(df["overtime_shift_allowance_range_max"], df["overtime_shift_allowance_range_unit"])]
    # Tier-3 dummy: worker's CHOICE between money and time-off (mode == 'both')
    df["overtime_comp_choice"] = df["overtime_compensation_mode"].astype(str).str.strip()\
        .eq("both").map({True: "True", False: "False"})
    BOOLS = BOOLEANS + ["overtime_comp_choice"]
    df = il.forward_fill(df, norm_cols)
    dmask = dmask.reindex(df.index)
    role_s, capv_s = il.role_caps(df, FIELDS, bounds)

    # shift-allowance zero-fill (2026-07-06): missing shift premium = 0 when the specific
    # boolean overtime_shift_allowance_present is False (genuine absence of a shift premium).
    df["overtime_numeric_z_availcase"] = il.magnitude_pooled(df, FIELDS, role_s, capv_s, "full", dmask)[0].round(4)
    il.apply_zerofill(df, [("overtime_shift_allowance_range_min", "overtime_shift_allowance_present")])

    params = []
    z_full, Z = il.magnitude_pooled(df, FIELDS, role_s, capv_s, "full", dmask, params)
    z_raw, _ = il.magnitude_pooled(df, FIELDS, role_s, capv_s, "raw", dmask, params)
    df["overtime_numeric_z"] = z_full.round(4); df["overtime_numeric_z_raw"] = z_raw.round(4)
    df["overtime_numeric_rankpct"] = il.pooled_pctile(z_full, dmask).round(4)
    fieldz = []
    for col, _, _ in FIELDS:
        fz = SHORT[col] + "_z"; df[fz] = Z[col].round(4); fieldz.append(fz)
    # sub-scores = available-case means over the SAME per-field pooled z's
    df["overtime_pay_z"] = Z[[c for c, _, _ in PAY]].mean(axis=1).round(4)
    df["overtime_protection_z"] = Z[[c for c, _, _ in PROT]].mean(axis=1).round(4)
    # equal-range percentile track (2026-07-07): overall + pay/protection sub-scores
    gen01, P = il.magnitude_pctile(df, FIELDS, role_s, capv_s, dmask)
    df["overtime_numeric_pctile"] = gen01.round(4)
    df["overtime_pay_prank"] = P[[c for c, _, _ in PAY]].mean(axis=1).round(4)
    df["overtime_protection_prank"] = P[[c for c, _, _ in PROT]].mean(axis=1).round(4)
    fieldpct = []
    for col, _, _ in FIELDS:
        fp = SHORT[col] + "_prank"; df[fp] = P[col].round(4); fieldpct.append(fp)
    il.save_params(TOPIC, params)

    cov, covn = il.coverage(df, BOOLS)
    df["overtime_coverage"] = cov.round(4); df["overtime_coverage_n"] = covn
    df["overtime_coverage_z"] = il.coverage_pooled_z(df["overtime_coverage"], dmask).round(4)
    df["overtime_coverage_pctile"] = il.pooled_pctile(df["overtime_coverage"], dmask).round(4)
    fullparts = pd.concat([il.field_variant("full", c, df, role_s, capv_s) for c, _, _ in FIELDS], axis=1)
    df["n_fields_pop"] = fullparts.notna().sum(axis=1); df["low_support"] = df["n_fields_pop"] < 2
    mw = df["overtime_max_hours_per_week_value__norm"]; md = df["overtime_max_hours_per_day_value__norm"]
    mr = df["overtime_min_rest_between_shifts_value__norm"]
    df["flag_overtime_anomaly"] = ((mw > 60) | (md > 12) | ((mr > 0) & (mr < 8))).fillna(False)

    inputs = {c + "__norm_ff": SHORT[c] for c, _, _ in FIELDS}
    out = df.rename(columns=inputs)
    keep = il.ID_COLS + list(inputs.values()) + ["unfav_eur", "shift_allowance_max_pct",
            "overtime_compensation_mode",
            "overtime_numeric_z", "overtime_numeric_z_availcase", "overtime_numeric_z_raw", "overtime_numeric_rankpct",
            "overtime_pay_z", "overtime_protection_z",
            "overtime_numeric_pctile", "overtime_pay_prank", "overtime_protection_prank"] + fieldz + fieldpct + \
           ["overtime_coverage", "overtime_coverage_z", "overtime_coverage_pctile", "overtime_coverage_n",
            "n_fields_pop", "low_support", "flag_overtime_anomaly"]
    out = out[[c for c in keep if c in out.columns]].sort_values(["cao_number", "file_date", "id"])
    out.to_csv(OUT, sep=";", index=False)
    print(f"wrote {OUT} ({len(out)} rows, {len(out.columns)} cols)")

    new = out[out["doc_is_newest"]]
    def m(s): return pd.to_numeric(s, errors="coerce")
    diag = pd.DataFrame({"metric": ["n_records", "n_caos", "n_newest", "share_low_support",
            "z_all_mean", "z_newest_mean", "pay_newest_mean", "protection_newest_mean",
            "corr_pay_protection", "corr_z_pctile", "flag_overtime_anomaly", "coverage_newest_mean"],
        "value": [len(out), out["cao_number"].nunique(), int(out["doc_is_newest"].sum()),
            round(out["low_support"].mean(), 3),
            round(m(out["overtime_numeric_z"]).mean(), 4), round(m(new["overtime_numeric_z"]).mean(), 4),
            round(m(new["overtime_pay_z"]).mean(), 4), round(m(new["overtime_protection_z"]).mean(), 4),
            round(m(new["overtime_pay_z"]).corr(m(new["overtime_protection_z"])), 3),
            round(m(out["overtime_numeric_z"]).corr(m(out["overtime_numeric_rankpct"])), 3),
            int(out["flag_overtime_anomaly"].sum()),
            round(m(new["overtime_coverage"]).mean(), 3)]})
    diag.to_csv(DIAG, sep=";", index=False); print(diag.to_string(index=False))


if __name__ == "__main__":
    main()
