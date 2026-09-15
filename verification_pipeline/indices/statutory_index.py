"""
STATUTORY INDEX — the law scored as if it were its own CAO file (v2 plan §6).

For each calendar month 1999-01 → today, build a pseudo-record holding the
era-correct statutory FLOOR/DEFAULT value per indexed field (caps and
informational rows are NOT a worker's package — left empty; nothing invented),
run it through the SAME unit canon, and score it with the PERSISTED pooled
parameters (scoring_params.csv) the CAO documents were scored with. Result: the
legal baseline on the same yardstick, so panel consumers can read the bargained
premium directly.

The statutory pseudo-CAO is a FULL FLOOR (2026-07-07): each field holds the legal
minimum where the law sets one (floor/default/floor_lift) AND 0 on every above-
statutory perk (EXTRA fields — the law grants nothing extra), and every coverage
boolean is False (share 0). So it ranks at/near the bottom each month, except where
the statutory minimum is genuinely decent (e.g. 2020s leave beats a 2000s CAO).

  leave    — statutory FRE-weeks per type (WAZO/WIEG/betaald ouderschapsverlof)
  absence  — vacation / holiday-allowance / sick-pay floors
  contract — ketenregeling defaults (3 contracts; 36→24→36 months)
  overtime — ATW rest/hours floors + shift-allowance perk at 0
  term/training/bonus/fringe/homeoffice — EXTRA perks at 0 (severance-extra, 13th-
             month, meal/relocation/commuting, stipend, training budget/days/reimb)
  wage     — WML vs the CAO wage floors (ratio_low_wml = 1.0 by definition)
  coverage — every topic's booleans False → coverage share 0, scored on each topic's
             coverage yardstick.
PENSION is excluded entirely (available-case-only hard rule: blanks are fund-
deferral, never a statutory baseline — no statutory pension score, by design).

Output: statutory_index.csv (one row per month).
Run: python3.13 indices/statutory_index.py
"""
import os, sys
from datetime import date
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
from parental_leave_index import statutory_floor, fre_from_segments

OUT = os.path.join(il.OUT, "statutory_index.csv")

# indexed fields per topic with their signs (mirror the drivers). A field yields a statutory
# value when its role is floor/default/floor_lift (the law's minimum) OR it is an EXTRA perk
# (baseline 0: the law grants nothing extra) — see STAT_ZERO_EXTRA. All other roles (cap,
# informational, open) are left empty. This makes the statutory pseudo-CAO a FULL floor:
# it holds the legal minimum where one exists and 0 on every above-statutory perk, so it
# ranks at/near the bottom each month (Hanna 2026-07-07). PENSION is excluded entirely
# (available-case-only hard rule — blanks are fund-deferral, never a statutory baseline).
TOPIC_FIELDS = {
    "term": [("term_employer_notice_value", +1), ("term_probation_fixedterm_value", -1),
             ("term_probation_indef_value", -1), ("term_notice_min_floor_value", +1),
             ("term_severance_extra_value", +1)],
    "contract": [("contract_ketenregeling_max_contracts_value", -1),
                 ("contract_ketenregeling_max_duration_value", -1),
                 ("contract_full_time_hours_value", -1),
                 ("contract_workhours_adjustment_tenure_requirement_value", -1)],
    "overtime": [("overtime_allowance_value", +1), ("overtime_allowance_range_max", +1),
                 ("overtime_shift_allowance_range_min", +1),
                 ("overtime_unfavourable_hours_allowance_value", +1),
                 ("overtime_trigger_daily_value", -1), ("overtime_trigger_weekly_value", -1),
                 ("overtime_min_rest_between_shifts_value", +1),
                 ("overtime_max_hours_per_week_value", -1), ("overtime_max_hours_per_day_value", -1)],
    "absence": [("leave_vacation_time_value", +1), ("leave_vacation_bonus_value", +1),
                ("leave_sickpay_continuation_value", +1), ("leave_sickpay_duration_value", +1),
                ("leave_short_term_care_value", +1), ("leave_short_term_care_pay_value", +1),
                ("leave_long_term_care_value", +1), ("leave_long_term_care_pay_value", +1)],
    "training": [("training_time_yearly_value", +1), ("training_budget_value", +1),
                 ("training_cost_reimbursement_value", +1)],
    "bonus": [("bonus_thirteenth_month_amt_value", +1), ("bonus_fixed_annual_lump_value", +1)],
    "fringe": [("fringe_commuting_allowance_value", +1), ("fringe_meal_benefit_amt_value", +1),
               ("fringe_relocation_allowance_value", +1)],
    "homeoffice": [("homeoffice_stipend_value", +1)],
}

# EXTRA perks — the law grants nothing above statutory, so the statutory pseudo-CAO scores 0
# (its floor). pension_retirement_age_early EXCLUDED (pension is available-case-only).
STAT_ZERO_EXTRA = {
    "term_severance_extra_value", "overtime_shift_allowance_range_min",
    "training_time_yearly_value", "training_budget_value", "training_cost_reimbursement_value",
    "bonus_thirteenth_month_amt_value", "bonus_fixed_annual_lump_value",
    "fringe_commuting_allowance_value", "fringe_meal_benefit_amt_value",
    "fringe_relocation_allowance_value", "homeoffice_stipend_value",
}

# coverage topics whose booleans the statutory pseudo-CAO holds ALL False (no bargained
# provision) -> coverage share 0 -> scored against that topic's coverage yardstick params.
# pension & overtime excluded (pension available-case-only; overtime has no coverage booleans).
STAT_COVERAGE_TOPICS = ["leave", "absence", "term", "contract", "training", "bonus", "fringe",
                        "homeoffice", "safety", "childcare", "ai"]   # absence added 2026-07-08


def _coverage_params(csvf, covcol):
    """Recompute a topic's coverage yardstick params (one doc per term_group, latest file) so
    the statutory row (coverage share = 0) can be scored on the same scale as the CAOs."""
    p = il.locate(csvf)
    if not os.path.exists(p):
        return None
    d = il.read_csv_safe(p)
    if covcol not in d.columns or "term_group" not in d.columns:
        return None
    d = d.dropna(subset=["term_group"]).reset_index(drop=True)
    # Yardstick = one doc per term_group, chosen by the SAME ordering as il.term_dedup_mask:
    # latest parsed file_date, then highest numeric id. (Was a string sort on file_date/id, which
    # mis-ordered e.g. id '10001' < '9989' and non-ISO dates.) The only residual vs the driver is
    # the _n_pop richness tiebreak, which can differ only among same-term same-date reprints.
    d["_date"] = d["file_date"].map(il.parse_date)
    d["_idnum"] = pd.to_numeric(d["id"], errors="coerce")
    idx = d.sort_values(["term_group", "_date", "_idnum"]).groupby("term_group").tail(1).index
    dmask = pd.Series(d.index.isin(idx), index=d.index)
    return il.pooled_params(pd.to_numeric(d[covcol], errors="coerce"), dmask)


def month_range(start=(1999, 1)):
    today = date.today()
    y, m = start
    while (y, m) <= (today.year, today.month):
        yield y, m
        m += 1
        if m == 13: y, m = y + 1, 1


def main():
    bounds = il.load_bounds()
    params = il.load_params()
    wml = il.load_wml()   # statutory wage = the WML in force, scored on the NOMINAL wage pool
    # coverage params per topic (statutory holds coverage share = 0 = all booleans False)
    COVP = {t: _coverage_params(f"{'parental_leave' if t=='leave' else t}_index.csv",
                                f"{t}_coverage") for t in STAT_COVERAGE_TOPICS}
    rows = []
    for y, m in month_range():
        d = date(y, m, 15)  # mid-month
        row = {"month": f"{y:04d}-{m:02d}"}
        zs = []
        # numeric-field topics: floor/default value at d (the legal minimum) OR 0 for EXTRA
        # perks (law grants nothing extra), scored with the pooled params
        for topic, fields in TOPIC_FIELDS.items():
            fz = []
            for field, sign in fields:
                role, val = il.bound_for(bounds, field, d)
                if role in ("floor", "default", "floor_lift") and val is not None:
                    v = val
                elif field in STAT_ZERO_EXTRA:
                    v = 0.0                          # the statutory floor for an above-stat perk
                else:
                    continue
                z = il.zscore_value(v, params.get((field, "full")))
                if z is not None:
                    fz.append(sign * z)
                    row[f"stat_{field}"] = v
            row[f"{topic}_numeric_z"] = round(sum(fz) / len(fz), 4) if fz else None
            if fz: zs.append(row[f"{topic}_numeric_z"])
        # coverage: statutory holds every boolean False -> share 0 -> scored on each topic's
        # coverage yardstick. (Not added to overall_numeric_z, which is magnitude; exposed for the
        # panel / combined so the statutory row is a full floor on breadth too.)
        for t in STAT_COVERAGE_TOPICS:
            zc = il.zscore_value(0.0, COVP.get(t))
            row[f"{t}_coverage_z"] = round(zc, 4) if zc is not None else None
        row["coverage_overall"] = 0.0
        # leave: era-correct statutory FRE, PER TYPE (2026-07-07) through the per-type leave
        # params, then averaged — matches the CAO leave index (equal weight per leave type).
        seg = statutory_floor(d)
        row["stat_leave_fre_weeks"] = round(sum(fre_from_segments(s) for s in seg.values()), 2)
        lz = [il.zscore_value(fre_from_segments(s), params.get((f"{t}_fre_stat", "full")))
              for t, s in seg.items()]
        lz = [v for v in lz if v is not None]
        row["leave_numeric_z"] = round(sum(lz) / len(lz), 4) if lz else None
        if lz: zs.append(row["leave_numeric_z"])
        # wage: the law scored as its own CAO — its whole scale sits at the WML in force this month,
        # scored on the NOMINAL wage pool (median & mean both = the WML EUR value).
        wml_eur = il.wml_at(wml, d)
        zw = il.zscore_value(wml_eur, params.get(("mw_median", "full"))) if wml_eur else None
        row["wage_median_z"] = round(zw, 4) if zw is not None else None
        if zw is not None: zs.append(row["wage_median_z"])
        zwm = il.zscore_value(wml_eur, params.get(("mw_mean", "full"))) if wml_eur else None
        row["wage_mean_z"] = round(zwm, 4) if zwm is not None else None
        row["overall_numeric_z"] = round(sum(zs) / len(zs), 4) if zs else None
        row["n_topics_scored"] = len(zs)
        rows.append(row)

    out = pd.DataFrame(rows)
    lead = ["month", "overall_numeric_z", "n_topics_scored",
            "leave_numeric_z", "absence_numeric_z", "contract_numeric_z", "overtime_numeric_z",
            "term_numeric_z", "wage_median_z"]
    out = out[[c for c in lead if c in out.columns] + [c for c in out.columns if c not in lead]]
    out.to_csv(OUT, sep=";", index=False)
    print(f"wrote {OUT} ({len(out)} months, {len(out.columns)} cols)")
    latest = out.iloc[-1]
    print("latest month:", latest["month"], "| statutory z per topic:",
          {t: latest.get(f"{t}_numeric_z", latest.get('wage_median_z') if t == 'wage' else None)
           for t in ["leave", "contract", "overtime", "wage"]},
          "| overall:", latest["overall_numeric_z"])
    _ck = ["leave_numeric_z", "absence_numeric_z", "contract_numeric_z", "overtime_numeric_z", "wage_median_z"]
    chg = out[["month"] + _ck].drop_duplicates(subset=_ck)
    print(f"{len(chg)} distinct statutory regimes across the window (change-points):")
    print(chg.to_string(index=False))


if __name__ == "__main__":
    main()
