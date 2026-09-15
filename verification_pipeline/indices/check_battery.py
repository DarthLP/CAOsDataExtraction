"""
CHECK BATTERY — three verification layers over the v2 indices (Hanna's double-check).

  1. CONVERTER SELF-TEST: hard asserts on every unit converter with known cases
     (incl. the contamination guards — a % in an hours field must return None).
  2. FIELD SANITY: per scored field, the converted distribution (p10/median/p90 in
     the canonical unit) vs a domain-expectation band -> OK / CHECK verdicts.
  3. SALARY UNIT-CLASS CHECK: median converted EUR/month per ORIGINAL unit class —
     if conversions are right, hourly-, weekly-, 4-week- and annual-converted
     medians must land near the monthly median, and all near ~1.2-1.8x WML.

Outputs: field_sanity_report.csv + prints. Run: python3.13 indices/check_battery.py
"""
import os, sys, re
import pandas as pd
import numpy as np
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import importlib

# ---- 1. converter self-test ------------------------------------------------
CASES = [
    ("months", 2, "months", 2), ("months", 6, "weeks", 6*12/52), ("months", 30, "days", 30/30.4),
    ("months", 21.7, "working days", 1.0), ("months", 1, "year", 12), ("months", 100, "% of salary", None),
    ("hours", 30, "minutes", 0.5), ("hours", 11, "hours", 11), ("hours", 2, "days", None),
    ("percent", 25, "%", 25), ("percent", 25, "", 25), ("percent", 500, "eur", None),
    ("days_per_year", 16, "hours per year", 2), ("days_per_year", 25, "days", 25),
    ("days_per_year", 2, "times weekly working hours", 10), ("days_per_year", 1, "months", 21.7),
    ("weeks", 6, "weeks", 6), ("weeks", 6, "times weekly working hours", 6),
    ("weeks", 12, "months", 52), ("weeks", 2, "years", 104), ("weeks", 10, "days", 2),
    ("eur", 500, "eur", 500), ("eur", 10, "% of salary", None),
    ("eur_per_month", 2, "eur per day", 43.4), ("eur_per_month", 2, "eur net per working day", 43.4),
    ("eur_per_month", 480, "eur per year", 40), ("eur_per_month", 30, "eur", None),
    ("eur_per_km", 0.21, "eur per km", 0.21), ("eur_per_km", 50, "eur per month", None),
    ("pct_annual", 1, "months", 100/12), ("pct_annual", 8.33, "%", 8.33),
    ("hours_per_week", 1976, "hours per year", 38), ("hours_per_week", 38, "hours", 38),
    ("months_salary", 26, "weeks", 6), ("months_salary", 3, "months salary", 3),
]
fails = 0
for canon, v, u, want in CASES:
    got = il.normalize(canon, v, u)
    ok = (got is None and want is None) or (got is not None and want is not None
                                            and abs(got - want) < 0.05)
    if not ok:
        print(f"  CONVERTER FAIL: normalize({canon!r}, {v}, {u!r}) = {got}, want {want}")
        fails += 1
print(f"1) converter self-test: {len(CASES) - fails}/{len(CASES)} passed"
      + ("  <-- FIX BEFORE TRUSTING" if fails else ""))

# ---- 2. field sanity vs domain expectations ---------------------------------
EXPECT = {  # field -> (median_lo, median_hi) in the canonical unit (NL domain knowledge)
    "leave_vacation_time_value": (22, 30), "leave_vacation_bonus_value": (7.5, 9),
    "leave_sickpay_continuation_value": (85, 100), "leave_sickpay_duration_value": (52, 110),
    "leave_short_term_care_value": (5, 12), "leave_short_term_care_pay_value": (70, 100),
    "leave_long_term_care_value": (3, 15), "leave_long_term_care_pay_value": (0, 60),
    "term_employer_notice_value": (1, 3), "term_employer_notice_range_max": (2, 6),
    "term_probation_fixedterm_value": (1, 2), "term_probation_indef_value": (1, 2),
    "term_notice_min_floor_value": (1, 3), "term_severance_extra_value": (0, 6),
    "contract_ketenregeling_max_contracts_value": (3, 3), "contract_ketenregeling_max_duration_value": (24, 48),
    "contract_full_time_hours_value": (36, 40), "contract_workhours_adjustment_tenure_requirement_value": (0, 26),
    "overtime_allowance_value": (25, 50), "overtime_allowance_range_max": (50, 100),
    "overtime_shift_allowance_range_min": (5, 25), "overtime_unfavourable_hours_allowance_value": (20, 60),
    "overtime_trigger_daily_value": (7.5, 9.5), "overtime_trigger_weekly_value": (36, 40),
    "overtime_min_rest_between_shifts_value": (10, 12), "overtime_max_hours_per_week_value": (45, 60),
    "overtime_max_hours_per_day_value": (9, 12),
    "training_time_yearly_value": (1, 6), "training_budget_value": (300, 2000),
    "training_cost_reimbursement_value": (75, 100),
    "bonus_thirteenth_month_amt_value": (7, 9), "bonus_fixed_annual_lump_value": (100, 1500),
    "fringe_commuting_allowance_value": (0.15, 0.25), "fringe_meal_benefit_amt_value": (3, 20),
    "fringe_relocation_allowance_value": (500, 10000),
    "homeoffice_stipend_value": (20, 60), "homeoffice_entitlement_value": (1, 4),
    "pension_employee_contrib_value": (2, 12), "pension_accrual_rate_value": (1.5, 2.0),
    "pension_franchise_value": (12000, 18000), "pension_retirement_age_early_value": (55, 65),
}
df = il.load_full_cao()
rows = []
for t in ["absence", "term", "contract", "overtime", "training", "bonus", "fringe",
          "homeoffice", "pension"]:
    m = importlib.import_module(f"{t}_index")
    clamp = getattr(m, "CLAMP", {})
    for col, canon, _ in m.FIELDS:
        conv = pd.to_numeric(pd.Series(
            [il.normalize(canon, v, u) for v, u in
             zip(df[col], df.get(il.unit_col(col), ""))], index=df.index), errors="coerce")
        if col in clamp:
            lo, hi = clamp[col]; conv = conv.where(conv.between(lo, hi))
        obs = conv.dropna()
        med = obs.median() if len(obs) else np.nan
        exp = EXPECT.get(col)
        verdict = "n/a"
        if exp and len(obs):
            verdict = "OK" if exp[0] <= med <= exp[1] else "CHECK"
        rows.append({"topic": t, "field": col, "canonical": canon, "n": len(obs),
                     "p10": round(obs.quantile(.1), 2) if len(obs) else None,
                     "median": round(med, 2) if len(obs) else None,
                     "p90": round(obs.quantile(.9), 2) if len(obs) else None,
                     "expected_median": f"{exp[0]}-{exp[1]}" if exp else "",
                     "verdict": verdict})
R = pd.DataFrame(rows)
R.to_csv(os.path.join(il.OUT, "field_sanity_report.csv"), sep=";", index=False)
bad = R[R.verdict == "CHECK"]
print(f"2) field sanity: {int((R.verdict=='OK').sum())} OK, {len(bad)} CHECK "
      f"of {len(R)} fields -> field_sanity_report.csv")
if len(bad):
    print(bad[["field", "n", "median", "expected_median"]].to_string(index=False))

# ---- 2b. TOTAL vs EXTRA semantics test --------------------------------------
from field_semantics import FIELD_SEMANTICS, IMPUTING_ROLES
bounds = il.load_bounds()
from datetime import date
ERAS = [date(2005, 6, 15), date(2016, 6, 15), date(2024, 6, 15)]   # sample the eras
sem_fail = []
sem_rows = []
scored_fields = {col for t in ["absence", "term", "contract", "overtime", "training",
                               "bonus", "fringe", "homeoffice", "pension"]
                 for col, _, _ in importlib.import_module(f"{t}_index").FIELDS}
for col in scored_fields:
    cls, note = FIELD_SEMANTICS.get(col, ("?", ""))
    roles = {r for frm, to, r, v, u in bounds.get(col, [])}
    imputes = bool(roles & IMPUTING_ROLES)
    ok = True; why = ""
    if cls == "EXTRA" and imputes:
        ok = False; why = f"EXTRA field but has imputing role {roles & IMPUTING_ROLES} -> would invent generosity"
    if cls == "TOTAL_HARD" and "floor_lift" not in roles:
        ok = False; why = f"TOTAL_HARD but no floor_lift (roles={roles}) -> below-floor not lifted"
    if cls == "TOTAL_SOFT" and not (roles & {"floor", "default", "cap"}):
        ok = False; why = f"TOTAL_SOFT but no floor/default/cap (roles={roles})"
    if not ok:
        sem_fail.append((col, why))
    sem_rows.append({"field": col, "class": cls, "roles": ",".join(sorted(roles)) or "(none)",
                     "imputes_blank": imputes, "ok": ok, "note": note})
pd.DataFrame(sem_rows).to_csv(os.path.join(il.OUT, "field_semantics_report.csv"), sep=";", index=False)
# additional: TOTAL_HARD 'full' variant must never sit below the floor
for t in ["absence"]:
    m = importlib.import_module(f"{t}_index")
    df2 = il.load_full_cao(); df2 = il.add_newest(df2)
    ncols = il.normalize_fields(df2, m.FIELDS, getattr(m, "CLAMP", {}))
    df2 = il.forward_fill(df2, ncols)
    rs, cs = il.role_caps(df2, m.FIELDS, bounds)
    for col, _, _ in m.FIELDS:
        if FIELD_SEMANTICS.get(col, ("",))[0] != "TOTAL_HARD":
            continue
        full = il.field_variant("full", col, df2, rs, cs).dropna()
        floorv = cs[col]
        below = (full < floorv.reindex(full.index)).sum()
        if below > 0:
            sem_fail.append((col, f"{below} 'full' values still below the floor after lift"))
print(f"2b) TOTAL/EXTRA semantics: {len(sem_rows)-len(sem_fail)}/{len(sem_rows)} fields consistent "
      f"-> field_semantics_report.csv" + ("  <-- FIX" if sem_fail else ""))
for col, why in sem_fail:
    print(f"    SEMANTIC FAIL: {col}: {why}")

# ---- 2c. per-field statutory-behavior table (blank / below / above) ----------
from datetime import date as _date
BEHAV = {
    "floor_lift": ("filled with floor", "LIFTED to floor", "kept"),
    "floor":      ("filled with floor", "kept (lawful deviation)", "kept"),
    "default":    ("filled with default", "kept (lawful deviation)", "kept"),
    "cap":        ("stays EMPTY (excluded)", "kept", "MASKED (illegal/error)"),
    "informational": ("stays EMPTY (excluded)", "kept", "kept"),
    None:         ("stays EMPTY (excluded)", "kept", "kept"),
}
brows = []
for t in ["absence", "term", "contract", "overtime", "training", "bonus", "fringe", "homeoffice", "pension"]:
    for col, canon, sign in importlib.import_module(f"{t}_index").FIELDS:
        cls = FIELD_SEMANTICS.get(col, ("?", ""))[0]
        r, v = il.bound_for(bounds, col, _date(2024, 6, 15))
        blank, below, above = BEHAV.get(r, BEHAV[None])
        brows.append({"topic": t, "field": col, "class": cls, "canonical": canon, "sign": sign,
                      "statutory_role": r or "none", "statutory_value": v,
                      "if_BLANK": blank, "if_BELOW": below, "if_ABOVE": above})
bdf = pd.DataFrame(brows)
bdf.to_csv(os.path.join(il.OUT, "field_statutory_behavior.csv"), sep=";", index=False)
nfill = bdf["if_BLANK"].str.startswith("filled").sum()
print(f"2c) statutory-behavior table -> field_statutory_behavior.csv "
      f"({nfill}/{len(bdf)} fields fill blanks with a statutory value; {len(bdf)-nfill} leave blanks empty)")

# ---- 2d. SIGN audit: does each field enter the z in the worker-generous direction? ----
SIGN_EXPECT = {  # +1 = a HIGHER value is MORE worker-generous; -1 = higher is LESS
    "leave_vacation_time_value": +1, "leave_vacation_bonus_value": +1,
    "leave_sickpay_continuation_value": +1, "leave_sickpay_duration_value": +1,
    "leave_short_term_care_value": +1, "leave_short_term_care_pay_value": +1,
    "leave_long_term_care_value": +1, "leave_long_term_care_pay_value": +1,
    "term_employer_notice_value": +1, "term_employer_notice_range_max": +1,
    "term_probation_fixedterm_value": -1, "term_probation_indef_value": -1,   # longer probation = worse
    "term_notice_min_floor_value": +1, "term_severance_extra_value": +1,
    "contract_ketenregeling_max_contracts_value": -1,   # more temp contracts before perm = worse
    "contract_ketenregeling_max_duration_value": -1,    # longer temp chain = worse
    "contract_full_time_hours_value": -1,               # more hours for full-time = worse
    "contract_workhours_adjustment_tenure_requirement_value": -1,  # longer wait to request = worse
    "overtime_allowance_value": +1, "overtime_allowance_range_max": +1,
    "overtime_shift_allowance_range_min": +1, "overtime_unfavourable_hours_allowance_value": +1,
    "overtime_trigger_daily_value": -1, "overtime_trigger_weekly_value": -1,   # higher trigger = worse
    "overtime_min_rest_between_shifts_value": +1,
    "overtime_max_hours_per_week_value": -1, "overtime_max_hours_per_day_value": -1,
    "training_time_yearly_value": +1, "training_budget_value": +1, "training_cost_reimbursement_value": +1,
    "bonus_thirteenth_month_amt_value": +1, "bonus_fixed_annual_lump_value": +1,
    "fringe_commuting_allowance_value": +1, "fringe_meal_benefit_amt_value": +1,
    "fringe_relocation_allowance_value": +1,
    "homeoffice_stipend_value": +1, "homeoffice_entitlement_value": +1,
    "pension_employee_contrib_value": -1,   # higher employee share = worse
    "pension_accrual_rate_value": +1,
    "pension_franchise_value": -1,          # higher franchise = less pensionable pay = worse
    "pension_retirement_age_early_value": -1,  # higher early-retirement age = retire later = worse
}
sign_fail = []
n_sign = 0
for t in ["absence", "term", "contract", "overtime", "training", "bonus", "fringe", "homeoffice", "pension"]:
    for col, canon, sign in importlib.import_module(f"{t}_index").FIELDS:
        n_sign += 1
        exp = SIGN_EXPECT.get(col)
        if exp is None:
            sign_fail.append((col, "no expected sign registered")); continue
        if sign != exp:
            sign_fail.append((col, f"sign {sign:+d} but expected {exp:+d}"))
print(f"2d) sign audit: {n_sign-len(sign_fail)}/{n_sign} fields enter in the correct generosity direction"
      + ("  <-- FIX" if sign_fail else ""))
for col, why in sign_fail:
    print(f"    SIGN FAIL: {col}: {why}")

# ---- 3. salary unit-class check ---------------------------------------------
try:
    from repo_paths import SALARY_PARSER_CSV
except ImportError:
    from pathlib import Path
    sys.path.insert(0, str(Path(HERE).parent))
    from repo_paths import SALARY_PARSER_CSV

SAL = str(SALARY_PARSER_CSV)
use = ["confidence_tier", "ft_hours"] + [f"salary_{n}_{p}" for n in (1, 2, 3)
       for p in ("amount", "unit", "hours_basis_ft_week", "start_date")]
s = pd.read_csv(SAL, sep=";", dtype=str, usecols=lambda c: c in use)
s = s[s["confidence_tier"].isin(["A", "B"])]
recs = []
for n in (1, 2, 3):
    a = s[f"salary_{n}_amount"].map(il.parse_float)
    hb = s[f"salary_{n}_hours_basis_ft_week"].map(il.parse_float)
    ft = s["ft_hours"].map(il.parse_float)
    hrs = hb.where(hb.between(10, 60)).fillna(ft.where(ft.between(10, 60)))
    u = s[f"salary_{n}_unit"].fillna("")
    conv = pd.Series(np.nan, index=s.index)
    conv = conv.mask(u.eq("monthly"), a)
    conv = conv.mask(u.eq("weekly"), a * 52 / 12)
    conv = conv.mask(u.eq("4-week"), a * 13 / 12)
    conv = conv.mask(u.eq("annual"), a / 12)
    conv = conv.mask(u.eq("hourly") & hrs.notna(), a * hrs * 52 / 12)
    recs.append(pd.DataFrame({"unit": u, "eur_month": conv}))
allp = pd.concat(recs)
allp = allp[allp["eur_month"].between(400, 25000)]
print("3) salary unit-class check (median converted EUR/month per ORIGINAL unit; "
      "should all be same order, ~1.2-1.8x WML):")
print(allp.groupby("unit")["eur_month"].agg(["median", "count"]).round(0).to_string())

# ── 4) percentile track sanity (equal-range gen01 / *_prank / *_pctile) ──────────
# Added 2026-07-07. Verify every RANK column (_prank per-field, _pctile, _gen01) is bounded
# [0,1] (or NaN), and that the equal-range gen01 agrees in DIRECTION with the z it parallels
# (Spearman > 0 per topic). Value columns ending in _pct (e.g. cost_reimb_pct) are NOT ranks
# and are excluded by the suffix filter.
from scipy.stats import spearmanr
_PCT_TOPICS = ["parental_leave", "absence", "term", "contract", "overtime", "training",
               "bonus", "fringe", "homeoffice", "pension"]
oob, dir_fail, checked = [], [], 0
for _t in _PCT_TOPICS:
    _p = il.locate(f"{_t}_index.csv")
    if not os.path.exists(_p):
        continue
    _d = il.read_csv_safe(_p)
    _key = "leave" if _t == "parental_leave" else _t
    for _c in [c for c in _d.columns if c.endswith(("_prank", "_pctile", "_rankpct"))]:
        _v = pd.to_numeric(_d[_c], errors="coerce").dropna()
        if len(_v) and (_v.min() < -1e-9 or _v.max() > 1 + 1e-9):
            oob.append(f"{_t}.{_c} range [{_v.min():.3f},{_v.max():.3f}]")
    _g = pd.to_numeric(_d.get(f"{_key}_numeric_pctile"), errors="coerce")
    _z = pd.to_numeric(_d.get(f"{_key}_numeric_z"), errors="coerce")
    if _g is not None and _z is not None and _g.notna().sum() > 20:
        checked += 1
        _rho = spearmanr(_g, _z, nan_policy="omit").correlation
        if not (_rho is not None and _rho > 0):
            dir_fail.append(f"{_t}: rho(gen01,z)={_rho}")
print(f"4) percentile-track sanity: {checked} topics with gen01, "
      f"{len(oob)} out-of-[0,1], {len(dir_fail)} direction-mismatch vs z"
      + (" -> ALL OK" if not oob and not dir_fail else ""))
for _m in oob + dir_fail:
    print(f"    CHECK: {_m}")

# ---- 5) statutory-fingerprint restatement screen (time-aware; diagnostic) ----
# Flags cells whose value equals a statutory constant valid at the document's date while a
# companion boolean claims a CAO enhancement. Suspects are a REVIEW QUEUE, not errors:
# rows already agent-adjudicated by the jump campaign are marked; NEVER_CHECKED = open.
try:
    import statutory_fingerprints as _sf
    _n, _by, _sp, _open = _sf.run()
    print(f"5) statutory-fingerprint screen: {_n} suspects ({_open} never agent-checked) "
          f"-> statutory_fingerprint_suspects.csv")
except Exception as _e:
    print(f"5) statutory-fingerprint screen: FAILED ({_e})")

# ---- 6) same-term coverage vs adjudication ledger ----
# The jump campaign drove this to a fixed point (0 uncovered). Any future edit layer that
# reintroduces uncovered sibling-diff sides must get its own ripple wave.
try:
    import same_term_coverage_check as _stc
    _sides, _unc = _stc.run()
    print(f"6) same-term coverage: {_sides} diff-cell sides, {len(_unc)} uncovered"
          + (" -> ALL COVERED" if not _unc else " !! NEEDS A RIPPLE WAVE (see same_term_coverage_check.py --list)"))
except Exception as _e:
    print(f"6) same-term coverage: FAILED ({_e})")
