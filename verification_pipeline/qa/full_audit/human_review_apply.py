"""Apply the per-case decisions for the 61 human-review items under the corrected
policy (explicit number -> set; statutory/silent/converted/wrong-slot -> empty;
genuine boolean judgment -> leave for Hanna). Writes human_review_recommendations.csv
(all 61 + rationale) and applies SET/EMPTY to proposed_corrected_dataset.csv +
apply_changelog.csv. KEEP = confirm (no change). HANNA = left as-is.

Run: python3.13 -m qa.full_audit.human_review_apply
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
PROPOSED = HERE / "proposed_corrected_dataset.csv"
CHANGELOG = HERE / "apply_changelog.csv"
REC = HERE / "human_review_recommendations.csv"

# (record_id, stem) -> (action, value, unit, note)
# action: SET | EMPTY | KEEP | HANNA
D = {
    # ---- SET (explicit source number / definition-resolved) ----
    ("171007", "leave_paid_paternity"): ("SET", "2.0", "days", "rose to 5 days from 2019-01-01 (WIEG) mid-term; keep start value, note the change"),
    ("1496008", "pension_premium_total"): ("SET", "12.65", "percent", "11% was EMPLOYER-only (2009); 12.65% is the only stated TOTAL (2010)"),
    ("1022013", "leave_paternity_explicitly_above_statutory"): ("SET", "False", "", "5 days = statutory WIEG minimum (1 week), not above statutory"),
    ("1193012", "leave_short_term_care_pay"): ("SET", "95.0", "percent", "tiered 95%(first 3d)/70%(next 5d); first-tier per dataset convention"),
    ("3854012", "contract_ketenregeling_max_duration"): ("SET", "3.0", "years", "source: 'three years'"),
    ("3821003", "leave_vacation_time"): ("SET", "28.0", "days per year", "source states 28 days (=205.2h); definition example uses days"),
    # ---- KEEP (confirm current; explicit / ingangsdatum-resolved) ----
    ("2674010", "leave_sickpay_continuation"): ("KEEP", "", "", "first-period 100% (6wk) per convention"),
    ("3768008", "leave_vacation_time"): ("KEEP", "", "", "25 days = value at ingangsdatum 2022 (pre-2023)"),
    ("3821002", "fringe_commuting_allowance"): ("KEEP", "", "", "EUR0.21 = ingangsdatum-2023 rate (0.22 only 2024 tail)"),
    ("1264001", "general_start_date"): ("KEEP", "", "", "2012 = ingangsdatum"),
    ("1494012", "bonus_fixed_annual_lump"): ("KEEP", "", "", "EUR1475 = value at ingangsdatum May 2023"),
    ("475007", "contract_full_time_hours"): ("KEEP", "", "", "1930 hrs/yr explicitly stated (=38h/wk)"),
    ("2674006", "overtime_selection_rule"): ("KEEP", "", "", "unspecified defensible"),
    ("3854006", "overtime_selection_rule"): ("KEEP", "", "", "unspecified defensible"),
    ("50009", "overtime_selection_rule"): ("KEEP", "", "", "unspecified defensible"),
    ("50010", "overtime_selection_rule"): ("KEEP", "", "", "unspecified defensible"),
    # ---- HANNA (genuine boolean judgment) ----
    ("1029012", "overtime_shift_allowance_present"): ("HANNA", "", "", "lean False (irregular-hours surcharge, not a rotating-shift allowance) — confirm"),
    ("1497002", "overtime_shift_allowance_present"): ("HANNA", "", "", "lean False (availability allowance, not a shift allowance) — confirm"),
}
# Everything else among the 61 -> EMPTY (policy: silent/statutory/converted/wrong-slot)
EMPTY_NOTE = {
    "leave_vacation_bonus": "no holiday-allowance % stated (8% was a statutory assumption)",
    "leave_paid_maternity": "duration only 'statutory'/component; no explicit full-maternity number",
    "leave_adoption_pay": "source gives a duration, no pay %",
    "leave_paid_paternity": "no explicit paternity duration (only calamity/childbirth-day)",
    "leave_parental_topup_pay": "scale-tiered 90%..50%, no single rate",
    "leave_parental_unpaid": "13x working week is PARTLY-PAID, not unpaid",
    "leave_short_term_care": "value is specialist-only; general gets palliative only",
    "leave_short_term_care_pay": "no pay rate stated for standard short-term care",
    "leave_sickpay_continuation": "no sick-pay section in source",
    "leave_sickpay_duration": "no fixed upper bound stated",
    "leave_vacation_time": "full entitlement depends on work-week hours; 144 is 36h-only",
    "overtime_allowance": "TOIL/matrix not a single % surcharge in source",
    "overtime_compulsory_annual": "source gives quarterly/weekend-days, not annual hours",
    "overtime_max_hours_per_day": "source gives night-shift/young-worker max, not a daily max",
    "overtime_max_hours_per_week": "figure not stated in source",
    "overtime_min_rest_between_shifts": "only post-night-series / ATW reference, no standard minimum",
    "overtime_trigger_daily": "no fixed daily threshold (varies per employee)",
    "overtime_unfavourable_hours_allowance": "value is a different (HDS/standby) rate, not unfavourable-hours",
    "training_budget": "EUR7500 is a specific work-to-work cap, not an annual budget",
    "training_time_yearly": "source gives a weekly maximum ('up to X/week'), not an annual figure",
    "contract_workhours_adjustment_tenure_requirement": "not stated in source (out-of-scope extract)",
    "fringe_meal_benefit_amt": "shift-length-tiered amounts, no single meal value",
    "fringe_relocation_allowance": "source gives a % cap, field wants EUR",
    "contract_minmax_hours": "15-hr is a 7:628 exemption threshold, not a contract range max",
}


def _setcell(byid, changes, rid, col, val, info):
    rec = byid.get(rid)
    if rec is None or col not in rec:
        return
    old = str(rec.get(col, "")).strip()
    if old == str(val).strip():
        return
    rec[col] = str(val).strip()
    changes.append((rid, col, "human_review", old, str(val).strip(), "applied"))


def main():
    info = common.classify_columns()
    pf = pd.read_csv(PROPOSED, sep=";", dtype=str, keep_default_na=False)
    order, cols = list(pf["id"]), list(pf.columns)
    byid = {r["id"]: dict(r) for _, r in pf.iterrows()}
    items = pd.read_csv(HERE / "human_review_items.csv", sep=";", dtype=str, keep_default_na=False)

    changes, recos = [], []
    n = {"SET": 0, "EMPTY": 0, "KEEP": 0, "HANNA": 0}
    for _, it in items.iterrows():
        rid, stem = it["record_id"], it["field_stem"]
        action, val, unit, note = D.get((rid, stem),
                                        ("EMPTY", "", "", EMPTY_NOTE.get(stem, "source silent / not explicit -> empty")))
        # resolve target columns
        vcol = stem + "_value" if (stem + "_value") in info else (stem if stem in info else None)
        ucol = stem + "_unit" if (stem + "_unit") in info else None
        rmin = stem + "_range_min" if (stem + "_range_min") in info else None
        rmax = stem + "_range_max" if (stem + "_range_max") in info else None
        runit = stem + "_range_unit" if (stem + "_range_unit") in info else None

        if action == "SET":
            if rmin or rmax:                       # range field (e.g. pension_premium_total)
                _setcell(byid, changes, rid, rmin, val, info)
                _setcell(byid, changes, rid, rmax, val, info)
                if unit and runit:
                    _setcell(byid, changes, rid, runit, unit, info)
            else:
                _setcell(byid, changes, rid, vcol, val, info)
                if unit and ucol:
                    _setcell(byid, changes, rid, ucol, unit, info)
        elif action == "EMPTY":
            for c in (vcol, ucol, rmin, rmax, runit):
                if c:
                    _setcell(byid, changes, rid, c, "", info)
        # KEEP / HANNA -> no cell change
        n[action] += 1
        recos.append({"record_id": rid, "field_stem": stem, "action": action,
                      "set_value": val, "set_unit": unit,
                      "current_in_proposed": it["current_value_in_proposed"], "note": note})

    pd.DataFrame([byid[i] for i in order], columns=cols).to_csv(PROPOSED, sep=";", index=False)
    add = pd.DataFrame(changes, columns=["record_id", "target_field", "route", "flagged_current", "new_value", "status"])
    cl = pd.read_csv(CHANGELOG, sep=";", dtype=str, keep_default_na=False)
    for c in cl.columns:
        if c not in add.columns:
            add[c] = ""
    pd.concat([cl, add[cl.columns]], ignore_index=True).to_csv(CHANGELOG, sep=";", index=False)
    pd.DataFrame(recos).to_csv(REC, sep=";", index=False)

    print(f"[human review apply] 61 items: SET={n['SET']} EMPTY={n['EMPTY']} KEEP={n['KEEP']} HANNA={n['HANNA']}")
    print(f"  cells changed this pass: {len(changes)}  -> recommendations: {REC.name}")


if __name__ == "__main__":
    main()
