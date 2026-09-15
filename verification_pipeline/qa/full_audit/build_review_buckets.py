"""Derive the human-review buckets from the consolidated per-topic actions.csv.

Reproducible: encodes every triage rule (clear-vs-audit, promotions, surcharge
normalization, statutory deferral) so re-running the consolidator never loses the
bucketing. Reads perfile_work/<topic>/actions.csv, writes ALL_actions.csv + the
review sheets, prints the summary.

SURFACE-ONLY. Run: python3.13 -m qa.full_audit.build_review_buckets
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
W = HERE / "perfile_work"
TOPICS = ["leave", "term", "overtime", "pension", "bonus", "fringe", "contract",
          "training", "safety", "wage", "homeoffice", "childcare"]

CONV = ["selection_rule", "dismissal_approval", "_note", "_rule_text", "_schedule",
        "_desc", "statutory_ref", "_exceptions", "_ref"]
STAT = ["paternity", "maternity", "adoption", "parental", "vacation_time", "sickpay",
        "short_term_care", "care_"]
SURCH = ["overtime_allowance", "unfavourable_hours_allowance", "shift_allowance", "overtime_stacking"]
DUR = ["paternity", "maternity", "adoption", "parental", "vacation"]
PROMOTE = {"overtime_stacking_rule", "childcare_childcare_support_present",
           "leave_sickpay_extra_insurance_present", "leave_parental_eligibility_present"}
INC = ["surcharge", "toeslag", "op het", "bovenop", "extra", "increment", "premie", "additional"]
TOT = ["of hourly", "of the hourly", "of wage", "of salary", "of base", "of the rate",
       "van het uur", "total"]


def _to_total(val, unit):
    v = common.to_float(val)
    if v is None:
        return None
    u = str(unit).lower()
    if any(k in u for k in INC):
        return 100.0 + v
    if any(k in u for k in TOT):
        return v
    return 100.0 + v if v < 100 else v


def _audit_reasons(r):
    out = []
    if r.source_stated != "True" and r.kind != "boolean":
        out.append("not_positively_stated")
    if any(s in r.field for s in CONV + STAT + SURCH):
        out.append("convention/statutory/surcharge")
    if r.kind == "numeric":
        dv, sv = common.to_float(r.dataset_value), common.to_float(r.source_value)
        if sv is None:
            out.append("source_not_numeric")
        else:
            dc, sc = common.unit_category(r.dataset_unit), common.unit_category(r.source_unit)
            if dc and sc and dc != sc:
                out.append("cross-unit-dimension")
            if dv and sv and min(abs(dv), abs(sv)) > 0 and max(abs(dv), abs(sv)) / min(abs(dv), abs(sv)) >= 10:
                out.append("magnitude>=10x")
    return out


def main():
    frames = [pd.read_csv(W / t / "actions.csv", sep=";", dtype=str, keep_default_na=False) for t in TOPICS]
    a = pd.concat(frames, ignore_index=True)
    a.to_csv(W / "ALL_actions.csv", sep=";", index=False)

    def wr(df, name):
        df.to_csv(W / name, sep=";", index=False)
        return len(df)

    # non-FIX buckets
    wr(a[a.proposed_action == "EMPTY"], "EMPTY_candidates.csv")
    wr(a[a.proposed_action == "REVIEW"], "REVIEW_needed.csv")
    wr(a[a.assessment == "UNSURE"], "UNSURE.csv")
    wr(a[a.assessment == "EXTRACTION_ERROR_SUSPECTED"], "EXTRACTION_ERROR_SUSPECTED.csv")
    wr(a[a.proposed_action == "KEEP"], "KEPT_silent_not_inherited.csv")

    # FIX partition
    fix = a[a.proposed_action == "FIX"].copy()
    defer_mask = (fix.kind == "numeric") & fix.field.apply(lambda f: any(s in f for s in DUR))
    defer = fix[defer_mask]

    def sur_equiv(r):
        if not any(r.field.startswith(s) for s in SURCH):
            return False
        if common.to_float(r.dataset_value) == 100 or common.to_float(r.source_value) == 100:
            return False
        dt, st = _to_total(r.dataset_value, r.dataset_unit), _to_total(r.source_value, r.source_unit)
        return dt is not None and st is not None and abs(dt - st) <= 0.05 * max(dt, st, 1)
    rest = fix[~defer_mask].copy()
    sur_mask = rest.apply(sur_equiv, axis=1)
    surn = rest[sur_mask]
    rest = rest[~sur_mask].copy()

    rest["audit_reason"] = rest.apply(lambda r: ";".join(_audit_reasons(r)), axis=1)
    clear_mask = (rest.audit_reason == "") | (rest.field.isin(PROMOTE) & (rest.quote.str.len() > 10))
    clear, audit = rest[clear_mask], rest[~clear_mask]

    wr(defer, "statutory_deferred.csv")
    wr(surn, "surcharge_normalize.csv")
    wr(clear, "FIX_clear.csv")
    wr(audit, "FIX_audit.csv")

    t = Counter(a.proposed_action)
    print(f"ALL_actions.csv = {len(a)} cells")
    for k in ["CONFIRM", "KEEP", "RECONCILE", "FIX", "EMPTY", "REVIEW"]:
        print(f"  {k:10s} {t[k]:6,}")
    print(f"\nFIX {len(fix)} split: CLEAR {len(clear)} | AUDIT {len(audit)} | "
          f"surcharge_normalize {len(surn)} | statutory_deferred {len(defer)}")
    print(f"KEEP (silent, sibling-differs, NOT inherited -> old value retained) = {t['KEEP']:,}")
    print(f"RECONCILE (remove value NO version supports) = {t['RECONCILE']:,}")
    print(f"\nYOUR MANUAL AUDIT = FIX_audit {len(audit)} + REVIEW {t['REVIEW']} = {len(audit)+t['REVIEW']:,}")


if __name__ == "__main__":
    main()
