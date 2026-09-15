"""consolidate_review.py — Build ONE master review file of every outstanding
Hanna item across all 12 topics. Reads (read-only) each topic's outputs +
the second-pass / verification / era / spelled-out artifacts, normalizes them
into a common schema, and writes qa/MASTER_REVIEW_FOR_HANNA.csv.

DESIGN (the important part):
  * `current_value` is ALWAYS the value live in the dataset today (read from
    scoped_records.csv) — NOT the pipeline's proposed correction.
  * value and unit live in SEPARATE columns (`<x>_value` / `<x>_unit`). A unit
    finding is routed to the `_unit` column, a value to the `_value` column;
    numbers are never left inside a `_unit` field.
  * PAIRING (Hanna's rule): a value is never added without its unit and a unit is
    never added without its value. A post-pass surfaces the missing half (from the
    accepted clean-win, or flags it) so every value↔unit pair is complete.
  * Each row has an explicit ACTION:
      ADD_VALUE / CHANGE_VALUE / ADD_UNIT / CLEAR_VALUE / VERIFY_DIGIT / REVIEW.
  * DECISIONS{} records Hanna's resolutions of DECIDE items (e.g. "store the total
    rate, make it clear in the unit") so a judgment call becomes a concrete change.
  * Clean-wins the full-text re-check REJECTED are silently excluded from the
    apply-set (no "do not apply" rows); live data for them is already correct.

Categories: FIX (P1/P2) · SUGGEST (P3) · DECIDE (P2). Nothing is auto-applied.
Output is UTF-8 with BOM so Excel renders the em-dashes correctly.
"""
from __future__ import annotations
import os
import re
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
TOPICS = ["overtime", "homeoffice", "training", "contract", "bonus", "fringe",
          "safety", "childcare", "ai", "wage", "term", "pension"]
RECHECK_TOPICS = {"term", "bonus", "wage", "pension"}
_BLANK = {"", "nan", "none", "null", "n/a"}

# ── Hanna's DECIDE resolutions (judgment calls turned into concrete changes) ──
# key = (topic, record_id, value_field) -> {value, unit, reason}.  Emitted up front
# as a value row + companion unit row; the field is then skipped by every source.
def _D(value, unit, reason):
    return dict(value=value, unit=unit, reason=reason)


DECISIONS = {
    ("overtime", "50012", "overtime_unfavourable_hours_allowance_value"): _D(
        "150", "% of hourly wage (TOTAL pay incl. base, not a surcharge delta)",
        "Resolved (Hanna): source table states Sunday = 150% as TOTAL compensation; "
        "store 150 and make the unit say it is the total rate."),
    # training cost reimbursement: source says costs fully borne by employer, no % → 100% of costs
    ("training", "233017", "training_cost_reimbursement_value"): _D(
        "100", "% of costs", "Resolved (Hanna): source states costs fully borne by employer, no explicit % → 100% of costs."),
    ("training", "35018", "training_cost_reimbursement_value"): _D(
        "100", "% of costs", "Resolved (Hanna): courses fully at employer's expense, no explicit % → 100% of costs."),
    ("training", "51017", "training_cost_reimbursement_value"): _D(
        "100", "% of costs", "Resolved (Hanna): study costs reimbursed in full by sector, no explicit % → 100% of costs."),
    ("training", "518006", "training_cost_reimbursement_value"): _D(
        "100", "% of costs", "Resolved (Hanna): mandatory training fully borne by employer, no explicit % → 100% of costs."),
    # fringe relocation: source confirms 12% of annual salary
    ("fringe", "214021", "fringe_relocation_allowance_value"): _D(
        "12", "% of annual salary", "Resolved (Hanna): source states a 12%-of-annual-salary relocation cost."),
    # term probation (233017): explicit fixed-term cap
    ("term", "233017", "term_probation_fixedterm_value"): _D(
        "2", "months", "Resolved (Hanna): CAO fixed-term probation cap = 2 months (contracts ≥2yr; 1 month for <2yr)."),
    # term VERIFY_DIGIT (read from source): record the base notice tier / probation cap + months
    ("term", "725023", "term_employer_notice_value"): _D(
        "2", "months", "From source: age-graded employer notice 2/<45, 3/45-54, 4/55+; base tier = 2 months."),
    ("term", "727036", "term_employer_notice_value"): _D(
        "1", "months", "From source: tenure-graded employer notice 1/2/3/4 months; base tier = 1 month (<5yr)."),
    ("term", "609002", "term_employer_notice_value"): _D(
        "1", "months", "From source: tenure-graded employer notice 1/2/3/4 months; base tier = 1 month (<5yr)."),
    ("term", "1618009", "term_employer_notice_value"): _D(
        "1", "months", "From source: tenure-graded notice 1/2/3 months (<6mo / 6-12mo / ≥12mo); base = 1 month; same schedule for employer & employee."),
    ("term", "1618009", "term_employee_notice_value"): _D(
        "1", "months", "From source: same 1/2/3-month tenure-graded schedule as employer; base = 1 month."),
    ("term", "163011", "term_probation_fixedterm_value"): _D(
        "2", "months", "From source: fixed-term probation cap up to 2 months (contracts ≥2yr; 1 month for <2yr)."),
    # round-2 DECIDE rulings (Hanna)
    ("homeoffice", "243025", "homeoffice_discretion"): _D(
        "employee_request", "", "Resolved (Hanna): employee request under Wet flexibel werken + bilateral agreement → canonical 'employee_request'."),
    ("homeoffice", "730012", "homeoffice_discretion"): _D(
        "other", "", "Resolved (Hanna): mutual tailor-made employer-employee arrangement; not OR-based or employee-initiated → canonical 'other'."),
    ("training", "301025", "training_cost_reimbursement_value"): _D(
        "1.5", "% of salary", "Resolved (Hanna): record the 1.5%-of-salary Career Budget accrual, not full-cost coverage."),
    ("training", "924016", "training_career_scan_freq_value"): _D(
        "1", "times per year", "Resolved (Hanna): the annual career/development discussion counts as a career scan → 1×/year."),
    # compound thirteenth-month amount: keep the % as the value AND preserve the +EUR 400 in the unit
    ("bonus", "163011", "bonus_thirteenth_month_amt_value"): _D(
        "1.15", "% of functional wage and allowances + EUR 400 fixed gross",
        "Resolved (Hanna): compound amount — store 1.15 as the % value and keep '+ EUR 400 fixed gross' in the unit so the fixed component is not lost."),
}

# Fields to LEAVE AS-IS (no row): a proposed change Hanna declined.
DROP = {
    ("overtime", "157017", "overtime_guaranteed_weekends_off_rule_text"),  # age-exemption ≠ weekends-off guarantee
    ("overtime", "83014", "overtime_guaranteed_weekends_off_rule_text"),
    ("training", "609002", "training_budget_value"),             # €750 voucher, not a per-employee entitlement
    ("fringe", "487015", "fringe_meal_benefit_present"),         # €3.40/day incidental ≠ meal benefit (keep False)
    ("safety", "1471012", "safety_workload_monitoring_present"),  # complaint-policy mention, no monitoring system
}

rows = []
SCOPED: dict[str, dict[str, dict]] = {}
COLS: dict[str, set] = {}
CLEANWIN_VAL: dict[tuple, str] = {}   # (topic,rid,field) -> accepted clean-win value


def _clean(s) -> str:
    return str(s).replace("\n", " ").replace("\r", " ").strip()


def _blank(s) -> bool:
    return _clean(s).lower() in _BLANK


def rd(p):
    return pd.read_csv(p, sep=";", dtype=str, keep_default_na=False) if os.path.exists(p) else None


def load_scoped():
    for tp in TOPICS:
        df = rd(f"{ROOT}/qa_{tp}/inputs/scoped_records.csv")
        if df is not None:
            idc = "id" if "id" in df.columns else df.columns[0]
            SCOPED[tp] = {str(r[idc]): r.to_dict() for _, r in df.iterrows()}
            COLS[tp] = set(df.columns)
        c = rd(f"{ROOT}/qa_{tp}/outputs/corrections.csv")
        if c is not None:
            for _, r in c.iterrows():
                if str(r.get("is_noop", "")).strip().lower() == "true":
                    continue
                v = _clean(r.get("csv_value_new", ""))
                if not _blank(v):
                    CLEANWIN_VAL[(tp, str(r["record_id"]), r["original_field"])] = v


def live(tp, rid, field) -> str:
    r = SCOPED.get(tp, {}).get(str(rid))
    return "" if not r else _clean(r.get(field, ""))


def value_field_of(unit_field):
    if unit_field.endswith("_range_unit"):
        base = unit_field[:-len("_range_unit")]
        return [base + "_range_min", base + "_range_max"]
    if unit_field.endswith("_unit"):
        return [unit_field[:-5] + "_value"]
    return []


def companion_unit_field(field):
    cand = None
    if field.endswith("_value"):
        cand = field[:-6] + "_unit"
    elif field.endswith("_range_min"):
        cand = field[:-len("_range_min")] + "_range_unit"
    elif field.endswith("_range_max"):
        cand = field[:-len("_range_max")] + "_range_unit"
    return cand


def split_num_unit(s):
    s = _clean(s)
    m = re.match(r"^([+-]?\d[\d.,]*)\s+(.+)$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    if re.match(r"^[+-]?\d[\d.,]*$", s):
        return s, ""
    return "", s


def add(priority, category, action, topic, rid, field, current, suggested, reason, source):
    rows.append({"priority": priority, "category": category, "action": action,
                 "topic": topic, "record_id": str(rid), "field": field,
                 "current_value": _clean(current)[:300],
                 "suggested_value": _clean(suggested)[:300],
                 "reason": _clean(reason)[:300], "source": source})


def emit_unit(topic, rid, value_field, target_unit, reason, source):
    uf = companion_unit_field(value_field)
    if not uf or uf not in COLS.get(topic, set()) or _blank(target_unit):
        return
    cur_u = live(topic, rid, uf)
    if _clean(target_unit) != cur_u:
        add("P2", "FIX", "ADD_UNIT", topic, rid, uf, cur_u, target_unit, reason, source)


# ─────────────────────────────────────────────────────────────────────────────
load_scoped()
dont_apply_keys = set()
decided_fields = set()


def emit_decisions():
    """Emit Hanna's DECIDE rulings up front as concrete value (+ unit) rows."""
    for (tp, rid, vf), d in DECISIONS.items():
        cur = live(tp, rid, vf)
        if not _blank(d["value"]) and _clean(d["value"]) != cur:
            add("P2", "SUGGEST", "ADD_VALUE" if not cur else "CHANGE_VALUE",
                tp, rid, vf, cur, d["value"], d["reason"], "decision")
        decided_fields.add((tp, str(rid), vf))
        uf = companion_unit_field(vf)
        if uf and uf in COLS.get(tp, set()):
            cu = live(tp, rid, uf)
            if d.get("unit") and _clean(d["unit"]) != cu:
                add("P2", "SUGGEST", "ADD_UNIT", tp, rid, uf, cu, d["unit"], d["reason"], "decision")
            decided_fields.add((tp, str(rid), uf))


def skip(tp, rid, field):
    return (tp, str(rid), field) in decided_fields or (tp, str(rid), field) in DROP


emit_decisions()


# 1+2. NHR items resolved through the second pass (deduped; current = live value)
sp = rd(f"{ROOT}/second_pass_nhr/outputs/enriched_needs_human_review.csv")
sp_by_key = {(r["topic"], str(r["record_id"]), r["field"]): r
             for _, r in (sp.iterrows() if sp is not None else [])}
for tp in TOPICS:
    n = rd(f"{ROOT}/qa_{tp}/outputs/needs_human_review.csv")
    if n is None:
        continue
    for _, r in n.iterrows():
        rid, field = str(r["record_id"]), r["original_field"]
        if skip(tp, rid, field):
            continue
        cur = live(tp, rid, field)
        s = sp_by_key.get((tp, rid, field))
        if s is not None:
            disp = s["disposition"]
            reason = s.get("human_review_reason", "")
            sv_raw = _clean(s.get("suggested_value", ""))
            su = _clean(s.get("suggested_unit", ""))
            if disp == "RESOLVED_CONFIRM":
                continue
            if field.endswith("_unit"):
                num, unit_tok = split_num_unit(sv_raw)
                target = su or unit_tok
                vf = field[:-5] + "_value"
                if num and vf in COLS.get(tp, set()):
                    cv = live(tp, rid, vf)
                    if _clean(num) != cv:
                        add("P3", "SUGGEST", "ADD_VALUE" if not cv else "CHANGE_VALUE",
                            tp, rid, vf, cv, num, reason, "second_pass")
            else:
                target = sv_raw
            if disp == "RESOLVED_CORRECT":
                if _blank(target):
                    add("P2", "DECIDE", "REVIEW", tp, rid, field, cur, "", reason, "second_pass")
                elif _clean(target) != cur:
                    add("P3", "SUGGEST", "ADD_VALUE" if not cur else "CHANGE_VALUE",
                        tp, rid, field, cur, target, reason, "second_pass")
                if not field.endswith("_unit") and su:
                    emit_unit(tp, rid, field, su, reason, "second_pass")
            else:  # ESCALATE
                add("P2", "DECIDE", "REVIEW", tp, rid, field, cur,
                    target if _clean(target) != cur else "", reason, "second_pass")
        else:
            new = _clean(r.get("csv_value_new", ""))
            reason = f"NHR ({_clean(r.get('verdict',''))}): {r.get('notes','')}"
            add("P2", "DECIDE", "REVIEW", tp, rid, field, cur,
                new if (new and new != cur) else "", reason, "needs_human_review")


# 3+6. Full-text verification (verify_changes for 8 topics; uncapped recheck for 4)
def emit_verification(df, source_tag, want_recheck):
    if df is None:
        return
    for _, r in df.iterrows():
        tp, field, rid = r["topic"], r["field"], str(r["record_id"])
        if (tp in RECHECK_TOPICS) != want_recheck:
            continue
        if skip(tp, rid, field):
            continue
        disp = r["disposition"]
        applied = _clean(r.get("applied_value", ""))
        cv = _clean(r.get("corrected_value", ""))
        cu = _clean(r.get("corrected_unit", ""))
        reason = r.get("verification_reason", "")
        cur = live(tp, rid, field)
        if disp == "NEEDS_CHANGE":
            if cv and cv == applied and cu:
                # value confirmed correct; only the companion unit is missing
                emit_unit(tp, rid, field, cu, reason, source_tag)
            else:
                # the pipeline's clean-win value (`applied`) is wrong/superseded — NEVER
                # apply it (covers the case where verification's answer == the live value,
                # e.g. a proposed True the source doesn't support, live already False).
                dont_apply_keys.add((tp, rid, field))
                if cv and _clean(cv) != cur:
                    add("P1", "FIX", "ADD_VALUE" if not cur else "CHANGE_VALUE",
                        tp, rid, field, cur, cv, reason, source_tag)
                    if cu:
                        emit_unit(tp, rid, field, cu, reason, source_tag)
                elif not cv and not cu and cur and cur == applied:
                    add("P1", "FIX", "CLEAR_VALUE", tp, rid, field, cur,
                        "(clear — value not supported by source)", reason, source_tag)
                # else: live already equals the corrected/empty target — no row; the wrong
                #       clean win is excluded via dont_apply above.
        elif disp == "ESCALATE":
            add("P2", "DECIDE", "REVIEW", tp, rid, field, cur,
                applied if _clean(applied) != cur else "", reason, source_tag)


emit_verification(rd(f"{ROOT}/verify_changes/outputs/verified_changes_review.csv"),
                  "verification", want_recheck=False)
for tp in RECHECK_TOPICS:
    emit_verification(rd(f"{ROOT}/qa_{tp}/recheck/outputs/verified_changes_review.csv"),
                      "recheck_fulltext", want_recheck=True)


# 4. term spelled-out / tenure-graded values
so = rd(f"{ROOT}/qa_term/outputs/spelled_out_values_for_review.csv")
if so is not None:
    for _, r in so.iterrows():
        rid, field = str(r["record_id"]), r["original_field"]
        if skip("term", rid, field):
            continue
        ev = _clean(r.get("evidence_quote", ""))
        reason = "value stated in source but spelled-out / tenure-graded — confirm the digit. " \
                 + _clean(r.get("notes", ""))
        if ev:
            reason += f"  [source: {ev[:90]}]"
        add("P2", "DECIDE", "VERIFY_DIGIT", "term", rid, field,
            live("term", rid, field), "", reason, "term_spelled_out")


# 5. pension era outliers — accrual=100% verified misread (clear value + unit)
eo = rd(f"{ROOT}/qa_pension/outputs/era_outliers.csv")
HOLISTIC_EV = {"157017": "The Vitality scheme includes 100% pension accrual.",
               "1287018": "a (fictitious) pensionable salary equal to 100% is used"}
if eo is not None:
    for _, r in eo.iterrows():
        f, rid = r["field"], str(r["record_id"])
        if skip("pension", rid, f):
            continue
        if "accrual_rate" in f:
            ev = HOLISTIC_EV.get(rid, "")
            reason = ("verified against source: the 100% is a salary-continuation / fictitious-"
                      "salary basis, NOT a DB accrual rate" + (f' ["{ev}"]' if ev else ""))
            add("P1", "FIX", "CLEAR_VALUE", "pension", rid, "pension_accrual_rate_value",
                live("pension", rid, "pension_accrual_rate_value"),
                "(clear — not a DB accrual rate)", reason, "era+holistic")
            uf = "pension_accrual_rate_unit"
            if not _blank(live("pension", rid, uf)):
                add("P1", "FIX", "CLEAR_VALUE", "pension", rid, uf, live("pension", rid, uf),
                    "(clear — no DB accrual rate to carry a unit)", reason, "era+holistic")


# ── PAIRING post-pass: never add a value without its unit (or vice versa) ────
present = {(x["topic"], x["record_id"], x["field"]) for x in rows}
surfaced_keys = set()
pairing_extra = []
for x in list(rows):
    tp, rid, f, act = x["topic"], x["record_id"], x["field"], x["action"]
    if act not in ("ADD_VALUE", "CHANGE_VALUE", "ADD_UNIT", "CLEAR_VALUE"):
        continue
    if f.endswith("_unit") or f.endswith("_range_unit"):
        # a unit is being set/cleared -> ensure the value half is present
        for vf in value_field_of(f):
            if vf not in COLS.get(tp, set()) or (tp, rid, vf) in present:
                continue
            if _blank(live(tp, rid, vf)):
                cw = CLEANWIN_VAL.get((tp, rid, vf), "")
                if cw and "CLEAR" not in act:
                    pairing_extra.append((x["priority"], x["category"], "ADD_VALUE", tp, rid, vf,
                                          "", cw, "paired value for the unit above (accepted clean-win)",
                                          "pairing"))
                    surfaced_keys.add((tp, rid, vf)); present.add((tp, rid, vf))
    else:
        # a value is being set -> ensure the unit half is present
        uf = companion_unit_field(f)
        if uf and uf in COLS.get(tp, set()) and (tp, rid, uf) not in present and "CLEAR" not in act:
            if _blank(live(tp, rid, uf)):
                pairing_extra.append((x["priority"], x["category"], "REVIEW", tp, rid, uf,
                                      "", "", "PAIRING: this value needs a unit — set the `_unit` column (from source)",
                                      "pairing"))
                present.add((tp, rid, uf))
for p in pairing_extra:
    add(*p)


# ── assemble, dedup, drop no-ops, sort, write (UTF-8 + BOM) ──────────────────
COLS_OUT = ["priority", "category", "action", "topic", "record_id", "field",
            "current_value", "suggested_value", "reason", "source"]
df = pd.DataFrame(rows, columns=COLS_OUT)


def _num_eq(a, b) -> bool:
    try:
        return float(str(a).strip().replace(",", ".")) == float(str(b).strip().replace(",", "."))
    except (ValueError, TypeError):
        return False


val_actions = ["ADD_VALUE", "CHANGE_VALUE", "ADD_UNIT"]
same = df.apply(lambda r: r["current_value"].strip() == r["suggested_value"].strip()
                or _num_eq(r["current_value"], r["suggested_value"]), axis=1)
noop = df["action"].isin(val_actions) & same
df = df[~noop]
action_rank = {"CHANGE_VALUE": 0, "CLEAR_VALUE": 1, "ADD_UNIT": 2, "ADD_VALUE": 3,
               "VERIFY_DIGIT": 4, "REVIEW": 5}
df["_a"] = df["action"].map(lambda a: action_rank.get(a, 9))
df = df.sort_values(["priority", "_a", "topic", "record_id"])
df = df.drop_duplicates(subset=["topic", "record_id", "field"], keep="first").drop(columns="_a").reset_index(drop=True)
out = f"{ROOT}/MASTER_REVIEW_FOR_HANNA.csv"
df.to_csv(out, sep=";", index=False, encoding="utf-8-sig")

# clean-win reference counts (apply these) — minus re-check rejections & surfaced pairs
print("=== ACCEPTED clean wins per topic (apply these) ===")
tot = 0
for tp in TOPICS:
    c = rd(f"{ROOT}/qa_{tp}/outputs/corrections.csv")
    if c is None:
        continue
    real = c[c["is_noop"].str.strip().str.lower() != "true"]
    n = rd(f"{ROOT}/qa_{tp}/outputs/needs_human_review.csv")
    nk = {(r["record_id"], r["original_field"]) for _, r in n.iterrows()} if n is not None else set()
    cw = sum(1 for _, r in real.iterrows()
             if (r["record_id"], r["original_field"]) not in nk
             and (tp, str(r["record_id"]), r["original_field"]) not in dont_apply_keys
             and (tp, str(r["record_id"]), r["original_field"]) not in surfaced_keys
             and not skip(tp, str(r["record_id"]), r["original_field"]))
    tot += cw
    if cw:
        print(f"  {tp:<11} {cw}")
print(f"  TOTAL clean wins to apply: {tot}   (re-check rejected {len(dont_apply_keys)}; "
      f"{len(surfaced_keys)} value-halves surfaced into the review for pairing)")

# ── apply_list.csv: every concrete cell-change to apply (review + clean wins) ──
# Consumed by apply_corrections.py. Each row = ONE cell (record_id, field) -> new_value,
# with expected_current so the apply step can verify it's editing the right cell.
apply_rows = []
for _, r in df.iterrows():                       # (A) the 124 reviewed edits
    if r["action"] in ("ADD_VALUE", "CHANGE_VALUE", "ADD_UNIT", "CLEAR_VALUE"):
        apply_rows.append({"record_id": r["record_id"], "topic": r["topic"], "field": r["field"],
                           "expected_current": r["current_value"],
                           "new_value": "" if r["action"] == "CLEAR_VALUE" else r["suggested_value"],
                           "action": r["action"], "source": "review"})
seen = {(x["record_id"], x["field"]) for x in apply_rows}
for tp in TOPICS:                                # (B) the accepted clean wins (value + unit)
    c = rd(f"{ROOT}/qa_{tp}/outputs/corrections.csv")
    if c is None:
        continue
    n = rd(f"{ROOT}/qa_{tp}/outputs/needs_human_review.csv")
    nk = {(r["record_id"], r["original_field"]) for _, r in n.iterrows()} if n is not None else set()
    for _, r in c[c["is_noop"].str.strip().str.lower() != "true"].iterrows():
        rid, f = str(r["record_id"]), r["original_field"]
        if (rid, f) in nk or (tp, rid, f) in dont_apply_keys or (tp, rid, f) in surfaced_keys \
                or skip(tp, rid, f) or skip(tp, rid, companion_unit_field(f) or ""):
            continue
        v, u = _clean(r.get("csv_value_new", "")), _clean(r.get("csv_unit_new", ""))
        if (rid, f) not in seen:
            apply_rows.append({"record_id": rid, "topic": tp, "field": f,
                               "expected_current": _clean(r.get("csv_value_old", "")),
                               "new_value": v, "action": "cleanwin", "source": "cleanwin"})
            seen.add((rid, f))
        uf = companion_unit_field(f)
        if uf and uf in COLS.get(tp, set()) and u and (rid, uf) not in seen:
            apply_rows.append({"record_id": rid, "topic": tp, "field": uf,
                               "expected_current": _clean(r.get("csv_unit_old", "")),
                               "new_value": u, "action": "cleanwin_unit", "source": "cleanwin"})
            seen.add((rid, uf))
ap = pd.DataFrame(apply_rows, columns=["record_id", "topic", "field", "expected_current",
                                       "new_value", "action", "source"])
ap.to_csv(f"{ROOT}/apply_list.csv", sep=";", index=False, encoding="utf-8-sig")
print(f"  apply_list.csv: {len(ap)} cell-changes "
      f"({int((ap['source']=='review').sum())} review + {int((ap['source']=='cleanwin').sum())} clean-win)")

print(f"\n=== MASTER_REVIEW_FOR_HANNA.csv: {len(df)} rows ===")
print(pd.crosstab(df["category"], df["action"]).to_string())
print("\nby action x topic:")
print(pd.crosstab(df["topic"], df["action"]).to_string())
print(f"\nwrote {out} (UTF-8 with BOM)")
