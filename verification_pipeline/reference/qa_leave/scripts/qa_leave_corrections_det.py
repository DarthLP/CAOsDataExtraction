"""
qa_leave_corrections_det.py — produce DETERMINISTIC corrections.

A correction is "deterministic" when we know the right fix without reading
source. Examples:
  - CARE_01 / pattern P7  : care_exceptions = True
  - PAR_03  / pattern P8  : parental_exceptions = True
  - P10_lib_day (lustrum hit) : liberation_day_annual = False
  - P3 hetero_on_age_only : hetero_present = False  (subject to review)
  - P6_above_statutory_no_value : has_above_statutory_* = False
  - LIB_01 unresolved : flagged for human, no auto-fix

Reads:
  qa_leave/outputs/leave_rule_violations.csv
  qa_leave/outputs/leave_pattern_flags.csv

Writes:
  qa_leave/outputs/corrections_deterministic.csv  (one row per proposed fix)

Schema:
  record_id;cao_number;file_name;topic_group;field;csv_value_old;csv_value_new;
  unit_new;severity;evidence_quote;fix_method;source_layer;reason
"""

from __future__ import annotations
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
L1_PATH = ROOT / "outputs" / "leave_rule_violations.csv"
PAT_PATH = ROOT / "outputs" / "leave_pattern_flags.csv"
INDEX_PATH = ROOT / "outputs" / "leave_qa_payload_index.csv"
SRC_PATH = ROOT / "inputs" / "extracted_data_non_salary.csv"
OUT_PATH = ROOT / "outputs" / "corrections_deterministic.csv"


# ----- Statutory baselines (era-aware) -----
# See docs/leave_qa_conventions.md for the full table and citations.
PAR_13_TO_26  = datetime(2009, 1, 1)
PAR_26_SPLIT  = datetime(2022, 8, 2)
PAT_2_TO_5    = datetime(2019, 1, 1)
PAT_WIEG_DATE = datetime(2020, 7, 1)
ADOPT_4_TO_6  = datetime(2019, 1, 1)


def _parse_dt(s: str):
    s = (s or "").strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def _to_float(v):
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError, AttributeError):
        return None


def _truthy(v): return str(v or "").strip().lower() == "true"
def _falsy(v):  return str(v or "").strip().lower() == "false"


def parental_baseline_unpaid(dt):
    """(unpaid_weeks, paid_weeks) for parental leave at the given ingangsdatum."""
    if dt is None: return None
    if dt < datetime(2001, 12, 1): return (6.0, 0.0)
    if dt < PAR_13_TO_26:           return (13.0, 0.0)
    if dt < PAR_26_SPLIT:           return (26.0, 0.0)
    return (17.0, 9.0)


def parental_matches_statutory(row, dt) -> bool:
    """True iff the parental record looks like a clean statutory restatement.

    Conditions: statutory_ref=True AND exceptions=False AND no eligibility
    thresholds AND no employer top-up AND unpaid_value either empty or
    matching the era baseline (post-2022 we accept 17 OR 26 — both readable
    as 'matches statutory').
    """
    if not _truthy(row.get("leave_parental_statutory_ref", "")):
        return False
    if not _falsy(row.get("leave_parental_exceptions", "")):
        return False
    # Eligibility thresholds disqualify
    if _to_float(row.get("leave_parental_min_tenure_value")):
        return False
    if _to_float(row.get("leave_parental_min_contract_length_value")):
        return False
    # Employer top-up disqualifies
    if _truthy(row.get("leave_parental_topup_present", "")):
        return False
    if _to_float(row.get("leave_parental_topup_pay_value")):
        return False
    # Unpaid value must be empty OR match era baseline
    base = parental_baseline_unpaid(dt)
    if base is None:
        return False
    stat_unpaid, stat_paid = base
    v = _to_float(row.get("leave_parental_unpaid_value"))
    if v is None:
        return True  # already empty, just clean ref/exceptions flags
    if dt is not None and dt >= PAR_26_SPLIT:
        # Post-Aug-2022 accepts both readings of "statutory"
        return v in (stat_unpaid, 26.0)
    return v == stat_unpaid


def care_matches_statutory(row) -> bool:
    """True iff the care record is a clean statutory restatement (WAZO stable since 2001)."""
    if not _truthy(row.get("leave_care_statutory_ref", "")):
        return False
    if not _falsy(row.get("leave_care_exceptions", "")):
        return False
    if _truthy(row.get("leave_care_topup_present", "")):
        return False
    # short-term: 2× weekly hours, 70% pay
    v = _to_float(row.get("leave_short_term_care_value"))
    if v is not None and v != 2.0:
        return False
    p = _to_float(row.get("leave_short_term_care_pay_value"))
    if p is not None and p != 70.0:
        return False
    # long-term: 6× weekly hours, 0% pay
    v = _to_float(row.get("leave_long_term_care_value"))
    if v is not None and v != 6.0:
        return False
    p = _to_float(row.get("leave_long_term_care_pay_value"))
    if p is not None and p != 0.0:
        return False
    return True


# Pairs are emitted as ONE correction row per (value_field, unit_field) pair —
# csv_value_old/csv_unit_old hold the OLD numbers/units and csv_value_new/unit_new
# are blanked. This keeps each row aligned to a single semantic cell.
PARENTAL_CLEAR_PAIRS = [
    ("leave_parental_min_tenure_value",          "leave_parental_min_tenure_unit"),
    ("leave_parental_min_contract_length_value", "leave_parental_min_contract_length_unit"),
    ("leave_parental_topup_pay_value",           "leave_parental_topup_pay_unit"),
    ("leave_parental_unpaid_value",              "leave_parental_unpaid_unit"),
]
PARENTAL_CLEAR_BOOLS = [
    "leave_parental_eligibility_present",
    "leave_parental_topup_present",
]
CARE_CLEAR_PAIRS = [
    ("leave_short_term_care_value",     "leave_short_term_care_unit"),
    ("leave_short_term_care_pay_value", "leave_short_term_care_pay_unit"),
    ("leave_long_term_care_value",      "leave_long_term_care_unit"),
    ("leave_long_term_care_pay_value",  "leave_long_term_care_pay_unit"),
]
CARE_CLEAR_BOOLS = [
    "leave_care_topup_present",
]


def emit_statutory_clear_rows(src_df, idx_by_id) -> list[dict]:
    """Emit corrections for records matching the era-statutory baseline. Each
    row is one semantic cell:
      - value/unit pairs → ONE row with field=value_field, csv_value_old +
        csv_unit_old populated from BOTH halves of the pair, both _new fields
        blanked.
      - boolean cells (eligibility_present, topup_present) → ONE row with
        csv_value_old=current bool, csv_value_new='False'.
    """
    PARENTAL_REASON = (
        "Statutory restatement: parental_statutory_ref=True, exceptions=False, "
        "value matches era baseline. Per schema convention, sub-fields should be empty."
    )
    CARE_REASON = (
        "Statutory restatement: care_statutory_ref=True, exceptions=False, "
        "values match WAZO baseline. Per schema convention, sub-fields should be empty."
    )

    def make_pair_row(rid, topic, value_field, unit_field, val_old, unit_old, layer, reason):
        return {
            "record_id": rid,
            "topic_group": topic,
            "field": value_field,
            "csv_value_old": val_old,
            "csv_unit_old": unit_old,
            "csv_value_new": "",
            "unit_new": "",
            "severity": "info",
            "evidence_quote": "",
            "fix_method": "statutory_clear",
            "source_layer": layer,
            "reason": reason,
        }

    def make_bool_row(rid, topic, fname, cur, layer, reason):
        return {
            "record_id": rid,
            "topic_group": topic,
            "field": fname,
            "csv_value_old": cur,
            "csv_unit_old": "",
            "csv_value_new": "False",
            "unit_new": "",
            "severity": "info",
            "evidence_quote": "",
            "fix_method": "statutory_clear",
            "source_layer": layer,
            "reason": reason,
        }

    rows: list[dict] = []
    for rid_key, rec in src_df.iterrows():
        rid = str(rid_key)
        meta = idx_by_id.get(rid)
        if meta is None:
            continue
        dt = _parse_dt(meta["ingangsdatum"]) if hasattr(meta, "__getitem__") else None

        # Parental
        if parental_matches_statutory(rec, dt):
            for vf, uf in PARENTAL_CLEAR_PAIRS:
                v_old = str(rec.get(vf, "")).strip()
                u_old = str(rec.get(uf, "")).strip()
                if v_old == "" and u_old == "":
                    continue
                rows.append(make_pair_row(rid, "parental", vf, uf, v_old, u_old,
                                          "L0_STATUTORY_CLEAR_parental", PARENTAL_REASON))
            for bf in PARENTAL_CLEAR_BOOLS:
                cur = str(rec.get(bf, "")).strip()
                if cur == "" or cur.lower() == "false":
                    continue
                rows.append(make_bool_row(rid, "parental", bf, cur,
                                          "L0_STATUTORY_CLEAR_parental", PARENTAL_REASON))

        # Care
        if care_matches_statutory(rec):
            for vf, uf in CARE_CLEAR_PAIRS:
                v_old = str(rec.get(vf, "")).strip()
                u_old = str(rec.get(uf, "")).strip()
                if v_old == "" and u_old == "":
                    continue
                rows.append(make_pair_row(rid, "care", vf, uf, v_old, u_old,
                                          "L0_STATUTORY_CLEAR_care", CARE_REASON))
            for bf in CARE_CLEAR_BOOLS:
                cur = str(rec.get(bf, "")).strip()
                if cur == "" or cur.lower() == "false":
                    continue
                rows.append(make_bool_row(rid, "care", bf, cur,
                                          "L0_STATUTORY_CLEAR_care", CARE_REASON))

    return rows


def main() -> None:
    l1 = pd.read_csv(L1_PATH, sep=";", dtype=str).fillna("")
    pat = pd.read_csv(PAT_PATH, sep=";", dtype=str).fillna("")
    idx = pd.read_csv(INDEX_PATH, sep=";", dtype=str).fillna("")
    idx_by_id = {str(r["record_id"]): r for _, r in idx.iterrows()}

    # Load source CSV for the L0 statutory-clear pass.
    src = pd.read_csv(SRC_PATH, sep=";", dtype=str).fillna("")
    src.set_index("id", inplace=True)
    in_scope = set(idx["record_id"].astype(str))
    src = src[src.index.astype(str).isin(in_scope)]

    rows: list[dict] = []

    # ----- Layer 0: statutory restatement clears (per docs/leave_qa_conventions.md)
    rows.extend(emit_statutory_clear_rows(src, idx_by_id))

    # ----- Deterministic from Layer 1 -----
    for _, r in l1.iterrows():
        rule_id = r.get("rule_id", "")
        record_id = str(r.get("record_id", ""))

        if rule_id.startswith("CARE_01"):
            rows.append({
                "record_id": record_id,
                "topic_group": "care",
                "field": "leave_care_exceptions",
                "csv_value_old": "False",
                "csv_value_new": "True",
                "unit_new": "",
                "severity": "high",
                "evidence_quote": "(CSV has populated care fields → CAO has CAO-specific provision → exceptions=True)",
                "fix_method": "deterministic",
                "source_layer": "L1_CARE_01",
                "reason": "Layer 1 CARE_01 fired: ref=True, exceptions=False, but care details filled. Mechanical fix: exceptions=True.",
            })

        elif rule_id.startswith("PAR_03"):
            rows.append({
                "record_id": record_id,
                "topic_group": "parental",
                "field": "leave_parental_exceptions",
                "csv_value_old": "False",
                "csv_value_new": "True",
                "unit_new": "",
                "severity": "high",
                "evidence_quote": "(CSV has populated parental eligibility → CAO has CAO-specific tenure rule → exceptions=True)",
                "fix_method": "deterministic",
                "source_layer": "L1_PAR_03",
                "reason": "Layer 1 PAR_03 fired: ref=True, exceptions=False, but eligibility filled. Mechanical fix: exceptions=True.",
            })

        elif rule_id.startswith("PAR_04"):
            rows.append({
                "record_id": record_id,
                "topic_group": "parental",
                "field": "leave_parental_topup_present",
                "csv_value_old": "False",
                "csv_value_new": "True",
                "unit_new": "",
                "severity": "medium",
                "evidence_quote": "(parental_topup_pay_value populated)",
                "fix_method": "deterministic",
                "source_layer": "L1_PAR_04",
                "reason": "topup_pay_value filled but topup_present=False. Mechanical fix: present=True.",
            })

        elif rule_id.startswith("STAT_01"):
            rows.append({
                "record_id": record_id,
                "topic_group": "maternity",
                "field": "leave_has_above_statutory_maternity",
                "csv_value_old": "True",
                "csv_value_new": "False",
                "unit_new": "",
                "severity": "medium",
                "evidence_quote": "(no maternity duration / note populated)",
                "fix_method": "deterministic",
                "source_layer": "L1_STAT_01",
                "reason": "Flag claims above-statutory but no maternity field populated. Mechanical fix: flag=False.",
            })

        elif rule_id.startswith("STAT_02"):
            rows.append({
                "record_id": record_id,
                "topic_group": "paternity",
                "field": "leave_paternity_explicitly_above_statutory",
                "csv_value_old": "True",
                "csv_value_new": "False",
                "unit_new": "",
                "severity": "medium",
                "evidence_quote": "(no paternity duration field populated)",
                "fix_method": "deterministic",
                "source_layer": "L1_STAT_02",
                "reason": "Flag claims above-statutory but no paternity field populated. Mechanical fix: flag=False.",
            })

    # ----- Deterministic from pattern detector -----
    for _, p in pat.iterrows():
        pid = p.get("pattern_id", "")
        record_id = str(p.get("record_id", ""))

        if pid == "P10_libday_lustrum_annotated_in_source":
            rows.append({
                "record_id": record_id,
                "topic_group": "vacation_holidays",
                "field": "leave_liberation_day_annual",
                "csv_value_old": "True",
                "csv_value_new": "False",
                "unit_new": "",
                "severity": "high",
                "evidence_quote": p.get("evidence_snippet", "")[:200],
                "fix_method": "deterministic",
                "source_layer": "pattern_P10",
                "reason": "Both flags True; source explicitly says lustrum. Annual=False is the deterministic fix.",
            })

        elif pid == "P6_above_statutory_no_value":
            field = p.get("field", "")
            rows.append({
                "record_id": record_id,
                "topic_group": p.get("topic_group", ""),
                "field": field,
                "csv_value_old": "True",
                "csv_value_new": "False",
                "unit_new": "",
                "severity": "medium",
                "evidence_quote": p.get("evidence_snippet", "")[:200],
                "fix_method": "deterministic",
                "source_layer": "pattern_P6",
                "reason": "Above-statutory flag True but no supporting value. Mechanical fix: flag=False.",
            })

        elif pid == "P1_wieg_paternity_duplicate":
            # Subagent follow-up review (May 2026) found that ALL 6 instances
            # of this pattern were misfires: the CAOs really do stack 5 weeks
            # WIEG (partially paid) + N weeks of additional unpaid leave on
            # top. The "same value in both fields" heuristic produces false
            # positives whenever a CAO chooses N=5 for the additional unpaid
            # period (which is common). We therefore demote this pattern from
            # an auto-fix to a flag for subagent review.
            rows.append({
                "record_id": record_id,
                "topic_group": "paternity",
                "field": "leave_unpaid_paternity_value",
                "csv_value_old": p.get("csv_value_old", ""),
                "csv_value_new": p.get("csv_value_old", ""),  # unchanged — DON'T auto-clear
                "unit_new": "",
                "severity": "medium",
                "evidence_quote": p.get("evidence_snippet", "")[:200],
                "fix_method": "review_recommended",
                "source_layer": "pattern_P1",
                "reason": (
                    "Same value in partially_paid and unpaid paternity. May be a duplicate "
                    "(extractor error) OR a genuine stacked entitlement (5 weeks WIEG + 5 "
                    "weeks additional unpaid). Subagent review required — DO NOT auto-clear."
                ),
            })

    # P3 hetero overcalling — propose False but mark as 'review-recommended'
    # because the rule has known false positives (e.g., uitzendkrachten contracts
    # where "uitzendbeding vs no uitzendbeding" IS a real worker split). We still
    # write the deterministic correction but flag fix_method as 'deterministic_review'.
    for _, p in pat.iterrows():
        if p.get("pattern_id") == "P3_hetero_on_age_only":
            record_id = str(p.get("record_id", ""))
            rows.append({
                "record_id": record_id,
                "topic_group": "general",
                "field": "leave_hetero_present",
                "csv_value_old": "True",
                "csv_value_new": "False",
                "unit_new": "",
                "severity": "medium",
                "evidence_quote": p.get("evidence_snippet", "")[:200],
                "fix_method": "deterministic_review",
                "source_layer": "pattern_P3",
                "reason": "hetero_present=True but source mentions only age cohorts. Note: detector may miss legitimate worker-group splits (uitzendbeding etc.) — review before applying.",
            })

    # ----- Write -----
    if not rows:
        rows = [{}]  # ensure header still gets written

    headers = [
        "record_id", "cao_number", "file_name", "topic_group", "field",
        "csv_value_old", "csv_unit_old", "csv_value_new", "unit_new",
        "severity", "evidence_quote", "fix_method", "source_layer", "reason",
    ]
    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(headers)
        for r in rows:
            if not r:
                continue
            meta = idx_by_id.get(str(r.get("record_id", "")), {})
            r["cao_number"] = meta.get("cao_number", "") if hasattr(meta, "get") else ""
            r["file_name"] = meta.get("file_name", "") if hasattr(meta, "get") else ""
            w.writerow([r.get(h, "") for h in headers])

    print(f"Wrote {len([r for r in rows if r])} deterministic corrections to {OUT_PATH}")
    print()
    if rows and rows[0]:
        from collections import Counter
        layer_counts = Counter(r.get("source_layer", "") for r in rows if r)
        print("By source layer:")
        for k, v in layer_counts.most_common():
            print(f"  {k}: {v}")
        print()
        method_counts = Counter(r.get("fix_method", "") for r in rows if r)
        print("By fix method:")
        for k, v in method_counts.most_common():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
