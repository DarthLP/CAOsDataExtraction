"""
qa_leave_l1_followup_worksheets.py — produce subagent worksheets that re-verify
the deterministic flips that nobody else looked at.

Why
---
The original pipeline flips boolean exception flags via Layer-1 deterministic
rules without ever asking an LLM whether the underlying field family is
right. For the 115 records affected by such flips, this script emits one
worksheet JSONL per rule group so a subagent pass can independently re-derive
the truth from the source CAO text.

Output (under outputs/l1_followup/):
  par_03_worksheet.jsonl       L1_PAR_03 (parental_exceptions: False -> True)
  care_01_worksheet.jsonl      L1_CARE_01 (care_exceptions: False -> True)
  stat_02_p6_worksheet.jsonl   L1_STAT_02 + pattern_P6 (paternity_above_statutory: True -> False)
  p10_worksheet.jsonl          pattern_P10 (liberation_day_annual: True -> False)
  p1_worksheet.jsonl           pattern_P1 (unpaid_paternity_value cleared)

Records with source_status='source_unavailable' cannot be verified by an LLM
and are listed separately in outputs/l1_followup/unverifiable_records.csv for
optional human review.

Each worksheet JSONL line has the shape:
  {
    "record_id": ..., "cao_number": ..., "file_name": ...,
    "rule_group": "par_03",          # one of {par_03, care_01, stat_02_p6, p10, p1}
    "topic_section": "...",           # source excerpt for the LLM
    "current_state": [
        {"field": "...", "csv_value": "...", "csv_unit": "..."}, ...
    ],
    "question": "..."                  # focused, neutral question
  }

The subagent should respond with a CSV (chunk_<group>_review.csv) of:
  record_id ; field ; verdict ; new_value ; new_unit ; evidence_quote ; confidence ; notes
where verdict ∈ {confirm_flip, revert_flip, supersede}.
"""
from __future__ import annotations
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DET_PATH = ROOT / "outputs" / "corrections_deterministic.csv"
PAYLOADS_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
INDEX_PATH = ROOT / "outputs" / "leave_qa_payload_index.csv"
OUT_DIR = ROOT / "outputs" / "l1_followup"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Field families per rule group — the cells the subagent should re-extract.
FIELD_FAMILIES = {
    "par_03": [
        "leave_parental_statutory_ref", "leave_parental_exceptions",
        "leave_parental_eligibility_present",
        "leave_parental_min_tenure_value", "leave_parental_min_tenure_unit",
        "leave_parental_min_contract_length_value", "leave_parental_min_contract_length_unit",
        "leave_parental_topup_present",
        "leave_parental_topup_pay_value", "leave_parental_topup_pay_unit",
        "leave_parental_unpaid_value", "leave_parental_unpaid_unit",
    ],
    "care_01": [
        "leave_care_statutory_ref", "leave_care_exceptions", "leave_care_topup_present",
        "leave_short_term_care_value", "leave_short_term_care_unit",
        "leave_short_term_care_pay_value", "leave_short_term_care_pay_unit",
        "leave_long_term_care_value", "leave_long_term_care_unit",
        "leave_long_term_care_pay_value", "leave_long_term_care_pay_unit",
    ],
    "stat_02_p6": [
        "leave_paternity_explicitly_above_statutory",
        "leave_paid_paternity_value", "leave_paid_paternity_unit",
        "leave_partially_paid_paternity_value", "leave_partially_paid_paternity_unit",
        "leave_partially_paid_paternity_pay_value", "leave_partially_paid_paternity_pay_unit",
        "leave_unpaid_paternity_value", "leave_unpaid_paternity_unit",
    ],
    "p10": [
        "leave_liberation_day_annual",
        "leave_liberation_day_lustrum",
        "leave_liberation_day_comp_note",
    ],
    "p1": [
        "leave_paid_paternity_value", "leave_paid_paternity_unit",
        "leave_partially_paid_paternity_value", "leave_partially_paid_paternity_unit",
        "leave_partially_paid_paternity_pay_value", "leave_partially_paid_paternity_pay_unit",
        "leave_unpaid_paternity_value", "leave_unpaid_paternity_unit",
    ],
}

# Topic groups in leave_qa_payloads.jsonl that contain the relevant text.
RULE_TOPICS = {
    "par_03":     ["parental"],
    "care_01":    ["care"],
    "stat_02_p6": ["paternity"],
    "p10":        ["vacation_holidays", "general"],
    "p1":         ["paternity"],
}

QUESTION = {
    "par_03": (
        "Does this CAO grant parental-leave provisions that DEVIATE from the statutory minimum "
        "(e.g. a tenure or contract-length requirement, an employer top-up of pay, an extended "
        "unpaid duration)? Re-derive the values from the source. Return verdict=confirm_flip "
        "if exceptions=True and the listed field values are correct, verdict=revert_flip if no "
        "deviations exist, verdict=supersede if exceptions=True is correct but one or more "
        "values need to be replaced (provide them)."
    ),
    "care_01": (
        "Does this CAO grant care-leave provisions that DEVIATE from the statutory minimum "
        "(short-term care duration/pay, long-term care duration/pay, employer top-up)? "
        "Same verdict semantics as above."
    ),
    "stat_02_p6": (
        "Does the source explicitly grant paternity / partner leave ABOVE the statutory "
        "minimum (i.e. paid days beyond the WIEG 1-week + 5-week supplementary regime, "
        "or any value in the paternity_value / partially_paid / unpaid fields)? Return "
        "verdict=confirm_flip if no above-statutory paternity is mentioned and the empty "
        "values are correct, verdict=revert_flip if above-statutory paternity exists "
        "(and supply the value), verdict=supersede if mixed."
    ),
    "p10": (
        "Does this CAO treat Liberation Day (Bevrijdingsdag, May 5) as an ANNUAL public "
        "holiday, a LUSTRUM-only holiday (every 5 years), or both? Return verdict=confirm_flip "
        "if lustrum-only is correct, verdict=revert_flip if it should be annual, verdict=supersede "
        "for any other reading."
    ),
    "p1": (
        "Does this CAO grant ADDITIONAL UNPAID paternity leave on top of the WIEG entitlement, "
        "or is the duplicated value just a data-entry error? Return verdict=confirm_flip if the "
        "unpaid field should be empty (CAO only grants the WIEG partially-paid leave), "
        "verdict=revert_flip if there really IS additional unpaid leave with the original value, "
        "verdict=supersede if the unpaid leave exists but with a different value (provide it)."
    ),
}


def collect_topic_section(payload: dict, topics: list[str]) -> str:
    """Pull the source text for the given topic groups out of a payload row."""
    src_text = payload.get("source_text") or ""
    if not src_text:
        return ""
    # The source_text is plain prose. Try to extract the topic-keyed portion if
    # the curated leave_information.md format is used; otherwise return as-is.
    return src_text


def main() -> None:
    # Load deterministic targets and group by rule
    by_rule_record: dict[str, dict[str, list[dict]]] = {k: {} for k in FIELD_FAMILIES}
    with DET_PATH.open() as f:
        rdr = csv.DictReader(f, delimiter=";")
        for row in rdr:
            sl = row["source_layer"]
            if sl == "L1_PAR_03":     group = "par_03"
            elif sl == "L1_CARE_01":  group = "care_01"
            elif sl in ("L1_STAT_02","pattern_P6"): group = "stat_02_p6"
            elif sl == "pattern_P10": group = "p10"
            elif sl == "pattern_P1":  group = "p1"
            else: continue
            by_rule_record[group].setdefault(row["record_id"], []).append(row)

    # Load index for source_status / cao_number / file_name
    idx: dict[str, dict[str,str]] = {}
    with INDEX_PATH.open() as f:
        for r in csv.DictReader(f, delimiter=";"):
            idx[r["record_id"]] = r

    # Stream payloads, picking up only the records we care about.
    target_rids = {rid for grp in by_rule_record.values() for rid in grp.keys()}
    payloads_by_rid: dict[str, dict] = {}
    with PAYLOADS_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            rid = str(obj.get("record_id", ""))
            if rid in target_rids:
                payloads_by_rid[rid] = obj
            if len(payloads_by_rid) == len(target_rids):
                break

    # Emit worksheets and unverifiable list
    unverifiable: list[dict] = []
    for group, recs in by_rule_record.items():
        wpath = OUT_DIR / f"{group}_worksheet.jsonl"
        n_written = 0
        with wpath.open("w") as f:
            for rid in sorted(recs.keys()):
                meta = idx.get(rid, {})
                src_status = meta.get("source_status", "")
                payload = payloads_by_rid.get(rid)
                topic_section = ""
                if payload and src_status == "matched":
                    topic_section = collect_topic_section(payload, RULE_TOPICS[group])
                if not topic_section:
                    unverifiable.append({
                        "record_id": rid,
                        "rule_group": group,
                        "source_status": src_status or "missing",
                        "cao_number": meta.get("cao_number", ""),
                        "file_name": meta.get("file_name", ""),
                    })
                    continue
                # Build current_state (post-flip values from corrections + remaining CSV values).
                current_state = []
                csv_fields = (payload or {}).get("csv_leave_fields", {})
                # Flatten csv_leave_fields into a single dict for lookup
                flat: dict[str, str] = {}
                for topic_dict in csv_fields.values():
                    if isinstance(topic_dict, dict):
                        for k, v in topic_dict.items():
                            flat[k] = "" if v is None else str(v)
                # Apply post-flip values from corrections_deterministic.csv
                for r in recs[rid]:
                    flat[r["field"]] = r["csv_value_new"]
                for fname in FIELD_FAMILIES[group]:
                    current_state.append({
                        "field": fname,
                        "csv_value": flat.get(fname, ""),
                        "csv_unit":  flat.get(fname.replace("_value","_unit"), "") if fname.endswith("_value") else "",
                    })
                item = {
                    "record_id": rid,
                    "cao_number": meta.get("cao_number", ""),
                    "file_name": meta.get("file_name", ""),
                    "rule_group": group,
                    "topic_section": topic_section,
                    "current_state": current_state,
                    "question": QUESTION[group],
                }
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                n_written += 1
        print(f"  {wpath.name}: {n_written} verifiable item(s)")

    # Unverifiable list
    upath = OUT_DIR / "unverifiable_records.csv"
    with upath.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["record_id","rule_group","source_status","cao_number","file_name"], delimiter=";")
        w.writeheader()
        for u in unverifiable:
            w.writerow(u)
    print(f"\nUnverifiable (no source text available): {len(unverifiable)} record-rule pair(s)")
    print(f"Listed in: {upath}")


if __name__ == "__main__":
    main()
