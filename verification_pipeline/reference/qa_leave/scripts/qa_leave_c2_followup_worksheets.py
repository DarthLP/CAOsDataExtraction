"""
qa_leave_c2_followup_worksheets.py — produce subagent worksheets for the
C2 deviations surfaced by the date-aware consistency scan.

C2 records are ones where:
  - leave_*_exceptions = False, AND
  - the record nonetheless shows a real era-aware deviation from the
    statutory baseline (different unpaid_value, min_tenure set, employer
    top-up, above-statutory care pay, etc.)

These are NOT cases where the deterministic loop already fired — they are
records the L1 rules missed, that need fresh subagent verification.

Output (under outputs/c2_followup/):
  parental_worksheet.jsonl   33 verifiable items
  care_worksheet.jsonl        5 verifiable items
  unverifiable_records.csv   37 records (source_unavailable)
"""
from __future__ import annotations
import csv
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_PATH    = ROOT / "outputs" / "leave_consistency_scan.csv"
INDEX_PATH   = ROOT / "outputs" / "leave_qa_payload_index.csv"
PAYLOADS_PATH= ROOT / "outputs" / "leave_qa_payloads.jsonl"
SRC_PATH     = ROOT / "inputs"  / "extracted_data_non_salary.csv"
OUT_DIR      = ROOT / "outputs" / "c2_followup"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- field families (what to show the subagent) ---
PARENTAL_FAMILY = [
    "leave_parental_statutory_ref", "leave_parental_exceptions",
    "leave_parental_eligibility_present",
    "leave_parental_min_tenure_value", "leave_parental_min_tenure_unit",
    "leave_parental_min_contract_length_value", "leave_parental_min_contract_length_unit",
    "leave_parental_topup_present",
    "leave_parental_topup_pay_value", "leave_parental_topup_pay_unit",
    "leave_parental_unpaid_value", "leave_parental_unpaid_unit",
]
CARE_FAMILY = [
    "leave_care_statutory_ref", "leave_care_exceptions", "leave_care_topup_present",
    "leave_short_term_care_value", "leave_short_term_care_unit",
    "leave_short_term_care_pay_value", "leave_short_term_care_pay_unit",
    "leave_long_term_care_value", "leave_long_term_care_unit",
    "leave_long_term_care_pay_value", "leave_long_term_care_pay_unit",
]

# --- per-era statutory context strings (so subagent has the right baseline) ---
def parental_era_context(ingangsdatum_str: str) -> str:
    dt = _parse_dt(ingangsdatum_str)
    if dt is None:
        return "Era unknown — apply Dutch parental leave statutory baseline as of the CAO date."
    if dt < datetime(2001, 12, 1):
        return "Era pre-2001-12-01: statutory parental leave was 6× weekly working hours unpaid."
    if dt < datetime(2009, 1, 1):
        return ("Era 2001-12 to 2009-01: WAZO statutory parental leave was 13× weekly working "
                "hours unpaid per child, taken before age 8.")
    if dt < datetime(2022, 8, 2):
        return ("Era 2009-01 to 2022-08: WAZO statutory parental leave was 26× weekly working "
                "hours unpaid per child, taken before age 8 (Wet uitbreiding ouderschapsverlof).")
    return ("Era from 2022-08-02 onwards: statutory parental leave is 26× weekly working hours "
            "TOTAL — of which the first 9× weeks are UWV-paid at 70%, and the remaining 17× "
            "weeks are unpaid (Wet betaald ouderschapsverlof).")

CARE_CONTEXT = (
    "Statutory care leave (WAZO, stable since 2001-12): short-term care = 2× weekly "
    "working hours per 12 months at minimum 70% pay; long-term care = 6× weekly "
    "working hours per 12 months UNPAID."
)

# --- questions ---
PARENTAL_QUESTION = (
    "Read the source. The CSV says exceptions=False but the parental fields "
    "show {detail}. Either: (a) the source matches the era-statutory baseline "
    "and the field values are wrong (data extraction error — reply "
    "verdict=value_correction with the correct values); (b) the source really "
    "does grant CAO-specific terms different from statutory and exceptions "
    "should be True (verdict=set_exceptions_true, optionally supersede values "
    "if any are wrong); (c) the source matches the values shown and they DO "
    "happen to equal era-statutory — exceptions=False stays correct (verdict="
    "confirm_no_exception, this should be rare given the deviation flag)."
)
CARE_QUESTION = (
    "Read the source. The CSV says care_exceptions=False but the care fields "
    "show {detail}. Decide whether (a) the values reflect a real CAO deviation "
    "(verdict=set_exceptions_true), (b) the values are wrong and should be "
    "corrected (verdict=value_correction), or (c) exceptions=False is correct "
    "as-is (verdict=confirm_no_exception)."
)


def _parse_dt(s):
    s = (s or "").strip()
    for fmt in ("%d-%m-%Y","%d/%m/%Y","%Y-%m-%d"):
        try: return datetime.strptime(s, fmt)
        except: pass
    return None


def main() -> None:
    # Load scan, filter to C2 actionables
    rows = []
    with SCAN_PATH.open() as f:
        for r in csv.DictReader(f, delimiter=";"):
            if r["check"].startswith("C2_"):
                rows.append(r)
    print(f"C2 rows: {len(rows)}")

    # Load index for source_status
    idx_by_rid = {}
    with INDEX_PATH.open() as f:
        for r in csv.DictReader(f, delimiter=";"):
            idx_by_rid[r["record_id"]] = r

    # Load source CSV for current_state lookup
    import pandas as pd
    df = pd.read_csv(SRC_PATH, sep=";", dtype=str).fillna("")
    df.set_index("id", inplace=True)

    # Load payloads (for topic_section)
    target_rids = set(r["record_id"] for r in rows)
    payloads = {}
    with PAYLOADS_PATH.open() as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
            except: continue
            rid = str(obj.get("record_id",""))
            if rid in target_rids:
                payloads[rid] = obj
            if len(payloads) == len(target_rids):
                break
    print(f"Payloads loaded: {len(payloads)}")

    # Group by check kind
    parental_items = []
    care_items     = []
    unverifiable   = []

    for r in rows:
        rid = r["record_id"]
        meta = idx_by_rid.get(rid, {})
        src_status = meta.get("source_status", "")
        if src_status != "matched":
            unverifiable.append({
                "record_id": rid,
                "check": r["check"],
                "source_status": src_status or "missing",
                "ingangsdatum": meta.get("ingangsdatum",""),
                "cao_number": meta.get("cao_number",""),
                "file_name": meta.get("file_name",""),
                "detail": r["detail"],
            })
            continue
        payload = payloads.get(rid)
        if not payload or not payload.get("source_text"):
            unverifiable.append({
                "record_id": rid,
                "check": r["check"],
                "source_status": "no_text_in_payload",
                "ingangsdatum": meta.get("ingangsdatum",""),
                "cao_number": meta.get("cao_number",""),
                "file_name": meta.get("file_name",""),
                "detail": r["detail"],
            })
            continue

        # Build current_state from source CSV
        if rid not in df.index:
            continue
        rec = df.loc[rid]
        family = PARENTAL_FAMILY if r["check"].startswith("C2_parental") else CARE_FAMILY
        current_state = []
        for fname in family:
            current_state.append({
                "field": fname,
                "csv_value": str(rec.get(fname, "")),
            })

        item = {
            "record_id": rid,
            "cao_number": meta.get("cao_number",""),
            "file_name": meta.get("file_name",""),
            "ingangsdatum": meta.get("ingangsdatum",""),
            "era": r["era"],
            "check": r["check"],
            "deviation_evidence": r["detail"],
            "topic_section": payload.get("source_text",""),
            "current_state": current_state,
        }

        if r["check"].startswith("C2_parental"):
            item["statutory_context"] = parental_era_context(meta.get("ingangsdatum",""))
            item["question"] = PARENTAL_QUESTION.format(detail=r["detail"])
            parental_items.append(item)
        else:
            item["statutory_context"] = CARE_CONTEXT
            item["question"] = CARE_QUESTION.format(detail=r["detail"])
            care_items.append(item)

    # Write worksheets
    pp = OUT_DIR / "parental_worksheet.jsonl"
    cp = OUT_DIR / "care_worksheet.jsonl"
    with pp.open("w") as f:
        for it in parental_items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    with cp.open("w") as f:
        for it in care_items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    print(f"  parental_worksheet.jsonl: {len(parental_items)} items")
    print(f"  care_worksheet.jsonl:     {len(care_items)} items")

    # Unverifiable list
    up = OUT_DIR / "unverifiable_records.csv"
    with up.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["record_id","check","source_status",
                                          "ingangsdatum","cao_number","file_name","detail"], delimiter=";")
        w.writeheader()
        for u in unverifiable:
            w.writerow(u)
    print(f"  unverifiable_records.csv: {len(unverifiable)} records")


if __name__ == "__main__":
    main()
