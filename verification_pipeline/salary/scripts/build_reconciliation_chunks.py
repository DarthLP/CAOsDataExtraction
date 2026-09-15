"""build_reconciliation_chunks.py — final check chunks.

For each test chunk, package up everything the auditor needs to decide whether
v1 is genuinely better than v0 — NOT just "looks faithful in isolation" but
"strictly improves on v0 without losing anything legitimate that v0 had."

Each chunk JSON contains:
  - wage_information_text  : the source bullets
  - v0_salary              : the production extraction (from llm_analysis/salary)
  - v1_salary              : the patched re-extraction
  - field_diff             : a pre-computed structural diff (rows only in v0,
                             only in v1, and same-key rows whose amounts differ)
  - v0_audit_summary       : what the Phase-3b auditor said about v0
  - v1_audit_summary       : what the v1 auditor said about v1

The subagent then writes a structured reconciliation verdict.
"""
from __future__ import annotations
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OLD = ROOT / "re_extraction_backup" / "analysis_originals"
NEW = ROOT / "re_extraction" / "v1_outputs"
CHUNKS_OLD = ROOT / "phase3b" / "chunks"
VERIFY_V0 = ROOT / "phase3b" / "results"
VERIFY_V1 = ROOT / "re_extraction" / "v1_verify_results"
RECON = ROOT / "re_extraction" / "reconciliation_chunks"
RECON.mkdir(parents=True, exist_ok=True)

TESTS = [("b0007", "1287", "Grafimedia cao 2022-2024 1 april 2022"),
         ("b0011", "1536", "CAO NU 20222-2023 definitieve tekst schoon"),
         ("b0014", "163",  "CAO_OV_tekst_2014_en_2015_DEFINITIEF"),
         ("b0022", "234",  "Jeugdzorg_Cao_2011_2013_def"),
         ("b0048", "823",  "2025-07-01 CAO MVT 2025 - 2027 - incl HFI DEF")]

_STEP_SYNONYMS = {
    "min": "min", "minimum": "min", "schaalmin": "min",
    "max": "max", "maximum": "max", "schaalmax": "max", "eindloon": "max",
    "entry": "entry", "ingang": "entry", "aanvang": "entry", "aanloop": "entry",
}


def _norm(s):
    s = str(s or "").strip().lower()
    return _STEP_SYNONYMS.get(s, s)


def row_key(r):
    return (
        str(r.get("jobgroup", "")).strip().lower(),
        _norm(r.get("step", "")),
        str(r.get("worker", "") or "").strip().lower(),
        str(r.get("age_group", "") or "").strip().lower(),
        str(r.get("education", "") or "").strip().lower(),
    )


def amount_map(r):
    """{start_date: amount} per row."""
    return {p.get("start_date"): p.get("amount") for p in r.get("timeline", [])}


def build_diff(v0_si, v1_si):
    v0_idx = {row_key(r): r for r in v0_si}
    v1_idx = {row_key(r): r for r in v1_si}
    only_v0 = []
    for k in v0_idx.keys() - v1_idx.keys():
        only_v0.append({"key": list(k), "row": v0_idx[k]})
    only_v1 = []
    for k in v1_idx.keys() - v0_idx.keys():
        only_v1.append({"key": list(k), "row": v1_idx[k]})
    same_different = []
    for k in v0_idx.keys() & v1_idx.keys():
        a0, a1 = amount_map(v0_idx[k]), amount_map(v1_idx[k])
        if a0 != a1:
            same_different.append({
                "key": list(k),
                "v0_amounts": a0, "v1_amounts": a1,
            })
    return {
        "n_v0_rows": len(v0_idx), "n_v1_rows": len(v1_idx),
        "n_unchanged": len(v0_idx.keys() & v1_idx.keys()) - len(same_different),
        "rows_only_in_v0": only_v0,
        "rows_only_in_v1": only_v1,
        "same_key_different_amounts": same_different,
    }


def main():
    for cid, cao, fname in TESTS:
        old_chunk = json.load(open(CHUNKS_OLD / f"{cid}.json", encoding="utf-8"))
        v0 = json.load(open(OLD / cao / f"{fname}_analysis.json", encoding="utf-8")).get("salary_information", [])
        v1 = json.load(open(NEW / f"{cid}_analysis.json", encoding="utf-8")).get("salary_information", [])
        v0_audit = json.load(open(VERIFY_V0 / f"{cid}.json", encoding="utf-8"))
        v1_audit = json.load(open(VERIFY_V1 / f"{cid}_v1.json", encoding="utf-8"))
        chunk = {
            "chunk_id": f"{cid}_recon",
            "cao_number": cao,
            "file_name": fname,
            "wage_information_text": old_chunk["wage_information_text"],
            "v0_salary": v0,
            "v1_salary": v1,
            "field_diff": build_diff(v0, v1),
            "v0_audit_summary": {
                "overall_assessment": v0_audit.get("overall_assessment"),
                "n_discrepancies": len(v0_audit.get("discrepancies", [])),
                "discrepancies": v0_audit.get("discrepancies", []),
            },
            "v1_audit_summary": {
                "overall_assessment": v1_audit.get("overall_assessment"),
                "n_discrepancies": len(v1_audit.get("discrepancies", [])),
                "discrepancies": v1_audit.get("discrepancies", []),
            },
        }
        p = RECON / f"{cid}_recon.json"
        p.write_text(json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8")
        d = chunk["field_diff"]
        print(f"{cid}: v0={d['n_v0_rows']} v1={d['n_v1_rows']} "
              f"unchanged={d['n_unchanged']} only_v0={len(d['rows_only_in_v0'])} "
              f"only_v1={len(d['rows_only_in_v1'])} same_key_diff_amt={len(d['same_key_different_amounts'])}")


if __name__ == "__main__":
    main()
