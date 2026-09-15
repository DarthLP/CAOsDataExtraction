"""
qa_leave_deep_sample.py — pick ~150 records for the deep (Claude-judged) sample.

Strategy:
  Bucket A (concentration):    2 most recent files per CAO that has > 5 matched records.
  Bucket B (Layer-1 errors):   every record_id flagged by ERROR-severity rules (PAR_03, CARE_01, LIB_01).
  Bucket C (random remainder): random fill until total ~ TARGET_N.

Writes:
  qa_leave/outputs/deep_sample/sample.jsonl       — per-record payloads (same shape as leave_qa_payloads.jsonl)
  qa_leave/outputs/deep_sample/sample_index.csv   — meta with bucket label per record
"""

from __future__ import annotations
import json
import random
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PAYLOADS_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
INDEX_PATH = ROOT / "outputs" / "leave_qa_payload_index.csv"
VIOLATIONS_PATH = ROOT / "outputs" / "leave_rule_violations.csv"
OUT_DIR = ROOT / "outputs" / "deep_sample"
OUT_DIR.mkdir(parents=True, exist_ok=True)
SAMPLE_PATH = OUT_DIR / "sample.jsonl"
SAMPLE_INDEX = OUT_DIR / "sample_index.csv"

TARGET_N = 150
RNG_SEED = 42


def parse_iso_date(s: str) -> tuple:
    """Coerce 'DD-MM-YYYY' or 'DD/MM/YYYY' or 'YYYY-MM-DD' to (YYYY, MM, DD); fallback (0,0,0)."""
    if not s or pd.isna(s):
        return (0, 0, 0)
    s = str(s).strip()
    for sep in ("-", "/"):
        if sep in s:
            parts = s.split(sep)
            if len(parts) == 3:
                try:
                    if len(parts[0]) == 4:
                        return (int(parts[0]), int(parts[1]), int(parts[2]))
                    else:
                        return (int(parts[2]), int(parts[1]), int(parts[0]))
                except ValueError:
                    return (0, 0, 0)
    return (0, 0, 0)


def main() -> None:
    rng = random.Random(RNG_SEED)
    idx = pd.read_csv(INDEX_PATH, sep=";", dtype=str)
    matched = idx[idx["source_status"] == "matched"].copy()

    # Add iso date column for sorting
    matched["_date_key"] = matched["ingangsdatum"].apply(parse_iso_date)

    bucket: dict[str, str] = {}  # record_id -> bucket label

    # --- Bucket A: 1 most recent file per CAO with >5 matched records ---
    counts = matched.groupby("cao_number").size()
    big_caos = sorted(counts[counts > 5].index.tolist())
    for cao in big_caos:
        sub = matched[matched["cao_number"] == cao].copy()
        sub = sub.sort_values("_date_key", ascending=False)
        for rid in sub["record_id"].head(1).tolist():
            bucket.setdefault(str(rid), "A_recent_in_big_cao")

    # --- Bucket B: all error-severity Layer-1 hits ---
    if VIOLATIONS_PATH.exists():
        viols = pd.read_csv(VIOLATIONS_PATH, sep=";", dtype=str)
        err_records = (
            viols[viols["severity"] == "error"]["record_id"].astype(str).unique().tolist()
        )
        # Restrict to records that have markdown source (matched)
        matched_ids = set(matched["record_id"].astype(str).tolist())
        for rid in err_records:
            if rid in matched_ids:
                bucket.setdefault(rid, "B_layer1_error")

    # --- Bucket C: random remainder until TARGET_N ---
    remaining = [rid for rid in matched["record_id"].astype(str).tolist() if rid not in bucket]
    rng.shuffle(remaining)
    while len(bucket) < TARGET_N and remaining:
        rid = remaining.pop()
        bucket[rid] = "C_random"

    # Cap if we overshot from buckets A+B
    if len(bucket) > TARGET_N:
        # Keep all of A and B, then trim C if needed
        a_b = {k: v for k, v in bucket.items() if v != "C_random"}
        c = {k: v for k, v in bucket.items() if v == "C_random"}
        if len(a_b) >= TARGET_N:
            bucket = a_b
        else:
            keep_c = list(c.keys())[: TARGET_N - len(a_b)]
            bucket = {**a_b, **{k: c[k] for k in keep_c}}

    # Build sample.jsonl by reading payloads
    selected = set(bucket.keys())
    rows: list[dict] = []
    with PAYLOADS_PATH.open("r", encoding="utf-8") as fin, SAMPLE_PATH.open(
        "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            o = json.loads(line)
            rid = str(o.get("record_id"))
            if rid in selected:
                fout.write(json.dumps(o, ensure_ascii=False) + "\n")
                rows.append(
                    {
                        "record_id": rid,
                        "cao_number": o.get("cao_number"),
                        "file_name": o.get("file_name"),
                        "ingangsdatum": o.get("ingangsdatum"),
                        "general_document_type": o.get("general_document_type"),
                        "bucket": bucket[rid],
                        "source_text_chars": len(o.get("source_text") or ""),
                    }
                )

    pd.DataFrame(rows).to_csv(SAMPLE_INDEX, sep=";", index=False)

    print(f"Total sample size:       {len(rows)}")
    print(f"  A (recent in big CAO): {sum(1 for r in rows if r['bucket']=='A_recent_in_big_cao')}")
    print(f"  B (layer1 error):      {sum(1 for r in rows if r['bucket']=='B_layer1_error')}")
    print(f"  C (random):            {sum(1 for r in rows if r['bucket']=='C_random')}")
    print(f"Distinct CAOs covered:   {len({r['cao_number'] for r in rows})}")
    print(f"Sample written to:       {SAMPLE_PATH}")
    print(f"Index written to:        {SAMPLE_INDEX}")


if __name__ == "__main__":
    main()
