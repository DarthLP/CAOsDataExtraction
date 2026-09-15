"""
qa_leave_p3_review_worksheets.py — build worksheets for subagent P3 review.

For each remaining P3 hit (where Python pre-filter could NOT confirm a
worker-group split), produce a worksheet with the targeted topic concat:
  general + vacation_holidays + sick + seniority_special

Subagent answers a single binary question per record:
  "Does the source describe distinct worker GROUPS (any kind) with different
   leave/seniority terms — beyond age cohorts and full-time-vs-part-time?"

Output:
  qa_leave/outputs/p3_review/p3_review_worksheets.jsonl
  qa_leave/outputs/p3_review/chunks/chunk_NNN.jsonl  (~30 items each)
  qa_leave/outputs/p3_review/chunk_index.csv
"""
from __future__ import annotations
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PAYLOADS_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
PAT_PATH = ROOT / "outputs" / "leave_pattern_flags.csv"
OUT_DIR = ROOT / "outputs" / "p3_review"
CHUNK_DIR = OUT_DIR / "chunks"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

ITEMS_PER_CHUNK = 30
RELEVANT_TOPICS = ("general", "vacation_holidays", "sick", "seniority_special")

# Reuse parser from qa_leave_worksheets
import sys
sys.path.insert(0, str(ROOT / "scripts"))
from qa_leave_worksheets import parse_topic_sections  # type: ignore


def main() -> None:
    pat = pd.read_csv(PAT_PATH, sep=";", dtype=str).fillna("")
    p3 = pat[pat["pattern_id"] == "P3_hetero_on_age_only"]
    p3_ids = set(p3["record_id"].astype(str).tolist())
    print(f"P3 records to review: {len(p3_ids)}")

    payloads: dict[str, dict] = {}
    with PAYLOADS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            if str(o.get("record_id")) in p3_ids:
                payloads[str(o["record_id"])] = o

    items: list[dict] = []
    for rid, payload in payloads.items():
        sections = parse_topic_sections(payload.get("source_text") or "")
        # Concatenate the targeted topics
        chunks_text = []
        for t in RELEVANT_TOPICS:
            sec = (sections.get(t) or "").strip()
            if sec:
                chunks_text.append(f"=== {t} ===\n{sec}")
        targeted_source = "\n\n".join(chunks_text)
        if not targeted_source:
            # Fallback: full source if no targeted sections at all
            targeted_source = payload.get("source_text") or ""
        items.append({
            "item_id": f"p3_review_{rid}",
            "record_id": rid,
            "cao_number": payload.get("cao_number", ""),
            "file_name": payload.get("file_name", ""),
            "ingangsdatum": payload.get("ingangsdatum", ""),
            "current_csv_value": "True",  # hetero_present is True (that's why P3 fired)
            "deterministic_proposed": "False",  # Python wants to flip
            "task": (
                "Read the source content. Does it describe DISTINCT WORKER GROUPS "
                "(any kind: contract type, function/role, location, sector subgroup, "
                "etc.) with different leave or seniority terms — BEYOND age cohorts "
                "and full-time-vs-part-time? Answer yes or no."
            ),
            "targeted_source": targeted_source,
        })

    # Write flat JSONL
    out_jsonl = OUT_DIR / "p3_review_worksheets.jsonl"
    with out_jsonl.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

    # Chunk
    chunks: list[list[dict]] = []
    for i in range(0, len(items), ITEMS_PER_CHUNK):
        chunks.append(items[i : i + ITEMS_PER_CHUNK])

    rows: list[dict] = []
    for i, ch in enumerate(chunks):
        path = CHUNK_DIR / f"chunk_{i:03d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for it in ch:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
        rows.append({
            "chunk_id": f"chunk_{i:03d}",
            "n_items": len(ch),
            "total_chars": sum(len(it["targeted_source"]) for it in ch),
        })
    pd.DataFrame(rows).to_csv(OUT_DIR / "chunk_index.csv", sep=";", index=False)

    print(f"Total review items:  {len(items)}")
    print(f"Chunks (size={ITEMS_PER_CHUNK}): {len(chunks)}")
    print(f"Output: {out_jsonl}")
    avg_chars = sum(len(it["targeted_source"]) for it in items) / max(len(items), 1)
    print(f"Avg targeted_source chars per item: {avg_chars:.0f}")


if __name__ == "__main__":
    main()
