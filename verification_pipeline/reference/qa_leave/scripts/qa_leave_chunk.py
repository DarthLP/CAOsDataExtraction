"""
qa_leave_chunk.py — split matched payloads into subagent-sized chunks.

Reads:  qa_leave/outputs/leave_qa_payloads.jsonl
Writes:
  qa_leave/outputs/subagent_chunks/chunk_{NN}.jsonl   (one record per line)
  qa_leave/outputs/subagent_chunks/chunk_index.csv    (chunk -> records mapping)

Only records with source_status == 'matched' are sent to subagents.
Records with source_status == 'source_unavailable' are dumped to a separate file
qa_leave/outputs/subagent_chunks/_source_unavailable.jsonl so they're auditable
but won't be judged.

Default chunk size: 30 records. Tune via CHUNK_SIZE.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PAYLOAD_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
CHUNK_DIR = ROOT / "outputs" / "subagent_chunks"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)
INDEX_PATH = CHUNK_DIR / "chunk_index.csv"
NA_PATH = CHUNK_DIR / "_source_unavailable.jsonl"

CHUNK_SIZE = 30


def run() -> None:
    # Load and split
    matched: list[dict] = []
    unavailable: list[dict] = []
    with PAYLOAD_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            (matched if o.get("source_status") == "matched" else unavailable).append(o)

    # Sort matched for stability: by cao_number then record_id
    def key(o):
        try:
            return (int(str(o.get("cao_number") or "0")), str(o.get("record_id") or ""))
        except ValueError:
            return (10**9, str(o.get("record_id") or ""))

    matched.sort(key=key)

    # Write unavailable file
    with NA_PATH.open("w", encoding="utf-8") as f:
        for o in unavailable:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")

    # Chunk
    n_chunks = (len(matched) + CHUNK_SIZE - 1) // CHUNK_SIZE
    rows: list[dict] = []
    for i in range(n_chunks):
        chunk = matched[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
        chunk_path = CHUNK_DIR / f"chunk_{i:03d}.jsonl"
        with chunk_path.open("w", encoding="utf-8") as f:
            for o in chunk:
                f.write(json.dumps(o, ensure_ascii=False) + "\n")
        rows.append({
            "chunk_id": f"chunk_{i:03d}",
            "chunk_path": str(chunk_path.relative_to(ROOT)),
            "n_records": len(chunk),
            "first_record_id": chunk[0]["record_id"],
            "last_record_id": chunk[-1]["record_id"],
            "first_cao": chunk[0]["cao_number"],
            "last_cao": chunk[-1]["cao_number"],
        })

    pd.DataFrame(rows).to_csv(INDEX_PATH, sep=";", index=False)

    print(f"Matched records:      {len(matched)}")
    print(f"Source-unavailable:   {len(unavailable)}  ->  {NA_PATH}")
    print(f"Chunks (size={CHUNK_SIZE}):  {n_chunks}")
    print(f"Index:                {INDEX_PATH}")


if __name__ == "__main__":
    run()
