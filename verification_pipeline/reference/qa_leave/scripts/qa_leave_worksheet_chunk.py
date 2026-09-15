"""Chunk worksheets.jsonl into subagent-sized batches (~30 items each)."""
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
WS_DIR = ROOT / "outputs" / "subagent_worksheets"
INPUT = WS_DIR / "worksheets.jsonl"
CHUNK_DIR = WS_DIR / "chunks"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

ITEMS_PER_CHUNK = 30


def main() -> None:
    records = [json.loads(l) for l in INPUT.read_text(encoding="utf-8").splitlines() if l.strip()]
    # Sort by record_id for stability
    records.sort(key=lambda r: int(r["record_id"]) if str(r["record_id"]).isdigit() else 0)

    chunks: list[list[dict]] = []
    cur: list[dict] = []
    cur_count = 0
    for r in records:
        n = len(r["items"])
        if cur_count + n > ITEMS_PER_CHUNK and cur:
            chunks.append(cur)
            cur, cur_count = [], 0
        cur.append(r)
        cur_count += n
    if cur:
        chunks.append(cur)

    rows: list[dict] = []
    for i, chunk in enumerate(chunks):
        path = CHUNK_DIR / f"chunk_{i:03d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for r in chunk:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        rows.append({
            "chunk_id": f"chunk_{i:03d}",
            "chunk_path": str(path.relative_to(ROOT)),
            "n_records": len(chunk),
            "n_items": sum(len(r["items"]) for r in chunk),
        })

    pd.DataFrame(rows).to_csv(CHUNK_DIR / "chunk_index.csv", sep=";", index=False)
    print(f"Total records: {len(records)}")
    print(f"Total items:   {sum(len(r['items']) for r in records)}")
    print(f"Chunks (target {ITEMS_PER_CHUNK} items each): {len(chunks)}")


if __name__ == "__main__":
    main()
