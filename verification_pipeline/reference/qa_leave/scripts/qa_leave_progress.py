"""
qa_leave_progress.py — track which correction-subagent chunks are done.

Run anytime to see current state. If a session runs out of context, the next
session can run this script and resume from the next un-done chunk.

Usage:
  python3 scripts/qa_leave_progress.py
"""
from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHUNKS_DIR = ROOT / "outputs" / "subagent_worksheets" / "chunks"

# Index of all chunks we expect (matches the chunker output)
INDEX_PATH = CHUNKS_DIR / "chunk_index.csv"


def main() -> None:
    if not CHUNKS_DIR.exists():
        print("No chunks directory yet — run qa_leave_worksheet_chunk.py first.")
        return

    # Find all chunk_NNN.jsonl files
    inputs = sorted(CHUNKS_DIR.glob("chunk_*.jsonl"))
    chunk_ids = []
    for p in inputs:
        m = re.match(r"chunk_(\d{3})\.jsonl$", p.name)
        if m:
            chunk_ids.append(int(m.group(1)))
    chunk_ids.sort()
    total = len(chunk_ids)

    # Find completed chunks (have a *_corrections.csv)
    done = set()
    for p in CHUNKS_DIR.glob("chunk_*_corrections.csv"):
        m = re.match(r"chunk_(\d{3})_corrections\.csv$", p.name)
        if m:
            done.add(int(m.group(1)))

    pending = [c for c in chunk_ids if c not in done]
    pct = 100 * len(done) / max(total, 1)

    print(f"Total chunks:  {total}")
    print(f"Done:          {len(done)}  ({pct:.1f}%)")
    print(f"Remaining:     {len(pending)}")
    print()
    if done:
        print(f"Completed chunk IDs: {sorted(done)[:5]}{'...' if len(done) > 5 else ''} (last: {max(done)})")
    if pending:
        print(f"Next 10 to run:      {pending[:10]}")
        # Print copy-paste-friendly chunk paths for next batch
        print()
        print("Paths for next batch (10):")
        for c in pending[:10]:
            print(f"  outputs/subagent_worksheets/chunks/chunk_{c:03d}.jsonl")
    else:
        print("ALL CHUNKS DONE.")


if __name__ == "__main__":
    main()
