"""build_re_extraction_inputs.py — bundle wage_information text for each test file.

For each (cao, filename) we want re-extracted, write a JSON containing only:
  - filename
  - wage_information_text (list of bullets from llm_extracted/new_flow/<cao>/<file>_extract.json)

Subagents read this + the patched prompt and produce a structured salary JSON.

Usage:
  python3 scripts/build_re_extraction_inputs.py b0007 b0011 b0014 b0022 b0048
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHUNKS = ROOT / "phase3b" / "chunks"
INPUTS = ROOT / "re_extraction" / "v1_inputs"
INPUTS.mkdir(parents=True, exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("chunk_ids", nargs="+", help="phase3b chunk ids, e.g. b0007 b0011")
    args = ap.parse_args()
    for cid in args.chunk_ids:
        cf = CHUNKS / f"{cid}.json"
        if not cf.exists():
            print(f"skip {cid}: chunk file not found"); continue
        c = json.load(open(cf, encoding="utf-8"))
        out = {
            "chunk_id": cid,
            "filename": c["file_name"],
            "cao_number": c["cao_number"],
            "wage_information_text": c["wage_information_text"],
        }
        outpath = INPUTS / f"{cid}.json"
        outpath.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  wrote {outpath.name}  ({len(out['wage_information_text'])} bullets, "
              f"{sum(len(s) for s in out['wage_information_text'])} chars)")


if __name__ == "__main__":
    main()
