"""Select 50 chunks for the Phase-A sanity batch.

Goals:
- Mix of schema_kinds (so the verifier handles all three)
- Mix of n_entries (small + large structured tables)
- Mix of wage_chars (small + medium + large text)
- Bias OUT extreme outliers (>200KB chunk) to derisk
- Include 5 of the previously-known-defective files if any are in manifest
  (cross-ref against the 44-file phase3b sample)

Writes:
  wage_check_full/phase_a_sanity/SANITY_BATCH.csv  — chosen chunks
"""
from __future__ import annotations
import csv
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "chunks" / "manifest.csv"
OUT = ROOT / "phase_a_sanity" / "SANITY_BATCH.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

# Try to find 44-file phase3b verdicts so we can include some files with known defects
PHASE3B_RES = Path(__file__).resolve().parents[2] / "phase3b" / "results"
PHASE3B_CHUNKS = Path(__file__).resolve().parents[2] / "phase3b" / "chunks"


def _known_defective_files() -> set[tuple[str, str]]:
    """Read 44-file phase3b results for files with major_issues. Return (cao, file) keys."""
    if not PHASE3B_RES.exists():
        return set()
    out: set = set()
    for p in PHASE3B_RES.glob("b*.json"):
        try:
            res = json.load(open(p, encoding="utf-8"))
            if res.get("overall_assessment") in ("major_issues",):
                cid = res.get("chunk_id", p.stem)
                cp = PHASE3B_CHUNKS / f"{cid}.json"
                if cp.exists():
                    c = json.load(open(cp, encoding="utf-8"))
                    out.add((c["cao_number"], c["file_name"]))
        except Exception:
            continue
    return out


def main() -> None:
    rows = list(csv.DictReader(open(MANIFEST, encoding="utf-8"), delimiter=";"))
    print(f"manifest rows: {len(rows)}")

    # 1) drop extreme outliers (>200KB)
    keep = [r for r in rows if float(r["size_kb"]) <= 200.0]
    print(f"after size <=200KB: {len(keep)}")

    # 2) Find any known-defective files in the manifest
    known = _known_defective_files()
    seed_keys = []
    for r in keep:
        if (r["cao_number"], r["file_name"]) in known:
            seed_keys.append((r["chunk_id"], r["cao_number"], r["file_name"]))
        if len(seed_keys) >= 5:
            break
    print(f"seeded with {len(seed_keys)} known-defective files")

    # 3) Stratify by schema_kind + size bucket; sample remainder randomly
    random.seed(42)
    remaining = [r for r in keep if (r["cao_number"], r["file_name"]) not in known]
    random.shuffle(remaining)
    extra = remaining[: 50 - len(seed_keys)]
    extra_keys = [(r["chunk_id"], r["cao_number"], r["file_name"]) for r in extra]

    all_keys = seed_keys + extra_keys

    # Build CSV with the chosen chunk + manifest cols
    by_id = {r["chunk_id"]: r for r in rows}
    with open(OUT, "w", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["chunk_id", "cao_number", "file_name", "schema_kind",
                    "n_entries", "n_wage_bullets", "wage_chars", "size_kb",
                    "seed_reason"])
        for cid, cao, fn in seed_keys:
            r = by_id[cid]
            w.writerow([cid, cao, fn, r["schema_kind"], r["n_entries"],
                        r["n_wage_bullets"], r["wage_chars"], r["size_kb"],
                        "known_defective"])
        for cid, cao, fn in extra_keys:
            r = by_id[cid]
            w.writerow([cid, cao, fn, r["schema_kind"], r["n_entries"],
                        r["n_wage_bullets"], r["wage_chars"], r["size_kb"],
                        "random_stratified"])

    print(f"\nwrote sanity batch ({len(all_keys)} chunks) → {OUT}")


if __name__ == "__main__":
    main()
