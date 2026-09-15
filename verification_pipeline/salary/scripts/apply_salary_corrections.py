"""apply_salary_corrections.py — write the 16 unit normalizations to a COPY of
the salary CSV. The source CSV under CAOsDataExtraction/ is NEVER modified.

Phase 3 subagent verification produced 16 high-confidence CORRECT_TO verdicts,
all for the same pattern: `salary_N_unit = 'd'` (a stray compact-schema short
code) → `'daily'` (the canonical form already present in the source JSON).

Safety gates (mirroring qa/apply_corrections.py for the non-salary side):
  1. Match ONLY on the unique per-file (entry_idx, file_name, cao_number) — the
     salary CSV's `id` column is the file id, NOT a row id, so it cannot be
     used alone (the same id repeats up to 66 times within one file).
  2. Verify the target cell currently equals 'd' before writing.
  3. Only the single named cell of the single matched row is ever written.

Outputs (all under qa/qa_salary/outputs/):
  corrected_salary.csv        — full salary CSV with the 16 cells flipped
  apply_salary_changelog.csv  — one row per applied change
  apply_salary_skipped.csv    — rows that failed safety gates (should be empty)
"""
from __future__ import annotations
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs"
PHASE3 = ROOT / "phase3"
try:
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"

csv.field_size_limit(10_000_000)


def collect_corrections() -> list[dict]:
    """Walk phase3/results, pull every CORRECT_TO verdict + its chunk metadata."""
    corrections: list[dict] = []
    for res_path in sorted((PHASE3 / "results").glob("*.json")):
        result = json.load(open(res_path, encoding="utf-8"))
        chunk = json.load(open(PHASE3 / "chunks" / f"{res_path.stem}.json", encoding="utf-8"))
        flag_map = {f["flag_id"]: f for f in chunk["flags"]}
        entry_map = {e["_entry_index"]: e for e in chunk["source_entries"]}
        for v in result.get("verdicts", []):
            if v.get("verdict") != "CORRECT_TO":
                continue
            f = flag_map.get(v["flag_id"])
            if f is None:
                continue
            # For unit-normalization corrections we re-derive the JSON value
            # from the source entries (the subagent's `json_supported_value`
            # was sometimes confused with the timeline amount — see c0032).
            ent = entry_map.get(f["entry_index"])
            tl = (ent or {}).get("timeline", [])
            sn = f["salary_n"]
            json_unit = tl[sn - 1].get("unit") if (sn and 0 <= sn - 1 < len(tl)) else None
            field = f["field"]
            new_value: str | None = None
            if field.endswith("_unit") and json_unit is not None:
                new_value = json_unit
            else:
                # Fall back to the subagent's proposal if it isn't a unit cell.
                new_value = v.get("json_supported_value")
            if new_value is None:
                continue
            corrections.append({
                "chunk_id": res_path.stem,
                "flag_id": v["flag_id"],
                "cao_number": chunk["cao_number"],
                "file_name": chunk["file_name"],
                "entry_idx": int(f["entry_index"]),
                "salary_n": sn,
                "field": field,
                "expected_current": f["csv_value"],
                "new_value": str(new_value),
                "rule": f["rule"],
                "confidence": v.get("confidence", ""),
            })
    return corrections


def main() -> None:
    corrections = collect_corrections()
    print(f"corrections collected from verdicts: {len(corrections)}")

    # Build lookup keyed on (cao, file_name, entry_idx). The entry_idx is the
    # 0-based sequential position of the CSV row within its (cao, file_name)
    # group, matching the JSON's salary_information index.
    target = defaultdict(list)
    for c in corrections:
        target[(c["cao_number"], c["file_name"])].append(c)

    changes: list[dict] = []
    skipped: list[dict] = []
    out_path = OUT_DIR / "corrected_salary.csv"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"reading + writing {SAL_CSV.name} ...", flush=True)
    file_row_idx: dict = defaultdict(int)
    with open(SAL_CSV, encoding="utf-8") as fin, \
         open(out_path, "w", encoding="utf-8") as fout:
        reader = csv.DictReader(fin, delimiter=";")
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames, delimiter=";")
        writer.writeheader()
        n_total = 0
        for row in reader:
            n_total += 1
            cao = row.get("cao_number", "").strip()
            fname = row.get("file_name", "").strip()
            key = (cao, fname)
            cur_idx = file_row_idx[key]
            file_row_idx[key] += 1

            for c in target.get(key, []):
                if c["entry_idx"] != cur_idx:
                    continue
                col = c["field"]
                if col not in row:
                    skipped.append({**c, "reason": "no_such_column"})
                    continue
                cur = row[col]
                if cur.strip() != c["expected_current"]:
                    skipped.append({**c, "reason": f"current_mismatch(base={cur!r},expected={c['expected_current']!r})"})
                    continue
                row[col] = c["new_value"]
                changes.append({**c, "old_value": cur, "applied": True})
            writer.writerow(row)
            if n_total % 50000 == 0:
                print(f"  ... {n_total} rows processed, {len(changes)} cells flipped", flush=True)

    # Write changelog + skipped (skipped should be empty if all gates pass)
    ch_path = OUT_DIR / "apply_salary_changelog.csv"
    sk_path = OUT_DIR / "apply_salary_skipped.csv"
    with open(ch_path, "w", encoding="utf-8") as f:
        if changes:
            w = csv.DictWriter(f, fieldnames=list(changes[0].keys()), delimiter=";")
            w.writeheader(); w.writerows(changes)
        else:
            f.write("(no changes)\n")
    with open(sk_path, "w", encoding="utf-8") as f:
        if skipped:
            w = csv.DictWriter(f, fieldnames=list(skipped[0].keys()), delimiter=";")
            w.writeheader(); w.writerows(skipped)

    print()
    print(f"rows processed   : {n_total}")
    print(f"cells changed    : {len(changes)}")
    print(f"cells skipped    : {len(skipped)}")
    print(f"\nwrote {out_path}")
    print(f"wrote {ch_path}")
    print(f"wrote {sk_path}")


if __name__ == "__main__":
    main()
