"""build_phase3_chunks.py — chunk the verifiable salary anomalies for subagent review.

Each chunk = one (cao, file_name), sliced to ≤ MAX_FLAGS flags per sub-chunk.
For each chunk we emit one self-contained JSON worksheet:

    qa/qa_salary/phase3/chunks/<chunk_id>.json

The chunk JSON contains everything the subagent needs to decide each flag WITHOUT
us telling it the answer:

    {
      "chunk_id": "c0042s01",
      "cao_number": "...",
      "file_name": "...",
      "schema_kind": "standard | compact_parallel | compact_nested_tl",
      "source_entries": [   # full file, normalized to standard schema
        { "jobgroup": ..., "step": ..., "worker": ...,
          "timeline": [ { "start_date": ..., "amount": ..., "unit": ..., "note": ..., "table_label": ...}, ... ] },
        ...
      ],
      "flags": [
        {
          "flag_id": "c0042s01_f03",
          "row_id": "...",
          "entry_index": 17,     # zero-based index into source_entries
          "salary_n": 2,         # 1-based; index into entry.timeline is (salary_n - 1)
          "field": "salary_2_amount",
          "csv_value": "...",
          "rule": "amount_out_of_range",     # bare rule, NO thresholds
          "csv_row_excerpt": {                # the CSV row's relevant cells, for context
              "jobgroup": ..., "step_label": ..., "worker_type": ...,
              "age_group": ..., "ft_hours": ..., "permanency": ..., "row_note": ...,
              "salary_1": {...}, "salary_2": {...}, "salary_3": {...}     # ±1 around the flagged N
          },
          "neighbor_entries": [16, 18]        # adjacent JSON entry indices for table context
        },
        ...
      ]
    }

We also write qa/qa_salary/phase3/chunks/manifest.csv listing every chunk's id,
file, flag count, JSON entry count — used by the dispatcher.

NEVER writes outside qa/.
"""
from __future__ import annotations
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

# Import the normalizer + schema detection from the scanner so chunks contain
# data in the SAME normalized form that the scanner reasoned about.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_anomalies import (                                                # noqa: E402
    normalize_entry, _schema_kind,
    META_FIELDS, TL_FIELDS,
)

ROOT = Path(__file__).resolve().parents[1]
PHASE3 = ROOT / "phase3"
CHUNKS = PHASE3 / "chunks"
CHUNKS.mkdir(parents=True, exist_ok=True)

ANOMALIES = ROOT / "outputs" / "salary_anomalies.csv"
try:
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR, LLM_ANALYSIS_SALARY_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"
    SAL_SRC = LLM_ANALYSIS_SALARY_DIR
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR, LLM_ANALYSIS_SALARY_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"
    SAL_SRC = LLM_ANALYSIS_SALARY_DIR

# Maximum flags per subagent worksheet. Chosen so the per-chunk JSON fits in
# ~30K input tokens: 50 flags × ~200 chars/flag + ≤100 sliced JSON entries
# (the touched ones plus a ±NEIGHBOR_WINDOW context window).
MAX_FLAGS = 50
# Per chunk, include each touched entry plus a window of neighbors so the
# subagent sees the surrounding table structure (jobgroup steps, year columns).
NEIGHBOR_WINDOW = 5

# Rules excluded from Phase 3 (informational only — one side just doesn't carry
# the field; not a CSV<->JSON disagreement we can adjudicate).
INFORMATIONAL_PREFIXES = (
    "meta_csv_only:", "meta_json_only:",
    "timeline_csv_only:", "timeline_json_only:",
    "timeline_overflow_in_csv",
)

# Strip rule details to give the subagent the bare name, no threshold.
# e.g. "amount_out_of_range:below_floor(800)" -> "amount_out_of_range"
#      "below_statutory(1934,ratio=0.43)"     -> "below_statutory_minimum"
def _bare_rule(rule: str) -> str:
    r = rule.split("(", 1)[0]
    r = r.split(":", 1)[0]
    if r == "below_statutory":
        return "below_statutory_minimum"
    if r.startswith("timeline_drop_gt_"):
        return "timeline_drop"
    return r


def _is_verifiable(rule: str) -> bool:
    return not any(rule.startswith(p) for p in INFORMATIONAL_PREFIXES)


def _csv_row_excerpt(row: dict, salary_n: int) -> dict:
    """Pull only the relevant CSV columns for one flagged cell.

    Includes meta + the flagged salary_N block + the ±1 neighbor blocks (so
    the subagent sees siblings in the row's timeline without being flooded by
    all 80 blocks)."""
    META_COLS = [c for c, _ in META_FIELDS] + ["row_note"]
    out = {c: row.get(c, "") for c in META_COLS if row.get(c, "") != ""}
    sal: dict = {}
    for n in (salary_n - 1, salary_n, salary_n + 1):
        if n < 1 or n > 80:
            continue
        block = {}
        for csvf, _ in TL_FIELDS:
            col = f"salary_{n}_{csvf}"
            v = row.get(col, "")
            if v != "":
                block[csvf] = v
        if block:
            sal[f"salary_{n}"] = block
    out.update(sal)
    return out


def _read_csv_rows_for_files(file_keys: set) -> dict:
    """Return {(cao, file_name): [csv_row_dict, …in CSV order]} for touched files.

    NOTE: the salary CSV's `id` column is the FILE id (every salary entry in
    one file shares it — e.g. id `1393012` appears 66 times). The unique
    per-row key is the sequential entry index within (cao, file_name), which
    also equals the index into the normalized JSON's salary_information array.
    Storing rows as an ordered list lets us look up by entry_idx safely.
    """
    out: dict = defaultdict(list)
    with open(SAL_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            k = (row["cao_number"].strip(), row["file_name"].strip())
            if k in file_keys:
                out[k].append(row)
    return out


def _load_and_normalize_file(cao: str, fname: str) -> tuple[str, list]:
    """Read the source JSON for one file and return (schema_kind, normalized entries)."""
    jpath = SAL_SRC / cao / f"{fname}_analysis.json"
    if not jpath.exists():
        return ("missing", [])
    si = json.load(open(jpath, encoding="utf-8")).get("salary_information", [])
    if not si:
        return ("empty", [])
    kind = _schema_kind(si[0])
    norm = [normalize_entry(e) for e in si]
    return (kind, norm)


def main() -> None:
    # 1. Read all verifiable flags grouped by (cao, file_name).
    flags_by_file: dict = defaultdict(list)
    with open(ANOMALIES, encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter=";"):
            if not _is_verifiable(r["rule"]):
                continue
            k = (r["cao_number"], r["file_name"])
            flags_by_file[k].append(r)
    file_keys = set(flags_by_file)
    print(f"verifiable flags : {sum(len(v) for v in flags_by_file.values())}")
    print(f"distinct files   : {len(file_keys)}")

    # 2. Pull the CSV row content for every touched file (ordered list per file).
    print("reading flagged CSV rows ...", flush=True)
    csv_rows = _read_csv_rows_for_files(file_keys)

    # 4. Walk files, normalize JSON once, slice flags into ≤MAX_FLAGS sub-chunks.
    manifest = []
    chunk_idx = 0
    skipped_files = 0
    for file_no, ((cao, fname), flag_rows) in enumerate(
        sorted(flags_by_file.items(), key=lambda x: x[0]), start=1
    ):
        schema, entries = _load_and_normalize_file(cao, fname)
        if not entries:
            skipped_files += 1
            print(f"  [skip] {cao}/{fname[:50]}  schema={schema}")
            continue

        # split flags into sub-chunks of ≤ MAX_FLAGS each
        for sub_no, start in enumerate(range(0, len(flag_rows), MAX_FLAGS), start=1):
            chunk_idx += 1
            sub_flags = flag_rows[start:start + MAX_FLAGS]
            chunk_id = f"c{chunk_idx:04d}"
            if len(flag_rows) > MAX_FLAGS:
                chunk_id += f"s{sub_no:02d}"

            chunk_flags = []
            rows_for_file = csv_rows.get((cao, fname), [])
            for i_in_chunk, fl in enumerate(sub_flags, start=1):
                rid = fl["row_id"]                          # FILE id, not row id
                try:
                    ent_idx = int(fl["entry_idx"])
                except (KeyError, ValueError):
                    ent_idx = -1
                # Look up CSV row by entry_idx (unique per row within a file).
                row = rows_for_file[ent_idx] if 0 <= ent_idx < len(rows_for_file) else None
                if row is None:
                    csv_excerpt = {}
                else:
                    n = int(fl["salary_n"]) if fl["salary_n"].strip() else 0
                    csv_excerpt = _csv_row_excerpt(row, n) if n else {
                        c: row.get(c, "") for c, _ in META_FIELDS if row.get(c, "") != ""
                    }

                neighbors = [j for j in (ent_idx - 1, ent_idx + 1)
                             if 0 <= j < len(entries) and j != ent_idx]

                chunk_flags.append({
                    "flag_id": f"{chunk_id}_f{i_in_chunk:02d}",
                    "row_id": rid,
                    "entry_index": ent_idx,
                    "salary_n": int(fl["salary_n"]) if fl["salary_n"].strip() else None,
                    "field": fl["field"],
                    "csv_value": fl["csv_value"],
                    "rule": _bare_rule(fl["rule"]),
                    "csv_row_excerpt": csv_excerpt,
                    "neighbor_entries": neighbors,
                })

            # Slice source_entries to ONLY the indices this chunk touches plus
            # a NEIGHBOR_WINDOW context band. Cuts median chunk size 3-5x for
            # files with hundreds of entries split across many sub-chunks.
            touched: set = set()
            for cf in chunk_flags:
                idx = cf["entry_index"]
                if idx < 0:
                    continue
                lo = max(0, idx - NEIGHBOR_WINDOW)
                hi = min(len(entries), idx + NEIGHBOR_WINDOW + 1)
                touched.update(range(lo, hi))
            # entries get an explicit "_entry_index" so the subagent can
            # cross-reference flag.entry_index to the right entry in the slice.
            sliced = [
                {"_entry_index": i, **entries[i]}
                for i in sorted(touched)
            ]
            chunk_obj = {
                "chunk_id": chunk_id,
                "cao_number": cao,
                "file_name": fname,
                "schema_kind": schema,
                "n_entries_total": len(entries),
                "n_entries_included": len(sliced),
                "source_entries": sliced,
                "flags": chunk_flags,
            }
            out_path = CHUNKS / f"{chunk_id}.json"
            out_path.write_text(json.dumps(chunk_obj, ensure_ascii=False, indent=2),
                                encoding="utf-8")
            manifest.append({
                "chunk_id": chunk_id,
                "cao_number": cao,
                "file_name": fname,
                "schema_kind": schema,
                "n_flags": len(sub_flags),
                "n_entries": len(entries),
                "size_kb": round(out_path.stat().st_size / 1024, 1),
            })
        if file_no % 25 == 0:
            print(f"  ... {file_no}/{len(flags_by_file)} files chunked, {chunk_idx} chunks so far")

    # 5. Manifest.
    with open(CHUNKS / "manifest.csv", "w", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(manifest[0].keys()), delimiter=";")
        w.writeheader()
        w.writerows(manifest)

    # 6. Summary.
    sizes = [m["size_kb"] for m in manifest]
    sizes.sort()
    flagcounts = [m["n_flags"] for m in manifest]
    flagcounts.sort(reverse=True)
    n_chunks = len(manifest)
    print()
    print(f"chunks written           : {n_chunks}")
    print(f"files skipped (no JSON)  : {skipped_files}")
    print(f"chunk size (KB)          : min={sizes[0]} med={sizes[n_chunks//2]} max={sizes[-1]}")
    print(f"flags per chunk          : max={flagcounts[0]} (cap={MAX_FLAGS})")
    print(f"total flags in chunks    : {sum(flagcounts)}")
    print(f"\nwrote {CHUNKS}/c*.json + manifest.csv")


if __name__ == "__main__":
    main()
