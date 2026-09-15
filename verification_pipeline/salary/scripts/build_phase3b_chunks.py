"""build_phase3b_chunks.py — chunks for verifying step 2 (text → Pydantic).

For each (cao, file_name), bundle exactly two things:

  1. wage_information_text  — the bullet-list text from
     llm_extracted/new_flow/<cao>/<file>_extract.json
     (NOT the full extract.json — only the wage_information array, to keep tokens down)

  2. structured_salary      — the salary_information array from
     llm_analysis/salary/<cao>/<file>_analysis.json
     (normalized to the standard schema so the subagent doesn't deal with
     three variants; same normalize_entry() used in Phase 1/3)

The subagent's job is to read the text, read the structured table, and tell us
whether step 2 (the Pydantic extraction) faithfully encoded what the text
describes — NOT whether the original LLM read the PDF correctly.

By default, --mode=sample writes 50 random chunks (cheap sanity pass).
--mode=flagged writes one chunk per file that has Phase-1 anomalies (~180
files). --mode=all writes one per non-empty file (2,728).

Outputs:
  qa/qa_salary/phase3b/chunks/<chunk_id>.json
  qa/qa_salary/phase3b/chunks/manifest.csv
"""
from __future__ import annotations
import argparse
import csv
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_anomalies import normalize_entry, _schema_kind         # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PHASE3B = ROOT / "phase3b"
CHUNKS = PHASE3B / "chunks"
CHUNKS.mkdir(parents=True, exist_ok=True)
ANOMALIES = ROOT / "outputs" / "salary_anomalies.csv"

try:
    from repo_paths import EXTRACTION_ROOT, LLM_EXTRACTED_DIR, LLM_ANALYSIS_SALARY_DIR
    PY = EXTRACTION_ROOT
    NEW_FLOW = LLM_EXTRACTED_DIR
    SAL_SRC  = LLM_ANALYSIS_SALARY_DIR
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from repo_paths import EXTRACTION_ROOT, LLM_EXTRACTED_DIR, LLM_ANALYSIS_SALARY_DIR
    PY = EXTRACTION_ROOT
    NEW_FLOW = LLM_EXTRACTED_DIR
    SAL_SRC  = LLM_ANALYSIS_SALARY_DIR


def _wage_info_text(cao: str, fname: str) -> list[str] | None:
    """Return the wage_information bullets as a list of strings, or None if missing."""
    p = NEW_FLOW / cao / f"{fname}_extract.json"
    if not p.exists():
        return None
    try:
        d = json.load(open(p, encoding="utf-8"))
    except Exception:
        return None
    wi = d.get("wage_information", []) or []
    # bullets are usually [["text"], ["text"], ...] — flatten to a list of strings
    out: list[str] = []
    for entry in wi:
        if isinstance(entry, list):
            for s in entry:
                if isinstance(s, str) and s.strip():
                    out.append(s.strip())
        elif isinstance(entry, str) and entry.strip():
            out.append(entry.strip())
    return out


def _structured_salary(cao: str, fname: str) -> tuple[str | None, list]:
    """Load + normalize the salary_information from llm_analysis."""
    p = SAL_SRC / cao / f"{fname}_analysis.json"
    if not p.exists():
        return ("missing", [])
    try:
        si = json.load(open(p, encoding="utf-8")).get("salary_information", []) or []
    except Exception:
        return ("error", [])
    if not si:
        return ("empty", [])
    kind = _schema_kind(si[0])
    return (kind, [normalize_entry(e) for e in si])


def _list_files() -> list[tuple[str, str]]:
    """All (cao, fname) for which BOTH a new_flow extract and a salary analysis exist."""
    pairs: list[tuple[str, str]] = []
    for ext in NEW_FLOW.rglob("*_extract.json"):
        cao = ext.parent.name
        fname = ext.stem.removesuffix("_extract")
        if (SAL_SRC / cao / f"{fname}_analysis.json").exists():
            pairs.append((cao, fname))
    return pairs


def _flagged_files() -> set[tuple[str, str]]:
    if not ANOMALIES.exists():
        return set()
    out: set = set()
    with open(ANOMALIES, encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter=";"):
            out.add((r["cao_number"], r["file_name"]))
    return out


def build(mode: str, sample_n: int) -> None:
    files = _list_files()
    print(f"files with BOTH new_flow + salary_analysis: {len(files)}")

    if mode == "sample":
        random.seed(42)
        pool = list(files)
        random.shuffle(pool)
        files = pool[:sample_n]
    elif mode == "flagged":
        flagged = _flagged_files()
        files = [f for f in files if f in flagged]
    # else "all" -> keep everything

    print(f"selected for {mode}: {len(files)} files")

    manifest = []
    skipped = []
    for i, (cao, fname) in enumerate(sorted(files), start=1):
        wi = _wage_info_text(cao, fname)
        kind, entries = _structured_salary(cao, fname)
        if not wi:
            skipped.append((cao, fname, "no_wage_info"))
            continue
        if not entries:
            skipped.append((cao, fname, f"structured:{kind}"))
            continue
        chunk_id = f"b{i:04d}"
        chunk = {
            "chunk_id": chunk_id,
            "cao_number": cao,
            "file_name": fname,
            "schema_kind": kind,
            "n_entries": len(entries),
            "wage_information_text": wi,         # list[str], one bullet per element
            "structured_salary": entries,        # normalized salary_information[]
        }
        out_path = CHUNKS / f"{chunk_id}.json"
        out_path.write_text(json.dumps(chunk, ensure_ascii=False, indent=2),
                            encoding="utf-8")
        manifest.append({
            "chunk_id": chunk_id,
            "cao_number": cao,
            "file_name": fname,
            "schema_kind": kind,
            "n_entries": len(entries),
            "n_wage_bullets": len(wi),
            "wage_chars": sum(len(s) for s in wi),
            "size_kb": round(out_path.stat().st_size / 1024, 1),
        })

    if manifest:
        with open(CHUNKS / "manifest.csv", "w", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(manifest[0].keys()), delimiter=";")
            w.writeheader(); w.writerows(manifest)
    if skipped:
        with open(CHUNKS / "skipped.csv", "w", encoding="utf-8") as f:
            f.write("cao;file_name;reason\n")
            for s in skipped:
                f.write(";".join(s) + "\n")

    sizes = sorted(m["size_kb"] for m in manifest)
    waged = sorted(m["wage_chars"] for m in manifest)
    print(f"\nchunks written: {len(manifest)}")
    print(f"skipped       : {len(skipped)}")
    if sizes:
        print(f"chunk size KB : min={sizes[0]} med={sizes[len(sizes)//2]} p99={sizes[int(len(sizes)*0.99)] if len(sizes)>=100 else sizes[-1]} max={sizes[-1]}")
        print(f"wage chars    : min={waged[0]} med={waged[len(waged)//2]} max={waged[-1]}")
    print(f"\nwrote {CHUNKS}/b*.json + manifest.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["sample", "flagged", "all"], default="sample")
    ap.add_argument("--sample-n", type=int, default=50)
    args = ap.parse_args()
    build(args.mode, args.sample_n)
