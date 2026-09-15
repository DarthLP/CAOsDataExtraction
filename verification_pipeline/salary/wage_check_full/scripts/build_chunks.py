"""build_chunks.py — build verification chunks for ALL 242 (cao, file) pairs.

One JSON chunk per file. Each chunk contains:
  - cao_number, file_name, chunk_id (stable, derived from cao+file)
  - schema_kind          — standard | compact_parallel | compact_nested_tl
  - n_entries            — # salary_information rows in v0
  - wage_information_text — list[str], one bullet per element (input to step 2)
  - structured_salary    — normalized salary_information from v0

The chunk_id is `c<NNNN>` where NNNN is the sorted index across all 242 files.
This is deterministic — re-running the script always produces the same id
for the same (cao, file_name).

Outputs:
  wage_check_full/chunks/<chunk_id>.json
  wage_check_full/chunks/manifest.csv     ← cao, file_name, chunk_id, n_entries, n_wage_bullets, size_kb
"""
from __future__ import annotations
import csv
import json
import sys
from pathlib import Path

# Re-use the normalize_entry / _schema_kind helpers from qa_salary/scripts/
QA_SALARY_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(QA_SALARY_SCRIPTS))
from scan_anomalies import normalize_entry, _schema_kind         # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CHUNKS = ROOT / "chunks"
CHUNKS.mkdir(parents=True, exist_ok=True)

try:
    from repo_paths import EXTRACTION_ROOT, LLM_EXTRACTED_DIR, LLM_ANALYSIS_SALARY_DIR
    PY = EXTRACTION_ROOT
    NEW_FLOW = LLM_EXTRACTED_DIR
    SAL_SRC  = LLM_ANALYSIS_SALARY_DIR
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from repo_paths import EXTRACTION_ROOT, LLM_EXTRACTED_DIR, LLM_ANALYSIS_SALARY_DIR
    PY = EXTRACTION_ROOT
    NEW_FLOW = LLM_EXTRACTED_DIR
    SAL_SRC  = LLM_ANALYSIS_SALARY_DIR


def _wage_info_text(cao: str, fname: str) -> list[str] | None:
    p = NEW_FLOW / cao / f"{fname}_extract.json"
    if not p.exists():
        return None
    try:
        d = json.load(open(p, encoding="utf-8"))
    except Exception:
        return None
    wi = d.get("wage_information", []) or []
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
    pairs: list[tuple[str, str]] = []
    for ext in NEW_FLOW.rglob("*_extract.json"):
        cao = ext.parent.name
        fname = ext.stem.removesuffix("_extract")
        if (SAL_SRC / cao / f"{fname}_analysis.json").exists():
            pairs.append((cao, fname))
    return sorted(pairs)


def build() -> None:
    files = _list_files()
    print(f"files with BOTH new_flow + salary_analysis: {len(files)}")

    manifest = []
    skipped = []
    for i, (cao, fname) in enumerate(files, start=1):
        wi = _wage_info_text(cao, fname)
        kind, entries = _structured_salary(cao, fname)
        if not wi:
            skipped.append((cao, fname, "no_wage_info"))
            continue
        if not entries:
            skipped.append((cao, fname, f"structured:{kind}"))
            continue
        chunk_id = f"c{i:04d}"
        chunk = {
            "chunk_id": chunk_id,
            "cao_number": cao,
            "file_name": fname,
            "schema_kind": kind,
            "n_entries": len(entries),
            "wage_information_text": wi,
            "structured_salary": entries,
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
        med = sizes[len(sizes)//2]
        p95 = sizes[int(len(sizes)*0.95)] if len(sizes) >= 20 else sizes[-1]
        print(f"chunk size KB : min={sizes[0]} med={med} p95={p95} max={sizes[-1]}")
        print(f"wage chars    : min={waged[0]} med={waged[len(waged)//2]} max={waged[-1]}")
    print(f"\nwrote {CHUNKS}/c*.json + manifest.csv")


if __name__ == "__main__":
    build()
