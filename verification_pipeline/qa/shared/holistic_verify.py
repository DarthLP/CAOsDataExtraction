"""holistic_verify.py — pipeline Stage 4.6: holistic per-record verification.

Closes the one gap L2 can't reach. L2 only scans EMPTY fields (missing values); the
guard chain + audit + era-floor + Stage-4.5 re-check only touch *changed* cells. So an
**already-populated** extractor value that is simply WRONG is never re-examined.

This stage takes, per record, ALL populated `<topic>_*` fields and the COMPLETE source
section, and asks a subagent: "is each of these extracted values correct/supported?"
→ CONFIRM / NEEDS_CHANGE / UNSUPPORTED per field. Surfacing only — never auto-applied.

Usage:
  python3.13 -m qa.shared.holistic_verify build <topic> [rid1,rid2,...]   # optional record subset (demo)
  python3.13 -m qa.shared.holistic_verify consolidate <topic>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from qa.shared import source_text_loader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CHAR_BUDGET = 60000          # full sections; ~few records/chunk
HARD_SECTION_CAP = 250000
_BLANK = {"", "nan", "none", "null", "n/a", "unknown"}


def _qa(topic: str) -> Path:
    return PROJECT_ROOT / "qa" / f"qa_{topic}"


def _populated_fields(row: dict, topic: str) -> list[dict]:
    out = []
    for k, v in row.items():
        if not k.startswith(f"{topic}_"):
            continue
        if str(v).strip().lower() in _BLANK:
            continue
        out.append({"field": k, "value": str(v)})
    return out


def build(topic: str, only_records: list[str] | None = None) -> None:
    source_text_loader.clear_cache()
    qa = _qa(topic)
    scoped = pd.read_csv(qa / "inputs" / "scoped_records.csv", sep=";", dtype=str, keep_default_na=False)
    chunks_dir = qa / "holistic" / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    for f in chunks_dir.glob("*.jsonl"):
        f.unlink()

    items = []
    for _, r in scoped.iterrows():
        rid = str(r.get("id", "")).strip()
        if only_records and rid not in only_records:
            continue
        fields = _populated_fields(r.to_dict(), topic)
        if not fields:
            continue
        full = source_text_loader.load_source_text(topic, rid)[:HARD_SECTION_CAP]
        items.append({"topic": topic, "record_id": rid,
                      "ingangsdatum": str(r.get("ingangsdatum", "")),
                      "extracted_fields": fields, "full_source_text": full})

    n, cur, cc = 0, [], 0
    def flush():
        nonlocal n, cur, cc
        if not cur:
            return
        n += 1
        (chunks_dir / f"hv_chunk_{n:03d}.jsonl").write_text(
            "\n".join(json.dumps(x, ensure_ascii=False) for x in cur) + "\n", encoding="utf-8")
        cur, cc = [], 0
    for it in items:
        c = len(it["full_source_text"]) + 200 * len(it["extracted_fields"]) + 500
        if cur and cc + c > CHAR_BUDGET:
            flush()
        cur.append(it); cc += c
    flush()
    nfields = sum(len(it["extracted_fields"]) for it in items)
    print(f"[{topic}] holistic: {len(items)} records / {nfields} populated fields -> {n} chunks"
          + (f" (subset {only_records})" if only_records else ""))


KEYS = ["topic", "record_id", "field", "disposition", "current_value",
        "corrected_value", "reason", "evidence_quote", "confidence"]


def consolidate(topic: str) -> None:
    qa = _qa(topic)
    out_dir = qa / "holistic" / "outputs"
    rows, bad = [], []
    for f in sorted(out_dir.glob("hv_chunk_*_verified.jsonl")):
        for ln, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError as e:
                bad.append(f"{f.name}:{ln} {e}"); continue
            rows.append({k: o.get(k, "") for k in KEYS})
    df = pd.DataFrame(rows, columns=KEYS)
    order = {"NEEDS_CHANGE": 0, "UNSUPPORTED": 1, "CONFIRM": 2}
    if len(df):
        df["_o"] = df["disposition"].map(lambda d: order.get(str(d), 9))
        df = df.sort_values(["_o", "record_id", "field"]).drop(columns="_o")
    outp = out_dir / "holistic_review.csv"
    df.to_csv(outp, sep=";", index=False)
    print(f"[{topic}] holistic consolidated {len(df)} field-verdicts -> {outp}")
    if len(df):
        print(df["disposition"].value_counts().to_string())
    for b in bad:
        print("  BAD:", b)


if __name__ == "__main__":
    mode, topic = sys.argv[1], sys.argv[2]
    if mode == "build":
        rec = sys.argv[3].split(",") if len(sys.argv) > 3 else None
        build(topic, rec)
    else:
        consolidate(topic)
