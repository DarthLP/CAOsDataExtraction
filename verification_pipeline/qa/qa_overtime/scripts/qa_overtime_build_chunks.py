"""qa_overtime_build_chunks.py — Stage 3 setup: build chunk JSONLs + system prompt (overtime).

Added 2026-05-27 for template consistency (overtime originally built chunks ad-hoc).
Standard 5-script layout; safe to re-run.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
QA = HERE.parent
PROJECT_ROOT = QA.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from qa.shared import worksheet_builder, logging_util, source_text_loader  # noqa: E402

DET = QA / "outputs" / "corrections_deterministic.csv"
WS = QA / "worksheets"


def main():
    print("=== Stage 3 setup (overtime) — chunk build ===")
    df = pd.read_csv(DET, sep=";", dtype=str, keep_default_na=False)
    print(f"  deterministic rows: {len(df)}")
    records = []
    for _, r in df.iterrows():
        records.append({
            "record_id": str(r["record_id"]), "cao_number": str(r.get("cao_number", "")),
            "file_name": str(r.get("file_name", "")), "ingangsdatum": str(r.get("ingangsdatum", "")),
            "era": "", "field": str(r["field"]),
            "csv_value_old": str(r.get("csv_value_old", "")), "csv_unit_old": str(r.get("csv_unit_old", "")),
            "flag_type": str(r.get("rule_id", "")), "flag_reason": str(r.get("notes", ""))[:200],
            "worksheet_mode": str(r.get("worksheet_mode", "blind")),
            "proposed_correction": None if not str(r.get("csv_value_new", "")).strip() else {
                "csv_value_new": str(r.get("csv_value_new", "")), "csv_unit_new": str(r.get("csv_unit_new", "")),
                "verdict": str(r.get("verdict", "")), "target_field": str(r.get("target_field", "")),
            },
        })
    source_text_loader.clear_cache()
    chunks = worksheet_builder.build_chunk_items(records, "overtime")
    print(f"  built {len(chunks)} chunks ({sum(len(c) for c in chunks)} items total)")
    WS.mkdir(parents=True, exist_ok=True)
    chunks_dir = WS / "chunks"; chunks_dir.mkdir(exist_ok=True)
    for i, chunk in enumerate(chunks, 1):
        n = worksheet_builder.write_chunk_jsonl(chunk, chunks_dir / f"chunk_{i:03d}.jsonl")
        print(f"  chunk_{i:03d}: {n} items")
    prompt = worksheet_builder.build_subagent_prompt("overtime")
    (WS / "system_prompt.txt").write_text(prompt, encoding="utf-8")
    logging_util.log_event("overtime", "stage_3_setup", "chunks built",
                           chunks=len(chunks), items=sum(len(c) for c in chunks),
                           prompt_tokens=len(prompt) // 4)


if __name__ == "__main__":
    main()
