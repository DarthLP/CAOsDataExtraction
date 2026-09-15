"""qa_wage_build_chunks.py — Stage 3 setup for wage.

Per Phase 0.5, wage shares the token-heavy wage_information.md, so per-item
caps are raised: soft 3,500 / hard 6,000 tokens.
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
    print("=== Stage 3 setup (wage) — raised caps 3500/6000 ===")
    df = pd.read_csv(DET, sep=";", dtype=str, keep_default_na=False)
    print(f"  det rows: {len(df)}")

    records = []
    for _, r in df.iterrows():
        records.append({
            "record_id": str(r["record_id"]), "cao_number": str(r.get("cao_number", "")),
            "file_name": str(r.get("file_name", "")), "ingangsdatum": str(r.get("ingangsdatum", "")),
            "era": "", "field": str(r["field"]),
            "csv_value_old": str(r.get("csv_value_old", "")), "csv_unit_old": str(r.get("csv_unit_old", "")),
            "flag_type": str(r.get("rule_id", "")), "flag_reason": str(r.get("notes", ""))[:200],
            "worksheet_mode": str(r.get("worksheet_mode", "blind")),
            "proposed_correction": None if (not str(r.get("csv_value_new", "")).strip()
                and not str(r.get("csv_unit_new", "")).strip()) else {
                "csv_value_new": str(r.get("csv_value_new", "")),
                "csv_unit_new": str(r.get("csv_unit_new", "")),
                "verdict": str(r.get("verdict", "")), "target_field": str(r.get("target_field", "")),
            },
        })

    source_text_loader.clear_cache()
    chunks = worksheet_builder.build_chunk_items(
        records, "wage",
        per_item_soft_target=3500, per_item_hard_cap=6000)
    print(f"  built {len(chunks)} chunks ({sum(len(c) for c in chunks)} items)")

    WS.mkdir(parents=True, exist_ok=True)
    cd = WS / "chunks"; cd.mkdir(exist_ok=True)
    for i, chunk in enumerate(chunks, 1):
        n = worksheet_builder.write_chunk_jsonl(chunk, cd / f"chunk_{i:03d}.jsonl")
    print(f"  wrote {len(chunks)} chunk files")

    prompt = worksheet_builder.build_subagent_prompt("wage")
    (WS / "system_prompt.txt").write_text(prompt, encoding="utf-8")
    print(f"  system prompt: {len(prompt)//4} tokens")

    logging_util.log_event("wage", "stage_3_setup", "chunks built",
                           chunks=len(chunks), items=sum(len(c) for c in chunks),
                           prompt_tokens=len(prompt)//4)


if __name__ == "__main__":
    main()
