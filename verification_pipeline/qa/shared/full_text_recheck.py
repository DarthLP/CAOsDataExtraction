"""full_text_recheck.py — Standard pipeline Stage 4.5: independently re-verify a
topic's accepted substantive corrections against the FULL source section.

Why this exists: Stage 3 subagents see only a *token-budget-truncated* slice of
the source (worksheet_builder.slice_for_item). This stage re-reads each accepted
"real change" (clean win, excluding mechanical `_unit`-only rows) against the
COMPLETE topic section from the extraction file — genuinely full, **no character
cap** (chunks are sized by a cumulative-char budget so large sections still fit a
single subagent context). Output is a surfacing artifact (CONFIRM / NEEDS_CHANGE /
ESCALATE) — never auto-applied.

Generalized from the one-off qa/verify_changes/ run (which covered 10 topics and
was capped at 32k chars — that cap truncated 6 bonus + 6 wage sections). This
module is topic-parameterized and uncapped; run it for every topic.

Usage:
  python3.13 -m qa.shared.full_text_recheck build   <topic>
  python3.13 -m qa.shared.full_text_recheck consolidate <topic>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from qa.shared import source_text_loader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CHAR_BUDGET = 55000          # ~14k tokens/chunk; big sections → fewer items/chunk
HARD_SECTION_CAP = 250000    # safety only (~62k tokens); real max section is ~38k


def _qa_dir(topic: str) -> Path:
    return PROJECT_ROOT / "qa" / f"qa_{topic}"


def _real_changes(topic: str) -> list[dict]:
    """Accepted substantive corrections = clean wins (is_noop!=true, not in NHR),
    excluding mechanical `_unit`-only rows (validated separately)."""
    qa = _qa_dir(topic)
    corr = pd.read_csv(qa / "outputs" / "corrections.csv", sep=";", dtype=str, keep_default_na=False)
    real = corr[corr["is_noop"].str.strip().str.lower() != "true"]
    nhr_path = qa / "outputs" / "needs_human_review.csv"
    nk = set()
    if nhr_path.exists():
        n = pd.read_csv(nhr_path, sep=";", dtype=str, keep_default_na=False)
        nk = {(r["record_id"], r["original_field"]) for _, r in n.iterrows()}
    out = []
    for _, r in real.iterrows():
        f = str(r["original_field"])
        if f.endswith("_unit"):
            continue
        if (r["record_id"], f) in nk:
            continue
        out.append(r.to_dict())
    return out


def build(topic: str) -> None:
    source_text_loader.clear_cache()
    qa = _qa_dir(topic)
    chunks_dir = qa / "recheck" / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    for f in chunks_dir.glob("*.jsonl"):
        f.unlink()

    items = []
    for r in _real_changes(topic):
        rid = str(r["record_id"]).strip()
        full = source_text_loader.load_source_text(topic, rid)[:HARD_SECTION_CAP]
        items.append({
            "topic": topic, "record_id": rid, "field": str(r["original_field"]),
            "csv_value_old": str(r.get("csv_value_old", "")),
            "csv_unit_old": str(r.get("csv_unit_old", "")),
            "applied_correction": {
                "verdict": str(r.get("verdict", "")),
                "csv_value_new": str(r.get("csv_value_new", "")),
                "csv_unit_new": str(r.get("csv_unit_new", "")),
                "evidence_quote": str(r.get("evidence_quote", "")),
                "confidence": str(r.get("confidence", "")),
                "notes": str(r.get("notes", "")),
            },
            "full_source_text": full,          # genuinely full — no 32k cap
        })

    # chunk by cumulative char budget (keeps big-section items in small chunks)
    n_chunks, cur, cur_chars = 0, [], 0
    def flush():
        nonlocal n_chunks, cur, cur_chars
        if not cur:
            return
        n_chunks += 1
        (chunks_dir / f"rc_chunk_{n_chunks:03d}.jsonl").write_text(
            "\n".join(json.dumps(x, ensure_ascii=False) for x in cur) + "\n", encoding="utf-8")
        cur, cur_chars = [], 0
    for it in items:
        c = len(it["full_source_text"]) + 500
        if cur and cur_chars + c > CHAR_BUDGET:
            flush()
        cur.append(it); cur_chars += c
    flush()
    print(f"[{topic}] {len(items)} real-change items -> {n_chunks} chunks (full section, char-budget {CHAR_BUDGET})")
    if not items:
        print(f"[{topic}] no substantive corrections to re-check (0 clean wins or all units).")


KEYS = ["topic", "record_id", "field", "disposition", "corrected_value",
        "corrected_unit", "verification_reason", "evidence_quote", "confidence",
        "applied_value", "applied_verdict"]


def consolidate(topic: str) -> None:
    qa = _qa_dir(topic)
    out_dir = qa / "recheck" / "outputs"
    rows, bad = [], []
    for f in sorted(out_dir.glob("rc_chunk_*_verified.jsonl")):
        for ln, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError as e:
                bad.append(f"{f.name}:{ln} {e}"); continue
            rows.append({k: o.get(k, "") for k in KEYS})
    df = pd.DataFrame(rows, columns=KEYS)
    order = {"NEEDS_CHANGE": 0, "ESCALATE": 1, "CONFIRM": 2}
    if len(df):
        df["_o"] = df["disposition"].map(lambda d: order.get(str(d), 9))
        df = df.sort_values(["_o", "record_id"]).drop(columns="_o")
    outp = out_dir / "verified_changes_review.csv"
    df.to_csv(outp, sep=";", index=False)
    print(f"[{topic}] consolidated {len(df)} rows -> {outp}")
    if len(df):
        print(df["disposition"].value_counts().to_string())
    for b in bad:
        print("  BAD:", b)


if __name__ == "__main__":
    mode, topic = sys.argv[1], sys.argv[2]
    {"build": build, "consolidate": consolidate}[mode](topic)
