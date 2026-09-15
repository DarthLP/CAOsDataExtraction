"""Build PRESERVATION-AWARE verification worksheets for the pending
unit_semantics (Check 5) flags only. Reuses verify.py's worksheet construction
(full source text + all populated topic fields incl. *_note) and writes to
chunks/topup_check5/<topic>/ + outputs/topup_check5/<topic>/.

Run:  python3.13 -m qa.full_audit.build_topup_check5
"""
from __future__ import annotations

import json
from collections import defaultdict

import pandas as pd

from qa.full_audit import verify as v
from qa.full_audit import verify_source, common

FLAGS = common.PROJECT_ROOT / "qa" / "full_audit" / "full_audit_flags.csv" \
    if hasattr(common, "PROJECT_ROOT") else None
CH = v.CHUNKS_DIR / "topup_check5"
OUTROOT = v.OUTPUTS_DIR / "topup_check5"

# Larger than the default 60k: these are targeted single-field judgments, so we
# pack more items/chunk to keep the dispatch to ~1-2 waves. Peak per-chunk read
# ~150k chars (~37k tokens) is comfortable for one subagent.
BUDGET = 150_000


def build() -> None:
    flags_path = v.FLAGS_CSV
    df = pd.read_csv(flags_path, sep=";", dtype=str, keep_default_na=False)
    pend = df[df["verify_verdict"].str.strip() == "pending"]

    info = common.classify_columns()
    flagged: dict[tuple[str, str], dict] = defaultdict(dict)
    meta: dict[tuple[str, str], dict] = {}
    for _, r in pend.iterrows():
        fld = r["field"]
        ci = info.get(fld)
        topic = ci.topic if ci else "meta"
        rid = r["record_id"]
        # _judgeable expects keys "severity" and "reason"
        flagged[(topic, rid)][fld] = {"severity": r["severity"], "reason": r["reason"]}
        meta[(topic, rid)] = {"cao_number": r["cao_number"],
                              "file_name": r["file_name"],
                              "ingangsdatum": r["ingangsdatum"]}

    recs = v._record_index()
    verify_source.clear_cache()

    items_by_topic: dict[str, list[dict]] = defaultdict(list)
    missing: list = []
    for (topic, rid), ff in flagged.items():
        rec = recs.get(rid)
        if rec is None:
            missing.append((topic, rid, "no_record")); continue
        m = meta[(topic, rid)]
        txt, origin = verify_source.resolve(topic, rid, m["cao_number"], m["file_name"])
        if origin == "no_source" or not txt.strip():
            missing.append((topic, rid, "no_source")); continue
        items_by_topic[topic].append({
            "topic": topic, "record_id": rid,
            "cao_number": m["cao_number"], "file_name": m["file_name"],
            "ingangsdatum": m["ingangsdatum"], "origin": origin,
            "fields": v._judgeable(rec, topic, ff),
            "full_source_text": txt[:v.HARD_SECTION_CAP],
        })

    if CH.exists():
        for f in CH.rglob("*.json"):
            f.unlink()
    CH.mkdir(parents=True, exist_ok=True)

    manifest: list[dict] = []
    for topic in sorted(items_by_topic):
        model = "opus" if topic in v.OPUS_TOPICS else "sonnet"
        tdir = CH / topic
        tdir.mkdir(parents=True, exist_ok=True)
        (OUTROOT / topic).mkdir(parents=True, exist_ok=True)
        cur: list[dict] = []
        cc = seq = 0

        def flush():
            nonlocal cur, cc, seq
            if not cur:
                return
            seq += 1
            name = f"chunk_{seq:03d}"
            (tdir / f"{name}.json").write_text(json.dumps(
                {"chunk_id": f"topup_check5/{topic}/{name}", "topic": topic,
                 "model": model, "items": cur}, ensure_ascii=False, indent=1),
                encoding="utf-8")
            manifest.append({
                "chunk_id": f"topup_check5/{topic}/{name}", "topic": topic,
                "model": model,
                "chunk_path": str((tdir / f"{name}.json").relative_to(v.VERIFY_DIR)),
                "out_path": f"outputs/topup_check5/{topic}/{name}_verified.jsonl",
                "n_items": len(cur),
                "n_flagged": sum(sum(1 for f in it["fields"] if f["flagged"])
                                 for it in cur),
                "record_ids": [it["record_id"] for it in cur],
            })
            cur, cc = [], 0

        for it in items_by_topic[topic]:
            c = v._item_cost(it)
            if cur and cc + c > BUDGET:
                flush()
            cur.append(it)
            cc += c
        flush()

    (CH / "manifest_topup_check5.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")

    nflag = sum(m["n_flagged"] for m in manifest)
    print(f"[topup_check5 build] {sum(len(x) for x in items_by_topic.values())} "
          f"resolvable items / {nflag} flagged cells, {len(missing)} unresolvable")
    print(f"  chunks: {len(manifest)} (all sonnet: "
          f"{all(m['model'] == 'sonnet' for m in manifest)})")
    for m in manifest:
        print(f"    {m['chunk_id']:38s} items={m['n_items']:2d} flagged={m['n_flagged']:2d}")
    if missing:
        print(f"  unresolvable (no source/record): {missing[:10]}")
    print(f"  manifest -> {CH / 'manifest_topup_check5.json'}")


if __name__ == "__main__":
    build()
