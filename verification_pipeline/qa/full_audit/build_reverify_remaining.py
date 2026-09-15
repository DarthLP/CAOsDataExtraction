"""Build re-verification worksheets for the REMAINING applied corrections not yet
re-checked (the 89 higher-risk ones are done) — clean numbers, enums, booleans,
dates. Same definition-anchored format. Then dispatch + Phase E applies fixes.

Run: python3.13 -m qa.full_audit.build_reverify_remaining
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from qa.full_audit import common, verify_source
from qa.full_audit.build_reverify_applied import schema_defs

HERE = Path(__file__).resolve().parent
CH = HERE / "verify" / "chunks" / "reverify_remaining"
OUTDIR = HERE / "verify" / "outputs" / "reverify_remaining"


def _stem(f):
    return re.sub(r"_(value|unit|range_min|range_max)$", "", f)


def main():
    defs = schema_defs()
    info = common.classify_columns()
    cl = pd.read_csv(HERE / "apply_changelog.csv", sep=";", dtype=str, keep_default_na=False)
    applied = cl[(cl["status"] == "applied") & (~cl["route"].astype(str).str.startswith("reverify_"))]
    done = set()
    rr = HERE / "reverify_applied_results.csv"
    if rr.exists():
        for _, r in pd.read_csv(rr, sep=";", dtype=str, keep_default_na=False).iterrows():
            done.add((r["record_id"], r["stem"]))

    base = {r["id"]: r for r in pd.read_csv(HERE.parent / "corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    prop = {r["id"]: r for r in pd.read_csv(HERE / "proposed_corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    ds = {r["id"]: r for r in pd.read_csv(common.DATASET_PATH, sep=";", dtype=str,
          keep_default_na=False).to_dict("records")}

    seen = {}
    for _, r in applied.iterrows():
        rid, f = r["record_id"], r["target_field"]
        stem = _stem(f)
        if (rid, stem) in done:
            continue
        ci = info.get(f)
        seen.setdefault((rid, stem), ci.topic if ci else "")

    verify_source.clear_cache()
    items = []
    for (rid, stem), topic in seen.items():
        rec = ds.get(rid, {})
        txt, origin = verify_source.resolve(topic, rid, rec.get("cao_number", ""), rec.get("file_name", ""))
        fields = []
        for suff in ("_value", "_unit", "_range_min", "_range_max", ""):
            col = stem + suff if suff else stem
            if col in info and (col in base.get(rid, {})):
                o, a = common.norm(base[rid].get(col, "")), common.norm(prop[rid].get(col, ""))
                if o != a:
                    fields.append({"field": col, "definition": defs.get(stem, ""), "original": o, "applied": a})
        if not fields:
            continue
        items.append({"record_id": rid, "stem": stem, "topic": topic,
                      "definition": defs.get(stem, "(no schema definition found)"),
                      "fields": fields, "full_source_text": txt[:60000], "src_origin": origin})

    for f in (CH.glob("*.json") if CH.exists() else []):
        f.unlink()
    CH.mkdir(parents=True, exist_ok=True)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    man, cid, cur, cc = [], 0, [], 0
    def flush():
        nonlocal cid, cur, cc
        if not cur:
            return
        cid += 1
        p = CH / f"chunk_{cid:02d}.json"
        p.write_text(json.dumps({"chunk_id": f"reverify_remaining/chunk_{cid:02d}", "items": cur}, ensure_ascii=False, indent=1))
        man.append({"chunk": f"verify/chunks/reverify_remaining/chunk_{cid:02d}.json",
                    "out": f"verify/outputs/reverify_remaining/chunk_{cid:02d}_reverified.jsonl", "n": len(cur)})
        cur, cc = [], 0
    for it in items:
        c = len(it["full_source_text"]) + 600
        if cur and cc + c > 55000:
            flush()
        cur.append(it); cc += c
    flush()
    (CH / "manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=1))
    print(f"[reverify_remaining build] {len(items)} (record,stem) pairs / {len(man)} chunks")
    print(f"  with source: {sum(1 for it in items if it['src_origin']!='no_source')}, no_source: {sum(1 for it in items if it['src_origin']=='no_source')}")
    print(f"  chunks: {len(man)}")


if __name__ == "__main__":
    main()
