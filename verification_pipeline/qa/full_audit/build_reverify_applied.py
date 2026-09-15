"""Build focused re-verification worksheets for the higher-risk APPLIED
corrections, giving each subagent the ORIGINAL EXTRACTION DEFINITION of the
field (from NON_SALARY_PROMPTS_AND_SCHEMA.md) so it judges the applied value+unit
against what the field is supposed to contain — not a guess.

Targets (from apply_changelog.csv, status=applied):
  routes split_value_unit / unit_pair_complete / unit_formatting, OR any applied
  cell with no verbatim source quote (dropout/lineage fills). Grouped to the
  value+unit PAIR per (record, field-stem).

Run: python3.13 -m qa.full_audit.build_reverify_applied
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from qa.full_audit import common, verify_source

HERE = Path(__file__).resolve().parent
SCHEMA = common.PROJECT_ROOT / "inputs" / "NON_SALARY_PROMPTS_AND_SCHEMA.md"
CH = HERE / "verify" / "chunks" / "reverify_applied"
OUTDIR = HERE / "verify" / "outputs" / "reverify_applied"
RISKY_ROUTES = {"split_value_unit", "unit_pair_complete", "unit_formatting"}


def _blank(s):
    return str(s).strip().lower() in ("", "nan", "none", "null")


def schema_defs() -> dict:
    """dataset stem -> human definition from the extraction schema."""
    txt = SCHEMA.read_text(encoding="utf-8").splitlines()
    entry = re.compile(r"^`([A-Za-z_]+)\.([A-Za-z0-9_]+)`\s*\|")
    out = {}
    for i, line in enumerate(txt):
        m = entry.match(line)
        if not m:
            continue
        prefix, field = m.group(1), m.group(2)
        topic = common.SCHEMA_PREFIX_TO_TOPIC.get(prefix)
        if not topic:
            continue
        stem = common.TOPIC_CSV_PREFIX[topic] + field
        desc = ""
        for j in range(i + 1, min(i + 3, len(txt))):
            if txt[j].strip() and not txt[j].startswith("`"):
                desc = txt[j].strip(); break
        out[stem] = desc
    return out


def main():
    defs = schema_defs()
    info = common.classify_columns()
    cl = pd.read_csv(HERE / "apply_changelog.csv", sep=";", dtype=str, keep_default_na=False)
    applied = cl[cl["status"] == "applied"]
    targets = applied[applied["route"].isin(RISKY_ROUTES) | applied["source_quote"].map(_blank)]

    base = {r["id"]: r for r in pd.read_csv(HERE.parent / "corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    prop = {r["id"]: r for r in pd.read_csv(HERE / "proposed_corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    ds = {r["id"]: r for r in pd.read_csv(common.DATASET_PATH, sep=";", dtype=str,
          keep_default_na=False).to_dict("records")}

    # group target cells -> value+unit pair per (record, stem)
    seen = {}
    for _, r in targets.iterrows():
        rid, f = r["record_id"], r["target_field"]
        stem = re.sub(r"_(value|unit|range_min|range_max)$", "", f)
        ci = info.get(f)
        seen.setdefault((rid, stem), {"record_id": rid, "stem": stem,
                                      "topic": ci.topic if ci else ""})

    items_by_topic = {}
    verify_source.clear_cache()
    for (rid, stem), d in seen.items():
        topic = d["topic"]
        rec = ds.get(rid, {})
        txt, origin = verify_source.resolve(topic, rid, rec.get("cao_number", ""), rec.get("file_name", ""))
        # gather the applied value/unit (proposed) + original (base) for stem's fields
        fields = []
        for suff in ("_value", "_unit", "_range_min", "_range_max"):
            col = stem + suff
            if col in info:
                fields.append({"field": col, "definition": defs.get(stem, ""),
                               "original": common.norm(base.get(rid, {}).get(col, "")),
                               "applied": common.norm(prop.get(rid, {}).get(col, ""))})
        # only keep fields that actually changed
        changed = [f for f in fields if f["original"] != f["applied"]]
        if not changed:
            continue
        items_by_topic.setdefault(topic, []).append({
            "record_id": rid, "stem": stem, "topic": topic,
            "definition": defs.get(stem, "(no schema definition found)"),
            "fields": changed, "full_source_text": txt[:60000], "src_origin": origin})

    for f in (CH.glob("*.json") if CH.exists() else []):
        f.unlink()
    CH.mkdir(parents=True, exist_ok=True)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    man, cid, cur, cc = [], 0, [], 0
    flat = [it for t in sorted(items_by_topic) for it in items_by_topic[t]]
    def flush():
        nonlocal cid, cur, cc
        if not cur:
            return
        cid += 1
        p = CH / f"chunk_{cid:02d}.json"
        p.write_text(json.dumps({"chunk_id": f"reverify_applied/chunk_{cid:02d}", "items": cur},
                                ensure_ascii=False, indent=1))
        man.append({"chunk": str(p.relative_to(HERE)), "out": f"verify/outputs/reverify_applied/chunk_{cid:02d}_reverified.jsonl",
                    "n": len(cur), "records": [x["record_id"] for x in cur]})
        cur, cc = [], 0
    for it in flat:
        c = len(it["full_source_text"]) + 600
        if cur and cc + c > 55000:
            flush()
        cur.append(it); cc += c
    flush()
    (CH / "manifest.json").write_text(json.dumps(man, ensure_ascii=False, indent=1))
    npairs = len(flat); ncells = sum(len(it["fields"]) for it in flat)
    print(f"[reverify build] {npairs} (record,field-stem) pairs / {ncells} changed cells / {len(man)} chunks")
    print(f"  with source: {sum(1 for it in flat if it['src_origin']!='no_source')}, no_source: {sum(1 for it in flat if it['src_origin']=='no_source')}")
    for m in man:
        print(f"  {m['chunk'].split('/')[-1]}: {m['n']} pairs -> {m['out']}")


if __name__ == "__main__":
    main()
