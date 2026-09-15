"""Per-file conflict verification — build worksheets (SURFACE-ONLY).

For each within-agreement CONFLICT (>=2 version-records of the same agreement give
different filled values), the only way to tell an extraction ERROR from a real
mid-term CHANGE is to RE-READ EACH FILE INDEPENDENTLY and compare. This builder
emits one worksheet per (agreement x topic): the conflicting fields (each with its
schema/pydantic definition + enum vocab = the REQUIRED context), and EACH involved
version-record's OWN full topic source block.

A subagent extracts, per file, what THAT file states for each field — blind to the
siblings and to the recorded value — then we compare afterwards (consolidate step):
  * files agree with each other, differ from dataset -> EXTRACTION ERROR
  * files state genuinely different values over time  -> REAL TEMPORAL CHANGE
  * a file is SILENT but dataset has a value          -> over-filled / inherited

NOTHING is applied. Writes perfile_units.jsonl + perfile_index.csv.

Run: python3.13 -m qa.full_audit.build_perfile_verify [--triages T1,T2] [--kinds K]
        [--topics t1,t2] [--limit N] [--pilot]
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

from qa.full_audit import common
from qa.full_audit.build_reverify_applied import schema_defs

HERE = Path(__file__).resolve().parent
CONFLICTS = HERE / "agreement_conflicts.csv"
UNITS_OUT = HERE / "perfile_units.jsonl"
INDEX_OUT = HERE / "perfile_index.csv"

_RID_RE = re.compile(r"(\d+)\(([^)]*)\)")
ORDER_DATE_COLS = ["datum_kennisgeving", "general_signing_date",
                   "general_chg_eff_date", "expiratiedatum"]

# Robust per-file source loading. The exact (cao, normalized_filename) matcher in
# source_text_loader misses ~903 in-scope records because some by_topic source-metadata
# filenames carry trailing non-breaking spaces / a surviving ".pdf" that defeat the
# single-extension strip. Stronger normalization (strip ALL extensions + drop every
# non-alphanumeric) recovers the record's OWN file unambiguously (skips the 72 that map
# to >1 candidate — never attach a sibling's text).
_STRONG_IDX: dict = {}


def _strong_fn(fn) -> str:
    s = str(fn).strip().lower()
    for _ in range(3):
        s = re.sub(r"\.[a-z0-9]{2,5}$", "", s)
    return re.sub(r"[^a-z0-9]", "", s)


def _robust_source(topic, rid, ds) -> str:
    from qa.shared import source_text_loader as stl
    blk = stl.load_source_text(topic, rid)
    if blk:
        return blk
    rec = ds.get(rid, {})
    cao = common.norm(rec.get("cao_number", ""))
    fn = common.norm(rec.get("file_name", ""))
    if topic not in _STRONG_IDX:
        from collections import defaultdict as _dd
        idx = _dd(list)
        try:
            for (c, bfn), btext in stl._load_blocks(topic).items():
                idx[(c, _strong_fn(bfn))].append(btext)
        except ValueError:
            pass
        _STRONG_IDX[topic] = idx
    cands = _STRONG_IDX[topic].get((cao, _strong_fn(fn)), [])
    return cands[0] if len(cands) == 1 else ""


def _stem_for(field, info):
    ci = info[field]
    return common.TOPIC_CSV_PREFIX.get(ci.topic, ci.topic + "_") + ci.base_field


def _involved(value_map):
    """[(record_id, date_str)] parsed from a conflict's value_map."""
    return [(m.group(1), m.group(2)) for m in _RID_RE.finditer(value_map)]


def build_units(cdf, ds, info, defs):
    """Group conflict rows into (agreement, topic) worksheet units."""
    by_unit = defaultdict(list)
    for _, r in cdf.iterrows():
        by_unit[(r["agreement_id"], r["topic"])].append(r)

    units = []
    for (aid, topic), rows in by_unit.items():
        # union of involved records across this unit's conflicting fields
        rec_ids = {}
        for r in rows:
            for rid, _d in _involved(r["value_map"]):
                rec_ids[rid] = True
        # field definitions (the REQUIRED subagent context)
        fields = []
        for r in rows:
            f = r["field"]
            ci = info.get(f)
            stem = _stem_for(f, info)
            fields.append({
                "field": f, "stem": stem, "kind": r["kind"],
                "schema_def": defs.get(stem, ""),
                "enum_values": sorted(common.ENUM_ALLOWED.get(f, []) ) or None,
                "dataset_value_map": r["value_map"],   # for our compare, NOT shown to agent
            })
        # each involved record's OWN full topic source block
        records = []
        for rid in rec_ids:
            rec = ds.get(rid, {})
            d = next((common.norm(rec.get(c, "")) for c in ORDER_DATE_COLS
                      if not common.is_blank(rec.get(c, ""))), "")
            block = _robust_source(topic, rid, ds)
            records.append({
                "record_id": rid, "filing_date": d,
                "doc_type": common.norm(rec.get("general_document_type", "")),
                "ttw": common.norm(rec.get("TTW", "")),
                "file_name": common.norm(rec.get("file_name", "")),
                "has_source": bool(block),
                "source_block": block,
            })
        units.append({
            "agreement_id": aid, "cao_number": rows[0]["cao_number"],
            "topic": topic, "n_fields": len(fields), "n_records": len(records),
            "all_have_source": all(r["has_source"] for r in records),
            "fields": fields, "records": records,
        })
    return units


def select_pilot(cdf):
    """A small, diverse pilot: unit-confusion (>=10x numeric), enum, genuine-
    temporal, and boolean — all in-scope, all files with source."""
    cdf = cdf[cdf.in_scope == "True"].copy()
    picks = []
    # 3 high-priority numeric (incl. big-magnitude %-vs-EUR), diverse topics
    hi = cdf[(cdf.triage.isin(["AMBIGUOUS_2REC", "LIKELY_EXTRACTION_ERROR"])) & (cdf.kind == "numeric")]
    for t in ["bonus", "overtime", "term"]:
        sub = hi[hi.topic == t]
        if len(sub):
            picks.append(sub.iloc[0])
    # 1 enum high-priority
    en = cdf[(cdf.triage.isin(["AMBIGUOUS_2REC", "LIKELY_EXTRACTION_ERROR"])) & (cdf.kind == "enum")]
    if len(en):
        picks.append(en.iloc[0])
    # 2 temporal (one bonus %-vs-EUR suspect, one non-bonus)
    tp = cdf[cdf.triage == "LIKELY_TEMPORAL_CHANGE"]
    if len(tp[tp.topic == "bonus"]):
        picks.append(tp[tp.topic == "bonus"].iloc[0])
    if len(tp[tp.topic != "bonus"]):
        picks.append(tp[tp.topic != "bonus"].iloc[0])
    # 2 boolean
    bl = cdf[cdf.triage == "BOOLEAN_DISAGREEMENT"]
    for t in ["ai", "overtime"]:
        sub = bl[bl.topic == t]
        if len(sub):
            picks.append(sub.iloc[0])
    return pd.DataFrame(picks)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--triages", default="")
    ap.add_argument("--kinds", default="")
    ap.add_argument("--topics", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--pilot", action="store_true")
    a = ap.parse_args()

    info = common.classify_columns()
    defs = schema_defs()
    ds = {r["id"]: r for r in pd.read_csv(common.PROJECT_ROOT / "qa" / "corrected_dataset.csv",
          sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    cdf = pd.read_csv(CONFLICTS, sep=";", dtype=str, keep_default_na=False)

    if a.pilot:
        cdf = select_pilot(cdf)
    else:
        if a.triages:
            cdf = cdf[cdf.triage.isin(a.triages.split(","))]
        if a.kinds:
            cdf = cdf[cdf.kind.isin(a.kinds.split(","))]
        if a.topics:
            cdf = cdf[cdf.topic.isin(a.topics.split(","))]
        cdf = cdf[cdf.in_scope == "True"]

    units = build_units(cdf, ds, info, defs)
    if a.limit and not a.pilot:
        units = units[:a.limit]

    with UNITS_OUT.open("w", encoding="utf-8") as fh:
        for u in units:
            fh.write(json.dumps(u, ensure_ascii=False) + "\n")
    idx = pd.DataFrame([{k: u[k] for k in ("agreement_id", "cao_number", "topic",
                        "n_fields", "n_records", "all_have_source")} for u in units])
    idx.to_csv(INDEX_OUT, sep=";", index=False)

    src_ok = sum(1 for u in units if u["all_have_source"])
    print(f"[build_perfile_verify] units={len(units)} (all-source={src_ok}) "
          f"fields={sum(u['n_fields'] for u in units)} -> {UNITS_OUT.name}, {INDEX_OUT.name}")
    for u in units:
        print(f"  {u['agreement_id']:22s} {u['topic']:9s} fields={u['n_fields']} "
              f"recs={u['n_records']} src={u['all_have_source']} "
              f"[{', '.join(f['field'] for f in u['fields'][:3])}]")


if __name__ == "__main__":
    main()
