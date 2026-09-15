"""Per-topic builder for the per-file conflict-verification campaign (SURFACE-ONLY).

Builds, for ONE topic, the merged (agreement x topic) worksheet units — each covering
ALL of that agreement's conflicting fields for the topic (numeric + enum + boolean),
so a single subagent re-reads the topic section once and re-extracts everything.
Excludes UNIT_WORDING (same quantity, different spelling) and ratio-equivalent numeric
conflicts (same quantity, different unit). In-scope only (needs source text).

Writes perfile_work/<topic>/:
  - uNNNN.json     blind agent-facing unit (NO dataset values; full source blocks)
  - _truth.json    dataset value map per (unit, field) — for the consolidate compare
  - manifest.csv   one row per unit (paths + field/record counts + status)

NOTHING applied. Run: python3.13 -m qa.full_audit.perfile_run --topic leave
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

from qa.full_audit import common
from qa.full_audit.build_perfile_verify import build_units
from qa.full_audit.build_reverify_applied import schema_defs

HERE = Path(__file__).resolve().parent
WORK = HERE / "perfile_work"
CONFLICTS = HERE / "agreement_conflicts.csv"


def _ratio_q(val, unit):
    f = common.to_float(val)
    if f is None:
        return None
    u = str(unit).lower()
    if "%" in u or "percent" in u or "procent" in u:
        return round(f / 100.0, 6)
    if re.search(r"(^|\W)(x|×|times|maal|keer|factor|fold|multiple)(\W|$)", u):
        return round(f, 6)
    return None


def _drop_ratio_equiv(cdf, ds, info):
    """Drop numeric conflicts whose records are the SAME quantity in different units."""
    keep = []
    for _, r in cdf.iterrows():
        if r["kind"] == "numeric":
            ci = info.get(r["field"])
            ucol = ci.unit_partner if ci else None
            qs, ok = [], True
            for rid in re.findall(r"(\d+)\(", r["value_map"]):
                rec = ds.get(rid, {})
                q = _ratio_q(rec.get(r["field"], ""), rec.get(ucol, "") if ucol else "")
                if q is None:
                    ok = False
                    break
                qs.append(q)
            if ok and len(set(qs)) == 1:
                continue
        keep.append(r)
    return pd.DataFrame(keep)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True)
    a = ap.parse_args()

    info = common.classify_columns()
    defs = schema_defs()
    ds = {r["id"]: r for r in pd.read_csv(common.PROJECT_ROOT / "qa" / "corrected_dataset.csv",
          sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    cdf = pd.read_csv(CONFLICTS, sep=";", dtype=str, keep_default_na=False)
    cdf = cdf[(cdf.triage != "UNIT_WORDING") & (cdf.in_scope == "True") & (cdf.topic == a.topic)]
    cdf = _drop_ratio_equiv(cdf, ds, info)

    units = build_units(cdf, ds, info, defs)
    units = [u for u in units if any(r["has_source"] for r in u["records"])]

    tdir = WORK / a.topic
    rdir = tdir / "results"
    rdir.mkdir(parents=True, exist_ok=True)
    truth, manifest = [], []
    for i, u in enumerate(units):
        uid = f"u{i:04d}"
        agent_fields = [{"field": f["field"], "kind": f["kind"],
                         "schema_def": f["schema_def"], "enum_values": f["enum_values"]}
                        for f in u["fields"]]
        agent_recs = [{"record_id": r["record_id"], "filing_date": r["filing_date"],
                       "doc_type": r["doc_type"], "ttw": r["ttw"],
                       "source_block": r["source_block"]}
                      for r in u["records"] if r["has_source"]]
        (tdir / f"{uid}.json").write_text(json.dumps(
            {"agreement_id": u["agreement_id"], "topic": u["topic"],
             "fields": agent_fields, "records": agent_recs}, ensure_ascii=False, indent=1),
            encoding="utf-8")
        truth.append({"uid": uid, "agreement_id": u["agreement_id"], "topic": u["topic"],
                      "fields": [{"field": f["field"], "kind": f["kind"],
                                  "dataset_value_map": f["dataset_value_map"]} for f in u["fields"]]})
        manifest.append({"uid": uid, "agreement_id": u["agreement_id"],
                         "n_fields": len(agent_fields), "n_records": len(agent_recs),
                         "unit_path": str(tdir / f"{uid}.json"),
                         "result_path": str(rdir / f"{uid}.json"), "status": "pending"})
    (tdir / "_truth.json").write_text(json.dumps(truth, ensure_ascii=False), encoding="utf-8")
    pd.DataFrame(manifest).to_csv(tdir / "manifest.csv", sep=";", index=False)

    print(f"[perfile_run] topic={a.topic} units={len(units)} "
          f"fields={sum(m['n_fields'] for m in manifest)} -> {tdir}/")
    print(f"  dispatch each uNNNN.json with PERFILE_INSTRUCTIONS.md; results -> {rdir}/")


if __name__ == "__main__":
    main()
