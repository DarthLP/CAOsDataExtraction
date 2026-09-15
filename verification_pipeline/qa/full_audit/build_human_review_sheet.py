"""Consolidate the re-verification's human-review items (UNSURE + no-clean-
replacement, from both passes) into ONE sheet for Hanna, enriched with the field
definition, the value currently sitting in proposed_corrected_dataset.csv, and
the source quote -> qa/full_audit/human_review_items.csv.

Run: python3.13 -m qa.full_audit.build_human_review_sheet
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from qa.full_audit import common
from qa.full_audit.build_reverify_applied import schema_defs

HERE = Path(__file__).resolve().parent
SOURCES = ["reverify_unsure_review.csv", "reverify_remaining_review.csv"]
OUT = HERE / "human_review_items.csv"
COLS = ["record_id", "cao_number", "topic", "field_stem", "field_definition",
        "current_value_in_proposed", "current_unit_in_proposed", "issue",
        "confidence", "reverify_note", "source_quote"]


def main():
    defs = schema_defs()
    info = common.classify_columns()
    prop = {r["id"]: r for r in pd.read_csv(HERE / "proposed_corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}
    ds = {r["id"]: r for r in pd.read_csv(common.DATASET_PATH, sep=";", dtype=str,
          keep_default_na=False).to_dict("records")}

    frames = []
    for s in SOURCES:
        p = HERE / s
        if p.exists():
            df = pd.read_csv(p, sep=";", dtype=str, keep_default_na=False)
            df["_src"] = s
            frames.append(df)
    allr = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    rows = []
    for _, r in allr.iterrows():
        rid, stem = r["record_id"], r["stem"]
        ci = info.get(stem) or info.get(stem + "_value")
        topic = ci.topic if ci else ""
        vcol = stem + "_value" if (stem + "_value") in info else (stem if stem in info else "")
        ucol = stem + "_unit" if (stem + "_unit") in info else ""
        rows.append({
            "record_id": rid,
            "cao_number": common.norm(ds.get(rid, {}).get("cao_number", "")),
            "topic": topic,
            "field_stem": stem,
            "field_definition": defs.get(stem, ""),
            "current_value_in_proposed": common.norm(prop.get(rid, {}).get(vcol, "")) if vcol else "",
            "current_unit_in_proposed": common.norm(prop.get(rid, {}).get(ucol, "")) if ucol else "",
            "issue": r.get("disposition", "") or r.get("verdict", ""),
            "confidence": r.get("confidence", ""),
            "reverify_note": r.get("note", ""),
            "source_quote": r.get("quote", ""),
        })
    df = pd.DataFrame(rows, columns=COLS).drop_duplicates(["record_id", "field_stem"])
    df = df.sort_values(["topic", "field_stem", "record_id"])
    df.to_csv(OUT, sep=";", index=False)
    print(f"[human review sheet] {len(df)} items -> {OUT}")
    print(df["topic"].value_counts().to_string())
    print()
    print("by issue:", dict(df["issue"].value_counts()))


if __name__ == "__main__":
    main()
