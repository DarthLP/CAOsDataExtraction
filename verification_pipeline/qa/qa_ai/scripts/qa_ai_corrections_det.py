"""qa_ai_corrections_det.py — Stage 2 driver for ai."""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
QA = HERE.parent
PROJECT_ROOT = QA.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from qa.shared import (presence_scan, source_text_loader, schema_lookup,
                       logging_util)  # noqa: E402
sys.path.insert(0, str(HERE))
import qa_ai_rules  # noqa: E402

SCOPED = QA / "inputs" / "scoped_records.csv"
DET_OUT = QA / "outputs" / "corrections_deterministic.csv"


def main():
    print("=== Stage 2 (ai) ===")
    scoped = pd.read_csv(SCOPED, sep=";", dtype=str, keep_default_na=False)
    print(f"  scoped records: {len(scoped)}")

    rule_rows = []
    for _, row in scoped.iterrows():
        rid = str(row.get("id", "")).strip()
        for rule in qa_ai_rules.RULES:
            v = rule(row, rid)
            if v is not None:
                v["cao_number"] = str(row.get("cao_number", ""))
                v["file_name"] = str(row.get("file_name", ""))
                v["ingangsdatum"] = str(row.get("ingangsdatum", ""))
                rule_rows.append(v)
    print(f"  L1 rule violations: {len(rule_rows)}")

    source_text_loader.clear_cache()
    presence_df = presence_scan.scan_topic_presence("ai", scoped, field_specific=True)
    print(f"  L2 presence triggers (field-specific): {len(presence_df)}")
    l1_keys = {(v["record_id"], v["field"]) for v in rule_rows}
    pre = len(presence_df)
    presence_df = presence_df[~presence_df.apply(
        lambda p: (str(p["record_id"]), str(p["field"])) in l1_keys, axis=1)]
    print(f"  filtered L2 overlaps with L1: {pre - len(presence_df)} dropped")

    presence_rows = []
    for _, p in presence_df.iterrows():
        presence_rows.append({
            "record_id": str(p["record_id"]), "rule_id": "L2", "field": str(p["field"]),
            "severity": "medium", "worksheet_mode": "extract",
            "csv_value_old": "", "csv_unit_old": "", "csv_value_new": "", "csv_unit_new": "",
            "verdict": "", "target_field": "",
            "notes": f"L2: field-specific match. {p['matched_phrases'][:120]}",
            "topic_group": "ai", "confidence": "",
            "evidence_quote": str(p.get("evidence_excerpt", ""))[:200],
        })
    meta = {str(r["id"]).strip(): (r.get("cao_number", ""), r.get("file_name", ""),
                                   r.get("ingangsdatum", "")) for _, r in scoped.iterrows()}
    for row in presence_rows:
        cao, fn, ing = meta.get(row["record_id"], ("", "", ""))
        row["cao_number"] = cao; row["file_name"] = fn; row["ingangsdatum"] = ing

    enum_fields = schema_lookup.get_topic_enum_fields("ai")
    enum_rows = []
    for f, valid in enum_fields.items():
        if f not in scoped.columns: continue
        for _, r in scoped.iterrows():
            v = str(r.get(f, "")).strip()
            if not v or v.lower() in ("nan", "unknown"): continue
            if v in valid: continue
            if (str(r["id"]).strip(), f) in l1_keys: continue
            enum_rows.append({
                "record_id": str(r["id"]).strip(), "rule_id": "ENUM", "field": f,
                "severity": "medium", "worksheet_mode": "informed",
                "csv_value_old": v, "csv_unit_old": "", "csv_value_new": "", "csv_unit_new": "",
                "verdict": "", "target_field": "",
                "notes": f"non-canonical enum value {v!r}; valid: {sorted(valid)}",
                "topic_group": "ai", "confidence": "", "evidence_quote": "",
                "cao_number": str(r.get("cao_number", "")), "file_name": str(r.get("file_name", "")),
                "ingangsdatum": str(r.get("ingangsdatum", "")),
            })
    print(f"  enum-mismatch items: {len(enum_rows)}")

    out_df = pd.DataFrame(rule_rows + presence_rows + enum_rows)
    DET_OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(DET_OUT, sep=";", index=False)
    print(f"\nWrote {len(out_df)} deterministic-correction rows to {DET_OUT}")
    if len(out_df):
        for rid, n in out_df.groupby("rule_id").size().sort_values(ascending=False).items():
            print(f"  {rid}: {n}")
    logging_util.log_event("ai", "stage_2", "deterministic corrections produced",
                           rule_violations=len(rule_rows), l2_triggers=len(presence_rows),
                           enum_mismatches=len(enum_rows), total=len(out_df))


if __name__ == "__main__":
    main()
