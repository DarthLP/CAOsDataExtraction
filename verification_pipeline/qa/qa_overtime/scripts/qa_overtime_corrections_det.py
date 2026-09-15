"""qa_overtime_corrections_det.py — Stage 2 driver for overtime.

Runs:
  1. L1 rules (qa_overtime_rules.py)
  2. L2 presence scan (qa.shared.presence_scan)

Combines into a single corrections_deterministic.csv aligned to the
aggregator's expected schema (PLAN.md §1.4 + Appendix A).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Resolve project root + shared lib import
HERE = Path(__file__).resolve().parent
QA_OVERTIME = HERE.parent
PROJECT_ROOT = QA_OVERTIME.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from qa.shared import (presence_scan, source_text_loader, schema_lookup,
                       logging_util)  # noqa: E402

# Import the rules module from the same scripts dir
sys.path.insert(0, str(HERE))
import qa_overtime_rules  # noqa: E402


SCOPED_CSV = QA_OVERTIME / "inputs" / "scoped_records.csv"
DET_OUT = QA_OVERTIME / "outputs" / "corrections_deterministic.csv"
RULE_OUT = QA_OVERTIME / "outputs" / "overtime_rule_violations.csv"


def main():
    # 1. Run L1 rules
    print(f"=== Stage 2 (overtime) ===")
    print(f"Loading {SCOPED_CSV} ...")
    scoped = pd.read_csv(SCOPED_CSV, sep=";", dtype=str, keep_default_na=False)
    print(f"  scoped records: {len(scoped)}")

    rule_rows = []
    for _, row in scoped.iterrows():
        rid = str(row.get("id", "")).strip()
        for rule in qa_overtime_rules.RULES:
            v = rule(row, rid)
            if v is not None:
                v["cao_number"] = str(row.get("cao_number", ""))
                v["file_name"] = str(row.get("file_name", ""))
                v["ingangsdatum"] = str(row.get("ingangsdatum", ""))
                rule_rows.append(v)
    print(f"  L1 rule violations: {len(rule_rows)}")

    # 2. Run L2 presence scan
    print(f"Running L2 presence scan ...")
    source_text_loader.clear_cache()
    presence_df = presence_scan.scan_topic_presence("overtime", scoped)
    print(f"  L2 presence triggers (empty field + topic in source): {len(presence_df)}")

    # 2a. Filter out L2 triggers that overlap with L1 rule firings.
    # An L1 rule fires when there's something specific to fix (verdict, proposed
    # correction). L2 just says "topic in source, field empty — please extract".
    # When both fire on the same (record_id, field), L1 wins — its proposed
    # correction is more informative. The aggregator dedups by last-write-wins,
    # so we must drop L2 rows BEFORE writing the combined CSV.
    l1_keys = {(v["record_id"], v["field"]) for v in rule_rows}
    pre = len(presence_df)
    presence_df = presence_df[~presence_df.apply(
        lambda p: (str(p["record_id"]), str(p["field"])) in l1_keys, axis=1)]
    print(f"  filtered L2 overlaps with L1: {pre - len(presence_df)} dropped, "
          f"{len(presence_df)} remain")

    # 3. Build presence rows in the same schema as rule violations
    presence_rows = []
    for _, p in presence_df.iterrows():
        presence_rows.append({
            "record_id": str(p["record_id"]),
            "rule_id": "L2",
            "field": str(p["field"]),
            "severity": "medium",
            "worksheet_mode": "extract",
            "csv_value_old": "",
            "csv_unit_old": "",
            "csv_value_new": "",
            "csv_unit_new": "",
            "verdict": "",
            "target_field": "",
            "notes": f"L2: topic in source, field empty. {p['matched_phrases'][:120]}",
            "topic_group": "overtime",
            "confidence": "",
            "evidence_quote": str(p.get("evidence_excerpt", ""))[:200],
            "cao_number": "",
            "file_name": "",
            "ingangsdatum": "",
        })
    # Backfill metadata from scoped CSV
    metadata = {str(r["id"]).strip(): (r.get("cao_number", ""),
                                       r.get("file_name", ""),
                                       r.get("ingangsdatum", ""))
                for _, r in scoped.iterrows()}
    for row in presence_rows:
        cao, fn, ing = metadata.get(row["record_id"], ("", "", ""))
        row["cao_number"] = cao
        row["file_name"] = fn
        row["ingangsdatum"] = ing

    # 3a. Enum-value mismatch scan (populated enum fields with non-canonical values)
    enum_fields = schema_lookup.get_topic_enum_fields("overtime")
    enum_rows = []
    l1_keys = {(v["record_id"], v["field"]) for v in rule_rows}
    for f, valid in enum_fields.items():
        if f not in scoped.columns:
            continue
        for _, r in scoped.iterrows():
            v = str(r.get(f, "")).strip()
            if not v or v.lower() in ("nan", "unknown"):
                continue
            if v in valid:
                continue
            key = (str(r["id"]).strip(), f)
            if key in l1_keys:
                continue
            enum_rows.append({
                "record_id": str(r["id"]).strip(),
                "rule_id": "ENUM",
                "field": f,
                "severity": "medium",
                "worksheet_mode": "informed",
                "csv_value_old": v, "csv_unit_old": "",
                "csv_value_new": "", "csv_unit_new": "",
                "verdict": "", "target_field": "",
                "notes": f"non-canonical enum value {v!r}; valid: {sorted(valid)}",
                "topic_group": "overtime",
                "confidence": "",
                "evidence_quote": "",
                "cao_number": str(r.get("cao_number", "")),
                "file_name":  str(r.get("file_name", "")),
                "ingangsdatum": str(r.get("ingangsdatum", "")),
            })
    print(f"  enum-mismatch items: {len(enum_rows)}")

    # 4. Combine
    combined = rule_rows + presence_rows + enum_rows
    if not combined:
        print("No deterministic corrections produced.")
        return

    # 5. Write deterministic-corrections in aggregator-input shape
    # Aggregator expects: record_id, field, csv_value_new, csv_unit_new,
    # verdict, target_field, evidence_quote, confidence, worksheet_mode,
    # severity, notes, topic_group, cao_number, file_name, ingangsdatum.
    out_df = pd.DataFrame(combined)
    DET_OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(DET_OUT, sep=";", index=False)
    print(f"Wrote {len(out_df)} deterministic-correction rows to {DET_OUT}")

    # Also save the L1 violations as a separate audit trail
    rule_df = pd.DataFrame(rule_rows)
    rule_df.to_csv(RULE_OUT, sep=";", index=False)

    # 6. Summary
    print(f"\nBy rule_id:")
    for rule_id, count in out_df.groupby("rule_id").size().sort_values(
            ascending=False).items():
        print(f"  {rule_id}: {count}")

    print(f"\nBy worksheet_mode:")
    for wm, count in out_df.groupby("worksheet_mode").size().sort_values(
            ascending=False).items():
        print(f"  {wm}: {count}")

    # 7. PLAN.md §0.4 stop conditions:
    # - severity in {medium, high} AND worksheet_mode='none' AND >25% of records
    bad = out_df[(out_df["severity"].isin(["medium", "high"])) &
                 (out_df["worksheet_mode"] == "none")]
    affected_records = bad["record_id"].nunique()
    if affected_records > 0.25 * len(scoped):
        print(f"\n⚠️  STOP CONDITION: {affected_records} records affected by silent "
              f"auto-correct rules (>25% of {len(scoped)}). Review rules before "
              f"Stage 3.")

    logging_util.log_event("overtime", "stage_2",
                          "deterministic corrections produced",
                          rule_violations=len(rule_rows),
                          l2_triggers=len(presence_rows),
                          total=len(out_df))


if __name__ == "__main__":
    main()
