"""Build the data-preserving reconciliation deliverable for the unit-kind
mismatch (Check 5) flags: qa/full_audit/unit_semantics_reconciliation.csv.

Per flagged cell it records — the ORIGINAL value+unit preserved verbatim, the
field's intended vs detected unit family, the preservation-aware verdict and
disposition note, the source quote, and any sibling *_note that holds the
recoverable duration. NOTHING is deleted; everything is surfaced for Hanna.

Run:  python3.13 -m qa.full_audit.build_unit_semantics_reconciliation
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from qa.full_audit import verify as v
from qa.full_audit import common

OUT = Path(__file__).resolve().parent / "unit_semantics_reconciliation.csv"
COLS = ["record_id", "cao_number", "field", "topic",
        "original_value", "original_unit", "field_intended_kind",
        "detected_kind", "verdict", "source_stated_value", "source_quote",
        "confidence", "disposition_note", "sibling_note"]

# topic -> the *_note field most likely to hold the recoverable duration
_NOTE_FOR = {
    "leave_paid_maternity_value": "leave_maternity_note",
    "leave_partially_paid_maternity_value": "leave_maternity_note",
    "leave_unpaid_maternity_value": "leave_maternity_note",
    "leave_parental_unpaid_value": "leave_parental_note",
}


def build() -> None:
    # 1) preservation-aware verdicts from the topup_check5 outputs
    verdicts: dict[tuple[str, str], dict] = {}
    for f in sorted((v.OUTPUTS_DIR / "topup_check5").rglob("*_verified.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            rid, fld = str(o.get("record_id", "")), str(o.get("field", ""))
            if rid and fld:
                verdicts[(rid, fld)] = o

    # 2) family info from the unit_semantics flag rows
    fam: dict[tuple[str, str], dict] = {}
    usf = pd.read_csv(common.PROJECT_ROOT / "qa" / "full_audit" / "unit_semantics.csv",
                      sep=";", dtype=str, keep_default_na=False) \
        if (common.PROJECT_ROOT / "qa" / "full_audit" / "unit_semantics.csv").exists() \
        else pd.DataFrame()
    for _, r in usf.iterrows():
        try:
            ctx = json.loads(r["stat_context"])
        except (ValueError, KeyError):
            ctx = {}
        fam[(r["record_id"], r["field"])] = ctx

    # 3) dataset for original value/unit + sibling notes
    ds = common.load_dataset()
    by_id = {common.norm(r.get("id", "")): r for r in ds.to_dict("records")}

    rows = []
    for (rid, fld), o in verdicts.items():
        rec = by_id.get(rid, {})
        ci = common.classify_columns().get(fld)
        ctx = fam.get((rid, fld), {})
        note_field = _NOTE_FOR.get(fld, "")
        sibling_note = common.norm(rec.get(note_field, "")) if note_field else ""
        rows.append({
            "record_id": rid,
            "cao_number": common.norm(rec.get("cao_number", "")),
            "field": fld,
            "topic": ci.topic if ci else "",
            "original_value": common.norm(rec.get(fld, "")),
            "original_unit": common.norm(rec.get(ci.unit_partner, "")) if ci and ci.unit_partner else "",
            "field_intended_kind": ctx.get("canonical_family", ""),
            "detected_kind": ctx.get("unit_family", ""),
            "verdict": str(o.get("verdict", "")),
            "source_stated_value": str(o.get("suggested_value", "")),
            "source_quote": str(o.get("quote", "")),
            "confidence": str(o.get("confidence", "")),
            "disposition_note": str(o.get("note", "")),
            "sibling_note": sibling_note,
        })

    order = {"NEEDS_CHANGE": 0, "RELOCATE": 1, "KEEP_NONSTANDARD_UNIT": 2,
             "UNSUPPORTED": 3, "CONFIRM": 4}
    rows.sort(key=lambda x: (order.get(x["verdict"], 9), x["field"], x["record_id"]))
    df = pd.DataFrame(rows, columns=COLS)
    df.to_csv(OUT, sep=";", index=False)
    print(f"[reconciliation] {len(df)} rows -> {OUT}")
    print(df["verdict"].value_counts().to_string())
    print(f"\nORIGINAL VALUES PRESERVED for all {len(df)} rows; none deleted.")


if __name__ == "__main__":
    build()
