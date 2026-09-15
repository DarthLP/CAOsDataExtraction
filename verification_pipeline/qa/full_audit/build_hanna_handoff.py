"""Consolidate every full-audit finding into ONE prioritized, action-labeled
review package for Hanna. Surface-only — applies nothing.

Inputs : full_audit_flags.csv (master flags + verify verdicts),
         verify/new_findings.csv (off-flag findings),
         unit_semantics_reconciliation.csv (rich disposition notes for the
         unit-kind contamination cells).
Outputs (under qa/full_audit/handoff/):
  hanna_review.csv          — all actionable rows, one per (record, field),
                              sorted by bucket: APPLY -> RELOCATE -> JUDGMENT -> KEEP
  01_corrections_to_apply.csv  (APPLY: concrete source-stated value changes)
  02_relocate.csv              (RELOCATE: move value to the correct field)
  03_needs_judgment.csv        (UNSUPPORTED / wrong-but-undetermined / no-source)
  04_keep_do_not_change.csv    (KEEP: valid data in a non-standard unit)
  HANDOFF_README.md         — how to read it; counts; the data-preservation rule.

Run:  python3.13 -m qa.full_audit.build_hanna_handoff
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
HANDOFF = HERE / "handoff"
COLS = ["priority", "bucket", "recommended_action", "record_id", "cao_number",
        "topic", "field", "current_value", "current_unit", "suggested_value",
        "source_quote", "confidence", "severity", "verdict", "origin",
        "reason_or_note"]


def _blank(s: str) -> bool:
    return str(s).strip().lower() in ("", "nan", "none", "null")


def _topic_of(ctx_or_field, field):
    ci = common.classify_columns().get(field)
    if ci:
        return ci.topic
    try:
        return json.loads(ctx_or_field).get("topic", "")
    except Exception:
        return ""


def build() -> None:
    HANDOFF.mkdir(exist_ok=True)
    flags = pd.read_csv(HERE / "full_audit_flags.csv", sep=";", dtype=str, keep_default_na=False)
    nf = pd.read_csv(HERE / "verify" / "new_findings.csv", sep=";", dtype=str, keep_default_na=False)
    rec_path = HERE / "unit_semantics_reconciliation.csv"
    recon = pd.read_csv(rec_path, sep=";", dtype=str, keep_default_na=False) if rec_path.exists() else pd.DataFrame()
    # (rid, field) -> rich disposition note for contamination cells
    disp = {(r["record_id"], r["field"]): r["disposition_note"] for _, r in recon.iterrows()}

    # value<->unit pairing: per (cao, unit_field) the modal non-blank unit across
    # the lineage, so a value-DROPOUT can also surface the unit that must be
    # recovered alongside the value (else a fixer fills the value but not the unit).
    from collections import Counter as _Ctr
    info = common.classify_columns()
    _modal: dict = {}
    for rec in common.load_dataset().to_dict("records"):
        cao = common.norm(rec.get("cao_number", ""))
        for col, ci in info.items():
            if ci.kind in ("unit", "numeric"):
                u = common.norm(rec.get(col, ""))
                if not _blank(u):
                    _modal.setdefault((cao, col), _Ctr())[u] += 1

    def _lineage_value(cao, value_field):
        c = _modal.get((cao, value_field))
        return c.most_common(1)[0][0] if c else ""

    def _lineage_unit(cao, value_field):
        ci = info.get(value_field)
        up = ci.unit_partner if ci else None
        if not up:
            return "", ""
        c = _modal.get((cao, up))
        return up, (c.most_common(1)[0][0] if c else "")

    rows = []

    def emit(bucket, prio, action, rid, cao, topic, field, cur, unit, sug,
             quote, conf, sev, verdict, origin, note):
        rows.append({
            "priority": prio, "bucket": bucket, "recommended_action": action,
            "record_id": rid, "cao_number": cao, "topic": topic, "field": field,
            "current_value": cur, "current_unit": unit, "suggested_value": sug,
            "source_quote": quote, "confidence": conf, "severity": sev,
            "verdict": verdict, "origin": origin, "reason_or_note": note,
        })

    # ---- flagged rows (master) -------------------------------------------------
    for _, r in flags.iterrows():
        v = r["verify_verdict"].strip()
        field = r["field"]
        topic = _topic_of(r.get("stat_context", ""), field)
        note = disp.get((r["record_id"], field), "") or r.get("reason", "")
        # 'dropout' = the value DROPPED OUT of this version's extraction (it is
        # MISSING) — recover/fill it, NOT delete. Surface the lineage candidate.
        is_dropout = "dropout" in r.get("reason", "")
        lin_v = _lineage_value(r["cao_number"], field) if is_dropout else ""
        up, lin_u = (_lineage_unit(r["cao_number"], field)
                     if (is_dropout and _blank(r["unit"])) else ("", ""))
        if is_dropout and lin_u:
            note = note + (f" || PAIRED UNIT '{up}' is also blank — lineage "
                           f"unit = '{lin_u}': recover the VALUE and the UNIT together.")
        common_args = dict(rid=r["record_id"], cao=r["cao_number"], topic=topic,
                           field=field, cur=r["value"], unit=r["unit"],
                           quote=r.get("verify_quote", ""), conf="",
                           sev=r.get("severity", ""), verdict=v,
                           origin="flagged", note=note)
        if v == "NEEDS_CHANGE":
            if not _blank(r["verify_suggested_value"]):
                emit("1_APPLY", 1, f"Change to {r['verify_suggested_value']!s} (source-stated)",
                     sug=r["verify_suggested_value"], **common_args)
            else:
                emit("3_JUDGMENT", 3, "Value is wrong but source gives no replacement — review",
                     sug="", **common_args)
        elif v == "RELOCATE":
            emit("2_RELOCATE", 2, "Move value to the correct field (see note); do not delete",
                 sug="", **common_args)
        elif v == "UNSUPPORTED":
            sg = r["verify_suggested_value"]
            act = (f"Current value not supported by source; source suggests '{sg}' — verify and decide"
                   if not _blank(sg) else
                   "Not supported by source — verify, then keep or remove")
            emit("3_JUDGMENT", 3, act, sug=sg, **common_args)
        elif v == "KEEP_NONSTANDARD_UNIT":
            emit("4_KEEP", 4, "VALID value in non-standard unit — PRESERVE; do not delete/convert",
                 sug="", **common_args)
        elif v == "unverified_no_source":
            if is_dropout:
                cand = f"'{lin_v}'" + (f" + unit '{lin_u}'" if lin_u else "")
                act = (f"LIKELY MISSING value (this is a 'dropout' = the value dropped "
                       f"out of this version's extraction, NOT a 'delete'); lineage "
                       f"suggests {cand}. No source section to confirm — check the "
                       f"original document and FILL.")
            else:
                act = "No source section available to verify — manual check"
            emit("3_JUDGMENT", 3, act, sug="", **common_args)
        # CONFIRM / pending / blank-low -> no action, omitted from handoff

    # ---- off-flag new findings -------------------------------------------------
    for _, r in nf.iterrows():
        v = r["verdict"].strip()
        cur = r.get("current_value", "")
        args = dict(rid=r["record_id"], cao=r["cao_number"],
                    topic=r.get("topic", ""), field=r["field"], cur=cur,
                    unit="", quote=r.get("quote", ""), conf=r.get("confidence", ""),
                    sev="", verdict=v, origin="off-flag", note=r.get("note", ""))
        if v == "NEEDS_CHANGE" and not _blank(r.get("suggested_value", "")):
            emit("1_APPLY", 1, f"Change to {r['suggested_value']!s} (source-stated, off-flag)",
                 sug=r["suggested_value"], **args)
        elif v == "NEEDS_CHANGE":
            emit("3_JUDGMENT", 3, "Wrong but no replacement value — review (off-flag)", sug="", **args)
        elif v == "UNSUPPORTED":
            sg = r.get("suggested_value", "")
            act = (f"Current value not supported; source suggests '{sg}' — verify (off-flag)"
                   if not _blank(sg) else
                   "Not supported by source — verify, then keep or remove (off-flag)")
            emit("3_JUDGMENT", 3, act, sug=sg, **args)

    df = pd.DataFrame(rows, columns=COLS)
    df = df.sort_values(["priority", "topic", "field", "record_id"]).reset_index(drop=True)
    df.to_csv(HANDOFF / "hanna_review.csv", sep=";", index=False)

    splits = {"1_APPLY": "01_corrections_to_apply.csv",
              "2_RELOCATE": "02_relocate.csv",
              "3_JUDGMENT": "03_needs_judgment.csv",
              "4_KEEP": "04_keep_do_not_change.csv"}
    counts = {}
    for bucket, fname in splits.items():
        sub = df[df["bucket"] == bucket]
        sub.to_csv(HANDOFF / fname, sep=";", index=False)
        counts[bucket] = len(sub)

    readme = f"""# Full-audit findings — review package for Hanna

Generated by `qa/full_audit/build_hanna_handoff.py`. **Surface-only: nothing has
been applied to any dataset.** `corrected_dataset.csv` and the raw extract are
untouched. These files tell you what the audit found; you decide what to apply.

`hanna_review.csv` is everything actionable in one place ({len(df)} rows, one per
(record, field)), sorted into four buckets. The per-bucket CSVs are the same rows
split out:

| File | Rows | What to do |
|---|---:|---|
| `01_corrections_to_apply.csv` | {counts['1_APPLY']} | **APPLY** — the source explicitly states a different value (`suggested_value`), with a verbatim `source_quote`. The safest changes. |
| `02_relocate.csv` | {counts['2_RELOCATE']} | **RELOCATE** — the value is real but sits in the wrong field; move it (see note). Do **not** delete. |
| `03_needs_judgment.csv` | {counts['3_JUDGMENT']} | **JUDGMENT** — UNSUPPORTED (not found in source), wrong-but-undetermined, or no-source. Verify before removing. |
| `04_keep_do_not_change.csv` | {counts['4_KEEP']} | **KEEP** — valid data in a non-standard unit (e.g. maternity "100% of salary" in a duration field, where the source states no weeks). **PRESERVE; do not delete or convert.** A schema/unit normalization is a separate decision. |

## A word on "dropout" (it does NOT mean delete)

Some reasons start with `[dropout]`. That is jargon for *the value dropped OUT of
this version's extraction* — i.e. the cell is **blank but probably shouldn't be**,
because the other versions of the same CAO all carry a value. The action is to
**recover/fill** it (the note gives the lineage's value and unit, e.g. `2.0` +
`months`), **never to delete it**. `suggested_value` stays blank for these because
the value comes from the *lineage*, not the *source text* — confirm against the
original document before filling.

## The data-preservation rule (important)

The audit never deletes. For the unit-kind mismatches especially, ~87% are KEEP:
the displaced value is the only entitlement data present and the source gives no
alternative, so blanking it would lose real information. Apply the `01` bucket
freely (source-grounded); treat `04` as "leave alone unless you decide to
re-encode the schema."

Columns: priority, bucket, recommended_action, record_id, cao_number, topic,
field, current_value, current_unit, suggested_value, source_quote, confidence,
severity, verdict, origin (flagged vs off-flag), reason_or_note.

CONFIRM verdicts (value verified correct) are omitted — no action needed.
Full provenance remains in `../full_audit_flags.csv` and `../unit_semantics_reconciliation.csv`.
"""
    (HANDOFF / "HANDOFF_README.md").write_text(readme, encoding="utf-8")

    print(f"[handoff] hanna_review.csv: {len(df)} actionable rows -> {HANDOFF}")
    for b, fn in splits.items():
        print(f"  {fn:32s} {counts[b]}")
    print(f"  (CONFIRM/pending/low omitted — no action)")
    print(f"  README -> {HANDOFF / 'HANDOFF_README.md'}")


if __name__ == "__main__":
    build()
