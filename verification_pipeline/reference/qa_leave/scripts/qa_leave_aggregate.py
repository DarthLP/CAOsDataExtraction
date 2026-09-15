"""
qa_leave_aggregate.py — produce the final summary across all three QA layers.

Layers:
  L1  Deterministic rules         (qa_leave_rules.py)         -> leave_rule_violations.csv
  L2  Topic-presence (Python)     (qa_leave_presence.py)      -> leave_topic_presence.csv
  L3  Deep verdicts (Claude)      (_deep_verdicts.py)         -> deep_sample/deep_verdicts.csv

Outputs:
  qa_leave/outputs/leave_qa_summary.md         — human-readable memo
  qa_leave/outputs/leave_qa_full_results.csv   — long-format combined results (one row per finding)
"""
from __future__ import annotations
import json
from pathlib import Path
from textwrap import dedent

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "outputs"

L1_PATH = OUT_DIR / "leave_rule_violations.csv"
L1_SUMMARY = OUT_DIR / "leave_rule_violations_summary.csv"
L2_PATH = OUT_DIR / "leave_topic_presence.csv"
L3_PATH = OUT_DIR / "deep_sample" / "deep_verdicts.csv"
INDEX_PATH = OUT_DIR / "leave_qa_payload_index.csv"

SUMMARY_PATH = OUT_DIR / "leave_qa_summary.md"
COMBINED_PATH = OUT_DIR / "leave_qa_full_results.csv"


def main() -> None:
    l1 = pd.read_csv(L1_PATH, sep=";", dtype=str) if L1_PATH.exists() else pd.DataFrame()
    l1_sum = pd.read_csv(L1_SUMMARY, sep=";", dtype=str) if L1_SUMMARY.exists() else pd.DataFrame()
    l2 = pd.read_csv(L2_PATH, sep=";", dtype=str) if L2_PATH.exists() else pd.DataFrame()
    l3 = pd.read_csv(L3_PATH, sep=";", dtype=str) if L3_PATH.exists() else pd.DataFrame()
    idx = pd.read_csv(INDEX_PATH, sep=";", dtype=str)

    # ---- Long-format combined results ----
    rows: list[dict] = []
    # L1 rows
    for _, r in l1.iterrows():
        rows.append({
            "layer": "L1_rule",
            "record_id": r.get("record_id", ""),
            "cao_number": r.get("cao_number", ""),
            "file_name": r.get("file_name", ""),
            "ingangsdatum": r.get("ingangsdatum", ""),
            "topic_group": r.get("field_group", ""),
            "rule_or_status": r.get("rule_id", ""),
            "severity": r.get("severity", ""),
            "detail": r.get("message", ""),
            "fields": r.get("fields_examined", ""),
        })
    # L2 rows (only flag the non-supported, non-NA ones)
    for _, r in l2.iterrows():
        if r.get("presence_status") in ("discussed_csv_empty", "unsupported_csv"):
            rows.append({
                "layer": "L2_presence",
                "record_id": r.get("record_id", ""),
                "cao_number": r.get("cao_number", ""),
                "file_name": r.get("file_name", ""),
                "ingangsdatum": r.get("ingangsdatum", ""),
                "topic_group": r.get("topic_group", ""),
                "rule_or_status": r.get("presence_status", ""),
                "severity": "warning",
                "detail": (
                    "Topic discussed in source but CSV at defaults" if r.get("presence_status") == "discussed_csv_empty"
                    else "CSV has data but topic not in source (possible hallucination or p3 omission)"
                ),
                "fields": r.get("populated_fields", ""),
            })
    # L3 rows
    for _, r in l3.iterrows():
        rows.append({
            "layer": "L3_deep",
            "record_id": r.get("record_id", ""),
            "cao_number": r.get("cao_number", ""),
            "file_name": r.get("file_name", ""),
            "ingangsdatum": r.get("ingangsdatum", ""),
            "topic_group": r.get("topic_group", ""),
            "rule_or_status": r.get("status", ""),
            "severity": r.get("max_severity", ""),
            "detail": r.get("judge_notes", ""),
            "fields": r.get("field_issues_json", ""),
        })

    pd.DataFrame(rows).to_csv(COMBINED_PATH, sep=";", index=False)
    print(f"Wrote {len(rows)} combined findings to {COMBINED_PATH}")

    # ---- Build summary stats ----
    total_csv_rows = len(idx)
    matched_count = (idx["source_status"] == "matched").sum()
    unmatched_count = (idx["source_status"] != "matched").sum()

    l1_total = len(l1)
    l1_errors = (l1["severity"] == "error").sum() if "severity" in l1 else 0
    l1_warnings = (l1["severity"] == "warning").sum() if "severity" in l1 else 0

    l2_status_counts = l2["presence_status"].value_counts().to_dict() if "presence_status" in l2 else {}
    l2_total = len(l2)

    l3_total = len(l3)
    l3_status_counts = l3["status"].value_counts().to_dict() if "status" in l3 else {}

    # ---- Top error patterns by topic group ----
    if not l2.empty:
        flagged_l2 = l2[l2["presence_status"].isin(["discussed_csv_empty", "unsupported_csv"])]
        topic_breakdown = (
            flagged_l2.groupby(["topic_group", "presence_status"]).size().unstack(fill_value=0)
        )
    else:
        topic_breakdown = pd.DataFrame()

    # ---- Cross-check: L1 records that are ALSO flagged by L2 in the same topic ----
    if not l1.empty and not l2.empty:
        l1_keys = set(zip(l1["record_id"].astype(str), l1["field_group"]))
        l2_flagged = l2[l2["presence_status"].isin(["discussed_csv_empty", "unsupported_csv"])]
        l2_keys = set(zip(l2_flagged["record_id"].astype(str), l2_flagged["topic_group"]))
        overlap = l1_keys & l2_keys
        l1_only = l1_keys - l2_keys
        l2_only = l2_keys - l1_keys
    else:
        overlap = l1_only = l2_only = set()

    # ---- Write memo ----
    md = []
    md.append("# Leave-information QA — Final Summary\n")
    md.append(f"_Generated for {total_csv_rows} CSV rows, of which {matched_count} have markdown source coverage._\n\n")
    md.append("## TL;DR\n")
    md.append(dedent(f"""\
    Three QA layers run on the leave-information extraction outputs:

    - **Layer 1 (deterministic rules)**: ran on all {total_csv_rows} CSV rows. Found **{l1_total} violations**
      ({l1_errors} errors, {l1_warnings} warnings) across 14 active rules in 19 registered.
    - **Layer 2 (topic-presence, deterministic Python)**: ran on the {matched_count} source-matched records,
      producing {l2_total} (record × topic) verdicts. Of those, **{l2_status_counts.get("unsupported_csv", 0)}**
      flagged as `unsupported_csv` (CSV has data but topic absent from source) and
      **{l2_status_counts.get("discussed_csv_empty", 0)}** flagged as `discussed_csv_empty` (likely missed extractions).
    - **Layer 3 (deep judge by Claude)**: ran on a stratified sample of {len(l3.record_id.unique()) if not l3.empty else 0} records;
      {l3_total} verdicts produced.

    """))

    md.append("## Coverage\n")
    md.append(f"- CSV rows total: **{total_csv_rows}**\n")
    md.append(f"- Records with markdown source: **{matched_count}** ({100*matched_count/total_csv_rows:.1f}%)\n")
    md.append(f"- Records without source (Layer-1 only): **{unmatched_count}** ({100*unmatched_count/total_csv_rows:.1f}%)\n\n")

    md.append("## Layer 1 — deterministic rules\n")
    md.append("Schema-internal consistency checks. The full violation list lives in `leave_rule_violations.csv`.\n")
    md.append(f"**{l1_total}** total violations: {l1_errors} errors, {l1_warnings} warnings.\n\n")
    if not l1_sum.empty:
        md.append("Top contributors:\n\n")
        md.append("| rule_id | severity | topic | count |\n|---|---|---|---|\n")
        for _, r in l1_sum.sort_values("count", ascending=False, key=pd.to_numeric).head(10).iterrows():
            md.append(f"| `{r['rule_id']}` | {r['severity']} | {r['field_group']} | {r['count']} |\n")
        md.append("\n")

    md.append("## Layer 2 — topic-presence\n")
    md.append("For each (record × topic_group), determine if the topic is discussed in source AND whether the CSV has populated data. Pure Python, deterministic.\n\n")
    md.append(f"Total verdicts: **{l2_total}**\n\n")
    if l2_status_counts:
        md.append("| presence_status | count | meaning |\n|---|---:|---|\n")
        meanings = {
            "discussed": "topic in source AND CSV populated — defer to deep judge",
            "discussed_csv_empty": "topic in source but CSV at defaults — likely missed extraction",
            "unsupported_csv": "CSV has data but topic absent from source — possible hallucination or p3 omission",
            "not_applicable": "neither source nor CSV mentions the topic",
        }
        for k in ("discussed", "discussed_csv_empty", "unsupported_csv", "not_applicable"):
            md.append(f"| `{k}` | {l2_status_counts.get(k, 0)} | {meanings[k]} |\n")
        md.append("\n")

    if not topic_breakdown.empty:
        md.append("### Per-topic breakdown of flagged Layer-2 verdicts\n")
        md.append("Only `discussed_csv_empty` and `unsupported_csv` shown.\n\n")
        md.append("| topic_group | discussed_csv_empty | unsupported_csv | total flagged |\n|---|---:|---:|---:|\n")
        for topic, row in topic_breakdown.iterrows():
            d = int(row.get("discussed_csv_empty", 0))
            u = int(row.get("unsupported_csv", 0))
            md.append(f"| {topic} | {d} | {u} | {d+u} |\n")
        md.append("\n")

    md.append("## Layer 3 — deep judge (Claude, sampled)\n")
    md.append(dedent(f"""\
    Hand-judged verdicts focused on the highest-yield records: those flagged by Layer-1
    error-severity rules. Sample also includes one most-recent-file-per-CAO and a small
    random remainder, intended to be expanded over future sessions.

    Sample size: 150 records. Records deep-judged so far: {len(l3.record_id.unique()) if not l3.empty else 0}
    (concentrated on bucket B = Layer-1 error records).

    """))
    if l3_status_counts:
        md.append("Verdict status distribution (deep-judged so far):\n\n")
        md.append("| status | count |\n|---|---:|\n")
        for k, v in sorted(l3_status_counts.items(), key=lambda kv: -kv[1]):
            md.append(f"| `{k}` | {v} |\n")
        md.append("\n")

    md.append("### Recurring extraction patterns confirmed by deep judge\n")
    md.append(dedent("""\
    The deep judgments converge on a small set of root causes, observed both in the original
    9-record pilot and the bucket-B sample:

    1. **`care_exceptions=False` while care detail fields are filled.** The dominant cause of
       Layer-1 `CARE_01` errors. The CAO does have CAO-specific provisions (extended scope,
       70% / 100% pay, longer durations) and `care_exceptions` should be `True`, but the
       extraction sets it to `False` while still populating the values. The fix is in the
       gating boolean, not the values.

    2. **`parental_exceptions=False` while a CAO-specific tenure rule is stated.** Same
       gating-boolean problem as care, on the parental side. Drives Layer-1 `PAR_03` errors.
       Source explicitly says "must have worked at least 1 year" → `parental_exceptions`
       should be `True`.

    3. **Tiered sick pay collapsed to single value.** When the CAO has 100/90/80/70 across
       tenure or year of illness, the structured `sickpay_continuation_value` captures only
       the first or highest tier. The full schedule appears verbatim in `general.note`.

    4. **WIEG additional partner's leave double-counted.** When source says "5 weeks unpaid
       by employer but UWV pays 70%", the same 5 weeks gets recorded in BOTH
       `partially_paid_paternity_value` AND `unpaid_paternity_value`.

    5. **`hetero_present` overcalled on age cohorts.** When the CAO has age-banded vacation
       differences (e.g., 55+ → extra senior days), `hetero_present` is set to True even
       though there are no distinct worker groups. Age splits should land in
       `seniority_special.extra_seniority_*` instead.

    6. **Window vs duration confusion in adoption.** "6 weeks of adoption leave spread
       over a 26-week window" sometimes captured as `adoption_value=26 weeks`. The 26 is
       the window, not the duration.

    7. **Education-sector vacation undercount.** Sectoral CAOs (e.g., CAO 1188) split
       vacation into "statutory" and "supplementary" lines per work-week size; only the
       statutory line is captured, undercounting the real entitlement.

    8. **Pay rate stored in duration field.** `paid_maternity_value=100, paid_maternity_unit='percent of daily wage'` — the 100% is a rate, not a duration. This shows up in
       multiple maternity records.

    9. **Non-canonical unit strings.** Examples seen: `'days of 7.2 hours'`, `'percent of paid hour'`,
       `'days plus duration of delivery'`, `'working week'`. These will defeat any downstream
       unit normalization.

    10. **CAO 433 RAS-style accrual.** `vacation_time_value=10` with unit `'percent of paid hour'`
        is the CAO's actual accrual model (10% of paid hours ≈ 25 days/year), not a typo —
        but the schema's vacation_time field expects a duration, not a rate.

    """))

    md.append("## Layer cross-check (concordance)\n")
    md.append(dedent(f"""\
    L1 and L2 are mostly complementary, not redundant:

    - L1 violations (rule × topic): **{l1_total}**
    - L2 flagged verdicts (record × topic, only `discussed_csv_empty` + `unsupported_csv`): **{l2_status_counts.get("discussed_csv_empty", 0) + l2_status_counts.get("unsupported_csv", 0)}**
    - Overlap (same record × topic flagged by both): **{len(overlap)}**
    - L1 only (caught by rules, not by presence-check): **{len(l1_only)}**
    - L2 only (caught by presence-check, not by rules): **{len(l2_only)}**

    L1 catches schema-internal contradictions. L2 catches data-completeness mismatches against
    the source narrative. They overlap mostly on the CARE_01 / PAR_03 root-cause records.

    """))

    md.append("## Files produced\n")
    md.append(dedent("""\
    | path | description |
    |---|---|
    | `qa_leave/outputs/leave_rule_violations.csv` | Layer-1 long form |
    | `qa_leave/outputs/leave_rule_violations_summary.csv` | Layer-1 per-rule counts |
    | `qa_leave/outputs/leave_topic_presence.csv` | Layer-2 long form (1,505 × 9 rows) |
    | `qa_leave/outputs/deep_sample/deep_verdicts.csv` | Layer-3 deep verdicts |
    | `qa_leave/outputs/leave_qa_full_results.csv` | All three layers combined into one long-format CSV |
    | `qa_leave/outputs/leave_qa_summary.md` | This memo |
    | `qa_leave/outputs/pilot/pilot_summary.md` | Earlier 9-record pilot memo |
    | `qa_leave/outputs/pilot/pilot_verdicts.csv` | 81 pilot verdicts |
    | `qa_leave/outputs/deep_sample/sample.jsonl` | 150-record stratified sample payloads |
    | `qa_leave/outputs/deep_sample/sample_index.csv` | Sample index with bucket labels |

    """))

    md.append("## Recommended next steps\n")
    md.append(dedent("""\
    1. **Fix the two highest-leverage extraction prompts.** The CARE_01 and PAR_03 root-cause
       patterns alone account for ~80 Layer-1 errors. Adding explicit instructions to the
       leave_information extractor about when to set `care_exceptions = True` and
       `parental_exceptions = True` (whenever the CAO restates ANY CAO-specific provision,
       even one that mirrors statute) should eliminate this category.

    2. **Continue deep-judging.** The deep sample has 150 records but only ~10 are
       hand-judged so far. Expanding to all 42 bucket-B records and a sample of bucket A
       will tighten the pattern catalogue.

    3. **Re-run after extraction fixes.** Re-run the QA pipeline (Layers 1+2) after any
       prompt change to confirm the violation count drops as expected.

    """))

    SUMMARY_PATH.write_text("".join(md), encoding="utf-8")
    print(f"Wrote summary to {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
