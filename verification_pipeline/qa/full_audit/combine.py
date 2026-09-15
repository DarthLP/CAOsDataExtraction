"""Combiner — dedup + prioritize the four checks into full_audit_flags.csv.

Reads the four per-check CSVs (each in common.FLAG_COLUMNS form) and produces the
single deliverable `full_audit_flags.csv` with the EXACT required schema:

    record_id; cao_number; file_name; ingangsdatum; field; value; unit; check;
    severity; reason; stat_context; verify_verdict; verify_quote;
    verify_suggested_value; suggested_review

Dedup / overlap policy
----------------------
A flag is a complaint about ONE cell = (record_id, field). Different checks
routinely complain about the same cell (e.g. a numeric value can trip Check 1's
robust_fence AND Check 2's numeric_odd). Those are ONE thing for a human to
verify, so we MERGE all signals for a (record_id, field) into a single row:
  - severity      = the strongest among the merged signals
  - check         = pipe-joined unique high-level check names
  - reason        = each signal's reason, prefixed with its [subcheck] label
  - stat_context  = structured JSON preserving topic + every signal's
                    subcheck/severity/reason/context (nothing is lost)
This collapses intra-check overlaps (same cell, two subchecks) too.

Verification columns (verify_verdict / verify_quote / verify_suggested_value /
suggested_review) are written EMPTY here; the holistic verification stage fills
them in afterwards.

Prioritization (sort)
---------------------
Primary: severity (high → medium → low).
Secondary: per-record flag count (records with more flagged cells first — a
record bristling with flags is a likelier systemic problem), then record_id and
field for determinism.

Run:  python3.13 -m qa.full_audit.combine
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
OUT = HERE / "full_audit_flags.csv"

# The per-check CSVs (high-level check name -> file).
CHECK_FILES = {
    "numeric_outlier": HERE / "outliers_numeric.csv",
    "cross_version": HERE / "cross_version.csv",
    "enum_format": HERE / "enum_format.csv",
    "value_unit_contamination": HERE / "value_unit_contamination.csv",
    "unit_semantics": HERE / "unit_semantics.csv",
}

# EXACT deliverable schema (do not reorder/rename).
FINAL_COLUMNS = [
    "record_id", "cao_number", "file_name", "ingangsdatum", "field", "value",
    "unit", "check", "severity", "reason", "stat_context",
    "verify_verdict", "verify_quote", "verify_suggested_value",
    "suggested_review",
]

# Severity scope that the verification stage will actually source-check. LOW
# signals (pervasive boolean toggles, orphan units, implausible-but-parseable
# dates) are surfaced in the CSV but flagged out of the verify scope.
VERIFY_SEVERITIES = {"high", "medium"}


def load_all() -> list[dict]:
    """Read every existing per-check CSV into internal FLAG_COLUMNS dicts."""
    from qa.shared import resilient_csv
    rows: list[dict] = []
    for check, path in CHECK_FILES.items():
        if not path.exists():
            continue
        df = resilient_csv.read_csv(path, delimiter=";")
        for r in df.to_dict("records"):
            r = {k: common.norm(v) for k, v in r.items()}
            if not r.get("record_id") and not r.get("field"):
                continue
            r["check"] = r.get("check") or check
            rows.append(r)
    return rows


def _parse_ctx(s: str):
    try:
        return json.loads(s) if s else {}
    except (ValueError, TypeError):
        return {"raw": s}


def merge(flags: list[dict]) -> list[dict]:
    """Merge per-check flags into one row per (record_id, field) cell."""
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for f in flags:
        groups[(f.get("record_id", ""), f.get("field", ""))].append(f)

    merged: list[dict] = []
    for (rid, field), signals in groups.items():
        # strongest signal first; stable secondary by check name
        signals.sort(key=lambda s: (common.SEVERITY_RANK.get(s.get("severity"),
                                                              9),
                                     s.get("check", "")))
        rep = signals[0]
        best_sev = signals[0].get("severity", "low")

        checks = sorted({s.get("check", "") for s in signals if s.get("check")})
        subchecks = [s.get("subcheck", "") for s in signals]
        topic = next((s.get("topic") for s in signals if s.get("topic")), "")
        if not topic:
            ci = common.classify_columns().get(field)
            topic = ci.topic if ci else "meta"

        reason = " || ".join(
            f"[{s.get('subcheck') or s.get('check')}] {s.get('reason', '')}"
            for s in signals)

        ctx = {
            "topic": topic,
            "checks": checks,
            "subchecks": subchecks,
            "n_signals": len(signals),
            "signals": [
                {"check": s.get("check"), "subcheck": s.get("subcheck"),
                 "severity": s.get("severity"), "reason": s.get("reason"),
                 "context": _parse_ctx(s.get("stat_context", ""))}
                for s in signals
            ],
        }

        merged.append({
            "record_id": rid,
            "cao_number": rep.get("cao_number", ""),
            "file_name": rep.get("file_name", ""),
            "ingangsdatum": rep.get("ingangsdatum", ""),
            "field": field,
            "value": rep.get("value", ""),
            "unit": rep.get("unit", ""),
            "check": "|".join(checks),
            "severity": best_sev,
            "reason": reason,
            "stat_context": json.dumps(ctx, ensure_ascii=False),
            "verify_verdict": "",
            "verify_quote": "",
            "verify_suggested_value": "",
            "suggested_review": "",
        })
    return merged


def prioritize(rows: list[dict]) -> list[dict]:
    """Sort by severity, then by per-record flag count (desc), then ids."""
    per_record = Counter(r["record_id"] for r in rows)
    return sorted(rows, key=lambda r: (
        common.SEVERITY_RANK.get(r["severity"], 9),
        -per_record[r["record_id"]],
        r["record_id"],
        r["field"],
    ))


def combine() -> list[dict]:
    return prioritize(merge(load_all()))


def main():
    rows = combine()
    from qa.shared import resilient_csv
    resilient_csv.write_csv(rows, OUT, fieldnames=FINAL_COLUMNS)

    by_sev = Counter(r["severity"] for r in rows)
    n_records = len({r["record_id"] for r in rows})
    in_scope_verify = sum(1 for r in rows if r["severity"] in VERIFY_SEVERITIES)
    verify_pairs = len({
        (json.loads(r["stat_context"]).get("topic", ""), r["record_id"])
        for r in rows if r["severity"] in VERIFY_SEVERITIES
    })
    print(f"[combine] {len(rows)} merged flags "
          f"({n_records} distinct records) -> {OUT}")
    for sev in ("high", "medium", "low"):
        print(f"  {sev:7s} {by_sev.get(sev, 0)}")
    print(f"  verify-scope (high+medium): {in_scope_verify} flags across "
          f"{verify_pairs} (topic,record) pairs")


if __name__ == "__main__":
    main()
