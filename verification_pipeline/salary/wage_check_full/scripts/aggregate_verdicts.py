"""aggregate_verdicts.py — collect verdict JSONs into a CSV summary.

Reads:    <phase_dir>/verdicts/*.json
Outputs:  <phase_dir>/VERDICTS.csv          — chunk_id, cao, file_name, assessment, confidence, n_discs, n_high, summary
          <phase_dir>/FLAGGED.csv           — only major_issues + minor_issues
          <phase_dir>/SUMMARY.md            — distribution table + top defect types

Usage:    python3 aggregate_verdicts.py <phase_a_sanity | phase_a_full>
"""
from __future__ import annotations
import csv
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHUNKS = ROOT / "chunks"


def load_manifest() -> dict[str, dict]:
    out = {}
    p = CHUNKS / "manifest.csv"
    if not p.exists():
        return out
    for r in csv.DictReader(open(p, encoding="utf-8"), delimiter=";"):
        out[r["chunk_id"]] = r
    return out


def main(phase_dir_name: str) -> None:
    phase_dir = ROOT / phase_dir_name
    verdicts_dir = phase_dir / "verdicts"
    if not verdicts_dir.exists():
        print(f"no verdicts dir at {verdicts_dir}", file=sys.stderr)
        sys.exit(1)

    manifest = load_manifest()
    rows = []
    by_assessment: Counter = Counter()
    by_type: Counter = Counter()
    by_severity: Counter = Counter()

    for p in sorted(verdicts_dir.glob("c*.json")):
        try:
            d = json.load(open(p, encoding="utf-8"))
        except Exception as e:
            print(f"skip {p}: {e}", file=sys.stderr)
            continue
        cid = d.get("chunk_id", p.stem)
        m = manifest.get(cid, {})
        a = d.get("overall_assessment", "?")
        by_assessment[a] += 1
        discs = d.get("discrepancies", []) or []
        n_high = 0
        for x in discs:
            by_type[x.get("type", "?")] += 1
            sv = x.get("severity", "?")
            by_severity[sv] += 1
            if sv == "high":
                n_high += 1
        rows.append({
            "chunk_id": cid,
            "cao_number": m.get("cao_number", ""),
            "file_name": m.get("file_name", ""),
            "schema_kind": m.get("schema_kind", ""),
            "n_entries": m.get("n_entries", ""),
            "wage_chars": m.get("wage_chars", ""),
            "size_kb": m.get("size_kb", ""),
            "assessment": a,
            "confidence": d.get("confidence", "?"),
            "n_discs": len(discs),
            "n_high": n_high,
            "summary": (d.get("summary", "") or "").replace("\n", " ").replace(";", ","),
        })

    rows.sort(key=lambda r: (r["assessment"], r["chunk_id"]))

    with open(phase_dir / "VERDICTS.csv", "w", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=";")
        w.writeheader(); w.writerows(rows)

    flagged = [r for r in rows if r["assessment"] in ("major_issues", "minor_issues")]
    with open(phase_dir / "FLAGGED.csv", "w", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=";")
        w.writeheader(); w.writerows(flagged)

    total = sum(by_assessment.values())
    pct = lambda n: f"{100*n/total:.1f}%" if total else "—"
    summary = []
    summary.append(f"# Phase verdict summary — {phase_dir_name}\n")
    summary.append(f"**Total verdicts:** {total}\n\n")
    summary.append("## Distribution\n\n| Assessment | Count | % |\n|---|---|---|\n")
    for k in ["faithful", "minor_issues", "major_issues", "not_verifiable"]:
        n = by_assessment.get(k, 0)
        summary.append(f"| {k} | {n} | {pct(n)} |\n")
    for k, n in by_assessment.items():
        if k not in {"faithful", "minor_issues", "major_issues", "not_verifiable"}:
            summary.append(f"| {k} | {n} | {pct(n)} |\n")

    summary.append("\n## Top discrepancy types\n\n| Type | Count |\n|---|---|\n")
    for k, n in by_type.most_common():
        summary.append(f"| {k} | {n} |\n")
    summary.append("\n## Severity\n\n| Severity | Count |\n|---|---|\n")
    for k, n in by_severity.most_common():
        summary.append(f"| {k} | {n} |\n")

    summary.append("\n## Flagged files (major + minor)\n\n")
    for r in flagged:
        summary.append(f"- **{r['chunk_id']}** ({r['cao_number']}, {r['assessment']}, {r['n_discs']} disc / {r['n_high']} high) — {r['summary'][:160]}\n")

    (phase_dir / "SUMMARY.md").write_text("".join(summary), encoding="utf-8")

    print(f"wrote {phase_dir}/VERDICTS.csv ({len(rows)} rows)")
    print(f"wrote {phase_dir}/FLAGGED.csv ({len(flagged)} rows)")
    print(f"wrote {phase_dir}/SUMMARY.md")
    print(f"\ndistribution: {dict(by_assessment)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python3 aggregate_verdicts.py <phase_a_sanity | phase_a_full>")
        sys.exit(1)
    main(sys.argv[1])
