"""Build a markdown comparison of the v0 (production) vs v1 (re-extracted) salary
analyses for the 5 test files. Shows the actual rows that changed, sized to fit
on one screen per defect.

Outputs:
  qa/qa_salary/re_extraction/V1_VS_V0_COMPARISON.md   — human-readable side-by-side
  qa/qa_salary/re_extraction/v1_diffs/<chunk>.md      — per-chunk deep dive
"""
from __future__ import annotations
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
OLD = ROOT / "re_extraction_backup" / "analysis_originals"
NEW = ROOT / "re_extraction" / "v1_outputs"
CHUNKS_OLD = ROOT / "phase3b" / "chunks"
VERIFY_V0 = ROOT / "phase3b" / "results"
VERIFY_V1 = ROOT / "re_extraction" / "v1_verify_results"
OUT = ROOT / "re_extraction" / "v1_diffs"
OUT.mkdir(parents=True, exist_ok=True)
SUMMARY = ROOT / "re_extraction" / "V1_VS_V0_COMPARISON.md"

TESTS = [
    ("b0007", "1287", "Grafimedia cao 2022-2024 1 april 2022",
     "Column misalignment in jobgroups G-K (used 'Entry from' column as 'Minimum')"),
    ("b0011", "1536", "CAO NU 20222-2023 definitieve tekst schoon",
     "Axis swap (jobgroup held Trede, step held Schaal)"),
    ("b0014", "163", "CAO_OV_tekst_2014_en_2015_DEFINITIEF",
     "Step-06 ambiguity (one row for two distinct step-06 cases)"),
    ("b0022", "234", "Jeugdzorg_Cao_2011_2013_def",
     "Trailing-column off-by-one on Period +1/+2"),
    ("b0048", "823", "2025-07-01 CAO MVT 2025 - 2027 - incl HFI DEF",
     "Right-shortfall duplication (rightmost value copied to 2-3 jobgroups)"),
]


def load_si(p: Path) -> list:
    return json.load(open(p, encoding="utf-8")).get("salary_information", [])


_STEP_SYNONYMS = {
    "min": "min", "minimum": "min", "schaalmin": "min",
    "max": "max", "maximum": "max", "schaalmax": "max", "eindloon": "max",
    "entry": "entry", "ingang": "entry", "aanvang": "entry", "aanloop": "entry",
}

def _norm_step(s: str) -> str:
    s = str(s or "").strip().lower()
    if not s:
        return ""
    return _STEP_SYNONYMS.get(s, s)

def row_key(r: dict) -> tuple:
    return (
        str(r.get("jobgroup", "")).strip().lower(),
        _norm_step(r.get("step", "")),
        str(r.get("worker", "") or "").strip().lower(),
        str(r.get("age_group", "") or "").strip().lower(),
        str(r.get("education", "") or "").strip().lower(),
    )


def short_timeline(tl: list) -> str:
    """Compact one-line view of a timeline."""
    if not tl:
        return "[]"
    items = []
    for p in tl[:5]:
        sd = p.get("start_date", "?")
        a = p.get("amount", "?")
        u = p.get("unit", "?")
        items.append(f"{sd}={a}{u[0] if u else ''}")
    extra = f" +{len(tl)-5}" if len(tl) > 5 else ""
    return "[" + " ".join(items) + extra + "]"


def per_file(cid: str, cao: str, fname: str, defect: str) -> None:
    old_p = OLD / cao / f"{fname}_analysis.json"
    new_p = NEW / f"{cid}_analysis.json"
    if not old_p.exists() or not new_p.exists():
        return
    old = load_si(old_p)
    new = load_si(new_p)

    old_idx = {row_key(r): r for r in old}
    new_idx = {row_key(r): r for r in new}
    only_old = set(old_idx) - set(new_idx)
    only_new = set(new_idx) - set(old_idx)
    in_both = set(old_idx) & set(new_idx)

    # Rows where the timeline VALUES differ between old and new (same key)
    changed = []
    for k in in_both:
        o, n = old_idx[k], new_idx[k]
        ot = {p.get("start_date"): p.get("amount") for p in o.get("timeline", [])}
        nt = {p.get("start_date"): p.get("amount") for p in n.get("timeline", [])}
        if ot != nt:
            changed.append((k, ot, nt))

    # verdicts for the prose summary
    v0 = json.load(open(VERIFY_V0 / f"{cid}.json", encoding="utf-8")) if (VERIFY_V0 / f"{cid}.json").exists() else {}
    v1 = json.load(open(VERIFY_V1 / f"{cid}_v1.json", encoding="utf-8")) if (VERIFY_V1 / f"{cid}_v1.json").exists() else {}

    lines = [
        f"# {cid}  CAO {cao}  —  {fname}",
        "",
        f"**Defect:** {defect}",
        "",
        f"| | Original (v0) | v1 (patched prompt) |",
        f"|---|---|---|",
        f"| Assessment | {v0.get('overall_assessment','?')} | {v1.get('overall_assessment','?')} |",
        f"| Discrepancies | {len(v0.get('discrepancies',[]))} | {len(v1.get('discrepancies',[]))} |",
        f"| SalaryRows | {len(old)} | {len(new)} |",
        f"| Rows only in v0 | {len(only_old)} | — |",
        f"| Rows only in v1 | — | {len(only_new)} |",
        f"| Rows w/ same key but different amounts | {len(changed)} | — |",
        "",
    ]

    # Sample: a few rows only in v0 (i.e. what v0 had that v1 dropped — likely fabrications)
    if only_old:
        lines += [
            "## Rows ONLY in v0 (likely fabrications / wrong axis / wrong column)",
            "",
            "| jobgroup | step | worker | age | education | timeline (first 3) |",
            "|---|---|---|---|---|---|",
        ]
        for k in list(only_old)[:10]:
            r = old_idx[k]
            lines.append(f"| `{k[0]}` | `{k[1]}` | `{k[2]}` | `{k[3]}` | `{k[4]}` | {short_timeline(r.get('timeline', []))} |")
        if len(only_old) > 10: lines.append(f"| … +{len(only_old)-10} more | | | | | |")
        lines.append("")

    if only_new:
        lines += [
            "## Rows ONLY in v1 (rows v0 was missing or had under a different key)",
            "",
            "| jobgroup | step | worker | age | education | timeline (first 3) |",
            "|---|---|---|---|---|---|",
        ]
        for k in list(only_new)[:10]:
            r = new_idx[k]
            lines.append(f"| `{k[0]}` | `{k[1]}` | `{k[2]}` | `{k[3]}` | `{k[4]}` | {short_timeline(r.get('timeline', []))} |")
        if len(only_new) > 10: lines.append(f"| … +{len(only_new)-10} more | | | | | |")
        lines.append("")

    if changed:
        lines += [
            "## Same (jobgroup, step, worker, age, edu) — DIFFERENT amounts",
            "",
            "| key | v0 timeline | v1 timeline |",
            "|---|---|---|",
        ]
        for k, ot, nt in changed[:15]:
            lines.append(f"| jg=`{k[0]}` step=`{k[1]}` age=`{k[3]}` | {ot} | {nt} |")
        if len(changed) > 15: lines.append(f"| … +{len(changed)-15} more | | |")
        lines.append("")

    # Discrepancies (from verification)
    if v0.get("discrepancies"):
        lines += ["## v0 discrepancies the auditor flagged (input → these are the defects)", ""]
        for d in v0["discrepancies"][:6]:
            lines.append(f"- **{d.get('severity','?')}** `{d.get('type','?')}`: {d.get('reasoning','')[:300]}")
        lines.append("")
    if v1.get("discrepancies"):
        lines += ["## v1 discrepancies (what's still wrong)", ""]
        for d in v1["discrepancies"][:6]:
            lines.append(f"- **{d.get('severity','?')}** `{d.get('type','?')}`: {d.get('reasoning','')[:300]}")
        lines.append("")
    else:
        lines += ["## v1 discrepancies", "", "_None — auditor reported `faithful`._", ""]

    lines += [
        f"## Source files",
        f"- v0 analysis: `qa/qa_salary/re_extraction_backup/analysis_originals/{cao}/{fname}_analysis.json`",
        f"- v1 analysis: `qa/qa_salary/re_extraction/v1_outputs/{cid}_analysis.json`",
        f"- wage_information text: `qa/qa_salary/phase3b/chunks/{cid}.json` (field `wage_information_text`)",
        f"- v0 verdict: `qa/qa_salary/phase3b/results/{cid}.json`",
        f"- v1 verdict: `qa/qa_salary/re_extraction/v1_verify_results/{cid}_v1.json`",
    ]
    out_path = OUT / f"{cid}.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  wrote {out_path.name}  (only_v0={len(only_old)}, only_v1={len(only_new)}, changed={len(changed)})")


def summary() -> None:
    rows = []
    for cid, cao, fname, defect in TESTS:
        v0 = json.load(open(VERIFY_V0 / f"{cid}.json", encoding="utf-8")) if (VERIFY_V0 / f"{cid}.json").exists() else {}
        v1 = json.load(open(VERIFY_V1 / f"{cid}_v1.json", encoding="utf-8")) if (VERIFY_V1 / f"{cid}_v1.json").exists() else {}
        old = load_si(OLD / cao / f"{fname}_analysis.json")
        new = load_si(NEW / f"{cid}_analysis.json")
        rows.append({
            "cid": cid, "cao": cao, "fname": fname, "defect": defect,
            "v0_assess": v0.get("overall_assessment","?"),
            "v1_assess": v1.get("overall_assessment","?"),
            "v0_n_disc": len(v0.get("discrepancies", [])),
            "v1_n_disc": len(v1.get("discrepancies", [])),
            "old_rows": len(old), "new_rows": len(new),
        })

    out = [
        "# v0 vs v1 salary extraction — side-by-side comparison",
        "",
        "**v0** = the production LLM-analysis JSONs (in `CAOsDataExtraction/outputs/llm_analysis/salary/`, backed up to `qa/qa_salary/re_extraction_backup/analysis_originals/`).  ",
        "**v1** = re-extracted by subagents using the patched prompt at `qa/qa_salary/re_extraction/prompts/extract_v1.md` (4 edits applied: ragged-row handling, axis labelling, adult inloop distinction, no empty timelines).",
        "",
        "## Top-line",
        "",
        "| Chunk | CAO | Defect | v0 → v1 assessment | v0 → v1 discrepancies | v0 → v1 row count |",
        "|---|---|---|---|---:|---:|",
    ]
    for r in rows:
        out.append(f"| [{r['cid']}](v1_diffs/{r['cid']}.md) | {r['cao']} | {r['defect'][:50]} | `{r['v0_assess']}` → `{r['v1_assess']}` | {r['v0_n_disc']} → **{r['v1_n_disc']}** | {r['old_rows']} → {r['new_rows']} |")
    out.append("")
    out += [
        "## Per-file deep dives",
        "",
        "Click each chunk in the table above for: every row only in v0 (likely fabrications/duplicates v1 dropped), every row only in v1 (rows v0 missed), every same-key row whose amounts changed, and the verdict from each verification pass.",
        "",
        "## Raw JSON for any pair",
        "",
        "If you want to diff the full JSON yourself:",
        "```bash",
        "diff <(jq . qa/qa_salary/re_extraction_backup/analysis_originals/1287/'Grafimedia cao 2022-2024 1 april 2022_analysis.json') \\",
        "     <(jq . qa/qa_salary/re_extraction/v1_outputs/b0007_analysis.json) | less",
        "```",
        "",
        "## Patched prompt",
        "",
        "The 4 edits are at `qa/qa_salary/re_extraction/prompts/extract_v1.md` — each marked **[EDIT-N]**.",
        "",
        "If you want to apply them to your production Gemini pipeline, the files to edit are:",
        "- `CAOsDataExtraction/schema/salary_schema.py` (SALARY_PROMPT)",
        "- `CAOsDataExtraction/schema/salary_prompt_split.py` (ATTEMPT_9 + ATTEMPT_10)",
        "- `CAOsDataExtraction/schema/salary_schema_compact.py` (SALARY_PROMPT_COMPACT)",
        "- `CAOsDataExtraction/schema/salary_schema_super_compact.py` (SALARY_PROMPT_SUPER_COMPACT)",
        "",
        "Backups of the originals are at `qa/qa_salary/re_extraction_backup/schema/`.",
    ]
    SUMMARY.write_text("\n".join(out), encoding="utf-8")
    print(f"\nwrote summary: {SUMMARY}")


def main():
    print("Building per-file diffs ...")
    for cid, cao, fname, defect in TESTS:
        per_file(cid, cao, fname, defect)
    print("\nBuilding top-level summary ...")
    summary()


if __name__ == "__main__":
    main()
