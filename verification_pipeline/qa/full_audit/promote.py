"""Promote proposed_corrected_dataset.csv to become qa/corrected_dataset.csv.
Guarded: verifies shape/ids match, BACKS UP the current corrected_dataset first,
writes a diff summary, promotes, then verifies the promoted file == proposed.

Run: python3.13 -m qa.full_audit.promote
"""
from __future__ import annotations

import shutil
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / "corrected_dataset.csv"
PROPOSED = HERE / "proposed_corrected_dataset.csv"
BACKUP = HERE.parent / "corrected_dataset.bak.2026-06-04.csv"
SUMMARY = HERE / "promotion_diff_summary.md"


def _blank(s):
    return str(s).strip().lower() in ("", "nan", "none", "null")


def main():
    base = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    prop = pd.read_csv(PROPOSED, sep=";", dtype=str, keep_default_na=False)

    # ---- guards ----
    assert list(base.columns) == list(prop.columns), "column mismatch — ABORT"
    assert len(base) == len(prop), f"row count mismatch {len(base)} vs {len(prop)} — ABORT"
    assert set(base["id"]) == set(prop["id"]), "id set mismatch — ABORT"
    info = common.classify_columns()
    bi = {r["id"]: r for r in base.to_dict("records")}
    pi = {r["id"]: r for r in prop.to_dict("records")}

    filled = changed = emptied = 0
    by_topic = Counter()
    by_field = Counter()
    for rid in base["id"]:
        for c in base.columns:
            if c == "id":
                continue
            o, n = str(bi[rid][c]).strip(), str(pi[rid][c]).strip()
            if o == n:
                continue
            ci = info.get(c)
            by_topic[ci.topic if ci else "meta"] += 1
            by_field[c] += 1
            if _blank(o) and not _blank(n):
                filled += 1
            elif not _blank(o) and _blank(n):
                emptied += 1
            else:
                changed += 1
    total = filled + changed + emptied

    lines = [f"# Promotion diff — proposed → corrected_dataset.csv ({total} cells)\n",
             f"Promoted 2026-06-04. Backup of the previous corrected_dataset at `{BACKUP.name}`.\n",
             f"- **filled** (was empty → value): {filled}",
             f"- **changed** (value → different value): {changed}",
             f"- **emptied** (value → empty, per no-statutory/no-invent policy): {emptied}\n",
             "## By topic\n", "| topic | cells changed |", "|---|---:|"]
    for t, n in by_topic.most_common():
        lines.append(f"| {t} | {n} |")
    lines += ["\n## Top 20 fields\n", "| field | cells |", "|---|---:|"]
    for f, n in by_field.most_common(20):
        lines.append(f"| {f} | {n} |")
    SUMMARY.write_text("\n".join(lines), encoding="utf-8")

    # ---- backup, promote, verify ----
    shutil.copy2(BASE, BACKUP)
    shutil.copy2(PROPOSED, BASE)
    promoted = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False)
    ok = promoted.equals(prop)

    print(f"[promote] {total} cells changed (filled={filled} changed={changed} emptied={emptied})")
    print(f"  backup  -> {BACKUP.name}")
    print(f"  promoted proposed -> {BASE.name}")
    print(f"  VERIFY promoted == proposed: {ok}")
    print(f"  by topic: {dict(by_topic.most_common())}")
    print(f"  diff summary -> {SUMMARY.name}")


if __name__ == "__main__":
    main()
