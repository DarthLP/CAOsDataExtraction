"""csv_recovery.py — repair subagent CSV rows with unquoted semicolons.

Subagents are instructed to RFC-4180-quote any field containing `;`, but
they frequently emit unquoted semicolons inside the two free-text columns
(`evidence_quote`, idx 6; `notes`, idx 11). That over-splits the row into
>12 fields. This module rejoins the split using the two boolean columns
(`topic_section_was_truncated`, `value_not_in_source`) as positional anchors.

Canonical 12-column layout:
  0 record_id
  1 original_field
  2 verdict
  3 target_field
  4 new_value
  5 new_unit
  6 evidence_quote      <- free text, may contain ';'
  7 confidence
  8 failure_modes_referenced
  9 topic_section_was_truncated   <- bool anchor
 10 value_not_in_source           <- bool anchor
 11 notes               <- free text, may contain ';'

Recovery for a row with N>12 fields:
  - cols 0..5 are taken verbatim from the left (record_id..new_unit; these
    are short, structured values that don't contain ';').
  - Scan from idx 6 for the FIRST adjacent (bool, bool) pair. That pair is
    (topic_section_was_truncated, value_not_in_source). Call its start `b`.
  - failure_modes_referenced = c[b-1]; confidence = c[b-2].
  - evidence_quote = ';'.join(c[6 : b-2])  (rejoins evidence over-splits)
  - notes = ';'.join(c[b+2 : ])            (rejoins notes over-splits)

Rows that can't be anchored (no boolean pair, or < 12 fields) are returned
unchanged and reported, so the caller can route them to manual review.
"""

from __future__ import annotations

import csv
from pathlib import Path

_BOOLS = {"true", "false"}

CANONICAL_HEADER = [
    "record_id", "original_field", "verdict", "target_field",
    "new_value", "new_unit", "evidence_quote", "confidence",
    "failure_modes_referenced", "topic_section_was_truncated",
    "value_not_in_source", "notes",
]


def _is_bool(s: str) -> bool:
    return s.strip().lower() in _BOOLS


def recover_row(cells: list[str]) -> tuple[list[str] | None, str]:
    """Return (recovered_12_col_row, status).
    status ∈ {'ok', 'recovered', 'unrecoverable_*'}.

    Anchors on the first adjacent (bool, bool) pair = (truncated, vnis).
    Left of it: confidence (b-2), fm (b-1). Right: notes = join(c[b+2:]).
    Fixed left: record_id, original_field, verdict, target_field, new_value
    (idx 0..4). The middle slice c[5:b-2] holds new_unit + evidence_quote:
      - 2 fields  -> unit, evidence              (clean 12-col)
      - >2 fields -> unit, evidence-over-split    (evidence had ';')
      - 1 field   -> unit missing, that field is evidence (free text)
      - 0 fields  -> both empty
    This recovers 11-col (missing unit), 12-col (clean), and 13+ (over-split).
    """
    n = len(cells)
    if n == 12:
        return cells, "ok"
    if n < 11:
        return None, "unrecoverable_short"
    # Scan for first adjacent boolean pair; need b-2 >= 5 so b >= 7.
    b = None
    for i in range(6, n - 1):
        if _is_bool(cells[i]) and _is_bool(cells[i + 1]):
            b = i
            break
    if b is None or b < 7:
        return None, "unrecoverable_no_bool_anchor"
    rid, field, verdict, target, value = cells[:5]
    confidence = cells[b - 2]
    fm = cells[b - 1]
    trunc = cells[b]
    vnis = cells[b + 1]
    notes = ";".join(cells[b + 2 :])
    middle = cells[5 : b - 2]
    if len(middle) == 0:
        unit, evidence = "", ""
    elif len(middle) == 1:
        # Heuristic: a lone middle field that reads like prose is the
        # evidence_quote (unit was dropped). A short token is the unit.
        m = middle[0]
        if len(m) <= 25 and (" " not in m.strip() or m.strip().lower().startswith(
                ("eur", "hours", "days", "%", "percent", "months", "years",
                 "contracts", "employees", "fte"))):
            unit, evidence = m, ""
        else:
            unit, evidence = "", m
    else:
        unit = middle[0]
        evidence = ";".join(middle[1:])
    return ([rid, field, verdict, target, value, unit, evidence,
             confidence, fm, trunc, vnis, notes], "recovered")


def recover_file(path: Path, write: bool = True) -> dict:
    """Recover one chunk CSV in place. Returns stats dict."""
    with path.open(encoding="utf-8") as fh:
        reader = csv.reader(fh, delimiter=";")
        header = next(reader)
        rows = list(reader)

    out_rows = []
    stats = {"ok": 0, "recovered": 0, "unrecoverable": 0, "dropped_junk": 0,
             "unrecoverable_rows": []}
    for r in rows:
        # Drop junk lines: empty, or a record_id that isn't a numeric id
        # (e.g. leaked tool-call syntax like '</content>', '</invoke>').
        rid0 = (r[0].strip() if r else "")
        if not rid0 or not rid0.replace(".", "").isdigit():
            stats["dropped_junk"] += 1
            continue
        fixed, status = recover_row(r)
        if status == "ok":
            out_rows.append(fixed)
            stats["ok"] += 1
        elif status == "recovered":
            out_rows.append(fixed)
            stats["recovered"] += 1
        else:
            out_rows.append(r)  # keep original; caller flags
            stats["unrecoverable"] += 1
            stats["unrecoverable_rows"].append((r[0] if r else "", status))

    if write:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter=";", quoting=csv.QUOTE_MINIMAL)
            writer.writerow(CANONICAL_HEADER)
            writer.writerows(out_rows)
    return stats


def recover_glob(chunks_dir: Path) -> dict:
    """Recover all chunk_*_corrections.csv in a directory."""
    total = {"ok": 0, "recovered": 0, "unrecoverable": 0}
    per_file = {}
    for f in sorted(chunks_dir.glob("chunk_*_corrections.csv")):
        s = recover_file(f, write=True)
        per_file[f.name] = s
        for k in ("ok", "recovered", "unrecoverable"):
            total[k] += s[k]
    return {"total": total, "per_file": per_file}


if __name__ == "__main__":
    import sys
    d = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    res = recover_glob(d)
    print(f"Recovery summary: {res['total']}")
    for name, s in res["per_file"].items():
        if s["recovered"] or s["unrecoverable"]:
            print(f"  {name}: recovered={s['recovered']}, "
                  f"unrecoverable={s['unrecoverable']}")
            for rid, why in s["unrecoverable_rows"]:
                print(f"      ! {rid}: {why}")
