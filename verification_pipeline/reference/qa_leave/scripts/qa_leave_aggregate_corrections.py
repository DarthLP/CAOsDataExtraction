"""
qa_leave_aggregate_corrections.py — combine deterministic + subagent corrections.

Pipeline:
  1. Read corrections_deterministic.csv  (auto-fixes from L1 errors and certain patterns)
  2. Read every subagent_worksheets/chunks/chunk_NNN_corrections.csv with a
     RESILIENT csv reader that handles known subagent CSV malformations
     (unquoted ; in evidence_quote/notes, botched escaping that merges
     unit_new+evidence_quote or evidence_quote+confidence into a single field,
     and rows broken across two physical lines by a stray newline).
  3. POST-PROCESSING: demote any high/medium row with empty/placeholder evidence_quote
     to low + csv_value_new=UNKNOWN. (Enforces strict rubric semantics regardless of
     subagent slip-ups.)
  4. Flag every row with `is_noop` ('yes' / 'no') and `changed`
     ('none' / 'value' / 'unit' / 'both'). `changed=none` is the same set as
     `is_noop=yes`; the other three values say WHAT shifted relative to the
     original CSV row, which is more useful in QA reports than a flat boolean.
  5. Combine into one long-format corrections.csv with consistent schema.

Output:
  qa_leave/outputs/corrections.csv
  qa_leave/outputs/corrections_audit.csv          (rows that were demoted, before/after)
  qa_leave/outputs/corrections_skipped_rows.csv   (chunk rows the resilient reader
                                                   could not repair; empty if all OK)

History note: an earlier version of this script used pandas' default C parser to read
chunk_*_corrections.csv. That silently mis-parsed rows whose evidence_quote contained
an unquoted `;`: pandas treated the leading columns as a MultiIndex and shifted every
field. The resilient reader below avoids that by parsing with csv.reader and applying
small repair heuristics.
"""
from __future__ import annotations
import csv
import io
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DET_PATH = ROOT / "outputs" / "corrections_deterministic.csv"
CHUNKS_DIR = ROOT / "outputs" / "subagent_worksheets" / "chunks"
P3_REVIEW_DIR = ROOT / "outputs" / "p3_review" / "chunks"
L1_FOLLOWUP_DIR = ROOT / "outputs" / "l1_followup"
C2_FOLLOWUP_DIR = ROOT / "outputs" / "c2_followup"
MANUAL_REVIEW_FIXES    = ROOT / "outputs" / "manual_review_followup" / "fixes.csv"
MANUAL_REVIEW_FIXES_V2 = ROOT / "outputs" / "manual_review_followup" / "fixes_v2.csv"
MANUAL_REVIEW_FIXES_V3 = ROOT / "outputs" / "manual_review_followup" / "fixes_v3.csv"
MANUAL_REVIEW_FIXES_V4 = ROOT / "outputs" / "manual_review_followup" / "fixes_v4.csv"
MANUAL_REVIEW_FIXES_V5 = ROOT / "outputs" / "manual_review_followup" / "fixes_v5.csv"
INDEX_PATH = ROOT / "outputs" / "leave_qa_payload_index.csv"
OUT_PATH = ROOT / "outputs" / "corrections.csv"
AUDIT_PATH = ROOT / "outputs" / "corrections_audit.csv"
SKIP_LOG_PATH = ROOT / "outputs" / "corrections_skipped_rows.csv"

# Schema of every chunk_*_corrections.csv produced by subagents.
CHUNK_HEADER = [
    "item_id", "record_id", "topic_group", "field",
    "csv_value_old", "csv_value_new", "unit_new",
    "evidence_quote", "confidence", "notes",
]
N_COLS = len(CHUNK_HEADER)
CONF_VALUES = {"high", "medium", "low"}


def is_placeholder_or_empty(val) -> bool:
    """True when evidence_quote is missing or a placeholder (not a verbatim quote)."""
    if val is None:
        return True
    if pd.isna(val):
        return True
    s = str(val).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return True
    # Placeholders like "(no relevant text in excerpt)", "(statutory NL ...)", etc.
    if s.startswith("(") and s.endswith(")"):
        return True
    return False


# Markers that all mean "no data" — equivalent for purposes of is_noop.
EMPTY_MARKERS = {"", "(empty)", "unknown", "nan", "none", "null"}


def _values_equiv(old, new) -> bool:
    """Loose equality on csv_value_old vs csv_value_new."""
    o = "" if old is None else str(old).strip()
    n = "" if new is None else str(new).strip()
    if o.lower() == n.lower():
        return True
    if o.lower() in EMPTY_MARKERS and n.lower() in EMPTY_MARKERS:
        return True
    try:
        if float(o) == float(n):
            return True
    except (ValueError, TypeError):
        pass
    return False


def _units_equiv(old, new) -> bool:
    """Loose equality on csv_unit_old vs unit_new.

      - both empty/UNKNOWN  -> equivalent (no info either way)
      - one empty/UNKNOWN, the other populated -> NOT equivalent
        (gaining or losing a unit IS a real correction)
      - both populated      -> exact case-folded match
    """
    o = "" if old is None else str(old).strip().lower()
    n = "" if new is None else str(new).strip().lower()
    o_empty = o in EMPTY_MARKERS
    n_empty = n in EMPTY_MARKERS
    if o_empty and n_empty:
        return True
    if o_empty != n_empty:
        return False
    return o == n


def is_noop_change(old_value, new_value, old_unit="", new_unit="") -> bool:
    """True if (csv_value_old, csv_unit_old) and (csv_value_new, unit_new) are
    equivalent (no real change)."""
    return _values_equiv(old_value, new_value) and _units_equiv(old_unit, new_unit)


def classify_change(old_value, new_value, old_unit="", new_unit="") -> str:
    """One of {'none', 'value', 'unit', 'both'} describing what shifted."""
    v_eq = _values_equiv(old_value, new_value)
    u_eq = _units_equiv(old_unit, new_unit)
    if v_eq and u_eq:
        return "none"
    if not v_eq and u_eq:
        return "value"
    if v_eq and not u_eq:
        return "unit"
    return "both"


# ----------------------------------------------------------- resilient CSV --
def _split_at_confidence(row: list[str]) -> list[str] | None:
    """For rows with > N_COLS fields: collapse extras using confidence position.

    Confidence (high/medium/low) is a single token at column index 8. When a row
    has extra `;` in evidence_quote or notes, we pivot on the LAST confidence
    token at or after index 7: everything between index 7 and the pivot becomes
    evidence_quote; everything after the pivot becomes notes.
    """
    cand = [i for i, v in enumerate(row)
            if i >= 7 and v.strip().strip('"').lower() in CONF_VALUES]
    if not cand:
        return None
    ci = cand[-1]
    fixed = (
        row[:7]
        + [";".join(row[7:ci])]
        + [row[ci]]
        + [";".join(row[ci + 1:])]
    )
    return fixed if len(fixed) == N_COLS else None


def _fix_unit_evidence_merge(row: list[str]) -> list[str] | None:
    """Repair rows of length N_COLS-1 where unit_new and evidence_quote merged.

    chunk_029-style pattern after csv.reader unquoting:
        field[6] = '%;"100% for the first 52 weeks ...'
    The original raw was meant to be `"%";"100% ..."` but was mistyped as
    `"%;""100% ..."`. We split field[6] at the first `;"`, with a fallback for
    the simpler `<unit>;<text>` shape.
    """
    if len(row) != N_COLS - 1:
        return None
    f6 = row[6]
    m = re.search(r';"', f6)
    if m:
        head, tail = f6[:m.start()], f6[m.end():]
        if 0 < len(head) <= 30:
            return row[:6] + [head, tail] + row[7:]
    if ";" in f6 and len(f6) > 1:
        unit, _, ev = f6.partition(";")
        if 0 < len(unit) <= 30:
            return row[:6] + [unit, ev] + row[7:]
    return None


def _fix_evidence_confidence_merge(row: list[str]) -> list[str] | None:
    """Repair rows of length N_COLS-1 where evidence_quote ate the confidence cell.

    chunk_070-style pattern after csv.reader unquoting: field[7] ends with
    `;high`, `;medium`, or `;low` (optionally followed by a stray `\"`).
    """
    if len(row) != N_COLS - 1:
        return None
    f7 = row[7]
    m = re.search(r';\s*"?(high|medium|low)"?\s*$', f7, re.IGNORECASE)
    if not m:
        return None
    ev = f7[:m.start()].rstrip()
    conf = m.group(1).lower()
    return row[:7] + [ev, conf] + row[8:]


def _try_merge_short_rows(rows: list[list[str]], i: int) -> tuple[list[str] | None, int]:
    """Try to combine row i with its successor(s) to recover an N_COLS-field row.

    chunk_103-style breakage: a stray newline split one logical row across two
    physical lines (e.g. `...;5;\\n"times average weekly hours per year";...`).
    csv.reader leaves the seed row with a trailing empty cell because of the `;`
    that came right before the newline; we drop that artefact before merging so
    the cell counts add up cleanly.
    """
    cur = list(rows[i])
    if cur and cur[-1] == "":
        cur = cur[:-1]
    j = i + 1
    while j < len(rows) and len(cur) < N_COLS:
        cur += rows[j]
        j += 1
    if len(cur) == N_COLS:
        return cur, j
    if len(cur) > N_COLS:
        fixed = _split_at_confidence(cur)
        if fixed is not None:
            return fixed, j
    return None, j


def _build_item_lookup(jsonl_path: Path) -> dict[str, dict[str, str]]:
    """Map item_id -> {record_id, csv_unit_old} for a chunk JSONL.

    Empty dict on any error (missing file, FUSE cache miss, parse error).
    """
    import json
    out: dict[str, dict[str, str]] = {}
    try:
        if not jsonl_path.exists():
            return out
        text = jsonl_path.read_text(encoding="utf-8")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        rid = str(obj.get("record_id", ""))
        for it in obj.get("items", []):
            iid = it.get("item_id")
            if not iid:
                continue
            out[iid] = {
                "record_id": rid,
                "csv_unit_old": str(it.get("csv_unit_old", "") or ""),
            }
    return out


def _is_template_placeholder(d: dict) -> bool:
    """True for rows that are leftover subagent worksheet templates rather
    than real verdicts. The pure-template form has placeholder strings
    everywhere — `record_id` like 'rec_NNN_NNN', `topic_group='leave_type'`,
    `field='field_name'`, `csv_value_old='old_val'` and `notes='placeholder'`.
    """
    rid = d.get("record_id", "").strip()
    if not (rid.startswith("rec_") and rid.replace("rec_","").replace("_","").isdigit()):
        return False
    tg  = d.get("topic_group", "").strip()
    fld = d.get("field", "").strip()
    return tg == "leave_type" and fld == "field_name"


def _read_chunk_resilient(path: Path) -> tuple[list[dict], list[dict]]:
    """Parse one chunk_*_corrections.csv. Returns (good_rows, skipped_rows).

    Also fills empty record_id values by looking up item_id in the matching
    chunk_NNN.jsonl (some subagents wrote empty record_id cells).
    """
    raw = path.read_text(encoding="utf-8")
    all_rows = list(csv.reader(io.StringIO(raw), delimiter=";", quotechar='"'))
    if not all_rows:
        return [], []

    header = [c.strip().strip('"') for c in all_rows[0]]
    if header != CHUNK_HEADER:
        return [], [{
            "file": path.name,
            "row_index": 1,
            "reason": f"unexpected header: {all_rows[0]}",
            "raw": str(all_rows[0]),
        }]

    # Look up sibling JSONL once per chunk: gives us record_id recovery AND
    # csv_unit_old so we can detect unit-only corrections in the noop check.
    jsonl_path = path.with_name(path.name.replace("_corrections.csv", ".jsonl"))
    item_lookup = _build_item_lookup(jsonl_path)

    good: list[dict] = []
    skipped: list[dict] = []

    def finalize(parsed: list[str]) -> dict | None:
        d = dict(zip(CHUNK_HEADER, parsed))
        if _is_template_placeholder(d):
            # Skip the leftover template row — emit nothing for it.
            return None
        iid = d.get("item_id", "").strip()
        meta = item_lookup.get(iid) if iid else None
        if meta:
            if not d.get("record_id", "").strip():
                d["record_id"] = meta["record_id"]
            d["csv_unit_old"] = meta["csv_unit_old"]
        else:
            d["csv_unit_old"] = ""
        return d

    i = 1
    while i < len(all_rows):
        row = list(all_rows[i])
        if not any(c.strip() for c in row):
            i += 1
            continue

        if len(row) == N_COLS:
            d = finalize(row)
            if d is None:
                skipped.append({"file": path.name, "row_index": i + 1,
                                "reason": "template placeholder skipped",
                                "raw": ";".join(row)[:500]})
            else:
                good.append(d)
            i += 1
            continue

        if len(row) > N_COLS:
            attempted = _split_at_confidence(row)
        else:
            attempted = (
                _fix_unit_evidence_merge(row)
                or _fix_evidence_confidence_merge(row)
            )
            if attempted is None:
                merged, new_i = _try_merge_short_rows(all_rows, i)
                if merged is not None:
                    d = finalize(merged)
                    if d is None:
                        skipped.append({"file": path.name, "row_index": i + 1,
                                        "reason": "template placeholder skipped",
                                        "raw": ";".join(row)[:500]})
                    else:
                        good.append(d)
                    i = new_i
                    continue

        if attempted is None or len(attempted) != N_COLS:
            skipped.append({
                "file": path.name,
                "row_index": i + 1,
                "reason": f"unrepairable (len={len(row)})",
                "raw": ";".join(row)[:500],
            })
        else:
            d = finalize(attempted)
            if d is None:
                skipped.append({"file": path.name, "row_index": i + 1,
                                "reason": "template placeholder skipped",
                                "raw": ";".join(row)[:500]})
            else:
                good.append(d)
        i += 1

    return good, skipped


def main() -> None:
    idx = pd.read_csv(INDEX_PATH, sep=";", dtype=str).fillna("")
    by_id = {str(r["record_id"]): r for _, r in idx.iterrows()}

    # ----- Load P3-review verdicts (one per record_id) -----
    p3_verdict_by_record: dict[str, dict] = {}
    if P3_REVIEW_DIR.exists():
        for cp in sorted(P3_REVIEW_DIR.glob("chunk_*_review.csv")):
            try:
                df = pd.read_csv(cp, sep=";", dtype=str).fillna("")
            except Exception:
                continue
            for _, r in df.iterrows():
                rid = str(r.get("record_id", ""))
                p3_verdict_by_record[rid] = {
                    "has_worker_group": r.get("has_worker_group", ""),
                    "evidence_quote": r.get("evidence_quote", ""),
                    "confidence": r.get("confidence", ""),
                    "notes": r.get("notes", ""),
                }

    p3_reverted = 0
    p3_confirmed = 0

    # ----- Load L1 follow-up review verdicts (optional, may be absent) -----
    # Map: (record_id, source_layer) -> {verdict, new_value, new_unit, evidence_quote, confidence, notes}
    l1_review_by_key: dict[tuple[str, str], dict] = {}
    LAYER_TO_GROUP = {
        "L1_PAR_03": "par_03", "L1_CARE_01": "care_01",
        "L1_STAT_02": "stat_02_p6", "pattern_P6": "stat_02_p6",
        "pattern_P10": "p10", "pattern_P1": "p1",
    }
    if L1_FOLLOWUP_DIR.exists():
        for group in set(LAYER_TO_GROUP.values()):
            rpath = L1_FOLLOWUP_DIR / f"{group}_review.csv"
            if not rpath.exists():
                continue
            # csv.reader (not pandas) — handles quoted notes with embedded ;.
            try:
                with rpath.open("r", encoding="utf-8", newline="") as f:
                    rows = list(csv.reader(f, delimiter=";", quotechar='"'))
            except Exception:
                continue
            if not rows:
                continue
            header = [c.strip() for c in rows[0]]
            for raw in rows[1:]:
                if not any(c.strip() for c in raw):
                    continue
                # Be tolerant: pad/truncate to header length.
                row = list(raw) + [""] * max(0, len(header) - len(raw))
                row = row[:len(header)]
                d = dict(zip(header, row))
                rid = str(d.get("record_id", "")).strip()
                fld = str(d.get("field", "")).strip()
                if not rid or not fld:
                    continue
                for layer, g in LAYER_TO_GROUP.items():
                    if g != group:
                        continue
                    l1_review_by_key[(rid, layer, fld)] = {
                        "verdict": str(d.get("verdict", "")).strip().lower(),
                        "new_value": str(d.get("new_value", "")),
                        "new_unit": str(d.get("new_unit", "")),
                        "evidence_quote": str(d.get("evidence_quote", "")),
                        "confidence": str(d.get("confidence", "")),
                        "notes": str(d.get("notes", "")),
                    }

    l1_confirmed = 0
    l1_reverted = 0
    l1_superseded = 0
    l1_orphan_supersedes = 0
    l1_consumed_keys: set[tuple[str, str, str]] = set()

    # ----- Deterministic corrections -----
    det_rows: list[dict] = []
    if DET_PATH.exists():
        det_df = pd.read_csv(DET_PATH, sep=";", dtype=str).fillna("")
        for _, r in det_df.iterrows():
            rid = str(r["record_id"])
            source_layer = r.get("source_layer", "")
            # Special handling for P3 flips: consult subagent review
            if source_layer == "pattern_P3":
                v = p3_verdict_by_record.get(rid)
                if v is not None:
                    if v["has_worker_group"] == "yes":
                        # Subagent says real worker groups exist → DO NOT flip
                        # Record this as a REJECTED correction so the audit trail is clear.
                        p3_reverted += 1
                        det_rows.append({
                            "source": "subagent",
                            "record_id": rid,
                            "cao_number": r.get("cao_number", "") or by_id.get(rid, {}).get("cao_number", ""),
                            "file_name": r.get("file_name", "") or by_id.get(rid, {}).get("file_name", ""),
                            "topic_group": "general",
                            "field": "leave_hetero_present",
                            "csv_value_old": "True",
                            "csv_value_new": "True",  # KEEP the original
                            "unit_new": "",
                            "evidence_quote": v["evidence_quote"],
                            "confidence": v["confidence"],
                            "severity": "info",
                            "fix_method": "no_change_subagent_review_rejected_flip",
                            "source_layer": "pattern_P3+review",
                            "notes": "P3 deterministic flip REVERTED by subagent review (real worker groups present in source). hetero_present stays True.",
                            "demoted": "no",
                        })
                        continue  # don't add the original P3 flip
                    else:
                        # Subagent says no worker groups (or low-confidence default) → CONFIRM flip
                        p3_confirmed += 1
                        det_rows.append({
                            "source": "deterministic",
                            "record_id": rid,
                            "cao_number": r.get("cao_number", "") or by_id.get(rid, {}).get("cao_number", ""),
                            "file_name": r.get("file_name", "") or by_id.get(rid, {}).get("file_name", ""),
                            "topic_group": r.get("topic_group", ""),
                            "field": r.get("field", ""),
                            "csv_value_old": r.get("csv_value_old", ""),
                            "csv_value_new": r.get("csv_value_new", ""),
                            "unit_new": r.get("unit_new", ""),
                            "evidence_quote": r.get("evidence_quote", ""),
                            "confidence": "high" if v["confidence"] == "high" else "medium",
                            "severity": r.get("severity", "medium"),
                            "fix_method": "deterministic" if v["confidence"] == "high" else "deterministic_review",
                            "source_layer": "pattern_P3+review",
                            "notes": (r.get("reason", "") + f" | subagent confirmed: {v['confidence']}").strip(),
                            "demoted": "no",
                        })
                        continue
                # No review verdict found — fall through to default behavior

            # ----- L1 follow-up review for non-P3 deterministic flips -----
            if source_layer in ("L1_PAR_03", "L1_CARE_01", "L1_STAT_02",
                                "pattern_P6", "pattern_P10", "pattern_P1"):
                key = (rid, source_layer, r.get("field", ""))
                v = l1_review_by_key.get(key)
                if v is not None:
                    l1_consumed_keys.add(key)
                    verdict = v["verdict"]
                    if verdict == "revert_flip":
                        # subagent says the original was right; emit an audit entry
                        l1_reverted += 1
                        det_rows.append({
                            "source": "subagent",
                            "record_id": rid,
                            "cao_number": r.get("cao_number", "") or by_id.get(rid, {}).get("cao_number", ""),
                            "file_name": r.get("file_name", "") or by_id.get(rid, {}).get("file_name", ""),
                            "topic_group": r.get("topic_group", ""),
                            "field": r.get("field", ""),
                            "csv_value_old": r.get("csv_value_old", ""),
                            "csv_value_new": r.get("csv_value_old", ""),  # KEEP original
                            "unit_new": "",
                            "evidence_quote": v["evidence_quote"],
                            "confidence": v["confidence"] or "medium",
                            "severity": "info",
                            "fix_method": "no_change_l1_review_rejected",
                            "source_layer": f"{source_layer}+review",
                            "notes": (
                                "L1 deterministic flip REVERTED by subagent review. "
                                + (v["notes"] or "")
                            ).strip(),
                            "demoted": "no",
                        })
                        continue
                    if verdict == "supersede" and v["new_value"]:
                        l1_superseded += 1
                        det_rows.append({
                            "source": "deterministic",
                            "record_id": rid,
                            "cao_number": r.get("cao_number", "") or by_id.get(rid, {}).get("cao_number", ""),
                            "file_name": r.get("file_name", "") or by_id.get(rid, {}).get("file_name", ""),
                            "topic_group": r.get("topic_group", ""),
                            "field": r.get("field", ""),
                            "csv_value_old": r.get("csv_value_old", ""),
                            "csv_value_new": v["new_value"],
                            "unit_new": v["new_unit"],
                            "evidence_quote": v["evidence_quote"],
                            "confidence": v["confidence"] or "medium",
                            "severity": r.get("severity", "high"),
                            "fix_method": "deterministic_superseded_by_review",
                            "source_layer": f"{source_layer}+review",
                            "notes": (r.get("reason", "") + " | subagent supersede: " + (v["notes"] or "")).strip(),
                            "demoted": "no",
                        })
                        continue
                    if verdict == "confirm_flip":
                        l1_confirmed += 1
                        # fall through to default emission below

            # `confidence` describes how well the SOURCE supports the answer.
            # For rule-based corrections (L0 clears + L1 boolean flips + pattern
            # detectors) there is no actual source check — the "evidence" is a
            # rule explanation in parentheses. We mark confidence as empty so
            # users filtering by "high-confidence evidence-based fixes" don't
            # accidentally pick them up.
            is_rule_based = (
                source_layer.startswith("L0_STATUTORY_CLEAR")
                or source_layer in ("L1_PAR_03", "L1_CARE_01", "L1_STAT_02",
                                    "pattern_P1", "pattern_P3", "pattern_P6",
                                    "pattern_P10")
            )
            is_clear = is_rule_based
            det_rows.append({
                "source": "deterministic",
                "record_id": rid,
                "cao_number": r.get("cao_number", "") or by_id.get(rid, {}).get("cao_number", ""),
                "file_name": r.get("file_name", "") or by_id.get(rid, {}).get("file_name", ""),
                "topic_group": r.get("topic_group", ""),
                "field": r.get("field", ""),
                "csv_value_old": r.get("csv_value_old", ""),
                "csv_unit_old": r.get("csv_unit_old", ""),
                "csv_value_new": r.get("csv_value_new", ""),
                "unit_new": r.get("unit_new", ""),
                "evidence_quote": r.get("evidence_quote", ""),
                "confidence": "" if is_clear else "high",
                "severity": r.get("severity", "high"),
                "fix_method": r.get("fix_method", "deterministic"),
                "source_layer": source_layer,
                "notes": r.get("reason", ""),
                "demoted": "no",
            })

    # ----- Emit orphan L1-review supersedes (sub-field overwrites that don't
    # correspond to any deterministic flip — e.g. agent says "exceptions=True
    # is right but min_tenure value should be 13 not 26"). -----
    seen_records_by_layer: dict[str, set[str]] = {}
    for k in l1_review_by_key.keys():
        rid, layer, _ = k
        seen_records_by_layer.setdefault(layer, set()).add(rid)
    for (rid, layer, fld), v in l1_review_by_key.items():
        if (rid, layer, fld) in l1_consumed_keys:
            continue
        if v["verdict"] != "supersede" or not v["new_value"]:
            continue
        meta = by_id.get(rid, {})
        l1_orphan_supersedes += 1
        det_rows.append({
            "source": "subagent",
            "record_id": rid,
            "cao_number": meta.get("cao_number", "") if hasattr(meta, "get") else "",
            "file_name": meta.get("file_name", "") if hasattr(meta, "get") else "",
            "topic_group": "",
            "field": fld,
            "csv_value_old": "",   # the original value isn't echoed by the review row
            "csv_value_new": v["new_value"],
            "unit_new": v["new_unit"],
            "evidence_quote": v["evidence_quote"],
            "confidence": v["confidence"] or "medium",
            "severity": "medium",
            "fix_method": "l1_review_supersede",
            "source_layer": f"{layer}+review_supersede",
            "notes": (
                "Subagent supersede of related field while reviewing the L1 "
                "deterministic flip. " + (v["notes"] or "")
            ).strip(),
            "demoted": "no",
        })

    # ----- C2 review verdicts (extraction_error / real_deviation) -----
    c2_extr_errors = 0
    c2_real_devs   = 0
    c2_confirms    = 0
    c2_demoted_no_source = 0

    def _evidence_indicates_missing_source(s: str) -> bool:
        """True when the C2 reviewer's evidence_quote signals they didn't
        actually have source text to verify against."""
        s = (s or "").strip().lower()
        if not s:
            return True
        return any(tok in s for tok in (
            "section missing",
            "section absent",
            "missing from",
            "not in source",
            "no source",
            "data missing",
            "extracted source data",
            "cannot be verified",
            "cannot verify",
            "consistent with",  # "consistent with CAO 254 pattern" — extrapolation
            "pattern",          # "CAO 254 pattern shows..."
        ))
    if C2_FOLLOWUP_DIR.exists():
        for kind, topic in (("parental", "parental"), ("care", "care")):
            rpath = C2_FOLLOWUP_DIR / f"{kind}_review.csv"
            if not rpath.exists():
                continue
            try:
                with rpath.open("r", encoding="utf-8", newline="") as f:
                    rows_csv = list(csv.reader(f, delimiter=";", quotechar='"'))
            except Exception:
                continue
            if not rows_csv:
                continue
            header = [c.strip() for c in rows_csv[0]]
            for raw in rows_csv[1:]:
                if not any(c.strip() for c in raw):
                    continue
                row = list(raw) + [""] * max(0, len(header) - len(raw))
                d = dict(zip(header, row[:len(header)]))
                rid = d.get("record_id", "").strip()
                verdict = d.get("verdict", "").strip().lower()
                if not rid or not verdict:
                    continue
                meta = by_id.get(rid, {})
                cao_number = meta.get("cao_number", "") if hasattr(meta, "get") else ""
                file_name = meta.get("file_name", "") if hasattr(meta, "get") else ""

                # Demote any verdict whose evidence shows the reviewer didn't
                # actually have source text. The original verdict was guesswork.
                if _evidence_indicates_missing_source(d.get("evidence_quote", "")):
                    c2_demoted_no_source += 1
                    det_rows.append({
                        "source": "subagent",
                        "record_id": rid, "cao_number": cao_number, "file_name": file_name,
                        "topic_group": topic, "field": f"leave_{topic}_exceptions",
                        "csv_value_old": "False", "csv_value_new": "False",
                        "unit_new": "",
                        "evidence_quote": d.get("evidence_quote", ""),
                        "confidence": "low",
                        "severity": "info",
                        "fix_method": "needs_human",
                        "source_layer": f"L1_C2_review_{topic}+demoted_no_source",
                        "notes": (
                            "C2 review verdict DEMOTED: reviewer reported no usable source text "
                            "to verify against. Original verdict='" + verdict + "' (notes: "
                            + (d.get("notes", "") or "") + "). Needs human source check."
                        ),
                        "demoted": "no",  # 'demoted' is reserved for the post-pass evidence demotion
                    })
                    continue

                if verdict == "extraction_error":
                    c2_extr_errors += 1
                    fld = d.get("new_value_field", "").strip() or f"leave_{topic}_*"
                    det_rows.append({
                        "source": "subagent",
                        "record_id": rid, "cao_number": cao_number, "file_name": file_name,
                        "topic_group": topic, "field": fld,
                        "csv_value_old": "", "csv_value_new": d.get("new_value", "").strip(),
                        "unit_new": d.get("new_unit", "").strip(),
                        "evidence_quote": d.get("evidence_quote", ""),
                        "confidence": d.get("confidence", "medium"),
                        "severity": "medium",
                        "fix_method": "c2_extraction_error",
                        "source_layer": f"L0_STATUTORY_CLEAR_{topic}+c2_review",
                        "notes": (
                            "C2 subagent review: extraction error. "
                            + d.get("notes", "")
                        ).strip(),
                        "demoted": "no",
                    })
                elif verdict == "real_deviation":
                    c2_real_devs += 1
                    # Flip exceptions=True
                    det_rows.append({
                        "source": "subagent",
                        "record_id": rid, "cao_number": cao_number, "file_name": file_name,
                        "topic_group": topic, "field": f"leave_{topic}_exceptions",
                        "csv_value_old": "False", "csv_value_new": "True",
                        "unit_new": "",
                        "evidence_quote": d.get("evidence_quote", ""),
                        "confidence": d.get("confidence", "medium"),
                        "severity": "high",
                        "fix_method": "c2_real_deviation",
                        "source_layer": f"L1_C2_review_{topic}",
                        "notes": (
                            "C2 subagent review: source confirms real deviation. "
                            + d.get("notes", "")
                        ).strip(),
                        "demoted": "no",
                    })
                    # Optionally also supersede a value
                    if d.get("new_value_field", "").strip():
                        det_rows.append({
                            "source": "subagent",
                            "record_id": rid, "cao_number": cao_number, "file_name": file_name,
                            "topic_group": topic, "field": d["new_value_field"].strip(),
                            "csv_value_old": "",
                            "csv_value_new": d.get("new_value", "").strip(),
                            "unit_new": d.get("new_unit", "").strip(),
                            "evidence_quote": d.get("evidence_quote", ""),
                            "confidence": d.get("confidence", "medium"),
                            "severity": "medium",
                            "fix_method": "c2_value_supersede",
                            "source_layer": f"L1_C2_review_{topic}",
                            "notes": "C2 subagent supersede of value alongside exception flip.",
                            "demoted": "no",
                        })
                elif verdict == "confirm_no_exception":
                    c2_confirms += 1
                    # Audit-only entry
                    det_rows.append({
                        "source": "subagent",
                        "record_id": rid, "cao_number": cao_number, "file_name": file_name,
                        "topic_group": topic, "field": f"leave_{topic}_exceptions",
                        "csv_value_old": "False", "csv_value_new": "False",
                        "unit_new": "",
                        "evidence_quote": d.get("evidence_quote", ""),
                        "confidence": d.get("confidence", "medium"),
                        "severity": "info",
                        "fix_method": "c2_confirm_no_exception",
                        "source_layer": f"L1_C2_review_{topic}",
                        "notes": (
                            "C2 subagent review: exceptions=False is correct. "
                            + d.get("notes", "")
                        ).strip(),
                        "demoted": "no",
                    })

    # ----- Manual-review fixes (corrections to subagent extraction errors) -----
    # When a row in corrections_manual_review.csv has been re-verified by a
    # second subagent pass, that fix overrides the original chunk verdict for
    # the (record_id, field) pair.
    manual_fix_overrides: dict[tuple[str, str], dict] = {}
    n_manual_clear = 0
    n_manual_correct = 0
    n_manual_move = 0
    n_manual_set_bool = 0
    fix_files = [p for p in (MANUAL_REVIEW_FIXES, MANUAL_REVIEW_FIXES_V2,
                              MANUAL_REVIEW_FIXES_V3, MANUAL_REVIEW_FIXES_V4, MANUAL_REVIEW_FIXES_V5) if p.exists()]
    for fixf in fix_files:
        try:
            with fixf.open("r", encoding="utf-8", newline="") as f:
                rdr = list(csv.reader(f, delimiter=";", quotechar='"'))
        except Exception:
            rdr = []
        if rdr:
            header = [c.strip() for c in rdr[0]]
            for raw in rdr[1:]:
                if not any(c.strip() for c in raw):
                    continue
                row = list(raw) + [""] * max(0, len(header) - len(raw))
                d = dict(zip(header, row[:len(header)]))
                rid = d.get("record_id", "").strip()
                of  = d.get("original_field", "").strip()
                tf  = d.get("target_field", "").strip() or of
                verdict = d.get("verdict", "").strip().lower()
                if not rid or not of:
                    continue
                # Conflict resolution: if two verdicts exist for the same
                # (rec, field), prefer 'clear' (safest) > 'set_boolean' >
                # 'move' > 'correct_in_place' > 'confirm'.
                priority = {"clear": 0, "set_boolean": 1, "move": 2,
                            "correct_in_place": 3, "confirm": 4}
                key = (rid, of)
                existing = manual_fix_overrides.get(key)
                if existing and priority.get(existing["verdict"], 99) <= priority.get(verdict, 99):
                    continue  # keep the higher-priority verdict already in place
                manual_fix_overrides[key] = {
                    "verdict": verdict,
                    "target_field": tf,
                    "new_value": d.get("new_value", ""),
                    "new_unit": d.get("new_unit", ""),
                    "evidence_quote": d.get("evidence_quote", ""),
                    "confidence": d.get("confidence", "medium"),
                    "notes": d.get("notes", ""),
                }
                if verdict == "clear":          n_manual_clear   += 1
                elif verdict == "correct_in_place": n_manual_correct += 1
                elif verdict == "move":         n_manual_move    += 1
                elif verdict == "set_boolean":  n_manual_set_bool += 1

    # ----- Subagent corrections (resilient read + post-processing demotion) -----
    subagent_rows: list[dict] = []
    audit_rows: list[dict] = []
    skipped_rows: list[dict] = []
    chunk_files = sorted(CHUNKS_DIR.glob("chunk_*_corrections.csv"))
    failed_chunks: list[tuple[str, int, int]] = []
    for cp in chunk_files:
        try:
            good, skipped = _read_chunk_resilient(cp)
        except Exception as exc:
            print(f"WARN: could not read {cp.name}: {exc}")
            continue
        # Detect chunks whose subagent run produced only placeholder rows.
        if good:
            placeholder_evidence = sum(
                1 for r in good
                if str(r.get("evidence_quote", "")).strip().lower() in
                   ("(placeholder)", "placeholder")
            )
            if placeholder_evidence == len(good):
                failed_chunks.append((cp.name, len(good), placeholder_evidence))
        elif skipped:
            failed_chunks.append((cp.name, 0, len(skipped)))
        if skipped:
            skipped_rows.extend(skipped)
        for r in good:
            confidence = (r.get("confidence", "") or "").strip().lower()
            evidence = r.get("evidence_quote", "")
            csv_value_new = r.get("csv_value_new", "")
            unit_new = r.get("unit_new", "")
            demoted = "no"
            # Strict semantics #1: high/medium requires verbatim evidence
            if confidence in ("high", "medium") and is_placeholder_or_empty(evidence):
                audit_rows.append({
                    "item_id": r.get("item_id", ""),
                    "record_id": r.get("record_id", ""),
                    "field": r.get("field", ""),
                    "before_confidence": confidence,
                    "before_value": csv_value_new,
                    "before_unit": unit_new,
                    "before_evidence": evidence,
                    "after_confidence": "low",
                    "after_value": "UNKNOWN",
                    "after_unit": "UNKNOWN",
                    "reason_demoted": "high/medium with no verbatim evidence",
                })
                confidence = "low"
                csv_value_new = "UNKNOWN"
                unit_new = "UNKNOWN"
                demoted = "yes"
            # Strict semantics #2: UNKNOWN MUST be low confidence (hard rule)
            elif confidence in ("high", "medium") and str(csv_value_new).strip().lower() == "unknown":
                audit_rows.append({
                    "item_id": r.get("item_id", ""),
                    "record_id": r.get("record_id", ""),
                    "field": r.get("field", ""),
                    "before_confidence": confidence,
                    "before_value": csv_value_new,
                    "before_unit": unit_new,
                    "before_evidence": evidence,
                    "after_confidence": "low",
                    "after_value": "UNKNOWN",
                    "after_unit": str(unit_new).strip() or "UNKNOWN",
                    "reason_demoted": "csv_value_new=UNKNOWN must be confidence=low",
                })
                confidence = "low"
                demoted = "yes"

            rid = str(r.get("record_id", ""))
            meta = by_id.get(rid, {})
            field_name = r.get("field", "")

            # Apply manual-review override if one exists for this (rec, field)
            override = manual_fix_overrides.get((rid, field_name))
            if override is not None:
                v = override["verdict"]
                if v == "clear":
                    csv_value_new = ""
                    unit_new = ""
                elif v == "correct_in_place":
                    csv_value_new = override["new_value"]
                    unit_new = override["new_unit"]
                elif v == "set_boolean":
                    csv_value_new = override["new_value"]
                    unit_new = ""
                elif v == "move":
                    # The moved-to row is emitted below; this original cell is cleared.
                    csv_value_new = ""
                    unit_new = ""
                evidence = override["evidence_quote"] or evidence
                confidence = override["confidence"] or confidence
                # demoted flag stays as-is (we're not re-demoting)

            subagent_rows.append({
                "source": "subagent",
                "record_id": rid,
                "cao_number": meta.get("cao_number", "") if hasattr(meta, "get") else "",
                "file_name": meta.get("file_name", "") if hasattr(meta, "get") else "",
                "topic_group": r.get("topic_group", ""),
                "field": field_name,
                "csv_value_old": r.get("csv_value_old", ""),
                "csv_unit_old": r.get("csv_unit_old", ""),
                "csv_value_new": csv_value_new,
                "unit_new": unit_new,
                "evidence_quote": evidence,
                "confidence": confidence,
                "severity": ("high" if confidence == "high" else "medium" if confidence == "medium" else "low"),
                "fix_method": (
                    "manual_review_fix" if override is not None
                    else ("from_source" if csv_value_new != "UNKNOWN" else "needs_human")
                ),
                "source_layer": "subagent+manual_review" if override is not None else "subagent",
                "notes": (
                    "Manual-review fix: " + override["notes"]
                    if override is not None else r.get("notes", "")
                ),
                "demoted": demoted,
            })

            # If verdict was 'move', emit a SECOND row for the target field with the supplied value.
            if override is not None and override["verdict"] == "move" and override["target_field"] != field_name:
                # Check if there's a separate override for the target field too
                # (e.g. v3 moved here, then v4 wants to clear it).
                tgt_override = manual_fix_overrides.get((rid, override["target_field"]))
                if tgt_override and tgt_override["verdict"] == "clear":
                    # Skip emission entirely — the clear supersedes the move.
                    pass
                else:
                    subagent_rows.append({
                        "source": "subagent",
                        "record_id": rid,
                        "cao_number": meta.get("cao_number", "") if hasattr(meta, "get") else "",
                        "file_name": meta.get("file_name", "") if hasattr(meta, "get") else "",
                        "topic_group": r.get("topic_group", ""),
                        "field": override["target_field"],
                        "csv_value_old": "",
                        "csv_unit_old": "",
                        "csv_value_new": override["new_value"],
                        "unit_new": override["new_unit"],
                        "evidence_quote": override["evidence_quote"],
                        "confidence": override["confidence"] or "medium",
                        "severity": "medium",
                        "fix_method": "manual_review_move",
                        "source_layer": "subagent+manual_review",
                        "notes": "Manual-review move: " + override["notes"],
                        "demoted": "no",
                    })

    # ----- Unit normalization pass -----
    # Clean up whitespace + canonicalize singular -> plural for the common
    # duration tokens. Keeps unit strings comparable across rows.
    def _normalize_unit(u: str) -> str:
        if not u:
            return u
        s = u.strip()
        # Placeholder/dash-only units are not real units — clear them.
        if s in ("-", "--", "—", "?", "??", "n/a", "N/A"):
            return ""
        # singular -> plural for the common tokens (only when the unit is
        # exactly the singular word, not when it's part of a phrase like
        # "times weekly working hour per year")
        sl = s.lower()
        for sing, plur in (("week","weeks"), ("day","days"),
                           ("month","months"), ("year","years"),
                           ("hour","hours")):
            if sl == sing:
                return plur
        return s

    for r in det_rows + subagent_rows:
        r["unit_new"] = _normalize_unit(r.get("unit_new", ""))
        r["csv_unit_old"] = _normalize_unit(r.get("csv_unit_old", ""))
        # Normalize literal 'null' (and 'None'/'nan') in csv_value_new to empty
        if str(r.get("csv_value_new", "")).strip().lower() in ("null","none","nan"):
            r["csv_value_new"] = ""

    # ----- Final demote pass: high/medium with placeholder/empty evidence -----
    # Catches the L1/P3/C2 review-derived rows the chunk-loop demoter missed.
    # Skip rule-based corrections (L0_STATUTORY_CLEAR / L1_*+rule-only) where
    # confidence is ALREADY blanked above — those legitimately have no source.
    n_post_demoted = 0
    for r in det_rows + subagent_rows:
        sl = str(r.get("source_layer", ""))
        if sl.startswith("L0_STATUTORY_CLEAR") and "+c2_review" not in sl and "+manual_review" not in sl:
            continue  # rule-driven clears legitimately have empty evidence
        # Skip pure-rule deterministic flips (no review touched them) where
        # confidence was already blanked.
        if r.get("confidence", "") == "":
            continue
        if r.get("confidence") in ("high", "medium"):
            ev = str(r.get("evidence_quote", "")).strip()
            if ev == "" or ev.startswith("("):
                r["confidence"] = "low"
                # Don't change csv_value_new here — the change is already in
                # the row; we're only demoting the confidence claim.
                n_post_demoted += 1

    # ----- Deduplicate (record_id, field) pairs across subagent rows -----
    # Some records appear in two chunks (extraction split mistake). When we
    # have multiple subagent rows for the same cell, keep the one with the
    # longest verbatim evidence quote; the rest are dropped.
    seen: dict[tuple[str, str], int] = {}  # (rid, field) -> idx in subagent_rows
    n_dedup_dropped = 0
    keep_idxs = []
    for i, r in enumerate(subagent_rows):
        key = (str(r.get("record_id", "")), r.get("field", ""))
        if key in seen:
            cur = seen[key]
            if len(str(r.get("evidence_quote", ""))) > len(str(subagent_rows[cur].get("evidence_quote", ""))):
                seen[key] = i
            n_dedup_dropped += 1
        else:
            seen[key] = i
    keep_set = set(seen.values())
    subagent_rows = [r for i, r in enumerate(subagent_rows) if i in keep_set]

    # ----- Write combined corrections -----
    cols = [
        "source", "record_id", "cao_number", "file_name", "topic_group", "field",
        "csv_value_old", "csv_unit_old", "csv_value_new", "unit_new",
        "evidence_quote", "confidence", "severity", "fix_method", "source_layer",
        "notes", "demoted", "is_noop", "changed",
    ]
    all_rows = det_rows + subagent_rows
    n_noops = 0
    change_kind_counts: dict[str, int] = {"none": 0, "value": 0, "unit": 0, "both": 0}
    for r in all_rows:
        kind = classify_change(
            r.get("csv_value_old", ""),
            r.get("csv_value_new", ""),
            r.get("csv_unit_old", ""),
            r.get("unit_new", ""),
        )
        r["changed"] = kind
        r["is_noop"] = "yes" if kind == "none" else "no"
        change_kind_counts[kind] += 1
        if kind == "none":
            n_noops += 1
    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(cols)
        for r in all_rows:
            w.writerow([r.get(c, "") for c in cols])

    # ----- Write audit -----
    if audit_rows:
        ap = pd.DataFrame(audit_rows)
        ap.to_csv(AUDIT_PATH, sep=";", index=False)
    else:
        with AUDIT_PATH.open("w", encoding="utf-8") as f:
            f.write("item_id;record_id;field;before_confidence;before_value;before_unit;before_evidence;after_confidence;after_value;after_unit;reason_demoted\n")

    # ----- Write skipped log (chunk rows the resilient reader could not repair) -----
    with SKIP_LOG_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(["file", "row_index", "reason", "raw"])
        for s in skipped_rows:
            w.writerow([s["file"], s["row_index"], s["reason"], s["raw"]])

    # ----- Stats -----
    print(f"Deterministic corrections (incl. P3-reviewed):  {len(det_rows)}")
    print(f"  P3 flips reverted by subagent review (yes):   {p3_reverted}")
    print(f"  P3 flips confirmed by subagent review (no):   {p3_confirmed}")
    print(f"  L1 follow-up flips confirmed:                 {l1_confirmed}")
    print(f"  L1 follow-up flips reverted:                  {l1_reverted}")
    print(f"  L1 follow-up flips superseded:                {l1_superseded}")
    print(f"  L1 follow-up sub-field supersedes (orphans):  {l1_orphan_supersedes}")
    print(f"  C2 review extraction errors:                  {c2_extr_errors}")
    print(f"  C2 review real deviations:                    {c2_real_devs}")
    print(f"  C2 review confirms (audit only):              {c2_confirms}")
    print(f"  C2 review DEMOTED (evidence shows no source): {c2_demoted_no_source}")
    if manual_fix_overrides:
        print(f"  Manual-review fixes applied:                  {len(manual_fix_overrides)}")
        print(f"    clear            : {n_manual_clear}")
        print(f"    correct_in_place : {n_manual_correct}")
        print(f"    move             : {n_manual_move}")
        print(f"    set_boolean      : {n_manual_set_bool}")
    if n_dedup_dropped:
        print(f"  Subagent rows deduplicated:                   {n_dedup_dropped}")
    if n_post_demoted:
        print(f"  Final demote pass (placeholder evidence):     {n_post_demoted}")
    print(f"Subagent corrections:      {len(subagent_rows)}  (from {len(chunk_files)} chunk file(s))")
    print(f"Demoted by post-pass:      {len(audit_rows)}")
    print(f"Skipped malformed rows:    {len(skipped_rows)}  -> {SKIP_LOG_PATH.name}")
    if failed_chunks:
        print(f"FAILED chunks (subagent run produced only placeholder output): {len(failed_chunks)}")
        for name, n_good, n_placeholder in failed_chunks:
            print(f"  {name}: {n_good} good row(s), {n_placeholder} placeholder/skipped")
        print(f"  These chunks need to be re-run by the subagent.")
    print(f"Total rows in corrections.csv: {len(all_rows)}")
    print(f"  changed=none  (no-op):                                  {change_kind_counts['none']}")
    print(f"  changed=value (value shifted, unit unchanged):          {change_kind_counts['value']}")
    print(f"  changed=unit  (unit shifted, value unchanged):          {change_kind_counts['unit']}")
    print(f"  changed=both  (value and unit both shifted):            {change_kind_counts['both']}")
    print(f"Output:  {OUT_PATH}")
    print(f"Audit:   {AUDIT_PATH}")
    if subagent_rows:
        sa = pd.DataFrame(subagent_rows)
        print()
        print("Subagent confidence distribution AFTER demotion:")
        print(sa.groupby("confidence").size().to_string())


if __name__ == "__main__":
    main()
