"""verify.py — full-audit holistic verification stage (build / status / consolidate).

The combiner produced `full_audit_flags.csv` with verify_* columns EMPTY. This
stage fills them by source-checking every high+medium flag, holistically per
(topic, record_id): one subagent item = one record's topic section, judged as a
whole so ALL populated fields of that topic are evaluated against the COMPLETE
source text at once (not just the flagged cell). That lets us (a) verify flagged
cells and (b) surface NEW findings on non-flagged fields the statistical checks
never suspected.

Pattern (mirrors qa/shared/holistic_verify.py): pure-Python `build` writes
self-contained chunk worksheets; the MAIN AGENT dispatches one Task subagent per
chunk (<=12 parallel; Opus for term/pension, Sonnet otherwise), each writing a
`*_verified.jsonl` checkpoint; pure-Python `consolidate` folds those verdicts
back into full_audit_flags.csv and emits new_findings.csv. Resumable: rebuilding
is deterministic and never clears completed outputs; consolidate works on
partial results (un-run pairs => verify_verdict="pending").

Source per (topic, record): `verify_source.resolve` — curated by_topic English
block for in-scope CAOs, else the new_flow `*_extract.json` section (READ-ONLY).
No source section => the pair is recorded in no_source.json and its flagged
cells consolidate to verify_verdict="unverified_no_source" (acceptable per spec).

Verdict vocabulary (from holistic_verify): CONFIRM / NEEDS_CHANGE / UNSUPPORTED.

Run:
  python3.13 -m qa.full_audit.verify build           # (re)build all worksheets
  python3.13 -m qa.full_audit.verify status          # done / pending per topic
  python3.13 -m qa.full_audit.verify prompt <chunk>  # print a subagent prompt
  python3.13 -m qa.full_audit.verify consolidate     # fold verdicts into CSV
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from qa.full_audit import common, verify_source
from qa.full_audit.combine import FINAL_COLUMNS, VERIFY_SEVERITIES

HERE = Path(__file__).resolve().parent
FLAGS_CSV = HERE / "full_audit_flags.csv"

VERIFY_DIR = HERE / "verify"
CHUNKS_DIR = VERIFY_DIR / "chunks"
OUTPUTS_DIR = VERIFY_DIR / "outputs"
MANIFEST = VERIFY_DIR / "manifest.json"
NO_SOURCE = VERIFY_DIR / "no_source.json"
NEW_FINDINGS = VERIFY_DIR / "new_findings.csv"
INSTRUCTIONS_MD = VERIFY_DIR / "SUBAGENT_INSTRUCTIONS.md"

CHAR_BUDGET = 60_000            # per chunk; mirrors holistic_verify
HARD_SECTION_CAP = 250_000      # per item source cap
OPUS_TOPICS = {"term", "pension"}

VERDICTS = ("CONFIRM", "NEEDS_CHANGE", "UNSUPPORTED")
# field kinds whose value we ask a subagent to judge (units travel attached to
# their value field; an orphan unit is judged on its own — see _judgeable).
_VALUE_KINDS = {"numeric", "enum", "boolean", "freetext", "date", "list"}

NEW_FINDING_COLUMNS = ["record_id", "cao_number", "file_name", "ingangsdatum",
                       "topic", "field", "current_value", "verdict",
                       "suggested_value", "quote", "confidence", "note"]


# ── flag-set / record helpers ────────────────────────────────────────────────
def _read_flags() -> list[dict]:
    from qa.shared import resilient_csv
    df = resilient_csv.read_csv(FLAGS_CSV, delimiter=";")
    return [{k: common.norm(v) for k, v in r.items()}
            for r in df.to_dict("records")]


def _topic_of_row(r: dict) -> str:
    try:
        return json.loads(r.get("stat_context") or "{}").get("topic", "") or "meta"
    except (ValueError, TypeError):
        return "meta"


def _pairs(flags: list[dict]):
    """Return (pairs, flagged, meta) for the verify scope (high+medium).

    pairs   : sorted unique [(topic, rid)]
    flagged : {(topic, rid): {field: {"severity","reason"}}}
    meta    : {(topic, rid): {"cao_number","file_name","ingangsdatum"}}
    """
    flagged: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    meta: dict[tuple[str, str], dict] = {}
    for r in flags:
        if r.get("severity") not in VERIFY_SEVERITIES:
            continue
        topic, rid, field = _topic_of_row(r), r.get("record_id", ""), r.get("field", "")
        if not rid or not field:
            continue
        key = (topic, rid)
        flagged[key][field] = {"severity": r.get("severity", ""),
                               "reason": r.get("reason", "")}
        meta.setdefault(key, {"cao_number": r.get("cao_number", ""),
                              "file_name": r.get("file_name", ""),
                              "ingangsdatum": r.get("ingangsdatum", "")})
    pairs = sorted(flagged.keys())
    return pairs, flagged, meta


def _record_index() -> dict[str, dict]:
    df = common.load_dataset()
    out: dict[str, dict] = {}
    for rec in df.to_dict("records"):
        out[common.norm(rec.get("id", ""))] = {k: common.norm(v)
                                               for k, v in rec.items()}
    return out


def _judgeable(rec: dict, topic: str, flagged_fields: dict[str, dict]) -> list[dict]:
    """All populated fields of `topic` for this record, each annotated with its
    paired unit and whether it is a flagged cell. Units ride along with their
    value field; an orphan unit (no populated value partner) is judged alone.
    Every flagged field is guaranteed present even if blank (e.g. dropouts)."""
    info = common.classify_columns()
    seen: set[str] = set()
    out: list[dict] = []

    def add(col: str, value: str, unit: str):
        if col in seen:
            return
        seen.add(col)
        fl = flagged_fields.get(col)
        out.append({
            "field": col,
            "value": value,
            "unit": unit,
            "group": info[col].group_key if col in info else "",
            "flagged": bool(fl),
            "flag_severity": fl["severity"] if fl else "",
            "flag_reason": fl["reason"] if fl else "",
        })

    for col, ci in info.items():
        if ci.topic != topic:
            continue
        v = common.norm(rec.get(col, ""))
        if ci.kind in _VALUE_KINDS:
            if common.is_blank(v) and col not in flagged_fields:
                continue
            add(col, v, common.unit_for(rec, col))
        elif ci.kind == "unit":
            if common.is_blank(v):
                continue
            partners = ci.value_partners or []
            orphan = all(common.is_blank(rec.get(p, "")) for p in partners)
            if orphan or col in flagged_fields:
                add(col, v, "")     # judge the unit text itself
    # safety net: any flagged field not yet captured (kind mismatch etc.)
    for col, fl in flagged_fields.items():
        add(col, common.norm(rec.get(col, "")), common.unit_for(rec, col))
    return out


# ── build ────────────────────────────────────────────────────────────────────
def _item_cost(item: dict) -> int:
    return len(item["full_source_text"]) + 220 * len(item["fields"]) + 600


def build() -> None:
    verify_source.clear_cache()
    VERIFY_DIR.mkdir(exist_ok=True)
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    for f in CHUNKS_DIR.rglob("*.json"):   # rebuild worksheets (NOT outputs)
        f.unlink()

    flags = _read_flags()
    pairs, flagged, meta = _pairs(flags)
    recs = _record_index()

    items_by_topic: dict[str, list[dict]] = defaultdict(list)
    no_source: list[dict] = []
    n_no_rec = 0
    for topic, rid in pairs:
        rec = recs.get(rid)
        if rec is None:
            n_no_rec += 1
            continue
        m = meta[(topic, rid)]
        txt, origin = verify_source.resolve(topic, rid, m["cao_number"], m["file_name"])
        ff = flagged[(topic, rid)]
        if origin == "no_source" or not txt.strip():
            no_source.append({"topic": topic, "record_id": rid,
                              "flagged_fields": sorted(ff.keys())})
            continue
        items_by_topic[topic].append({
            "topic": topic,
            "record_id": rid,
            "cao_number": m["cao_number"],
            "file_name": m["file_name"],
            "ingangsdatum": m["ingangsdatum"],
            "origin": origin,
            "fields": _judgeable(rec, topic, ff),
            "full_source_text": txt[:HARD_SECTION_CAP],
        })

    manifest: list[dict] = []
    cid = 0
    for topic in sorted(items_by_topic):
        model = "opus" if topic in OPUS_TOPICS else "sonnet"
        tdir = CHUNKS_DIR / topic
        tdir.mkdir(parents=True, exist_ok=True)
        (OUTPUTS_DIR / topic).mkdir(parents=True, exist_ok=True)
        cur: list[dict] = []
        cc = 0
        seq = 0

        def flush():
            nonlocal cur, cc, seq, cid
            if not cur:
                return
            seq += 1
            cid += 1
            name = f"chunk_{seq:03d}"
            cpath = tdir / f"{name}.json"
            cpath.write_text(json.dumps(
                {"chunk_id": f"{topic}/{name}", "topic": topic, "model": model,
                 "items": cur}, ensure_ascii=False, indent=1), encoding="utf-8")
            manifest.append({
                "chunk_id": f"{topic}/{name}",
                "topic": topic, "model": model,
                "chunk_path": str(cpath.relative_to(VERIFY_DIR)),
                "out_path": f"outputs/{topic}/{name}_verified.jsonl",
                "n_items": len(cur),
                "n_fields": sum(len(it["fields"]) for it in cur),
                "n_flagged": sum(sum(1 for f in it["fields"] if f["flagged"])
                                 for it in cur),
                "record_ids": [it["record_id"] for it in cur],
            })
            cur, cc = [], 0

        for it in items_by_topic[topic]:
            c = _item_cost(it)
            if cur and cc + c > CHAR_BUDGET:
                flush()
            cur.append(it)
            cc += c
        flush()

    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=1),
                        encoding="utf-8")
    NO_SOURCE.write_text(json.dumps(no_source, ensure_ascii=False, indent=1),
                         encoding="utf-8")
    INSTRUCTIONS_MD.write_text(_instructions_text(), encoding="utf-8")

    n_items = sum(len(v) for v in items_by_topic.values())
    n_fields = sum(m["n_fields"] for m in manifest)
    n_flagged = sum(m["n_flagged"] for m in manifest)
    print(f"[verify build] {len(pairs)} high+medium (topic,record) pairs")
    print(f"  resolvable: {n_items} items -> {len(manifest)} chunks "
          f"({n_fields} fields judged, {n_flagged} of them flagged cells)")
    print(f"  no_source : {len(no_source)} pairs -> {NO_SOURCE.name}")
    if n_no_rec:
        print(f"  WARNING: {n_no_rec} pairs had no matching dataset record")
    by_topic = Counter(m["topic"] for m in manifest)
    for t in sorted(by_topic):
        mdl = "opus" if t in OPUS_TOPICS else "sonnet"
        print(f"    {t:11s} {by_topic[t]:3d} chunks  [{mdl}]")
    print(f"  manifest  -> {MANIFEST.name}   instructions -> {INSTRUCTIONS_MD.name}")


# ── status ─────────────────────────────────────────────────────────────────--
def _manifest() -> list[dict]:
    return json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else []


def _is_done(entry: dict) -> bool:
    p = VERIFY_DIR / entry["out_path"]
    return p.exists() and p.stat().st_size > 0


def status() -> None:
    man = _manifest()
    if not man:
        print("[verify status] no manifest — run `build` first.")
        return
    done = [m for m in man if _is_done(m)]
    pend = [m for m in man if not _is_done(m)]
    by_topic_total = Counter(m["topic"] for m in man)
    by_topic_done = Counter(m["topic"] for m in done)
    print(f"[verify status] {len(done)}/{len(man)} chunks verified "
          f"({len(pend)} pending)")
    for t in sorted(by_topic_total):
        print(f"  {t:11s} {by_topic_done[t]:3d}/{by_topic_total[t]:3d}")
    if pend:
        print("  next pending chunks:",
              ", ".join(m["chunk_id"] for m in pend[:12]))
    ns = json.loads(NO_SOURCE.read_text(encoding="utf-8")) if NO_SOURCE.exists() else []
    print(f"  no_source pairs (auto unverified): {len(ns)}")


# ── subagent prompt ────────────────────────────────────────────────────────--
def _instructions_text() -> str:
    return f"""# Full-audit verification — subagent instructions

You verify extracted Collective-Bargaining-Agreement (CAO) field values against
source text. You are given ONE chunk file (JSON). It has `items`; each item is
ONE record's data for ONE topic, with its OWN `full_source_text`.

TREAT EACH ITEM INDEPENDENTLY. Judge an item's fields ONLY against that same
item's `full_source_text`. NEVER use one item's source to judge another's.

The source text is ENGLISH (the upstream extractor translated the Dutch CAO).
Judge meaning, not surface form: "EUR 1.500 per maand" supports value 1500 with
unit "EUR per month"; numbers may be written in words; dates may be phrased.

For EACH field in an item, assign a verdict:
  - CONFIRM      : the source clearly supports the extracted value.
  - NEEDS_CHANGE : the source clearly states a DIFFERENT value for this field.
                   Provide `suggested_value` (the source's value) + a verbatim
                   `quote` copied EXACTLY from full_source_text.
  - UNSUPPORTED  : the value cannot be found or derived from the source (possible
                   hallucination, or the source is silent). `quote` "" unless a
                   nearby passage explains the call.

BOOLEAN fields (value is True or False — e.g. *_present, *_exists, *_applies):
  A False/blank boolean means "this provision is NOT present in this CAO".
  Absence of evidence supports a negative, so:
  - False / blank when the source does not establish the provision: CONFIRM.
  - False but the source CLEARLY describes the provision existing: NEEDS_CHANGE
    (suggest True) with a verbatim quote.
  - True and the source describes the provision: CONFIRM.
  - True but the source is silent / contradicts it: UNSUPPORTED (a positive
    claim needs support). Do NOT mark a False boolean UNSUPPORTED just because
    the source is silent — that is the expected default, so CONFIRM it.

BLANK / MISSING non-boolean values (the field's "value" is empty — usually a
"dropout" flag asking whether an extraction was MISSED):
  - Source contains a value that was left out → NEEDS_CHANGE (suggested_value +
    verbatim quote).
  - Source is silent on this field → a blank is correct: CONFIRM.

EMPTY / THIN SOURCE: some items have little or no real content (e.g. an empty
`[]` block). Then: blank fields and False booleans → CONFIRM; a populated
non-boolean value or a True boolean → UNSUPPORTED (nothing in the source backs
it).

Rules:
  - `quote` MUST be copied verbatim from full_source_text (no paraphrase). Keep
    it short (<=240 chars). Empty string if you have no supporting passage.
  - A field carries an attached `unit`; judge value AND unit together. If only
    the unit is wrong, that is NEEDS_CHANGE with the corrected unit in
    `suggested_value`.
  - Be conservative: only NEEDS_CHANGE when the source is explicit. If unsure
    between CONFIRM and UNSUPPORTED, prefer UNSUPPORTED and say why in `note`.
  - `confidence` ∈ high|medium|low.

OUTPUT — write a JSONL file (one JSON object per line, UTF-8) to the EXACT path
you are given. Emit a line for:
  - EVERY field with "flagged": true  (whatever the verdict — including CONFIRM),
  - any other field ONLY when its verdict is NEEDS_CHANGE or UNSUPPORTED.
Do NOT emit lines for non-flagged CONFIRM fields (keeps output focused).

Each line MUST have these keys:
  {{"topic","record_id","field","verdict","suggested_value","quote",
    "confidence","was_flagged","note"}}
verdict ∈ {VERDICTS}. was_flagged = the field's "flagged" boolean.

Write ONLY the JSONL file. Do not print the verdicts back."""


def prompt(chunk_id: str) -> None:
    """Print a self-contained dispatch prompt for one chunk (by chunk_id or
    1-based manifest index)."""
    man = _manifest()
    entry = None
    if chunk_id.isdigit():
        i = int(chunk_id) - 1
        if 0 <= i < len(man):
            entry = man[i]
    if entry is None:
        entry = next((m for m in man if m["chunk_id"] == chunk_id), None)
    if entry is None:
        print(f"no such chunk: {chunk_id}")
        return
    cpath = VERIFY_DIR / entry["chunk_path"]
    opath = VERIFY_DIR / entry["out_path"]
    print(f"""MODEL: {entry['model']}   (chunk {entry['chunk_id']}, \
{entry['n_items']} records, {entry['n_fields']} fields, {entry['n_flagged']} flagged)

--- PROMPT ---
You are verifying extracted Dutch CAO field values against source text.

Read your chunk worksheet (JSON): {cpath}
Follow the instructions verbatim at: {INSTRUCTIONS_MD}

Write your JSONL verdicts to EXACTLY this path (create parent dirs if needed):
  {opath}

Each item in the chunk is one record's fields for one topic, with its own
`full_source_text`. Judge each item ONLY against its own source. Emit a JSONL
line for every flagged field (any verdict) and for any non-flagged field whose
verdict is NEEDS_CHANGE or UNSUPPORTED, with keys: topic, record_id, field,
verdict (CONFIRM|NEEDS_CHANGE|UNSUPPORTED), suggested_value, quote (verbatim,
<=240 chars), confidence (high|medium|low), was_flagged, note. Write only the
file.""")


# ── consolidate ────────────────────────────────────────────────────────────--
def _load_verdicts() -> dict[tuple[str, str], dict]:
    """(record_id, field) -> verdict line. A field belongs to exactly one topic,
    so (rid, field) is unique across all outputs."""
    idx: dict[tuple[str, str], dict] = {}
    new_findings: list[dict] = []
    bad: list[str] = []
    for f in sorted(OUTPUTS_DIR.rglob("*_verified.jsonl")):
        for ln, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError as e:
                bad.append(f"{f.name}:{ln} {e}")
                continue
            rid, field = str(o.get("record_id", "")), str(o.get("field", ""))
            if not rid or not field:
                continue
            idx[(rid, field)] = o
    if bad:
        for b in bad[:20]:
            print("  BAD:", b)
    return idx


def consolidate() -> None:
    from qa.shared import resilient_csv
    flags = _read_flags()
    verdicts = _load_verdicts()
    ns = json.loads(NO_SOURCE.read_text(encoding="utf-8")) if NO_SOURCE.exists() else []
    no_source_pairs = {(d["topic"], d["record_id"]) for d in ns}
    recs = _record_index()

    counts = Counter()
    for r in flags:
        if r.get("severity") not in VERIFY_SEVERITIES:
            r["verify_verdict"] = r.get("verify_verdict", "") or ""
            counts["out_of_scope_low"] += 1
            continue
        topic, rid, field = _topic_of_row(r), r.get("record_id", ""), r.get("field", "")
        v = verdicts.get((rid, field))
        if v:
            verdict = str(v.get("verdict", "")).upper()
            r["verify_verdict"] = verdict
            r["verify_quote"] = str(v.get("quote", ""))
            r["verify_suggested_value"] = str(v.get("suggested_value", ""))
            r["suggested_review"] = ("yes" if verdict in (
                "NEEDS_CHANGE", "UNSUPPORTED",
                "RELOCATE", "KEEP_NONSTANDARD_UNIT") else "")
            counts[verdict or "EMPTY_VERDICT"] += 1
        elif (topic, rid) in no_source_pairs:
            r["verify_verdict"] = "unverified_no_source"
            r["verify_quote"] = ""
            r["verify_suggested_value"] = ""
            r["suggested_review"] = "yes"
            counts["unverified_no_source"] += 1
        else:
            r["verify_verdict"] = "pending"
            r["verify_quote"] = ""
            r["verify_suggested_value"] = ""
            r["suggested_review"] = ""
            counts["pending"] += 1

    resilient_csv.write_csv(flags, FLAGS_CSV, fieldnames=FINAL_COLUMNS)

    # new findings: non-flagged NEEDS_CHANGE / UNSUPPORTED
    info = common.classify_columns()
    nf_rows: list[dict] = []
    for (rid, field), o in verdicts.items():
        if o.get("was_flagged"):
            continue
        verdict = str(o.get("verdict", "")).upper()
        if verdict not in ("NEEDS_CHANGE", "UNSUPPORTED"):
            continue
        rec = recs.get(rid, {})
        ci = info.get(field)
        nf_rows.append({
            "record_id": rid,
            "cao_number": rec.get("cao_number", ""),
            "file_name": rec.get("file_name", ""),
            "ingangsdatum": rec.get("ingangsdatum", ""),
            "topic": ci.topic if ci else str(o.get("topic", "")),
            "field": field,
            "current_value": rec.get(field, ""),
            "verdict": verdict,
            "suggested_value": str(o.get("suggested_value", "")),
            "quote": str(o.get("quote", "")),
            "confidence": str(o.get("confidence", "")),
            "note": str(o.get("note", "")),
        })
    order = {"NEEDS_CHANGE": 0, "UNSUPPORTED": 1}
    nf_rows.sort(key=lambda x: (order.get(x["verdict"], 9), x["record_id"], x["field"]))
    resilient_csv.write_csv(nf_rows, NEW_FINDINGS, fieldnames=NEW_FINDING_COLUMNS)

    total_scope = sum(counts[k] for k in counts if k != "out_of_scope_low")
    print(f"[verify consolidate] {FLAGS_CSV.name} verify_* filled")
    for k in ("CONFIRM", "NEEDS_CHANGE", "UNSUPPORTED", "unverified_no_source",
              "pending", "EMPTY_VERDICT"):
        if counts.get(k):
            print(f"  {k:22s} {counts[k]}")
    print(f"  (verify-scope flags: {total_scope}; low/out-of-scope: "
          f"{counts['out_of_scope_low']})")
    print(f"  new findings (non-flagged NEEDS_CHANGE/UNSUPPORTED): {len(nf_rows)} "
          f"-> {NEW_FINDINGS.name}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "status"
    if mode == "build":
        build()
    elif mode == "status":
        status()
    elif mode == "prompt":
        prompt(sys.argv[2])
    elif mode == "consolidate":
        consolidate()
    else:
        print(__doc__)
