"""
qa_leave_worksheets.py — produce per-record subagent worksheets for source-dependent flags.

Each worksheet collects ALL source-dependent flags (Layer 1 errors not auto-fixed,
pattern hits not auto-fixed, Layer 2 'discussed_csv_empty' for critical fields)
plus a compact source excerpt around the relevant topic.

Output: qa_leave/outputs/subagent_worksheets/worksheets.jsonl  (one record per line)

Each line contains:
  {
    "record_id", "cao_number", "file_name", "ingangsdatum",
    "items": [
      {"item_id", "topic_group", "field", "csv_value_old", "csv_unit_old",
       "flag_type", "flag_reason", "source_excerpt"}
    ]
  }

Subagent task: for each item, produce csv_value_new + unit_new + evidence_quote.

We don't include things already auto-corrected (P3, CARE_01, PAR_03, etc).
"""
from __future__ import annotations
import csv
import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PAYLOADS_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
L1_PATH = ROOT / "outputs" / "leave_rule_violations.csv"
L2_PATH = ROOT / "outputs" / "leave_topic_presence.csv"
PAT_PATH = ROOT / "outputs" / "leave_pattern_flags.csv"
DET_PATH = ROOT / "outputs" / "corrections_deterministic.csv"

OUT_DIR = ROOT / "outputs" / "subagent_worksheets"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "worksheets.jsonl"
INDEX_PATH = OUT_DIR / "worksheet_index.csv"

# How much source text around the relevant topic to include per item
EXCERPT_WINDOW = 600

# Topic-group → search anchors in source text (to extract excerpt — fallback only)
TOPIC_ANCHORS: dict[str, list[str]] = {
    "general": ["general leave enhancements", "minimum-cao"],
    "maternity": ["maternity leave", "zwangerschapsverlof", "bevallingsverlof", "pregnancy"],
    "paternity": ["paternity", "partner leave", "geboorteverlof", "birth leave", "WIEG"],
    "adoption": ["adoption", "foster", "adoptieverlof"],
    "parental": ["parental leave", "ouderschapsverlof"],
    "sick": ["sickness", "sick pay", "incapacity for work", "loondoorbetaling", "ziekte"],
    "care": ["care leave", "zorgverlof", "short-term care", "long-term care", "kortdurend zorgverlof"],
    "vacation_holidays": ["vacation", "holiday allowance", "vakantie", "Liberation Day"],
    "seniority_special": ["special leave", "seniorendagen", "extra leave for older"],
}

# How a section's leading label maps to our topic_group (case-insensitive prefix match).
# Order matters: longer / more specific prefixes first within a topic.
SECTION_PREFIX_TO_TOPIC: list[tuple[str, str]] = [
    # general
    ("general leave enhancements",       "general"),
    ("any other leave",                  "general"),
    ("other leave-related",              "general"),
    ("leave calculation",                "general"),
    ("rules on pension accrual",         "general"),
    ("unpaid leave",                     "general"),
    ("paid leave",                       "general"),
    # maternity
    ("maternity leave",                  "maternity"),
    ("pregnancy leave",                  "maternity"),
    ("pregnancy and maternity",          "maternity"),
    # paternity
    ("paternity/partner leave",          "paternity"),
    ("paternity leave",                  "paternity"),
    ("partner leave",                    "paternity"),
    ("birth leave",                      "paternity"),
    ("additional paternity leave",       "paternity"),
    ("aanvullend geboorteverlof",        "paternity"),
    ("geboorteverlof",                   "paternity"),
    # adoption
    ("adoption and foster leave",        "adoption"),
    ("adoption",                         "adoption"),
    ("foster leave",                     "adoption"),
    # parental
    ("parental leave",                   "parental"),
    ("ouderschapsverlof",                "parental"),
    # sick
    ("sickness",                         "sick"),
    ("sick pay",                         "sick"),
    # care
    ("care leave",                       "care"),
    ("short-term care",                  "care"),
    ("long-term care",                   "care"),
    ("kortdurend zorgverlof",            "care"),
    ("langdurend zorgverlof",            "care"),
    ("calamity leave",                   "care"),
    ("calamiteitenverlof",               "care"),
    ("short-term absence",               "care"),
    ("kort verzuim",                     "care"),
    # vacation_holidays
    ("vacation and holiday allowance",   "vacation_holidays"),
    ("vacation entitlement",             "vacation_holidays"),
    ("holiday allowance",                "vacation_holidays"),
    ("rules for taking vacation",        "vacation_holidays"),
    ("vacation days during sickness",    "vacation_holidays"),
    ("payout of unused vacation",        "vacation_holidays"),
    ("working time reduction",           "vacation_holidays"),
    ("public holidays",                  "vacation_holidays"),
    ("paid public holidays",             "vacation_holidays"),
    ("statutory vacation days expiry",   "vacation_holidays"),
    ("expiration of statutory vacation", "vacation_holidays"),
    ("buying and selling vacation",      "vacation_holidays"),
    ("collective vacation",              "vacation_holidays"),
    ("vacation hours",                   "vacation_holidays"),
    ("distinction between statutory",    "vacation_holidays"),
    # seniority_special
    ("special leave",                    "seniority_special"),
    ("special leaves",                   "seniority_special"),
    ("religious leave",                  "seniority_special"),
    ("extra vacation for young",         "seniority_special"),
    ("extra vacation for older",         "seniority_special"),
    ("extra non-statutory vacation",     "seniority_special"),
    ("seniority- or age-based extra",    "seniority_special"),
    ("seniority-based extra",            "seniority_special"),
    ("age-based extra leave",            "seniority_special"),
]

# Minimum chars in a topic_section before we consider it "good enough" without fallback
MIN_TOPIC_SECTION_CHARS = 200

_JSON_BLOCK_RE = re.compile(r"```json\s*(\[.*?\])\s*```", re.DOTALL)


def parse_topic_sections(source_text: str) -> dict[str, str]:
    """Parse the p3 markdown's embedded JSON array and bucket sections by topic_group.

    Returns dict: topic_group -> concatenated section text (joined with '\n').
    Each topic_group key in TOPIC_ANCHORS will be present (empty string if no match).
    """
    out: dict[str, list[str]] = {t: [] for t in TOPIC_ANCHORS.keys()}
    m = _JSON_BLOCK_RE.search(source_text or "")
    if not m:
        return {k: "" for k in out}
    try:
        arr = json.loads(m.group(1))
    except Exception:
        return {k: "" for k in out}
    for sec in arr:
        if not isinstance(sec, list) or not sec:
            continue
        # Each section is a list of strings. The first one typically carries the
        # topic label. Concatenate the whole section for the matched topic.
        head = str(sec[0]).strip().lower()
        text = "\n".join(str(x) for x in sec)
        topic = None
        for prefix, t in SECTION_PREFIX_TO_TOPIC:
            if head.startswith(prefix):
                topic = t
                break
        if topic is None:
            # Unknown label: skip (or could put in "general")
            continue
        out[topic].append(text)
    return {k: "\n\n".join(v) for k, v in out.items()}


def _resolve_context(parsed: dict[str, str], topic: str, full_source: str) -> tuple[str, str]:
    """Decide what context to give the subagent for this (record, topic).

    Priority:
      1. parsed topic_section (>= MIN_TOPIC_SECTION_CHARS) → context_type='topic_section'
      2. excerpt fallback if some content but short                → context_type='topic_section_partial'
      3. full source_text                                          → context_type='full_source'
      4. empty (extreme edge case)                                 → context_type='empty'

    Returns (text, context_type).
    """
    section = (parsed or {}).get(topic, "") or ""
    if len(section) >= MIN_TOPIC_SECTION_CHARS:
        return section, "topic_section"
    if section:
        # Have something but short — augment with a wider excerpt around topic anchor
        ex = excerpt(full_source, topic, window=1200)
        # Prefer the longer of the two; if excerpt is bigger, use it but mark as partial
        if len(ex) > len(section):
            return ex, "topic_section_partial"
        return section, "topic_section_partial"
    # Section missing entirely — fall back to full source
    if full_source:
        return full_source, "full_source"
    return "", "empty"


def excerpt(source_text: str, topic: str, window: int = EXCERPT_WINDOW) -> str:
    """Return a window of source_text around the first matching anchor for the topic.
    Used only as a fallback hint; the topic_section is the primary signal."""
    if not source_text:
        return ""
    src_lower = source_text.lower()
    for anchor in TOPIC_ANCHORS.get(topic, []):
        i = src_lower.find(anchor.lower())
        if i >= 0:
            start = max(0, i - 80)
            end = min(len(source_text), i + window)
            return source_text[start:end].strip()
    return ""


def main() -> None:
    # Load all the inputs
    l1 = pd.read_csv(L1_PATH, sep=";", dtype=str).fillna("")
    l2 = pd.read_csv(L2_PATH, sep=";", dtype=str).fillna("")
    pat = pd.read_csv(PAT_PATH, sep=";", dtype=str).fillna("")
    det = pd.read_csv(DET_PATH, sep=";", dtype=str).fillna("")

    # Set of (record_id, field) already covered by deterministic corrections
    det_covered = set(zip(det["record_id"].astype(str), det["field"].astype(str)))

    # Load payloads keyed by record_id; pre-parse topic sections for each
    payloads: dict[str, dict] = {}
    parsed_sections: dict[str, dict[str, str]] = {}
    with PAYLOADS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            if o.get("source_status") == "matched":
                rid = str(o["record_id"])
                payloads[rid] = o
                parsed_sections[rid] = parse_topic_sections(o.get("source_text") or "")

    # Build per-record items
    items_by_record: dict[str, list[dict]] = {}

    def _add(rid: str, item: dict) -> None:
        items_by_record.setdefault(rid, []).append(item)

    item_counter = [0]

    def _next_id() -> str:
        item_counter[0] += 1
        return f"item_{item_counter[0]:05d}"

    # --- L1 source-dependent rules ---
    # FIELD_ROLE_01 errors → need source to find the actual duration
    # MAT_02 / SICK_01 etc. → need source to verify the number
    L1_SOURCE_DEPENDENT = (
        "FIELD_ROLE_01_duration_field_has_non_duration_unit",
        "MAT_02_paid_maternity_plausible_range",
        "SICK_01_sickpay_duration_plausible",
        "VAC_01_vacation_time_plausible",
        "VAC_02_vacation_bonus_plausible",
        "SICK_02_continuation_pct_plausible",
        "SICK_03_topup_present_consistent_with_pay",
        "MAT_01_partially_paid_pay_requires_partially_paid_value",
        "CARE_02_pay_requires_value",
        "SEN_01_schedule_requires_present",
    )
    for _, r in l1.iterrows():
        rule_id = r.get("rule_id", "")
        if not rule_id.startswith(L1_SOURCE_DEPENDENT):
            continue
        rid = str(r["record_id"])
        topic = r.get("field_group", "")
        # field is the first one in fields_examined
        fields = r.get("fields_examined", "").split(",")
        primary_field = (fields[0].strip() if fields else "")
        if (rid, primary_field) in det_covered:
            continue
        # Extract csv_value_old and csv_unit_old from the values column
        values_raw = r.get("values", "")
        # values look like "value=100.0, unit='percent of daily wage'"
        m_val = re.search(r"value=([^,;]+)", values_raw)
        m_unit = re.search(r"unit=['\"]([^'\"]*)['\"]", values_raw)
        csv_val = m_val.group(1).strip() if m_val else ""
        csv_unit = m_unit.group(1).strip() if m_unit else ""
        payload = payloads.get(rid)
        if not payload:
            continue
        ts, ctx_type = _resolve_context(parsed_sections.get(rid, {}), topic, payload.get("source_text") or "")
        _add(rid, {
            "item_id": _next_id(),
            "topic_group": topic,
            "field": primary_field,
            "csv_value_old": csv_val,
            "csv_unit_old": csv_unit,
            "flag_type": "L1",
            "flag_reason": f"{rule_id}: {r.get('message', '')}",
            "context_type": ctx_type,
            "topic_section": ts,
        })

    # --- Pattern hits source-dependent (skip ones already deterministically corrected) ---
    PATTERN_SOURCE_DEPENDENT = ("P2_", "P4_", "P5_", "P9_", "P10_libday_both_true_unresolved")
    for _, p in pat.iterrows():
        pid = p.get("pattern_id", "")
        if not pid.startswith(PATTERN_SOURCE_DEPENDENT):
            continue
        rid = str(p["record_id"])
        field = p.get("field", "")
        if (rid, field) in det_covered:
            continue
        payload = payloads.get(rid)
        if not payload:
            continue
        ts, ctx_type = _resolve_context(parsed_sections.get(rid, {}), p.get("topic_group", ""), payload.get("source_text") or "")
        _add(rid, {
            "item_id": _next_id(),
            "topic_group": p.get("topic_group", ""),
            "field": field,
            "csv_value_old": p.get("csv_value_old", ""),
            "csv_unit_old": "",
            "flag_type": f"pattern:{pid}",
            "flag_reason": p.get("reason", "")[:300],
            "context_type": ctx_type,
            "topic_section": ts,
        })

    # --- L2 discussed_csv_empty for CRITICAL numeric fields only ---
    # We don't include all 859 because many are notes / soft fields. Focus on
    # numeric duration / pay value fields that materially affect downstream analysis.
    # FIX A: expand each (record, topic) into one item per critical field that is
    # currently null. This replaces the "(any in topic)" placeholder.
    CRITICAL_FIELDS_BY_TOPIC: dict[str, list[str]] = {
        "maternity": [
            "leave_paid_maternity_value",
            "leave_partially_paid_maternity_value",
            "leave_partially_paid_maternity_pay_value",
        ],
        "paternity": [
            "leave_paid_paternity_value",
            "leave_partially_paid_paternity_value",
            "leave_partially_paid_paternity_pay_value",
        ],
        "adoption": [
            "leave_adoption_value",
            "leave_adoption_pay_value",
        ],
        "parental": [
            "leave_parental_unpaid_value",
            "leave_parental_topup_pay_value",
            "leave_parental_min_tenure_value",
        ],
        "sick": [
            "leave_sickpay_duration_value",
            "leave_sickpay_continuation_value",
            "leave_sick_topup_present",
        ],
        "care": [
            "leave_short_term_care_value",
            "leave_short_term_care_pay_value",
            "leave_long_term_care_value",
            "leave_long_term_care_pay_value",
        ],
        "vacation_holidays": [
            "leave_vacation_time_value",
            "leave_vacation_bonus_value",
        ],
    }

    def _is_default(val) -> bool:
        if val is None:
            return True
        s = str(val).strip()
        return s == "" or s.lower() in ("nan", "none", "null", "false")

    for _, r in l2.iterrows():
        if r.get("presence_status") != "discussed_csv_empty":
            continue
        rid = str(r["record_id"])
        topic = r.get("topic_group", "")
        crit_fields = CRITICAL_FIELDS_BY_TOPIC.get(topic, [])
        if not crit_fields:
            continue
        payload = payloads.get(rid)
        if not payload:
            continue
        topic_csv = (payload.get("csv_leave_fields") or {}).get(topic, {}) or {}
        ts, ctx_type = _resolve_context(parsed_sections.get(rid, {}), topic, payload.get("source_text") or "")
        for field in crit_fields:
            if field not in topic_csv:
                continue
            if not _is_default(topic_csv.get(field)):
                continue  # Already populated; skip
            if (rid, field) in det_covered:
                continue
            _add(rid, {
                "item_id": _next_id(),
                "topic_group": topic,
                "field": field,
                "csv_value_old": "(empty)",
                "csv_unit_old": "",
                "flag_type": "L2_discussed_csv_empty",
                "flag_reason": (
                    f"Topic '{topic}' is discussed in source but {field} is at default. "
                    "If the topic_section explicitly states a value for this field, "
                    "extract it. Otherwise emit UNKNOWN with confidence=low."
                ),
                "context_type": ctx_type,
                "topic_section": ts,
            })

    # --- Write JSONL + index ---
    # FIX: each item carries a topic_section (just the relevant slice of the p3
    # markdown for that topic). No more bloated full-source-per-record. Subagent
    # reads topic_section per item — focused but complete context.
    index_rows: list[dict] = []
    with OUT_PATH.open("w", encoding="utf-8") as fout:
        for rid, items in sorted(items_by_record.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0):
            payload = payloads.get(rid, {})
            ws = {
                "record_id": rid,
                "cao_number": payload.get("cao_number", ""),
                "file_name": payload.get("file_name", ""),
                "ingangsdatum": payload.get("ingangsdatum", ""),
                "items": items,
            }
            fout.write(json.dumps(ws, ensure_ascii=False) + "\n")
            index_rows.append({
                "record_id": rid,
                "cao_number": payload.get("cao_number", ""),
                "file_name": payload.get("file_name", ""),
                "n_items": len(items),
                "total_topic_section_chars": sum(len(it.get("topic_section", "")) for it in items),
            })

    pd.DataFrame(index_rows).to_csv(INDEX_PATH, sep=";", index=False)
    print(f"Records with worksheets: {len(items_by_record)}")
    print(f"Total source-dependent items: {sum(len(v) for v in items_by_record.values())}")
    print(f"Output:  {OUT_PATH}")
    print(f"Index:   {INDEX_PATH}")
    if items_by_record:
        from collections import Counter
        types = Counter(it["flag_type"] for items in items_by_record.values() for it in items)
        print()
        print("By flag_type:")
        for k, v in types.most_common():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
