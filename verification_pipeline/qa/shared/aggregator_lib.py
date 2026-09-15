"""Aggregator — reconciles deterministic + subagent corrections + manual
overrides into the final corrections.csv per (record_id, field).

Implements PLAN.md §1.4 reconciliation:
  1. Both layers agree -> det+sub_agree
  2. Only det fires -> det_only
  3. Only sub fires -> sub_only
  4. Disagreement:
     (a1) sub=unable_to_verify + det.worksheet_mode='none' -> det wins
     (a2) sub=unable_to_verify + det.worksheet_mode != 'none' -> det_sub_conflict
     (b)  sub confidence in {high,medium} + non-placeholder evidence -> sub wins
     (c)  else -> det_sub_conflict
  5. det=clear + sub=confirm high-conf -> det_sub_conflict (rule miscalibration)

Plus: manual_override (verdict from manual_review_lib) takes precedence.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from qa.shared import resilient_csv, manual_review_lib


@dataclass
class AggregationPaths:
    deterministic_csv: Path
    subagent_csv_glob: str
    corrections_out: Path
    audit_out: Path
    topic: str  # for manual_review_lib lookup


@dataclass
class AggregationResult:
    total_rows: int
    by_fix_method: dict[str, int]
    by_changed: dict[str, int]
    is_noop_count: int
    real_corrections: int


# Output column order (PLAN.md Appendix A schema + leave-aggregator additions)
OUTPUT_COLUMNS = [
    "record_id", "cao_number", "original_field", "topic_group",
    "csv_value_old", "csv_unit_old",
    "verdict", "target_field",
    "csv_value_new", "csv_unit_new",
    "evidence_quote", "confidence",
    "fix_method", "failure_modes_referenced",
    "topic_section_was_truncated", "dropped_passages_summary",
    "value_not_in_source",
    "is_noop", "changed", "notes",
]


# === Helpers ===
_EMPTY_MARKERS = {"", "unknown", "nan", "none", "null", "n/a"}


def _values_equiv(a, b) -> bool:
    """Case-insensitive + numeric-tolerance equivalence. Comma-decimal
    representations ('4,5') are normalized to '4.5' before float parse."""
    sa = str(a).strip().lower()
    sb = str(b).strip().lower()
    if sa == sb:
        return True
    if sa in _EMPTY_MARKERS and sb in _EMPTY_MARKERS:
        return True
    try:
        return float(sa.replace(",", ".")) == float(sb.replace(",", "."))
    except (ValueError, TypeError):
        return False


def _units_equiv(a, b) -> bool:
    sa = str(a).strip().lower()
    sb = str(b).strip().lower()
    if sa == sb:
        return True
    if sa in _EMPTY_MARKERS and sb in _EMPTY_MARKERS:
        return True
    # Common pluralization equivalences
    pairs = [("week", "weeks"), ("day", "days"), ("hour", "hours"),
             ("month", "months"), ("year", "years"),
             ("uur", "uren"), ("week", "weken"), ("dag", "dagen")]
    for x, y in pairs:
        if {sa, sb} == {x, y}:
            return True
    return False


def classify_change(value_old, value_new, unit_old="", unit_new="") -> str:
    """One of {'none', 'value', 'unit', 'both'}."""
    v_eq = _values_equiv(value_old, value_new)
    u_eq = _units_equiv(unit_old, unit_new)
    if v_eq and u_eq:
        return "none"
    if not v_eq and u_eq:
        return "value"
    if v_eq and not u_eq:
        return "unit"
    return "both"


def _is_placeholder_evidence(quote: str) -> bool:
    """Evidence is 'placeholder' if it's empty, parens-only, or known markers."""
    s = (quote or "").strip()
    if not s:
        return True
    if s.startswith("(") and s.endswith(")"):
        return True
    low = s.lower()
    return low in {"none", "n/a", "unknown", "(no relevant text)",
                   "(section missing)"}


# === Loaders ===
def _load_det(path: Path):
    """Returns dict[(record_id, field)] -> row.
    Expects deterministic CSV columns: record_id, field, csv_value_new,
    csv_unit_new, verdict, target_field, evidence_quote, confidence,
    worksheet_mode, severity, notes, plus all the base record metadata.
    """
    if not path.exists():
        return {}
    df = resilient_csv.read_csv(path, delimiter=";")
    out = {}
    for _, row in df.iterrows():
        rid = str(row.get("record_id", "")).strip()
        field = (str(row.get("field", "")).strip() or
                 str(row.get("original_field", "")).strip())
        if not rid or not field:
            continue
        out[(rid, field)] = row.to_dict()
    return out


def _load_subagent(glob_pattern: str):
    """Aggregate all chunk_*_corrections.csv. Dedup by (record_id, field):
    longest evidence_quote wins (matches qa_leave_aggregate_corrections.py
    lines 1043-1056)."""
    out_by_key: dict[tuple[str, str], dict] = {}
    paths = sorted(glob.glob(glob_pattern))
    for p in paths:
        path = Path(p)
        try:
            df = resilient_csv.read_csv(path, delimiter=";")
        except Exception:
            try:
                df = resilient_csv.read_csv(path, delimiter=",")
            except Exception:
                continue
        for _, row in df.iterrows():
            rd = row.to_dict()
            rid = str(rd.get("record_id", "")).strip()
            # subagent rows may use 'field' or 'original_field'
            field = (str(rd.get("field", "")).strip() or
                     str(rd.get("original_field", "")).strip())
            if not rid or not field:
                continue
            key = (rid, field)
            existing = out_by_key.get(key)
            new_evidence = str(rd.get("evidence_quote", ""))
            if existing is None or \
               len(new_evidence) > len(str(existing.get("evidence_quote", ""))):
                out_by_key[key] = rd
    return out_by_key


# === §1.4 Reconciliation ===
def _reconcile(det: dict, sub: dict) -> tuple[str, dict]:
    """Returns (fix_method, winning_row_dict).
    fix_method ∈ {det+sub_agree, det_only, sub_only, det_sub_conflict}.
    """
    det_verdict = (det.get("verdict", "") or "").lower()
    sub_verdict = (sub.get("verdict", "") or "").lower()
    det_value = str(det.get("csv_value_new", "")).strip()
    sub_value = str(sub.get("csv_value_new", "")).strip()
    det_unit = str(det.get("csv_unit_new", det.get("unit_new", ""))).strip()
    sub_unit = str(sub.get("csv_unit_new", sub.get("unit_new", ""))).strip()

    # Special case: det has no actual verdict (L2 presence trigger,
    # worksheet_mode=extract with no proposed correction). Det was just
    # asking "please extract this"; sub provides the answer. The result
    # is sub_only regardless of sub's verdict.
    if not det_verdict:
        return ("sub_only", dict(sub))

    # Rule 1: agree
    if (det_verdict == sub_verdict and
            _values_equiv(det_value, sub_value) and
            _units_equiv(det_unit, sub_unit)):
        merged = dict(sub)  # subagent has evidence_quote etc.
        return ("det+sub_agree", merged)

    # Rule 5 edge case: det=clear, sub=confirm with high-conf -> conflict
    sub_conf = (sub.get("confidence", "") or "").lower()
    if det_verdict == "clear" and sub_verdict == "confirm" and sub_conf == "high":
        return ("det_sub_conflict", dict(sub))

    # Rule 4 (disagreement): apply ordered resolution
    if sub_verdict == "unable_to_verify":
        det_wm = (det.get("worksheet_mode", "") or "").lower()
        if det_wm == "none":
            # (a1) trusted deterministic rule -> det wins
            return ("det_only", dict(det))
        # (a2) risky deterministic rule + sub can't verify -> conflict
        return ("det_sub_conflict", dict(sub))

    # (b) sub has high/medium confidence + non-placeholder evidence -> sub wins
    if sub_conf in {"high", "medium"} and \
       not _is_placeholder_evidence(sub.get("evidence_quote", "")):
        return ("sub_only", dict(sub))

    # (c) else -> conflict
    return ("det_sub_conflict", dict(sub))


# === Output row construction ===
def _make_output_row(source_row: dict,
                     fix_method: str,
                     det_row: Optional[dict] = None,
                     sub_row: Optional[dict] = None) -> dict:
    """Standardize a row into the corrections.csv schema.

    `source_row` is the AUTHORITATIVE row chosen by reconciliation (the winner
    for a conflict, or det/sub for det_only/sub_only). Decision fields
    (verdict, value, unit, evidence, confidence) must come from it. det_row /
    sub_row are only fallbacks for METADATA the winner may lack (cao_number,
    file_name, etc.). Earlier this preferred det_row first, which silently
    overwrote a subagent's value with the det rule's proposal on sub_only rows
    (e.g. an R1 boolean proposing True when the subagent answered False).
    """
    def _g(*keys, default=""):
        # 1. The winner (source_row) wins across all alias keys.
        for k in keys:
            v = source_row.get(k)
            if v not in (None, ""):
                return v
        # 2. Metadata backfill from det then sub.
        for k in keys:
            v = (det_row or {}).get(k) if det_row else None
            if v not in (None, ""):
                return v
            v = (sub_row or {}).get(k) if sub_row else None
            if v not in (None, ""):
                return v
        return default

    out = {
        "record_id":        str(source_row.get("record_id", "")).strip(),
        "cao_number":       str(_g("cao_number")).strip(),
        "original_field":   str(_g("field", "original_field")).strip(),
        "topic_group":      str(_g("topic_group", "field_group")).strip(),
        "csv_value_old":    str(_g("csv_value_old")).strip(),
        "csv_unit_old":     str(_g("csv_unit_old")).strip(),
        "verdict":          str(_g("verdict")).strip(),
        "target_field":     str(_g("target_field")).strip(),
        # Subagent CSV uses 'new_value'/'new_unit'; det uses 'csv_value_new'/'csv_unit_new'
        "csv_value_new":    str(_g("csv_value_new", "new_value")).strip(),
        "csv_unit_new":     str(_g("csv_unit_new", "new_unit", "unit_new")).strip(),
        "evidence_quote":   str(_g("evidence_quote")).strip(),
        "confidence":       str(_g("confidence")).strip().lower(),
        "fix_method":       fix_method,
        "failure_modes_referenced": str(_g("failure_modes_referenced")).strip(),
        "topic_section_was_truncated": str(_g("topic_section_was_truncated", default="False")).strip(),
        "dropped_passages_summary": str(_g("dropped_passages_summary", default="")).strip(),
        "value_not_in_source": str(_g("value_not_in_source", default="False")).strip(),
        "notes":            str(_g("notes")).strip(),
    }
    # is_noop / changed
    kind = classify_change(out["csv_value_old"], out["csv_value_new"],
                           out["csv_unit_old"], out["csv_unit_new"])
    out["changed"] = kind
    out["is_noop"] = "True" if kind == "none" else "False"
    return out


def suppress_noop_vs_actual_csv(corrections_csv: Path, scoped_csv: Path) -> int:
    """Reclassify as is_noop any correction whose proposed new value equals the
    value already in the scoped CSV for that (record_id, original_field).

    Why this is needed: L2 presence-trigger worksheet items carry
    `csv_value_old=""` (the scan only flags "field empty + topic present"),
    so when a subagent *confirms* an existing value (e.g. answers `False` on a
    `*_present` boolean that is already `False` in the CSV), the aggregator's
    is_noop check compares "" vs "False" and wrongly records a change. The
    authoritative comparison is against the ACTUAL CSV value.

    For `*_unit` fields the new content may live in csv_value_new or csv_unit_new
    — both are checked against the CSV's unit column.

    Skips NHR rows. Rewrites corrections.csv in place. Returns count suppressed.
    """
    if not corrections_csv.exists() or not scoped_csv.exists():
        return 0
    import pandas as pd

    corr = pd.read_csv(corrections_csv, sep=";", dtype=str, keep_default_na=False)
    scoped = pd.read_csv(scoped_csv, sep=";", dtype=str, keep_default_na=False)
    scoped_by_id = {str(r["id"]).strip(): r for _, r in scoped.iterrows()}

    suppressed = 0
    for idx, r in corr.iterrows():
        if str(r.get("is_noop", "")).strip().lower() == "true":
            continue
        if str(r.get("fix_method", "")).strip() == "needs_human_review":
            continue
        field = str(r["original_field"]).strip()
        rid = str(r["record_id"]).strip()
        srow = scoped_by_id.get(rid)
        if srow is None or field not in scoped.columns:
            continue
        actual = srow.get(field, "")
        new_val = str(r.get("csv_value_new", "")).strip()
        new_unit = str(r.get("csv_unit_new", "")).strip()
        # The effective new content written to `field`: value column normally,
        # or the unit string for a `_unit` field that used new_unit.
        effective = new_val if new_val else (new_unit if field.endswith("_unit") else "")
        if not effective:
            continue  # clearing or empty — not a confirm-existing case
        # Compare against actual CSV value (numeric-tolerant + unit pluralization).
        is_same = _values_equiv(actual, effective) or _units_equiv(actual, effective)
        if not is_same:
            continue
        # If a unit is also being changed on a value field, only suppress when
        # that unit matches the CSV's paired unit column too.
        if new_val and new_unit and not field.endswith("_unit"):
            unit_field = None
            if field.endswith("_value"):
                unit_field = field[:-6] + "_unit"
            if unit_field and unit_field in scoped.columns:
                if not _units_equiv(srow.get(unit_field, ""), new_unit):
                    continue  # unit genuinely changes — keep
        corr.at[idx, "is_noop"] = "True"
        corr.at[idx, "changed"] = "none"
        note = str(corr.at[idx, "notes"]).strip()
        corr.at[idx, "notes"] = ("suppressed: new value equals existing CSV value "
                                 "(confirmation, not a change); " + note).strip("; ")
        suppressed += 1
    if suppressed:
        corr.to_csv(corrections_csv, sep=";", index=False)
    return suppressed


_NUMERIC_FIELD_SUFFIXES = ("_range_min", "_range_max", "_value")


def suppress_boolean_on_numeric_field(corrections_csv: Path) -> int:
    """Reclassify as is_noop any real correction that writes a boolean
    True/False into a numeric field (`*_range_min`, `*_range_max`, `*_value`).

    L2 fires on numeric range/value fields when the topic is mentioned. A
    subagent that can't extract a number sometimes confuses the field with its
    sibling boolean (e.g. sets `contract_part_time_range_min = True` meaning
    "part-time is allowed"). A range bound or amount can never be True/False,
    so the correction is type-invalid garbage — drop it (the real signal, if
    any, belongs in the sibling `*_allowed`/`*_present` field, handled
    separately).

    Skips rows already flagged needs_human_review so the review queue stays
    consistent. Rewrites corrections.csv in place. Returns count suppressed.
    """
    if not corrections_csv.exists():
        return 0
    import pandas as pd

    corr = pd.read_csv(corrections_csv, sep=";", dtype=str, keep_default_na=False)
    suppressed = 0
    for idx, r in corr.iterrows():
        field = str(r["original_field"]).strip()
        if not field.endswith(_NUMERIC_FIELD_SUFFIXES):
            continue
        if str(r.get("is_noop", "")).strip().lower() == "true":
            continue
        if str(r.get("fix_method", "")).strip() == "needs_human_review":
            continue
        val = str(r.get("csv_value_new", "")).strip().lower()
        if val not in ("true", "false"):
            continue
        corr.at[idx, "is_noop"] = "True"
        corr.at[idx, "changed"] = "none"
        note = str(corr.at[idx, "notes"]).strip()
        corr.at[idx, "notes"] = (
            "suppressed: boolean value on numeric field (likely confused with "
            "sibling _allowed/_present); " + note).strip("; ")
        suppressed += 1
    if suppressed:
        corr.to_csv(corrections_csv, sep=";", index=False)
    return suppressed


def suppress_unit_without_value(corrections_csv: Path, scoped_csv: Path) -> int:
    """Reclassify as is_noop any real correction that sets a `*_unit` field
    while its paired value field stays empty.

    L2 fires on `_unit` fields independently of `_value`. A subagent can often
    infer a unit ("% of hourly rate") from generic language even when it can't
    extract a number, producing a unit-only correction with no value — which is
    not actionable for a human reviewer and inflates the clean-win count.

    A `_unit` correction is suppressed when ALL of:
      - original_field ends in '_unit' and is not already a noop
      - the correction actually sets a unit (the unit string lands in either
        csv_value_new — subagents sometimes put it there for a `_unit` field —
        or csv_unit_new)
      - the paired value field(s) (`<base>_value`, `<base>_range_min/max`) are
        empty in the scoped CSV
      - no sibling correction in this file sets one of those value fields

    NOTE: for a `_unit` field the "content" is the unit string itself, so it may
    legitimately appear in csv_value_new. What makes the correction unactionable
    is the PAIRED VALUE field being empty, not this row's value column.

    Rewrites corrections.csv in place. Returns the number of rows suppressed.
    """
    if not corrections_csv.exists() or not scoped_csv.exists():
        return 0
    import pandas as pd

    corr = pd.read_csv(corrections_csv, sep=";", dtype=str, keep_default_na=False)
    scoped = pd.read_csv(scoped_csv, sep=";", dtype=str, keep_default_na=False)
    scoped_by_id = {str(r["id"]).strip(): r for _, r in scoped.iterrows()}

    def _nonempty(v) -> bool:
        return str(v).strip().lower() not in _EMPTY_MARKERS

    # Which (record_id, value_field) get a value from a CLEAN correction that
    # targets an actual value/range field (not a `_unit` field)? NHR siblings
    # are excluded: a `_unit` clean win whose paired value is only pending
    # human review is not independently shippable, so the unit is suppressed.
    set_by_corr: set[tuple[str, str]] = set()
    for _, r in corr.iterrows():
        f = str(r["original_field"]).strip()
        if f.endswith("_unit"):
            continue
        if str(r.get("is_noop", "")).strip().lower() == "true":
            continue
        if str(r.get("fix_method", "")).strip() == "needs_human_review":
            continue
        if _nonempty(r.get("csv_value_new", "")):
            set_by_corr.add((str(r["record_id"]).strip(), f))

    suppressed = 0
    for idx, r in corr.iterrows():
        field = str(r["original_field"]).strip()
        if not field.endswith("_unit"):
            continue
        if str(r.get("is_noop", "")).strip().lower() == "true":
            continue
        # Only suppress clean-win unit rows — leave NHR rows in the review queue
        # so the needs_human_review.csv stays consistent with corrections.csv.
        if str(r.get("fix_method", "")).strip() == "needs_human_review":
            continue
        # Does this correction actually set a unit string?
        unit_content = (r.get("csv_value_new", "") or r.get("csv_unit_new", ""))
        if not _nonempty(unit_content):
            continue  # clearing/empty — not setting a unit
        base = field[:-5]
        rid = str(r["record_id"]).strip()
        srow = scoped_by_id.get(rid)
        value_fields = [base + "_value", base + "_range_min", base + "_range_max"]
        has_value = False
        for vf in value_fields:
            if srow is not None and vf in scoped.columns and _nonempty(srow.get(vf, "")):
                has_value = True
                break
            if (rid, vf) in set_by_corr:
                has_value = True
                break
        if has_value:
            continue
        # Suppress: unit set with no value anywhere — not actionable.
        corr.at[idx, "is_noop"] = "True"
        corr.at[idx, "changed"] = "none"
        note = str(corr.at[idx, "notes"]).strip()
        corr.at[idx, "notes"] = ("suppressed: unit set without a value; " + note).strip("; ")
        suppressed += 1

    if suppressed:
        corr.to_csv(corrections_csv, sep=";", index=False)
    return suppressed


# === Main entry ===
def run_aggregation(paths: AggregationPaths) -> AggregationResult:
    """Reconcile deterministic + subagent + manual overrides.
    Writes corrections.csv and corrections_audit.csv.
    """
    det_by_key = _load_det(paths.deterministic_csv)
    sub_by_key = _load_subagent(paths.subagent_csv_glob)
    overrides = manual_review_lib.load_manual_overrides(paths.topic)

    all_keys = set(det_by_key.keys()) | set(sub_by_key.keys()) | set(overrides.keys())

    final_rows = []
    audit_rows = []  # rows preserving both det + sub for conflicts

    for key in all_keys:
        rid, field = key
        det = det_by_key.get(key)
        sub = sub_by_key.get(key)
        override = overrides.get(key)

        if override:
            # Build synthetic source_row from override + det/sub fallback
            source = {"record_id": rid, **(override)}
            row = _make_output_row(source, "manual_override",
                                   det_row=det, sub_row=sub)
            row["verdict"] = override["verdict"]
            row["csv_value_new"] = override.get("csv_value_new", "")
            row["csv_unit_new"] = override.get("csv_unit_new", "")
            row["target_field"] = override.get("target_field", "")
            row["notes"] = (override.get("notes", "") or
                            row.get("notes", "")).strip()
            # recompute is_noop after override applied
            kind = classify_change(row["csv_value_old"], row["csv_value_new"],
                                   row["csv_unit_old"], row["csv_unit_new"])
            row["changed"] = kind
            row["is_noop"] = "True" if kind == "none" else "False"
            final_rows.append(row)
            continue

        if det and sub:
            fix_method, winner = _reconcile(det, sub)
            source = {"record_id": rid, **winner}
            row = _make_output_row(source, fix_method, det_row=det, sub_row=sub)
            final_rows.append(row)
            if fix_method == "det_sub_conflict":
                # Audit trail: preserve both
                audit_rows.append(_make_output_row(
                    {"record_id": rid, **det}, "audit_det", det_row=det))
                audit_rows.append(_make_output_row(
                    {"record_id": rid, **sub}, "audit_sub", sub_row=sub))
        elif det:
            source = {"record_id": rid, **det}
            final_rows.append(_make_output_row(source, "det_only", det_row=det))
        elif sub:
            source = {"record_id": rid, **sub}
            final_rows.append(_make_output_row(source, "sub_only", sub_row=sub))

    # Write outputs
    n_final = resilient_csv.write_csv(final_rows, paths.corrections_out,
                                      fieldnames=OUTPUT_COLUMNS, delimiter=";")
    n_audit = resilient_csv.write_csv(audit_rows, paths.audit_out,
                                      fieldnames=OUTPUT_COLUMNS, delimiter=";")

    # Compute summary
    by_method: dict[str, int] = {}
    by_changed: dict[str, int] = {}
    noop_count = 0
    for r in final_rows:
        by_method[r["fix_method"]] = by_method.get(r["fix_method"], 0) + 1
        by_changed[r["changed"]] = by_changed.get(r["changed"], 0) + 1
        if r["is_noop"] == "True":
            noop_count += 1
    return AggregationResult(
        total_rows=n_final,
        by_fix_method=by_method,
        by_changed=by_changed,
        is_noop_count=noop_count,
        real_corrections=n_final - noop_count,
    )
