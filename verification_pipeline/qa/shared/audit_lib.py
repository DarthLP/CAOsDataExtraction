"""Audit checks A1-A16 over corrections.csv.

See `qa/conventions/audit_checks.md` for the catalogue. Output:
  - AuditReport: in-memory summary returned by run_audit
  - needs_human_review.csv: written by flag_outliers_as_needs_human, one row
    per audit-flagged correction with the matching check ID in `notes`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from qa.shared import resilient_csv


# === Configuration ===
A13_SAMPLE_SIZE_FLOOR = 10
A13_DISAGREEMENT_THRESHOLD = 0.20

_BOOLEAN_SUFFIXES = ("_present", "_exceptions", "_statutory_ref",
                     "_above_statutory", "_annual", "_lustrum",
                     "_required", "_allowed", "_explicitly_above_statutory")
# Pay-rate fields: expressed as "% of base salary" (capped at 100).
# Note: surcharge/allowance fields (overtime_allowance_value etc.) are NOT
# included here — surcharges legitimately exceed 100% (e.g. 125-200%).
_PAY_RATE_RE = re.compile(r"_pay_value$|_continuation_value$|_premium_value$|"
                          r"_contribution_value$|_accrual_rate_value$|"
                          r"_topup_pay_value$")

# Surcharge fields where 100-300% is normal. Flag only values > 300 as outliers.
_SURCHARGE_RE = re.compile(r"_allowance_value$|_allowance_range_(?:min|max)$")
_DURATION_VALUE_RE = re.compile(r"_value$")  # broad; refined by NOT matching _pay_value


@dataclass
class AuditCheck:
    id: str
    name: str
    count: int
    sample_rows: list[dict] = field(default_factory=list)
    extra: dict = field(default_factory=dict)


@dataclass
class AuditReport:
    checks: list[AuditCheck]
    a13_per_rule_rates: dict[str, tuple[int, float]] = field(default_factory=dict)
    flagged_keys: set[tuple[str, str]] = field(default_factory=set)  # (record_id, original_field) -> NHR

    def summary(self) -> dict[str, int]:
        return {c.id: c.count for c in self.checks}


# === Helpers ===
def _is_boolean_field(f: str) -> bool:
    return any(f.endswith(s) for s in _BOOLEAN_SUFFIXES)


def _is_pay_rate(f: str) -> bool:
    return bool(_PAY_RATE_RE.search(f))


def _is_surcharge(f: str) -> bool:
    """Surcharge fields where 100-300% is normal."""
    return bool(_SURCHARGE_RE.search(f))


def _is_duration_value(f: str) -> bool:
    """Heuristic: ends in _value but is NOT a pay-rate field."""
    return f.endswith("_value") and not _is_pay_rate(f)


def _is_year_like(v: str) -> bool:
    try:
        n = int(float(v))
        return 1990 <= n <= 2030
    except (ValueError, TypeError):
        return False


def _is_truthy(v: str) -> bool:
    return v.strip().lower() in {"true", "yes", "1", "ja"}


def _is_falsy(v: str) -> bool:
    return v.strip().lower() in {"false", "no", "0", "nee"}


def _is_bool_value(v: str) -> bool:
    return _is_truthy(v) or _is_falsy(v)


def _is_placeholder_evidence(quote: str) -> bool:
    s = (quote or "").strip()
    if not s:
        return True
    if s.startswith("(") and s.endswith(")"):
        return True
    return s.lower() in {"none", "n/a", "unknown"}


def _is_normalized_field(field: str, topic_group: str = "") -> bool:
    """A 'normalized' field holds a controlled-vocabulary token (a `_unit`
    field, or a topic enum), so the canonical value is deliberately NOT a
    verbatim substring of the source evidence (e.g. source 'per kilometer' ->
    canonical 'EUR per km'). The A17 literal-substring check must not penalise
    these when real evidence is present — that's normalization, not fabrication.
    """
    f = (field or "").strip()
    if f.endswith("_unit"):
        return True
    tg = (topic_group or "").strip().lower()
    if tg:
        try:
            from qa.shared import schema_lookup
            if f in set(schema_lookup.get_topic_enum_fields(tg)):
                return True
        except Exception:
            pass
    return False


def _is_negative_verdict(row: dict) -> bool:
    """A 'negative' verdict per A15 criteria."""
    v = row.get("verdict", "").strip().lower()
    if v in ("clear", "unable_to_verify"):
        return True
    if v == "set_boolean" and _is_falsy(row.get("csv_value_new", "")):
        return True
    if (row.get("csv_value_new", "").strip().upper() == "UNKNOWN" and
            row.get("confidence", "").strip().lower() == "low"):
        return True
    return False


# === Main audit ===
def run_audit(corrections_csv: Path) -> AuditReport:
    """Run all 16 checks. Returns AuditReport with counts + flagged keys
    (which get routed to needs_human_review.csv)."""
    df = resilient_csv.read_csv(corrections_csv, delimiter=";")
    rows = df.to_dict("records") if len(df) else []

    flagged: set[tuple[str, str]] = set()

    def _flag(r: dict, check_id: str):
        key = (str(r.get("record_id", "")).strip(),
               str(r.get("original_field", "")).strip())
        flagged.add(key)

    # === A1: missing record_id ===
    a1 = []
    for r in rows:
        if not str(r.get("record_id", "")).strip():
            a1.append(r)
            _flag(r, "A1")

    # === A2: leading/trailing whitespace ===
    a2 = []
    for r in rows:
        v = r.get("csv_value_new", "")
        if isinstance(v, str) and v != v.strip():
            a2.append(r)
            _flag(r, "A2")

    # === A3: high/medium confidence with placeholder evidence ===
    a3 = []
    for r in rows:
        conf = r.get("confidence", "").strip().lower()
        if conf in {"high", "medium"} and _is_placeholder_evidence(r.get("evidence_quote", "")):
            a3.append(r)
            _flag(r, "A3")

    # === A4: UNKNOWN with confidence != low ===
    a4 = []
    for r in rows:
        if str(r.get("csv_value_new", "")).strip().upper() == "UNKNOWN":
            conf = r.get("confidence", "").strip().lower()
            if conf and conf != "low":
                a4.append(r)
                _flag(r, "A4")

    # === A5: number value with unit_new=UNKNOWN ===
    a5 = []
    for r in rows:
        try:
            float(str(r.get("csv_value_new", "")).strip())
        except (ValueError, TypeError):
            continue
        if str(r.get("csv_unit_new", "")).strip().upper() == "UNKNOWN":
            a5.append(r)
            _flag(r, "A5")

    # === A6: boolean field with non-boolean value ===
    a6 = []
    for r in rows:
        f = r.get("original_field", "")
        if _is_boolean_field(f):
            v = str(r.get("csv_value_new", "")).strip()
            if v and v.upper() != "UNKNOWN" and not _is_bool_value(v):
                a6.append(r)
                _flag(r, "A6")

    # === A7: pay-rate field with value > 100 ===
    # Overtime/shift/unfavourable-hours allowances represent SURCHARGE rates
    # where values like 125, 150, 200 are valid (time-and-a-half, double-time).
    # A7 applies only to leave/sick/parental pay-continuation fields where
    # >100% genuinely is "impossible" (you can't be paid more than full wages).
    a7 = []
    overtime_surcharge_re = re.compile(r"^overtime_(allowance|shift_allowance|unfavourable_hours_allowance)")
    for r in rows:
        f = r.get("original_field", "")
        if _is_pay_rate(f) and not overtime_surcharge_re.match(f):
            try:
                n = float(str(r.get("csv_value_new", "")).strip())
                if n > 100:
                    a7.append(r)
                    _flag(r, "A7")
            except (ValueError, TypeError):
                pass

    # === A8: year-like value in non-pay duration field ===
    a8 = []
    for r in rows:
        f = r.get("original_field", "")
        if _is_duration_value(f) and _is_year_like(str(r.get("csv_value_new", ""))):
            a8.append(r)
            _flag(r, "A8")

    # === A9: pay-rate field with duration unit ===
    a9 = []
    duration_units = {"weeks", "week", "weken", "days", "day", "dagen",
                      "hours", "hour", "uur", "uren", "months", "month",
                      "maanden", "years", "year", "jaar"}
    for r in rows:
        f = r.get("original_field", "")
        u = str(r.get("csv_unit_new", "")).strip().lower()
        if _is_pay_rate(f) and u in duration_units:
            a9.append(r)
            _flag(r, "A9")

    # === A10: conflicting (record_id, field) pairs with different csv_value_new ===
    a10 = []
    by_key: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        key = (str(r.get("record_id", "")).strip(),
               str(r.get("original_field", "")).strip())
        by_key.setdefault(key, []).append(r)
    for key, group in by_key.items():
        if len(group) <= 1:
            continue
        vals = {str(r.get("csv_value_new", "")).strip() for r in group}
        if len(vals) > 1:
            a10.extend(group)
            for r in group:
                _flag(r, "A10")

    # === A11: topic-group / field-name mismatch ===
    a11 = []
    for r in rows:
        tg = str(r.get("topic_group", "")).strip().lower()
        f = str(r.get("original_field", "")).strip().lower()
        if tg and f and "_" in f:
            # f should start with tg (e.g. "leave_paid_..." for topic_group "leave")
            field_prefix = f.split("_")[0]
            if tg != field_prefix and not f.startswith(tg + "_"):
                a11.append(r)
                _flag(r, "A11")

    # === A12: demoted rows where post-state is NOT (low + UNKNOWN) ===
    # Demotion logic: when subagent emitted high/medium confidence with
    # placeholder evidence, the aggregator should demote to low+UNKNOWN.
    # If A12 fires, that demotion didn't happen.
    a12 = []
    for r in rows:
        if _is_placeholder_evidence(r.get("evidence_quote", "")) and \
           r.get("confidence", "").strip().lower() != "low":
            a12.append(r)
            _flag(r, "A12")

    # === A13: deterministic-subagent disagreement rate per rule ===
    # Looks at fix_method=det_sub_conflict rows, grouped by det rule signature.
    # Rule signature inferred from notes or topic_group + verdict pattern.
    rule_stats: dict[str, dict[str, int]] = {}
    for r in rows:
        method = r.get("fix_method", "")
        # Approximate "rule firings": count rows where det fired (det_only,
        # det+sub_agree, det_sub_conflict) — group by topic_group + verdict.
        if method not in ("det_only", "det+sub_agree", "det_sub_conflict"):
            continue
        rule_key = f"{r.get('topic_group', '')}|{r.get('verdict', '')}"
        s = rule_stats.setdefault(rule_key, {"firings": 0, "disagreements": 0})
        s["firings"] += 1
        if method == "det_sub_conflict":
            s["disagreements"] += 1
    a13_flagged_rules: dict[str, tuple[int, float]] = {}
    for rule, st in rule_stats.items():
        if st["firings"] < A13_SAMPLE_SIZE_FLOOR:
            continue
        rate = st["disagreements"] / st["firings"]
        if rate > A13_DISAGREEMENT_THRESHOLD:
            a13_flagged_rules[rule] = (st["firings"], rate)

    # === A14: high-confidence verdict on truncated source ===
    a14 = []
    for r in rows:
        if str(r.get("topic_section_was_truncated", "")).strip().lower() != "true":
            continue
        conf = r.get("confidence", "").strip().lower()
        verdict = r.get("verdict", "").strip().lower()
        if conf in {"high", "medium"} and \
           verdict in {"clear", "correct_in_place", "confirm"}:
            a14.append(r)
            _flag(r, "A14")

    # === A15: negative verdict on truncated source ===
    a15 = []
    for r in rows:
        if str(r.get("topic_section_was_truncated", "")).strip().lower() != "true":
            continue
        if _is_negative_verdict(r):
            a15.append(r)
            _flag(r, "A15")

    # === A16: value present in CSV but not found in source ===
    a16 = []
    for r in rows:
        if str(r.get("value_not_in_source", "")).strip().lower() != "true":
            continue
        if r.get("verdict", "").strip().lower() == "confirm":
            a16.append(r)
            _flag(r, "A16")

    # === A17: evidence_quote doesn't contain the proposed new_value ===
    # For value-changing verdicts (correct_in_place, move), the evidence must
    # literally contain the proposed value. Catches subagents that quote the
    # source-block header instead of the verbatim value-bearing line.
    a17 = []
    for r in rows:
        verdict = (r.get("verdict", "") or "").lower()
        if verdict not in {"correct_in_place", "move"}:
            continue
        new_val = (r.get("csv_value_new", "") or "").strip()
        evidence = (r.get("evidence_quote", "") or "")
        # Skip empty / placeholder values
        if not new_val or new_val.upper() == "UNKNOWN":
            continue
        # Skip True/False bool values — evidence may describe the rule textually
        if new_val.lower() in {"true", "false"}:
            continue
        # Check: does the value (or its decimal-comma variant) appear in evidence?
        variants = {new_val, new_val.replace(".", ","), new_val.replace(",", ".")}
        if not any(v in evidence for v in variants):
            # Normalized fields (units/enums) use controlled-vocabulary tokens
            # that are not verbatim source; only flag when no real evidence
            # backs the normalization (else it's fabrication, not normalization).
            if (_is_normalized_field(r.get("original_field", ""),
                                     r.get("topic_group", ""))
                    and not _is_placeholder_evidence(evidence)):
                continue
            a17.append(r)
            _flag(r, "A17")

    # === A18: unable_to_verify verdict but a change was applied ===
    # A sub-driven row whose verdict is unable_to_verify must not ship a value
    # change as a clean win. These arise when a det rule proposes a value
    # (e.g. an inversion) the subagent could not confirm; they belong in human
    # review, not the clean-win set.
    a18 = []
    for r in rows:
        if (r.get("verdict", "") or "").strip().lower() != "unable_to_verify":
            continue
        if str(r.get("is_noop", "")).strip().lower() == "true":
            continue
        # A18 is defined as "unable_to_verify verdict but a change WAS applied".
        # An empty/UNKNOWN new_value applies no change, so there is nothing to
        # review — do not flag it.
        nv = str(r.get("csv_value_new", "")).strip()
        if not nv or nv.upper() == "UNKNOWN":
            continue
        a18.append(r)
        _flag(r, "A18")

    checks = [
        AuditCheck("A1", "missing record_id", len(a1), a1[:5]),
        AuditCheck("A2", "whitespace in csv_value_new", len(a2), a2[:5]),
        AuditCheck("A3", "high/medium conf with placeholder evidence", len(a3), a3[:5]),
        AuditCheck("A4", "UNKNOWN with confidence != low (HARD)", len(a4), a4[:5]),
        AuditCheck("A5", "number value with unit_new=UNKNOWN", len(a5), a5[:5]),
        AuditCheck("A6", "boolean field with non-boolean", len(a6), a6[:5]),
        AuditCheck("A7", "pay-rate field with value > 100", len(a7), a7[:5]),
        AuditCheck("A8", "year-like in duration field", len(a8), a8[:5]),
        AuditCheck("A9", "pay-rate field with duration unit", len(a9), a9[:5]),
        AuditCheck("A10", "conflicting (rid, field) pairs", len(a10), a10[:5]),
        AuditCheck("A11", "topic-group / field-name mismatch", len(a11), a11[:5]),
        AuditCheck("A12", "demoted but post-state not (low+UNKNOWN)", len(a12), a12[:5]),
        AuditCheck("A13", "det-sub disagreement rate per rule >20%",
                   len(a13_flagged_rules),
                   sample_rows=[], extra={"per_rule": a13_flagged_rules}),
        AuditCheck("A14", "high-conf verdict on truncated source", len(a14), a14[:5]),
        AuditCheck("A15", "negative verdict on truncated source", len(a15), a15[:5]),
        AuditCheck("A16", "value_not_in_source with verdict=confirm", len(a16), a16[:5]),
        AuditCheck("A17", "evidence_quote doesn't contain proposed value", len(a17), a17[:5]),
        AuditCheck("A18", "unable_to_verify verdict but a change was applied", len(a18), a18[:5]),
    ]
    return AuditReport(checks=checks,
                       a13_per_rule_rates=a13_flagged_rules,
                       flagged_keys=flagged)


def _per_row_audit_ids(rows: list[dict]) -> dict[tuple[str, str], list[str]]:
    """Recompute, for every row, which audit IDs would fire. This is the
    same logic as `run_audit` but tracks every row, not just the first 5.
    Used by `flag_outliers_as_needs_human` to label notes precisely.
    """
    out: dict[tuple[str, str], list[str]] = {}

    def _add(r, cid):
        key = (str(r.get("record_id", "")).strip(),
               str(r.get("original_field", "")).strip())
        out.setdefault(key, []).append(cid)

    for r in rows:
        if not str(r.get("record_id", "")).strip():
            _add(r, "A1")
        v = r.get("csv_value_new", "")
        if isinstance(v, str) and v != v.strip():
            _add(r, "A2")
        conf = (r.get("confidence", "") or "").strip().lower()
        if conf in {"high", "medium"} and _is_placeholder_evidence(r.get("evidence_quote", "")):
            _add(r, "A3")
        if str(r.get("csv_value_new", "")).strip().upper() == "UNKNOWN":
            if conf and conf != "low":
                _add(r, "A4")
        try:
            float(str(r.get("csv_value_new", "")).strip())
            if str(r.get("csv_unit_new", "")).strip().upper() == "UNKNOWN":
                _add(r, "A5")
        except (ValueError, TypeError):
            pass
        f = r.get("original_field", "")
        if _is_boolean_field(f):
            bv = str(r.get("csv_value_new", "")).strip()
            if bv and bv.upper() != "UNKNOWN" and not _is_bool_value(bv):
                _add(r, "A6")
        if _is_pay_rate(f):
            try:
                if float(str(r.get("csv_value_new", "")).strip()) > 100:
                    _add(r, "A7")
            except (ValueError, TypeError):
                pass
        if _is_duration_value(f) and _is_year_like(str(r.get("csv_value_new", ""))):
            _add(r, "A8")
        duration_units = {"weeks","week","weken","days","day","dagen","hours","hour",
                          "uur","uren","months","month","maanden","years","year","jaar"}
        if _is_pay_rate(f) and str(r.get("csv_unit_new", "")).strip().lower() in duration_units:
            _add(r, "A9")
        # A11 — topic-group / field-name mismatch
        tg = str(r.get("topic_group", "")).strip().lower()
        fl = str(r.get("original_field", "")).strip().lower()
        if tg and fl and "_" in fl:
            pre = fl.split("_")[0]
            if tg != pre and not fl.startswith(tg + "_"):
                _add(r, "A11")
        # A12 — placeholder evidence not demoted
        if _is_placeholder_evidence(r.get("evidence_quote", "")) and conf and conf != "low":
            _add(r, "A12")
        # A14 — high-conf on truncated
        if (str(r.get("topic_section_was_truncated", "")).strip().lower() == "true"
                and conf in {"high","medium"}
                and (r.get("verdict","").strip().lower() in {"clear","correct_in_place","confirm"})):
            _add(r, "A14")
        # A15 — negative verdict on truncated
        if (str(r.get("topic_section_was_truncated", "")).strip().lower() == "true"
                and _is_negative_verdict(r)):
            _add(r, "A15")
        # A16 — value not in source + confirm
        if (str(r.get("value_not_in_source", "")).strip().lower() == "true"
                and (r.get("verdict","").strip().lower() == "confirm")):
            _add(r, "A16")
        # A17 — evidence doesn't contain proposed value
        verdict = (r.get("verdict","") or "").lower()
        if verdict in {"correct_in_place","move"}:
            nv = (r.get("csv_value_new","") or "").strip()
            if nv and nv.upper() != "UNKNOWN" and nv.lower() not in {"true","false"}:
                ev = r.get("evidence_quote","") or ""
                variants = {nv, nv.replace(".",","), nv.replace(",",".")}
                if not any(v in ev for v in variants):
                    # Skip normalized (unit/enum) fields backed by real evidence
                    # — canonical tokens are not verbatim source (see A17 above).
                    if not (_is_normalized_field(r.get("original_field",""),
                                                 r.get("topic_group",""))
                            and not _is_placeholder_evidence(ev)):
                        _add(r, "A17")
        # A18 — unable_to_verify verdict but a change was applied
        if ((r.get("verdict", "") or "").strip().lower() == "unable_to_verify"
                and str(r.get("is_noop", "")).strip().lower() != "true"):
            _nv18 = str(r.get("csv_value_new", "")).strip()
            if _nv18 and _nv18.upper() != "UNKNOWN":  # empty value = no change applied
                _add(r, "A18")
    # A10 — conflicting (rid, field) pairs — done across rows
    from collections import defaultdict
    by_key = defaultdict(list)
    for r in rows:
        by_key[(str(r.get("record_id","")).strip(), str(r.get("original_field","")).strip())].append(r)
    for key, group in by_key.items():
        if len(group) > 1:
            vals = {str(r.get("csv_value_new","")).strip() for r in group}
            if len(vals) > 1:
                for r in group:
                    _add(r, "A10")
    return out


def flag_outliers_as_needs_human(corrections_csv: Path,
                                 audit: AuditReport,
                                 nhr_out: Path,
                                 include_det_sub_conflict: bool = True) -> int:
    """For each audit-flagged row, set fix_method=needs_human_review in
    corrections.csv (in-place rewrite) and write a copy to
    needs_human_review.csv with the audit category in `notes`.

    Also routes `fix_method=det_sub_conflict` rows to NHR (they are
    rule-vs-subagent disagreements which require human resolution per
    PLAN.md §1.4 rule (a2) and (c)).

    Returns number of NHR rows written.
    """
    if not corrections_csv.exists():
        return 0
    df = resilient_csv.read_csv(corrections_csv, delimiter=";")
    df_rows = df.to_dict("records")

    # Full per-row audit-id mapping (not capped at 5)
    audit_id_by_key = _per_row_audit_ids(df_rows)

    nhr_rows = []
    updated_rows = []
    for r in df_rows:
        key = (str(r.get("record_id", "")).strip(),
               str(r.get("original_field", "")).strip())
        is_conflict = include_det_sub_conflict and \
            r.get("fix_method", "") == "det_sub_conflict"
        is_audit_flag = key in audit.flagged_keys
        is_noop = str(r.get("is_noop", "")).strip().lower() == "true"

        # A no-op makes no change to the data, so an audit flag on it is not
        # actionable — there is nothing to ship or correct. Only det_sub_conflict
        # no-ops are routed (the rule and subagent disagree on what SHOULD happen,
        # which needs human resolution even if the net change is currently none).
        if is_audit_flag and is_noop and not is_conflict:
            updated_rows.append(r)
            continue

        if is_audit_flag or is_conflict:
            ids = audit_id_by_key.get(key, [])
            if is_conflict:
                ids = ids + ["det_sub_conflict"]
            if not ids:
                ids = ["AUDIT"]
            note = (r.get("notes", "") or "").strip()
            new_r = dict(r)
            new_r["fix_method"] = "needs_human_review"
            new_r["notes"] = f"audit_flagged:{','.join(ids)}; " + note
            updated_rows.append(new_r)
            nhr_rows.append(new_r)
        else:
            updated_rows.append(r)

    # Rewrite corrections.csv with the updated fix_methods
    from qa.shared.aggregator_lib import OUTPUT_COLUMNS
    resilient_csv.write_csv(updated_rows, corrections_csv,
                            fieldnames=OUTPUT_COLUMNS, delimiter=";")
    n = resilient_csv.write_csv(nhr_rows, nhr_out,
                                fieldnames=OUTPUT_COLUMNS, delimiter=";")
    return n
