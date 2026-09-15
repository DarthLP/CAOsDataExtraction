"""
qa_leave_patterns.py — codify the 10 recurring extraction patterns observed
in the pilot, run them across the full source-matched dataset.

These patterns CAN'T be caught by Layer 1 (which checks schema-internal
consistency only) or Layer 2 (which only checks topic-presence). They sit
in the gap: cross-field, cross-source patterns that need both CSV and source.

Reads:  qa_leave/outputs/leave_qa_payloads.jsonl  (matched records only)
Writes: qa_leave/outputs/leave_pattern_flags.csv  (one row per detected pattern hit)

Each detector returns: list of dicts with keys:
  pattern_id, severity, topic_group, field, csv_value_old, suggested_fix,
  evidence_snippet (from source), reason
"""

from __future__ import annotations
import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAYLOADS_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
OUT_PATH = ROOT / "outputs" / "leave_pattern_flags.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truthy(v) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() in ("true", "1", "yes", "y")


def _falsy(v) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() in ("false", "0", "no", "n")


def _to_float(v):
    if v is None or str(v).strip() == "" or str(v).lower() == "nan":
        return None
    try:
        return float(str(v).replace(",", "."))
    except ValueError:
        return None


def _has_value(v) -> bool:
    if v is None:
        return False
    s = str(v).strip()
    return s != "" and s.lower() not in ("nan", "none", "null")


def _excerpt(source_text: str, search_terms: list[str], window: int = 350) -> str:
    """Return a short excerpt of source_text around the first matching term."""
    if not source_text:
        return ""
    src = source_text.lower()
    for term in search_terms:
        i = src.find(term.lower())
        if i >= 0:
            start = max(0, i - 50)
            end = min(len(source_text), i + window)
            return source_text[start:end].strip()
    return ""


# ---------------------------------------------------------------------------
# Pattern detectors  — each returns list[dict] of flags for one record
# ---------------------------------------------------------------------------


def p1_wieg_paternity_duplicate(payload: dict) -> list[dict]:
    """WIEG additional partner's leave duplicated in partially_paid AND unpaid."""
    csv = payload["csv_leave_fields"]["paternity"]
    pp_val = _to_float(csv.get("leave_partially_paid_paternity_value"))
    up_val = _to_float(csv.get("leave_unpaid_paternity_value"))
    pp_unit = (csv.get("leave_partially_paid_paternity_unit") or "").lower()
    up_unit = (csv.get("leave_unpaid_paternity_unit") or "").lower()

    if pp_val is None or up_val is None:
        return []
    if pp_val != up_val:
        return []
    # Same numeric value in both. If unit also matches roughly, it's almost
    # certainly the WIEG additional 5 weeks duplicated.
    week_in_pp = "week" in pp_unit
    week_in_up = "week" in up_unit
    if not (week_in_pp and week_in_up):
        return []
    return [{
        "pattern_id": "P1_wieg_paternity_duplicate",
        "severity": "high",
        "topic_group": "paternity",
        "field": "leave_unpaid_paternity_value",
        "csv_value_old": str(up_val),
        "suggested_fix": "null",
        "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                     ["additional birth", "aanvullend geboorteverlof",
                                      "WIEG", "supplementary birth", "five weeks", "5 weeks"]),
        "reason": (
            f"Same value {pp_val} appears in both partially_paid_paternity_value and "
            f"unpaid_paternity_value with week-based units. WIEG additional partner's "
            f"leave is statutory UWV-paid at 70%; belongs in partially_paid only."
        ),
    }]


def p2_tiered_sick_collapse(payload: dict) -> list[dict]:
    """Sickpay continuation captured as 100% but source mentions 90%/70% tiers."""
    csv = payload["csv_leave_fields"]["sick"]
    cont_val = _to_float(csv.get("leave_sickpay_continuation_value"))
    cont_unit = (csv.get("leave_sickpay_continuation_unit") or "").lower()
    if cont_val != 100.0:
        return []
    if "%" not in cont_unit and "percent" not in cont_unit:
        return []
    src = (payload.get("source_text") or "").lower()
    # Look for tier hints
    tier_hints = [
        "90%", "80%", "70%", "second year", "13th to 15th",
        "weeks 27-52", "weeks 53-78", "13e tot 15e", "tweede jaar",
    ]
    matched = [h for h in tier_hints if h in src]
    if len(matched) < 1:
        return []
    return [{
        "pattern_id": "P2_tiered_sick_collapse",
        "severity": "medium",
        "topic_group": "sick",
        "field": "leave_sickpay_continuation_value",
        "csv_value_old": "100.0",
        "suggested_fix": "(keep first-tier 100, but add tier note)",
        "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                     ["90%", "70%", "second year", "tweede jaar"]),
        "reason": (
            f"continuation=100% but source mentions tier(s) {matched}. "
            f"Tiered structure (typically 100/90/80/70 across 26+26+26+26 weeks) "
            f"is captured only at first tier; full schedule should be in note."
        ),
    }]


def p3_hetero_on_age_only(payload: dict) -> list[dict]:
    """hetero_present=True but only age-based seniority differences in source."""
    gen = payload["csv_leave_fields"]["general"]
    seniority = payload["csv_leave_fields"]["seniority_special"]
    if not _truthy(gen.get("leave_hetero_present")):
        return []
    src = (payload.get("source_text") or "").lower()
    # EXPANDED vocabulary of worker-group indicators (covers more sectors).
    # Anything in this list, if found in source, signals a real worker-group
    # split → P3 should NOT fire (hetero_present=True is supported).
    group_hints = [
        # Construction / sectoral splits
        "bouwplaats", "uta", "uta-werknemer", "uta employees", "uta-employees",
        "uta workers", "field worker", "veldwerker", "werknemer in dienst van",
        # Office vs operational / blue vs white collar
        "office staff", "kantoor", "kantoorpersoneel",
        "office and administrative", "office workers", "kantoormedewerkers",
        "manual workers", "operational staff", "operationele",
        "production worker", "productiemedewerker",
        "warehouse staff", "magazijnmedewerker",
        "maintenance staff", "technical staff", "technici",
        "support staff", "ondersteunend",
        "shift workers", "ploegendienst", "day workers", "dagdienst",
        "weekend workers", "weekenddienst",
        "drivers", "chauffeurs",
        "pickers", "orderpicker",
        # Contract type splits (uitzend etc.)
        "uitzendbeding", "no uitzendbeding", "without uitzendbeding",
        "fixed-term contract", "indefinite-term contract",
        "temporary employment contract", "permanent contract",
        "vacation worker", "holiday worker", "vakantiekracht",
        "on-call", "oproepkracht", "min-max contract",
        "payroll worker", "payroll-werknemer",
        # Education / public-sector splits
        "management and teachers", "directie en leraren",
        "oop", "ondersteunend onderwijspersoneel", "onderwijsondersteunend",
        "teachers", "docenten", "leraren",
        "school management", "schoolleiders",
        "support staff in education",
        # Generic group-split phrasings (lower-precision, matched anywhere)
        "applies to ... and ...", "for ... and ...",
        "for the following groups", "for the following categories",
        "verschillende werknemersgroepen", "different worker groups",
        "different categories of employees",
        "for both ... and ...",
        # Heading patterns the p3 markdown export uses ("For X workers:" etc.)
        "for construction site", "voor bouwplaatswerknemers",
        "for office workers", "voor kantoormedewerkers",
        "for hotel employees", "for restaurant",
        "for foreign workers", "voor buitenlandse",
    ]
    matched_group = [h for h in group_hints if h in src]
    # Regex pass for "For X employees:" / "For X workers:" headers (group definitions)
    if not matched_group:
        for m in re.finditer(r"\bfor\s+([a-z][a-z\s\-]{1,40}?)\s+(employees|workers|staff|personnel|werknemers|medewerkers)\b", src):
            label = m.group(0)
            # Skip generic "for all employees" / "for the employees"
            if "all" in label or "the employee" in label or "an employee" in label:
                continue
            matched_group.append(label.strip())
            if len(matched_group) >= 3:
                break
    if matched_group:
        # Real worker-group split — hetero_present=True is supported
        return []
    # Look for age hints
    age_hints = [
        "55 years", "55 jaar", "60 years", "60 jaar", "57 years", "57 jaar",
        "older employees", "oudere werknemers", "extra leave for older",
        "seniorendagen", "senior days",
    ]
    matched_age = [h for h in age_hints if h in src]
    has_age_seniority = _truthy(seniority.get("leave_extra_seniority_present"))
    if matched_age and not matched_group:
        return [{
            "pattern_id": "P3_hetero_on_age_only",
            "severity": "medium",
            "topic_group": "general",
            "field": "leave_hetero_present",
            "csv_value_old": "True",
            "suggested_fix": "False",
            "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                         ["older employees", "55 years", "60 years",
                                          "extra leave for older", "seniorendagen"]),
            "reason": (
                f"hetero_present=True but source mentions age-based extras "
                f"({matched_age[:3]}) without distinct worker groups. Age cohorts "
                f"belong in extra_seniority_*; hetero is for worker-group splits. "
                f"extra_seniority_present={'True' if has_age_seniority else 'False'}."
            ),
        }]
    return []


def p4_window_vs_duration_adoption(payload: dict) -> list[dict]:
    """adoption_value=26 weeks (window) vs ~6 weeks (actual duration)."""
    csv = payload["csv_leave_fields"]["adoption"]
    val = _to_float(csv.get("leave_adoption_value"))
    unit = (csv.get("leave_adoption_unit") or "").lower()
    if val is None or "week" not in unit:
        return []
    if val < 20 or val > 30:
        return []
    src = (payload.get("source_text") or "").lower()
    # Look for window-vs-duration hints
    window_hints = ["spread over", "spread out", "te spreiden over",
                    "binnen een periode van", "within a 26-week", "within 26 weeks"]
    six_hints = [" 6 weeks", "six weeks", "6 consecutive weeks", "zes weken",
                 "maximum of 6", "max 6"]
    has_window = any(h in src for h in window_hints)
    has_six = any(h in src for h in six_hints)
    if has_window and has_six:
        return [{
            "pattern_id": "P4_adoption_window_as_duration",
            "severity": "high",
            "topic_group": "adoption",
            "field": "leave_adoption_value",
            "csv_value_old": str(val),
            "suggested_fix": "6.0",
            "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                         ["spread", "te spreiden", "consecutive weeks"]),
            "reason": (
                f"adoption_value={val} weeks. Source contains both 'window' wording "
                f"(spread over) and '6 weeks' duration. Likely 26 captures the window, "
                f"6 is the actual leave duration."
            ),
        }]
    return []


def p5_education_vacation_undercount(payload: dict) -> list[dict]:
    """Education-sector vacation undercount: only statutory line captured."""
    csv = payload["csv_leave_fields"]["vacation_holidays"]
    val = _to_float(csv.get("leave_vacation_time_value"))
    unit = (csv.get("leave_vacation_time_unit") or "").lower()
    if val is None:
        return []
    src = (payload.get("source_text") or "").lower()
    # Education indicators
    edu_hints = ["oop", "teachers", "management and teachers",
                 "school year", "school holidays", "supplementary vacation",
                 "directie en leraren", "supplementary holiday"]
    has_edu = any(h in src for h in edu_hints)
    if not has_edu:
        return []
    # Look for "supplementary" + numeric hints in source that exceed CSV
    supp_match = re.search(r"supplementary\s+vacation\s+leave\s+for\s+oop\s+is:\s+(\d+)\s+hours?", src)
    if not supp_match:
        return []
    supp = int(supp_match.group(1))
    # If the CSV captured only ~160 (statutory) but source has supp=266 etc.
    if "hour" in unit and val < 200 and supp > 50:
        suggested = val + supp
        return [{
            "pattern_id": "P5_education_vacation_undercount",
            "severity": "high",
            "topic_group": "vacation_holidays",
            "field": "leave_vacation_time_value",
            "csv_value_old": str(val),
            "suggested_fix": str(suggested),
            "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                         ["supplementary vacation", "supplementary holiday",
                                          "statutory vacation"]),
            "reason": (
                f"vacation_time_value={val} hours/year captures only statutory line; "
                f"source explicitly says supplementary={supp} hours. Sum: {suggested}."
            ),
        }]
    return []


def p6_above_statutory_no_value(payload: dict) -> list[dict]:
    """has_above_statutory_maternity / paternity_explicitly_above_statutory = True
    but no maternity/paternity value to back it up."""
    out = []
    mat = payload["csv_leave_fields"]["maternity"]
    if _truthy(mat.get("leave_has_above_statutory_maternity")):
        any_val = any(
            _has_value(mat.get(f))
            for f in ("leave_paid_maternity_value", "leave_partially_paid_maternity_value",
                      "leave_unpaid_maternity_value", "leave_maternity_note")
        )
        if not any_val:
            out.append({
                "pattern_id": "P6_above_statutory_no_value",
                "severity": "medium",
                "topic_group": "maternity",
                "field": "leave_has_above_statutory_maternity",
                "csv_value_old": "True",
                "suggested_fix": "False",
                "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                             ["maternity", "zwangerschap"]),
                "reason": "Flag set True but no maternity duration / note populated.",
            })
    pat = payload["csv_leave_fields"]["paternity"]
    if _truthy(pat.get("leave_paternity_explicitly_above_statutory")):
        any_val = any(
            _has_value(pat.get(f))
            for f in ("leave_paid_paternity_value", "leave_partially_paid_paternity_value",
                      "leave_unpaid_paternity_value")
        )
        if not any_val:
            out.append({
                "pattern_id": "P6_above_statutory_no_value",
                "severity": "medium",
                "topic_group": "paternity",
                "field": "leave_paternity_explicitly_above_statutory",
                "csv_value_old": "True",
                "suggested_fix": "False",
                "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                             ["paternity", "geboorteverlof"]),
                "reason": "Flag set True but no paternity duration field populated.",
            })
    return out


def p7_care_filled_but_exceptions_false(payload: dict) -> list[dict]:
    """care_statutory_ref=True AND care_exceptions=False but care detail fields filled.
    This is the dominant CARE_01 root cause — already in Layer 1, but the
    pattern includes the SUGGESTED FIX (set exceptions=True) which Layer 1
    doesn't provide."""
    csv = payload["csv_leave_fields"]["care"]
    if not _truthy(csv.get("leave_care_statutory_ref")):
        return []
    if not _falsy(csv.get("leave_care_exceptions")):
        return []
    detail_filled = any(
        _has_value(csv.get(f))
        for f in ("leave_short_term_care_value", "leave_short_term_care_pay_value",
                  "leave_long_term_care_value", "leave_long_term_care_pay_value")
    )
    if not detail_filled:
        return []
    return [{
        "pattern_id": "P7_care_exceptions_false_with_details",
        "severity": "high",
        "topic_group": "care",
        "field": "leave_care_exceptions",
        "csv_value_old": "False",
        "suggested_fix": "True",
        "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                     ["care leave", "short-term care", "long-term care",
                                      "zorgverlof", "kortdurend zorgverlof"]),
        "reason": (
            "care_statutory_ref=True AND care_exceptions=False but detail fields "
            "(short_term_care/long_term_care) populated. CAO has CAO-specific "
            "provisions, so exceptions should be True. (Same as Layer 1 CARE_01 "
            "but with explicit suggested fix.)"
        ),
    }]


def p8_parental_filled_but_exceptions_false(payload: dict) -> list[dict]:
    """Same as P7 but for parental: statutory_ref=True, exceptions=False, but
    eligibility sub-fields are populated."""
    csv = payload["csv_leave_fields"]["parental"]
    if not _truthy(csv.get("leave_parental_statutory_ref")):
        return []
    if not _falsy(csv.get("leave_parental_exceptions")):
        return []
    elig_filled = any(
        _has_value(csv.get(f))
        for f in ("leave_parental_min_tenure_value",
                  "leave_parental_min_contract_length_value")
    )
    if not elig_filled:
        return []
    return [{
        "pattern_id": "P8_parental_exceptions_false_with_eligibility",
        "severity": "high",
        "topic_group": "parental",
        "field": "leave_parental_exceptions",
        "csv_value_old": "False",
        "suggested_fix": "True",
        "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                     ["parental leave", "ouderschapsverlof",
                                      "must have worked", "at least one year"]),
        "reason": (
            "parental_statutory_ref=True AND parental_exceptions=False but "
            "eligibility sub-fields are filled. CAO has tenure/contract-length "
            "exception, so exceptions should be True. (Same as Layer 1 PAR_03 "
            "but with explicit suggested fix.)"
        ),
    }]


def p9_unpaid_paternity_with_uwv_in_source(payload: dict) -> list[dict]:
    """unpaid_paternity_value populated but source mentions UWV/benefit pays the
    period — this is the WIEG additional partner's leave; should be partially_paid."""
    csv = payload["csv_leave_fields"]["paternity"]
    up_val = _to_float(csv.get("leave_unpaid_paternity_value"))
    if up_val is None:
        return []
    src = (payload.get("source_text") or "").lower()
    # Only flag if UWV/benefit explicitly mentioned in paternity context
    if not any(t in src for t in ["uwv", "benefit", "uitkering", "70%", "70 percent"]):
        return []
    if "partner" not in src and "geboorte" not in src and "paternity" not in src:
        return []
    # Don't double-flag if P1 already caught this record
    pp_val = _to_float(csv.get("leave_partially_paid_paternity_value"))
    if pp_val == up_val:
        return []  # P1 will handle
    return [{
        "pattern_id": "P9_unpaid_paternity_should_be_partially_paid",
        "severity": "medium",
        "topic_group": "paternity",
        "field": "leave_unpaid_paternity_value",
        "csv_value_old": str(up_val),
        "suggested_fix": "(move to partially_paid_paternity_value with pay=70)",
        "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                     ["UWV", "benefit", "70%", "uitkering"]),
        "reason": (
            f"unpaid_paternity={up_val} but source mentions UWV/benefit. The 5-week "
            "WIEG additional partner's leave is UWV-paid at 70%; belongs in "
            "partially_paid_paternity_value, not unpaid_paternity_value."
        ),
    }]


def p10_lib_day_both_true_in_source(payload: dict) -> list[dict]:
    """LIB_01 fired (both Liberation Day flags True). Try to determine which is
    correct from source: 'lustrum' / 'every 5 years' → lustrum=True, annual=False."""
    csv = payload["csv_leave_fields"]["vacation_holidays"]
    if not (_truthy(csv.get("leave_liberation_day_annual"))
            and _truthy(csv.get("leave_liberation_day_lustrum"))):
        return []
    src = (payload.get("source_text") or "").lower()
    is_lustrum = any(t in src for t in [
        "lustrum", "every 5 years", "every five years", "om de vijf jaar",
        "in lustrum years", "2010, 2015", "2015, 2020", "2020, 2025",
    ])
    if is_lustrum:
        return [{
            "pattern_id": "P10_libday_lustrum_annotated_in_source",
            "severity": "high",
            "topic_group": "vacation_holidays",
            "field": "leave_liberation_day_annual",
            "csv_value_old": "True",
            "suggested_fix": "False",
            "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                         ["lustrum", "every 5 years", "in lustrum years"]),
            "reason": ("Both Liberation Day flags True, but source explicitly says "
                       "lustrum / every 5 years. Annual flag is the wrong one."),
        }]
    return [{
        "pattern_id": "P10_libday_both_true_unresolved",
        "severity": "medium",
        "topic_group": "vacation_holidays",
        "field": "leave_liberation_day_annual",
        "csv_value_old": "True",
        "suggested_fix": "(needs human — source ambiguous)",
        "evidence_snippet": _excerpt(payload.get("source_text") or "",
                                     ["liberation day", "5 mei", "bevrijdingsdag"]),
        "reason": "Both flags True; source doesn't clearly say lustrum or annual.",
    }]


PATTERNS = [
    p1_wieg_paternity_duplicate,
    p2_tiered_sick_collapse,
    p3_hetero_on_age_only,
    p4_window_vs_duration_adoption,
    p5_education_vacation_undercount,
    p6_above_statutory_no_value,
    p7_care_filled_but_exceptions_false,
    p8_parental_filled_but_exceptions_false,
    p9_unpaid_paternity_with_uwv_in_source,
    p10_lib_day_both_true_in_source,
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def main() -> None:
    rows: list[dict] = []
    n_records = 0
    n_flagged = 0
    with PAYLOADS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            o = json.loads(line)
            if o.get("source_status") != "matched":
                continue
            n_records += 1
            record_flags = []
            for fn in PATTERNS:
                try:
                    record_flags.extend(fn(o))
                except Exception as exc:
                    record_flags.append({
                        "pattern_id": getattr(fn, "__name__", "unknown"),
                        "severity": "error",
                        "topic_group": "_meta",
                        "field": "",
                        "csv_value_old": "",
                        "suggested_fix": "",
                        "evidence_snippet": "",
                        "reason": f"detector raised: {exc!r}",
                    })
            if record_flags:
                n_flagged += 1
            for f_ in record_flags:
                rows.append({
                    "record_id": o.get("record_id"),
                    "cao_number": o.get("cao_number"),
                    "file_name": o.get("file_name"),
                    "ingangsdatum": o.get("ingangsdatum"),
                    "general_document_type": o.get("general_document_type"),
                    **f_,
                })

    with OUT_PATH.open("w", newline="", encoding="utf-8") as fout:
        w = csv.writer(fout, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        if rows:
            w.writerow(list(rows[0].keys()))
            for r in rows:
                w.writerow([r.get(k, "") for k in rows[0].keys()])
        else:
            w.writerow([
                "record_id", "cao_number", "file_name", "ingangsdatum",
                "general_document_type", "pattern_id", "severity",
                "topic_group", "field", "csv_value_old", "suggested_fix",
                "evidence_snippet", "reason",
            ])

    print(f"Records scanned:  {n_records}")
    print(f"Records flagged:  {n_flagged}")
    print(f"Total flag rows:  {len(rows)}")
    print(f"Output:           {OUT_PATH}")
    print()
    if rows:
        from collections import Counter
        pcounts = Counter(r["pattern_id"] for r in rows)
        print("Hits by pattern:")
        for k, v in pcounts.most_common():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
