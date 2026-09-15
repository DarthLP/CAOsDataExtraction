"""Era-aware statutory baselines per topic.

USED POST-HOC ONLY. These baselines are NEVER injected into subagent prompts
(that would risk the model anchoring on / filling in the statutory figure — see
`phase_4_5_era_baseline_review.md`). Subagents extract only what the CAO states.
After extraction, `check_outside` compares the extracted value to the statutory
baseline for the record's `ingangsdatum` and a caller FLAGS outliers for human
review. Nothing here ever auto-changes data.

Each `BaselineSpec` has a `direction`:
  - "floor"         : CAO may grant >= statutory; flag readings strictly BELOW.
  - "cap"           : CAO may grant <= statutory; flag readings strictly ABOVE.
  - "informational" : statutory fact for reference only; never auto-flagged.

Approved with Hanna 2026-05-24 (Phase 4.5). Values marked there are encoded here;
notice schedules / AOW table / retirement ages / ketenregeling are kept as
*reference constants* below (informational — not auto-checked), because a single
flat threshold can't represent a tenure/age schedule and Hanna chose
informational treatment for them.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Optional

_date = datetime.date


@dataclass
class BaselineSpec:
    """One era-baseline entry."""
    field: str
    value: float
    unit: str
    era_start: datetime.date            # inclusive
    era_end: Optional[datetime.date]    # None = open-ended
    statutory_source: str               # e.g. "BW 7:673 (transitievergoeding)"
    direction: str = "floor"            # "floor" | "cap" | "informational"


# ─── LEAVE (Phase 1; minima → floors) ────────────────────────────────────────
_LEAVE_BASELINES = [
    BaselineSpec("leave_paid_maternity_value", 16, "weeks",
                 _date(1990, 1, 1), None, "WAZO art. 3:1", "floor"),
    BaselineSpec("leave_paid_paternity_value", 1, "weeks",
                 _date(2019, 1, 1), None, "WIEG 2019", "floor"),
    BaselineSpec("leave_partially_paid_paternity_value", 5, "weeks",
                 _date(2020, 7, 1), None, "WIEG art. 4:2a", "floor"),
    BaselineSpec("leave_parental_partial_value", 9, "weeks",
                 _date(2022, 8, 2), None, "WIEG 2022 (paid parental)", "floor"),
]


# ─── TERM (Phase 4.5) ────────────────────────────────────────────────────────
# Probation (proeftijd) — BW 7:652. Absolute statutory maximum is 2 months
# (bracket-specific lower limits need contract length, which we don't reliably
# have, so we flag only the unambiguous > 2 months). CAP.
# Transitievergoeding annual maximum — BW 7:673. CAP (loose upper sanity bound
# on the severance figure; "or 1 gross annual salary if higher" cannot be
# checked without salary, so this only catches implausibly large EUR amounts).
_TV_CAP_BY_YEAR = {
    2015: 75000, 2016: 76000, 2017: 77000, 2018: 79000, 2019: 81000,
    2020: 83000, 2021: 84000, 2022: 86000, 2023: 89000, 2024: 94000,
    2025: 98000, 2026: 102000,
}


def _severance_cap_specs():
    specs = []
    years = sorted(_TV_CAP_BY_YEAR)
    for y in years:
        end = _date(y, 12, 31)
        if y == years[-1]:
            end = None  # latest cap applies open-ended to future dates
        for f in ("term_severance_extra_value",):
            specs.append(BaselineSpec(
                f, _TV_CAP_BY_YEAR[y], "EUR",
                _date(y, 1, 1), end,
                f"BW 7:673 transitievergoeding max ({y})", "cap"))
    return specs


_TERM_BASELINES = [
    BaselineSpec("term_probation_fixedterm_value", 2, "months",
                 _date(1999, 1, 1), None, "BW 7:652 (max proeftijd)", "cap"),
    BaselineSpec("term_probation_indef_value", 2, "months",
                 _date(1999, 1, 1), None, "BW 7:652 (max proeftijd)", "cap"),
] + _severance_cap_specs()


# ─── PENSION (Phase 4.5) ─────────────────────────────────────────────────────
# Witteveen maximum annual accrual (middelloon) — CAP. Above the era cap is a
# fiscal impossibility ⇒ extraction error. (Eindloon caps are slightly lower;
# using the middelloon cap is the lenient choice — only flags clear violations.)
# NOTE: post Wet toekomst pensioenen (1 Jul 2023) DB accrual is being phased out
# to flat-premium DC through 1 Jan 2028, so a flagged post-2023 accrual may be a
# legacy DB scheme or a misread — the flag reason says so; the human decides.
# Pension franchise (AOW-franchise) — FLOOR. Year-specific fiscal minimums vary
# a lot and can legitimately be ~€10.5k (e.g. an hourly-derived minimum franchise:
# €10,479 in 2017, €12,953 in 2016 — both real, both stated in source). So the
# floor is conservative (€8k/yr) and ONLY catches clear garbage (an article number
# or a sub-annual figure left un-annualized). Monthly/weekly readings are annualized
# before comparison (see check_outside), so "€1,329.62 per month" → €15,955/yr passes.
_PENSION_BASELINES = [
    BaselineSpec("pension_accrual_rate_value", 2.25, "%",
                 _date(1900, 1, 1), _date(2013, 12, 31),
                 "Witteveen (pre-2014, middelloon)", "cap"),
    BaselineSpec("pension_accrual_rate_value", 2.15, "%",
                 _date(2014, 1, 1), _date(2014, 12, 31),
                 "Witteveen 2014 (middelloon)", "cap"),
    BaselineSpec("pension_accrual_rate_value", 1.875, "%",
                 _date(2015, 1, 1), None,
                 "Witteveen 2015 (middelloon)", "cap"),
    BaselineSpec("pension_franchise_value", 8000, "EUR",
                 _date(2015, 1, 1), None,
                 "AOW-franchise fiscal minimum (conservative floor; monthly annualized)", "floor"),
]


# ─── CONTRACT (already QA'd; informational only) ─────────────────────────────
_CONTRACT_BASELINES: list[BaselineSpec] = []  # ketenregeling = informational ref


# ─── Informational reference data (NOT auto-checked) ─────────────────────────
# Kept for documentation / future human-facing annotation only. Per Hanna these
# are "informational": a CAO may lawfully deviate, so we never auto-flag them.
STATUTORY_EMPLOYER_NOTICE_MONTHS = {  # BW 7:672, by years of service
    "<5": 1, "5-10": 2, "10-15": 3, ">=15": 4}
STATUTORY_EMPLOYEE_NOTICE_MONTHS = 1
PENSIOENRICHTLEEFTIJD = {  # fiscal target age (NOT AOW age)
    "pre-2014": 65, "2014": 67, "2018+": 68}
AOW_AGE_BY_YEAR = {  # state pension age (years, months) by year reached
    2013: (65, 1), 2014: (65, 2), 2015: (65, 3), 2016: (65, 6), 2017: (65, 9),
    2018: (66, 0), 2019: (66, 4), 2020: (66, 4), 2021: (66, 4), 2022: (66, 7),
    2023: (66, 10), 2024: (67, 0), 2025: (67, 0), 2026: (67, 0), 2027: (67, 3)}
KETENREGELING = {  # BW 7:668a — max contracts / max months / reset-gap months
    "pre-2015-07": (3, 36, 3), "2015-07_2019": (3, 24, 6), "2020+": (3, 36, 6)}


_BASELINES_BY_TOPIC = {
    "leave": _LEAVE_BASELINES,
    "term": _TERM_BASELINES,
    "pension": _PENSION_BASELINES,
    "contract": _CONTRACT_BASELINES,
}


def _units_comparable(unit: str, spec_unit: str) -> bool:
    """Only compare a reading to a baseline when their units are compatible.
    A blank unit is assumed to be the expected unit (so we still catch e.g. a
    bare accrual number). Mismatched bases (EUR vs months) are skipped."""
    u = (unit or "").strip().lower()
    s = (spec_unit or "").strip().lower()
    if not u:
        return True
    if "%" in s:
        return "%" in u or "percent" in u or "procent" in u
    if "eur" in s or "€" in s:
        return "eur" in u or "€" in u or "euro" in u
    if "month" in s:
        return "month" in u or "maand" in u
    if "week" in s:
        return "week" in u or "weken" in u
    return u == s


class EraBaseline:
    """Post-hoc statutory-baseline lookups + outlier check for a topic."""

    def __init__(self, topic: str):
        self.topic = topic
        self._baselines = _BASELINES_BY_TOPIC.get(topic, [])

    def baseline_for(self, field: str,
                     ingangsdatum: datetime.date) -> Optional[BaselineSpec]:
        """Statutory baseline for the field at the given date, or None."""
        for spec in self._baselines:
            if spec.field != field:
                continue
            if ingangsdatum < spec.era_start:
                continue
            if spec.era_end is not None and ingangsdatum > spec.era_end:
                continue
            return spec
        return None

    def check_outside(self, field: str, value, unit: str,
                      ingangsdatum: datetime.date
                      ) -> tuple[bool, Optional[BaselineSpec], str]:
        """Returns (is_outside, spec, reason). is_outside is True only for a
        floor reading strictly below, or a cap reading strictly above, with a
        compatible unit. Informational baselines (and missing ones) never flag.
        NEVER mutates anything — the caller surfaces the flag for human review.
        """
        spec = self.baseline_for(field, ingangsdatum)
        if spec is None or spec.direction == "informational":
            return (False, spec, "")
        try:
            v = float(str(value).strip().replace(",", "."))
        except (TypeError, ValueError):
            return (False, spec, "")
        if not _units_comparable(unit, spec.unit):
            return (False, spec, "")
        # Annualize sub-annual EUR readings to the spec's annual basis, so a
        # franchise quoted "per month" isn't spuriously flagged below an annual
        # floor (e.g. €1,329.62/month → €15,955/yr).
        ann_note = ""
        u = (unit or "").strip().lower()
        if "eur" in spec.unit.strip().lower() and v:
            if "month" in u or "maand" in u:
                v, ann_note = v * 12, f" (annualized {value}×12)"
            elif "week" in u or "weken" in u:
                v, ann_note = v * 52, f" (annualized {value}×52)"
        outside = False
        rel = ""
        if spec.direction == "floor" and v < spec.value:
            outside, rel = True, "BELOW statutory floor"
        elif spec.direction == "cap" and v > spec.value:
            outside, rel = True, "ABOVE statutory cap"
        if not outside:
            return (False, spec, "")
        reason = (f"{field}={v}{ann_note} is {rel} {spec.value} {spec.unit} "
                  f"({spec.statutory_source}) at {ingangsdatum.isoformat()}")
        if field == "pension_accrual_rate_value" and ingangsdatum >= _date(2023, 7, 1):
            reason += " — note: post-WTP (1 Jul 2023) DB accrual is converting to DC; may be legacy/ misread"
        return (True, spec, reason)

    # Backward-compatible floor helper (leave / earlier callers).
    def is_below_statutory(self, field: str, value, unit: str,
                           ingangsdatum: datetime.date
                           ) -> tuple[bool, Optional[BaselineSpec]]:
        spec = self.baseline_for(field, ingangsdatum)
        if spec is None or spec.direction != "floor":
            return (False, spec)
        if not _units_comparable(unit, spec.unit):
            return (False, spec)
        try:
            v = float(str(value).strip().replace(",", "."))
        except (TypeError, ValueError):
            return (False, spec)
        return (v < spec.value, spec)


# ─── Post-hoc topic-wide outlier flagging (surface for review, never mutate) ──
def _parse_date(s) -> Optional[datetime.date]:
    # The scoped CSV uses Dutch DD/MM/YYYY (e.g. "01/04/2011"); also accept ISO
    # and dash variants. Order matters: try the slash DD/MM/YYYY the data uses.
    s = (str(s) or "").strip()[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _unit_field(value_field: str) -> Optional[str]:
    return value_field[:-6] + "_unit" if value_field.endswith("_value") else None


def flag_topic_outliers(topic: str, scoped_records: list[dict],
                        corrections: Optional[list[dict]] = None) -> list[dict]:
    """For each scoped record's FINAL value on a baseline-checked (floor/cap)
    field, flag readings outside the statutory bound at that record's
    ingangsdatum. The FINAL value is the clean-win correction if one exists,
    else the existing scoped-CSV value. Returns outlier dicts for surfacing to
    human review — NEVER mutates anything, never auto-corrects.

    `scoped_records` / `corrections` are lists of plain dicts (pandas-free so
    this module stays dependency-light; the aggregate driver passes
    df.to_dict("records")).
    """
    eb = EraBaseline(topic)
    checked = sorted({s.field for s in eb._baselines if s.direction != "informational"})
    if not checked:
        return []
    overrides: dict[tuple[str, str], tuple[str, str]] = {}
    for c in (corrections or []):
        if str(c.get("is_noop", "")).strip().lower() == "true":
            continue
        key = (str(c.get("record_id", "")).strip(),
               str(c.get("original_field", "")).strip())
        overrides[key] = (str(c.get("csv_value_new", "")),
                          str(c.get("csv_unit_new", "")))
    out = []
    for rec in scoped_records:
        rid = str(rec.get("id", "")).strip()
        ing = _parse_date(rec.get("ingangsdatum", ""))
        if ing is None:
            continue
        for f in checked:
            uf = _unit_field(f)
            if (rid, f) in overrides:
                val, unit = overrides[(rid, f)]
                source = "correction"
            else:
                val = str(rec.get(f, ""))
                unit = str(rec.get(uf, "")) if uf else ""
                source = "existing_csv"
            if not str(val).strip():
                continue
            outside, spec, reason = eb.check_outside(f, val, unit, ing)
            if outside:
                out.append({
                    "record_id": rid, "field": f, "value": val, "unit": unit,
                    "ingangsdatum": ing.isoformat(), "value_source": source,
                    "direction": spec.direction,
                    "statutory_value": spec.value, "statutory_unit": spec.unit,
                    "statutory_source": spec.statutory_source, "reason": reason,
                })
    return out
