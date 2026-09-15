"""Check 5 — unit-kind semantic mismatch (value/unit *family* contamination).

The gap this closes (found 2026-06-02): Check 4 (value_unit_contamination) only
catches *structural* slips — a number in the unit slot, unit text in the value
slot, a mashed "<n> <unit>" pair. It does NOT catch the case where BOTH cells are
individually clean but semantically belong to a different KIND of field:

    leave_paid_maternity_value = 100   unit = "% of salary"

100 parses fine as a number; "% of salary" is a fine unit string — so Check 4 is
silent. But `leave_paid_maternity_value` is canonically a DURATION field (modal
value 16.0 = the statutory 16 weeks; 454/607 cells carry a time unit). The 100 is
the *pay rate* bleeding into a *duration* field. 198 records carry this exact
contamination; summing the column as "weeks" is corrupted by every one of them.

cross_version misses it (the value is consistent across a CAO's versions),
numeric_outlier misses it (100 isn't an outlier when 198 records share it), and
enum_format doesn't apply. Hence this check.

METHOD — per numeric value field that has a paired unit column:
  1. Map every non-blank unit string to a coarse *family* via unit_category:
        pay   = percent | money          (a rate or an amount of pay)
        time  = hours | weeks | months | years | days
        count = count | fte
        distance = distance
        (other / blank -> ignored)
     pay deliberately merges % and EUR: "8% holiday allowance" and "€2083"
     are two legitimate encodings of the SAME pay field and must NOT flag.
  2. The field's CANONICAL family = the dominant family across the dataset,
     but only if it's a clear plurality (>= CANON_FRAC of classified cells and
     >= MIN_SAMPLE cells). Fields without a clear canonical family are
     "ambiguous dual-use" — reported separately, never per-cell flagged (we
     can't tell which half is wrong).
  3. Flag each populated value cell whose unit family != the canonical family.
        time<->pay  -> high  (the duration/rate confusion; wrecks aggregates)
        any other cross-family mismatch -> medium

Surfacing only. Run:  python3.13 -m qa.full_audit.unit_semantics
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from qa.full_audit import common

OUT = Path(__file__).resolve().parent / "unit_semantics.csv"

# Tunables. We only flag when a field has a clear plurality family, so a
# minority unit is genuine contamination rather than a legitimately dual-encoded
# field.
CANON_FRAC = 0.55      # dominant family must be >= 55% of classified cells
MIN_SAMPLE = 10        # ... over at least this many classified cells
AMBIG_LO = 0.40        # fields with dominant frac in [LO, CANON_FRAC) -> reported


def mismatch_severity(canon_fam: str, cell_fam: str) -> str | None:
    """Directional incompatibility. Returns 'high' | 'medium' | None.

    We ONLY police TIME-canonical fields, for precision. Rationale:
      - Into a TIME field, NOTHING else belongs — a leave/notice DURATION can
        never legitimately be a '% of salary' or '€'. pay-in-time is the classic
        rate-bleed (the maternity=100 case) -> HIGH; foreign count/distance units
        (e.g. '0.5 fte' in an hours field) -> MEDIUM.
      - PAY-canonical fields are deliberately NOT policed: severance & allowances
        legitimately carry €, '%', 'months of salary' (time) AND 'times salary'
        (count) — three valid encodings — so any cross-family rule there floods
        false positives. Those are surfaced via the 'ambiguous dual-use' report.
    """
    if canon_fam != "time" or cell_fam == "time" or not cell_fam:
        return None
    return "high" if cell_fam == "pay" else "medium"

# Family token lists. We do NOT reuse common.unit_category here: it checks "%"
# before "weeks", so it misreads "weeks at 100% pay" (a DURATION, value=16) as
# pay. Instead we use "earliest dimension token wins" — the head of the unit
# phrase decides. "weeks at 100% pay" -> time (weeks leads); "percent of annual
# working hours" -> pay (percent leads). This is the crux of telling a
# duration-annotated-with-pay from a rate-of-time.
_FAMILY_TOKENS: dict[str, tuple[str, ...]] = {
    # "period"/"periode" are TIME: "pay period", "payroll period" are spans, and
    # a multiplier "2 times weekly working hours" is a duration (the noun decides,
    # not the "times"). So "times"/"maal"/"keer" are NOT dimension tokens.
    "time": ("week", "weken", "hour", "uur", "uren", "day", "dag", "month",
             "maand", "year", "jaar", "jaren", "annual", "period", "periode"),
    # Pay DIMENSION markers are only the rate/money symbols. Words like
    # "salary"/"wage"/"pay" are qualifiers ("percent OF salary", "pay PERIOD")
    # and must not by themselves mark a unit as pay.
    "pay": ("%", "percent", "procent", "eur", "€", "euro"),
    "count": ("fte", "step", "trede"),
    "distance": ("km", "kilomet"),
}


def unit_family(unit: str) -> str:
    """Semantic family of a unit string by EARLIEST dimension token ('' if none).

    Whichever family's token appears first in the string wins, so a unit's head
    noun decides its dimension regardless of trailing qualifiers.
    """
    u = str(unit).strip().lower()
    if not u or u in common._BLANK:
        return ""
    best_fam, best_pos = "", 10 ** 9
    for fam, toks in _FAMILY_TOKENS.items():
        ps = [u.find(t) for t in toks if t in u]
        if ps:
            p = min(ps)
            if p < best_pos:
                best_pos, best_fam = p, fam
    return best_fam


def _canonical_families(recs, value_unit):
    """For each unit column, the (canonical_family, frac, counter) over the
    dataset, or None if no clear canonical family."""
    # unit_col -> Counter of families seen
    fam_by_unit: dict[str, Counter] = defaultdict(Counter)
    unit_cols = {ucol for _v, ucol in value_unit}
    for rec in recs:
        for ucol in unit_cols:
            fam = unit_family(common.norm(rec.get(ucol, "")))
            if fam:
                fam_by_unit[ucol][fam] += 1

    canon: dict[str, tuple[str, float, Counter]] = {}
    ambiguous: list[tuple[str, str, float, dict]] = []
    for ucol, ctr in fam_by_unit.items():
        total = sum(ctr.values())
        if total < MIN_SAMPLE:
            continue
        fam, n = ctr.most_common(1)[0]
        frac = n / total
        if frac >= CANON_FRAC:
            canon[ucol] = (fam, frac, ctr)
        elif frac >= AMBIG_LO and len(ctr) > 1:
            ambiguous.append((ucol, fam, frac, dict(ctr)))
    return canon, ambiguous


def run(return_ambiguous: bool = False):
    df = common.load_dataset()
    recs = df.to_dict("records")
    value_unit = common.value_unit_pairs()
    canon, ambiguous = _canonical_families(recs, value_unit)

    flags: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for rec in recs:
        for vcol, ucol in value_unit:
            if ucol not in canon:
                continue
            v = common.norm(rec.get(vcol, ""))
            u = common.norm(rec.get(ucol, ""))
            if common.is_blank(v) or common.is_blank(u):
                continue
            fam = unit_family(u)
            canon_fam, frac, ctr = canon[ucol]
            if not fam:
                continue
            sev = mismatch_severity(canon_fam, fam)
            if sev is None:
                continue
            key = (common.norm(rec.get("id", "")), vcol)
            if key in seen:
                continue
            seen.add(key)
            reason = (
                f"[unit_kind_mismatch] unit {u!r} is '{fam}', but {vcol} is "
                f"canonically '{canon_fam}' ({ctr[canon_fam]}/{sum(ctr.values())}"
                f"={frac:.0%} of cells) — a '{fam}' value likely contaminating a "
                f"'{canon_fam}' field")
            flags.append(common.make_flag(
                rec, vcol, "unit_semantics", sev, reason,
                json.dumps({"unit_family": fam, "canonical_family": canon_fam,
                            "canonical_frac": round(frac, 3),
                            "family_counts": dict(ctr), "unit_field": ucol},
                           ensure_ascii=False),
                subcheck="unit_kind_mismatch", value=v, unit=u))
    if return_ambiguous:
        return flags, ambiguous
    return flags


def main():
    flags, ambiguous = run(return_ambiguous=True)
    from qa.shared import resilient_csv
    resilient_csv.write_csv(flags, OUT, fieldnames=common.FLAG_COLUMNS)
    by_sev = Counter(f["severity"] for f in flags)
    by_field = Counter(f["field"] for f in flags)
    print(f"[unit_semantics] {len(flags)} unit-kind-mismatch flags -> {OUT}")
    print("  by severity:", dict(by_sev))
    print("  top fields:")
    for fld, n in by_field.most_common(12):
        print(f"    {fld:48s} {n}")
    if ambiguous:
        print(f"\n  AMBIGUOUS dual-use fields (NOT flagged per-cell; "
              f"dominant family {int(AMBIG_LO*100)}-{int(CANON_FRAC*100)}% — "
              f"schema/extractor should disambiguate):")
        for ucol, fam, frac, ctr in sorted(ambiguous, key=lambda x: -sum(x[3].values())):
            print(f"    {ucol:48s} dominant '{fam}' {frac:.0%}  {ctr}")


if __name__ == "__main__":
    main()
