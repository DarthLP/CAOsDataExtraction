"""
qa_leave_consistency_scan.py — date-aware internal consistency scan.

Where qa_leave_rules.py runs SCHEMA-internal checks on the raw input CSV, this
script runs CONTENT-level consistency checks on the POST-deterministic-flip
state, using statutory baselines that depend on the CAO's ingangsdatum.

Why date-aware? The Dutch parental and paternity baselines have changed
several times. A 2008 CAO with 13× weekly working hours of unpaid parental
leave is just restating the WAZO baseline of its era; a 2024 CAO with the
same 13 is below the new 2022 baseline (17 unpaid + 9 paid). Treating both
the same way produces dozens of false-positive "deviations" that aren't
deviations at all.

Inputs:
  inputs/extracted_data_non_salary.csv
  outputs/corrections_deterministic.csv  (deterministic flips applied first)
  outputs/leave_qa_payload_index.csv

Output:
  outputs/leave_consistency_scan.csv
    columns: record_id, ingangsdatum, era, check, severity, detail

Checks emitted (one row each):
  C1_parental_exc_true_no_details        — exceptions=True but no detail field populated
  C1_care_exc_true_no_details            — same for care
  C2_parental_exc_false_with_deviation   — exceptions=False but era-aware real deviation
  C2_care_exc_false_with_deviation       — same for care
  C3_paternity_above_stat_no_value       — flag=True but no paternity value populated
  C4_liberation_day_both_true            — annual AND lustrum both True (mutual exclusion)

Statutory baselines (parental; weekly working hours):
  pre 2001-12-01                              :  6 unpaid
  2001-12-01 to 2009-01-01                    : 13 unpaid
  2009-01-01 to 2022-08-02                    : 26 unpaid
  2022-08-02 onwards                          : 17 unpaid + 9 UWV-paid at 70%

Statutory baselines (paternity; days paid + weeks 70%):
  pre 2019-01-01                              :  2 days paid
  2019-01-01 to 2020-07-01                    :  5 days paid (1 week)
  2020-07-01 onwards                          :  5 days paid + 5 weeks WIEG @ 70%

Care leave: 2× weekly working hours short-term @ 70%, 6× weekly working
hours long-term unpaid. Stable since WAZO 2001.
"""
from __future__ import annotations
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC_PATH   = ROOT / "inputs" / "extracted_data_non_salary.csv"
DET_PATH   = ROOT / "outputs" / "corrections_deterministic.csv"
INDEX_PATH = ROOT / "outputs" / "leave_qa_payload_index.csv"
OUT_PATH   = ROOT / "outputs" / "leave_consistency_scan.csv"

# ---- Statutory transition dates ----
PAR_13_TO_26   = datetime(2009, 1, 1)
PAR_26_SPLIT   = datetime(2022, 8, 2)   # Wet betaald ouderschapsverlof
PAT_2_TO_5     = datetime(2019, 1, 1)
PAT_WIEG_DATE  = datetime(2020, 7, 1)


# ---- helpers ----
EMPTY_MARKERS = {"", "(empty)", "unknown", "nan", "none", "null"}

def _truthy(v): return str(v or "").strip().lower() == "true"
def _falsy(v):  return str(v or "").strip().lower() == "false"
def _empty(v):  return str(v or "").strip().lower() in EMPTY_MARKERS

def _to_float(v):
    try:
        return float(str(v).replace(",", "."))
    except (ValueError, TypeError, AttributeError):
        return None

def _parse_dt(s: str) -> datetime | None:
    s = (s or "").strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def parental_baseline(dt: datetime | None) -> tuple[float, float] | None:
    """(unpaid_weeks, paid_weeks_at_70pct)."""
    if dt is None: return None
    if dt < datetime(2001, 12, 1):  return (6.0, 0.0)
    if dt < PAR_13_TO_26:           return (13.0, 0.0)
    if dt < PAR_26_SPLIT:           return (26.0, 0.0)
    return (17.0, 9.0)


def paternity_baseline(dt: datetime | None) -> tuple[float, float, float] | None:
    """(paid_days, partial_paid_weeks, partial_paid_pct)."""
    if dt is None: return None
    if dt < PAT_2_TO_5:    return (2.0, 0.0, 0.0)
    if dt < PAT_WIEG_DATE: return (5.0, 0.0, 0.0)
    return (5.0, 5.0, 70.0)


# ---- field families ----
PARENTAL_DETAIL_FIELDS = [
    "leave_parental_min_tenure_value", "leave_parental_min_contract_length_value",
    "leave_parental_topup_pay_value", "leave_parental_unpaid_value",
]
PARENTAL_BOOL_FIELDS = [
    "leave_parental_eligibility_present", "leave_parental_topup_present",
]
CARE_DETAIL_FIELDS = [
    "leave_short_term_care_value", "leave_short_term_care_pay_value",
    "leave_long_term_care_value", "leave_long_term_care_pay_value",
]
CARE_BOOL_FIELDS = ["leave_care_topup_present"]
PATERNITY_VALUE_FIELDS = [
    "leave_paid_paternity_value", "leave_partially_paid_paternity_value",
    "leave_unpaid_paternity_value",
]


def _any_filled(row, num_fields, bool_fields):
    if any(_to_float(row.get(f)) for f in num_fields):
        return True
    if any(_truthy(row.get(f)) for f in bool_fields):
        return True
    return False


def has_real_parental_deviation(row, dt) -> str | None:
    """Return human-readable evidence string if a deviation exists, else None."""
    if _truthy(row.get("leave_parental_topup_present")):
        return "topup_present=True"
    v = _to_float(row.get("leave_parental_topup_pay_value"))
    if v is not None and v > 0:
        # Topup pay > 0 always indicates an employer top-up (above the UWV/statutory
        # 70% which is funded by UWV, not the employer).
        return f"topup_pay={v}>0"
    if _to_float(row.get("leave_parental_min_tenure_value")):
        return "min_tenure_value present (statutory has none)"
    if _to_float(row.get("leave_parental_min_contract_length_value")):
        return "min_contract_length_value present (statutory has none)"
    # Era-aware unpaid_value comparison
    base = parental_baseline(dt)
    if base is None:
        return None
    stat_unpaid = base[0]
    v = _to_float(row.get("leave_parental_unpaid_value"))
    unit = str(row.get("leave_parental_unpaid_unit") or "").lower()
    if v is None:
        return None
    # Skip if unit is clearly not a duration unit (those are caught by FIELD_ROLE_01)
    if unit and not any(tok in unit for tok in ("week", "hour", "month", "day")):
        return None
    if v != stat_unpaid:
        return f"unpaid={v} ≠ era-statutory {stat_unpaid}"
    return None


def has_real_care_deviation(row) -> str | None:
    if _truthy(row.get("leave_care_topup_present")):
        return "care_topup_present=True"
    v = _to_float(row.get("leave_short_term_care_pay_value"))
    if v is not None and v > 70:
        return f"short_term_care_pay={v}>70 (above statutory 70% min)"
    v = _to_float(row.get("leave_long_term_care_pay_value"))
    if v is not None and v > 0:
        return f"long_term_care_pay={v}>0 (statutory long-term care is unpaid)"
    return None


def has_real_paternity_above_statutory(row, dt) -> bool:
    """True if the source has paternity values that exceed the era-statutory baseline."""
    base = paternity_baseline(dt)
    if base is None:
        return False
    paid_days_stat, partial_weeks_stat, partial_pct_stat = base
    # 'paid' field — typically days
    v = _to_float(row.get("leave_paid_paternity_value"))
    unit = str(row.get("leave_paid_paternity_unit") or "").lower()
    if v is not None and "day" in unit and v > paid_days_stat:
        return True
    if v is not None and "week" in unit and v > 1:  # > 1 week
        return True
    # 'partially_paid' field — typically weeks at some pct
    v = _to_float(row.get("leave_partially_paid_paternity_value"))
    if v is not None and v > partial_weeks_stat:
        return True
    pct = _to_float(row.get("leave_partially_paid_paternity_pay_value"))
    if pct is not None and pct > partial_pct_stat:
        return True
    # 'unpaid' field — pre-WIEG context only
    if _to_float(row.get("leave_unpaid_paternity_value")):
        return True
    return False


def main() -> None:
    df = pd.read_csv(SRC_PATH, sep=";", dtype=str).fillna("")
    idx = pd.read_csv(INDEX_PATH, sep=";", dtype=str).fillna("")
    in_scope = set(idx["record_id"].astype(str))
    df = df[df["id"].astype(str).isin(in_scope)].copy()
    df.set_index("id", inplace=True)
    print(f"Records in scope: {len(df)}")

    # Apply ALL deterministic flips (L0 statutory_clear + L1 + pattern detectors)
    # so the consistency scan sees the true post-correction state.
    det = pd.read_csv(DET_PATH, sep=";", dtype=str).fillna("")
    for _, r in det.iterrows():
        rid = str(r["record_id"]); fld = r["field"]; new_v = r["csv_value_new"]
        if rid in df.index and fld in df.columns:
            df.at[rid, fld] = new_v
    print(f"Applied {len(det)} deterministic corrections")

    idx_by_rid = {r["record_id"]: r for _, r in idx.iterrows()}

    rows: list[dict] = []
    for rid, rec in df.iterrows():
        meta = idx_by_rid.get(rid)
        ingangsdatum = meta["ingangsdatum"] if meta is not None else ""
        dt = _parse_dt(ingangsdatum)
        era_par = ("pre-2001" if dt and dt < datetime(2001,12,1)
                   else "2001-2009" if dt and dt < PAR_13_TO_26
                   else "2009-2022" if dt and dt < PAR_26_SPLIT
                   else "post-2022" if dt
                   else "unknown")

        # Parental
        exc = rec.get("leave_parental_exceptions", "")
        any_par = _any_filled(rec, PARENTAL_DETAIL_FIELDS, PARENTAL_BOOL_FIELDS)
        if _truthy(exc) and not any_par:
            rows.append({
                "record_id": rid, "ingangsdatum": ingangsdatum, "era": era_par,
                "check": "C1_parental_exc_true_no_details",
                "severity": "info",
                "detail": "exceptions=True but no parental detail field populated (likely uncaptured exception type)",
            })
        if _falsy(exc) and any_par:
            ev = has_real_parental_deviation(rec, dt)
            if ev:
                rows.append({
                    "record_id": rid, "ingangsdatum": ingangsdatum, "era": era_par,
                    "check": "C2_parental_exc_false_with_deviation",
                    "severity": "high",
                    "detail": ev,
                })

        # Care (statutory baseline date-stable)
        exc = rec.get("leave_care_exceptions", "")
        any_care = _any_filled(rec, CARE_DETAIL_FIELDS, CARE_BOOL_FIELDS)
        if _truthy(exc) and not any_care:
            rows.append({
                "record_id": rid, "ingangsdatum": ingangsdatum, "era": era_par,
                "check": "C1_care_exc_true_no_details",
                "severity": "info",
                "detail": "exceptions=True but no care detail field populated (likely uncaptured exception type)",
            })
        if _falsy(exc) and any_care:
            ev = has_real_care_deviation(rec)
            if ev:
                rows.append({
                    "record_id": rid, "ingangsdatum": ingangsdatum, "era": era_par,
                    "check": "C2_care_exc_false_with_deviation",
                    "severity": "high",
                    "detail": ev,
                })

        # Paternity above-statutory flag without supporting value
        if _truthy(rec.get("leave_paternity_explicitly_above_statutory")):
            if not any(_to_float(rec.get(f)) for f in PATERNITY_VALUE_FIELDS):
                rows.append({
                    "record_id": rid, "ingangsdatum": ingangsdatum, "era": era_par,
                    "check": "C3_paternity_above_stat_no_value",
                    "severity": "medium",
                    "detail": "above_statutory=True but no paternity value populated",
                })

        # Liberation day mutual exclusion
        if _truthy(rec.get("leave_liberation_day_annual")) and _truthy(rec.get("leave_liberation_day_lustrum")):
            rows.append({
                "record_id": rid, "ingangsdatum": ingangsdatum, "era": era_par,
                "check": "C4_liberation_day_both_true",
                "severity": "medium",
                "detail": "annual=True AND lustrum=True (logical contradiction)",
            })

    # Write
    cols = ["record_id", "ingangsdatum", "era", "check", "severity", "detail"]
    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") for c in cols])

    print()
    print(f"Total rows: {len(rows)}")
    print(f"Output: {OUT_PATH}")
    print()
    print("By check:")
    print(pd.DataFrame(rows).groupby("check").size().to_string() if rows else "(empty)")


if __name__ == "__main__":
    main()
