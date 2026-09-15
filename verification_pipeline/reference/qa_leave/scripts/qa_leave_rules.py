"""
qa_leave_rules.py — Layer 1 deterministic QA for leave_* fields.

Reads:
  qa_leave/inputs/extracted_data_non_salary.csv

Writes:
  qa_leave/outputs/leave_rule_violations.csv          (long format, one row per violation)
  qa_leave/outputs/leave_rule_violations_summary.csv  (counts per rule_id)

This script runs SCHEMA-INTERNAL consistency checks only. It does NOT compare
against the source CAO text. It catches contradictions inside the extracted
record itself (e.g. min_tenure filled but eligibility_present=false).

The rule list is documented inline. To add or change a rule, edit the RULES
list and re-run.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent  # qa_leave/
CSV_PATH = ROOT / "inputs" / "extracted_data_non_salary.csv"
OUT_DIR = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

VIOLATIONS_PATH = OUT_DIR / "leave_rule_violations.csv"
SUMMARY_PATH = OUT_DIR / "leave_rule_violations_summary.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truthy(val) -> bool:
    """True iff CSV cell parses to a True boolean."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in {"true", "1", "yes", "y"}


def _falsy(val) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in {"false", "0", "no", "n"}


def _is_blank(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    s = str(val).strip()
    return s == "" or s.lower() == "nan" or s.lower() == "none"


def _has_value(val) -> bool:
    return not _is_blank(val)


def _to_float(val):
    if _is_blank(val):
        return None
    try:
        return float(str(val).replace(",", "."))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------
# Each rule is a callable: row -> list[dict] of violations (empty list if OK).
# Each violation dict has keys:
#   rule_id, severity, field_group, message, fields_examined (str), values (str)


RULE_REGISTRY: list = []


def rule(rule_id: str, severity: str = "error"):
    """Decorator to register a rule."""

    def deco(fn):
        fn.rule_id = rule_id
        fn.severity = severity
        RULE_REGISTRY.append(fn)
        return fn

    return deco


def _v(rule_id, severity, field_group, message, fields, values):
    return {
        "rule_id": rule_id,
        "severity": severity,
        "field_group": field_group,
        "message": message,
        "fields_examined": fields,
        "values": values,
    }


# --- Parental ---------------------------------------------------------------


@rule("PAR_01_eligibility_required_for_tenure")
def r_par_01(row):
    """If parental_min_tenure is filled, parental_eligibility_present must be True."""
    if (
        _has_value(row.get("leave_parental_min_tenure_value"))
        and not _truthy(row.get("leave_parental_eligibility_present"))
    ):
        return [
            _v(
                r_par_01.rule_id,
                r_par_01.severity,
                "parental",
                "parental_min_tenure_value is filled but parental_eligibility_present is not True",
                "leave_parental_min_tenure_value, leave_parental_eligibility_present",
                f"tenure={row.get('leave_parental_min_tenure_value')!r}, "
                f"eligibility_present={row.get('leave_parental_eligibility_present')!r}",
            )
        ]
    return []


@rule("PAR_02_eligibility_required_for_contract_length")
def r_par_02(row):
    """If parental_min_contract_length is filled, parental_eligibility_present must be True."""
    if (
        _has_value(row.get("leave_parental_min_contract_length_value"))
        and not _truthy(row.get("leave_parental_eligibility_present"))
    ):
        return [
            _v(
                r_par_02.rule_id,
                r_par_02.severity,
                "parental",
                "parental_min_contract_length_value is filled but parental_eligibility_present is not True",
                "leave_parental_min_contract_length_value, leave_parental_eligibility_present",
                f"contract_length={row.get('leave_parental_min_contract_length_value')!r}, "
                f"eligibility_present={row.get('leave_parental_eligibility_present')!r}",
            )
        ]
    return []


@rule("PAR_03_statutory_ref_no_exceptions_eligibility_must_be_empty")
def r_par_03(row):
    """If parental_statutory_ref=True AND parental_exceptions=False, eligibility sub-fields must be empty."""
    if (
        _truthy(row.get("leave_parental_statutory_ref"))
        and _falsy(row.get("leave_parental_exceptions"))
    ):
        bad = []
        for f in (
            "leave_parental_min_tenure_value",
            "leave_parental_min_tenure_unit",
            "leave_parental_min_contract_length_value",
            "leave_parental_min_contract_length_unit",
        ):
            if _has_value(row.get(f)):
                bad.append(f)
        if bad:
            return [
                _v(
                    r_par_03.rule_id,
                    r_par_03.severity,
                    "parental",
                    "parental_statutory_ref=True and parental_exceptions=False, but eligibility sub-fields are filled",
                    ", ".join(bad),
                    "; ".join(f"{f}={row.get(f)!r}" for f in bad),
                )
            ]
    return []


@rule("PAR_04_topup_pay_requires_topup_present")
def r_par_04(row):
    if (
        _has_value(row.get("leave_parental_topup_pay_value"))
        and not _truthy(row.get("leave_parental_topup_present"))
    ):
        return [
            _v(
                r_par_04.rule_id,
                r_par_04.severity,
                "parental",
                "parental_topup_pay_value is filled but parental_topup_present is not True",
                "leave_parental_topup_pay_value, leave_parental_topup_present",
                f"pay={row.get('leave_parental_topup_pay_value')!r}, present={row.get('leave_parental_topup_present')!r}",
            )
        ]
    return []


# --- Care -------------------------------------------------------------------


@rule("CARE_01_statutory_ref_no_exceptions_with_real_deviation")
def r_care_01(row):
    """Fires only when there is CLEAR EVIDENCE the CAO deviates from the
    statutory baseline (Wabz/WAZO) — not when the CAO just restates it.

    Statutory baseline:
      - short-term care: 2× weekly working hours per 12 months, paid ≥70%
      - long-term care:  6× weekly working hours per 12 months, UNPAID

    Strong deviation signals (any of these):
      a. leave_care_topup_present = True
      b. leave_short_term_care_pay_value > 70 (% — above statutory minimum)
      c. leave_long_term_care_pay_value > 0  (any pay during long-term care
         is a deviation since statutory long-term care is unpaid)
      d. leave_short_term_care_value or leave_long_term_care_value populated
         with a unit that does NOT contain 'week' / 'hour' (i.e. not a
         restatement of the statutory '2×/6× weekly working hours' phrasing)

    A prior version of this rule fired whenever any care detail field was
    non-empty. That over-fired heavily — 40 of 41 cases reviewed by a
    follow-up subagent were just statutory restatements, not real deviations.
    See `outputs/l1_followup/care_01_review.csv` for the audit trail.
    """
    if not (
        _truthy(row.get("leave_care_statutory_ref"))
        and _falsy(row.get("leave_care_exceptions"))
    ):
        return []

    evidence: list[str] = []

    # (a) explicit top-up
    if _truthy(row.get("leave_care_topup_present")):
        evidence.append("leave_care_topup_present=True")

    # (b) short-term pay above statutory 70%
    v = _to_float(row.get("leave_short_term_care_pay_value"))
    if v is not None and v > 70:
        evidence.append(f"leave_short_term_care_pay_value={v} > statutory 70")

    # (c) long-term pay > 0 (statutory long-term care is unpaid)
    v = _to_float(row.get("leave_long_term_care_pay_value"))
    if v is not None and v > 0:
        evidence.append(f"leave_long_term_care_pay_value={v} > statutory 0")

    # (d) duration with non-statutory-shape unit
    for vfld, ufld in (
        ("leave_short_term_care_value", "leave_short_term_care_unit"),
        ("leave_long_term_care_value",  "leave_long_term_care_unit"),
    ):
        if _has_value(row.get(vfld)):
            unit = str(row.get(ufld) or "").lower()
            if unit and not any(tok in unit for tok in ("week", "hour")):
                evidence.append(f"{vfld} has non-statutory unit {unit!r}")

    if not evidence:
        return []

    return [
        _v(
            r_care_01.rule_id,
            r_care_01.severity,
            "care",
            "care_statutory_ref=True and care_exceptions=False, "
            "but evidence shows CAO-specific deviation from statutory baseline",
            "leave_care_topup_present, leave_short_term_care_pay_value, "
            "leave_long_term_care_pay_value, leave_short_term_care_unit, "
            "leave_long_term_care_unit",
            "; ".join(evidence),
        )
    ]


@rule("CARE_02_pay_requires_value")
def r_care_02(row):
    """short/long-term care PAY filled but VALUE empty is suspicious."""
    out = []
    pairs = (
        ("leave_short_term_care_value", "leave_short_term_care_pay_value"),
        ("leave_long_term_care_value", "leave_long_term_care_pay_value"),
    )
    for v_field, p_field in pairs:
        if _has_value(row.get(p_field)) and not _has_value(row.get(v_field)):
            out.append(
                _v(
                    r_care_02.rule_id,
                    "warning",
                    "care",
                    f"{p_field} is filled but corresponding {v_field} is empty",
                    f"{v_field}, {p_field}",
                    f"{v_field}={row.get(v_field)!r}, {p_field}={row.get(p_field)!r}",
                )
            )
    return out


# --- Liberation Day ---------------------------------------------------------


@rule("LIB_01_annual_and_lustrum_mutually_exclusive")
def r_lib_01(row):
    if _truthy(row.get("leave_liberation_day_annual")) and _truthy(
        row.get("leave_liberation_day_lustrum")
    ):
        return [
            _v(
                r_lib_01.rule_id,
                "error",
                "vacation_holidays",
                "Both liberation_day_annual and liberation_day_lustrum are True (mutually exclusive)",
                "leave_liberation_day_annual, leave_liberation_day_lustrum",
                f"annual={row.get('leave_liberation_day_annual')!r}, lustrum={row.get('leave_liberation_day_lustrum')!r}",
            )
        ]
    return []


# --- Maternity / Paternity --------------------------------------------------


@rule("MAT_01_partially_paid_pay_requires_partially_paid_value")
def r_mat_01(row):
    out = []
    pairs = (
        (
            "leave_partially_paid_maternity_value",
            "leave_partially_paid_maternity_pay_value",
            "maternity",
        ),
        (
            "leave_partially_paid_paternity_value",
            "leave_partially_paid_paternity_pay_value",
            "paternity",
        ),
    )
    for v_field, p_field, group in pairs:
        if _has_value(row.get(p_field)) and not _has_value(row.get(v_field)):
            out.append(
                _v(
                    r_mat_01.rule_id,
                    "warning",
                    group,
                    f"{p_field} is filled but {v_field} is empty",
                    f"{v_field}, {p_field}",
                    f"{v_field}={row.get(v_field)!r}, {p_field}={row.get(p_field)!r}",
                )
            )
    return out


@rule("MAT_02_paid_maternity_plausible_range")
def r_mat_02(row):
    """paid_maternity in weeks should typically be between 14 and 30."""
    val = _to_float(row.get("leave_paid_maternity_value"))
    unit = str(row.get("leave_paid_maternity_unit") or "").lower()
    if val is not None and "week" in unit and (val < 10 or val > 40):
        return [
            _v(
                r_mat_02.rule_id,
                "warning",
                "maternity",
                f"paid_maternity={val} weeks is outside the plausible range (10-40 weeks)",
                "leave_paid_maternity_value, leave_paid_maternity_unit",
                f"value={val}, unit={unit!r}",
            )
        ]
    return []


# --- Vacation ---------------------------------------------------------------


@rule("VAC_01_vacation_time_plausible")
def r_vac_01(row):
    """vacation_time plausibility check.

    Recognizes three accrual models:
      - 'days per year' (or just 'days'): typical NL range 15-50.
      - 'hours per year' (or just 'hours'): typical NL range 100-400.
      - 'hours per month' / 'hours per fully worked month' (uitzendkrachten model):
        typical range 8-35, since 25 days/yr × 7.2 hr/day ÷ 12 ≈ 15 hours/month.
    """
    val = _to_float(row.get("leave_vacation_time_value"))
    unit_raw = str(row.get("leave_vacation_time_unit") or "")
    unit = unit_raw.lower()
    if val is None:
        return []
    # Detect uitzendkrachten "per month" model first (uses 'hour' AND 'month')
    if "hour" in unit and "month" in unit:
        if val < 8 or val > 35:
            return [
                _v(
                    r_vac_01.rule_id,
                    "warning",
                    "vacation_holidays",
                    f"vacation_time={val} {unit_raw} is outside plausible per-month accrual range (8-35)",
                    "leave_vacation_time_value, leave_vacation_time_unit",
                    f"value={val}, unit={unit_raw!r}",
                )
            ]
        return []
    # Annual hours
    if "hour" in unit and "year" in unit:
        if val < 100 or val > 400:
            return [
                _v(
                    r_vac_01.rule_id,
                    "warning",
                    "vacation_holidays",
                    f"vacation_time={val} {unit_raw} is outside the plausible range for hours/year (100-400)",
                    "leave_vacation_time_value, leave_vacation_time_unit",
                    f"value={val}, unit={unit_raw!r}",
                )
            ]
        return []
    # Days (assume per year)
    if "day" in unit:
        if val < 15 or val > 50:
            return [
                _v(
                    r_vac_01.rule_id,
                    "warning",
                    "vacation_holidays",
                    f"vacation_time={val} {unit_raw} is outside the plausible range for days/year (15-50)",
                    "leave_vacation_time_value, leave_vacation_time_unit",
                    f"value={val}, unit={unit_raw!r}",
                )
            ]
        return []
    # Bare 'hour' with no horizon — treat as per-year (legacy behavior).
    if "hour" in unit:
        if val < 100 or val > 400:
            return [
                _v(
                    r_vac_01.rule_id,
                    "warning",
                    "vacation_holidays",
                    f"vacation_time={val} {unit_raw} (no horizon stated) is outside the plausible range for hours/year (100-400)",
                    "leave_vacation_time_value, leave_vacation_time_unit",
                    f"value={val}, unit={unit_raw!r}",
                )
            ]
    return []


@rule("VAC_02_vacation_bonus_plausible")
def r_vac_02(row):
    """vacation_bonus is typically 8% of annual salary in NL CAOs."""
    val = _to_float(row.get("leave_vacation_bonus_value"))
    unit = str(row.get("leave_vacation_bonus_unit") or "").lower()
    if val is None:
        return []
    if "%" in unit or "percent" in unit:
        if val < 4 or val > 12:
            return [
                _v(
                    r_vac_02.rule_id,
                    "warning",
                    "vacation_holidays",
                    f"vacation_bonus={val}% is outside the plausible range (4-12%)",
                    "leave_vacation_bonus_value, leave_vacation_bonus_unit",
                    f"value={val}, unit={unit!r}",
                )
            ]
    return []


# --- Sick pay ---------------------------------------------------------------


@rule("SICK_01_sickpay_duration_plausible")
def r_sick_01(row):
    """Dutch sick pay continuation is typically 104 weeks. Flag duration > 200 weeks or extremely short positive values."""
    val = _to_float(row.get("leave_sickpay_duration_value"))
    unit = str(row.get("leave_sickpay_duration_unit") or "").lower()
    if val is None:
        return []
    if "week" in unit and (val < 52 or val > 208):
        return [
            _v(
                r_sick_01.rule_id,
                "warning",
                "sick",
                f"sickpay_duration={val} weeks is outside typical NL range (52-208)",
                "leave_sickpay_duration_value, leave_sickpay_duration_unit",
                f"value={val}, unit={unit!r}",
            )
        ]
    return []


@rule("SICK_02_continuation_pct_plausible")
def r_sick_02(row):
    """Sick-pay continuation rate as a single % should be 50-100."""
    val = _to_float(row.get("leave_sickpay_continuation_value"))
    unit = str(row.get("leave_sickpay_continuation_unit") or "").lower()
    if val is None:
        return []
    if ("%" in unit or "percent" in unit) and (val < 50 or val > 100):
        return [
            _v(
                r_sick_02.rule_id,
                "warning",
                "sick",
                f"sickpay_continuation={val}% is outside plausible range (50-100)",
                "leave_sickpay_continuation_value, leave_sickpay_continuation_unit",
                f"value={val}, unit={unit!r}",
            )
        ]
    return []


# --- Hetero presence --------------------------------------------------------


@rule("HET_01_hetero_false_no_range_expected")
def r_het_01(row):
    """If leave_hetero_present is False, no AmountRange-style range fields should appear within leave.
    (leave schema does not actually have AmountRange leave fields, but extra_seniority_schedule
    being filled with multi-tier text while hetero=False is suspicious.)"""
    # Currently soft check: if hetero=False but extra_seniority_present=True, still allowed (age/tenure based,
    # not group-based hetero). So this rule is intentionally light — kept as placeholder for future.
    return []


# --- Above-statutory flag consistency --------------------------------------


@rule("STAT_01_above_statutory_maternity_requires_value")
def r_stat_01(row):
    """has_above_statutory_maternity=True is more credible if some maternity field is filled."""
    if _truthy(row.get("leave_has_above_statutory_maternity")):
        any_filled = any(
            _has_value(row.get(f))
            for f in (
                "leave_paid_maternity_value",
                "leave_partially_paid_maternity_value",
                "leave_unpaid_maternity_value",
                "leave_maternity_note",
            )
        )
        if not any_filled:
            return [
                _v(
                    r_stat_01.rule_id,
                    "warning",
                    "maternity",
                    "has_above_statutory_maternity=True but no maternity duration/note field is filled",
                    "leave_has_above_statutory_maternity, leave_paid_maternity_value, leave_partially_paid_maternity_value, leave_unpaid_maternity_value, leave_maternity_note",
                    f"flag=True, all maternity values empty",
                )
            ]
    return []


@rule("STAT_02_paternity_above_statutory_requires_value")
def r_stat_02(row):
    if _truthy(row.get("leave_paternity_explicitly_above_statutory")):
        any_filled = any(
            _has_value(row.get(f))
            for f in (
                "leave_paid_paternity_value",
                "leave_partially_paid_paternity_value",
                "leave_unpaid_paternity_value",
            )
        )
        if not any_filled:
            return [
                _v(
                    r_stat_02.rule_id,
                    "warning",
                    "paternity",
                    "paternity_explicitly_above_statutory=True but no paternity duration field is filled",
                    "leave_paternity_explicitly_above_statutory, leave_paid_paternity_value, leave_partially_paid_paternity_value, leave_unpaid_paternity_value",
                    "flag=True, all paternity values empty",
                )
            ]
    return []


# --- Extra seniority --------------------------------------------------------


@rule("SEN_01_schedule_requires_present")
def r_sen_01(row):
    if (
        _has_value(row.get("leave_extra_seniority_schedule"))
        and not _truthy(row.get("leave_extra_seniority_present"))
    ):
        return [
            _v(
                r_sen_01.rule_id,
                "warning",
                "seniority",
                "extra_seniority_schedule is filled but extra_seniority_present is not True",
                "leave_extra_seniority_schedule, leave_extra_seniority_present",
                f"present={row.get('leave_extra_seniority_present')!r}, "
                f"schedule_len={len(str(row.get('leave_extra_seniority_schedule') or ''))}",
            )
        ]
    return []


# --- Sick top-up ------------------------------------------------------------


@rule("SICK_03_topup_present_consistent_with_pay")
def r_sick_03(row):
    """If sick_topup_present=False but sickpay_continuation>70, suspicious (typical statutory minimum is 70%)."""
    if _falsy(row.get("leave_sick_topup_present")):
        val = _to_float(row.get("leave_sickpay_continuation_value"))
        unit = str(row.get("leave_sickpay_continuation_unit") or "").lower()
        if val is not None and ("%" in unit or "percent" in unit) and val > 75:
            return [
                _v(
                    r_sick_03.rule_id,
                    "warning",
                    "sick",
                    f"sick_topup_present=False but sickpay_continuation={val}% (>75% suggests employer top-up exists)",
                    "leave_sick_topup_present, leave_sickpay_continuation_value, leave_sickpay_continuation_unit",
                    f"topup_present=False, continuation={val}%",
                )
            ]
    return []


# --- Field-role consistency (duration fields must have duration units) ------
#
# A field designated to hold a DURATION (weeks/months/years/days/hours) must
# have a unit string that contains one of those tokens. Otherwise it's likely
# carrying a pay-rate, a window, or a non-canonical accrual model and will
# corrupt downstream aggregation.

DURATION_TOKENS = ("week", "month", "year", "day", "hour")

# fields the schema describes as a duration
DURATION_FIELDS = (
    ("leave_paid_maternity_value", "leave_paid_maternity_unit", "maternity"),
    ("leave_partially_paid_maternity_value", "leave_partially_paid_maternity_unit", "maternity"),
    ("leave_unpaid_maternity_value", "leave_unpaid_maternity_unit", "maternity"),
    ("leave_paid_paternity_value", "leave_paid_paternity_unit", "paternity"),
    ("leave_partially_paid_paternity_value", "leave_partially_paid_paternity_unit", "paternity"),
    ("leave_unpaid_paternity_value", "leave_unpaid_paternity_unit", "paternity"),
    ("leave_adoption_value", "leave_adoption_unit", "adoption"),
    ("leave_parental_unpaid_value", "leave_parental_unpaid_unit", "parental"),
    ("leave_sickpay_duration_value", "leave_sickpay_duration_unit", "sick"),
    ("leave_short_term_care_value", "leave_short_term_care_unit", "care"),
    ("leave_long_term_care_value", "leave_long_term_care_unit", "care"),
)


@rule("FIELD_ROLE_01_duration_field_has_non_duration_unit", severity="error")
def r_field_role_01(row):
    """Flag any duration field whose unit string lacks week/month/year/day/hour.

    This catches: pay rates stored in duration fields ('percent of daily wage'),
    accrual models stored in duration fields ('percent of paid hour'),
    and arbitrary non-canonical units.
    """
    out = []
    for v_field, u_field, group in DURATION_FIELDS:
        val = _to_float(row.get(v_field))
        unit_raw = row.get(u_field)
        if val is None:
            continue  # Field not populated, skip
        unit = str(unit_raw or "").lower()
        if not unit:
            # Value present but unit missing — flag separately as low severity
            out.append(_v(
                r_field_role_01.rule_id, "warning", group,
                f"{v_field}={val} is set but {u_field} is empty",
                f"{v_field}, {u_field}",
                f"value={val}, unit=(empty)",
            ))
            continue
        if not any(tok in unit for tok in DURATION_TOKENS):
            # Unit has no duration token — likely a rate or accrual model
            out.append(_v(
                r_field_role_01.rule_id, "error", group,
                f"{v_field}={val} has unit {unit_raw!r} which is not a duration "
                f"(no week/month/year/day/hour token). Likely a pay rate or "
                f"accrual model stored in a duration field.",
                f"{v_field}, {u_field}",
                f"value={val}, unit={unit_raw!r}",
            ))
    return out


# --- Unit allowlist (light) -------------------------------------------------


@rule("UNIT_01_vacation_time_unit_in_allowlist")
def r_unit_01(row):
    val = _to_float(row.get("leave_vacation_time_value"))
    unit = str(row.get("leave_vacation_time_unit") or "").strip().lower()
    if val is None or not unit:
        return []
    ok_tokens = ("day", "hour")
    if not any(tok in unit for tok in ok_tokens):
        return [
            _v(
                r_unit_01.rule_id,
                "warning",
                "vacation_holidays",
                f"vacation_time_unit={unit!r} does not match expected pattern (days/hours per year)",
                "leave_vacation_time_unit",
                f"unit={unit!r}",
            )
        ]
    return []


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run() -> None:
    if not CSV_PATH.exists():
        sys.exit(f"CSV not found at {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, sep=";", dtype=str, low_memory=False)
    print(f"[qa_leave_rules] Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"[qa_leave_rules] {len(RULE_REGISTRY)} rules registered")

    rows_out: list[dict] = []
    for idx, row in df.iterrows():
        record_id = row.get("id") or f"row_{idx}"
        cao_number = row.get("cao_number")
        file_name = row.get("file_name")
        ingangsdatum = row.get("ingangsdatum")
        for fn in RULE_REGISTRY:
            try:
                vios = fn(row)
            except Exception as exc:
                vios = [
                    {
                        "rule_id": getattr(fn, "rule_id", fn.__name__),
                        "severity": "error",
                        "field_group": "_meta",
                        "message": f"Rule raised exception: {exc!r}",
                        "fields_examined": "",
                        "values": "",
                    }
                ]
            for v in vios:
                rows_out.append(
                    {
                        "record_id": record_id,
                        "cao_number": cao_number,
                        "file_name": file_name,
                        "ingangsdatum": ingangsdatum,
                        **v,
                    }
                )

    out_df = pd.DataFrame(rows_out)
    if out_df.empty:
        print("[qa_leave_rules] No violations found.")
        out_df = pd.DataFrame(
            columns=[
                "record_id",
                "cao_number",
                "file_name",
                "ingangsdatum",
                "rule_id",
                "severity",
                "field_group",
                "message",
                "fields_examined",
                "values",
            ]
        )
    out_df.to_csv(VIOLATIONS_PATH, sep=";", index=False)
    print(f"[qa_leave_rules] Wrote {len(out_df)} violation rows to {VIOLATIONS_PATH}")

    # Summary
    if not out_df.empty:
        summary = (
            out_df.groupby(["rule_id", "severity", "field_group"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["severity", "count"], ascending=[True, False])
        )
    else:
        summary = pd.DataFrame(
            columns=["rule_id", "severity", "field_group", "count"]
        )
    summary.to_csv(SUMMARY_PATH, sep=";", index=False)
    print(f"[qa_leave_rules] Wrote summary ({len(summary)} rule groups) to {SUMMARY_PATH}")


if __name__ == "__main__":
    run()
