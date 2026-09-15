"""Shared field metadata + helpers for the full-dataset audit.

Single source of truth for:
  - loading the dataset (semicolon-delimited, READ-ONLY)
  - classifying all 317 columns (boolean / numeric / unit / enum / freetext /
    date / list / meta / id) and tying each value column to its unit partner
  - enum allowed-value sets (from inputs/NON_SALARY_PROMPTS_AND_SCHEMA.md)
  - number / date parsing tuned to this dataset's observed formats
  - the in-scope (95 curated CAOs) vs out-of-scope (147) split
  - unit-category classification (percent / money / hours / weeks / ...)

Empirically grounded (see the audit task's orientation step):
  - value cells are clean `.`-decimal floats (no commas/currency/alpha)
  - unit cells are free-form strings (3,779 distinct tokens) — NEVER numeric
  - booleans are exactly "True"/"False"/"" ; enums hold off-vocabulary values
  - Dutch meta dates are day-first DD/MM/YYYY or DD-MM-YYYY; general_*_date ISO
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field as _dc_field
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATASET_PATH = PROJECT_ROOT / "inputs" / "extracted_data_non_salary.csv"
SCHEMA_PATH = PROJECT_ROOT / "inputs" / "NON_SALARY_PROMPTS_AND_SCHEMA.md"

# 13 topic sections that have a holistic source section + the general block.
TOPICS = ["leave", "overtime", "homeoffice", "training", "contract", "bonus",
          "fringe", "safety", "childcare", "ai", "term", "pension", "wage"]

# CSV column prefix per topic (matches schema_lookup._TOPIC_TO_CSV_PREFIX).
TOPIC_CSV_PREFIX = {
    "leave": "leave_", "overtime": "overtime_", "homeoffice": "homeoffice_",
    "training": "training_", "contract": "contract_", "bonus": "bonus_",
    "fringe": "fringe_", "safety": "safety_", "childcare": "childcare_",
    "ai": "ai_", "term": "term_", "pension": "pension_", "wage": "wage_",
    "general": "general_",
}

# schema_prefix -> our topic key
SCHEMA_PREFIX_TO_TOPIC = {
    "leave_information": "leave", "overtime_information": "overtime",
    "homeoffice_information": "homeoffice", "training_information": "training",
    "contract_type_information": "contract", "bonuses_info": "bonus",
    "fringe_benefits_information": "fringe", "safety_information": "safety",
    "childcare_information": "childcare", "ai_information": "ai",
    "termination_information": "term", "pension_information": "pension",
    "wage_scales_info": "wage", "general_information": "general",
}

ID_COL = "id"
# Non-topic metadata (live outside general_*).
META_COLS = {"cao_number", "id", "TTW", "ingangsdatum", "expiratiedatum",
             "datum_kennisgeving", "file_name"}
# Date-typed columns (validated by the format check, not enum/numeric).
DATE_COLS = {"ingangsdatum", "expiratiedatum", "datum_kennisgeving",
             "general_start_date", "general_expiry_date", "general_signing_date",
             "general_chg_eff_date", "general_retro_start_date",
             "general_retro_end_date", "general_avv_start_date",
             "general_avv_end_date"}
# list[str] columns (comma/semicolon joined in the CSV).
LIST_COLS = {"general_updated_topics"}

# Enum allowed-value sets, keyed by REAL CSV column name, from the schema doc.
ENUM_ALLOWED: dict[str, set[str]] = {
    "general_document_type": {"full_cao_original", "full_cao_update",
        "partial_amendment_of_original", "partial_amendment_of_latest",
        "annex", "protocol", "other_supplement", "unspecified"},
    "general_cao_scope_type": {"sectoral", "single_company", "group",
        "association_limited", "occupational_niche", "unspecified", "other"},
    "pension_pension_type": {"DB", "DC", "hybrid", "unspecified", "other"},
    "pension_selection_rule_pension": {"majority_headcount",
        "office_vs_field_rule", "base_tier", "latest_year", "other",
        "unspecified"},
    "term_selection_rule_notice": {"majority_headcount", "base_tier",
        "office_vs_field_rule", "latest_year", "unspecified", "other"},
    "term_dismissal_approval": {"UWV", "Judge", "Both", "None", "Conditional",
        "unspecified", "other"},
    "overtime_selection_rule": {"majority_headcount", "base_tier",
        "office_vs_field_rule", "latest_year", "unspecified", "other"},
    "overtime_compensation_mode": {"monetary_pay", "TOIL", "both",
        "unspecified", "other"},
    "overtime_stacking_rule": {"highest_only", "cumulative", "unclear",
        "unspecified", "other"},
    "homeoffice_discretion": {"employer_only", "joint_with_OR",
        "employee_request", "unspecified", "other"},
    "fringe_meal_benefit_type": {"free_meals", "subsidised_canteen",
        "meal_vouchers", "meal_allowance", "other", "unspecified"},
    "childcare_provider_scope": {"any", "contracted_only", "sector_only",
        "company_only", "unspecified", "other"},
    "childcare_public_coord": {"top_up_after_public_benefit", "within_fiscal_max",
        "gross_before_public_benefit", "unspecified", "other"},
    "ai_ai_automated_decisions": {"never", "with_human_review", "unspecified",
        "other"},
}

_BLANK = {"", "nan", "none", "null", "n/a", "unknown", "na"}


def is_blank(v) -> bool:
    return str(v).strip().lower() in _BLANK


def norm(s) -> str:
    return str(s).strip()


# ── number parsing ───────────────────────────────────────────────────────────
def to_float(s) -> Optional[float]:
    """Strict numeric parse for value cells (observed: clean `.`-decimal).
    Returns None for blank or any non-numeric content."""
    if s is None:
        return None
    t = str(s).strip()
    if t.lower() in _BLANK:
        return None
    try:
        return float(t)
    except ValueError:
        return None


_LEADING_NUM_RE = re.compile(r"[-+]?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?|[-+]?\d*[.,]?\d+")


def extract_leading_number(s) -> Optional[float]:
    """Lenient: pull the first numeric token out of a possibly-contaminated
    string (e.g. '€1.500 per maand' -> 1500.0, '12,5%' -> 12.5). Used by the
    contamination check, NOT by the strict outlier stats."""
    if s is None:
        return None
    t = str(s).strip()
    m = _LEADING_NUM_RE.search(t)
    if not m:
        return None
    tok = m.group(0)
    # disambiguate decimal vs thousands separators
    if "." in tok and "," in tok:
        if tok.rfind(",") > tok.rfind("."):      # 1.500,50 -> comma decimal
            tok = tok.replace(".", "").replace(",", ".")
        else:                                     # 1,500.50 -> dot decimal
            tok = tok.replace(",", "")
    elif "," in tok:
        # only commas: 3-digit groups => thousands, else decimal comma
        if re.fullmatch(r"[-+]?\d{1,3}(?:,\d{3})+", tok):
            tok = tok.replace(",", "")
        else:
            tok = tok.replace(",", ".")
    try:
        return float(tok)
    except ValueError:
        return None


# ── date parsing (day-first Dutch + ISO) ─────────────────────────────────────
def parse_date(s) -> Optional[date]:
    t = (str(s) or "").strip()[:10]
    if t.lower() in _BLANK:
        return None
    # ISO if it starts with a 4-digit year; else day-first.
    fmts = (("%Y-%m-%d", "%Y/%m/%d") if re.match(r"\d{4}[-/]", t)
            else ("%d-%m-%Y", "%d/%m/%Y"))
    for fmt in fmts + ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    return None


def looks_like_date_token(s) -> bool:
    return parse_date(s) is not None


# ── unit-category classification ─────────────────────────────────────────────
def unit_category(unit: str) -> str:
    """Coarse category of a unit string (Dutch + English). 'other' if unknown,
    '' if blank."""
    u = str(unit).strip().lower()
    if not u or u in _BLANK:
        return ""
    if "%" in u or "percent" in u or "procent" in u:
        return "percent"
    if "eur" in u or "€" in u or "euro" in u or "geld" in u:
        return "money"
    if "fte" in u:
        return "fte"
    if "km" in u or "kilometer" in u:
        return "distance"
    if "hour" in u or "uur" in u or "uren" in u:
        return "hours"
    if "week" in u or "weken" in u or "wekelijk" in u:
        return "weeks"
    if "maand" in u or re.search(r"\bmonth", u):
        return "months"
    if "year" in u or "jaar" in u or "jaren" in u or "annual" in u:
        return "years"
    if "day" in u or "dag" in u:
        return "days"
    if "contract" in u or "employee" in u or "werknemer" in u or "times" in u \
            or "keer" in u or "maal" in u or "step" in u or "trede" in u:
        return "count"
    return "other"


# Period qualifier extraction. Two units with the same coarse dimension but a
# different time base (e.g. "hours per week" 38 vs "hours per year" 1900) are NOT
# comparable — pooling them is the dominant source of false outliers. Precedence
# matters: longer/more-specific spans first ("four weeks" before "week",
# "year"/"annual" before "week" so "... per week on an annual basis" reads year).
_PERIOD_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("4wk",    ("four weeks", "four-week", "4 weeks", "per 4 weeks", "vier weken",
                "four week")),
    ("year",   ("year", "annual", "annually", "per annum", "jaar", "jaren",
                "jaarbasis")),
    ("month",  ("month", "maand")),
    ("week",   ("week", "weken", "weekly", "wekelijk", "workweek")),
    ("shift",  ("shift", "dienst", "ploeg")),
    ("period", ("pay period", "pay-period", "per period", "periode")),
    ("day",    ("day", "daily", "dag")),
    ("hour",   ("per hour", "hourly", "per uur")),
]


def unit_period(unit: str) -> str:
    """Extract the time-base qualifier from a unit string ('' if none)."""
    u = str(unit).strip().lower()
    if not u or u in _BLANK:
        return ""
    for period, needles in _PERIOD_PATTERNS:
        if any(nd in u for nd in needles):
            return period
    return ""


def unit_signature(unit: str) -> str:
    """Fine-grained like-with-like grouping key: coarse dimension + time base.

    'hours per week' -> 'hours/week', 'hours per year' -> 'hours/year',
    'days' -> 'days', 'EUR per month' -> 'money/month', '' -> '∅'.
    Unlike unit_category (used by the contamination check), this keeps the period
    so weekly/annual/monthly cohorts of the same field don't pool together.
    """
    dim = unit_category(unit)
    if dim == "":
        return "∅"
    per = unit_period(unit)
    return f"{dim}/{per}" if per else dim


# A few coarse unit tokens we recognize as units when found embedded in a value
# (Dutch + English). Used by the contamination check.
UNIT_WORDS = [
    "per month", "per maand", "per week", "per year", "per jaar", "per hour",
    "per uur", "per day", "per dag", "per km", "per kilometer", "percent",
    "procent", "months", "maanden", "weeks", "weken", "hours", "uren", "uur",
    "days", "dagen", "years", "jaren", "jaar", "eur", "euro", "fte",
    "monthly wage", "hourly", "annual salary", "of salary", "of wage",
    "one-off", "eenmalig",
]
UNIT_SYMBOLS = ["€", "%"]


# ── schema type parsing ──────────────────────────────────────────────────────
_SCHEMA_ENTRY_RE = re.compile(r"^`([A-Za-z_]+)\.([A-Za-z0-9_]+)`\s*\|\s*`([^`]+)`",
                              re.MULTILINE)


@lru_cache(maxsize=1)
def _schema_types() -> dict[tuple[str, str], str]:
    """Return {(topic, base_field): type} parsed from the schema doc.
    type ∈ {bool, str, Amount, AmountRange, float, list[str], int}."""
    out: dict[tuple[str, str], str] = {}
    if not SCHEMA_PATH.exists():
        return out
    for m in _SCHEMA_ENTRY_RE.finditer(SCHEMA_PATH.read_text(encoding="utf-8")):
        schema_prefix, base, typ = m.group(1), m.group(2), m.group(3).strip()
        topic = SCHEMA_PREFIX_TO_TOPIC.get(schema_prefix)
        if topic:
            out[(topic, base)] = typ
    return out


# ── column classification ────────────────────────────────────────────────────
@dataclass
class ColumnInfo:
    name: str
    topic: str                 # one of TOPICS, "general", or "meta"
    kind: str                  # boolean|numeric|unit|enum|freetext|date|list|meta|id
    base_field: str            # schema base field (groups value+unit+range)
    group_key: str             # f"{topic}.{base_field}" — ties value↔unit
    schema_type: Optional[str] = None
    unit_partner: Optional[str] = None        # for value/min/max cols
    value_partners: list = _dc_field(default_factory=list)  # for unit cols
    enum_values: Optional[set] = None
    role: str = ""             # value|min|max|unit|scalar (within its group)


def _topic_of(col: str) -> tuple[str, str]:
    """Return (topic, csv_prefix) for a column, or ('meta','') for metadata."""
    if col in META_COLS:
        return ("meta", "")
    for topic, pref in TOPIC_CSV_PREFIX.items():
        if col.startswith(pref):
            return (topic, pref)
    return ("meta", "")


@lru_cache(maxsize=1)
def classify_columns() -> dict[str, ColumnInfo]:
    """Classify every dataset column. Cached."""
    cols = dataset_columns()
    types = _schema_types()
    info: dict[str, ColumnInfo] = {}

    for col in cols:
        topic, pref = _topic_of(col)

        if col == ID_COL:
            info[col] = ColumnInfo(col, "meta", "id", col, "meta.id", role="scalar")
            continue
        if col in DATE_COLS:
            base = col[len(pref):] if pref else col
            info[col] = ColumnInfo(col, topic, "date", base,
                                   f"{topic}.{base}", "str", role="scalar")
            continue
        if col in LIST_COLS:
            base = col[len(pref):] if pref else col
            info[col] = ColumnInfo(col, topic, "list", base,
                                   f"{topic}.{base}", "list", role="scalar")
            continue
        if topic == "meta":
            info[col] = ColumnInfo(col, "meta", "meta", col, f"meta.{col}",
                                   role="scalar")
            continue

        rest = col[len(pref):]
        # determine role/base from suffix. Keep a trailing "_range" in the base
        # so an Amount field (e.g. employee_contrib) and its sibling AmountRange
        # (employee_contrib_range) stay in SEPARATE groups, each with one unit.
        if rest.endswith("_unit"):
            base, role, kind = rest[:-len("_unit")], "unit", "unit"
        elif rest.endswith("_min"):
            base, role, kind = rest[:-len("_min")], "min", "numeric"
        elif rest.endswith("_max"):
            base, role, kind = rest[:-len("_max")], "max", "numeric"
        elif rest.endswith("_value"):
            base, role, kind = rest[:-len("_value")], "value", "numeric"
        else:
            base, role = rest, "scalar"
            st = types.get((topic, base), "")
            if col in ENUM_ALLOWED:
                kind = "enum"
            elif st == "bool":
                kind = "boolean"
            elif st == "float" or st == "int":
                kind = "numeric"
            elif st in ("Amount", "AmountRange"):
                # shouldn't happen for a no-suffix col, but guard
                kind = "numeric"
            else:
                kind = "freetext"

        gk = f"{topic}.{base}"
        st = types.get((topic, base))
        info[col] = ColumnInfo(col, topic, kind, base, gk, st, role=role,
                               enum_values=ENUM_ALLOWED.get(col))

    # wire value↔unit partners within each group
    by_group: dict[str, list[ColumnInfo]] = {}
    for ci in info.values():
        by_group.setdefault(ci.group_key, []).append(ci)
    for grp, members in by_group.items():
        units = [m for m in members if m.kind == "unit"]
        values = [m for m in members if m.kind == "numeric"]
        if units and values:
            u = units[0]
            for v in values:
                v.unit_partner = u.name
            u.value_partners = [v.name for v in values]
    return info


# ── dataset access ───────────────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _load_df():
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from qa.shared import resilient_csv
    return resilient_csv.read_csv(DATASET_PATH, delimiter=";")


def load_dataset():
    """Return the full dataset as a pandas DataFrame (str dtype, READ-ONLY)."""
    return _load_df().copy()


@lru_cache(maxsize=1)
def dataset_columns() -> tuple:
    return tuple(_load_df().columns)


# ── in-scope CAO set (95 curated) ────────────────────────────────────────────
@lru_cache(maxsize=1)
def inscope_caos() -> frozenset:
    """The 95 curated CAOs that have by_topic source blocks (in-scope set)."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    from qa.shared import source_text_loader
    blocks = source_text_loader._load_blocks("overtime")
    return frozenset(cao for (cao, _fn) in blocks.keys())


# ── helpers for checks ───────────────────────────────────────────────────────
def numeric_columns() -> list[str]:
    return [c for c, ci in classify_columns().items() if ci.kind == "numeric"]


def unit_columns() -> list[str]:
    return [c for c, ci in classify_columns().items() if ci.kind == "unit"]


def enum_columns() -> list[str]:
    return [c for c, ci in classify_columns().items() if ci.kind == "enum"]


def boolean_columns() -> list[str]:
    return [c for c, ci in classify_columns().items() if ci.kind == "boolean"]


def value_unit_pairs() -> list[tuple[str, str]]:
    """List of (value_or_range_col, unit_col) pairs."""
    out = []
    for c, ci in classify_columns().items():
        if ci.kind == "numeric" and ci.unit_partner:
            out.append((c, ci.unit_partner))
    return out


def field_topic(col: str) -> str:
    return classify_columns()[col].topic


# ── canonical flag emission (shared across all four checks) ──────────────────
# Internal per-check CSV schema. The combiner maps these into the final
# full_audit_flags.csv schema (adding verify_* + suggested_review).
FLAG_COLUMNS = ["record_id", "cao_number", "file_name", "ingangsdatum", "topic",
                "field", "value", "unit", "check", "subcheck", "severity",
                "reason", "stat_context"]

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}


def unit_for(rec: dict, field: str) -> str:
    """The unit cell paired with a value/min/max field, '' if none."""
    ci = classify_columns().get(field)
    if ci and ci.unit_partner:
        return norm(rec.get(ci.unit_partner, ""))
    return ""


def make_flag(rec: dict, field: str, check: str, severity: str, reason: str,
              stat_context: str = "", subcheck: str = "",
              value=None, unit=None) -> dict:
    """Build one canonical flag row from a dataset record dict."""
    ci = classify_columns().get(field)
    topic = ci.topic if ci else "meta"
    if value is None:
        value = norm(rec.get(field, ""))
    if unit is None:
        unit = unit_for(rec, field)
    return {
        "record_id": norm(rec.get("id", "")),
        "cao_number": norm(rec.get("cao_number", "")),
        "file_name": norm(rec.get("file_name", "")),
        "ingangsdatum": norm(rec.get("ingangsdatum", "")),
        "topic": topic,
        "field": field,
        "value": value,
        "unit": unit,
        "check": check,
        "subcheck": subcheck,
        "severity": severity,
        "reason": reason,
        "stat_context": stat_context,
    }


def era_band(d: Optional[date]) -> str:
    """Coarse era band for per-(field×era) distributions."""
    if d is None:
        return "unknown"
    y = d.year
    if y < 2010:
        return "pre2010"
    if y < 2015:
        return "2010_2014"
    if y < 2020:
        return "2015_2019"
    return "2020plus"


if __name__ == "__main__":
    # Self-check / inventory dump.
    import collections
    cols = dataset_columns()
    info = classify_columns()
    print(f"dataset: {len(_load_df())} records x {len(cols)} columns")
    print(f"in-scope CAOs: {len(inscope_caos())}")
    kinds = collections.Counter(ci.kind for ci in info.values())
    print("kind counts:", dict(kinds))
    # unclassified / sanity
    types = _schema_types()
    print(f"schema leaf types parsed: {len(types)}")
    npairs = len(value_unit_pairs())
    print(f"value↔unit pairs: {npairs}")
    print(f"enum cols: {len(enum_columns())}  numeric: {len(numeric_columns())} "
          f" unit: {len(unit_columns())}  bool: {len(boolean_columns())}")
    # cols whose group has a unit but no value partner (orphan units) or v.v.
    for c in unit_columns():
        if not info[c].value_partners:
            print("  ORPHAN unit (no value partner):", c)
    # freetext / scalar enum cols not in ENUM_ALLOWED but maybe enum-ish
    freetext = [c for c, ci in info.items() if ci.kind == "freetext"]
    print(f"freetext cols: {len(freetext)}")
    print("sample freetext:", freetext[:12])
