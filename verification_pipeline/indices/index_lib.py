"""
Shared library for the generosity/protection indices — v2 (2026-07-05).

v2 construction (INDICES_V2_PLAN.md; v1 archived in _old_v1/):
  - TIME AXIS = file_date = datum_kennisgeving (the document's edition/publication
    date; 99.96% populated; fallback ingangsdatum, flagged). Ordering, forward-fill,
    "newest doc" and the monthly panel all run on file_date, so mid-term
    republications (updates) enter the timeline when they were published.
  - ONE POOLED Z PER FILE per topic: each field's cardinal "full" value is
    standardised once against ALL full-CAO documents of all years pooled
    (winsorised 1/99, z clipped ±3) — levels stay comparable across time.
    No within-vintage-year / within-calendar-year / within-active-set variants.
  - YARDSTICK DE-DUP: the pooled winsor bounds and mu/sd are computed on ONE doc
    per term_group (cao_number+ingangsdatum; latest kennisgeving edition), then
    ALL docs are scored against those parameters — reprint-happy CAOs don't
    distort the yardstick, but every edition still gets its score.
  - Per-field pooled parameters are persisted to scoring_params.csv so the
    statutory pseudo-file (statutory_index.py) and any re-score use the same
    yardstick.
  - Unit normalisation, plausibility clamps, era-aware statutory caps/floors/
    defaults ("full" cardinal variant) and available-case aggregation carry over
    from v1 unchanged.

FUSE/iCloud note: this repo is on a mount where pandas' high-level readers can
deadlock and du/ls report 0 bytes; we read via a raw os.read loop.
"""
from __future__ import annotations
import os, re, math
from io import BytesIO
from datetime import date, datetime
import pandas as pd
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)  # indices/ is a top-level dir

# ── subfolder layout (2026-07-15 reorg: indices/ root keeps only *.py + *.md + rebuild.sh +
#    all_indices.xlsx; data files live in one of these three) ─────────────────────────────
OUT = os.path.join(HERE, "out")            # pipeline-REGENERATED outputs (index csvs, panels, factor analysis...)
CORR = os.path.join(HERE, "corrections")   # audit trail: every applied correction-campaign log
REV = os.path.join(HERE, "review")         # review queues & registries (hand/agent-curated, NOT regenerated)
for _d in (OUT, CORR, REV):
    os.makedirs(_d, exist_ok=True)


def locate(name):
    """Find `name` among HERE/out/corrections/review (in that order) for READS. Falls back to
    HERE/name (even if it doesn't exist yet) so a not-yet-created output still resolves to a
    sensible path. Writers should target the explicit OUT/CORR/REV constant instead."""
    for base in (HERE, OUT, CORR, REV):
        p = os.path.join(base, name)
        if os.path.exists(p):
            return p
    return os.path.join(HERE, name)


CORRECTED_CSV = os.path.join(PROJECT_ROOT, "qa", "corrected_dataset.csv")
STATUTORY_CSV = os.path.join(OUT, "statutory_all.csv")   # built by statutory_sync.py (source: review/statutory_timeline.xlsx)
WML_CSV = os.path.join(OUT, "wml_timeline.csv")          # built by statutory_sync.py (source: review/statutory_timeline.xlsx)
SCORING_PARAMS_CSV = os.path.join(OUT, "scoring_params.csv")

# ── FUSE-safe IO ──────────────────────────────────────────────────────────
def read_csv_safe(path, sep=";"):
    fd = os.open(path, os.O_RDONLY); chunks = []
    while True:
        c = os.read(fd, 1 << 20)
        if not c: break
        chunks.append(c)
    os.close(fd)
    return pd.read_csv(BytesIO(b"".join(chunks)), sep=sep, low_memory=False, dtype=str).fillna("")

# ── parsing ───────────────────────────────────────────────────────────────
def parse_float(x):
    if x is None: return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"): return None
    m = re.match(r"[-+]?\d*\.?\d+", s.replace(",", "."))
    return float(m.group()) if m else None

def parse_date(s):
    s = (s or "").strip().replace("/", "-")
    if not s: return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).date()
        except ValueError: continue
    return None

def year_of(s):
    d = parse_date(s)
    return d.year if d else None

# ── unit normalisation (unchanged from v1) ────────────────────────────────
WEEK, DAY, MONTH, YEAR = 1.0, 0.2, 13.0/3.0, 52.0
_PCT = re.compile(r"%|percent", re.I)
_EUR = re.compile(r"\beur|euro|€", re.I)

def to_months(v, u):
    """months canonical (notice, probation, ketenregeling duration). HEAD-TOKEN rule
    (2026-07-09 normalizer audit — the flat branch order let LATE tokens win:
    '3 years minus one day' hit the day branch -> 0.099 months, and
    '26 weeks (statutory 1 month per Art. 7:672 BW)' hit the month branch -> 26 months).
    The first duration token decides; digit-week qualifiers ('4-week pay periods') are
    stripped first. period = wage-payment-period = 1 calendar month (resolved earlier)."""
    if v is None: return None
    lo = (u or "").strip().lower()
    if not lo: return None
    lo2 = re.sub(r"\d+(?:[.,]\d+)?\s*-\s*week", " ", lo)   # '4-week pay periods' -> qualifier
    if not lo2.strip(): lo2 = lo
    positions = []
    for name, pat in [("month", r"\bmonths?\b|maand"), ("period", r"period"),
                      ("workday", r"work(?:ing)?[_\s]*day"), ("day", r"calendar[_\s]*day|\bdays?\b|dagen"),
                      ("week", r"\bweeks?\b|weken"), ("year", r"\byears?\b|jaar|jaren"),
                      ("pct", r"%|percent")]:
        m = re.search(pat, lo2)
        if m: positions.append((m.start(), name))
    if not positions: return None
    head = min(positions)[1]
    if head == "pct": return None
    if head in ("month", "period"): return v
    if head == "workday": return v * (1.0/21.7)
    if head == "day": return v * (1.0/30.4)
    if head == "week": return v * (12.0/52.0)
    if head == "year": return v * 12.0
    return None

def to_hours(v, u):
    if v is None: return None
    lo = (u or "").strip().lower()
    if not lo: return None
    if re.search(r"minute|minuten", lo): return v / 60.0
    if re.search(r"\bhours?\b|\buur\b|uren", lo): return v
    return None  # 'days' in an hours field, % contamination, etc. -> drop

def to_percent(v, u):
    if v is None: return None
    lo = (u or "").strip().lower()
    if _PCT.search(lo): return v           # % present wins — units like '8% of annual income,
                                           # minimum €1,410/yr' are real percentages with a EUR
                                           # FLOOR mentioned (2026-07-09 normalizer audit)
    if _EUR.search(lo): return None        # EUR absolute (no %), not a %
    if lo == "": return v                  # bare number in a %-field treated as %
    return None

def to_count(v, u):
    return v  # contracts / employees: the value itself is the count

_QUALIFIER_RE = re.compile(  # '36-hour week' / '(full-time, 36 hours/week)' / 'for 36 hours per week'
    r"\(?\s*(?:for\s+)?(?:full[- ]?time,?\s*)?\d+(?:[.,]\d+)?\s*-?\s*(?:hours?|uur)\s*"
    r"(?:per\s*|/\s*|\s+)?(?:work\s*)?(?:week|workweek)\s*\)?", re.I)

def _head_token(lo):
    """First-occurring unit class in the string — the unit's HEAD is what the value IS;
    later tokens are qualifiers ('weeks at 100% pay' head=week; '% of salary for 52 weeks'
    head=pct). 2026-07-09 normalizer audit. Returns (class, position) or (None, -1)."""
    _MULT = r"(?:\btimes?\b|\bx\b)[\s\w]{0,40}?(?:week|work(?:ing)?[\s_]*hours)"
    classes = [
        ("mult",  _MULT),                              # 'N times/x (avg) weekly …' / 'times working hours'
        ("wkh",   r"(?:number[\s_]*of[\s_]*)?(?:agreed[\s_]*|average[\s_]*|contractual[\s_]*)?"
                  r"work(?:ing)?[\s_]*hours[\s\w]{0,20}per[\s\w]{0,12}week"),   # 'working hours per week' = 1 workweek
        ("pct",   r"%|percent"),
        ("hour",  r"hour|uur|uren"),
        ("day",   r"(?<!holi)day|(?<!holi)dag|shift|dienst"),   # substring: workdays/werkdag; guard 'holiday'
        ("week",  r"workweeks?|\bweeks?\b|weekly|weken"),
        ("month", r"\bmonths?\b|maand"),
        ("year",  r"\byears?\b|jaar|jaren"),
    ]
    best = (None, 10**9)
    for name, pat in classes:
        m = re.search(pat, lo)
        if m and m.start() < best[1]:
            best = (name, m.start())
    # the multiplier idiom wins whenever it starts at/before the token it contains
    m = re.search(_MULT, lo)
    if m and m.start() <= best[1]:
        return ("mult", m.start())
    return best if best[0] else (None, -1)

def to_days(v, u):
    """days/year canonical (training time, vacation, short care). HEAD-TOKEN rule
    (2026-07-09 normalizer audit): digit-qualifiers like '36-hour week' are stripped first,
    then the first duration/percent token decides the class. mult ('N times/x ... week[ly]')
    and week-head ('week of working hours', 'weekly working hours') = N weeks -> x5 working
    days; hour-head -> /8 annual hours ('hours per week' -> x52/8 weekly rate); pct-head ->
    None (fraction of worktime, unconvertible — used to fabricate day counts); day-head -> x1
    ('per week/month' frequencies -> x52/x12, unit conversion of a stated frequency);
    month-head -> x21.7."""
    if v is None: return None
    lo = _QUALIFIER_RE.sub(" ", (u or "").strip().lower())
    if not lo.strip(): lo = (u or "").strip().lower()   # unit WAS only the qualifier -> keep raw
    head, _ = _head_token(lo)
    if head in ("mult", "week", "wkh"): return v * 5.0
    if head == "pct": return None
    if head == "hour":
        if re.search(r"(?:per|/|a)\s*(?:work\s*)?week", lo): return v * 52.0 / 8.0
        return v / 8.0
    if head == "day":
        if re.search(r"per\s*week|/\s*week", lo): return v * 52.0
        if re.search(r"per\s*month|per\s*maand", lo): return v * 12.0
        return v
    if head == "month": return v * 21.7
    return None

def to_weeks(v, u):
    """weeks canonical (sick-pay duration, long-term care). HEAD-TOKEN rule (2026-07-09
    normalizer audit — see to_days): mult/week-head = N weeks -> v (covers 'weekly working
    hour(s)/duration/period/time', 'weekly hours equivalent', 'x weekly working time',
    'weeks at 100% pay' — the pay-% is a qualifier, the value IS weeks); pct-head -> None
    ('100% of monthly salary for 52 weeks' is a pay rate stuffed in a weeks field — it used
    to return 100 weeks); day/shift-head /5; month-head x13/3; year-head x52; hour-head ->
    None (bare hours lack a weekly context)."""
    if v is None: return None
    lo = _QUALIFIER_RE.sub(" ", (u or "").strip().lower())
    if not lo.strip(): lo = (u or "").strip().lower()
    head, _ = _head_token(lo)
    if head in ("mult", "week", "wkh"): return v
    if head == "pct": return None
    if head == "month": return v * (13.0 / 3.0)
    if head == "year": return v * 52.0
    if head == "day": return v / 5.0
    return None

def to_eur(v, u):
    if v is None: return None
    lo = (u or "").strip().lower()
    if _PCT.search(lo): return None
    if _EUR.search(lo) or lo == "": return v
    return None

def to_eur_per_month(v, u):
    if v is None: return None
    lo = (u or "").strip().lower()
    if not _EUR.search(lo): return None
    if re.search(r"per\s*month|per\s*maand", lo): return v
    # 'per (home) work(ing/ed) day' variants included — same 21.7 working-days/month factor
    if re.search(r"per\s*(full\s*)?(home\s*)?(work(ing|ed)?\s*)?day|per\s*(werk)?dag", lo): return v * 21.7
    if re.search(r"per\s*week", lo): return v * 4.33
    if re.search(r"per\s*year|per\s*jaar|annual", lo): return v / 12.0
    # "eur over N (calendar) years/period" — a fixed lump amortised over N*12 months
    # (salvage 2026-07-07: the only clean unit-conversion win the audit found; ~13 docs).
    m = re.search(r"over\s*(\d+)[\s-]*(?:calendar[\s-]*)?years?", lo)
    if m and int(m.group(1)) > 0: return v / (int(m.group(1)) * 12.0)
    return None  # bare EUR ambiguous for a stipend

def to_eur_per_km(v, u):
    if v is None: return None
    lo = (u or "").strip().lower()
    if _EUR.search(lo) and re.search(r"km|kilomet", lo): return v
    return None

def to_pct_annual(v, u):
    """13th-month value as % of ANNUAL salary. Canonical encodings (schema
    NON_SALARY_PROMPTS_AND_SCHEMA.md): a COUNT of months (unit 'monthly wage', v=1 -> one
    month -> 8.33%) OR a percentage (unit '% of annual salary', v=50 -> 50%). The extractor
    also emits a hybrid '% of monthly salary' whose VALUE is already the annual-equivalent %
    (e.g. 8.33 = one month), NOT 8.33% of a single month — so a %-unit is taken as annual %.
    GUARD: a single-month %-base with an implausibly large value (>50, i.e. a genuine fraction
    of ONE month like '100% of monthly salary') is divided by 12 to annualise; this rescues the
    pathological case without touching the ~8.33 annual-% rows (verified: 0 current rows > 50)."""
    if v is None: return None
    lo = (u or "").strip().lower()
    if _PCT.search(lo):
        single_month = re.search(r"month|maand", lo) and not re.search(
            r"12|twelve|annual|year|jaar|preceding|earned in", lo)
        if single_month and v > 50:            # true fraction of one month -> annualise
            return v / 12.0
        return v                               # value already expressed as % of annual
    if re.search(r"month|maand", lo): return v * (100.0 / 12.0)   # count of months -> % annual
    return None

def split_severance(v, u):
    out = {"months_salary": None, "eur": None, "pct": None}
    if v is None: return out
    lo = (u or "").strip().lower()
    if re.search(r"month|monthly|times.*(salary|income|pay|wage)|x\s*salary|periodic income|salaris", lo):
        out["months_salary"] = v
    elif re.search(r"\bweeks?\b", lo):
        out["months_salary"] = v * (12.0/52.0)     # weeks-of-salary -> months
    elif _EUR.search(lo):
        out["eur"] = v
    elif _PCT.search(lo):
        out["pct"] = v
    return out

def normalize(canonical, v, u):
    v = parse_float(v)
    if canonical == "months":        return to_months(v, u)
    if canonical == "hours":         return to_hours(v, u)
    if canonical == "percent":       return to_percent(v, u)
    if canonical in ("contracts", "employees", "count"): return to_count(v, u)
    if canonical == "days_per_year":  return to_days(v, u)
    if canonical == "weeks":          return to_weeks(v, u)
    if canonical == "eur":            return to_eur(v, u)
    if canonical == "eur_per_month":  return to_eur_per_month(v, u)
    if canonical == "eur_per_km":     return to_eur_per_km(v, u)
    if canonical == "pct_annual":     return to_pct_annual(v, u)
    if canonical == "hours_per_week":
        lo = (u or "").lower()
        if _PCT.search(lo): return None
        # 'per week' WINS over annual qualifiers: '38 hours per week on an annual basis' is a
        # weekly figure with an averaging note — it used to hit the /52 branch and become 0.73
        # (2026-07-09 normalizer audit). Only a genuinely annual total ('1872 hours per year')
        # divides by 52.
        if re.search(r"per\s*week|/\s*week|weekly", lo): return v
        if re.search(r"per\s*year|yearly|annual", lo): return (v/52.0) if v is not None else None
        return v
    if canonical == "months_salary": return split_severance(v, u)["months_salary"]
    return v

# ── statutory bounds (read live from statutory_all.csv) ───────────────────
def load_bounds(path=STATUTORY_CSV):
    df = read_csv_safe(path)
    bounds = {}
    for _, r in df.iterrows():
        role = r["statutory_role"].strip()
        # floor_lift = HARD minimum right (one-sided dwingend recht): blanks filled AND
        # stated below-floor values lifted to the floor — the worker legally gets at least
        # the floor no matter what the CAO says (vacation days, vakantiegeld, sick pay,
        # WAZO care leave). Plain 'floor' only fills blanks (deviatable provisions like
        # ATW min rest, where a lower stated value can be legal).
        if role not in ("cap", "floor", "default", "floor_lift"): continue
        val = parse_float(r["value"])
        if val is None: continue
        bounds.setdefault(r["field"].strip(), []).append(
            (parse_date(r["effective_from"]), parse_date(r["effective_to"]), role, val, r["unit"].strip()))
    return bounds

def bound_for(bounds, field, d):
    """Return (role, value) for `field` effective at date d; (None, None) if none."""
    for frm, to, role, val, unit in bounds.get(field, []):
        if (frm is None or (d and d >= frm)) and (to is None or (d and d <= to)):
            return role, val
    return None, None

def load_wml(path=WML_CSV):
    """WML series as a sorted list of (effective_from: date, eur_month: float)."""
    df = read_csv_safe(path)
    out = [(parse_date(r["effective_from"]), parse_float(r["wml_month_eur"])) for _, r in df.iterrows()]
    return sorted([(d, v) for d, v in out if d and v is not None])

def wml_at(series, d):
    """Adult statutory minimum monthly wage effective at date d (None before the series)."""
    cur = None
    for frm, v in series:
        if d and d >= frm: cur = v
        else: break
    return cur

# ── panel ops (v2: file_date = datum_kennisgeving) ───────────────────────
ID_COLS = ["cao_number", "id", "file_date", "ingangsdatum", "year", "file_name",
           "document_type", "doc_is_newest", "pub_lag_months", "term_group"]

def load_full_cao(path=CORRECTED_CSV):
    df = read_csv_safe(path)
    df = df[df["general_document_type"].str.startswith("full_cao")].copy()
    ing = df["ingangsdatum"].map(parse_date)
    ken = df["datum_kennisgeving"].map(parse_date)
    df["_date"] = [k if k is not None else i for k, i in zip(ken, ing)]   # THE time axis
    df["file_date_fallback"] = [k is None for k in ken]
    df["file_date"] = [d.isoformat() if d else "" for d in df["_date"]]
    df["year"] = [d.year if d else None for d in df["_date"]]
    df["pub_lag_months"] = [round((f - i).days / 30.44, 1) if (f is not None and i is not None) else None
                            for f, i in zip(df["_date"], ing)]
    df["term_group"] = df["cao_number"].astype(str) + "|" + df["ingangsdatum"].astype(str)
    df["document_type"] = df["general_document_type"]
    # tie-break helpers: populated-cell count (extraction richness) + numeric id
    df["_n_pop"] = (df.astype(str) != "").sum(axis=1)
    df["_idnum"] = pd.to_numeric(df["id"], errors="coerce")
    return df.reset_index(drop=True)

def add_newest(df):
    """doc_is_newest = the latest file per cao_number by file_date
    (ties: richer extraction, then higher id)."""
    s = df.sort_values(["cao_number", "_date", "_n_pop", "_idnum"])
    idx = s.groupby("cao_number").tail(1).index
    df["doc_is_newest"] = df.index.isin(idx)
    return df

def term_dedup_mask(df):
    """One doc per term_group (latest kennisgeving edition; ties as add_newest) —
    the POOLED-YARDSTICK subset. All docs are still scored."""
    s = df.sort_values(["term_group", "_date", "_n_pop", "_idnum"])
    idx = s.groupby("term_group").tail(1).index
    return pd.Series(df.index.isin(idx), index=df.index)

def forward_fill(df, raw_cols):
    """Within each cao_number, sort by file_date and carry the last non-empty value
    forward. Blank in a newer full doc = unchanged / still in force."""
    df = df.sort_values(["cao_number", "_date", "_idnum"]).copy()
    for col in raw_cols:
        df[col + "_ff"] = df.groupby("cao_number")[col].ffill()
    return df

# ── pooled standardisation (v2 core) ──────────────────────────────────────
def pooled_params(x, dmask, min_n=10):
    """Winsor bounds + mu/sd of `x` over the yardstick subset. None if degenerate."""
    obs = pd.to_numeric(x[dmask], errors="coerce").dropna()
    if len(obs) < min_n: return None
    lo, hi = obs.quantile(0.01), obs.quantile(0.99)
    w = obs.clip(lo, hi)
    sd = w.std(ddof=0)
    if not sd or sd == 0 or math.isnan(sd): return None
    return {"lo": float(lo), "hi": float(hi), "mu": float(w.mean()), "sd": float(sd), "n": int(len(obs))}

def pooled_z(x, p):
    """z against persisted/computed pooled params; winsorised, clipped ±3."""
    x = pd.to_numeric(x, errors="coerce")
    if p is None: return pd.Series(np.nan, index=x.index)
    return ((x.clip(p["lo"], p["hi"]) - p["mu"]) / p["sd"]).clip(-3, 3)

def pooled_pctile(x, dmask, min_n=10):
    """Percentile rank of x within the yardstick pool's ECDF (robustness column)."""
    x = pd.to_numeric(x, errors="coerce")
    obs = np.sort(x[dmask].dropna().values)
    if len(obs) < min_n: return pd.Series(np.nan, index=x.index)
    r = pd.Series(np.searchsorted(obs, x.values, side="right") / len(obs), index=x.index)
    return r.where(x.notna())

def zscore_value(v, p):
    """Score a single cardinal value (e.g. a statutory floor) with pooled params."""
    if v is None or p is None: return None
    return float(min(3.0, max(-3.0, (min(p["hi"], max(p["lo"], v)) - p["mu"]) / p["sd"])))

def save_params(topic, entries, path=SCORING_PARAMS_CSV):
    """Persist this topic's pooled parameters (replace-by-topic, sorted => deterministic).
    entries: dicts with field, variant, canonical, sign, winsor_lo/hi, mu, sd, n."""
    cols = ["topic", "field", "variant", "canonical", "sign", "winsor_lo", "winsor_hi", "mu", "sd", "n"]
    rows = []
    if os.path.exists(path):
        old = read_csv_safe(path)
        rows = [{c: r.get(c, "") for c in cols} for _, r in old.iterrows() if r["topic"] != topic]
    for e in entries:
        rows.append({"topic": topic, **{c: e.get(c, "") for c in cols if c != "topic"}})
    rows.sort(key=lambda r: (str(r["topic"]), str(r["field"]), str(r["variant"])))
    pd.DataFrame(rows, columns=cols).to_csv(path, sep=";", index=False)

def load_params(path=SCORING_PARAMS_CSV):
    """{(field, variant): params-dict} for scoring external values (statutory index)."""
    df = read_csv_safe(path)
    out = {}
    for _, r in df.iterrows():
        try:
            out[(r["field"], r["variant"])] = {
                "lo": float(r["winsor_lo"]), "hi": float(r["winsor_hi"]),
                "mu": float(r["mu"]), "sd": float(r["sd"]), "n": int(float(r["n"])),
                "sign": float(r["sign"]), "topic": r["topic"], "canonical": r["canonical"]}
        except (ValueError, TypeError):
            continue
    return out

# ── cardinal "full" variant (unchanged v1 semantics) ──────────────────────
def unit_col(col):
    """Resolve a value column's unit column: foo_value -> foo_unit;
    foo_range_min / foo_range_max share foo_range_unit."""
    if col.endswith("_value"): return col[:-6] + "_unit"
    if col.endswith("_min") or col.endswith("_max"): return col.rsplit("_", 1)[0] + "_unit"
    return col + "_unit"

def normalize_fields(df, fields, clamps=None):
    """fields: list of (value_col, canonical_unit, sign). Adds <col>__norm (numeric).
    clamps: {value_col: (lo, hi)} plausibility bounds — outside => NaN."""
    clamps = clamps or {}
    cols = []
    for col, canon, _ in fields:
        ucol = unit_col(col)
        s = pd.to_numeric(
            pd.Series([normalize(canon, v, u) for v, u in zip(df[col], df.get(ucol, ""))],
                      index=df.index), errors="coerce")
        if col in clamps:
            lo, hi = clamps[col]
            s = s.where((s >= lo) & (s <= hi))
        df[col + "__norm"] = s
        cols.append(col + "__norm")
    return cols

def apply_zerofill(df, spec):
    """Presence-gated (or unconditional) zero-fill of EXTRA amount fields, on the forward-
    filled column <field>__norm_ff. spec = list of (field, boolean_or_None):
      boolean given  -> blank becomes 0 only when that boolean is False (genuine absence);
                        blank with the boolean True stays NaN (present-but-unquantified).
      boolean = None -> blank becomes 0 unconditionally (use only where a blank reliably
                        means "no benefit", e.g. severance-extra).

    CRITICAL (2026-07-07 fix): a value STATED in a unit we can't convert normalises to NaN,
    but that is NOT a blank — the CAO granted the benefit, we just can't measure it. Zero-
    filling it would score a real benefit as 'absent' (this wrongly zeroed 210 severance docs
    stated as '€ one-off' / '% of daily wage'). So we only zero-fill a NaN that was NEVER
    stated in ANY edition of the CAO (raw cell empty, forward-looking within cao_number).
    Present-but-unconvertible stays NaN = available-case, exactly like pension deferrals."""
    for fld, boolean in spec:
        ffc = fld + "__norm_ff"
        if ffc not in df.columns:
            continue
        raw = df[fld].astype(str).str.strip() if fld in df.columns else pd.Series("", index=df.index)
        stated = (raw.ne("") & raw.str.lower().ne("nan")).astype(int)
        if "cao_number" in df.columns:                    # ever-stated up to this edition
            stated = stated.groupby(df["cao_number"]).cummax()
        truly_blank = df[ffc].isna() & stated.eq(0)       # NaN AND never stated (any unit)
        if boolean is None:
            df[ffc] = df[ffc].where(~truly_blank, 0.0)
        elif boolean in df.columns:
            gate = df[boolean].astype(str).str.strip().str.lower()
            absent = gate.eq("false")
            # GATE-FLIP GUARD (2026-07-07): the presence boolean itself is noisily extracted
            # (~9% flips between editions). Only zero-fill when the benefit is CONSISTENTLY absent
            # — the gate is never True in ANY edition of the CAO. If it was True somewhere, the
            # benefit exists in the agreement and a False here is extraction noise, not absence
            # (this wrongly zeroed ~1,396 docs before the guard).
            if "cao_number" in df.columns:
                ever_true = gate.eq("true").groupby(df["cao_number"]).transform("max")
                absent = absent & ~ever_true
            df[ffc] = df[ffc].where(~(truly_blank & absent), 0.0)
    return df

def stratified_zpr(df, field, bases, dmask, sign, params_out=None):
    """z + percentile for a field whose value lives in MULTIPLE incompatible unit-bases
    (e.g. training budget in € vs % of salary vs % of wage-sum). Standardise WITHIN each
    base (its own yardstick μ/σ and ECDF), so a doc's score is comparable across bases —
    a +1 z in € and a +1 z in %-of-salary both mean '1σ above that base's mean'. Returns
    ONE per-doc z and ONE per-doc percentile (the doc's own base), NaN where no base matches.
    bases = [(name, unit_regex)] evaluated in order (first match wins)."""
    u = df.get(unit_col(field), pd.Series("", index=df.index)).astype(str).str.lower()
    val = pd.to_numeric(df[field], errors="coerce")
    z = pd.Series(np.nan, index=df.index); pr = pd.Series(np.nan, index=df.index)
    taken = pd.Series(False, index=df.index)
    for name, rgx in bases:
        m = u.str.contains(rgx, regex=True, na=False) & val.notna() & ~taken
        taken |= m
        x = val.where(m)
        p = pooled_params(x, dmask)
        zb = sign * pooled_z(x, p)
        prb = pooled_pctile(x, dmask)
        if sign < 0:
            prb = 1 - prb
        z = z.where(~m, zb); pr = pr.where(~m, prb)
        if params_out is not None and p:
            params_out.append({"field": f"{field}[{name}]", "variant": "strat", "canonical": name,
                               "sign": sign, "winsor_lo": p["lo"], "winsor_hi": p["hi"],
                               "mu": p["mu"], "sd": p["sd"], "n": p["n"]})
    return z, pr


def role_caps(df, fields, bounds):
    """Statutory role/value per field at each doc's file_date."""
    role_s, capv_s = {}, {}
    for col, _, _ in fields:
        rv = [bound_for(bounds, col, d) for d in df["_date"]]
        role_s[col] = pd.Series([r for r, _ in rv], index=df.index)
        capv_s[col] = pd.to_numeric(pd.Series([v for _, v in rv], index=df.index), errors="coerce")
    return role_s, capv_s

def field_variant(variant, col, df, role_s, capv_s):
    norm = df[col + "__norm"]; norm_ff = df[col + "__norm_ff"]
    role, capv = role_s[col], capv_s[col]
    if variant == "raw":
        return norm.copy()
    x = norm_ff.copy()                                   # full forward-fills first
    fill = role.isin(["floor", "default", "floor_lift"]) & x.isna()
    x = x.where(~fill, capv)
    lift = (role == "floor_lift") & x.notna() & (x < capv)  # hard minimum: below-floor -> floor
    x = x.where(~lift, capv)
    if variant == "full":
        x = x.mask((role == "cap") & (x > capv))
    return x

def magnitude_pooled(df, fields, role_s, capv_s, variant, dmask, params_out=None):
    """Pooled, signed, available-case topic score.
    Returns (topic_z Series, per-field SIGNED z DataFrame keyed by value_col)."""
    zs = {}
    for col, canon, sign in fields:
        x = field_variant(variant, col, df, role_s, capv_s)
        p = pooled_params(x, dmask)
        if params_out is not None:
            e = {"field": col, "variant": variant, "canonical": canon, "sign": sign,
                 "winsor_lo": "", "winsor_hi": "", "mu": "", "sd": "", "n": 0}
            if p: e.update({"winsor_lo": p["lo"], "winsor_hi": p["hi"],
                            "mu": p["mu"], "sd": p["sd"], "n": p["n"]})
            params_out.append(e)
        zs[col] = sign * pooled_z(x, p)
    Z = pd.DataFrame(zs)
    return Z.mean(axis=1), Z

def magnitude_pctile(df, fields, role_s, capv_s, dmask):
    """Generosity PERCENTILE score — the equal-range alternative to magnitude_pooled.
    Each field is ECDF-ranked on the yardstick pool (NO winsor, NO ±3 clip — rank is
    inherently outlier-robust and clip only creates ties), then oriented so higher =
    more generous (rank for +1 fields, 1-rank for -1 fields), then averaged available-
    case. Unlike z, every field contributes the identical [0,1] range, so being best in
    a bunched field (e.g. reimbursement, whose z max is only +0.33) fully offsets being
    worst in a spread field. Returns (topic_gen01 in [0,1], per-field pctile DataFrame)."""
    pcs = {}
    for col, canon, sign in fields:
        x = field_variant("full", col, df, role_s, capv_s)
        r = pooled_pctile(x, dmask)                  # fraction of yardstick <= x, [0,1]
        pcs[col] = r if sign > 0 else (1.0 - r)      # generosity orientation
    P = pd.DataFrame(pcs)
    return P.mean(axis=1), P

# ── coverage ──────────────────────────────────────────────────────────────
def coverage(df, booleans):
    present = pd.concat([df[b].astype(str).str.strip().str.lower().eq("true")
                         for b in booleans], axis=1)
    return present.mean(axis=1), present.shape[1]

def coverage_pooled_z(cov, dmask):
    """Pooled z of the 0-1 coverage share (same yardstick de-dup as magnitude)."""
    cov = pd.to_numeric(cov, errors="coerce")
    return pooled_z(cov, pooled_params(cov, dmask))

# ── generic topic builder (drivers with no special handling) ──────────────
# --- NAMING SCHEME (see indices/NAMING.md) — single source of truth --------------------
# Dual-track topics (numeric magnitude AND coverage booleans) get numeric/coverage/combined.
DUAL_TOPICS = ["leave", "absence", "term", "contract", "overtime", "training", "bonus", "fringe",
               "homeoffice", "pension"]   # absence gained a coverage track 2026-07-08


def apply_scheme(df):
    """Map a composite-style frame (internal names {t}_z=magnitude, {t}_combined_z, {t}_gen01,
    {t}_combined01, overall_gen01, overall_combined01) to the published taxonomy:
       dual topic  : {t}_numeric_z, {t}_coverage_z, {t}_z(=combined),
                     {t}_numeric_pctile, {t}_coverage_pctile, {t}_pctile(=combined)
       single-track: {t}_z stays the one track; redundant combined dupes dropped; gen01->pctile
       overall     : overall_gen01->overall_numeric_pctile, overall_combined01->overall_pctile
    Idempotent: a frame already in the taxonomy is returned unchanged."""
    import pandas as _pd  # local (module already imports pandas as pd, but keep self-contained)
    ren, drop = {}, []
    for t in DUAL_TOPICS:
        if f"{t}_z" in df.columns and f"{t}_combined_z" in df.columns:
            ren[f"{t}_z"] = f"{t}_numeric_z"; ren[f"{t}_combined_z"] = f"{t}_z"
        if f"{t}_gen01" in df.columns and f"{t}_combined01" in df.columns:
            ren[f"{t}_gen01"] = f"{t}_numeric_pctile"; ren[f"{t}_combined01"] = f"{t}_pctile"
    for t in ("wage", "safety", "childcare", "ai"):   # single-track: combined == one track
        for suf in ("_combined_z", "_combined01"):
            if f"{t}{suf}" in df.columns:
                drop.append(f"{t}{suf}")
        if f"{t}_gen01" in df.columns:
            ren[f"{t}_gen01"] = f"{t}_pctile"
    if "overall_gen01" in df.columns:
        ren["overall_gen01"] = "overall_numeric_pctile"   # numeric-only roll-up (companion)
    if "overall_combined01" in df.columns:
        ren["overall_combined01"] = "overall_pctile"      # PRIMARY overall rank (combined)
    for c in list(df.columns):                                   # safety net: any stray *_gen01
        if c.endswith("_gen01") and c not in ren:
            ren[c] = c[:-6] + "_pctile"
    return df.drop(columns=[c for c in drop if c in df.columns]).rename(columns=ren)


def build_simple_index(topic, fields, booleans, short, clamp, descriptive, out_path, diag_path,
                       derived_bools=(), passthrough=(), zerofill=(), stratified=()):
    """v2 generic builder. `fields`: (col, canonical, sign); `descriptive`:
    (out_col, value_col, picker(v,u)) split-off buckets. Emits per doc (NAMING.md taxonomy):
      <topic>_numeric_z (magnitude, full)  <topic>_numeric_z_raw  <topic>_numeric_rankpct
      <topic>_numeric_pctile (equal-range) per-field signed z (<short>_z)
      coverage + <topic>_coverage_z + <topic>_coverage_pctile
    `derived_bools`: (out_col, source_col, {values}) — boolean dummies from
    categorical columns, appended to the coverage set. `passthrough`: raw columns
    carried into the output unchanged (descriptive categoricals).
    Pooled params persisted to scoring_params.csv (variant full + raw)."""
    bounds = load_bounds()
    df = load_full_cao(); df = add_newest(df)
    dmask = term_dedup_mask(df)
    print(f"{topic}: {len(df)} full-CAO docs, {df['cao_number'].nunique()} CAOs, "
          f"yardstick {int(dmask.sum())} term-deduped docs")
    norm_cols = normalize_fields(df, fields, clamp) if fields else []
    for outc, vcol, picker in descriptive:
        df[outc] = [picker(parse_float(v), u) for v, u in zip(df[vcol], df.get(unit_col(vcol), ""))]
    booleans = list(booleans)
    for outc, src, vals in derived_bools:
        s = df[src].astype(str).str.strip()
        mask = (s != "") & (s.str.lower() != "nan") if vals is None else s.isin(vals)  # None => presence-of-value
        df[outc] = mask.map({True: "True", False: "False"})
        booleans.append(outc)
    params = []
    fieldz_cols = []
    fieldpct_cols = []
    if fields:
        df = forward_fill(df, norm_cols)
        dmask = dmask.reindex(df.index)  # forward_fill re-sorted; realign
        role_s, capv_s = role_caps(df, fields, bounds)
        # PRESENCE-GATED ZERO-FILL (promoted 2026-07-06): an EXTRA amount field's blank
        # becomes 0 (genuine absence => least generous) ONLY when its presence boolean is
        # explicitly False; blank with the boolean True stays excluded (stated-but-
        # unquantified). Keep the pre-zerofill score as <topic>_z_availcase for comparison.
        if zerofill:
            z_av, _ = magnitude_pooled(df, fields, role_s, capv_s, "full", dmask)
            df[f"{topic}_numeric_z_availcase"] = z_av.round(4)
            apply_zerofill(df, zerofill)
        z_full, Z = magnitude_pooled(df, fields, role_s, capv_s, "full", dmask, params)
        z_raw, _ = magnitude_pooled(df, fields, role_s, capv_s, "raw", dmask, params)
        df[f"{topic}_numeric_z"] = z_full.round(4)          # magnitude track (was {t}_z)
        df[f"{topic}_numeric_z_raw"] = z_raw.round(4)
        df[f"{topic}_numeric_rankpct"] = pooled_pctile(z_full, dmask).round(4)  # robustness rank (was {t}_pctile)
        for col, _, _ in fields:
            fz = short[col] + "_z"
            df[fz] = Z[col].round(4); fieldz_cols.append(fz)
        # EQUAL-RANGE percentile track (2026-07-07): per-field ECDF rank -> generosity 0-1,
        # averaged. Cures the asymmetric z-range problem; computed on RAW values (no clip).
        gen01, P = magnitude_pctile(df, fields, role_s, capv_s, dmask)
        df[f"{topic}_numeric_pctile"] = gen01.round(4)      # equal-range magnitude pctile (was {t}_gen01)
        for col, _, _ in fields:
            fp = short[col] + "_prank"     # per-field percentile RANK ([0,1]); '_prank' avoids
            df[fp] = P[col].round(4); fieldpct_cols.append(fp)   # collision with _pct VALUE cols
        # STRATIFIED fields (2026-07-08): value lives in multiple incompatible unit-bases (e.g.
        # training budget € / %-salary / %-wage-sum). Standardise WITHIN each base so the ONE
        # output z & prank are comparable across bases; fold into the topic magnitude mean like
        # any field. Genuine absence (gate never True in the CAO & never stated) -> floor.
        for sfield, ssign, sbases, sshort, sgate in stratified:
            sz, spr = stratified_zpr(df, sfield, sbases, dmask, ssign, params)
            if sgate and sgate in df.columns:
                raw = df[sfield].astype(str).str.strip()
                ever_stated = (raw.ne("") & raw.str.lower().ne("nan")).groupby(df["cao_number"]).transform("max").astype(bool)
                gate_ever = df[sgate].astype(str).str.strip().str.lower().eq("true").groupby(df["cao_number"]).transform("max").astype(bool)
                floor = sz.isna() & ~ever_stated & ~gate_ever      # consistent genuine absence
                sz = sz.where(~floor, -3.0); spr = spr.where(~floor, 0.0)
            df[sshort + "_z"] = sz.round(4); fieldz_cols.append(sshort + "_z")
            df[sshort + "_prank"] = spr.round(4); fieldpct_cols.append(sshort + "_prank")
            Z[sfield] = sz; P[sfield] = spr
        if stratified:                                             # recompute topic aggregates
            z_full = Z.mean(axis=1); gen01 = P.mean(axis=1)
            df[f"{topic}_numeric_z"] = z_full.round(4)
            df[f"{topic}_numeric_rankpct"] = pooled_pctile(z_full, dmask).round(4)
            df[f"{topic}_numeric_pctile"] = gen01.round(4)
        df["n_fields_pop"] = Z.notna().sum(axis=1)
    else:
        df["n_fields_pop"] = 0
    df["low_support"] = (df["n_fields_pop"] < 2) if fields else False
    if booleans:
        cov, covn = coverage(df, booleans)
        df[f"{topic}_coverage"] = cov.round(4); df[f"{topic}_coverage_n"] = covn
        covz = coverage_pooled_z(df[f"{topic}_coverage"], dmask).round(4)
        df[f"{topic}_coverage_z"] = covz
        # coverage on the same equal-range [0,1] scale as gen01 (ECDF rank of the share)
        covpct = pooled_pctile(df[f"{topic}_coverage"], dmask).round(4)
        # DEGENERATE GUARD (2026-07-08): a near-constant coverage share (e.g. ~99% of CAOs have
        # no AI clause) has no pooled variance -> coverage_z is all-NaN and the ECDF percentile
        # is meaningless (every tied doc maps to ~1.0). Blank BOTH rather than ship a misleading
        # 0.99-for-everyone rank. The raw share ({topic}_coverage) still carries the signal.
        if covz.notna().sum() == 0:
            print(f"  {topic}: coverage share is ~constant (no pooled variance) -> "
                  f"coverage_z & coverage_pctile left blank; use {topic}_coverage (raw share)")
            covpct = pd.Series(np.nan, index=df.index)
        df[f"{topic}_coverage_pctile"] = covpct
    save_params(topic, params)

    inputs = {c + "__norm_ff": short[c] for c, _, _ in fields}
    out = df.rename(columns=inputs)
    magcols = (([f"{topic}_numeric_z", f"{topic}_numeric_z_raw", f"{topic}_numeric_rankpct", f"{topic}_numeric_pctile"]
               + ([f"{topic}_numeric_z_availcase"] if zerofill else []) + fieldz_cols + fieldpct_cols)) if fields else []
    covcols = ([f"{topic}_coverage", f"{topic}_coverage_z", f"{topic}_coverage_pctile",
                f"{topic}_coverage_n"] if booleans else [])
    keep = ID_COLS + list(inputs.values()) + [d[0] for d in descriptive] + list(passthrough) \
           + magcols + covcols + ["n_fields_pop", "low_support"]
    out = out[[c for c in keep if c in out.columns]].sort_values(["cao_number", "file_date", "id"])
    out.to_csv(out_path, sep=";", index=False)
    print(f"  wrote {out_path} ({len(out)} rows, {len(out.columns)} cols)")

    new = out[out["doc_is_newest"]]
    def m(s): return pd.to_numeric(s, errors="coerce")
    rows = [("n_records", len(out)), ("n_caos", out["cao_number"].nunique()),
            ("n_newest", int(out["doc_is_newest"].sum())),
            ("share_low_support", round(out["low_support"].mean(), 3))]
    if fields:
        rows += [("z_newest_mean", round(m(new[f"{topic}_numeric_z"]).mean(), 4)),
                 ("z_newest_scored", int(m(new[f"{topic}_numeric_z"]).notna().sum())),
                 ("z_all_mean", round(m(out[f"{topic}_numeric_z"]).mean(), 4)),
                 ("corr_z_pctile", round(m(out[f"{topic}_numeric_z"]).corr(m(out[f"{topic}_numeric_rankpct"])), 3)),
                 ("corr_z_raw", round(m(out[f"{topic}_numeric_z"]).corr(m(out[f"{topic}_numeric_z_raw"])), 3))]
    if booleans:
        rows += [("coverage_newest_mean", round(m(new[f"{topic}_coverage"]).mean(), 3))]
    diag = pd.DataFrame(rows, columns=["metric", "value"])
    diag.to_csv(diag_path, sep=";", index=False)
    print(diag.to_string(index=False))
    return out
