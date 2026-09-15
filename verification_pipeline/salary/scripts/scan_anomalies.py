"""scan_anomalies.py — Phase 1 deterministic outlier scan over the salary CSV.

Reads (READ-ONLY) two external files:
  · /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/outputs/excel/new_results/
        extracted_data_salary.csv   (244k rows × 737 cols; one row per scale entry)
  · /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/outputs/llm_analysis/salary/
        <cao>/<file_name>_analysis.json   (one JSON per source file; CSV row i ↔
        JSON salary_information[i]; salary_N_* ↔ timeline[N-1].*)

Writes everything to qa/qa_salary/outputs/ — the Python project is never modified.

Three layers (each layer's rule names are stable and re-used by Phase 4):

  Layer 1 — CSV ↔ JSON consistency (per cell)
    For every populated CSV cell that has a JSON counterpart, flag any difference
    after normalisation. Rule names: meta_mismatch:<field>, timeline_mismatch:<field>.

  Layer 2 — numeric outliers (amount + unit + year)
    Unit-aware hard bounds: monthly < 800 or > 15000 / hourly < 5 or > 100 /
    annual < 5000 or > 250000, etc. Rule names: amount_out_of_range,
    amount_without_unit, noncanonical_unit, increase_pct_extreme,
    suspicious_placeholder.

  Layer 3 — semantic / internal-consistency (per row)
    Contradictions like worker_type='youth' & age_group='25+'.
    Rule names: youth_with_adult_age, is_entry_with_senior_step, ft_hours_out_of_range.

Output:  qa/qa_salary/outputs/salary_anomalies.csv  (one row per flagged cell)
         qa/qa_salary/outputs/salary_anomalies_summary.txt  (counts + Phase-3 estimate)
"""
from __future__ import annotations
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

try:
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR, LLM_ANALYSIS_SALARY_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"
    SAL_SRC = LLM_ANALYSIS_SALARY_DIR
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR, LLM_ANALYSIS_SALARY_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"
    SAL_SRC = LLM_ANALYSIS_SALARY_DIR
OUT_DIR = Path(__file__).resolve().parent.parent / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Layer 2 thresholds (hard bounds; below/above = almost certainly wrong) ───
# Tuned from the discovery: monthly median €2,637 (p99 €9,643); hourly median
# €14.80 (p99 €28); annual median €39k. These bounds exclude only clear errors.
AMOUNT_BOUNDS = {
    "monthly":  (800, 15000),
    "month":    (800, 15000),     # non-canonical variant
    "hourly":   (5, 100),
    "hour":     (5, 100),
    "annual":   (5000, 250000),
    "yearly":   (5000, 250000),
    "4-week":   (800, 12000),
    "weekly":   (150, 2000),
    "week":     (150, 2000),
    "daily":    (25, 800),
    "day":      (25, 800),
    "period":   (800, 15000),     # 4-week-ish
}
CANONICAL_UNITS = set(AMOUNT_BOUNDS) | {
    "per match", "per block", "block",
    "per offshore day", "offshore day", "per activity",
    "per meeting", "3-hour activity",
}
PLACEHOLDERS = {0.0, 1.0, 99.0, 100.0, 1111.0, 9999.0, 11111.0, 99999.0}
INC_PCT_BOUNDS = (-5.0, 15.0)

# Dutch monthly statutory minimum wage (adult 21+, gross, full-time). Used as a
# SOFT floor — we flag only adult monthly amounts below 60% of nominal, which
# catches likely-bad rows without false-positiving youth scales / part-time rows
# that legitimately sit below statutory minimum. Source: SZW historical tables.
STATUTORY_MIN_MONTHLY = {
    2010: 1400, 2011: 1410, 2012: 1450, 2013: 1470, 2014: 1490,
    2015: 1500, 2016: 1520, 2017: 1550, 2018: 1580, 2019: 1620,
    2020: 1680, 2021: 1700, 2022: 1750, 2023: 1934, 2024: 2070,
    2025: 2100,
}
STATUTORY_FLOOR_FRAC = 0.60      # flag only if < 60 % of nominal
# Drops > this fraction between consecutive timeline entries within one row =
# suspicious. Wage cuts of 30 % do not happen in CAOs except as a unit error.
TIMELINE_DROP_THRESHOLD = 0.30

# JSON ↔ CSV field map
META_FIELDS = [
    ("jobgroup", "jobgroup"), ("step_label", "step"), ("worker_type", "worker"),
    ("is_entry", "is_entry"), ("age_group", "age_group"), ("education", "education"),
    ("ft_hours", "ft_hours"), ("permanency", "permanency"),
    ("hours_type", "hours_type"), ("row_note", "row_note"),
]
TL_FIELDS = [
    ("start_date", "start_date"), ("end_date", "end_date"),
    ("amount", "amount"), ("unit", "unit"),
    ("table_label", "table_label"), ("increase_percent", "inc_pct"),
    ("holiday_in_amount", "holiday_incl"), ("note", "note"),
    ("hours_basis_ft_week", "hours_basis_ft_week"),
]

_BLANK_TOKENS = {"", "none", "null", "nan", "n/a"}

# ── Three JSON schema variants seen across the 2,470 non-empty files ─────────
#   (1) standard_nested        : 2,248 files — full English keys + nested timeline[]
#   (2) compact_parallel(am)   :    91 files — short meta keys + parallel arrays
#                                   (sd[], am[], un[], …); no `tl` and no `timeline`
#   (3) compact_nested_tl      :   131 files — short meta keys + nested `tl[]` of
#                                   short-key objects {sd, ed, am, un, ip, nt}
# All three are normalized to standard_nested before comparison.
# The standard JSON uses verbose meta keys (jobgroup, ft_hours, …) but the
# nested timeline still uses SHORT keys: `inc_pct` and `holiday_incl`. The
# compact normalizers therefore emit the same short timeline-key names so the
# comparison loop below sees consistent JSON keys regardless of source schema.
_COMPACT_META = {"jg": "jobgroup", "st": "step", "wr": "worker",
                 "ag": "age_group", "eu": "education", "pe": "permanency",
                 "rn": "row_note", "ft": "ft_hours", "fh": "ft_hours",
                 "ht": "hours_type", "is": "is_entry", "ie": "is_entry",
                 "pm": "permanency"}
_COMPACT_ARRAYS = {"sd": "start_date", "ed": "end_date", "am": "amount",
                   "un": "unit", "tl": "table_label", "ip": "inc_pct",
                   "hi": "holiday_incl", "nt": "note",
                   "hb": "hours_basis_ft_week"}
# Inside a nested `tl[]` object (schema #3) — short keys per row.
_COMPACT_TL_KEYS = {"sd": "start_date", "ed": "end_date", "am": "amount",
                    "un": "unit", "ip": "inc_pct", "nt": "note",
                    "hi": "holiday_incl", "hb": "hours_basis_ft_week",
                    "tl": "table_label"}
_COMPACT_UNIT = {"m": "monthly", "h": "hourly", "a": "annual",
                 "w": "weekly", "4w": "4-week", "4-w": "4-week",
                 "d": "daily", "p": "period"}


def _schema_kind(entry: dict) -> str:
    """standard | compact_parallel | compact_nested_tl | unknown."""
    if not isinstance(entry, dict):
        return "unknown"
    if "timeline" in entry:
        return "standard"
    if "tl" in entry and isinstance(entry.get("tl"), list):
        return "compact_nested_tl"
    if any(k in entry for k in ("am", "sd", "un")):
        return "compact_parallel"
    return "unknown"


def _normalize_compact_parallel(entry: dict) -> dict:
    out: dict = {}
    for short, full in _COMPACT_META.items():
        if short in entry and entry[short] is not None:
            out[full] = entry[short]
    arrays = {full: entry[short] for short, full in _COMPACT_ARRAYS.items() if short in entry}
    n = 0
    for k in ("amount", "start_date"):
        if k in arrays and isinstance(arrays[k], list):
            n = max(n, len(arrays[k]))
    tl: list = []
    for i in range(n):
        t: dict = {}
        for full, arr in arrays.items():
            if isinstance(arr, list):
                if i < len(arr):
                    t[full] = arr[i]
            else:
                t[full] = arr
        u = t.get("unit", "")
        if isinstance(u, str) and u.lower() in _COMPACT_UNIT:
            t["unit"] = _COMPACT_UNIT[u.lower()]
        tl.append(t)
    out["timeline"] = tl
    return out


def _normalize_compact_nested_tl(entry: dict) -> dict:
    """Schema #3: short meta keys + `tl: [ {sd,ed,am,un,ip,nt}, … ]`."""
    out: dict = {}
    for short, full in _COMPACT_META.items():
        if short in entry and entry[short] is not None:
            out[full] = entry[short]
    tl_in = entry.get("tl") or []
    tl_out: list = []
    for t_short in tl_in:
        if not isinstance(t_short, dict):
            continue
        t_full: dict = {}
        for short, full in _COMPACT_TL_KEYS.items():
            if short in t_short and t_short[short] is not None:
                t_full[full] = t_short[short]
        u = t_full.get("unit", "")
        if isinstance(u, str) and u.lower() in _COMPACT_UNIT:
            t_full["unit"] = _COMPACT_UNIT[u.lower()]
        tl_out.append(t_full)
    out["timeline"] = tl_out
    return out


def normalize_entry(entry):
    """Return a standard-schema entry regardless of input. Idempotent on standard."""
    kind = _schema_kind(entry)
    if kind == "compact_parallel":
        return _normalize_compact_parallel(entry)
    if kind == "compact_nested_tl":
        return _normalize_compact_nested_tl(entry)
    return entry


def _norm(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in _BLANK_TOKENS:
        return ""
    return s


def _norm_compare(a, b) -> bool:
    """Field-equivalence: blank/None ≈ ""; numeric tolerance; case-insensitive strings."""
    na, nb = _norm(a), _norm(b)
    if na == "" and nb == "":
        return True
    if na == "" or nb == "":
        return False
    # numeric tolerance
    try:
        return float(na.replace(",", ".")) == float(nb.replace(",", "."))
    except ValueError:
        pass
    return na.lower() == nb.lower()


def _year_of(dstr) -> int | None:
    m = re.match(r"(\d{4})", _norm(dstr) or "")
    return int(m.group(1)) if m else None


def _to_float(v):
    try:
        return float(_norm(v).replace(",", "."))
    except (ValueError, TypeError):
        return None


def amount_out_of_range(amount: float, unit: str):
    u = unit.strip().lower()
    if u not in AMOUNT_BOUNDS:
        return None
    lo, hi = AMOUNT_BOUNDS[u]
    if amount < lo:
        return f"below_floor({lo})"
    if amount > hi:
        return f"above_ceiling({hi})"
    return None


def is_youth_row(row) -> bool:
    """True iff this row plausibly describes a youth scale (sub-statutory wages OK)."""
    wt = _norm(row.get("worker_type", "")).lower()
    age = _norm(row.get("age_group", "")).lower()
    if wt in ("youth", "youngster", "minderjarig", "jeugd"):
        return True
    if re.search(r"\b1[5-9]\b|\b2[0-1]\b|jeugd|young|minderjarig", age):
        return True
    return False


def below_statutory(amount: float, unit: str, year: int | None, youth: bool):
    """Return (floor, ratio) if amount is < STATUTORY_FLOOR_FRAC × nominal; else None."""
    if youth or year is None or unit.lower() not in ("monthly", "month"):
        return None
    y = min(max(year, 2010), 2025)
    nominal = STATUTORY_MIN_MONTHLY.get(y)
    if nominal is None:
        return None
    floor = nominal * STATUTORY_FLOOR_FRAC
    if amount < floor:
        return (round(floor), round(amount / nominal, 2))
    return None


def semantic_row_flags(row):
    """Layer-3 semantic checks; returns [(field, rule, detail), ...]."""
    flags = []
    wt = _norm(row.get("worker_type", "")).lower()
    age = _norm(row.get("age_group", "")).lower()
    is_e = _norm(row.get("is_entry", "")).lower()
    step = _norm(row.get("step_label", "")).lower()
    fth = _to_float(row.get("ft_hours", ""))

    if wt in ("youth", "youngster", "minderjarig") and re.search(r"23\s*of\s*ouder|25\+|adult|volwassen", age):
        flags.append(("worker_type", "youth_with_adult_age", f"worker={wt!r}/age={age!r}"))
    if is_e in ("true", "yes", "1") and re.search(r"step\s*([5-9]|1\d)|trede\s*([5-9]|1\d)", step):
        flags.append(("is_entry", "is_entry_with_senior_step", f"step={step!r}"))
    if fth is not None and (fth < 20 or fth > 45):
        flags.append(("ft_hours", "ft_hours_out_of_range", f"{fth}"))
    return flags


def scan() -> None:
    out_path = OUT_DIR / "salary_anomalies.csv"
    summary_path = OUT_DIR / "salary_anomalies_summary.txt"
    rule_counter: Counter = Counter()
    flags_per_file: Counter = Counter()
    rows_with_flag: set = set()
    n_rows = 0
    n_json_missing = 0
    n_json_len_mismatch = 0
    n_cells_compared = 0

    # The salary blocks. 80 maximum, but most rows only use 1-3.
    # Build column groups once.
    sample = next(csv.DictReader(open(SAL_CSV, encoding="utf-8"), delimiter=";"))
    salary_blocks = []
    for n in range(1, 81):
        cols = [(f"salary_{n}_{csvf}", jsonf) for csvf, jsonf in TL_FIELDS
                if f"salary_{n}_{csvf}" in sample]
        if cols:
            salary_blocks.append((n, cols))

    with open(SAL_CSV, encoding="utf-8") as fcsv, \
         open(out_path, "w", encoding="utf-8") as fout:
        writer = csv.writer(fout, delimiter=";")
        writer.writerow(["row_id", "cao_number", "file_name", "entry_idx",
                         "salary_n", "field", "csv_value", "json_value",
                         "rule", "layer"])
        reader = csv.DictReader(fcsv, delimiter=";")

        json_cache: dict = {}      # (cao,file_name) -> salary_information[] or None
        file_row_idx: defaultdict = defaultdict(int)

        for row in reader:
            n_rows += 1
            cao = _norm(row.get("cao_number", ""))
            fname = _norm(row.get("file_name", ""))
            rid = _norm(row.get("id", ""))
            key = (cao, fname)

            # ── load JSON for this (cao, file_name) once ────────────────────
            if key not in json_cache:
                jpath = SAL_SRC / cao / f"{fname}_analysis.json"
                if not jpath.exists():
                    json_cache[key] = None
                    n_json_missing += 1
                else:
                    try:
                        json_cache[key] = json.load(open(jpath, encoding="utf-8")).get("salary_information", [])
                    except Exception as e:
                        json_cache[key] = None
                        n_json_missing += 1
            si = json_cache[key]
            i = file_row_idx[key]
            file_row_idx[key] += 1

            # ── Layer 1: CSV ↔ JSON consistency ─────────────────────────────
            jrow = None
            if si is not None and i < len(si):
                jrow = normalize_entry(si[i])
            elif si is not None:
                n_json_len_mismatch += 1

            row_flagged = False
            if jrow is not None:
                for csvf, jsonf in META_FIELDS:
                    cv = row.get(csvf, "")
                    jv = jrow.get(jsonf, "")
                    n_cells_compared += 1
                    if _norm_compare(cv, jv):
                        continue
                    # Split: blank-JSON is not a CSV<->JSON disagreement, it's
                    # "JSON didn't carry this field" — informational only.
                    if _norm(jv) == "" and _norm(cv) != "":
                        rule = f"meta_csv_only:{csvf}"
                    elif _norm(cv) == "" and _norm(jv) != "":
                        rule = f"meta_json_only:{csvf}"
                    else:
                        rule = f"meta_mismatch:{csvf}"
                    writer.writerow([rid, cao, fname, i, "", csvf,
                                     _norm(cv), _norm(jv), rule, "L1"])
                    rule_counter[rule] += 1
                    flags_per_file[key] += 1
                    row_flagged = True
                tl = jrow.get("timeline", []) or []
                for n, cols in salary_blocks:
                    if n - 1 >= len(tl):
                        # if CSV has a populated salary_N but JSON has fewer timeline entries
                        amt = row.get(f"salary_{n}_amount", "")
                        if _norm(amt) != "":
                            writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                             _norm(amt), "",
                                             "timeline_overflow_in_csv", "L1"])
                            rule_counter["timeline_overflow_in_csv"] += 1
                            flags_per_file[key] += 1
                            row_flagged = True
                        continue
                    jentry = tl[n - 1]
                    for csvf, jsonf in cols:
                        cv = row.get(csvf, "")
                        jv = jentry.get(jsonf, "")
                        n_cells_compared += 1
                        if _norm_compare(cv, jv):
                            continue
                        suffix = csvf.split('_', 2)[-1]
                        if _norm(jv) == "" and _norm(cv) != "":
                            rule = f"timeline_csv_only:{suffix}"
                        elif _norm(cv) == "" and _norm(jv) != "":
                            rule = f"timeline_json_only:{suffix}"
                        else:
                            rule = f"timeline_mismatch:{suffix}"
                        writer.writerow([rid, cao, fname, i, n, csvf,
                                         _norm(cv), _norm(jv), rule, "L1"])
                        rule_counter[rule] += 1
                        flags_per_file[key] += 1
                        row_flagged = True

            # ── Layer 2: numeric outliers per populated salary block ─────────
            youth = is_youth_row(row)
            prev_for_drop = None         # (n, amount, unit) for within-row drop check
            for n, _cols in salary_blocks:
                amt = _to_float(row.get(f"salary_{n}_amount", ""))
                unit = _norm(row.get(f"salary_{n}_unit", ""))
                start = row.get(f"salary_{n}_start_date", "")
                yr = _year_of(start)
                if amt is None:
                    if unit:
                        # unit set but amount empty — not necessarily wrong; skip
                        pass
                    continue
                # ─ strictly-negative amount (0.0 is handled by suspicious_placeholder) ─
                if amt < 0:
                    writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                     amt, unit, "amount_negative", "L2"])
                    rule_counter["amount_negative"] += 1
                    flags_per_file[key] += 1
                    row_flagged = True
                # amount present
                if unit == "":
                    writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                     amt, "", "amount_without_unit", "L2"])
                    rule_counter["amount_without_unit"] += 1
                    flags_per_file[key] += 1
                    row_flagged = True
                elif unit.lower() not in CANONICAL_UNITS:
                    writer.writerow([rid, cao, fname, i, n, f"salary_{n}_unit",
                                     unit, "", "noncanonical_unit", "L2"])
                    rule_counter["noncanonical_unit"] += 1
                    flags_per_file[key] += 1
                    row_flagged = True
                else:
                    rng = amount_out_of_range(amt, unit)
                    if rng:
                        writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                         amt, unit, f"amount_out_of_range:{rng}", "L2"])
                        rule_counter["amount_out_of_range"] += 1
                        flags_per_file[key] += 1
                        row_flagged = True
                    # ─ statutory-minimum (year-aware, adults only; skip placeholders) ─
                    sm = below_statutory(amt, unit, yr, youth) if amt not in PLACEHOLDERS else None
                    if sm:
                        floor, ratio = sm
                        writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                         amt, f"{unit} {yr}",
                                         f"below_statutory({floor},ratio={ratio})", "L2"])
                        rule_counter["below_statutory_minimum"] += 1
                        flags_per_file[key] += 1
                        row_flagged = True
                if amt in PLACEHOLDERS:
                    writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                     amt, "", "suspicious_placeholder", "L2"])
                    rule_counter["suspicious_placeholder"] += 1
                    flags_per_file[key] += 1
                    row_flagged = True
                inc = _to_float(row.get(f"salary_{n}_increase_percent", ""))
                if inc is not None and (inc < INC_PCT_BOUNDS[0] or inc > INC_PCT_BOUNDS[1]):
                    writer.writerow([rid, cao, fname, i, n, f"salary_{n}_increase_percent",
                                     inc, "", "increase_pct_extreme", "L2"])
                    rule_counter["increase_pct_extreme"] += 1
                    flags_per_file[key] += 1
                    row_flagged = True
                # ─ within-row timeline drop (consecutive blocks, SAME UNIT only) ─
                # gated on identical units so a monthly→weekly recode isn't flagged
                if prev_for_drop is not None and amt > 0:
                    pn, pamt, punit = prev_for_drop
                    same_unit = punit and unit and punit.lower() == unit.lower()
                    if same_unit and pamt > 0 and amt < pamt * (1 - TIMELINE_DROP_THRESHOLD):
                        writer.writerow([rid, cao, fname, i, n, f"salary_{n}_amount",
                                         amt, f"prev_salary_{pn}_amount={pamt} unit={unit}",
                                         f"timeline_drop_gt_{int(TIMELINE_DROP_THRESHOLD*100)}pct", "L2"])
                        rule_counter["timeline_drop"] += 1
                        flags_per_file[key] += 1
                        row_flagged = True
                if amt > 0:
                    prev_for_drop = (n, amt, unit)

            # ── Layer 3: semantic / internal-consistency ─────────────────────
            for field, rule, detail in semantic_row_flags(row):
                writer.writerow([rid, cao, fname, i, "", field, _norm(row.get(field, "")), detail, rule, "L3"])
                rule_counter[rule] += 1
                flags_per_file[key] += 1
                row_flagged = True

            if row_flagged:
                rows_with_flag.add(rid)

            if n_rows % 25000 == 0:
                print(f"  ... scanned {n_rows} rows, flagged so far {sum(rule_counter.values())}")

    # ── summary ──────────────────────────────────────────────────────────────
    flagged_files = [f for f, n in flags_per_file.items() if n > 0]
    lines = [
        f"rows scanned          : {n_rows}",
        f"cells compared (L1)   : {n_cells_compared}",
        f"rows with ≥1 flag     : {len(rows_with_flag)}",
        f"distinct files flagged: {len(flagged_files)}  (of {len(json_cache)} files seen)",
        f"JSONs missing/unloadable: {n_json_missing}",
        f"JSON length < CSV rows: {n_json_len_mismatch} (rows beyond JSON length)",
        "",
        "=== flags by rule ===",
    ]
    for rule, n in rule_counter.most_common():
        lines.append(f"  {n:>8}  {rule}")
    lines += ["", "=== Phase-3 subagent batch estimate (one chunk per file_name with flags) ==="]
    flags_per_file_list = sorted(flags_per_file.values(), reverse=True)
    if flags_per_file_list:
        med = flags_per_file_list[len(flags_per_file_list)//2]
        p99 = flags_per_file_list[len(flags_per_file_list)//100] if len(flags_per_file_list) > 100 else flags_per_file_list[0]
        lines += [
            f"  chunks (files w/ flags): {len(flagged_files)}",
            f"  flags/chunk: min=1 med={med} p99={p99} max={flags_per_file_list[0]}",
            f"  total flags: {sum(flags_per_file.values())}",
        ]
    text = "\n".join(lines)
    summary_path.write_text(text + "\n", encoding="utf-8")
    print("\n" + text)
    print(f"\nwrote {out_path}")
    print(f"wrote {summary_path}")


if __name__ == "__main__":
    scan()
