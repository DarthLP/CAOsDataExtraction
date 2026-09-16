"""scan_cross_row.py — cross-row deterministic checks within a file.

Adds two integrity rules that the per-row scanner can't see:

  step_monotonicity_violation
    Within (cao, file_name, jobgroup, worker_type) and a single salary_N
    timeline block, amounts should be non-decreasing as step rises (a higher
    step in the same jobgroup ladder pays at least as much). A drop of more
    than `STEP_TOLERANCE` × the prior step is flagged.

  ft_hours_inconsistent_in_file
    Within a (cao, file_name), `ft_hours` is almost always a single value
    (e.g. 38 across a CAO). When ≥75 % of the rows in a file share one
    ft_hours value and ≥20 rows are populated, rows whose ft_hours diverges
    from that majority are flagged.

Output is appended to outputs/salary_anomalies.csv with `entry_idx` set to the
zero-based file row index (same convention as the per-row scanner). A separate
salary_anomalies_cross_row.csv is written for stand-alone inspection.

READ-ONLY for everything under CAOsDataExtraction/.
"""
from __future__ import annotations
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_anomalies import _norm, _to_float, _year_of, AMOUNT_BOUNDS                # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs"
PY_PROJECT = Path("/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction")
SAL_CSV = PY_PROJECT / "outputs" / "excel" / "new_results" / "extracted_data_salary.csv"
try:
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from repo_paths import EXTRACTION_ROOT, EXCEL_RESULTS_DIR
    PY_PROJECT = EXTRACTION_ROOT
    SAL_CSV = EXCEL_RESULTS_DIR / "extracted_data_salary.csv"

# Drop > STEP_TOLERANCE from prior step is suspicious. Set high (20 %) so we
# only flag egregious violations — small dips between numeric steps are common
# and benign (rounding, transitional CAOs). Egregious drops usually indicate a
# scale-misread (missing digit) or jobgroup-mixup.
STEP_TOLERANCE = 0.20
# ft_hours majority threshold for the "inconsistent" check to fire.
FT_MAJORITY_FRAC = 0.75
FT_MIN_ROWS = 20

# Step monotonicity is only meaningful when steps are clearly ordered. We
# restrict the check to PURE NUMERIC steps within one (jobgroup, worker).
# Skipping named steps ("Maximum", "Aanloopstap 1", "Functie-eindsalaris 12")
# avoids false positives where the sort order is undefined.
_PURE_NUMERIC = re.compile(r"^\s*\d+(?:\.\d+)?\s*$")

CSV_SAFETY_LIMIT = 10_000_000     # csv field size cap; salary CSV has wide rows
csv.field_size_limit(CSV_SAFETY_LIMIT)


# Step labels include "0", "1", "5", "8a", "55%", "I", "II", "Pa". We need a
# sortable numeric key that respects the obvious ordering.
_INT_PREFIX = re.compile(r"^\s*(-?\d+)")
_PCT_PREFIX = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*%")


def _step_sort_key(s: str):
    """Return a sortable tuple from a step label. Numeric prefixes first, then
    percentages by their value, then a fallback string compare."""
    s = _norm(s)
    if not s:
        return (3, "")
    m = _PCT_PREFIX.match(s)
    if m:
        return (0, float(m.group(1)))
    m = _INT_PREFIX.match(s)
    if m:
        # 8a / 8b after 8
        rest = s[m.end():].strip().lower()
        return (1, int(m.group(1)), rest)
    return (2, s.lower())


def _populated_salary_blocks(sample_row: dict) -> list[int]:
    """Return sorted list of distinct N for which `salary_N_amount` exists.

    Use a regex to match EXACTLY `salary_<int>_amount`, not the broader
    `endswith("_amount")` which also picks up `salary_<N>_holiday_in_amount`
    and other suffixes (caused us to iterate every N twice last run)."""
    pat = re.compile(r"^salary_(\d+)_amount$")
    ns = set()
    for c in sample_row.keys():
        m = pat.match(c)
        if m:
            ns.add(int(m.group(1)))
    return sorted(ns)


def scan() -> None:
    rule_counter: Counter = Counter()
    flags: list[dict] = []                 # collected; written at end

    # 1. read all rows into per-file lists (preserve row order)
    print("loading CSV rows ...", flush=True)
    file_rows: dict = defaultdict(list)
    with open(SAL_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        sample = None
        for entry_idx_in_file, row in enumerate(reader):
            if sample is None:
                sample = row
            cao = _norm(row.get("cao_number", ""))
            fname = _norm(row.get("file_name", ""))
            rid = _norm(row.get("id", ""))
            key = (cao, fname)
            # entry_idx_in_file counts globally; we need per-file index
            file_rows[key].append({"row_id": rid, "raw": row})

    # rebuild per-file entry index (0-based per file)
    salary_ns = _populated_salary_blocks(sample) if sample else []
    print(f"files          : {len(file_rows)}")
    print(f"salary_N blocks: {len(salary_ns)} (1..{salary_ns[-1] if salary_ns else 0})")

    # 2. STEP MONOTONICITY
    print("step monotonicity ...", flush=True)
    n_files_step = 0
    for (cao, fname), rows in file_rows.items():
        # Group by (jobgroup, worker_type)
        by_grp: dict = defaultdict(list)
        for entry_idx, r in enumerate(rows):
            raw = r["raw"]
            jg = _norm(raw.get("jobgroup", ""))
            wt = _norm(raw.get("worker_type", ""))
            step = _norm(raw.get("step_label", ""))
            if not jg or not step:
                continue
            by_grp[(jg, wt)].append({
                "entry_idx": entry_idx,
                "row_id": r["row_id"],
                "step": step,
                "raw": raw,
            })
        any_flag = False
        for (jg, wt), grp in by_grp.items():
            # Restrict to pure-numeric steps only (sort order well-defined).
            numeric_grp = [g for g in grp if _PURE_NUMERIC.match(g["step"])]
            if len(numeric_grp) < 2:
                continue
            sorted_grp = sorted(numeric_grp, key=lambda x: float(x["step"]))
            # For each salary_N, check monotonicity (compare consecutive steps
            # only when both have populated amount + identical unit + same year).
            for n in salary_ns:
                prev = None
                for r in sorted_grp:
                    raw = r["raw"]
                    amt = _to_float(raw.get(f"salary_{n}_amount", ""))
                    unit = _norm(raw.get(f"salary_{n}_unit", "")).lower()
                    yr = _year_of(raw.get(f"salary_{n}_start_date", ""))
                    if amt is None or amt <= 0 or unit not in AMOUNT_BOUNDS:
                        continue
                    if prev is not None:
                        p_amt, p_unit, p_yr, p_step, p_row = prev
                        if p_unit == unit and p_yr == yr and amt < p_amt * (1 - STEP_TOLERANCE):
                            flags.append({
                                "row_id": r["row_id"],
                                "cao_number": cao,
                                "file_name": fname,
                                "entry_idx": r["entry_idx"],
                                "salary_n": n,
                                "field": f"salary_{n}_amount",
                                "csv_value": str(amt),
                                "json_value": (f"prev_step={p_step!r} prev_amount={p_amt} "
                                               f"(jobgroup={jg!r} worker={wt!r} year={yr})"),
                                "rule": "step_monotonicity_violation",
                                "layer": "L4",
                            })
                            rule_counter["step_monotonicity_violation"] += 1
                            any_flag = True
                    prev = (amt, unit, yr, r["step"], r["row_id"])
        if any_flag:
            n_files_step += 1
    print(f"  step monotonicity flags : {rule_counter.get('step_monotonicity_violation', 0)} across {n_files_step} files")

    # 3. ft_hours CONSISTENCY
    print("ft_hours consistency ...", flush=True)
    n_files_ft = 0
    for (cao, fname), rows in file_rows.items():
        ft_vals = []
        for entry_idx, r in enumerate(rows):
            v = _to_float(r["raw"].get("ft_hours", ""))
            if v is not None and 20 <= v <= 45:    # ignore out-of-range; per-row rule covers those
                ft_vals.append((entry_idx, r["row_id"], v))
        if len(ft_vals) < FT_MIN_ROWS:
            continue
        c = Counter(v for _, _, v in ft_vals)
        modal_val, modal_cnt = c.most_common(1)[0]
        if modal_cnt / len(ft_vals) < FT_MAJORITY_FRAC:
            continue            # no clear majority; skip
        # Flag the dissenters (non-modal values in a majority-modal file)
        any_flag = False
        for entry_idx, rid, v in ft_vals:
            if v != modal_val:
                flags.append({
                    "row_id": rid,
                    "cao_number": cao,
                    "file_name": fname,
                    "entry_idx": entry_idx,
                    "salary_n": "",
                    "field": "ft_hours",
                    "csv_value": str(v),
                    "json_value": f"file_modal={modal_val} ({modal_cnt}/{len(ft_vals)})",
                    "rule": "ft_hours_inconsistent_in_file",
                    "layer": "L4",
                })
                rule_counter["ft_hours_inconsistent_in_file"] += 1
                any_flag = True
        if any_flag:
            n_files_ft += 1
    print(f"  ft_hours consistency flags: {rule_counter.get('ft_hours_inconsistent_in_file', 0)} across {n_files_ft} files")

    # 4. Write the cross-row anomalies file + APPEND to the main anomalies file.
    cross_path = OUT_DIR / "salary_anomalies_cross_row.csv"
    with open(cross_path, "w", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["row_id", "cao_number", "file_name", "entry_idx",
                                          "salary_n", "field", "csv_value", "json_value",
                                          "rule", "layer"], delimiter=";")
        w.writeheader()
        w.writerows(flags)
    print(f"wrote {cross_path}  ({len(flags)} rows)")

    main_path = OUT_DIR / "salary_anomalies.csv"
    with open(main_path, "a", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["row_id", "cao_number", "file_name", "entry_idx",
                                          "salary_n", "field", "csv_value", "json_value",
                                          "rule", "layer"], delimiter=";")
        w.writerows(flags)
    print(f"appended {len(flags)} rows to {main_path}")

    # 5. Update summary file.
    summary_path = OUT_DIR / "salary_anomalies_summary.txt"
    extra = "\n=== cross-row flags (L4) ===\n"
    for r, n in rule_counter.most_common():
        extra += f"  {n:>8}  {r}\n"
    summary_path.write_text(summary_path.read_text(encoding="utf-8") + extra, encoding="utf-8")
    print("summary updated")


if __name__ == "__main__":
    scan()
