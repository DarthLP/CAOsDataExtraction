"""
Wage indices v2 — one row per (cao_number, calendar year), built from the NEW
deterministic-parser salary dataset (100% amount provenance, per-row confidence tiers):

  CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv  (READ-ONLY)

Construction (INDICES_V2_PLAN.md §7):
  - Melt the wide salary_N_* point groups to long (one row per wage point).
  - FILTERS: confidence tier A+B only; adult rows (age_group max < 21 excluded);
    entry/aanloop rows split out (mw_entry_*), not mixed into the scale ladder.
  - UNIT normalisation to EUR/month: monthly x1, weekly x52/12, 4-week x13/12,
    annual /12, hourly x hours x 52/12 ONLY when the hours basis is known
    (point-level hours_basis_ft_week, else row ft_hours) — never a guessed workweek.
  - DE-DUP (version-aware, two-stage): per (cao, effective-date, job-cell, unit) key keep
    ALL rows of the winning edition (max kennisgeving_rank), then drop exact-value repeats.
    Pure delta editions (partial/annex/supplement) dropped.
  - TERM PRECEDENCE (2026-07-08): a wage point from term T at effective date S is dropped
    when another term of the same CAO started at I_B with T.ingangs < I_B <= S — the
    successor agreement governs S, so the expiring term's pre-announced forward table is
    stale. Dropped rows logged to mw_stale_forward_dropped.csv.
  - YEAR GRAIN (req 5): year = the wage point's own effective year (salary_N_start_date),
    NOT the file's date — wage tables carry their own dates.
  - SKILL SPLIT (req 6): worker-type labels are too sparse to be primary (~17% filled),
    so the split is by SCALE POSITION within (cao, year): p10 = low-skill floor,
    p90 = high-skill top, plus p25/median/mean/p75. A secondary per-worker_type
    table (mw_by_worker_type.csv) covers the labelled subset, honestly partial.
  - NATIONAL FLOOR: WML (wml_timeline.csv, year-average of the Jan/Jul revisions)
    + ratio columns; ratios are the cross-time comparable quantities (nominal EUR
    levels trend with inflation).
  - POOLED WAGE Z (NAMING.md): wages are scored on NOMINAL EUR levels (no WML normalisation) —
    wage_median_z (z of mw_median, the HEADLINE), wage_mean_z (z of mw_mean), wage_span_z.
    WML normalisation lives ONLY in the ratio value columns (ratio_low/median/mean/high_wml).
    Params persisted to scoring_params.csv (topic 'wage') so the statutory index can score WML.

Outputs: mw_indices.csv, mw_by_worker_type.csv, mw_indices_diagnostics.csv
Run: python3.13 indices/mw_indices.py
"""
from __future__ import annotations
import os, re, sys
from pathlib import Path
from io import BytesIO
from datetime import date
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import index_lib as il

SALARY_CSV = Path(os.path.expanduser(
    "~/Documents/Python/CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv"))
try:
    from repo_paths import SALARY_PARSER_CSV
except ImportError:
    sys.path.insert(0, str(HERE.parent))
    from repo_paths import SALARY_PARSER_CSV

SALARY_CSV = SALARY_PARSER_CSV
OUT_CSV = Path(il.OUT) / "mw_indices.csv"
CAO_WORKWEEK_CSV = Path(il.locate("cao_workweek.csv"))   # per-CAO full-time workweek (regex+agent from source; hand/agent-curated registry in review/)
STATUTORY_WORKWEEK = 36.0  # fallback when a CAO's workweek can't be found: NL statutory-minimum
                           # -hourly reference basis (36h) — the closest to a statutory workweek NL has
OUT_WT = Path(il.OUT) / "mw_by_worker_type.csv"
OUT_DIAG = Path(il.OUT) / "mw_indices_diagnostics.csv"

TIERS = {"A", "B"}
THIN_OBS = 5
BAND = (400.0, 25000.0)          # loose plausibility band on EUR/month (residual junk guard)
# Valid wage-table year band. Upper bound is a rolling horizon (this build year + 2) so
# legitimately pre-announced future wage tables are NOT silently dropped as junk. Was a
# hardcoded 2027, which would have started binning valid 2028+ tables from 2026 onward.
YEARS = (2000, date.today().year + 2)
META = ["cao_number", "file_name", "jobgroup", "step_label", "worker_type",
        "is_entry", "age_group", "ft_hours", "confidence_tier",
        "term_group", "kennisgeving_rank", "document_type"]


def _read_bytes(path: Path) -> BytesIO:
    fd = os.open(str(path), os.O_RDONLY); chunks = []
    try:
        while True:
            c = os.read(fd, 1 << 20)
            if not c: break
            chunks.append(c)
    finally:
        os.close(fd)
    return BytesIO(b"".join(chunks))


def parse_hours(s):
    v = il.parse_float(s)
    return v if (v is not None and 10 <= v <= 60) else None


_AGE_NUM = re.compile(r"\d+")
def is_youth(age_group: str) -> bool:
    """True when the row is a youth scale: every parseable age < 21 ('18', '15 t/m 20').
    '21 or older', '23 of ouder', unparseable or blank -> adult (keep)."""
    s = (age_group or "").strip()
    if not s: return False
    ages = [int(a) for a in _AGE_NUM.findall(s) if 10 <= int(a) <= 70]
    if not ages: return False
    return max(ages) < 21


def to_monthly(amount, unit, hours):
    """EUR/month or None (unknown unit / hourly without an hours basis)."""
    if amount is None: return None, "unparseable"
    u = unit.strip().lower() if isinstance(unit, str) else ""
    if u == "monthly": return amount, None
    if u == "weekly": return amount * 52.0 / 12.0, None
    if u == "4-week": return amount * 13.0 / 12.0, None
    if u == "period": return amount * 13.0 / 12.0, None  # periodesalaris = 4-weekly (13/yr)
    if u == "annual": return amount / 12.0, None
    if u == "hourly":
        if hours is None: return None, "hourly_no_basis"
        return amount * hours * 52.0 / 12.0, None
    return None, "unit_unknown"


def melt_points():
    """Long frame of wage points from the wide parser CSV (tier A+B rows only)."""
    with open(SALARY_CSV, "rb") as f:
        header = f.readline().decode("utf-8-sig").rstrip("\r\n").split(";")  # tolerate CRLF (last col)
    ns = sorted({int(m.group(1)) for c in header
                 for m in [re.match(r"salary_(\d+)_amount$", c)] if m})
    usecols = META + [f"salary_{n}_{p}" for n in ns
                      for p in ("start_date", "amount", "unit", "hours_basis_ft_week")]
    usecols = [c for c in usecols if c in header]
    df = pd.read_csv(_read_bytes(SALARY_CSV), sep=";", dtype=str, usecols=usecols)
    df = df[df["confidence_tier"].isin(TIERS)].reset_index(drop=True)
    print(f"  {len(df)} tier-A/B rows (of the parser CSV), {df['cao_number'].nunique()} CAOs, "
          f"{len(ns)} point slots")
    parts = []
    for n in ns:
        a = f"salary_{n}_amount"
        if a not in df.columns: continue
        sub = df[df[a].notna()][META + [f"salary_{n}_start_date", a, f"salary_{n}_unit",
                                        f"salary_{n}_hours_basis_ft_week"]].copy()
        if not len(sub): continue
        sub.columns = META + ["start_date", "amount", "unit", "hours_basis"]
        parts.append(sub)
    long = pd.concat(parts, ignore_index=True)
    print(f"  {len(long)} wage points melted")
    return long


def main():
    print(f"Reading {SALARY_CSV} ...")
    pts = melt_points()
    drops = {}

    # normalise to EUR/month. Hours-basis priority for hourly rows:
    #   1) per-point hours_basis  2) row ft_hours (both parser-found from the table)
    #   3) the CAO's full-time workweek (cao_workweek.csv, found in the source prose by
    #      regex+agent — a CAO-level constant applied to all its tables)
    #   4) STATUTORY_WORKWEEK (36h) when the workweek can't be found anywhere.
    # This replaces a blanket assumed workweek with each CAO's actual one. Tagged for audit.
    cao_ww = {}
    if CAO_WORKWEEK_CSV.exists():
        import csv as _csv
        for _r in _csv.DictReader(open(CAO_WORKWEEK_CSV)):
            try: cao_ww[str(_r["cao_number"])] = float(_r["ft_workweek"])
            except (TypeError, ValueError): pass
    amt = pts["amount"].map(il.parse_float)
    hrs, hsrc = [], []
    for hb, ft, cao in zip(pts["hours_basis"], pts["ft_hours"], pts["cao_number"]):
        if il.parse_float(hb):
            hrs.append(parse_hours(hb)); hsrc.append("basis")
        elif il.parse_float(ft):
            hrs.append(parse_hours(ft)); hsrc.append("ft_hours")
        elif str(cao) in cao_ww:
            hrs.append(cao_ww[str(cao)]); hsrc.append("cao_workweek")
        else:
            hrs.append(STATUTORY_WORKWEEK); hsrc.append("statutory")
    pts["hours_source"] = hsrc
    conv = [to_monthly(a, u, h) for a, u, h in zip(amt, pts["unit"], hrs)]
    pts["eur_month"] = [c[0] for c in conv]
    reasons = pd.Series([c[1] for c in conv])
    for r, n in reasons.value_counts(dropna=True).items():
        drops[f"drop_{r}"] = int(n)
    pts = pts[pts["eur_month"].notna()].copy()

    # plausibility band + year
    inband = pts["eur_month"].between(*BAND)
    drops["drop_out_of_band"] = int((~inband).sum())
    pts = pts[inband].copy()
    yr = pd.to_datetime(pts["start_date"], format="%Y-%m-%d", errors="coerce").dt.year
    pts["year"] = yr
    ok = yr.between(*YEARS)
    drops["drop_bad_year"] = int((~ok).sum())
    pts = pts[ok].copy()
    pts["year"] = pts["year"].astype(int)

    # adult / youth / entry
    pts["youth"] = pts["age_group"].fillna("").map(is_youth)
    pts["entry"] = pts["is_entry"].fillna("").eq("True")
    drops["drop_youth"] = int(pts["youth"].sum())

    # Term precedence (Hanna 2026-07-08): an edition's wage table is superseded once a LATER
    # TERM takes effect. A wage point at effective date S from term T is stale iff another term
    # of the same CAO started at I_B with T.ingangs < I_B <= S — i.e. a successor CAO was already
    # in force at S, yet we would be using the expiring term's pre-announced future raise for S.
    # Drop it; the panel/ladder forward-fills the successor term's latest table. term=(cao,ingangs),
    # parsed from term_group ("cao|dd/mm/yyyy"). Retroactive tables (S < own ingangs) are never stale.
    import bisect
    def _pd8(x):                              # date string -> YYYYMMDD int or None
        s = str(x).strip()
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
        if m: return int(m.group(1) + m.group(2) + m.group(3))
        m = re.match(r"(\d{2})[-/](\d{2})[-/](\d{4})", s)
        if m: return int(m.group(3) + m.group(2) + m.group(1))
        return None
    pts["_ing"] = pts["term_group"].map(lambda t: _pd8(str(t).split("|", 1)[1]) if "|" in str(t) else None)
    pts["_sd"] = pts["start_date"].map(_pd8)
    starts_by_cao = {c: sorted({v for v in g if v is not None})
                     for c, g in pts.groupby("cao_number")["_ing"]}
    def _stale(cao, ing, sd):
        if ing is None or sd is None: return False
        ts = starts_by_cao.get(cao, [])
        i = bisect.bisect_right(ts, sd) - 1        # governing term = latest term-start <= sd
        return i >= 0 and ts[i] > ing              # a later term governs sd -> this row is stale
    stale_mask = pts.apply(lambda r: _stale(r["cao_number"], r["_ing"], r["_sd"]), axis=1)
    drops["drop_stale_forward_term"] = int(stale_mask.sum())
    if stale_mask.any():
        pts.loc[stale_mask, ["cao_number", "file_name", "term_group", "start_date",
                             "jobgroup", "step_label", "eur_month"]].to_csv(
            Path(il.OUT) / "mw_stale_forward_dropped.csv", sep=";", index=False)
    pts = pts[~stale_mask].drop(columns=["_ing", "_sd"]).copy()

    # Version-aware de-dup (CAO_VERSION_SELECTION_PLAN.md: TAG-don't-MERGE), two-stage.
    # Most CAOs re-publish the same term several times (term_group); each edition re-states its
    # own window of wage tables -> cross-edition double-counts (incl. OCR/rounding jitter the old
    # value-based dedup let through). But a single edition can legitimately hold SEVERAL rows with
    # the same labels (36h/38h variant tables, sub-populations split only by table title), so we
    # must never collapse WITHIN a file. Rule: per (cao, effective-date, job-cell, unit) key,
    # keep ALL rows of the WINNING file (the edition with max kennisgeving_rank = most
    # consolidated); drop other files' rows for that key. Then drop only exact-value repeats.
    if "document_type" in pts.columns:   # drop pure deltas; keep full_cao_* + un-joined blanks
        is_delta = pts["document_type"].fillna("").str.startswith(
            ("partial", "annex", "other_supplement"))
        drops["drop_delta_edition"] = int(is_delta.sum())
        pts = pts[~is_delta].copy()
    before = len(pts)
    KEY = ["cao_number", "start_date", "jobgroup", "step_label", "age_group",
           "worker_type", "unit"]
    for k in KEY:                        # merge/dedup must treat missing labels as equal
        pts[k] = pts[k].fillna("")
    pts = pts.assign(_krank=pd.to_numeric(pts.get("kennisgeving_rank"),
                                          errors="coerce").fillna(0))
    winner = (pts.sort_values(["_krank", "file_name"], ascending=False)
                 .drop_duplicates(subset=KEY)[KEY + ["file_name"]]
                 .rename(columns={"file_name": "_winfile"}))
    pts = pts.merge(winner, on=KEY, how="left")
    pts = pts[pts["file_name"] == pts["_winfile"]]
    pts = pts.drop_duplicates(subset=KEY + ["eur_month"])   # exact repeats within the winner
    pts = pts.drop(columns=["_krank", "_winfile"]).copy()
    drops["dedup_removed"] = before - len(pts)
    print(f"  {len(pts)} points after unit/band/year filters + dedup "
          f"({', '.join(f'{k}={v}' for k, v in sorted(drops.items()))})")

    main_pts = pts[~pts["youth"] & ~pts["entry"]]
    entry_pts = pts[~pts["youth"] & pts["entry"]]

    def ladder(g):
        a = g["eur_month"].to_numpy()
        p10, p25, p50, p75, p90 = np.percentile(a, [10, 25, 50, 75, 90])
        return pd.Series({
            "mw_low": round(p10, 2), "mw_q25": round(p25, 2), "mw_median": round(p50, 2),
            "mw_mean": round(float(a.mean()), 2), "mw_q75": round(p75, 2),
            "mw_high": round(p90, 2),
            "mw_span_pct": round((p90 - p10) / p10 * 100.0, 2) if p10 > 0 else np.nan,
            "n_obs": len(a)})

    out = (main_pts.groupby(["cao_number", "year"], sort=True)
           .apply(ladder, include_groups=False).reset_index())
    out["n_obs"] = out["n_obs"].astype(int)
    out["flag_thin"] = out["n_obs"] < THIN_OBS
    ent = (entry_pts.groupby(["cao_number", "year"])["eur_month"]
           .agg(mw_entry_median="median", n_entry="size").round(2).reset_index())
    out = out.merge(ent, on=["cao_number", "year"], how="left")

    # WML: year average of the Jan/Jul statutory revisions + ratios
    wml = il.load_wml()
    def wml_year(y):
        vals = [v for v in (il.wml_at(wml, date(y, 1, 1)), il.wml_at(wml, date(y, 7, 1))) if v]
        return round(sum(vals) / len(vals), 2) if vals else None
    out["wml_month"] = out["year"].map(wml_year)
    for num, name in [("mw_low", "ratio_low_wml"), ("mw_median", "ratio_median_wml"),
                      ("mw_mean", "ratio_mean_wml"), ("mw_high", "ratio_high_wml"),
                      ("mw_entry_median", "ratio_entry_wml")]:
        out[name] = (out[num] / out["wml_month"]).round(4)

    # pooled wage z (all cao-year cells are the pool; params persisted for statutory)
    allmask = pd.Series(True, index=out.index)
    params = []
    for src, zcol, canon in [("mw_median", "wage_median_z", "eur_month"),   # nominal median = headline
                             ("mw_mean", "wage_mean_z", "eur_month"),        # nominal mean
                             ("mw_span_pct", "wage_span_z", "pct")]:
        x = pd.to_numeric(out[src], errors="coerce")
        p = il.pooled_params(x, allmask)
        out[zcol] = il.pooled_z(x, p).round(4)
        e = {"field": src, "variant": "full", "canonical": canon, "sign": 1,
             "winsor_lo": "", "winsor_hi": "", "mu": "", "sd": "", "n": 0}
        if p: e.update({"winsor_lo": p["lo"], "winsor_hi": p["hi"], "mu": p["mu"],
                        "sd": p["sd"], "n": p["n"]})
        params.append(e)
    il.save_params("wage", params)

    out.to_csv(OUT_CSV, sep=";", index=False)
    print(f"Wrote {OUT_CSV}  ({len(out)} rows, {len(out.columns)} cols)")

    # secondary: labelled worker-type split (honestly partial coverage)
    wt = main_pts[main_pts["worker_type"].fillna("").str.strip() != ""]
    wt_out = (wt.groupby(["cao_number", "year", "worker_type"])["eur_month"]
              .agg(median="median", mean="mean", n="size").round(2).reset_index())
    wt_out.to_csv(OUT_WT, sep=";", index=False)
    print(f"Wrote {OUT_WT}  ({len(wt_out)} rows; labelled share of points = "
          f"{len(wt)/max(len(main_pts),1):.3f})")

    diag_rows = [
        ("n_cao_year_rows", len(out)), ("n_unique_caos", out["cao_number"].nunique()),
        ("year_min", int(out["year"].min())), ("year_max", int(out["year"].max())),
        ("n_points_used", int(out["n_obs"].sum())),
        ("median_mw_low", round(out["mw_low"].median(), 2)),
        ("median_mw_mean", round(out["mw_mean"].median(), 2)),
        ("median_mw_high", round(out["mw_high"].median(), 2)),
        ("median_ratio_low_wml", round(out["ratio_low_wml"].median(), 3)),
        ("median_ratio_mean_wml", round(out["ratio_mean_wml"].median(), 3)),
        ("share_flag_thin", round(out["flag_thin"].mean(), 4)),
        ("n_entry_cells", int(out["n_entry"].notna().sum())),
        ("labelled_worker_type_share", round(len(wt)/max(len(main_pts),1), 4)),
    ] + sorted(drops.items())
    diag = pd.DataFrame(diag_rows, columns=["metric", "value"])
    diag.to_csv(OUT_DIAG, sep=";", index=False)
    print(diag.to_string(index=False))


if __name__ == "__main__":
    main()
