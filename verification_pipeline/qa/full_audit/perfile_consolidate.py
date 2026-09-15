"""Consolidate a topic's per-file verification results into proposed actions.

Reads perfile_work/<topic>/results/*.json (subagent outputs) + _truth.json, and for
each (record, field) compares what the subagent independently extracted from THAT
record's own file to what the dataset stores — UNIT-AWARE, so the same entitlement in
a different unit (e.g. 2 years == 104 weeks, 150% == 1.5x) is NOT a false error.

Proposed action per (record, field):
  EMPTY    — every file is silent on a field the dataset populated (wrong-slot/contam.)
  FIX      — this record's own file states a DIFFERENT quantity than the dataset holds
  RECONCILE— this record's file is silent but a fuller sibling carries it (agreement value)
  CONFIRM  — dataset matches the source (after unit-normalization)
  REVIEW   — UNSURE / units not comparable / can't settle

SURFACE-ONLY: writes perfile_work/<topic>/actions.csv + prints a summary. Applies nothing.

Run: python3.13 -m qa.full_audit.perfile_consolidate --topic leave
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
WORK = HERE / "perfile_work"

# time bases -> weeks (so years/months/weeks/days are comparable; week=7 days, year=52 wk)
_TIME_MULT = [("year", 52.0), ("jaar", 52.0), ("jaren", 52.0), ("annual", 52.0),
              ("month", 52.0 / 12), ("maand", 52.0 / 12),
              ("week", 1.0), ("weken", 1.0),
              ("day", 1.0 / 7), ("dag", 1.0 / 7), ("dagen", 1.0 / 7)]


def _dsmap(vm):
    out = {}
    for chunk in str(vm).split(";"):
        m = re.match(r"\s*(.+?)=\[(.*)\]\s*$", chunk)
        if not m:
            continue
        for rid in re.findall(r"(\d+)\(", m.group(2)):
            out[rid] = m.group(1).strip()
    return out


def _qty(val, unit):
    """(dimension, normalized_value) or None. Time -> weeks, percent -> fraction."""
    f = common.to_float(val)
    if f is None:
        return None
    cat = common.unit_category(unit)
    u = str(unit).lower()
    if cat in ("weeks", "months", "years", "days"):
        for k, mlt in _TIME_MULT:
            if k in u:
                return ("time", f * mlt)
        return ("time?", f)            # time-ish but no recognizable base
    if cat == "percent":
        return ("ratio", f / 100.0)
    if cat == "hours":
        return ("hours", f)
    if cat == "money":
        return ("money", f)
    if cat in ("fte", "distance", "count"):
        return (cat, f)
    if cat == "":
        return ("nounit", f)
    return ("other", f)


_SUR_MARK = ("surcharge", "toeslag", "extra", "bovenop", "premium", "increment",
             "on top", "op het uur", "boven het")
_TOT_MARK = ("of hourly", "of the hourly", "of hourly rate", "of wage", "of salary",
             "of base", "van het uur", "of the rate")


def _surcharge_total_equiv(dv, du, sv, su, tol):
    """25% SURCHARGE on the rate == 125% OF the rate. Fires only when exactly one
    side is explicitly a surcharge/increment and the other explicitly a total."""
    a, b = common.to_float(dv), common.to_float(sv)
    if a is None or b is None:
        return False
    du_, su_ = str(du).lower(), str(su).lower()
    d_sur, s_sur = any(m in du_ for m in _SUR_MARK), any(m in su_ for m in _SUR_MARK)
    d_tot, s_tot = any(m in du_ for m in _TOT_MARK), any(m in su_ for m in _TOT_MARK)
    if d_sur and s_tot:
        return abs((100 + a) - b) <= tol * max(100 + a, b, 1e-9)
    if s_sur and d_tot:
        return abs((100 + b) - a) <= tol * max(100 + b, a, 1e-9)
    return False


def _cmp(dv, du, sv, su, tol=0.05):
    """'match' | 'differ' | 'unknown' — unit-aware quantity comparison."""
    a, b = _qty(dv, du), _qty(sv, su)
    if a is None or b is None:
        return "unknown"
    (da, va), (db, vb) = a, b
    if da == "ratio" and db == "ratio" and _surcharge_total_equiv(dv, du, sv, su, tol):
        return "match"
    eq = abs(va - vb) <= tol * max(abs(va), abs(vb), 1e-9)
    if da == db:
        return "match" if eq else "differ"
    # different dimensions: if one side has no/unknown unit, only the raw numbers can
    # hint -> match if equal, else 'unknown' (don't assert a FIX we can't trust)
    if "nounit" in (da, db) or "time?" in (da, db):
        return "match" if eq else "unknown"
    return "differ"                    # clearly different dimensions = different quantity


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True)
    a = ap.parse_args()
    tdir = WORK / a.topic
    truth = {t["uid"]: t for t in json.loads((tdir / "_truth.json").read_text(encoding="utf-8"))}
    info = common.classify_columns()
    byid = {r["id"]: r for r in pd.read_csv(common.PROJECT_ROOT / "qa" / "corrected_dataset.csv",
            sep=";", dtype=str, keep_default_na=False).to_dict("records")}

    rows, tally = [], Counter()
    done = 0
    for t_uid, t in truth.items():
        rp = tdir / "results" / f"{t_uid}.json"
        if not rp.exists():
            continue
        done += 1
        try:
            res = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            tally["PARSE_FAIL"] += 1
            continue
        pf = {}
        for e in res.get("per_file", []):
            pf.setdefault(e["field"], {})[str(e["record_id"])] = e
        pfd = {e["field"]: e for e in res.get("per_field", [])}

        for fdef in t["fields"]:
            field = fdef["field"]
            ci = info.get(field)
            ucol = ci.unit_partner if ci else None
            is_num = bool(ci and ci.kind == "numeric")
            is_bool = bool(ci and ci.kind == "boolean")
            dm = _dsmap(fdef["dataset_value_map"])
            verdict = pfd.get(field, {})
            assess = verdict.get("assessment", "MISSING")
            agr = str(verdict.get("agreement_level_value", "")).strip()
            agru = str(verdict.get("agreement_level_unit", "")).strip()
            # Fallback: if the agent left agreement_level_value blank, derive it from
            # the sibling records' OWN stated extractions (modal stated value). Recovers
            # SILENT_SIBLINGS reconcile targets that would otherwise drop to REVIEW.
            if common.is_blank(agr):
                cnt = Counter()
                for e in pf.get(field, {}).values():
                    cv = str(e.get("value", "")).strip()
                    if e.get("stated") and cv and not common.is_blank(cv):
                        cnt[cv] += 1
                if cnt:
                    agr = cnt.most_common(1)[0][0]
                    if common.is_blank(agru):
                        agru = next((str(e.get("unit", "")).strip() for e in pf.get(field, {}).values()
                                     if str(e.get("value", "")).strip() == agr), "")
            # Booleans: the agreement value is decided by EVIDENCE — True if any version
            # asserts the provision, else False (absence is a real verdict). This fixes
            # all-silent presence-booleans (REVIEW -> CONFIRM/RECONCILE) WITHOUT flipping
            # a thin re-filing that is merely silent on a provision the agreement has.
            if is_bool:
                _bv = [str(e.get("value", "")).strip().lower() for e in pf.get(field, {}).values()]
                agr, agru = (("True", "") if any(v == "true" for v in _bv) else ("False", ""))
            # did ANY version positively state this field? If so, a silent record's
            # value must NOT be inherited from that sibling (never assume silence=same).
            any_positive = any(e.get("stated") for e in pf.get(field, {}).values())
            for rid, dv in dm.items():
                rec = byid.get(rid, {})
                dunit = common.norm(rec.get(ucol, "")) if ucol else ""
                src = pf.get(field, {}).get(rid, {})
                stated = bool(src.get("stated", False))
                sval = str(src.get("value", "")).strip()
                sunit = str(src.get("unit", "")).strip()

                if assess == "TEMPORAL_CHANGE":
                    action = "CONFIRM"
                elif assess in ("UNSURE", "MISSING"):
                    action = "REVIEW"
                elif not stated:
                    if common.is_blank(dv):
                        action = "CONFIRM"
                    elif assess == "CONSISTENT" and common.is_blank(agr):
                        action = "EMPTY"                 # all files silent -> spurious value
                    elif common.is_blank(agr):
                        action = "REVIEW"
                    elif is_num:                         # silent here; does dataset match the agreement?
                        c = _cmp(dv, dunit, agr, agru)
                        # 'differ' + a sibling stated it => would be sibling-inherited => KEEP old value
                        action = {"match": "CONFIRM",
                                  "differ": ("KEEP" if any_positive else "RECONCILE"),
                                  "unknown": "REVIEW"}[c]
                    elif str(dv).strip().lower() == str(agr).strip().lower():
                        action = "CONFIRM"
                    else:
                        # silent record disagrees with the agreement value:
                        #  sibling asserted it -> KEEP the record's own (old) value (never inherit)
                        #  NO version asserts it -> RECONCILE (remove a value no source supports)
                        action = "KEEP" if any_positive else "RECONCILE"
                else:
                    # this record's own file states a value -> compare to dataset
                    if is_num:
                        c = _cmp(dv, dunit, sval, sunit)
                        action = {"match": "CONFIRM", "differ": "FIX", "unknown": "REVIEW"}[c]
                    else:
                        sc = str(sval).strip().lower()
                        dc = str(dv).strip().lower()
                        action = "CONFIRM" if sc == dc else "FIX"
                tally[action] += 1
                rows.append({
                    "topic": a.topic, "uid": t_uid, "agreement_id": t["agreement_id"],
                    "record_id": rid, "field": field, "kind": fdef["kind"],
                    "dataset_value": dv, "dataset_unit": dunit,
                    "source_stated": stated, "source_value": sval, "source_unit": sunit,
                    "assessment": assess, "agreement_level_value": agr,
                    "agreement_level_unit": agru, "proposed_action": action,
                    "fix_target_value": (sval if action == "FIX" else
                                         (agr if action == "RECONCILE" else "")),
                    "fix_target_unit": (sunit if action == "FIX" else
                                        (agru if action == "RECONCILE" else "")),
                    "explanation": verdict.get("explanation", ""),
                    "quote": str(src.get("quote", ""))[:200],
                })
    out = tdir / "actions.csv"
    pd.DataFrame(rows).to_csv(out, sep=";", index=False)
    print(f"[perfile_consolidate] topic={a.topic} units_done={done}/{len(truth)} "
          f"record-cells={len(rows)}")
    print(f"  actions: {dict(tally)}")
    print(f"  -> {out}")


if __name__ == "__main__":
    main()
