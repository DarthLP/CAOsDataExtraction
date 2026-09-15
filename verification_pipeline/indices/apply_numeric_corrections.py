"""apply_numeric_corrections.py — aggregate the Sonnet-verified NUMERIC same-term regression
fixes (numbatch_*_result.json) and apply them to the CANONICAL corrected dataset, safely.

This is the numeric analogue of apply_boolean_corrections.py, and the SECOND numeric pass
(the first was the 10 hand-picked regression_review fixes = L8). Scope: the 668 non-leave/
non-overall SAME_TERM_republication discrepancy pairs, each source-verified by a Sonnet agent
that classified the discrepancy (A_error / B_error / both_wrong / unit_diff / ambiguous_multi /
both_ok / cant_tell) and proposed a fix ONLY for the first three.

GATE (strict — numerics are noisy; the L4 lesson was 83% of numeric same-term diffs are
legitimate/convention, not errors):
  * verdict in {A_error, B_error, both_wrong}
  * a non-empty fix list with an explicit new_value
  * confidence in {high, med}  (low dropped)
  * non-empty evidence_quote
  * current cell value is numeric AND differs from new_value (idempotent re-runs)

copy -> verify -> promote: a timestamped backup is written before any edit; the change-log
names every cell + why (evidence quote). Reversible.

Usage:
  python3.13 indices/apply_numeric_corrections.py           # DRY RUN (report + sheet only)
  python3.13 indices/apply_numeric_corrections.py --apply   # write dataset + backup + log
"""
import os, sys, glob, json, shutil
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import pandas as pd

SCRATCH = os.environ.get("SCRATCH",
    "/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/aee1fa63-1570-4fe8-9320-95880827606a/scratchpad")
FIX_VERDICTS = {"a_error", "b_error", "both_wrong"}
OK_CONF = {"high", "med", "medium"}


def _num(x):
    try:
        float(str(x)); return True
    except Exception:
        return False


def load_results():
    rows = []
    for f in sorted(glob.glob(f"{SCRATCH}/numbatch_*_result.json")):
        try:
            data = json.load(open(f))
        except Exception:
            print(f"  ! unreadable {os.path.basename(f)}"); continue
        if not isinstance(data, list):
            data = [data]
        for r in data:
            if not isinstance(r, dict):
                continue
            verdict = str(r.get("verdict", "")).lower().strip()
            conf = str(r.get("confidence", "")).lower().strip()
            ev = str(r.get("evidence_quote", "")).strip()
            field = str(r.get("field", "")).strip()
            for fx in (r.get("fix") or []):
                if not isinstance(fx, dict):
                    continue
                rid = str(fx.get("id", "")).strip()
                nv = fx.get("new_value")
                rows.append({"record_id": rid, "field": field, "new_value": nv,
                             "verdict": verdict, "confidence": conf, "evidence": ev[:160],
                             "pair_id": r.get("pair_id", ""), "batch": os.path.basename(f)})
    return pd.DataFrame(rows)


def main(apply=False):
    r = load_results()
    print(f"raw fix rows from results: {len(r)}")
    if not len(r):
        print("no results found yet."); return
    # gate
    g = r[r["verdict"].isin(FIX_VERDICTS) & r["confidence"].isin(OK_CONF)
          & r["evidence"].str.len().gt(0) & r["new_value"].map(_num)].copy()
    g["new_value"] = g["new_value"].map(lambda v: str(v).strip())
    # dedupe: one fix per (record, field) — prefer high over med
    g["_c"] = g["confidence"].map(lambda c: 2 if c == "high" else 1)
    g = g.sort_values("_c", ascending=False).drop_duplicates(["record_id", "field"]).drop(columns="_c")
    print(f"gated fixes (verdict+conf+evidence+numeric): {len(g)}")
    print("verdict mix (all rows):"); print(r["verdict"].value_counts().to_string())

    d = il.read_csv_safe(il.CORRECTED_CSV)
    d["id"] = d["id"].astype(str)
    idx = {rid: i for i, rid in enumerate(d["id"])}
    changes, skip = [], {"no_record": 0, "no_field": 0, "current_nonnumeric": 0, "already": 0}
    for _, x in g.iterrows():
        i = idx.get(x["record_id"])
        if i is None: skip["no_record"] += 1; continue
        if x["field"] not in d.columns: skip["no_field"] += 1; continue
        cur = str(d.at[i, x["field"]]).strip()
        if not _num(cur): skip["current_nonnumeric"] += 1; continue
        if float(cur) == float(x["new_value"]): skip["already"] += 1; continue
        changes.append({"record_id": x["record_id"], "cao_number": d.at[i, "cao_number"],
                        "field": x["field"], "old_value": cur, "new_value": x["new_value"],
                        "verdict": x["verdict"], "confidence": x["confidence"],
                        "evidence": x["evidence"], "_i": i})
    ch = pd.DataFrame(changes)
    print(f"\nWOULD CHANGE {len(ch)} cells | skipped: {skip}")
    if len(ch):
        print("by field:"); print(ch["field"].value_counts().head(20).to_string())
        print("\nsample (record | field | old -> new | conf | evidence):")
        for _, c in ch.head(25).iterrows():
            print(f"  {c['record_id']} | {c['field']} | {c['old_value']} -> {c['new_value']} "
                  f"| {c['confidence']} | {c['evidence'][:70]}")
    log = os.path.join(il.CORR, "numeric_corrections_v2_applied.csv")
    (ch.drop(columns="_i") if len(ch) else pd.DataFrame()).to_csv(log, sep=";", index=False)
    print(f"\nchange-log -> {log}")
    if not apply:
        print("DRY RUN — pass --apply to write the dataset (backup made first).")
        return
    if not len(ch):
        print("nothing to apply."); return
    bak = il.CORRECTED_CSV + ".bak_numfix2"
    if not os.path.exists(bak):
        shutil.copy2(il.CORRECTED_CSV, bak); print(f"backup -> {bak}")
    for _, c in ch.iterrows():
        d.at[c["_i"], c["field"]] = c["new_value"]
    d.to_csv(il.CORRECTED_CSV, sep=";", index=False)
    print(f"APPLIED {len(ch)} cells to {il.CORRECTED_CSV}")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
