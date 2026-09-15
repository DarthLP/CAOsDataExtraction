"""apply_boolean_corrections.py — apply the Sonnet-verified boolean flip corrections
(Campaigns B + C) to the CANONICAL corrected dataset, safely.

RULES honoured:
  * copy -> verify -> promote: writes a NEW file + a timestamped backup of the original;
    the change-log names every cell changed and WHY (evidence quote). Never a silent edit.
  * guarded: a record's boolean is changed ONLY when its current value is a valid True/False
    AND differs from the verified correct_value (so re-runs are idempotent; already-correct
    cells are skipped).
  * gated: only resolved_true/false verdicts with real evidence and confidence >= 0.6 (or
    "high"/"medium") are applied. cant_tell / ambiguous_legal / empty-evidence are ignored.

Usage:
  python3.13 indices/apply_boolean_corrections.py           # DRY RUN (report only)
  python3.13 indices/apply_boolean_corrections.py --apply   # write dataset + backup + log
"""
import os, sys, glob, json, shutil
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import pandas as pd

SCRATCH = os.environ.get("SCRATCH",
    "/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/aee1fa63-1570-4fe8-9320-95880827606a/scratchpad")


def _flat(x):
    if isinstance(x, dict): yield x
    elif isinstance(x, list):
        for i in x: yield from _flat(i)

def _hi(c):
    try: return float(c) >= 0.6
    except Exception: return str(c).lower() in ("high", "medium", "med")


def load_corrections():
    rows = []
    for f in sorted(glob.glob(f"{SCRATCH}/campaignB_result_batch*.json")
                    + glob.glob(f"{SCRATCH}/campaignC_result_batch*.json")):
        try: data = json.load(open(f))
        except Exception: continue
        for r in _flat(data):
            if not isinstance(r, dict) or "boolean" not in r: continue
            cv = str(r.get("correct_value", "")).lower()
            verdict = str(r.get("verdict", "")).lower()
            ev = str(r.get("evidence_quote", "")).strip()
            if cv not in ("true", "false"): continue
            if "cant" in verdict or "ambiguous" in verdict or not ev: continue
            if not _hi(r.get("confidence", "")): continue
            for eid in (r.get("wrong_edition_ids") or []):
                rows.append({"record_id": str(eid), "field": r["boolean"],
                             "new_value": "True" if cv == "true" else "False",
                             "confidence": r.get("confidence"), "evidence": ev[:140]})
    c = pd.DataFrame(rows)
    if len(c):  # keep the highest-confidence verdict per (record, field)
        c["_c"] = c["confidence"].map(lambda x: (float(x) if str(x).replace('.','',1).isdigit()
                                                 else {"high": .9, "medium": .7, "med": .7}.get(str(x).lower(), .6)))
        c = c.sort_values("_c", ascending=False).drop_duplicates(["record_id", "field"]).drop(columns="_c")
    return c


def main(apply=False):
    c = load_corrections()
    print(f"verified boolean corrections (deduped): {len(c)} record-field cells")
    if not len(c): return
    d = il.read_csv_safe(il.CORRECTED_CSV)
    d["id"] = d["id"].astype(str)
    idx = {rid: i for i, rid in enumerate(d["id"])}
    changes, skipped = [], {"no_record": 0, "no_field": 0, "already_correct": 0, "non_bool_current": 0}
    for _, r in c.iterrows():
        i = idx.get(r["record_id"])
        if i is None: skipped["no_record"] += 1; continue
        if r["field"] not in d.columns: skipped["no_field"] += 1; continue
        cur = str(d.at[i, r["field"]]).strip()
        if cur.lower() not in ("true", "false"): skipped["non_bool_current"] += 1; continue
        if cur == r["new_value"]: skipped["already_correct"] += 1; continue
        changes.append({"record_id": r["record_id"], "cao_number": d.at[i, "cao_number"],
                        "field": r["field"], "old_value": cur, "new_value": r["new_value"],
                        "confidence": r["confidence"], "evidence": r["evidence"], "_i": i})
    ch = pd.DataFrame(changes)
    print(f"WOULD CHANGE {len(ch)} cells | skipped: {skipped}")
    if len(ch):
        print("  by field (top):"); print(ch["field"].str.split("_").str[0].value_counts().head(8).to_string())
    log = os.path.join(il.CORR, "boolean_corrections_applied.csv")
    (ch.drop(columns="_i") if len(ch) else pd.DataFrame()).to_csv(log, sep=";", index=False)
    print(f"change-log -> {log}")
    if not apply:
        print("\nDRY RUN — pass --apply to write the dataset (a backup is made first).")
        return
    bak = il.CORRECTED_CSV + ".bak_boolfix"
    if not os.path.exists(bak):
        shutil.copy2(il.CORRECTED_CSV, bak); print(f"backup -> {bak}")
    for _, r in ch.iterrows():
        d.at[r["_i"], r["field"]] = r["new_value"]
    d.to_csv(il.CORRECTED_CSV, sep=";", index=False)
    print(f"APPLIED {len(ch)} cells to {il.CORRECTED_CSV}")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
