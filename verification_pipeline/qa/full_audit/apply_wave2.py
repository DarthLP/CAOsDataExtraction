"""Layer 5 apply: FIX_audit/surcharge/statutory adjudications (Haiku, sonnet-spot-verified)
+ Hanna's numeric rulings (DELETE patterns + 1-month conditional). Writes a COPY
(corrected_dataset.wave2_applied.csv) + reversible changelog. Run: python3.13 -m qa.full_audit.apply_wave2
"""
import json, sys
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SB = Path("/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/39c3970b-a39f-43eb-8cb6-0474c11fbfaa/scratchpad")
BASE = ROOT/"qa/corrected_dataset.csv"
OUT  = ROOT/"qa/corrected_dataset.wave2_applied.csv"
LOG  = HERE/"wave2_apply_changelog.csv"

def L(p): return pd.read_csv(p, sep=";", dtype=str, keep_default_na=False, engine="python", on_bad_lines="skip")
ad = L(SB/"fixaudit/adjudicated_fixes.csv")
# exclude the 3 spot-verify disagreements
spot = json.load(open(SB/"fixaudit/spot/verdicts.json"))
dis = {(v["record_id"], v["field"]) for v in spot["verdicts"] if v["check"]=="DISAGREE"}
ad = ad[~ad.apply(lambda r: (r.record_id, r.field) in dis, axis=1)]
nu = L(HERE/"numerics_resolution.csv")

df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
idx = {rid: i for i, rid in enumerate(df["id"])}
changes, skipped = [], []
def set_cell(rid, field, newv, newu, src, oldv_expect=None):
    if rid not in idx or field not in df.columns:
        skipped.append((rid, field, "missing row/col")); return
    i = idx[rid]
    old = df.at[i, field]
    if old == newv: return  # no-op
    df.at[i, field] = newv
    changes.append({"record_id": rid, "field": field, "old_value": old, "new_value": newv, "source": src})
    ufield = field[:-6] + "_unit" if field.endswith("_value") else (field + "_unit" if field+"_unit" in df.columns else None)
    if newu and ufield and ufield in df.columns:
        uold = df.at[i, ufield]
        if uold != newu:
            df.at[i, ufield] = newu
            changes.append({"record_id": rid, "field": ufield, "old_value": uold, "new_value": newu, "source": src})

def clean(x):
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s
for _, r in ad.iterrows():
    newv = clean(r["final_value"])
    newu = clean(r["final_unit"])
    if not newv and r["kind"] != "boolean":
        skipped.append((r["record_id"], r["field"], "empty final_value")); continue
    set_cell(r["record_id"], r["field"], newv, newu, f"L5:{r['bucket']}:{r['verdict']}")
for _, r in nu.iterrows():
    set_cell(r["record_id"], r["field"], r["new_value"], r["new_unit"], f"L5:{r['ruling']}")

df.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")
pd.DataFrame(changes).to_csv(LOG, sep=";", index=False, encoding="utf-8-sig")
# verify: diff vs base == changelog
a = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
b = pd.read_csv(OUT, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
ne = (a != b)
print(f"applied {len(changes)} cell-changes -> {OUT.name}")
print(f"diff vs base = {int(ne.values.sum())} | changelog = {len(changes)} | MATCH={int(ne.values.sum())==len(changes)}")
print(f"rows {b.shape[0]} (== base {a.shape[0]}) | skipped: {len(skipped)}")
for s in skipped[:8]: print("  SKIP", s)
