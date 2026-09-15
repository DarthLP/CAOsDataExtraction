"""Layer 6 apply: CHECK-pattern resolutions (Hanna-accepted suggestions). Copy + changelog + verify."""
from pathlib import Path
import pandas as pd
HERE = Path(__file__).resolve().parent; ROOT = HERE.parent.parent
BASE = ROOT/"qa/corrected_dataset.csv"; OUT = ROOT/"qa/corrected_dataset.l6_applied.csv"; LOG = HERE/"l6_apply_changelog.csv"
res = pd.read_csv(HERE/"check_resolution.csv", sep=";", dtype=str, keep_default_na=False)
df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
idx = {rid: i for i, rid in enumerate(df["id"])}
changes, skipped = [], []
for _, r in res.iterrows():
    if r.action == "KEEP": continue
    rid, field = r.record_id, r.field
    if rid not in idx or field not in df.columns: skipped.append((rid, field)); continue
    i = idx[rid]
    newv = "" if r.action == "BLANK" else r.new_value
    old = df.at[i, field]
    if old != newv:
        df.at[i, field] = newv
        changes.append({"record_id": rid, "field": field, "old_value": old, "new_value": newv, "source": r.ruling})
    if r.action == "BLANK" or (r.action == "SET" and r.new_unit):
        uf = field[:-6]+"_unit" if field.endswith("_value") else field+"_unit"
        if uf in df.columns:
            uold = df.at[i, uf]; unew = "" if r.action=="BLANK" else r.new_unit
            if uold != unew:
                df.at[i, uf] = unew
                changes.append({"record_id": rid, "field": uf, "old_value": uold, "new_value": unew, "source": r.ruling})
df.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")
pd.DataFrame(changes).to_csv(LOG, sep=";", index=False, encoding="utf-8-sig")
a = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
b = pd.read_csv(OUT, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
ne = int((a != b).values.sum())
print(f"applied {len(changes)} cell-changes | diff={ne} MATCH={ne==len(changes)} | skipped={len(skipped)}", skipped[:5])
