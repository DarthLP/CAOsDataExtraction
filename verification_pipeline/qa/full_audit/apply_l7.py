"""Layer 7 apply: wave2-residue suggestions (Hanna-accepted). Copy + full-provenance changelog + verify.
Changelog carries WHY (why_suggestion) and the source QUOTE per cell for traceability.
Run: python3.13 -m qa.full_audit.apply_l7
"""
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
BASE = ROOT / "qa/corrected_dataset.csv"
OUT = ROOT / "qa/corrected_dataset.l7_applied.csv"
LOG = HERE / "l7_apply_changelog.csv"

res = pd.read_csv(HERE / "wave2_residue.csv", sep=";", dtype=str, keep_default_na=False)
df = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
idx = {rid: i for i, rid in enumerate(df["id"])}


def clean_unit(u):
    u = str(u).strip()
    return u.split("(")[0].strip() if u else ""


changes, skipped = [], []


def set_cell(rid, field, newv, newu, why, quote, verdict):
    if rid not in idx or field not in df.columns:
        skipped.append((rid, field))
        return
    i = idx[rid]
    old = df.at[i, field]
    if old != newv:
        df.at[i, field] = newv
        changes.append({"record_id": rid, "field": field, "old_value": old, "new_value": newv,
                        "source": f"L7:residue:{verdict}", "why": why, "quote": str(quote)[:300]})
    uf = field[:-6] + "_unit" if field.endswith("_value") else None
    if uf and uf in df.columns and (newu or newv == ""):
        uold = df.at[i, uf]
        unew = "" if newv == "" else newu
        if unew != "" or newv == "":
            if uold != unew:
                df.at[i, uf] = unew
                changes.append({"record_id": rid, "field": uf, "old_value": uold, "new_value": unew,
                                "source": f"L7:residue:{verdict}", "why": why, "quote": str(quote)[:300]})


for _, r in res.iterrows():
    s = str(r["suggestion"]).strip()
    if s == "KEEP_DATASET" or not s:
        continue
    if s == "BLANK":
        set_cell(r.record_id, r.field, "", "", r.why_suggestion, r.quote, "BLANK")
    elif s == "APPLY_TARGET":
        set_cell(r.record_id, r.field, str(r.fix_target_value).strip(), clean_unit(r.fix_target_unit),
                 r.why_suggestion, r.quote, "APPLY_TARGET")
    elif s.startswith("ADJUST"):
        rest = s[len("ADJUST"):].strip()
        if r["kind"] == "enum":
            v, u = rest, ""
        else:
            parts = rest.split(" ", 1)
            v = parts[0]
            u = parts[1].strip() if len(parts) > 1 else ""
        set_cell(r.record_id, r.field, v, u, r.why_suggestion, r.quote, "ADJUST")
    else:
        skipped.append((r.record_id, r.field, "unparsed:" + s))

df.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")
pd.DataFrame(changes).to_csv(LOG, sep=";", index=False, encoding="utf-8-sig")
a = pd.read_csv(BASE, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
b = pd.read_csv(OUT, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig").set_index("id")
ne = int((a != b).values.sum())
print(f"applied {len(changes)} cell-changes | diff={ne} MATCH={ne == len(changes)} | skipped={len(skipped)}", skipped[:6])
