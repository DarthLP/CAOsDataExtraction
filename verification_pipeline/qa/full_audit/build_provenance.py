"""Build the MASTER PROVENANCE file: every cell that differs from the original extraction,
with the full chain of changes (which layer, old -> new, why, source quote).

Outputs (qa/full_audit/):
  PROVENANCE_all_layers.csv — one row per (record_id, field, layer) change event, chain-ordered.
  PROVENANCE_summary.md     — coverage check: every raw-vs-canonical diff must be explained
                              by the changelog chain; orphans/supersessions reported.
Run: python3.13 -m qa.full_audit.build_provenance
"""
from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

def L(p, **kw):
    return pd.read_csv(p, sep=";", dtype=str, keep_default_na=False,
                       engine="python", on_bad_lines="skip", **kw)

LAYERS = [
    ("L1_pertopic_master_review", ROOT / "qa/apply_changelog.csv"),
    ("L2_full_audit_promotion",   HERE / "apply_changelog.csv"),
    ("L3_perfile_fix_clear",      HERE / "perfile_apply_changelog.csv"),
    ("L4_absence_removals",       HERE / "removals_apply_changelog.csv"),
    ("L5_convention_adjudication", HERE / "wave2_apply_changelog.csv"),
    ("L6_check_pattern_rulings",  HERE / "l6_apply_changelog.csv"),
    ("L7_residue_suggestions",    HERE / "l7_apply_changelog.csv"),
]

def pick(df, *names):
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([""] * len(df))

rows = []
for layer, path in LAYERS:
    if not path.exists():
        print(f"  !! missing changelog: {path}")
        continue
    df = L(path)
    if "status" in df.columns:  # L2 logs skipped/conflict rows too — keep only applied
        df = df[df["status"] == "applied"].reset_index(drop=True)
    rid = pick(df, "record_id", "id")
    fld = pick(df, "field", "target_field", "column")
    old = pick(df, "old_value", "base_actual", "old", "value_old")
    new = pick(df, "new_value", "new", "value_new")
    why = pick(df, "why", "reason", "source", "route", "note")
    quote = pick(df, "quote", "source_quote", "evidence_quote")
    for i in range(len(df)):
        rows.append({"record_id": str(rid.iloc[i]), "field": str(fld.iloc[i]),
                     "layer": layer, "old_value": str(old.iloc[i]), "new_value": str(new.iloc[i]),
                     "why": str(why.iloc[i])[:300], "quote": str(quote.iloc[i])[:300]})
    # L3 logs value+unit in one row: expand the unit change as its own event
    if layer == "L3_perfile_fix_clear" and "unit_field" in df.columns:
        for _, r in df.iterrows():
            uf, ou, nu = str(r.get("unit_field", "")), str(r.get("old_unit", "")), str(r.get("new_unit", ""))
            if uf and nu != "" and ou != nu:
                rows.append({"record_id": str(r["record_id"]), "field": uf,
                             "layer": layer, "old_value": ou, "new_value": nu,
                             "why": "paired unit of the value fix", "quote": str(r.get("quote", ""))[:300]})

prov = pd.DataFrame(rows)
order = {name: i for i, (name, _) in enumerate(LAYERS)}
prov["_o"] = prov["layer"].map(order)
prov = prov.sort_values(["record_id", "field", "_o"]).drop(columns="_o")
prov.to_csv(HERE / "PROVENANCE_all_layers.csv", sep=";", index=False, encoding="utf-8-sig")

# ---- coverage check vs ground truth (raw vs canonical) ----
raw = L(ROOT / "inputs/extracted_data_non_salary.csv").set_index("id")
cur = pd.read_csv(ROOT / "qa/corrected_dataset.csv", sep=";", dtype=str, keep_default_na=False,
                  encoding="utf-8-sig").set_index("id")
cc = [c for c in cur.columns if c in raw.columns]
ne = (cur[cc] != raw.loc[cur.index, cc])
diff_keys = {(str(r), str(c)) for r, c in zip(*[x for x in ne.values.nonzero()]) for r, c in [(cur.index[r], cc[c])]}
chain_keys = set(zip(prov["record_id"], prov["field"]))
orphans = diff_keys - chain_keys           # changed vs raw but no changelog entry
inert = chain_keys - diff_keys             # logged but final == raw (reverted/normalized back)
# chain-final consistency: last event's new_value should equal canonical
last = prov.drop_duplicates(["record_id", "field"], keep="last")
mismatch = 0
for _, r in last.iterrows():
    if r.record_id in cur.index and r.field in cur.columns:
        if str(cur.at[r.record_id, r.field]) != r.new_value:
            mismatch += 1

summary = f"""# Provenance summary — raw extraction vs canonical (G6)

- Cells differing raw -> canonical: **{int(ne.values.sum())}** across {int(ne.any(axis=1).sum())} records / {int((ne.sum(axis=0) > 0).sum())} columns
- Change events logged across L1-L7: **{len(prov)}** on {len(chain_keys)} distinct cells
- Diff cells WITHOUT a changelog entry (orphans): **{len(orphans)}**
- Logged cells whose final value returned to the raw value (net-zero chains): **{len(inert)}**
- Chain-final vs canonical mismatches (should be ~0; >0 means a later layer isn't last in file order): **{mismatch}**

Every change event carries: layer, old -> new, why, and the source quote where the applying
layer recorded one. Full per-cell chains: `PROVENANCE_all_layers.csv` (sorted record, field, layer).
Layer methodology: `docs/DATA_LINEAGE.md`. Reversal: apply each layer's changelog old_values in
reverse layer order, or restore the dated backups in `qa/_old/`.
"""
(HERE / "PROVENANCE_summary.md").write_text(summary, encoding="utf-8")
print(summary)
if orphans:
    pd.DataFrame(sorted(orphans), columns=["record_id", "field"]).to_csv(
        HERE / "provenance_orphans.csv", sep=";", index=False)
    print("orphans -> provenance_orphans.csv")
