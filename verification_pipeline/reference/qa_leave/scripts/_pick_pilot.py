"""Pick 10 stratified pilot records and dump their full payloads."""
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

idx = pd.read_csv(ROOT / "outputs" / "leave_qa_payload_index.csv", sep=";", dtype=str)
viols = pd.read_csv(ROOT / "outputs" / "leave_rule_violations.csv", sep=";", dtype=str)

matched = idx[idx["source_status"] == "matched"].copy()
viol_record_ids = set(viols["record_id"].astype(str).tolist())

pilot_ids: list[str] = []

# Bucket 1: 3 records that have at least one Layer-1 violation, spread across CAOs
have_v = matched[matched["record_id"].astype(str).isin(viol_record_ids)]
seen_cao: set[str] = set()
for _, r in have_v.iterrows():
    cao = r["cao_number"]
    if cao in seen_cao:
        continue
    pilot_ids.append(str(r["record_id"]))
    seen_cao.add(cao)
    if len(pilot_ids) >= 3:
        break

# Bucket 2: 3 records by document type variety (excluding ones already picked)
remaining = matched[~matched["record_id"].astype(str).isin(pilot_ids)].copy()
for dt in ["full_cao_original", "full_cao_update", "partial_amendment_of_latest"]:
    cand = remaining[remaining["general_document_type"] == dt]
    if not cand.empty:
        pilot_ids.append(str(cand.iloc[0]["record_id"]))

# Bucket 3: 2 records from different big CAOs (Bouw=10, Beveiliging=1264) for diversity
remaining = matched[~matched["record_id"].astype(str).isin(pilot_ids)].copy()
for cao in ["10", "1264"]:
    cand = remaining[remaining["cao_number"] == cao]
    if not cand.empty:
        pilot_ids.append(str(cand.iloc[0]["record_id"]))

# Bucket 4: 2 random other records
remaining = matched[~matched["record_id"].astype(str).isin(pilot_ids)].copy()
for _, r in remaining.sample(n=2, random_state=42).iterrows():
    pilot_ids.append(str(r["record_id"]))

pilot_ids = pilot_ids[:10]
print(f"Picked {len(pilot_ids)} pilot records")
print()

# Now dump their full payloads to a single file
payload_path = ROOT / "outputs" / "leave_qa_payloads.jsonl"
pilot_path = ROOT / "outputs" / "pilot" / "pilot_payloads.jsonl"
pilot_path.parent.mkdir(parents=True, exist_ok=True)

picked: dict[str, dict] = {}
with payload_path.open("r", encoding="utf-8") as f:
    for line in f:
        o = json.loads(line)
        if str(o["record_id"]) in pilot_ids:
            picked[str(o["record_id"])] = o

# Preserve user's order
ordered = [picked[i] for i in pilot_ids if i in picked]

with pilot_path.open("w", encoding="utf-8") as f:
    for o in ordered:
        f.write(json.dumps(o, ensure_ascii=False) + "\n")

print(f"Wrote {len(ordered)} payloads to {pilot_path}")
print()
print("Pilot records selected:")
for o in ordered:
    n_filled = sum(1 for grp in o["csv_leave_fields"].values() for v in grp.values() if v not in (None, ""))
    print(
        f"  {o['record_id']:>10s}  CAO={o['cao_number']:<5s}  "
        f"doc_type={o.get('general_document_type','?'):<32s}  "
        f"ing={o.get('ingangsdatum','?'):<12s}  "
        f"src_chars={len(o.get('source_text') or ''):>5d}  "
        f"csv_fields_filled={n_filled}"
    )
