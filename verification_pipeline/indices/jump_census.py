"""Overall-index jump census (durable replacement for the campaign's scratchpad final_census.py).

Scans composite_index.csv for consecutive-edition OVERALL_Z events per CAO:
  - drops <= -0.4
  - V-dips: drop <= -0.3 followed by a rebound >= +0.3 within the next 2 editions
  - Lambda-spikes: rise >= +0.5 followed by a drop <= -0.5 (mirror of a V)
and reports whether each event's dip-side record is covered by the campaign
verification trail (adjudication ledger / big_drops_attribution / thin-doc registry).
End-state 2026-07-13: 1 drop (CAO 65 remplaçanten series switch, PDF-verified genuine),
0 V-dips, 0 uncovered. Run: python3 jump_census.py
"""
import os
import sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il

def run():
    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    comp["overall_z"] = pd.to_numeric(comp["overall_z"], errors="coerce")
    comp["_fd"] = comp["file_date"].map(il.parse_date)
    cs = comp.dropna(subset=["_fd", "overall_z"]).sort_values(["cao_number", "_fd", "id"])

    covered = set()
    led_p = il.locate("jump_campaign_adjudications.csv")
    if os.path.exists(led_p):
        covered |= set(pd.read_csv(led_p, sep=";", dtype=str)["record_id"].astype(str))
    bda_p = il.locate("big_drops_attribution.csv")
    if os.path.exists(bda_p):
        b = il.read_csv_safe(bda_p)
        for c in ("id_dip", "id", "record_id"):
            if c in b.columns:
                covered |= set(b[c].astype(str)); break
    reg_p = il.locate("thin_docs_reviewed.csv")
    if os.path.exists(reg_p):
        covered |= set(il.read_csv_safe(reg_p)["id"].astype(str))

    topics = [c for c in comp.columns if c.endswith("_z") and c not in
              ("overall_z",) and not c.endswith("_numeric_z") and "var" not in c]
    drops, vdips, spikes = [], [], []
    for cao, g in cs.groupby("cao_number"):
        g = g.reset_index(drop=True)
        z = g["overall_z"]
        for i in range(1, len(g)):
            d1 = z[i] - z[i - 1]
            rec = {"cao": cao, "id_prev": g.loc[i - 1, "id"], "id_ev": g.loc[i, "id"],
                   "delta": round(float(d1), 2), "covered": str(g.loc[i, "id"]) in covered}
            if d1 <= -0.4:
                dz = {t: pd.to_numeric(g.loc[i, t], errors="coerce") -
                          pd.to_numeric(g.loc[i - 1, t], errors="coerce") for t in topics}
                dz = {k: v for k, v in dz.items() if pd.notna(v)}
                if dz:
                    drv = min(dz, key=dz.get)
                    rec["driver"], rec["driver_d"] = drv, round(float(dz[drv]), 2)
                drops.append(rec)
            if d1 <= -0.3:
                for j in range(i + 1, min(i + 3, len(g))):
                    if z[j] - z[i] >= 0.3:
                        vdips.append({**rec, "rebound": round(float(z[j] - z[i]), 2)}); break
            if d1 >= 0.5:
                for j in range(i + 1, min(i + 3, len(g))):
                    if z[j] - z[i] <= -0.5:
                        spikes.append({**rec, "fall": round(float(z[j] - z[i]), 2)}); break
    # a V-dip that is also a listed drop is reported once, as the drop
    drop_keys = {(d["cao"], d["id_ev"]) for d in drops}
    vdips = [v for v in vdips if (v["cao"], v["id_ev"]) not in drop_keys]
    return drops, vdips, spikes

if __name__ == "__main__":
    drops, vdips, spikes = run()
    unc = [d for d in drops + vdips + spikes if not d["covered"]]
    print(f"census: {len(drops)} drops <= -0.4 | {len(vdips)} extra V-dips | "
          f"{len(spikes)} Lambda-spikes | uncovered by verification trail: {len(unc)}")
    for d in drops:
        print("  DROP:", d)
    for v in vdips:
        print("  VDIP:", v)
    for s in spikes:
        print("  SPIKE:", s)
