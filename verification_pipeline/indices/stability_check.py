"""
STABILITY CHECK — do z-scores move "weirdly" between a CAO's documents, and WHY?

Movement is measured on the RAW / EXTRACTION-ONLY variants (leave_z_extr, <topic>_z_raw),
which carry NO statutory imputation — so era-driven "law steps" are excluded and every
remaining jump is either a genuine renegotiation or extraction noise.

Each consecutive document pair within a CAO (ordered by file_date) is classified:
  SAME_TERM  — same term_group (cao + ingangsdatum): a re-published EDITION of the SAME
               agreement. Non-salary terms are term-stable, so a jump here is EXTRACTION
               NOISE (a flipped/renumbered value in a re-issue).
  NEW_TERM   — a different term_group: a NEW contract (new bargaining round). A jump here
               can be a REAL renegotiation.
For every flagged pair the DRIVER is named: the input variable whose (normalised) value
changed most between the two documents, with its before/after values.

Outputs:
  stability_summary.csv     per score: within-term noise + consecutive-jump stats (raw variants)
  stability_offenders.csv    flagged pairs (|Δ raw z| > 0.5): topic, relation, driver, values
Run: python3.13 indices/stability_check.py
"""
import os, sys, importlib
import numpy as np
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

# topic -> (index csv, raw-z column, {input short col: label})
TOPICS = {}
for t in ["term", "contract", "overtime", "training", "bonus", "fringe", "homeoffice", "pension", "absence"]:
    m = importlib.import_module(f"{t}_index")
    shorts = list(getattr(m, "SHORT", {}).values())
    TOPICS[t] = (f"{t}_index.csv", f"{t}_numeric_z_raw", shorts)
TOPICS["leave"] = ("parental_leave_index.csv", "leave_numeric_z_extr",
                   ["maternity_fre_extr", "paternity_fre_extr", "adoption_fre_extr", "parental_fre_extr"])

JUMP = 0.5


def main():
    # assemble a per-doc frame: id, cao, term_group, file_date + every raw z + every driver col
    base = il.read_csv_safe(il.locate("term_index.csv"))[
        ["id", "cao_number", "term_group", "file_date", "ingangsdatum"]].copy()
    frames = {"base": base}
    for t, (csv, zcol, shorts) in TOPICS.items():
        d = il.read_csv_safe(il.locate(csv))
        keep = ["id"] + [c for c in [zcol] + shorts if c in d.columns]
        frames[t] = d[keep].rename(columns={zcol: f"{t}__z"})
    df = base
    for t in TOPICS:
        df = df.merge(frames[t], on="id", how="left")
    df["_fd"] = df["file_date"].map(il.parse_date)
    df = df.dropna(subset=["_fd"]).sort_values(["cao_number", "_fd", "id"]).reset_index(drop=True)
    # OVERALL raw score = mean of the per-topic raw z's (statutory-free, same era-exclusion)
    _tcols = [f"{t}__z" for t in TOPICS]
    df["overall__z"] = df[_tcols].apply(pd.to_numeric, errors="coerce").mean(axis=1)

    # ---- within-term noise summary (same agreement editions, raw variants) ----
    summ = []
    for t in TOPICS:
        z = pd.to_numeric(df[f"{t}__z"], errors="coerce")
        g = df.assign(_z=z).groupby("term_group")["_z"]
        rng = (g.max() - g.min())[g.count() >= 2].dropna()
        d = df.assign(_z=z).groupby("cao_number")["_z"].diff().abs().dropna()
        summ.append({"score": f"{t}_z_raw", "n_multi_edition_terms": int(len(rng)),
                     "term_range_median": round(rng.median(), 3) if len(rng) else None,
                     "term_range_p90": round(rng.quantile(.9), 3) if len(rng) else None,
                     "share_terms_gt_0.5": round((rng > .5).mean(), 3) if len(rng) else None,
                     "n_pairs": int(len(d)), "jump_p90": round(d.quantile(.9), 3) if len(d) else None,
                     "share_jumps_gt_0.5": round((d > .5).mean(), 3) if len(d) else None})
    pd.DataFrame(summ).to_csv(os.path.join(il.OUT, "stability_summary.csv"), sep=";", index=False)

    # ---- enriched consecutive-pair offenders (incl. OVERALL; direction + regression flags) ----
    # DIRECTION: a CAO's package should trend BETTER over time. A pair that WORSENED (delta<0) is
    # flagged; a SAME_TERM re-issue that worsened is the most suspicious (a re-published edition of
    # the SAME agreement should not change — a material drop is almost certainly extraction noise
    # in the newer file, worth manual review). Statutory era-steps are already excluded (raw z's).
    ITER = list(TOPICS.items()) + [("overall", (None, "overall__z", _tcols))]
    off = []
    for cao, g in df.groupby("cao_number"):
        g = g.reset_index(drop=True)
        for i in range(len(g) - 1):
            a, b = g.iloc[i], g.iloc[i + 1]
            same = a["term_group"] == b["term_group"]
            rel = "SAME_TERM_republication" if same else "NEW_TERM_contract"
            for t, (csv, zcol, shorts) in ITER:
                za, zb = pd.to_numeric(a[f"{t}__z"], errors="coerce"), pd.to_numeric(b[f"{t}__z"], errors="coerce")
                if pd.isna(za) or pd.isna(zb) or abs(zb - za) <= JUMP:
                    continue
                # driver = input column (or, for 'overall', the topic z) with the largest change
                drv, dv0, dv1, best = "", "", "", -1.0
                for col in shorts:
                    if col not in g.columns: continue
                    va, vb = pd.to_numeric(a.get(col), errors="coerce"), pd.to_numeric(b.get(col), errors="coerce")
                    if pd.isna(va) and pd.isna(vb): continue
                    scale = np.nanmax([abs(va) if not pd.isna(va) else 0,
                                       abs(vb) if not pd.isna(vb) else 0, 1.0])
                    chg = abs((0 if pd.isna(vb) else vb) - (0 if pd.isna(va) else va)) / scale
                    if chg > best:
                        best, drv = chg, col.replace("__z", "") if t == "overall" else col
                        dv0 = "" if pd.isna(va) else round(float(va), 2)
                        dv1 = "" if pd.isna(vb) else round(float(vb), 2)
                worsened = zb < za
                off.append({"topic": t, "cao_number": cao, "relation": rel,
                            "delta_raw_z": round(float(zb - za), 3),
                            "direction": "worsened" if worsened else "improved",
                            "flag_regression": bool(same and worsened),   # same agreement got worse
                            "id_from": a["id"], "id_to": b["id"],
                            "date_from": a["file_date"], "date_to": b["file_date"],
                            "raw_z_from": round(float(za), 3), "raw_z_to": round(float(zb), 3),
                            "driver_field": drv, "driver_from": dv0, "driver_to": dv1})
    O = pd.DataFrame(off).reindex(
        pd.DataFrame(off)["delta_raw_z"].abs().sort_values(ascending=False).index) if off else pd.DataFrame()
    O.to_csv(os.path.join(il.OUT, "stability_offenders.csv"), sep=";", index=False)

    pd.set_option("display.width", 240)
    nreg = int(O["flag_regression"].sum()) if len(O) else 0
    print(f"wrote stability_summary.csv + stability_offenders.csv ({len(O)} flagged pairs, "
          f"statutory era-steps EXCLUDED via raw variants)")
    if len(O):
        print(f"\ndirection: {int((O['direction']=='improved').sum())} improved, "
              f"{int((O['direction']=='worsened').sum())} worsened")
        print(f"SAME_TERM regressions (re-issue got worse = likely extraction error): {nreg}")
        print("\nby topic x relation:")
        print(O.groupby(["topic", "relation"]).size().unstack(fill_value=0).to_string())
        print("\ntop 12 SAME_TERM regressions (review these — a re-issue should not worsen):")
        reg = O[O["flag_regression"]].head(12)
        print(reg[["topic", "cao_number", "delta_raw_z", "driver_field", "driver_from",
                   "driver_to", "date_from", "date_to"]].to_string(index=False) if len(reg) else "  (none)")


if __name__ == "__main__":
    main()
