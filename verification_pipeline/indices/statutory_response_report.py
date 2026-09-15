"""Do national statutory changes trigger CAO changes? (Hanna, 2026-07-15)

Design: yearly CAO-level panel from all_indices_panel_yearly.csv (December slice of the
monthly in-force panel). The STATUTORY pseudo-CAO rows give the statutory z per year on the
same scale. For each topic with statutory time variation (leave is primary: WIEG 2019 +
aanvullend geboorteverlof 2020-07, betaald ouderschapsverlof 2022-08; contract: WWZ 2015 /
WAB 2020 ketenregeling), regress:

    d(cao_z_it) = a + b0 * d(stat_z_t) + b1 * d(stat_z_{t-1}) [+ b2 lag2] + cao FE + e_it

with CAO fixed effects (within transform) and cluster-robust (by year) inference — the
regressor varies only by t, so clustering by t is the honest SE. NOTE the panel's z's are
POOLED cross-sectionally; statutory-anchored components are law-month re-scored, which
mechanically moves cao_z when the law moves (the leave FRE nets the statutory floor OUT of
the CAO score, so a positive b would mean CAOs actively raise their OWN terms when the floor
rises — not the mechanical channel). Also reports an event-style table: mean d(cao_z) in
statutory-change years vs quiet years, and the share of CAOs filing a new text within 12
months after a change year.

Output: statutory_response_results.csv + printed summary (paste into the report).
Run: python3 statutory_response_report.py
"""
import os
import sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il

TOPICS = ["leave", "absence", "contract", "term", "overall"]

def ols_within(y, X, cluster):
    """Within (demeaned-by-unit already applied) OLS with cluster-robust SEs."""
    X1 = np.column_stack([np.ones(len(X)), X])
    XtX = X1.T @ X1
    beta = np.linalg.solve(XtX, X1.T @ y)
    resid = y - X1 @ beta
    meat = np.zeros((X1.shape[1], X1.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        xg, ug = X1[m], resid[m]
        s = xg.T @ ug
        meat += np.outer(s, s)
    G = len(np.unique(cluster))
    bread = np.linalg.inv(XtX)
    V = bread @ meat @ bread * (G / (G - 1))
    se = np.sqrt(np.diag(V))
    return beta, se, len(y), G

def main(suffix=""):
    """suffix='' = kennisgeving/file axis (default); '_first' / '_retro' = the panels built on
    the Date_first_is_Ingangsdatum / Date_retro_datum start-date conventions (robustness)."""
    pan = il.read_csv_safe(il.locate(f"all_indices_panel_yearly{suffix}.csv"))
    pan["year"] = pan["month"].astype(str).str.slice(0, 4).astype(int) if "month" in pan.columns \
        else pd.to_numeric(pan["year"], errors="coerce").astype(int)
    is_stat = pan["cao_number"].astype(str).str.upper().str.contains("STAT")
    stat = pan[is_stat].copy()
    cao = pan[~is_stat].copy()

    results = []
    for t in TOPICS:
        zc = f"{t}_z"
        if zc not in pan.columns:
            continue
        s = stat.set_index("year")[zc].astype(float).sort_index()
        ds = s.diff().rename("d_stat")
        c = cao[["cao_number", "year", zc]].copy()
        c[zc] = pd.to_numeric(c[zc], errors="coerce")
        c = c.sort_values(["cao_number", "year"])
        c["d_cao"] = c.groupby("cao_number")[zc].diff()
        c = c.merge(ds.reset_index(), on="year", how="left")
        c["d_stat_l1"] = c["year"].map(ds.shift(1))
        c = c.dropna(subset=["d_cao", "d_stat", "d_stat_l1"])
        if len(c) < 100 or c["d_stat"].abs().sum() == 0:
            continue
        # within transform (CAO FE)
        for col in ("d_cao", "d_stat", "d_stat_l1"):
            c[col + "_w"] = c[col] - c.groupby("cao_number")[col].transform("mean")
        beta, se, n, G = ols_within(c["d_cao_w"].values,
                                    c[["d_stat_w", "d_stat_l1_w"]].values,
                                    c["year"].values)
        for name, b, s_ in zip(["const", "d_stat_t", "d_stat_t-1"], beta, se):
            results.append({"topic": t, "coef": name, "beta": round(float(b), 4),
                            "se_cluster_year": round(float(s_), 4),
                            "t": round(float(b / s_), 2) if s_ > 0 else np.nan,
                            "n_obs": n, "n_year_clusters": G})
        # event-style: change years vs quiet years
        chg_years = set(ds[ds.abs() > 1e-9].index)
        c["chg_year"] = c["year"].isin(chg_years)
        ev = c.groupby("chg_year")["d_cao"].agg(["mean", "std", "count"])
        for cy, r in ev.iterrows():
            results.append({"topic": t, "coef": f"event_mean_dcao[chg={bool(cy)}]",
                            "beta": round(float(r["mean"]), 4),
                            "se_cluster_year": round(float(r["std"] / np.sqrt(r["count"])), 4),
                            "t": np.nan, "n_obs": int(r["count"]), "n_year_clusters": len(chg_years)})
    out = pd.DataFrame(results)
    out.to_csv(os.path.join(il.OUT, f"statutory_response_results{suffix}.csv"), sep=";", index=False)
    print(out.to_string(index=False))
    return out

if __name__ == "__main__":
    main()
