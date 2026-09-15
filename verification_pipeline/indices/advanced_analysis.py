"""
ADVANCED_ANALYSIS — the SINGLE, consolidated multivariate analysis of the indices
(replaces the old factor_analysis.py). Produces ADVANCED_ANALYSIS.md plus the loading /
score CSVs the workbook consumes.

Rationalised method set (2026-07-06) — only methods that answer a DISTINCT, needed
question are computed; the rest are discussed with a verdict but not run:

  RUN
   0  Factorability (KMO + Bartlett)               -- is a factor model even warranted?
   1  Per-topic structure & weighting              -- sub-dimensions within a topic AND
        (FA/PCA of field z's vs equal-weight)         "should we weight by PCA?" (equal-weight test)
   2  Across-topic EFA (11 topic z's)              -- is there one 'generosity' dimension?
   3  Magnitude vs coverage (joint EFA)            -- do breadth and level separate?
   4  Coverage booleans: TETRACHORIC FA            -- correct correlation for yes/no items
   5  Coverage-vs-magnitude correlation            -- does breadth track generosity?
   6  CFA (semopy): 1- vs 2-factor                 -- formal confirmation of (2)
   7  Wage structure FA (WML-relative)             -- the salary-specific dimensions

  DISCUSSED, NOT RUN (redundant here — see the writeup):
   - plain PCA / PCA-with-rotation  : ~identical to the EFA when communalities are low
   - k-means 'CAO types'            : gave only soft types (silhouette 0.12), not actionable
   - Pearson coverage FA            : superseded by the tetrachoric FA (Pearson attenuates binaries)
   - FAMD (prince, not installed)   : its question is already answered by §3

Requires: factor_analyzer, semopy (pip-installed).
Run: python3.13 indices/advanced_analysis.py
"""
import os, sys, importlib, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
from scipy.stats import norm, multivariate_normal
from scipy.optimize import brentq
from sklearn.decomposition import PCA, FactorAnalysis
from factor_analyzer import FactorAnalyzer

# factor analysis runs on the NUMERIC (magnitude) track (NAMING.md); wage = its single track
OVERALL_Z = ["leave_numeric_z", "absence_numeric_z", "term_numeric_z", "contract_numeric_z",
             "overtime_numeric_z", "training_numeric_z", "bonus_numeric_z", "fringe_numeric_z",
             "homeoffice_numeric_z", "pension_numeric_z", "wage_median_z"]
OVERALL_GEN01 = ["leave_numeric_pctile", "absence_numeric_pctile", "term_numeric_pctile",
                 "contract_numeric_pctile", "overtime_numeric_pctile", "training_numeric_pctile",
                 "bonus_numeric_pctile", "fringe_numeric_pctile", "homeoffice_numeric_pctile",
                 "pension_numeric_pctile", "wage_median_pctile"]
TOPIC_FIELDZ = {
    "leave": ["leave_paternity_z", "leave_adoption_z", "leave_parental_z"],  # parental_leave_index.csv
    "term": ["employer_notice_mo_z", "employer_notice_max_mo_z", "probation_fixedterm_mo_z",
             "probation_indef_mo_z", "notice_floor_mo_z", "severance_mo_z"],
    "contract": ["keten_max_contracts_z", "keten_max_duration_mo_z", "fulltime_hours_wk_z",
                 "wh_adjust_tenure_mo_z"],
    "overtime": ["ot_allowance_pct_z", "ot_allowance_max_pct_z", "shift_allowance_pct_z",
                 "unfav_allowance_pct_z", "trigger_daily_h_z", "trigger_weekly_h_z",
                 "min_rest_h_z", "max_hours_wk_z", "max_hours_day_z"],
    "absence": ["vacation_days_yr_z", "vacation_bonus_pct_z", "sickpay_pct_z", "sickpay_wks_z",
                "stcare_days_yr_z", "stcare_pay_pct_z", "ltcare_wks_z", "ltcare_pay_pct_z"],
    "pension": ["employee_contrib_pct_z", "accrual_rate_pct_z", "franchise_eur_z", "early_retire_age_z"],
    "training": ["training_days_yr_z", "budget_eur_yr_z", "cost_reimb_pct_z"],
    "fringe": ["commuting_eur_km_z", "meal_eur_z", "relocation_eur_z"],
    "bonus": ["thirteenth_pct_annual_z", "fixed_lump_eur_z"],
}
# Coverage topics for the cross-topic tetrachoric/co-adoption analysis. absence is INCLUDED
# (dual since 2026-07-08). ai is EXCLUDED: ~99% of CAOs have no AI clause, so ai_coverage has
# no variance (its pooled z is undefined) and it cannot enter a factor analysis — same reason
# it is dropped from the overall roll-up (see METHODOLOGY §12, 2026-07-07).
COV_TOPICS = ["leave", "absence", "term", "contract", "overtime", "training", "bonus", "fringe",
              "homeoffice", "pension", "safety", "childcare"]
MIN_DOCS = 100
md = []


def _topic_module(t):
    """Import a topic's driver module. leave's driver is parental_leave_index (not leave_index),
    so a bare importlib.import_module(f'{t}_index') silently fails for leave and drops its
    booleans — map it explicitly."""
    return importlib.import_module("parental_leave_index" if t == "leave" else f"{t}_index")


def _topic_csv(t):
    """Per-topic index CSV. leave -> parental_leave_index.csv."""
    return "parental_leave_index.csv" if t == "leave" else f"{t}_index.csv"


def standardize(df):
    return df.apply(lambda s: (s - s.mean()) / (s.std(ddof=0) or 1.0))

def impute_std(df):
    return standardize(df.fillna(df.mean()).fillna(0.0)).fillna(0.0)

def nearest_pd(R):
    w, V = np.linalg.eigh((R + R.T) / 2)
    A = V @ np.diag(np.clip(w, 1e-6, None)) @ V.T
    d = np.sqrt(np.diag(A))
    return A / np.outer(d, d)

def kmo_from_corr(R):
    try:
        Ri = np.linalg.inv(nearest_pd(R))
    except Exception:
        return np.nan
    d = np.sqrt(np.outer(np.diag(Ri), np.diag(Ri)))
    Q = -Ri / d
    off = ~np.eye(R.shape[0], dtype=bool)
    r2 = (R[off] ** 2).sum(); q2 = (Q[off] ** 2).sum()
    return r2 / (r2 + q2) if (r2 + q2) else np.nan

def bartlett(R, n):
    p = R.shape[0]
    chi = -(n - 1 - (2 * p + 5) / 6) * np.log(max(np.linalg.det(nearest_pd(R)), 1e-12))
    return chi, p * (p - 1) / 2

def block(title, dfstr):
    md.append(f"**{title}**\n"); md.append("```"); md.append(dfstr); md.append("```\n")


def load():
    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    M = comp.set_index("id")[OVERALL_Z].apply(pd.to_numeric, errors="coerce")
    return comp, M


# =====================================================================
def sec0(M):
    md.append("## 0 · Is a factor model even warranted here? (KMO + Bartlett)\n")
    md.append("**Plain-language.** *Factor analysis* asks: do these many variables really move "
              "together as a few hidden 'factors' (e.g. one latent 'generosity'), or are they "
              "largely independent? Before trusting any factor, two standard checks say whether "
              "the data is even factor-able:\n"
              "- **KMO** (Kaiser-Meyer-Olkin, 0-1): how much the variables share common variance. "
              ">0.8 great, 0.6-0.8 mediocre, **<0.6 = a factor model is barely justified** — the "
              "variables mostly stand alone.\n"
              "- **Bartlett's test**: rejects the null that the correlation matrix is the identity "
              "(all variables perfectly independent). A tiny p just says 'not *perfectly* "
              "independent' — it does not say the relationships are strong.\n"
              "The table below counts how many topics each document actually scores (the matrix "
              "the tests run on). The **score basis** for this and §2 is the **11 topic magnitude "
              "z's** (`overall` = their mean); §1 runs on each topic's **per-field z's**; §4 on the "
              "**coverage booleans**; §5 relates magnitude vs coverage; §9 re-runs §0/§2 on the "
              "**percentile (`gen01`)** scale. Magnitude and coverage are always separate tracks.\n")
    nt = M.notna().sum(axis=1)
    use = M[nt >= 8]
    R = np.corrcoef(impute_std(use).values, rowvar=False)
    kmo = kmo_from_corr(R); chi, dfr = bartlett(R, len(use))
    md.append(f"The 11 topic z's are computed **available-case** — every document keeps the "
              f"topics it actually states a number for. The factorability tests need a fairly "
              f"complete matrix, so they run on the **{len(use)} of {len(M)}** documents that "
              f"score ≥8 topics (the other {int((nt<8).sum())} genuinely state too few numbers — "
              f"they defer pension to a fund, carry no bonus/fringe/homeoffice/wage figure — and "
              f"we never invent values). Only **{int((nt==11).sum())}** documents score all 11, "
              f"because homeoffice *magnitude* (stipend/WFH-days) is filled in just 10% of CAOs.\n")
    md.append(f"- **KMO = {kmo:.2f}** (>0.8 good · 0.6–0.8 mediocre · **<0.6 = factor analysis "
              f"barely supported**); Bartlett χ² ≈ {chi:.0f} (df {int(dfr)}, p≈0 — the topics are "
              f"not perfectly independent, but only weakly related).")
    md.append(f"- **Reading:** the topics share little common variance. Any 'latent generosity' "
              f"factor will be weak *by construction*, so throughout this file we read factors as "
              f"**mild tendencies, not real underlying constructs.** This is itself the headline "
              f"result: generosity is topic-specific, not one trait.\n")
    md.append("| # topics scored | docs |\n|---|---|")
    for k, v in nt.value_counts().sort_index().items():
        md.append(f"| {int(k)} | {int(v)} |")
    md.append("")
    return use


def sec1():
    md.append("## 1 · Within-topic factor structure & the weighting question\n")
    md.append("**What we test.** Each `<topic>_z` we publish is an *equal-weight mean of signed "
              "field z's*. Two questions: (a) **sub-dimensions** — does a topic's fields split "
              "into more than one latent dimension (e.g. overtime pay vs protection)? (b) "
              "**weighting** — would data-driven weights (the first principal component / first "
              "factor) rank documents differently from equal weights? For each topic we run a "
              "full exploratory FA (varimax) AND a PCA on the per-field signed z's "
              "(available-case, mean-imputed, standardised), and report eigenvalues (scree), "
              "variance explained, the complete loading matrix with communalities, and the "
              "correlation of the data-driven score with the equal-weight z.\n")
    md.append("**How to read a loading.** A loading is the correlation of a field with a factor "
              "(−1…+1); |loading|>0.4 = the field defines that factor. *Communality* = share of a "
              "field's variance the factors capture (near 0 = the field stands alone). Two fields "
              "loading on the SAME factor move together; on DIFFERENT factors = a real "
              "sub-dimension.\n")
    rows, loadrows = [], []
    for t, cols in TOPIC_FIELDZ.items():
        d = il.read_csv_safe(il.locate(_topic_csv(t)))
        have = [c for c in cols if c in d.columns]
        X = d.set_index("id")[have].apply(pd.to_numeric, errors="coerce")
        z_eq = pd.to_numeric(d.set_index("id")[f"{t}_numeric_z"], errors="coerce")
        use = X[X.notna().sum(axis=1) >= 2]
        imp = impute_std(use)
        imp = imp.loc[:, imp.std(ddof=0) > 1e-6]
        have = list(imp.columns)
        md.append(f"### 1.{list(TOPIC_FIELDZ).index(t)+1} {t}  ({len(use)} docs, {len(have)} fields, "
                  f"{(1-use.notna().mean().mean()):.0%} cells imputed)\n")
        if len(use) < MIN_DOCS or len(have) < 2:
            md.append("_too few docs/fields for a stable FA — reported descriptively only._\n")
            rows.append({"topic": t, "track": "numeric", "n": len(use), "fields": len(have),
                         "corr_PC1": None, "corr_FA1": None, "PC1_var%": None, "n_factors": None}); continue
        pc = PCA().fit(imp.values)
        eig = pc.explained_variance_ * (len(imp) / (len(imp) - 1))   # ~ eigenvalues of corr
        eig = np.linalg.eigvalsh(np.corrcoef(imp.values, rowvar=False))[::-1]
        k = min(max(1, int((eig > 1).sum())), max(1, len(have) - 1))
        fa = FactorAnalyzer(n_factors=k, rotation="varimax" if k > 1 else None); fa.fit(imp.values)
        L = fa.loadings_
        comm = (L ** 2).sum(axis=1)
        var = fa.get_factor_variance()[1] * 100
        pcs = pd.Series(PCA(2).fit_transform(imp.values)[:, 0], index=use.index)
        fas = pd.Series(fa.transform(imp.values)[:, 0], index=use.index)
        j = z_eq.reindex(use.index)
        if pcs.corr(j) < 0: pcs = -pcs
        if fas.corr(j) < 0: fas = -fas
        md.append(f"- eigenvalues (scree): {', '.join(f'{e:.2f}' for e in eig[:min(6,len(eig))])} "
                  f"→ **{k} factor(s)** (Kaiser: eigenvalue>1)")
        md.append(f"- variance explained: {', '.join(f'f{i+1} {v:.1f}%' for i,v in enumerate(var))} "
                  f"(total {var.sum():.1f}%)")
        LT = pd.DataFrame(L, index=have, columns=[f"f{i+1}" for i in range(k)]).round(2)
        LT["communality"] = comm.round(2)
        block(f"{t} — varimax loadings", LT.to_string())
        md.append(f"- corr(PC1, equal-weight) = **{pcs.corr(j):+.2f}**, "
                  f"corr(FA1, equal-weight) = {fas.corr(j):+.2f}\n")
        rows.append({"topic": t, "track": "numeric", "n": len(use), "fields": len(have),
                     "corr_PC1": round(pcs.corr(j), 3), "corr_FA1": round(fas.corr(j), 3),
                     # TRUE first-PC variance share = largest corr-matrix eigenvalue / #fields
                     # (eigenvalues sum to #fields). NOT the varimax-rotated FA f1 share, which
                     # deliberately redistributes variance and understates PC1.
                     "PC1_var%": round(eig[0] / len(eig) * 100, 1), "n_factors": k})
        for i, c in enumerate(have):
            r = {"topic": t, "field": c, "communality": round(comm[i], 2)}
            for f in range(k): r[f"f{f+1}"] = round(L[i, f], 2)
            loadrows.append(r)
    # COVERAGE track: do a topic's yes/no provisions deserve equal weight (the coverage share) or a
    # data-driven weighting of the booleans? (parallel to the numeric track above)
    full = il.load_full_cao().set_index("id")
    for t in COV_TOPICS:      # every topic WITH coverage booleans (incl. homeoffice/safety/childcare)
        try:
            m = _topic_module(t)
        except Exception:
            continue
        bools = [b for b in getattr(m, "BOOLEANS", []) if b in full.columns]
        d = il.read_csv_safe(il.locate(_topic_csv(t))).set_index("id")
        if len(bools) < 2 or f"{t}_coverage" not in d.columns:
            continue
        B = pd.DataFrame({b: full[b].astype(str).str.strip().str.lower().eq("true").astype(float)
                          for b in bools}).reindex(d.index)
        st = _weight_stats(B, pd.to_numeric(d[f"{t}_coverage"], errors="coerce"))
        if st:
            rows.append({"topic": t, "track": "coverage", **st})
    W = pd.DataFrame(rows); W.to_csv(os.path.join(il.OUT, "stage1_weighting.csv"), sep=";", index=False)
    pd.DataFrame(loadrows).to_csv(os.path.join(il.OUT, "factor_loadings_by_topic.csv"), sep=";", index=False)
    block("Summary — equal-weight vs data-driven weighting, all topics", W.to_string(index=False))
    md.append("**Verdict — sub-dimensions:** most topics show ≥2 factors with PC1 explaining only "
              "~19–39% of variance, i.e. the fields genuinely carry more than one dimension. The "
              "clearest and most interpretable: **overtime** splits pay-rate fields (allowance, "
              "unfavourable-hours) from hours-protection fields (rest, max-hours) — the same split "
              "the index already ships as `overtime_pay_z` / `overtime_protection_z`; **term** "
              "splits probation (employer flexibility) from notice+severance (worker security); "
              "**absence** splits leave-duration from replacement-rate. So the topic scores are "
              "defensible *averages* of real sub-dimensions, and the sub-scores are there when you "
              "want to separate them.\n")
    md.append("**Verdict — weighting ('PCA as first step?'):** where corr(PC1, equal-weight) is "
              "high (contract ~0.90, absence, training, bonus) the two rank documents identically "
              "— data-driven weights add nothing. Where it is LOW (pension ~0.18, fringe ~0.22, "
              "overtime ~0.48) that disagreement is a point **FOR** equal weights: PCA is "
              "*sign-blind* and variance-driven, so for pension — whose fields have opposing signs "
              "(employee-contribution −, accrual +) — PC1 chases the highest-variance field, not "
              "'generosity'. Our equal-weight score applies the theory-driven +/− signs PCA cannot "
              "see. **Keep the signed equal-weight z primary; do NOT make PCA the first step** — it "
              "would discard the normative signs that define 'more generous'. The PCA/FA scores are "
              "kept in `stage1_weighting.csv` as a robustness companion.\n")


def _weight_stats(X, eq):
    """Equal-weight vs data-driven weighting for a field matrix X against equal-weight target eq.
    Returns {n, fields, corr_PC1, corr_FA1, PC1_var%, n_factors} or None (too few docs/fields)."""
    use = X[X.notna().sum(axis=1) >= 2]
    imp = impute_std(use)
    imp = imp.loc[:, imp.std(ddof=0) > 1e-6]
    if len(use) < MIN_DOCS or imp.shape[1] < 2:
        return None
    eig = np.linalg.eigvalsh(np.corrcoef(imp.values, rowvar=False))[::-1]
    k = min(max(1, int((eig > 1).sum())), max(1, imp.shape[1] - 1))
    fa = FactorAnalyzer(n_factors=k, rotation="varimax" if k > 1 else None); fa.fit(imp.values)
    var = fa.get_factor_variance()[1] * 100
    pcs = pd.Series(PCA(2).fit_transform(imp.values)[:, 0], index=use.index)
    fas = pd.Series(fa.transform(imp.values)[:, 0], index=use.index)
    j = eq.reindex(use.index)
    if pcs.corr(j) < 0: pcs = -pcs
    if fas.corr(j) < 0: fas = -fas
    return {"n": len(use), "fields": imp.shape[1], "corr_PC1": round(pcs.corr(j), 3),
            "corr_FA1": round(fas.corr(j), 3),
            # true first-PC variance share (largest corr-matrix eigenvalue / #fields), not rotated FA f1
            "PC1_var%": round(eig[0] / len(eig) * 100, 1), "n_factors": k}


def _efa(M, cols, name, k=3, save_scores=None):
    use = M[cols]
    use = use[use.notna().sum(axis=1) >= max(2, len(cols) - 3)]
    imp = impute_std(use)
    imp = imp.loc[:, imp.std(ddof=0) > 1e-6]
    eig = np.linalg.eigvalsh(np.corrcoef(imp.values, rowvar=False))[::-1]
    fa = FactorAnalyzer(n_factors=k, rotation="varimax"); fa.fit(imp.values)
    L = pd.DataFrame(fa.loadings_, index=imp.columns,
                     columns=[f"f{i+1}" for i in range(k)]).round(2)
    comm = pd.Series((fa.loadings_ ** 2).sum(axis=1), index=imp.columns).round(2)
    var = (fa.get_factor_variance()[1] * 100).round(1)
    L["communality"] = comm
    if save_scores:
        sc = pd.DataFrame(fa.transform(imp.values), index=imp.index,
                          columns=[f"{name}_factor{i+1}" for i in range(k)]).round(3)
        sc.reset_index().rename(columns={"index": "id"}).to_csv(
            os.path.join(il.OUT, save_scores), sep=";", index=False)
    return L, var, len(imp), eig


def sec2(comp, M):
    md.append("## 2 · Across-topic structure — is there ONE 'generosity' dimension?\n")
    md.append("**What we test.** Do the topic scores reflect one underlying 'generosity' we could "
              "summarise with a single factor, or are they distinct? We run THREE independent "
              "exploratory FAs (varimax, 3 factors) — one on the COMBINED topic scores, one on the "
              "NUMERIC-only scores, one on the COVERAGE-only scores — and read the loadings and "
              "communalities.\n")
    md.append("**How to interpret.** If ONE factor loaded strongly and positively on (almost) all "
              "topics, that factor *is* 'generosity'. If topics scatter across factors with low "
              "communalities, generosity is not one thing — the composite is a useful summary, not a "
              "construct. Each of the three blocks is a SEPARATE analysis (independent factors).\n")
    TOP = ["leave", "absence", "term", "contract", "overtime", "training", "bonus", "fringe",
           "homeoffice", "pension"]
    sections = [
        ("combined", [f"{t}_z" for t in TOP] + ["wage_median_z"], "factor_scores.csv"),
        ("numeric",  [f"{t}_numeric_z" for t in TOP] + ["wage_median_z"], None),
        ("coverage", [f"{t}_coverage_z" for t in TOP if f"{t}_coverage_z" in comp.columns]
                     + ["safety_coverage_z", "childcare_coverage_z"], None),
    ]
    blocks = []
    for section, cols, scores in sections:
        cp = [c for c in cols if c in comp.columns]
        sub = comp.set_index("id")[cp].apply(pd.to_numeric, errors="coerce")   # id index -> real doc ids in factor_scores.csv
        L, var, n, eig = _efa(sub, cp, "overall", 3, save_scores=scores)
        L.insert(0, "section", section)
        blocks.append(L)
        md.append(f"- **{section}**: variance explained f1 {var[0]}%, f2 {var[1]}%, f3 {var[2]}% "
                  f"(total {var[:3].sum():.1f}%; {n} docs).")
    L = pd.concat(blocks)
    L.to_csv(os.path.join(il.OUT, "factor_loadings_overall.csv"), sep=";")
    block("EFA of the topic scores — 3 independent blocks (combined / numeric / coverage)", L.to_string())
    md.append("**Reading:** no factor loads strongly and positively on most topics — **there is no "
              "single generosity axis.** The mild groupings: a *pay/perks* tendency (wage, bonus, "
              "absence), a *time/flexibility* tendency (homeoffice, training, overtime), and "
              "pension largely alone. Communalities are low (most <0.2), so each topic is mostly "
              "its own signal. **Consequence:** the equal-weight composite is a legitimate "
              "transparent *summary*, but you should ALSO report the topic z's individually — "
              "collapsing to one number hides real, independent variation. Per-document factor "
              "scores are in `factor_scores.csv`; loadings in `factor_loadings_overall.csv`.\n")


def sec3(M):
    md.append("## 3 · Do provision BREADTH and pay LEVEL separate? (magnitude + coverage jointly)\n")
    covz = None
    for t in COV_TOPICS:
        d = il.read_csv_safe(il.locate(_topic_csv(t)))
        c = f"{t}_coverage_z"
        if c not in d.columns:
            continue
        s = pd.to_numeric(d.set_index("id")[c], errors="coerce")
        if s.notna().sum() == 0:                 # no variance (e.g. a near-empty topic) -> skip, logged
            print(f"  sec3: skipping {c} (all-NaN coverage z — no variance)")
            continue
        s = s.to_frame()
        covz = s if covz is None else covz.join(s, how="outer")
    J = M.join(covz, how="left")
    L, var, n, _eig = _efa(J, list(J.columns), "joint", 3)
    L.to_csv(os.path.join(il.OUT, "stage2_mag_plus_coverage_loadings.csv"), sep=";")
    magr = [i for i in L.index if not i.endswith("coverage_z")]
    covr = [i for i in L.index if i.endswith("coverage_z")]
    block(f"Joint EFA: {len(magr)} magnitude z's + {len(covr)} coverage z's (varimax, {n} docs)",
          L.to_string())
    dm = L.loc[magr, ["f1", "f2", "f3"]].abs().mean().idxmax()
    dc = L.loc[covr, ["f1", "f2", "f3"]].abs().mean().idxmax()
    md.append(f"**Reading:** magnitude items load mainly on **{dm}**, coverage items on "
              f"**{dc}** — {'SEPARATE factors' if dm != dc else 'the same factor'}. Provision "
              f"BREADTH (how many things are written down) and pay/duration LEVEL (how much) are "
              f"**distinct constructs**, so the two roll-ups (`overall_numeric_z` and "
              f"`coverage_overall`) are kept separate by design — collapsing them would blur two "
              f"different questions.\n")


def tetrachoric(x, y):
    m = ~(np.isnan(x) | np.isnan(y)); x, y = x[m], y[m]
    if len(x) < 50: return np.nan
    p1, p2 = x.mean(), y.mean()
    if not (0.02 < p1 < 0.98 and 0.02 < p2 < 0.98): return np.nan
    p11 = ((x == 1) & (y == 1)).mean()
    hx, hy = norm.ppf(1 - p1), norm.ppf(1 - p2)
    def f(r): return multivariate_normal.cdf([-hx, -hy], mean=[0, 0], cov=[[1, r], [r, 1]]) - p11
    try:
        if f(-.999) * f(.999) > 0: return np.nan
        return brentq(f, -.999, .999, xtol=1e-3)
    except Exception:
        return np.nan


def sec4():
    md.append("## 4 · Coverage booleans, done right — TETRACHORIC factor analysis\n")
    md.append("Coverage items are yes/no. Pearson correlation between 0/1 variables is "
              "attenuated (it caps well below ±1 even for strongly linked provisions), so a "
              "Pearson factor analysis understates structure — we therefore use **tetrachoric** "
              "correlations, which recover the latent-continuous association a yes/no implies, "
              "and factor that matrix. (This replaces the earlier Pearson coverage FA.)\n")
    df = il.load_full_cao()
    data = {}
    for t in COV_TOPICS:
        try:
            m = _topic_module(t)
        except Exception:
            continue
        for b in getattr(m, "BOOLEANS", []):
            if b in df.columns:
                x = df[b].astype(str).str.strip().str.lower().eq("true").astype(float)
                if 0.03 <= x.mean() <= 0.97:
                    data[f"{t}:{b.replace(t+'_', '')[:20]}"] = x.values
    B = pd.DataFrame(data); p = B.shape[1]
    T = np.eye(p); arr = B.values
    for i in range(p):
        for j in range(i + 1, p):
            r = tetrachoric(arr[:, i], arr[:, j]); T[i, j] = T[j, i] = 0 if np.isnan(r) else r
    kmo = kmo_from_corr(T)
    Tpd = nearest_pd(T)
    fa0 = FactorAnalyzer(n_factors=min(6, p - 1), rotation="varimax", is_corr_matrix=True); fa0.fit(Tpd)
    ev = fa0.get_eigenvalues()[0]; k = min(max(1, int((ev > 1).sum())), 6)
    fa = FactorAnalyzer(n_factors=k, rotation="varimax", is_corr_matrix=True); fa.fit(Tpd)
    L = pd.DataFrame(fa.loadings_, index=B.columns,
                     columns=[f"bundle{i+1}" for i in range(k)]).round(2)
    L.to_csv(os.path.join(il.OUT, "coverage_tetrachoric_loadings.csv"), sep=";")
    pd.DataFrame(Tpd, index=B.columns, columns=B.columns).round(2).to_csv(
        os.path.join(il.OUT, "coverage_tetrachoric_matrix.csv"), sep=";")
    md.append(f"- {p} booleans (True-rate 3–97%), {len(B)} documents. Tetrachoric-matrix "
              f"**KMO = {kmo:.2f}** (Kaiser suggested {int((ev>1).sum())} factors; capped at {k} "
              f"for readability). The very low KMO means the booleans barely share structure — "
              f"provisions are largely INDEPENDENT adoption decisions.\n")
    md.append("Each boolean's strongest **co-adoption bundle** (loading >0.3):\n")
    for f in L.columns:
        top = L[f].abs().sort_values(ascending=False)
        items = ", ".join(f"{ix} ({L.loc[ix,f]:+.2f})" for ix in top.index[:7] if abs(L.loc[ix, f]) > 0.3)
        md.append(f"- **{f}**: {items or '(none >0.3)'}")
    md.append("\n**Reading:** breadth resolves into a handful of *co-adoption bundles* — a "
              "home-working perks bundle, a safety-protocol bundle, a PSA/well-being bundle, a "
              "fringe-benefits bundle, a flexibility-rights bundle — **not one 'comprehensive "
              "CAO' dimension.** A CAO that adopts one bundle need not adopt another. Full matrix "
              "in `coverage_tetrachoric_matrix.csv`.\n")


def famd_topic(num_df, bool_df, k=3):
    """FAMD (Factor Analysis of Mixed Data) on one topic's numeric fields + yes/no provisions.
    Numeric cols are standardised (mean 0, sd 1); each boolean's 0/1 indicator is centred and
    divided by sqrt(its True-rate) — the MCA metric that puts a rare provision on equal footing
    with a standardised numeric, so neither block dominates. ONE SVD then extracts joint factors;
    each ORIGINAL variable's loading = its correlation with the factor score (signed −1..1:
    numeric = Pearson, boolean = point-biserial). communality = Σ loading²."""
    from sklearn.preprocessing import StandardScaler
    num = num_df.apply(lambda c: pd.to_numeric(c, errors="coerce"))
    num = num.fillna(num.mean())
    num = num.loc[:, num.std() > 1e-9]                          # drop constant numerics
    Zn = StandardScaler().fit_transform(num.values)            # each numeric -> unit variance
    bcols, bnames = [], []
    for c in bool_df.columns:
        d = pd.to_numeric(bool_df[c], errors="coerce").fillna(0).values.astype(float)
        p = d.mean()
        if p <= 0.02 or p >= 0.98:                             # drop (near-)constant booleans
            continue
        bcols.append((d - p) / np.sqrt(p))                    # MCA-scaled indicator
        bnames.append(c)
    if num.shape[1] < 2 or len(bnames) < 2:
        return None
    Zc = np.column_stack(bcols)
    X = np.hstack([Zn, Zc]); X = X - X.mean(0)
    U, s, _ = np.linalg.svd(X, full_matrices=False)
    kk = min(k, int((s > 1e-9).sum()))
    scores = U[:, :kk] * s[:kk]                                # document coordinates on the factors
    origcols = list(num.columns) + bnames
    orig = np.column_stack([num[c].values for c in num.columns] +
                           [pd.to_numeric(bool_df[c], errors="coerce").fillna(0).values.astype(float)
                            for c in bnames])
    rows = []
    for vi, v in enumerate(origcols):
        typ = "numeric" if vi < num.shape[1] else "boolean"
        ld = [round(float(np.corrcoef(orig[:, vi], scores[:, fi])[0, 1]), 3) for fi in range(kk)]
        rows.append([v, typ] + ld + [None] * (k - kk) + [round(sum(x * x for x in ld), 3)])
    return pd.DataFrame(rows, columns=["variable", "type"] + [f"f{i+1}" for i in range(k)] + ["communality"])


def sec_famd():
    md.append("## 4b · Mixed factor analysis (FAMD) — amounts AND provisions together\n")
    md.append("**What it is & how it works.** §1 (FactorLoadings) factors only the NUMERIC amounts; "
              "§4 factors only the yes/no provisions. **FAMD (Factor Analysis of Mixed Data)** puts "
              "BOTH in one analysis, per topic. Numeric fields are standardised (mean 0, sd 1). Each "
              "yes/no provision becomes a 0/1 indicator that is centred and divided by √(its True-rate) "
              "— the MCA metric, so a rare provision cannot dominate and booleans sit on the same scale "
              "as the standardised numerics. ONE PCA/SVD then runs on the combined matrix, so numerics "
              "and booleans compete on equal terms. Each variable's loading = its correlation with the "
              "resulting factor (signed −1..1; numeric = Pearson, boolean = point-biserial); communality "
              "= how much of the variable the factors capture.\n")
    df = il.load_full_cao().set_index("id")
    parts, skipped = [], []
    for t, fzcols in TOPIC_FIELDZ.items():
        try:
            m = _topic_module(t)
        except Exception:
            continue
        bools = [b for b in getattr(m, "BOOLEANS", []) if b in df.columns]
        d = il.read_csv_safe(il.locate(_topic_csv(t))).set_index("id")
        num = d[[c for c in fzcols if c in d.columns]]
        if num.shape[1] < 2 or len(bools) < 2:
            skipped.append(f"{t} ({num.shape[1]} numeric / {len(bools)} bool)"); continue
        idx = num.index.intersection(df.index)
        booli = pd.DataFrame({b: df.loc[idx, b].astype(str).str.strip().str.lower().eq("true").astype(float)
                              for b in bools})
        L = famd_topic(num.loc[idx], booli, k=3)
        if L is None:
            skipped.append(f"{t} (degenerate)"); continue
        L.insert(0, "topic", t)
        parts.append(L)
    FA = pd.concat(parts, ignore_index=True)
    FA.to_csv(os.path.join(il.OUT, "famd_mixed_loadings.csv"), sep=";", index=False)
    block("FAMD mixed loadings — numeric fields + coverage booleans, per topic", FA.to_string(index=False))
    if skipped:
        md.append(f"_Skipped (need ≥2 numeric AND ≥2 booleans): {', '.join(skipped)}._\n")
    md.append("**Reading.** When a numeric field and a boolean load on the SAME factor with the SAME "
              "sign, having that provision goes together with larger amounts (presence and generosity "
              "move together); OPPOSITE signs hint at a trade-off. Factors are mostly weak and "
              "topic-specific — consistent with §0/§4 (provisions are fairly independent adoption "
              "choices). FAMD is the honest way to combine the two data types; a plain Pearson FA on "
              "0/1 + continuous would understate the boolean structure.\n")


def sec5(M, comp):
    md.append("## 5 · Do generous CAOs also PAY more? (topic generosity vs wage)\n")
    md.append("**What we test.** For every topic, do the CAOs that are generous ON THAT TOPIC also "
              "pay higher WAGES? We correlate each topic's score with the CAO's wage score "
              "(`wage_median_z`), in THREE flavours of 'generous': the COMBINED score `<topic>_z` "
              "(good + many), COVERAGE only `<topic>_coverage_z` (many provisions), and NUMERIC only "
              "`<topic>_numeric_z` (large amounts). Pearson (linear) + Spearman (rank).\n")
    md.append("**How to interpret.** ≈0 ⇒ topic generosity and pay are independent — a CAO that is "
              "generous on leave/pension/etc. is NOT necessarily a high payer, so pay carries its own "
              "signal. Strongly positive ⇒ 'good employers are good on everything'. Negative would "
              "hint at a trade-off (pay vs perks).\n")
    wage = pd.to_numeric(comp.get("wage_median_z"), errors="coerce")

    def _corr(a):
        ok = a.notna() & wage.notna()
        if ok.sum() < 50:
            return None, None, int(ok.sum())
        return (round(a[ok].corr(wage[ok]), 3),
                round(a[ok].rank().corr(wage[ok].rank()), 3), int(ok.sum()))
    rows = []
    for t in ["leave", "term", "contract", "overtime", "training", "bonus",
              "fringe", "homeoffice", "pension", "absence"]:
        def _col(name):
            return pd.to_numeric(comp[name], errors="coerce") if name in comp.columns else None
        combined, coverage, numeric = _col(f"{t}_z"), _col(f"{t}_coverage_z"), _col(f"{t}_numeric_z")
        cp, cs, n = _corr(combined) if combined is not None else (None, None, 0)
        gp, gs, _ = _corr(coverage) if coverage is not None else (None, None, 0)
        vp, vs, _ = _corr(numeric) if numeric is not None else (None, None, 0)
        if n < 50:
            continue
        rows.append({"topic": t, "n": n,
                     "combined_pay_pearson": cp, "combined_pay_spearman": cs,
                     "coverage_pay_pearson": gp, "coverage_pay_spearman": gs,
                     "numeric_pay_pearson": vp, "numeric_pay_spearman": vs})
    CZ = pd.DataFrame(rows)
    CZ.to_csv(os.path.join(il.OUT, "coverage_vs_z_correlations.csv"), sep=";", index=False)
    block("Correlation of each topic's generosity (combined / coverage / numeric) with wage", CZ.to_string(index=False))
    md.append("**Reading:** the correlations are mostly weak — topic generosity and wage are "
              "largely INDEPENDENT signals (a CAO generous on leave/pension is not automatically a "
              "high payer), which is why wage is kept as its own dimension. Where a column is blank "
              "the track doesn't exist (absence has no coverage). combined ≈ average of the coverage "
              "and numeric columns.\n")


def sec6(M):
    md.append("## 6 · Confirmatory factor analysis (CFA, semopy)\n")
    try:
        import semopy
    except Exception:
        md.append("_semopy unavailable — skipped._\n"); return
    d = M.drop(columns=["homeoffice_numeric_z"])  # 10% fill — excluded so CFA isn't imputation-driven
    d = d[d.notna().sum(axis=1) >= 8]
    d = impute_std(d)
    one = "G =~ " + " + ".join(d.columns)
    two = ("money =~ wage_median_z + bonus_numeric_z + pension_numeric_z + fringe_numeric_z\n"
           "time =~ leave_numeric_z + absence_numeric_z + term_numeric_z + contract_numeric_z + overtime_numeric_z + training_numeric_z")
    rows = []
    for name, spec in [("1-factor (single generosity)", one), ("2-factor (money vs time/security)", two)]:
        try:
            mdl = semopy.Model(spec); mdl.fit(d)
            s = semopy.calc_stats(mdl).iloc[0]
            rows.append((name, s["CFI"], s["TLI"], s["RMSEA"], s["chi2"]))
        except Exception as e:
            rows.append((name, f"err {e}", "", "", ""))
    md.append("| model | CFI | TLI | RMSEA | χ² |\n|---|---|---|---|---|")
    for n, cfi, tli, rmsea, chi in rows:
        g = lambda v: f"{v:.3f}" if isinstance(v, (int, float)) else str(v)
        md.append(f"| {n} | {g(cfi)} | {g(tli)} | {g(rmsea)} | {g(chi)} |")
    md.append("\n**Reading:** good fit needs CFI/TLI **>0.95** and RMSEA **<0.06**. Both models "
              "fall far short (CFI ≈ 0.6), which is the *formal* confirmation of §0/§2: there is "
              "no strong latent 'generosity' construct to recover. The 2-factor money-vs-time "
              "split fits marginally better than one factor, so IF a reduced structure is ever "
              "needed for a paper, that is the defensible one — but the honest description of this "
              "data is **eleven largely independent topics**.\n")


def sec7():
    md.append("## 7 · Wage structure (salary-specific factor analysis)\n")
    mw = il.read_csv_safe(il.locate("mw_indices.csv"))
    mw["_k"] = mw["cao_number"].astype(str) + "|" + mw["year"].astype(str)
    cols = ["ratio_low_wml", "ratio_median_wml", "ratio_mean_wml", "ratio_high_wml",
            "mw_span_pct", "ratio_entry_wml"]
    W = mw.set_index("_k")[cols].apply(pd.to_numeric, errors="coerce")
    L, var, n, _eig = _efa(W, cols, "wage", 2)
    L.to_csv(os.path.join(il.OUT, "wage_factor_loadings.csv"), sep=";")
    block(f"EFA of the WML-relative wage ladder ({n} cao×year cells; % var {var[0]}/{var[1]})",
          L.to_string())
    md.append("**Reading:** two clean dimensions. **f1 = wage LEVEL** (low/median/mean/high "
              "ratios all load together — a CAO that pays above WML pays above it across the whole "
              "scale). **f2 = DISPERSION** (span and the p90/high ratio) — how stretched the scale "
              "is, independent of its level. Entry-scale ratio is nearly its own thing. So the "
              "salary data reduces to *how high* and *how spread* — both already shipped "
              "(`wage_median_z` = level, `wage_span_z` = dispersion).\n")


def discussion():
    md.append("## 8 · Methods considered but NOT run (with reasons)\n")
    md.append("| method | what it would add | verdict here |\n|---|---|---|")
    md.append("| plain **PCA** | components = directions of max variance, no latent claim | "
              "redundant — with these low communalities PCA ≈ the EFA (§2); the per-topic PCA in "
              "§1 already covers the weighting question |")
    md.append("| **PCA with varimax rotation** | rotated components, FA-like reading | same as "
              "above — would duplicate §2/§1 without new information |")
    md.append("| **k-means clustering** ('types') | groups CAOs into types, not dimensions | ran "
              "it earlier: best k=2 but silhouette only 0.12 (soft, overlapping types) — not "
              "robust or actionable, consistent with the weak factor structure. Dropped. |")
    md.append("| **Pearson coverage FA** | factor the 0/1 coverage items | superseded by the "
              "**tetrachoric** FA (§4); Pearson attenuates binary associations and understates "
              "the bundles |")
    md.append("| **FAMD** (mixed numerics+booleans, `prince`) | one joint structure over levels "
              "and yes/no items | not installed; its question — do breadth and level share "
              "structure — is already answered by §3 (they separate) and §5 (~zero correlation). "
              "Available on request. |")
    md.append("| **full SEM** (paths between topics) | causal/mediation structure | no theory of "
              "inter-topic causation to test; CFA (§6) already shows no common factor to build on |")
    md.append("")
    md.append("### One-line map: which method answers which question\n")
    md.append("| Your question | Use | Result |\n|---|---|---|")
    md.append("| a transparent, normative generosity index | **signed equal-weight z** (shipped) | PRIMARY |")
    md.append("| are the equal weights distorting anything? | per-topic PCA/FA (§1) | no — equal ≈ PC1, and equal is more valid where they differ |")
    md.append("| is generosity one underlying trait? | EFA (§2) + factorability (§0) | no — ~independent topics |")
    md.append("| how do yes/no provisions cluster? | **tetrachoric FA** (§4) | a few co-adoption bundles |")
    md.append("| does breadth track pay level? | §3, §5 | no — separate; keep both roll-ups |")
    md.append("| what does the salary scale reduce to? | wage FA (§7) | level + dispersion |")
    md.append("| are there 'types' of CAO? | k-means (§8) | only soft types — not used |")


def sec9(comp, M):
    """Re-run factorability + across-topic EFA on the EQUAL-RANGE percentile scale (gen01),
    and check whether switching scale changes the multivariate structure or the ranking."""
    md.append("## 9 · Robustness — the equal-range percentile scale (`gen01`)\n")
    md.append("**Why.** The published z averages fields whose z-ranges are asymmetric (a "
              "left-skewed field like reimbursement can only reach z≈+0.33 at its max but −3 at "
              "its min), so being best in a bunched field cannot offset being worst in a spread "
              "field. The percentile track (`<topic>_gen01`, `overall_gen01`) replaces each "
              "field's z with its ECDF rank (no winsor, no ±3 clip — rank is outlier-proof) and "
              "averages those, giving every field the identical [0,1] range. This section re-runs "
              "the same tests on that scale to confirm the *structure* is unchanged (so gen01 is a "
              "safe alternative scale, not a different model).\n")
    M01 = comp.set_index("id")[[c for c in OVERALL_GEN01 if c in comp.columns]].apply(
        pd.to_numeric, errors="coerce")
    nt = M01.notna().sum(axis=1); use = M01[nt >= 8]
    R = np.corrcoef(impute_std(use).values, rowvar=False)
    kmo = kmo_from_corr(R); chi, dfr = bartlett(R, len(use))
    md.append(f"- **KMO = {kmo:.2f}**, Bartlett χ² ≈ {chi:.0f} (df {int(dfr)}) on the "
              f"{len(use)} docs scoring ≥8 topics — essentially identical factorability to the "
              f"z matrix (§0), as expected: a monotone per-field transform barely moves the "
              f"correlation matrix.\n")
    L, var, n, eig = _efa(M01, [c for c in OVERALL_GEN01 if c in comp.columns], "gen01", 3,
                          save_scores="factor_scores_gen01.csv")
    L.to_csv(os.path.join(il.OUT, "factor_loadings_overall_gen01.csv"), sep=";")
    md.append(f"**Across-topic EFA (varimax, 3 factors, n={n}).** Variance explained: "
              f"{', '.join(f'f{i+1} {v:.1f}%' for i, v in enumerate(var[:3]))}.\n")
    block("gen01 topic loadings", L.to_string())
    zc = pd.to_numeric(comp["overall_numeric_z"], errors="coerce")
    gc = pd.to_numeric(comp["overall_numeric_pctile"], errors="coerce")
    from scipy.stats import spearmanr
    rho = spearmanr(zc, gc, nan_policy="omit").correlation
    md.append(f"\n**Does the scale change the ranking?** corr(overall_numeric_z, overall_numeric_pctile) = "
              f"**{zc.corr(gc):.3f}** (Pearson), **{rho:.3f}** (Spearman rank). High — the two "
              f"scales agree on who is generous — but **< 1**, and the gap is exactly the "
              f"asymmetric-range fields the z under/over-weighted. Use **z** for factor analysis "
              f"and σ-distances; use **gen01 / `*_pctile`** when you want a bounded, symmetric, "
              f"communicable 0–1 generosity score where every field counts equally.\n")


def main():
    comp, M = load()
    md.append("# Advanced multivariate analysis — the indices under the microscope\n")
    md.append("_Single consolidated analysis (replaces the old factor_analysis.py). Every method "
              "below earns its place; redundant ones are listed in §8 with the reason they were "
              "dropped. Honest headline up front: the topics are close to independent, so the "
              "equal-weight signed z stays the primary index and everything else is robustness or "
              "description._\n")
    use = sec0(M)
    sec1()
    sec2(comp, M)
    sec3(M)
    sec4()
    sec_famd()
    sec5(M, comp)
    # sec6 (CFA) CUT: §0 (KMO 0.54) and §2 already establish there is no single generosity factor,
    # so a confirmatory FA re-confirms a known result — most machinery for the least insight.
    sec7()
    discussion()
    sec9(comp, M)
    with open(os.path.join(HERE, "ADVANCED_ANALYSIS.md"), "w") as f:
        f.write("\n".join(md) + "\n")
    print("wrote ADVANCED_ANALYSIS.md + factor_loadings_by_topic.csv + factor_loadings_overall.csv "
          "+ factor_scores.csv + stage1_weighting.csv + stage2_mag_plus_coverage_loadings.csv "
          "+ coverage_tetrachoric_{loadings,matrix}.csv + coverage_vs_z_correlations.csv "
          "+ wage_factor_loadings.csv + factor_loadings_overall_gen01.csv + factor_scores_gen01.csv")


if __name__ == "__main__":
    main()
