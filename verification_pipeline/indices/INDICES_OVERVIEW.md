# Indices v2 — catalogue

**Built 2026-07-05 on the v2 construction** (design + rationale: [INDICES_V2_PLAN.md](INDICES_V2_PLAN.md); methodology: [README.md](README.md); v1 archived in `_old_v1/`).

The three sentences that define v2:
1. Every full-CAO document gets **one pooled z per topic** — standardised against ALL documents of ALL years (winsorised 1/99, ±3 clip, yardstick de-duplicated to one doc per term) — so scores are comparable **across time**, and levels/trends are visible.
2. The time axis is the **file date** (`datum_kennisgeving`, the edition/publication date), so mid-term republications (updates) enter the timeline when they were published.
3. Sources: non-salary = **`qa/corrected_dataset.csv`** (canonical, G33 / 34 layers — ledger `docs/DATA_LINEAGE.md`); salary = the **deterministic-parser dataset** `CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv` (tier A+B rows).

## Outputs

| File | Grain | What it is |
|---|---|---|
| `<topic>_index.csv` ×14 | document | pooled `<topic>_z` (+ `_z_raw`, `_pctile`), per-field z's, coverage + coverage_z. Topics: term, contract, overtime (pay/protection sub-z's), training, bonus, fringe, homeoffice, pension, childcare/safety/ai (coverage-only), leave (`out/parental_leave_index.csv`: FRE-weeks + leave coverage), **absence** (NEW 2026-07-05: vacation days, vakantiegeld %, sick-pay %/weeks, care leave — statutory floors imputed era-aware) |
| `out/mw_indices.csv` | cao × year | wage-scale ladder from the parser dataset: `mw_low` (p10) … `mw_high` (p90), `mw_mean`, entry scales separate, WML ratios, pooled `wage_z`/`wage_rel_z`/`wage_low_rel_z`. `year` = the wage table's OWN effective date (salary_N_start_date), not file_date and not the agreement's ingangsdatum |
| `out/mw_by_worker_type.csv` | cao × year × worker_type | secondary labelled split (~15% of points labelled — partial by construction) |
| `out/wml_timeline.csv` | statutory revision | national minimum wage series 2002→2026 (derived rows flagged `please_verify`) |
| `out/statutory_index.csv` | month | **the law as its own file**: era-correct floors/defaults scored with the same pooled yardstick (leave, contract, overtime, wage; other topics have no scalar statutory package) |
| `out/composite_index.csv` | document | `overall_z_mean` (equal-weight mean of the **11** generosity z's incl. absence + wage) + `overall_z_var` (package lopsidedness) + coverage rollup |
| `out/all_indices_panel_monthly.csv` | cao × month | **the in-force panel**: file in force = latest file_date ≤ month end (round-up); scores constant per file; `STATUTORY` pseudo-CAO rows included; wage broadcast per year; `is_stale` > 48 months |
| `out/all_indices_panel_yearly.csv` | cao × year | December slice of the monthly panel |
| `out/panel_aggregates_monthly.csv` | month | cross-CAO **mean & variance** of every z + overall percentiles + statutory reference |
| `out/cao_agreement_level.csv` | document (by term) | **the admin-environment export** (build_cao_level_export.py): all NON-SALARY scores + term bookkeeping (term_edition_seq / n_editions_in_term / is_last_filed_in_term) + retro/AVV/signing fields + ready-made start dates `Date_first_is_Ingangsdatum` / `Date_retro_datum`. **NO wage columns; overall roll-ups EXCLUDE wage and are suffixed `_without_wage`** (wage is a CAO-year fact — join `mw_indices.csv` by calendar year instead; verified: re-adding wage reproduces the composite's overalls). The panel's non-salary scores are reconstructable from it on any date convention |
| `out/factor_loadings_by_topic.csv`, `out/factor_loadings_overall.csv`, `out/factor_scores.csv`, `FACTOR_ANALYSIS.md` | — | varimax factor analyses on the per-field z's (per topic) and the 10 topic z's (overall) |
| `out/scoring_params.csv` | field | the persisted pooled yardstick (winsor bounds, μ, σ, n per field/variant) |
| `out/all_indices_combined.csv` | document | every topic's columns merged on `id` (199 cols) |
| `all_indices.xlsx` | 26 tabs | everything above; every tab opens with a collapsible column legend (Composite + PanelMonthlyFirst tabs retired 2026-07-15 — AgreementLevel supersedes both) |
| `out/statutory_all.csv` + `review/statutory_timeline.xlsx` | change-point | statutory reference (built by `build_statutory.py`; xlsx has per-topic tabs + WML tab) |

## Headline findings (2026-07 build)

- **Levels drift is real and now visible**: cross-CAO mean `overall_z_mean` rises from −0.18 (Dec 2014) to +0.12 (Dec 2025); dispersion widens (var 0.07→0.09). Under v1's within-year z this was invisible by construction.
- **The law moved faster than the CAOs on leave**: statutory leave z −0.86 (1999) → +1.44 (post-Aug-2022 paid parental leave) — the legal package now exceeds most CAOs' stated packages. Contract protection stepped +0.69 with WWZ 2015 and back to +0.18 with WAB 2020. Statutory overall reference: −0.29 (2014) → +0.36 (2022+).
- **CAO wage floors sit ~14% above the WML** (median `ratio_low_wml` = 1.138); mean scale wage ≈ 1.50× WML.
- **CAOs beat the law widely on absence**: median 25 vacation days (statutory 20), 100% year-1 sick pay (statutory 70%); vakantiegeld sits AT the 8% floor almost everywhere. The statutory absence package scores −0.57 vs the CAO pool.
- **There is no single "generous CAO" trait**: the overall factor analysis finds no dominant factor (all communalities ≤ 0.30; factor 1 explains ~2%). Topic generosities are largely independent — the equal-weight composite is a transparent summary, not a latent dimension. Report topics (or the factor blocks: money ≈ term/bonus/wage, care ≈ leave/training) separately when it matters.
- v1→v2 continuity: Spearman 0.965–0.998 between v1 `_active_z` and v2 pooled z on newest docs per topic — the cross-sectional ranking is preserved.

## Rebuild order

```
(edit review/statutory_timeline.xlsx)      # HANNA'S GROUND TRUTH — never regenerated by code
python3 statutory_sync.py           # workbook -> out/statutory_all.csv + out/wml_timeline.csv
python3 <topic>_index.py  (×13)     # incl. parental_leave_index.py + absence_index.py
python3 mw_indices.py               # wage (needs wml_timeline)
python3 statutory_index.py          # needs scoring_params from the drivers
python3 composite_index.py          # needs all topic CSVs + mw
python3 build_panel_monthly.py      # needs composite + statutory + mw (LAW-MONTH re-scoring)
python3 advanced_analysis.py        # ALL multivariate analysis -> ADVANCED_ANALYSIS.md
python3 build_combined.py           # workbook last
python3 stability_check.py / check_battery.py / unit_loss_report.py   # diagnostics
```

**Analyses & checks (2026-07-06):**
- `check_battery.py` — 35/35 converter self-tests, 41/41 field medians in NL domain bands
  (`out/field_sanity_report.csv`), TOTAL-vs-EXTRA semantics (`field_semantics.py` registry →
  `out/field_semantics_report.csv`, 41/41 consistent), salary unit-class medians aligned.
- `stability_check.py` — movement measured on RAW/extraction variants (statutory era-steps
  excluded); each flagged pair names its driver variable and is tagged
  `SAME_TERM_republication` (extraction noise) vs `NEW_TERM_contract` (real renegotiation).
- `advanced_analysis.py` → `ADVANCED_ANALYSIS.md` — the SINGLE consolidated multivariate
  analysis (replaced factor_analysis.py). Runs only the methods that earn their place:
  factorability (KMO/Bartlett), within-topic structure + equal-weight-vs-PCA weighting test,
  across-topic EFA, magnitude-vs-coverage separation, **tetrachoric** coverage FA, coverage-vs-z
  correlation, CFA (semopy), wage-structure FA. Redundant methods (plain PCA, k-means,
  Pearson coverage FA, FAMD) are listed in §8 with the reason dropped. Headline: topics are
  near-independent (KMO 0.56, CFA CFI≈0.6) → equal-weight signed z stays PRIMARY; the rest is
  robustness/description.
`build_statutory.py` is RETIRED to a guarded bootstrap (it once overwrote Hanna's
hand-maintained workbook — recovered in `_recovered/`). Every script is deterministic.

**Statutory semantics:** `floor_lift` = hard minimum right (blanks filled AND stated
below-floor values lifted — vacation, vakantiegeld, sick pay, care leave); `floor`/`default`
= blank-fill only (deviatable provisions: ATW rest, ketenregeling); `cap` = above-cap masked.
The monthly panel re-scores statutory-anchored components at the PANEL month's era, so a
law change hits every in-force CAO the month it takes effect. Only **12 of 41 fields fill
blanks with a statutory value** (floor/floor_lift/default); the other 29 leave blanks empty
(cap/informational/EXTRA) — full per-field table in `out/field_statutory_behavior.csv`.

**Presence-gated zero-fill (2026-07-06):** for 5 EXTRA amount fields (bonus 13th-month, fringe
meal/relocation, homeoffice stipend/entitlement) a missing value is set to **0** when the
topic's presence boolean is False (genuine absence), so an absent benefit lowers the score
instead of being skipped — better matching "total package generosity". Each numeric is gated by
its OWN specific boolean (stipend ↔ stipend_present), so "has WFH rights but no stipend" → 0,
while "stipend exists, amount not extracted" stays excluded. The pre-zerofill score is retained
as `<topic>_z_availcase`. Registry + rationale in `field_semantics.py`; per-field statutory +
zero-fill behavior in `out/field_statutory_behavior.csv`.

**⛔ PENSION EXCEPTION — available-case ONLY, never impute.** Pension is the one topic where
blanks are NEVER filled (no statutory imputation, no absence zero-fill, no normal-age fill for
early-retirement). A blank pension field almost always means "deferred to the sector fund"
(value unknown), NOT genuine absence, so those CAOs are left empty and simply not ranked on
that field. This is deliberate (Hanna, 2026-07-06) — do not "fix" it by imputing.
