# Indices v2 — redesign plan (pooled z · monthly panel · statutory · new salary source)

**Owner:** Hanna · **Drafted:** 2026-07-05 · **Status:** DESIGN — awaiting sign-off, nothing built.
Supersedes the v1 construction described in `README.md` / `GENEROSITY_TOPICS_DESIGN.md`
(v1 stays reproducible; its outputs will be archived, not deleted).

Hanna's 10 change requests, verbatim mapping: §2 (=req 1), §3 (=req 9), §4 (=req 2),
§5 (=req 3), §6 (=req 4), §7 (=reqs 5+6+10-salary), §8 (=req 8), §9 (=req 7),
§1 (=req 10-non-salary).

---

## 0. Inputs (all read-only)

| File | Role | Why this one |
|---|---|---|
| `qa/corrected_dataset.csv` | ALL non-salary fields | canonical (G9 at planning time; G33 / 34 layers as of 2026-07-15, full provenance). Already what v1 reads — req 10 is already satisfied for non-salary. |
| `CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv` | ALL wage/salary data | **NEW source (req 10)** — the deterministic-parser dataset: 359,056 rows, 100% amount provenance, per-row confidence tiers (A 72% / B 20% / C 7% / D 1%). Replaces the old un-QA'd `salary_increase_events_derived.csv`. External path, read-only, same as v1's convention. |
| `indices/out/statutory_all.csv` | statutory floors/caps/defaults per field per era | already the single machine-readable statutory source (built by `build_statutory.py`); reused for imputation AND the new statutory pseudo-file (§6). |
| `indices/out/parental_leave_index.csv` construction (leave overlay + FRE logic) | leave cardinal values | FRE-weeks logic is validated & era-aware; kept, re-scored under v2. |
| `qa/corrected_dataset.csv` column `datum_kennisgeving` | the **file date** | 2,738/2,739 populated; the document's page-header edition date. Established in the version-selection work as the only reliable *edition* clock (`signing_date` 26% filled; `id` doesn't track recency; `ingangsdatum` is term-level and constant across a term's republications). |

Scope filter unchanged: `general_document_type` startswith `full_cao` (2,698 docs; the 41
partial/annex docs excluded).

---

## 1. What stays from v1 (deliberately)

- **Unit normalisation** (`index_lib.normalize` family) — canonical unit per field, the
  hard-won `*_unit_audit` logic. Untouched.
- **Per-field plausibility clamps** and **era-aware statutory caps/floors/defaults** for the
  cardinal "full" variant (norm → clamp → forward-fill → statutory floor/default impute →
  statutory cap). This produces the *cardinal* value per field per doc; v2 only changes how
  that cardinal is **standardised** and **placed in time**.
- **Field lists, signs, coverage boolean sets** per topic (plus leave coverage added, §8).
- **Available-case aggregation** (no false zeros), winsorise 1/99 + z-clip ±3.
- Empty≠zero doctrine, full-CAO filter, FUSE-safe IO, `;` CSVs, determinism requirement.

---

## 2. ONE pooled z per file (req 1)

**Change.** Drop *all* within-group standardisation (within-vintage-year `_mag_*`,
within-calendar-year `_panel_z`, within-active-set `_active_z`). Each field's cardinal
"full" value is standardised **once, against ALL full-CAO documents of all years pooled**:

```
field_z = clip( (x − μ_pool) / σ_pool , ±3 ),   μ/σ on the 1–99% winsorised pool
topic_z = signed available-case mean of its field_z's        (one number per document)
```

**Why this is the right move for the new design:**
- Within-year z *erases secular trends by construction* (every year is re-centred to 0).
  Since the whole point of the monthly panel (§4) and the mean/variance series (§5) is to
  *see* how generosity evolves, the standardisation must **preserve levels over time**.
  Pooled z does: a 2005 doc and a 2025 doc are measured on the same yardstick.
- One score per file = exactly the shape the factor analysis (§9) needs (a clean
  docs × features matrix, no group-dependent scores).
- Simpler: no year-bin logic, no drift-by-reneg, no three parallel score families.

**Kept variants:** `raw` (no impute/cap) and `full` (headline) z per topic; pooled
percentile-rank as the robustness column. (The `capped`/`imputed` middle diagnostics are
dropped — they were 2×2 diagnostics for the v1 design discussion; say the word and they
come back, the machinery stays.)

**Double-counting guard (important, new):** several CAOs republish the same term many
times (median 11 full docs/CAO). If every edition enters the pooled μ/σ with weight 1,
frequently-reprinting CAOs distort the yardstick. Fix without needing the full Wave-5
build: compute the **winsor bounds and μ/σ on one doc per (cao_number, ingangsdatum)
term-group** (the latest `datum_kennisgeving` edition), then **score all docs** against
those parameters. Every file still gets its z (updates included, req 9); the yardstick
just isn't biased by reprint frequency.

**Persisted scoring parameters:** per-field `μ, σ, winsor_lo, winsor_hi` written to
`indices/out/scoring_params.csv`. This is what makes the statutory pseudo-file (§6) — and any
future re-score — consistent with the CAO scores.

---

## 3. The file date: `datum_kennisgeving` replaces `ingangsdatum` (req 9)

**Change.** Everywhere v1 used `ingangsdatum` as the document's time coordinate
(ordering within CAO, forward-fill order, is-newest, panel explode), v2 uses
**`file_date = datum_kennisgeving`**.

**Why:** `ingangsdatum` is the *term* start — constant across all republications of a term,
so mid-term updates (new wage rounds, changed provisions folded into a republished full
text) all collapse onto the term start and the panel never sees them. `datum_kennisgeving`
is the *edition* date (99.96% populated, verified the most reliable ordering field in the
version-selection investigation). Using it means **every update enters the timeline at the
moment it was published** — req 9's "so we have the updates included".

Mechanics:
- Within-CAO ordering & forward-fill: sort by `file_date` (was `_date`=ingangsdatum).
- `doc_is_newest` (was `is_active`): max `file_date` per `cao_number`.
- The 1 record with empty kennisgeving: fall back to `ingangsdatum`, flag in diagnostics.
- Semantics caveat (documented, accepted): kennisgeving is a *publication* clock. A base
  file published shortly before/after its term start shifts the panel entry by that lag.
  Diagnostic column `pub_lag_months = file_date − ingangsdatum` (per doc) so extreme lags
  are visible; no hybrid logic unless the diagnostic shows it matters.
- `ingangsdatum`/`expiratiedatum` stay in the outputs as descriptive columns (they still
  define the legal term; they just no longer define the time axis).

---

## 4. Calendar-month panel (req 2)

**Change.** `build_panel.py` (cao × calendar-**year**, within-year re-standardised) is
replaced by **`build_panel_monthly.py`** → `out/all_indices_panel_monthly.csv`:

- **Grain:** one row per `cao_number × month` (YYYY-MM), from the month of the CAO's first
  `file_date` through the build month.
- **In-force rule (the "round up"):** the file in force in month *m* = the file with the
  **latest `file_date` ≤ last day of m**. So when a new file appears anywhere inside a
  month — even the 30th — that month is attributed to the **new** file. That is exactly the
  requested round-up: old file active before + new file starts in the same month ⇒ the
  month counts as the new file's.
  - Two files published in the same month ⇒ the later `file_date` wins the whole month;
    same-day tie ⇒ the richer extraction (more populated fields), then higher `id`.
- **Values:** the in-force file's pooled `<topic>_z` / coverage / composite — **constant
  while the file is in force** (no re-standardisation per period; all panel movement is
  real file turnover, which makes the panel interpretable: a step = a new edition).
- **Columns:** `cao_number, month, in_force_id, file_date, ingangsdatum, pub_lag_months,
  months_since_file, doc_is_newest, is_stale` (> 48 months since file_date), all
  `<topic>_z`, `<topic>_coverage`, `overall_z_mean`, `overall_z_var` (§5a), wage columns
  broadcast from the CAO-year wage table (§7), and the `STATUTORY` pseudo-rows (§6).
- A **yearly convenience roll-up** (`out/all_indices_panel_yearly.csv`, December's row per
  year — end-of-year state) for analyses that don't need month grain.

Size sanity: 242 CAOs × ~270 months ≈ 65k rows — trivially fine as CSV and xlsx tab.

---

## 5. Overall mean & variance z (req 3)

Built at **both** levels (both cheap; both plausible readings of the request):

**(a) Per file — across topics:** `overall_z_mean` = equal-weight available-case mean of
the 13 topic z's (the composite, now defined for *every* doc, not just active ones);
`overall_z_var` = variance across those topic z's = how *uneven* a CAO's generosity is
across topics (a CAO can be mean-average but lopsided: great leave, poor pension).
`n_topics_scored` alongside, as v1.

**(b) Per month — across CAOs (the landscape series):** new
`out/panel_aggregates_monthly.csv`: for each month, over all CAOs in force, the **mean** and
**variance** (+ n, p10/p50/p90) of each `<topic>_z` and of `overall_z_mean`. This is the
series pooled z makes meaningful: mean ↑ = the bargaining landscape got more generous;
variance ↓ = agreements converging. (Under v1's within-year z these were 0/1 by
construction — uninformative.)

---

## 6. Statutory z — the law as its own file (req 4)

New **`statutory_index.py`** → `out/statutory_index.csv`, and `STATUTORY` rows in the panel:

- For each month, build a **pseudo-record**: for every indexed field, the statutory
  **floor/default** value effective that month per `out/statutory_all.csv` (era-aware —
  ketenregeling 36→24→36 months etc.). Fields with only a `cap`/`informational`/no anchor
  stay **empty** (a worker without a CAO doesn't "get" a cap; and we never invent values —
  same doctrine as everywhere).
- Normalise those values through the **identical** unit canon and score with the
  **persisted pooled parameters** (`out/scoring_params.csv`, §2) → `statutory_<topic>_z` per
  month, plus a statutory `overall_z_mean` over the topics that have anchors
  (leave via the era-aware statutory FRE-weeks already computed in the leave build; term,
  contract, overtime-hours, wage-floor via WML §7; no statutory row for
  training/bonus/fringe/homeoffice/childcare/ai — genuinely no numeric legal package).
- **Why:** puts the legal floor on the same yardstick as the CAOs, so the panel directly
  shows the **bargained premium over the law** and how it moved as laws changed (WWZ 2015,
  WAB 2020, WIEG 2019/2020…). Joined into the monthly panel as `cao_number = "STATUTORY"`
  so every downstream consumer gets it for free.

---

## 7. Wage track v2 (reqs 5, 6, 10-salary)

`mw_indices.py` is rebuilt on the **parser dataset** (`extracted_data_salary_v2.csv`):

- **Melt** the wide file (`salary_N_*` point groups) to long: one row per
  (cao, file, jobgroup/step identity, effective date, amount, unit).
- **Filters:** confidence tier **A+B only** (92.5% of rows; C/D excluded, count reported in
  diagnostics); adult rows (youth `age_group` rows excluded); monthly-unit amounts (hourly
  kept but converted only when `ft_hours`/`hours_basis` known, else excluded — no invented
  conversion); `is_entry` rows **kept but split out** (they're the true hiring floor —
  reported as `mw_entry`, not mixed into the scale percentiles).
- **Year grain (req 5):** everything aggregated per `(cao_number, calendar year)` where
  year = year of the wage point's own `salary_N_start_date` (NOT the file's date — wage
  tables carry their own effective dates). No exact-date wage panel; the monthly panel
  broadcasts the year's row to its 12 months.
- **High vs low worker types (req 6):** the direct labels are too sparse to be the primary
  split (`worker_type` 16.7% filled, `education` ~0%, `age_group` 4.8%) — so:
  - **Primary = scale-position proxy:** within (cao, year), percentiles of the monthly
    amounts across all scale cells: `mw_low` = p10, `mw_q25`, `mw_median`, `mw_mean`
    (**new**, req 6), `mw_q75`, `mw_high` = p90, `mw_span_pct` = (p90−p10)/p10. Scale
    position ≈ skill/seniority gradient; it's label-free and comparable across CAOs
    (labels are per-CAO idiosyncratic — A/B/C vs I..IX vs named functions).
  - **Secondary (descriptive, where labels exist):** per (cao, year, `worker_type`) means
    for the ~17% labelled subset → `out/mw_by_worker_type.csv`, honestly flagged partial.
- **National wage floor (req 6):** new `wml_timeline` block in `build_statutory.py`'s DATA
  (year → statutory adult minimum monthly wage 1999→2026, incl. the 2024 switch to the
  hourly minimum, converted at the statutory 36-h basis; each row with source link +
  `please_verify` for Hanna — same review pattern as the other statutory values). Output
  columns: `wml_month`, `ratio_low_wml` = mw_low/WML, `ratio_mean_wml`. The WML series
  also feeds the statutory pseudo-file's wage dimension (§6).
- **Wage z:** `wage_z` = pooled z of `mw_mean` (and `wage_low_z` of `mw_low`) across all
  (cao, year) cells — the wage dimension joins the topic z family, the composite, the
  panel, and the factor analysis (v1 had no wage dimension in the composite at all).

---

## 8. Leave coverage (req 8)

v1's leave index is FRE-magnitude-only — the one topic with no coverage score. Add it:
`leave_coverage` = share of the **12 generosity booleans** (from the corrected dataset:
`has_leave_enhancements, has_above_statutory_maternity, paternity_explicitly_above_statutory,
parental_eligibility_present, parental_topup_present, abortion_present, sick_topup_present,
sickpay_extra_insurance_present, care_topup_present, liberation_day_annual,
liberation_day_lustrum, extra_seniority_present`; the 5 structural flags
`*_statutory_ref, *_exceptions, hetero_present` excluded — same generosity-vs-structural
rule as every other topic) + pooled `leave_coverage_z`. Leave joins the coverage roll-up
and the panel's coverage columns.

---

## 9. Factor analysis (req 7)

New **`factor_analysis.py`** (sklearn 1.5.2 available: `FactorAnalysis(rotation='varimax')`),
two levels:

- **Per topic:** input = the docs × per-field signed pooled-z matrix (drivers will now
  export their per-field z columns, not just the topic mean — needed anyway for FA).
  Docs with < 2 populated fields in the topic excluded; remaining NaNs mean-imputed with
  the imputation share reported. Deliver: eigenvalue/scree table (factor-count guidance,
  Kaiser rule), loadings, communalities, explained variance, factor scores per doc.
  Topics with too little magnitude data for a meaningful FA (childcare, ai, homeoffice)
  are skipped with the reason logged, not forced.
- **One overall:** docs × the 13 `<topic>_z` (+ `wage_z`) matrix → same procedure. Answers
  "is there one generosity dimension, or e.g. a money factor (wage/bonus/pension) vs a
  time/security factor (leave/term/contract)?" — the natural check on the equal-weight
  composite; if factor 1 dominates, the composite is validated; if not, the loadings say
  what to report separately.
- Outputs: `out/factor_loadings_by_topic.csv`, `out/factor_loadings_overall.csv`,
  `out/factor_scores.csv`, human summary `FACTOR_ANALYSIS.md`.

---

## 10. Outputs after v2 (the target state)

| File | Grain | New/changed |
|---|---|---|
| `<topic>_index.csv` ×13 | document | CHANGED: pooled `<topic>_z` (+ per-field z, pctile), file_date columns; vintage/active scores gone |
| `out/mw_indices.csv` (+ `out/mw_by_worker_type.csv`) | cao × year | REBUILT on parser v2 source; new percentile ladder, mean, entry, WML ratios |
| `out/statutory_index.csv` | month | NEW — the law scored as a file |
| `out/all_indices_panel_monthly.csv` | cao × month (+ STATUTORY rows) | NEW — replaces the yearly panel |
| `out/all_indices_panel_yearly.csv` | cao × year | NEW convenience (Dec slice) |
| `out/panel_aggregates_monthly.csv` | month | NEW — cross-CAO mean/variance series |
| `out/composite_index.csv` | document | CHANGED: all docs, `overall_z_mean` + `overall_z_var` |
| `factor_*.csv` + `FACTOR_ANALYSIS.md` | — | NEW |
| `out/scoring_params.csv` | field | NEW — persisted pooled μ/σ/winsor (the yardstick) |
| `all_indices.xlsx` / `out/all_indices_combined.csv` | — | REBUILT tabs + legends |

**Removed/superseded** (archived to `indices/_old_v1/`, not deleted — same pattern as
`qa/_old/`): `build_panel.py` + `all_indices_panel.csv`, the `_mag_capped/_imputed`
variants, `_vintage_z`/`_panel_z`/`_active_z` families, `year_bins`/`active_group`
machinery, old `mw_indices` outputs.

---

## 11. Edge cases & rules (decided up front)

1. **Reprint over-weighting** → pooled μ/σ computed on one-doc-per-term-group, all docs
   scored (§2). `term_group` derived inline (cao+ingangsdatum) — does not need Wave 5.
2. **Missing kennisgeving (1 doc)** → fallback ingangsdatum + flag.
3. **Same-month multiple editions** → latest file_date wins; same-day → richer extraction,
   then id (tie-break logged in diagnostics).
4. **Stale trailing rows** → `is_stale` after 48 months without a newer file (panel keeps
   them; consumers filter).
5. **Hourly wage rows without a known hours basis** → excluded from mw percentiles (never
   convert on a guessed workweek), counted in diagnostics.
6. **Statutory pseudo-file** never gets imputation *from* CAO data; only fields with an
   explicit statutory floor/default value are populated.
7. **No invented WML/statutory numbers**: the WML table ships with `please_verify` rows for
   Hanna exactly like the existing statutory timeline.
8. **Determinism**: every script re-run must be byte-identical; verified before promotion,
   as v1.

---

## 12. Build order

1. `build_statutory.py` + WML timeline (small, unblocks §6/§7). → Hanna verify pass.
2. `index_lib` v2: file_date, pooled z + params persistence, term-group de-dup for the
   yardstick, per-field z export. Re-run 12 drivers + leave (incl. leave coverage).
3. `mw_indices` v2 on parser source (+ worker-type table + WML join). Reconciliation
   report vs old mw output (distribution shifts explained).
4. `statutory_index.py`.
5. `build_panel_monthly.py` (+ yearly slice + aggregates; STATUTORY rows joined).
6. `composite_index.py` v2 (mean + variance, all docs).
7. `factor_analysis.py`.
8. `build_combined.py` (xlsx tabs + legends), `INDICES_OVERVIEW.md` + `README.md` rewrite,
   archive v1 outputs to `indices/_old_v1/`.

**Validation before hand-over:** re-run determinism; rank-correlation old `_active_z` vs
new pooled z on the newest docs (expect high but not 1 — level effects now included);
monthly-panel latest slice ≡ newest-doc cross-section (consistency identity); statutory z
below CAO median in every anchored topic (sanity); mw new-vs-old scatter + explanation of
big movers; FA factor-1 loadings all generosity-positive (sign sanity).

---

## 13. Open points for Hanna (recommendations included, none block the build start)

1. **Req-3 reading** — I build both (§5a per-file across-topics + §5b per-month
   across-CAOs). Cheap; drop one later if noise.
2. **WML yearly values** — will be researched with sources, but land as `please_verify`
   rows before the wage ratios are trusted.
3. **Entry scales** — recommended: separate `mw_entry` column, excluded from the p10 floor
   (else CAOs with aanloopschalen look artificially stingy). Say the word to fold them in.
4. **Panel start** — first file_date per CAO (data-driven, recommended) vs a fixed
   2000-01 start for a balanced window. I'll default to data-driven.
5. **48-month stale threshold** — carried over from v1 (was 4 years); adjustable constant.
