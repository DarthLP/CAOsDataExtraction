# Indices column-naming taxonomy (canonical)

**Single source of truth for what every score column means. Read this before touching any
`indices/*.py` driver or reading any `*_index.csv`.** The rename that established this taxonomy
was applied 2026-07-08 (see METHODOLOGY §12 Decision Log). The helper `index_lib.apply_scheme()`
is the one place that maps internal names → this taxonomy; `index_lib.DUAL_TOPICS` lists the
dual-track topics.

## The mental model

Every topic is scored on up to **three tracks**, each available on **two scales**:

| track | what it captures | source |
|---|---|---|
| **numeric** | how MUCH a provision gives (amounts, durations, %) | numeric fields → pooled z |
| **coverage** | how MANY provisions exist (breadth) | yes/no booleans → share 0-1 → pooled z |
| **combined** | the headline = mean of numeric & coverage | computed in composite |

| scale | suffix | meaning |
|---|---|---|
| **z** | `_z` | pooled, winsorised 1/99, ±3-clipped standardisation vs the all-years term-deduped pool. 0 = average CAO. The factor-analysis input. |
| **pctile** | `_pctile` | equal-range percentile (0-1), 0 = least … 1 = most generous. The ranking scale. (Formerly called `gen01`.) |

## Column names per topic `{t}`

| name | track · scale | notes |
|---|---|---|
| `{t}_numeric_z` | numeric · z | magnitude. **Was `{t}_z` before 2026-07-08.** |
| `{t}_coverage_z` | coverage · z | breadth |
| `{t}_z` | **combined · z** | **HEADLINE** = mean(numeric_z, coverage_z). **Was `{t}_combined_z`.** |
| `{t}_numeric_pctile` | numeric · pctile | **Was `{t}_gen01`.** |
| `{t}_coverage_pctile` | coverage · pctile | |
| `{t}_pctile` | **combined · pctile** | **HEADLINE.** **Was `{t}_combined01`.** |
| `{t}_numeric_z_raw` | numeric · z (raw) | no forward-fill/impute/cap. Was `{t}_z_raw`. |
| `{t}_numeric_rankpct` | numeric robustness rank | pooled percentile-rank of the numeric z. Was `{t}_pctile`. |
| `{t}_coverage` | coverage share 0-1 | raw boolean share (not standardised) |

> ⚠️ **The bare `{t}_z` means COMBINED, not magnitude.** If you want magnitude, use
> `{t}_numeric_z`. This is the single most common mistake — the pre-2026-07-08 code used
> `{t}_z` for magnitude.

### Topic roles (which tracks exist)

- **Dual-track** (`index_lib.DUAL_TOPICS`): leave, **absence**, term, contract, overtime, training,
  bonus, fringe, homeoffice, pension — all three tracks. `{t}_z` = combined. (absence gained a
  coverage track 2026-07-08: the 3 sickness/care top-up booleans moved from leave → absence.)
- **Numeric-only**: wage — no coverage booleans (wage instead has median/mean variants, see below).
- **Coverage-only**: safety, childcare, ai — no magnitude, so the topic's score is
  `{t}_coverage_z` / `{t}_coverage_pctile`; there is **no** bare `{t}_z`.
  ⚠️ **ai is EXCLUDED from the overall roll-ups and from out/composite_index.csv** (99.3% of CAOs
  have no AI clause — era-artifact noise; decided 2026-07-07, see METHODOLOGY §12). Its
  per-topic out/ai_index.csv is still produced. So `overall_z` = 10 dual `{t}_z` + `wage_median_z`
  + `safety_coverage_z` + `childcare_coverage_z` (13 inputs, available-case).

## Overall roll-ups (composite / panel)

The **COMBINED** roll-ups are the PRIMARY headline (front of the dashboard); the numeric-only ones
sit in the light-blue right-hand section.

| name | meaning |
|---|---|
| `overall_z` | **PRIMARY z**. Available-case mean of the topics' **combined** z (numeric + coverage). |
| `overall_pctile` | **PRIMARY rank (0-1)**. Mean of the topics' **combined** pctiles. |
| `overall_z_var` | variance across the topics' **combined** z's = package lopsidedness. Pairs with `overall_z`. |
| `overall_numeric_z` | NUMERIC-only companion. Mean of the topics' numeric z's (excludes coverage). |
| `overall_numeric_z_var` | NUMERIC-only companion variance. Pairs with `overall_numeric_z` (panel only). |
| `overall_numeric_pctile` | NUMERIC-only companion. Mean of the numeric pctiles. |
| `coverage_overall`, `coverage_overall_z`, `coverage_overall_pctile` | provision breadth roll-up. |
| `n_topics_scored`, `n_topics_combined` | availability counts. |
| `*_without_wage` | **AgreementLevel-only suffix (2026-07-15)**: the same roll-up over the 12 NON-SALARY inputs (wage excluded). The agreement-level export carries ONLY these (and no wage columns at all) because at document grain wage is a CAO-year fact; the panel keeps the wage-inclusive plain names. Never compare a `_without_wage` overall to a plain one. |

The primaries (`overall_z`, `overall_pctile`, `overall_z_var`) lead the dashboard; the numeric-only
companions sit in the light-blue right-hand section. (On the AgreementLevel tab the primaries are the
`_without_wage` variants.)

## Wage (special — no numeric/coverage split; instead median vs mean, NOMINAL)

Wage has no coverage booleans. Its z scores are **NOMINAL** (raw EUR, no WML normalisation) — the
only WML-normalised quantities are the `ratio_*_wml` VALUE columns. There is no bare `wage_z`.

| column (composite/panel) | ← mw_indices name | basis | role |
|---|---|---|---|
| **`wage_median_z`** | `wage_median_z` | z of mw_median (nominal EUR) | **HEADLINE** — the wage dimension in `overall_z` |
| **`wage_median_pctile`** | (ECDF) | | HEADLINE rank — the wage dim in `overall_pctile` |
| `wage_mean_z` | `wage_mean_z` | z of mw_mean (nominal EUR) | companion (not in the overall) |
| `wage_mean_pctile` | (ECDF) | | companion |
| `ratio_low_wml`, `ratio_median_wml`, `ratio_mean_wml`, `ratio_high_wml` | value | mw_p / WML | the ONLY WML-normalised columns (reference values) |
| `wage_src_year` | value | — | the wage-table year actually carried into this doc (composite wage join is AS-OF/forward-fill since 2026-07-08: a table stays in force until replaced, so gap-year docs carry the last known year instead of losing the wage input). Lives in composite_index.csv only — the AgreementLevel export dropped it with the whole wage block (2026-07-15, see `*_without_wage`) |

- Every overall roll-up takes the **median** variant (`wage_median_z` / `wage_median_pctile`).
  The statutory row = the WML in force that month, scored on the nominal wage pool.
- **Why nominal?** Comparisons are made WITHIN a calendar year, where WML is a constant — so
  normalising wouldn't change ranks, and the cross-year rise of the nominal z is itself a signal.
  WML normalisation is kept available in the `ratio_*_wml` value columns for reference.

## Where each name lives

- **`{topic}_index.csv`, `out/parental_leave_index.csv`** — per-topic detail: `{t}_numeric_z`,
  `{t}_coverage_z`, `{t}_numeric_pctile`, `{t}_coverage_pctile`, `{t}_numeric_z_raw`,
  `{t}_numeric_rankpct`, per-field `{short}_z` / `{short}_prank`. **No bare `{t}_z`** (combined is
  a composite-level concept).
- **`out/composite_index.csv`, `out/all_indices_combined.csv`** — full taxonomy incl. headline `{t}_z`,
  `{t}_pctile`, and overall roll-ups. Produced by `il.apply_scheme()`.
- **`out/all_indices_panel_monthly.csv`** — carries the numeric track (`{t}_numeric_z`) + overall +
  coverage shares. The combined headline `{t}_z`/`{t}_pctile` and the numeric/coverage breakdown are
  assembled for the workbook by `build_combined.build_panel()` (grafting coverage from the in-force doc).
- **`all_indices.xlsx`** — the published workbook; hover any header or see the Dictionary tab.

## If you add a new topic / column

1. Emit the per-topic detail with `_numeric_z` / `_coverage_z` / `_numeric_pctile` /
   `_coverage_pctile` (via `il.build_simple_index`, which already does this).
2. Add the topic to `index_lib.DUAL_TOPICS` only if it has BOTH numeric fields and coverage booleans.
3. The composite's `il.apply_scheme()` will publish the headline `{t}_z` / `{t}_pctile` automatically.
4. Never reintroduce a bare `{t}_z` meaning magnitude, or a `gen01` suffix.
5. If the topic's driver is a CUSTOM module (not `build_simple_index`), expose a module-level
   `BOOLEANS = [...]` list so the factor/FAMD analysis can introspect it — `advanced_analysis`
   maps `leave`→`parental_leave_index` via `_topic_module`/`_topic_csv`; add a similar mapping for
   any new custom driver whose module/CSV name differs from `{topic}`.

## Reading caveats (before you quote a number)

- **Blank vs imputed vs zero-filled numeric cells**: see METHODOLOGY §7.2 (decision table). Short
  version: a blank becomes a number ONLY when a statute guarantees it OR a presence boolean confirms
  absence; **pension is always available-case (never imputed)** because blanks are sector-fund
  deferral, not absence (§7.1).
- **Multivariate analyses (factor analysis, Bartlett, correlations) run on document-editions, not
  deduped terms** — treat their inference as descriptive (METHODOLOGY §11.1).
- **z-rank ≠ pctile-rank within a topic** — two different aggregations, both correct (§11.1).
