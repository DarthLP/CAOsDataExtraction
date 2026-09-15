# DATA GUIDE — how every number is computed (written for humans AND for an LLM assistant)

Purpose: a single self-contained document that lets a fresh reader (or a Claude working in
another environment, e.g. the CBS/admin-data enclave) understand exactly what each dataset,
index and panel contains, how it was computed, and which caveats apply. Everything referenced
lives in this repo; deeper rationale is in `indices/METHODOLOGY.md` (why) and
`docs/DATA_LINEAGE.md` (audit trail of all 34 correction layers).

---

## 1. The data chain, one paragraph

Dutch CAO PDFs (uitvoeringarbeidsvoorwaardenwetgeving.nl) → parsed to markdown → LLM-extracted
to per-topic structured records (Gemini pipeline, external repo `CAOsDataExtraction`) →
**`qa/corrected_dataset.csv`** = the canonical cell-level dataset (2,739 records × 318 cols,
242 CAOs, key `id`; 34 audited correction layers applied — every change quote-grounded and
logged, see `indices/corrections/corrections_log.xlsx`) → per-topic index scripts (`indices/*_index.py`)
score each document → `indices/out/composite_index.csv` (one row per document) →
`indices/build_panel_monthly.py` expands to the **cao × month in-force panel**
(`out/all_indices_panel_monthly.csv`) → `all_indices.xlsx` is the human workbook.

## 2. The two date axes (READ THIS FIRST — it answers "why file_date?")

Every record carries several dates. Two matter:

- **`ingangsdatum`** — the agreement term's contractual start date. CAO texts are routinely
  signed and filed MONTHS after this date and apply retroactively to it (terugwerkende
  kracht + nabetaling is standard Dutch practice for wage increases among ORGANIZED
  employers). The corpus itself says so: the `general_retro_*` fields record explicit
  retroactivity clauses.
- **`datum_kennisgeving` (= `file_date` in the indices)** — the date SZW sent the
  kennisgeving van ontvangst (KVO) after the text was filed. Under the Wet op de
  loonvorming (art. 4) CAO wage provisions cannot formally enter into force before the KVO,
  and an AVV (extension to unorganized employers) NEVER works retroactively — unbound firms
  owe the new terms only from the AVV date forward.

**The monthly panel uses `file_date` (KVO) as the in-force axis** — deliberately: it is the
first date the text was (a) publicly knowable and (b) legally operative for the whole covered
population, and it is measured without the noise that plagues `ingangsdatum` (version-selection
findings: ingangsdatum is frequently missing/wrong/duplicated across editions). Consequence:
the panel measures terms as they became *known/enforceable*, not as they were *retro-dated*.
If your research design needs the retroactive-coverage view (e.g. wage costs actually owed for
month m), rebuild the panel on `ingangsdatum` from the CAO-level export (§5) and treat the
KVO−ingangsdatum gap as announcement lag; the export carries both dates plus the retro fields
so either axis can be constructed.

In-force rule (panel): the document in force in month m = the one with the latest
file_date ≤ end of m; a document owns months until the next document's month. Statutory-
anchored components are re-scored at the PANEL month's legal era ("law-month re-scoring"):
a law change hits every in-force CAO the month the law changes, not at the CAO's next filing.

## 3. Scores: what a z means here

- Per topic there are two tracks (see `indices/NAMING.md`, the naming authority):
  **magnitude** `{t}_numeric_z` (pooled z-scores of the numeric generosity fields, sign-
  corrected so higher = more generous, clipped, averaged) and **coverage** `{t}_coverage`
  (share of the topic's presence booleans that are True). Bare **`{t}_z` = the combined
  mean of the two.** A parallel percentile track (`_pctile`) rescales each field by
  equal-range percentile instead of z.
- **`overall_z`** = mean of the combined topic z's (+ `wage_median_z` as the wage headline);
  `overall_numeric_z` = magnitude-only roll-up. PENSION is available-case only (blanks are
  fund-deferral, never imputed). Zero-fill applies only to genuinely absent EXTRA benefits,
  gated on the benefit never being True in ANY edition of the CAO.
- **Wage**: `mw_indices.py` melts every edition's salary tables, dedups per wage cell, takes
  the CAO's median/mean minimum wage; scored NOMINAL (`wage_median_z` = z of mw_median);
  WML ratios exist only as `ratio_*_wml` value columns. **The wage `year` is the wage
  table's OWN effective date** (`salary_N_start_date`, the "per 1 July 2023" printed on the
  scale) — NOT the file_date and NOT the agreement's ingangsdatum. Wage tables carry their
  own dates, so the wage series is already on its natural (effectively retroactive) axis.
  Collision rules when files overlap: (1) same (cao, effective date, job-cell, unit) in
  several editions → the latest-filed edition (max kennisgeving rank) wins, never collapsing
  within one file; different effective dates in the same year (Jan scale + Jul raise) both
  pool into the year's ladder — ACROSS files: the cao×year row is a CAO-level fact, so a
  document row's wage block (joined by filing year) reflects the CAO's full ladder for
  that year, not only the document's own printed tables (incl. a mild look-ahead: a
  March-filed doc's year row may contain a raise filed later that same year — accepted,
  wage lives on the calendar axis); (2) term precedence — an expiring term's pre-announced
  forward table is dropped where a successor agreement (later ingangsdatum ≤ effective
  date) governs; drops logged to `mw_stale_forward_dropped.csv`; retroactive tables are
  never stale. Delta/annex editions are excluded from the wage build entirely.
  `mw_indices.csv` is NOT forward-filled (a cao×year row exists only where a text states a
  scale for that effective year; 427 internal gap-years across 138 CAOs; 27/242 CAOs have
  no parsed wage rows) — the fill is the consumer's as-of join. Verified on the panel:
  after merge_asof, 100% of remaining wage nulls are months before the CAO's first parsed
  wage year (5,078 cao-months, 13%); zero internal gaps survive.

## 4. The STATUTORY reference ("Statutory" tab / statutory pseudo-CAO)

Built by `indices/build_statutory.py`. It is a **hand-curated change-point table** — one row
per statutory entitlement per validity window: value, unit, role, the statute's NAME
(WAZO art. 5:6, BW 7:629, WWZ, WAB, WML art. 15, ATW, …) and a SOURCE LINK to
wetten.overheid.nl / Staatsblad / rijksoverheid.nl. Sources were verified against those
official portals in 2026-06; a few commencement dates carry explicit "VERIFY" notes in the
script (e.g. exact WAZO care-leave introduction day). Outputs: `out/statutory_all.csv`
(machine-readable; what the indices consume) and `review/statutory_timeline.xlsx` (formatted).
Roles: `floor`/`floor_lift`/`cap`/`default` rows actively feed the scoring (statutory floor
under CAO values, era-correct ketenregeling defaults, ATW caps); `informational` rows are
documentation only (e.g. "pension participation is mandatory by law in verplichtgesteld
sectors even when the CAO is silent"). The statutory pseudo-CAO is scored through the same
index pipeline and appears as its own rows in the panel — so "CAO generosity relative to the
legal floor" is directly computable.

## 5. CAO-agreement-level export (for the admin-data environment)

`indices/out/cao_agreement_level.csv` (~2,698 document rows in ~1,552 agreement terms, 242
CAOs), built by `indices/build_cao_level_export.py`; also the `AgreementLevel` tab in
all_indices.xlsx. **One row per DOCUMENT, organized by agreement term** — deliberately NOT
collapsed to one row per term, because within a term (same cao_number + ingangsdatum) several
files arrive over time (mostly wage-table updates, occasionally other conditions) and all
share the ingangsdatum; collapsing would lose the within-term evolution and with it the exact
panel. Term bookkeeping per row: `term_edition_seq` (1,2,… by file_date within the term),
`n_editions_in_term`, `is_last_filed_in_term` (filter on it for the one-row-per-term view).
Every row carries both date axes, retro/AVV/signing fields, thin-doc carry flags
(`thin_doc`, `scores_carried_from`, `score_src_id`) and all NON-SALARY scores. **The export
contains NO wage columns, and its overall roll-ups EXCLUDE wage** (2026-07-15): they are
named `overall_z_without_wage`, `overall_pctile_without_wage`, `overall_z_var_without_wage`,
`overall_numeric_z_without_wage`, `overall_numeric_pctile_without_wage` (+ matching
`n_topics_*_without_wage` counts) = the same equal-weight available-case roll-ups over the
12 non-salary inputs (10 dual topics' combined score + safety & childcare coverage).
Rationale: a wage year-row is a CAO-level, calendar-dated fact pooled across files (incl.
files filed later the same year), so a document-grain wage column would carry borrowed /
partly look-ahead information; verified: adding wage_median back into the mean reproduces
the composite's wage-inclusive overall_z exactly. PanelMonthly keeps the wage-inclusive
`overall_z` under the plain names — the two overalls are intentionally NOT comparable
across files. The monthly panel's NON-SALARY score columns are exactly reconstructable
from this file on any axis; two panel ingredients are time-varying and need their own
tables: (1) WAGE — the panel's wage block is as-of joined per (cao, calendar year of the
month) from `mw_indices.csv` (wage years = the scales' OWN effective dates, not filing
dates; one document carries several future years' wage tables; gap years forward-fill);
(2) the statutory-anchored topic z's are law-month re-scored in the panel (needs
`statutory_all.csv`). Rebuild recipe: expand documents by the chosen start-date column,
then merge_asof the wage table on year (direction backward).

**The FIRST-FILE axis:** the term's FIRST file (lowest file_date in the term) starts at
month(ingangsdatum) — the initial text is what governs from term start — while every LATER
file keeps its own file-month start (mid-term updates apply going forward). This axis lives
as the `Date_first_is_Ingangsdatum` column of the export (a standalone
"PanelMonthlyFirst"/"PanelMonthlyRetro" tab existed briefly on 2026-07-15 and was retired the
same day as redundant; build_panel_monthly.py main(axis="first", suffix="_first") can still
materialize it as a monthly panel with law-month statutory re-scoring if ever needed).
PanelMonthly (kennisgeving axis) = filed-and-enforceable view; the first-file axis =
term-start-coverage view; `pub_lag_months` quantifies where they differ.

**Ready-made start-date columns in the export** (next to file_date/ingangsdatum):
`Date_first_is_Ingangsdatum` = the first-file rule per document (term_edition_seq 1 →
ISO(ingangsdatum), later editions → file_date; always filled, ISO format).
`Date_retro_datum` = the same, overridden by the EXPLICIT `general_retro_start_date` from the
corrected dataset where one exists (556/2,698 documents; 393 actually change date — mostly
later editions whose retro clause the first-file rule cannot see); otherwise it equals
Date_first_is_Ingangsdatum. Explicit retro clauses exist for only ~25% of records (671/2,739
flagged, 567 with start dates), so the first-file rule is the robust default anchor and
Date_retro_datum the refinement for finer designs.

## 6. Quality state (what you can rely on)

- 34 correction layers, ~12k corrected cells; every change in
  `indices/corrections/corrections_log.xlsx` (per-layer tabs, old→new, verbatim source quote,
  provenance). Mechanically verified: logs replay exactly; zero unlogged changes; churn 0.9%.
- Standing battery (`indices/check_battery.py`, run by `indices/rebuild.sh`): sign/semantics
  audits, percentile sanity, **statutory-fingerprint screen** (time-aware restatement
  suspects; ~1.15k never-agent-checked rows = the queue for the next holistic pass) and the
  **same-term coverage invariant** (every sibling-edition disagreement has both sides
  agent-adjudicated; currently 0 uncovered).
- Jump census (`indices/jump_census.py`): 1 verified genuine overall drop (CAO 65 orchestra
  remplaçanten series switch), 0 unexplained V-dips/spikes.
- Same-term boolean flip rate: 9.1% → 3.3% mean (monitor: `indices/boolean_flip_rate.py`).
- Open judgment items live in `indices/review/open_cant_tells_for_hanna.csv` (bucketized; also a
  tab in indices/corrections/corrections_log.xlsx).
- Known residual risk class: cells wrong the SAME way in every edition are invisible to
  the jump/diff-based QA; the fingerprint queue plus a future holistic verify-every-value
  pass address this.

## 7. Reproducing everything

```
cd indices && bash rebuild.sh     # drivers -> wage -> statutory -> composite -> panel -> workbook -> battery
python3 jump_census.py            # census
python3 same_term_coverage_check.py   # coverage invariant
python3 build_cao_level_export.py     # the agreement-level export (§5)
```
Environment: python3.13, pandas, openpyxl; CSV delimiter is `;` throughout.
