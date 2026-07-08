# Salary parser — delivered dataset & known limitations

## What this is
A deterministic replacement/augmentation of the second-LLM salary step. It parses the
first-LLM wage-table grids (`outputs/llm_extracted/new_flow/<cao>/*_extract.json`,
`wage_information`) directly into the salary schema, using the old LLM only to cross-check
labels (never amounts). Output: **`outputs/parser_salary/extracted_data_salary_v2.csv`**
(semicolon-delimited, same wide schema as `extracted_data_salary.csv` + confidence columns).

**Current dataset (2026-07-08): 359,474 rows, A+B = 95.4%** (A 254,334 · B 88,777 ·
C 13,693 · D 2,670), 100% amount+date provenance. Every row carries version tags (term_group /
kennisgeving_rank / base_id / n_editions / document_type). Wage index (`indices/mw_indices.py`)
covers 215 CAOs (version-aware two-stage dedup); per-file workweek maps back-fill hourly ft_hours. **Rebuild = one command:
`python3 salary_parser/deliver.py`** — stage 5.5 auto-re-applies every relabel + agent-extract
merge onto the freshly-parsed rolecheck_all (youth-strip + coverage gate) before flatten, so no
manual re-application is needed; the HARD GUARD aborts if amount provenance drops below 100%.

## Guarded agent extraction (hard structural tables) — `agent_extract.py`
For the ~84 files whose tables the deterministic parser cannot lay out (2948 VVT
section-tables, 759 SAG MIDDEN-ladders, 725 diagonals — mostly-C/D files), a **guarded
whole-file agent pass** replaces the parser's flagged rows:
1. `build`  — emit the raw tables + the file's SOURCE NUMBER UNIVERSE + date universe.
2. a Haiku agent extracts every wage cell (jobgroup/step/worker/age/unit/amount/date).
3. `validate` — **G1 (hard): every amount must be an exact member of the source number
   universe** → provenance is preserved by construction; off-source amounts are rejected,
   never merged. Plus G2 date-in-universe, G3 unit whitelist, G4 coordinate-label-in-source.
4. `merge` — accepted rows REPLACE that file's rows in `rolecheck_all`, tagged
   `role_verdict=AGENT_EXTRACT`, `label_source=agent_extract` → **tier B** (labels are
   agent-provided, amounts are source-proven). A `.preagent.bak` of each doc is kept.

The guard is the safety net: in wave 1 a decimal-format misread (`526.00`→52600) produced
2007 bad amounts and **all 2007 were rejected** — 0 wrong amounts entered. Only targeted at
deliberately-selected mostly-C/D files; never touches clean files.

**Wave 1 (2026-07-07):** 4 files (2948 VVT section-table, 1193, 2165, 429) — the parser had
~1,481 C/D flagged rows across them; agent extraction replaced them with **1,205 tier-B rows
at 0 C/D and 100% amount provenance** (e.g. 2948 source-amount coverage 28% → 87%). Accepted
rows persisted under `outputs/parser_salary/agent_extracted/<wave>/`. 544 was held out (the
agent correctly skips its per-ton/premium tables → too much base-coverage loss to replace).

**DURABILITY:** like `relabel_merged`, agent merges live in `rolecheck_all` which `deliver.py`
regenerates. After any `deliver.py` re-run, re-apply with
`python3 salary_parser/agent_extract.py merge outputs/parser_salary/agent_extracted/<wave>`
for each wave dir, then `flatten_csv.py`.

## Core guarantee
- **100% amount provenance** — every emitted amount appears verbatim as a source cell
  (0 invented), verified deterministically. ONE documented exception class (2026-07-05,
  Hanna-authorized): **mangled-decimal rescue** — an upstream artifact like `2.18454`
  (a European `2.184,54` that lost its comma) is rescued by a digit-preserving decimal
  shift (→ 2184.54) ONLY when the raw cell has a single dot with 4-6 decimals, a nonzero
  integer part, sits ≳50× below its own table's clean-cell median, and the rescued value
  lands within 4× of that median. Corpus-wide this fires on exactly **2 cells (CAO 592)**;
  tiny genuine factors (`0.0018`, CAO 3866 multiplier tables) are refused by the guards.
  Rescued points carry `value_source=rescaled` + a `rescaled from <raw>` note in the CSV,
  are capped at tier B, and the provenance audit counts them separately (never as verbatim).
- **100% date provenance of dated points** — dates only from column headers / titles / date
  row-axes; 5.9% of points are honestly undated (source states no date — never guessed).
- **Labels only re-roled from the parser's own tokens**; no old-LLM value is written into a row
  (worker/education/etc are filled only as a table-level constant, tagged `worker_source`).

## Scale
359,474 rows over ~2,700 files (all versions of 242 CAOs; 2026-07-08 run, after relabel +
chunked structural re-extraction + workweek/unit/note fixes). Row-confidence tiers:
| Tier | Meaning | Rows | Share |
|---|---|---|---|
| A | confirmed high (parser + old-LLM vocab agree) | 256,218 | 71.8% |
| B | confirmed medium / re-roled / no old-LLM to compare / rescaled / haiku-relabel | 71,597 | 20.1% |
| C | soft flag (label unconfirmed, dup-date, gross-inversion, junk-label, ragged-pad) | 26,189 | 7.3% |
| D | hard flag (magnitude outlier, min>max, nonwage-smallint) | 2,662 | 0.7% |

**A+B = 91.9%** are high-trust (up from 83.2% pre-audit). Filter `confidence_tier IN
('A','B')` for the clean subset; `flag_reasons` explains every C/D row.

2026-07-05 fix: **number-first step headers** ("0 Functional year" … "9 Functional years",
CAO 709 class) are now recognized as a step axis — previously all such columns collapsed
into one dup-date identity and the header's "year" leaked into unit detection as `annual`.
Now each column is its own step, dup_date on those files drops to 0, unit reads correctly
(verified by a 13-cell Haiku spot-check incl. the step-0/step-9 staircase edges). Zero
CAOs lost points in the regression diff.

2026-07-07 **holistic-audit fixes** (a 12-file all-variable Haiku check found these; each
verified against source, corpus-diffed at 100% provenance — see `PARSER_RULES.md`):
- **Zero-drop**: `0`/`0.00` placeholder cells are no longer emitted as wages (~3.3k spurious
  rows removed, 2.7k of them previously tier A).
- **Footnote strip**: `2185.13*`/`†` starred cells (usually "= statutory minimum") are now
  parsed, not dropped (+points on CAO 1165, 4202 HISWA, …).
- **Year-band salary matrices**: bare integers 1900–2035 in ZKN/metal Trede grids
  (`1919 | 1938 | 1980`) were voided by the calendar-year guard, misclassifying whole money
  columns as keys and fusing amounts into the step label. Now detected per table
  (`table_yearband_is_salary`: money straddling the band) and treated as wages. Fixed CAO
  3688 (864 junk rows → 672 clean), 214, and the metal cluster.
- **Ragged merged tables**: youth+function-year grids (metal/technical "Age/Function Years"
  tables, CAO 2297/826) have narrow youth rows and wide step rows under one superset header;
  column classification now uses the whole column, so scale columns are no longer misread as
  keys. Age rows → `age_group`, step rows → `step`. CAO 2297: 22 junk → 0; 826: 300 → 0.
- **Repeated header row** (`Group | 1 | 2 | … | 11` mid-table, CAO 776) is skipped, not
  emitted as amounts 1..11.
- **Labels**: `Functieaanvangsalaris`→`is_entry`; `basis van 12 maanden`→`annual` (CAO 632
  Banken 23k–133k now annual); `maandloon A`/`uurloon B` value headers → jobgroup A/B + unit.
- **New honest flags**: `junk_label` (money-sized/clock token in a label = mis-parse → C) and
  `nonwage_smallint` (small-integer ordinal rows → D).

Net effect: junk_label rows roughly halved, A+B share rose.

2026-07-08 **label/unit/timeline round** (Hanna follow-up; all corpus-diffed at 100%
provenance, label/unit-only so points unchanged):
- **Magnitude unit inference** — when the source states NO pay period anywhere, `unit` is
  inferred from amount magnitude (`<60`→hourly, `1500-8000`→monthly, `≥20000`→annual;
  mid-gaps stay null) and TAGGED `unit_inferred_magnitude` in `flag_reasons` so it is
  auditable, never silent. Hourly/annual are unambiguous; monthly overlaps 4-week/weekly, so
  those are a flagged best-guess (CAO 3335 sector scales now `monthly`).
- **Transposed jobgroup columns** — `C (monthly wage)` / `A and B (monthly wage)` (CAO 1644):
  the `(…wage)` clause set the unit and blocked jobgroup extraction, so age-rows collapsed
  into one same-date row. Now the scale label is extracted despite the unit clause → rows
  split correctly by (age × jobgroup).
- **Progression-step columns** — `1e halfjaar (uur)` / `2e halfjaar` / `inloop` (CAO 10 Bouw)
  are now distinct STEPS, not a collapsed same-date timeline; fixed `3e halfjaar`→`annual`
  misread (the `jaar` in `halfjaar`). The `(scale)` fallback placeholder is gone (→ null).
- **Pure-% labels** — `40%` / `87,50%` from youth `staffel` columns are moved to `row_note`
  (`staffel jobgroup=87,50%`) and the label nulled (never a valid jobgroup/step name).
- **Eligibility dates** — `hired before July 1, 1994` (CAO 750) is stripped before the title
  date is read, so it is not mistaken for the wage-effective date.

Confidence tiers below are the 2026-07-08 (v18) run. ~8.2k rows still carry `dup_date` where
the source has genuinely multiple amount columns at one date (min/max, hour-variants, or the
same scale defined in two tables) — flagged, amounts correct.

## Confidence columns (appended to each row)
`confidence_tier` (A-D) · `role_verdict` (CONFIRMED/RE_ROLED/FLAG/…) · `flag_reasons`
(pipe-separated) · `label_source` · `worker_source` · `source_file` · `coverage_missing_tables`.

## Pipeline (all deterministic / no LLM at run time)
`run_parser_pipeline.py`: parse+audit → count-gate → role-check merge → confidence roll-up.
`all_files.py` runs it over every version file; `flatten_csv.py` emits the wide CSV;
`quality_checks.py` provides the free coverage + monotonicity checks.

## Known limitations (Haiku-verified patterns; all are FLAGGED, never silently wrong)
Amounts are correct in essentially all of these — the issues are structure/labels, and each
is routed to a C/D tier rather than shipped as tier A:

1. **Two-axis tables** (e.g. ORBA-band columns × Group rows, CAO 190): the parser may take
   the column header as jobgroup where the row label was intended. Amounts correct, jobgroup
   label wrong. Not auto-fixed — the fix would regress transposed step-as-column tables.
2. **Sub-category columns** (department/afdeling columns, CAO 449): multiple columns collapse
   into one row's timeline → `dup_date` flag. Amounts correct; row should be split per column.
3. **Min/max interior-gap staircases** (CAO 637): rows with blank interior cells can misalign
   a max column. Caught by the min>max check (tier D). Historically regression-prone to fix.
4. **Step-number fragmentation** (CAO 604): when a class's step/periodiek count changes across
   versions (27→28), the same scale appears as two rows instead of one timeline. No data loss,
   just fragmentation.
5. **`missing_tables` (5 CAOs: 848, 1228, 1393, 1646, 2018)**: a salary table the old LLM has
   but the parser did not emit (corroborated real gap). Flagged at CAO level.
6. **Upstream PDF corruption** (e.g. CAO 1188 `310` for ~3100): the parser faithfully captures
   a value the source itself has wrong. Not a parser issue; a magnitude-outlier flag catches
   the gross cases.
7. **Benign flags** (not errors): `gross_step_inversion` false-positives when a step field
   mixes age and experience labels (CAO 140); `dup_date`/`magnitude` false-positives on
   legitimate dual-unit-per-date tables (CAO 27). These sit in C, cost only a review glance.

## Not attempted (by design)
- **Cross-version timeline merging** — scale renames/regrades make versions non-comparable;
  merging would fabricate continuity. Output stays per-(file). Canonical-version selection is
  a separate downstream workstream. `duplicate_files.csv` flags near-identical version files.
- Auto-fixing flagged rows, statutory/silent value inference, unit conversion, doc-date fallback.

## Full low-confidence sweep (2026-07-04)
ALL 61 llm_fallback CAOs were Haiku-diagnosed (trimmed positional payloads). Outcome:
**~50% are correct data conservatively flagged** (25 of the 50 newly checked at 90-100%
positional accuracy — the flags cost a review glance, no data damage); the other half have
genuine structural label issues of the known-limitation classes below. Two additional safe
fixes came out of the sweep and are in the parser:
- dotted classification codes ('01.04.05') no longer parse as pseudo-amounts;
- tantième / leave-buy-back supplement tables excluded (not base pay).
Confirmed NOT-safely-fixable classes staying in llm_fallback (route to fallback/manual):
two-axis label grids (ORBA×Group), scheme fusion (DEEL A %WML + DEEL B scales in one
block), composite age/tenure cells ('18/0'), multiplier tables needing a reference-table
join (CAO 3866), upstream column-bleed (ipnr glued to amounts, CAO 1574), mixed
monthly/annual timelines (CAO 4273).

## Verification performed
100% deterministic provenance + coverage + monotonicity, plus ~30 Haiku ground-truth audits
across the fix iterations and a stratified gate (8/12 clean PASS on auto_accept; the failures
were fixed at root or structurally demoted). Real bugs found & fixed this way: transposed
step-columns, three single-string table formats, preamble headers, multi-dot decimals
(`1.653.60`), percent-only columns, cross-table-family merge contamination.
