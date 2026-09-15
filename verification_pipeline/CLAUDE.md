# Dutch Bargaining Agreements — Project Context

Data extraction + QA pipeline for Dutch Collective Bargaining Agreements (CAOs).
Owner: Hanna (hannaw.econ@gmail.com).

## What to read first

1. `README.md`                             — project overview + the copy→correct→index pipeline
2. `docs/DATA_LINEAGE.md`                  — which dataset is canonical, what's applied vs pending
3. `docs/DECISIONS.md`                     — why the pipeline works the way it does
4. `qa/PLAN.md`                            — architecture and conventions
5. `qa/conventions/general_conventions.md` — rules every subagent must follow
6. `reference/qa_leave/README.md`          — reference implementation status

`qa/EXPERT_IMPLEMENTATION_PLAN.md` is the (now largely historical) ordered action plan.
If you have not read these, stop and read them before doing data work.

## Where things live

- `inputs/`              — raw CSVs and per-topic source-text MDs. READ-ONLY.
- `reference/qa_leave/`  — reference implementation, COMPLETE, do not modify
    (moved from top-level `qa_leave/` on 2026-07-01).
  - `reference/qa_leave/inputs` is a symlink to `../../inputs` for backward-compat
    with leftover scripts that hard-code the old path.
- `qa/`                  — QA pipeline engine (active work).
- `qa/corrected_dataset.csv` — CANONICAL corrected dataset. Never edit in place;
  apply→copy→verify→promote. See `docs/DATA_LINEAGE.md` + `qa/DATASETS.md`.
- `qa/shared/`           — topic-agnostic reusable code.
- `qa/conventions/`      — subagent rules + failure-mode catalogue.
- `qa/qa_<topic>/`       — per-topic outputs and scripts.
- `qa/full_audit/`       — cross-cutting dataset-internal QA + verification (surface-only).
- `indices/`             — Stage 2 derived indices, TOP-LEVEL (read the canonical dataset).
    - `indices/METHODOLOGY.md` = the WHY/rationale doc (z formula, two scales z + gen01,
      two tracks magnitude/coverage, statutory floor, zero-fill, per-topic inventory,
      **Decision Log — append to it on any indices modelling change**). Results live in
      `indices/ADVANCED_ANALYSIS.md`. ⛔ PENSION is available-case only (never impute blanks).
- `salary/`              — separate salary-scale QA track, TOP-LEVEL.
- `reference/qa_leave/`  — frozen leave reference implementation.
- `docs/`                — DATA_LINEAGE, DECISIONS, plans; `docs/archive/` = history.

## Hard rules (do not violate)

- NEVER modify files in `inputs/` or `reference/qa_leave/`.
- NEVER edit `corrections.csv` directly — always through the aggregator.
- NEVER spawn a fix-subagent on audit-flagged rows. Surface, do not auto-fix.
- NEVER invent values not in the supplied source text.
- NEVER modify `qa/conventions/general_conventions.md` without explicit ask.

## Critical: source text is ENGLISH

The per-topic source files (`inputs/by_topic/*_information.md`) are
**English-translated** content. The upstream extractor translated the
original Dutch CAOs into English. Therefore:

- **Keyword sets in `qa/shared/field_keywords.py` must be ENGLISH-PRIMARY.**
  When seeding `_MANUAL_FIELD_KEYWORDS` for a new topic, write 15-30
  natural English phrases per field as the bulk of the keyword set.

- **Retain Dutch terms ONLY when they survive translation** — i.e. when
  they actually appear verbatim in the English-translated source:
  - Statutory acronyms: AOW, WAB, WWZ, BW, UWV, ATW, WAZO, WIEG, BHV,
    EHBO, PSA, FTE, RI&E, ORT, TOIL, AVG, ZW, WIA, WGA, RVU, VPL, EVC, OR
  - Pension fund names: ABP, BPF, PME, PMT, PFZW, PNO, bpfBOUW,
    "Stichting Bedrijfstakpensioenfonds"
  - Legal acts/concepts: "ketenregeling", "ketenbepaling",
    "transitievergoeding", "kantonrechter", "Scheidsgerecht",
    "Wet flexibel werken", "Wfa", "Burgerlijk Wetboek", "BW", "Artikel X:Y"
  - Untranslatable concepts: "mantelzorg", "kraamverlof",
    "ouderschapsverlof", "zwangerschapsverlof", "eindejaarsuitkering",
    "13e maand", "dertiende maand", "vakantiegeld", "vakantietoeslag",
    "tijd voor tijd", "TVT", "compensatieuren", "thuiswerkvergoeding",
    "instaptrede", "trede", "ploegendienst", "ploegentoeslag",
    "onregelmatigheidstoeslag", "consignatie"
  - Formal scheme names: "O&O-fonds", "scholingsfonds",
    "studiekostenbeding", "terugbetalingsbeding",
    "Regeling Vervroegde Uittreding", "Witteveen", "Witteveenkader"

- **DO NOT include Dutch multi-word sentence-fragments** like "door
  werkgever in acht te nemen", "in geld uitbetaald", "naar keuze van de
  werknemer". These were translated to English by the upstream extractor
  and will NEVER match in source. They only inflate the keyword set
  without ever firing.

- **Verify the language profile when adding a new topic's keywords.**
  Quick check: read 5 random JSON-array passages from
  `inputs/by_topic/<topic>_information.md`. They should be mostly English
  sentences with occasional Dutch acronyms/proper nouns. If a topic file
  has substantially more Dutch than the others, flag it — could be an
  upstream extractor issue or an untranslated section.

See `qa/shared/field_keywords.py` module docstring for the canonical
pattern and `qa/PLAN.md` §3.4 for the maintenance workflow.

## Stop conditions

See `qa/PLAN.md` §0.4 for the full list. Single source of truth — do not
duplicate here. When in doubt, stop and ask.

## Environment

- Python: `python3` 3.13.0 (no PEP 668 enforcement — `--break-system-packages`
  flag NOT needed).
- pip: 24.2.
- Tabular: pandas + the resilient_csv shared module.
- CSV delimiter: `;` (semicolon) for `inputs/extracted_data_non_salary.csv`.
- Git: not in use for this project (single-user research workflow).

## Subagent execution

- Task tool, subagent_type: general-purpose (or Explore for read-only).
- Parallelism: **MAX_PARALLEL_HARD_CAP = 12** (verified empirically in Phase 0.5).
- Default `max_parallel = 12` in `subagent_runner.run_chunks`. Do not exceed.
- Each subagent prompt must be self-contained.
- **ALWAYS give a verification/judgment subagent the field's ORIGINAL EXTRACTION
  CONTEXT** — the schema/prompt description of what the field is meant to contain
  (the pydantic field description in `inputs/NON_SALARY_PROMPTS_AND_SCHEMA.md`,
  e.g. `leave_information.paid_maternity` = "Duration of fully paid maternity
  leave, value=16, unit='weeks'"). A subagent that doesn't know what a field
  *means* judges values against a guess. **Lesson (2026-06, full_audit):** the
  definition-LESS holistic verification had a **~22% error rate** on its proposed
  corrections; re-running each correction WITH its field definition as the anchor
  caught them (probation "max" 1→2 months, surcharge "max" 17→80%, pay-% wrongly
  placed in a duration field, etc.). Pass the definition for every field judged.
- **Never fill a statutory/silent value.** Only write a value the source states
  EXPLICITLY (a number). Source silent → leave the field empty. Source says
  "statutory" without a figure → leave empty. Do NOT convert/annualize/infer
  (e.g. "1 day/week" → annual days) — that invents a number. Put the displaced
  fact in a note, not the cell. (Reinforces the global "NEVER invent values" rule.)
- Subagents must NOT spawn further subagents.
- Pause between topics to manage weekly rate limit.
- Model: **Haiku by default for subagent campaigns** (Hanna's rule, 2026-07-03) — but
  CALIBRATE on a labelled slice first when the task shape is new, and use the calibration
  to pick the tier (e.g. snippet-judging needed Sonnet: Haiku was equally safe but 54%
  over-cautious). Opus for pension/term-grade legal semantics. Always pair a cheap-model
  campaign with an independent spot-verify gate before applying.

## Context budgets (enforced by qa/shared/worksheet_builder.py)

- Static system prompt: ≤ 3,500 tokens (raised from 3,000 in Phase 2 — actual conventions files came in larger than PLAN.md §4.5 estimates).
- Per-item context, soft target: 2,000 tokens; hard cap: 4,000 tokens.
- Total chunk context: ≤ 50,000 tokens.
- Chunk size: 15-20 items (builder computes dynamically per per-item size).
- The builder raises `BudgetExceeded` if any cap is exceeded.

**Per-topic overrides** (see Phase 0.5 findings):
- `bonus` and `wage` share `wage_information.md` averaging ~3,779 tokens/section.
  Set per-item soft target = 3,500 and hard cap = 6,000 in
  `qa_bonus_aggregate.py` and `qa_wage_aggregate.py`.
- `leave` averages ~2,107 tokens; borderline. Watch truncation rate if re-run.
- `overtime` averages ~1,555 tokens; default caps are fine.

## Project scope

- 95 unique CAOs × ~16 versions = **1,505 records per topic** (the curated
  "100 biggest CAOs" subset; actual is 95).
- 12 non-leave topics: bonus, wage, pension, term, overtime, training,
  homeoffice, contract, safety, childcare, ai, fringe.
- Total CSV parses to 2,739 records across 242 unique CAOs (key column `id`);
  ~1,505 records across 95 CAOs are the per-topic QA scope. (Earlier docs said
  2,836/338 — that was a raw line count; 2,739 is the pandas record count.)

## After each completed stage

- Update `qa/README.md` status board.
- Append a log line to `qa/qa_<topic>/run.log`.
