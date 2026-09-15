# Roadmap — everything that is left (master to-do)

Written 2026-07-03, after the Layer-4 (G3) promotion. **This supersedes `qa/MASTER_REVIEW_FOR_HANNA.md`
as the master to-do.** State of the world: dataset = G3 (raw + 4 verified layers, ~6,917 cells),
indices rebuilt on it, 152 tests green, docs in sync (`docs/DATA_LINEAGE.md` is the ledger).

> **UPDATE (2026-07-03, later): ✅ WAVES 2 + 3 EXECUTED — dataset = G4 (Layer 5, ~9,022 cells vs raw), indices rebuilt, 152 green.**
> Wave 1 partially cleared: 1.1 rulings received & applied (DELETE patterns + conditional); 1.2 default-keep stands (optional).
> Wave 3 done: statutory_deferred folded into the L5 adjudication; `RETIRED_QUEUES.md` written; audit prompt archived.
> **UPDATE 2: CHECK-pattern suggestions accepted → Layer 6 applied (151 cells) → dataset = G5 (9,155 (measured) cells vs raw).**
> **UPDATE 3 (2026-07-08): dataset = G9 — L7 residue (40) + L8 boolean/numeric flip fixes (3,559, all 242 CAOs via `source_lookup.py`) + L9 numeric same-term fixes (61, two-pass verified) + L10 consistency-unify/tie-break/recovery (116, incl. Hanna's tie decisions). Two-pass verify (Sonnet propose → independent 2nd-reader) now STANDARD for numerics. Indices rebuilt, battery green. All 30 ambiguous ties resolved.**
> Still open: 1.3–1.7 (Hanna) · Wave-2 residue skim (48 rows, `qa/full_audit/wave2_residue.csv`) ·
> Wave 4 (holistic, deferred) · Wave 5 (version selection) · salary track (**21 unparsed-wage CAOs — triage pending**) ·
> optional: consistently-wrong booleans (invisible to flip method), leave presence/absence blank-vs-value flips.
>
> **UPDATE 4 (2026-07-15): dataset = G33 (34 layers).** The jump campaign (L14–L19), tier-A/B/C
> same-term campaigns (L20, L24–L26), CANT_TELL closures (L21, L29, L32–L33), Hanna rulings (L22),
> corrupt-parse re-extraction (L23), ripple closure to fixed point (L27–L28), the L30 audit +
> L31 restorative re-verification (field-boundary doctrine), and the L34 follow-up-flags closure are
> ALL DONE — details in `docs/DATA_LINEAGE.md` + `indices/corrections/corrections_log.xlsx`.
> End state: same-term coverage 0 uncovered, census 1 verified genuine drop, battery green,
> boolean flip rate 9.1%→3.3%. Indices v2 workbook `indices/all_indices.xlsx` (agreement-level
> export + ready-made start-date columns for the admin environment). **Now open:** 30 judgment
> cells (`indices/review/all_open_cant_tells.csv`, incl. the 249001 cross-session disagreement) ·
> ~1,150 statutory-fingerprint suspects = the Wave-4 holistic queue · schema-boundary rulings
> (job_allowances vs qual_bonus; safety_committee vs sectoral arbo) · 3313004 on-disk parse
> artifacts need refresh · stub extracts 465001/465011/506001 + 1618/20001 blackouts re-extraction.

Organized as **waves**: each wave's items can run together; later waves depend on earlier ones.
Every item says WHO acts, WHAT happens, EFFORT, and WHY it exists.

---

## Wave 1 — Hanna's decision batch (~1–2 h of your time, zero tokens)

Everything here is input the pipeline is waiting on. One sitting clears it all.

| # | Item | Where | Your action | Size |
|---|---|---|---|---|
| 1.1 | **Numeric removals — pattern rulings** | `qa/full_audit/removals_numeric_ruling_sheet.csv` | Fill `RULING` (KEEP_VALUE / BLANK / CASE_BY_CASE) per field-pattern row | 45 rows (covers 547 cells) |
| 1.2 | **Bool/enum removals — review skim** | `qa/full_audit/removals_bool_enum_review.csv` | Rows are grouped by field with snippet + judge's reason; mark keep/flip (bulk-mark by field group is fine) | 929 rows ≈ realistically ~60 field groups |
| 1.3 | **Statutory VERIFY rows** | `indices/review/statutory_timeline.xlsx` (tagged "VERIFY") | Confirm/correct: adoption-leave 4→6 wk date, 2026 severance cap, pre-2015 ketenregeling | 3 values |
| 1.4 | **Salary step-2 sign-offs** | `salary/SALARY_QA_MEMO.md` §Open items | Accept the 8 unit fixes; decide the 6 CAO-408 placeholders | 2 decisions |
| 1.5 | **Retire the agreement-consistency queues?** | `qa/full_audit/agreement_conflicts.csv` (18,515) + `backfill_candidates.csv` (17,033) | Decision: mark SUPERSEDED. Rationale: the per-file campaign re-verified every within-agreement conflict at cell level (stronger evidence), and back-fill-from-siblings is banned by *never-assume-silence*. Residual value ≈ none. | 1 decision |
| 1.6 | **`LIFECYCLE_STAGE_PLAN.md` — keep or archive?** | `docs/` | Still want lifecycle_stage flags? If not → `docs/archive/` | 1 decision |
| 1.7 | **Git snapshot** | repo (1 commit, everything untracked) | Approve one commit of the current clean state (protection against iCloud sync accidents; no push needed) | 1 decision |

`FULL_DATASET_AUDIT_PROMPT.md` is already executed (the full_audit) — I'll archive it in Wave 3 unless you object.

---

## Wave 2 — the closing correction campaign (me; ≈ 1 session, roughly Layer-3/4 scale)

Runs once Wave 1 items 1.1–1.2 are back. Produces **Layer 5 → G4** via the standard
apply→copy→verify→promote→rebuild discipline.

| # | Item | Method | Size |
|---|---|---|---|
| 2.1 | **Phase C: FIX_audit adjudication** — *approved (method + conventions: CAO base figure, surcharge = increment)* | Definition-anchored subagent pass per (agreement × topic) unit, reusing the perfile worksheets; each cell resolved under the conventions; quote-backed survivors → apply list; ambiguous residue → small human queue. Calibrate on a labelled slice first (same pattern as L4); Haiku vs Sonnet decided by that calibration, per your model rule. | 1,416 cells |
| 2.2 | **Resolve numerics per your 1.1 rulings** | Pure code: BLANK → remove; KEEP_VALUE → unify the ruled value across that agreement's siblings (evidence-based, not inheritance — the ruling declares the shared text supports it); CASE_BY_CASE → append to 2.1's residue queue | 547 cells |
| 2.3 | **Apply your 1.2 review marks** | Pure code from your marked CSV | ≤929 cells |
| 2.4 | **surcharge_normalize** | Superseded by the increment convention — re-derive the 41 toward increments inside 2.1 (they're overtime FIX_audit cells by nature) | 41 cells |
| 2.5 | **Promotion + rebuild** | One combined L5 apply-copy, changelog, backup `_old/corrected_dataset.bak.<date>.csv`, promote → G4, rebuild all indices, pytest, docs/ledger/memory sync | — |

**Not in Wave 2 (parked deliberately):** `REVIEW` 498 (campaign itself called them undecidable — revisit
only if 2.1 produces a convention that resolves subfamilies) and `statutory_deferred` 172 (Wave 3).

---

## Wave 3 — deterministic statutory step + queue retirement (me; small, mostly code)

| # | Item | What | Size |
|---|---|---|---|
| 3.1 | **statutory_deferred (172 cells)** | These are cells whose source only cites the statutory rule. Under the *no-statutory-fill* policy the value stays EMPTY; the deterministic step is to (a) confirm each cite matches `indices/out/statutory_all.csv` for its era, (b) blank any numeric that is a statutory restatement, (c) log the statutory pointer in a note column — never in the value. Small script + spot-check; no LLM needed for most. | 172 cells |
| 3.2 | **Retire superseded legacy queues** | Cross-check the old full-audit residue (`handoff/03_needs_judgment` ~629, `02_relocate` 17, `unit_semantics_reconciliation` 309, NHR/verify escalations ~14) against the L1–L5 changelogs; most cells have since been adjudicated by stronger layers. Output: one short `RETIRED_QUEUES.md` saying per queue what covered it, and a residual list (expected: small) for a final skim. | doc + script |
| 3.3 | **Archive housekeeping** | `FULL_DATASET_AUDIT_PROMPT.md` → archive; `MASTER_REVIEW_FOR_HANNA.{md,csv}` → marked superseded by this roadmap; (optional, Hanna-gated) `reference/qa_leave/` one-off scripts → `reference/qa_leave/archive/` | tiny |

---

## Wave 4 — holistic rollout (me; the one big remaining verification campaign — deferred by your call)

**What:** the only layer that re-reads **already-populated** cells that never conflicted (per-file only
covered conflict cells; L2 presence only scans empties). Validated on pension; found 2 net-new errors
per ~35 fields there. **12 topics remain ≈ 400+ chunks.**

**Why it might matter:** it's the last systematic blind spot — a wrong value that is *consistent across
versions* has never been re-examined by anything.

**Scope options when you green-light it** (pick then):
- **(a) Full rollout** — all populated fields × 95 in-scope CAOs × 12 topics. Multi-window, largest spend.
- **(b) Index-weighted** — only fields that actually enter the indices (roughly halves the cell count;
  covers everything that affects your analyses).
- **(c) Risk-sampled** — 1 topic (e.g. overtime, most numeric) as a measurement pass; decide full/stop
  from its hit-rate. Cheapest way to learn whether (a) is worth it. **Recommended entry point.**

**Sequencing:** after Wave 2 (so it re-reads G4, not a moving target). Model per calibration; snippet-style
pre-screens don't work here (it must *read* values in context), so this is genuinely campaign-priced.

---

## Wave 5 — version selection & de-duplication (the analytical capstone; design already agreed)

Per `docs/CAO_VERSION_SELECTION_PLAN.md` (status: *design agreed, not yet built*). Now **more** valuable:
L4/L5 made siblings consistent, so picking a canonical edition is cleaner.

1. **Version-tagging builder** — add `term_group`, `kennisgeving_rank`, `base_id` to `qa/corrected_dataset.csv`
   and `salary/outputs/corrected_salary.csv` (group `cao_number`×`ingangsdatum`, rank by `datum_kennisgeving`;
   robust file_name matching). *Prerequisite: locate your already-run comparison output (plan §prereq).*
2. **QA similarity diagnostic** — 4-gram Jaccard + numeric-diff between consecutive editions; flags
   "similar input → divergent output" extraction-reliability cases.
3. **Manual queues** — 22 only-partial groups + step-2 flags.
4. **Indices integration** — `indices/index_lib.py` consumes `term_group`/`kennisgeving_rank` (dedup to one
   edition per term before standardising); before/after comparison for sign-off.

---

## Continuous / external track — salary (yours, in progress)

- **Step-1 re-extraction rollout** (v1 prompt validated; ~20% of files have major extraction defects).
  When it lands: re-export `extracted_data_salary.csv` → re-run salary step-2 QA → regenerate
  `salary_increase_events_derived.csv` upstream → **rebuild `mw_indices`** (currently the one index on
  un-QA'd data — documented caveat in `docs/DATA_LINEAGE.md`).
- Step-2 sign-offs are Wave 1 item 1.4.

---

## Dependency map

```
Wave 1 (your batch)
  1.1, 1.2 ──► Wave 2 (L5 campaign → G4) ──► Wave 3 (statutory + retirement) ──► Wave 4 (holistic, on G4)
  1.3 ──► statutory rebuild (any time)                                            │
  1.5, 1.6, 1.7 ── independent                                                    ▼
  1.4 ──► salary step-1 (your track) ──► mw index rebuild                    Wave 5 (version selection)
```

## Effort summary

| Wave | Who | Effort | Dataset effect |
|---|---|---|---|
| 1 | Hanna | ~1–2 h | unblocks everything |
| 2 | Claude | 1 session, ~L4-scale tokens | **L5 → G4** (~2–3k cells) |
| 3 | Claude | small (code + docs) | ≤172 cells + queue closure |
| 4 | Claude | LARGE (campaign; tiered options) | unknown — measures the last blind spot |
| 5 | Claude | medium (build + integrate) | new columns + deduped indices |
| salary | Hanna → Claude | external | mw index onto QA'd data |

**When everything above is done, the project is fully closed:** every surfaced queue adjudicated or
explicitly retired, every layer applied and documented, indices on deduplicated QA'd data, and no
verification blind spots left unmeasured.
