# CAO QA Pipeline — How It Works (end-to-end summary)

Written 2026-05-27, after completing all 12 non-leave topics. This explains the
methodology, every stage, and each guard in the aggregate chain
(`csv_recovery → aggregate → suppress-noop → suppress-boolean-on-numeric → audit →
NHR → suppress-unit-without-value`), plus the cross-cutting verification layers.

Companion docs: `PLAN.md` (architecture), `README.md` (status + counts),
`CORRECTIONS_SCHEMA.md` (output schema), `MASTER_REVIEW_FOR_HANNA.md` (the to-do list).

---

## 1. Goal & guiding principles

We QA an upstream extractor's structured CAO data (1,505 records/topic; 95 in-scope
CAOs) against the **English-translated** source texts in `inputs/by_topic/*_information.md`.

- **Catch false-negatives, not just typos.** The extractor's biggest risk is *missing*
  a provision (leaving a field empty/False when the CAO actually has one). So the core
  trigger (L2) deliberately fires on **empty/False fields whose keywords appear in source**.
- **Deterministic where possible, LLM only where needed.** Schema-internal checks and
  statutory comparisons are code; reading the Dutch-legal prose is the subagent's job.
- **Surface, never auto-fix.** Audit-flagged rows go to a human-review file; corrections
  are never hand-edited — only the aggregator writes `corrections.csv`.
- **Never invent values; never feed the model statutory numbers** (it would anchor and
  fill them in). Statutory checks are post-hoc, in code.

---

## 2. Per-topic pipeline (Stages 0–5) — in detail

Each topic has 5 standard scripts in `qa/qa_<topic>/scripts/`. Quick map, then a
full description of what actually happens in each stage.

| Stage | Script | One-liner |
|---|---|---|
| 0 | (hazard memo) | Recon the source + schema; list extraction traps; PROCEED / HARD-STOP. |
| 1 | `qa_<topic>_scope.py` | Pick the latest doc per CAO → 95 records. |
| 2 | `qa_<topic>_rules.py` + `_corrections_det.py` | Generate worksheet items: L1 rules + L2 presence + ENUM. |
| 3 | `qa_<topic>_build_chunks.py` | Slice source per item → chunks → subagents extract/verify → 12-col CSVs. |
| 4 | `qa_<topic>_aggregate.py` | Reconcile, run guard chain (§3), audit, route NHR, (era flag). |
| 5 | (memo + FM) | Summary memo, failure-mode entries, board, run.log. |

### Stage 0 — Hazard check (human + memo; no code)
Before touching a topic we *characterise* it so the run is calibrated:
1. **Language profile** — read ~5–30 random source passages from
   `inputs/by_topic/<topic>_information.md` and confirm they're English-translated with
   only retained Dutch acronyms/proper-nouns (AOW, BW, bpfBOUW, trede…). If a file is
   substantially more Dutch than the others, flag it (upstream issue). This dictates that
   keyword sets must be **English-primary** (`field_keywords.py`).
2. **Schema shape** — enumerate the topic's fields and classify each: master boolean,
   presence boolean, enum (+ its canonical values), numeric value, range (min/max/unit),
   free-text note/rule. Note paired relationships (a `*_present` flag ↔ its `*_rule` note).
3. **Hazard list** — the topic-specific extraction traps, each with a status
   (CONFIRMED / RISK / NOTED). Examples that shaped real runs: "automatic periodieken read
   as performance-based" (wage), "'excedentregeling' appears only in the prompt label"
   (pension), "'100% accrual maintained' ≠ a DB accrual rate" (pension),
   "inloopperiode is a *lower* start, not a higher entry step" (wage).
4. **Pipeline sanity** — caught real bugs here (e.g. the ENUM scan mis-reading the free-text
   `ai_policy_note` as an enum → fixed `schema_lookup` to exclude `_note`/`_text` suffixes).
5. **Decision** — PROCEED, or **HARD-STOP** and pause for Hanna if there's a schema gap
   (missing field, ambiguous enum). Output: the Stage-0 section of the topic memo. If the
   topic is new, also seed its keyword set in `field_keywords.py`.

### Stage 1 — Scope filter (`qa_<topic>_scope.py`)
`scope_filter.load_full_csv()` loads the full `inputs/extracted_data_non_salary.csv`
(2,836 records / 338 CAOs). `most_recent_doc_per_cao(full, topic)` keeps, **per in-scope
CAO, only the single most-recent document version** (each of the 95 curated CAOs has ~16
historical versions; we QA the latest). → **95 records → `inputs/scoped_records.csv`**.
Logs `total_csv / scoped_records / unique_caos`.

### Stage 2 — Deterministic layer (`_rules.py` + `_corrections_det.py`)
Produces `corrections_deterministic.csv` — the **worksheet items** the subagents will work,
from three independent generators. Each item carries a `worksheet_mode` that tells the
subagent how to treat it.
- **L1 — schema-internal rules** (`_rules.py`, `RULES` list). Pure consistency, **no
  statutory facts**. Per record, each rule may emit an item. Patterns used: master-boolean
  False but detail fields populated → propose `set_boolean=True` (mode **blind** — the
  subagent re-derives the answer without seeing our proposal); a present-flag False while
  its paired rule-note is filled; a numeric range with `min > max` (likely swapped).
- **L2 — field-presence scan** (`presence_scan.scan_topic_presence`, mode **extract**).
  The false-negative net. For every **empty / False** field, it checks whether the topic's
  field keywords (`field_keywords.py`) occur in that record's source text; if so it emits an
  "the extractor may have missed something here — go read it" item. This is the bulk of the
  worksheet (e.g. 1,040 for pension) and **most resolve to "confirm, correctly empty"** —
  the keyword was a passing mention, not a real provision.
- **ENUM scan** (mode **informed**). For a populated enum field holding a value outside its
  canonical set → emit an item to fix it. Free-text `_note`/`_text`/`_rule_text` fields are
  excluded (they're not enums).
- L2 items overlapping an L1 item (same record+field) are dropped. Combined → the det CSV
  with metadata (cao_number, file_name, ingangsdatum) attached.

### Stage 3 — Worksheets + subagent review (`qa_<topic>_build_chunks.py` + subagents)
`build_chunk_items` (`worksheet_builder`) turns each det row into a self-contained
worksheet item: record metadata, the field + its current CSV value, the flag reason, the
`worksheet_mode`, any proposed correction, and — crucially — the **source section sliced to
a token budget** (`source_text_loader.slice_for_item`: anchor on the field's keywords,
expand to whole paragraphs, rank value-bearing passages, truncate to the per-item cap
[soft 2,000 / hard 4,000 tokens; 3,500/6,000 for the token-heavy wage+bonus source], and
set `topic_section_was_truncated` if anything was dropped). Items are packed into **chunks
of ~15–20** under a 50k-token total-context cap, written as JSONL, plus a
`system_prompt.txt` (conventions + verdict vocabulary + the exact output format).

Each chunk → **one subagent** (Sonnet default; **Opus** for the era-heavy term & pension).
For every item the subagent reads the (sliced) source and returns one **verdict** —
`confirm` / `clear` / `correct_in_place` / `move` / `set_boolean` / `unable_to_verify` —
with a **verbatim `evidence_quote`**, confidence, any failure-modes referenced, and
pass-through truncation/value-not-in-source flags. Output is a **12-column semicolon CSV**:
`record_id;original_field;verdict;target_field;new_value;new_unit;evidence_quote;confidence;failure_modes_referenced;topic_section_was_truncated;value_not_in_source;notes`.

### Stage 4 — Aggregate (`qa_<topic>_aggregate.py`)
Reconciles each (record, field) across the det proposal and the subagent answer into one of
`det_only` / `sub_only` / `det+sub_agree` / `det_sub_conflict`, picks the **winner's** value,
then runs the full guard chain (§3), the audit (§4), NHR routing, and — for term/pension —
the post-hoc era-outlier flag (§5). Emits `corrections.csv` (+ `corrections_audit.csv`,
`needs_human_review.csv`, `era_outliers.csv`). Buckets: **clean win / NHR / no-op**.

### Stage 5 — Memo + failure modes + board
Write `qa_<topic>/<topic>_summary_memo.md` (Stage 0–4 results, clean wins, NHR, era
outliers, proposed conventions); add/append `conventions/failure_modes/per_topic/<topic>.md`
(the reusable traps future runs of this topic should heed); update the `README.md` status
board + authoritative counts; append `qa_<topic>/run.log`.

---

## 3. The aggregate guard chain (the heart of Stage 4)

Run in this order by every `qa_<topic>_aggregate.py`. Each guard exists because a specific
failure mode was observed and fixed this session.

1. **`csv_recovery.recover_glob`** (`shared/csv_recovery.py`) — subagents sometimes emit a
   12-column row with **unquoted `;`** inside free-text (notes/evidence), splitting it to
   13+ columns. Recovery anchors on the first adjacent (bool,bool) pair (truncated, vnis),
   takes the 5 fixed left fields, rejoins the unit+evidence middle and the notes tail back
   to 12 columns. Idempotent; drops junk lines (non-numeric record_id). Without it, a single
   stray `;` corrupts the whole topic's parse.

2. **`run_aggregation`** (`shared/aggregator_lib.py`) — reconciles the deterministic
   proposal and the subagent answer per (record, field): `det_only` / `sub_only` /
   `det+sub_agree` / `det_sub_conflict`. **The deepest bug fixed this session lived here:**
   `_make_output_row` was pulling the output value from the *det rule's* proposal even on
   `sub_only` rows, silently overwriting the subagent's answer. Fixed to faithfully take the
   reconciliation **winner's** value (subagent on sub_only). This both *deflated* overtime
   (98→198 once subagent extractions stopped being masked) and *inflated* others
   (homeoffice 21→1) — i.e. it had been corrupting every topic.

3. **`suppress_noop_vs_actual_csv`** — L2 worksheets carry an empty `csv_value_old`, so a
   subagent that *confirms an existing CSV value* registered as a phantom "change". This
   reclassifies `is_noop=True` when the proposed value already equals the real scoped-CSV
   value. Huge: 279 bonus, 234 fringe, 84 contract, … phantom changes removed.

4. **`suppress_boolean_on_numeric_field`** — drops `True/False` written into numeric
   `_range/_value` fields (a subagent type error). 41 contract.

5. **`audit_lib.run_audit`** — 18 checks (A1–A18) over `corrections.csv`; see §4.

6. **`flag_outliers_as_needs_human`** — routes audit-flagged rows to
   `needs_human_review.csv`. Guard: a row that's `is_noop=True` is NOT routed (a no-op needs
   no review) **unless** it's a det_sub_conflict. This cut bonus NHR 316→3.

7. **`suppress_unit_without_value`** — drops a `_unit` correction whose paired value field
   is empty/not-a-clean-win (a unit with nothing to attach to). 19 overtime.

**Then** (term/pension only) **`era_baselines.flag_topic_outliers`** — see §5.

Result buckets: **clean win** (real change, not NHR), **NHR** (surfaced for human),
**no-op** (confirmation / suppressed).

---

## 4. The audit (A1–A18) — and the two fixes this session

`audit_lib.py` runs 18 checks (catalogue in `conventions/audit_checks.md`). Examples: A1
missing record_id; A6 non-boolean in a boolean field; A7 pay-rate > 100%; A10 conflicting
duplicate rows; A14/A15 truncated-source flags; A16 value-not-in-source + confirm.

Two were **over-firing** and were refined this session (the diagnosis that 76% of the
original 126 NHR were false positives):

- **A17** ("evidence_quote doesn't literally contain the proposed value") was firing on
  `_unit`/enum fields whose value is a **canonical normalization** — e.g. source "€0.23 per
  km" → unit "EUR per km" is correct but not a verbatim substring. Fix: A17 skips normalized
  (unit/enum) fields **when backed by real evidence** (still fires on placeholder evidence =
  fabrication).
- **A18** ("unable_to_verify but a change was applied") was firing when **no** value was
  applied (empty/UNKNOWN) — contradicting its own definition. Fix: A18 skips empty/UNKNOWN.

Effect across the 10 done topics: **NHR 126 → 58, clean wins 323 → 389.** 6 regression tests.

---

## 5. Cross-cutting verification layers (beyond per-topic)

- **Era baselines (Phase 4.5)** — `shared/era_baselines.py`. Statutory floors/caps
  (transitievergoeding, probation cap, Witteveen accrual cap, franchise floor; AOW table /
  notice schedule are informational). **Post-hoc only — never in the subagent prompt** (so
  the model can't anchor on or fill in a statutory number). After aggregation,
  `flag_topic_outliers` compares each record's FINAL value (correction overlay → else CSV)
  at its `ingangsdatum` and surfaces below-floor / above-cap readings. Caught 2 real
  extraction errors (pension accrual=100%). **Bug fixed:** `_parse_date` didn't read the
  CSV's `DD/MM/YYYY` dates → the era flagging silently no-op'd until fixed (+2 tests).

- **NHR second pass** (`second_pass_nhr/`) — the 58 surviving NHR items re-read with the
  **full** source section, tagged RESOLVED_CONFIRM / RESOLVED_CORRECT / ESCALATE.

- **Full-text re-check / Stage 4.5** (`shared/full_text_recheck.py`) — every accepted
  substantive clean win (non-`_unit`) re-verified against the **complete, uncapped** source
  section (the earlier one-off was capped at 32k chars, which truncated 6 bonus + 6 wage
  sections — now fixed; chunks sized by char-budget so full sections fit). The module covers
  all topics, but per-topic recheck **outputs exist only for bonus, term, wage** (pension's
  `recheck/` has no outputs); the other topics were covered by the earlier one-off
  `verify_changes/` run and, later, superseded by the per-file verification layer.
  Output: per-topic `recheck/outputs/verified_changes_review.csv`.

- **Holistic record verification / Stage 4.6** (`shared/holistic_verify.py` + `HOLISTIC_PROMPT.md`)
  — the one layer that re-reads **already-populated** extractor values (L2 only scans empties;
  the audit/era/4.5 only touch changed cells, so a wrong *populated* value is otherwise never
  re-examined). Per record it sends ALL populated `<topic>_*` fields + the COMPLETE source →
  subagent judges each: CONFIRM / NEEDS_CHANGE / UNSUPPORTED. **Demonstrated on 5 pension
  records (73 fields): reproduced both known `accrual=100%` errors AND found 2 net-new
  populated-value errors no other layer catches (a fabricated early-retirement age; a misread
  selection-rule enum).** Output: `qa_<topic>/holistic/outputs/holistic_review.csv`. Full
  rollout (all populated fields × 95 records × 12 topics ≈ 400+ chunks) is a deliberate
  multi-rate-window project; the module is in place and validated.

All four are surfacing layers — nothing is auto-applied.

---

## 6. What this session changed (chronological)

1. Ran **wage** end-to-end (3 clean wins).
2. **A17/A18 audit refinements** + re-aggregated all 10 done topics (NHR 126→58, clean 323→389).
3. **NHR second pass** (58 items, full text) → 47 resolved-correct suggestions, 6 escalate.
4. **Verification pass** on all 226 accepted real changes (full text) → 13 content errors found.
5. **Era baselines** encoded post-hoc + `flag_topic_outliers` + tests; approved by Hanna (Phase 4.5).
6. Ran **term** (18 clean wins) and **pension** (0 clean wins, 5 era outliers incl. 2 accrual=100% errors) on Opus.
7. Fixed the **era `_parse_date`** bug (DD/MM/YYYY) and the **32k full-text cap**; generalized
   the full-text re-check into a reusable Stage 4.5 and ran it for term/bonus/wage/pension.
8. Added the missing standard scope/build_chunks scripts to overtime & homeoffice.
9. Consolidated everything into `MASTER_REVIEW_FOR_HANNA.csv`.

**Test suite: 92 passing.**

---

## 7. Where everything lives

- Per topic: `qa/qa_<topic>/` — `scripts/` (5 standard), `inputs/scoped_records.csv`,
  `outputs/{corrections.csv, needs_human_review.csv, corrections_audit.csv, era_outliers.csv}`,
  `recheck/outputs/verified_changes_review.csv`, `*_summary_memo.md`, `run.log`.
- Shared code: `qa/shared/` — `aggregator_lib, audit_lib, csv_recovery, era_baselines,
  full_text_recheck, presence_scan, source_text_loader, schema_lookup, field_keywords,
  worksheet_builder` (+ `tests/`).
- Conventions: `qa/conventions/` — `audit_checks.md`, `failure_modes/per_topic/<topic>.md`.
- Cross-cutting runs: `qa/second_pass_nhr/`, `qa/verify_changes/`.
- **Master to-do: `qa/MASTER_REVIEW_FOR_HANNA.{csv,md}`.**
- Era gate: `phase_4_5_era_baseline_review.md` (root).
