# Expert Implementation Plan — Non-Leave CAO QA

Ordered action plan for executing the 12-topic non-leave QA pipeline. Architectural details live in `PLAN.md`. This document is what you actually follow, step by step.

> **Execution note (2026-05-17 session — Phase 0.5 + Phase 1 completed):** Git was not used in this session per Hanna's request. All `git init`, `git mv`, `git commit`, and branch-creation steps below were either skipped or executed as plain `mv` / file-creation operations. Future sessions may re-introduce git if useful. The Phase 1 deliverables are documented in `qa/README.md` and `../phase_0_5_findings.md` instead of commit history. Verification checks 1 (commit log) and 10 (clean git status) from "Verification" section did not apply; all other checks passed.

**Wall-clock estimate:** 2–3 weeks. Most of the elapsed time is review pauses + weekly rate-limit pacing, not Claude Code compute.

**Decision principle:** when this plan and PLAN.md disagree on order or scope, this plan wins. When they disagree on architecture, PLAN.md wins.

---

## Phase 0 — Read and acknowledge

**Step 0.** Read PLAN.md in full + this document + list `qa_leave/` directory. Then STOP. Write a 400-word summary back to Hanna describing:
1. Architecture (layered review, worksheet modes, reconciliation order)
2. Topic order and the rationale for putting pension/term last
3. Where the hard stops are (PLAN.md §0.4)
4. What you will produce in Phase 0.5 and Phase 1
5. The two open prerequisites you need Hanna to answer (do non-leave `by_topic/*.md` files exist? what's the actual Task-tool batch limit today?)

Do not proceed until Hanna acknowledges.

**Acceptance:** Hanna replies with "go ahead" or equivalent.

---

## Phase 0.5 — Prerequisite check (NEW, blocking)

The earlier draft of this plan assumed `inputs/by_topic/<topic>_information.md` exists for all 12 topics. Verify before any code is written. Also verify a few empirical facts the rest of the plan depends on.

**Step 0.5.1 — Confirm `by_topic/` source files exist and verify the topic → filename mapping.** List `qa_leave/inputs/by_topic/` (the current location; Phase 1 Step 0 will move it). For each of the 11 non-leave topics, confirm the expected source file exists per the §0.6a mapping table in PLAN.md.

**Findings to record in `qa/qa_overtime/run.log`:**
- File sizes per topic file (for size-vs-runtime modeling later).
- Confirmation that `wage_information.md` is the shared source for both `bonus` and `wage` (per the schema's "1→1 mapping except wage_information feeds two outputs" note).
- Filename mismatches that the loader must handle: `termination_information.md` (topic=term), `contract_type_information.md` (topic=contract), `fringe_benefits_information.md` (topic=fringe), `AI_information.md` (topic=ai, case difference).
- Presence of `general_information.md` (auxiliary, not a QA topic).

- **If all 12 mapped files exist:** proceed.
- **If any are missing:** STOP and report to Hanna. The pipeline cannot run without them.
- **If the schema's bonus/wage shared-source statement turns out to be wrong (`bonus_information.md` exists separately):** update PLAN.md §0.6a before proceeding.

**Step 0.5.2 — Empirically verify Task-tool batch limit.** Send a single message with N parallel Task calls (trivial subagent: "respond with 'ok'"). Try N=3, N=8, N=15 in three messages.

- Record which N values returned all responses without drops, partial outputs, or 429s.
- The verified ceiling becomes `MAX_PARALLEL_HARD_CAP`. Default `max_parallel = min(12, MAX_PARALLEL_HARD_CAP)`.
- Document in `qa/qa_overtime/run.log` as the first entry.

**Step 0.5.3 — Confirm Python environment.** Run `python3 -m pip --version`. If PEP 668 enforcement is active, add `--break-system-packages` to the allow-list. If not, leave it out.

**Step 0.5.4 — Confirm `inputs/extracted_data_non_salary.csv` matches the 2,739-record + 100-biggest-CAO assumption.** Run a quick `wc -l` + a unique-CAO count. If the numbers diverge meaningfully from what PLAN.md states, STOP and reconcile before Phase 1.

**Step 0.5.5 — Spot-check source-text format across topics.** Open `inputs/by_topic/overtime_information.md`, `contract_type_information.md`, and `pension_information.md`. Confirm all three use the same per-CAO `## header` + topic-grouped JSON-array layout as the leave file. If any differ, document the variants and design the `source_text_loader.py` adapter in Phase 1 accordingly. Also check for cross-topic bleed (does pension content appear in the wage file, etc.) — if it does, document and plan for `presence_scan` to handle it.

**Step 0.5.6 — Topic-keyword coverage and source-length distribution.** Two related checks driving slicer behavior (§4.5):

1. **Keyword review.** Open `qa/shared/topic_keywords.py` (created in Phase 1 Step 8). For each of the 11 non-leave topics, sanity-check the synonym list against 3-5 random excerpts of that topic's source file. If you find topic-relevant content that *doesn't* contain any of the listed keywords, expand the list. Generous-not-narrow: false-positive anchors are cheap, missed topics are expensive.

2. **Length distribution.** For each topic, compute a histogram of slicer output sizes across ~50 random scoped records. Report: median tokens, 90th/95th/99th percentile, count exceeding the 2,000-token soft target, count exceeding the 4,000-token hard cap. Record in `qa/qa_overtime/run.log`.

- If > 20% of records hit the soft target for a topic, the slicer's defaults are too tight for that topic — consider raising the per-item soft target to 3,000 in `qa_<topic>_aggregate.py`.
- If > 5% exceed the hard cap, raise the hard cap for that topic (to 6,000) before running, since dropping that many records to NHR isn't acceptable.
- If pension or term show heavy tails, that's expected — those topics often have lengthy structured sections.

This step *informs* the per-topic overrides but does not change the shared defaults.

**Acceptance:** all five sub-steps done; results committed as one `qa/setup: phase 0.5 prerequisites verified` commit. Hanna sees the empirical numbers (batch limit, file sizes, format variants) before Phase 1 starts.

---

## Phase 1 — Repository setup (~3–4 hours on branch `qa/setup`)

Commit after each step that produces a file. Branch: `qa/setup`.

**Step 0.** Move `qa_leave/inputs/` to project-root `inputs/`. Leave a symlink at the old path (`ln -s ../inputs qa_leave/inputs`) so any leftover qa_leave/ scripts still resolve. Verify by reading one source file via the new path. Commit as `qa/setup: move inputs/ to project root with symlink`.

Rationale: the plan refers to `inputs/by_topic/<topic>_information.md` throughout; with inputs/ inside qa_leave/, the new pipeline would have to either rewrite every reference (ugly) or read across the qa_leave/ boundary (couples frozen reference to active work). Moving is cleanest.

**Step 1.** Create `CLAUDE.md` at repo root using the template in PLAN.md §0.1. Commit.

**Step 2.** Create `.claude/settings.json` using PLAN.md §0.3, with the pip flag tuned per Step 0.5.3. Commit.

**Step 3.** Create `qa/.gitignore` with patterns from PLAN.md §0.2. Commit.

**Step 4.** Create seed conventions files, folder structure per PLAN.md §2:
- `qa/conventions/general_conventions.md` (verbatim PLAN.md §3.1)
- `qa/conventions/audit_checks.md` (verbatim PLAN.md §3.3)
- `qa/conventions/failure_modes/general.md` (verbatim §3.2.2 — 6 GENERAL_FM)
- `qa/conventions/failure_modes/per_topic/leave.md` (verbatim §3.2.3 — 6 LEAVE_FM)
- `qa/conventions/failure_modes/archive/.gitkeep`

Do NOT create empty stub files for the 11 non-leave topics — `worksheet_builder.select_relevant_failure_modes` should handle missing files gracefully (returns empty list). Less noise in git, less false-confidence about "all topics ready."

Commit.

**Step 5.** Copy this plan into `qa/EXPERT_IMPLEMENTATION_PLAN.md`. Copy `non_leave_qa_pipeline_plan.md` into `qa/PLAN.md`. Originals stay at repo root as historical drafts. Commit.

**Step 6.** Create `qa/CORRECTIONS_SCHEMA.md` from PLAN.md Appendix A. Commit.

**Step 7.** Stub out `qa/shared/*.py` with signatures from PLAN.md §4.1 — but **do not implement `conventions_updater.py`**. Stage 5a is manual until topics 1–3 prove the need.

Specifically include the slicer infrastructure (per §4.5):

- `qa/shared/source_text_loader.py` — `load_source_text`, `slice_for_item`, `TOPIC_TO_FILENAME` (§0.6a). The slicer is the heart of §4.5; implement carefully.
- `qa/shared/topic_keywords.py` — `TOPIC_KEYWORDS` dict (§4.1, §3.4). For each topic, read its schema entry in `NON_SALARY_PROMPTS_AND_SCHEMA.md` and extract terms; add Dutch/English synonyms, abbreviations, statutory acronyms. Generous defaults — false-positive anchor = small cost, missed-topic = big cost. Phase 0.5 Step 0.5.6 reviews coverage.
- `qa/shared/value_variants.py` — `generate(value, unit)` returning ~20 plausible source-text representations of the CSV value (decimal-comma swaps, unit synonyms, spelled-out low integers, currency symbols). Used by `slice_for_item` for the value-anchor pass.

Write `qa/shared/__init__.py`. Commit.

**Step 8.** Create initial `qa/README.md` with the §11 status board, all non-leave topics in `-` state, leave in `done`. Commit.

**Step 9.** Post a Phase 1 summary to Hanna: list of files created, link to status board, confirmation that hard rules + stop conditions live in PLAN.md §0.4 (with `CLAUDE.md` pointing there). Wait for sign-off.

**Acceptance:** Hanna replies "go to Phase 2."

---

## Phase 2 — Reference port + smoke test (~3–4 hours on branch `qa/setup`)

Still on `qa/setup`. Phase 2 must pass before any topic work begins.

**Step 10.** Pre-flight cleanup of `qa_leave/` per PLAN.md §0.6. Inspect actual folder, build candidate delete list (with sizes + mtimes), present to Hanna for per-file approval, then delete and commit as `qa_leave: pre-handoff cleanup of interim files`. Write `qa_leave/README.md` (cleanup date, canonical entrypoints, pointer to `corrections.csv`).

**Step 11.** Port logic from `qa_leave/` into `qa/shared/`. Port these modules:
- `aggregator_lib.py`
- `audit_lib.py`
- `resilient_csv.py`
- `manual_review_lib.py`
- `presence_scan.py`

Implement from scratch following PLAN.md §4:
- `era_baselines.py` (start with leave-era data; pension/term content added before Phase 3.5)
- `scope_filter.py`
- `source_text_loader.py` (handles format variants discovered in Step 0.5.5)
- `subagent_runner.py`
- `worksheet_builder.py` (with `BudgetExceeded` enforcement)
- `logging_util.py`

Commit each module separately.

If porting reveals a pre-existing bug in the leave reference, **surface to Hanna** before fixing (PLAN.md §0.2 bug protocol).

**Step 12.** Minimal unit tests in `qa/shared/tests/`:
- `test_aggregator.py` — one test per reconciliation branch in PLAN.md §1.4 (det+sub_agree, det_only, sub_only, conflict-unable_to_verify-trusted-rule, conflict-unable_to_verify-risky-rule, conflict-confidence-wins, clear-vs-confirm edge case). **7 tests.**
- `test_audit.py` — one test per check A1–A16, plus one for the A13 sample-size floor (rule with 5 firings at 40% disagreement should NOT trip). **18 tests.**
- `test_worksheet_builder.py` — `BudgetExceeded` raises; per-item trimming behavior; missing topic FM file is graceful. **3 tests.**
- `test_source_text_loader.py` — slicer atomicity and the two-pass anchoring (§4.5): keyword anchor on a list item captures list header + preceding siblings; conditional-clause anchor captures full conditional + all consequents (Dutch and English openers); section header included; truncation flag set when low-relevance passages dropped; single oversized passage routes to NHR; ±500 char default window; value-anchor pass finds the CSV value when present; value-anchor multi-match ranked by topic-keyword proximity; `value_not_in_source=true` set when CSV has value but no source variant; list detection covers `a)`, `1.`, `i)`, `-`, `•`, Dutch ordinals; `dropped_passages` descriptors recorded correctly. **12 tests.**
- `test_value_variants.py` — variant generation: decimal-comma swap, unit synonyms, spelled-out low integers, currency symbols. **4 tests.**

Skip `test_era_baselines.py` for now — era data isn't comprehensive until before Phase 4.5; add it then with pension/term boundary cases.

Run `python3 -m pytest qa/shared/tests/`. Commit.

**Step 13.** Reproducibility smoke test. Re-run the leave aggregator using the ported `qa/shared/aggregator_lib.py` against existing leave inputs.

**Prerequisite:** `manual_review_lib.py` must load `qa_leave/outputs/manual_review_followup/fixes_v1..v5.csv` in the same priority order as the original aggregator. Verify before running.

**Comparison criterion (set equality on key tuple, with explicit tolerance):**

Compute `(record_id, field, csv_value_new, unit_new)` sets after filtering `is_noop=False` on both old and new `corrections.csv`. Compare:

- **0 differences:** smoke test passes. Commit `qa/setup: smoke test passes, sets identical`.
- **1–5 differences AND all are float-formatting (e.g. `100` vs `100.0`):** acceptable. Document each in the commit body. Commit.
- **6+ differences OR any non-formatting differences:** STOP. The port has a regression. Report to Hanna with sample rows.

**What the smoke test catches:** new regressions introduced during the port.
**What it does NOT catch:** pre-existing bugs in the leave reference (port faithfully preserves them) AND Opus-specific behavior (leave was Sonnet). The first 2–3 Opus chunks in pension/term get extra Hanna review to compensate.

**Acceptance:** smoke test passes or differences are within tolerance and documented. Merge `qa/setup` to `main` with Hanna's approval. Phase 2 done.

---

## Phase 3 — Overtime end-to-end (~3–5 hours on branch `qa/overtime`)

Switch to fresh branch `qa/overtime`. This is the validation run for the whole pipeline.

**Step 14.** Stage 0 — Hazard check for overtime (PLAN.md §5 Stage 0).

1. Write the hazard list for overtime: tier schedules (`X% beyond N hours`), comp-time-vs-pay choice, overtime trigger threshold by FTE/sector, weekend/holiday multipliers, on-call vs overtime distinction.
2. Stratified-sample 30+ excerpts from `inputs/by_topic/overtime_information.md`:
   - 2+ from each of: health, education, public, manufacturing, retail, services
   - 5+ random from remainder
3. For each, check the three Stage 0 questions (PLAN.md §5).
4. Write findings into `qa/qa_overtime/overtime_summary_memo.md` under a "Stage 0" section.

**Hard stop:** if any gap found → write the candidate gap with excerpt + recommendation, pause, wait for Hanna. Otherwise commit `qa/overtime: stage 0 — no gaps found, 30 excerpts sampled`.

**Step 15.** Stage 1 — Scope filter. Run `scope_filter.most_recent_doc_per_cao("overtime")`. Confirm row count is plausible (≤ 1,505, expected ~1,400–1,500). Append `run.log` line. Commit.

**Step 16.** Stage 2 — Deterministic layer. Write `qa_overtime_rules.py` and `qa_overtime_patterns.py` adapting the L1 template. Each rule declares `worksheet_mode` and `severity` per PLAN.md §5 Stage 2. Run `qa_overtime_corrections_det.py`. Emit `corrections_deterministic.csv`.

**Hard stops:**
- Any rule with `severity ∈ {medium, high}` and `worksheet_mode="none"` auto-corrects > 25% of in-scope records.
- Any rule looks suspicious in spot-check.

Append `run.log`. Commit.

**Step 17.** Stage 3 — Subagent review. Build chunk JSONLs with `worksheet_builder.build_chunk_items`. Confirm `BudgetExceeded` does not raise. Run `subagent_runner.run_chunks` with verified parallelism from Step 0.5.2, Sonnet model.

**Hard stops:**
- Any chunk returns no output or only template/placeholder rows.
- Subagent run reports > 30% `unable_to_verify` (signals prompt or source-loader bug).

After completion: Stage 3.5 re-check. Scan output for `unable_to_verify` with high-quality evidence-quote — > 5 such cases sharing a structural pattern means a late-stage schema gap. If found, halt before Stage 4.

Append `run.log`. Commit.

**Step 18.** Stage 4 — Aggregate + audit + flag. Run `aggregator_lib.run_aggregation`, then `audit_lib.run_audit`, then `flag_outliers_as_needs_human`. Compute A13 (with sample-size floor of 10).

Write `overtime_summary_memo.md` body: total real corrections, breakdown by `changed` / `fix_method` / audit flag, NHR count + categories, A13 rates per rule with firing counts.

**Hard stops:**
- NHR count > `max(20, 2.0 × leave_NHR_rate × scoped_count)`.
- Non-statutory-clear corrections change > 10% of fields.
- Any A13 rule (≥ 10 firings, > 20% disagreement).

Commit `qa/overtime: stage 4 — <summary>` with memo body in commit message.

**Step 19.** Stage 5a — Conventions update (manual). Read audit + NHR. Propose new failure-mode entries for `failure_modes/per_topic/overtime.md` (or `general.md` if cross-topic) **in chat**. Each proposal includes all four template slots; flag "low confidence" if any slot can't be filled.

**Step 20.** Stage 5b — Hanna merges. Hanna edits and pastes accepted entries into the canonical file. Commit `qa/overtime: stage 5 — conventions merged`. Update status board.

**Step 21.** Phase 3 wrap. Merge `qa/overtime` to `main`. Post a summary to Hanna:
- `corrections.csv` stats (total rows, real rows, by `fix_method`, by `changed`)
- A13 hits and rule miscalibrations to flag
- New conventions added
- Empirical validation results: what surprised you about the pipeline, what's brittle, what should change before homeoffice

**Wait for sign-off before Phase 4.**

---

## Phase 4 — Iterate on topics 2–9 (~1 week wall-clock)

Topics: homeoffice → training → contract → bonus → fringe → safety → childcare → ai.

**Per-topic loop (~3–4 hours Claude time + ~30 min Hanna time, spread over a day or two each):**

1. Branch `qa/<topic>` from `main`.
2. Run Stages 0 → 1 → 2 → 3 → 3.5 → 4 → 5a → 5b as in Phase 3.
3. Update status board after each stage.
4. Commit per stage.
5. Phase summary to Hanna, wait for sign-off, merge to `main`.

**Pacing rules:**
- One topic per day max, to spread Task-tool load.
- After topic 3 (training), evaluate whether Stage 5a manual process is working or whether it's worth building `conventions_updater.py`. If Claude reliably produces good candidates with consistent template fill, keep manual. If candidates are inconsistent or Hanna is editing heavily, consider automation.
- Watch for cumulative prompt bloat. After topic 5, sample-check the prompt size in `worksheet_builder.build_subagent_prompt` for current topic. If approaching the 3K token cap, trigger pruning (PLAN.md §3.2.5) early.

**Per-topic notes (Stage 0 hazard hints):**

| Topic | Stage 0 hazards to specifically check |
|---|---|
| homeoffice | Stipend amounts vs equipment-only; mandatory in-office days; sector variation |
| training | WAB 2020 study-cost recovery; individual vs collective budget split |
| contract | WAB 2020 chain-rule (ketenregeling); fixed-term cap evolution; zero-hours |
| bonus | 13th month vs end-of-year vs profit-share splits; sector minima |
| fringe | Commuting (€/km vs €/month vs OV); meal; relocation; gifts |
| safety | PPE clauses; safety committees; sector-specific (chemical, healthcare) |
| childcare | Tax-credit interactions; sector subsidies; age caps |
| ai | Notification clauses; training; monitoring opt-outs; very new — small N |

If any of these reveals a schema gap → Stage 0 hard stop, normal protocol.

---

## Phase 4.5 — Era-baseline checkpoint (before topic 10)

**Step.** Before starting term (topic 10), present `qa/shared/era_baselines.py` to Hanna for verification. Specifically:
- **Term:** WWZ effective dates (1 Jan 2015, 1 July 2015), WAB transition (1 Jan 2020), transitievergoeding formula changes (pre-2015, 2015–2019, 2020+), chain-rule (ketenregeling) era boundaries.
- **Pension:** AOW age progression by birth year (table, not formula), accrual cap evolution (Witteveenkader changes), premium-split conventions, ABP/sector-pension fund exceptions.

Write `qa/shared/tests/test_era_baselines.py` covering each boundary date.

**Hard stop:** do NOT run topic 10 until Hanna acknowledges the baselines are correct. A wrong baseline produces systematic false "below-statutory" clears (PLAN.md §12).

---

## Phase 5 — Hard topics (term, pension) + wrap-up (~1 week wall-clock)

**Step.** Run topic 10 (term) per the Phase 4 loop, but with these adjustments:
- **Opus for Stage 3** subagent runs on records where `era_baselines.is_below_statutory` flagged or where the WAB transition is in play.
- **Smaller chunks** (15 items instead of 20–25) to keep Opus reasoning quality high.
- **Flag the first 2–3 Opus chunk outputs** for extra Hanna review — these are the first end-to-end test of Opus paths (smoke test only covered Sonnet).

After term: phase summary, sign-off, merge.

**Step.** Run topic 11 (pension) per same Opus protocol. Pension is the highest-complexity topic; expect more A13 hits and more Stage 5a proposals. Don't be surprised if conventions grow by 4–6 new entries.

**Step.** Run topic 12 (wage) per Phase 4 loop. Should be quick.

**Step.** Final wrap-up:
- Status board all-`done`.
- `qa/README.md` gets a "Project complete" section: aggregate stats (total records, total corrections, share by topic), pointer to per-topic memos.
- Final summary to Hanna: what worked, what didn't, what should be different for the next extraction-QA cycle.

---

## Decision points (where Claude pauses for Hanna explicitly)

| Where | What Hanna decides |
|---|---|
| Phase 0 step 0 | Go/no-go on overall plan |
| Phase 0.5 step 1 | What to do if `by_topic/*.md` files are missing |
| Phase 1 step 9 | Approve Phase 1 outputs before Phase 2 |
| Phase 2 step 10 | Per-file approval for `qa_leave/` cleanup |
| Phase 2 step 13 | Approve smoke test result (or investigate regression) |
| Each topic Stage 0 | Schema gap → extend / document / defer |
| Each topic Stage 4 (if stop triggered) | Ship / hold / re-run |
| Each topic Stage 5b | Accept / edit / reject convention proposals |
| Phase 4 after topic 3 | Build `conventions_updater.py` or keep manual |
| Phase 4.5 | Approve `era_baselines.py` for term + pension |
| Phase 5 first Opus chunks | Approve Opus output quality |
| Each topic Phase wrap | Approve merge to `main` |

That's ~30 explicit pause points across the project. Most are quick (< 10 min for Hanna). The two heavyweight ones are Phase 0.5 prerequisite results and Phase 4.5 era-baseline review.

---

## What is deferred (not built unless need emerges)

These were in earlier drafts; cut for now. Add only when a concrete pain point demonstrates the need:

- **`conventions_updater.py`** — Stage 5a is manual until topics 1–3 prove the volume justifies automation.
- **`.candidate` file workflow** — manual paste replaces it.
- **Slash commands** (`/qa-status`, `/qa-start-topic`, `/qa-run-stage`) — script repetition over 12 topics may justify these by topic 6; revisit then.
- **Pre-commit hooks** for conventions — skipped; Hanna's manual review is the gate.
- **`schema_gap_audit/` folder** — gap notes live in each topic's `<topic>_summary_memo.md`.
- **`CHANGELOG.md`** — use `git log` with the standardized commit-message format.
- **Separate `HANDOFF.md`** — this file replaces it.
- **`leave_qa_postmortem.md`** — narrative onboarding doc dropped. The architectural lessons it captured are encoded directly in PLAN.md (Context section: baseline numbers + P1 false-positive lesson; §0.4: A13 rule-quality audit; §1.4: separate det/sub fix_methods; §5 Stage 0: schema-gap audit before QA). If a future onboarding pass wants a narrative, regenerate from those sources.
- **Comprehensive `era_baselines.py` test suite** before Phase 4.5 — added with the term + pension review.
- **`test_era_baselines.py`** in Phase 2 — added in Phase 4.5 with the pension/term cases.

If any of these become needed during execution, surface the pain point to Hanna with a concrete proposal before building.

---

## What to bring back from each topic run

Per PLAN.md Appendix B:
- Final `corrections.csv`.
- Summary memo (including Stage 0 hazard-check result).
- Stage 5a proposed entries (in chat, ready for Hanna paste).
- Note on A13 hits.
- Updated status board.
- `run.log`.

End of expert plan.
