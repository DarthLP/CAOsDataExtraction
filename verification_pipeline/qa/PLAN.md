# Non-Leave CAO Variables — QA Pipeline Plan

> **Layout note (2026-07-01, post-reorg):** this plan predates the repo reorganization and
> describes the original layout. Current layout is in the root `README.md`. Mapping:
> top-level `qa_leave/` → `reference/qa_leave/` · `qa/indices/` → `indices/` ·
> `qa/qa_salary/` → `salary/` · root phase/plan docs → `docs/` + `docs/archive/`.
> The architecture, conventions, and stop conditions (§0.4) described here remain authoritative.

Hand-off document for Claude Code. Self-contained: implements the QA pipeline for the 12 non-leave topics (bonus, wage, pension, term, overtime, training, homeoffice, contract, safety, childcare, ai, fringe), using the existing `qa_leave/` work as the reference implementation.

**Reading order on session start:**
1. This document, in full.
2. `qa/conventions/general_conventions.md` (created in Phase 1).
3. `qa_leave/` reference implementation (after Phase 2 cleanup).

The `EXPERT_IMPLEMENTATION_PLAN.md` is the ordered action plan; this document is the architectural reference it points back to.

---

## Context

The leave QA established a working pattern: deterministic rules + LLM subagent review + a final audit, with `is_noop` flagging and statutory-baseline awareness. That run produced ~2,157 real corrections out of ~87,290 leave fields (~2.5%). Most corrections were unit additions or value/unit fixes from subagent re-extraction; a smaller chunk were boolean flips from rules and statutory restatements that got cleared.

**Leave-run baseline numbers (calibration anchors for non-leave runs):**
- Subagent reach: ~3.5% of cells (the rest passed through silently — mostly truly-unchanged, with a sliver of deterministic-only fixes).
- Correction yield among subagent-reached cells: ~78% (i.e. the targeting was efficient — most cells the subagent saw were ones that needed work).
- Real correction rate: ~2.5% of all cells.

A non-leave topic that diverges sharply from these (e.g. 20% subagent reach, or 10% correction rate) is signalling either a topic that's genuinely messier or a rule miscalibration. Worth pausing to investigate.

**One specific lesson encoded in the architecture.** In the leave run, Pattern-detector P1 (duplicate paternity values) had a 100% false-positive rate. The lesson: heuristic detectors must default to `worksheet_mode ∈ {"informed", "blind"}` so the subagent verifies them against source. Reserve `worksheet_mode="none"` for rules with schema-internal logic that can't be wrong (mutual exclusions, required-field-pairs, unambiguous statutory restatements). When in doubt, route through the subagent.

The 12 non-leave topics are larger in aggregate than leave. Each has its own quirks: pension has the most statutory churn (AOW age, accrual caps), term has the WWZ/WAB transitions, overtime is well-defined, ai is small and recent. This plan applies the leave template to each topic with four adjustments:

1. **Single subagent pass per topic, no iteration loop.** Audit findings tag rows as `needs_human_review` rather than spawning a second subagent.
2. **Shared library + conventions folder.** Reusable code lives in `qa/shared/`. Discoveries live in `qa/conventions/`. Each topic's subagents read the conventions on every run, so later topics inherit earlier lessons.
3. **Stage 0 hazard check before each topic** (replaces open-ended schema sampling — see §5 Stage 0).
4. **Layered review by default.** Deterministic rules and subagent review can both fire on the same `(record_id, field)`. Aggregator reconciles. (See §1.)

**Stage 5a is manual.** Claude reads the audit + `needs_human_review.csv` and proposes failure-mode entries in chat; Hanna edits and pastes into the canonical conventions file directly. No `.candidate` files, no automated `conventions_updater.py`. If patterns emerge that justify automation after topics 1–3, add it then.

Scope across all topics: the 100 biggest CAOs (the set already covered by `leave_information.md` — 1,505 records), with one document per CAO (the most recent version that has source text in `inputs/by_topic/<topic>_information.md`).

---

## 0. One-time repository setup

### 0.1 `CLAUDE.md` at the repository root

Claude Code reads `CLAUDE.md` on every session start. Keep it pointer-heavy, not encyclopedic. Stop conditions live in §0.4 of this plan — `CLAUDE.md` references that section rather than restating it, to avoid drift.

**Required content:**

```
# Dutch Bargaining Agreements — Project Context

Data extraction + QA pipeline for Dutch Collective Bargaining Agreements (CAOs).
Owner: Hanna (hannaw.econ@gmail.com).

## What to read first
1. `qa/PLAN.md`                            — architecture and conventions
2. `qa/EXPERT_IMPLEMENTATION_PLAN.md`      — ordered action plan
3. `qa/conventions/general_conventions.md` — rules every subagent must follow
4. `qa_leave/README.md`                    — reference implementation status

If you have not read these, stop and read them before doing data work.

## Where things live
- `inputs/`            — raw CSVs and per-topic source-text MDs. READ-ONLY.
- `qa_leave/`          — reference implementation, COMPLETE, do not modify.
- `qa/`                — non-leave QA pipeline (active work).
- `qa/shared/`         — topic-agnostic reusable code.
- `qa/conventions/`    — the decisions folder.
- `qa/qa_<topic>/`     — per-topic outputs and scripts.

## Hard rules (do not violate)
- NEVER modify files in `inputs/` or `qa_leave/`.
- NEVER edit `corrections.csv` directly — always through the aggregator.
- NEVER spawn a fix-subagent on audit-flagged rows. Surface, do not auto-fix.
- NEVER invent values not in the supplied source text.
- NEVER modify `qa/conventions/general_conventions.md` without explicit ask.

## Stop conditions
See `qa/PLAN.md` §0.4 for the full list. Single source of truth — do not
duplicate here. When in doubt, stop and ask.

## Environment
- Python: `python3`. Use `python3 -m pip` for installs.
- Tabular: pandas + the resilient_csv shared module.

## Subagent execution
- Task tool, subagent_type: general-purpose. No API key needed.
- Parallelism: empirically verified in Phase 0.5. Default 12, do not exceed
  what the verification establishes.
- Each subagent prompt must be self-contained.
- Subagents must NOT spawn further subagents.
- Pause between topics to manage weekly rate limit.
- Model: Sonnet default. Opus for pension and term. Haiku not used.

## Context budgets (enforced by qa/shared/worksheet_builder.py)
- Static system prompt: ≤ 3,000 tokens.
- Per-item context: ≤ 1,500 tokens.
- Total chunk context: ≤ 50,000 tokens.
- Chunk size: 20-25 items.
- The builder raises `BudgetExceeded` if any cap is exceeded.

## After each completed stage
- Update `qa/README.md` status board.
- Append a log line to `qa/qa_<topic>/run.log`.
- Commit with a meaningful message.
```

### 0.2 Git strategy

QA artifacts are decisions and data — version them.

**Branching:** one branch per topic, named `qa/<topic>`. Merge to `main` after Hanna sign-off. Setup work goes onto `qa/setup` first.

**Commit cadence:** after each completed stage. Message format: `qa/<topic>: stage <N> — <one-line summary>`. The Stage 4 summary memo becomes the Stage 4 commit-message body.

**What is tracked vs. gitignored:**

| Path | Tracked | Reason |
|---|---|---|
| `qa/shared/**/*.py` | yes | Code. |
| `qa/conventions/**/*.md` | yes | Decisions. |
| `qa/qa_<topic>/scripts/**/*.py` | yes | Code. |
| `qa/qa_<topic>/outputs/corrections.csv` | yes | Primary output. |
| `qa/qa_<topic>/outputs/corrections_audit.csv` | yes | Audit trail. |
| `qa/qa_<topic>/outputs/needs_human_review.csv` | yes | Human-review queue. |
| `qa/qa_<topic>/outputs/<topic>_summary_memo.md` | yes | Stage 4 memo. |
| `qa/qa_<topic>/run.log` | yes | Topic execution log. |
| `qa/qa_<topic>/worksheets/chunks/*.jsonl` | gitignored | Large, regenerable. |
| `qa/qa_<topic>/outputs/subagent_worksheets/**/*.csv` | gitignored | Large, regenerable. |
| `qa/qa_<topic>/outputs/corrections_deterministic.csv` | gitignored | Intermediate. |
| `qa/qa_<topic>/inputs/scoped_records.csv` | gitignored | Regenerable from `inputs/`. |

Add a `qa/.gitignore` with these patterns during Phase 1.

**Bug-in-shared/ protocol:** if porting `qa_leave/` to `qa/shared/` reveals a bug, fix in `qa/shared/`, not in `qa_leave/`. Re-run the leave aggregator using the patched library; verify per the Phase 2 smoke test tolerance (see `EXPERT_IMPLEMENTATION_PLAN.md` Phase 2 Step 13). If the bug is pre-existing rather than introduced by the port, surface it to Hanna before fixing — the port is for refactoring, not silent bug fixing. A pre-existing bug fix gets its own commit (separate from the port), needs explicit sign-off, and **the corrected `corrections.csv` replaces the old one as the new regression target** for any future re-runs. Don't leave stale baselines lying around — they'll trip the next smoke test for the wrong reason.

### 0.3 Bash command permissions

Pre-approve common bash commands via `.claude/settings.json`:

```json
{
  "permissions": {
    "allow": [
      "Bash(python3 *)",
      "Bash(python3 -m pip install *)",
      "Bash(cat *)", "Bash(head *)", "Bash(tail *)", "Bash(wc *)",
      "Bash(grep *)", "Bash(rg *)", "Bash(find *)", "Bash(ls *)",
      "Bash(mkdir -p *)", "Bash(cp *)", "Bash(mv *)",
      "Bash(git status*)", "Bash(git diff*)", "Bash(git log*)",
      "Bash(git add *)", "Bash(git commit -m *)",
      "Bash(git checkout *)", "Bash(git branch *)"
    ],
    "deny": [
      "Bash(rm -rf *)", "Bash(git push *)", "Bash(git reset --hard *)"
    ]
  }
}
```

Irreversible commands stay out of the allow-list and require per-invocation approval. Check `python3 -m pip --version` in Phase 0.5: if PEP 668 enforcement is active, add `--break-system-packages` to the pip allow pattern.

### 0.4 Stop conditions

Claude Code MUST stop, summarize, and ask Hanna when any of the following happens:

1. **Schema hazard hit (Stage 0).** Write the gap note. Wait for Hanna's call on extend/document/defer. Do not proceed past Stage 0.
2. **`needs_human_review.csv` exceeds the threshold for a topic.** Threshold formula: `max(20, 2.0 × leave_run_NHR_rate × in_scope_records)` where `leave_run_NHR_rate` is computed during Phase 2 from `qa_leave/outputs/needs_human_review.csv` and stored in `qa/qa_overtime/run.log`. The fixed-50 threshold from earlier drafts was a guess; using the leave run as the baseline grounds it. Summarize NHR categories, ask whether to ship or hold.
3. **A rule's A13 disagreement rate > 20% AND that rule fired on ≥ 10 records.** The sample-size floor avoids tripping on 2-of-5 noise. Below 10 firings, log the rate but do not stop. Disable the rule before the next topic if it does trip.
4. **Source-text format differs from leave.** Write the adapter once, document the variant, ask Hanna to confirm before processing data.
5. **A subagent run returns no output or only template/placeholder rows.** Do NOT proceed to Stage 4. Inspect the chunk JSONL, the subagent prompt, and re-run with diagnostics.
6. **Plan or conventions ambiguous on a real case.** Ask Hanna, propose an addition to `per_topic/<topic>_conventions.md`, do not improvise.
7. **A Stage 2 deterministic rule auto-corrects > 25% of records — scoped.** `L0_STATUTORY_CLEAR` legitimately fires on ~20-30% of records for topics where many CAOs restate statutory baselines. That is correct, not bloat. The stop threshold applies only to rules where BOTH:
   - `worksheet_mode = "none"` (silent auto-correct), AND
   - `severity` is `medium` or `high` (not statutory_clear / cosmetic).
8. **A topic's `corrections.csv` changes > 10% of non-statutory-clear fields.** Statutory-clear corrections set sub-fields to empty; that legitimately changes a lot of fields without indicating a problem. Compute the threshold over non-statutory-clear rows only.

When in doubt, stop and ask. Pause cost is small; running 12 topics with a subtly wrong rule is expensive.

### 0.5 What NOT to do

1. **Never delete files in `inputs/`.** Even if a file appears unused.
2. **Never write to `corrections.csv` directly.** Always through `aggregator_lib.run_aggregation`.
3. **Never spawn a fix-subagent on audit-flagged rows.** Fix-loops compound errors — explicit design lesson from the leave run.
4. **Never invent values not in the supplied source text.** If the source doesn't say it, `verdict=unable_to_verify` and the row routes to human review.
5. **Never modify `qa_leave/`** (frozen reference) **or `general_conventions.md`** (human-maintained).

### 0.6a Inputs location and topic → filename mapping

**Inputs currently live at `qa_leave/inputs/`, not at the project root** as earlier drafts assumed. The plan refers to `inputs/by_topic/<topic>_information.md` throughout; Phase 0.5 / Phase 1 must reconcile this. Recommended: **move `qa_leave/inputs/` to project-root `inputs/`** during Phase 1 Step 0, leaving a symlink at the old path so any leftover qa_leave scripts still resolve. Alternative (less clean): update every plan reference to `qa_leave/inputs/...` and accept the qa_leave/ folder owning the source data forever. Move-and-symlink is preferred.

**Topic → source-file mapping** (verified against `qa_leave/inputs/by_topic/` in Phase 0.5):

| Topic (code/plan name) | Source file | Notes |
|---|---|---|
| leave | `leave_information.md` | reference, complete |
| overtime | `overtime_information.md` | 1:1 |
| homeoffice | `homeoffice_information.md` | 1:1 |
| training | `training_information.md` | 1:1 |
| contract | `contract_type_information.md` | filename ≠ topic; loader maps |
| bonus | `wage_information.md` | **shared source with `wage`** — see schema note |
| fringe | `fringe_benefits_information.md` | filename ≠ topic; loader maps |
| safety | `safety_information.md` | 1:1 |
| childcare | `childcare_information.md` | 1:1 |
| ai | `AI_information.md` | filename casing differs; loader uses case-insensitive lookup |
| term | `termination_information.md` | filename ≠ topic; loader maps |
| pension | `pension_information.md` | 1:1 |
| wage | `wage_information.md` | **shared source with `bonus`** — see schema note |

**Schema note on shared sources.** `NON_SALARY_PROMPTS_AND_SCHEMA.md` explicitly documents: *"wage_information feeds two outputs: bonuses_info and wage_scales_info."* So bonus and wage are distinct **topics** (separate fields in the CSV, separate `qa_bonus/` and `qa_wage/` runs) but share **source text**. When processing bonus, `source_text_loader.load_source_text("bonus", record_id)` should return the same text that `load_source_text("wage", record_id)` returns, and the L2 presence scan + per-item slicing handle the filtering down to bonus-relevant sections.

**Implementation:** `source_text_loader.py` owns the topic → filename map. Keep it as a single constant `TOPIC_TO_FILENAME` dict at the top of the module; case-insensitive filesystem match handled inside the loader. No call site outside the loader should hard-code filenames.

**One extra file: `general_information.md`** (51K lines) exists in `by_topic/` but is not a QA topic. Treat as an auxiliary reference; the source loader may opt to include short snippets from it when topic-specific sections are sparse, but this is not currently planned.

### 0.6 Pre-flight cleanup of `qa_leave/`

Before `qa_leave/` is used as the reference implementation, it needs a cleanup pass. Procedure:

1. Inspect actual folder structure.
2. Compare against the proposed cleanup list below. Drop list items not in folder; flag folder items not in list.
3. Produce a "files to delete" list (with sizes + last-modified dates) and present to Hanna for per-line approval.
4. After confirmation, delete and commit as `qa_leave: pre-handoff cleanup of interim files`.

**Proposed cleanup list (verify against actual folder before acting):**

```
qa_leave/
├── scripts/
│   ├── qa_leave_aggregate_corrections.py     ← KEEP (canonical)
│   ├── qa_leave_aggregate_corrections_v2.py  ← DELETE (interim)
│   ├── qa_leave_l1_followup_worksheets.py    ← KEEP
│   ├── qa_leave_c2_followup_worksheets.py    ← KEEP
│   └── qa_leave_consistency_scan.py          ← KEEP
├── outputs/
│   ├── corrections.csv                       ← KEEP (final)
│   ├── corrections_audit.csv                 ← KEEP
│   ├── corrections_skipped_rows.csv          ← KEEP
│   ├── corrections_manual_review.csv         ← DELETE (interim)
│   ├── corrections_manual_review_v2.csv      ← DELETE
│   ├── leave_consistency_scan_tightened.csv  ← DELETE (superseded)
│   ├── manual_review_followup/
│   │   ├── fixes.csv, fixes_v2..v5.csv       ← KEEP (audit trail)
│   │   └── worksheet*.jsonl                  ← KEEP (audit trail)
│   ├── l1_followup/                          ← KEEP
│   ├── c2_followup/                          ← KEEP
│   └── subagent_worksheets/chunks/
│       ├── chunk_*_corrections.csv           ← KEEP
│       └── chunk_*_corrections.csv.fixed     ← DELETE (scratch)
└── docs/                                     ← KEEP
```

After cleanup, write `qa_leave/README.md`: cleanup date, canonical entrypoints, pointer to `corrections.csv` as final output.

---

## 1. Architecture

### 1.1 Subagent triggers — most fields never reach the subagent

A field is sent to a subagent worksheet only if at least one of these fires:

| Trigger | What it means | Source |
|---|---|---|
| **L1 violation with known false-positive risk** | Deterministic rule flipped a flag, but the rule is known to over-fire (e.g. statutory restatements). | `qa_<topic>_rules.py` |
| **L2 presence mismatch** | Source mentions the topic but the field is empty. | topic-presence scan |
| **Pattern-detector hit** | Known recurring extraction-error shape. | `qa_<topic>_patterns.py` |
| **L1 schema violation needing source** | Internal inconsistency rules can't auto-fix. | `qa_<topic>_rules.py` |

If none fire, the field never reaches the subagent. It still may appear in `corrections.csv` if a Stage 2 rule with `worksheet_mode="none"` fires (silent auto-correct). Cells that hit no rule and no trigger are left alone and don't appear in `corrections.csv` at all. In the leave run, ~3.5% of cells reached the subagent.

### 1.2 Layered review — deterministic and subagent overlap

Stage 2 (deterministic) is NOT a routing gate. A field that hits a Stage 2 rule can ALSO appear in a Stage 3 subagent worksheet if the rule has known false-positive risk or other triggers fire.

Stage 2 auto-corrects schema-internal inconsistencies WITHOUT reading source. Stage 3 reads source for the small subset that needs it. Aggregator merges.

### 1.3 Subagent worksheet item — three modes

Each item has a `worksheet_mode`, chosen per rule when the chunk JSONL is built:

- **`extract`** — no prior verdict (L2 presence mismatch). CSV field is empty; topic discussed in source. Subagent extracts.
- **`blind`** — there IS a deterministic suggestion, but the subagent does not see it. Independent verification. Default for boolean / structural rules.
- **`informed`** — worksheet item includes `proposed_correction` from Stage 2, framed as a hypothesis. For pattern-detector hits where the rule is heuristic.

Per-rule flag in `qa_<topic>_rules.py`: `worksheet_mode = "extract" | "blind" | "informed" | "none"`. `"none"` means silent auto-correct.

The audit separates counts by `worksheet_mode` to track which modes are most error-prone.

### 1.4 Aggregator reconciliation rules

Implemented in `qa/shared/aggregator_lib.run_aggregation`. Applied per `(record_id, field)`:

1. **Both layers fire, agree** on `(verdict, new_value, new_unit)` → one row, `fix_method=det+sub_agree`. Highest confidence.
2. **Only deterministic fires** → `fix_method=det_only`.
3. **Only subagent fires** → `fix_method=sub_only`.
4. **Both fire, disagree** — resolve in strict order:
   - **(a1)** If subagent verdict is `unable_to_verify` AND the deterministic rule has `worksheet_mode="none"` (trusted enough to auto-correct without verification) → deterministic wins.
   - **(a2)** If subagent verdict is `unable_to_verify` AND the deterministic rule has `worksheet_mode ∈ {extract, blind, informed}` (rule already flagged as needing source) → `fix_method=det_sub_conflict`, route to human. Rule was risky, subagent couldn't confirm, don't apply.
   - **(b)** Else if subagent has `confidence ∈ {high, medium}` AND `evidence_quote` is non-placeholder → subagent wins.
   - **(c)** Else → `fix_method=det_sub_conflict`, human review. Both rows preserved in `corrections_audit.csv`.
5. **Edge case — deterministic says `clear`, subagent says `confirm` with high confidence**: route to human (`fix_method=det_sub_conflict`). The rule called the value garbage but the source supports it. Signals rule miscalibration.

### 1.5 No fix subagent, no v2/v3/v4 loop

Audit-flagged rows become `needs_human_review`. Outliers are surfaced, not auto-corrected.

---

## 2. Folder structure

**Target structure (post-Phase-1).** Currently `inputs/` lives at `qa_leave/inputs/`; Phase 1 Step 0 moves it to project root with a symlink left behind (§0.6a). All paths below assume the post-move layout.

```
.
├── CLAUDE.md                               ← §0.1
├── .claude/
│   └── settings.json                       ← §0.3
├── inputs/                                 ← read-only (moved from qa_leave/inputs/)
│   ├── extracted_data_non_salary.csv
│   ├── NON_SALARY_PROMPTS_AND_SCHEMA.md
│   └── by_topic/
│       ├── leave_information.md
│       ├── overtime_information.md
│       ├── termination_information.md      ← maps to topic "term" (§0.6a)
│       ├── contract_type_information.md    ← maps to topic "contract"
│       ├── fringe_benefits_information.md  ← maps to topic "fringe"
│       ├── AI_information.md               ← maps to topic "ai"
│       ├── wage_information.md             ← shared source for "bonus" + "wage"
│       ├── general_information.md          ← auxiliary, not a topic
│       └── …
├── qa_leave/                               ← reference, complete
│   └── README.md
└── qa/
    ├── README.md                          ← top-level status board (§11)
    ├── PLAN.md                            ← this plan
    ├── EXPERT_IMPLEMENTATION_PLAN.md      ← ordered action plan
    ├── CORRECTIONS_SCHEMA.md              ← §0.7
    ├── .gitignore                         ← §0.2
    ├── conventions/
    │   ├── general_conventions.md         ← universal rules
    │   ├── audit_checks.md                ← 13 audit categories
    │   ├── failure_modes/
    │   │   ├── general.md                ← cross-topic (~6 entries)
    │   │   ├── per_topic/
    │   │   │   ├── leave.md             ← topic-specific (~10-12)
    │   │   │   ├── pension.md
    │   │   │   └── …
    │   │   └── archive/                  ← retired entries
    │   └── per_topic/
    │       ├── leave_conventions.md
    │       ├── pension_conventions.md
    │       └── …
    ├── shared/                             ← topic-agnostic code
    │   ├── resilient_csv.py
    │   ├── aggregator_lib.py              ← reconciliation + is_noop/changed
    │   ├── manual_review_lib.py
    │   ├── audit_lib.py
    │   ├── era_baselines.py
    │   ├── scope_filter.py
    │   ├── source_text_loader.py
    │   ├── presence_scan.py
    │   ├── subagent_runner.py
    │   ├── worksheet_builder.py           ← prompt + chunk builder w/ budget
    │   ├── logging_util.py
    │   ├── tests/                         ← minimal suite (see §10 Step 12a)
    │   └── __init__.py
    └── qa_<topic>/
        ├── README.md
        ├── run.log
        ├── inputs/                        ← scoped_records.csv (gitignored)
        ├── scripts/
        │   ├── qa_<topic>_rules.py
        │   ├── qa_<topic>_patterns.py
        │   ├── qa_<topic>_corrections_det.py
        │   └── qa_<topic>_aggregate.py
        ├── worksheets/
        │   └── chunks/                     ← *.jsonl, gitignored
        └── outputs/
            ├── <topic>_rule_violations.csv
            ├── corrections_deterministic.csv  ← gitignored
            ├── subagent_worksheets/           ← *.csv, gitignored
            ├── corrections.csv                ← TRACKED
            ├── corrections_audit.csv          ← TRACKED
            ├── needs_human_review.csv         ← TRACKED
            └── <topic>_summary_memo.md        ← TRACKED
```

Deliberately removed from the earlier draft: `CHANGELOG.md` (use `git log`), separate `HANDOFF.md` (the expert plan replaces it), `schema_gap_audit/` folder (gap notes live in `<topic>_summary_memo.md` or `run.log`), `.candidate` files (Stage 5a is manual). Slash commands and pre-commit hooks deferred indefinitely.

---

## 3. Seed conventions (write these before topic 1)

### 3.1 `qa/conventions/general_conventions.md`

```
1. SOURCE OF TRUTH
   - Verbatim quote from the supplied topic_section is the only valid evidence
     for confidence=high or medium.
   - If a value is not stated in source, csv_value_new=UNKNOWN, confidence=low,
     evidence_quote=(no relevant text in excerpt). No exceptions.

2. STATUTORY MINIMA ARE FLOORS
   - Dutch labour law sets minima. A CAO can grant MORE than statutory, never less.
   - If your reading produces a value BELOW the era-statutory baseline, treat
     as extraction error: verdict=clear or move (depending on context).
   - DO NOT classify "below statutory" as a real CAO deviation.

3. CONVENTION: STATUTORY RESTATEMENT ⇒ EMPTY VALUES
   - When a CAO simply restates the statutory baseline, the schema convention
     is: statutory_ref=True, exceptions=False, all sub-fields empty.
   - Era-baselines live in qa/shared/era_baselines.py.

4. NEVER EXTRAPOLATE ACROSS CAOs
   - If source doesn't cover the specific record, verdict=unable_to_verify.
   - Do NOT infer based on CAO number, sibling records, or "the pattern".

5. ARTICLE NUMBERS / DATES ≠ VALUES
   - "Article 91" → 91 in a leave field = extraction error. Clear.
   - "2014" in a duration field = year reference. Clear.
   - Watch for "<number> found in source" — pure extractor garbage.

6. PAY-RATE FIELDS MUST HAVE % UNITS, DURATION FIELDS MUST HAVE TIME UNITS
   - leave_*_pay_value with unit=weeks → wrong field. Move or clear.
   - leave_*_value (duration) with unit=% → wrong field. Move or clear.

7. DECIMAL POINT STRIP
   - "0.43%" extracted as 43 in a pay-rate field = stripped decimal.
   - If pay-rate value > 100, suspect decimal-strip. Confirm or clear.

8. UNKNOWN ⇒ confidence=low (hard rule)
   - csv_value_new=UNKNOWN MUST be paired with confidence=low.

9. VERDICT VOCABULARY + FIX_METHOD VOCABULARY
   Verdicts — what the subagent emits per item:
     - confirm            : no change needed; CSV value is correct
     - clear              : value is garbage; csv_value_new=""
     - correct_in_place   : same field, fix value/unit
     - move               : value belongs in different field; supply target_field
     - set_boolean        : boolean field; new_value ∈ {True, False}
     - unable_to_verify   : source doesn't allow a verdict
   fix_method — aggregator assigns exactly one per row:
     - det+sub_agree      : both layers fired and produced the same result
     - det_only           : only deterministic produced a correction
     - sub_only           : only subagent produced a correction
     - det_sub_conflict   : both fired and disagreed without resolution
     - needs_human_review : audit-flagged or aggregator-routed for human

10. NO COSMETIC NOISE IN CORRECTIONS
    - 100.0 → 100 with no other change is a noop (filtered automatically).
    - "weeks" → "weeks" with leading space is normalized.

11. LAYERED REVIEW IS THE NORM
    - Deterministic rules and subagent review can both fire on the same
      (record_id, field). Subagent verdicts with verbatim source evidence
      override deterministic rules. Disagreements without source evidence
      route to human review, never silently to one side.

12. WORKSHEET MODES
    Every item has a worksheet_mode:
      - extract  : no prior verdict. CSV field is empty; extract if possible.
      - blind    : a deterministic rule produced a verdict but you do NOT
                   see it. Verify independently against source.
      - informed : a deterministic rule produced a verdict, included as
                   proposed_correction. Treat as a hypothesis to test
                   against source, NOT as a fact to rubber-stamp.
```

### 3.2 `qa/conventions/failure_modes/` — folder, not single file

**Why a folder and not one file.** A tagged single-file approach scales poorly once the catalogue grows past ~15 entries: the prompt-build code has to filter at runtime by topic-tag, the file becomes hard to read, and there's no natural cap on size. The folder split keeps each topic's patterns local, predictable in size, and naturally bounded — each subagent loads only `general.md` + its own topic file, regardless of how many other topics have run.

```
qa/conventions/failure_modes/
├── general.md                  ← cross-topic patterns (~6 entries, ~600 tokens)
├── per_topic/
│   ├── leave.md               ← ~6 entries, ~1.2K tokens
│   ├── pension.md             ← ~6-10 entries
│   └── …
└── archive/
    └── <topic>_<date>.md      ← retired entries (see pruning policy)
```

A subagent for topic X reads, at prompt-build time, **only** `failure_modes/general.md` (always) and `failure_modes/per_topic/<X>.md` (always, may be empty on first run). Nothing else from this folder. Total budget ~1.5–2K tokens regardless of how many topics ran before.

**Cap per file: 10-12 entries.** When a file exceeds the cap, oldest-not-recently-triggered entries get archived. The archive stays in git but is never loaded into a prompt.

#### 3.2.1 Strict entry template

Every entry follows this exact shape. Vague entries don't help subagents.

```markdown
### <TOPIC>_FM_<NN> — <one-line title>

**Trigger** — when this fires:
  - In source: <specific words/phrases to scan for>
  - In CSV state: <specific field/value patterns>
  - In flag_reason: <patterns the L1 or pattern detectors produced>

**Wrong outcome** — what previous runs got wrong:
  - field=<exact_field_name>
  - csv_value_new=<value or pattern>
  - unit_new=<unit or empty>

**Correct outcome**:
  - verdict=<one of: confirm | clear | correct_in_place | move | set_boolean | unable_to_verify>
  - if move: target_field=<exact_field_name>
  - csv_value_new=<value or formula>
  - unit_new=<unit>

**Concrete example**:
  - record_id=<id from the run>
  - Source quote: "<≤200 char verbatim>"
  - Before: <field>=<wrong_value>
  - After: <field>=<right_value>
```

#### 3.2.2 Seed: `qa/conventions/failure_modes/general.md`

Cross-topic patterns that bit us across multiple topics in the leave run.

```markdown
### GENERAL_FM_01 — Article numbers extracted as values

**Trigger**:
  - In source: contains "Article <N>", "## <N>", or "section <N>" near topic
  - In CSV state: any *_value field equals <N> from the article reference
**Wrong outcome**: field=<any *_value>, csv_value_new=<article number>
**Correct outcome**: verdict=clear
**Concrete example**:
  - Source quote: "Artikel 91: Zwangerschapsverlof"
  - Before: leave_paid_maternity_value=91 weeks
  - After: cleared

### GENERAL_FM_02 — Decimal-point strip

**Trigger**:
  - In source: "0.XX%", "0,XX%" or similar
  - In CSV state: pay-rate field with value > 100
**Wrong outcome**: field=<*_pay_value>, csv_value_new=<XX without decimal>
**Correct outcome**: verdict=correct_in_place, csv_value_new=<0.XX>, unit_new=%
**Concrete example**:
  - Source quote: "premie van 0,43% van het pensioengevend salaris"
  - Before: pension_employer_premium_value=43
  - After: pension_employer_premium_value=0.43, unit=%

### GENERAL_FM_03 — Year-like values in duration fields

**Trigger**:
  - In CSV state: any *_value (non-pay) in 1990-2030 range
  - In source: same number as a date or year reference
**Wrong outcome**: field=<any duration *_value>, csv_value_new=<year>
**Correct outcome**: verdict=clear
**Concrete example**:
  - Source quote: "Per 1 januari 2014 is de regeling gewijzigd"
  - Before: leave_partially_paid_paternity_value=2014
  - After: cleared

### GENERAL_FM_04 — "X found in source" extractor garbage

**Trigger**:
  - In evidence_quote (prior run): literal "<number> found in source"
  - In CSV state: any *_value matching <number>
**Wrong outcome**: field=any, csv_value_new=<number>
**Correct outcome**: verdict=clear
**Concrete example**:
  - Before: leave_paid_maternity_value=16, evidence="16 found in source"
  - After: cleared

### GENERAL_FM_05 — Field-type / unit mismatch

**Trigger**:
  - In CSV state: pay-rate field with unit ∈ {weeks, days, hours, months},
    OR duration field with unit ∈ {%, percent}
**Wrong outcome**: value is in wrong field for its unit semantics
**Correct outcome**: verdict=move (target_field matching unit) OR clear if no fit
**Concrete example**:
  - Before: leave_paid_maternity_pay_value=16, unit=weeks
  - After: move → leave_paid_maternity_value=16, unit=weeks

### GENERAL_FM_06 — Source-missing extrapolation

**Trigger**:
  - In evidence_quote: "(section missing)", "(no source)", "consistent with
    CAO X pattern", or any reference to sibling records
**Wrong outcome**: confidence ∈ {high, medium} with no real source evidence
**Correct outcome**: verdict=unable_to_verify, confidence=low
**Concrete example**:
  - Before: verdict=confirm, confidence=high, evidence="(section missing)"
  - After: verdict=unable_to_verify, confidence=low
```

#### 3.2.3 Seed: `qa/conventions/failure_modes/per_topic/leave.md`

```markdown
### LEAVE_FM_01 — Kraamverlof / geboorteverlof confused with maternity

**Trigger**:
  - In source: "kraamverlof", "geboorteverlof", or "birth leave"
  - In CSV state: leave_paid_maternity_value is set; leave_paid_paternity_value empty
**Wrong outcome**: field=leave_paid_maternity_value, csv_value_new=<paternity duration>
**Correct outcome**: verdict=move, target_field=leave_paid_paternity_value
**Concrete example**:
  - Source quote: "Bij geboorte van een kind: 1 week kraamverlof met volledig loon"
  - Before: leave_paid_maternity_value=1 week
  - After: leave_paid_paternity_value=1 week

### LEAVE_FM_02 — Tiered sick-pay schedules collapsed

**Trigger**:
  - In source: "100% / 90% / 85% / 70%" or "Year 1 100%, Year 2 70%"
  - In CSV state: only one tier captured in leave_sick_continuation_value
**Wrong outcome**: field=leave_sick_continuation_value, csv_value_new=<just one tier>
**Correct outcome**:
  - verdict=correct_in_place
  - csv_value_new=<FIRST tier>, unit_new=%
  - notes="tier_schedule=100/90/85/80 across 26+26+26+26 weeks"
**Concrete example**:
  - Source quote: "In year 1: 100%; year 2: 70% of base salary"
  - Before: leave_sick_continuation_value=70
  - After: leave_sick_continuation_value=100, notes="tier_schedule=100/70 across year1/year2"

### LEAVE_FM_03 — WIEG supplementary paternity misfiled

**Trigger**:
  - In source: "aanvullend geboorteverlof", "supplementary birth leave", "WIEG",
    "up to 5 weeks" + "UWV", or "70%"
  - In CSV state: leave_partially_paid_paternity_value empty AND
    leave_paid_paternity_value=5 (or leave_unpaid_paternity_value=5)
  - ingangsdatum: 2020-07-01 or later
**Wrong outcome**: field=leave_paid_paternity_value OR leave_unpaid_paternity_value
**Correct outcome**:
  - verdict=move, target_field=leave_partially_paid_paternity_value
  - csv_value_new=5, unit_new=weeks
  - also set leave_partially_paid_paternity_pay_value=70, unit_new="% of daily wage"
**Concrete example**:
  - record_id=433011, cao_number=433
  - Source quote: "As of 1 July 2020, the employee can take up to 5 weeks
    of additional paternity leave"
  - Before: leave_paid_paternity_value=5 weeks
  - After: leave_partially_paid_paternity_value=5 weeks,
    leave_partially_paid_paternity_pay_value=70 % of daily wage

### LEAVE_FM_04 — Holiday allowance bleeding into maternity

**Trigger**:
  - In source: "vakantiegeld", "holiday allowance", "8%" near holiday discussion
  - In CSV state: any leave_*_maternity_* field equals 8 with unit=%
**Wrong outcome**: field=leave_*_maternity_*, csv_value_new=8, unit_new=%
**Correct outcome**: verdict=clear (8% is holiday allowance, not maternity pay)
**Concrete example**:
  - Source quote: "Vakantietoeslag bedraagt 8% van het brutoloon"
  - Before: leave_paid_maternity_pay_value=8, unit=%
  - After: cleared

### LEAVE_FM_05 — Post-Aug-2022 parental: statutory restatement

Primary enforcement is `L0_STATUTORY_CLEAR` in Stage 2. This entry is defensive
for records that slip past L0 (e.g. when trigger phrases don't match exactly).

**Trigger**:
  - In source: "26 weken" + "ouderschapsverlof" + ("9 weken betaald" OR "UWV")
  - ingangsdatum: 2022-08-02 or later
  - CSV state: parental sub-fields populated but no CAO-specific deviation
**Wrong outcome**: parental sub-fields populated with statutory 26 = 9 + 17 numbers
**Correct outcome**:
  - verdict=clear on sub-fields
  - set leave_parental_statutory_ref=True; leave_parental_exceptions=False
**Concrete example**:
  - Source quote: "Vanaf 2 augustus 2022 heeft elke werknemer recht op 26 weken
    ouderschapsverlof, waarvan 9 weken betaald door UWV"
  - Before: leave_parental_partial_value=9, leave_parental_unpaid_value=17
  - After: both cleared, statutory_ref=True

### LEAVE_FM_06 — Template / placeholder rows leak through

**Trigger**:
  - In subagent output: record_id matches "rec_NNN_NNN"
  - OR topic_group="leave_type", field="field_name", value="example"
**Wrong outcome**: any row with template / placeholder values
**Correct outcome**: reject the row at aggregator-read time
**Concrete example**:
  - record_id="rec_000_000", field="field_name" → drop
```

#### 3.2.4 Stage 5a discipline (manual)

After Stage 4, Claude reads the audit + `needs_human_review.csv` and proposes new failure-mode entries **in chat**. Each proposal must include all four slots of the §3.2.1 template; if a slot can't be filled, the proposal is flagged "low confidence — needs Hanna decision before adding."

Hanna edits and pastes accepted entries directly into the canonical `failure_modes/per_topic/<topic>.md` (or `general.md` if cross-topic). Commit as `qa/<topic>: stage 5 — conventions merged`. `general_conventions.md` is never updated by this process.

Trigger thresholds for proposing an entry:
- A specific extraction-error pattern appears in > 3 records, OR
- A rule disagreed with subagent > 20% of the time on ≥ 10 firings (A13 hit), OR
- The same audit flag appears > 5 times with similar reason.

If after topics 1–3 the manual process feels repetitive, build `conventions_updater.py` then — with grounding in actual patterns observed.

#### 3.2.5 Pruning policy

When a file exceeds 12 entries, move oldest-not-triggered entries to `archive/<topic>_<YYYY-MM-DD>.md`. "Not triggered" means: not referenced in the last three topic runs' audit/needs-human output for that topic family. Archive stays in git but never loads into prompts.

**Tracking which FMs were invoked.** Subagent output CSV has a `failure_modes_referenced` column (list of FM IDs); each subagent reports which patterns matched its current item. Pruning uses this column to compute triggered-vs-not.

This was a choice between two approaches: (A) the explicit subagent-reported column above, which is precise but requires the subagent contract to include the new field; (B) regex-matching each FM's `trigger.source_keywords` against `corrections_audit.csv` and `needs_human_review.csv`, which works without changing the contract but is less precise. Approach A was selected for precision. If the column ever proves unreliable (subagents skip it, format drifts), fall back to Approach B without changing the rest of the pipeline.

### 3.3 `qa/conventions/audit_checks.md`

```
A1   Records missing record_id
A2   csv_value_new with leading/trailing whitespace
A3   high/medium confidence with placeholder evidence (starts with "(" or empty)
A4   csv_value_new=UNKNOWN with confidence != low (HARD RULE violation)
A5   Number value with unit_new=UNKNOWN
A6   Boolean field with non-boolean
A7   Pay-rate field with value > 100 (impossible %)
A8   Year-like values (1990-2030) in non-pay duration fields
A9   Pay-rate fields with duration unit
A10  Conflicting (record_id, field) pairs with different csv_value_new
A11  Topic-group / field-name mismatch
A12  Demoted rows where post-state is NOT (low + UNKNOWN)
A13  Deterministic-subagent disagreement rate per rule
     → if rule fired ≥10 times AND disagrees >20%, surface the rule.
       Indicates miscalibration — review before next topic.
       Below 10 firings: log rate but do not surface.
A14  High-confidence verdicts on truncated sources
     → any row where topic_section_was_truncated=true AND confidence ∈
       {high, medium} AND verdict ∈ {clear, correct_in_place, confirm}.
       Surfaces cases where the subagent was confident on a slice that
       had whole passages dropped — sanity-check before applying.
       Routes to needs_human_review automatically.
A15  Negative verdict on truncated source
     → any row where topic_section_was_truncated=true AND verdict ∈
       {clear, unable_to_verify} OR (verdict=set_boolean AND new_value=False)
       OR (csv_value_new=UNKNOWN with confidence=low).
       The "answer might be in a dropped passage" case. Routes to NHR
       with the dropped_passages_summary attached so the reviewer can
       quickly judge whether the missed content was actually relevant.
A16  Value present in CSV but not found in source (value-anchor miss)
     → any row where the slicer recorded value_not_in_source=true AND the
       subagent emitted verdict=confirm. Signals either a unit mismatch,
       a decimal-strip (FM_02), or that the source uses a paraphrase the
       value-variant generator missed. Routes to NHR.
```

### 3.4 Keyword catalogues (two layers)

The slicer (§4.5) and the L2 presence scan use TWO layers of keyword lists. Both are seed conventions treated with the same care as `general_conventions.md` and `failure_modes/`.

**Layer 1 — Topic keywords** (`qa/shared/topic_keywords.py`): broad, topic-level. Single `TOPIC_KEYWORDS: dict[str, list[str]]` constant. Gates whether a record discusses the topic at all.

**Layer 2 — Field-specific keywords** (`qa/shared/field_keywords.py`): narrow, per-field. `_MANUAL_FIELD_KEYWORDS: dict[tuple[str, str], list[str]]` keyed by `(topic, base_field_name)`. Discriminates which specific fields in that record have evidence in source.

**CRITICAL — source text is ENGLISH-translated.** The per-topic source files (`inputs/by_topic/*_information.md`) contain English-translated content produced by the upstream extractor. Therefore both keyword catalogues must be **ENGLISH-PRIMARY**. Dutch terms are retained ONLY when they survive translation:

  - Statutory acronyms: AOW, WAB, WWZ, BW, UWV, ATW, WAZO, WIEG, BHV, EHBO, PSA, FTE, RI&E, ORT, TOIL, AVG, ZW, WIA, WGA, RVU, VPL, EVC, OR
  - Pension fund names: ABP, BPF, PME, PMT, PFZW, PNO, bpfBOUW, "Stichting Bedrijfstakpensioenfonds"
  - Legal acts/concepts: "ketenregeling", "transitievergoeding", "kantonrechter", "Wet flexibel werken", "Wfa", "Burgerlijk Wetboek", "Artikel X:Y BW"
  - Untranslatable concepts: "mantelzorg", "kraamverlof", "ouderschapsverlof", "zwangerschapsverlof", "eindejaarsuitkering", "13e maand", "dertiende maand", "vakantiegeld", "tijd voor tijd", "TVT", "compensatieuren", "thuiswerkvergoeding", "instaptrede", "trede", "ploegendienst", "ploegentoeslag", "onregelmatigheidstoeslag", "consignatie"
  - Formal scheme names: "O&O-fonds", "scholingsfonds", "studiekostenbeding", "terugbetalingsbeding", "Regeling Vervroegde Uittreding", "Witteveen", "Witteveenkader"

**DO NOT include Dutch multi-word sentence-fragments** like "door werkgever in acht te nemen", "in geld uitbetaald", "naar keuze van de werknemer". These were translated to English by the upstream extractor and will NEVER match in source. They only inflate the keyword set without ever firing.

**Verify language profile when seeding a new topic.** Read 5 random JSON-array passages from `inputs/by_topic/<topic>_information.md`. They should be mostly English sentences with occasional Dutch acronyms/proper nouns. If a topic file looks substantially more Dutch than the others, flag it — could indicate an upstream extractor issue.

**Initial seed (Phase 1 Step 7).** For each topic AND for each base field within that topic:
1. Open the schema entry in `NON_SALARY_PROMPTS_AND_SCHEMA.md` (e.g. `### bonuses_info`).
2. List the natural-English ways CAOs describe each field's concept. Aim for 15-30 phrases per field. Include both short anchors ("notice period", "surcharge") and longer formal phrasings ("the notice period for the employer is", "X% surcharge on the basic hourly wage").
3. Add the retained Dutch terms from the lists above where they apply to that field.
4. Verify each entry actually matches by running `field_keywords.has_field_mention(topic, field, sample_source_text)` on a real source passage from `inputs/by_topic/<topic>_information.md`.

The leave-run vocabulary in `TOPIC_KEYWORDS["leave"]` is a useful starting point for topic-level keywords (leave/holiday/vacation/care leave/maternity/paternity — all preserved in translation).

**Phase 0.5 Step 0.5.6 review.** Before any runs, sample 3-5 random excerpts per topic and check: is there topic-relevant content that doesn't trigger on any listed keyword? If yes, expand the list. This is a *coverage* check, not a tuning exercise.

**Feedback loop signals (mined automatically each Stage 5a).** `conventions_updater` — when it's eventually built; until then, Claude reads these manually each Stage 5a:

1. **Full-source fallback rate.** Records routed to `full_source` mode mean keywords missed something the presence scan caught. If > 5% of a topic's records fall back, the keyword list is too narrow. Action: add the terms the presence scan used as triggers but that keyword scan didn't.

2. **`unable_to_verify` on extract-mode items.** When the L2 presence scan said "topic IS discussed in source" but the subagent extract returns `unable_to_verify`, that's often a slice problem. If > 10% of `extract` items return `unable_to_verify` for a topic, sample 5 and check: did the slicer pull the right passage? If passages look irrelevant, the keyword list is matching wrong content; if no passages came back, the keyword list missed.

3. **A14/A15/A16 audit hits.** High-confidence-on-truncated, negative-on-truncated, and value-anchor-miss are all signals about slicer quality. Track per topic.

4. **Manual NHR findings.** When Hanna reviews NHR rows and finds "the answer was in source but slicer missed it," the missing term goes into the keyword list with the source-quote referenced in the commit message.

**Stage 5a output — keyword expansion candidates** (alongside failure-mode candidates):

```markdown
## Keyword expansion candidates for <topic>

### Added (high confidence — multiple records affected)
- `<term>` — found in source of records [X, Y, Z], not currently anchoring.
  Example: "<source quote showing term>"
  
### Suggested (low confidence — single record, judgment call)
- `<term>` — found in record X only. May be sector-specific.
```

Hanna reviews, edits if needed, pastes accepted terms into `TOPIC_KEYWORDS[topic]`. Commit message: `qa/<topic>: stage 5 — keywords expanded (<terms>)`.

**Pruning.** Keyword lists don't grow forever. If a term has never anchored a useful passage across 3 topic runs, archive it (move to `topic_keywords_archive.py` with a comment showing where it was). Loaded analogous to failure-modes archive — kept for history, not loaded into the slicer. Tracked via the `keyword_hits` column on subagent output: a term that never appears in `keyword_hits` is a candidate for pruning.

**Hard rule.** Never auto-edit `topic_keywords.py`. Like failure-modes, this is a human-merged convention. Auto-application of keyword changes mid-pipeline could change slicer behavior silently between topics.

---

## 4. Shared library specifications

### 4.1 Modules

```python
# qa/shared/source_text_loader.py
TOPIC_TO_FILENAME = {
    "leave":      "leave_information.md",
    "overtime":   "overtime_information.md",
    "homeoffice": "homeoffice_information.md",
    "training":   "training_information.md",
    "contract":   "contract_type_information.md",
    "bonus":      "wage_information.md",          # shared with wage
    "fringe":     "fringe_benefits_information.md",
    "safety":     "safety_information.md",
    "childcare":  "childcare_information.md",
    "ai":         "AI_information.md",            # case differs on case-sensitive FS
    "term":       "termination_information.md",
    "pension":    "pension_information.md",
    "wage":       "wage_information.md",          # shared with bonus
}

def load_source_text(topic: str, record_id: str) -> str:
    """Look up per-record source text for a given topic.
       Maps topic → filename via TOPIC_TO_FILENAME (§0.6a), reads
       inputs/by_topic/<filename> once, caches per-record.
       Filesystem lookup is case-insensitive (handles AI_information.md
       on case-sensitive filesystems).
       Must handle cross-topic mentions: pension premiums sometimes appear
       in wage sections; bonus and wage share a source file by design.
       Format variants documented in Phase 0.5."""

def slice_for_item(topic: str, record_id: str, field: str,
                    csv_value_old=None, csv_unit_old=None) -> SliceResult:
    """Returns the keyword + value-anchored, structurally-expanded source
       slice for one worksheet item. See §4.5 for the algorithm.

       Two anchor passes:
         - Keyword anchors from topic_keywords.TOPIC_KEYWORDS[topic]
         - Value anchors from value_variants.generate(csv_value_old, csv_unit_old)
           when csv_value_old is non-empty (informed/blind modes); skipped for
           pure extract mode.

       SliceResult fields:
         - text: the assembled topic_section string
         - context_type: 'topic_section' | 'topic_section_partial' | 'full_source'
         - was_truncated: bool (whole low-relevance passages dropped to fit)
         - keyword_hits: list of (anchor_term, byte_offset) for audit
         - value_hits: list of (variant_used, byte_offset) for audit
         - value_not_in_source: bool — csv_value provided but no variant found
         - dropped_passages: list[DroppedPassage] — descriptors of what was
           set aside; each carries (section_header, first_100_chars,
           anchors_matched, byte_offset). For visibility in audit + NHR.
       Raises RouteToHumanReview if a single expanded passage exceeds the
       4,000-token hard cap (handled upstream by worksheet_builder)."""

# qa/shared/topic_keywords.py
TOPIC_KEYWORDS = {
    "pension":   ["pensioen", "ABP", "premie", "AOW", "ouderdomspensioen",
                  "partnerpensioen", "nabestaandenpensioen", "Witteveen",
                  "opbouw", "franchise", "dekkingsgraad", "regeling",
                  "pension"],
    "overtime":  ["overwerk", "meeruren", "overuren", "toeslag", "overtime",
                  "extra hours", "TVT", "tijd-voor-tijd", "tijd voor tijd",
                  "compensatieuren"],
    # … one entry per topic. Seed by reading the topic's schema entry in
    # NON_SALARY_PROMPTS_AND_SCHEMA.md plus obvious Dutch/English variants.
    # The §3.2.4 feedback loop expands lists from audit signals.
}
"""Broad synonym lists per topic. Generous, not narrow — false-positive
   anchors are cheap (extra paragraph in slice); missing the only paragraph
   that mentions the topic is expensive. Reviewed in Phase 0.5 Step 0.5.6
   before any runs; refined via Stage 5a feedback loop (§3.2.4)."""

# qa/shared/value_variants.py
def generate(value, unit) -> list[str]:
    """Generate plausible source-text representations of (value, unit).

       Number variants:
         - decimal comma ↔ decimal point (4.5 ↔ 4,5)
         - with/without trailing zero (4 ↔ 4.0 ↔ 4,0)
         - spelled-out low integers (1..12 in Dutch and English)
       Unit variants:
         - percent: '%', ' %', ' percent', ' procent'
         - hours: ' uur', ' uren', ' u', ' hours', ' hour', ' h'
         - weeks: ' weken', ' wk', ' weeks', ' week'
         - days: ' dagen', ' dgn', ' days', ' day'
         - months: ' maanden', ' mnd', ' months', ' month'
         - euros: '€', ' EUR', ' euro', ' euros', ' eur'
         - one-off, monthly, yearly: as full words

       Returns up to ~20 distinct strings. The slicer treats any match as
       an anchor; topic-keyword proximity ranks them."""

# qa/shared/presence_scan.py
def scan_topic_presence(topic: str, scoped_records: pd.DataFrame) -> pd.DataFrame:
    """For each (record_id, field), determine whether the topic is discussed
       in source AND the CSV field is empty (the L2 trigger).
       Returns: record_id, field, presence_flag, evidence_excerpt.
       Generalized from qa_leave_presence.py."""

# qa/shared/scope_filter.py
def get_100_biggest_caos() -> set[str]:
    """Returns the 1,505 (cao_number, file_name) pairs covered by leave_information.md."""

def most_recent_doc_per_cao(records: pd.DataFrame, topic: str) -> pd.DataFrame:
    """Filters records to latest ingangsdatum per cao_number where source
       text exists in inputs/by_topic/<topic>_information.md."""

# qa/shared/era_baselines.py
class EraBaseline:
    def __init__(self, topic: str): ...
    def baseline_for(self, field: str, ingangsdatum: datetime) -> BaselineSpec:
        """Era-statutory baseline for the field at the given date."""
    def is_below_statutory(self, field: str, value, unit, dt) -> tuple[bool, BaselineSpec | None]:
        """Returns (is_below, baseline_spec). Baseline returned so subagent
           can frame evidence with the expected baseline when relevant."""

# qa/shared/aggregator_lib.py
def run_aggregation(topic: str, paths: AggregationPaths) -> AggregationResult:
    """Reads deterministic + subagent corrections.
       Applies §1.4 reconciliation rules.
       Applies overrides from manual_review_lib.
       Runs unit normalization.
       Classifies is_noop / changed.
       Assigns fix_method per row (see convention #9).
       Writes corrections.csv and corrections_audit.csv."""

# qa/shared/audit_lib.py
def run_audit(corrections_csv: Path) -> AuditReport:
    """13 audit checks. Returns counts + per-check sample rows.
       Includes A13: per-rule disagreement rate, with sample-size floor of 10."""

def flag_outliers_as_needs_human(corrections_csv: Path, audit: AuditReport) -> None:
    """For each audit-flagged row, sets fix_method=needs_human_review and writes
       a copy to needs_human_review.csv with the audit category."""

# qa/shared/manual_review_lib.py
def load_manual_overrides(topic: str) -> dict[(str, str), dict]:
    """Optional human-edited override file per topic. Keys: (record_id, field).
       Overrides take precedence over deterministic and subagent results.
       For non-leave topics, defaults to empty unless Hanna creates a fixes.csv."""

# qa/shared/subagent_runner.py
def run_chunks(chunks: list[Path], system_prompt: str,
               max_parallel: int = 12, model: str = "sonnet") -> list[Path]:
    """Spawns parallel subagent runs, one per chunk. Returns paths to
       chunk_NNN_corrections.csv outputs. Default parallelism verified
       empirically in Phase 0.5. Default model Sonnet; pass model='opus'
       only for pension/term on explicit request."""

# qa/shared/worksheet_builder.py
def build_subagent_prompt(topic: str) -> str:
    """Assembles static subagent system prompt by reading:
         - qa/conventions/general_conventions.md
         - qa/conventions/failure_modes/general.md            (cap: 6 entries)
         - qa/conventions/failure_modes/per_topic/<topic>.md  (cap: 12 entries)
         - qa/conventions/per_topic/<topic>_conventions.md
         - era baseline table for <topic>
       Plus verdict vocab + strict rules + task spec.
       Raises BudgetExceeded if total > 3,000 tokens."""

def build_chunk_items(records: pd.DataFrame, topic: str,
                      max_section_tokens: int = 1500) -> list[dict]:
    """Builds chunk-JSONL items. Per item:
         - Slices topic_section to field-relevant chunk (per-item, not per-record)
         - Sets context_type ∈ {topic_section, topic_section_partial, full_source}
         - Trims to max_section_tokens
         - If trim unavoidable, drops item and routes record to needs_human_review
       Per-item total cap: 1,500 tokens (incl. metadata)."""

def select_relevant_failure_modes(topic: str, max_count: int = 12) -> list[dict]:
    """Reads failure_modes/general.md and failure_modes/per_topic/<topic>.md.
       Returns up to max_count entries, formatted per §3.2.1."""

# qa/shared/logging_util.py
def log_event(topic: str, stage: str, message: str, **fields) -> None:
    """Appends one structured line to qa/qa_<topic>/run.log.
       Format: ISO-timestamp | stage | message | k=v k=v …"""
```

**Deliberately not in `qa/shared/`** for the initial setup: `conventions_updater.py`. Stage 5a is manual until patterns observed across topics 1–3 justify automation.

### 4.2 Concurrency and model defaults

Subagents are spawned via Claude Code's **Task tool** (`subagent_type: general-purpose`). No external API key needed — they run under the active Claude Code session. `subagent_runner.run_chunks` is a thin wrapper around batched Task calls.

**Parallelism:**
- Default `max_parallel = 12`.
- **Empirical verification in Phase 0.5:** run 3 trial Task calls in one message, then 8, then 15. Whichever reliably executes without drops becomes the verified ceiling. Document the result in `qa/qa_overtime/run.log` and use as the hard cap.
- One subagent per chunk JSONL. If chunks > max_parallel, batch sequentially across messages.

**Model selection:**
- **Sonnet** is default. Handles structured CSV + verbatim-quote extraction reliably and is cheaper. The leave run was almost entirely Sonnet and produced clean output.
- **Opus** for pension era-baseline interpretation and term WAB transition logic. ~10–20% of total calls, concentrated in topics 10 and 11. **Caveat:** the leave smoke test only validates Sonnet paths. Opus-specific behavior has no port-time regression test; flag the first 2–3 Opus chunks for extra Hanna review.
- **Haiku** not used. System prompts are too long for Haiku accuracy.

**Hard rules for subagents:**
- Each prompt is **self-contained**. The subagent cannot see the parent's context.
- Subagents **must NOT spawn further subagents.** No nested Task calls.

**Rate-limit management:**
- Weekly limits matter for a 12-topic run. **Pause between topics** to spread load. Hanna's Stage 5b review per topic naturally inserts the gap.
- **Honest budget:** 12 topics × 30–90 min subagent runtime per topic, plus reviews, plus rate-limit pauses, is realistically a **2–3 week wall-clock project**, not a sprint. Schedule accordingly.
- If a topic produces > 30 chunks, split Stage 3 across multiple messages with explicit breaks.

**Where per-topic overrides live.** Defaults (Sonnet, `max_parallel=12`, 20–25 items/chunk) belong in `subagent_runner.run_chunks` in the shared library. Per-topic overrides — Opus for pension/term, smaller chunks for Opus calls, lower parallelism for very-large items — live in that topic's `qa_<topic>_aggregate.py` script, passed as explicit kwargs to `run_chunks`. Do not edit the shared defaults to suit one topic; that's how silent drift starts.

### 4.3 Logging conventions

Every topic run appends to `qa/qa_<topic>/run.log` via `logging_util.log_event`. Required events:
- Stage start and end (with timestamps + duration).
- Row counts at each stage boundary (input → output).
- Subagent runs: chunk count, parallelism, model, duration, success/failure counts.
- Audit summary: total flags by category, A13 hits with disagreement rates.
- File-modified list for the run.

### 4.4 Idempotency

- **Deterministic (re-runs identical):** Stage 1 (scope filter), Stage 2 (rules + patterns), Stage 4 (aggregator + audit).
- **Stochastic:** Stage 3 (subagent at temperature > 0).

Re-running Stage 2 on the same input must produce identical `corrections_deterministic.csv` — if not, bug in rules or shared library. Re-running Stage 3 produces slightly different verdicts on edge cases. Note in each topic's summary memo.

**Debugging Stage 3 stochasticity.** When chasing a Stage 3 disagreement that may be noise vs. a real subagent quirk, drop the model temperature to its lowest supported setting and, if the model interface accepts a seed parameter, fix the seed. This won't make Stage 3 byte-identical across runs (Claude isn't fully deterministic even at temp=0), but it dramatically narrows the variance so the underlying signal becomes visible. Reset to default temperature for production runs — too-low temperature has been observed to hurt extraction quality on long structured prompts.

### 4.5 Context budgets (enforced by `worksheet_builder.py`)

| Component | Cap | Notes |
|---|---|---|
| Static system prompt | 3,000 tokens | Builder raises `BudgetExceeded`. |
| └─ general_conventions.md | ~700 tokens | Stable. |
| └─ failure_modes/general.md | ~600 tokens | Cap 6 entries. |
| └─ failure_modes/per_topic/<topic>.md | ~700 tokens | Cap 10-12 entries. |
| └─ per_topic/<topic>_conventions.md | ~400 tokens | Topic-only. |
| └─ era baseline table | ~200 tokens | Current topic only. |
| └─ verdict vocab + strict rules + task spec | ~400 tokens | Stable. |
| Per-item context (`topic_section` + metadata), soft target | 2,000 tokens | Most items fit. |
| Per-item context, hard cap | 4,000 tokens | Beyond this, drop lowest-relevance passages. |
| Total chunk context | 50,000 tokens | System prompt + all items. |
| Chunk size | 15-20 items | Adjusts to per-item size; builder computes dynamically. |

**Source-text slicing — keyword-anchored, atomic passages, generous defaults.**

The slicer's job is to give the subagent enough context to answer correctly without burying the signal. Two failure modes to avoid: (a) cutting a passage mid-content so the subagent sees an incoherent half-sentence, (b) missing the structural context that gives a passage its meaning. Example of (b): a CAO that reads *"If on sick leave: (a) … (b) … (c) the pension is accrued in full amount"* — when processing pension, the keyword "pension" only hits in (c), but to know (c) applies, the subagent needs the "If on sick leave:" preamble and the (a) (b) siblings. Slicing on the keyword alone would misread (c) as an unconditional rule.

**Slicing algorithm (in `source_text_loader.slice_for_item`):** Two anchor passes (keyword + value), structurally expanded, merged, then size-fitted with full visibility into what was dropped.

**Pass 1 — Keyword anchors.** Each topic has a broad synonym list in `qa/shared/topic_keywords.py` (§4.1) — Dutch + English, abbreviations, statutory acronyms, related concepts. Generous, not narrow. Pension example: `["pensioen", "ABP", "premie", "AOW", "ouderdomspensioen", "partnerpensioen", "nabestaandenpensioen", "Witteveen", "opbouw", "franchise", "dekkingsgraad", "regeling", "pension"]`. The §3.2.4 feedback loop expands the lists when audit data shows misses.

**Pass 2 — Value anchors (when applicable).** When the CSV already has a value for the field being reviewed (i.e. `worksheet_mode ∈ {blind, informed}` — not pure `extract`), the slicer ALSO searches the source for the value itself, plus reasonable variants. Examples:
- CSV says `pension_accrual_value=4, unit=%` → search `["4%", "4 %", "4,0%", "4.0%", "4 percent", "4 procent", "vier procent"]`.
- CSV says `overtime_trigger_value=36, unit=hours` → search `["36 uur", "36 hours", "36 uren", "36u"]`.
- CSV says `paid_paternity_value=5, unit=weeks` → search `["5 weken", "5 weeks", "5 wk"]`.

Variants include: numeric format variants (decimal comma vs point, with/without leading zero), unit synonyms (uur/uren/u/hour/hours), spelled-out numerals (low integers only), and currency symbols if applicable (€100, EUR 100, 100 EUR). Variant generation lives in `qa/shared/value_variants.py`.

Value anchors give the subagent the specific source passage(s) the existing value likely came from. For `informed` mode this lets the subagent verify the proposed correction against the actual quote; for `blind` mode it surfaces the existing value's source so the subagent can confirm or contradict.

**Pass 3 — Structural expansion (applies to both anchor passes).** For every anchor hit, expand outward until structural parents are captured. The slicer is list-format-agnostic and conditional-aware:

- **List item containment**: if the anchor sits inside a list item, include the list-introducing line (typically ending with `:`) AND all preceding sibling items in the same list. List items detected across all common forms:
  - `a) b) c)`, `(a) (b) (c)`, `A. B. C.`
  - `1) 2) 3)`, `1. 2. 3.`, `(1) (2) (3)`
  - `i) ii) iii)`, `I. II. III.` — Roman numerals lower and upper
  - `-`, `*`, `•`, `–`, `—` — dash and bullet styles
  - Dutch ordinals: "ten eerste", "ten tweede", "ten derde", "onder a", "onder b"
  - Numbered headings like `1.1`, `2.3.4` (capture the section)
- **Conditional clauses**: if the anchor is inside or downstream of a conditional opener — Dutch: "indien", "wanneer", "bij", "als", "in geval van", "mocht", "tenzij"; English: "if", "when", "should", "in case of", "in the event of", "unless" — include the full conditional and ALL its consequents (the (a) (b) (c) of the sick-leave example), not just the matching consequent.
- **Section headers**: include the nearest preceding `##` or `###` (or numbered heading like `1.1`).
- **Default**: expand to nearest paragraph boundary with a ±500-character context window. Generous, not minimal.

**Atomicity rule.** Expanded passages are atomic — **never cut mid-passage**. If a passage doesn't fit, the whole passage is dropped, never partial. Truncation flag has clean meaning.

**Merge.** Adjacent or overlapping expanded passages merge into one. A value-anchored passage that overlaps a keyword-anchored passage merges naturally.

**Rank.** Each passage gets a relevance score = `(keyword_density × 1.0) + (value_match ? 2.0 : 0)`. Value-matching passages are weighted higher because they directly address the field under review.

**Size-fit.** Concatenate top-ranked passages until adding the next would exceed the cap:
- Total ≤ 2,000 tokens → `context_type=topic_section`, no truncation.
- 2,000 < total ≤ 4,000 → `context_type=topic_section_partial`, no truncation.
- Total > 4,000 → drop lowest-ranked passages one at a time until ≤ 4,000. Set `topic_section_was_truncated=true`. Record each dropped passage's descriptor (see below).
- A single passage alone exceeds 4,000 → route record to `needs_human_review` with reason `oversized_passage`.

**Dropped-passage descriptors (visibility).** For every passage that's dropped, record a short descriptor in the worksheet item:
- Section header it came from (if any)
- First 100 chars of the passage
- Which anchors matched (which topic keywords + any value match)
- Byte offset in source (for later spot-checking)

This becomes `dropped_passages: list[DroppedPassage]` on the worksheet item, and surfaces through the subagent output and into `corrections.csv` as a serialized JSON column `dropped_passages_summary`. A reviewer reading a row with `verdict=clear` or `verdict=unable_to_verify` and `topic_section_was_truncated=true` can immediately see what was set aside.

**Multi-match handling for value anchors.** A common case: CSV says `4%` and source mentions "4%" in 12 places, most unrelated to the topic (inflation index, holiday bonus, etc.). Strategy:
- Capture all value matches (no early filtering).
- Rank each by topic-keyword proximity (smaller char-distance to nearest topic-keyword anchor = higher rank).
- Keep top 5 by combined score.
- If zero value matches found but CSV has a value, that's a signal — record `value_not_in_source=true` on the worksheet item. Could be unit mismatch ("4%" in CSV but source says "0.04"), decimal-strip (FM_02), or genuine extraction error.

**`full_source` fallback** when keyword AND value scans both return zero hits in a source that the presence scan flagged as topic-relevant. Logs a Stage 5a action item to expand the keyword list. Default behavior: use full source up to the 4,000-token cap, otherwise route to NHR.

**Why generous cutoffs.** The leave run's 1,500-token cap occasionally cut anchored passages. Raising the soft target to 2,000 and the hard cap to 4,000, combined with atomicity, means: most items fit comfortably; a minority truncate (flagged, biased conservative); a tiny fraction route to NHR. Chunk size drops naturally from 20-25 to 15-20 items to stay under 50K total — builder computes dynamically per chunk.

**Truncation flag semantics.** `topic_section_was_truncated=true` means whole less-relevant passages were dropped, NOT that a passage was cut mid-content. The subagent reads this as "you have the highest-density material but may be missing context from elsewhere in the source — see `dropped_passages` for what was set aside."

**What does NOT go in prompts:** full schema docs, prior subagent outputs, sibling records' data, other topics' conventions or failure modes, the plan document, full source CSV.

**Why this is enforced in code, not by convention.** Prompt bloat compounds silently — each topic adds entries, each entry adds tokens, and by topic 8 the prompt could double; accuracy degrades because relevant rules get buried. The builder turns "too big" into a hard build-time error so the issue surfaces immediately rather than as a slow accuracy decline that's hard to attribute.

---

## 5. Per-topic execution (5 stages)

### Stage 0 — Hazard check (replaces open-ended schema audit)

**Goal:** detect schema fields that cannot represent something the source actually says, before running QA. The parental-leave gap was discovered during the leave QA run; sampling 5–10 excerpts will not reliably surface rare structural mismatches.

**Two-part procedure:**

**Part A — Hazard list (deterministic).** Before sampling, list the specific schema-vs-reality hazards expected for the topic. For each, check whether the schema can represent it:
- pension: AOW age progression, accrual cap evolution, premium splits, opt-out clauses, partnership pensions
- term: WAB transition, transitievergoeding formula by era, chain-rule (ketenregeling), zero-hours contracts
- overtime: tier schedules (X% / Y% beyond N hours), comp-time-vs-pay choice
- contract: probation length variants, fixed-term cap by era
- bonus: 13th month vs end-of-year vs profit-share splits, sector minima
- training: study cost recovery (WAB), individual study budget vs collective
- homeoffice: stipend amounts, equipment, mandatory days
- safety: PPE clauses, safety committees
- childcare: tax-credit interactions, sector subsidies
- ai: notification clauses, training requirements, monitoring opt-outs
- fringe: commuting (€/km vs €/month), meal, relocation, gifts
- wage: entry steps, performance steps, sector tables

**Part B — Stratified sampling (30+ excerpts).** Sample at least 30 excerpts from `inputs/by_topic/<topic>_information.md`, stratified:
- ≥ 2 CAOs from each major sector (health, education, public, manufacturing, retail, services)
- ≥ 2 from each era boundary the topic crosses
- ≥ 5 randomly selected from the remainder

For each sampled excerpt, check the three questions: does source mention something the field can't represent, does source describe structure the schema flattens, does source describe a concept with no matching field?

**Output:** a Stage 0 section in `qa/qa_<topic>/<topic>_summary_memo.md` containing the hazard-list check results + sample list + any gaps found. Even when no gaps surface, document the samples checked — makes "no gaps" auditable.

**HARD STOP — schema decisions are Hanna's.** If any gap is found, pause and write a short note summarizing each candidate with the excerpt and a recommended option (extend schema / document gap / defer affected sub-fields). Do NOT proceed past Stage 0 until Hanna acknowledges.

Working around schema gaps in clever-but-wrong ways — inventing fields, stuffing values into adjacent columns, redefining a field's semantics mid-topic — is the single biggest failure mode for this pipeline. Treat Stage 0 as a hard gate, not a checkbox.

**Stage 3.5 re-check.** Even with stratified sampling, rare gaps can slip through. After Stage 3, before Stage 4, scan the subagent output for `unable_to_verify` verdicts with high evidence-quote relevance — these are records where source clearly says something but the schema couldn't capture it. If > 5 such cases share a structural pattern, raise as a late-stage gap and halt before Stage 4.

### Stage 1 — Scope filter

**Input:** `inputs/extracted_data_non_salary.csv` (full 2,739 records).

**Action:** `scope_filter.most_recent_doc_per_cao(topic)`:
- Keep only CAOs in the 100-biggest set (1,505 `cao_number` values).
- For each CAO, keep the record with the latest `ingangsdatum` where `inputs/by_topic/<topic>_information.md` contains a matching section.

**Output:** `qa_<topic>/inputs/scoped_records.csv` (≤ 1,505 records, gitignored).

**Idempotency:** deterministic.

### Stage 2 — Deterministic layer

**Input:** scoped records + schema.

**Action:** Write `qa_<topic>_rules.py` and `qa_<topic>_patterns.py` adapting the L1 pattern from leave. Run them; emit `corrections_deterministic.csv` via `qa_<topic>_corrections_det.py`.

**Rules to consider per topic (template from leave):**
- Mutual-exclusion checks (annual XOR lustrum; full_time XOR part_time).
- Required-field-pair checks (value present → unit present).
- Range checks (overtime trigger 30–50 hours, full-time 32–40 hours).
- Statutory restatement clears (per convention #3).
- Boolean-flag consistency (`*_present` True ⇒ corresponding value populated).

**Per-rule metadata:** every rule declares `worksheet_mode ∈ {"extract", "blind", "informed", "none"}` per §1.3. Also declares `severity ∈ {statutory_clear, cosmetic, medium, high}` for §0.4 item 7.

**Stop condition:** if a rule with `severity ∈ {medium, high}` and `worksheet_mode="none"` auto-corrects > 25% of in-scope records, halt and review.

**Idempotency:** deterministic.

### Stage 3 — Subagent review (single pass)

**Input:** records flagged at L2-presence layer, pattern-detector layer, or Stage 2 rules with `worksheet_mode != "none"`.

**Action:** Generate chunk worksheets (same JSONL format as leave). Use `subagent_runner.run_chunks` with §4.2 defaults.

**Worksheet item fields:**

```
record_id, cao_number, file_name, ingangsdatum, era,
field, csv_value_old, csv_unit_old, flag_type, flag_reason,
topic_section,                       # keyword + value-anchored, atomic passages (§4.5)
context_type,                        # ∈ {topic_section, topic_section_partial, full_source}
topic_section_was_truncated,         # bool — whole low-relevance passages dropped to fit
dropped_passages,                    # list of {section, first_100_chars, anchors_matched, byte_offset}
value_not_in_source,                 # bool — CSV had a value but no source passage contained it
worksheet_mode,                      # ∈ {extract, blind, informed}
proposed_correction                  # only present when worksheet_mode == "informed"
```

Built by `worksheet_builder.build_chunk_items`, which enforces per-item budget and chooses `context_type`.

**Output:** `chunk_NNN_corrections.csv` files (gitignored).

**Stop condition:** if a subagent returns no output or only template/placeholder rows, halt; do NOT proceed to Stage 4.

**Idempotency:** stochastic.

### Stage 4 — Aggregate + audit + flag

**Input:** all corrections sources (deterministic + subagent + optional manual overrides).

**Action:**
1. `aggregator_lib.run_aggregation` — applies §1.4 reconciliation rules; assigns `fix_method`.
2. `audit_lib.run_audit` — runs the 13 checks (A13 honors sample-size floor).
3. For every audit-flagged row, set `fix_method=needs_human_review` and copy to `needs_human_review.csv` with the audit category in notes.
4. Compute A13 metric — disagreement rate per rule with firing-count gate. Surface rules with ≥ 10 firings and > 20% disagreement.

No fix subagent. No v2/v3/v4 loop.

**Output:**
- `qa_<topic>/outputs/corrections.csv` (final, tracked)
- `qa_<topic>/outputs/corrections_audit.csv` (tracked)
- `qa_<topic>/outputs/needs_human_review.csv` (tracked)
- `qa_<topic>/outputs/<topic>_summary_memo.md` (tracked): Stage 0 hazard-check result, total real changes, breakdown by `changed`, by `fix_method`, by audit flag, A13 rates.

**Stop conditions:**
- `needs_human_review.csv` > threshold from §0.4 item 2.
- `corrections.csv` would change > 10% of non-statutory-clear fields.

**Commit:** `qa/<topic>: stage 4 — <summary>`. Summary memo body becomes commit-message body.

**Idempotency:** deterministic (given same Stage 2/3 inputs).

### Stage 5 — Conventions update (manual)

After Stage 4, Claude reads the audit + `needs_human_review.csv` and proposes new failure-mode entries **in chat** per §3.2.4. Each proposal includes all four slots of the §3.2.1 template; missing-slot proposals are flagged "low confidence."

Hanna edits and pastes accepted entries into the canonical file directly. Commit as `qa/<topic>: stage 5 — conventions merged`.

`general_conventions.md` is never updated by this process.

After topics 1–3, if Claude reliably produces good candidates, consider building `conventions_updater.py` — grounded in observed patterns.

---

## 6. Subagent prompt template

Assembled by `worksheet_builder.build_subagent_prompt(topic)`. Slots:

1. Role description (static)
2. `qa/conventions/general_conventions.md` (verbatim)
3. `qa/conventions/failure_modes/general.md` (verbatim, cap 6)
4. `qa/conventions/failure_modes/per_topic/<topic>.md` (verbatim, cap 10–12)
5. `qa/conventions/per_topic/<topic>_conventions.md` (verbatim if exists)
6. Era-baseline table for current topic (from `era_baselines.py`)
7. Task spec, verdict vocab, strict rules (static)

```
You are a correction subagent for a Dutch CAO <TOPIC> QA pipeline.

═══ CONVENTIONS (READ FIRST) ═══
<verbatim qa/conventions/general_conventions.md>

═══ FAILURE MODES — GENERAL ═══
<verbatim qa/conventions/failure_modes/general.md, up to 6 entries>

═══ FAILURE MODES — TOPIC ═══
<verbatim qa/conventions/failure_modes/per_topic/<topic>.md, up to 12 entries>

═══ PER-TOPIC CONVENTIONS ═══
<verbatim qa/conventions/per_topic/<topic>_conventions.md if it exists>

═══ ERA-AWARE STATUTORY BASELINE ═══
<topic-specific baseline table from era_baselines.py>

═══ TASK ═══
Input file:   <absolute path to chunk_NNN.jsonl>
Output file:  <absolute path to chunk_NNN_corrections.csv>

Each line of input JSONL is one item with:
  record_id, cao_number, file_name, ingangsdatum, era,
  field, csv_value_old, csv_unit_old, flag_type, flag_reason,
  topic_section (verbatim source text),
  worksheet_mode ∈ {extract, blind, informed},
  proposed_correction (only when worksheet_mode == "informed")

For each item, emit one CSV row with these semicolon-delimited columns:
  record_id ; original_field ; verdict ; target_field ; new_value ; new_unit ;
  evidence_quote ; confidence ; failure_modes_referenced ; notes

═══ VERDICT VOCABULARY ═══
  confirm            : CSV value matches source; no change
  clear              : value is wrong/garbage; new_value=""
  correct_in_place   : same field, fix value/unit
  move               : value belongs in different field; set target_field
  set_boolean        : boolean field; new_value ∈ {True, False}
  unable_to_verify   : source doesn't allow a verdict; routes to human

═══ STRICT RULES ═══
  - UNKNOWN MUST be confidence=low
  - evidence_quote must be verbatim from topic_section
    (or "(no relevant text)" with confidence=low)
  - Never extrapolate across CAOs
  - DO NOT trust patterns from sibling records
  - Below-statutory readings = extraction error, not deviation
  - Process EVERY item — do not emit placeholder rows
  - Respect worksheet_mode:
      * extract  → CSV field is empty; extract from source if possible
      * blind    → verify the field against source independently
      * informed → proposed_correction is a hypothesis to test, not a fact
  - List failure_modes_referenced as comma-separated FM IDs you applied
    (empty if none)
  - When topic_section_was_truncated=true: whole less-relevant passages
    were dropped to fit. The remaining text is intact but may be missing
    context from elsewhere in the source. Prefer verdict=unable_to_verify
    over clear/correct_in_place/confirm unless the visible slice is
    unambiguous and self-contained. Look at dropped_passages to judge
    whether any dropped section was likely relevant; if so, downgrade
    confidence or pick unable_to_verify.
  - When value_not_in_source=true: the CSV has a specific value but the
    slicer found no passage in source containing that value (or its
    common variants). Treat with caution: if your reading agrees with
    the CSV value despite no anchor match, the source may use a
    paraphrase — set confidence=medium not high. If your reading
    contradicts, that's evidence of an extraction error.

Use the Write tool to produce the CSV. Report back: counts per verdict +
3 notable cases under 150 words.
```

---

## 7. Topic execution order

| Order | Topic | Rationale |
|---|---|---|
| 1 | overtime | Small (~12 fields), well-defined ranges. Validates the pipeline. |
| 2 | homeoffice | Small, recent, low era-complexity. |
| 3 | training | Mostly mechanical (yearly hours, budget). **Era note:** WAB study-cost recovery clause (2020) — verify `era_baselines.py` covers it before Stage 2. |
| 4 | contract | WAB 2020 era awareness — first topic that exercises `era_baselines.py` heavily. |
| 5 | bonus | 13th month + various amounts; moderate complexity. |
| 6 | fringe | Many sub-categories (commuting, meal, relocation); routine. |
| 7 | safety | Mostly booleans, similar to homeoffice. |
| 8 | childcare | Eligibility ranges; some statutory. |
| 9 | ai | Small, new (mostly booleans + policy presence). |
| 10 | term | High era-complexity (WWZ, WAB, transitievergoeding). Save until conventions mature. Era-baseline review checkpoint before this. |
| 11 | pension | Highest era-complexity (AOW age, accrual caps, premium splits). Last hard topic. Era-baseline review before this. |
| 12 | wage | Small (entry steps, performance steps). Quick wrap-up. |

The conventions file matures during topics 1–9, setting up the hardest two for success.

---

## 8. Success criteria per topic

A topic is "done" when:
1. `corrections.csv` exists with `is_noop`, `changed`, and `fix_method` columns populated.
2. 13-check audit reports zero issues, or all remaining issues are surfaced in `needs_human_review.csv`.
3. Summary memo states: Stage 0 hazard-check result, total real corrections, share of total fields, breakdown by `changed`/`fix_method`/audit flag, NHR count, list of new convention patterns proposed.
4. Stage 5a proposals presented in chat; Stage 5b merged by Hanna.
5. `qa/README.md` status board updated.
6. `qa/qa_<topic>/run.log` contains all stage events.
7. All required outputs committed and branch merged with Hanna's sign-off.

---

## 9. Estimated scope

**Claude Code time (autonomous compute):**
- 12 × Stage 0 (hazard list + 30-excerpt sample + write-up: ~30–45 min) = ~7–9 hours
- 12 × Stage 1 (~5 min: one-line call) = ~1 hour
- 12 × Stage 2 (~1 hour rules dev) = ~12 hours
- 12 × Stage 3 (~30–90 min subagent runtime) = ~12–18 hours
- 12 × Stage 4 + Stage 5a (~30 min) = ~6 hours

→ ~38–46 hours of Claude Code time. Plus Phase 0.5+1+2 setup (~3–5 hours once).

**Hanna time (review):**
- 12 × Stage 0 acknowledgment (~10–30 min) = ~3–6 hours
- 12 × Stage 5b convention review/merge (~20 min) = ~4 hours
- 12 × final summary review (~15 min) = ~3 hours
- 2 × era-baseline review before topics 10/11 (~30 min) = ~1 hour

→ ~11–14 hours of review time across the project.

**Wall-clock:** 2–3 weeks realistic, given weekly Task-tool rate limits + per-topic pause for Hanna review + interrupt-driven nature of an async workflow. Don't promise faster.

Pension and term eat ~30% of Claude Code time on their own.

---

## 10. First-run kickoff checklist

See `qa/EXPERT_IMPLEMENTATION_PLAN.md` for the actionable ordered sequence. That file is the source of truth for execution order; this section is preserved as architectural reference and the cross-reference to Phase tags used in commit messages.

**Phase 0** — Read and acknowledge.
**Phase 0.5** — Prerequisite check. Verify `by_topic/*.md` files exist for all 11 non-leave topics; empirically verify Task-tool batch limit; confirm schema is current.
**Phase 1** — Repository setup on branch `qa/setup`.
**Phase 2** — Reference implementation prep on branch `qa/setup`. Smoke test must pass with concrete tolerance before merging.
**Phase 3** — First topic execution (overtime) on branch `qa/overtime`.
**Phase 4** — Topics 2–9 (homeoffice through ai), one per day.
**Phase 4.5** — Era-baseline review before topic 10 (term).
**Phase 5** — Hard topics (term, pension) + wage wrap-up.

---

## 11. Status board

Maintained by Claude Code, updated after each stage completes.

```markdown
# CAO QA Pipeline — Status Board

| Topic       | Stage 0 | Stage 1 | Stage 2 | Stage 3 | Stage 4 | Stage 5 | Last run |
|-------------|---------|---------|---------|---------|---------|---------|----------|
| leave       | n/a     | done    | done    | done    | done    | done    | 2026-04-xx |
| overtime    | pending | -       | -       | -       | -       | -       | -        |
| homeoffice  | -       | -       | -       | -       | -       | -       | -        |
| training    | -       | -       | -       | -       | -       | -       | -        |
| contract    | -       | -       | -       | -       | -       | -       | -        |
| bonus       | -       | -       | -       | -       | -       | -       | -        |
| fringe      | -       | -       | -       | -       | -       | -       | -        |
| safety      | -       | -       | -       | -       | -       | -       | -        |
| childcare   | -       | -       | -       | -       | -       | -       | -        |
| ai          | -       | -       | -       | -       | -       | -       | -        |
| term        | -       | -       | -       | -       | -       | -       | -        |
| pension     | -       | -       | -       | -       | -       | -       | -        |
| wage        | -       | -       | -       | -       | -       | -       | -        |

States: `-` not started, `pending` blocked on Hanna, `in_progress`, `done`, `n/a`.
```

---

## 12. Known risks to actively manage

**Schema gaps blocking topics.** Stage 0 must pause for Hanna. If Claude Code tries to work around gaps by inventing fields or stuffing values into wrong columns, downstream corrections become unreliable. Hard gate.

**Convention prompt bloat.** Per-file caps (6 in `general.md`, 10–12 in any `per_topic/<topic>.md`) enforced by `worksheet_builder.build_subagent_prompt`. Pruning policy (§3.2.5) archives oldest-not-triggered entries when caps exceeded.

**Era baselines for pension and term.** Wrong baseline produces systematic false "below-statutory" clears. Hanna verifies `era_baselines.py` for these topics in Phase 3.5 before running them.

**Weekly Task-tool rate limits + per-message batch limit.** Two layers:
- **Per-message batch limit:** verified empirically in Phase 0.5; use that value as ceiling.
- **Weekly limit:** don't queue topics back-to-back. Per-topic Stage 5b review naturally spreads load. If a topic > 30 chunks, split Stage 3 across messages.

**Source-text cross-topic bleed.** Pension premiums sometimes appear in wage sections; overtime in collective working-time sections. `source_text_loader.load_source_text` and `presence_scan.scan_topic_presence` must handle cross-topic mentions. Document the strategy in Phase 0.5.

**Opus path has no port-time regression test.** Leave run was Sonnet, so the smoke test only validates Sonnet behavior. For the first 2–3 Opus chunks (early pension/term), flag for extra Hanna review before scaling out.

---

## Appendix A — `qa/CORRECTIONS_SCHEMA.md`

```markdown
# corrections.csv — schema reference

Each row represents one correction proposal for one (record_id, field) pair.

## Columns

| Column | Type | Description |
|---|---|---|
| record_id | str | The CSV row this correction applies to. |
| cao_number | str | The CAO this record belongs to. |
| original_field | str | The field being corrected. |
| topic_group | str | Topic family (e.g. overtime, pension). |
| csv_value_old | str | Original value in the CSV. |
| csv_unit_old | str | Original unit in the CSV. |
| verdict | enum | confirm / clear / correct_in_place / move / set_boolean / unable_to_verify |
| target_field | str | Set only when verdict=move. |
| csv_value_new | str | Corrected value, or "" for clear, or UNKNOWN. |
| csv_unit_new | str | Corrected unit. |
| evidence_quote | str | Verbatim source text supporting the verdict. |
| confidence | enum | high / medium / low |
| fix_method | enum | det+sub_agree / det_only / sub_only / det_sub_conflict / needs_human_review |
| failure_modes_referenced | str | Comma-separated FM IDs the subagent applied. |
| topic_section_was_truncated | bool | True if whole low-relevance passages were dropped by the slicer. Passed through from the worksheet item. |
| dropped_passages_summary | json-str | Serialized list of `{section, first_100_chars, anchors_matched}` for each dropped passage. Empty when not truncated. Lets a reviewer see what was set aside without re-running the slicer. |
| value_not_in_source | bool | True if CSV had a value but no variant of it was found in the source slice. Signals possible unit-mismatch, decimal-strip, or paraphrase. |
| is_noop | bool | True if cosmetic-only (filter out for analysis). |
| changed | enum | none / value / unit / both |
| notes | str | Free text (tier schedules, multi-value notes, etc.). |

## How to use this file

- For analysis: filter `is_noop=False`.
- For human review: filter `fix_method=needs_human_review`.
- For high-confidence corrections: `confidence=high AND fix_method=det+sub_agree`.
- `csv_value_new=""` (clear) means original was garbage; should be blanked.
- `csv_value_new=UNKNOWN` with `confidence=low` means source didn't allow a
  verdict — different from "" (clear).
```

---

## Appendix B — What to bring back from each topic run

- The final `corrections.csv`.
- The audit summary memo (including Stage 0 hazard-check result).
- Stage 5a proposed failure-mode entries (in chat, ready for Hanna paste).
- A short note flagging any A13 hits (rules with ≥ 10 firings and > 20% disagreement).
- Updated `qa/README.md` status board.
- `run.log` for the topic.

End of plan.
