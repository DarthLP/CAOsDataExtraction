# CAO Project — Re-verified State & Reorganization Plan

**Date:** 2026-07-01 · **Author:** Claude Code (planning pass) · **Status:** proposal, awaiting decisions
**Scope of this file:** documentation only. Creating this plan is the *only* change made; no data,
code, or existing doc was modified. Everything below is a proposal gated on the decisions in Part VIII.

---

## Part I — Re-verified current state (ground truth as of 2026-07-01)

### I.1 Data lineage — there are now THREE dataset generations

```
UPSTREAM (external project, NOT in this repo)
  /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/
    outputs/excel/new_results/extracted_data_non_salary.csv   ← byte-identical to inputs/ copy
          │  (copy, read-only)
          ▼
G0  inputs/extracted_data_non_salary.csv        2,739 rows × 317 cols · 242 CAOs · key col = `id`
          │
          │  Layer 1: per-topic clean-wins + master review  (apply_corrections.py + apply_list.csv)
          │           = 514 cells / 111 records
          ▼
     corrected_dataset.bak.2026-06-04.csv        (= G0 + Layer 1; the pre-promotion snapshot)
          │
          │  Layer 2: full-audit promotion  (promote.py, 2026-06-04, twice-verified)
          │           = 781 cells / 522 records
          ▼
G1  qa/corrected_dataset.csv                     = G0 + 1,295 cells / 611 records / 168 cols
          │                                         ← THIS is what indices/ currently read
          │  Layer 3: per-file FIX_clear only  (apply_perfile_fixes.py, applied to a COPY)
          │           = 3,318 cells / 1,000 records / 110 fields
          ▼
G2  qa/corrected_dataset.perfile_applied.csv     = G1 + 3,318 cells (raw→G2 = 4,585 cells / 1,366 recs)
                                                    ← newest & most-corrected; NOT yet canonical,
                                                      NOT read by indices
```

**No doc declares which of G1/G2 is canonical.** This is the single most important open item.

### I.2 Applied-vs-pending ledger (updated after your FIX_clear apply)

| Correction body | cells | applied to | status |
|---|---:|---|---|
| Per-topic clean-wins + master review (Layer 1) | 514 | G1 (`corrected_dataset.csv`) | ✅ applied |
| Full-audit promotion (Layer 2) | 781 | G1 | ✅ applied, twice-verified |
| **Per-file `FIX_clear`** (Layer 3) | **3,318** | **G2 (`…perfile_applied.csv`)** | ✅ applied to a copy, reversible |
| Per-file `FIX_audit` (convention calls) | 1,416 | — | ⚠️ pending (human) |
| Per-file `RECONCILE` (remove-unsupported) | 2,960 | — | ⚠️ pending (policy call) |
| Per-file `EMPTY` (absence removals) | 848 | — | ⚠️ pending (policy call) |
| Per-file `REVIEW` (undecidable) | 498 | — | ⚠️ pending |
| Per-file `statutory_deferred` | 172 | — | ⚠️ pending (deterministic statutory step) |
| Per-file `surcharge_normalize` | 41 | — | ⚠️ pending (mechanical) |
| Per-file `KEEP` (never inherit) | 2,681 | — | ✔ no-change by design |
| Per-file `CONFIRM` (already correct) | 13,434 | — | ✔ no-change by design |
| **Holistic populated-value re-read** | 1/13 topics | pension only | ⚠️ not rolled out |

**Pending, dataset-changing:** ≈ **5,935 perfile cells** (FIX_audit + RECONCILE + EMPTY + REVIEW +
statutory + surcharge) **+ holistic rollout to 12 topics**. Everything else is applied or no-change.

### I.3 What is genuinely complete

- All **13 topics** QA'd end-to-end (Stages 0–5). Yield 407 clean / 59 NHR (non-leave) + 40/0 (leave).
- **Two guarded, verified apply layers** in G1; **one** in G2 (FIX_clear).
- **Era baselines** (Phase 4.5): approved, encoded post-hoc, validated. Test suite ~92 passing.
- **Indices**: all 13 topics + composite + panel + statutory — built on **G1** (correct source *at build
  time*; now one generation behind G2).
- **Salary step-2 QA**: complete; `qa/qa_salary/outputs/corrected_salary.csv` produced (3 sign-offs open).

### I.4 Documentation & memory staleness found

| Doc | problem | fix |
|---|---|---|
| root `README.md` | is the **upstream CAOsDataExtraction** README (foreign, stale here) | replace with a real project README |
| `qa/full_audit/perfile_work/PERFILE_REVIEW_SUMMARY.md` | says *"Nothing has been applied"* (now false — FIX_clear applied); pre-split `RECONCILE 5641` (now KEEP 2681 + RECONCILE 2960) | update to post-apply, post-split numbers |
| `CLAUDE.md` | record counts *"2,836 records / 338 CAOs"*; file parses to **2,739 / 242** | reconcile the numbers |
| `qa/indices/README.md` | describes only 2 of 13 indices | rewrite to match `INDICES_OVERVIEW.md` |
| `qa/PIPELINE_SUMMARY.md` | claims Stage-4.5 recheck *"covers all 12 topics"*; only 3 exist (bonus/term/wage) | correct the claim |
| `qa/qa_pension/PENSION_RESUME.md`, `qa/qa_term/TERM_RESUME.md` | describe interrupted work that **was** finished | archive |
| `qa/qa_leave/` | leave never got its Stage-5 memo | write it (or note as intentionally skipped) |
| auto-memory `indices-workstream-state` | says *"3 remain (safety, ai, pension)"* — all 13 done | update |
| (missing) | no doc records the G1→G2 lineage or the canonical pointer | new `docs/DATA_LINEAGE.md` |

### I.5 Structural problems (the "mess")

- **Root clutter:** 10 orphan one-off scripts (already gitignored), 6 scattered planning/phase `.md`, the foreign README.
- **Path-split artifacts** (unquoted `Dutch Bargaining Agreements` in a `cp`): `Agreements/` (only `.DS_Store`), `Bargaining/` (empty), parent-level `Dutch/` (empty). Safe to delete.
- **138 byte-identical duplicate `* 2.json`** in `full_audit/verify/chunks/`; `contract_summary_memo 2.md` (stale); 22 `.DS_Store`.
- **Correction engine loose in `qa/` root**: `apply_*.py/.csv`, `consolidate_review.py`, three `corrected_dataset*.csv` mixed with planning docs → pipeline isn't legible from the tree.
- **Cross-cutting verification scattered**: `full_audit/`, `second_pass_nhr/`, `verify_changes/` at `qa/` root with no grouping.
- **`qa_leave/` (top-level reference)** holds ~37 one-off chunk scripts beside the canonical ones (frozen reference + a live data path — gated behind your approval).
- **Indices stale vs G2** (read G1); **canonical dataset undeclared**.

---

## Part II — Target folder structure (precise)

### II.1 Design principles

1. **Pipeline-legible:** the top level should read `inputs → qa (correct) → indices`, with a visible split between *what is applied* (`corrections/`) and *what is surfaced for review* (`verification/`).
2. **Agent-friendly:** one obvious entry doc (`README.md` → `docs/`), accurate `CLAUDE.md`, a single `DATA_LINEAGE.md` that answers "which dataset is real and what's in it."
3. **Path-safe:** `qa/` is NOT renamed (dozens of scripts hard-code `qa/…`, `common.PROJECT_ROOT`, `index_lib.CORRECTED_CSV`). Moves that touch code paths are isolated into **Tier 2** with an explicit update list.
4. **Reversible & tested:** every move is `git mv`-able and followed by `python3.13 -m pytest qa/shared/tests` (currently ~92 passing) before the next step.

### II.2 Target tree (annotated; ⚠ = touches a hard-coded path)

```
Dutch Bargaining Agreements/
├── README.md                       ▲ REPLACE  foreign upstream README → real project README (pipeline overview)
├── CLAUDE.md                       ● UPDATE   pointers, record counts, canonical-dataset rule
├── .gitignore .claude/             ○ keep
│
├── docs/                           ✦ NEW  — single home for architecture / decisions / status
│   ├── PLAN.md                     ⇙ MOVE from qa/PLAN.md            ⚠ (referenced by CLAUDE.md, memos)
│   ├── PIPELINE.md                 ⇙ MOVE/RENAME from qa/PIPELINE_SUMMARY.md
│   ├── DATA_LINEAGE.md             ✦ NEW — G0→G1→G2 lineage + applied-vs-pending ledger + canonical pointer
│   ├── DECISIONS.md                ✦ NEW — the "why" log (absorbs APPLY_POLICY.md + methodology rules)
│   ├── STATUS.md                   ⇙ MOVE from qa/README.md (status board + counts)
│   ├── CORRECTIONS_SCHEMA.md       ⇙ MOVE from qa/
│   └── archive/                    ✦ NEW — historical, never loaded into prompts
│       ├── EXPERT_IMPLEMENTATION_PLAN.md   (from qa/)
│       ├── phase_0_5_findings.md · phase_2_notes.md · phase_4_5_era_baseline_review.md
│       ├── qa_leave_cleanup_candidates.md
│       └── PENSION_RESUME.md · TERM_RESUME.md   (from qa/qa_pension, qa/qa_term)
│
├── inputs/                         ○ keep (read-only, Stage 0)
│
├── qa/                             ○ KEEP NAME (Stage 1 engine)
│   ├── shared/ conventions/ qa_<topic>/        ○ keep in place
│   ├── corrections/                ✦ NEW subdir  ⚠ update apply_*.py + index_lib.py paths (Tier 2)
│   │   ├── apply_corrections.py · consolidate_review.py
│   │   ├── apply_list.csv · apply_changelog.csv · apply_skipped.csv · nonleave_source_corrections.csv
│   │   └── datasets/
│   │       ├── corrected_dataset.csv                 (canonical — see Part III)
│   │       ├── corrected_dataset.perfile_applied.csv
│   │       ├── corrected_dataset.bak.2026-06-04.csv
│   │       └── MANIFEST.md                          ✦ NEW — one row per generation, what's in it
│   └── verification/               ✦ NEW subdir (surface-only layers grouped)  ⚠ light path updates
│       ├── full_audit/  second_pass_nhr/  verify_changes/
│
├── indices/                        ⇙ OPTIONAL promote from qa/indices  ⚠ update index_lib PROJECT_ROOT math
│
├── salary/                         ⇙ OPTIONAL promote from qa/qa_salary (separate track)
│
└── reference/
    └── qa_leave/                   ⇙ MOVE from top-level; one-off scripts → reference/qa_leave/archive/
```

### II.3 Two tiers (so nothing breaks by surprise)

- **Tier 1 — safe, zero code-path risk (recommended first):** everything in `docs/` (moving *markdown only*),
  new README, fix stale docs, write `DATA_LINEAGE.md` + `DECISIONS.md` + `datasets/MANIFEST.md` (as a doc,
  datasets stay put for now), memory updates, and the **cleanup** in Part VI. No `.py` path references change;
  markdown moves can't break the pipeline.
- **Tier 2 — deeper regroup, requires code edits + a test run:** the `qa/corrections/`, `qa/corrections/datasets/`,
  `qa/verification/` subdirs; promoting `indices/` and `salary/` to top level; moving `qa_leave/` under `reference/`.
  Each requires updating hard-coded paths. Exact reference list to update:
  - `qa/indices/index_lib.py:25` `CORRECTED_CSV = …/qa/corrected_dataset.csv`
  - `qa/full_audit/apply_perfile_fixes.py:18` `OUT = PROJECT_ROOT/"qa"/"corrected_dataset.perfile_applied.csv"`
  - `qa/apply_corrections.py`, `qa/consolidate_review.py` (dataset + apply_list paths)
  - `qa/full_audit/promote.py`, `common.py` (PROJECT_ROOT-relative dataset paths)
  - `qa/shared/manual_review_lib.py` (reads `qa_leave/outputs/…` at runtime → update if `qa_leave/` moves)
  - grep gate before executing Tier 2:
    `grep -rn "corrected_dataset\|qa/indices\|qa_leave/\|qa/qa_salary\|PROJECT_ROOT" qa --include=*.py`

### II.4 Per-item disposition (every current root entry)

| current path | action | destination | tier |
|---|---|---|---|
| `README.md` (foreign) | replace | new project README | 1 |
| `CLAUDE.md` | edit in place | — | 1 |
| `qa/PLAN.md` `qa/PIPELINE_SUMMARY.md` `qa/CORRECTIONS_SCHEMA.md` `qa/README.md` | move | `docs/` | 1 |
| `qa/EXPERT_IMPLEMENTATION_PLAN.md` | move | `docs/archive/` | 1 |
| `phase_0_5_findings.md` `phase_2_notes.md` `phase_4_5_era_baseline_review.md` `qa_leave_cleanup_candidates.md` | move | `docs/archive/` | 1 |
| `CAO_VERSION_SELECTION_PLAN.md` `LIFECYCLE_STAGE_PLAN.md` `FULL_DATASET_AUDIT_PROMPT.md` | keep (forward-looking) → `docs/` | `docs/` | 1 |
| `Agreements/` `Bargaining/` `../Dutch/` | delete | — | 1 |
| 10 orphan root `*.py/*.sh` (gitignored) | delete | — | 1 |
| 138 `* 2.json` + `contract_summary_memo 2.md` + 22 `.DS_Store` | delete | — | 1 |
| `qa/apply_*.{py,csv}` `qa/consolidate_review.py` `qa/nonleave_source_corrections.csv` | move | `qa/corrections/` | 2 |
| `qa/corrected_dataset*.csv` | move | `qa/corrections/datasets/` | 2 |
| `qa/full_audit/` `qa/second_pass_nhr/` `qa/verify_changes/` | move | `qa/verification/` | 2 |
| `qa/indices/` | promote | `indices/` | 2 |
| `qa/qa_salary/` | promote | `salary/` | 2 |
| top-level `qa_leave/` | move + archive one-offs | `reference/qa_leave/` | 2 |

---

## Part III — Data canonicalization plan (the most important part)

### III.1 The problem
Three generations exist (G0 raw, G1 `corrected_dataset.csv`, G2 `…perfile_applied.csv`). Indices read G1.
G2 is newer and more correct but unlabelled as canonical. Anyone (human or agent) opening the folder
cannot tell which file is "the dataset."

### III.2 Proposed scheme — explicit versioning + a declared pointer
1. A `datasets/MANIFEST.md` with one row per generation: filename, parent, what was applied, cell count, verified?, date.
2. A **single declared canonical file** named `corrected_dataset.csv` (keep the stable name so `index_lib.py`
   never needs to chase a moving filename); older generations kept as suffixed snapshots.
3. Every future apply follows the existing discipline: apply to a *new* suffixed copy → verify → then
   *promote* by copying over `corrected_dataset.csv` (with a dated `.bak`), exactly like the 2026-06-04 promotion.

### III.3 Promote G2 → canonical? (decision D2)
`FIX_clear` is high-confidence by construction (new value = the record's *own* source, quote-backed, same
unit dimension, non-convention) and fully reversible. Options:
- **A (recommended): promote G2 → `corrected_dataset.csv`.** Backup current G1 as
  `corrected_dataset.bak.2026-07-01.csv`; copy G2 over `corrected_dataset.csv`; indices auto-pick it up on
  next build. Optionally run a formal re-verify pass first (G2 has only mechanical verification so far).
- **B: keep G1 canonical**, treat G2 as a candidate pending a formal re-verify.
- **C: full versioning with a `current` symlink** and repoint `index_lib.py` to the symlink.

### III.4 Re-point / rebuild indices
Whichever canonical is chosen, **rebuild the indices** so they reflect it (they currently embed G1). If G2
is promoted under the stable name, the rebuild is just re-running the index scripts; no code change.
Also flag the known caveat: the **wage-floor (`mw`) index** reads an *external, un-QA'd* upstream salary
file (`CAOsDataExtraction/…/salary_increase_events_derived.csv`) — unaffected by any of this and worth a
one-line note in `DATA_LINEAGE.md`.

---

## Part IV — Documentation plan (create / update / fix — with the WHY)

**Create**
- `docs/DATA_LINEAGE.md` — *why:* the one place that answers "which dataset is real, what's in it, what's
  pending." Contains the G0→G2 diagram, the Part I.2 ledger, the MANIFEST link, the canonical pointer.
- `docs/DECISIONS.md` — *why:* the rationale log so choices aren't re-litigated. Seed from existing sources:
  surface-never-auto-fix; never-feed-statutory-numbers; era-baselines-post-hoc-only; definition-anchored
  re-verification (~22% catch); **"never assume silence = same"** (the KEEP split); FIX_clear-only apply
  rule (absorb `APPLY_POLICY.md` here and leave a stub pointer).
- `qa/corrections/datasets/MANIFEST.md` — *why:* machine-and-human readable generation table.
- Project `README.md` — *why:* first thing anyone sees; currently a foreign doc.

**Update / fix**
- `CLAUDE.md`: pointer paths (→ `docs/`), record counts (2,739/242), add "canonical dataset =
  `qa/corrections/datasets/corrected_dataset.csv`; see `docs/DATA_LINEAGE.md`".
- `PERFILE_REVIEW_SUMMARY.md`: post-apply status + post-split bucket numbers.
- `qa/indices/README.md`: rewrite to the 13-topic + composite + panel reality (or fold into `INDICES_OVERVIEW.md`).
- `PIPELINE_SUMMARY.md` (→ `docs/PIPELINE.md`): correct the "Stage-4.5 covers all 12 topics" claim.
- Leave Stage-5 memo: write it or explicitly mark skipped in `qa/qa_leave/run.log`.

**Archive** (move to `docs/archive/`, don't delete — git-less repo, keep history on disk)
- `EXPERT_IMPLEMENTATION_PLAN.md`, `phase_0_5_findings.md`, `phase_2_notes.md`,
  `phase_4_5_era_baseline_review.md`, `qa_leave_cleanup_candidates.md`, `PENSION_RESUME.md`, `TERM_RESUME.md`.

---

## Part V — Memory plan (`~/.claude/.../memory/`)

- **Update** `indices-workstream-state.md`: all 13 topics DONE + composite + panel (remove "3 remain").
- **Add** `dataset-generations.md` (project): G0/G1/G2, what each applied, which is canonical, upstream source path.
- **Add** `apply-policy-fix-clear.md` (feedback): only write a cell when the record's OWN source positively
  states a different value; never inherit on silence; never remove on absence; convention fields → human.
- **Add** `upstream-source-path.md` (reference): `/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/…`.
- **Update** `MEMORY.md` index lines accordingly.

---

## Part VI — Cleanup actions (precise)

**Delete (safe — artifacts / byte-identical dupes / gitignored):**
- Dirs: `Agreements/`, `Bargaining/`, parent `../Dutch/` (verify empty-but-`.DS_Store` first).
- `qa/full_audit/verify/chunks/**/* 2.json` (138; confirm byte-identical with a `diff` loop first),
  `qa/qa_contract/contract_summary_memo 2.md` (stale), all `.DS_Store` (22).
- 10 root orphan scripts (already gitignored): `correction_processor.py do_process.sh exec_processor.py
  final_correction_processor.py process_chunk.py process_chunk_018.py process_corrections.py
  quick_process.py run_processor.sh smart_correction_processor.py`.

**Archive (move, keep on disk):** the historical `.md` in Part IV; the ~37 one-off `qa_leave/` root scripts
→ `reference/qa_leave/archive/` (Hanna-gated per existing `qa_leave_cleanup_candidates.md` policy).

**Verify-before-delete gate:** `find . -name "* 2.json" -exec sh -c 'diff -q "$1" "${1% 2.json}.json" >/dev/null || echo "DIVERGES: $1"' _ {} \;`
(only delete the ones that print nothing).

---

## Part VII — Pending correction work (options, not auto-done)

1. **Rest of the per-file queue (~5,935 cells).** Recommended: *document in `DATA_LINEAGE.md`*, then adjudicate
   in this safe order — `surcharge_normalize` (41, mechanical) → `statutory_deferred` (172, deterministic step)
   → `FIX_audit` (1,416, convention calls) → `RECONCILE`/`EMPTY` (3,808, policy call on absence-removal) →
   `REVIEW` (498). Each applied to a new suffixed copy, verified, then promoted — same discipline as FIX_clear.
2. **Holistic rollout (12/13 topics).** The only layer that re-reads already-populated values. Big multi-rate-window
   job (~400+ chunks). Recommended: schedule as its own campaign after the reorg, not folded in here.
3. **Salary track sign-offs.** 8 unit fixes + 6 CAO-408 placeholders + optional step-1 re-extraction rollout.

---

## Part VIII — Decisions required

| # | Decision | Recommendation | Why / alternative |
|---|---|---|---|
| **D1** | Reorg depth | **Tier 1 now**, Tier 2 after | Tier 1 is zero-risk (markdown + cleanup); Tier 2 needs code-path edits + a test run. |
| **D2** | Canonical dataset | **Promote G2 → `corrected_dataset.csv`** (backup G1), then rebuild indices | FIX_clear is high-confidence + reversible; keeps indices current. Alt: keep G1 until a formal re-verify of G2. |
| **D3** | Remaining perfile queue + holistic | **Document as pending now**; adjudicate later in the Part VII order | Preserves the surface-never-auto-fix discipline; nothing lost. Alt: apply `surcharge_normalize`+`statutory_deferred` now (lowest-risk). |
| **D4** | Junk removal | **Delete safe junk, archive history** | Artifacts/dupes are recoverable/regenerable; history moved not lost. Alt: archive-only. |
| **D5** | Re-verify G2 before promoting? | **Optional** — a formal definition-anchored pass on the 3,318 FIX_clear cells | FIX_clear already == own-source; a pass would match the Layer-2 rigor. |

---

## Part IX — Execution sequence (once decisions are set)

**Phase A — Tier 1, no-risk (do first):**
1. `mkdir docs docs/archive` · `git mv`/move the markdown per II.4 · leave datasets/code untouched.
2. Write `README.md`, `docs/DATA_LINEAGE.md`, `docs/DECISIONS.md`, `datasets/MANIFEST.md`.
3. Fix stale docs (PERFILE summary, indices README, CLAUDE.md counts, PIPELINE claim, leave memo).
4. Update memory files + `MEMORY.md`.
5. Cleanup Part VI (with the verify-before-delete gate).
6. **Checkpoint:** `python3.13 -m pytest qa/shared/tests` still ~92 passing (should be unaffected).

**Phase B — canonicalization (D2):**
7. If promoting G2: backup G1 → `…bak.2026-07-01.csv`; copy G2 over `corrected_dataset.csv`; record in MANIFEST.
8. Rebuild indices; sanity-diff index outputs vs prior; note the mw external-salary caveat.

**Phase C — Tier 2 regroup (only if D1 = Tier 2):**
9. Create `qa/corrections/`, `qa/corrections/datasets/`, `qa/verification/`; `git mv` targets.
10. Update every path in II.3's list; re-run pytest; re-run one index build end-to-end as a smoke test.
11. Promote `indices/`, `salary/`, `reference/qa_leave/`; update `manual_review_lib.py` + `index_lib.py` roots.
12. Full smoke test: one topic aggregate + one index build + pytest.

**Phase D — pending work (D3):** per Part VII, on explicit go-ahead only.

---

## Part X — Risks & guardrails

- **Path breakage (Tier 2):** mitigated by the grep gate + pytest after each move; datasets keep stable names.
- **iCloud/FUSE quirks:** timestamps/sizes unreliable on this mount — verify by *content* (`diff`, pandas), not `ls`.
- **Irreversible deletes:** only delete gitignored orphans, verified-identical dupes, and empty artifact dirs;
  everything else is *moved*. Datasets are never deleted, only backed-up-and-promoted.
- **Losing the pending backlog:** the ~5,935-cell queue and holistic status are written into `DATA_LINEAGE.md`
  so they remain visible even after the folder is tidied.
```
