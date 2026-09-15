# Decisions — why the pipeline works the way it does

The rationale log. Each entry is a decision that shapes the pipeline, with the reason it was made.
New decisions append here; corrections to the dataset follow these rules.

---

### Source of truth is the English-translated source text
The upstream extractor translated the original Dutch CAOs to English. The per-topic
`inputs/by_topic/*_information.md` are therefore **English-primary**. Keyword sets in
`qa/shared/field_keywords.py` must be English-primary; Dutch terms are retained only when they
survive translation (statutory acronyms, fund names, untranslatable concepts). *Why:* Dutch
sentence-fragments never match the translated source and only inflate keyword sets.

### Catch false-negatives, not just typos
The core trigger (L2 presence scan) fires on **empty/False fields whose keywords appear in source**.
*Why:* the extractor's biggest risk is *missing* a provision, not mistyping one.

### Deterministic where possible, LLM only for prose
Schema-internal checks and statutory comparisons are code; reading Dutch-legal prose is the subagent's
job. *Why:* determinism is cheaper, reproducible, and can't hallucinate.

### Surface, never auto-fix
Audit-flagged and cross-cutting-verification rows go to review files; only guarded aggregators/apply
scripts write datasets. *Why:* fix-loops compound errors (an explicit lesson from the leave run).

### Never invent values; never feed the model statutory numbers
If the source doesn't state a value → `unable_to_verify`, leave empty. Statutory floors/caps are checked
**post-hoc in code** (`qa/shared/era_baselines.py`), never shown in the subagent prompt. *Why:* a
statutory number in the prompt anchors the model into filling it in as if it were the CAO's value.

### Definition-anchored verification
Every verification subagent is given the field's original schema/pydantic definition as the anchor.
*Why:* a definition-less holistic pass had a ~22% error rate on its own corrections (e.g. probation
max 1→2 months, surcharge max 17→80%); re-running with the definition caught them. (2026-06.)

### Never assume silence = same (per-file consolidator, Hanna, 2026-06)
When one version-record of an agreement is silent on a field and a sibling differs, **keep the silent
record's own value** (`KEEP`); do not inherit the sibling's. Only remove a value when **no** version's
source supports it (`RECONCILE`). *Why:* thin/jumpy documents are common; inheriting on silence would
fabricate data the record never stated.

### Per-file apply policy — `FIX_clear` only (2026-06 → promoted 2026-07-01)
Of the six per-file action types, only **`FIX_clear`** is auto-applied: the record's **own source**
positively states a different value, quote-backed, same unit dimension, non-convention. Everything else
is a human queue — `FIX_audit` (statutory base-vs-total, surcharge, tier enums), `RECONCILE`/`EMPTY`
(absence-based removals), `REVIEW` (undecidable). Full table in `qa/full_audit/APPLY_POLICY.md`.
*Why:* the residual buckets mix genuine errors with convention/representation differences a human must
adjudicate; auto-applying them would corrupt correct data.

### Apply to a copy, verify, then promote
Every apply writes a **new** dataset file, is verified (before-cell == recorded old value; nothing
changed outside the changelog; spot-checks vs source), logged reversibly, and only then promoted over
`corrected_dataset.csv` with a dated `.bak`. *Why:* reversibility + auditability; the base is never
edited in place.

### Indices are built on the corrected dataset
`indices/index_lib.py` (top-level `indices/`) reads `qa/corrected_dataset.csv`. *Why:* indices should reflect QA'd data.
Exception: the wage-floor (`mw`) index reads external upstream salary data by necessity (that corpus is
separate and not QA'd here) — a documented caveat, not a bug.

### Value conventions: CAO base figure + surcharge increment (Hanna, 2026-07-01)
When CAO text and dataset disagree on representation: durations/amounts store the **CAO's own
figure excluding statutory add-ons** (e.g. paternity = the CAO's 2 days, not 5 incl. statutory
WAZO); overtime surcharges store the **increment** (25), not the total (125). *Why:* consistent
with the no-statutory policy and with how most of the dataset is already encoded. Consequences:
`FIX_audit` cells are adjudicated under these rules; the `surcharge_normalize` draft (41 rows,
drafted toward totals) is superseded — those cells are re-derived toward increments or dropped.

### Backlog apply plan (Hanna, 2026-07-01)
RECONCILE (2,960) + EMPTY (848) absence-removals: **sample-verify ~100 against source, then apply
all if clean** (copy → changelog → verify → promote). FIX_audit (1,416): **definition-anchored
adjudication** under the conventions above, then apply survivors; residue → small human queue.
REVIEW (498) + statutory_deferred (172) stay parked. Holistic rollout: deferred until after these applies.

### Folder organisation (2026-07-01)
Root: `README.md`, `CLAUDE.md`, `docs/`, `inputs/`, `qa/`, plus the promoted pipeline dirs
`indices/` (Stage 2), `salary/` (separate track), `reference/qa_leave/` (frozen reference).
Cross-cutting docs live in `docs/`; history in `docs/archive/`. **`qa/` was not renamed, the
apply hub + datasets stay at `qa/` root, and `full_audit/` stays inside `qa/`** — *why:*
`full_audit` is imported as the `qa.full_audit` package across 34 files (incl. the load-bearing
`apply_perfile_fixes.py`) and the apply scripts hard-code sibling paths to `qa_<topic>/`; moving
them risks silently breaking manually-run, partly-untested scripts for cosmetic gain. The three
promoted dirs each had their `Path(__file__)` anchors patched and were verified (tests + rebuild).
