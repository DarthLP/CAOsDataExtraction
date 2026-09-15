# Legacy queue status — retired vs live (Wave 3, 2026-07-03)

Cross-check of every legacy surface-only queue against the applied layers (L1–L5 changelogs,
6,762 applied keys) and the per-file campaign's 24,999 adjudicated cells.
Residual rows: `retired_queues_residual.csv`.

## RETIRED — superseded, do not work these

| Queue | rows | why retired |
|---|---|---|
| `agreement_conflicts.csv` (2,631 high-priority) | 18,515 | **Methodologically superseded**: the per-file campaign's input WAS this conflict set — every within-agreement conflict was independently re-verified per record (3,091 units / 24,999 cells) and resolved into CONFIRM/KEEP/FIX/RECONCILE/EMPTY/REVIEW, which then fed L3–L5. |
| `backfill_candidates.csv` (7,572 high-conf) | 10,637 | **Retired by policy**: these are sibling-inheritance proposals; the *"never assume silence = same"* ruling (2026-06-05) forbids exactly this. The KEEP bucket (2,681) is the policy's positive record. |
| `second_pass_nhr/` outputs | 58 | Consumed into the 2026-05 master review → L1 apply. |
| `verify_changes/` outputs | 226 | Consumed into the 2026-05 master review → L1 apply. |
| `MASTER_REVIEW_FOR_HANNA.csv` | 126 | Fully resolved + applied (L1). |
| `handoff/01_corrections_to_apply.csv` | — | Applied as L2 (781-cell promotion). |
| `FIX_clear` / `FIX_audit` / `statutory_deferred` / `surcharge_normalize` / RECONCILE+EMPTY | — | Applied/adjudicated as L3, L4, L5 (see `docs/DATA_LINEAGE.md`). |

## LIVE — genuine residual queues (low priority, never adjudicated by any later layer)

| Queue | residual rows | nature |
|---|---|---|
| `handoff/03_needs_judgment.csv` | 545 | full-audit judgment calls that never entered the per-file scope (mostly out-of-conflict cells) |
| `unit_semantics_reconciliation.csv` | 283 | KEEP_NONSTANDARD_UNIT etc. — preservation decisions, not errors |
| `needs_manual_placement.csv` | 68 | unit-dimension changes needing a value / ambiguous multivalue |
| `handoff/02_relocate.csv` | 17 | cross-field moves |
| `reverify_unsure_review.csv` | 10 | old reverify UNSURE tail |
| **Wave-2 residue** (`wave2` RESIDUE + unjudged) | 48 | adjudication residue incl. 3 spot-verify disagreements |
| `removals_bool_enum_review.csv` | 929 | L4 judge-vs-campaign disagreements — **default = keep, review optional** |
| CHECK-pattern numerics (pending Hanna's OK on `removals_numeric_suggestions.md`) | ~104 | suggestions written, awaiting approval |

These live queues are documentation-complete and reversible; none blocks the canonical dataset.
