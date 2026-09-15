# qa_leave — Frozen leave reference implementation

**Status: COMPLETE. Do not modify** (hard rule in `CLAUDE.md`). Moved from top-level
`qa_leave/` to `reference/qa_leave/` on 2026-07-01.

This is the original leave-topic QA run that established the pipeline pattern
(deterministic rules + LLM subagent review + audit) later generalized into `qa/shared/`
for the 12 non-leave topics. It produced ~2,157 real corrections out of ~87,290 leave
fields (~2.5%) — the calibration baseline quoted in `qa/PLAN.md`.

## What matters in here

| Path | What |
|---|---|
| `scripts/` | Canonical entrypoints (`qa_leave_aggregate_corrections.py` etc.). |
| `outputs/corrections.csv` | The final leave corrections output. |
| `outputs/manual_review_followup/` | **Live data path** — read at runtime by `qa/shared/manual_review_lib.py` for the leave topic. |
| `docs/` | The original leave prompts + conventions (predecessors of `qa/conventions/`). |
| `inputs` | Symlink → `../../inputs` (backward-compat for scripts that hard-code the old path). |
| `*.py` at this root | ~37 one-off chunk-processing drivers from the original run. Historical; not imported by anything. Kept per the do-not-modify rule (cleanup proposal: `docs/archive/qa_leave_cleanup_candidates.md`). |

Note: the **in-pipeline** leave run (standardized re-run through `qa/shared/`, 2026-05-27,
40 clean / 0 NHR) lives at `qa/qa_leave/` — a different directory from this frozen reference.
