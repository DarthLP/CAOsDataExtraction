# Leave — Stage 5 closeout memo (retroactive)

Written 2026-07-01 as a retroactive closeout — leave was the only topic whose Stage-5 memo
was never written (its run.log ended at stage_4). All facts below are from
[run.log](run.log) and the status board; nothing new was run.

## What this directory is

The **standardized re-run** of the leave topic through the shared pipeline
(`qa/shared/`), executed 2026-05-27 so leave has the same artifact set as the 12
non-leave topics. The **original** leave QA (the much larger run that seeded the whole
methodology, ~2,157 corrections) is the frozen reference at `reference/qa_leave/`.

## Results (2026-05-27 standardized re-run)

| Stage | Result |
|---|---|
| 1 · scope | 2,739 total → **95 records / 95 CAOs** |
| 2 · deterministic | 47 rule violations + 3,021 L2 presence triggers + 0 enum = 3,068 items |
| 3 · subagents | 165 chunks / 3,068 items (prompt ~3.6k tokens) |
| 4 · aggregate | 3,350 rows → **42 real · 40 clean wins · 0 NHR · 0 era outliers** |
| 5 · closeout | this memo (retroactive). FM entries: `qa/conventions/failure_modes/per_topic/leave.md` (seeded from the original run — no new FM candidates emerged from the re-run). |

The 40 clean wins were routed through `consolidate_review.py` → `apply_list.csv` and are
part of correction **Layer 1** in `qa/corrected_dataset.csv` (see `docs/DATA_LINEAGE.md`).

## Notes

- 0 NHR / 0 era outliers is expected: the original reference run + its manual-review
  follow-ups had already resolved leave's hard cases; the re-run's role was standardization.
- The parental-leave **index** deliberately does NOT consume the qa_leave statutory-blanking
  corrections (wrong for a levels index); it applies only the 66 source-verified overlay fixes
  (`indices/corrections/leave_source_corrections.csv`). See `indices/parental_leave_index.py` docstring.
