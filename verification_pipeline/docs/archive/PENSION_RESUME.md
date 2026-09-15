# PENSION run — RESUME checkpoint (paused on rate limit)

**Paused 2026-05-25:** hit the session/weekly rate limit again mid-Stage-3.
Resets **~04:30 Europe/Madrid**. No subagents until then.

## State: Stage 3 partial — 39/53 chunks complete

- **Stage 0/1/2 done:** 95 scoped records, 2 L1 + 1040 L2, 0 ENUM → 53 chunks (20 items each).
- **Pipeline + era flagging**: `qa_pension_aggregate.py` wires the same (term-validated)
  guards + post-hoc `flag_topic_outliers("pension", …)` (accrual cap / franchise floor;
  unit-tested). Era baselines applied **post-hoc only** — never in the prompt.

### Chunks COMPLETE (39) — do NOT re-run
`001`–`039` (all present, validated 12-col by their subagents).

Quality across all 39: overwhelmingly `confirm` / `unable_to_verify` with **empty
values** — pension is heavily *fund-deferred* (premiums/accrual/franchise "as per the
fund regulations", or DC schemes with no DB accrual rate), so most fields are correctly
empty. **Zero statutory fill-in** (no Witteveen %, no AOW age, no franchise € invented)
across calibration + all waves. Conservative booleans held (the "excedentregeling" L2
keyword fires from the question-stem on many records → subagents correctly kept
`excedent_present` False/empty every time). Strong trap-avoidance: SAZAS/WGA sickness %s,
excedent-salary thresholds, illness-year accrual (100%/70%), Generatiepact/80-90-100
figures, and cost-split ratios all correctly kept out of contribution/accrual/franchise.

### Chunks TO RE-RUN (14) — after the 04:30 reset
`040 041 042 043 044 045 046 047 048 049 050 051 052 053`
(040–042 launched but didn't write before the limit; 043–050 failed with the limit;
051–053 were never launched.)

## Resume steps
1. After reset, relaunch the 14 chunks on **Opus**, waves of ≤12. Prompt (same as the validated run):
   > QA correction subagent for the Dutch CAO PENSION topic. Read BOTH `qa/qa_pension/worksheets/system_prompt.txt` AND `qa/qa_pension/worksheets/PENSION_RUN_NOTES.md`. Process every item in `qa/qa_pension/worksheets/chunks/chunk_NNN.jsonl` and write a 12-column semicolon CSV (header + one row per item, same order) to `qa/qa_pension/outputs/subagent_worksheets/chunks/chunk_NNN_corrections.csv`. Do NOT spawn subagents. One-line summary when done.
2. Verify all 53 present; run `python3.13 qa/qa_pension/scripts/qa_pension_aggregate.py`.
3. Integrity double-check (rows==input, dup/leak=0; verify clean wins vs source; **check `era_outliers.csv`** — watch CAO 509001's 2.05% accrual and CAO 43020's 2.1% accrual, both ABOVE the post-2015 Witteveen 1.875% cap *if* captured/post-2015).
4. Memo + `conventions/failure_modes/per_topic/pension.md` (PENSION_FM: fund-deferred→empty; excedent-keyword false-positive; DC/WTP no-accrual; value/range split; concept-distinction traps) + README board + run.log. **Pension is the last topic → all 12 done.**

## Known issue to check post-run (value/range/cross-chunk split)
Several records state concrete values in source that land in fields split across chunks:
- **509001**: accrual 2.05%/yr, employer 1.5%/employee 1%, franchise €12,232.
- **43020**: accrual 2.1%, 55/45 split.
- **1296017** (ICK): total premiums 31.7%/22.3%, accrual 1.823%, franchise €19,795, age 68.
- **331004 / 592012 / 234 / Zoetwaren(359014)**: single contribution %s (15.5%, 4.275%, 11.2%, 10%/20%).
Verify at double-check whether these scalar values are (a) already in the dataset (extractor got them), (b) captured via their scalar-field chunk, or (c) genuinely uncaptured — same investigation done for term (where they were mostly already in the dataset).
