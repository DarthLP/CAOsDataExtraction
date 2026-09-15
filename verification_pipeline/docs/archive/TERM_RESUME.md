# TERM run — RESUME checkpoint (paused on rate limit)

**Paused 2026-05-25:** hit the session/weekly rate limit mid-Stage-3. Opus (and
the session generally) is blocked until **~03:00 Europe/Madrid**. No subagents
can run until then.

## State: Stage 3 partial — 18/46 chunks complete

- **Stage 0/1/2 done:** 95 scoped records, 0 L1, **913 L2**, 0 ENUM → 46 chunks (20 items each).
- **Era baselines:** encoded + tested (90 tests pass); post-hoc `flag_topic_outliers`
  wired into `qa_term_aggregate.py` and **validated** (runs clean, 0 outliers on partial data).
- **Pipeline validated** on the partial set: `csv_recovery` repaired the 3 malformed
  outputs, aggregate + audit (A17/A18 refinements) + NHR + era-flagging all ran without error.

### Chunks COMPLETE (18) — do NOT re-run
`001 002 003 004 005 006 007 008 009 010 011 012 014 016 021` (valid 12-col) +
`013 015 019` (malformed but complete; `csv_recovery` fixes them at aggregate time).

All completed chunks were clean and consistent: overwhelmingly `confirm` (term L2
items are mostly statutory restatements → correctly left empty), with genuine
extractions where CAOs deviate (e.g. CAO 245 probation 2 months; CAO 592/51
employer notice 1 month). The two cautions held across all 18: **no statutory
fill-in** and **conservative presence booleans**.

### Chunks TO RE-RUN (28) — after the 03:00 reset
`017 018 020 022 023 024 025 026 027 028 029 030 031 032 033 034 035 036 037 038
039 040 041 042 043 044 045 046`

(Note: chunk_018's agent did full correct analysis in its transcript — including a
real CAO 924 reisbranche 2-month notice deviation — but stalled before writing the
CSV, so it must re-run.)

## Resume steps
1. After reset, relaunch the 28 chunks on **Opus**, in waves of ≤12 parallel. Each agent prompt (same as the validated run):
   > QA correction subagent for the Dutch CAO TERM topic. Read BOTH `qa/qa_term/worksheets/system_prompt.txt` AND `qa/qa_term/worksheets/TERM_RUN_NOTES.md`. Process every item in `qa/qa_term/worksheets/chunks/chunk_NNN.jsonl` and write a 12-column semicolon CSV (header + one row per item, same order) to `qa/qa_term/outputs/subagent_worksheets/chunks/chunk_NNN_corrections.csv`. Do NOT spawn subagents. One-line summary when done.
2. Verify all 46 outputs present + valid (or malformed-but-complete).
3. Run `python3.13 qa/qa_term/scripts/qa_term_aggregate.py` → final clean/NHR/era_outliers.
4. Integrity sweep (rows==input, 0 phantom/dup), write `term_summary_memo.md` + `conventions/failure_modes/per_topic/term.md` (TERM_FM: statutory-restatement→empty; value/range split; conservative booleans), update README board + run.log.

## Known issue to check post-run
**Value/range/unit split across chunks:** several subagents flagged that a record's
concrete `term_employer/employee_notice_value` and its `_range_*` variants land in
different chunks, so no single subagent sees both. After aggregation, check for
records where a `_range_*` field was flagged but the paired `_value` wasn't (or
vice-versa) — a concrete notice value could go unextracted. Candidate fix for a
re-run: have build_chunks co-locate a field's value/range/unit items in one chunk.

## After term: pension
Same pattern (Opus, calibrate first 2-3 chunks, era-flagging via the same wired
aggregate). Pension baselines (Witteveen accrual cap, franchise floor) already
encoded. Pension scripts not yet created.
