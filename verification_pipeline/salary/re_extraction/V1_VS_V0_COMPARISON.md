# v0 vs v1 salary extraction — side-by-side comparison

**v0** = the production LLM-analysis JSONs (in `CAOsDataExtraction/outputs/llm_analysis/salary/`, backed up to `qa/qa_salary/re_extraction_backup/analysis_originals/`).  
**v1** = re-extracted by subagents using the patched prompt at `qa/qa_salary/re_extraction/prompts/extract_v1.md` (4 edits applied: ragged-row handling, axis labelling, adult inloop distinction, no empty timelines).

## Top-line

| Chunk | CAO | Defect | v0 → v1 assessment | v0 → v1 discrepancies | v0 → v1 row count |
|---|---|---|---|---:|---:|
| [b0007](v1_diffs/b0007.md) | 1287 | Column misalignment in jobgroups G-K (used 'Entry  | `major_issues` → `faithful` | 8 → **0** | 22 → 27 |
| [b0011](v1_diffs/b0011.md) | 1536 | Axis swap (jobgroup held Trede, step held Schaal) | `major_issues` → `faithful` | 5 → **0** | 245 → 222 |
| [b0014](v1_diffs/b0014.md) | 163 | Step-06 ambiguity (one row for two distinct step-0 | `major_issues` → `minor_issues` | 7 → **1** | 201 → 218 |
| [b0022](v1_diffs/b0022.md) | 234 | Trailing-column off-by-one on Period +1/+2 | `major_issues` → `faithful` | 5 → **0** | 256 → 219 |
| [b0048](v1_diffs/b0048.md) | 823 | Right-shortfall duplication (rightmost value copie | `minor_issues` → `faithful` | 6 → **1** | 81 → 73 |

## How to read the per-file diffs

The headline finding is the **`v0 → v1 assessment` column** above. For all 5 test files the assessment improved from `major_issues`/`minor_issues` to `faithful` (or `minor_issues` with one residual low-severity item). That is the ground truth.

The "Rows only in v0" / "Rows only in v1" counts in the per-file pages can look alarming on chunks where the fix involves a structural relabelling — for example:
- **b0011** axis-swap fix: v0 had `jobgroup="0"`, v1 has `jobgroup="P"` etc. → almost every row key differs even though the underlying numbers overlap heavily.
- **b0014** step-06 disambiguation: v0 collapsed two step-06 cases into one row each; v1 splits them via `age_group`. → every step-06 row becomes "new" because the key now carries an age band.
- **b0022** trailing-column fix: v0 used step labels `"Period +1"`, `"Period +2"`; v1 uses `"+1"`, `"+2"`. → every key differs.

So treat the per-file "only_v0/only_v1" counts as **structural change indicators**, not error counts. For the substantive question ("did the defects actually clear?") the verdict files (`v0` in `qa/qa_salary/phase3b/results/`, `v1` in `qa/qa_salary/re_extraction/v1_verify_results/`) are the source of truth.

## Per-file deep dives

Click each chunk in the table above for the row-level breakdown. **The cleanest examples** (where keys overlap naturally) are:
- **[b0007](v1_diffs/b0007.md)** — see the 5 same-key amount changes where v0 had the "Entry from" value mislabelled as "Minimum" and v1 has the true Minimum.
- **[b0048](v1_diffs/b0048.md)** — see the exact 8 fabricated v0 rows (`I/5..10` and `J/5..10` duplicates of the rightmost value) that v1 correctly omitted.

For **b0011, b0014, b0022** the cleanest way to see the improvement is to read the v0 vs v1 verdict files directly, not the row-level diff (since axis-swap or label rewrites change almost every key).

## Raw JSON for any pair

If you want to diff the full JSON yourself:
```bash
diff <(jq . qa/qa_salary/re_extraction_backup/analysis_originals/1287/'Grafimedia cao 2022-2024 1 april 2022_analysis.json') \
     <(jq . qa/qa_salary/re_extraction/v1_outputs/b0007_analysis.json) | less
```

## Patched prompt

The 4 edits are at `qa/qa_salary/re_extraction/prompts/extract_v1.md` — each marked **[EDIT-N]**.

If you want to apply them to your production Gemini pipeline, the files to edit are:
- `CAOsDataExtraction/schema/salary_schema.py` (SALARY_PROMPT)
- `CAOsDataExtraction/schema/salary_prompt_split.py` (ATTEMPT_9 + ATTEMPT_10)
- `CAOsDataExtraction/schema/salary_schema_compact.py` (SALARY_PROMPT_COMPACT)
- `CAOsDataExtraction/schema/salary_schema_super_compact.py` (SALARY_PROMPT_SUPER_COMPACT)

Backups of the originals are at `qa/qa_salary/re_extraction_backup/schema/`.