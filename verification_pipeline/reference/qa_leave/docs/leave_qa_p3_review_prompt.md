# P3 hetero_present Review Subagent Prompt

**Version:** v1 (2026-04-29)
**Used by:** subagents reviewing the 160 records flagged by pattern P3 (hetero_present=True overcalled)

You answer ONE binary question per record: *"Does the source describe distinct worker GROUPS with different leave/seniority terms beyond age and full-time-vs-part-time?"*

## What counts as a worker group

A worker group is a category of employees defined by something OTHER than age or hours-fraction:

- **Contract type**: uitzendbeding vs no uitzendbeding, fixed-term vs indefinite, permanent vs payroll, vacation/holiday workers, on-call, min-max
- **Function / role**: bouwplaats vs UTA, office vs production, drivers vs warehouse, teachers vs OOP, technical vs administrative
- **Sector subgroup**: hotel vs restaurant within hospitality, money/value logistics vs event security
- **Location**: foreign workers, expats
- **Other**: any explicit "for X workers..." vs "for Y workers..." split with different leave/seniority

## What does NOT count as a worker group

- Age cohorts: "employees aged 55+", "young employees under 19" → these belong in `extra_seniority`, not `hetero_present`
- Full-time vs part-time: pro-rata accrual is universal, not heterogeneity
- Pre/post effective-date cohorts of the SAME worker type (transitional rules)
- Tenure cohorts within the SAME worker type

## Input

Each line in the worksheet JSONL is one record:

```
{
  "item_id": "p3_review_NNNNN",
  "record_id": "...",
  "cao_number": "...",
  "file_name": "...",
  "ingangsdatum": "...",
  "current_csv_value": "True",
  "deterministic_proposed": "False",
  "task": "...",
  "targeted_source": "concatenation of general + vacation_holidays + sick + seniority_special sections from p3 markdown"
}
```

Read `targeted_source` carefully. Look for any worker-group split as defined above.

## Output

CSV, semicolon-separated, UTF-8. Header:

```
item_id;record_id;has_worker_group;evidence_quote;confidence;notes
```

- `item_id`, `record_id`: copy verbatim
- `has_worker_group`: `yes` or `no`
  - `yes` = source has a real worker-group split → DO NOT flip hetero_present (keep True)
  - `no` = source has only age/part-time cohorts → apply the deterministic flip (False)
- `evidence_quote`:
  - if `yes`: a verbatim quote from `targeted_source` showing the group split (≤ 200 chars)
  - if `no`: `(no worker groups found; only age/part-time cohorts)`
- `confidence`: `high` (clear signal in source), `medium` (some signal but ambiguous), `low` (source unclear; default to deterministic proposal)
- `notes`: optional, ≤ 100 chars

## Hard rules

1. One row per input item. No skipping.
2. Use Python's csv module. Quote fields with semicolons properly.
3. If `has_worker_group=yes`, `evidence_quote` MUST be a verbatim source quote (not a placeholder).
4. If you can't tell from the source, use `confidence=low` with `has_worker_group=no` (defer to Python's deterministic proposal).
