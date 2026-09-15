# Salary anomaly verification — subagent task

You are verifying suspicious cells in a Dutch CAO (Collective Bargaining Agreement) salary CSV
against the LLM-extracted JSON source the CSV came from. **Your job is to read the source
JSON for each flagged cell and decide whether the CSV value is supported by the JSON.** You
are not optimising anything; you are reporting what the source says.

## Inputs (you read these)

You receive ONE chunk JSON file. Its structure:

```jsonc
{
  "chunk_id": "c0042s01",
  "cao_number": "1234",
  "file_name": "CAO_Foo_2022",
  "schema_kind": "standard | compact_parallel | compact_nested_tl",
  "n_entries_total": 462,                  // total entries in the source file
  "n_entries_included": 47,                // entries shipped in this chunk's slice
  "source_entries": [                      // normalized to a single schema for you
    {
      "_entry_index": 17,                  // ORIGINAL file index — use this to match flags
      "jobgroup": "5", "step": "3", "worker": "adult", "age_group": "22+",
      "ft_hours": 36.0, "permanency": null, "hours_type": null, "row_note": "...",
      "timeline": [
        { "start_date": "2022-01-01", "end_date": "2022-06-30",
          "amount": 2540.0, "unit": "monthly",
          "table_label": "Salarisschaal 5 — trede 3",
          "note": "incl. 8% vakantiegeld",
          "inc_pct": 2.0, "holiday_incl": true }
      ]
    },
    ...
  ],
  "flags": [
    {
      "flag_id": "c0042s01_f03",
      "row_id": "1234008",
      "entry_index": 17,                   // matches source_entries[*]._entry_index
      "salary_n": 2,                       // 1-based; entry.timeline[salary_n - 1]
      "field": "salary_2_amount",
      "csv_value": "25400.0",              // what the CSV currently has
      "rule": "amount_out_of_range",       // see "Rule cheat-sheet" below
      "csv_row_excerpt": {                 // the CSV row's surrounding context
        "jobgroup": "5", "step_label": "3", "worker_type": "adult",
        "salary_1": { "start_date": "2021-01-01", "amount": "2490", "unit": "monthly" },
        "salary_2": { "start_date": "2022-01-01", "amount": "25400", "unit": "monthly" },
        "salary_3": { "start_date": "2023-01-01", "amount": "2580", "unit": "monthly" }
      },
      "neighbor_entries": [16, 18]         // for table-context lookups in source_entries
    },
    ...
  ]
}
```

## Output (you write this)

Write a single JSON file to `qa/qa_salary/phase3/results/<chunk_id>.json` with
**one verdict per flag, in the same order as the input flags**:

```jsonc
{
  "chunk_id": "c0042s01",
  "verdicts": [
    {
      "flag_id": "c0042s01_f03",
      "verdict": "CORRECT_TO",                // or CONFIRM_CSV or UNDECIDABLE
      "json_supported_value": "2540.0",        // omit if CONFIRM_CSV or UNDECIDABLE
      "json_supported_unit": "monthly",        // only when the CSV unit also needs change
      "evidence_quote": "timeline[0].amount = 2540.0, unit 'monthly'",
      "confidence": "high",                    // high | medium | low
      "reasoning": "JSON entry 17 timeline[0] gives 2540.0 monthly; the CSV's 25400 is a 10x scale error (extra digit). Sibling year-over-year (salary_1=2490, salary_3=2580) corroborates the 2540 magnitude."
    },
    ...
  ]
}
```

## How to verify each flag

For each flag:

1. **Find the matching JSON entry.** Look in `source_entries` for the entry whose
   `_entry_index` equals the flag's `entry_index`. If `salary_n` is set, the
   relevant timeline row is `entry.timeline[salary_n - 1]`.

2. **Read what the JSON actually says** for the field in question. Note any text in
   `note`, `table_label`, or the entry's `row_note` — these often disambiguate.

3. **Compare the CSV value to what the JSON contains.** Use the `csv_row_excerpt`
   for sibling context (the same row's adjacent timeline blocks) and use the
   `neighbor_entries` indices in `source_entries` for table-context (e.g., the
   adjacent step in the same jobgroup).

4. **Pick a verdict:**

   - **`CONFIRM_CSV`** — the JSON entry's value matches the CSV value
     (allowing for numeric tolerance, unit normalization, casing). The flag is
     a false positive from the deterministic scan.
   - **`CORRECT_TO`** — the JSON contains a different value that the CSV value
     contradicts. Put the JSON's value into `json_supported_value`. **Only set
     this if a specific replacement value is unambiguously present in the JSON.**
     Never invent a value, never compute one.
   - **`UNDECIDABLE`** — the JSON does not contain enough information to confirm
     or refute. (E.g., the field is null in the JSON, the table_label/note are
     uninformative, the row's context is ambiguous.) **`UNDECIDABLE` is a fine
     verdict — use it whenever the JSON cannot adjudicate.**

5. **Quote evidence verbatim.** `evidence_quote` must be a short text snippet
   copied directly from the JSON (e.g., the value at `timeline[N].amount`, or a
   text fragment from `note` / `row_note` / `table_label`). Do not paraphrase.

## Rule cheat-sheet — what each rule means (no thresholds, no expected values)

- `amount_out_of_range` — the CSV amount looked unusually high or low for its
  unit. Verify what the JSON's `amount`/`unit` pair says, and check
  `table_label`/`note` for unit hints (e.g., "per 4-week period").
- `below_statutory_minimum` — the CSV monthly amount is unusually low for an
  adult full-time wage in the year inferred from `start_date`. Check the JSON
  entry's `worker`, `age_group`, `row_note`, and `timeline[n-1].note` — is this
  actually a youth row, a part-time row, or an allowance/supplement
  mislabelled as a base wage?
- `suspicious_placeholder` — the CSV amount equals a value frequently used as
  a placeholder (0, 1, 99, 100, 9999, …). Does the JSON's `amount` agree, or
  does it contain a real number?
- `timeline_drop` — within the same CSV row, the amount dropped by ≥30% from
  one consecutive timeline block to the next. Same unit on both sides. Verify
  against the JSON's timeline: is this a real cut, or a unit/scale error?
- `timeline_mismatch:unit` — the CSV's `salary_N_unit` disagrees with the JSON
  timeline entry's `unit`. Which one does the source (note/table_label) support?
- `noncanonical_unit` — the CSV's `salary_N_unit` is a string we don't recognise
  as canonical. What does the JSON say? Is it a legitimate non-canonical
  unit (e.g., per-event) or a transcription artefact?
- `ft_hours_out_of_range` — the row's `ft_hours` is outside the typical
  20–45 hours/week band. Does the JSON's `ft_hours` or `row_note` support it?
- `increase_pct_extreme` — `salary_N_increase_percent` is far from the typical
  CAO range. Compare to the actual `amount` delta in the timeline.
- `amount_without_unit` — the CSV amount is populated but the unit cell is blank.
  Does the JSON's timeline entry have a unit value?
- `step_monotonicity_violation` — within the same (jobgroup, worker_type), the
  CSV amount of this step is lower than a preceding step's amount in the same
  year. The `csv_value` is the current step's amount; `json_value` (in the
  worksheet field of the same name) shows `prev_step=…, prev_amount=…`. Check
  the JSON: does the source actually have a lower amount here than at a lower
  step (which would suggest a real CAO with a special rule), or is this an
  extraction error (missing digit, mislabelled jobgroup)? Note: the comparison
  is only fired between pure-numeric step labels.
- `ft_hours_inconsistent_in_file` — this row's `ft_hours` differs from the
  modal ft_hours of the file. The `json_value` field shows
  `file_modal=… (count/total)`. Check the JSON entry's `ft_hours` (or
  surrounding `row_note`): does the source legitimately have a different
  ft_hours for this row (e.g., part-time scale within a full-time CAO), or
  was the file modal value missed/dropped during extraction?

## Hard rules

- **Never invent a value.** If you propose a `CORRECT_TO`, the value must come
  verbatim from the JSON (one specific timeline entry's field). If no such
  value exists, the verdict is `UNDECIDABLE`.
- **Quote evidence.** Always paste the literal JSON text (≤ ~120 chars) that
  drove your verdict. If you cannot quote anything, use `UNDECIDABLE`.
- **Per-flag, one verdict.** Same `flag_id` order as the input. Do not skip flags.
- **Numeric tolerance.** Treat `2540.0` and `2540` and `2,540` as equal. Treat
  `monthly` and `month` and `m` as the same unit.
- **Confidence.**
  - `high` — JSON unambiguously supports the verdict.
  - `medium` — JSON supports it but with some interpretation needed (e.g., the
    value must be inferred from `note` or `table_label` rather than directly
    from the `amount` field).
  - `low` — JSON suggests the verdict but is partially ambiguous; a human
    second-look is warranted.

## Reminders

- You are not editing the CSV. You are issuing verdicts about specific cells.
- A flag being verifiable does NOT mean the CSV is wrong; many will be `CONFIRM_CSV`.
- A flag being a true mismatch does NOT always have a JSON-supported fix; many
  will be `UNDECIDABLE`.
- The scanner only **noticed** what was unusual; you are the one deciding what
  the JSON actually says about it.
