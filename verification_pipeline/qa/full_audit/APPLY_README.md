# Applied corrections — proposed corrected dataset

Generated 2026-06-02 by `apply_corrections.py` (Phase A) + `apply_corrections_phaseB.py`
+ `apply_corrections_phaseB2.py`. These apply the full-audit's source-grounded
corrections onto a **copy** of `qa/corrected_dataset.csv`.

**The base `corrected_dataset.csv` and the raw extract were NOT modified.** All
output is new files under `qa/full_audit/`. Everything is reversible via the
changelog.

## Files

| File | What it is |
|---|---|
| `proposed_corrected_dataset.csv` | a copy of `corrected_dataset.csv` with **888 cells corrected**. Review this; if approved, it can replace the base. |
| `apply_changelog.csv` | every cell action: `record_id, target_field, route, flagged_current, base_actual, new_value, status`. Fully reversible. |
| `needs_manual_placement.csv` | **76** corrections NOT auto-applied — they need a human (see below). |

## What was applied (888 cells)

- 428 clean numeric value fixes · 247 enum fixes · 76 boolean fixes · 3 date · 3 freetext
- **95 value+unit splits** — a suggestion like `"16.0 weeks"` was split to value=`16.0`
  **and** unit=`weeks`, so both cells are placed correctly (this is the value↔unit
  pairing).
- **33 paired-unit completions** — where a value was corrected (e.g. `564.85→3.5`)
  its partner unit was also fixed (`EUR/year → % of annual income`) so the pair
  stays consistent.

## Apply rule (per cell)

`base == new` → already correct (skip) · `base == flagged_current` → APPLY ·
`base == anything else` → **conflict_skip** (a per-topic-pipeline edit already
changed it; left untouched — 11 such cells, see changelog).

## The 76 left for a human (`needs_manual_placement.csv`)

- **45 unit-dimension/timebase changes with no paired value correction** — e.g.
  a unit flagged `EUR → %` or `per week → per year`. Changing the unit alone would
  make the cell inconsistent, and the audit had no verified value to pair it with,
  so the **value also needs review**. Do not apply the unit alone.
- **31 ambiguous multi-value suggestions** — tiered/alternative source values
  (e.g. `"100% for week 1; 70% for weeks 2-3"`, `"1 week or 2 days depending on
  reading"`). No single value can be placed automatically.

## NOT in this apply (still surface-only, by design)

The KEEP_NONSTANDARD_UNIT (269), UNSUPPORTED, RELOCATE, and unverified/dropout
rows from the handoff are **not** applied — they need human judgment or are valid
as-is. See `handoff/` and `unit_semantics_reconciliation.csv`.
