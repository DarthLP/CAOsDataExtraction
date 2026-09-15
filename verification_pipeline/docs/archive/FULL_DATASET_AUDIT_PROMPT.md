# Task: Full-dataset internal QA audit (all 2,739 records) — outlier & mistake detection

## Read first (orient before coding)
- `CLAUDE.md` — hard rules, scope, conventions
- `qa/PLAN.md` and `qa/conventions/general_conventions.md` — architecture & rules
- `qa/CORRECTIONS_SCHEMA.md` — existing flag/correction schema
- `qa/README.md` — status board (what's already been done; don't redo it)

**Why this task exists:** the existing pipeline source-verified only the *latest version per CAO* for the 95 curated CAOs — 303 distinct records across 95 CAOs. The other ~2,436 records (older versions of those CAOs, plus all 147 out-of-scope CAOs) have never been checked. This task adds a **broad, dataset-internal audit across ALL records and ALL fields** to surface likely mistakes and outliers. It deliberately does **not** require source text — full source re-verification is out of scope for this run.

## Scope
- Input: `inputs/extracted_data_non_salary.csv` (semicolon-delimited; 2,739 records, 242 CAOs, 317 columns). **READ-ONLY.**
- Cover **every record and every field**. Do **not** apply the `scope_filter` one-per-CAO reduction.

## Hard guardrails (do not violate)
- NEVER modify `inputs/`, `qa_leave/`, `corrected_dataset.csv`, or any existing `corrections.csv`.
- This is a **surfacing** task: produce FLAGS for human review. Never auto-fix, never write values into the dataset, never invent values. If a field isn't supported by data you have, leave it and say so.
- The verification stage WILL READ raw extracts under `CAOsDataExtraction/outputs/llm_extracted/new_flow/<cao>/` for the 147 out-of-scope CAOs — reading is fine. **Ask before WRITING anything to `CAOsDataExtraction/`.**
- Reuse existing shared code: `qa/shared/resilient_csv.py` (I/O), `qa/shared/schema_lookup.py` (field/enum definitions), `qa/shared/value_variants.py` (numeric/unit normalization), `qa/shared/era_baselines.py` (statutory floors/caps), `qa/shared/holistic_verify.py` (per-record/topic full-section verification — the verification stage), `qa/shared/source_text_loader.py`, `qa/shared/worksheet_builder.py`, `qa/shared/subagent_runner.py`. Run scripts with `python3.13`. Semicolon (`;`) delimiter throughout.
- If you parallelize with subagents, MAX_PARALLEL_HARD_CAP = 12.

## Checks to implement (across all fields)
Create a new module `qa/full_audit/` with one script per check, plus a combiner. Each flag is a *candidate for review*, not a correction.

### 1. Statistical outliers (numeric fields)
- Normalize value+unit first (use `value_variants`); compare like-with-like.
- Build a robust distribution **per field**, and also per `(field × topic)` and, where meaningful, per `(field × era/year-band)` — norms differ by sector and over time.
- Flag values outside robust fences: MAD-based modified z-score |z| > 3.5, and/or outside `[Q1 − 3·IQR, Q3 + 3·IQR]`. Record value, field stats (median, IQR, n), score, direction.
- Also flag scale-error signatures (value ≈ 100× or 1000× the field median → likely decimal/unit slip), impossible negatives, and zeros where a value is expected.
- Reuse `era_baselines.check_outside` to flag statutory floor/cap breaches — now run it across **all** records, not just the scoped ones.

### 2. Cross-version consistency (same CAO across versions)
- Group by `cao_number`, sort by `ingangsdatum`; compare consecutive versions field-by-field.
- Flag: order-of-magnitude or >50% numeric jumps; a populated value going empty then re-populated (drop-out); boolean flips; enum changes; unit changes on the same field.
- CAOs legitimately get renegotiated, so **prioritize implausible/structural changes** (type changes, unit changes, magnitude jumps) over normal drift. For each flag, output the full version timeline of that `(cao, field)` and label likely-error vs plausible-renegotiation.

### 3. Enum & format validity (all fields)
- Enum fields: validate every value against the allowed set from `schema_lookup`/schema; flag non-canonical values.
- Format: malformed numbers (stray text, wrong decimal/thousands separators), wrong-type values (boolean text in a numeric field, number in a boolean field), value-without-unit / unit-without-value, units outside the field's allowed set.
- Dates: impossible dates, expiry before start, implausible signing vs start.
- Cross-field: `range_min > range_max`; a `_present`/master flag False while detail fields are populated (and vice versa).

### 4. Value/unit cross-contamination & swaps (paired value/unit fields)
For every `_value`↔`_unit` (and `range_min`/`range_max`↔`_unit`) pair:
- Flag a value/numeric field whose content carries **unit-like text** ("per month", "per hour", "EUR", "€", "%", "days", "hours", and Dutch equivalents "per maand", "uur", "dagen", …) instead of, or on top of, a bare number.
- Flag a `_unit` field that holds a **numeric value** ("1500", "12,5") instead of a unit token.
- Flag a number and its unit **mashed into one cell** (e.g. value = "€1.500 per maand").
- Flag value/unit **swaps** (the number sitting in the unit column and the unit in the value column).
Recognise unit tokens via `value_variants` and the unit vocabulary in `schema_lookup`; support **Dutch and English** unit words, since some source survives untranslated.

## Verification stage — ONE subagent per (record, topic): whole section, all fields at once
Do **not** verify flags one cell at a time. **Group the flags by `(topic, record_id)` and run a single subagent per group that reads the COMPLETE topic section once and judges every field together.** The section text dominates the context, so checking all of a section's fields costs essentially the same as checking one — this collapses the subagent call count (critical for the weekly rate limit) and lets the model reason across fields in the same section.
- **Reuse `qa/shared/holistic_verify.py`** — this already *is* exactly this stage (pipeline Stage 4.6): per record it sends ALL populated `<topic>_*` fields + the full source section to one subagent → `CONFIRM` / `NEEDS_CHANGE` / `UNSUPPORTED` per field, surfacing only, never auto-applied. Invoke it on the flagged record subset per topic: `python3.13 -m qa.shared.holistic_verify build <topic> <rid1,rid2,…>` then `… consolidate <topic>`.
- **Verify the whole flagged section, not just the flagged cells.** For any `(record, topic)` with ≥1 flag you're already loading the full section, so check *all* populated fields of that section in the same pass — marginal cost ≈ 0, and it catches wrong-but-unflagged populated values (the gap holistic_verify exists to close). This does **not** grow the call count, which is driven by the number of flagged sections, not fields-per-section.
- **Source per record:** English `by_topic` section for the 95 in-scope CAOs (via `source_text_loader`); for the other 147 CAOs, the full topic content from `CAOsDataExtraction/outputs/llm_extracted/new_flow/<cao>/`. No usable source → mark `unverified_no_source` and leave for human review.
- Require a verdict + **verbatim supporting quote** per field. Never auto-apply; never invent a value not in the text. Respect `worksheet_builder` budgets, `MAX_PARALLEL_HARD_CAP = 12`, and model defaults (Opus for term & pension); chunk by cumulative-char budget (a few records per chunk) — no per-item truncation.
- Merge verdicts back onto `full_audit_flags.csv`; **additionally surface any `NEEDS_CHANGE`/`UNSUPPORTED` verdicts that land on non-flagged fields** as new findings.
- (`qa/shared/full_text_recheck.py`, Stage 4.5, is the narrower variant that re-reads only *changed* cells — use holistic_verify here since we want the whole section's fields.)

## Output (everything in `qa/full_audit/`)
- One CSV per check: `outliers_numeric.csv`, `cross_version.csv`, `enum_format.csv`, `value_unit_contamination.csv`.
- Combined `full_audit_flags.csv` with schema: `record_id; cao_number; file_name; ingangsdatum; field; value; unit; check; severity; reason; stat_context; verify_verdict; verify_quote; verify_suggested_value; suggested_review`. Dedup overlapping flags; sort by severity, then by flag-count per record.
- `full_audit_summary.md`: counts per check / per topic / per field; **verification-verdict mix (CONFIRM / NEEDS_CHANGE / UNSUPPORTED / unverified_no_source)**; **new findings** (NEEDS_CHANGE/UNSUPPORTED on non-flagged fields surfaced by the whole-section pass); the top ~50 highest-priority likely errors (one-line reason + verdict each); a coverage note (records / CAOs / fields covered, how many flagged sections had source available, and the subagent call count); and a list of fields no check applied to.
- Append a line to `qa/full_audit/run.log`.

## Process
1. Read the docs above; confirm dataset shape (2,739 × 317) and pull the field/enum inventory from `schema_lookup` **before** coding.
2. Implement the four checks as small, testable scripts; add a couple of unit tests each, mirroring the repo's test convention.
3. Run them on the full dataset to produce the raw flag set.
4. **Double-check:** group flags by `(topic, record_id)` and run holistic_verify on the flagged record subsets — one subagent per record/topic, full section, all populated fields at once. Attach each verdict + quote; surface new findings on non-flagged fields.
5. **Verify before reporting:** spot-check ~15 flagged cells by eye against the raw record and the source; report each check's flag rate and the verdict mix (guard against a false-positive flood); confirm nothing was written outside `qa/full_audit/` (and nothing written to `CAOsDataExtraction/`).

## Deliverable
A prioritized, deduplicated `full_audit_flags.csv` — each flag carrying its full-topic **verification verdict + supporting quote** — plus `full_audit_summary.md`, for me to review. Surface only — change no data. Reading `CAOsDataExtraction/` extracts is expected; **stop and ask before writing anything into the dataset or into `CAOsDataExtraction/`.**
