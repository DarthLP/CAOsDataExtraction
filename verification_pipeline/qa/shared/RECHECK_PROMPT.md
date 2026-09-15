# Full-text re-check instructions (pipeline Stage 4.5) — read fully

You are INDEPENDENTLY RE-VERIFYING accepted data corrections for a Dutch CAO
(collective labour agreement) extraction project, against the COMPLETE source
section (Stage-3 subagents saw only a truncated slice — you have the whole thing).

## Input
Your chunk is JSONL (one object per line). Each item:
- `topic`, `record_id`, `field`
- `csv_value_old` / `csv_unit_old` — value in the dataset BEFORE the correction
- `applied_correction` = `{verdict, csv_value_new, csv_unit_new, evidence_quote, confidence, notes}` — **`csv_value_new` is the value being applied; that is what you check.**
- `full_source_text` — the COMPLETE source section (English-translated, retained Dutch like AOW/BW/Witteveen/trede). This is ground truth.

## Per item, assign ONE disposition
- **CONFIRM** — the applied value+unit is correct and supported by the full source.
- **NEEDS_CHANGE** — it's wrong: the source supports a different value/unit, or the field should be empty, or a boolean was flipped the wrong way. Put the right answer in `corrected_value`/`corrected_unit` (empty string if it should be blank).
- **ESCALATE** — genuine ambiguity / judgment call (conflicting source, no canonical mapping, field-scope unclear).

## Rules
- Ground everything in `full_source_text`; NEVER invent a value not in it.
- A canonical unit/enum token need not be a verbatim substring (normalization is fine); judge whether it correctly represents the source.
- For numbers, check the value AND its basis (surcharge vs total; per-day vs per-week; gross vs net; a statutory restatement that should be empty).
- `evidence_quote` MUST be a verbatim substring of `full_source_text` (or empty if genuinely absent).
- `verification_reason` REQUIRED, one sentence: why right (CONFIRM), what's wrong+fix (NEEDS_CHANGE), or the specific decision needed (ESCALATE).

## Output
Write JSON Lines to the output path in your task message — one object per input item, SAME order, keys:
`{"topic","record_id","field","disposition","corrected_value","corrected_unit","verification_reason","evidence_quote","confidence","applied_value","applied_verdict"}`
(`confidence`=your high/medium/low; `applied_value`/`applied_verdict`= copy the input's `csv_value_new`/`verdict`.)

Process ALL items, keep order. Do NOT spawn subagents. Reply with a one-line disposition summary + any NEEDS_CHANGE record_ids.
