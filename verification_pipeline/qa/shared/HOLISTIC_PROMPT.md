# Holistic record verification instructions (pipeline Stage 4.6) — read fully

You are auditing an upstream extractor's output for a Dutch CAO (collective labour
agreement). For each record you get the COMPLETE source section and ALL of its
already-populated extracted fields. Your job: decide, **per field**, whether the
extracted value is actually correct/supported by the source. This catches *wrong
populated values* that the rest of the pipeline (which only re-checks empty or
changed fields) never re-examines.

## Input
Your chunk is JSONL, one object per line. Each is a RECORD:
- `topic`, `record_id`, `ingangsdatum`
- `extracted_fields` — a list of `{field, value}` (every non-empty `<topic>_*` field for this record)
- `full_source_text` — the COMPLETE source section (English-translated; retained Dutch like AOW/BW/Witteveen/trede). Ground truth.

## For EACH field, assign ONE disposition
- **CONFIRM** — the extracted value is correct and supported by the source.
- **NEEDS_CHANGE** — the value is wrong: source supports a different value, OR the value
  is a misread (e.g. a Generatiepact "100% accrual" stored as a DB accrual rate; an article
  number stored as a duration; a unit mismatch). Put the right answer in `corrected_value`.
- **UNSUPPORTED** — the source contains no basis for this value at all (possible fabrication
  or a value carried over from elsewhere). `corrected_value` = "" if it should be empty.

You do NOT need to flag missing fields (empty fields are handled elsewhere) — only judge the
populated values you're given. Skip nothing you're given.

## Rules
- Ground every judgment in `full_source_text`; NEVER invent a value not in it.
- Canonical unit/enum tokens need not be verbatim (normalization is fine) — judge meaning.
- Booleans: CONFIRM True only if the source explicitly supports it; flag over-asserted Trues.
- `evidence_quote` MUST be a verbatim substring of `full_source_text` (or "" if UNSUPPORTED).
- `reason` REQUIRED, one sentence.

## Output
Write JSON Lines to the output path in your task message — **one object per (record, field)**
you were given, keys:
`{"topic","record_id","field","disposition","current_value","corrected_value","reason","evidence_quote","confidence"}`
(`current_value` = the value you were given; `confidence` = high/medium/low.)

Process every field of every record in the chunk. Do NOT spawn subagents. Reply with a
one-line summary: counts by disposition + any NEEDS_CHANGE/UNSUPPORTED record_id:field.
