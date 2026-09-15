# Unit-kind mismatch verification — PRESERVATION-AWARE subagent instructions

You verify a special class of Dutch CAO data-quality flags: **unit-kind
mismatches**. A numeric field has a value whose UNIT family disagrees with the
field's intended kind — e.g. `leave_paid_maternity_value = 100` with unit
`% of salary`, but that field is by schema a DURATION ("weeks of fully paid
maternity leave"). The 100 is a *pay rate* sitting in a *duration* slot.

You are given ONE chunk file (JSON) with `items`; each item is ONE record's data
for ONE topic, with its OWN `full_source_text` and all its populated fields. The
flagged field(s) carry a `flag_reason` explaining the specific mismatch (the
detected unit family vs the field's canonical family).

TREAT EACH ITEM INDEPENDENTLY. Judge only against that item's own
`full_source_text` (and its own note fields, e.g. `*_note`). The source is
ENGLISH (translated from Dutch). Judge meaning, not surface form.

## THE GOLDEN RULE — NEVER DESTROY DATA, NEVER INVENT IT

The displaced value is real information. Your job is to preserve it and, where
the source explicitly supports it, recover the field-appropriate value. You must
do BOTH of the following for every flagged field:

1. **Preserve** — in `note`, always state what the current (displaced) value
   represents and where it belongs. NEVER recommend simply blanking a cell.
2. **Never invent** — only put a number in `suggested_value` if the source
   EXPLICITLY states it. Do NOT compute, convert, annualize, or infer (e.g. do
   NOT turn "10% of paid hours" into a days/year figure). If the field-appropriate
   value is not explicitly in the source, leave `suggested_value` empty.

## Decide a verdict for each flagged field

- **NEEDS_CHANGE** — the source EXPLICITLY states the field-appropriate value
  (the value in the field's intended kind, e.g. the maternity-leave DURATION in
  weeks). Set `suggested_value` to that source-stated value, give a verbatim
  `quote`, and in `note` record the displaced value you are replacing (e.g.
  "displaced pay-rate 100% — 'fully paid' implies 100%; duration 16 weeks
  recovered from source/note").

- **KEEP_NONSTANDARD_UNIT** — the current value IS supported by the source but is
  expressed in a unit that doesn't match this field's convention, AND the source
  does NOT state a value in the field's intended kind. This is a valid alternative
  encoding (e.g. vacation "accrues at 10% of paid hours" with no absolute
  days/year given). Leave `suggested_value` EMPTY. In `note`: "VALID value in
  non-standard unit (<orig value+unit>); source states no <intended-kind> value;
  PRESERVE as-is, human/schema to reconcile; do NOT delete or convert." Give a
  `quote` showing the source supports the value.

- **RELOCATE** — the displaced value belongs in a DIFFERENT existing field (look
  at the item's other fields; e.g. a partially-paid pay-rate belongs in
  `*_partially_paid_*_pay`). Leave `suggested_value` empty. In `note`: name the
  target field and the value to move, and the source quote. Do NOT move it
  yourself — surface only.

- **UNSUPPORTED** — the value is NOT found or derivable from the source at all
  (possible hallucination). `note` explains; `suggested_value` empty.

- **CONFIRM** — only if, on reading the source, the value is actually correct for
  this field as-is (i.e. the flag was a false positive). `note` says why.

`confidence` ∈ high | medium | low. `quote` MUST be copied verbatim from
`full_source_text` (<=240 chars); "" if you genuinely have none.

## Output

Write a JSONL file (one JSON object per line, UTF-8) to the EXACT path you are
given. Emit a line for EVERY flagged field (whatever the verdict). Each line:

  {"topic","record_id","field","verdict","suggested_value","quote",
   "confidence","was_flagged","note"}

- `verdict` ∈ NEEDS_CHANGE | KEEP_NONSTANDARD_UNIT | RELOCATE | UNSUPPORTED | CONFIRM
- `was_flagged` = true for these (they are all flagged cells).
- `note` is MANDATORY and must always preserve the original value's meaning.

Write ONLY the JSONL file. Do not print the verdicts back. Do not spawn subagents.
