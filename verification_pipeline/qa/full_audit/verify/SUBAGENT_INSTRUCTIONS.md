# Full-audit verification — subagent instructions

You verify extracted Collective-Bargaining-Agreement (CAO) field values against
source text. You are given ONE chunk file (JSON). It has `items`; each item is
ONE record's data for ONE topic, with its OWN `full_source_text`.

TREAT EACH ITEM INDEPENDENTLY. Judge an item's fields ONLY against that same
item's `full_source_text`. NEVER use one item's source to judge another's.

The source text is ENGLISH (the upstream extractor translated the Dutch CAO).
Judge meaning, not surface form: "EUR 1.500 per maand" supports value 1500 with
unit "EUR per month"; numbers may be written in words; dates may be phrased.

For EACH field in an item, assign a verdict:
  - CONFIRM      : the source clearly supports the extracted value.
  - NEEDS_CHANGE : the source clearly states a DIFFERENT value for this field.
                   Provide `suggested_value` (the source's value) + a verbatim
                   `quote` copied EXACTLY from full_source_text.
  - UNSUPPORTED  : the value cannot be found or derived from the source (possible
                   hallucination, or the source is silent). `quote` "" unless a
                   nearby passage explains the call.

BOOLEAN fields (value is True or False — e.g. *_present, *_exists, *_applies):
  A False/blank boolean means "this provision is NOT present in this CAO".
  Absence of evidence supports a negative, so:
  - False / blank when the source does not establish the provision: CONFIRM.
  - False but the source CLEARLY describes the provision existing: NEEDS_CHANGE
    (suggest True) with a verbatim quote.
  - True and the source describes the provision: CONFIRM.
  - True but the source is silent / contradicts it: UNSUPPORTED (a positive
    claim needs support). Do NOT mark a False boolean UNSUPPORTED just because
    the source is silent — that is the expected default, so CONFIRM it.

BLANK / MISSING non-boolean values (the field's "value" is empty — usually a
"dropout" flag asking whether an extraction was MISSED):
  - Source contains a value that was left out → NEEDS_CHANGE (suggested_value +
    verbatim quote).
  - Source is silent on this field → a blank is correct: CONFIRM.

EMPTY / THIN SOURCE: some items have little or no real content (e.g. an empty
`[]` block). Then: blank fields and False booleans → CONFIRM; a populated
non-boolean value or a True boolean → UNSUPPORTED (nothing in the source backs
it).

Rules:
  - `quote` MUST be copied verbatim from full_source_text (no paraphrase). Keep
    it short (<=240 chars). Empty string if you have no supporting passage.
  - A field carries an attached `unit`; judge value AND unit together. If only
    the unit is wrong, that is NEEDS_CHANGE with the corrected unit in
    `suggested_value`.
  - Be conservative: only NEEDS_CHANGE when the source is explicit. If unsure
    between CONFIRM and UNSUPPORTED, prefer UNSUPPORTED and say why in `note`.
  - `confidence` ∈ high|medium|low.

OUTPUT — write a JSONL file (one JSON object per line, UTF-8) to the EXACT path
you are given. Emit a line for:
  - EVERY field with "flagged": true  (whatever the verdict — including CONFIRM),
  - any other field ONLY when its verdict is NEEDS_CHANGE or UNSUPPORTED.
Do NOT emit lines for non-flagged CONFIRM fields (keeps output focused).

Each line MUST have these keys:
  {"topic","record_id","field","verdict","suggested_value","quote",
    "confidence","was_flagged","note"}
verdict ∈ ('CONFIRM', 'NEEDS_CHANGE', 'UNSUPPORTED'). was_flagged = the field's "flagged" boolean.

Write ONLY the JSONL file. Do not print the verdicts back.