# Per-file conflict verification — subagent instructions

You verify ONE agreement's conflicting fields for ONE topic by re-reading EACH
version-record's OWN source file INDEPENDENTLY, so we can tell an extraction error
from a real over-time change. You will be told the path of a unit JSON and a result
path.

## Input (the unit JSON)
- `fields`: the fields to verify. Each has:
  - `field` — the dataset column name.
  - `kind` — `numeric` | `enum` | `boolean`.
  - `schema_def` — the EXACT definition of what this field must capture. THIS IS YOUR
    GROUND TRUTH for what to look for. Read it carefully before judging.
  - `enum_values` — if non-null, the ONLY allowed answers.
- `records`: one per version-record/file of the SAME agreement (same cao + same
  ingangsdatum). Each has `record_id`, `filing_date`, `doc_type`, `ttw`, and
  `source_block` = that file's FULL topic source text (English-translated from Dutch).

## Task
For EACH record and EACH field, read ONLY that record's own `source_block` and decide
what THAT file explicitly states for the field. Treat each file independently — do NOT
assume the files agree, and do NOT carry a value from one file to another.

## Rules (critical — this is a no-invent research dataset)
1. **Use `schema_def`** to understand precisely what the field means and what evidence
   counts. If `enum_values` is given, the answer must be one of them.
2. **NEVER invent.** Only report a value the file states EXPLICITLY (a number, an enum
   token, or a clear True/False).
3. **NO STATUTORY / NO SILENT FILL.** If the file is silent, or only says
   "statutory"/"by law"/"wettelijk" without an explicit figure, set `"stated": false`.
   Do NOT convert, annualize, or infer (e.g. do not turn "1 day per week" into an
   annual number).
4. **Record value and unit exactly as the source expresses them** (e.g. value "2.01",
   unit "% of base wage"; value "700", unit "EUR one-off"; value "5", unit "days").
   For booleans `"value"` is "True"/"False" and `"unit"` is "". For enums `"value"` is
   the enum token and `"unit"` is "".
5. **Unit-equivalence — IMPORTANT.** Two files that express the SAME quantity in
   DIFFERENT units are NOT in conflict. Normalize before judging:
   `150 "% of hourly"` == `1.5 "× hourly"`; `1 "week"` == `5 "working days"`;
   `12 "months"` == `1 "year"`. If, after converting to a common unit, the files match,
   the assessment is CONSISTENT. Capture each file's value in its own source's terms,
   but say so in the explanation.
6. **Booleans / thin re-filings.** Set True only if the file's text explicitly contains
   the described provision. A thin interim re-filing (often `ttw="yes"` or a
   `full_cao_update`) that simply does NOT restate a section is False FOR THAT FILE — but
   if a fuller sibling clearly has the provision, the AGREEMENT has it (see SILENT_SIBLINGS).
7. `"quote"`: a verbatim snippet (<=200 chars) from that file supporting your reading,
   or "" if silent.
8. **CAO-specific vs statutory / multi-component values — IMPORTANT.** Many provisions
   have several components: a CAO grants 2 paternity days AND statutory WAZ adds 3; or
   vacation = 144 statutory hours + 21.8 ABOVE-statutory hours. **Do NOT silently SUM
   these into the headline `value`.** Put in `value` the single figure the source most
   directly states for THIS field (normally the CAO's own / base figure — e.g. paternity
   `2`, vacation `144`), and DESCRIBE the other component(s) — the separately-stated
   statutory add-on or the above-statutory enhancement — in `explanation` (and you may
   note it in `unit`). Only report a summed TOTAL when the field's `schema_def`
   EXPLICITLY asks for the total entitlement. This keeps `value` comparable to how the
   field was originally extracted and avoids inflating it with separately-stated
   statutory minimums (project policy: never fold in the statutory baseline).

## Then classify each field (compare ONLY what you extracted)
- `CONSISTENT` — all files that state it agree (after unit-normalization).
- `EXTRACTION_ERROR_SUSPECTED` — stating files disagree in a way that looks like a
  misread or a unit/scale mix-up (e.g. one is an absolute EUR amount, another a
  percentage of the same thing; or a value contradicts the file's own text).
- `TEMPORAL_CHANGE` — files state genuinely different values consistent with a real
  one-way change across `filing_date` (e.g. a raise written into a later file).
- `SILENT_SIBLINGS` — only some files state it; the others are genuinely silent (thin
  re-filings). Those silent ones should be empty/False, NOT a different number; the
  agreement-level value is what the full document(s) state.
- `UNSURE` — the text cannot settle it.

Set `agreement_level_value`/`agreement_level_unit` to the single correct value for the
agreement (leave "" for TEMPORAL_CHANGE or UNSURE).

## Output
Write your result JSON to the given result path (use the Write tool), AND make your
final message ONLY that same JSON (one fenced ```json block, no prose). Exact shape:

```json
{
 "agreement_id": "...", "topic": "...",
 "per_file": [
   {"record_id":"...","field":"...","stated":true,"value":"...","unit":"...","quote":"..."}
 ],
 "per_field": [
   {"field":"...","assessment":"CONSISTENT|EXTRACTION_ERROR_SUSPECTED|TEMPORAL_CHANGE|SILENT_SIBLINGS|UNSURE",
    "explanation":"...","agreement_level_value":"","agreement_level_unit":""}
 ]
}
```
