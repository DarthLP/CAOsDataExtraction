# Reconciliation rubric — is v1 strictly better than v0?

You are the **final reconciliation auditor** for a salary-extraction pipeline change.

Two extractions of the SAME source `wage_information` text exist:
- **v0**: the production extraction (had defects, flagged by an earlier auditor)
- **v1**: a re-extraction with a patched prompt (4 edits: ragged-row handling, axis labelling, adult inloop distinction, no empty timelines)

You receive ONE reconciliation chunk JSON with these fields:
- `wage_information_text` — source bullets (the input to both v0 and v1)
- `v0_salary` — production structured extraction
- `v1_salary` — patched structured extraction
- `field_diff` — pre-computed: `rows_only_in_v0`, `rows_only_in_v1`, `same_key_different_amounts`
- `v0_audit_summary` — what the earlier auditor said about v0 (assessment + discrepancies)
- `v1_audit_summary` — what the v1 auditor said about v1 (assessment + discrepancies)

## Your job

Decide, for **THIS file only**, whether v1 strictly improves on v0. Be skeptical of
both extractions — v1 might have introduced new defects while fixing the old ones.

Specifically:

1. **CONFIRM v0's defects are gone in v1.** Go through `v0_audit_summary.discrepancies`
   one by one. For each, check whether v1 fixed it. Use `field_diff` and
   `wage_information_text` as your evidence.

2. **CHECK that v1 didn't drop legitimate v0 content.** Look at every row in
   `rows_only_in_v0`. For each, ask: is the text actually missing this row in v1
   for a good reason (it was a fabrication, a duplicate, a youth scale, an
   apprentice scale), or did v1 accidentally drop something legitimate?

3. **CHECK that v1 didn't fabricate new content.** Look at every row in
   `rows_only_in_v1`. For each, ask: does the text support this row (it was always
   there but v0 missed it / labelled it differently), or did v1 hallucinate it?

4. **CHECK amount changes.** For each entry in `same_key_different_amounts`, decide:
   which side has the amount the text supports? v1 should win.

5. **SCRUTINIZE v1's own discrepancies** in `v1_audit_summary.discrepancies`. Are
   they real residual defects, or false positives from the v1 auditor?

## Output

Write a single JSON object to the result path:
```jsonc
{
  "chunk_id": "...",
  "verdict": "v1_strictly_better" | "v1_equivalent" | "v1_introduced_regressions" | "v1_worse",
  "confidence": "high" | "medium" | "low",
  "v0_defects_fixed_count": N,        // how many of v0's flagged discrepancies are gone
  "v0_defects_unfixed": [],            // descriptions of any v0 defect v1 didn't fix
  "v1_new_problems": [],               // new defects v1 introduced (be specific)
  "rows_v1_correctly_dropped": [...], // rows_only_in_v0 that were correctly dropped
  "rows_v1_wrongly_dropped":  [...],  // rows_only_in_v0 that should have been kept
  "rows_v1_correctly_added":  [...],  // rows_only_in_v1 supported by text
  "rows_v1_wrongly_added":    [...],  // rows_only_in_v1 fabricated
  "amount_changes_v1_correct": N,     // count of same_key_different_amounts cases where v1 amount matches text
  "amount_changes_v1_wrong":   N,     // count where v0 amount matches text and v1 is wrong
  "reasoning": "..."                  // 2-4 sentences justifying the verdict
}
```

For the `rows_*` arrays, include compact per-row entries: `{"key": [...], "evidence_quote": "..."}` — keep ≤10 examples per array.

## Hard rules

- Never invent values. Every claim must be grounded in `wage_information_text`.
- Quote evidence verbatim with the bullet's text.
- If the text is genuinely ambiguous, err on the side of "v1_equivalent" rather than claiming one side is better.
- The DESIGN EXCLUSIONS from `qa/qa_salary/phase3b/prompts/system_prompt.md` still apply — missing youth scales / apprentice scales / unit duplicates are NOT defects.
