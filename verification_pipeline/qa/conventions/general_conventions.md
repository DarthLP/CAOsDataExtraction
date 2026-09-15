# General conventions — every subagent must follow these

1. SOURCE OF TRUTH
   - Verbatim quote from the supplied topic_section is the only valid evidence
     for confidence=high or medium.
   - If a value is not stated in source, csv_value_new=UNKNOWN, confidence=low,
     evidence_quote=(no relevant text in excerpt). No exceptions.

2. STATUTORY MINIMA ARE FLOORS
   - Dutch labour law sets minima. A CAO can grant MORE than statutory, never less.
   - If your reading produces a value BELOW the era-statutory baseline, treat
     as extraction error: verdict=clear or move (depending on context).
   - DO NOT classify "below statutory" as a real CAO deviation.

3. CONVENTION: STATUTORY RESTATEMENT ⇒ EMPTY VALUES
   - When a CAO simply restates the statutory baseline, the schema convention
     is: statutory_ref=True, exceptions=False, all sub-fields empty.
   - Era-baselines live in qa/shared/era_baselines.py.

4. NEVER EXTRAPOLATE ACROSS CAOs
   - If source doesn't cover the specific record, verdict=unable_to_verify.
   - Do NOT infer based on CAO number, sibling records, or "the pattern".

5. ARTICLE NUMBERS / DATES ≠ VALUES
   - "Article 91" → 91 in a leave field = extraction error. Clear.
   - "2014" in a duration field = year reference. Clear.
   - Watch for "<number> found in source" — pure extractor garbage.

6. PAY-RATE FIELDS MUST HAVE % UNITS, DURATION FIELDS MUST HAVE TIME UNITS
   - leave_*_pay_value with unit=weeks → wrong field. Move or clear.
   - leave_*_value (duration) with unit=% → wrong field. Move or clear.

7. DECIMAL POINT STRIP
   - "0.43%" extracted as 43 in a pay-rate field = stripped decimal.
   - If pay-rate value > 100, suspect decimal-strip. Confirm or clear.

8. UNKNOWN ⇒ confidence=low (hard rule)
   - csv_value_new=UNKNOWN MUST be paired with confidence=low.

9. VERDICT VOCABULARY + FIX_METHOD VOCABULARY
   Verdicts — what the subagent emits per item:
     - confirm            : no change needed; CSV value is correct
     - clear              : value is garbage; csv_value_new=""
     - correct_in_place   : same field, fix value/unit
     - move               : value belongs in different field; supply target_field
     - set_boolean        : boolean field; new_value ∈ {True, False}
     - unable_to_verify   : source doesn't allow a verdict
   fix_method — aggregator assigns exactly one per row:
     - det+sub_agree      : both layers fired and produced the same result
     - det_only           : only deterministic produced a correction
     - sub_only           : only subagent produced a correction
     - det_sub_conflict   : both fired and disagreed without resolution
     - needs_human_review : audit-flagged or aggregator-routed for human

10. NO COSMETIC NOISE IN CORRECTIONS
    - 100.0 → 100 with no other change is a noop (filtered automatically).
    - "weeks" → "weeks" with leading space is normalized.

11. LAYERED REVIEW IS THE NORM
    - Deterministic rules and subagent review can both fire on the same
      (record_id, field). Subagent verdicts with verbatim source evidence
      override deterministic rules. Disagreements without source evidence
      route to human review, never silently to one side.

12. WORKSHEET MODES
    Every item has a worksheet_mode:
      - extract  : no prior verdict. CSV field is empty; extract if possible.
      - blind    : a deterministic rule produced a verdict but you do NOT
                   see it. Verify independently against source.
      - informed : a deterministic rule produced a verdict, included as
                   proposed_correction. Treat as a hypothesis to test
                   against source, NOT as a fact to rubber-stamp.
