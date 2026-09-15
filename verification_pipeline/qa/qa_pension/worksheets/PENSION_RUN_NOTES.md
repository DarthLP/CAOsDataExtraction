# PENSION run — extra rules (read together with system_prompt.txt)

These override/sharpen the generic conventions for the pension topic:

1. **No statutory fill-in.** Extract ONLY what the CAO source (`topic_section`)
   explicitly states. Do NOT supply statutory figures from your own knowledge —
   the Witteveen maximum accrual rate (e.g. 1.875%), the AOW-franchise minimum,
   the AOW age, or the pensioenrichtleeftijd (67/68). Those statutory checks are
   done separately downstream. If the source merely restates/references the
   statutory or fund scheme without its own number (e.g. "as per the pension
   regulations of bpfBOUW/ABP/PFZW", "conform de pensioenregeling", "the fund
   determines the premium"), leave the field empty (verdict=confirm if already
   empty, else clear) — never copy in a number you know from elsewhere.

2. **Be conservative with presence/boolean fields.** For `pension_has_pension_scheme`,
   `pension_mandatory_participation`, `pension_excedent_present`,
   `pension_premium_eq_split`, `pension_hetero_pension`, `pension_accrual_stat_leaves`,
   `pension_accrual_illness_y2`: set True only on explicit, specific evidence.
   `premium_eq_split=True` requires the source to actually state a 50/50
   employer/employee split — not just "the employer contributes". When unsure
   prefer confirm / unable_to_verify.

3. **WTP transition (Wet toekomst pensioenen, in force 1 Jul 2023).** Many schemes
   are converting from DB (accrual %) to flat-premium DC through 2028. So a
   MISSING or unusual `pension_accrual_rate_value` on a post-2023 record can be
   entirely normal (the scheme has no accrual rate) — do NOT invent one. Capture
   the accrual rate only if the source states a concrete DB accrual %.

4. **Distinguish concepts:** the employee contribution (`employee_contrib`) is the
   employee's share; `premium_total` is the combined premium — don't conflate.
   `franchise` is the AOW-offset euro amount (salary slice not pensioned), NOT a
   franchise-business concept and NOT the threshold/drempel for participation.

5. **evidence_quote** must be a verbatim substring of the item's `topic_section`
   AND contain the proposed value/unit; otherwise verdict=unable_to_verify,
   confidence=low (do not fabricate a digit, e.g. for spelled-out percentages).

6. **CSV hygiene:** double-quote any free-text field containing a semicolon so every
   row stays exactly 12 columns. Pass through `topic_section_was_truncated` and
   `value_not_in_source` unchanged.
