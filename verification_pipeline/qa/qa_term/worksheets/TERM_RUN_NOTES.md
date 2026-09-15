# TERM run — extra rules (read together with system_prompt.txt)

These override/sharpen the generic conventions for the term (termination) topic:

1. **No statutory fill-in.** Extract ONLY what the CAO source (`topic_section`)
   explicitly states. Do NOT supply statutory minima/maxima from your own
   knowledge of Dutch labour law (BW 7:672 notice schedule, proeftijd limits,
   transitievergoeding, AOW age). Statutory cross-checks happen separately
   downstream. If the source merely restates or references the statutory scheme
   (e.g. "conform artikel 7:672 BW", "wettelijke opzegtermijn", "volgens de
   wet"), treat it as a statutory restatement and leave the field empty
   (verdict=confirm if already empty, else clear) — never copy in the statutory
   number.

2. **Be conservative with presence/boolean fields.** For `term_has_termination_rules`,
   `term_notice_tenure_present`, `term_probation_allowed`, `term_end_at_AOW_auto`,
   `term_hetero_present`, `term_shorten_notice_uwv`, `term_sick_dismissal_prot`,
   `term_severance_ww_supplement`: set True only on explicit, specific evidence.
   Never assert True on a vague or tangential mention. When unsure prefer
   confirm / unable_to_verify.

3. **evidence_quote** must be a verbatim substring of the item's `topic_section`
   AND contain the proposed value/unit. If you cannot find such a quote, use
   verdict=unable_to_verify, confidence=low.

4. **CSV hygiene:** double-quote any free-text field (especially `notes` and
   `evidence_quote`) that contains a semicolon, so every row stays exactly 12
   columns.

5. Pass through `topic_section_was_truncated` and `value_not_in_source` from the
   input item unchanged.
