# Phase 3b — verifying step-2 extraction (text → Pydantic schema)

You are auditing **one pipeline step**:

```
wage_information_text  ──[step 2: LLM with Pydantic schema]──►  structured_salary
```

The `wage_information_text` is the LLM's first-pass extraction from the CAO
PDF (a list of bullet points describing the wage system). The
`structured_salary` is what came out of step 2 — a structured array of
salary scale entries, each with timelines of `{start_date, amount, unit, …}`.

**Your job: does the structured_salary faithfully encode what the text describes,
RESPECTING the step-2 extractor's design exclusions?**

You are NOT checking whether the text itself is correct (we'd need the PDF for
that). You are checking whether step 2 faithfully translated the text into
the structured table — but only for content that step 2 was actually asked to
extract.

## DESIGN EXCLUSIONS — DO NOT flag these as defects

The step-2 extraction prompt explicitly excludes the following. If the text
mentions something in this list and the structured data omits it, that is
**INTENTIONAL behavior, not a defect** — do NOT record it as a discrepancy.

1. **Youth / under-23 age scales.** The extractor's rule:
   > "Create distinct SalaryRow objects for each adult-eligible age band:
   > open-ended adult bands (e.g., '22+', '21 and older'), OR bands that
   > intersect ages 23-65. IGNORE age and job groups limited to workers
   > under 23 (e.g., '16-20', '20') unless the group is open-ended ('20+')
   > or spans older ages ('18-65')."
   
   So missing rows for ages 15, 16, 17, 18, 19, 20, 21, 22 — even with
   amounts explicitly tabulated in the text — are CORRECT. Do not flag.

2. **Non-standard worker categories.** The extractor's rule:
   > "EXCLUDE … non-standard worker roles like apprentices, interns,
   > trainees, or foremen."
   
   So missing tables for: apprentices, BBL/BOL students, leerlingen,
   stagiaires, interns, trainees, foremen / voorlieden, entry/learning
   scales (aanloopschalen, leerperiode, instaptredes, inloopschalen) tied
   to apprentice status. Do not flag. (Caveat: aanloopschalen/inloop tied
   to ADULTS — not students — DO belong in structured. Use context.)

3. **Allowances, bonuses, overtime, irregular hours, reimbursements.**
   So missing rows for: holiday allowance (vakantietoeslag), end-of-year
   bonus, one-off payments (eenmalige uitkering), shift allowances,
   overtime, BHV/EHBO allowance, jubilee gratuities, meal vouchers,
   travel reimbursement, profit sharing. Do not flag.

4. **Unit-conversion duplicates.** The extractor's rule:
   > "SKIP tables that are identical except for unit conversion (monthly
   > vs hourly vs weekly vs 4-week vs yearly); keep ONE version (prefer
   > monthly if present)."
   
   So missing hourly/weekly/4-week variants of an already-monthly scale
   are CORRECT. Do not flag.

5. **General narrative rules.** Progression rules, entry-placement rules,
   how-to-classify-functions descriptions, statutory minimum (WML)
   references that don't have their own tabulated amounts — these are
   prose, not scale rows. The extractor isn't asked to encode them. Do
   not flag.

6. **Inferred / derived values.** If the text describes a percentage
   formula (e.g. "youth wage = 50% of adult wage at age 16") but doesn't
   tabulate the resulting amounts, the extractor isn't asked to compute
   them. Do not flag.

## What you SHOULD still flag (real defects)

- **Missing adult scales / jobgroups** the text actually tabulates with
  amounts at age 23+ (or for open-ended adult bands). E.g., text shows
  10 jobgroups A-J at 23+, structured has only 8.
- **Missing effective dates** the text gives for the scales that ARE
  included. E.g., text says "Per 1 Jan 2024 amounts in Table B are…"
  but structured only has the 2023 amounts.
- **Wrong amounts / column misalignment.** E.g., extractor read the
  wrong column for a scale (off-by-one shift, swapped jobgroups, used
  "Entry from" column as "Minimum").
- **Wrong unit or `inc_pct`.** E.g., text says "Per 1 Jan increase 2.0%"
  and structured has `inc_pct: null` or `inc_pct: 1.0`.
- **Wrong axis encoding.** E.g., text uses "Scale | Trede" but
  structured swaps them (`jobgroup` holds trede, `step` holds scale).
- **Row duplication / fabrication.** E.g., the same (jobgroup, step) row
  appearing twice; or a value duplicated across two jobgroups when the
  text shows it only once.
- **Wrong dates.** E.g., text says "per 1 July" but structured has
  "01-06-…".

## Inputs (you read these)

A single chunk JSON file:

```jsonc
{
  "chunk_id": "b0028",
  "cao_number": "3618",
  "file_name": "...",
  "schema_kind": "standard | compact_parallel | compact_nested_tl",
  "n_entries": 80,
  "wage_information_text": [             // bullet-by-bullet
    "job classifications, function groups, ...",
    "Functiefamilie Primair: ...",
    "P.04 | Productiemedewerker | 3 | 3",
    "age-related or service-year/...: ...",
    "general wage increases: per 1 January 2025 ...",
    ...
  ],
  "structured_salary": [                  // normalized to standard schema
    { "jobgroup": "Schaal 1", "step": "Trede 0",
      "worker": null, "is_entry": null, "age_group": null,
      "ft_hours": 38.0,
      "timeline": [
        { "start_date": "2025-01-01", "amount": 2324.12, "unit": "monthly",
          "table_label": "per 1 January 2025", "note": null, ... },
        ...
      ] },
    ...
  ]
}
```

## Output (you write this)

Write a single JSON object to
`qa/qa_salary/phase3b/results/<chunk_id>.json`:

```jsonc
{
  "chunk_id": "b0028",
  "overall_assessment": "faithful | minor_issues | major_issues | not_verifiable",
  "confidence": "high | medium | low",
  "summary": "1-3 sentence summary of how well the structured data matches the text",

  "scale_structure": {
     "scales_in_text": ["Schaal 3", "Schaal 4", ...],      // jobgroups/scales the text mentions
     "scales_in_structured": ["Schaal 1", "Schaal 2", ...], // unique jobgroups in structured_salary
     "match": "exact | superset | subset | mismatch",
     "notes": "..."
  },

  "increase_dates": {
     "dates_in_text": ["2025-01-01", "2025-07-01", ...],   // increase/effective dates the text names
     "dates_in_structured": ["2025-01-01", ...],            // unique start_date values
     "match": "exact | superset | subset | mismatch",
     "notes": "..."
  },

  "discrepancies": [                       // empty if no issues
    {
       "type": "missing_scale | extra_scale | missing_date | wrong_unit | wrong_amount | missing_increase | unsupported_value | other",
       "severity": "high | medium | low",
       "text_says": "verbatim or paraphrased text claim",
       "structured_says": "what the structured_salary contains for the same thing",
       "evidence_quote": "verbatim text quote (≤ 200 chars)",
       "reasoning": "1-2 sentences"
    }
  ]
}
```

## Heuristics

- **faithful** = the structured data lines up with every concrete claim in the
  text (scales, jobgroups, dates, percentage increases, special features). Some
  silence in the text on specific values is OK — those are not discrepancies.
- **minor_issues** = small mismatches like one missing date, a label
  discrepancy, an off-by-one count.
- **major_issues** = the structured data misses or contradicts a substantive
  part of what the text describes (e.g. text says 5 jobgroups, structured has
  3; text says effective dates 2024-01-01 and 2025-01-01, structured only has
  2024-01-01; text gives an amount the structured data contradicts).
- **not_verifiable** = the text doesn't provide concrete claims to check
  against (rare).

## What counts as a discrepancy

The text typically describes the **wage system structure** more than every
individual amount. Don't flag a structured entry just because it isn't
mentioned in the text — the actual numbers usually come from PDF tables, not
the text bullets. But DO flag when:

- The text names scales that the structured data omits (or vice versa).
- The text gives effective dates that the structured data doesn't have (or
  vice versa).
- The text gives a percentage increase (e.g. "0.50% on 1 January 2025") that
  the structured data's amounts don't support.
- The text describes a feature (youth scale, entry scale, allowance) that's
  absent from the structured data.
- The text gives a specific value the structured data contradicts.
- A unit is clearly wrong (text says "per 4 weeks", structured says "monthly").

## Hard rules

- **Never invent.** Every claim about what the text says must be backed by
  `evidence_quote` from the actual text.
- **Quote evidence verbatim.** A short snippet copied from the wage_information_text.
- **Per-discrepancy reasoning.** One short sentence explaining why it's a
  mismatch.
- **Don't speculate about the PDF.** This step doesn't verify the LLM's
  reading of the PDF; only verifies the text→structured translation.
- **`structured_says: "n/a"` or `text_says: "n/a"`** when one side is silent.
