# CAO-specific extraction overrides

A short list of irreducible per-CAO quirks the general prompt can't handle
cleanly. The extractor checks this file by `cao_number` AND `file_name`
substring; if a match is found, the override note is applied IN ADDITION to
the general prompt rules.

**Keep this file SMALL.** If you find yourself adding more than ~15 entries,
that's a signal the general prompt needs another [EDIT-N], not more
overrides. Per-CAO rules become tech debt fast.

---

## Format

Each override:
```
### <cao_number> [— optional filename hint]
**Pattern:** what's unusual about this CAO  
**Apply:** what to do for it
**Why the general prompt doesn't cover it:** one sentence
```

---

## Known overrides

### 427 — Tandtechniek
**Pattern:** Wage scales are referenced by name (Function groups 0, A-H) but
the actual euro amounts live in `www.bvtandtechniek.nl/cao` (Appendix 2),
not in the source bullets. Source DOES print 3 general increase percentages
with dates (2% on 2021-07-01, 2.5% on 2022-07-01, 1% on 2022-12-01).
**Apply:** EMIT one SalaryRow per function group (0, A, B, C, D, E, F, G, H)
with `timeline: []` and `row_note: "Function groups 0-H referenced; concrete
amounts in external Appendix 2 (bvtandtechniek.nl/cao), not tabulated in
wage_information"`. Then under [EDIT-5] the general rule already covers this.
**Why:** External-reference structural extraction. [EDIT-5] covers it; this
note exists to confirm 427 is the canonical example of the case.

### 1612 — Integrale Cao Kinderopvang
**Pattern:** Wage amounts are organized as a SINGLE unified salary-number
ladder (Salary Numbers 1-64 or 1-66 depending on year). A SEPARATE matrix
in Appendix 1 maps Schaal 2-12 → ranges of Salary Numbers (e.g. Schaal 1
covers Nrs 1-9, Schaal 2 covers Nrs 5-15, …).
**Apply:** Two valid encodings:
  - (1) Encode each (Schaal × Nr) pair using the per-Schaal mini-tables (the
    encoding used in 2023-2024 CAOs of this sector — gives more rows but
    captures the Schaal→Nr mapping in the structured data).
  - (2) Encode just the unified Salary Number ladder with `jobgroup: "Salary
    scale"` and `step: "1".."66"` (the encoding used in 2021-2022 CAOs of
    this sector — fewer rows but loses the Schaal→Nr mapping).
  Prefer (1) when per-Schaal mini-tables exist in the source.
**Why:** Two valid structural choices; the general prompt doesn't force one.

### 254 — retail (and similar files where header has 1a-N but data row has N values)
**Pattern:** Salary table header explicitly lists `1a` as a column, but the
"Amount of last increment" row has only N values matching columns `1b..N`,
suggesting column `1a` is structurally empty across the entire scale.
**Apply:** Confirm column `1a` is genuinely empty by checking BOTH the
salary-amount row AND the "Amount of last increment" row in the source.
If both row types omit a value for column `1a`, drop `1a` entirely (do not
emit). If only one row omits it, treat as ragged-row per [EDIT-3] and
re-examine column alignment.
**Why:** Verifies the "is this column genuinely empty?" decision the
ragged-row rule already makes — guards against auditor false-positives.

---

## Adding a new override

If you find a new pattern that can't be handled by [EDIT-1..5], add it here
with the format above. But first ask: could the general prompt be amended
to cover this case AND similar cases across other CAOs you might not have
seen yet? If yes, add an [EDIT-N] instead. Only add a CAO-specific
override when the pattern is genuinely unique to that file.
