# Per-Topic Failure Modes — Termination (term)

Added 2026-05-25 from the term run (95 records, 913 L2 items, 18 clean wins, 1 NHR,
0 era outliers, 11 spelled-out values surfaced). Highest-era-complexity topic; run on
Opus. Era baselines are applied **post-hoc only** (never in the prompt).

## TERM_FM_01 — Statutory restatement ⇒ leave empty (no fill-in)

**When it applies**: the CAO says notice/probation/severance follows "the statutory
period", "Article 7:672/7:652/7:673 BW", "de wettelijke regeling", "the Civil Code".

**Subagent action**: do NOT copy the statutory number into the field. Leave it empty
(verdict=confirm if already empty, else clear). The statutory cross-check is done
downstream by `era_baselines` — never supply a figure from your own knowledge. This is
the dominant pattern: most CAOs defer termination to statute, so most fields are correctly
empty. (Validated: 894/913 items were no-ops, 0 era outliers — nothing fabricated.)

**CSV impact**: all `term_*_notice_*`, `term_probation_*`, `term_severance_*`.

## TERM_FM_02 — Spelled-out numbers → unable_to_verify (surface, don't fabricate)

**When it applies**: the CAO states a genuine non-statutory value but spells the number
out as a word ("a notice period of **two months**", "up to **two months** probation")
with no digit.

**Subagent action**: the strict evidence rule requires the proposed digit to appear
verbatim in the quote. A spelled-out word fails this, so route to
`verdict=unable_to_verify`, confidence=low, value EMPTY — do NOT fabricate the digit.
These are then surfaced for human digit-confirmation (see
`qa_term/outputs/spelled_out_values_for_review.csv`). Genuine values; trivially
human-resolved. (Seen on CAO 725023, 609002, 727036, 163011.)

**CSV impact**: `term_employer_notice_value`, `term_probation_fixedterm_value`, etc.

## TERM_FM_03 — Tenure-graded notice has no single scalar (value/range split)

**When it applies**: the CAO gives a tenure schedule (e.g. 1/2/3/4 months by years of
service, à la BW 7:672 but on the CAO's own authority — CAO UMC 1618009).

**Subagent action**: there is no single "typical" scalar to put in
`term_employer/employee_notice_value` — the schedule belongs in `term_notice_tenure_rule`
(and `term_notice_tenure_present=True`). If only the scalar value field is in your chunk,
route to `unable_to_verify` rather than pick an arbitrary tier. **Pipeline note**: the L2
scan distributes a field family (value / range / unit / tenure_rule) across different
chunks, so no single subagent sees them together. For a re-run, have build_chunks
co-locate a record's notice value+range+unit+tenure items in one chunk.

## TERM_FM_04 — Distinguish termination concepts (trap list)

**Subagent action**: do NOT cross-populate these — each is a different field (or none):
- **Internship/apprentice length** ("2.5 months required internship") ≠ probation.
- **Probation cap** ("2 months") ≠ a notice period.
- **WW/ZW supplements** (% top-ups, wachtgeld, SPAWW, activation schemes, 10%-of-daily-wage)
  ≠ a quantified EUR `term_severance_extra_value`; they have no single EUR amount.
- **RVU early-exit thresholds / salary caps** (e.g. €4,000, €27,276) ≠ severance.
- **Reintegration/outplacement bonuses** (VWNW, market-conform rate) ≠ tenure-based severance.
- **Retirement notification** ("4 months before AOW") ≠ a termination notice period.
- **Notice that varies by salary scale or job group** ≠ tenure-based → `notice_tenure_present=False`.

**Conservative booleans**: set presence flags True only on explicit, specific evidence;
prefer confirm/False-with-reasoning. (No presence boolean was over-asserted across 46 chunks.)
