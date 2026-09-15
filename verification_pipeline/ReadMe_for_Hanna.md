# ReadMe for Hanna — the CAO generosity data: the whole pipeline, the formulas, and every judgment call

Plain-language but complete companion to `all_indices.xlsx`. The technical twin (file paths,
repro commands, written for a Claude in the admin environment) is
`indices/DATA_GUIDE_FOR_ANALYSIS.md`; the cell-by-cell audit trail is
`indices/corrections/corrections_log.xlsx`; the design-decision log with full rationale is
`indices/METHODOLOGY.md`.

## 1. What the workbook contains

One Excel file, 26 tabs, four families:

- **PanelMonthly** — the scores as one row per CAO per *calendar month*, using
  whichever document was in force that month. This is the analysis-ready object.
- **AgreementLevel** — one row per CAO *document* (2,698 dated full texts across 242 CAOs,
  ~1,552 agreement terms), organized by agreement term, with all **non-salary scores**
  (per-topic z's and percentiles plus the overall composites) and the term bookkeeping:
  each file numbered within its term (1, 2, …, by filing date), `n_editions_in_term`, a
  last-in-term flag (filter on it for a one-row-per-term view), both date axes,
  retroactivity/AVV/signing fields and the carry flags. **This tab contains no wage
  data, and its overall scores exclude wage — they are named `overall_z_without_wage`,
  `overall_pctile_without_wage`, etc. to make that unmistakable.** The reason: a wage
  ladder is a property of the CAO and the calendar year (one year's ladder can pool
  scales from several files, including files filed later the same year), so pinning it
  to a single document row would attribute borrowed, partly future information to that
  document. Wage lives at its own grain in the Wage tab (next tab) and joins back when
  you build a panel; PanelMonthly keeps the wage-inclusive `overall_z`, which is why the
  two overall scores carry different names — they measure different things and should
  not be compared directly. The monthly panel is reconstructable from this tab on any
  date convention: expand the documents by your chosen start-date column, then attach
  wage from the Wage tab by calendar year (see the Wage bullet for the recipe). (This
  tab absorbed the former *Composite* tab and the former *PanelMonthlyFirst* tab, whose
  rule became the first of the two ready-made start-date columns below; the wage block
  was removed on 2026-07-15 when the overall scores were switched to the without-wage
  definition.) Next to the two raw dates:
  - **Date_first_is_Ingangsdatum** — the first-file rule: a term's *first* file starts at
    its ingangsdatum, every later file in the term starts at its own file_date. (Always
    filled, always in year-month-day format.)
  - **Date_retro_datum** — the same dates, except where the agreement text itself states an
    explicit retroactivity start date (the `retro_start_date` column, present for 556 of
    the 2,698 documents): there the explicit date wins. Where no explicit clause exists,
    it simply equals Date_first_is_Ingangsdatum. This matters mostly for *later* editions
    in a term — a wage update filed mid-term that says "applies retroactively from 1
    April" gets 1 April here, which the first-file rule alone cannot know (393 documents
    change date this way).
- **Wage** — sits directly after AgreementLevel as its **companion table**: the wage-scale
  ladder per CAO per *calendar year* (levels p10–p90, mean, entry scales, minimum-wage
  ratios, wage z). Important: the year here is the **wage table's own effective date** as
  printed in the CAO text (scales say "per 1 July 2023") — *not* the filing date and *not*
  the agreement's ingangsdatum. Wage tables carry their own dates, so a scale effective
  1 January 2023 inside a document filed in June 2023 counts for 2023 from the start —
  the retroactivity question that the start-date columns solve for the other conditions
  is already built into the wage data itself. When several files overlap, two rules decide
  which table counts: (1) if two editions of the same CAO print a table for the *same*
  effective date, the **latest-filed edition wins** (its restatement is the most
  consolidated; the older file's version of that table is dropped — but two tables with
  *different* effective dates, e.g. a January scale and a July raise, both count, so a
  year's ladder can pool scales from several files: the wage row is a property of the
  CAO-year, not of any single document — which is exactly why the AgreementLevel tab
  carries no wage columns and scores documents without wage); (2) if
  an expiring agreement had pre-announced a table into a period that a *successor
  agreement* governs, the **successor's table wins** — a superseded agreement's announced
  future raise is void, and every such dropped point is logged. This is the table to join
  when generating a panel from AgreementLevel:
  expand the documents by your chosen start-date column, then attach each panel month's
  wage from its calendar year here (carrying the last known year forward) — that
  reproduces PanelMonthly's wage columns exactly. Note the tab itself is deliberately
  **not** forward-filled — a year appears only if a text actually states a scale for it
  (so a filled year is never mistaken for a newly agreed one); the carry-forward happens
  at the join. After the join no internal gaps remain: any wage still missing in the
  panel means "before this CAO's first parsed scale" (or one of the ~27 CAOs whose
  salary tables couldn't be parsed at all), never a bridgeable hole.
- **Topic tabs** (Leave, Term, Contract, Overtime, Training, Bonus, Fringe, Homeoffice,
  Pension, Absence, Safety, Childcare) — the per-topic detail behind every score: each
  field's converted value, its z, its percentile, and the coverage booleans.
- **Statutory + factor/diagnostic tabs** — the legal-floor timeline, the statutory
  pseudo-CAO, factor analyses, field-weighting diagnostics.

## 2. The pipeline, end to end

Seven stages; everything downstream of stage 1 is reproducible from the repo.

1. **Source documents.** All CAO texts filed with the Ministry of Social Affairs (SZW,
   via uitvoeringarbeidsvoorwaardenwetgeving.nl): 2,700+ PDFs for the 242 CAOs in scope,
   1999–2026. Each filing gets a receipt date from the ministry (the *kennisgeving van
   ontvangst*) — this date matters later.
2. **Parsing.** Every PDF is converted to a full-text document. Native text layers are
   read directly; scans are OCR'd. (Two documents needed hand repair — a rotated scan and
   a corrupt font; see §8, failure mode 4.)
3. **Extraction.** An LLM reads each parsed document and fills a fixed questionnaire of
   ~318 fields across 13 topics (leave, sickness/absence, contract types, termination,
   overtime, training, bonuses, fringe benefits, home office, pension, safety, childcare,
   AI), translating to English and recording durations, percentages, amounts, and yes/no
   provisions. Salary tables go through a *separate deterministic parser* (not an LLM) —
   see §6.
4. **Correction & verification.** The raw extraction looked complete but contained
   systematic errors. Thirty-four audited correction layers (~12,000 distinct corrected
   cells; ~21,000 logged change events, since some cells were revisited) were applied,
   every change logged with the verbatim source quote. §8 explains the whole machinery:
   how errors were *found*, how each proposed fix was *verified* before it was allowed to
   touch the data, what the layers actually *did* (grouped into four waves), what kinds of
   changes were made and why, and what remains open.
5. **Scoring.** Each document gets per-topic scores on two tracks (how *much* and how
   *broad*), plus overall composites, on two parallel scales (z and percentile). All
   formulas in §4.
6. **Composite file.** One row per document with all scores and metadata.
7. **Panel.** The document rows are expanded to a CAO × month panel using the in-force
   rule (§3), with the statutory floor re-scored in the month a law changes. The workbook
   is generated from these outputs.

## 3. The two clocks: ingangsdatum vs. file_date

Every document carries two meaningful dates, and they answer different questions:

- **`ingangsdatum`** — the agreement term's contractual start date. Texts are routinely
  signed and filed *months* after this date and then apply retroactively to it
  (back-pay for organized employers is standard practice). It is also noisy in the data:
  sometimes missing, wrong, or duplicated across editions. It serves as the *grouping*
  key — one term = one (CAO, ingangsdatum) pair.
- **`file_date`** — the ministry-receipt date (kennisgeving van ontvangst). Under the Wet
  op de loonvorming, CAO wage provisions cannot formally take effect before this date, and
  an AVV extension to unorganized employers never works backwards. It is reliably recorded
  for 99.96% of documents. It serves as the *clock*.

One term usually produces **several files over its life** — the initial text, then updates
(mostly refreshed wage tables, occasionally other conditions). All share the same
ingangsdatum, so only file_date can order them.

**The in-force rule.** For CAO c in month m, the document in force is the one filed most
recently up to that month:

```
D(c, m)  =  the document d of c with the largest file_date(d) ≤ end of month m
```

A document owns months until the next filing replaces it. The gap between ingangsdatum and
first filing is often close to a year (`pub_lag_months` shows it per row) — genuine
publication lag, not a data error.

**Law-month re-scoring.** When a *statute* changes, every CAO's statutory-anchored
components are re-scored in that calendar month, not at the CAO's next filing — so legal
changes never appear with artificial delay. (Consequence for research: apparent movement
in a CAO's score in a law month is the *floor* moving, not the bargaining parties acting.
The statutory pseudo-CAO, §7, lets you separate the two.)

**The first-file axis.** If your design needs terms to start at their contractual start
date, use AgreementLevel's Date_first_is_Ingangsdatum column (a term's first file
backdated to its ingangsdatum, later files at their filing date) as the panel axis.
Explicit retroactivity clauses exist in the text of ~25% of documents (671 of 2,739
records; 567 with usable start dates) and travel with every row of the AgreementLevel
tab — including as the ready-made Date_retro_datum column — so finer retro designs can
be built; the first-file rule is the robust default.

## 4. The scores — every formula

### 4.1 Magnitude ("how much"): pooled, winsorized, clipped, signed z-scores

**One ruler for all years.** Each numeric field is standardized once, against all full-CAO
documents of *all* years pooled — not within-year. A 2005 document and a 2024 document are
scored on the same ruler, which is what makes the panel comparable over time.

**The yardstick (de-duplication).** The distribution parameters are computed on *one
document per term* (the latest filing of each (CAO, ingangsdatum) group), so that a CAO
re-issued 16 times does not weigh 16× in the ruler. All documents are then *scored*
against those parameters — the dedup shapes the ruler, not who gets measured.

**Per field f with raw value x, four steps:**

1. *Unit canonicalization.* Convert to one canonical unit (days/year, weeks, months, €,
   %, hours/week). "€ per 3-year period" → per year; "2× the weekly working hours" →
   hours via the CAO's own modal full-time workweek (never a guessed one).
2. *Winsorize* at the yardstick's 1st and 99th percentiles [lo_f, hi_f], to keep wild
   extraction outliers out of the ruler:

   w = min( hi_f , max( lo_f , x ) )

3. *Standardize and clip* against the yardstick mean μ_f and standard deviation σ_f:

   z_f = clip( (w − μ_f) / σ_f , −3 , +3 )

4. *Orient* by the field's generosity sign s_f ∈ {+1, −1} (+1 where more is better:
   vacation days, notice, allowance %; −1 where less is better: probation length,
   employee pension contribution, full-time hours, early-retirement age):

   g_f = s_f · z_f

**Topic magnitude** = the available-case mean over the fields the document actually
populates (F = populated fields of topic t):

```
M_t  =  (1/|F|) · Σ_{f ∈ F} g_f
```

A field with no value is *skipped*, not counted as zero — a document is not penalized for
a benefit it simply didn't quantify (exceptions: the statutory floor and zero-fill rules
in §5). M_t is missing only if the document populates none of the topic's fields.

*Why z and not min–max scaling:* the min and max of a field are by definition its two most
extreme (least trustworthy) values, so min–max lets one outlier define the whole scale;
z keeps a meaningful zero (= the average CAO), unit variance (fields average on equal
footing) and σ-distances, which factor analysis requires. The ±3 clip touches only ~1% of
values, all in the noisy tail.

*Incompatible units (stratified standardization).* A few fields state the same concept in
units that cannot be converted into each other without inventing data — training budget
comes as € (703 docs), % of salary (175), or % of the employer's wage sum (296). Each
document is standardized *within its own unit base*:

```
z = clip( (x − μ_base) / σ_base , ±3 )
```

Since every base is centred at 0 with unit variance, "+1σ in €" and "+1σ in %-of-salary"
both read as "one σ above average generosity for that kind of budget," and the field
enters the topic mean as one variable. This keeps all stated budgets instead of silently
dropping the ~40% non-€ ones.

### 4.2 Coverage ("how broad")

Coverage is the share of the topic's yes/no provisions the CAO grants at all
(13th month? employer childcare? extra disability insurance? …):

```
C_t  =  (# booleans True) / (# booleans in topic t)
```

and that 0–1 share is then put through the same pooled-z recipe (same yardstick dedup) to
give the coverage z. Why a fraction rather than z-scoring each boolean: a near-constant
boolean explodes — a provision that 98.6% of CAOs have puts the rare "no" at −8σ, so one
common yes/no would swing the topic harder than three amounts combined, exactly backwards.

### 4.3 Combined topic score and the overall composites

```
topic_z_t      =  ½ · ( M_t(z-scored) + coverage_z_t )        ← the headline per topic
overall_z      =  mean over topics of topic_z_t               ← includes wage (§6) as the
                                                                 wage_median_z headline
overall_z_var  =  variance across the topic z's               ← how lopsided the package is
overall_numeric_z = the magnitude-only roll-up (no coverage)
```

Two variants of the overall composites exist, by tab: **PanelMonthly** carries the
wage-inclusive `overall_z`/`overall_pctile` as above (13 inputs: 10 dual topics + wage +
safety & childcare coverage). **AgreementLevel** carries `overall_z_without_wage` /
`overall_pctile_without_wage` (the same formula over the 12 non-salary inputs), because at
document grain wage is a CAO-year fact that would attribute pooled, partly later-filed
information to a single document. The different names are deliberate — the two overalls
measure different things and should not be compared across tabs.

Equal weights everywhere — across fields within a topic, across the two tracks, across
topics. That is a deliberate, transparent default: any reweighting can be done downstream
because every component is exposed in the topic tabs; the index never hides a weighting
choice. The two-track design (rather than pooling amounts and booleans into one mean) is
also deliberate: with one pooled mean, the amount-vs-breadth weighting would be set by how
many fields of each type the extractor happened to create per topic (a schema accident),
missing amounts and observed booleans would be forced under one missing-data rule, and the
diagnostic difference between "flatly absent" (low on both tracks) and "present but thin"
(low amount, decent breadth) would be lost.

### 4.4 The percentile track (the second scale)

z-averaging has one distortion: skewed fields have asymmetric z-ranges after clipping
(a field where nearly everyone sits at 100% runs about [−3.0, +0.33]; a right-skewed
budget runs [−0.74, +3.0]), so being best in a bunched field cannot offset being worst in
a spread field. The percentile track cures this: each field is ranked by its empirical
CDF,

```
p_f  =  rank of x among all yardstick values of f   ∈ [0, 1]
```

and topic/overall percentile scores are the means of those ranks (oriented so higher =
more generous). Ranks need no winsorizing and no clip — an outlier just gets rank ≈ 1 and
cannot dominate a mean. The cost: a rank scale is nonlinear (it compresses tails — €40k
and €15k above the median both sit near 1.0). The two scales agree on who is generous
(corr ≈ 0.77) but not perfectly — the gap is exactly the skewed fields.

**Which scale for what:** use **z** for factor analysis, σ-distance statements and
anything linear; use **percentile** for bounded, symmetric, equal-influence rankings and
communication. Both exist for every field, topic and composite.

## 5. Filling rules — what a blank means, and when it is filled

A blank cell can mean four different things, and the pipeline treats them differently:

1. **Statutory floor (TOTAL fields).** For entitlements where a statute guarantees a
   minimum (maternity leave, sick pay, care leave, notice, the fixed-term chain rule…),
   a CAO that is silent still owes the legal minimum. Where the statutory timeline (§7)
   has an era-correct value S_f(era), blanks are imputed:

   - *floor / default*: blank → S_f(era of the document); a stated value is kept as-is.
   - *floor-lift*: blank → S_f, **and** a stated below-floor value is lifted to S_f
     (the law overrides).
   - *cap*: blanks stay blank; a stated above-cap value is masked (the law caps it).

   Only 12 of 41 statutory-linked fields fill blanks; the era matters (the chain rule was
   36 months, then 24 in 2015–2019, then 36 again — each document is scored against the
   law of its own date, and the panel re-scores when the law changes).
2. **Pension — the hard exception, never imputed.** Most CAOs defer pensions entirely to
   a sector fund ("pension is regulated in the fund's scheme") and state no numbers.
   That is *deferral, not absence* — imputing anything (statutory, zero, a normal age)
   would systematically punish exactly the sectors with the strongest fund arrangements.
   Pension scores are computed only over the CAOs that state their own terms.
3. **Zero-fill (EXTRA benefits).** For genuinely optional extras (13th month, meal/
   relocation/commuting allowances, home-office stipend, shift allowance, training
   time/budget/reimbursement), a blank amount becomes a rankable 0 — but only when the
   benefit is genuinely absent: the presence boolean is False **and** the benefit is
   never True in *any* edition of that CAO. A benefit that exists but is unquantified in
   one edition stays missing (available-case), not zero. Why not impute an amount
   instead: the project's hard "never invent values" rule — you can have any two of
   {no invented numbers; each benefit counted once; no information lost}, and we chose
   to keep the first and third (the price: a benefit's presence shows up in both tracks,
   so combined scores modestly over-weight the zero-filled extras).
4. **Everything else** stays blank and is skipped available-case.

## 6. Wage

Salary tables are *not* LLM-extracted: a deterministic parser reads each edition's wage
grids directly (100% of amounts carry provenance to a grid cell; per-row confidence tiers,
only the top tiers used). Construction, per CAO × calendar year:

- melt every edition's grid to one row per wage point; drop non-adult and entry/aanloop
  rows (tracked separately); normalize every amount to €/month (weekly × 52/12, 4-weekly
  × 13/12, annual ÷ 12, hourly × the row's own stated hours basis — never a guessed
  workweek);
- de-duplicate version-aware (for the same wage cell keep the latest edition's rows, then
  drop exact repeats), drop pure delta/annex editions, and apply term precedence: a wage
  point dated after a successor agreement's start is stale (the successor governs) and is
  dropped;
- the year is the wage point's *own* effective date (wage tables carry their own dates),
  not the file's date;
- worker-type labels are too sparse (~17%) to be the primary skill split, so the split is
  by *scale position*: p10 of the CAO's wage ladder = low-skill floor, p90 = top, plus
  p25 / median / mean / p75.

Headline: **wage_median_z** = pooled z of the CAO's median negotiated wage, scored on
*nominal euros* deliberately — normalizing by the minimum wage would entangle CAO
generosity with statutory WML policy, which moves twice a year on its own schedule.
The WML comparisons exist as separate ratio columns:

```
ratio_median_wml  =  mw_median(c, y) / WML(y)      (same for low / mean / high)
```

with WML(y) the year-average of the January/July statutory revisions.

## 7. The statutory reference

The Statutory tab is not extracted from any CAO: it is a hand-built change-point timeline
of the Dutch legal floor — one row per entitlement per validity window (maternity 16
weeks; sick pay 70% for 104 weeks; the chain rule with its 2015 and 2020 reversals;
minimum wage; working-time caps; …), each with the statute's name (WAZO, BW 7:629, WWZ,
WAB, WML, ATW…) and a link to the official publication, checked against wetten.overheid.nl
and the Staatsblad in June 2026. It does two jobs:

1. the **floor** under CAO scores (§5.1), era-correct per document and per panel month;
2. a **pseudo-CAO**: the legal floor itself is scored through the identical pipeline and
   appears as its own rows in every tab — so "generosity relative to the law" is directly
   measurable, and a law change is visible on the same scale as a bargaining change.
   (We used this to test whether statutory changes trigger CAO responses — they don't;
   the apparent co-movement is the floor re-scoring, see ADVANCED_ANALYSIS §10.)

## 8. Data quality: the correction machinery, all 34 layers, and why to trust it

### 8.1 How errors were found — structure instead of re-reading everything

Reading 2,700 documents twice was never feasible, so errors were hunted where the data
*must* be internally consistent and isn't:

- **Same-term disagreement.** One agreement term is usually printed several times
  (initial text + updates). Two printings of the *same* text that disagree on a field
  cannot both be right — every such flip is a free, certain error detector.
- **Jumps and V-shapes over time.** A CAO's topic score that leaps between consecutive
  editions, or dips and recovers in a V, is either a genuine renegotiation or an
  extraction error — each event was classified by reading the text, never by the pattern
  alone (genuine amendments were found and deliberately left alone).
- **Statutory fingerprints.** A cell whose value exactly equals the statutory constant of
  its era while being coded as an above-statutory enhancement is a restatement suspect.
- **Distribution outliers.** Values only reachable through unit errors (520 weeks of
  leave) surfaced through the winsorization diagnostics.
- **Independent audits.** Random samples of *applied fixes* were re-derived from scratch
  by fresh agents to test the process itself (§8.4).

### 8.2 How each fix was decided — the verification protocol

No pattern rule ever changed a cell directly. Every suspect went through this chain:

1. **A reading agent** re-read the document's own source text (full text, not snippets —
   we measured early on that truncated snippets produce ~97% false "the clause is absent"
   verdicts) and proposed a verdict, always with a verbatim quote and the *exact* field
   definition in hand (withholding the definition was measured to cause ~22% wrong
   corrections — an agent that doesn't know what a field means judges against a guess).
2. **A second, independent, deliberately skeptical agent** re-derived the verdict from
   scratch. It rejected 35–65% of first-pass proposals depending on the campaign —
   computed values, cumulative misreads, unit errors, over-reach. Only doubly-confirmed
   fixes were applied.
3. **Disagreements went to a stronger arbiter model**, not to whichever agent was louder.
4. **Evidence tiers.** If the extraction was insufficient to judge, the agent escalated
   to the parsed full text; if the parse itself was suspect, to a fresh OCR of the
   original PDF. Every fix records which tier its evidence came from (this matters:
   quotes from the Dutch full text can never appear verbatim in the English extraction,
   and checking at the wrong tier once produced a batch of false alarms).
5. **Hard rules throughout:** never invent a value that is not literally in the text
   (silence stays blank, "statutory applies" without a figure stays blank, no unit
   conversion that fabricates a number); a fixed convention catalogue (below) so that
   identical situations are always decided identically; and cells the text genuinely
   cannot settle become explicit CANT_TELL items for your decision queue instead of
   silent guesses.
6. **Ripple closure.** Fixing one edition can expose the same error on a sibling edition
   that previously "agreed" — after every campaign the newly exposed siblings were
   re-adjudicated, wave after wave, until a pass produced zero new suspects (a fixed
   point), so no fix left a half-corrected family behind.

### 8.3 What the 34 layers actually did (grouped into four waves)

**Wave 1 — foundational per-topic QA (layers 1–7, ≈9,200 cells).** Topic-by-topic
campaigns over the curated 95-CAO core: per-topic subagent review with human sign-off
(514), a full-dataset audit with definition-anchored re-verification (781 — this pass is
where the 22%-over-correction lesson was learned), per-document fixes where the corrected
value was quote-backed in the record's own source (3,318), verified removals of coded
benefits whose evidence didn't survive re-reading (2,332 — mostly True→False on
boilerplate), adjudication of every cell touched by the convention rulings you made
(2,105: base-figure vs surcharge, first-tier, strict enums…), and two rounds of reviewed
residual suggestions (191).

**Wave 2 — cross-edition consistency (layers 8–13, ≈4,800 cells).** The same-term
disagreement detector at full scale: every boolean that flips between printings of the
same agreement source-verified (3,559 fixes), numeric same-term discrepancies with the
two-pass protocol (61 — the second reader rejected half the first pass, exactly the
over-correction risk showing up and being stopped), consistency-unification where
editions picked different values from the same menu plus tie-breaks under your rules
(111), date-collision fixes where two documents shared a date and the wrong one was
being read (36), and one systemic recode: 1,071 pension contribution cells where "% of
premium" had been stored as if it were "% of salary".

**Wave 3 — the jump-verification campaign (layers 14–28, ≈5,900 cells).** Every
cross-edition score jump, dip, V-shape and family anomaly in the whole corpus,
tier by tier: downward dips, family sweeps around them, topic-level and upward jumps,
same-term events (902 cells), full-text resolution of cells the extracts could not
settle (106), your 22 per-case rulings, the re-extraction of the two fabricated
documents from re-OCR'd PDFs (252 cells), "sandwich" events where both flanking editions
agree against the middle (461), the two big cross-term tiers — suspicious jumps that
later revert (1,410) and persistent ones (1,465) — and then ripple closure in seven
waves (724) until zero uncovered sibling disagreements remained.

**Wave 4 — closure and independent audit (layers 29–34, ≈1,000 cells).** Post-campaign
extensions (full-text closure of remaining undecidable cells, a dedicated part-time
screen, 274 cells); an **independent end-to-end audit** — a stratified random sample of
applied fixes re-derived from scratch by fresh agents, disputes ruled by a stronger
model (verdict: mechanically perfect replay, but it *indicted the restoration direction*,
see failure mode 2); the consequence: **all 1,628 restored benefits re-examined** against
the field-boundary question, ~a quarter reverted (516 cells); two closure layers
resolving the remaining undecidable cells by family consistency (48) and by full-text
re-reading (90); and a final layer closing the follow-up flags those two had raised
(37 cells — e.g. a family of maritime agreements whose shift-allowance figures were
really unfavourable-hours supplements, re-routed to the right field).

### 8.4 What the changes look like, and why they were made

Concretely, a "corrected cell" is one of five things, in decreasing frequency:

- **True → False** (the largest class): a coded benefit whose evidence turned out to be
  legal boilerplate, a heading, a recommendation, an exclusion list, a niche-group rule,
  or a misfiled neighbouring concept. Removing these *raises* the meaning of every
  remaining True.
- **False/blank → True/value** (restorations): benefits the extractor missed but the
  full text states — each restoration later re-verified in the wave-4 re-examination.
- **Value → value**: wrong tier picked from a schedule (conventions: multi-tier sick pay
  = the year-1 rate; probation = the maximum stated tier; notice = the base tier), wrong
  unit, wrong denominator ("% of premium" as "% of salary"), or a same-menu
  inconsistency unified across editions.
- **Value → blank**: derived numbers (a weekly figure computed from a 4-week rule),
  niche-group figures in typical-worker fields, era-stale figures, statutory
  restatements in enhancement fields. Blank is the honest state — the scoring treats it
  available-case (§5).
- **Record-level repairs**: the two fabricated documents rebuilt from fresh OCR, and the
  thin-document carries of §9.

The failure modes that *caused* those changes, in decreasing order of size:

1. **Legal boilerplate mistaken for generosity** (largest class). CAOs restating the
   statutory care-leave terms, the standard chain rule, or the legal transition payment
   were coded as if the CAO granted something extra. Cure: a "precedent catalogue" every
   judging agent applies — a restatement is not a top-up, a recommendation or "parties
   will examine…" is not an entitlement, a benefit named only inside an exclusion list is
   not granted — plus a standing screen flagging any cell whose value equals the
   statutory constant of its era while claiming to be an enhancement.
2. **Right fact, wrong pigeonhole.** A shift surcharge recorded as a job allowance, a
   doctor's-visit rule as care leave, sickness-absence registration as workload
   monitoring. This one humbled us: it survived the two-agent check because *both* agents
   were watching for boilerplate traps, not misfiled concepts — two checkers sharing a
   blind spot are one checker. An independent audit caught it; we then re-examined **all
   1,628 restored benefits** with one question — "does this clause answer *this* field's
   question, for the *typical* worker?" — and about a quarter were reverted.
3. **Niche groups presented as the norm.** Provisions for apprentices, under-18s,
   over-55s or pension-age workers filling fields meant for the typical worker. Same
   cure as (2).
4. **Fabricated documents.** Two files were invented nearly wholesale by the extractor
   choking on a rotated scan and a corrupt font — one contained a fully fictional 572-row
   salary table. Both were re-OCR'd from the original PDFs and rebuilt end to end (full
   text, extraction, and a verified replacement salary table), so the on-disk source
   artifacts now match the corrected data. Related: one agreement's probation article was
   "shadowed" by an unfilled model-contract appendix; only reading the full original text
   of every edition showed the real rule existed in 2010–2012 and genuinely disappeared
   later.
5. **Unit chaos.** "10 days per year" once became 520 weeks. The unit converter was
   rebuilt and regression-tested over all 56,000 numeric cells before adoption.
6. **Messy dates and duplicate printings.** Drafts filed next to final versions, same-day
   duplicates, terms whose editions disagree. The cure is §3's design (file_date as the
   clock, ingangsdatum as the grouping key) plus the explicit edition numbering within
   each term.
7. **Thin documents** — a partial capture scored at face value reads as "this sector
   abolished nearly all benefits", which is false. See §9.
8. **Ambiguous cells that full text cannot settle** (two equally supported readings, a
   selection among stated tiers, a schema-boundary call). These were resolved by explicit
   conventions (multi-tier sick pay = the year-1 rate; probation = the maximum stated
   tier; consistency across same-term editions), and the residue — currently **28 items**
   — sits in `indices/review/open_cant_tells_for_hanna.csv` as your decision queue.

### 8.5 Why you can trust the cleanup itself

Every logged change replays exactly into the
dataset and nothing changed that is not logged (verified mechanically; churn 0.9%);
a stratified random sample of applied fixes was re-derived from scratch by fresh agents
with disputes settled by a stronger model — which is exactly how failure mode (2)
surfaced, i.e. the audit was allowed to indict the process and did; and the noise
thermometer — how often the same yes/no field disagrees between printings of the same
agreement — fell from 9.1% to ~3.3%, the remainder individually verified as genuine
draft-vs-final differences.

### 8.6 Known limitation, kept deliberately visible

A value that is wrong the *same way in every edition* never produces a jump or a flip, so
the consistency-based detectors of §8.1 cannot see it. Rather than pretending this class
doesn't exist, the statutory-fingerprint screen keeps a standing worklist of such
suspects (~1,150 rows, never yet agent-checked) as the queue for a future
verify-every-value pass.

## 9. Carried scores (`scores_carried_from`): why, and each case

Some filings typed as "full CAO text" are nothing of the kind — a mis-ingested appendix,
an umbrella shell deferring to companion booklets, a deviations-only amendment, or a
capture that missed most of the document. Scoring such a file at face value fabricates
"all benefits abolished"; legally and practically, **the previous agreement's conditions
remain in force until actually replaced**. So a confirmed thin capture *carries* the
scores of the right fuller document and says so in two columns (`thin_doc=True`,
`scores_carried_from=<the document whose scores it wears>`). Safeguards: no candidate is
ever carried automatically — each was source-verified by an agent reading the full text
first (two candidates turned out to be *real* scope changes and are protected from
carrying); each deviations-style document was additionally checked clause-by-clause
against the carried scores to confirm none of its stated deviations contradicts a carried
value; and the carry is visible per row, so any analysis can exclude carried rows.

| Document | Carried from | What the document actually is (root cause, source-verified) |
|---|---|---|
| 1029015 — VWH 2018 "mantel" | previous full VWH edition | An umbrella shell: it states outright that conditions live in companion regulation booklets. The booklets' content = the prior full edition's terms, which remained in force. |
| 2299006 — RPO 2016-17 "Bijlage 1" | CAO PO 1494016 (cross-CAO) | An *appendix* was ingested as if it were the CAO. The RPO explicitly adopts the national primary-education CAO — the national CAO's same-term edition is what actually governed. |
| 2299002 — RPO deviations doc | CAO PO 1494005 (cross-CAO) | A deviations-only supplement (a few committee/union rules); everything else is the national CAO PO covering exactly its period. |
| 2299003 — RPO deviations doc | CAO PO 1494009 (cross-CAO) | Literally a find-and-replace notice on one chapter of the CAO PO — the edition it rewrites is the substance. |
| 233009 — Houtwerkende ind. 2019 | previous full edition | **Not a capture failure and not a full text**: the PDF itself is a genuine 3-page extension protocol (extends the term to 31-12-2019, adds one voluntary 4-day-week clause for 55+, and *refers* readers to the 2015-17 booklet, "cao boekje pag. 61", instead of restating it). Parsing and extraction captured everything that exists; its own few provisions don't touch typical-worker fields, so the carry is safe. |
| 1494007 — Appo & GOVak 2018-2023 | substantive same-term edition | A fund-apportionment *procedure* document (how labour-market money is collected), not employment terms. |
| 408003 — Schippersinternaten 2022-23 | its predecessor | States it "largely follows CAO Jeugdzorg" and lists only deviations; the predecessor is the closest full statement of what applies. Clause-by-clause check: vacation hours (172.8), the seniority-leave table, the parental-leave rule and the social plan all *verbatim confirm* the carried values — **but one genuine conflict**: its own text says the old irregular-hours (ORT) percentage scale is valid only until 30 Sept 2022, after which **CAO Jeugdzorg's ORT scale** applies — so the carried overtime/ORT percentages are stale for ~14 of the term's 20 months. The replacement number is not quoted in the document itself, so it is flagged (not silently "fixed"); see the open-flags file. |
| 35010 — AVV gazette reprint | previous full edition | A gazette reprint of only the posted-worker clauses; every other chapter is absent *by design of the reprint*, not abolished. Clause-by-clause check: every number the reprint restates (3.5% personal allowance, 36h full-time floor, lustrum Liberation Day, O&O-fonds) matches the carried edition **verbatim — carry safe**. |
| 941011 — Water companies 2011-12 | same-term reissue | **A genuine full 78-page CAO whose extraction run failed on 6 topics**: the parsed text demonstrably contains the full overtime, termination, telework, contract, probation and safety chapters, but the extractor returned them empty — a run-specific extraction miss, not a document problem. The same term's later reissues extracted those exact topics correctly, so the carry restores what the failed run lost. |
| 433016 — Schoonmaak "met renvooi" | previous full edition | An extension reprint containing only the named extended articles (all transfer-of-undertaking mechanics). Clause-by-clause check: every quantified provision it restates (the VET wage-supplement rule, the above-CAO buy-out formula, AOW auto-end, workload-measurement timing) **verbatim confirms** the carried record; its one new number (a 48h/week threshold) is a narrow contract-change-eligibility test, not a redefinition of any captured field — **carry safe**. |
| 1285003 — Tank & Was, 22-june draft | same-term July reissues | An early draft, partially captured; the same term's reissues are complete. |
| 227006 — Weefselkweek 2022-2027 | 227007 (same edition, fuller capture) | The 2022 restructuring made this CAO a satellite of the Glastuinbouw CAO, incorporated as a separately-attached annex — missing from this capture but embedded in its twin filing 227007. Deliberately NOT carried from the pre-2022 edition: the restructuring made those self-contained rules stale. Deviation check: the three cells later filled from the twin (38h full-time, 100% overtime max, 2-month probation) are safe — none appears in the document's own explicit list of *excluded* Glastuinbouw articles. **Caution for any future carry**: vocational training (Art 41 §1&4) IS on the exclusion list, so the twin's training-fund/mandatory-training values must never be copied over (the current divergent values are correct); pension is likewise excluded and correctly scored from 227006's own text (1.75% accrual, optional BPL). |
| **Never carried:** 65001 — remplaçanten | — | A genuinely separate sub-CAO for substitute musicians sharing the CAO number; the score drop is a real scope difference. |
| **Never carried:** 1471006 — GEO Proces 2021 | — | A real one-year scope split (Proces only; 2022 explicitly rejoins Proces+Services). |
| **Confirmed complete:** 310006 — Margarine 2011-12 | — | Looked thin, but its own text genuinely states more modest terms; carrying would have overwritten real content. |

*Deviation check (2026-07-15):* each deviations-style document above (408003, 35010,
433016, 227006) was read clause-by-clause and its stated provisions compared against the
carried-from document's values field by field. Result: 35010 and 433016 fully safe (every
restated number matches verbatim); 227006 safe with the training-carve-out caution noted
in its row; 408003 has **one flagged conflict** (the ORT percentage superseded by CAO
Jeugdzorg's scale from Oct 2022 — the correct number lives in another CAO's text and was
not imported, per the no-invention rule). Full field-by-field record:
`indices/review/thin_doc_deviation_check.csv`; the 408003 conflict also sits in the
open-flags file `indices/review/l32_followup_flags.csv`.

## 10. How to use this data — a short checklist

- **Panel questions** ("what applied in month m?") → PanelMonthly; retro-coverage
  questions → build the panel from AgreementLevel's Date_first_is_Ingangsdatum (or
  Date_retro_datum for the explicit-clause version); per-term questions → AgreementLevel
  filtered to last-in-term; document-level questions → AgreementLevel.
- **Excluding carried rows:** filter `thin_doc = False` (or `scores_carried_from` empty).
- **Excluding stale in-force stretches:** `months_since_file` / `is_stale` flag documents
  that have been in force unusually long past their expiry.
- **Statutory vs. bargaining movement:** compare against the statutory pseudo-CAO rows;
  remember law months move every statutory-anchored score mechanically (§3).
- **Scale choice:** z for anything linear/factor-analytic; percentile for bounded
  equal-influence rankings (§4.4).
- **Pension:** available-case only — do not read a missing pension score as "no pension".
- **Wage:** nominal — deflate or use the `ratio_*_wml` columns for cross-time comparisons.
- **Open items:** your decision queue is `indices/review/open_cant_tells_for_hanna.csv`
  (28 items); the standing same-way-wrong suspect list is the statutory-fingerprint
  queue (~1,150 rows, never agent-checked, candidate for the next holistic pass).
