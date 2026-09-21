# Indices — Methodology & Rationale (WHY we do it this way)

This is the **design-rationale** companion to the code. `ADVANCED_ANALYSIS.md` reports
results; this file explains *why* every modelling choice is what it is, general first
then per-topic. **When a modelling decision changes, update this file** — add a dated
entry to the Decision Log (§12) and edit the relevant section. Keep it in sync with
`index_lib.py`, the per-topic `*_index.py` headers, and the memory note
`indices-workstream-state.md`.

Owner: Hanna. Last structural update: 2026-07-08.

> **Naming note (2026-07-08).** The published column names follow `indices/NAMING.md` (the
> authority). The old `gen01` suffix was renamed to `pctile` everywhere: `{t}_gen01` →
> `{t}_numeric_pctile`, `overall_gen01` → `overall_numeric_pctile`, `{t}_combined01` →
> `{t}_pctile`, `overall_combined01` → `overall_pctile`. The bare `{t}_z` now means the
> **combined** headline (mean of numeric + coverage); magnitude is `{t}_numeric_z`. The body
> below uses the current names; older Decision Log entries (§12) keep the names in force on
> their date.

---

## 1. What the indices are

Per **full-CAO document** (one row per edition of a CAO, not just the newest), we build
a **generosity/protection score per topic**, then roll them into a cross-topic composite.
Each topic emits **two parallel scores**:

- **magnitude** (`<topic>_numeric_z`) — *how much* is offered, from the numeric fields.
- **coverage** (`<topic>_coverage`, `<topic>_coverage_z`) — *how many* provisions are
  present, from the boolean fields.

They are combined only at the end (`<topic>_z = mean(numeric_z, coverage_z)` — the bare
`{t}_z` is the **combined** headline). Higher = more worker-generous, always (signs enforce
this — §6).

---

## 2. The pooled-z design (the spine)

**One pooled z per file.** Each field is standardised **once**, against all full-CAO
documents of **all years** — not within-year, within-vintage, or within-active-set. This
keeps levels comparable across time by construction: a 2005 doc and a 2024 doc are scored
on the same ruler.

**Time axis = `datum_kennisgeving` (file/edition date), not `ingangsdatum`.** The edition
date is 99.96% populated and means "when this text was published," so re-issues and
updates are correctly included. Fallback to `ingangsdatum` only when the file date is
missing.

**Yardstick de-duplication.** The pooled μ/σ and winsor bounds are computed on **one
document per term-group** (`cao_number` + `ingangsdatum`, keeping the latest
`datum_kennisgeving`) so that a CAO re-issued 16 times does not get 16× the weight in the
distribution. **All** documents are then *scored* against those yardstick params — dedup
affects the ruler, not who gets measured. (`term_dedup_mask` in `index_lib.py`.)

Why: without dedup, a handful of frequently-re-issued CAOs would define the mean and
variance and distort everyone's z. Without pooling across years, scores would not be
comparable over time — the whole point of a panel.

---

## 3. Magnitude scoring — four steps per field

For each numeric field, in order (`field_variant` + `pooled_z` in `index_lib.py`):

1. **Unit-canonicalise** — convert to one canonical unit (days/yr, €, %, weeks, months,
   hours/wk…). "€ per 3-year period" → annual; "1× weekly working hours" → ×5 days / ×1
   week; etc. Hand-reading a raw cell can differ from the scored value because of this
   step — trust the per-field `_z` columns.
2. **Winsorise** to the yardstick's 1st/99th percentile (raw scale) — trims wild
   extraction outliers before they touch μ/σ.
3. **Standardise** `z = (x − μ) / σ` against the pooled (yardstick) params.
4. **Clip** to ±3, then **apply the sign** (§6).

**The exact formula.** For field *f* with value `x`, sign `s_f ∈ {+1,−1}`, yardstick winsor
bounds `[lo_f, hi_f]` and yardstick mean/sd `μ_f, σ_f`:

```
  w      = min(hi_f, max(lo_f, x))          # winsorise to 1/99 pct (raw scale)
  z_f    = clip( (w − μ_f) / σ_f , −3, +3 ) # standardise, then hard-clip
  g_f    = s_f · z_f                          # orient so higher = more generous

  topic magnitude  Z_topic(doc) = mean_{f ∈ populated fields} g_f     (available-case)
```

μ_f, σ_f, lo_f, hi_f are computed **once** on the yardstick (one doc per term-group) and
persisted in `out/scoring_params.csv`; every doc is then scored against them. The mean is
**available-case** — a field with no value is *dropped from the average* (not counted as 0),
so a doc isn't penalised for a field it simply didn't state — **unless** zero-fill applies (§8).
`Z_topic` is NaN only when a doc populates *zero* of the topic's fields.

The topic magnitude = **available-case mean** of the per-field signed z's: a field with no
value is *skipped* (not zeroed), so a doc that simply didn't state one field is not
penalised for it — **unless** zero-fill applies (§8).

### 3.1 Why clipped z and not a 0–1 scale (min-max)

Min-max `(x−min)/(max−min)` looks tidy but is the *least* robust choice: the min and max
**are** the two most extreme values, so one outlier defines the scale and crushes everyone
else toward 0/1. To stop that you must winsorise the endpoints — which is the clip again,
relabelled. It also gives fields **unequal variances** (a bunched field and a spread field
both span [0,1] but carry different information), which distorts averages and **breaks
factor analysis / PCA** (they expect unit-variance inputs — a hard requirement here).

z keeps three properties we need: a **meaningful centre** (0 = average CAO), **unit
variance** (fields average on equal footing), and **σ-distances preserved** (a much larger
amount reads as much larger). The ±3 clip is a *minor* concession — after winsorising, only
~1% of values reach it, all in the noisy tail where the exact number is least trustworthy.

### 3.2 The equal-range percentile track (`pctile`) — IMPLEMENTED alongside z
*(This scale was called `gen01` before the 2026-07-08 rename; the columns are now the
`_pctile` family — see the naming note at the top and `indices/NAMING.md`.)*

Two distinct problems pushed us to add a second, bounded [0,1] scale — not to replace z:

**(i) The z-range asymmetry (the decisive one).** z averages fields whose *ranges* differ
because their distributions are skewed. Reimbursement % is left-skewed (almost everyone at
100%), so after winsor/clip its z runs about **[−3.0, +0.33]**: being *best* is worth only
+0.33, being *worst* −3.0. Budget is right-skewed → **[−0.74, +3.0]**. So under z-averaging,
**being best in a bunched field cannot offset being worst in a spread field** — equal
weights are not equal influence.

**(ii) The hard-clip tie.** A value at 3.5σ and one at 8σ both clip to +3 — indistinguishable.

Both are cured by ranking each field to its **ECDF percentile** and averaging those. Every
field then contributes the identical symmetric [0,1] range (worst 0, median 0.5, best 1),
and there are no clip ties. **Percentile needs neither winsorising nor the ±3 clip — rank is
inherently outlier-proof** (an extreme just gets rank ≈ 1.0; it cannot dominate a mean, and a
monotone clip wouldn't change a rank anyway). This is the answer to "do we clip for pctile?"
— **no, and we don't need to.**

The cost of a rank scale is that it is **nonlinear** (compresses tails, stretches middle:
€40k and €15k both sit near 1.0) and turns Pearson into rank correlations. So we keep **both**:

- **z** — the analytical scale: linear σ-distances, unit variance, the factor-analysis input.
- **`<topic>_numeric_pctile`** (mean of per-field percentile ranks, oriented so higher = more
  generous), **`<field>_prank`** (per-field rank), **`<topic>_coverage_pctile`**,
  **`overall_numeric_pctile`**, **`coverage_overall_pctile`**, **`<topic>_pctile`** (combined),
  **`overall_pctile`** (combined, PRIMARY) — the equal-range, bounded, thesis-friendly 0–1
  generosity scale where every field counts equally. Computed in `magnitude_pctile()`
  (`index_lib.py`).

The two scales rank docs similarly (corr ≈ 0.77 Pearson / 0.76 Spearman on the overall
composite) but **not identically** — the gap is exactly the skewed fields z under/over-weights.
§9 of `ADVANCED_ANALYSIS.md` re-runs the factorability + across-topic EFA on the percentile
scale and confirms the multivariate *structure* is unchanged (KMO ≈ 0.54, same weak-factor
verdict), so the pctile track is a safe alternative *scale*, not a different *model*.
`check_battery.py §4` asserts every rank column is in [0,1] and agrees in direction with its z.

**What we still do NOT do:** make **min-max** the core scale (its endpoints *are* the two
outliers → one bad value defines the ruler; §3.1), or drop the ±3 clip from the **z** track
(it caps outlier influence in the *mean*, where rank's robustness doesn't apply).

### 3.3 Stratified standardisation — one field, several incompatible units

**The problem.** A few fields report the *same concept* in **incompatible units** that can't be
unit-converted without external data. The clearest case is **`training_budget`**: some CAOs give
it in **€** (703 docs), some as **% of the employee's salary** (an individual budget, 175), some
as **% of the employer's wage sum** (a collective sector-fund levy, 296). You cannot z-score €750
against "1 % of salary" — they're different quantities, and converting either way needs a salary
or a payroll figure the row doesn't carry (the no-invent rule forbids it).

**The fix — standardise WITHIN each base.** Compute a *separate* yardstick μ/σ (and ECDF) for each
unit-base, and score each doc against **its own base**:

```
  budget_z(doc) = clip( (value − μ_base) / σ_base , ±3 )        # base = the doc's own unit
```

Because every base is centred to mean 0 / sd 1, the results become **comparable**: a **+1σ in €**
and a **+1σ in %-of-salary** both mean "1σ above average generosity *for that base*." Same for the
percentile (rank within base) — 90th-percentile-€ and 90th-percentile-%-salary are both "top-10%
budget generosity." The topic then averages this **one** `budget_z` like any other field. Verified
empirically: each base standardises to ≈mean 0.

**Design choices:**
- **One output, not three.** The split is *internal*; the doc emits a single `budget_z` /
  `budget_prank` (its base's score). (`il.stratified_zpr`; `build_simple_index(stratified=…)`.)
- **"% of wage sum" stays in magnitude** as the 3rd base. Each doc has exactly one unit, so the
  three bases are **disjoint sets of docs** — no within-field double-count. Its overlap with the
  `training_fund_present` boolean is existence-vs-amount (the two-track split, §5), not a duplicate.
- **Genuine absence → floor.** A doc with no budget in any base *and* a consistently-absent gate
  (training rights never True in the CAO) is a real "no training budget" → floored (z = −3,
  prank = 0, bottom); a doc that *has* training but didn't quantify the budget stays available-case.
- **`training_cost_reimbursement`** is scored **%-only** (a reimbursement rate); its 183 €-valued
  rows are misfiled budget amounts → captured in the descriptive companion
  `training_cost_reimbursement_eur` (not scored — € reimbursement isn't comparable to % without the
  total cost).

Why not just pick one unit and drop the rest? That would discard ~40 % of stated budgets (the
non-€ ones) — exactly the kind of silent loss §8 warns against. Stratification keeps every stated
budget, on one comparable scale.

---

## 4. Coverage scoring

Coverage = **fraction of the topic's presence booleans that are True** (0/n … n/n), then
that 0–1 share is itself pooled-z'd (`coverage_pooled_z`, same yardstick dedup). A False
counts as 0 (a provision the CAO chose not to include), a True as 1.

### 4.1 Why a fraction, not per-boolean z's — Reason 1: z-scoring booleans blows up on rare values

If we z-scored each boolean individually and averaged them (the "pool everything" idea), a
**near-constant** boolean detonates the mean. Example — `training_has_training_rights` is
98.6% True, so σ≈0.118; the rare False lands at **(0−0.986)/0.118 = −8.3**, clipped to −3.
A single yes/no would then swing the topic as hard as three amounts combined — backwards,
because the *most common* provision should carry the *least* discriminating weight.
Coverage-as-a-fraction avoids this entirely: a rare flag is just one term in a fraction and
can't dominate.

---

## 5. Why two separate tracks, not one overall mean of all variables

### Reason 2 — the amount/breadth split would be an accident of the schema
With one pooled mean, the amount-vs-breadth weighting is set by **how many fields of each
type the extractor happened to create**. Training (3 numerics + 3 booleans) would be ~50/50;
bonus (2 numerics + 8 booleans) would be ~20/80. That split isn't a decision — it's a
schema artefact. Two tracks make it a **principled 50/50** in every topic (and a knob you
can reweight).

### Reason 3 — the tracks have different missing-data meaning
A **numeric** can be genuinely *missing* (present-but-unquantified → skipped, available-
case). A **boolean** is almost always *observed* (True/False). Pooling forces one rule
across both; separate tracks let magnitude do available-case skipping while coverage does
fraction-of-present.

### Reason 4 — interpretability
Separate tracks are *why* we can tell "flatly absent" (low on both) from "present but thin"
(low amount, decent breadth). Collapsed into one number, that diagnostic is lost.

**Cost, stated honestly:** for the handful of zero-filled EXTRA benefits (§8) the *presence*
shows up in both tracks, so the combined score modestly over-weights them and
corr(magnitude, coverage) rises. Accepted as the price of losing no information (§8.2).

---

## 6. Signs & TOTAL/EXTRA semantics

Every field enters in the **worker-generosity direction** (`SIGN_EXPECT`, audited by
`check_battery.py` §2d — currently 40/40 correct):
- `+1` where more = better (vacation days, notice, allowance %, accrual rate…).
- `−1` where less = better (probation length, franchise, employee contribution %,
  early-retirement age, full-time hours, ketenregeling duration…).

Field **semantics classes** (`field_semantics.py`, tested in `check_battery.py` §2b):
- **TOTAL_HARD** — a statutory hard minimum (`floor_lift`): blank → floor **and** a stated
  below-floor value is lifted to the floor.
- **TOTAL_SOFT** — statutory `floor`/`default` (blank-filled only, a stated deviation is
  kept) or `cap` (blank stays empty; above-cap masked).
- **TOTAL_OPEN** — no statute on the field.
- **EXTRA** — an above-statutory perk, baseline 0, never statutory-imputed (the zero-fill
  candidates, §8).

---

## 7. Statutory handling ("empty = statutory" — but only where a statute exists)

For TOTAL fields with an era-aware statutory value (`out/statutory_all.csv`, derived from
Hanna's hand-maintained `review/statutory_timeline.xlsx` — **ground truth, never regenerated**):

- **floor / default** → impute into blanks in the `full` variant (the worker gets at least
  the law), keep a stated value as-is.
- **floor_lift** → impute into blanks **and** lift a stated below-floor value to the floor.
- **cap** → blank stays empty; a stated above-cap value is masked (the law caps it).
- **informational / none** → nothing imputed.

Only **12 of 41** fields fill blanks; 29 leave them empty (`out/field_statutory_behavior.csv`).
We **never invent** a value: source silent → empty; "statutory" with no figure → empty; no
unit conversion that fabricates a number.

### 7.1 Pension is the hard exception — available-case ONLY (⛔ never impute)
**Why the blanks are not absences.** In the Netherlands most sectors have a *bedrijfstakpensioenfonds*
(industry-wide pension fund — ABP, PFZW, PMT, bpfBOUW, PME, …) with **mandatory participation**
(*verplichtstelling*, Wet Bpf 2000). Where such a fund exists, it — not the CAO — sets the
contribution split, accrual rate, franchise and retirement age; the CAO simply points to the
fund and states no figures of its own. So a blank pension cell means **"governed by the fund,
terms not restated in the CAO"** (deferral), **not** "no pension" and **not** "the statutory
minimum". A large share of a worker's pension generosity is therefore ruled *via the fund* and is
invisible to a CAO-text extractor by construction.

**Consequences for scoring.** pension fields are only 14–38% filled, so we do **not** impute
statutory, zero, or normal-age values into the blanks. The pension **numeric** index is strictly
available-case — it reflects only the minority of CAOs (often company schemes / *excedent* top-ups)
that state their own terms, and is **not** representative of the sector-fund majority. This is a
**hard rule** recorded in `MEMORY.md` and `indices-workstream-state.md`.

**Do we capture the fund at all?** Partly, on the COVERAGE track, not the numeric one. There is no
pension-fund *name* variable, but `pension_mandatory_participation` (participation in a mandated
fund) and `pension_excedent_present` are coverage booleans, and `pension_pension_type` (DB/DC/hybrid)
is carried descriptively. Caveat: in *verplichtgesteld*-bpf sectors participation is mandatory **by
law** even when the CAO is silent, so even the coverage flag *understates* how many workers are in a
fund (a `statutory_all` informational row notes this). The honest summary: fund **existence** is
weakly proxied by coverage; fund **generosity levels** are simply not in the CAO text and are left
blank rather than guessed.

### 7.2 Reading rule — when is a numeric field blank, statutory-filled, or zero-filled?
At-a-glance decision for any magnitude cell (the full worked logic is `il.field_variant` +
`il.apply_zerofill`; the per-field behaviour is in `out/field_statutory_behavior.csv`):

| situation | what the cell gets | why |
|---|---|---|
| CAO states a number | the stated value (clamped; `floor_lift` also lifts a below-floor value to the floor) | the source is authoritative |
| blank **and** field has a statutory `floor`/`default`/`floor_lift` at that date | the **statutory value** (12 of 41 fields) | the worker is legally guaranteed at least the floor |
| blank **and** field is an **EXTRA** perk **and** its presence boolean is **False** | **0** (zero-fill) | boolean confirms genuine absence → least generous, rankable |
| blank **and** field is an EXTRA perk **and** its presence boolean is **True** | **left blank** (available-case) | present-but-unquantified — dropped from the mean, never guessed |
| blank **and** field is EXTRA with an unconditional fill (`term_severance_extra`) | **0** | a blank here reliably means "none" |
| blank **and** field is `cap`/`informational`/`none` | **left blank** | law caps or says nothing scalar |
| blank **and** topic is **pension** | **always left blank** (§7.1) | fund-deferral, not absence — never impute |
| blank with no rule above | **left blank**, dropped from the available-case mean | never invent a value |

Two invariants behind the table: **(1)** a blank is only ever turned into a number when either a
statute guarantees it or a boolean *confirms* its absence — never on a bare guess; **(2)** pension
overrides every fill rule (available-case only).

---

## 8. Zero-fill (turning genuine absence into a rankable 0)

### 8.1 What and when
An **EXTRA** amount field's blank becomes **0** (genuine absence = least generous) **only
when a presence boolean says the benefit is absent** — `apply_zerofill(df, spec)`:
- `(field, boolean)` → blank → 0 **iff** the boolean is False; blank with boolean True stays
  NaN (present-but-unquantified, available-case).
- `(field, None)` → blank → 0 unconditionally (only where a blank reliably means "none",
  e.g. `term_severance_extra`).

The pre-zerofill score is kept as `<topic>_numeric_z_availcase` for comparison. Adding the zeros
shifts the pooled μ/σ slightly, so *every* doc's z re-centres a touch — expected.

Gating on the **False side is safe regardless of the boolean's skew** (rights=False is an
unambiguous absence). The "<90%-present / discriminating" heuristic governs **materiality**
(does the fill move enough rows), **not safety**.

Currently zero-filled (9 fields): bonus 13th-month, fringe meal/relocation/commuting,
homeoffice stipend, overtime shift-allowance, training time/budget/reimbursement.
`term_severance_extra` is the one unconditional fill.

### 8.2 The trilemma (why two-part, not disjoint or imputation)
Combining a presence boolean with an amount is **semicontinuous / zero-inflated** data. You
can have any **two** of three properties, never all three:

| approach | no invented numbers | one combined score, each benefit counted once | no info lost |
|---|---|---|---|
| **Two-part (what we do)** | ✅ | ✗ (presence in both tracks) | ✅ |
| **DISJOINT** (drop gate boolean from coverage) | ✅ | ✅ | ✗ (loses present-but-unquantified) |
| **Impute the amount** | ✗ | ✅ | ✅ |

The project's "never invent values" rule kills imputation. Between the other two we chose
**two-part**: keep the presence booleans in coverage *and* zero-fill the amount in
magnitude. DISJOINT was tried and reverted because it dropped the present-but-unquantified
group (e.g. fringe meal: 42 CAOs / 17% have a meal benefit but state no amount — they'd
vanish from both tracks). The cost of two-part (a benefit's presence sits in both tracks) is
the accepted price of losing nothing.

---

## 9. Composite / combining across topics

Published names follow `indices/NAMING.md` (`{t}_z` = **combined**, `{t}_numeric_z` = magnitude).

- `{t}_z` = mean(`{t}_numeric_z`, `{t}_coverage_z`) — the per-topic **combined** headline.
- **`overall_z`** (PRIMARY) — equal-weight available-case mean of the **combined** scores over
  the 10 dual topics' `{t}_z` + `wage_median_z` + `safety_coverage_z` + `childcare_coverage_z`
  (13 inputs; `ai` excluded — see §12, 2026-07-07). Averaged over only the topics a doc is
  scored on (`n_topics_combined`).
- `overall_z_var` — variance across those **combined** inputs = how *uneven* (lopsided) the
  package is. Pairs with `overall_z`.
- `overall_numeric_z` / `overall_numeric_z_var` — the NUMERIC-only companion mean and its
  matching variance (magnitude track only; excludes coverage).
- `coverage_overall` / `_z` — provision breadth across the coverage topics.
- Equal weights are one transparent default; every component is exposed to reweight.

Wage joins by `(cao_number, file-year)` from `out/mw_indices.csv`. Wage enters `overall_z` as the
**nominal** `wage_median_z` (z of `mw_median` EUR); WML-normalised quantities live ONLY in the
`ratio_*_wml` value columns (see NAMING.md "Wage" section).

---

## 10. Per-topic reference (fields, signs, coverage, special handling)

| topic | magnitude fields (sign) | coverage booleans | statutory | special |
|---|---|---|---|---|
| **parental_leave** | FRE-weeks bundle (maternity/paternity/adoption/parental top-up), +1 | leave provision booleans | floor per type, imputed (`_with_statutory` variant) | separate FRE builder; unpaid=0, partial×rate |
| **absence** (Tier-1) | vacation days, holiday %, sick-pay % & weeks, short/long care days/weeks & pay, all +1 | — (its booleans live in leave coverage) | floors imputed (20 days, 8% vakantiegeld, 70% sick pay, WAZO care) | best-filled unused fields; sick-pay stores year-1 tier (C3) |
| **term** | employer notice(+), notice range-max(+), probation fixed/indef(−), notice floor(+), severance-extra months(+) | notice-tenure, sick-dismissal-prot, WW-supplement | none scalar | severance-extra **unconditional zero-fill**; EUR/% severance → descriptive; sick_dismissal_prot partly restates BW 7:670 |
| **contract** | keten max-contracts(−), keten max-duration(−), full-time hours(−), workhours-adjust tenure(−) | conversion-right, workhours-adjust-right, part-time-allowed | ketenregeling default (3 contracts; 36→24→36 mo) imputed | part_time_allowed partly restates WAA/Wfw; zero-hour/minmax excluded (ambiguous sign) |
| **overtime** | pay: allowance%(+), allowance-max%(+), shift-allowance-min%(+), unfavourable%(+), trigger daily/weekly h(−); protection: min-rest h(+, floor 11), max h/wk(−, cap 60), max h/day(−, cap 12) | derived comp-choice only | ATW 11h rest floor | shift-allowance zero-filled (gated on shift_allowance_present); pay & protection sub-scores |
| **training** | time days/yr(+), **budget (STRATIFIED: €/%-salary/%-wage-sum, §3.3)**(+), cost-reimbursement %(+) | fund-present, mandatory-paid, **has-training-rights** | none scalar (mandatory-paid free is 2022 law, informational) | zero-filled gated on has_training_rights; budget standardised within-base → one budget_z; reimbursement %-only + €-companion; **has_training_rights in coverage 2026-07-07** (§12) |
| **bonus** | 13th-month % of annual(+), fixed annual lump €(+) | profit-share, performance, qual, retire-gratuity, seniority, job-allowances, sign-on, **13th-month** | none | 13th-month zero-filled gated on the 13th-month bool; lump %-bucket → descriptive |
| **fringe** | commuting €/km(+), meal €(+), relocation €(+) | bike, internet/phone, health-insurance, insurance/savings, certs-paid, **meal/relocation/commuting present** | none | all 3 amounts zero-filled gated on their present-booleans; per-month commuting → descriptive |
| **homeoffice** | stipend €/month(+) | has-WFH-rights, costs-reimbursed, **stipend-present** | none | stipend zero-filled gated on stipend_present; **entitlement (WFH days) DROPPED** (1.3% filled, degenerate) |
| **pension** | employee-contrib %(−), accrual %(+), franchise €(−), early-retire-age(−) | excedent, accrual-in-stat-leave, accrual-illness-y2, premium-50/50, mandatory-participation | ⛔ **NONE — available-case only, never impute** (§7.1) | retire-age-normal descriptive (statutory AOW); type (DB/DC/hybrid) & *_range not scored (no clean generosity direction / too sparse); accrual=100% clamp catches Generatiepact misread |
| **childcare** | — (all <2% filled) | support, in-house, discount, priority-access | — | coverage-only; ~8% of CAOs mention support |
| **safety** | — (no numeric fields) | 12 provisions (harassment, integrity, counsellor, RI&E/PSA, arbodienst, checkups, workload, wellbeing…) | — | coverage-only; several Arbowet-mandated → coverage understates legal floor |
| **ai** | — (~1% filled) | policy-exists, governance-body, training-rights | — | coverage-only, near-empty; nascent (2024+) |
| **wage** | `wage_rel_z` (vs WML), `wage_low_rel_z` (low earners) | — | WML floor by year (`ratio_low_wml`=1.0 by def.) | separate salary-parser track; joins by (cao, year) |
| **statutory** | the law scored as its own pseudo-file per calendar month | — | *is* the statute | leave/contract/overtime/wage have a scoreable floor; term/pension/training/bonus/fringe/homeoffice/childcare/ai have no scalar statute → no statutory score by design |

Descriptive-not-scored fields (kept as context, excluded from z): a field is descriptive
when it has **no clean generosity direction** (categorical like DB/DC/hybrid), is **mostly a
restatement of law** (pension retire-age-normal = AOW), or is **too sparse/redundant**
(pension *_range, ~13%). These aren't defects — they lack a scoreable direction.

### 10.1 Full inventory — every magnitude field (sign) and every coverage boolean

**Magnitude fields** (sign in brackets; higher score = more generous):
- **leave** — per-type FRE weeks: paternity(+), adoption(+), parental(+). *maternity excluded*
  (statutory-constant); `leave_z_fre_total` = summed-FRE companion.
- **absence** — vacation_time(+), vacation_bonus(+), sickpay_continuation(+), sickpay_duration(+),
  short_term_care(+), short_term_care_pay(+), long_term_care(+), long_term_care_pay(+).
- **term** — employer_notice(+), employer_notice_range_max(+), probation_fixedterm(−),
  probation_indef(−), notice_min_floor(+), severance_extra(+).
- **contract** — ketenregeling_max_contracts(−), ketenregeling_max_duration(−),
  full_time_hours(−), workhours_adjustment_tenure_requirement(−).
- **overtime** — *pay*: allowance(+), allowance_range_max(+), shift_allowance_range_min(+),
  unfavourable_hours_allowance(+), trigger_daily(−), trigger_weekly(−); *protection*:
  min_rest_between_shifts(+), max_hours_per_week(−), max_hours_per_day(−).
- **training** — time_yearly(+), budget(+, STRATIFIED across €/%-salary/%-wage-sum — §3.3),
  cost_reimbursement(+, %-only; €-rows → `training_cost_reimbursement_eur` companion).
- **bonus** — thirteenth_month_amt(+), fixed_annual_lump(+).
- **fringe** — commuting_allowance €/km(+), meal_benefit €(+), relocation_allowance €(+).
- **homeoffice** — stipend €/month(+).
- **pension** — employee_contrib(−), accrual_rate(+), franchise(−), retirement_age_early(−).
- **wage** — wage_rel_z (mean vs WML), wage_low_rel_z (low earners). *(salary track.)*
- **absence-magnitude only**; **safety / childcare / ai** have NO magnitude fields (coverage-only).

**Coverage booleans** (every one; presence = True counts toward breadth):
- **leave** (12) — has_leave_enhancements, has_above_statutory_maternity,
  paternity_explicitly_above_statutory, parental_eligibility_present, parental_topup_present,
  abortion_present, sick_topup_present, sickpay_extra_insurance_present, care_topup_present,
  liberation_day_annual, liberation_day_lustrum, extra_seniority_present.
- **term** (3) — notice_tenure_present, sick_dismissal_prot, severance_ww_supplement.
- **contract** (3) — conversion_rights_temp_to_perm_present, workhours_adjustment_right_present,
  part_time_allowed.
- **training** (3) — fund_present, mandatory_training_paid, has_training_rights.
- **bonus** (8) — profit_sharing_present, performance_bonus_present, qual_bonus_present,
  retire_gratuity_present, seniority_loyalty_bonus, job_allowances_present, sign_on_bonus_present,
  thirteenth_month.
- **fringe** (8) — bike_scheme_present, internet_or_phone_reimbursement_present,
  health_insurance_support_present, insurance_or_savings_benefit_present,
  mandatory_certifications_paid, meal_benefit_present, relocation_allowance_present,
  commuting_allowance_present.
- **homeoffice** (3) — has_homeoffice_rights, costs_reimbursed, stipend_present.
- **pension** (5) — excedent_present, accrual_stat_leaves, accrual_illness_y2, premium_eq_split,
  mandatory_participation.
- **safety** (12) — harassment_protocol_present, integrity_protocol_present,
  confidential_counsellor_present, reporting_channel_external, safety_training_present,
  safety_committee_present, rie_psa_required, psa_prevention_measures_present,
  arbodienst_access_provided, preventive_medical_checkup_present, workload_monitoring_present,
  wellbeing_program_present.
- **childcare** (4) — childcare_support_present, inhouse_present, discount_present, priority_access.
- **ai** (3) — ai_policy_exists, ai_governance_body_present, ai_training_rights_present.
  *(ai excluded from the overall roll-up; columns still produced.)*
- **overtime / absence** — no coverage booleans (overtime carries only a derived comp-choice flag).

---

## 11. Verification (`check_battery.py`, must stay green)

1. converter self-test (35/35) · 2. field sanity · 2b. TOTAL/EXTRA semantics (40/40) ·
2c. statutory-behaviour table · 2d. sign audit (40/40) · 3. salary unit-class ·
**4. percentile-track sanity** (every `_prank`/`_pctile`/`_rankpct` in [0,1]; each
`{t}_numeric_pctile` agrees in direction with its z — all OK). Also: fringe/training
determinism, availcase-vs-zerofill deltas, corr(magnitude, coverage).
*(NB: `check_battery.py`'s §4 comments/print text still say "gen01" — cosmetic only; the
columns it actually checks are the `_pctile`/`_prank`/`_rankpct` family.)*

### 11.1 Design notes — how to read the outputs correctly (not bugs)
Three properties of the pipeline that surprise readers but are intentional:

1. **The multivariate analyses run on document-editions, not deduped terms.** §1–§4/§7's factor
   analyses, correlations and Bartlett test use all ~2,698 full-CAO editions (one row per file),
   while the pooled-z *yardstick* is term-deduped (~1,552). So a frequently-reissued CAO is
   weighted more, and **inferential statistics overstate independent evidence** — read Bartlett's
   χ²/p and KMO as *descriptive*, not as hypothesis tests. (The per-doc z's themselves are fine;
   only the cross-doc inference is affected.) Dedup would, if anything, *weaken* the already-weak
   common factor.
2. **z-rank and pctile-rank of the same topic legitimately differ.** The z track averages
   per-field signed z's; the pctile track averages per-field ECDF ranks. For a
   multi-field topic these are different aggregations, so `{t}_numeric_z` and `{t}_numeric_pctile`
   can rank docs differently (e.g. absence rank-corr ≈ 0.67). Both are correct; use z for
   σ-distances/factor analysis, pctile for a bounded equal-weight 0–1 rank (§3.2).
3. **FAMD boolean weighting is exact only for rare provisions.** The FactorMixed tab scales each
   boolean by (d−p)/√p using only the *positive* dummy, so a provision contributes variance ≈ 1−p:
   a *rare* provision sits on equal footing with a unit-variance numeric (the goal), but a *common*
   one is slightly down-weighted vs canonical two-dummy FAMD. Boolean blanks are read as False
   ("not extracted" = "absent", matching the coverage convention). A few per-topic loadings are
   boundary Heywood cases (communality = 1.0 → that "factor" is essentially one variable) — legal,
   but don't over-read a single-variable factor.

---

## 12. Decision Log (append newest at top; edit the relevant section above too)

- **2026-09-22 (REVERTED: the `n_topics_scored >= 8` ranking-exclusion gate — Hanna).**
  An automated report-refinement pass had added a coverage-quality gate to
  `Reports/Analysis/scripts/04_indices.py` that dropped any document scoring on fewer than
  8 of the 13 domains from the top/bottom generosity ranking. It excluded 38 documents and,
  at CAO level, exactly one agreement (CAO 3690, Stichting BibliotheekWerk — all 12 editions
  at exactly 7 topics), taking the rankable set from 242 to 241. REVERTED because the
  threshold was never independently justified *for ranking*: it was borrowed from
  `ADVANCED_ANALYSIS.md`, where `>=8` exists for a different reason (correlation-matrix
  completeness for the factorability/KMO tests). The gate was also never logged here, unlike
  every comparable modelling call. It affected no z-score, percentile, factor loading or
  correlation — `04_indices.py` only reads `composite_index.csv`. The whole ranking block was
  removed rather than just the gate, because the top/bottom table it fed is no longer
  included in the report (its `\ref` in `Indices.tex` had become a dangling "Table ??").
  The `thin_doc` exclusion is unaffected and still applies elsewhere. §11.

- **2026-09-21 (panel `leave_numeric_z`: per-type, closing the 2026-07-07 TODO).**
  `build_panel_monthly.py` still scored the panel's `leave_numeric_z` from the summed-FRE
  method (`fre_total_with_statutory` params) after `parental_leave_index.py` moved the
  composite to per-type (paternity/adoption/parental, equal-weight, 2026-07-07) — the same
  column name meant two different constructions depending on which output file you read.
  FIXED: panel `leave_numeric_z` now averages three per-type z's, each `max(CAO-extracted,
  statutory floor for that type) `re-scored monthly per the law-month rule, using the
  already-persisted `{type}_fre_stat` params — identical formula to the composite, just
  evaluated every panel month instead of once per document. The old summed-FRE quantity is
  kept, not deleted, as an unscored reference column `leave_fre_total_monthly`. §5, §10.

- **2026-09-17 (Reports/Analysis: "latest-CAO cross-section" vs. "active in-force stock" —
  Hanna caught the misnomer).** `Non-Salary.tex` introduced "latest-CAO cross-section" as the
  242-row, one-row-per-CAO object ($N=242$, `build_latest_cao_view`), then labelled every
  subsequent `_latest_cao_view` figure the same way — but those figures are the CAO×year
  forward-filled panel (`build_latest_cao_forward_fill_by_file`; 3,621 rows, active-CAO count
  rising 1→242 over 2004–2026), a different object. DECISION: reserve "latest-CAO cross-section"
  for the true $N=242$ objects (Table 1 / point-share macros); rename every panel figure's
  title/caption to "active in-force stock" (matching `General.tex`'s pre-existing term for the
  same construction). Also fixed: panel figures' x-axis label was hardcoded "Contract start
  year" even though the axis is calendar year of active in-force status — now conditional on
  `use_latest_cao_view`; two figures (`indices_wage_ladder_vs_wml.png`,
  `salary_boolean_shares_by_contract_year*.png`) were missing `enforce_integer_year_axis` and
  could render fractional-year ticks (confirmed on the wage-ladder figure: `2010.0, 2012.5, …`)
  — both now call it. Verified presentation-only: pixel-diffed all 54 report figures before/after
  in `Reports/Analysis/.bak_figures_2026-09-17/`; the 26 changed figures differ only in isolated
  title/x-label pixel bands (or, for the two tick fixes, gridline positions) — no data-line pixel
  changed. `General.tex` itself was not edited (already used the correct term).

- **2026-07-15 (AgreementLevel export: overall scores WITHOUT wage; wage block removed —
  Hanna).** At document grain the wage columns were a borrowed CAO-year fact: the cao×year
  ladder pools scales from several files (a March-filed doc's year row can contain a raise
  filed in August), so per-document wage attribution was wrong in both directions. DECISION:
  cao_agreement_level.csv / AgreementLevel tab now carries NO wage columns (wage_*, mw_*,
  ratio_*_wml, wml_month, wage_src_year all dropped) and its overall roll-ups exclude wage,
  renamed with the explicit suffix `_without_wage` (overall_z/pctile/z_var,
  overall_numeric_z/pctile/z_var, n_topics_* counts; 12 non-salary inputs = 10 dual topics'
  combined + safety & childcare coverage). Guarded: the build asserts that re-adding
  wage_median to the mean reproduces the composite's wage-inclusive roll-ups (<5e-4).
  PanelMonthly is UNCHANGED (wage-inclusive overall_z, plain names) — wage joins the panel
  at its natural cao×year grain from the Wage companion tab. The deliberate name split
  prevents cross-tab comparison of two differently-defined overalls. composite_index.csv
  itself unchanged (still wage-inclusive; the panel reads it).

- **2026-07-15 (statutory-response robustness on the two new date axes).**
  build_panel_monthly gained axis="retro" (per-document Date_retro_datum from the
  agreement-level export: explicit general_retro_start_date where stated, else the
  first-file rule, else file_date) alongside axis="first"; statutory_response_report
  gained a suffix param. Core specs re-run on all three yearly panels: leave d_stat β =
  1.08 / 1.06 / 1.05 (file / first / retro), leave change-year event mean Δz = 0.383 /
  0.378 / 0.378, contract β = 0.278 / 0.276 / 0.277 — the "co-movement is mechanical,
  no behavioural response" conclusion is date-axis-invariant (expected: the law-month
  re-scoring channel dominates regardless of when documents start). Suffixed outputs
  kept in out/ (all_indices_panel_*_{first,retro}.csv, statutory_response_results_*.csv);
  still not part of the default rebuild.

- **2026-07-15 (workbook slimmed: Composite + PanelMonthlyFirst tabs retired — Hanna).**
  PanelMonthlyFirst deleted as redundant: its rule is exactly the export's
  `Date_first_is_Ingangsdatum` column (build_panel_monthly keeps axis="first" as a
  capability, no longer built by default; _first CSVs → out/_superseded/). Composite tab
  deleted after a machine check that AgreementLevel is a STRICT SUPERSET: identical 2,698
  id rows, every Composite column present (export's score block widened by the 8 it
  missed: overall_z_var, overall_numeric_z_var, coverage_overall, n_topics_scored,
  n_topics_combined, term_group, document_type, wage_src_year), and 0 cell mismatches
  across all shared columns. composite_index.csv itself is unchanged — it remains the
  pipeline's per-document build artifact (panel + export read it); only the workbook tab
  went. Workbook now 26 tabs; Deck/dictionary/docs updated.

- **2026-07-15 (PanelMonthlyRetro → PanelMonthlyFirst; export gains two ready-made
  start-date columns — Hanna).** RENAME: the second panel axis is now `PanelMonthlyFirst`
  (build_panel_monthly axis="first", out/all_indices_panel_monthly_first.csv + yearly +
  aggregates; TAB_RENAMES maps the old workbook tab name so a saved manual order survives).
  Reason: the rule backdates only the TERM'S FIRST file to its ingangsdatum; it does NOT
  apply the explicit retroactivity dates, so "Retro" over-claimed. NEW COLUMNS in
  cao_agreement_level.csv / AgreementLevel tab, next to file_date+ingangsdatum, both ISO:
  (1) `Date_first_is_Ingangsdatum` = the PanelMonthlyFirst rule per document
  (term_edition_seq 1 → ingangsdatum, later editions → own file_date; 0 empties);
  (2) `Date_retro_datum` = same, overridden by the EXPLICIT general_retro_start_date from
  the corrected dataset where present (556/2,698 rows; 393 actually differ — mostly later
  editions whose retro clause the first-file rule can't see), else falls back to (1).
  Verified: 0 rule violations either column, 0 unparseable retro dates. Old _retro CSVs
  moved to out/_superseded/.

- **2026-07-15 (Composite = PanelMonthly mirror; agreement-level export; folder reorg).**
  Hanna: (1) Composite tab/CSV now carries every PanelMonthly column that exists at document
  level — full wage block (mw_low/median/mean/high + all four ratio_*_wml + wml_month, AS-OF
  joined by file-year like the panel) and `score_src_id` (= scores_carried_from else own id);
  only month/in_force_id/months_since_file/is_stale/n_caos_in_month remain panel-only
  (panel mechanics). Both tabs share the same `_dashboard_order` layout. (2) NEW
  `out/cao_agreement_level.csv` = `AgreementLevel` tab (build_cao_level_export.py, v2): one
  row per DOCUMENT organized by agreement term with term_edition_seq / n_editions_in_term /
  is_last_filed_in_term — NOT collapsed per term (Hanna: within-term files share one
  ingangsdatum, mostly wage-table updates; collapsing loses the exact panel). Both date axes +
  retro/AVV/signing fields travel per row. (3) NEW RETRO-AXIS PANEL `PanelMonthlyRetro` =
  out/all_indices_panel_monthly_retro.csv (build_panel_monthly axis="retro", HANNA'S RULE):
  the term's FIRST file is backdated to month(ingangsdatum) — the initial text is what
  retroactively governs (terugwerkende kracht standard for bound employers; AVV never
  retroactive) — while LATER files keep their file-month start (mid-term updates apply
  forward, not backwards). Explicit general_retro_* clauses cover only ~25% of records
  (671/2,739; 567 dated) → ingangsdatum is the robust default anchor; the retro fields travel
  in the export for finer designs. PanelMonthly stays the filed-and-enforceable view;
  pub_lag_months quantifies the divergence. (4) Folders reorganized:
  indices/{out,corrections,review}/ + qa/backups/; ~30 scripts + 13 docs path-updated,
  rebuild verified green twice. Workbook: new tabs now insert at their TABS-list position
  (PanelMonthly → AgreementLevel → PanelMonthlyRetro → Composite); a manual reorder in Excel
  still survives rebuilds. Plain-language companion doc: root `ReadMe_for_Hanna.md`.

- **2026-07-13 (L31 — RESTORATIVE RE-VERIFICATION COMPLETE; the audit's follow-up executed).**
  All 1,628 standing restorations (False→True + blank→fill) re-judged with the field-boundary
  question primary; 12 readers → 5 evidence-tier-aware gates → 372/426 disputes confirmed →
  516 cells corrected (`L31_field_boundary`). The audit's 5/11 restorative sample rate implied
  ~45%; the full pass measured **25% flip rate** (412/1,628 proposed, 372 confirmed ≈ 23%) —
  the sample overestimated but the direction held. TWO DOCTRINE LESSONS:
  (1) **Evidence-tier must travel with every correction.** 54 gate rejections were nearly all
  reader false-alarms declaring markdown-tier fixes "unsourced" after checking only the extract;
  the gates found every disputed quote verbatim in the parsed markdown. Any future re-verification
  worklist MUST carry the original fix's evidence tier, and judges MUST check at that tier.
  (2) **The boundary question generalizes**: confirmed systemic mis-routings now named in the
  extractor prompt candidates — ploegentoeslag ≠ job allowance, calamiteitenverlof ≠ care leave,
  verzuim ≠ werkdruk, PMO/PAGO ≠ arbodienst, niche groups ≠ typical worker,
  premium_eq_split = future-changes clause only. Open schema-boundary rulings for Hanna:
  job_allowances vs qual_bonus (the schema's own example is diploma-based), safety_committee
  vs sectoral arbo arrangements (extractor probe bundles them). Post-state: coverage 0 uncovered,
  census 1 verified drop, flip-rate 3.3%.

- **2026-07-13 (L30 — INDEPENDENT AUDIT VERDICT: the campaign's weak side is the RESTORATIVE
  direction).** End-to-end verification of the whole campaign: (1) mechanical — every one of
  the 5,614 logged changes replays exactly into the dataset, the dataset differs from the
  pre-L20 backup ONLY where a log says so (0 unlogged), churn 0.9%, all 17 A→B→A reverts
  audited clean; (2) statistical — changes ran 2,004 remove-phantom vs 1,700
  restore/recover (not a one-sided purge), overall_z mean −0.007; (3) substantive — 39-cell
  stratified sample re-derived by 2 fresh auditors, 10 disputes ruled by Opus. **Sample error
  rates by direction: remove True→False 0/14, numeric 0/2, clear 1/5, fill blank→value 2/7,
  restore False→True 5/11.** The phantom-removal machinery (never-derive + statutory + scope
  discipline) is sound; restorations over-credited related-but-wrong clauses (doctor-visit
  hours ≠ care leave; verzuim registration ≠ workload monitoring; ploegentoeslag ≠ job
  allowance; PersVeilig project funding ≠ safety training; 55+ clause ≠ general right).
  n is small (wide CIs) but the direction signal is one-sided. CONSEQUENCES: (a) 8+2 cells
  repaired (`corrections/l30_corrections_applied.csv`); (b) queued follow-up: re-verify the ~1,700
  restorative cells (False→True + blank→fill) with an explicitly field-boundary-focused gate
  ("does this clause satisfy THIS field's definition, or a sibling field's?"); (c) 506009
  excedent re-ruled False by Opus (in-document Bijlage V content suffices — the fund-deferral
  blank rule applies only when the document itself carries NO relevant scheme content);
  (d) 187-family and PSA-vs-RI&E field-boundary review items added to the open file. Auditor
  fallibility note: the auditor was itself wrong twice (1646007 sibling-import claim refuted
  verbatim-in-document; 118017), so single-auditor DISAGREEs are proposals, never auto-applied.

- **2026-07-13 (RULING — pension booleans under FULL scheme deferral → BLANK, not False).**
  Hanna's challenge on 506009 (`pension_excedent_present`): when a CAO regulates pension
  entirely "bij afzonderlijke cao" with texts request-only, the existence of an
  excedentregeling is UNKNOWABLE from the document — the silent-boolean→False rule (which
  presumes CAO silence means the CAO grants nothing) does not apply, because the CAO
  explicitly says the provisions exist elsewhere. This extends the pension available-case
  hard rule from numerics to booleans under full-scheme deferral. 506009+506010 → blank.
  Scoring note: blank and False are equivalent in the coverage share (`eq("True")`), so this
  is semantic hygiene, not a score change.

- **2026-07-13 (L29 — POST-CAMPAIGN EXTENSIONS: CANT_TELL full-text closure, part-time screen,
  durable QA infrastructure; G28).**
  (1) **CANT_TELL escalation completed for ALL tiers.** The L21 full-text escalation had only
  covered the tier-A-era 114 CTs; the tier-B/C/ripple campaigns left 435 open CTs at the extract
  tier. All 434 resolvable ones re-adjudicated against parsed markdown (5 Sonnet readers:
  132 FIX / 255 LEAVE / 47 still-CT) → independent skeptical gate re-derived every FIX
  (121/132 approved, 92%; rejections incl. a fabricated citation, an inferred lustrum rule,
  2 field-routing errors, derived sums) → 163 cells applied (`L29_ct_fulltext`).
  Gate-confirmed convention: `leave_sickpay_continuation_value` = the YEAR-1 rate (tier
  schedule goes in notes) — matching existing corrections.csv practice; C3-first-tier analog.
  (2) **`contract_part_time_allowed` deterministic screen** (feared corpus-wide heading noise):
  evidence scan of all 2,739 records showed the fear was largely unfounded — only 186 suspects
  (37 True-with-zero-part-time-text, 149 False-despite-part-time-text). Agent-read: 54 FIX
  (46 under-coded Falses — pro-rata/deeltijd content lives in OTHER topic sections; 8 phantom
  Trues) / 130 LEAVE / 2 CT (`L29_part_time_screen`). Precedent: pro-rata part-time content
  anywhere in the document supports True; the boilerplate heading and bare Wfw request-right
  restatements do not.
  (3) **Durable QA infrastructure moved into the repo** (was scratchpad-only, tmp-reaper risk):
  `corrections/jump_campaign_adjudications.csv` (full adjudication ledger: ~15.9k (record,field) keys with
  FIXED/LEAVE_GENUINE/CANT_TELL/ADJUDICATED_EVENT_SIDE dispositions), `rebuild.sh`,
  `jump_census.py` (drops/V-dips/Λ census), `same_term_coverage_check.py` (battery check 6:
  every same-term diff side must be in the ledger — fires after any edit layer that creates
  new sibling diffs → run a ripple wave), `statutory_fingerprints.py` (battery check 5:
  TIME-AWARE statutory-constant + enhancement-claim screen; windows: WIEG 2019/2020-07,
  ketenregeling 3/24 only in the WWZ era [2015-07, 2020-01) else 3/36, UWV parental top-up
  from 2022-08-02; ~1,254 suspects of which ~1,151 never agent-checked = the
  same-way-wrong-everywhere review queue for a future holistic pass),
  `boolean_flip_rate.py` (same-term boolean flip monitor: **mean 9.1% → 3.2%, max 29.7% →
  10.4%** pre→post campaign; residual is adjudicated-genuine — do NOT consensus-override it,
  monitor for regression instead).
  (4) **Upstream prevention**: 7-bullet "common false-positive traps" block added to
  `NON_SALARY_PROMPT` in CAOsDataExtraction/schema/non_salary_schema.py (statutory
  restatements, recommendations, exclusion lists, heading labels, template shadowing,
  RI&E≠PSA, never-derive) — applies to future extraction generations.

- **2026-07-12 (L21–L28 — CAMPAIGN CLOSURE: every index-jump tier adjudicated; G27, 4,427 cells
  across 8 layers).** Full details per layer in `docs/DATA_LINEAGE.md` G20–G26. Decisions that
  bind future work:
  (1) **Evidence-tier ladder is doctrine**: extract < parsed markdown < re-OCR of the PDF, and
  *extract-silence ≠ document-silence*. CANT_TELLs are re-adjudicated at the NEXT tier before
  being ruled on (L21: 62 of 114 resolved by full text alone); each fix is verified against ITS
  OWN evidence tier.
  (2) **Template shadowing** is a catalogued extraction failure mode (4th class, joining
  rotated-scan garble, corrupted-font parse, schema-question echo): the extractor grabs an
  UNFILLED model-contract appendix and drops the governing body article. Found on 2948 probation
  (body "Artikel 8a … maximaal twee maanden" present 2010-2012, shadowed by the template) — the
  full-text tiebreak REVERSED the extract-tier consensus (L25a, 31 cells). Rule: before blanking
  a "template-only" value, sweep the FAMILY's parsed full texts for a body article.
  (3) **Tier dedup rule**: a ≥0.5 one-way jump that is the first leg of a V/Λ belongs to tier B
  (sandwich), not tier C — tier-B core cells are excluded from tier-C worklists; no cell is
  adjudicated twice under different framings.
  (4) **Ripple closure is iterated to a fixed point**: applying fixes exposes the same phantom
  on previously-agreeing siblings; each apply is followed by a coverage census (adjudicated-keys
  ledger vs all same-term diffs) and a new reader wave until 0 uncovered sides remain
  (L27: 662 sides → L28 waves: 168 → 44 → 22 → 11 → 7 → 0; geometric convergence, ~2-4× per wave).
  (5) **Genuine amendments are preserved against verified siblings** when the target's OWN text
  supports its value — same-term agreement is a selector, never evidence (L27: 238 of 674 LEAVE).
  (6) Campaign hygiene: unique helper-script names per agent (a shared script cross-contaminated
  one verdict file — caught by aggregation key-validation); quote-provenance checks (word-level
  containment) before any gate; SECMAP mapping for schema-dotted field names.
  END-STATE: overall census **1 drop ≤−0.4 (CAO 65 remplaçanten series switch, PDF-verified
  genuine), 0 V-dips, 0 Λ-spikes, 0 uncovered same-term sides**; battery green (39/39 signs,
  pctile ALL OK). Side-finding integrated into `qa/reextraction/`: 310005's 572 upstream salary
  rows are fabricated; a verified 252-row replacement is staged for the salary track.

- **2026-07-09 (UNIT-NORMALIZER repair — HEAD-TOKEN rule; scoring change, no dataset change).**
  Tier A's 19 COSMETIC verdicts (same fact, different unit wording, yet a big z jump) exposed
  unit-parser failures. A full audit (regression harness `norm_regress.py`: normalize() dumped
  for all 56,451 populated numeric scoring cells before/after; every diff reviewed by family)
  found and fixed, in `index_lib.py`:
  (1) **hours_per_week order bug** — "38 hours per week on an annual basis" hit the annual/52
  branch → 0.73 h/wk (8 cells); 'per week' now wins over annual qualifiers.
  (2) **days_per_year**: '…for 36-hour week' qualifiers false-triggered the weeks×5 branch
  (237.4 h/yr → 1187 days); %-of-worktime units fabricated day counts (9.77% → 1.22 days);
  'day per week' frequencies returned 1 day/yr; multiplier idioms ('x average weekly hours')
  mis-parsed.
  (3) **weeks**: '10 days per year' matched the YEAR token → **520 weeks** (35+ cells, worst
  pre-existing bug); 'weekly working hour(s)/duration/period', 'weekly hours equivalent',
  'x weekly working time', 'times working hours' silently dropped; '100% of monthly salary for
  52 weeks' returned 100 weeks; '6 months (half of weekly working hours)' returned 6 weeks.
  (4) **months**: '3 years minus one day' → 0.099 months; '26 weeks (…statutory 1 month per
  Art 7:672 BW)' → 26 months.
  (5) **percent**: real percentages with a EUR FLOOR in the unit ('8% of annual income,
  minimum €1,410/yr') were killed by the EUR-guard (29 cells).
  **Design: HEAD-TOKEN rule** — digit-qualifiers ('36-hour week', '4-week pay periods') are
  stripped, then the FIRST duration/percent token in the unit string decides what the value IS;
  later tokens are qualifiers. Multiplier idioms ('N times/x … week(ly)/working hours' = N weeks
  of worktime) win at equal position. %-of-worktime in duration fields → None (unconvertible;
  honest drop beats a fabricated number). Frequencies convert as units (day per week → ×52,
  same doctrine as eur-per-day → per-month).
  **Impact**: 351 of 56,451 conversions changed — 69 rescued from silent drops, 37 fabrications
  now dropped, the rest corrected magnitudes; battery green (39/39 signs, pctile ALL OK).
  Knock-ons re-censused and closed like any other change (new same-term diffs adjudicated,
  1 new overall drop [187] source-checked).

- **2026-07-09 (L20 — TIER A same-term campaign, amendment-aware; G19, 902 cells).** Hanna's go,
  with two constraints honoured throughout: (1) verification = agents READING THE FULL EXTRACTED
  SOURCE (source_lookup extracts, never PDFs, never snippets — complete topic passages both sides,
  full-13-key scan before any blank/False); (2) **same-term is a SELECTOR, not a verdict** —
  GENUINE_AMENDMENT was a first-class outcome and 162 events (14%) were confirmed real
  tussentijdse wijzigingen and LEFT. Scope: worklist REBUILT post-L19 (645 events / 1,144
  scoring-relevant changed cells after filtering to index-input fields and normalized-value
  differences — notes/format/unit-cosmetics dropped). Pipeline: 11 Sonnet first readers (verdicts:
  456 EXTRACT_INCONSISTENT / 396 PHANTOM / 162 GENUINE_AMENDMENT / 113 CANT_TELL / 19 COSMETIC)
  → mechanical QUOTE-PROVENANCE check (word-level containment of every evidence quote in its
  attributed document; 1,600+ checked, 15 hard fails incl. 3 other-document bleeds — all
  re-derived at gate) → 8 independent Sonnet gates on 764 deduped fix targets (**611 confirmed /
  153 rejected ≈ 20%**; rejection classes: arithmetic-derived values [12mo = 2×6, 5.175% = 20.7%/4],
  cross-edition imports where the target doc is silent, same-doc text supporting the current value,
  category mismatches [WGA-premium recoupment ≠ insurance, statutory WAZO formula ≠ top-up,
  jubilee ≠ retire-gratuity]) → RIPPLE closure: fixes create new same-term diffs vs neighbours;
  iterated read→gate→apply until **0 cells lack two-side adjudication** (70 + 8 + 5 + 2 cells over
  4 closure rounds). **Opus judge layer** (Hanna's tiering: hard legal semantics) decided 11
  LEAVE-vs-gate tensions via three principles — NEVER-DERIVE is value-intrinsic (transfers across
  editions), TEXT-IDENTITY transfers gate rulings, GENUINE-DIFFERENCE lets editions diverge — and
  reverted 3 L20 over-flips (2 readers vs 1 standard). POST: overall census unchanged at 3 drops +
  1 V-dip (all verified real, 0 uncovered); same-term events 645→254; the remaining 409 changed
  cells are adjudicated genuine amendments / cant-tells / cosmetics. Change-log
  `indices/corrections/tierA_corrections_applied.csv` (provenances `L20_tierA_same_term/_ripple/_judge/
  _ripple2/_ripple3/_ripple4`), backups `qa/backups/.bak_tierA*`. Tier B (cross-term ±0.5 reversals,
  ~490 families) remains OPTIONAL — tier A's measured phantom rate (~53% of adjudicated cells
  fix-worthy after gate) suggests it would find real errors; Hanna's call on cost.

- **2026-07-09 (L19 consistency-unify of ambiguous cells).** The 107 CANT_TELL cells (82 families)
  from the topic sweeps were screened under the L10-derived rule *same fact + same text → same value*
  (a CONSISTENCY claim, never a truth claim; hard limits: never against verified-real divergence,
  units checked, texts compared verbatim): 22 proposals → independent gate → **14 applied (G18)**.
  The 8 rejections protected anchors that were themselves suspect (1193's genuinely oscillating
  editions; 2520's possibly over-coded sibling; M&T '55+ reclassification' boilerplate = a recurring
  job-allowances false-positive trigger — all three parked as family-level flags). Post-L19 overall
  census: 3 drops / 2 V-dips / 1 Λ / 17 up-jumps (one riser unified away). Campaign total 556 cells.
  **Evidence-tier note (be honest when citing):** bulk cell verifications are EXTRACT-level (full
  passages, two independent readers, verbatim quotes) + family consistency; overall-level events are
  additionally PDF-verified. An extract-silent verdict does not prove PDF-silence — the residual is
  documented; a PDF-level pass over corrected families is the optional closing step. Tier A
  (same-term jumps ≥0.5) subsequently RAN as L20 — see the newer entry above.

- **2026-07-09 (L18 — TOPIC-LEVEL + UPWARD sweep; PDF-grounded; TERMINAL three-level census).**
  Extended the jump campaign to (a) UPWARD jumps/Λ-spikes (phantom Trues appearing are as fake as
  drops) and (b) TOPIC-level jumps invisible at the overall (a ±3.3σ topic swing dilutes to ~0.26):
  274 topic-level events enumerated → 162 uncovered families verified edition-by-edition (7 Sonnet
  sweeps → 4 independent gates, 16 gate rejections incl. sweep errors caught) + original-PDF checks
  for all remaining overall drops and parked cases. **Applied as L18 (G17): 400 cells** (incl. a
  post-apply audit fix: 3340001's EJU value was moved to thirteenth_month_amt but the mis-filed
  fixed_annual_lump source cell wasn't cleared — double-count caught and fixed) (change-log
  `corrections/topic_sweep_corrections_applied.csv`; backup `.bak_topicsweep`). Headlines: 310005's homeoffice
  block was **extractor FABRICATION** (source is a rotated 2-up scan with no text layer; re-OCR at
  correct orientation → zero telework content) — cleared; overtime `compensation_mode` is
  systematically noisy (extracts drop 'kan overeenkomen' conditionals — 20 editions → both, 10 →
  unspecified where no overtime article exists, 2 PDF-verified); vakantietoeslag/EJU vs 13th-month
  mapping fixed; 76 childcare + 84 homeoffice phantom/inconsistent cells; numeric extraction errors
  (summed paternity days, missed supplements, AOW-scope figures). 397003 'contamination' REFUTED by
  its PDF (shared sector boilerplate — value real). `source_lookup` hardened twice (extension/prefix
  + underscore-collision bugs; 9 collision groups). **TERMINAL census** (from the campaign's start):
  overall drops ≤−0.4: 23→**3** (1944 Colland-list rewrite, 310 real decline, 65 remplaçanten scope —
  all PDF-verified); V-dips ±0.3: 17→**2**; Λ-spikes: **1**; up-jumps ≥0.4: 24→**18** (verified real,
  mostly 2021+ telework/fiscal-era); topic-level events: 274→**130**, every one belonging to a family
  source-verified this campaign (genuine or CANT_TELL). Total campaign: **542 cells** (L14–L18).
  Battery green throughout.

- **2026-07-09 (L17 addendum — 3313 pension family; FINAL terminal census).** The post-L16 census
  surfaced one shifted driver (3313's V-dip driver moved to pension after the sick-topup restoration);
  the family sweep + independent second read found CAO 3313's `pension_mandatory_participation` and
  `accrual_stat_leaves` Trues largely UNSUPPORTED (topic passages present but no explicit statement;
  premium-payment ≠ accrual) → **14 cells → False (L17, G16)**, 3 explicit "participation is
  mandatory" Trues kept, 1 CANT_TELL held out; flagged out-of-scope: 3313004 accrual_illness_y2
  looks unsupported-FALSE (inverse case). **FINAL terminal census: drops ≤−0.4 = 8** (65, 310, 1471,
  2143, 50, 1060, 1944, 1264 — all source-verified genuine or adjudicated-unknowable), **V-dips
  ±0.3 = 3** (125 filing-order/pre-telework artifact, 50 & 65 real — all adjudicated), **≥0.5 = 0**.
  Total dip campaign: 142 source-verified cells (L14-L17), every apply through an independent gate.

- **2026-07-09 (L16 family-consistency completion — TERMINAL dip state).** The L15 corrections moved
  cliffs one edition earlier where families were only fixed at the flip pair, so a final family sweep
  covered every remaining edition of the L15-corrected fields; the independent final gate confirmed
  **77 APPLY / 19 kept-as-is / 0 rejections** (applied as **L16, G15**, backup `.bak_family2`,
  change-log `indices/corrections/dip_family2_corrections_applied.csv`). Family end-states: 2535 childcare &
  homeoffice False everywhere ('SF MITT' fund goals; 'thuiswerker' = piece-rate home production, not
  telework); 791 childcare/profit-sharing/performance False everywhere (à-la-Carte swap; advisory
  boilerplate); 1022 phantom safety/childcare cleared but its 7 REAL employer-childcare-contribution
  editions preserved; 1264 hetero_pension False everywhere (flat 40/60 split). ⚠ **Gate finding: some
  of these phantom Trues were INTRODUCED by L8's flip campaign** (evidence mis-cited, e.g. piece-rate
  text read as telework reimbursement) — treat L8 cells with that caveat. **TERMINAL census**
  (from the original 23 drops ≤−0.4 / 17 V-dips ±0.3 / 2 ≥0.5): **8 drops / 4 V-dips / 0 ≥0.5**, and
  every survivor is a source-verified genuine change or adjudicated-unknowable: 65 (remplaçanten
  scope), 310 (real decline), 1471 (Proces/Services split), 2143 (childcare removal), 50 (money-only
  overtime), 1060 (TOIL switch), 1944 (real Colland fund provision, dip unknowable), 1264 (sustained
  Article-11 contraction, no rebound). Battery green throughout.

- **2026-07-09 (L15 three-sided sweep applied + CROSS-CAO carry for explicit deferrals).**
  (a) Completed the three-sided (prev/dip/next) verification of every remaining dip event plus
  family sweeps of the corrected fields: two Sonnet sweep passes → **independent final-gate
  adjudication (41 APPLY / 57 SKIP)** under an explicit framework (F1 silent-True→False per the
  1029009 precedent; F2 restorations only with same-term or both-neighbours evidence; F3
  definition-bar — "provides" vs "mentions"; F4 per-CAO consistency). Headlines: CAO 549 childcare
  over-coded in ALL 8 editions; **CAO 2948 pension accrual-in-leave systemically under-extracted
  (12 more editions → True)**; the gate REJECTED the 125 homeoffice "restoration" (a genuine
  pre-telework era, the sweep's prev was mis-ordered) and killed weak restorations (3313 insurance,
  488 reporting-channel — the latter exposing an internally split prev-term flagged as a separate
  issue). Applied as **L15 (G14, 41 cells)**, backup `.bak_dipsweep`, change-log
  `indices/corrections/dip_sweep_corrections_applied.csv`. (b) **Cross-CAO carry shipped** (registry column
  `carry_from_id` + composite honoring it): a THIN_CONFIRMED doc that explicitly defers to ANOTHER
  CAO carries that CAO's in-force edition instead of its own stale predecessor. Verified case:
  RPO (2299) → the national **CAO PO = cao 1494** (confirmed by self-descriptions + the RPO deltas'
  verbatim references); mapping 2299006→1494016, 2299002→1494005 (period-overlap pick over the
  strict file-date rule — the carry represents in-force conditions; 3-month filing lag = normal
  publication lag), 2299003→1494009. RPO deviations (identity/union-branding) touch no scored
  field. Dip census after L15: drops ≤−0.4 → 10, V-dips ±0.3 → 4, ±0.5 → 0; a follow-up family
  pass covers the L15-corrected fields' remaining editions (cliffs move when only the flip pair
  is fixed — family consistency is the terminal state).

- **2026-07-09 (final dip census — EVERY remaining event source-checked; resolver bug found+fixed).**
  Enumerated every surviving event (`indices/review/final_dip_census.csv`): after all interventions,
  **14 drops ≤−0.4 + 3 borderline V-dips, and 17/17 are covered by a source-verification artifact.**
  The 4 events that had only been *attributed* were source-checked (two-pass): 254001 & 549004
  childcare True were over-extraction (cafeteria money-goal swap / scheduling protection ≠ "employer
  provides") → corrected (L14 now 10 cells, G13); CAO 50's overtime narrowing to money-only is REAL;
  1060023's TOIL is correct-as-is. The 1060 second-read **caught a latent `source_lookup.resolve()`
  bug**: `os.path.splitext` ate legitimate suffixes ('…2024.def') and the first-match-wins fallback
  returned a DIFFERENT EDITION on long shared prefixes — fixed (exact full-name match first, real
  doc-extensions only, ranked longest/closest partial match). Blast-radius audit: 132/2,698
  resolutions changed (all inspected samples now correct); **none of the applied L14 corrections and
  13/14 registry verdicts stood on identical resolutions**; the one affected verdict (1285003) was
  re-checked against its TRUE extract and flipped COMPLETE_DOC→THIN_CONFIRMED (8 of 13 topics empty;
  same-term juli reissues populate them) → now carried (10 thin docs total). Remaining big drops are
  ALL either verified-genuine (65 scope, 310 real decline, 1471 split, 2143 childcare removal, 50
  money-only, 1060 TOIL) or campaign-checked single flips where the source is silent (CANT_TELL) or
  confirms the False. Battery green.

- **2026-07-08 (dip remediation EXECUTED: corrections applied + THIN-DOCUMENT rule shipped).**
  (a) The 7 two-pass-verified boolean corrections were APPLIED to qa/corrected_dataset.csv
  (backup `.bak_dipfix`; change-log `indices/corrections/dip_boolean_corrections_applied.csv`; provenance
  `L14_dip_boolean_verification`): 1535 childcare→True, 488 PMO check-up→True, 2948 pension
  accrual-in-leave→True (med), and CAO 1029's 4 childcare True→False (Bijlage III.5 is a
  non-binding recommendation; one edition fully silent — all 10 editions of the family are now
  source-checked). (b) **THIN-DOCUMENT rule implemented** (`composite_index.apply_thin_doc_carry`):
  candidate = populated cells <80% AND True-booleans <85% vs the CAO's last non-thin edition
  (chained baseline; first docs never flag); remedy = carry the last full edition's SCORE row
  forward, ONLY for editions **source-verified** as thin in the registry
  `indices/review/thin_docs_reviewed.csv` (verdict THIN_CONFIRMED; GENUINE_CHANGE/COMPLETE_DOC are never
  carried). New candidates get NO automatic remedy — they are surfaced in
  `out/thin_doc_candidates_unreviewed.csv` for source review first (hard rule: check the source that
  it really is a thin/not-complete document before marking). Carried rows are MARKED
  (`thin_doc=True`, `scores_carried_from=<id>`) in composite, panel and workbook; the panel's
  law-month re-scoring reads the carried source doc (`score_src_id`). 10 editions carried after
  review: 1029015 (mantel), 2299006/2299002/2299003 (appendix + deviations-delta chain on CAO PO),
  233009 (failed extraction), 1494007 (fund-procedure doc), 408003 (deviations delta), 35010 (AVV
  posted-workers reprint), 433016 (renvooi extension reprint) — 9 editions carried. Protected as
  genuine: 65001 (remplaçanten sub-CAO), 1471006 (Proces/Services scope split), 1285003 (complete,
  false positive), and **310006 — verdict REVISED from downstream-loss to COMPLETE_DOC** after a
  cell-level recovery check (indices/review/cao310_recovery_proposed.csv, 90 loss candidates → 1 recoverable):
  its "lost" cells are legitimately absent (statutory-only leave text, genuinely more modest terms,
  contradicting booleans) — the decline is REAL, so it is NOT carried; only its dropped probation
  unit was restored (L14 cell 8). 941011 confirmed thin but below the chained criterion
  (conservative miss, same-term reissues recover it months later). **Effect measured**: consecutive
  drops ≤−0.4: 23→16; V-dips ±0.3: 17→11; ±0.5: 2→0; every remaining big drop is either a verified
  genuine change (2143 childcare removal, 1471 split, 310's real decline) or a source-silent/
  confirmed-False single flip. Battery green.

- **2026-07-08 (downward-jump investigation + composite wage as-of fill).** Investigated the V-shaped
  dips (overall_z drop ~0.3–1.2 then rebound) and all 22 consecutive-doc drops ≤ −0.4: **0/22 look like
  genuine benefit rollbacks.** Two mechanisms: (1) **whole-document extraction sparsity** (11/22) — an
  edition extracted much emptier than its neighbours (populated cells −15–38%, True-booleans collapsing,
  e.g. CAO 1029's 2018 doc: 21/61 → 0/61 True) drags most topics down at once; next file recovers.
  (2) **near-degenerate coverage scales** (11/22) — childcare (4 booleans, ~2–9% true-rates, 91% of docs
  at share 0, winsor puts share 0.25 at the +3 clip) swings ±3.33σ on ONE boolean flip = ±0.33 on
  overall_z (single-track → full weight); overtime's 1-boolean coverage swings ±3.10σ but is halved by
  its numeric track. Safety is fine (12 booleans, max single-flip swing 0.36). Evidence:
  `review/big_drops_attribution.csv`, `review/v_dips_evidence.csv`, `review/coverage_flip_sensitivity.csv`,
  `review/overall_z_option_variants.csv` (option simulations). **APPLIED FIX: composite wage join is now AS-OF
  (forward-fill)** — a doc filed in a gap year (no parsed wage table that calendar year) carries the last
  known year's wage instead of silently losing the wage input from the available-case overall (mirrors the
  panel's as-of fill; new `wage_src_year` column exposes the carry; 2,161 → 2,444 docs with wage).
  ~~OPEN: drop childcare from the overall~~ → Hanna chose **correction over dropping**; see next entry.

- **2026-07-08 (dip remediation via CORRECTION, not dropping — two-pass source campaign).** Verified all
  56 flipped booleans behind the dips against each edition's raw extract (Sonnet propose → independent
  Sonnet second reader; 40–60% of proposals rejected at pass 2, re-validating the two-pass standard).
  Outcome: **7 corrections STAGED** in `qa/dip_boolean_corrections_staged.csv` (NOT applied): 2 confirmed
  True-restorations (1535 childcare AVOM benefit; 488 PMO check-up), 1 downgraded-to-med (2948 pension
  accrual-in-leave, voluntary/conditional), and **4 reverse corrections for CAO 1029** — its childcare
  True labels are over-extraction (Bijlage III.5 is a non-binding employer-association *recommendation*;
  one edition is fully silent), so True→False across the family, which removes 1029's spurious dips at
  the data level. 10 flips confirmed genuinely False (incl. CAO 2143's explicit childcare removal — a
  REAL rollback); 41 unjudgeable (dip edition's extract silent). **Blackout-edition diagnosis**
  (`indices/review/blackout_diagnosis.csv`): 2 upstream extractor failures (2299 = appendix file ingested, 233),
  1 downstream loss (310 — raw extract rich, dataset row empty), 3 **thin-by-design** docs (1029 mantel/
  umbrella CAO deferring to companion docs; 408 deviations-only delta; 1494 fund-procedure doc) that
  should NOT be scored as if they abolished provisions, 2 genuine scope changes (65 remplaçanten sub-CAO
  sharing cao_number — version-linking issue; 1471 narrowed successor). OPEN: thin-document handling
  (flag + carry-forward/not-scoreable) — proposal pending Hanna's decision.

- **2026-07-08 (QA pass — 7 correctness/labelling fixes + latent-trap hardening).** Post-review
  fixes, all rebuilt + battery-green: (a) **leave now appears in FactorLoadings & FactorMixed** —
  `advanced_analysis` imported a non-existent `leave_index` under a bare `except`, silently dropping
  leave; added `_topic_module`/`_topic_csv` (leave→parental_leave) and put `leave` in `TOPIC_FIELDZ`;
  exposed `parental_leave_index.BOOLEANS` at module level. (b) **absence added to `COV_TOPICS`; `ai`
  removed** (its coverage share is ~constant → no pooled variance → z undefined; it was silently
  shrinking the coverage FA). Added a degenerate-coverage guard in `build_simple_index` that blanks
  `coverage_z` AND the (meaningless) `coverage_pctile` when the share has no variance. (c)
  **`out/factor_scores.csv` `id`** was row numbers, now real doc ids (`set_index("id")`). (d) **`PC1_var%`**
  now the TRUE first-PC variance share (largest corr-matrix eigenvalue ÷ #fields), not the rotated
  FA-f1 share it reported before. (e) **Panel `overall_z_var` is now the COMBINED variance** matching
  the combined `overall_z` (was the numeric-track variance); numeric companion renamed
  `overall_numeric_z_var`. Panel now grafts `{t}_coverage_z` and computes combined `{t}_z` +
  `overall_z` for CAO rows, so the PRIMARY overall is law-month re-scored too. (f) **Statutory panel
  rows** now carry `{t}_coverage_z` (share 0 → deeply −z) and get a combined `overall_z`/`overall_z_var`
  — previously their combined `{t}_z` was numeric-only, **overstating the legal floor**. (g) **Wage /
  WageWorkerType tabs keep their `year` column** (was dropped globally; those cao×year tabs have no
  other date key). (h) **~13th-month `%-of-monthly-salary`**: `to_pct_annual` now guards a genuine
  single-month fraction (value > 50 → ÷12); today's rows (~8.33 = annual-equivalent %) are unchanged.
  Latent traps hardened: `build_combined.apply_scheme` now delegates to `il.apply_scheme` (removed the
  drifted absence-mishandling copy); panel `merge_asof` keys cast to int64 (was an int-vs-float crash
  risk); composite `wage_gen01` now uses the term-deduped ECDF like every other pctile (was a
  reprint-weighted rank); `mw_indices.YEARS` upper bound is now `today+2` (was a hardcoded 2027);
  statutory coverage tie-break sorts on parsed date + numeric id. Reading rules (§7.1/§7.2) and design
  notes (§11.1) added.


- **2026-07-08 (absence→dual, overall rename, FAMD).** (a) **absence is now a DUAL topic**: the 3
  sickness/care top-up booleans (`leave_sick_topup_present`, `leave_sickpay_extra_insurance_present`,
  `leave_care_topup_present`) MOVED from leave_coverage → a new **absence coverage** track (they belong
  to absence's sickpay/care concepts). Effects: leave_coverage 12→9 booleans; absence gains
  `absence_coverage`/`_z`/`_pctile` and a combined `absence_z` = mean(numeric, coverage); absence added
  to composite COV/COVZ, statutory coverage, panel COVC, `index_lib.DUAL_TOPICS`. (b) **`overall_z_mean`
  → `overall_numeric_z`** (synchronised with `overall_numeric_pctile`); the panel-aggregate derivatives
  became `overall_numeric_z_mean`/`_var`. `overall_z_var` kept (numeric package lopsidedness). (c) NEW
  **FactorMixed tab (FAMD)** — factor analysis of mixed data per topic: numeric fields standardised +
  each boolean MCA-scaled (0/1 indicator centred, ÷√True-rate) so neither block dominates; one SVD →
  joint factors; each variable's loading = its correlation with the factor score (numeric Pearson /
  boolean point-biserial), communality = Σloading². Implemented in numpy/sklearn (prince not installed).
  Runs for the 7 topics with ≥2 numeric AND ≥2 booleans (overtime skipped: 0 booleans). Confirms
  presence & amount of the SAME benefit load together (e.g. bonus 13th-month amount + boolean on f1).
  Battery green.

- **2026-07-08 (workbook presentation + combined-primary).** (a) COMBINED roll-ups are now the
  PRIMARY overall headline: `overall_combined_z` / `overall_combined_pctile` sit at the front of
  Composite/Panel; the NUMERIC-only roll-ups `overall_z_mean` (unchanged name) and
  `overall_numeric_pctile` (was `overall_pctile`/`overall_gen01`) + the per-topic coverage shares +
  counts moved to the light-blue right-hand section. `overall_z_mean` is numeric-only, available-case.
  (b) WAGE exposes `wage_median_z`+`wage_mean_z` (+ `_pctile` each); overall uses the median.
  (c) **CoverageVsLevel redesigned**: was breadth-vs-magnitude within a topic; now "do generous CAOs
  PAY more?" — per topic, correlation of the topic's combined / coverage / numeric score with the
  CAO's wage (`wage_median_z`), Pearson + Spearman (6 cols); `pearson_newest` dropped. (d) Tabs:
  `WageStructure` merged into `FactorLoadings` (wage-ladder rows appended, topic=wage);
  `Stage1Weighting` → `FieldWeighting` (stage 1 = combine a topic's FIELDS; stage 2 = combine topics);
  analysis-tab summaries rewritten in plain language (FactorOverall/Loadings/Scores, CoverageBundles
  bundleN = the Nth co-adoption cluster NOT a count). (e) Manual tab reorder in Excel now SURVIVES a
  rebuild (`_existing_sheet_order` reads the saved order; renames mapped via TAB_RENAMES). Battery green.
  OPEN (Hanna): move the 3 sickness/care booleans (`leave_sick_topup_present`,
  `leave_sickpay_extra_insurance_present`, `leave_care_topup_present`) from leave_coverage into a new
  absence coverage track? (absence is currently numeric-only by design).

- **2026-07-08 — column-naming taxonomy + headline wage = median-relative (pipeline-wide rename).**
  Two changes, both applied through the whole driver chain (topics → leave → mw → composite →
  statutory → panel → factor analysis → workbook) and verified value-preserving.
  (a) **NAMING taxonomy** (see `indices/NAMING.md`, the single source of truth): every topic is now
  three tracks × two scales — `{t}_numeric_z`/`{t}_coverage_z`/**`{t}_z`(=combined headline)** and
  `{t}_numeric_pctile`/`{t}_coverage_pctile`/**`{t}_pctile`(=combined)**. `gen01` → `pctile`
  everywhere; `overall_gen01` → `overall_pctile`, `overall_combined01` → `overall_combined_pctile`.
  ⚠️ The bare `{t}_z` now means COMBINED, not magnitude — use `{t}_numeric_z` for magnitude. Rename
  is pure (topic/leave/statutory CSVs value-identical vs `.bak_rename`); `il.apply_scheme()` is the
  one mapping function, `il.DUAL_TOPICS` the dual-track list. Fixes the old footgun where `{t}_z` =
  magnitude in the CSVs but combined in the workbook, and where `wage_z` was nominal in mw_indices
  but relative in composite. Also resolves the "homeoffice looks empty" report: the panel headline is
  now the combined `{t}_z` (100% populated), with the numeric-blank magnitude relegated to the breakdown.
  (b) **Headline wage score = median-relative** (Hanna's pick): the wage dimension in every overall
  roll-up is now `wage_median_z = z(mw_median/WML)` (was mean), more robust to scale-step skew. Wage
  has no numeric/coverage split; instead it exposes `wage_median_z`/`wage_mean_z` +
  `wage_median_pctile`/`wage_mean_pctile` (+ low/high/nominal). `wage_nominal_z` (nominal mean) stays
  OUT of the overall (inflation drift). Effect on `overall_z_mean` ≈ 0.011 mean-abs (1 of 11 dims).
  **Why ratio-to-WML for wage but not elsewhere:** wage is an absolute EUR level that inflates, so it
  needs detrending against the year's statutory minimum; the other topics (weeks/%/months) are already
  inflation-neutral and use raw values. (c) **Workbook layout**: composite/panel dashboard-ordered
  (aggregate roll-ups first → headline z's grouped → headline pctiles grouped → wage/level detail →
  numeric/coverage breakdown + counts in a light-blue right-hand section); topic tabs lead with the
  topic-level scores; factor tabs share one column order (communality after the labels); analysis-tab
  columns (PC1_var%, corr_PC1, bundleN, communality, …) now have plain-language definitions + hover notes.
  Battery green; backups in `indices/.bak_rename/`.

  **Q&A captured (Hanna, same session):** (i) `overall_z_mean` is available-case — a doc scored on 9
  topics averages those 9 (n_topics_scored). (ii) absence has NO coverage track by design (its presence
  booleans live in `leave_coverage`, shared source) → absence is numeric-only. (iii) `overall_pctile`
  (mean of NUMERIC pctiles) vs `overall_combined_pctile` (mean of COMBINED pctiles, incl. coverage): the
  combined is the headline; the numeric one is the magnitude-only companion. (iv) "Stage 1" = combining
  fields WITHIN a topic (stage 2 = across topics); Stage1Weighting tests equal-weight vs PCA/FA field weights.

- **2026-07-08 — pension `employee_contrib` SYSTEMIC %-of-premium fix (dataset L13, indices rebuilt).**
  Follow-up to the collision finding: `pension_employee_contrib_value` was frequently the employee's share
  OF THE PENSION PREMIUM (employer/employee split, e.g. 45/50%), not % of salary as the field requires — a
  different quantity that can't be converted without inventing a number → **blanked (available-case, aligned
  with §7.1 pension available-case-only)**. Detection in 3 passes: (a) unit denominator = premium (448 rows,
  deterministic; Sonnet source-calibration 6/6 decidable = premium-share; salary/'premium-bearing salary'
  units KEPT); (b) same-CAO bare-unit siblings carrying an identical premium value — required because
  `forward_fill` re-injects the error from siblings (139 rows); (c) plausibility ≥20% (source-verified 8/8 =
  premium-share; a real employee salary-contribution rate is 3–10%, rarely >15%) (36 rows). **1,071 cells /
  ~90 CAOs blanked.** Effect: index `employee_contrib` now spans 0–19.6% (median 7.3), 0 residual ≥20; the
  employee-contrib **yardstick re-centres** off the (previously inflated) mean, shifting ~1.2k pension_z's — a
  correct consequence of removing distortive values. CAOs whose only contrib figure was the premium-share go
  available-case (pension_z NaN). Backups `.bak_pre-L13{,b,c}`; changelogs `qa/l13{,b,c}_*`.
- **2026-07-08 — wage index: TERM PRECEDENCE filter (`mw_indices.py`).** The per-cell dedup kept every
  distinct wage effective-date but did NOT enforce edition precedence across terms: an expiring term's
  *pre-announced future raise* (a table dated after the successor term's `ingangsdatum`) survived into the
  successor's window (Hanna's Q). Fix: a wage point from term T at effective date S is dropped iff another
  term of the same CAO started at I_B with `T.ingangs < I_B <= S` (successor governs S; the panel/ladder
  forward-fills). Retroactive tables (S < own ingangs) are never stale. **Impact: 6,895 stale rows / 15 CAOs
  dropped, but yearly median wage 2434.22→2433.30 (−0.04%), 215 CAOs unchanged, only 1 (cao,year) aggregate
  cell removed** — corrects the within-year distribution, not levels. Dropped rows → `out/mw_stale_forward_dropped.csv`.
  Generalises the earlier file-C/collision guard (which only covered the identical-cell restatement case). §2.
- **2026-07-08 — dataset L11+L12 promoted; indices rebuilt.** Canonical corrected_dataset.csv → G11
  (date-collision picked-record two-pass fixes: L11 27 corrections, L12 2). Full index chain re-run
  (13 topic + mw + statutory + composite + panel + advanced + combined); `check_battery` green
  (converter 35/35, sign 39/39, semantics 39/39, gen01 bounds OK). NOTE: value/boolean corrections
  propagate, but a few blank-outs are masked by within-CAO `forward_fill` when sibling editions carry the
  value — esp. **pension `employee_contrib` (CAO 43/429): a SYSTEMIC %-of-premium mis-extraction across all
  editions** (not collision-specific) → still 45/50 in the index; a real distortion (sign −1 penalises it)
  flagged for a separate all-editions pension fix.
- **2026-07-08 (verification pass — CORRECTIONS to the two entries below).** A same-day audit of
  the version-aware work found 5 real defects; all fixed:
  1. **Dedup was too aggressive** — 40% of its removals (75k pts) were SAME-file rows (36h/38h
     variant tables, sub-populations split only by table title), i.e. real dispersion, not
     cross-edition repeats; the "median moved only 0.43%" sign-off was too weak a test (medians
     are robust — the damage hid in quartiles/span/n_obs). Replaced with a TWO-STAGE dedup: per
     (cao, date, jobgroup, step, age, worker, unit) key keep ALL rows of the winning file (max
     kennisgeving_rank), never collapse within a file; then drop exact-value repeats only.
  2. **Metadata join collided on file_name** — the same file_name exists under several CAOs
     ('HB 5e editie 2024' under 822/824/826/827/2297; 'Zuivel I' under 157+563): filename-only
     join gave them the first row's metadata. Now keyed (cao_number, file_name). This also fixes
     the long-standing 563→157 cao mislabel — CAO 563 now enters the index as its own CAO (215).
  3. **31,288 rows (51 files) had NO version tags / doc metadata** — files the old salary CSV
     never held (never-parsed/agent-extracted). Fallback join to the NON-salary CSV (all 51
     present, exact names) → 0 untagged rows.
  4. **951 files (40%) carry DASH-format dates** (DD-MM-YYYY): the rank sort silently treated
     them as missing → edition order degraded to id-order (forbidden as recency signal). Parser
     now accepts both formats and normalizes the term_group date (no false term-splits found).
  5. `dedup_removed` double-counted the delta-edition drops (accounting only).
  **Final effect vs the pre-version-aware baseline:** 215 CAOs (was 214; +563), yearly median
  wage ±0.28%, observations +2.5% (400,772 vs 390,921 — the corrected dedup collapses editions
  but PRESERVES age/worker/unit splits the old value-dedup was flattening). Dataset rows/tiers
  unchanged (359,474; A+B 95.4%). Lesson: sign off composition (what was removed), not just
  levels; and any file_name join in this corpus must be keyed (cao, file).
- **2026-07-08 — wage index now VERSION-AWARE (term_group dedup).** Most CAOs re-publish the same
  term several times; each edition re-states its own window of wage tables (two-clocks: the term is
  constant, each wage table has its own effective date). The salary parser now TAGS every row
  (`term_group` = cao+ingangsdatum, `kennisgeving_rank` = 1..N by edition date, `base_id`,
  `n_editions`, `document_type` — see salary_parser/version_tag.py, per CAO_VERSION_SELECTION_PLAN.md
  TAG-don't-MERGE). `mw_indices.py` de-dup was upgraded from value-based
  (`cao,start_date,eur_month,jobgroup,step`) to VERSION-aware: keep the full timeline (every wage
  effective-date) but collapse each `(cao, effective-date, job-cell)` to the LATEST edition's value
  (max kennisgeving_rank). This also removes the amount-jitter double-counts (same cell re-stated
  with OCR/rounding noise across editions) the old dedup let through. Pure delta editions
  (partial_amendment/annex/supplement, 3,723 pts) dropped. **Effect (before/after sign-off):** CAO
  coverage unchanged (214), yearly median wage moves a median 0.43% (2012-2025) → levels undistorted,
  but observations −16% (390,921→328,318) — the removed rows were genuine cross-edition repeats, not
  a biased slice. Also fixed a latent CRLF header-parse bug (last column silently dropped). §1.
- **2026-07-08 — hourly workweek fallback 36h CONFIRMED (not era-varying).** Question raised: should
  source-silent CAOs use an older statutory 38/40h by era? Finding: NL has no *statutory* full-time
  workweek (the ATW caps maxima; "full-time" is CAO-set). The norm fell 40h (pre-mid-1980s ADV) →
  38h → 36h, and 36h is the basis of the 2024 statutory minimum-hourly wage. But every source-silent
  file is 2010+, well after the 40h era, so no older statute applies. Empirical check: CAO 634 (the
  one silent CAO with a clean full-time hourly AND monthly population) backs out to implied workweek
  35.2h across 2010-2023 → 36h validated, no era drift; applying 40h would over-state. Where a real
  38/40h is stated it is already used per-file (727=38, 3688=40). ⇒ keep 36h (tagged, reversible). §—
- **2026-07-08 — dataset advanced G6→G9 via source-verified correction layers L8–L10 (indices rebuilt).**
  The canonical `qa/corrected_dataset.csv` gained three correction layers driven from the indices side
  (all logged in `docs/DATA_LINEAGE.md` + `PROVENANCE_all_layers.csv`): **L8** = 3,559 boolean-flip +
  numeric-regression fixes (cross-edition inconsistency, Sonnet-on-snippets over all 242 CAOs via
  `source_lookup.py`); **L9** = 61 numeric same-term extraction-error fixes; **L10** = 111 consistency-unify
  + tie-break + full-passage recovery (incl. Hanna's per-case tie decisions). **Durable method rule adopted:
  two-pass verification is now STANDARD for numeric corrections** — a Sonnet proposer then an INDEPENDENT
  second reader that re-reads source fresh and defaults to REJECT; it rejected ~50–65% of first-pass numeric
  fixes (computed values, cumulative/unit misreads, truncated-passage over-reach), reproducing the known
  ~22 %+ numeric over-correction risk and stopping it pre-promotion. Blank-fills require the figure to be
  literally present in that document (never statutory-inferred). All topic + downstream indices rebuilt on
  each promotion; battery green, gen01 sanity OK. NB (leave): `leave_z`/`leave_gen01` score **per-type z +
  percentile ranks** (paternity/adoption/parental; maternity excluded, ~zero variance) — the `*_fre_extr`
  figures are the intermediate FRE-weeks that FEED the z, not the score itself. **Bug caught in a verify
  pass:** L10's majority-value unify compared RAW values ignoring UNIT FAMILIES and clobbered 7 cells where
  editions stated the same fact in different units (`sickpay_duration` "2 years"==104 weeks; `ketenregeling`
  weeks-vs-years; a pension % on two different denominators) — reverted, terms parked in
  `review/unit_incompatible_review.csv`. Rule: any value-unify/majority collapse must FIRST check unit compatibility
  (or convert to a canonical unit). §—
- **2026-07-08 — unit_inferred_magnitude spot-checked → ACCEPT (with oob sub-flag).** Source-anchored
  150-row audit (3 Haiku judges, oversampled out-of-band) of the ~79,500 magnitude-inferred-unit
  rows: reweighted unit-error 1.93% (in-band 1.7% vs out-of-band 7.8%). Accepted as-is (amounts stay
  100% provenance-correct; only the unit is uncertain). Added a free `unit_inferred_oob` flag (3,440
  rows) so analysts can drop just the risky 4.3% instead of all inferred rows. No tier change.
- **2026-07-08 — training budget STRATIFIED (multi-base standardisation).** `training_budget_value`
  lives in 3 incompatible units — € (703), %-of-salary individual budget (175/252 after unit
  match), %-of-wage-sum collective levy (296). Standardise WITHIN each base (own yardstick μ/σ +
  ECDF), output ONE comparable `budget_z` / `budget_prank` (a +1σ € doc == a +1σ %-salary doc).
  Helper `il.stratified_zpr`; `build_simple_index(stratified=…)` folds it into the topic mean.
  "% of wage sum" kept as the 3rd magnitude base (each doc uses one base → no within-field
  double-count; overlap with `training_fund_present` is existence-vs-amount, the usual two-track).
  Genuine absence (rights=False & never stated) → floor (z=−3, prank=0). cost_reimbursement is
  %-only; its 183 € rows → descriptive companion `training_cost_reimbursement_eur` (not scored).
  Verified: each base standardises to ~mean 0; chain + battery green. §3.
- **2026-07-08 — Campaign B: top-40 flip-CAOs boolean verification** (Sonnet-on-snippets, 10 agents,
  pre-extracted inline snippets → ~10× cheaper than file-reading). `review/boolean_corrections_top40.csv`:
  **1,442 trustworthy corrections / 1,898 per-record fixes across 40 CAOs** (1,218 should-be-TRUE
  = under-extraction; 224 should-be-false = over-extraction), only ~11% unresolved. Corrects the
  Haiku picture: the flips are overwhelmingly REAL extraction errors, not inherent ambiguity;
  Haiku-on-snippets only agreed 78% and missed 8/11 over-extractions → Sonnet needed. Surfaced,
  not applied. Source now via `source_lookup` over new_flow (all 242 CAOs).
- **2026-07-08 — boolean flips source-verified** (5 Haiku agents, 10 top-flip CAOs, 250 unique
  verdicts → `review/boolean_review.csv` + `review/boolean_errors_candidates.csv`). **CAVEAT: Haiku quality varied
  a lot** — some agents quoted source, others guessed from priors ("13th month is standard Dutch
  practice"). After flagging prior-based evidence + requiring high confidence: **64 TRUSTWORTHY
  source-grounded errors (62 should-be-TRUE, 2 should-be-false)** — the dominant signal is
  UNDER-extraction (a provision present but dropped in some editions → coverage slightly UNDERSTATED).
  Also ~89 genuinely-ambiguous (legally-implied safety Arbowet — inherent), ~53 cant_tell, 22 prior-
  based (discarded). Lesson reaffirmed (CLAUDE.md): snippet/boolean judging needs Sonnet for a
  reliable full campaign; this covered only 10 of 84 in-scope flip-CAOs. Numeric staged corrections
  independently re-confirmed (all 3). All surfaced, NOT applied.
- **2026-07-08 — gate-flip guard on zero-fill** (`apply_zerofill`). The presence booleans that
  gate zero-fill are themselves ~9% noisy (bonus 13th-month 12%, relocation 11%, overtime-shift
  10%). A False in a noisy edition was zeroing a numeric even when the benefit demonstrably exists
  elsewhere in the CAO — **~1,396 docs wrongly zeroed**. Fix: only zero-fill when the gate is
  never True in ANY edition of the CAO (`groupby(cao).transform("max")`); a benefit present
  anywhere = present-but-unquantified = available-case. Extends the earlier "ever-stated" guard
  from the numeric value to the gate boolean. Battery green. §8.
- **2026-07-08 — 6 confirmed extraction errors staged for QA** (`qa/regression_corrections_staged.csv`,
  apply_list format: record_id/field/expected_current/new_value/action). PROPOSALS only — surfaced
  for Hanna, NOT auto-applied (project rule). [Path updated: these go through a layer apply script
  following the `qa/apply_collision_l11.py` / `qa/apply_pension_premium_l13.py` pattern, which starts
  from the current canonical dataset. The L1 script named here originally is archived at
  `qa/_archive/apply_corrections.py` — it starts from the raw G0 extract and must not be used.]
  Covers the
  direct raw-field errors (pension early-retire ×2 high; contract full-time-hours ×7, short-care ×1
  med); FRE-derived (paternity/adoption) + the 54 maternity-16→0 need raw-weeks-field mapping first.
- **2026-07-07 — boolean same-term consistency check** (`review/boolean_flips.csv` detail +
  `review/boolean_flip_summary.csv`). Coverage booleans flip True↔False between editions of the SAME
  agreement at a **per-boolean rate of ~9%** (median 7%); because there are ~64 booleans this
  makes 96% of multi-edition terms show ≥1 flip, but the flips are near-symmetric noise (FALSE
  edition sparser only 58% — NOT amendment-omission). Noisiest (>15%): fringe insurance/savings,
  safety arbodienst/PSA/wellbeing, pension mandatory_participation, term ww_supplement — mostly
  **legally-implied** provisions where True/False is genuinely ambiguous (already caveated in the
  topic docstrings). **Index impact is modest**: the coverage SHARE averages over each topic's
  booleans, so within-term coverage range is median 0.00, p90 ~0.17–0.25 (1–2 booleans). Surfaced,
  not fixed. Optional denoise (term-level boolean consensus) judged low-value given coverage is
  already stable.
- **2026-07-07 — regression verification** (`review/regression_review.csv`). The 49 in-scope material
  same-term regressions were source-verified (Haiku calibration + inline snippet adjudication).
  Result: **6 confirmed extraction errors** (pension 791 early-retire 65→55 *high*; leave 306
  paternity 0→1 *high*; leave 243 adoption 0→6; absence 1287 short-care 4→10; contract 750/1022
  full-time hours) — surfaced, NOT auto-fixed (project rule). **25/49 were `driver_stable`** — the
  named field didn't change; the z moved from a DIFFERENT field or an availability flip (a field
  appearing/disappearing between editions), i.e. not a value error. 15 need manual review (figure
  not in the matched passage). Plus a **systematic bulk pattern**: 54 same-term `maternity 16→0`
  where amendment editions (*nota van wijziging*) omit maternity → extracted 0; index-irrelevant
  (maternity excluded from scoring) but a QA-data issue. Verification limited to the 95 curated
  `by_topic` CAOs (indices span 242).
- **2026-07-07 — over-time anomaly report enhanced** (`stability_check.py` → `out/stability_offenders.csv`):
  added an **overall** raw-score row, a `direction` (improved/worsened) column, and a
  `flag_regression` (SAME_TERM re-issue that worsened = likely extraction error). Statutory era-
  steps already excluded (raw variants). 443 same-term regressions surfaced (e.g. leave maternity
  16→0, fringe meal 25→2.5, pension early-retire 55→65 across re-issues) — a review queue.
- **2026-07-07 — unit-salvage audit (4 subagents).** Only ONE clean conversion win found &
  added: homeoffice `"eur over N years"` → `/(N·12)`. Everything else is genuinely different
  quantities (needs salary/distance/wage/premium → forbidden) or already captured descriptively;
  correctly available-case (esp. after the zero-fill fix). Details + extraction flags (training
  €-reimbursement mis-file; budget_pct base-mixing) in `unit_salvage_findings.md`. pension
  "fraction of premium" confirmed NOT ×100 (% of premium ≠ % of salary).
- **2026-07-07 — workbook cleanup.** `all_indices.xlsx`: added a front **Deck** tab (overview +
  two-scale/two-track/statutory explainer + tab index); dropped `PanelYearly` & `PanelAggregates`
  tabs; dropped derived columns (`doc_is_newest`, `year`, `months_since_file`, `is_stale`,
  `pub_lag_months`) from every tab; reordered; refreshed stale legend text (10→11 topics, 9→8
  coverage). Factor-tab communality handled in the §9 advanced rebuild.
- **2026-07-07 — `ai` dropped from the overall roll-up.** 99.3% of CAOs have no AI clause yet
  (era-artifact, not a bargaining choice) → removed from `COV`/`COVZ` in composite (and panel
  `COVC`). `out/ai_index.csv`/`ai_coverage` still produced for reference. Childcare KEPT (its zeros
  are genuine "offers nothing" declines = real signal; drop changes rank corr <0.006). §9.
- **2026-07-07 — statutory pseudo-CAO is now a FULL FLOOR.** Added EXTRA perks at 0 (severance-
  extra, 13th-month, meal/relocation/commuting, stipend, training budget/days/reimb, shift-
  allowance) and coverage booleans all-False (share 0, scored on each topic's coverage yardstick)
  across all topics except **pension** (available-case-only, excluded entirely). Result: the law
  ranks near the bottom each month (only ~4.5% of newest CAOs below its magnitude overall), except
  where the statutory minimum is genuinely good (2020s leave > a 2000s CAO — the era effect Hanna
  flagged). Panel STATUTORY rows carry the full floor. §7.
- **2026-07-07 — panel: wage forward-filled + rank columns added.** Wage is now an AS-OF
  (backward `merge_asof`) join per (cao, year) — a scale stays in force until renegotiated, so
  the last known year carries forward. Panel `wage_z` nulls 52.5% → 17.7% (remainder = CAOs with
  no wage data at all). Added `overall_gen01`, `coverage_overall_pctile`, `overall_combined01`
  (per-doc ranks, NOT law-month re-ranked — labelled). Panel law-month leave still uses the
  summed-FRE companion params (TODO: move to per-type to match composite).
- **2026-07-07 — parental leave scored PER TYPE, not as one summed FRE.** The 4 types were
  summed into one FRE then z'd, letting maternity's 16 wks dominate so paternity enhancements
  (the most-bargained, 76%) barely moved the score. Now each type is an equal-weight +1 field
  (`leave_<type>_z`, `leave_<type>_prank`); `leave_z`/`leave_gen01` = mean across types.
  **Maternity excluded** (uniformly 16 wks @100% WAZO → ~zero variance, no signal; kept as
  descriptive `maternity_fre_stat`). Summed-FRE kept as companion `leave_z_fre_total`. corr(new,
  old) = 0.973. `statutory_index` leave updated to per-type too. §10, §2 special-case note.
- **2026-07-07 — `out/date_collisions.csv` added** (`date_collisions.py`): the 8 CAO groups (16 docs)
  where 2+ full-CAO docs share an identical axis date; tie-break = richer extraction then higher
  id (all 8 resolved). None on fallback dates. Minor known edge: a few collide across *different*
  ingangsdatum on one publication day.
- **2026-07-07 — zero-fill bug fix: present-but-unconvertible ≠ absent.** A value stated in a
  unit we can't convert normalises to NaN; the old zero-fill treated that as blank → 0, scoring
  a real benefit as absent. Worst hit: **210 `term_severance_extra` docs** (unconditional fill,
  no gate) stated as "€ one-off" / "% of daily wage" → all scored 0. Fix (`apply_zerofill`): only
  zero-fill a NaN that was **never stated in any edition** of the CAO (raw cell empty, cummax
  within `cao_number`); present-but-unconvertible stays NaN = available-case (like pension). After
  fix: 0 wrongly-zeroed severance, 195 correctly available-case; genuinely-blank still fill.
  Complements the unit-salvage campaign (recover conversions → those docs become scored). §8.
- **2026-07-07 — equal-range percentile track (`gen01`) added for ALL topics + overall.**
  Cures the z-range asymmetry Hanna identified (best-in-a-bunched-field can't offset
  worst-in-a-spread-field, because reimbursement's z tops out at +0.33 while budget's reaches
  +3). Each field → ECDF percentile (NO winsor, NO ±3 clip — rank is outlier-proof) → averaged
  → `<topic>_gen01`, with `<field>_prank`, `<topic>_coverage_pctile`, `overall_gen01`,
  `coverage_overall_pctile`, `<topic>_combined01`, `overall_combined01`. z kept as the
  analytical scale (factor analysis, σ-distances). corr(overall z, gen01) ≈ 0.77 — similar
  ranking, but the gap is exactly the skewed fields. FA re-run on gen01 (`ADVANCED_ANALYSIS.md`
  §9) → same structure (KMO ≈ 0.54); battery §4 asserts bounds + direction. Naming: per-field
  rank suffix is `_prank` (NOT `_pct`, which is reserved for percent VALUE columns like
  `cost_reimb_pct`). §3.2.
- **2026-07-07 — `has_training_rights` added to training coverage (fix a).** Diagnosed a
  backwards ranking: a rights=True-but-unquantified doc (magnitude NaN, fund & mandatory
  both False) scored *below* a rights=False no-training doc, because the right lived in no
  track (excluded from coverage as near-constant). Adding it: near-constant ⇒ ~0 variance ⇒
  negligible dilution, but it flags the 1.4% rights=False docs → coverage 0/3 → they fall to
  the true bottom (combined −2.233) and **zero has-training docs now rank below the worst
  no-training doc** (was 62). Chose (a) over (b) "floor rights=False magnitude to −3" because
  (b) fabricates amount-extremity (0 days/€ isn't a rare value) and breaks magnitude
  semantics. §4.1, §10.
- **2026-07-07 — training presence-gated zero-fill (reversed the earlier "no zero-fill").**
  Gate = `has_training_rights` (98.4% True). The 42 rights=False docs state zero training
  numbers (verified) → blank = 0; the 237 rights=True-but-unquantified keep blank
  (available-case). Earlier reasoning conflated naive fill-all-blanks (would hit the 237)
  with presence-gated fill (only the 42). §8.
- **2026-07-07 — scale decision: keep clipped-z, not min-max, and not a smooth 0–1 squash
  as the core.** Rationale in §3.1–§3.2. Percentile (`*_pctile`) is the 0–1 presentation
  view; a z→[0,1] remap is available as cosmetic display only.
- **2026-07-06 — two-part over DISJOINT (info-loss fix).** Restored gate booleans to
  coverage after DISJOINT dropped the present-but-unquantified group. §8.2.
- **2026-07-06 — zero-fill promoted to primary for EXTRA absences; homeoffice entitlement
  dropped** (1.3% filled, degenerate). §8, §10.
- **2026-07-05 — Tier-1 absence index added; Tier-2/3 fields added across topics.**
- **(earlier) — statutory workbook established as ground truth**; `build_statutory.py`
  retired/guarded; `statutory_sync.py` derives CSVs *from* the workbook. §7.


### 2026-07-08 — hourly default-workweek fallback (coverage vs precision)
`mw_indices.to_monthly` converted hourly→monthly ONLY when the workweek was known
(point hours_basis, else row ft_hours), else dropped (`drop_hourly_no_basis`, ~69k points).
Several CAOs are hourly-ONLY with no stated workweek anywhere (1496 Bakkers 20,960 hourly
rows, 2535, 433, 35, 932) → they were entirely NaN in the wage index. FIX: fall back to
`DEFAULT_FT_HOURS = 38.0` (Dutch full-time norm) when both bases are absent, tagged
`hours_source='default'`. Impact: +7 CAOs into the index (205→212), +~51k points,
median_mw_mean €2542→€2530 (−0.5%, negligible) — the plausibility band still guards outliers.
Rationale: a 38h monthly proxy is far better than NaN for a wage-LEVEL/coverage index and
the near-flat median shows it doesn't distort the distribution. Auditable via hours_source.

### 2026-07-08 (refinement) — per-CAO workweek instead of blanket 38h
Replaced the flat DEFAULT_FT_HOURS=38 fallback with each CAO's ACTUAL full-time workweek.
Found the workweek from the source prose ("38-urige werkweek", "arbeidsduur 38 uur/week"):
49 CAOs by deterministic regex over the extracts + 9 more by one Haiku agent over hour-context
snippets = 58 CAOs (indices/review/cao_workweek.csv). Hours-basis priority for hourly rows is now:
point hours_basis > row ft_hours (parser) > cao_workweek.csv > STATUTORY_WORKWEEK=36h.
Firing: 85% of hourly points use the CAO-found workweek, 14% parser hours, 1% (836 pts, 10 CAOs
with no stated workweek: ranges/seasonal/maritime) the 36h statutory fallback. median_mw_mean
€2530 (unchanged) but conversion is now per-CAO-accurate (40h CAOs -> higher monthly, 36h -> lower).
STATUTORY_WORKWEEK=36 chosen as NL's statutory-minimum-hourly reference basis (no true NL statutory
workweek exists). tagged hours_source in the melt for audit.

### 2026-07-08 (correction) — multiple workweeks per file -> use the MODAL full-time one
Checked per-file (Hanna's Q): 43 of 49 CAOs state MULTIPLE distinct workweeks and 445 files alone
contain >1 (727: 32/37/38/40). Cause: a file mixes the main full-time norm with sub-sector norms
(Retail shops 38h vs distribution 40h) AND regex noise (a 30/32h part-time/ADV/leave mention).
FIX: (1) the parser's per-TABLE ft_hours from the title stays authoritative and is used FIRST, per
table; (2) for tables with NO stated workweek, use the CAO's MODAL full-time workweek from a
FULL-TIME-CONTEXT regex (arbeidsduur/voltijd/urige werkweek), excluding part-time/ADV/verlof/60-jaar
segments, taking the most frequent value. This corrected 11 CAOs vs the first-hit (35: 40->36,
750: 38->40, 3798: 38->36...) and flags 4 as ambiguous (592: 38/40/36, 1264: 40=38 tie) -> tagged
source='modal_ambiguous' in indices/review/cao_workweek.csv, still best-guess. Index impact negligible (median
€2530->€2530: a ±2h workweek is ~±5% on monthly, well within the pooled-z/band tolerance). The
residual gap-table ambiguity is inherent (source states the workweek in general provisions, not per
table) and is auditable via hours_source + the source column.
