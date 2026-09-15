# Phase 4.5 — Era-Baseline Review (for Hanna's approval)

**Drafted by Claude Code, 2026-05-23. Blocking gate for `term` and `pension`.**
Per `qa/PLAN.md` §7 (rows 10–11), §10 (Phase 4.5), and §12 ("Era baselines for
pension and term — wrong baseline produces systematic false 'below-statutory'
clears. Hanna verifies `era_baselines.py` for these topics before running them.")

---

## 0. What you are approving, and why it blocks two topics

`qa/shared/era_baselines.py` supplies the **era-statutory baseline** for a field at a
record's `ingangsdatum`. Two pipeline behaviours depend on it (PLAN.md conventions #2 and #3):

- **Convention #2 — statutory minima are floors.** If a subagent's reading produces a
  value *below* the era baseline, that's treated as an **extraction error** (verdict
  `clear`/`move`), not a real CAO deviation.
- **Convention #3 — statutory restatement ⇒ empty values.** When a CAO merely restates
  the statutory baseline, the schema convention is to leave sub-fields empty.

So a **wrong baseline silently corrupts QA**: too high → real CAO values get wiped as
"below statutory"; too low → genuine sub-statutory extraction errors slip through. `term`
and `pension` are the two topics where this bites hardest, which is why they are gated on
your sign-off.

**This document does not change any code.** It lists the values I propose to encode, each
marked *proposed — verify*, with the statute and my confidence. After you approve/correct
them, I will write them into `era_baselines.py`, add regression tests, and only then run
term and pension. Nothing is committed to the code until you sign off here.

---

## 1. CRITICAL: the file is still a Phase-1 skeleton

`era_baselines.py` today contains **only the 4 leave baselines** (`_LEAVE_BASELINES`).
`EraBaseline.__init__` does:

```python
if topic == "leave":
    self._baselines = _LEAVE_BASELINES
else:
    self._baselines = []        # populated in Phase 2 / 4.5  ← never happened
```

So **every non-leave topic currently gets an empty baseline list.** There is no
transitievergoeding formula, no AOW table, no Witteveen cap, no ketenregeling data. The
non-leave topics QA'd so far (overtime…wage) didn't need floors, so this was latent — but
term and pension cannot run meaningfully against `[]`. Phase 4.5 is therefore **"approve
the values to add," not "review existing code."**

---

## 2. DECISION NEEDED: floors vs caps vs informational

`era_baselines.py` only models **floors** — its one comparison method is
`is_below_statutory(...)`. But the term/pension baselines come in **three kinds**, and
treating them all as floors would be wrong:

| Kind | Meaning | Violation = error when… | Examples |
|---|---|---|---|
| **FLOOR** | CAO may grant ≥ statutory, never less | CAO value **below** baseline | transitievergoeding; pension franchise (min) |
| **CAP** | CAO may grant ≤ statutory, never more | CAO value **above** baseline | probation length; Witteveen accrual rate |
| **INFORMATIONAL** | CAO may legally deviate either way | *never* auto-flag; baseline is context for the subagent only | employer/employee notice; pensioenrichtleeftijd; AOW-auto-end age; ketenregeling |

**Proposed code change** (for your approval): add a `direction` field to `BaselineSpec`
(`"floor" | "cap" | "informational"`) and a companion `is_outside_statutory(...)` that
checks below-for-floor / above-for-cap / never-for-informational. The subagent prompt
keeps receiving the baseline as context in all three cases ("statutory X at this date was
Y"); only floors and caps produce an auto-`clear`/`move` signal.

- [ ] **Approve** adding `direction` to `BaselineSpec` (floor/cap/informational), or
- [ ] keep floor-only and I encode **only** the FLOOR baselines, treating caps/informational as prompt context with no auto-flag.

---

## 3. TERM — proposed baselines

Fields in scope (from the CSV header): `term_employer_notice_*`, `term_employee_notice_*`,
`term_notice_min_floor_*`, `term_probation_fixedterm_*`, `term_probation_indef_*`,
`term_severance_extra_*`/`_formula`, `term_end_at_AOW_auto`.

### 3.1 Notice periods — BW 7:672 — **INFORMATIONAL** (CAO deviation permitted)
Statutory **employer** notice by length of service; **employee** notice 1 month.

| Service | Employer notice | Confidence |
|---|---|---|
| < 5 yr | 1 month | high |
| 5–<10 yr | 2 months | high |
| 10–<15 yr | 3 months | high |
| ≥ 15 yr | 4 months | high |
| (employee, any) | 1 month | high |

Stable since the 1999 Flexwet; **unaffected by WWZ/WAB** → single era, `1999-01-01`→open.
**Treat as informational, not a floor:** a CAO can lawfully shorten employer notice and
lengthen employee notice (BW 7:672 lid 6–7). So a CAO notice below the schedule is *not*
automatically an extraction error. Use it only as subagent context.

- [ ] Confirm the schedule and the "informational, not floor" treatment.

### 3.2 Probation (proeftijd) — BW 7:652 — **CAP**
Maximum probation length; a CAO **cannot exceed** these:

| Contract length | Max probation | Era |
|---|---|---|
| ≤ 6 months | **0** (none permitted) | from **2015-01-01** (WWZ) |
| > 6 months, < 2 years | 1 month | all |
| ≥ 2 years / indefinite | 2 months | all |

Era boundary: the "no probation for ≤6-month contracts" rule began **1 Jan 2015**; before
that, short contracts could carry probation. Confidence: high.

- [ ] Confirm caps + the 2015-01-01 boundary, and that probation is a **cap** (value above max = extraction error / unlawful clause).

### 3.3 Transitievergoeding (statutory severance) — BW 7:673 — **FLOOR** (the cleanest one)

| Era | Eligibility | Formula | Cap (indexed) |
|---|---|---|---|
| **pre-2015-01-01** | — | *no statutory transitievergoeding*; kantonrechtersformule (A×B×C) via courts | n/a |
| **2015-01-01 → 2019-12-31** (WWZ) | ≥ 24 months service | 1/6 month-salary per completed 6 mo for first 10 yr (=⅓ mo/yr); 1/4 per 6 mo beyond 10 yr (=½ mo/yr). 50+ surcharge until 2020. | €75k(2015) → €81k(2019), or 1 annual salary if higher |
| **2020-01-01 →** (WAB) | from day 1 | **⅓ month-salary per year of service, pro-rata**; no 10-yr step-up, no 50+ surcharge | €83k(2020) → ~€98k(2025) |

Confidence: **high on the formulas and era boundaries**; **medium on the exact annual cap
figures** (indexed yearly — I'd rather you supply the authoritative cap table than I guess).
Use as a FLOOR for `term_severance_extra_*`: a CAO severance reading below the era
transitievergoeding for the relevant tenure ⇒ likely extraction error or statutory
restatement (→ empty per convention #3).

- [ ] Confirm formulas + era dates. - [ ] Supply/confirm the annual **cap** table (2015–2026).

### 3.4 Auto-end at AOW age — `term_end_at_AOW_auto` — **INFORMATIONAL** (uses §5 AOW table)
The pensioenontslagbeding (BW 7:669 lid 4) lets a contract end at AOW age. The AOW age
itself is the era table in **§5**. Boolean field, so no floor/cap — the AOW table is only
context for interpreting "ends at state pension age."

---

## 4. PENSION — proposed baselines

Fields: `pension_accrual_rate_*`, `pension_franchise_*`, `pension_retire_age_normal_*`,
`pension_retirement_age_early_*`, `pension_retire_age_deferred_*`,
`pension_employee_contrib_*`, `pension_premium_total_*`, `pension_premium_eq_split`, etc.

### 4.1 Witteveen accrual cap — `pension_accrual_rate` — **CAP**
Maximum fiscally-facilitated annual accrual; a scheme **cannot exceed** these:

| Era | Middelloon (avg-pay) | Eindloon (final-pay) |
|---|---|---|
| until 2013-12-31 | 2.25% | 2.00% |
| 2014-01-01 | 2.15% | 1.90% |
| 2015-01-01 | **1.875%** | 1.657% |

**WTP caveat:** the Wet toekomst pensioenen (in force **1 July 2023**) phases out DB
accrual caps as schemes convert to flat-premium DC (transition window to **1 Jan 2028**).
So the accrual-rate cap is only meaningful for records **before** a scheme's WTP
conversion. Confidence: high on rates/dates; the WTP transition needs your judgment on how
to treat 2023–2028 records.

- [ ] Confirm middelloon/eindloon caps + dates + how to handle WTP-transition records.

### 4.2 AOW-franchise (minimum franchise) — `pension_franchise` — **FLOOR, annually indexed**
The minimum franchise (salary slice not pensioned, since AOW covers it) is set yearly and
depends on scheme type (middelloon vs eindloon, and a permitted lower franchise for some
schemes). I am **not confident on exact figures** (roughly €14k–€18k, 2021–2024). **Please
supply the authoritative annual minimum-franchise table** rather than have me guess.
Confidence: low on values, high that it's a floor.

- [ ] Supply the minimum-franchise table (by year, by scheme type) — or mark this field "no auto-flag."

### 4.3 Normal / early / deferred retirement age — **INFORMATIONAL**
- **Pensioenrichtleeftijd** (fiscal target age, distinct from AOW): 65 until 2013 → **67**
  (2014-01-01) → **68** (2018-01-01). Many DB schemes set normal age to this.
- **AOW age**: separate, see §5. Some schemes peg normal retirement to AOW.
- Early/deferred ages are scheme-specific (RVU early-exit window 2021–2025 ≈ up to 3 yr
  before AOW). No single statutory value → treat all three as **informational** context,
  no auto-flag. Confidence: high on pensioenrichtleeftijd dates.

- [ ] Confirm pensioenrichtleeftijd progression + informational treatment.

### 4.4 Premium / contribution / split — `pension_employee_contrib`, `pension_premium_total`, `pension_premium_eq_split` — **NO BASELINE**
There is **no statutory floor or cap** on pension premium, the employee/employer split, or
a 50/50 split — these are entirely CAO/fund-determined. I propose **no era baseline** for
these fields, so the subagent must never flag them as below/above statutory. Flagging this
explicitly so we don't accidentally invent a baseline.

- [ ] Confirm: no baseline for premium/contribution/split.

---

## 5. AOW age table (shared by term §3.4 and pension §4.3) — **reference data**

AOW eligibility age by the calendar year the person reaches it:

| Year | AOW age | | Year | AOW age |
|---|---|---|---|---|
| 2013 | 65 + 1 mo | | 2020 | 66 + 4 mo |
| 2014 | 65 + 2 mo | | 2021 | 66 + 4 mo |
| 2015 | 65 + 3 mo | | 2022 | 66 + 7 mo |
| 2016 | 65 + 6 mo | | 2023 | 66 + 10 mo |
| 2017 | 65 + 9 mo | | 2024 | 67 + 0 mo |
| 2018 | 66 + 0 mo | | 2025 | 67 + 0 mo |
| 2019 | 66 + 4 mo | | 2026 | 67 + 0 mo |
| | | | 2027 | 67 + 3 mo *(provisional)* |

Confidence: high through 2026; 2027+ depends on the life-expectancy formula. Used as
context only (informational).

- [ ] Confirm the AOW table (esp. 2025–2027).

---

## 6. CONTRACT — ketenregeling (already QA'd; included for completeness / re-run)

`contract` is already `done`, but the WAB transition heavily exercises era logic
(PLAN.md §7 row 4) and a future re-run would benefit. BW 7:668a chain rule — **INFORMATIONAL**
(CAO deviation is explicitly permitted for seasonal/uitzend):

| Era | Max contracts | Max duration | Reset gap |
|---|---|---|---|
| pre-2015-07-01 | 3 | 36 months | > 3 months |
| 2015-07-01 → 2019-12-31 (WWZ) | 3 | **24 months** | > 6 months |
| 2020-01-01 → (WAB) | 3 | 36 months | > 6 months |

`max_contracts_value` = 3 throughout; `max_duration_value` = 36 → 24 → 36. Confidence: high.

- [ ] Confirm (optional — only matters if contract is re-run).

---

## 7. Open questions I cannot resolve without you

1. **Annually-indexed figures** — transitievergoeding caps (§3.3) and the minimum franchise
   (§4.2). I'd rather you supply the authoritative tables than I guess.
2. **WTP transition (2023–2028)** — how should the accrual-rate cap (§4.1) treat records in
   the conversion window? Hard cap until each scheme converts? No flag during transition?
3. **Floor/cap/informational model** (§2) — approve the `direction` field, or restrict me to
   floors only?
4. **Severance below-statutory action** — confirm verdict should be `clear` when a CAO
   severance reads below the era transitievergoeding (per convention #2), vs `move`.

---

## 8. After your approval — what I'll do (no action until then)

1. Encode the approved baselines in `qa/shared/era_baselines.py` (with `direction` if
   approved in §2), wiring `term`, `pension`, and `contract` lists in `EraBaseline.__init__`.
2. Add regression tests (`test_era_baselines.py`): floor below → flag; cap above → flag;
   informational → never flag; era-boundary dates resolve to the right spec.
3. Run **term** end-to-end (Opus; smaller 15-item chunks per §6/§4.5; flag the first 2–3
   Opus chunks for your extra review since the Opus path has no port-time regression test).
4. Run **pension** end-to-end (Opus), same first-chunks-flagged caution.
5. Update the README board + this doc's status to "approved / encoded."

**Until §2–§7 are signed off, term and pension stay blocked.**

---

### Approval summary (tick to approve, or annotate to correct)
- [ ] §2 floor/cap/informational model (+ `direction` field)
- [ ] §3.1 notice periods (informational)
- [ ] §3.2 probation caps + 2015 boundary
- [ ] §3.3 transitievergoeding formulas + era dates **+ cap table**
- [ ] §4.1 Witteveen accrual caps + WTP handling
- [ ] §4.2 minimum-franchise table (or "no flag")
- [ ] §4.3 pensioenrichtleeftijd + retirement ages (informational)
- [ ] §4.4 no baseline for premium/contribution/split
- [ ] §5 AOW age table
- [ ] §6 ketenregeling (optional)
- [ ] §7 open questions answered
