# Generosity / Protection Indices — term, contract, overtime

> **v2 note (2026-07-05):** field choices, signs, unit handling and clamps in this document
> remain in force; the STANDARDISATION sections (within-year z, active-set z, 4 magnitude
> variants) are superseded by the pooled-z design in [INDICES_V2_PLAN.md](INDICES_V2_PLAN.md).

**Status:** v0 DESIGN, awaiting Hanna sign-off. No code built yet.
**Pattern:** design-doc-first → review → build (mirrors the leave index + Phase 4.5).
**Source:** `qa/corrected_dataset.csv`, **full-CAO records only** (`general_document_type`
starts with `full_cao` → 2,697 of 2,739; the 42 partial amendments/supplements/
annexes are excluded — their blanks are *missing data*, not statutory).

All fill-rates / value / unit tallies below are measured on the 2,697 full-CAO records.

---

## 0. Why these can't just reuse the leave FRE-weeks recipe

The leave index works because every leave component converts to one natural unit —
**weeks of full-rate-equivalent pay**. Term/contract/overtime have **no common
cardinal unit**: you cannot add "2 months notice" + "€4,000 severance" + "25%
overtime surcharge" into one meaningful number the way you add FRE-weeks.

So the construction is different:
1. Normalise each field to a **canonical unit** (below).
2. Apply statutory handling **per field** (floor / cap / tenure-graded / none — it
   is *not* uniform, even within one topic).
3. Assign each field an explicit **sign** (does "more" mean more or less generous?).
4. Publish **per-field normalised sub-scores** + a transparent **composite**
   (z-score or min-max, documented weights) — never a single black-box number.

This is more debatable than FRE-weeks, which is exactly why it needs sign-off before code.

---

## 1. Universal rules (all three topics)

### 1.1 Full-CAO filter
Compute only on `is_full_cao == True`. Carry `document_type` through for audit.

### 1.2 Unit normalisation — the top hazard
Same column mixes genuinely different units **and** different wordings of the same
unit. Reuse and extend the leave index's `duration_to_weeks` machinery as a
**shared `unit_normalize` module**. Canonical unit per field is fixed in §2–§4.
Concrete hazards measured:

| field | units seen (count) | canonical | hazard |
|---|---|---|---|
| `term_*_notice` | month×725, months×380, weeks×25, calendar days×17 | **months** | wording (month/months) + real (weeks/days) |
| `term_severance_extra` | EUR one-off, month salary, EUR, % of daily wage | **split by kind** | ⚠ 3 incompatible meanings in one column |
| `contract_ketenregeling_max_duration` | months×970, years×349, weeks×8 | **months** | years vs months |
| `overtime_trigger_daily` | hours×924, hours per day×128, **minutes×47** | **hours** | minutes must ÷60 |
| `overtime_*_allowance` | percent×390, percent of hourly wage×153, EUR per hour×6 | **% of hourly wage** | a few EUR/hour outliers |
| `contract_full_time_hours` | hours per week×2112, hours per year×125 | **hours/week** | annual vs weekly |

`term_severance_extra` is the worst: values are `1, 10, 2` (months of salary) mixed
with `4000` (EUR) mixed with `100` (%). **It cannot be a single index column.**
Proposed: split into `severance_months_salary`, `severance_eur`, `severance_pct`
by unit, and index only the months-of-salary variant (most comparable); surface the
others descriptively. Needs your call.

**Unit audit (generated — review these):** [term_unit_audit.csv](review/term_unit_audit.csv),
[contract_unit_audit.csv](review/contract_unit_audit.csv),
[overtime_unit_audit.csv](review/overtime_unit_audit.csv) — every distinct unit string per
field → canonical unit + conversion + action (convert/keep/flag/drop/split/exclude).
Most fields normalise cleanly to one canonical unit. Outcome — three fields need
special handling, one needs your rule:
- `term_severance_extra` → **SPLIT** ≈ months-of-salary 147 / EUR 105 / % 104 (roughly
  even thirds). Index the months-of-salary bucket; surface EUR + % separately.
- `overtime_unfavourable_hours_allowance` → **SPLIT**: % 1,183 but **184 EUR absolute**
  amounts (EUR/hour, EUR/shift…) that can't be %-indexed without an hourly-wage base.
- `overtime_compulsory_annual` → **EXCLUDE**: mixes hours/yr with counts of Sundays /
  night shifts / sea days — incommensurable; drop from the index (or keep as a separate
  descriptor only).
- **Wage-payment-period notice** (`loonbetalingstijdvak` — "pay period" / "payment
  period", ~60 cells across the notice fields): **RESOLVED → 1 calendar month (×1.0)**,
  the Dutch standard (BW 7:623, wages paid ≥ monthly). The 4-weekly (`vierwekelijks`,
  ×0.92) case is mostly temp/flex (uitzend) sectors — a small minority, not reliably
  identifiable per-CAO, so monthly is the default. Rows flagged in the audit.

### 1.3 Sign convention (explicit per field)
`+1` = larger value is **more** worker-generous; `−1` = larger value is **less**.
Stored in a per-field config so a sign is never implicit. (e.g. notice `+1`,
probation `−1`, overtime surcharge `+1`, ketenregeling-max-contracts `−1`.)

### 1.4 Statutory handling taxonomy (per field, post-hoc only — never in any prompt)
- **floor** → empty/silent = statutory minimum applies → impute it (e.g. overtime
  min-rest 11 h, ATW).
- **cap** → statutory maximum; do **not** impute into empties; flag values *beyond*
  the cap as extraction error (e.g. probation 2 mo, overtime max-hours).
- **tenure-graded → informational** → statutory value depends on tenure/age we
  don't hold per record; record the schedule for reference, do **not** impute a
  scalar (e.g. employer notice BW 7:672).
- **none** → no statutory anchor; empty = genuinely not provided → contributes 0 /
  NaN to the sub-score, never imputed (e.g. overtime surcharge %, severance amount).

Era-awareness still matters: **ketenregeling max duration changed 24→36 months on
1 Jan 2020 (WWZ→WAB)** — a statutory step exactly like the leave changes; select by
`ingangsdatum`. Reuse the leave index's date parser.

---

## 2. TERM — employment-security index

| field | fill | canonical | sign | statutory (BW) | empty → |
|---|---|---|---|---|---|
| `term_employer_notice_value` | 45% | months | **+1** | 7:672 tenure-graded → informational | NaN (don't impute scalar) |
| `term_employee_notice_value` | 59% | months | −1* | 7:672 = 1 month | 1 month |
| `term_notice_min_floor_value` | 27% | months | +1 | — | NaN |
| `term_probation_fixedterm_value` | 67% | months | **−1** | 7:652 **cap 2 mo** | NaN; flag if >2 |
| `term_probation_indef_value` | 70% | months | **−1** | 7:652 **cap 2 mo** | NaN; flag if >2 |
| `term_severance_extra_value` | 13% | **split** (mo-salary / EUR / %) | +1 | 7:673 transitievergoeding (+ era EUR caps in `era_baselines.py`) | NaN |

\* employee notice sign is debatable — shorter notice = more worker *freedom* but is
a minor component; consider weight ≈ 0 or drop. Your call.

Sub-index = sign-weighted z-scores of {employer_notice, probation (both), severance
months-salary}. Booleans `term_notice_tenure_present` (61%), `term_hetero_present`
(62%) are context, not inputs. Probation values cluster at the statutory 2-mo cap
(1,808 of indef = 2.0) — so probation mostly discriminates the *below-2* generous
CAOs.

---

## 3. CONTRACT — security / flexibility index

| field | fill | canonical | sign | statutory | empty → |
|---|---|---|---|---|---|
| `contract_ketenregeling_max_contracts_value` | 44% | contracts | **−1** | WWZ/WAB = 3 | 3 (statutory default) |
| `contract_ketenregeling_max_duration_value` | 50% | months | **−1** | **24 mo pre-2020 → 36 mo from 2020** | era default |
| `contract_full_time_hours_value` | 92% | hours/week | −1 | — (sector norm) | NaN (use as normaliser, not score) |
| `contract_workhours_adjustment_tenure_requirement_value` | 13% | months | −1 | Wfw | NaN |

More contracts / longer chain before a permanent contract = **less** security → sign
`−1`. The ketenregeling fields are a real statutory-fill case: empty = the statutory
default (3 contracts / era-appropriate duration) applies by law. Booleans
`ketenregeling_deviation_present` (69%), `conversion_rights_temp_to_perm_present`
(53%), `workhours_adjustment_right_present` (67%) → optional **coverage** sub-score.

Note 6-contract chains are common (361 records) — sectoral WAB deviations (seasonal
work); these are genuine, not errors.

---

## 4. OVERTIME — generosity + protection index

| field | fill | canonical | sign | statutory (ATW) | empty → |
|---|---|---|---|---|---|
| `overtime_allowance_value` | 36% | % of hourly wage | **+1** | none | NaN (not provided) |
| `overtime_unfavourable_hours_allowance_value` | 52% | % of hourly wage | +1 | none | NaN |
| `overtime_trigger_daily_value` | 44% | hours (÷60 for minutes!) | −1 | none (contractual) | NaN |
| `overtime_trigger_weekly_value` | 56% | hours | −1 | none (≈ the workweek) | NaN |
| `overtime_max_hours_per_week_value` | 67% | hours | −1 (protection) | **cap** 48 avg / 60 abs | flag if >60 |
| `overtime_min_rest_between_shifts_value` | 48% | hours | +1 (protection) | **floor 11 h** | 11 h |

Two conceptually distinct things live here — **pay generosity** (surcharge %) and
**protection** (rest/hours caps). Recommend **two sub-scores**, not one: a
`overtime_pay_score` (the two surcharge fields) and a `overtime_protection_score`
(rest floor + max-hours). Min-rest clusters at the statutory 11 h (720 records) — so
it mostly flags the *more* protective CAOs above 11. Watch `trigger_daily=0.5`
(165 records) — likely "0.5 h grace" or contamination; inspect before trusting.

---

## 5. Composite construction

Per topic: `score = Σ_field  sign_field × weight_field × z(normalised_value)`, where
`z` standardises across full-CAO records **within era** (so the 2020 ketenregeling
step and any statutory drift don't masquerade as bargaining differences). Default
weights = equal; published alongside the components so any reviewer can reweight.
min-max (0–1) offered as an alternative to z-scores for interpretability. Records
with too few populated fields get a `low_support` flag rather than a misleadingly
precise score.

Like leave, publish **two views**: `*_level` (with statutory floors imputed where
the taxonomy says floor) and `*_premium` (deviation from era-statutory, isolating
bargaining). Caps and none-fields appear only in the premium/raw view.

---

## 6. Output schema (per topic, one row per full-CAO record)

```
cao_number, id, ingangsdatum, file_name, document_type, is_full_cao, era_resolved,
<field>_norm        # canonical-unit value (or imputed floor)
<field>_z           # within-era standardised
<topic>_pay_score / _protection_score / _security_score   # sub-scores
<topic>_composite   # signed weighted sum
n_fields_populated, low_support, flags   # e.g. probation_over_cap, minutes_converted
```

Plus `<topic>_index_diagnostics.csv` (fill rates, unit-conversion counts, flag
counts) and a **`<topic>_unit_audit.csv`** (every distinct unit string per field →
canonical mapping) so the normalisation is reviewable the way you asked.

---

## 7. Decisions needed before build

1. **Severance** (`term_severance_extra`): split by unit and index only
   months-of-salary? Or model EUR via the BW 7:673 formula? (Recommend split.)
2. **Composite method**: z-score (default) vs min-max; equal weights vs your weights.
3. **Employee-notice sign**: +1, −1, or drop (weight 0)?
4. **Overtime**: two sub-scores (pay + protection) vs one — recommend two.
5. **Statutory taxonomy per field**: confirm the floor/cap/informational/none
   assignments in §2–§4 (these get encoded post-hoc, never shown to a model).
6. Confirm **full-CAO-only** (excludes 42 partials) and **within-era standardisation**.

## 8. Build plan (after sign-off)
1. Shared `unit_normalize.py` (extend leave's duration logic; add per-field canonical
   maps) + `<topic>_unit_audit.csv` for your review **first**.
2. `term_index.py`, `contract_index.py`, `overtime_index.py` on the full-CAO set.
3. Validate each against 10–15 source-read records; sanity-plot score distributions
   by era; reconcile against the `_present` booleans.
