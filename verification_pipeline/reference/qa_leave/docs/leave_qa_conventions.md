# Leave-field schema conventions (QA edition)

These are the rules the QA pipeline applies to interpret `leave_*` fields.
They make the schema self-consistent across statutory regimes that have
changed over time (parental leave 2009 / 2022; paternity 2019 / 2020).

## Core convention: "statutory ⇒ empty values"

When a CAO simply restates the statutory baseline for a topic, the record
should look like this in the CSV:

```
leave_<topic>_statutory_ref = True
leave_<topic>_exceptions    = False
leave_<topic>_<sub_field>_value = ''     ← empty
leave_<topic>_<sub_field>_unit  = ''     ← empty
... all other sub-fields empty as well
```

This is already endorsed by the schema documentation
(`inputs/NON_SALARY_PROMPTS_AND_SCHEMA.md`):

> *"When parental_statutory_ref true and parental_exceptions false, omit
> eligibility sub-fields."*

The QA pipeline extends the same rule to all `leave_*` topics (parental,
care, paternity, adoption, maternity), and to ALL sub-fields (not just
eligibility).

A consumer of the data who needs the actual numbers for a statutory-only
record looks them up via the era-baseline table below, keyed on
`ingangsdatum`.

## Why this convention

1. **Resolves the post-2022 parental-leave ambiguity.** Post-Aug-2022 the
   statutory parental leave is 26 weeks total, of which 9 are UWV-paid at
   70% and 17 are unpaid. Recording the unpaid portion (17) is awkward
   because the schema lacks a `parental_partially_paid_value` field for
   the duration of the paid portion. With this convention, statutory
   records simply have empty fields and the question disappears.

2. **Avoids data drift across regime changes.** A CAO from 2008 stating
   "13 weeks unpaid parental leave" is restating that era's statutory
   baseline. A CAO from 2015 stating "26 weeks" is restating its era's
   baseline. Without era-aware handling these read as inconsistent; under
   the convention they are both just "statutory" with empty values.

3. **Reduces false positives in deterministic rules.** With statutory
   restatements cleared, deterministic L1 rules (e.g. "exceptions=False
   but value populated") only fire on real deviations.

## Era-statutory table

### Parental leave (`leave_parental_*`)

| Era boundary | Total leave | Paid portion | Unpaid portion | Source |
|---|---|---|---|---|
| pre 2001-12-01 | 6 weeks unpaid | – | 6 weeks | Pre-WAZO |
| 2001-12-01 to 2009-01-01 | 13 weeks unpaid | – | 13 weeks | WAZO art. 6:2 |
| 2009-01-01 to 2022-08-02 | 26 weeks unpaid | – | 26 weeks | Wet uitbreiding ouderschapsverlof (Stb. 2009 nr 87) |
| 2022-08-02 onwards | 26 weeks total | 9 weeks @ 70% UWV | 17 weeks | Wet betaald ouderschapsverlof |

A record matches statutory when ALL of:
* `parental_statutory_ref = True`
* `parental_exceptions = False`
* No min-tenure / min-contract-length set
* No employer top-up beyond the era's UWV-funded statutory rate
* `unpaid_value` is empty OR equal to the era-baseline unpaid weeks
  (post-Aug-2022 we accept either 17 or 26: 17 = unpaid portion only,
  26 = total, both readable as "statutory")

### Paternity / partner leave (`leave_paid_paternity_*`, `leave_partially_paid_paternity_*`, `leave_unpaid_paternity_*`)

| Era boundary | Paid days | Partially-paid weeks | Pay rate (partial) | Source |
|---|---|---|---|---|
| pre 2019-01-01 | 2 days | – | – | WAZO original kraamverlof |
| 2019-01-01 to 2020-07-01 | 5 days (1 week) | – | – | Wet invoering geboorteverlof |
| 2020-07-01 onwards | 5 days | 5 weeks | 70% via UWV | Wet invoering extra geboorteverlof (WIEG) |

### Care leave (`leave_short_term_care_*`, `leave_long_term_care_*`)

Stable since WAZO 2001-12-01:

| Type | Duration | Pay |
|---|---|---|
| short-term (kortdurend zorgverlof) | 2× weekly working hours per 12 months | minimum 70% of wages |
| long-term (langdurend zorgverlof) | 6× weekly working hours per 12 months | unpaid (0%) |

### Adoption leave (`leave_adoption_*`)

| Era boundary | Duration | Pay rate | Source |
|---|---|---|---|
| pre 2015-01-01 | 4 weeks | UWV at maximum daily wage | WAZO original |
| 2015-01-01 to 2019-01-01 | 4 weeks | UWV at 100% | (kept) |
| 2019-01-01 onwards | 6 weeks | UWV at 100% | Wet betaald adoptieverlof |

## Detecting deviations

A record is a real deviation (i.e. exceptions should be True, or values are
non-statutory and need to remain populated) when ANY of:

1. An eligibility threshold is set (`min_tenure_value > 0` or `min_contract_length_value > 0`).
   Statutory has no tenure requirement.
2. Employer top-up beyond statutory (parental: any topup_pay > the era's
   UWV-funded portion; care short-term pay > 70%; care long-term pay > 0).
3. A duration value differs from the era baseline (e.g. parental unpaid =
   13 weeks in a 2024 CAO).
4. A pay rate above statutory.

The `qa_leave_consistency_scan.py` script implements these tests with era
awareness and writes `outputs/leave_consistency_scan.csv`.

## Cleanup pass

Before C2 review by subagents, the corrections pipeline emits
`STATUTORY_CLEAR` corrections for any record matching the era-baseline.
This ensures only genuine deviations remain on the C2 review queue.

Layer name in `corrections_deterministic.csv`: `L0_STATUTORY_CLEAR`.

```
fix_method = statutory_clear
source_layer = L0_STATUTORY_CLEAR_<topic>
csv_value_old = <whatever the CSV had>
csv_value_new = (empty)
unit_new = (empty)
```

## Practical implication for downstream code

Code that aggregates leave durations across CAOs MUST do an era-aware
lookup against the table above when a record's value field is empty AND
its statutory_ref is True. A blank field is a presence claim ("matches
statutory"), not absence of data.

A truly absent record is one where `statutory_ref` is False or empty AND
all sub-fields are empty.
