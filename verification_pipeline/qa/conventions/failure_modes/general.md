# General failure modes (cross-topic)

Cap: 6 entries. Loaded into every subagent prompt regardless of topic.
See PLAN.md §3.2.1 for the strict entry template.

### GENERAL_FM_01 — Article numbers extracted as values

**Trigger**:
  - In source: contains "Article <N>", "## <N>", or "section <N>" near topic
  - In CSV state: any *_value field equals <N> from the article reference
**Wrong outcome**: field=<any *_value>, csv_value_new=<article number>
**Correct outcome**: verdict=clear
**Concrete example**:
  - Source quote: "Artikel 91: Zwangerschapsverlof"
  - Before: leave_paid_maternity_value=91 weeks
  - After: cleared

### GENERAL_FM_02 — Decimal-point strip

**Trigger**:
  - In source: "0.XX%", "0,XX%" or similar
  - In CSV state: pay-rate field with value > 100
**Wrong outcome**: field=<*_pay_value>, csv_value_new=<XX without decimal>
**Correct outcome**: verdict=correct_in_place, csv_value_new=<0.XX>, unit_new=%
**Concrete example**:
  - Source quote: "premie van 0,43% van het pensioengevend salaris"
  - Before: pension_employer_premium_value=43
  - After: pension_employer_premium_value=0.43, unit=%

### GENERAL_FM_03 — Year-like values in duration fields

**Trigger**:
  - In CSV state: any *_value (non-pay) in 1990-2030 range
  - In source: same number as a date or year reference
**Wrong outcome**: field=<any duration *_value>, csv_value_new=<year>
**Correct outcome**: verdict=clear
**Concrete example**:
  - Source quote: "Per 1 januari 2014 is de regeling gewijzigd"
  - Before: leave_partially_paid_paternity_value=2014
  - After: cleared

### GENERAL_FM_04 — "X found in source" extractor garbage

**Trigger**:
  - In evidence_quote (prior run): literal "<number> found in source"
  - In CSV state: any *_value matching <number>
**Wrong outcome**: field=any, csv_value_new=<number>
**Correct outcome**: verdict=clear
**Concrete example**:
  - Before: leave_paid_maternity_value=16, evidence="16 found in source"
  - After: cleared

### GENERAL_FM_05 — Field-type / unit mismatch

**Trigger**:
  - In CSV state: pay-rate field with unit ∈ {weeks, days, hours, months},
    OR duration field with unit ∈ {%, percent}
**Wrong outcome**: value is in wrong field for its unit semantics
**Correct outcome**: verdict=move (target_field matching unit) OR clear if no fit
**Concrete example**:
  - Before: leave_paid_maternity_pay_value=16, unit=weeks
  - After: move → leave_paid_maternity_value=16, unit=weeks

### GENERAL_FM_06 — Source-missing extrapolation

**Trigger**:
  - In evidence_quote: "(section missing)", "(no source)", "consistent with
    CAO X pattern", or any reference to sibling records
**Wrong outcome**: confidence ∈ {high, medium} with no real source evidence
**Correct outcome**: verdict=unable_to_verify, confidence=low
**Concrete example**:
  - Before: verdict=confirm, confidence=high, evidence="(section missing)"
  - After: verdict=unable_to_verify, confidence=low
