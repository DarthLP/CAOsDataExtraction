# Topic-specific failure modes — overtime

Cap: 10-12 entries. Loaded only when the subagent's topic is `overtime`.

### OVERTIME_FM_01 — Multi-tier rates collapsed to single allowance

**Trigger**:
  - In source: pattern of distinct percentages tied to day/time
    (e.g. "150% Mon-Sat, 200% Sat 12:00-24:00 / Sun / holiday")
    OR sequential surcharge tables
  - In CSV state: `overtime_allowance_value` captures one rate;
    `overtime_allowance_range_min/max` may or may not be filled

**Wrong outcome**:
  - field=overtime_allowance_value, csv_value_new=<single rate, e.g. 150>
  - notes=empty (full tier schedule lost)

**Correct outcome**:
  - verdict=correct_in_place
  - csv_value_new=<FIRST tier shown in source>, csv_unit_new=% of hourly rate
  - notes="tier_schedule=150/200 Mon-Sat/Sat-Sun-holiday"
  - if multiple tiers AND no `hetero_present`-style flag in CSV:
    set csv_value_new to lowest tier, notes captures full schedule

**Concrete example**:
  - Source quote: "200% Saturday 12:00-24:00 and Sunday/holiday; 150% other overtime"
  - Before: overtime_allowance_value=150, notes=""
  - After: overtime_allowance_value=150, notes="tier_schedule=150/200 across other/Sat-Sun-holiday"


### OVERTIME_FM_02 — Age-conditional compulsory overtime captured verbatim

**Trigger**:
  - In source: "aged 50/55+ not obliged", "youngsters under 18 may not work overtime",
    "ouder dan 55 niet verplicht tot overwerk"
  - In CSV state: `overtime_compulsory_annual_value` is empty
    OR `overtime_guaranteed_weekends_off_rule_text` is empty

**Wrong outcome**:
  - clearing the rule because it doesn't fit a numeric field
  - inventing a value for `compulsory_annual_value`

**Correct outcome**:
  - verdict=move OR correct_in_place
  - target_field=overtime_guaranteed_weekends_off_rule_text
  - csv_value_new=<verbatim rule, ≤200 chars>
  - confidence=high if source quote is unambiguous

**Concrete example**:
  - Source quote: "Werknemers van 55 jaar en ouder zijn niet verplicht overwerk te verrichten."
  - Before: overtime_guaranteed_weekends_off_rule_text=""
  - After: overtime_guaranteed_weekends_off_rule_text="55+ not obligated for overtime"


### OVERTIME_FM_03 — Per-group hetero rates noted but groups not captured

**Trigger**:
  - In source: distinct overtime rules for two named groups (e.g. drivers vs technical staff;
    full-time vs part-time; technical units of 5 vs 10 persons)
  - In CSV state: `overtime_hetero_present` is False OR
    `overtime_allowance_range_min/max` empty despite source showing distinct values

**Wrong outcome**:
  - field=overtime_allowance_value, csv_value_new=<one group's rate>
  - hetero_present remains False

**Correct outcome**:
  - verdict=correct_in_place on `overtime_hetero_present` → True
  - populate `overtime_allowance_range_min` + `_max` + `_unit`
  - notes="groups=drivers,technical_staff (or whatever the source identifies)"

**Concrete example**:
  - Source quote: "Drivers: 125%. Technical personnel: 150%."
  - Before: overtime_allowance_value=125, hetero_present=False
  - After: overtime_hetero_present=True, allowance_range_min=125, _max=150,
    notes="groups=drivers,technical"


### OVERTIME_FM_04 — Overtime rate expressed as "% of monthly salary per hour"

**Trigger**:
  - In source: sub-1% value (e.g. "0,78%", "0,89%", "1,12%") combined with "monthly salary" / "maandsalaris" / "month's salary"
  - In CSV state: `overtime_allowance_value` is empty OR populated with the integer (e.g. 78 instead of 0.78)

**Wrong outcome**:
  - Treating the sub-1 value as a decimal-strip case (GENERAL_FM_02) and either clearing it
    or multiplying it by 100. The number IS sub-1% and the unit is "% of monthly salary per hour", not "% of hourly rate".

**Correct outcome**:
  - verdict=correct_in_place
  - csv_value_new=<sub-1 value verbatim, e.g. 0.78>, csv_unit_new=**"% of monthly salary"** (NOT "% of hourly rate")
  - notes="rate basis is monthly salary, not hourly rate"

**Concrete example**:
  - record_id=824017, CAO 824. Source: "0,78% of monthly salary per overtime hour, weekday first 2 hours"
  - Before: overtime_allowance_value="" (or =78, unit="%")
  - After: overtime_allowance_value=0.78, unit="% of monthly salary"
  - Note: GENERAL_FM_02 (decimal-strip) does NOT apply here — verify the unit semantics first.


### OVERTIME_FM_05 — Agency CAO (uitzendbureau) defers all rates to hirer's CAO

**Trigger**:
  - In source: "remains part of the hirer's remuneration", "inlenersbeloning", "Specific rates are not detailed in the CAO text", "Working Hours Act applies"
  - CAO type: agency / uitzendbureau (most commonly CAO 633 = ABU, CAO 1060 = NBBU)
  - Multiple worksheet items for the same record_id return `unable_to_verify` together

**Wrong outcome**:
  - Subagent extracts a value from the hirer-related text (e.g. "20% surcharge" mentioned only as an example), OR
  - Setting `has_overtime_rules=False` (the agency CAO DOES have meta-rules; it just doesn't carry numeric values for the working CAO).

**Correct outcome**:
  - verdict=unable_to_verify for ALL numeric overtime fields on this record
  - confidence=low
  - evidence_quote captures the defer-to-hirer language verbatim
  - notes="agency CAO; values deferred to hirer's CAO per inlenersbeloning rule"
  - has_overtime_rules can be True (the CAO regulates overtime), but numeric fields stay empty

**Concrete example**:
  - record_id=633030, CAO 633 (ABU). Source: "Specific rates are not detailed in the CAO text" — defers to hirer.
  - All 20 numeric fields in that chunk → unable_to_verify with confidence=low.


### OVERTIME_FM_06 — Period-mismatched trigger or compulsory cap (schema gap, not a fix)

**Trigger**:
  - Source states a trigger or cap in a period that doesn't map to `trigger_daily`/`trigger_weekly`/`compulsory_annual`. Common patterns:
    - "more than 432 hours per 12 weeks"
    - "10 hours per 4-week period"
    - "988 hours per 26 weeks"
    - "quarterly trigger" / "kwartaal" / "3-month period"

**Wrong outcome**:
  - Subagent invents a per-week or per-year approximation without source basis (e.g. 432/12=36/week, or 10×13=130/year).

**Correct outcome**:
  - verdict=unable_to_verify (the schema can't represent the period)
  - evidence_quote captures the period-specific phrasing verbatim
  - notes="period-mismatch: source uses <N-week/quarterly> aggregate, no daily/weekly/annual breakdown"

**Note**: This is a schema gap rather than a recoverable failure. The correct answer is already "unable_to_verify"; this entry documents the pattern so subagents don't fabricate.

**Concrete example**:
  - record_id=26017, CAO 26 (Vleessector). Source: "more than 432 hours per 12 weeks or 468 hours per 3 months."
  - For overtime_trigger_weekly_value: verdict=unable_to_verify, evidence_quote="more than 432 hours per 12 weeks", notes="period-mismatch: 12-week aggregate, not weekly".
