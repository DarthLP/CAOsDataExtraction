# Topic-specific failure modes — leave

Cap: 10-12 entries. Loaded only when the subagent's topic is `leave`.

### LEAVE_FM_01 — Kraamverlof / geboorteverlof confused with maternity

**Trigger**:
  - In source: "kraamverlof", "geboorteverlof", or "birth leave"
  - In CSV state: leave_paid_maternity_value is set; leave_paid_paternity_value empty
**Wrong outcome**: field=leave_paid_maternity_value, csv_value_new=<paternity duration>
**Correct outcome**: verdict=move, target_field=leave_paid_paternity_value
**Concrete example**:
  - Source quote: "Bij geboorte van een kind: 1 week kraamverlof met volledig loon"
  - Before: leave_paid_maternity_value=1 week
  - After: leave_paid_paternity_value=1 week

### LEAVE_FM_02 — Tiered sick-pay schedules collapsed

**Trigger**:
  - In source: "100% / 90% / 85% / 70%" or "Year 1 100%, Year 2 70%"
  - In CSV state: only one tier captured in leave_sick_continuation_value
**Wrong outcome**: field=leave_sick_continuation_value, csv_value_new=<just one tier>
**Correct outcome**:
  - verdict=correct_in_place
  - csv_value_new=<FIRST tier>, unit_new=%
  - notes="tier_schedule=100/90/85/80 across 26+26+26+26 weeks"
**Concrete example**:
  - Source quote: "In year 1: 100%; year 2: 70% of base salary"
  - Before: leave_sick_continuation_value=70
  - After: leave_sick_continuation_value=100, notes="tier_schedule=100/70 across year1/year2"

### LEAVE_FM_03 — WIEG supplementary paternity misfiled

**Trigger**:
  - In source: "aanvullend geboorteverlof", "supplementary birth leave", "WIEG",
    "up to 5 weeks" + "UWV", or "70%"
  - In CSV state: leave_partially_paid_paternity_value empty AND
    leave_paid_paternity_value=5 (or leave_unpaid_paternity_value=5)
  - ingangsdatum: 2020-07-01 or later
**Wrong outcome**: field=leave_paid_paternity_value OR leave_unpaid_paternity_value
**Correct outcome**:
  - verdict=move, target_field=leave_partially_paid_paternity_value
  - csv_value_new=5, unit_new=weeks
  - also set leave_partially_paid_paternity_pay_value=70, unit_new="% of daily wage"
**Concrete example**:
  - record_id=433011, cao_number=433
  - Source quote: "As of 1 July 2020, the employee can take up to 5 weeks
    of additional paternity leave"
  - Before: leave_paid_paternity_value=5 weeks
  - After: leave_partially_paid_paternity_value=5 weeks,
    leave_partially_paid_paternity_pay_value=70 % of daily wage

### LEAVE_FM_04 — Holiday allowance bleeding into maternity

**Trigger**:
  - In source: "vakantiegeld", "holiday allowance", "8%" near holiday discussion
  - In CSV state: any leave_*_maternity_* field equals 8 with unit=%
**Wrong outcome**: field=leave_*_maternity_*, csv_value_new=8, unit_new=%
**Correct outcome**: verdict=clear (8% is holiday allowance, not maternity pay)
**Concrete example**:
  - Source quote: "Vakantietoeslag bedraagt 8% van het brutoloon"
  - Before: leave_paid_maternity_pay_value=8, unit=%
  - After: cleared

### LEAVE_FM_05 — Post-Aug-2022 parental: statutory restatement

Primary enforcement is `L0_STATUTORY_CLEAR` in Stage 2. This entry is defensive
for records that slip past L0 (e.g. when trigger phrases don't match exactly).

**Trigger**:
  - In source: "26 weken" + "ouderschapsverlof" + ("9 weken betaald" OR "UWV")
  - ingangsdatum: 2022-08-02 or later
  - CSV state: parental sub-fields populated but no CAO-specific deviation
**Wrong outcome**: parental sub-fields populated with statutory 26 = 9 + 17 numbers
**Correct outcome**:
  - verdict=clear on sub-fields
  - set leave_parental_statutory_ref=True; leave_parental_exceptions=False
**Concrete example**:
  - Source quote: "Vanaf 2 augustus 2022 heeft elke werknemer recht op 26 weken
    ouderschapsverlof, waarvan 9 weken betaald door UWV"
  - Before: leave_parental_partial_value=9, leave_parental_unpaid_value=17
  - After: both cleared, statutory_ref=True

### LEAVE_FM_06 — Template / placeholder rows leak through

**Trigger**:
  - In subagent output: record_id matches "rec_NNN_NNN"
  - OR topic_group="leave_type", field="field_name", value="example"
**Wrong outcome**: any row with template / placeholder values
**Correct outcome**: reject the row at aggregator-read time
**Concrete example**:
  - record_id="rec_000_000", field="field_name" → drop
