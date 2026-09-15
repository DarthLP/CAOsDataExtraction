# Topic-specific failure modes — homeoffice

Cap: 10-12 entries. Loaded only when the subagent's topic is `homeoffice`.

### HOMEOFFICE_FM_01 — Stipend unit variation (€/day vs €/month)

**Trigger**:
  - In source: "€X per [full] home working day", "EUR per day", "EUR per maand", "EUR per month"
  - In CSV state: `homeoffice_stipend_value` is empty OR populated with mismatched unit

**Wrong outcome**:
  - field=homeoffice_stipend_value, value=numeric, unit="EUR per month" when source says per day
  - OR clearing the value because unit doesn't match the schema example

**Correct outcome**:
  - verdict=correct_in_place
  - csv_value_new=<numeric from source>, csv_unit_new=<exact unit from source>
  - notes="unit verified per source: <verbatim quote with unit>"

**Concrete example**:
  - record_id=…, CAO 301 (Sociaal Werk)
  - Source quote: "A reimbursement of at least €2.15 per full home working day is provided"
  - Before: homeoffice_stipend_value="", unit=""
  - After: homeoffice_stipend_value=2.15, unit="EUR per home working day"


### HOMEOFFICE_FM_02 — Agency / staffing CAO defers to client (mirrors OVERTIME_FM_05)

**Trigger**:
  - In source: "part of the client's remuneration", "inlenersbeloning", "subject to the hiring employer's arrangement", agency/staffing CAOs (ABU, NBBU, common ICK contexts)
  - CSV state: many homeoffice numeric fields empty AND source defers to client

**Wrong outcome**:
  - Inventing a stipend value from a generic example mentioned in source
  - Setting `has_homeoffice_rights=False` (the agency CAO does mention home office; it just defers numeric values)

**Correct outcome**:
  - verdict=unable_to_verify for stipend / entitlement / costs_reimbursed numeric fields
  - confidence=low
  - evidence_quote captures the defer-to-client language verbatim
  - notes="agency CAO; values deferred to hirer per remuneration scheme"
  - `has_homeoffice_rights` can be True if the CAO regulates the topic conceptually

**Concrete example**:
  - record_id=…, CAO 1296 (ICK 2017)
  - Source quote: "Home office allowances are part of the client's remuneration (Article 16 lid 1 sub i)"
  - Before: homeoffice_stipend_value="some-extracted-value"
  - After: homeoffice_stipend_value="", verdict=unable_to_verify, notes="agency CAO defers"


### HOMEOFFICE_FM_03 — "Necessary facilities" without amount → costs_reimbursed=True

**Trigger**:
  - In source: "provides necessary facilities", "necessary equipment is provided", "employer provides equipment", "no specific amounts mentioned"
  - CSV state: `homeoffice_costs_reimbursed` is False/empty despite source confirming employer provides

**Wrong outcome**:
  - costs_reimbursed=False because no numeric stipend is named

**Correct outcome**:
  - verdict=set_boolean
  - target_field=homeoffice_costs_reimbursed
  - new_value=True
  - confidence=high if quote is explicit
  - notes="employer provides facilities; no specific amount named in source"

**Concrete example**:
  - record_id=…, CAO 43
  - Source quote: "The employer offers the necessary facilities for working from home (no specific amounts mentioned)."
  - Before: homeoffice_costs_reimbursed=False
  - After: homeoffice_costs_reimbursed=True


### HOMEOFFICE_FM_04 — OR/PVT consultation → discretion='joint_with_OR'

**Trigger**:
  - In source: "in consultation with the Works Council (OR)", "with the consent of the OR", "in agreement with the OR or PVT", "the employer and OR establish", "company regulation with OR consent"
  - CSV state: `homeoffice_discretion` empty OR populated with non-canonical value

**Wrong outcome**:
  - discretion="" (missed extraction)
  - discretion="other" or non-canonical free-text

**Correct outcome**:
  - verdict=correct_in_place
  - csv_value_new=`joint_with_OR` (literal canonical enum value)
  - confidence=high
  - evidence_quote captures the OR/PVT clause verbatim

**Concrete example**:
  - record_id=…, CAO 1612 (Kinderopvang)
  - Source quote: "The employer establishes a company regulation for home work/telework with the consent of the Works Council (OR) or Personnel Representation (PVT)"
  - Before: homeoffice_discretion=""
  - After: homeoffice_discretion="joint_with_OR"
