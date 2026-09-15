# Per-Topic Failure Modes — Training

Added 2026-05-19 from training run (95 scoped records, 104 real corrections, 45 clean wins, 59 NHR).

## TRAINING_FM_01 — Time allowance unit polymorphism

**When it applies**: source states yearly training time in days (e.g. "three days of paid training leave per year") OR hours (e.g. "40 hours of training per year"). Sometimes uses period-then-numeric phrasing ("annually receive three days").

**Subagent action**: record `time_yearly.value` and `time_yearly.unit` exactly as stated. Both `days per year` and `hours per year` are valid units. Do NOT convert days↔hours.

**CSV impact**: `training_time_yearly_value` + `training_time_yearly_unit`.

**Example**: CAO 1536 (NU 2024): "Employees receive at least three development days annually for development and training" → value=3, unit="days per year".

## TRAINING_FM_02 — Career-scan frequency unit inversion

**When it applies**: source states career advice/career scan period using "every X years" or "once every X years" phrasing. Schema-canonical unit is `times per year`, so a "every 5 years" provision = `value=0.2`, `unit="times per year"`. The deterministic rule (R5) proposes this inversion; subagent should verify the source genuinely says "every X years" and accept the inversion.

**Subagent action**: if source uses period phrasing ("every X years"), accept the proposed `value=1/X, unit='times per year'`. Cite the period phrasing in evidence even though the canonical unit string doesn't appear verbatim — note in `notes` that this is a frequency inversion of the source period.

**CSV impact**: `training_career_scan_freq_value` + `training_career_scan_freq_unit`.

**Example**: CAO 26 (Vleessector): "An employee can request career advice once every five years" → value=0.2, unit="times per year".

## TRAINING_FM_03 — Sector training fund triggers fund_present=True

**When it applies**: source mentions a sectoral fund that finances training — common names include O&O-fonds, scholingsfonds, opleidingsfonds, OSV-fonds, RAS, FKB, DOORZAAM, etc. Even when the employer doesn't directly reimburse, the fund's existence sets `fund_present=True`.

**Subagent action**: set `fund_present=True` whenever the source names any sectoral/CAO training fund. Note the fund name in `notes`.

**CSV impact**: `training_fund_present`.

**Example**: CAO 163 (OV/Transport): "the OSV-fonds and a new vitality and development fund (to be established by Jan 1, 2015) can finance training and development" → fund_present=True.

## TRAINING_FM_04 — WAB 2020 mandatory-training reclaim carve-out

**When it applies**: source explicitly states that *mandatory* training costs cannot be reclaimed from the employee (a WAB 2020 statutory rule), but other (voluntary/employee-initiated) study costs CAN be reclaimed under a `studiekostenbeding` / `terugbetalingsbeding`. The carve-out does NOT mean `reclaim_clause_present=False`.

**Subagent action**: `reclaim_clause_present=True` if a clause for voluntary training reclaim exists, even if mandatory training is carved out. Record the carve-out scope in `notes`.

**CSV impact**: `training_reclaim_clause_present`.

**Example**: CAO 433 (2024): "Training costs for RAS-subsidized training cannot be reclaimed from the employee" + reclaim clause for non-RAS studies → reclaim_clause_present=True with carve-out noted.

## TRAINING_FM_05 — Agency / staffing CAO defers to hirer

**When it applies**: agency / uitzend / detachering CAOs (e.g. NBBU, ABU, DOORZAAM) refer training entitlements to the hirer's CAO or to a sector fund (DOORZAAM, STAF) rather than directly granting them.

**Subagent action**: set `has_training_rights=True` (the CAO grants rights via the fund) and `fund_present=True`. Note the deferral pattern in `notes`. Do NOT set per-employer training budget unless explicitly stated.

**CSV impact**: `training_has_training_rights`, `training_fund_present`, `training_note`.

**Example**: CAO Phase 1-2 agency: "The agency is obliged to spend at least 1.02% of the actual wage of agency workers in phase 1-2 on promoting sustainable employability, with unspent portions transferred to the DOORZAAM foundation." → fund_present=True; budget=1.02 unit="percent of actual wage".
