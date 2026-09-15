# Per-Topic Failure Modes — Contract

Added 2026-05-19 from contract run (95 scoped records; Stage 3 in progress).

## CONTRACT_FM_01 — Full-time hours stated annually (BJA)

**When it applies**: source defines full-time on a calendar-year basis (Basic Annual Working Hours / BJA, common in Metalektro and some industrial CAOs) rather than per week.

**Subagent action**: keep `full_time_hours.unit='hours per year'` and record the annual value. Do NOT convert to a weekly figure. The schema accepts either unit.

**CSV impact**: `contract_full_time_hours_value` + `contract_full_time_hours_unit`.

**Example**: CAO 487 (Metalektro): "'Full-time' (voltijd) refers to the number of hours to be worked (on a calendar year basis) equal to the Basic Annual Working Hours (BJA)".

## CONTRACT_FM_02 — Snake_case unit normalization

**When it applies**: extractor emitted a unit with underscores (`hours_per_week`, `hours_per_month`) instead of the canonical space-separated form.

**Subagent action**: normalize to `hours per week` (etc.). Deterministic rule R2 proposes this; subagent confirms. Pure cleanup, no semantic change.

**CSV impact**: any `contract_*_unit` field.

## CONTRACT_FM_03 — Singular→plural unit normalization

**When it applies**: unit recorded in singular (`year`, `contract`, `employee`) where the schema examples use plural.

**Subagent action**: normalize `year`→`years`, `contract`→`contracts`, `employee`→`employees`. Deterministic rule R3 proposes this.

**CSV impact**: `contract_ketenregeling_max_duration_unit`, `contract_ketenregeling_max_contracts_unit`, `contract_workhours_adjustment_*_unit`.

## CONTRACT_FM_04 — Ketenregeling deviation only when CAO genuinely diverges

**When it applies**: source restates the statutory chain rule (post-2020 WAB: 3 contracts / 3 years / >6-month break; pre-2020 WWZ: 3/3/6-month break; pre-2015: 3/3 with 3-month break) WITHOUT changing it. Restating the law is NOT a deviation.

**Subagent action**: set `ketenregeling_deviation_present=False` when the CAO merely cites/restates Article 7:668a BW or the statutory limits. Set `True` only when the CAO explicitly extends/reduces the number of contracts, the duration, or the break period (often via Wfa sector exemption). When False, leave `max_contracts` / `max_duration` empty per convention (they describe the *deviation*, not the statutory baseline).

**CSV impact**: `contract_ketenregeling_deviation_present`, `contract_ketenregeling_max_contracts_*`, `contract_ketenregeling_max_duration_*`.

**Example**: CAO 730 (record 730012): "the CAO explicitly follows Article 7:668a BW" → deviation_present=False. CAO 234 (Youth Care): BBL exemption + AOW-specific 6-contracts/4-years rule → genuine deviation for those subgroups, document in note.

## CONTRACT_FM_05 — Multi-tier full-time hours within one CAO

**When it applies**: a single CAO defines different full-time weeks for different branches/job groups (e.g. Home Furnishings 37h vs other branches 38h).

**Subagent action**: use the majority-headcount group's value per the schema's `selection_rule` convention; note the variation in `notes`. Do not invent a range.

**CSV impact**: `contract_full_time_hours_value` + `notes`.

**Example**: CAO 727 (Retail Non-Food): "A full-time employee is an employee whose agreed working hours are 38 hours per week, and in Home Furnishings, 37 hours per week" → value=38 (majority), note the 37h Home Furnishings exception.
