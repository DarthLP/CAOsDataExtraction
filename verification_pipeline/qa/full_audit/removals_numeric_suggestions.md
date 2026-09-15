# Suggestions for the 15 "CHECK AND GIVE SUGGESTION" patterns (~104 cells)

Companion to `removals_numeric_ruling_sheet.csv` (your file is untouched). Each suggestion is
grounded in the campaign explanations + source quotes in `removals_numeric_cells.csv`.
Reply per row (or "accept all") and I apply them mechanically. Nothing below is applied yet.

| # | field (n) | suggestion | why |
|---|---|---|---|
| 1 | `leave_long_term_care_value` (9) | **CASE: KEEP if a figure is quoted, BLANK if only "statutory" referenced** | 750-family quotes "max 30 days/year, 8 paid" (a real CAO figure → keep); others cite statutory long-term care leave with no number → blank per no-statutory-fill. |
| 2 | `overtime_min_rest_between_shifts_value` (9) | **CASE: KEEP 14/8 (stated post-shift rests), BLANK 36** | "14h rest after night shift ending after 02:00" is a genuine CAO rest rule; 36.0 is the statutory WEEKLY rest (ATW) mis-slotted into a between-shifts field. |
| 3 | `overtime_max_hours_per_day_value` (9) | **BLANK all** | Same source family as `overtime_max_hours_per_week_value`, which you ruled DELETE — the quotes are illustrative schedule examples ("Normal: 9h/shift… with overtime 11h"), not a CAO-set cap. Consistency with your weekly ruling. |
| 4 | `term_severance_extra_value` (7) | **BLANK all** | 2037/2273 are statutory transition-payment (Art 7:673 BW) amounts, not EXTRA severance; PAWW is employee-borne. No quantified extra exists. |
| 5 | `contract_ketenregeling_max_contracts_value` (6) | **CASE: KEEP the 2s, BLANK the 3s** | The quote states the temp-deviation "in 2 contracts" (explicit); the 3s restate the statutory default → blank per no-statutory. |
| 6 | `training_cost_reimbursement_value` (6) | **BLANK all + note** | The RAS scheme reimburses €140–1,400 *per course type* — a table, not a single value; storing one endpoint is arbitrary. Note the scheme in the record's note field. |
| 7 | `contract_minmax_hours_range_min` (6) | **CASE: normalize 683 pair to 75/125 "percent of contract hours"; KEEP 34 h/wk; BLANK 0.0** | 683 is a ±25% bandwidth contract — siblings encode it inconsistently (−25/25 vs 125); one canonical form. The 0.0 is an on-call (Art 7:628a) construct, not a bandwidth minimum. **This answers your "what about the min?"** — it's the mirror of the max you kept; keep both, normalized. |
| 8 | `overtime_allowance_range_min` (6) | **BLANK all + note** | The 8.5–45% are additive irregularity allowances (ORT) mis-slotted into the overtime-surcharge field; they belong conceptually to `unfavourable_hours_allowance`. Note in record. |
| 9 | `training_career_scan_freq_value` (6) | **BLANK all** | Sources describe the ontwikkelscan as one-time; no frequency stated. (The indices already dropped this field for unit-semantics chaos.) |
| 10 | `leave_short_term_care_value` (6) | **BLANK all** | The 10s restate the statutory short-term-care concept without a CAO figure; the 24s are the "monetary value of 24 hours" compensation construct, not a leave duration. |
| 11 | `leave_paid_paternity_value` (5) | **BLANK all + note (FM_03)** | The 100s are a PAY-% in a DURATION field (mis-slot); the 5s are WIEG supplementary birth leave, which per failure-mode LEAVE_FM_03 belongs in `partially_paid_paternity`. |
| 12 | `overtime_shift_allowance_range_max` (3) | **BLANK all** | All files have `shift_allowance_present=False`; per schema the range is omitted when the provision is absent. |
| 13 | `overtime_allowance_range_max` (2) | **CASE: KEEP 1.12 (unit "percent of monthly salary per hour"), BLANK 100** | This CAO expresses overtime as %-of-period-salary per hour ("max 1.21% of four-weekly salary") — 1.12 is a real value in that system; the 100 has no support. |
| 14 | `pension_employee_contrib_value` (2) | **BLANK both + note the ratio** | Source states "employee pays one-quarter of the 20.7% total" — 5.175 is computed, not stated (violates never-derive); 25 is a premium-share ratio, not a %-of-salary contribution. Put "1/4 of total premium" in the note. |
| 15 | `bonus_fixed_annual_lump_value` (19) | **BLANK all — and do NOT insert 8.33%** | **Answers your 8.3% question:** this field is the fixed *EUR lump sum* field; the 13th-month-as-8.33% belongs in the `bonus_thirteenth_month_*` fields, which exist separately. Writing 8.33% here would recreate exactly the %-in-EUR-field mis-slotting we're cleaning. The current values are misfiled fragments (2.38% year-end pct, 1.5% levensloop, one-off €425). |

**Also, your two KEEP-row questions:**
- `contract_part_time_range_max` = **1983.6** → it's the CAO's **full-time annual-hours norm** (38h × 52.2 weeks = 1,983.6; the CAO defines "part-time = annual norm below 1983.6"). As an upper bound on part-time hours it's semantically defensible — your KEEP stands; unit is "hours per year".
- `contract_minmax_hours_range_max` "what about the min?" → see row 7: the min is the bandwidth mirror (−25% / 34 h/wk / 0.0); suggestion keeps the real pair, normalized, and blanks the on-call 0.0.
