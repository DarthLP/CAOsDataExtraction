# Salary extraction — HARD RULES (read before touching the parser or briefing any agent)

These are the invariants the deterministic salary parser enforces and that every
verification/relabel/extraction agent MUST be told. They exist because each one was a
real bug found against source. Violating them silently corrupts the dataset.

## Provenance (never invent)

1. **Every amount must appear verbatim in the source `wage_information` text.** The only
   authorized exceptions are (a) the mangled-decimal rescue (`2.18454`→`2184.54`, a
   digit-preserving decimal shift, tagged `value_source=rescaled`) and (b) documented
   mechanical date derivations (`end_date`, season `2016/17`→`2016-07-01`). Nothing else.
2. **Source silent → leave empty.** "statutory"/"WML"/"conform CAO" with no figure → empty.
   Do NOT annualize, convert, or infer a number (`1 day/week` → annual days is inventing).
3. **Unit comes from the source, not from magnitude.** If the title/header states no pay
   period, `unit` stays empty even when the amount is "obviously" monthly. Assigning a unit
   from magnitude alone is inventing. (CAO 3335 PLb: monthly-sized min/max scales, title
   silent → `unit=None` on purpose; the amount is still 100% provenanced.)

## Numbers that look like something else

4. **A bare integer in 1900–2035 is usually a WAGE, not a year.** In salary matrices
   (ZKN/metal Trede grids, WML columns) amounts like `1924 | 1935 | 1980 | 2026` fall in
   the calendar-year band. The parser only treats a bare year-band integer as a *year* when
   the table has NO money continuum straddling the band (see `table_yearband_is_salary`);
   a birth-year cohort column (`1957 | 1958 | …`) is the real year case. Agents judging a
   cell: if the surrounding column/row is euro amounts, `1935` is a salary.
5. **`0` / `0.00` cells are placeholders, not wages.** Sources print 0 where a scale has no
   such step. The parser drops zero amounts; agents must never "confirm" a 0 as a wage.
6. **Footnote markers stay attached to numbers.** `2185.13*`, `1840,11†` — the `*`/`†` is a
   footnote (often "= statutory minimum"), the number is real. Strip the marker, keep the
   amount.
7. **Small whole numbers (< 13) in a non-hourly table are ordinals, not wages.** ORBA
   points, function-group indices, `leerjaar` counters. Flagged `nonwage_smallint`→D.
8. **A repeated header row inside the data is not data.** `Group | 1 | 2 | … | 11`
   reappearing mid-table is column indices, not a wage row — skip it.

## Table structure

9. **Mixed age/step key column.** In metal/technical "Age/Function Years" grids the first
   column holds AGES for the youth rows (`16 years`…`21 years`) then STEPS for the mature
   rows (`0`…`10`). Age rows → `age_group`; step rows → `step`. The education/scale columns
   (`WML | VBO/MAVO | A/2 | B/3 …`) are the jobgroups.
10. **Ragged merged tables:** youth rows are narrow, function-year rows are wide, under one
    superset header. Classify columns from the WHOLE column (index-aligned), not only the
    full-width rows — otherwise the scale columns look empty and get misread as keys.
11. **Two key columns** (e.g. `leeftijd | anciënniteit | …`, or `0/3 mnd | step | …`): both
    belong in the label so two source rows don't collapse to one identity. (163 OV, 679.)
12. **Unit keywords, earliest-mention wins,** after stripping FTE clauses. `basis van 12
    maanden` = annual (a 12-month sum), not monthly — it must outrank the bare `maand`
    inside `maanden`. (CAO 632 Banken.)

## Labels

13. **Money-sized tokens (≥ 800) or clock artifacts (`3:00 AM`) inside jobgroup/step/worker
    mean the row is mis-parsed** (a fused amount leaked into a label). Flagged `junk_label`.
    Do not ship such a label as truth.
14. **`Functieaanvangsalaris` / `aanvang…` / `aanloop` / `instap` rows are entry rows** →
    `is_entry=True`, still included.
15. **Youth / apprentice / BBL tables are EXCLUDED by design** (teen filter). Their absence
    is intended, NOT a coverage bug. `coverage_missing_tables` does not count them.

## Guard adjudication (relabel pipeline)

16. When a guard rejects an agent's relabel, **adjudicate against SOURCE FACTS in code, not
    by majority vote.** Every reject class audited 2026-07 was the agent contradicting the
    row's own cell anchors (G4), moving/adding amounts (G1), or inventing tokens (G2) —
    zero guard-wrong rejects. But always re-verify against source before trusting a guard.

See `salary_parser/salary_parser.py` for the implementations and the CAO IDs each rule was
derived from. See `DELIVERY.md` for the pipeline and metrics.
