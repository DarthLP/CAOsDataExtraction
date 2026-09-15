# Phase verdict summary — phase_a_sanity
**Total verdicts:** 50

## Distribution

| Assessment | Count | % |
|---|---|---|
| faithful | 25 | 50.0% |
| minor_issues | 15 | 30.0% |
| major_issues | 10 | 20.0% |
| not_verifiable | 0 | 0.0% |

## Top discrepancy types

| Type | Count |
|---|---|
| wrong_amount | 43 |
| missing_scale | 16 |
| other | 9 |
| unsupported_value | 8 |
| missing_increase | 6 |
| extra_scale | 6 |
| missing_date | 1 |
| wrong_unit | 1 |

## Severity

| Severity | Count |
|---|---|
| high | 45 |
| medium | 25 |
| low | 20 |

## Flagged files (major + minor)

- **c0024** (10, major_issues, 3 disc / 2 high) — Adult construction site (A-E) and UTA function-level (1-6) core scales at 21+ are present with correct amounts, units, dates, and inc_pct. However, (1) the adul
- **c0266** (1287, major_issues, 7 disc / 5 high) — Scale coverage (A-K) and effective dates (5 versions) match the text exactly. However, for jobgroups G-K — where the source row has three numeric cells (Entry f
- **c0455** (1536, major_issues, 6 disc / 4 high) — The structured extraction has a systematic axis-swap defect in Table 4.1: structured jobgroup values (0..18) hold what the text labels as Trede (row axis 0..15)
- **c0600** (163, major_issues, 7 disc / 2 high) — Scale and date coverage are right (scales 1-18 + chauffeursloonschaal + chauffeur personenauto, three effective dates 2014-01-01 / 2014-07-01 / 2015-01-01, chau
- **c0865** (2165, major_issues, 2 disc / 1 high) — Axes are systematically swapped: the structured rows put the ROW-axis label (trede/step) into `jobgroup` and the COLUMN-axis label (function group FG 1-9) into 
- **c0909** (227, major_issues, 11 disc / 10 high) — Structured data captures the seven adult function groups (1-7) across both tables (per 1 dec 2018, per 1 jan 2020) and the eight function-year steps (21 jaar/0-
- **c0927** (2297, major_issues, 6 disc / 5 high) — Structured covers all 5 effective dates and jobgroups A-J across all 5 tables, but contains systematic column-misalignment errors: (1) every J-column step 0-4 i
- **c1339** (3335, major_issues, 2 disc / 1 high) — The 13-jobgroup salary table is duplicated rather than encoded. Each Functieschaal has its Minimum and Maximum amounts split into TWO separate SalaryRow objects
- **c2281** (721, major_issues, 8 disc / 8 high) — The 2011 and 2012 monthly salary tables are correctly identified, with the right effective dates (2011-04-01, 2012-04-01), unit (monthly), inc_pct (1.25%), and 
- **c2425** (759, major_issues, 6 disc / 6 high) — Adult scale structure (LG 1-LG 9), four effective dates, and inc_pct values are encoded correctly. However, the extractor exhibits a systematic off-by-one colum
- **c0033** (1022, minor_issues, 2 disc / 1 high) — The structured_salary faithfully encodes the three main 2011-01-01 wage tables: Carrièrepatronen OBP (scales 1-18), Carrièrepatronen OP (LB-LE), and the Entry a
- **c0330** (1424, minor_issues, 2 disc / 0 high) — The structured data faithfully encodes the three tabulated Hay-based salary scales (1 April 2012, 1 October 2012, 1 April 2013) for all 12 salary groups x 3 ref
- **c0457** (1536, minor_issues, 7 disc / 0 high) — Structured_salary correctly captures the main Salaristabel per 01-02-2019 with all 18 adult jobgroups plus the H2/H1/P/SA/TOIO columns, and the WML-tied disabil
- **c0477** (1547, minor_issues, 2 disc / 0 high) — The structured_salary faithfully encodes the six adult salary groups (Groep 1-6) with Starting/End salary across all three effective dates (2019-01-01, 2020-01-
- **c0616** (1639, minor_issues, 2 disc / 0 high) — Structured data faithfully captures all six function groups across the three tabulated effective dates (2017-02-01, 2018-11-01, 2019-07-01), with correct amount
- **c0753** (1944, minor_issues, 1 disc / 0 high) — Structured_salary faithfully encodes scales C-H across all four effective dates (2025-07-01, 2026-01-01, 2026-07-01, 2027-01-01) with correct amounts, units (mo
- **c1139** (26, minor_issues, 3 disc / 0 high) — Structured data faithfully encodes the 5 function levels (B-F), 3 effective dates (2019-11-01, 2021-03-01, 2022-01-01), both 36h and 38h work-week variants, and
- **c1160** (2746, minor_issues, 1 disc / 0 high) — The structured_salary faithfully encodes the three wage tables (1 Jan 2013, 1 Apr 2013, 1 Jan 2014) for Functiegroepen I-XI with both Basisloon and Eindloon ste
- **c1404** (35, minor_issues, 1 disc / 0 high) — Structured output faithfully encodes the four Loongroepen (II, III, IV, V) x twelve Periodieken (0.5-6.0) x four effective dates (2021-10-01, 2022-03-01, 2023-0
- **c1435** (359, minor_issues, 1 disc / 0 high) — The main Salary Scale (Groups 1-10 with periodieken A,0-13) is faithfully encoded: all amounts, column-to-jobgroup mapping, ragged-row handling, the 2017-01-01 
- **c1926** (51, minor_issues, 2 disc / 0 high) — Adult scale extraction (jobgroups A-I, Exp. Year 0-12) is faithful: amounts, jobgroups, steps, two effective dates (2016-07-01 and 2017-01-01), inc_pct values (
- **c2078** (625, minor_issues, 1 disc / 0 high) — The structured_salary faithfully encodes the 18 main wage scales (Min/Max plus per-scale Aanloopsalaris bands matching the article H-3 cap rule) and the student
- **c2239** (679, minor_issues, 3 disc / 0 high) — Two wage tables (rijdend per 1 juli 2016 / 1 januari 2017 and niet rijdend per 1 juli 2016 / 1 januari 2017) are extracted with the right scale axes, dates, and
- **c2367** (730, minor_issues, 2 disc / 0 high) — Structured data faithfully encodes the adult (v.a. 23 years) experience-band scales for both the 2011-04-01 and 2012-07-01 versions. Two minor issues: (1) inc_p
- **c2418** (759, minor_issues, 1 disc / 0 high) — The structured_salary faithfully encodes the LG 1-9 hourly wage scales across the three effective dates (2021-06-20, 2022-01-03, 2023-01-02), correctly omits Ma
