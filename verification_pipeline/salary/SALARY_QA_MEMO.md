# Salary CSV — Quality Assurance Memo (for Hanna)

> **⚠️ SUPERSEDED (2026-07-07).** This memo and `outputs/corrected_salary.csv` describe the
> **OLD second-LLM salary dataset** (`CAOsDataExtraction/outputs/excel/new_results/extracted_data_salary.csv`,
> a static Feb-2025 export). That pipeline has been **replaced** by the deterministic
> salary parser. The **canonical salary dataset is now**
> `CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv`
> (100% amount provenance; see `salary_parser/DELIVERY.md` + `salary_parser/PARSER_RULES.md`).
>
> Nothing is double-extracted: the old CSV was the *second* LLM's output; the parser reads
> the *first* LLM's wage grids directly. The old CSV and `corrected_salary.csv` are **not
> regenerated** and are kept only for historical comparison. When you open a "salary dataset",
> use the parser CSV (v2). The `0.0` placeholders and unit issues noted below are already
> handled in v2 (zero-drop, footnote-strip, unit-from-title fixes).

> **Layout note (2026-07-01):** this track moved from `qa/qa_salary/` to top-level `salary/`.
> All `qa/qa_salary/...` paths in this memo and in the historical run artifacts
> (`phase3*/`, `re_extraction/`, `wage_check_full/`) refer to the pre-move layout — map them to `salary/...`.

Generated 2026-05-29. Scope: the second extraction step.

```
PDF  ─[LLM extraction]→  llm_analysis/*.json  ─[Pydantic→CSV exporter]→  extracted_data_salary.csv
       (step 1)                                  (step 2 — what we verified)
```

We verified that **step 2** (the Pydantic/CSV exporter that turns the LLM's JSON
into the salary CSV) is faithful. We did NOT verify step 1 (the LLM's
extraction from the PDF). Concerns about JSON-side data quality are noted as
side observations in the appendix but are not in scope.

## TL;DR

**Step 2 is working correctly.** 9,097 suspicious cells were independently
adjudicated against the JSON source by subagents:

- **99.76 %** = the CSV value matches the JSON exactly (`CONFIRM_CSV`).
- **8 cells** in 2 files genuinely needed correction — all the same step-2 bug:
  the compact-schema unit code `'d'` from the JSON wasn't expanded to
  `'daily'` when the exporter wrote it to `salary_N_unit` columns. Fixed in
  `corrected_salary.csv`.
- **6 cells** (0.07 %) are `UNDECIDABLE` — the JSON itself contains `0.0`
  with an extractor-written note saying "inserted as placeholder". Step 2
  faithfully copied the placeholder; there is no different value to use.

## What was done

| Phase | What | Result |
|---|---|---|
| **1 — deterministic scan** | `qa/qa_salary/scripts/scan_anomalies.py` + `scan_cross_row.py` over the full CSV | 34,740 raw flags; 9,097 verifiable (after dropping informational `csv_only`/`json_only` and unverifiable `timeline_overflow`) |
| **2 — schema reconciliation** | normalize 3 JSON schemas (standard nested, compact parallel, compact nested-tl) so every CSV cell could be compared | dropped 71k phantom flags |
| **3 — subagent verification** | 308 chunks × ≤ 50 flags, Sonnet subagents, structured JSON verdict per cell | 9,097 verdicts (99.76 % CONFIRM_CSV) |
| **4 — apply corrections** | `apply_salary_corrections.py` writes a corrected copy of the salary CSV | 16 cells changed (`'d'` → `'daily'`) |

Source under `CAOsDataExtraction/` is **never modified**. All outputs live in
`qa/qa_salary/`.

## Phase 1 deterministic scanner — rules

Per-row (`scan_anomalies.py`):
| Rule | Final count | Notes |
|---|---|---|
| `amount_out_of_range` | 5,402 | Unit-aware bounds (`monthly < 800 / > 15,000` etc.) |
| `suspicious_placeholder` | 1,360 | 0/1/99/100/9999 etc. |
| `below_statutory_minimum` | 1,067 | Year-aware (€1,400-€2,070 floor at 60 %), adults only |
| `timeline_drop` | 129 | Drop > 30 % between consecutive blocks, same unit |
| `noncanonical_unit` | 10 | Unit string not in canonical set |
| `timeline_mismatch:unit` | 8 | CSV unit ≠ JSON unit |
| `ft_hours_out_of_range` | 66 | Outside 20-45 h/week |
| `increase_pct_extreme` | 16 | Outside [-5 %, 15 %] |
| `amount_without_unit` | 1 | Amount populated, unit blank |

Informational only (NOT sent to subagents — one side just doesn't carry the
field, not a CSV/JSON disagreement):
| Rule | Count |
|---|---|
| `timeline_csv_only:holiday_in_amount` | 24,159 |
| `meta_json_only:is_entry` | 1,357 |

Cross-row (`scan_cross_row.py`):
| Rule | Final count | Notes |
|---|---|---|
| `step_monotonicity_violation` | 424 | Within (jobgroup, worker, year/salary_N), amounts non-decreasing as step rises. Restricted to pure-numeric steps; 20 % tolerance |
| `ft_hours_inconsistent_in_file` | 741 | Row's ft_hours ≠ file's modal ft_hours (≥ 75 % majority, ≥ 20 rows) |

## Phase 3 verdicts (9,097 cells)

```
CONFIRM_CSV    9,075  (99.76 %)
CORRECT_TO        16  (0.18 %)
UNDECIDABLE        6  (0.07 %)
```

Confidence: 8,996 high · 101 medium

By rule:
```
amount_out_of_range            5,348  →  5,345 CONFIRM,  3 UNDEC
below_statutory_minimum        1,052  →  1,052 CONFIRM
suspicious_placeholder         1,360  →  1,357 CONFIRM,  3 UNDEC
ft_hours_inconsistent_in_file    695  →    695 CONFIRM
ft_hours_out_of_range             66  →     66 CONFIRM
increase_pct_extreme              16  →     16 CONFIRM
noncanonical_unit                  9  →      1 CONFIRM,  8 CORRECT_TO
step_monotonicity_violation      414  →    414 CONFIRM
timeline_drop                    129  →    129 CONFIRM
timeline_mismatch                  8  →      8 CORRECT_TO
```

### The corrections actually applied

All cells: CSV `salary_N_unit = 'd'` → JSON's actual `'daily'`. A stray
compact-schema short code that was never expanded during CSV export.

The 16 verdicts collapse to **8 unique cells** (each cell was flagged by both
`timeline_mismatch:unit` and `noncanonical_unit`; the second hit was correctly
skipped by the apply-safety gate once the first had already flipped the cell).

| CAO | file | entries touched | cells |
|---|---|---|---|
| 1494 | `Def CAO-PO 2023-2024 23012024` | 1 row (entry 201) | 2 (`salary_3_unit`, `salary_6_unit`) |
| 1577 | `Cao_theater_en_dans_DEF_4` | 2 rows (entries 207, 208) | 6 (`salary_1_unit`, `salary_2_unit`, `salary_3_unit` each) |

Details in `qa/qa_salary/outputs/apply_salary_changelog.csv` (8 rows actually
applied) and `apply_salary_skipped.csv` (8 duplicate-rule rows correctly
skipped by the gate).

### The 6 UNDECIDABLE (Hanna's call)

One entry in CAO 408 (Function Group XI Basic wage) where the LLM extractor's
own `note` says: *"No explicit amount provided in the table for Function Group
XI Basic wage for adults, assuming 0 as placeholder based on table structure."*

The CSV mirrors the JSON's `0.0` faithfully. Options:
- Leave as `0.0` (current). The placeholder is honest about being a placeholder.
- Blank the cell. Loses the structural-mention but removes the misleading number.
- Re-extract from the source PDF for that single entry.

Recommended: leave as-is; the extractor's note is the audit trail. Flag in
metadata as "self-disclaimed placeholder."

## Appendix: Side observations about step-1 (NOT verified, out of scope)

**These are speculation, not findings.** During Phase 3, subagents kept
noticing JSON values that *look* implausible (e.g., €100k labeled "monthly",
many `0.0` placeholders, single-digit "monthly" amounts). Step 2 faithfully
copied those values into the CSV — so the CSV is correct against the JSON,
and from this verification's perspective there is nothing to fix.

But the observations might be useful as a future signal **if** you want to
audit step-1 (the LLM extraction from the PDF) separately. **We did not open
the PDFs.** All of the below is pattern-matching from JSON context and
could be wrong. Ordered by number of suspicious cells the subagents called
out:

| # cells | CAO | Pattern | Files affected |
|---|---|---|---|
| 253 | **234** Jeugdzorg | `0.0` placeholders for empty cells in higher periodieks (steps 10-13) | 10 |
| 103 | **1165** Recreatie | Same: `0.0` for unpopulated periodieks in Schaal 1-3 | 3 |
| 73 | **632** NVB Banken | Amounts of €25k-€155k labeled `"monthly"` — almost certainly annual mislabel | 3 |
| 70 | **709** Vleeswaren | `0.0` for "0 Functiejaar" rows across multiple jobgroups | 10 |
| 55 | **1630** Cao Gemeenten | `0.0` for Kunstzinnige-vorming sub-scales | 2 |
| 51 | **750** Catering | `0.0` for Bedrijfscatering/Inflightcatering higher steps | 1 |
| 41 | **634** Bedrijfsverzorgingsdiensten | `0.0` for sub-tables on alternate hours-bases | 5 |
| 37 | **214** Cao OB | Single-digit "monthly" amounts (likely leading digits dropped) — Schaal 2 step 3 = 1.0 monthly | 2 |
| 35 | **4091** Cao SGO | `0.0` placeholders for Aanloopbedrag 2/3 and Uitloopbedrag 2/3 sub-tiers | 3 |
| 22 | **80** Waterbouw | Hourly rate tiers (100%/105%/.../18.5%) stored as a "timeline" — scanner reads 175%→18.5% as a 90 % drop | 4 |
| 14 | **1809** KNVB Referees | Small per-match fees (€100-€2,000) flagged as below statutory but are legit per-match payments | 2 |
| 12 | **227** Cao Weefselkweek | Same `0.0` placeholder pattern | 2 |
| 7 | **1496** Bakkersbedrijf | `0.0` for some age-cohort cells | 1 |
| 7 | **408** Schippersinternaten | LLM-flagged self-disclaimer placeholders (also in UNDECIDABLE above) | 1 |
| 6 | **2746** Cao Leisure | Same `0.0` placeholder pattern | 1 |

Also worth a re-read:
- **CAO 475 Sport** (Definitieve cao Sport 2022-2023): scale values like
  €99/month, €98/month — almost certainly a leading-digit drop (real values
  ≈ €1,980, €2,330, ...).
- **CAO 3335 PLb 2023-2024**: senior scales €150k-€300k labeled "monthly" —
  same annual-mislabel pattern as CAO 632.
- **CAO 3866 Non-EU Offshore Fishers**: monthly amounts €300-€900 (Dutch
  statutory minimum is €1,700+) — these are likely **USD-denominated** per
  ILO Joint Maritime Commission; not a CAO error, just needs a currency tag.
- **CAO 880 Stilte/Stewardry**: entry-level `salary_2_amount = 18247.6`
  monthly clearly should be `1824.77` (10× scale, sibling entries confirm) —
  but JSON also has the wrong value.

### If you want to audit step-1 later

These would be reasonable starting points (each needs PDF spot-checks to
confirm anything before acting):
1. The `0.0`-placeholder cluster (CAOs 234, 1165, 1630, 750, 4091, 1496,
   2746, 227) — open the PDFs for 2-3 rows each. If the PDF table cells are
   actually empty, the LLM is recording "empty" as `0.0` instead of `null`,
   and a re-extraction with stronger empty-cell handling could fix that.
2. CAOs 632 (Banks), 3335 (PLb) — verify the "monthly" unit label in the PDF.
   If they're actually annual, that's an LLM unit-labeling error.
3. CAO 214, 475 — verify whether the PDF rows really show €1 or €98 per
   month, or whether the LLM truncated leading digits.
4. CAO 3866 — verify currency in PDF (likely USD per ILO context).
5. CAO 80 (Waterbouw) — verify whether the salary_1..salary_8 columns are
   chronological or rate-tier multiples; if the latter, the schema may want
   an explicit `is_rate_table` flag.

**None of this is something I verified.** Don't act on it without a PDF
spot-check first.

## Files produced

```
qa/qa_salary/
├── scripts/
│   ├── scan_anomalies.py              — per-row scan
│   ├── scan_cross_row.py              — step-monotonicity + ft_hours consistency
│   ├── build_phase3_chunks.py         — chunker for subagent review
│   └── apply_salary_corrections.py    — apply 16 unit corrections
├── outputs/
│   ├── salary_anomalies.csv           — full deterministic flag list
│   ├── salary_anomalies_cross_row.csv — cross-row flags only
│   ├── salary_anomalies_summary.txt   — counts by rule
│   ├── corrected_salary.csv           — full CSV with 16 cells flipped
│   ├── apply_salary_changelog.csv     — the 16 changes applied
│   └── apply_salary_skipped.csv       — should be empty
├── phase3/
│   ├── prompts/system_prompt.md       — subagent rubric (no thresholds, no expected values)
│   ├── chunks/c????.json              — 308 worksheet JSONs
│   ├── chunks/manifest.csv            — chunk index
│   └── results/c????.json             — 308 verdict JSONs
└── SALARY_QA_MEMO.md                  — this document
```

## Conventions verified

- The salary CSV's `id` column is the **file id**, not a row id (the same id
  repeats up to 66 times per file). The unique key per row is `(cao_number,
  file_name, entry_idx)` where `entry_idx` is the 0-based position within
  the CSV's contiguous (cao, file) group. Confirmed across all 244,327 rows:
  100 % contiguous, 0 interleavings, CSV row count == JSON
  `salary_information` length in 10/10 sampled files.
- Three JSON schema variants exist in the corpus:
  - `standard_nested` (2,248 files) — full English keys + nested `timeline[]`
  - `compact_nested_tl` (131 files) — short meta keys (`jg`/`st`/`wr`/...) +
    nested `tl[]` of short-key objects (`sd`/`am`/`un`/`ip`/`nt`)
  - `compact_parallel` (91 files) — short meta keys + parallel arrays
    (`sd[]`, `am[]`, `un[]`, ...)
  All three are normalized to standard form by `normalize_entry()` before
  comparison.
- 217 files have empty `salary_information`; 100 JSON files are missing or
  unloadable. Together these account for ~12 % of the CSV's source-file
  population; flags from these files are silently skipped.

## Open items needing Hanna

1. **Accept the 8 unit-normalization corrections** in `corrected_salary.csv`
   (deterministic, JSON-grounded — this is the actual deliverable).
2. **Decide on the 6 self-disclaimed placeholders** in CAO 408 (recommended:
   leave as `0.0` since the extractor's note is the audit trail).
3. **(Optional)** If you want to audit step-1 (PDF→JSON), the Appendix has a
   list of CAOs to spot-check first. Not in scope for this verification, not
   confirmed against PDFs.
