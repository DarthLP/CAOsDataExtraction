# Overtime — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-18)

### Hazard list (defined before sampling)

The overtime schema (`NON_SALARY_PROMPTS_AND_SCHEMA.md` lines 572-637, 17 fields) was reviewed against expected real-world patterns. Hazards to validate via sampling:

| H# | Hazard | Schema field(s) | Status |
|---|---|---|---|
| H1 | Multi-tier overtime rates (different % per overtime tier or day-of-week) | `allowance` (single value) + `allowance_range` (min/max) — RANGE only, not schedule | **REAL gap** |
| H2 | Daily AND weekly AND multi-week triggers (e.g. 12-week or 3-month aggregates) | `trigger_daily`, `trigger_weekly` only | **REAL gap** |
| H3 | Compensation mode = choice (employee picks pay vs TOIL) | `compensation_mode` enum has `'both'` value | OK |
| H4 | Stacking rules (do night + weekend surcharges add or take highest?) | `stacking_rule` (str) | OK |
| H5 | Age-conditional compulsory overtime (e.g. "50+ not obliged") | None — must go to `guaranteed_weekends_off_rule_text` or notes | Schema gap |
| H6 | Per-group differences (driver vs office; full-time vs part-time) | `hetero_present` + `allowance_range` | Partial (no group-name capture) |
| H7 | "Plus hours" vs "overtime" distinction (some CAOs treat hours-over-roster differently) | None — collapses to single `compensation_mode` | Schema gap |
| H8 | Min rest between shifts; max hours per day/week | Direct fields | OK |
| H9 | Compulsory annual overtime cap | `compulsory_annual` | OK |
| H10 | Shift allowance distinct from overtime | `shift_allowance_*` fields | OK |
| H11 | Unfavourable-hours allowance (night, weekend) | `unfavourable_hours_allowance` | OK |

### Sampling — 30 random CAOs

Sampled with `random.seed(42)` from the 1,505 records in `inputs/by_topic/overtime_information.md`. Excerpts saved at `/tmp/overtime_sample.txt` (647 lines). Reviewed manually.

**Findings against hazards:**

**H1 (multi-tier rates) — CONFIRMED hazard, multiple instances:**

- CAO 26 (Vleessector, ingangsdatum 2014-04-01): *"25% on basic hourly wage for overtime Mon-Sat. 35% on day off (first 2 hours). 50% Sat/Sun. 100% public holidays."* — 4 distinct rates that collapse to `allowance=25` + `allowance_range=25..100`.
- CAO 83 (Betonproducten, 2021-10-01): *"200% for Saturday 12:00-24:00 and Sunday/holiday; 150% for all other overtime."* — 2 tiers; `compensation_mode=monetary_pay` captures mode but not the schedule.
- CAO 148 (Graan, 2017-04-01): "various surcharges depending on day". Multiple rates.
- CAO 163 (OV/Transport, 2014-01-01): Different rates for driving vs technical personnel; uses "meeruren" vs "overuren" terminology.
- CAO 306 (GIL?, 2014-04-01): 30% surcharge + 100% holiday allowance — TIER pattern.

**H2 (multi-period triggers) — CONFIRMED hazard:**

- CAO 26 has triggers at three levels: daily (>9.5h or >9h depending on schedule), weekly (>42h), and 12-week-aggregate (>432h). Schema captures daily + weekly only.
- CAO 35 (textile): triggers vary by shop size (parttime drivers in 5-or-fewer-person units use 36h/week; 10-person textile units use 40h/week).

**H5 (age-conditional compulsory overtime) — CONFIRMED, common pattern:**

- CAO 26: "Employees aged 55 or older are not obliged to work overtime."
- CAO 233 (Hoveniers, 2015): "Aged 50+ cannot be compelled to work overtime. Under-18s may not work overtime at all."
- CAO 306: "Mandatory if necessary due to company operations. This obligation does not apply to employees older than 50."
- CAO 157 (Zuivel, 2012): "55 and older not obligated."
- Highly recurrent across samples. Goes into the verbatim `guaranteed_weekends_off_rule_text` field by convention. The schema doesn't have a structured field; flagging.

**H6 (per-group differences) — CONFIRMED:**

- CAO 163 splits driving personnel vs technical personnel with different overtime definitions.
- CAO 35 splits by company size (textile units of 5 vs 10 persons).
- Schema's `hetero_present=True` flag is set in these cases but the group identifiers and per-group values are not captured.

**H7 (plus-hours vs overtime) — CONFIRMED, less common:**

- CAO 163 explicitly distinguishes "meeruren" (hours beyond roster due to extension) from "overuren" (overtime per ATW thresholds).
- CAO 306: "balance of plus and minus hours may never exceed ±12; excess paid out as overtime" — this is a flex-balance system, not classical overtime.

**Hazards NOT triggered by the sample:**

- H3 (compensation choice): `'both'` value handles the cases observed.
- H4 (stacking): when present, the verbatim `stacking_rule` field captures it.
- H8/H9/H10/H11: standard fields work; no widespread misfits observed.

### Recommendation: PROCEED with caveats documented

None of the confirmed hazards are blockers for running the QA on overtime — they are **schema-design limitations** rather than **wrong-data hazards**. The pipeline can produce useful corrections within the current schema; multi-tier schedules and age-conditional rules will be flagged for human review with the relevant text captured in `notes` or in `guaranteed_weekends_off_rule_text`.

Specific defensive moves baked into the Stage 2 rules and Stage 3 prompts:

1. **Treat `allowance_range` as informative, not definitive.** When `hetero_present=True` and the source clearly shows multiple distinct rates, the subagent should set `allowance` to the FIRST tier with `notes="tier_schedule=25/35/50/100 Mon-Sat/day-off/Sat-Sun/holiday"` — mirrors the `LEAVE_FM_02` pattern for tiered sick pay.
2. **Multi-period triggers**: if source mentions a weekly aggregate (e.g. >432h/12 weeks), populate `trigger_weekly` with the simple weekly value (>42h) and note the longer-period rule in `notes`. The longer-period trigger usually mirrors the weekly limit anyway.
3. **Age-conditional compulsory overtime**: capture verbatim into `guaranteed_weekends_off_rule_text` (free-text). No structured field is needed.
4. **Per-group differences (hetero_present)**: when groups have distinct values, populate the "majority headcount" group as the typical group (matching the schema's `selection_rule`), set `hetero_present=True`, fill `allowance_range` with min/max across groups.

**HARD STOP CHECK: no, do not stop.** The hazards are manageable; the schema accommodates the common patterns; recommendations above describe how subagents should handle the edge cases.

### Stage 0 conclusion

Proceeding to Stage 1. Three new failure-mode entries to seed in `qa/conventions/failure_modes/per_topic/overtime.md` for the Stage 3 prompts:

- **OVERTIME_FM_01** — Multi-tier rates collapse: set `allowance` to first tier, full schedule in `notes`. Mirrors LEAVE_FM_02.
- **OVERTIME_FM_02** — Age-conditional compulsory overtime: capture in `guaranteed_weekends_off_rule_text`.
- **OVERTIME_FM_03** — Per-group hetero: set `hetero_present=True` + `allowance_range`; note group identifiers in `notes`.

(These get written into `qa/conventions/failure_modes/per_topic/overtime.md` as part of Stage 2 setup below.)

---

## Stage 1 — Scope filter

Full CSV (2,739 records) → **95 scoped records** (1 per CAO, latest version with non-empty overtime source). Saved to `qa/qa_overtime/inputs/scoped_records.csv`.

## Stage 2 — Deterministic layer

10 L1 rules in [qa_overtime_rules.py](scripts/qa_overtime_rules.py) + L2 presence scan over the 95 scoped records:

| Source | Count | Worksheet mode |
|---|---|---|
| L2 (presence: field empty + topic in source) | 927 | extract |
| R4 (daily trigger outside [6, 14] hours) | 14 | blind |
| R5 (weekly trigger outside [30, 60]) | 5 | blind |
| R6 (allowance_range filled but hetero_present not True) | 3 | informed |
| R9 (min_rest unit wrong) | 2 | informed |
| R8 (max_weekly < trigger_weekly) | 1 | blind |

**952 worksheet items** for Stage 3. R4 cluster is a real extraction pattern: extractor pulled "1 hour past normal" as the daily trigger threshold (should be ~8h, not 0.5h).

## Stage 3 — Subagent review

48 chunks (~20 items each) processed across 4 batches of 12 parallel subagents. Sonnet model. All 48 returned cleanly.

### Verdict distribution

| Verdict | Count | % |
|---|---|---|
| unable_to_verify | 624 | 65.5% |
| correct_in_place | 220 | 23.1% |
| confirm | 47 | 4.9% |
| clear | 44 | 4.6% |
| set_boolean | 16 | 1.7% |
| move | 1 | 0.1% |

### Failure-mode usage

| FM | Hits | Pattern |
|---|---|---|
| OVERTIME_FM_01 | 124 | Multi-tier rate collapse |
| OVERTIME_FM_02 | 36 | Age-conditional compulsory overtime |
| GENERAL_FM_05 | 7 | Field/unit mismatch |
| OVERTIME_FM_03 | 5 | Per-group hetero rates |
| GENERAL_FM_06 | 2 | Source-missing extrapolation |
| GENERAL_FM_02 | 1 | Decimal-point strip |

### CSV format issues (recovered)

~205 subagent rows initially had unquoted `;` in evidence_quotes that broke CSV parsing. A post-processing recovery script re-joined the over-split fields. Strengthened the prompt for batch 4 with explicit RFC 4180 quoting instructions — that batch had only 4 minor over-splits. Final corpus: 952 rows, 0 violations.

### Confidence cleanup (post-recovery)

After recovery, 45 rows had `UNKNOWN` + non-low confidence (HARD rule violation) and 115 had blank confidence. All forced to `confidence=low` via post-processing. 1 row had high confidence on placeholder evidence — demoted.

## Stage 3.5 — Late schema-gap re-check

Scanned the 624 `unable_to_verify` rows for recurring structural patterns:

| Pattern | Hits | Already in Stage 0 hazards? |
|---|---|---|
| Compulsory overtime mentioned but no annual cap | 85 | Yes (H5 age-conditional) |
| Multi-period trigger (4-week / 26-week / quarter) | 62 | Yes (H2) |
| Working Hours Act standard default | 26 | No (implicit — CAO defers to law) |
| Agency CAO defers to hirer (633, 1060) | 23 | No (new pattern) |
| Specific to driver/sector subgroup | 3 | Yes (H6 per-group) |

**No NEW blocking gaps.** Two new patterns worth documenting in Stage 5a: agency-CAO-defers-to-hirer and Working-Hours-Act-default. Neither blocks Stage 4.

## Stage 4 — Aggregator + audit

`run_aggregation` reconciled 952 deterministic + 952 subagent rows. Three bugs surfaced and fixed during Stage 4:

1. **§1.4 reconciliation didn't handle L2 rows correctly.** L2 has empty verdict (just presence-trigger); reconciliation treated this as "det disagrees with sub" producing 624 spurious conflicts. Fixed: when det.verdict is empty, treat as sub_only regardless of sub's verdict.
2. **Subagent CSV column name mismatch.** Subagent writes `new_value`/`new_unit`; aggregator's `_make_output_row` only looked for `csv_value_new`/`csv_unit_new`. 220 correct_in_place subagent corrections had their proposed values silently dropped. Fixed: `_g` falls back to `new_value`/`new_unit`.
3. **A7 over-broad.** "Pay-rate field with value > 100" is the right check for leave (you can't get paid >100% of wages) but wrong for overtime surcharges where 125%, 150%, 200% are valid. Fixed: A7 excludes `overtime_allowance*`, `overtime_shift_allowance*`, `overtime_unfavourable_hours_allowance*` fields.

### Final corrections.csv summary

| Metric | Value |
|---|---|
| Total rows | 952 |
| is_noop (no real change) | 691 |
| **Real corrections** | **261** |
| `sub_only` (subagent overrode or L2 trigger) | 947 |
| `det_sub_conflict` (NHR) | 5 |

By `changed`:
- `value` only: 91
- `unit` only: 49
- `both`: 121
- `none` (noop): 691

By confidence (real corrections only):
- high: 208 (79.7%)
- medium: 29 (11.1%)
- low: 24 (9.2%)

**Top fields with real corrections:**
- overtime_allowance_value: 60 (mostly OVERTIME_FM_01 multi-tier extractions)
- overtime_allowance_unit: 60
- overtime_unfavourable_hours_allowance_value: 27
- overtime_unfavourable_hours_allowance_unit: 28
- overtime_trigger_daily_value: 18 (R4 clears confirmed)
- overtime_shift_allowance_present: 13 (mostly set_boolean=False)
- overtime_guaranteed_weekends_off_rule_text: 12 (FM_02 age-conditional captures)
- overtime_hetero_present: 6

### Audit

After A7 fix: **0 audit hits, 0 NHR rows**. The 5 `det_sub_conflict` cases (3 hetero_present + 2 min_rest unit) are routed to `corrections_audit.csv` with both det and sub rows preserved — those are 5 cases where Hanna decides.

### A13 disagreement rates

Not surfaced — fewer than 10 firings for any meaningful rule grouping after the L2-shortcut fix.

---

## Sanity-check pass — quality issues found

After the initial Stage 4 declaration of "0 audit hits," a deeper sanity check surfaced three real quality issues. **Two were fixed in code for future topics; one was retroactively applied via a new audit check on the overtime output.**

### Issue 1 — Subagents didn't receive schema field descriptions

Confirmed: zero mentions of `compensation_mode`, `stacking_rule`, `selection_rule`, `min_rest_between_shifts` semantics in the system prompt. Subagents inferred field meaning from names alone. The schema's enum vocabularies (e.g. `compensation_mode ∈ {monetary_pay, TOIL, both, unspecified, other}`) were never communicated. **Result:** zero corrections on those three enum fields — subagents conservatively returned `unable_to_verify` when they could have populated values like `TOIL` for CAOs that explicitly offer time-off-in-lieu.

**Fix for future topics:** `worksheet_builder.build_subagent_prompt` now includes the topic's schema section from `NON_SALARY_PROMPTS_AND_SCHEMA.md` verbatim, with an explicit instruction to use canonical enum values. Cap raised to 6,000 tokens. 5 new tests cover this. **Cannot retroactively fix overtime** — would require re-running Stage 3 with the new prompt.

### Issue 2 — Enum / non-flagged fields never reach the worksheet

`compensation_mode`, `stacking_rule`, `selection_rule` had **0 worksheet items**: (a) my L1 rules don't flag them, (b) the CSV already has populated values, so L2 doesn't fire. The original extractor's values for these fields pass through completely unreviewed.

**Implication:** populated-but-potentially-wrong fields are a blind spot. This is consistent with PLAN.md §1.1's design (most fields never reach the subagent — ~3.5% in leave run), but worth noting for fields with canonical enum constraints.

**Mitigation considered:** add a pattern detector that flags populated enum fields where the value isn't in the canonical set. Deferred — affects topic order, not this overtime run.

### Issue 3 — Evidence quotes don't literally support the proposed value (A17)

163 of 261 real corrections (62%) had evidence_quotes starting with the source-block metadata header (`sgeving:`, `ingangsdatum:`) rather than the verbatim line containing the proposed value. For sampled cases, the new_value (e.g. "35", "0.78") did NOT appear in the stored evidence at all.

The subagent's *reasoning* may have used the full topic_section correctly, but the *citation* it stored is weak — fails the convention "evidence_quote must be verbatim from topic_section AND must literally contain the proposed new_value."

**Fix:** new audit check **A17** added — `verdict ∈ {correct_in_place, move}` AND new_value not in evidence_quote → route to NHR. Skips booleans and UNKNOWN. Decimal-comma variant (0.78 ↔ 0,78) is matched. The system prompt was also updated for future topics to explicitly forbid citing the metadata header.

**Result after A17 routing on overtime:**

| Bucket | Count | Notes |
|---|---|---|
| Total worksheet items | 952 | |
| is_noop (no real change) | 691 | |
| **Real corrections produced** | **261** | (raw count) |
| → routed to NHR by A17 (weak evidence) | 163 | The subagent's answer might still be right; needs human verification |
| → routed to NHR by `det_sub_conflict` | 5 | R6 / R9 cases |
| → **clean real corrections** | **93** | High confidence + value appears in evidence verbatim |

By confidence among the 98 non-NHR real corrections:
- high: 68
- medium: 10
- low: 20

### What to actually trust

- **93 clean real corrections** (mostly OVERTIME_FM_01 multi-tier collapses with verbatim evidence). These can ship as-is.
- **163 NHR rows** need human review. Most are likely correct extractions where the subagent just cited the wrong portion of the source. Sample-checking ~20 would tell us the recovery rate.
- **5 det_sub_conflict rows** are the R6/R9 cases where rule and subagent disagreed.

**Total true Hanna-review workload: 168 rows** (163 weak-evidence + 5 conflicts).

## Stage 5b — Items for Hanna

1. **OVERTIME_FM_04 / FM_05 / FM_06** added to [overtime.md](../conventions/failure_modes/per_topic/overtime.md). Review and edit as needed; no action required unless changes wanted.
2. **NHR queue (post-rerun): 96 + 5 = 101 rows** in [needs_human_review.csv](outputs/needs_human_review.csv). Down from 168 pre-rerun.
3. **Future-topic prompt changes (applied):**
   - **Per-item field_description** — each chunk JSONL item now carries its specific field's schema description (from `NON_SALARY_PROMPTS_AND_SCHEMA.md`). Subagents no longer need to infer field semantics from names alone.
   - **Per-item `enum_values`** — for enum-typed fields, the chunk item carries the canonical enum set. Subagents are required to use literal enum strings (no paraphrases like "time off in lieu" → must be `TOIL`).
   - **`schema_lookup.get_topic_enum_fields()`** — new helper that scans the schema for fields with quoted enum values; used to flag CSV records where the existing value isn't canonical (caught 2 cases in overtime: `not_cumulative` and `mutually_exclusive` on `overtime_stacking_rule` — both reset to canonical `other`).
   - **A17 audit** — value-bearing verdicts must have evidence quotes that literally contain the proposed value.
   - **System prompt is leaner** (4,286 tokens for overtime vs 5,000+ when schema was embedded) — per-item context carries the topic-specific detail.

## Rerun outcome (2026-05-18, ~30 min)

163 A17 + 2 enum-mismatch items were re-processed in 9 chunks with the new prompt + per-item field_description + strict evidence rule.

| Metric | Pre-rerun | Post-rerun | Δ |
|---|---|---|---|
| Real corrections (raw) | 261 | 207 | −54 (rerun subagents stricter; downgraded weak-evidence to unable_to_verify) |
| → NHR (audit-flagged) | 168 | 101 | −67 (−40%) |
| → Clean wins | 93 | 111 | +18 (+19%) |
| A17 hits | 163 | 96 | −67 |

**Clean wins by confidence:** 71 high, 20 medium, 20 low.

The remaining 96 A17 hits are dominated by `*_unit` fields where the canonical unit string ("% of hourly rate") doesn't appear verbatim in source even though semantically equivalent phrases do ("hourly wage", "hourly salary"). Honest tradeoff: those corrections are probably right but the strict audit flags them for human verification rather than auto-accept.

**Total Hanna-review workload: 101 rows.** The 111 clean wins can ship as-is.

---

## v2 — field-specific keyword rerun (2026-05-19)

After homeoffice (which used `field_specific=True` L2 + the leaner per-item field_description prompt), we re-ran overtime with the same field-specific pipeline to validate v1's "broad presence trigger + retroactive A17" approach against v2's "field-specific keywords + per-item schema + strict evidence rule from the start."

### Why re-run

v1 used `presence_scan` in topic-mode: any keyword from any of the topic's fields triggered L2 on every empty field. v2 uses `field_specific=True` so only field-relevant keywords trigger that field's L2. Concretely:

- v1: 927 L2 triggers (one per empty field per scoped record where any topic keyword matched the source)
- v2: ~700 L2 triggers (one per `(record_id, field)` where field-specific keywords matched)

The field-specific keywords for overtime were built into `qa/shared/field_keywords.py` (English-PRIMARY per CLAUDE.md), with retained Dutch terms only for the acronyms / legal concepts that survive the upstream translation: ATW, ORT, TOIL, "tijd voor tijd", "overuren", "meeruren", "ploegentoeslag", "consignatie", "ploegendienst", etc.

### v2 totals (on disk, qa/qa_overtime/outputs/)

| Metric | v1 (pre-rerun memo) | v2 (current outputs/) | Δ |
|---|---|---|---|
| Worksheet items | 952 | 902 | −50 |
| is_noop (no real change) | 691 | 684 | −7 |
| Real corrections (raw) | 207 | 218 | +11 |
| Clean wins | 106 | 127 | +21 |
| NHR (audit-flagged) | 101 | 91 | −10 |
| `det_only` rows | (n/a) | 334 | |
| `sub_only` rows | (n/a) | 471 | |
| `needs_human_review` | (n/a) | 97 | |

Net: **+21 clean wins, −10 NHR**, fewer worksheet items overall. The field-specific anchoring eliminated low-value `L2 → unable_to_verify` chunk overhead, and the leaner per-item schema injection (rather than v1's retroactive A17 rerun) produced more correct extractions on first pass.

### Audit hits (v2)

A12, A14, A15, A17 all fired (counts in `corrections_audit.csv` — currently rolled into NHR rather than a separate audit file). A17 is still the dominant flag for unit fields where the canonical unit string ("% of hourly rate") doesn't appear verbatim in source even though semantically equivalent phrases do — same finding as v1.

### v1 outputs preserved

`qa/qa_overtime/outputs/v1_baseline/` keeps the v1 corrections, NHR, audit, and deterministic CSVs untouched as the pre-rerun baseline.

**Final v2 workload: 91 NHR rows for Hanna review; 127 clean wins ready to ship.**

### v2.1 — CSV-recovery correction (2026-05-21)

A later audit found the v2 corrections.csv had been built from **un-recovered subagent chunks**: 191 of 856 subagent rows had unquoted `;` in evidence/notes, and 42 of them landed in corrections.csv as misaligned phantom rows (a verdict word like `unable_to_verify` sitting in the `original_field` column). The other ~149 had truncated/shifted evidence and confidence.

**Fix:** ran the new `qa/shared/csv_recovery.py` over the overtime chunks (191 recovered, 3 junk lines dropped, 0 unrecoverable), then re-aggregated with the new reproducible `qa_overtime_aggregate.py` driver. Pre-fix corrections.csv preserved at `corrections_prefix_backup.csv`; raw chunks at `subagent_worksheets/chunks_raw_backup/`.

| Metric | v2 (mangled) | v2.1 (recovered) | Δ |
|---|---|---|---|
| Total rows | 902 | 860 | −42 (phantom rows merged into their real keys) |
| is_noop | 684 | 595 | −89 |
| Real corrections (raw) | 218 | 265 | +47 |
| Clean wins | 127 | 134 | +7 |
| NHR | 91 | 135 | +44 |

The 42-row drop is exactly the count of phantom misaligned rows, now correctly merged. Real corrections and NHR both rose because the recovered rows carry their proper verdicts, values, and evidence — previously those were silently dropped or miscategorised. Audit after recovery: **A17=131, A15=4, A14=2** (A17 still dominated by unit-canonicalisation where "% of hourly rate" isn't verbatim in source).

### v2.2 — unit-without-value suppression (2026-05-21)

A content audit of the 134 clean wins found **19 were `*_unit` corrections setting "% of hourly rate" on a field whose paired `*_value` was empty and not itself a clean win** (e.g. `overtime_unfavourable_hours_allowance_unit` set while `..._allowance_value` stayed blank). A unit with no value is not actionable for Hanna and was inflating the clean-win count.

Root cause: L2 fires on `_unit` fields independently of `_value`, and the subagent can infer the canonical unit ("% of hourly rate") from generic surcharge language even when no number is extractable. (38 raw corrections were affected; 19 were clean wins — the rest were already NHR-flagged and stay in the review queue.)

**Fix:** new `aggregator_lib.suppress_unit_without_value()` reclassifies such clean-win unit rows to `is_noop` (runs after NHR flagging, skips NHR rows so the review queue stays consistent). Wired into all aggregate drivers; 2 regression tests added.

| Metric | v2.1 | v2.2 (final) |
|---|---|---|
| Real corrections (raw) | 265 | 265 |
| NHR | 135 | 135 |
| **Clean wins** | 134 | **115** |
| meaningless unit-only clean wins | 19 | **0** |

**Final overtime result: 115 clean wins ready to ship; 135 NHR rows for Hanna.** The earlier 218/127/91 (and the intermediate 134) overcounted due to (a) the CSV-mangling bug and (b) unit-without-value noise — both now fixed.

### v2.3 — project-wide noop-vs-CSV correction (2026-05-22)

A later project-wide audit found a third inflation source affecting **every** topic: L2 worksheet items carry `csv_value_old=""`, so a subagent *confirming* a value already in the CSV registered as a phantom change. New `aggregator_lib.suppress_noop_vs_actual_csv` compares the proposed value to the **actual** scoped CSV value and reclassifies confirmations as no-ops. For overtime this removed **17** spurious clean wins.

**Authoritative overtime result: 98 clean wins; 135 NHR.**

