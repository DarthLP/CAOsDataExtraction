# CAO QA Pipeline — Status Board

Maintained by Claude Code. Updated after each stage completes.

> **Dataset state (2026-07-15): G33 — 34 correction layers.** This board tracks the per-topic
> QA stages (all done). Everything after that — the correction campaigns L8–L34, audits and
> closures — is ledgered in [../docs/DATA_LINEAGE.md](../docs/DATA_LINEAGE.md) (narrative) and
> `../indices/corrections/corrections_log.xlsx` (per-change log).

| Topic       | Stage 0 | Stage 1 | Stage 2 | Stage 3 | Stage 4 | Stage 5 | Last run   |
|-------------|---------|---------|---------|---------|---------|---------|------------|
| leave       | n/a     | done    | done    | done    | done    | done    | 2026-04-xx |
| overtime    | done    | done    | done    | done    | done    | done    | 2026-05-19 |
| homeoffice  | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| training    | done    | done    | done    | done    | done    | done    | 2026-05-19 |
| contract    | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| bonus       | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| fringe      | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| safety      | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| childcare   | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| ai          | done    | done    | done    | done    | done    | done    | 2026-05-22 |
| term        | done    | done    | done    | done    | done    | done    | 2026-05-25 |
| pension     | done    | done    | done    | done    | done    | done    | 2026-05-27 |
| wage        | done    | done    | done    | done    | done    | done    | 2026-05-23 |

**States**: `-` not started · `pending` blocked on Hanna · `in_progress` · `done` · `n/a`

## Phase status

- **Phase 0** (read + acknowledge): done
- **Phase 0.5** (prerequisite verification): done — see `../docs/archive/phase_0_5_findings.md`
- **Phase 1** (repository setup): done as of 2026-05-17
- **Phase 2** (port qa_leave → qa/shared/): done as of 2026-05-18 (leave smoke test deferred; see `../docs/archive/phase_2_notes.md`)
- **Phase 3** (overtime end-to-end): done as of 2026-05-21 — v2.2 (field-specific L2 + CSV recovery + unit-without-value suppression): 265 real corrections, **115 clean + 135 NHR**, 6 FM entries. v1 baseline at `qa_overtime/outputs/v1_baseline/`; pre-recovery v2 at `corrections_prefix_backup.csv`. Two bugs fixed: CSV-mangling (`csv_recovery.py`) and unit-set-without-value noise (`aggregator_lib.suppress_unit_without_value`).
- **Phase 4** (topics 2–9): **DONE** — all 8 done (overtime, homeoffice, training, contract, bonus, fringe, safety, childcare, ai). 9 non-leave topics QA'd (overtime was Phase 3).
- **Phase 5 wage wrap-up**: **DONE** as of 2026-05-23 — 95 scoped records, 163 worksheet items, **3 clean wins (CAO 721017 perf_step_var, CAO 1471012 pers_allow), 0 NHR**. The extractor's wage-mechanics flags were ~98% accurate; scale movement is almost always automatic (periodieken/functiejaren), so the targeted discretionary provisions are rare. See `qa_wage/wage_summary_memo.md` + WAGE_FM_01..03. (Term + pension are the only topics left, gated on Phase 4.5.)
- **Phase 4.5** (era-baseline review with Hanna): **NEXT, blocking — AWAITING HANNA**. Review doc drafted at `../docs/archive/phase_4_5_era_baseline_review.md`. **Key finding: `era_baselines.py` is still a Phase-1 skeleton — it has only the 4 leave baselines; every non-leave topic gets `[]`.** So the gate is "approve the proposed statutory values before they're written in," not "review existing code." The doc proposes era-segmented baselines for term (notice periods, probation, transitievergoeding), pension (Witteveen accrual cap, franchise, AOW/pensioenrichtleeftijd), and contract (ketenregeling) — each marked *proposed, verify* with statute cites. It also surfaces a design gap: `is_below_statutory` only models **floors**, but probation length and the Witteveen accrual rate are **caps**. **term** and **pension** cannot run until Hanna signs off.

### AUTHORITATIVE clean-win / NHR counts (after the 2026-05-24 A17/A18 audit refinement)

| Topic | clean | NHR | (was: clean / NHR) |
|---|---|---|---|
| overtime | 235 | 7 | 198 / 45 |
| homeoffice | 3 | 0 | 1 / 2 |
| training | 38 | 45 | 24 / 60 |
| contract | 12 | 2 | 12 / 2 |
| bonus | 29 | 2 | 28 / 3 |
| fringe | 46 | 1 | 34 / 13 |
| safety | 21 | 1 | 21 / 1 |
| childcare | 2 | 0 | 2 / 0 |
| ai | 0 | 0 | 0 / 0 |
| wage | 3 | 0 | 3 / 0 |
| term | 18 | 1 | — (new) |
| pension | 0 | 0 | — (new) |
| **total (12 topics — ALL DONE)** | **407** | **59** | **323 / 126** |

**pension (2026-05-27, Opus):** 95 records, 1042 L2 → **0 clean wins, 0 NHR, 5 era-baseline outliers**. Overwhelmingly fund-deferred (CAOs point to bpfBOUW/ABP/PFZW/PMT regs without stating figures → fields correctly empty; records with concrete values already had them in the dataset). The 5 era outliers include **2 genuine extraction errors** (157017, 1287018: `accrual_rate=100%` — a Generatiepact "100% accrual maintained" phrase misread as a DB rate, caught by the Witteveen cap) + 3 franchise-floor flags to eyeball (`era_outliers.csv`). See `qa_pension/pension_summary_memo.md` + PENSION_FM_01..04.

**Era-baseline `_parse_date` bug found + fixed (2026-05-27).** The post-hoc era flagging silently no-op'd for term + pension because `_parse_date` couldn't read the scoped CSV's Dutch `DD/MM/YYYY` dates. Fixed (+2 regression tests, **92 passing**); re-ran both — term genuinely 0 outliers, pension 5. This validated the Phase 4.5 era effort end-to-end.

> **ALL 12 NON-LEAVE TOPICS COMPLETE.** Era baselines (Phase 4.5) approved by Hanna, encoded post-hoc (never in the prompt), and validated in production.

**term (2026-05-25, Opus):** 95 records, 913 L2 → **18 clean wins, 1 NHR, 0 era-baseline outliers**. Overwhelmingly statutory-restatement (894 no-ops, correctly empty — the "no statutory fill-in" rule held, 0 fabricated figures). **Plus 11 spelled-out / tenure-graded notice·probation values surfaced** for human digit-confirmation in `qa_term/outputs/spelled_out_values_for_review.csv` (genuine values the subagents declined to digit-ize per the strict-evidence rule). Era baselines ran **post-hoc only** — never shown to the model. Only **pension** remains.

**2026-05-24 audit refinement (A17/A18 normalization-aware).** A diagnostic on the 126 NHR items found ~76% were a single audit false-positive: A17 (literal-substring evidence check) and A18 (unable_to_verify routing) misfired on `_unit`/enum fields whose value is a *canonical normalization* (e.g. source "€0.23 per km" → unit "EUR per km" is correct but not a verbatim substring). Fixes: **A17** skips normalized fields backed by real (non-placeholder) evidence; **A18** no longer fires when no value is actually applied (empty/UNKNOWN — matching its own definition). Result: **NHR 126→58, clean 323→389.** The surviving 58 are genuine (det-vs-sub conflicts, non-empty `unable_to_verify`, placeholder-evidence, truncation).

**Second-pass NHR review (full-text).** The 58 survivors were sent through a full-source-text second pass (`qa/second_pass_nhr/`, JSONL output) that tags each with a `human_review_reason` and a disposition (RESOLVED_CONFIRM / RESOLVED_CORRECT / ESCALATE) for Hanna. This is a surfacing/enrichment layer — **nothing is written to any `corrections.csv`**; RESOLVED_CORRECT items are *suggestions* for Hanna to apply. (47 RESOLVED_CORRECT, 5 CONFIRM, 6 ESCALATE; all 47 evidence quotes verbatim-in-source.)

**Verification pass on accepted clean wins (full-text).** All **226** substantive (non-`_unit`) clean-win corrections across the 10 topics were independently re-checked against full source (`qa/verify_changes/`, see `VERIFY_SUMMARY.md`). Result: **195 CONFIRM (86%), 23 NEEDS_CHANGE, 8 ESCALATE**. The 23 refine to **13 genuine content errors** (6 of them over-asserted `True` presence flags; record 20002/contract cited evidence absent from its source), **6 genuinely-missing units**, and **4 false alarms**. Surfacing only — not applied. Two systemic patterns to consider before term/pension: the first pass occasionally over-asserts presence booleans on weak evidence, and split value/unit rows can drop a unit.

**Environment note (2026-05-22):** shell `python3` may resolve to system Python 3.9 (no pandas). Use **`python3.13`** to run pipeline scripts.

> These supersede ALL earlier per-topic numbers in this file's history and the topic memos. Several rounds of correctness fixes landed this session; the table above is the trustworthy final state.

**Data-quality fixes this session** (all in shared code, wired into every aggregate driver, regression-tested — 72 tests passing):
- **A17/A18 normalization-aware (2026-05-24)** — A17 no longer flags `_unit`/enum fields whose canonical value is backed by real evidence (it's normalization, not fabrication); A18 no longer fires when no value is applied (empty/UNKNOWN). Cleared ~68 false-positive NHR across done topics (NHR 126→58, clean 323→389) and prevents the same flood in term/pension. 6 new tests.
- `csv_recovery.py` — repairs subagent rows with unquoted `;` (idempotent).
- **`_make_output_row` winner-value precedence (the deepest bug)** — on a `sub_only` row the output value was being pulled from the *det rule's* proposal instead of the subagent's answer, silently overwriting subagent decisions across every topic. This both *inflated* topics where subagents rejected det proposals (homeoffice 21→1, bonus, contract) and *deflated* overtime (98→198: subagents' correct extractions were masked by det values that then failed A17→NHR). Now output faithfully reflects the reconciliation winner.
- `suppress_noop_vs_actual_csv` — drops corrections whose value equals the existing CSV value (L2 confirmations registering as phantom changes): 279 bonus, 234 fringe, 84 contract, 17 overtime, 12 training, 4 homeoffice.
- `suppress_boolean_on_numeric_field` — drops `True/False` written into numeric `_range/_value` fields (41 contract).
- `suppress_unit_without_value` — drops `_unit` corrections with no paired value (19 overtime).
- **A18 audit** — `unable_to_verify` + applied change → NHR.
- **NHR-noop guard** — `flag_outliers` no longer routes `is_noop=True` rows to NHR (a no-op needs no review) unless det_sub_conflict; this cut bonus NHR 316→3.

**Clean-win regression fixes (2026-05-28, source-verified).** A sanity scan of `corrected_dataset.csv` surfaced **18 cells / 14 records** where accepted clean-wins had introduced errors, verified against the LLM extracts (`CAOsDataExtraction/outputs/llm_extracted/new_flow/<cao>/`) and raw Dutch PDFs:
- **9× `training_career_scan_freq_unit`** — clean-win overwrote the correct text unit `years`/`year` with its reciprocal as a number (`0.25`, `0.2`, …). Fixed by deleting the bad rows → original unit restored; values untouched.
- **3× `overtime_allowance_unit`** (826016, 26017, 748016) — unit held a copy of the value; corrected to the proper percent string (value confirmed in source, e.g. "25% of the hourly wage").
- **`overtime_compulsory_annual_value` 557002** (rule sentence in a numeric field), **`term_employer_notice` 433027** and **`fringe_relocation_allowance` 1612020** (value+unit unsupported by source) — bad clean-win ADDs, reverted to blank.
- **`overtime_unfavourable_hours_allowance_unit` 683012** → `percent` (value 100 left as-is, unverified).
- Per-cell record in [nonleave_source_corrections.csv](nonleave_source_corrections.csv); applied via `apply_list.csv` + `apply_corrections.py` (in `apply_changelog.csv`). **Verified NOT an error:** `overtime_max_hours_per_week=72` (26 rows, 6 CAOs) — all performing-arts CAOs under the Arbeidstijdenbesluit (cao 2143 source: "max 72 hours per week").
- ⚠️ These edits live in `apply_list.csv`; if `consolidate_review.py` regenerates it, re-apply from `nonleave_source_corrections.csv`.

## Full-audit (dataset-internal, ALL 2,739 records × 317 fields) — 2026-05-31

A broad, dataset-internal QA sweep independent of the per-topic pipeline, in `full_audit/`.
Five structural checks → **5,088 flags** (originally 4,778 from 4 checks; Check 5 added 310),
then **holistic per-(topic, record) verification** of the high+medium flags
(read the complete topic source, judge every populated field at once). **Surface-only — nothing
written to the dataset or `CAOsDataExtraction/`.**

- Verdicts (3,157 verified): **CONFIRM 2,120 · NEEDS_CHANGE 765 · UNSUPPORTED 272** (+113 no_source); ~32.8% real-issue rate, 0 pending.
- **405 off-flag findings** (`verify/new_findings.csv`) — wrong values the four heuristics structurally couldn't see (caught only because verify judged *all* fields of a flagged record's topic).
- Per-check precision: enum_format 52.6% > cross_version 35.3% > era_baseline 22.0% > numeric_outlier 17.9%. `value_unit_contamination` fired 0 (only catches structural slips).
- **Check 5 `unit_semantics` (added 2026-06-02)** closes that gap: flags cells whose unit *family* (time/pay/count/distance) disagrees with the field's canonical family — e.g. `leave_paid_maternity_value=100` with a `% of salary` unit (pay-rate in a DURATION field; 198 records). **351 flags (341 high)**, 310 new. 9 tests.
  - **Preservation-aware verification:** 10 subagents under never-delete/never-invent rules → **KEEP_NONSTANDARD_UNIT 269 (87%) · NEEDS_CHANGE 21 · RELOCATE 17 · CONFIRM/UNSUPPORTED 1/1**. Most are valid data in a non-standard unit (no source duration) → **preserve, don't delete**; a naive blanking fix would have destroyed ~286 cells. Per-cell detail + original values preserved in `unit_semantics_reconciliation.csv`.
- Spot-check: 14/14 cells sound (flag + verdict). Dispatch: 434 worksheets, ≤12 parallel, opus for term/pension.
- Full writeup: [full_audit/full_audit_summary.md](full_audit/full_audit_summary.md); master output [full_audit/full_audit_flags.csv](full_audit/full_audit_flags.csv).

### Corrections applied + promoted — 2026-06-04

Surfaced flags were routed to corrections on a **copy**, then **twice-verified** — holistic, then **definition-anchored re-verification** (each subagent given the field's pydantic/schema description), which caught a **~22% error rate** in the first pass (e.g. probation max 1→2 mo, surcharge max 17→80%, `overtime_selection_rule`→`other` ×29). The 61-item human-review tail was resolved under a **no-statutory / no-invent policy** (explicit source number or empty; never fill statutory/silent). Then **promoted** → `corrected_dataset.csv`: **781 cells** (filled 208 · changed 527 · emptied 46), with backup `corrected_dataset.bak.2026-06-04.csv`, `apply_changelog.csv`, and `promotion_diff_summary.md`. Policy now in `CLAUDE.md`.

### Agreement-consistency QA layer (surface-only) — 2026-06-04

Regrouped by **agreement = `cao_number` × `ingangsdatum`** — the true version-record unit (the old `cross_version` check grouped by `cao_number` alone, across all years, and was noisy). Within-agreement structured-field **consistency = 83.28%**, validated as a **regression metric** (rose 83.14%→83.28% from original→promoted: prior corrections made siblings *more* consistent). Surfaces **17,033 back-fill cells** (one version blank, a sibling fills it; 7,572 high-confidence) and **18,515 conflicts** triaged to a **2,631 high-priority** review queue (1,667 in-scope; 266 numeric ≥10× gaps = likely %-vs-EUR confusion). **Nothing applied.** Outputs: `agreement_index.csv`, `backfill_candidates.csv`, `agreement_conflicts.csv`, `agreement_consistency_summary.md`. 10 tests.

### Per-file conflict verification — all 13 topics (surface-only) — 2026-06-05

Independent, source-grounded re-verification of **every within-agreement conflict**: for each
field where an agreement's version-records disagreed, a subagent re-read **each record's own
source file blind** (schema-def anchored) and we compared to the dataset. **3,091 units /
24,999 cells** across 13 source-backed topics (`general` + out-of-scope CAOs have no source; a
filename-normalization fix recovered 903 in-scope records). Consolidator is unit-aware (2 yr ≡
104 wk, 150 % ≡ 1.5×), no-sum-statutory, surcharge ≡ total, boolean-by-evidence. **Final:
CONFIRM 13,434 · RECONCILE 5,641 · FIX 4,578 · EMPTY 848 · REVIEW 498** → CONFIRM+RECONCILE
**76 %**, FIX+EMPTY 22 %, REVIEW 2 %. Models: Sonnet default, Opus for term+pension, Haiku for
the last 5 topics (bake-off-validated). **FIX is a review queue** (statutory base-vs-total,
surcharge-vs-total, tier enums), not auto-apply. Package: `full_audit/perfile_work/` → `ALL_actions.csv`, `PERFILE_REVIEW_SUMMARY.md`,
per-topic `actions.csv`, raw `results/u*.json`; rules in `PERFILE_INSTRUCTIONS.md`.

**Applied (2026-06-05):** only **FIX_clear** (2,949 changes grounded 100% in each record's *own*
source) written to a **copy** → `qa/corrected_dataset.perfile_applied.csv` (3,318 cells / 1,000
records; paired units included). Verified: 0 changes outside the changelog, original
`corrected_dataset.csv` untouched, reversible via `full_audit/perfile_apply_changelog.csv`. NOT
applied (per `full_audit/APPLY_POLICY.md`): FIX_audit (convention), RECONCILE + EMPTY (absence
removals), KEEP (never inherit from siblings), REVIEW, statutory_deferred, surcharge_normalize.

### G2 promoted to canonical + indices rebuilt + repo reorg — 2026-07-01

**Supersedes the "corrected_dataset.csv untouched" note above.** The `FIX_clear` copy
(`corrected_dataset.perfile_applied.csv`) was **promoted to `qa/corrected_dataset.csv`** (canonical):
+3,318 cells / 1,000 records vs the pre-per-file G1, now snapshotted as
`qa/_old/corrected_dataset.bak.2026-07-01.csv` (backups moved to `qa/_old/`; the `perfile_applied.csv`
copy was deleted after promotion — byte-identical). All **13 indices rebuilt** on it (21 outputs changed;
`mw`/`ai` unchanged by design). Still surface-only (≈ 5,935 cells): FIX_audit 1,416 · RECONCILE 2,960 ·
EMPTY 848 · REVIEW 498 · statutory_deferred 172 · surcharge_normalize 41. Lineage + ledger:
[../docs/DATA_LINEAGE.md](../docs/DATA_LINEAGE.md); manifest [DATASETS.md](DATASETS.md).
**Repo reorg:** root cleaned (junk + artifact dirs removed); planning/phase docs → `docs/` + `docs/archive/`;
new `README.md`, `docs/DATA_LINEAGE.md`, `docs/DECISIONS.md`, `qa/DATASETS.md`.

### Layers 5+6: convention adjudication + Hanna rulings promoted (G4→G5) — 2026-07-03

**L5 (2,105 cells):** FIX_audit 1,416 + surcharge_normalize 41 + statutory_deferred 172 adjudicated
per cell by **12 Haiku agents** under the ruled conventions (base figure · surcharge=increment · first
tier · representation-equality · strict enums · quote-evidence); independent **Sonnet spot-verify
57/60 agree** (3 disagreements excluded → residue). Plus Hanna's ruling sheet: 22 DELETE patterns
(345 blanks) + the notice-range 1-month conditional (13 sets). **L6 (151 cells):** the 15 CHECK
patterns resolved per accepted suggestions (81 blanks, 683-bandwidth normalized to 75/125, 19 keeps).
Canonical = **G5** (9,155 (measured) cells vs raw); backups `_old/…pre-wave2` (G3) + `_old/…pre-L6` (G4);
changelogs `wave2_apply_changelog.csv` + `l6_apply_changelog.csv`. Indices rebuilt each promotion;
152 tests green. Residue for a final skim: `wave2_residue.csv` (48). Queue closure: `RETIRED_QUEUES.md`.

### Layer 4: verified absence-removals promoted (G3) — 2026-07-03

The RECONCILE+EMPTY queue went through a two-stage gate instead of blanket apply: a **100-cell
stratified sample vs full source found 15% false-removals** (numerics 83% — carved out entirely as
convention disputes), then all 3,119 keyword-flagged bool/enum cells were **snippet-judged** (Sonnet,
calibrated on the sample: catches 4/5 known-bad; Haiku rejected — same safety but 54% over-caution).
**Applied to canonical: 2,332 cells** (1,813 bool `True→False` + 519 enum) = **G3**, backup
`_old/corrected_dataset.bak.2026-07-03.csv`, reversible via `full_audit/removals_apply_changelog.csv`.
Indices rebuilt (20 outputs changed). **Open queues for Hanna:** 929 AFFIRMED/UNCLEAR →
`full_audit/removals_bool_enum_review.csv`; 547 numerics → `full_audit/removals_numeric_ruling_sheet.csv`;
plus FIX_audit 1,416 · REVIEW 498 · statutory_deferred 172 · surcharge_normalize 41. Conventions adopted
(2026-07-01): **CAO base figure + surcharge increment** — see `docs/DECISIONS.md`.

## Key references

- Summary indices (derived products, separate from the QA pipeline): [indices/INDICES_OVERVIEW.md](../indices/INDICES_OVERVIEW.md) — **v2 since 2026-07-05**: one pooled z per file per topic (comparable across years), `datum_kennisgeving` time axis, cao×month in-force panel + cross-CAO mean/variance series, statutory pseudo-file on the same yardstick, wage ladder from the parser salary dataset (p10…p90 + WML ratios), leave coverage, factor analyses. Design: [indices/INDICES_V2_PLAN.md](../indices/INDICES_V2_PLAN.md) · methodology: [indices/README.md](../indices/README.md) · v1 archived in `indices/_old_v1/`
- Architecture: [PLAN.md](PLAN.md)
- Ordered execution: [EXPERT_IMPLEMENTATION_PLAN.md](EXPERT_IMPLEMENTATION_PLAN.md)
- Conventions every subagent reads: [conventions/general_conventions.md](conventions/general_conventions.md)
- 16-check audit catalogue: [conventions/audit_checks.md](conventions/audit_checks.md)
- Output schema: [CORRECTIONS_SCHEMA.md](CORRECTIONS_SCHEMA.md)

## Empirically verified facts (from Phase 0.5)

- `MAX_PARALLEL_HARD_CAP` = **12** (Task-tool batch limit)
- Python 3.13.0 + pip 24.2, NO PEP 668 enforcement (no `--break-system-packages` needed)
- CSV delimiter: `;` (semicolon)
- Scope: 95 CAOs × ~16 versions = **1,505 records per topic**
- bonus + wage share `wage_information.md` (per schema); per-item slice caps need raise to 3,500/6,000
