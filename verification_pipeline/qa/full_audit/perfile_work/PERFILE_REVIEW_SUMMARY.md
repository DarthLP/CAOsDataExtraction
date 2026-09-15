# Per-file conflict verification — review package (SURFACE-ONLY)

Independent, source-grounded re-verification of every **within-agreement conflict** in
the dataset. An *agreement* = `cao_number` × `ingangsdatum` (the true version-record
unit). Where its version-records disagreed on a field, a subagent re-read **each
record's own source file independently** (blind to the dataset value and to its
siblings), re-extracted the field using the field's pydantic/schema definition as the
anchor, then we compared the independent extractions to what the dataset holds.

**STATUS (2026-07-01): `FIX_clear` (3,318 cells) has been applied and promoted into
`corrected_dataset.csv`** (see `../APPLY_POLICY.md`, `../../DATASETS.md`, `../../../docs/DATA_LINEAGE.md`).
The remaining buckets below — FIX_audit, RECONCILE, EMPTY, REVIEW, statutory_deferred,
surcharge_normalize (≈ 5,935 cells) — are **still a surface-only review queue for Hanna.**
Note: the `RECONCILE 5641` in the table below is the pre-split figure; after the
"never assume silence = same" fix it is **KEEP 2,681 + RECONCILE 2,960**.

## Coverage
- **13 source-backed topics**, **3,091 agreement×topic units**, **24,999 (record × field) cells**.
- Excluded: `general_*` (no `by_topic` source file) and out-of-scope CAOs (only the 95
  curated CAOs have source text). A filename-normalization fix recovered **903** in-scope
  records that the exact matcher had missed.

## Results

| topic | model | cells | CONFIRM | RECONCILE | FIX | EMPTY | REVIEW |
|---|---|---:|---:|---:|---:|---:|---:|
| leave | sonnet | 5139 | 2913 | 1295 | 713 | 72 | 146 |
| term | opus | 2705 | 1408 | 517 | 477 | 207 | 96 |
| overtime | sonnet | 3298 | 1526 | 248 | 1197 | 231 | 96 |
| pension | opus | 1991 | 1007 | 549 | 193 | 164 | 78 |
| bonus | sonnet+haiku | 1757 | 972 | 505 | 251 | 27 | 2 |
| fringe | sonnet+haiku | 2257 | 1268 | 546 | 401 | 28 | 14 |
| contract | sonnet | 2017 | 1182 | 504 | 272 | 44 | 15 |
| training | sonnet+haiku | 957 | 449 | 236 | 174 | 69 | 29 |
| safety | sonnet | 3531 | 1954 | 813 | 754 | 0 | 10 |
| wage | haiku | 880 | 517 | 288 | 75 | 0 | 0 |
| homeoffice | haiku | 365 | 190 | 104 | 56 | 6 | 9 |
| childcare | sonnet | 102 | 48 | 36 | 15 | 0 | 3 |
| **TOTAL** | | **24999** | **13434** | **5641** | **4578** | **848** | **498** |

**CONFIRM + RECONCILE = 76%** (high-confidence) · **FIX + EMPTY = 22%** · **REVIEW = 2%**.

## What each action means
- **CONFIRM** — the dataset matches the record's own source (unit-aware). No change.
- **RECONCILE** — this record is silent but a fuller sibling carries the value; the
  agreement-level value should be inherited (the "thin docs are jumpy" case). Target in
  `fix_target_value`.
- **FIX** — the record's own file states a *different* quantity than the dataset holds.
  Target in `fix_target_value`/`fix_target_unit`. **Read with the caveats below.**
- **EMPTY** — every version is silent on a field the dataset populated → likely a
  wrong-slot/contamination value to remove.
- **REVIEW** — genuinely undecidable: a numeric/enum field no version states yet the
  dataset has a value (299 numeric + 183 enum + 16 boolean = 498).

## Caveats — FIX is a REVIEW queue, not an auto-apply list
The consolidator is unit-aware (2 yr ≡ 104 wk, 150% ≡ 1.5×), respects the no-sum-statutory
rule, handles surcharge≡total (25% surcharge ≡ 125% of rate), and decides booleans by
evidence. Residual FIX still mixes genuine errors with **convention/representation
differences** a human must adjudicate:
- **Statutory base vs total** — e.g. paternity 2 (CAO) vs 5 (incl. statutory WAZ days);
  vacation 144 (statutory) vs 165.8 (incl. above-statutory). Per project policy the
  subagents kept the CAO base figure, but the dataset's choice varies by field.
- **Surcharge vs total** — overtime fields store the increment; only cleanly-marked
  cases are auto-equated.
- **Tier-selection enums** — `*_selection_rule`, `term_dismissal_approval` are interpretive.
- **Representation** — e.g. `2/3` vs `66.7` (same value, different spelling).

## Models & validation
Sonnet by default; **Opus** for term + pension (complex legal topics); **Haiku** for the
last five topics (cost). A Haiku-vs-Sonnet bake-off on high-REVIEW units confirmed Haiku
does **not** under-read — it found identical content on training and agreed on wage
absence; differences were cosmetic (labeling/representation).

## Files
- `ALL_actions.csv` — all 24,999 rows, one combined sheet (cols: topic, agreement_id,
  record_id, field, dataset_value/unit, source_value/unit, assessment, proposed_action,
  fix_target_value/unit, explanation, quote).
- `<topic>/actions.csv` — per-topic.
- `<topic>/results/u*.json` — raw per-unit subagent verdicts (auditable).
- `PERFILE_INSTRUCTIONS.md` — the exact rules every subagent followed.

## Suggested next step
Filter `ALL_actions.csv` to `proposed_action in (FIX, EMPTY)` for the dataset-changing
review (5,426 cells); `RECONCILE` (5,641) is the thin-refiling inheritance set. Applying
any of it would follow the project's usual guarded, reversible apply onto a *new*
proposed copy with re-verification — only on explicit go-ahead.
