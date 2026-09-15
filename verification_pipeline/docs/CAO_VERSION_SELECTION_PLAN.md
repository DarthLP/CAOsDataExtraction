# CAO Version Selection & De-duplication — Plan

**Owner:** Hanna · **Drafted:** 2026-06-27 · **Status:** design agreed, not yet built

The problem: most CAOs have **many source files for the same agreement** — the original
plus mid-term republications of the integral text. We need a principled way to pick the
canonical record(s) per CAO so downstream analysis (incl. `indices/`) isn't double-counting
or mixing versions.

---

## 0. Data & artifacts

| What | Where |
|---|---|
| Final non-salary table | `inputs/extracted_data_non_salary.csv` (= `CAOsDataExtraction/outputs/excel/new_results/extracted_data_non_salary.csv`) |
| Final salary table (long) | `CAOsDataExtraction/outputs/excel/new_results/extracted_data_salary.csv` |
| Original parsed text ("step 1") | `CAOsDataExtraction/outputs/parsed_pdfs/parsed_pdfs_markdown/<cao>/<file>.md` |
| Post-extraction JSON ("step 2") | `CAOsDataExtraction/outputs/llm_extracted/new_flow/<cao>/<file>_extract.json` |
| Pipeline | p2_extract (PDF→md) → p3_llmExtraction (gemini-2.5-flash, temp 0) → p4_analysis (sets `general_document_type`/`general_updated_topics`) → p5 excel |

Scale: 2,739 rows · 242 CAOs · **1,614 `(cao, start, expiry)` term-groups, 643 with >1 file.**
(NB: project QA scope is the 95-CAO / 1,505-record subset — confirm which subset dedup runs on.)

---

## 1. Findings (evidence-based)

1. **`general_document_type` separates full vs delta.** full_cao_original (2062) + full_cao_update
   (635) = standalone full CAOs. partial_amendment_*/annex/other_supplement (~40) = deltas only
   (verified: a "Bijlage" is 12 KB and self-describes as the term's new clauses vs ~150 KB full).
   **22 term-groups have *only* a partial** → no full version exists.

2. **Two duplicate patterns, indistinguishable from metadata alone:**
   - **A — editorial dupes** (e.g. CAO 10 2021-2022): same agreement re-issued; **final values
     identical** (17/17 wage cells, 0 disagreements); differ only by edition date + OCR noise.
   - **B — real versions** (e.g. CAO 633 2012-2017): successive mid-term republications;
     **values evolve**; need the latest *complete* one.

3. **Two clocks.** Term (Looptijd) = `ingangsdatum → expiratiedatum`, **constant** across a term's
   editions. Each **wage table has its own effective date** (`salary_N_start_date`) scattered inside
   the term. Each republication carries only its **window** of wage tables (CAO 633 juli-2017 edition
   holds **only 2017** wages; 2013 wages live only in the 2012-2014 editions). ⇒ picking one latest
   edition **loses early-term wage history**.

4. **Date-field reliability** (coverage): `ingangsdatum` 100%, `expiratiedatum` 100%,
   `datum_kennisgeving` **99%** (= the document's page-header edition date), `general_start_date` 99%,
   **`general_signing_date` only 26%** (and it's the *signing* of the text, often the original term
   signing — not the edition date). `id` does **not** track recency (INTERACTIEF had the lowest id but
   the latest kennisgeving).

5. **Extraction is noisy, two ways:**
   - Passage **count** is meaningless (gemini @ temp 0 splits the same facts into a varying number of
     bullets — 79 vs 199 items but comparable char volume). Failure is **step-2 packaging
     non-determinism, not step-1 parse, not lost content.**
   - But extraction also **drops real content** run-to-run: CAO 10 `_2` (latest plain-text edition)
     lost the **main adult wage tables** → 11 salary rows vs 17 for its siblings.
   - And per-field wobble: in **Pattern A (identical source)**, 58/211 non-salary fields still
     "differ" — boolean flips (`wage_entry_step_exp_present` F/T), unit rewordings (`%`/`percent`,
     `month`/`months`), even a number disagreeing (`term_probation_fixedterm_value` 2.0 vs 1.0).
     ⇒ **non-salary cross-edition variation is mostly extraction noise, not real change.**

6. **Real change is ~salary-only.** Structural non-salary fields are term-stable; real mid-term
   non-salary changes are rare and flagged by `general_updated_topics`.

7. **Content-equivalence metric.** char-**4-gram Jaccard** on whitespace-stripped text is the right
   measure (editorial 0.994 / 0.999; real versions 0.64 / 0.88). Binary page-hash **under-reports**
   (86% for an editorial pair — header-date+OCR jitter). Word-cosine is **useless** (~1.0 even for
   genuinely different editions). Pair Jaccard with a **numeric-token multiset diff** (`\d+[.,]\d+`)
   for the targeted "did money/% change" question (editorial diff 0-3; real diff 110-257).

---

## 2. Decisions

- **Keep only `full_cao_*`.** Drop partial/annex/supplement; **queue the 22 only-partial groups**
  for manual review.
- **Group** by `cao_number` + `ingangsdatum` (primary; 100% complete, term-level). `expiratiedatum`
  secondary (occasionally mis-scraped). **Do NOT group on `signing_date`.**
- **Order/select editions** by `datum_kennisgeving` (latest = most consolidated). **Never** use `id`
  or `signing_date` for recency.
- **No forced canonical pick in the data.** We tag editions (below) and leave the choice to analysis
  time: base = `kennisgeving_rank == 1`, updates = rank > 1. If a single term-level snapshot is needed,
  the analyst takes rank 1 (or the latest, or whichever fits) — not a baked-in column.
- **Completeness guard stays a *diagnostic* concept** = extraction richness (populated salary rows /
  non-empty fields), **not** char-size. Used to flag a thin edition (CAO 10 `_2`: 11 vs 17 rows), not to
  auto-select.
- **The equivalence verdict is NOT used for selection** — "take whichever edition you want by rank"
  doesn't need it. The 4gram-Jaccard + numeric-diff comparison is kept **only as a QA diagnostic**:
  where files that are near-identical in text (Jaccard≈1) produce **different output values**, flag that
  as an extraction-reliability problem.
- **De-dup approach = TAG, don't MERGE** (revised 2026-06-27 — replaces the earlier salary-union idea).
  Pooling wage rows across editions is fragile: the `(worker_type, jobgroup, step_label, effective_date)`
  keys often don't match across editions (different label wording; CAO 10 `_2` shared **zero** keys with
  its siblings). So instead of merging, **keep every edition's rows and add relating columns** to BOTH
  the non-salary and salary corrected tables.
- **`document_type` does NOT distinguish base from update within a term** — 71% of multi-file term-groups
  have uniform `document_type`, and even the mixed 28% are unreliable (the 2021-2022 quad was all
  `full_cao_original` though some were later updates). So **derive base/update from `kennisgeving` order**,
  not from `document_type`. (An `edition_role` column copied from `document_type` was dropped as redundant.)
  Columns to add (per source-file record) — **minimal set** (revised 2026-06-27):
  | column | meaning | how derived |
  |---|---|---|
  | `term_group` | links all editions of one term | `cao_number` + `ingangsdatum` |
  | `kennisgeving_rank` | 1 = base (earliest) … N = latest | order by `datum_kennisgeving` |
  | `base_id` | convenience pointer to the base file (derivable from `term_group`+rank — keep or drop) | `id` of the rank-1 record |
  base = `kennisgeving_rank == 1`; updates = rank > 1; all related via `term_group`. Nothing is lost or
  merged: a **wage timeline** pulls all editions of a `term_group`; **term-level snapshot** = take rank 1
  (or whichever edition the analyst wants). Merge moves to analysis time.
- **Dropped columns** (and why): `edition_role` (just repeats `document_type`, which doesn't separate
  base/update anyway); `is_base` (= `kennisgeving_rank == 1`); `is_canonical` (don't need a forced single
  snapshot); `is_salary_update` (a naive "amounts differ from base" boolean is **True almost everywhere** —
  even editorial reprints differ by OCR/rounding noise, so it's misleading). If a real
  "carries a mid-term wage round" signal is ever wanted, derive it as a **diagnostic** from
  *new wage-table effective dates vs base* (immune to amount jitter), NOT as a corrected-data column.

---

## 3. Build plan (ordered)

Targets: write the new columns into **`qa/corrected_dataset.csv`** (non-salary) and
**`salary/outputs/corrected_salary.csv`** (salary), via the aggregator — never hand-edit.

1. **Version-tagging builder** — compute and add `term_group`, `kennisgeving_rank`, `base_id` to both
   corrected tables.
   - Group on `cao_number` + `ingangsdatum`; keep only `full_cao_*`; rank editions by `datum_kennisgeving`
     (rank 1 = earliest = base). `base_id` = id of the rank-1 record (derived from kennisgeving order, NOT
     `document_type`).
   - Robust **file_name → record matching** so the salary and non-salary rows for one source file get
     the same tags (normalize trailing spaces, `.pdf`/`.docx`/`.pdf.pdf`, case/space).
2. **QA similarity diagnostic** (separate report, not part of selection) — per term-group, consecutive
   **4-gram-Jaccard + numeric-diff** (Dutch number normalization `1.234,56` ↔ `1234.56` first) to label
   editorial-vs-real, cross-checked against whether the **output values** differ. Surfaces
   extraction-reliability flags where similar input → divergent output.
3. **Manual-review queues** — (a) 22 only-partial groups; (b) extraction-reliability flags from step 2;
   (c) optional cross-CAO firm-name duplicate scan.
4. **Indices integration** (`indices/index_lib.py`) — make `add_active`/`within_year` consume
   `term_group` + `kennisgeving_rank` (e.g. dedup to one edition per term before standardising) instead
   of re-deriving from raw `ingangsdatum`; show **before/after** on a couple of index bins for sign-off.

**Prerequisite:** locate Hanna's already-run comparison output and confirm whether it tags at the
file level (one row per source file) so step 1 extends it rather than redoing it.

---

## 4. Open questions & difficulties

- **Hanna's existing comparison.** A version comparison was already run — need to find its output and
  whether it tags at file level, so the tagging builder extends it instead of duplicating it. *(blocker
  for step 1)*
- **Completeness guard, concretely.** Only relevant if a snapshot is chosen downstream. "Thin" needs a
  threshold — e.g. salary-row / non-empty-field count well below the term's max — to flag editions like
  CAO 10 `_2` (11 vs 17 rows). A diagnostic, not used to auto-select.
- **Term-level analysis carries single-extraction noise.** Taking rank 1 (or any single edition) gives
  one edition's noisy extraction. The repeated-measurement *consensus* idea is deferred — could be added
  later as a downstream step over the tagged editions if the noise matters for a given field.
- **`general_updated_topics` reliability.** LLM-derived; weak real-vs-noise signal if noisy. Spot-validate
  before relying on it to mark genuine mid-term changes.
- **`ingangsdatum` as the group key.** Verified constant within the 633 term, but need a dataset-wide
  check that it never *varies* inside a single real term (would wrongly split a `term_group`), and that
  `expiratiedatum` drift (e.g. 633 ingang 29/03/2009 had two expiry values) doesn't fragment groups.
- **Threshold robustness (diagnostic).** `4gram-Jaccard ≥ 0.98` / "numeric-diff ≈ 0" need validation
  across CAOs of different length / OCR quality before trusting the editorial-vs-real labels.
- **Salary as a timeline.** Each edition holds only its wage-table window; some editions extracted **0**
  wage years (e.g. CAO 633 05/11/2012). Tagging preserves all of it, but anyone building a wage timeline
  from a `term_group` must expect year-gaps and possibly re-extract/hand-fill specific `(cao, year)`
  tables. (Only relevant if a salary union is built downstream.)
- **Index standardisation axis.** `index_lib.within_year` bins by **`ingangsdatum`-year**, but wage
  values have their **own effective year**. For long terms (633: all 2011-2017 wages sit under
  ingangsdatum-year 2012) this mixes different actual-year wage levels into one bin — a methodological
  issue beyond dedup, worth raising separately.
- **Scope.** CSV = 242 CAOs / 2,739 rows vs QA scope 95 CAOs / 1,505 records — confirm which set the
  tagged outputs feed.
- **Cross-CAO duplicates.** 135 start dates shared across `cao_number`s, but almost all are Jan-1
  calendar coincidence, not true duplicates. Only a firm-name match would indicate a real cross-number
  dup — not yet investigated.
