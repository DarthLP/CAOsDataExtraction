# CAO Pipeline Summary

**End-to-End Extraction, Deterministic Salary Parsing, Multi-Layer Verification, and Derived Indices Pipeline**

*Replication Manual and Technical Documentation — September 2026*

---

# Introduction and Architecture Overview

All code, schemas, and configurations for this project are unified in a single reproducible repository at:

<https://github.com/DarthLP/CAOsDataExtraction>

## Project Scope and Objective

In the Netherlands, collective labour agreements (*Collectieve Arbeidsovereenkomsten*, CAOs) govern employment terms, salary scales, working hours, leave entitlements, pensions, and workplace conditions for the vast majority of employees. These legal instruments are published as PDF documents by the Ministry of Social Affairs and Employment (SZW) on the portal `uitvoeringarbeidsvoorwaardenwetgeving.nl`.

The objective of this project is to construct an auditable, deterministic, and empirically rigorous pipeline that:

1.  Scrapes and parses more than 1,500 historical and active CAO PDF documents;

2.  Extracts structured clauses across 13 distinct thematic domains (wages, pensions, leave, working hours, contract types, training, etc.);

3.  Parses multi-tier salary grids minimizing hallucination risk;

4.  Audits and corrects extraction errors through a multi-layer verification protocol;

5.  Derives standardised generosity indices (pooled-z and percentile tracks) and nominal wage ladders anchored to statutory minimum wage schedules;

6.  Produces a balanced monthly in-force panel workbook for empirical econometric research.

## The Unified Pipeline Layout

The project integrates three sequential stages under a unified layout (deterministic salary parsing as Stage 1b):

```text
Stage 1: Extraction (pipelines/)
  |-- p1_webscraping.py    -> Download PDFs from official SZW portal
  |-- p2_extract.py        -> Text extraction (PyPDF2 + Tesseract OCR)
  |-- p3_llmExtraction.py  -> First-pass Gemini extraction (13 topics)
  |-- p4_analysis.py       -> Schema-enforced Pydantic structured output
  \-- p5_excel_creation.py -> Flat tabular output (extracted_data_non_salary.csv)

Stage 1b: Deterministic Salary Parsing (salary_parser/)
  \-- deliver.py           -> Deterministic wage parsing (100% amount provenance)

Stage 2: Non-Salary Correction & Verification (verification_pipeline/qa/)
  \-- corrected_dataset.csv -> Final corrected dataset (152 regression tests in qa/)

Stage 3: Derived Indices & Panel Generation (verification_pipeline/indices/)
  \-- rebuild.sh           -> Pooled-z scores, wage index, monthly in-force panel
```

*End-to-end architecture of the unified CAO extraction and verification pipeline.*

## Canonical Artifacts

To prevent ambiguity regarding data lineage, Table 1 records the single canonical artifact for each stage of the pipeline.

| **Stage** | **Artifact Path** | **Format** | **Integrity / Verification Standard** |
|:---|:---|:--:|:---|
| Raw Input | `inputs/pdfs/input_pdfs/` | PDF | SZW government portal downloads. |
| Stage 1 (p3) | `outputs/llm_extracted/new_flow/` | JSON | English-translated topic text passages. |
| Stage 1 (p5) | `outputs/excel/new_results/extracted_data_non_salary.csv` | CSV | Flat tabular non-salary extracts from p4 Pydantic models. |
| Stage 1b | `outputs/parser_salary/extracted_data_salary_v2.csv` | CSV | 359,474 rows × 749 cols (delimiter “;”), 100% amount provenance, Tiers A+B (343,111 rows, 95.45%). |
| Stage 2 | `verification_pipeline/qa/corrected_dataset.csv` | CSV | Canonical corrected dataset: 2,739 rows × 318 cols, 242 unique CAOs (delimiter “;”), 34 audited layers (152 regression tests in `qa/`). |
| Stage 3 | `verification_pipeline/indices/out/composite_index.csv` | CSV | 2,698 rows × 112 cols (delimiter “;”), pooled-z and percentile ranks across 10 domains. |
| Stage 3 | `verification_pipeline/indices/out/mw_indices.csv` | CSV | 1,970 rows × 22 cols (delimiter “;”), statutory wage index series and nominal wage ladders. |
| Stage 3 | `verification_pipeline/indices/out/scoring_params.csv` | CSV | 89 parameter rows × 10 cols (delimiter “;”) covering 50 scored field variants. |

*Canonical artifacts across all pipeline stages.*

# Stage 1: Extraction Pipeline (Substages p1–p5)

The upstream extraction pipeline converts unstructured CAO PDF documents into structured JSON and tabular extracts. It operates across five sequential active substages in `pipelines/`: `p1_webscraping.py` → `p2_extract.py` → `p3_llmExtraction.py` → `p4_analysis.py` → `p5_excel_creation.py`.

## Substage 1: Web Scraping (`p1_webscraping.py`)

CAO agreements are downloaded from the official Dutch labour conditions portal (`uitvoeringarbeidsvoorwaardenwetgeving.nl`) using Selenium automation, guided by the agreement frequency register `inputs/excel/inputExcel/CAO_Frequencies_2014.xlsx`.

The scraping logic implements three specific selection criteria:

- **Link Parity Filter**: The portal presents document links in duplicate pairs; the scraper selects every second link (indices 0, 2, 4, …) to eliminate redundant downloads.

- **Multi-Part Concatenation**: If a single agreement filing consists of multiple PDF components (e.g. main text and annexes), the scraper combines them into a unified PDF file saved under the primary document title.

- **Directory Partitioning**: Documents are saved into CAO number folders (`inputs/pdfs/input_pdfs/[CAO_NUMBER]/`). The pair `(cao_number, filename)` serves as the immutable primary key for every document, with scraping logs preserved in `extracted_cao_info.csv`.

## Substage 2: PDF Parsing and OCR (`p2_extract.py`)

PDF parsing in `pipelines/p2_extract.py` uses a multi-method fallback hierarchy:

1.  Direct digital text extraction via `pdfplumber` and `PyPDF2`;

2.  Optical Character Recognition (OCR) via Tesseract for scanned image pages;

3.  Automated glyph and font repair, systematically resolving `/uniXXXX` and `/GXXX` font corruptions;

4.  Emits cleaned Markdown and structural JSON representations to `outputs/parsed_pdfs/[CAO_NUMBER]/`.

## Substage 3: First-Pass LLM Extraction (`p3_llmExtraction.py`)

Substage 3 invokes Google Gemini 2.5 Flash (`gemini-2.5-flash`) via `pipelines/p3_llmExtraction.py` to extract text passages across 13 distinct thematic topics:

- General metadata, Wage scales, Pensions, Bonuses, Termination;

- Leave, Overtime, Training;

- Home office, Contract types, Workplace safety, Childcare, AI regulations.

The output is saved as a per-document JSON file under `outputs/llm_extracted/new_flow/[CAO_NUMBER]/[FILE]_extract.json`. Crucially, upstream prompts translate the original Dutch legal text into English during this pass, establishing the English-language baseline used in subsequent QA stages.

#### Sampling Hyperparameters and the Retry Escalation Ladder

To maximize stability and reproducibility, the first attempt of every LLM invocation in `pipelines/p3_llmExtraction.py` enforces strict sampling parameters via `ExtractionConfig`:

```text
model = 'gemini-2.5-flash',  temperature = 0.0,  top_p = 0.1,  top_k = 1,
seed = 42,  max_tokens = 65536,  candidate_count = 1
```

With `presence_penalty = 0`, `frequency_penalty = 0`, and `thinking_budget = -1`, these settings lock the model into a deterministic decoding regime on the first attempt. However, these parameters are not held fixed across retries.[^1]

## Substage 4: Schema-Enforced Analysis (`p4_analysis.py`)

Substage 4 (`pipelines/p4_analysis.py`) validates and transforms the raw p3 topic passages into Pydantic models defined in `schema/non_salary_schema.py`:

- Non-salary fields are processed in three logical batches to manage context limits: (1) `gen_bon_wag_pen_ter`, (2) `lea_ove_tra`, and (3) `hom_con_saf_chi_ai_fri`.

- A lock-file mechanism prevents race conditions during multi-process execution.

- An exponential backoff retry ladder with error classification categorizes schema validation failures, token truncation errors, and API rate limits. As in Substage 3, this ladder escalates temperature and `top_p` on later tries (+0.1/+0.2/+0.3 by the 3rd/4th/5th+ try, up to `max_retries = 5`), so the deterministic settings above hold only for the first attempt.

#### Architectural Pivot: Retirement of the Stochastic Salary Ladder

In the initial pipeline design, `pipelines/p4_analysis.py` attempted to extract multi-tier salary grids using an LLM retry ladder spanning four schemas (`Salary­Extraction­Schema`, `Salary­Extraction­Schema­Compact`, `Salary­Extraction­Schema­Super­Compact`, and `salary_schema_split.py`). Comprehensive auditing revealed a high ($\approx 20\%$) major-issue error rate. So, this salary extraction was entirely excised from `p4_analysis.py` and migrated to the deterministic, rule-based parser in Substage 1b (`salary_parser/deliver.py`). Substage 4 was retained strictly for non-salary domain schemas.

## Substage 5: Excel and CSV Creation (`p5_excel_creation.py`)

Substage 5 (`pipelines/p5_excel_creation.py`) merges the validated non-salary JSON blocks with document metadata and dates from `extracted_cao_info.csv`, writing the flat tabular extracts to `outputs/excel/new_results/extracted_data_non_salary.csv`.

# Stage 1b: Deterministic Salary Parser

## Motivation: Replacing the Stochastic LLM Ladder

Extraction originally ran as two LLM passes. The first (`pipelines/p3_llmExtraction.py`) lifts *verbatim* text snippets and table grids out of each document; the second (`pipelines/p4_analysis.py`) forces those snippets into the Pydantic salary schema via a four-tier retry ladder. Empirical verification in `verification_pipeline/salary/wage_check_full/` localised the damage to the *second* pass: the first pass copies tables most of the time faithfully, whereas the schema-filling pass suffered an unacceptable $\approx 20\%$ major-issue rate. Typical failures included:

1.  **Token Exhaustion Truncation**: Large multi-grade salary matrices routinely exceeded model output limits (`max_tokens_truncated_1..4`), truncating tables mid-grid;

2.  **Hallucinations and Interpolations**: The LLM invented missing scale steps, annualized monthly wages without textual warrant, or interpolated missing increments;

3.  **European Decimal Confusion**: Inconsistent parsing of European period-thousands and comma-decimals (e.g. 2.185,50 vs 2,185.50) generated factor-of-1,000 magnitude errors;

4.  **Column Drift and Misalignment**: Attention drift across complex grids (e.g. youth steps on the left, tenure steps on the right, and scale grades in columns) misassigned amounts across job classifications.

To eliminate these vulnerabilities, only the second pass was replaced, by a dedicated, deterministic parsing engine located in `salary_parser/`. The parser re-extracts from the *first*-pass output (`outputs/llm_extracted/new_flow/`), not from the PDF-derived markdown: the faithfully copied table grids are kept, and only the stochastic schema-filling step is discarded.[^2]

## Single-Entrypoint Pipeline: `deliver.py`

The entire Stage 1b salary parsing and delivery workflow is executed via a single reproducible command:

```bash
python3 salary_parser/deliver.py
```

An optional `--quick` flag restricts execution to the first file of each CAO for rapid regression and metric verification. The pipeline executes six deterministic stages (with zero runtime LLM calls) plus stage 5.5 to re-apply audited merges:

1.  **Parse & Audit** (`run_all.py`): Parses the first file of each CAO from `outputs/llm_extracted/new_flow/[CAO_NUMBER]/[FILE]_extract.json` (`wage_information` table grids) and runs `source_audit.py`. An automated **Hard Abort Guard** immediately raises `SystemExit` if source amount provenance is below 100.00%.

2.  **Count Comparison Gate** (`count_gate.py`): Compares parser-extracted amounts against historical LLM counts per file as a soft anomaly flag.

3.  **Role Cross-Validation** (`role_check.py`): Validates job-group, step, and worker-type tokens against historical LLM extracted vocabulary, enriching worker classifications and increase percentages.

4.  **Confidence Roll-Up** (`confidence.py`): Evaluates structural integrity, assigns row-level confidence tiers (A, B, C, D), and groups CAOs into acceptance buckets (`auto_accept`, `needs_review`, `llm_fallback`).

5.  **Corpus-Wide Version File Parsing** (`all_files.py`): Extends parsing and role-checking across all $\approx 2,700$ CAO edition files in the repository, outputting `duplicate_files.csv` to diagnose identical filings.

6.  **Guarded Merge Re-Application** (`reapply_merges`): The guarded agent loops of §3.4 were run once, offline, and their audited results were stored as standalone patch sets rather than written back into the parser. Because Stage 5 rebuilds `rolecheck_all` from scratch and would otherwise discard them, Stage 5.5 replays those patches: label fixes first (`outputs/parser_salary/relabel_merged/` via `relabel_merge.py`), then whole-file agent extractions (`outputs/parser_salary/agent_extracted/` via `agent_extract.py`).

7.  **Wide CSV Export and Version Tagging** (`flatten_csv.py`): Flattens parsed JSON structures into the canonical wide dataset `outputs/parser_salary/extracted_data_salary_v2.csv` (359,474 rows × 749 columns, delimiter “;”). Joins document metadata, appends 7 confidence columns, appends 5 version-tag columns from `version_tag.py`, and writes run logs and JSON manifests to `outputs/parser_salary/runs/`.

## Core Invariants and Parsing Rules

The parser operates via *input inversion*: rather than asking an LLM to generate salary tables, the parser directly ingests the raw Markdown and HTML table grids extracted during Stage 3 (`pipelines/p3_llmExtraction.py`). The LLM’s role is demoted to a closed-choice classifier: it may only pick a label from a fixed, pre-enumerated vocabulary when a job-group header or period unit is ambiguous. It never sees or emits monetary values.

The parser enforces strict deterministic invariants documented in `salary_parser/PARSER_RULES.md`:

- **Amount Provenance Guarantee with Hard Abort Guard**: Every single emitted monetary amount must exist verbatim as a substring within the source text or table cell. The parser never converts, annualizes, multiplies, or infers numbers.[^3]

- **Header Date Grounding**: Effective wage dates (`salary_start_date`) are parsed strictly from table column headers, row-date keys, or table description titles. The parser strictly refuses to backfill missing table dates using document-level signing or publication dates (`ingangsdatum` or `datum_kennisgeving`). Rows where the header provides no date remain honestly null ($\approx 5.9\%$ of entries, by design).

- **Zero-Drop Invariant**: Table cells containing `0` or `0.00` indicate empty scale steps or structural placeholders.

- **Footnote Stripping**: Footnote markers attached to amounts (e.g. `2185.13*`, `1840,11`) are stripped, preserving the underlying numeric value verbatim.

- **Youth and Apprentice Table Exclusions**: Youth (*jeugdloon*), apprentice (*leerling*), and BBL tables are deliberately excluded via teen filters. Their omission is by design.

- **Small Ordinal Integers ($< 13$) and Junk Label Demotions**: Whole numbers $< 13$ in non-hourly tables represent ORBA points or scale indices, not wages; these are tagged `nonwage_smallint` and demoted to Tier D. Money-sized tokens ($\ge 800$) or clock artifacts (e.g. `3:00 AM`) appearing in jobgroup or step labels indicate cell fusion; these are tagged `junk_label`.

## Guarded Agent Loops

Where heuristic parsing requires agent review for complex layouts (e.g. tagged rows or tables, diagonal matrices or multi-section tables), two strictly guarded loops are deployed:

- **Label-Only Relabelling (Gates G1–G4)**: The agent may only assign standardised scale labels (e.g. job grade, step) to existing parsed rows without altering numbers.

- **Source-Number Universe Guard**: If an agent proposes a tabular re-extraction for complex layouts, every extracted figure is checked against the multiset of all numbers appearing in the source passage. In Wave 1 of testing, this guard rejected 2,007 candidate values that were hallucinated or calculated by the agent, ensuring zero false numbers entered the dataset.

## Version-Selection Tagging Engine (`version_tag.py`)

A few CAOs have multiple source files for the same collective agreement (the original filing plus mid-term integral text republications). To prevent double-counting across editions while retaining complete wage timelines, `salary_parser/version_tag.py` appends five version-selection columns to every row:

1.  `term_group`: Primary key composite `cao_number|ingangsdatum`, constant across all editions of an agreement term.

2.  `kennisgeving_rank`: Chronological edition rank ($1 = \text{earliest/base}, \dots, N = \text{latest}$) sorted by official notification date (`datum_kennisgeving`).

3.  `base_id`: Unique record identifier of the rank-1 base filing.

4.  `n_editions`: Total count of source files in the `term_group`.

5.  `document_type`: Joined document classification (e.g. `full_cao_...`), enabling downstream filtering of partial amendment deltas.

## Confidence Tiers and Live Dataset Distribution

Every parsed salary row is tagged with a confidence tier (A–D) reflecting structural completeness and validation checks. Table 2 reports the live counts and shares from `outputs/parser_salary/extracted_data_salary_v2.csv`.

| **Tier** | **Count** | **Share (%)** | **Definition and Use in Empirical Analysis** |
|:--:|---:|---:|:---|
| **A** | 254,334 | 70.75 | Perfect provenance, unambiguous unit, valid scale and step, complete header date. Primary canonical sample. |
| **B** | 88,777 | 24.70 | Minor header ambiguity or inferred periodic unit; passed all numeric validity bounds. Included in primary sample. |
| **C** | 13,693 | 3.81 | Flagged for review; missing full-time hours basis or ambiguous progression structure. Excluded from wage indices. |
| **D** | 2,670 | 0.74 | Severe layout collision, unresolvable units, or rejected by source guards. Excluded. |
| **Total** | **359,474** | **100.00** | **Tiers A + B Analytic Coverage: 95.45% (343,111 rows)** |

*Salary parser confidence tier distribution in the canonical dataset.*

The primary wage indices and descriptive analyses restrict the sample strictly to Tiers A and B, delivering an analytic dataset of 343,111 rows with 100% amount provenance.

# Stage 2: Non-Salary Correction and Verification

## Stage-0 Ingest and Canonical Artifact Architecture

Stage 2 of the pipeline systematically audits and corrects upstream non-salary extractions across 34 structured correction layers ($L_1 \to L_{34}$, corresponding to generations $G_0 \to G_{33}$ documented in `verification_pipeline/docs/DATA_LINEAGE.md`). The canonical dataset is:

`verification_pipeline/qa/corrected_dataset.csv`

This canonical table contains exactly **2,739 rows, 318 columns, and 242 unique collective agreements** (`cao_number`), formatted as semicolon-delimited (`;`) UTF-8 text. It reflects Generation $G_{33}$—the final cumulative state following thirty-four sequential verification layers over the raw upstream extract ($G_0$). All Stage 3 derived indices and downstream panel regressions consume this file directly.

The correction architecture strictly enforces data immutability through a four-stage state-transition protocol:

**apply ⟶ copy ⟶ verify ⟶ promote**

No correction is ever applied directly in place. All candidate modifications are captured in immutable changelogs, validated against programmatic integrity gates, and promoted only after passing comprehensive regression suites.

## The 34-Layer Correction Lineage (Generations G0–G33)

#### What every layer tests.

The unit under test is a single *cell*: the pair (record, field), where a record is one edition of one agreement and a field is one of the 318 schema columns. Across all 34 layers the adjudication question is invariant:

*Does this edition’s own source text state this value **explicitly**, under **this field’s** schema definition?*

Each cell receives one of three verdicts: `KEEP` (supported), `FIX` (unsupported—corrected to the stated value, or blanked / set `False`), or `CANT_TELL` (undecidable at the evidence tier available, escalated to the next tier).[^4] What varies between layers is therefore not the question but two operational choices: **which cells are nominated as suspect** (the detector), and **which evidence tier** the reader is permitted to consult.

#### The detectors.

Suspicion is generated by five mechanisms:

| **Detector** | **Suspicion logic** | **Layers** |
|:---|:---|:---|
| Deterministic rules | Cell-local violations: impossible percentages ($>100$), unit-family mismatch, year-like value in a duration field, article number captured as a value, decimal-point strip, placeholder evidence | $L_1$, $L_3$–$L_7$, $L_{29}$ |
| Cross-edition flip | A boolean or numeric changes between editions of one agreement; at most one side can be correct | $L_8$ |
| Same-term discrepancy | Two editions of the *same* term (a republication) disagree; a difference with no legal cause is an extraction error | $L_9$, $L_{10}$, $L_{20}$ |
| Index-jump anomaly | $\vert \Delta z\vert  \ge 0.5$ between editions, partitioned into tier A (same-term), tier B ($V/\Lambda$ sandwich: a value spikes and reverts, so the middle edition is the suspect), and tier C (one-way jumps, reverting or persisting) | $L_{14}$–$L_{28}$ |
| Family consistency | Records sharing (`cao_number`, `ingangsdatum`) should carry identical values | $L_{15}$–$L_{19}$, $L_{32}$ |

Two layers fall outside this scheme: $L_{23}$ re-extracts entire documents whose upstream parse was fabrication-riddled, and $L_{30} \to L_{31}$ re-audits by *error direction* after a stratified random sample revealed that errors were one-sided (§4.6).

#### What counts as an error.

The corrections resolve seven recurring defect classes: **phantom True** (a boolean asserting a provision the document never grants—the single largest class); **false absence** (a provision present in the document but absent from the extract); **field-boundary misrouting** (a genuine clause credited to the wrong field);[^5] **unit and denominator errors** (most consequentially, percent-of-premium stored as percent-of-salary, the 1,071-cell $L_{13}$ correction); **invented numbers** (derived or statutory-inferred figures appearing nowhere verbatim); **template shadowing** (an unfilled model-contract appendix read as an agreed term); and **corrupt parse** (whole extractions hallucinated from defective OCR).

Two things are explicitly *not* tested. The layers do not adjudicate the legal merit of an agreement’s provisions, and they never consult statutory baselines to *source* a value: a value matching its era’s statutory constant is a suspicion flag only (the standing fingerprint queue described in §4.6), never evidence for filling a cell.[^6]

The 34 layers were executed in four historical waves, summarised below by layer, generation range, and cells changed.

- **Wave 1 — per-topic QA and schema audits ($L_1$–$L_7$, $G_0 \to G_6$, ~9,200 cells).** Deterministic-rule and human review over 95 core agreements: 514 cells ($L_1$); 781 net cells from twice-verified holistic and definition-anchored review ($L_2$); 3,318 quote-backed per-file fixes ($L_3$); 2,332 verified absence removals ($L_4$—1,813 boolean demotions and 519 enum normalizations, with numerics carved out after a 100-cell pilot showed an 83% false-removal rate on numbers); and 2,105 cells adjudicated under six written conventions—base figure, surcharge increment, first tier, representation equality, strict enums, quote evidence ($L_5$; 12 subagents at 57/60 spot-check agreement, plus a human ruling sheet retiring 22 patterns). $L_6$–$L_7$ cleared 151 flagged patterns and a 40-cell residue queue.

- **Wave 2 — cross-edition and same-term consistency ($L_8$–$L_{13}$, $G_7 \to G_{12}$, ~4,800 cells).** 3,559 flip inconsistencies (3,549 boolean, 10 numeric) resolved by multi-reader majority vote against source text ($L_8$); 61 same-term numeric fixes surviving from 668 candidates after a second reader rejected half ($L_9$); 111 unifications, tie-breaks and full-passage recoveries, including 7 unit-blind clobbers reverted and all 30 residual ties broken ($L_{10}$); 36 date-collision discrepancies ($L_{11}$–$L_{12}$); and the 1,071-cell pension denominator blanking ($L_{13}$).

- **Wave 3 — jump campaigns, family sweeps, re-extraction, ripple closure ($L_{14}$–$L_{28}$, $G_{13} \to G_{27}$, ~5,900 cells).** Family, secondary-mover and topic sweeps ($L_{14}$–$L_{19}$; 77 family-consistency cells including the CAO 1022 childcare phantoms, 400 cells across 162 families, 14 ambiguous cells settled on text identity); the tier A same-term campaign with word-level quote provenance (902 cells over 645 jump events, $L_{20}$); 106 `CANT_TELL` cells escalated to full text and re-OCR ($L_{21}$); 22 human rulings on residual pay tiers and probation caps ($L_{22}$); full re-extraction of the two corrupt-parse agreements, CAO 3313004 and CAO 310005 (252 cells, $L_{23}$); the tier B sandwich (461 cells), tier C reverting (1,410) and tier C persistent (1,465) campaigns ($L_{24}$–$L_{26}$); and six ripple waves driving uncovered sibling sides to a fixed point ($662 \to 168 \rightarrow 44 \rightarrow 22 \rightarrow 11 \rightarrow 7 \rightarrow 0$; 713 cells, $L_{27}$–$L_{28}$).

- **Wave 4 — closure and restorative re-verification ($L_{29}$–$L_{34}$, $G_{28} \to G_{33}$, ~1,000 cells).** 274 post-campaign cells, including full-text closure on 435 `CANT_TELL` cells and a deterministic part-time screen over all 2,739 records ($L_{29}$); the 39-cell stratified audit with Opus arbitration of 10 disputes ($L_{30}$), whose directional finding forced re-verification of all 1,628 standing restorations and blank-to-value fills under strict field-boundary discipline, changing 516 cells—220 de-restorations, 155 numeric/blank fixes, 141 companion units ($L_{31}$); 132 remaining `CANT_TELL` cells closed by family consistency and markdown-tier full-text gates ($L_{32}$–$L_{33}$); and 37 follow-up flags, among them the CAO 759 quarterly cap, CAO 3768 AOW restatements, CAO 1214 December bonus unit relabeling and CAO 1029 maritime shift routing ($L_{34}$)—yielding the canonical $G_{33}$ dataset.

## Active Automated Regression Test Suite

To guarantee ongoing pipeline integrity and prevent regressions across schema revisions, the repository maintains an active automated regression test suite that re-checks the correction layers code’s logic:

`cd verification_pipeline && python3 -m pytest qa -q`

The suite executes **152 automated regression tests** across 13 test modules, running cleanly in under 5 seconds with zero failures. The test battery is partitioned into two specialized directories:

1.  **Full Audit Integrity Tests** (`verification_pipeline/qa/full_audit/tests/`, 59 tests): Validates multi-edition agreement consistency (`test_agreement_consistency.py`, 10 tests), cross-version difference tracking (`test_cross_version.py`, 11 tests), enum string canonicalization against schema rules (`test_enum_format.py`, 15 tests), numeric distribution bounds and plausibility clamps (`test_outliers_numeric.py`, 7 tests), unit family semantic compatibility (`test_unit_semantics.py`, 9 tests), and value-unit contamination separation (`test_value_unit_contamination.py`, 7 tests).

2.  **Shared Component and Invariant Tests** (`verification_pipeline/qa/shared/tests/`, 93 tests): Validates winner-selection logic in rule arbitration (`test_aggregator.py`, 14 tests), 18 data invariant rules covering whitespace, placeholder leakage, and unparseable units (`test_audit.py`, 29 tests), CSV delimiter splitting recovery and quote repair (`test_csv_recovery.py`, 7 tests), statutory floors and caps across historical legislation eras (`test_era_baselines.py`, 21 tests), section slicing and topic mapping in source loaders (`test_source_text_loader.py`, 13 tests), Dutch spelling variants and decimal comma handling (`test_value_variants.py`, 4 tests), and worksheet context token budgets (`test_worksheet_builder.py`, 5 tests).

## Worksheet Modes and Context Budgets

Verification tasks are partitioned into subagent worksheets managed by `worksheet_builder.py`. Each item is presented in one of three modes:

- **Extract**: Field is currently empty; subagent searches source text for explicit numbers.

- **Blind**: A deterministic rule flagged the cell, but the proposed value is hidden; the subagent inspects source text independently.

- **Informed**: The subagent is presented with the existing value and the proposed change as a hypothesis to test skeptically.

Worksheet chunking enforces hard token caps: $\le 3{,}500$ static system prompt tokens, 2,000–4,000 tokens per item context, and $\le 50{,}000$ total chunk context (15–20 items per chunk).

## Multi-Agent Verification Protocol

To eliminate single-agent hallucination and bias, corrections follow a three-tier arbitration chain:

1.  **Proposer**: Generates candidate corrections backed by verbatim quotes from the English source text.

2.  **Skeptical Second Reader**: An independent subagent tasked specifically with attempting to falsify the proposed correction.

3.  **Opus Arbiter**: High-capacity model (`claude-opus`) acting as final judge on disputed cases or complex legal clauses (especially in pensions and termination).

## Methodological Lessons

Three failure modes recurred often enough to change the protocol:

- **Extract-silence is not document-silence.** Judging a cell from extracted snippets alone produced a $\approx 97\%$ false-absence rate: the reader saw absence in the snippet, not in the document. No verdict may rest below the full-document tier.

- **Definition-less drift.** Without descriptions in the prompt, $\approx 22\%$ of proposed corrections erred in the same way—payment percentages routed into duration fields, statutory references read as agreed terms.

- **Deletion bias.** An unconstrained reader treats a non-standard unit as an error. Preservation-aware rules kept 269 of 308 unit-semantics cells (87% `KEEP`).

#### Residual State of the Canonical Dataset

Three figures describe where the corpus stands at $G_{33}$:

- **Log fidelity.** The $L_{30}$ mechanical audit replayed every logged change against the pre-$L_{20}$ backup: zero replay mismatches, zero unlogged changes, total churn 0.9%.

- **Noise level.** The boolean flip rate — how often a single yes/no field disagrees between two printings of the same agreement (`boolean_flip_rate.py`) — stands at a mean of 3.3%, down from 9.1% before the jump campaigns; the residue is individually verified draft-versus-final difference.

- **Open queues.** 28 cells the full text cannot settle remain in `indices/review/open_cant_tells_for_hanna.csv` as a human decision queue, and roughly 1,150 statutory-fingerprint suspects have never been agent-checked — the standing worklist for the blind spot above, where an error replicated identically across every edition triggers no disagreement detector.

# Stage 3: Derived Generosity and Wage Indices

Stage 3 of the pipeline converts the verified non-salary dataset (`corrected_dataset.csv`) and the deterministic salary dataset (`extracted_data_salary_v2.csv`) into standardised generosity indices, statutory-adjusted scores, multivariate factor structures, and a continuous monthly panel. This section details the rebuild orchestration, the architectural evolution, parameter registry mechanics, statutory adjustments, wage indexation, monthly panel integration rules, and empirical multivariate properties.

## Architecture of the Active Rebuild Pipeline

The derived indices and monthly panel generation are orchestrated end-to-end by the shell script `verification_pipeline/indices/rebuild.sh`. Executed under strict error trapping (`set -e` and `set -o pipefail`), the script enforces the sequential execution of 21 core Python scripts and validation routines:

1.  **Thirteen Topic Index Drivers**: Thirteen dedicated topic scripts independently compute unit-normalised values, apply statutory boundaries, forward-fill intra-term values, and standardise scores against persisted parameters:

    - `bonus_index.py`: Scores 2 numeric fields (13th-month bonus, fixed lump-sum payment) and 8 presence booleans.

    - `pension_index.py`: Scores 4 numeric fields (employee contribution rate, accrual rate, franchise amount, early retirement age) and 5 presence booleans under strictly available-case rules.

    - `term_index.py`: Scores 6 numeric fields (employer notice, notice maximum, probation in fixed-term contracts, probation in indefinite contracts, notice floor, severance pay) and 3 booleans.

    - `overtime_index.py`: Scores 9 numeric fields (standard premium, maximum premium, shift allowance minimum, unfavourable hours premium, daily trigger, weekly trigger, minimum rest between shifts, maximum weekly hours, maximum daily hours) and 0 booleans.

    - `training_index.py`: Scores 2 cardinal fields (yearly training time, cost reimbursement) alongside stratified training budgets (`eur`, `pct_salary`, `pct_wagesum`) and 3 booleans.

    - `homeoffice_index.py`: Scores 1 numeric field (monthly stipend value) and 3 presence booleans.

    - `contract_index.py`: Scores 4 numeric fields (ketenregeling maximum successive contracts, maximum chain duration, full-time weekly hours, tenure requirement for working hours adjustment) and 3 booleans.

    - `safety_index.py`: Coverage-only module evaluating 12 occupational safety and health (PSA, PMO/PAGO, confidential counsellor) booleans.

    - `childcare_index.py`: Coverage-only module evaluating 4 employer-provided childcare and allowance booleans.

    - `ai_index.py`: Coverage-only module tracking 3 artificial intelligence and algorithmic management governance booleans.

    - `fringe_index.py`: Scores 3 numeric fields (commuting allowance per km, meal stipend, relocation reimbursement) and 8 presence booleans.

    - `absence_index.py`: Scores 8 numeric fields (statutory and extra vacation days, holiday allowance bonus, sick pay continuation percentage, sick pay duration, short-term care days and pay, long-term care weeks and pay) and 3 booleans.

    - `parental_leave_index.py`: Computes full-rate equivalent (FRE) weeks across maternity, paternity, adoption, and parental leave (`fre_total_extracted`, `fre_total_with_statutory`) alongside 12 family leave generosity booleans.

2.  **Wage Track Engine** (`mw_indices.py`): Ingests the deterministic salary dataset (`extracted_data_salary_v2.csv`, Tiers A and B), aggregates by `(cao_number, calendar_year)`, derives scale percentiles (p10 to p90), merges the statutory minimum wage series (`wml_timeline.csv`), and writes `out/mw_indices.csv` (1,970 rows × 22 columns).

3.  **Statutory Legal Floor Index** (`statutory_index.py`): Constructs monthly pseudo-records (1999 to present) from `out/statutory_all.csv` and `out/wml_timeline.csv`, scoring prevailing statutory floors on the identical pooled parameters to create the benchmark pseudo-agreement `STATUTORY`.

4.  **Cross-Topic Composite Builder** (`composite_index.py`): Consolidates topic z-scores and wage dimensions into document-level composite metrics (`overall_numeric_z`, `overall_z_var`, `coverage_overall`), auditing thin documents against `review/thin_docs_reviewed.csv` and emitting `out/composite_index.csv` (2,698 rows × 112 columns).

5.  **Monthly Panel Generator** (`build_panel_monthly.py`): Constructs the balanced calendar-month panel, executing the in-force “round-up” allocation, dynamic law-month re-scoring, and annual wage broadcasting, outputting `out/all_indices_panel_monthly.csv`, `out/all_indices_panel_yearly.csv`, and `out/panel_aggregates_monthly.csv`.

6.  **Multivariate Factor Analysis** (`advanced_analysis.py`): Executes Kaiser-Meyer-Olkin (KMO) tests, Bartlett tests of sphericity, within-topic factor analyses, PCA decompositions, and factor score extractions, outputting detailed diagnostics to `ADVANCED_ANALYSIS.md` and associated CSVs (see §5.8).

7.  **Agreement-Level Export Engine** (`build_cao_level_export.py`): Structures document records into bargaining agreement terms (`term_group` = `cao_number` × `ingangsdatum`) with edition ordering (`term_edition_seq`, `n_editions_in_term`, `is_last_filed_in_term`) and two ready-made start-date axes: `Date_first_is_Ingangsdatum`, where a term’s first file starts at its `ingangsdatum` and later files at their own `file_date`; and `Date_retro_datum`, the same axis overridden by `retro_start_date` wherever the text states an explicit retroactivity clause. The export carries **no wage columns**, and its roll-ups correspondingly exclude wage and are suffixed `_without_wage`: a wage ladder is a CAO × calendar-year fact whose year may pool scales from several files, so attaching it to a single document row would import partly later-filed information into that row. The wage-inclusive `overall_z` lives in the monthly panel instead; the two roll-ups measure different things and are not comparable (`out/cao_agreement_level.csv`).

8.  **Consolidated Multi-Tab Workbook Engine** (`build_combined.py`): Merges all topic columns, composite indices, panel tables, and codebook legends into a styled multi-tab openpyxl workbook (`all_indices.xlsx`) and combined flat CSV (`out/all_indices_combined.csv`).

9.  **Automated Verification Battery** (`check_battery.py`): Executes a 6-stage validation suite auditing unit conversion math, Dutch institutional sanity bounds, salary unit-class plausibility, percentile monotonicity, statutory fingerprints, and sibling same-term difference coverage.

| **Step** | **Script** | **Primary Inputs** | **Key Generated Outputs** |
|:---|:---|:---|:---|
| 1–13 | Topic Drivers (`*_index.py`) | `corrected_dataset.csv`, `statutory_all.csv` | `out/<topic>_index.csv`, diagnostic CSVs |
| 14 | `mw_indices.py` | `extracted_data_salary_v2.csv`, `wml_timeline.csv` | `out/mw_indices.csv`, `out/mw_by_worker_type.csv` |
| 15 | `statutory_index.py` | `statutory_all.csv`, `scoring_params.csv` | `out/statutory_index.csv` |
| 16 | `composite_index.py` | Topic CSVs, `mw_indices.csv`, `thin_docs_reviewed.csv` | `out/composite_index.csv` |
| 17 | `build_panel_monthly.py` | `composite_index.csv`, `mw_indices.csv`, `statutory_all.csv` | `all_indices_panel_monthly.csv`, `panel_aggregates_monthly.csv` |
| 18 | `advanced_analysis.py` | `composite_index.csv` | `ADVANCED_ANALYSIS.md`, `out/factor_*.csv` |
| 19 | `build_cao_level_export.py` | `composite_index.csv`, `corrected_dataset.csv` | `out/cao_agreement_level.csv` |
| 20 | `build_combined.py` | All topic outputs, panel files | `all_indices.xlsx`, `all_indices_combined.csv` |
| 21 | `check_battery.py` | Canonical CSVs, normaliser library | -stage validation report |

*Stage 3 Rebuild Pipeline Orchestration (`rebuild.sh`)*

## Core Design Choices of the Active (v2) Architecture

Four decisions, specified in `INDICES_V2_PLAN.md`, govern how scores are constructed. (Lessons learned from an experimental v1 prototype which is fully superseded.)

#### 1. Pooled-Z Standardisation

Every continuous indicator receives a single **pooled-z score** per field, standardised against the grand pool of all full-CAO documents across all years. A 2005 agreement and a 2025 agreement are thus measured on the identical yardstick, preserving secular bargaining trends and macroeconomic level shifts.

#### 2. Term-Group Deduplicated Yardstick

Dutch CAOs are frequently amended and republished (median 11 full-text editions per agreement), so counting every edition equally would let reprint-heavy sectors dominate the scale. Winsorisation thresholds (1st and 99th percentiles), $\mu$, and $\sigma$ are therefore computed on exactly one winning edition per `(cao_number, ingangsdatum)` term group—the edition with the latest publication date (`datum_kennisgeving`). These parameters are then frozen, persisted to `scoring_params.csv`, and used to score *all* documents, so that mid-term amendments and the autonomous `STATUTORY` pseudo-record share one unbiased yardstick.

#### 3. The Publication Date Clock (`datum_kennisgeving`)

The chronological axis is `file_date = datum_kennisgeving`, the official government notification date (populated in 99.96% of documents; falling back to `ingangsdatum` on exactly one record, ID 1564 / CAO 1017). Because `ingangsdatum` is identical across all republications within a bargaining cycle, indexing on it would collapse mid-term wage rounds onto the original start date; the notification clock instead enters each amended text in the calendar month it became legally binding.

#### 4. Law-Month Re-Scoring

Statutory-anchored provisions in the monthly panel are re-evaluated against the legal era prevailing in each panel month. When national legislation moves a statutory minimum (the WWZ in July 2015, the WAB in January 2020, the WIEG paternity leave expansions in 2019–2020), every active in-force agreement immediately reflects the new floor, so the measured quantity remains the negotiated premium over the law as the law shifts.

## The Pooled-Z Generosity Formula and Parameter Registry

Numeric cardinal indicators are transformed through a strict five-step standardisation sequence:
$$
\begin{aligned}
\text{Raw Cardinal } x_{it} 
&\xrightarrow{\text{1. Canonicalise}} x'_{it} \\
&\xrightarrow{\text{2. Winsorise (1/99)}} x''_{it} = \max(W_{j,0.01}, \min(W_{j,0.99}, x'_{it})) \\
&\xrightarrow{\text{3. Standardise}} z^*_{it} = \frac{x''_{it} - \mu_{j,\text{pool}}}{\sigma_{j,\text{pool}}} \\
&\xrightarrow{\text{4. Clip}} z_{it} = \max(-3, \min(3, z^*_{it})) \\
&\xrightarrow{\text{5. Worker Direction}} \tilde{z}_{ijt} = S_j \cdot z_{it}
\end{aligned}
$$
where:

- $x'_{it}$ represents the value converted into the topic’s canonical unit (e.g., standard workweek hours, gross monthly euros, or annual vacation days) via `index_lib.normalize()`;

- $W_{j,0.01}$ and $W_{j,0.99}$ are the 1st and 99th empirical percentiles of field $j$, computed exclusively on the term-group deduplicated sample;

- $\mu_{j,\text{pool}}$ and $\sigma_{j,\text{pool}}$ are the empirical mean and standard deviation of field $j$ computed on the winsorised deduplicated sample;

- $S_j \in \{+1, -1\}$ is the domain sign indicating whether higher values represent greater generosity from the worker’s perspective (e.g., $+1$ for vacation days, commuting allowances, and severance pay; $-1$ for probation duration, weekly working hours, employee pension contributions, and ketenregeling limits).

#### Why Winsorise and Clip

Both steps cap, not discard, extreme values, and guard against different failure modes. Per-field yardstick samples are small (as few as $n=10$) and extraction-derived, so a single unit-conversion error or genuinely unusual clause can dominate an otherwise thin sample: winsorising (step 2) bounds each value’s contribution *before* it enters $\mu_{j,\text{pool}}$ and $\sigma_{j,\text{pool}}$, preventing one outlier from inflating $\sigma$ and compressing every other document’s score toward zero. Clipping (step 4) then bounds the resulting z-score itself, since a value just inside the winsor bounds can still standardise past $\pm3$ when $\sigma$ is small.

#### The Persisted Parameter Registry (`scoring_params.csv`)

To guarantee reproducibility and allow external datasets (including statutory baselines) to be evaluated on the identical yardstick, all standardisation parameters are persisted in `verification_pipeline/indices/out/scoring_params.csv`. The registry contains **exactly 89 parameter rows** across 10 structured columns (`topic;field;variant;canonical;sign;winsor_lo;winsor_hi;mu;sd;n`):

- **78 rows for 39 Scored Fields × 2 Variants**: Each of the 39 scored cardinal fields is registered in both its `full` (statutory-adjusted and forward-filled) and `raw` (unimputed baseline) variants:

  - Absence: 8 fields (16 parameter rows);

  - Bonus: 2 fields (4 parameter rows);

  - Contract: 4 fields (8 parameter rows);

  - Fringe: 3 fields (6 parameter rows);

  - Homeoffice: 1 field (2 parameter rows);

  - Overtime: 9 fields (18 parameter rows);

  - Pension: 4 fields (8 parameter rows);

  - Term: 6 fields (12 parameter rows);

  - Training: 2 fields (4 parameter rows).

- **3 Stratified Training Budget Rows** (`strat` variant): Covers units in euros (`training_budget_value[eur]`), percentage of individual salary (`training_budget_value[pct_salary]`), and percentage of total wage sum (`training_budget_value[pct_wagesum]`).

- **5 Leave Full-Rate Equivalent (FRE) Rows**: 4 in the `full` variant (`fre_total_with_statutory`, `paternity_fre_stat`, `parental_fre_stat`, `adoption_fre_stat`) and 1 in the `raw` variant (`fre_total_extracted`).

- **3 Wage Scale Dimensions** (`full` variant): Covers `mw_mean`, `mw_median`, and the wage compression metric `mw_span_pct`.

The sum yields exactly $78 + 3 + 5 + 3 = 89$ registered parameter rows.

## The Two Tracks: Magnitude and Coverage

To prevent conflating qualitative institutional mentions with quantitative generosity levels, Stage 3 maintains two parallel measurement tracks:

- **Magnitude Track ($z$ and $gen01$)**: Measures the quantitative generosity of continuous provisions (days, hours, euros, percentages). In addition to the unbounded z-score, each topic driver generates an empirical percentile rank ($gen01 \in [0, 1]$). Ranks need neither winsorisation nor clipping, so an outlier simply takes rank $\approx 1$ and cannot dominate a mean.

- **Coverage Track**: Aggregates binary presence indicators within each topic into a bounded coverage proportion ($coverage \in [0, 1]$), accompanied by its standardised score ($coverage\_z$). Topics lacking cardinal fields (such as `safety`, `childcare`, and `ai`) operate exclusively on the coverage track.

#### Aggregation Across Fields and Topics

Both tracks aggregate available-case: a field the document does not populate is skipped, never read as zero, so a document is not penalised for a benefit it simply did not quantify. Writing $F_t$ for the populated field set of topic $t$ and $\tilde{z}_{ijt}$ for the oriented field score of the five-step sequence above,
$$
\begin{aligned}
M_t &= \frac{1}{|F_t|}\sum_{j \in F_t} \tilde{z}_{ijt},
\qquad
Z_t = \tfrac{1}{2}\left( M^{z}_{t} + C^{z}_{t} \right),
\\[2pt]
\text{overall} &= \frac{1}{|T|}\sum_{t \in T} Z_t,
\qquad
\text{overall variance} = \operatorname{Var}_{t \in T}\left( Z_t \right),
\end{aligned}
$$
where $M^{z}_{t}$ is the standardised magnitude track (`<topic>_numeric_z`), $C^{z}_{t}$ the standardised coverage track (`<topic>_coverage_z`), and $Z_t$ the combined per-topic headline (`<topic>_z`). A topic scores as missing only where the document populates none of its fields, and the variance is defined only where at least two topics score. The overall roll-ups carry the `_without_wage` suffix at document grain (§5.1); the numeric-only companion is the same mean taken over $M^{z}_{t}$ alone.

#### Choosing a Scale

The two scales order agreements almost identically — the correlation between `overall_numeric_z` and `overall_numeric_pctile` is 0.810 (Pearson) and 0.801 (Spearman) — but not exactly, and the gap is precisely the asymmetric-range fields. Use $z$ for factor analysis and $\sigma$-distance statements; use the percentile track for bounded, symmetric, equal-influence rankings.

## Statutory Floors, Autonomous Pseudo-CAO, and the Pension Rule

#### Statutory Minimum Adjustments

Under Dutch labour law, collective agreements may improve upon statutory employment conditions, but cannot derogate below mandatory legal protections. Each statutory-linked field carries a role in `out/statutory_all.csv` fixing how the legal value interacts with the extracted cell (`index_lib.py`):

- `floor` / `default`: a blank cell is filled with the statutory value prevailing in that legal era (e.g., 20 days of statutory annual leave; 70% statutory sick pay; statutory ketenregeling limits); a stated value is kept as extracted.

- `floor_lift`: as above, *and* a stated value below the statutory minimum is lifted to it — the one-sided mandatory right overrides the text.

- `cap`: blanks stay blank; a stated value above the legal maximum is masked to missing rather than scored.

- `informational`: held for reference and never used to fill or bound a cell.

Roles are assigned per field and per validity window, so every document is adjusted against the law of its own date.

#### Zero-Fill of Optional Extras

Statutory floors do not reach genuinely optional benefits (13th-month bonus; meal, relocation and commuting allowances; the home-office stipend; training time and budget), where a blank may mean either “not granted” or “granted but unquantified”. `apply_zerofill()` in `index_lib.py` converts such a blank into a rankable $0$ only under a double gate: the benefit’s presence boolean is `False` in *every* edition of that CAO, *and* the raw cell was never stated in any unit in any edition of that CAO. If either condition fails the cell stays missing and available-case — in particular, a benefit stated in a unit the normaliser cannot convert is present-but-unmeasurable, not absent. The pre-zero-fill score is retained alongside as `<topic>_numeric_z_availcase`, so the rule’s effect on any topic is always recoverable.

#### The Autonomous Statutory Pseudo-CAO (`STATUTORY`)

Rather than treating statutory law merely as an internal imputation constant, `statutory_index.py` scores the statutory legal floor as an autonomous pseudo-agreement with `cao_number = 'STATUTORY'`. Evaluated across all continuous calendar months from January 1999 to the present using the identical pooled parameters from `scoring_params.csv`, the statutory pseudo-agreement provides a living baseline directly on the common yardstick. For perks where the law provides zero entitlement (e.g., 13th-month bonuses, meal allowances, travel reimbursements), the statutory score is zero. Consequently, comparing an in-force CAO against the statutory line immediately isolates the negotiated bargaining premium over statutory law.

#### The Pension Hard Rule (Strictly Available-Case Only)

> **Mandatory Invariant:** The pension topic is **strictly available-case only**. Blank pension entries are NEVER imputed with zero, statutory baselines, or cross-sectional averages.

In the Netherlands, collective pension schemes are typically administered by mandatory industry-wide pension funds (*bedrijfstakpensioenfondsen*, BPFs) rather than negotiated inside individual CAO texts. An agreement lacking explicit pension text almost invariably participates in a sector fund rather than providing zero retirement coverage. Zero-filling or statutory imputing blank pension entries would manufacture severe synthetic variation based entirely on document reporting scope rather than true employee benefits.

## The Wage Track Engine

Wage indexation is executed by `verification_pipeline/indices/mw_indices.py`, built directly on the deterministic salary parser output (`extracted_data_salary_v2.csv`):

- **Analytical Sample Filter**: Incorporates only high-confidence Tier A and Tier B records (343,111 rows, representing 95.45% of the parser dataset). Lower-confidence Tiers C and D are excluded.

- **Adult Full-Time Normalisation**: Filters out youth scales and unresolvable hourly rates lacking an explicit workweek basis. Periodic amounts are normalised to monthly gross full-time equivalent euros using standard full-time hours conversions ($4\text{-week} \times 13/12$, $\text{weekly} \times 52/12$, $\text{hourly} \times \text{workweek} \times 52/12$).

- **Year Grain and Edition Precedence**: A wage row is keyed to the calendar year of the wage table’s *own* effective date as printed in the CAO (`salary_N_start_date`), not to `file_date` and not to `ingangsdatum`, so the series already sits on its natural retroactive axis and a single document typically contributes tables to several years. Overlaps resolve in two stages: within a key of (CAO, effective date, job cell, unit) the latest-filed edition’s rows win; and under **term precedence** a wage point whose effective date falls after a *successor* term has taken effect is dropped as a stale pre-announced table (logged to `out/mw_stale_forward_dropped.csv`), while retroactive points dated before their own term’s `ingangsdatum` are never stale. A year therefore appears in `mw_indices.csv` only where a text states a scale for it; the carry across gap years happens at the panel join (§5.7).

- **Scale Percentiles and WML Ratios**: For each `(cao_number, calendar_year)`, the engine calculates scale percentiles representing the wage ladder distribution: `mw_low` (p10), `mw_q25`, `mw_median`, `mw_mean`, `mw_q75`, `mw_high` (p90), and wage dispersion `mw_span_pct` = $(\text{p90} - \text{p10})/\text{p10}$. Linking the adult statutory minimum wage timeline (`wml_timeline.csv`, 1999–2026), the engine derives real policy ratios: `ratio_low_wml`, `ratio_median_wml`, and `ratio_mean_wml`.

- **Nominal Scale Justification**: Wage z-scores (`wage_median_z`, `wage_mean_z`) are standardised on nominal gross monthly euros rather than WML-deflated ratios. Normalising against WML at index time would conflate statutory government policy interventions with negotiated collective bargaining gains.

## Monthly Panel Integration Rules

The balanced panel generation script `build_panel_monthly.py` explodes the document-level composite index across continuous calendar months under strict institutional rules:

1.  **Panel Grain and Time Scope**: The panel constructs a continuous monthly series at the `cao_number` × `month` (YYYY-MM) grain, initiating at the calendar month of each CAO’s earliest observed publication date and extending through the active build month. The autonomous `STATUTORY` pseudo-CAO is joined across all months.

2.  **The In-Force “Round-Up” Allocation Rule**: For any calendar month $m$, the agreement in force is defined as the document possessing the latest `file_date` (`datum_kennisgeving`) $\le$ the final calendar day of month $m$. Under this rule, if a new agreement is published on any day within month $m$ (even the 28th or 30th), the entirety of month $m$ is attributed to the newly published agreement. Same-month document collisions are resolved in favour of the later publication date; same-day ties are resolved by extraction completeness (count of populated non-null fields), followed by higher numeric document ID.

3.  **Forward-Fill and Legal Nawerking**: When a Dutch collective labour agreement (cao) expires, provisions that have become part of an individual employment contract may continue to govern that contract through nawerking, until they are replaced or amended by a valid individual or collective agreement. The monthly panel operationalises this principle by forward-filling the scores of the last in-force agreement across subsequent months until a newer edition is registered. To safeguard empirical analyses, agreements that remain without a successor for more than 48 months are tagged with the indicator `is_stale = True`.

4.  **Dynamic Law-Month Re-Scoring**: While unanchored bargained provisions remain constant throughout a document’s tenure, statutory-anchored components (family leave, statutory vacation and sick pay floors, ketenregeling limits, and ATW working hours constraints) are dynamically re-scored against the legal era of each specific panel month. When national statutory reforms take effect, every active in-force CAO immediately reflects the updated statutory baseline. Family leave is scored per type (paternity, adoption, parental; maternity excluded as statutorily constant), matching the cross-sectional composite; the legacy summed-FRE total is retained only as an unscored reference column (`leave_fre_total_monthly`).

5.  **Thin-Document Score Carryover**: Certain published editions represent procedural annexes, wage protocol updates, or appendix adjustments misclassified as `full_cao` documents while containing $<80\%$ field coverage. When human or agent audit confirms a thin document (`review/thin_docs_reviewed.csv`), the monthly panel carries forward the non-salary scores from the preceding substantive agreement edition via the tracking column `score_src_id`. The carry is visible per row — `thin_doc` marks the edition and `scores_carried_from` names the document whose scores it wears — so any analysis can exclude carried rows outright. No candidate is ever carried automatically: the registry holds 12 confirmed thin editions alongside 3 that a reader source-verified as genuine scope or content differences and which are explicitly protected from carrying, and an unreviewed candidate is scored at face value until confirmed against the full text.

6.  **Annual Wage Broadcasting**: Because wage scales are agreed on multi-year or annual cycles rather than monthly increments, the annual wage metrics from `mw_indices.csv` are broadcast across the calendar months of each year using backward as-of merges. Internal gap years between wage rounds are forward-filled under the doctrine of wage maintenance.

The monthly panel engine emits three core datasets: `out/all_indices_panel_monthly.csv` (the primary micro-panel), `out/all_indices_panel_yearly.csv` (a convenient December year-end snapshot), and `out/panel_aggregates_monthly.csv` (cross-CAO monthly landscape averages, standard deviations, and percentiles).

## Multivariate Statistical Findings and Dimensionality

The multivariate structure of the derived indices is systematically evaluated in `verification_pipeline/indices/advanced_analysis.py`, which outputs its formal findings to `ADVANCED_ANALYSIS.md` (referenced historically as `out/factor_summary.txt`):

#### Sampling Adequacy and Bartlett’s Test

Factorability diagnostics evaluated on the matrix of documents scoring at least 8 topics ($N = 2,444$ documents) reveal:

- **Kaiser-Meyer-Olkin (KMO) Measure**: **0.64** (specifically 0.6396). In psychometric and econometric factor analysis, a KMO in the 0.60–0.70 range is classified as mediocre common variance.

- **Bartlett’s Test of Sphericity**: $\chi^2 \approx 2201.35$, $\text{df} = 55$, $p \approx 0.0$ ($p < 10^{-10}$).

#### Substantive Economic Interpretation

While Bartlett’s test strongly rejects the null hypothesis that topic correlation matrices are identity matrices ($p \approx 0$), the modest KMO measure of 0.64 delivers a crucial empirical finding: **collective bargaining generosity cannot be collapsed into a single unidimensional construct**. Bargaining provisions share limited common variance across distinct domains; an agreement offering exceptionally generous parental leave or training budgets does not systematically offer high early retirement provisions, expansive severance floors, or shorter working weeks. Generosity is multi-faceted and domain-specific. This empirical finding rigorously validates the pipeline’s core architectural decision: rather than substituting an artificial, single latent factor score from principal component analysis, the pipeline publishes both the domain-specific topic z-scores and an **equal-weight available-case composite index** (`overall_numeric_z` and `overall_z`) alongside its cross-topic variance (`overall_z_var`).

#### Topic-Wage Orthogonality

Correlating each topic’s generosity score with scale wage levels (`wage_median_z`) across the $N = 2,444$ document sample demonstrates weak linear and rank associations (Pearson correlations ranging from $-0.07$ to $+0.36$). Non-salary generosity does not serve as a direct substitute for lower wages (the compensating differentials hypothesis does not dominate collective bargaining in the aggregate), nor do high-paying CAOs uniformly dominate all non-wage benefits. Wage levels therefore carry distinct economic information.

# End-to-End Replication and Verification

This section provides the complete operational protocol for replicating all pipeline outputs, data artifacts, and analytical reports from a clean repository clone.

## Environment Setup and Prerequisites

The pipeline executes on POSIX-compliant environments (macOS or Linux) running Python 3.13.0+. Core Python packages and LLM dependencies are installed via:

```bash
# Install core Python requirements and Google GenAI SDK
pip install -r requirements.txt
pip install git-filter-repo google-genai
```

Every script on the replication path below resolves its paths dynamically, relative to the repository root or its own enclosing directory (e.g. `cd "$(dirname "$0")"` in `rebuild.sh`), so none of them hardcodes a workstation-specific path.

## Downstream Reports Architecture

Downstream dissemination is partitioned into two specialized report suites within the `Reports/` directory:

1.  **Technical Replication Manual** (`Reports/Pipeline/`):

    - **Master Document**: `Reports/Pipeline/report/CAOPipeline.tex`, compiled to `CAOPipeline.pdf` via `latexmk -pdf`.

    - **Scope**: Documents the end-to-end extraction methodology, the deterministic salary parser architecture, the 34-layer non-salary correction framework (G0→G33), the pooled-z scoring formulas, the monthly panel in-force rules, and automated verification batteries.

    - **Structure**: Modular LaTeX fragments organized under `sections/01_intro.tex` through `sections/07_appendices.tex`.

2.  **Descriptive Summary Report** (`Reports/Analysis/`):

    - **Master Orchestrator**: `Reports/Analysis/scripts/run_all.sh`.

    - **Master Document**: `Reports/Analysis/main.tex`, compiled via `xelatex` (two passes) to generate `main.pdf`.

    - **Sub-Analysis Pipeline**:

      1.  `01_general.py`: Extracts CAO contract counts, date timelines, and sectoral coverage distributions.

      2.  `02_non_salary.py`: Summarises non-salary provisions and quantifies the empirical impact of the 34 QA layers.

      3.  `03_salary.py`: Ingests the deterministic salary parser dataset (`extracted_data_salary_v2.csv`, Tiers A+B) to regenerate all wage percentiles, dispersion curves, and wage growth series.

      4.  `04_indices.py`: Computes composite generosity ranking tables, factor loading summaries, and cross-topic correlations.

      5.  `xelatex`: Typesets the final publication document.

    - **Macro Traceability Guarantee**: Every inline numerical claim in the text is bound to an auto-generated macro in `tables/macros.tex`, and all summary tables are directly input from `tables/tab_*.tex`. The descriptive report contains strictly zero hand-typed figures.

## Sequential End-to-End Replication Protocol

From the repository root (`CAOsDataExtraction/`), execute the stages in the following sequential order:

1.  **Stage 1: Upstream Text Extraction (Optional if Cached)** Raw extraction outputs from Dutch government PDFs are version-controlled and archived. If executing from raw PDF files, run the active five-stage pipeline:

    ```bash
    python pipelines/p1_webscraping.py
    python pipelines/p2_extract.py
    python pipelines/p3_llmExtraction.py
    python pipelines/p4_analysis.py
    python pipelines/p5_excel_creation.py
    ```

    Stage 3 utilizes Gemini 2.5 Flash under frozen hyperparameters (`temperature=0.0`, `top_p=0.1`, `top_k=1`, `seed=42`, `max_tokens=65536`).

2.  **Stage 1b: Deterministic Salary Parsing** Execute the deterministic salary parsing engine:

    ```bash
    python salary_parser/deliver.py
    ```

    Transforms the unstructured LLM wage tables into `outputs/parser_salary/extracted_data_salary_v2.csv` (359,474 rows across 749 columns), enforcing 100% amount provenance and assigning confidence Tiers A–D.

3.  **Stage 2: Verification and QA Regression Suite** Execute the automated test suite across the 34-layer correction framework:

    ```bash
    cd verification_pipeline
    python3 -m pytest qa -q
    cd ..
    ```

    Verifies that all 152 automated regression tests pass with zero failures, confirming the structural integrity and historical consistency of the canonical dataset `verification_pipeline/qa/corrected_dataset.csv`.

4.  **Stage 3: Derived Indices Rebuild and Validation Battery** Execute the master indices rebuild script, which concludes by running the validation battery:

    ```bash
    cd verification_pipeline/indices
    ./rebuild.sh
    cd ../..
    ```

    This sequentially regenerates:

    - All 13 topic generosity indices (`out/<topic>_index.csv`);

    - The nominal wage index and WML ratio metrics (`out/mw_indices.csv`);

    - The autonomous monthly statutory floor pseudo-CAO (`out/statutory_index.csv`);

    - The cross-topic composite dataset (`out/composite_index.csv`);

    - The balanced monthly in-force panel (`out/all_indices_panel_monthly.csv`);

    - Multivariate factor analysis models and diagnostics (`ADVANCED_ANALYSIS.md`);

    - Agreement-level term export (`out/cao_agreement_level.csv`);

    - Consolidated multi-tab workbook (`all_indices.xlsx`).

    The script concludes by running `check_battery.py`, confirming 100% passage across all 6 validation stages (converter self-test, field sanity, salary unit class, percentile monotonicity, statutory fingerprints, and same-term sibling coverage).

## Determinism vs. Stochasticity Guarantees

The pipeline establishes a rigorous boundary between stochastic extraction and deterministic downstream synthesis:

- **Stochastic Extraction with Deterministic Preservation (Stages 1 and 2)**: Upstream LLM reasoning and agent adjudications naturally exhibit run-to-run variation. This is controlled by deterministic regex pre-filtering, Pydantic schema validation, and complete changelog recording. Once verified, edits are preserved in `verification_pipeline/qa/corrected_dataset.csv` and never regenerated in place.

- **Strict Mathematical Determinism (Stage 1b, Stage 3, and downstream report scripts)**: The salary parser, topic index engines, statutory scoring, monthly panel generator, and downstream LaTeX summary scripts are pure, deterministic Python algorithms. Holding the upstream canonical datasets constant, executing `salary_parser/deliver.py`, `rebuild.sh`, and `run_all.sh` is **guaranteed to produce byte-identical numerical outputs, CSV tables, and macro definitions**.

# Prompts and Extraction Schemas

Prompt templates and Pydantic schemas are version-controlled in the repository:

- **Non-Salary Prompt & Schema**: `schema/non_salary_schema.py`. A compact markdown export is available at `outputs/llm_extracted/excel_test/all_sections/NON_SALARY_PROMPTS_AND_SCHEMA.md`.

- **Salary Schema**: `schema/salary_schema.py` and `salary_parser/PARSER_RULES.md`.

- **Correction Schema**: `verification_pipeline/qa/CORRECTIONS_SCHEMA.md`.

# LLM Model Tiering and Calibration

Model selection follows a validated capability-cost hierarchy:

1.  **Gemini 2.5 Flash (`gemini-2.5-flash`)**: Ingest extraction (p3) and initial schema analysis (p4). High context capacity (1M tokens) enables ingesting full multi-page CAO chapters.

2.  **Claude Haiku**: Default model for routine subagent review worksheets and boolean presence checks. Fast, low cost.

3.  **Claude Sonnet**: Calibrated for complex multi-condition verification. In testing, Haiku was equally safe but 54% over-cautious when judging snippet absences; Sonnet was selected for all snippet-to-full-text arbitration.

4.  **Claude Opus**: Standing arbiter for pension clause interpretations, statutory chain-rule (*ketenregeling*) deviations, and termination legal disputes.

# Worked Example: End-to-End CAO Tracking

To illustrate the transformations applied by the pipeline, consider CAO 1022 (Secondary Vocational Education, *MBO*):

1.  **Raw Document**: The base collective agreement is `inputs/pdfs/input_pdfs_non_extra/1022/CAO_MBO_2018_2020_in_Word.pdf` (document ID 1022011); a separate 2020–2021 amendment agreement in the same directory carries no salary tables of its own.

2.  **p3 Extraction**: Text sections and salary-table markdown are extracted to JSON in `outputs/llm_extracted/new_flow/1022/`.

3.  **Deterministic Salary Parser**: Document 1022011 parses to 273 salary rows across 30 scales and 14 scale steps, 97.4% Tier A confidence (266 A, 7 C). Across all 14 salary-bearing documents for CAO 1022, 3,682 rows were extracted at 96.0% Tier A+B coverage.

4.  **QA Layer 16 Verification**: Layer 16 (`L16_dip_family_consistency`) corrected record 1022009’s `childcare_childcare_support_present` from `True` to `False`: the upstream extractor had read a general employer-policy intention clause as an explicit provision.

5.  **Derived Generosity Indices**: CAO 1022’s directly stated 30.0 days of annual vacation leave standardises to $\text{leave\_numeric\_z} = -0.7389$ against the cross-CAO distribution, feeding into the record’s overall composite score.

[^1]: `ExtractionConfig` allows up to `max_retries = 8`, and `get_adjusted_parameters()` raises temperature and `top_p` on later tries: the 4th try runs at `temperature = 0.1, top_p = 0.2`, the 5th at `temperature = 0.2, top_p = 0.3`, and the 6th and 8th tries switch to split salary/non-salary extraction (at the base and `+0.1` settings respectively) with per-half caching and a merge step. From the 3rd try onward, failure-aware guidance text is also appended to the prompt when the prior attempt was truncated or empty, so the prompt itself varies across retries. Transient API errors (503s, per-minute 429s) retry the same attempt index and so do not perturb parameters. Escalation only fires after a failed attempt, so the large majority of documents are extracted at `temperature = 0.0`; the attempt index and adjusted parameters used for each file are recorded in its log, keeping the escalated minority auditable.

[^2]: Although unreliable on the numbers themselves, this exploratory LLM pass was not wasted: it surfaced the structural vocabulary of the source tables — how job-group and worker/function labels are named, how age-cohort and tenure steps are laid out, and how scale headers vary across CAOs — which directly informed the column- and key-classification rules later hand-coded in `salary_parser/PARSER_RULES.md`.

[^3]: **The CAO 592 2-Cell Exception (Mangled-Decimal Rescue)**: In the entire corpus of 359,474 parsed salary rows, exactly one documented numerical derivation exists. Upstream OCR occasionally dropped a comma in Dutch four-decimal amounts (e.g. `2.184,54` became `2.18454`). Regex `MANGLED_DEC` detects single-dot numbers with 4–6 decimal places; a decimal shift is permitted only if the raw value sits $\ge 50\times$ below the table’s clean median and the rescaled value falls within $[0.25, 4.0] \times \text{median}$. This fires on **exactly 2 cells in CAO 592**. Rescued cells are tagged `value_source=rescaled` and capped at Tier B.

[^4]: Two rules bound the verdict space. First, the grounding doctrine: if the document is silent, or says “statutory” without naming a figure, the cell is left empty—values are never derived, annualized, summed, or inferred from legislation. Second, the evidence tier: verification reads keyword snippets, then the full topic extract, then the parsed full-text markdown, then re-OCR’d PDF text, and a higher tier always outranks a lower one. Snippet-only reading alone produced a $\approx 97\%$ false-absence rate (§4.6), so extract-silence is never treated as document-silence.

[^5]: Confirmed systemic mis-routings include *ploegentoeslag* (shift allowance) booked as a job allowance, *calamiteitenverlof* (emergency leave) as care leave, absence registration as workload monitoring, and clauses restricted to niche groups (youth, apprentices, 55+, AOW-age) credited to the typical worker.

[^6]: This also delimits the residual risk. Four of the five detectors require cross-record *disagreement* in order to fire, so an error replicated identically across every edition of an agreement is invisible to all of them—the structural motivation for the standing cross-sectional fingerprint queue described in §4.6.
