# Dutch Bargaining Agreements — CAO Data QA & Indices

QA pipeline and analytical indices for Dutch **Collective Labour Agreements (CAOs)**.
It takes an upstream LLM extraction of ~2,739 CAO records, corrects it against the
English-translated source texts, and derives per-topic "generosity" indices.
Owner: Hanna (hannaw.econ@gmail.com).

> **New here? Read in this order:** this file → [docs/DATA_LINEAGE.md](docs/DATA_LINEAGE.md)
> (which dataset is real, what's in it, what's pending) → [docs/DECISIONS.md](docs/DECISIONS.md)
> (why the pipeline works the way it does) → [qa/PLAN.md](qa/PLAN.md) (architecture) →
> [qa/conventions/general_conventions.md](qa/conventions/general_conventions.md) (rules every subagent follows).

---

## The pipeline: copy → correct → index

```
UPSTREAM (separate project, NOT in this repo)
  /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/
    outputs/excel/new_results/extracted_data_non_salary.csv
        │  copy (read-only)
        ▼
STAGE 0 · INGEST   inputs/extracted_data_non_salary.csv       2,739 records × 317 cols · 242 CAOs
                   inputs/by_topic/*_information.md            English-translated source text
        │
        ▼
STAGE 1 · CORRECT  Deterministic (schema rules + presence scan + enum)  ─┐
  per topic ×13    LLM subagents (verify/extract vs source)              ─┤→ qa/qa_<topic>/outputs/corrections.csv
                   Aggregator + guard chain + audit (A1–A18)             ─┘
                        │
                        ├─ APPLIED  → qa/corrected_dataset.csv   (see docs/DATA_LINEAGE.md)
                        └─ Cross-cutting verification (full_audit, per-file, agreement, holistic)
                                    → surfaced for review; high-confidence subset applied
        │
        ▼
STAGE 2 · INDEX    indices/  → 13 topic indices + composite + in-force yearly panel
                   (built on qa/corrected_dataset.csv)

SEPARATE TRACK     salary/ → corrected_salary.csv (salary scales; distinct dataset)
```

**Guiding principles** (full rationale in [docs/DECISIONS.md](docs/DECISIONS.md)):
catch false-negatives (missed provisions), deterministic where possible + LLM only for prose,
**surface never auto-fix**, never invent values, never feed the model statutory numbers.

---

## Where things live

| Path | What |
|---|---|
| `inputs/` | **Read-only** raw extract + per-topic English source texts. Stage 0. |
| `qa/` | Stage 1 correction engine. |
| `qa/shared/` | Topic-agnostic reusable code (aggregator, audit, slicer, …). |
| `qa/conventions/` | Rules + failure-mode catalogue every subagent reads. |
| `qa/qa_<topic>/` | Per-topic runs (13 topics): scripts, worksheets, `outputs/corrections.csv`, memo, run.log. |
| `qa/corrected_dataset.csv` | **The canonical corrected dataset** — see [docs/DATA_LINEAGE.md](docs/DATA_LINEAGE.md) + [qa/DATASETS.md](qa/DATASETS.md). |
| `qa/full_audit/` | Cross-cutting dataset-internal QA + per-file conflict verification (mostly surface-only). |
| `indices/` | Stage 2 derived indices (top level). Read `qa/corrected_dataset.csv`. |
| `salary/` | Separate salary-scale QA track (top level). |
| `reference/qa_leave/` | Frozen leave **reference implementation** — do not modify. |
| `docs/` | Cross-cutting docs: DATA_LINEAGE, DECISIONS, plans; `docs/archive/` = history. |

## Current state (2026-07-15)

- **All 13 topics** QA'd end-to-end. Canonical dataset = `qa/corrected_dataset.csv` (**G33**) = raw +
  **thirty-four audited correction layers** (~12k corrected cells, every change quote-grounded; full
  ledger: [docs/DATA_LINEAGE.md](docs/DATA_LINEAGE.md), per-change log:
  `indices/corrections/corrections_log.xlsx`).
- **Indices v2 built on it**: per-topic + overall z/percentile scores, cao×month panel, statutory
  pseudo-CAO, agreement-level export for the admin environment — workbook `indices/all_indices.xlsx`,
  plain-language guide `ReadMe_for_Hanna.md`, technical guide `indices/DATA_GUIDE_FOR_ANALYSIS.md`.
- **QA end state**: same-term coverage invariant 0 uncovered, jump census 1 verified genuine drop,
  standing battery green (`indices/check_battery.py` via `indices/rebuild.sh`).
- **What's left:** 30 open judgment cells (`indices/review/all_open_cant_tells.csv`), ~1,150
  statutory-fingerprint suspects (holistic-pass queue), two schema-boundary rulings for Hanna —
  see **[docs/ROADMAP.md](docs/ROADMAP.md)**.

## Environment

- Python `python3.13` (3.13.0, pip 24.2, no PEP 668). `pandas` + `qa/shared/resilient_csv.py`.
- CSV delimiter: `;` (semicolon), UTF-8-BOM. Key column: `id`.
- Tests: `python3.13 -m pytest qa -q` (152 passing).
- Rebuild indices: see [indices/INDICES_OVERVIEW.md](indices/INDICES_OVERVIEW.md) §Files (drivers + build order).
