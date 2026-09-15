# Agreement-consistency QA layer (surface-only)

Dataset: `corrected_dataset.csv` — 2739 records, 1562 agreements (cao x ingangsdatum), 653 with >=2 records.

Structured fields measured: 263 (numeric/unit/enum/boolean; freetext notes & dates excluded).

## Regression metric (watch this across every future correction pass)

- **within-agreement consistency = 83.28%** (identical 73.67% + back-fillable gap)
- a correct edit should move this UP; a pass that lowers it is suspect.

## (agreement x structured-field) breakdown

- identical across all versions : 81553 (73.7%)
- completeness GAP (back-fill)  : 10637 (9.6%) -> 17033 blank cells fillable
- CONFLICT (>=2 distinct)       : 18515 (16.7%) -> 2631 high-priority

## Back-fill candidates by confidence

- high (thin re-filing inherits a fuller original): 7572
- medium: 3065

### back-fill blank cells by topic

| topic | cells |
|---|---:|
| overtime | 4084 |
| leave | 3650 |
| term | 2771 |
| contract | 1761 |
| pension | 1728 |
| training | 1225 |
| fringe | 770 |
| bonus | 673 |
| general | 245 |
| childcare | 77 |
| homeoffice | 42 |
| ai | 7 |

## Conflicts by triage (routing for Stage 3)

| triage | n | meaning |
|---|---:|---|
| AMBIGUOUS_2REC | 1730 | 1-vs-1, needs source/definition |
| LIKELY_EXTRACTION_ERROR | 901 | >=3 recs, non-monotonic -> fix |
| LIKELY_TEMPORAL_CHANGE | 1125 | clean step over time -> keep both |
| BOOLEAN_DISAGREEMENT | 8699 | True vs False across versions |
| UNIT_WORDING | 6060 | same period, different spelling -> not real |

## Conflicts by topic

| topic | conflicts |
|---|---:|
| leave | 5019 |
| overtime | 2966 |
| safety | 1693 |
| term | 1665 |
| pension | 1427 |
| fringe | 1278 |
| contract | 1204 |
| training | 1142 |
| bonus | 992 |
| general | 411 |
| wage | 396 |
| homeoffice | 242 |
| childcare | 72 |
| ai | 8 |

Artifacts: `agreement_index.csv`, `backfill_candidates.csv`, `agreement_conflicts.csv`. Nothing applied — surface-only.