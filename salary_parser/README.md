# salary_parser — deterministic salary-table parser + role-check merge

Deterministic replacement/augmentation for the **salary half of `pipelines/p4_analysis.py`**.
Instead of asking an LLM to re-read the wage tables, it **parses the first-LLM extract grid**
(`outputs/llm_extracted/new_flow/<cao>/*_extract.json`, key `wage_information`) directly into
the salary schema, then cross-validates the labels against the old LLM output
(`outputs/llm_analysis/salary/`).

Owner: Hanna. Graduated from prototype 2026-07-03. Design docs:
`~/Desktop/salary_parser_results_v4/PLAN_next_phases.md` + `PLAN_phase3_rolecheck.md`.

## Guarantees / principles
- **100% amount provenance** — every emitted amount exists verbatim as a cell in the source
  grid; the parser NEVER invents, converts or annualizes a number. Statutory-only cells
  (`WML`, `nntb`) emit nothing.
- **Dates** only from column headers / table titles / date row-axes — never the document date.
  Undated points stay null (~6%, by design).
- **Labels**: parser's own grid tokens. The old LLM is consulted only for its field
  *vocabularies* (role-check), never for per-amount label assignments (those carry its
  misplacement bug). Re-roling permutes the parser's own tokens between fields.
- **worker / inc_pct**: table- or date-level attributes only (constant across the matched
  old subset / title regex).
- Cross-block timeline merge is scoped per **table family** (date-stripped desc) so different
  populations never share a row.
- Apprentice/`leerling`/aanloop-`start` tables and youth rows are excluded by design
  (entry scales in main tables are kept).

## Files
| file | role |
|---|---|
| `paths.py` | central path resolution (run from any CWD) |
| `salary_parser.py` | the deterministic parser (grid -> SalaryRow dicts + table diagnostics) |
| `source_audit.py` | source-grounded provenance + coverage audit |
| `run_all.py` | corpus harness: parse all CAOs, aggregate audit |
| `old_norm.py` | old-LLM normalizer (3 schemas + empties) + `file_vocab()` |
| `count_gate.py` | per-file parser-vs-old amount counts (SOFT flag; Haiku adjudicates) |
| `role_check.py` | label role cross-validation + worker/inc_pct enrichment |
| `confidence.py` | Phase-5 roll-up: row tiers A-D + CAO buckets (auto_accept / needs_review / llm_fallback) |
| `quality_checks.py` | free deterministic classified-coverage + gross-monotonicity checks |
| `all_files.py` | Phase-6-lite: run role-check over ALL version files + duplicate-file diagnostic |
| `flatten_csv.py` | Phase-9: flatten to wide `extracted_data_salary_v2.csv` + confidence columns |
| `run_parser_pipeline.py` | first-file entry point (4 stages); full delivery = all_files.py + flatten_csv.py |
| `DELIVERY.md` | delivered-dataset description + known-limitations |

## Run
```bash
python3 salary_parser/run_parser_pipeline.py          # full corpus (3 stages)
python3 salary_parser/run_parser_pipeline.py 51 1618  # verbose role-check on specific CAOs
```
Output -> `outputs/parser_salary/` (`run_all.json`, `count_gate.json`, `rolecheck/<cao>.json`).
Existing pipeline outputs are never modified.

## Current state (2026-07-08)
**359,474 rows, A+B = 95.4%** (A 254,334 · B 88,777 · C 13,693 · D 2,670), 100% amount+date
provenance. Built by: deterministic parser → guarded haiku_relabel (9.3k rows) → guarded
agent-extraction incl. per-table chunking (23k rows). Rebuild: `python3 salary_parser/deliver.py`
(re-applies all LLM merges). Wage index: `indices/mw_indices.py`, 214 CAOs. See `DELIVERY.md`
for the pipeline and `NEXT_STEPS.md` for open items. The snapshot below is the historical
graduation baseline (pre-agent-extraction), kept for provenance.

## State at graduation (2026-07-03, 242 CAOs, first-producing file per CAO)
221 CAOs producing · 51,513 points · amount provenance 100% · date provenance 100% of dated
points · coverage 76.3% · rows: 21,112 CONFIRMED (75.7%) / 1,336 RE_ROLED / ~3,750 flagged
(soft≫hard) / 1,684 no-old-LLM · worker filled 4,222 · verified by 13 Haiku ground-truth
audits across 2 fix iterations.

Phase-5 confidence roll-up: row tiers A=19,371 / B=4,407 / C=2,912 / D=1,192
(A+B = 85.3%); CAO buckets **150 auto_accept / 23 needs_review / 48 llm_fallback**
(of 221 producing; the other 21 CAOs have no salary tables). Tier logic: role-verdict
base + downgrades for `_dup_date`, hard junk-label tables, and min>max violations;
auto_accept requires >=95% A+B and zero D rows (Hanna's strict bar).

## Known limitations / next phases
- Per-file only: version selection / cross-version timelines (one canonical record set per
  CAO) not built yet (Phase 6), then confidence roll-up (Phase 5), ≥95% Haiku gate (Phase 7).
- `429`-class multi-table blocks (no `Columns:` markers) still parse to 0; ~77 mangled-header
  tables are hard-flagged (`jobgroup tokens unrecognized`).
- `14,835`-style comma-3-decimals is ambiguous (hourly decimal vs thousands) — undecidable
  without column context; handled by flagging, not guessing.
