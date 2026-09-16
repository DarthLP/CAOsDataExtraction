# New Material Source Map

Mapping between repository documentation, source files, and the sections of the expanded pipeline replication guide.

---

## 1. Chapter and Section Mapping

| New Guide Section | Primary Content / Focus | Repo Sources Feeds |
|---|---|---|
| **Part I · Introduction & Extraction Architecture** | Overview, whole-chain map, canonical artifacts, p1–p5 pipeline stages | `README.md`, `CLAUDE.md`, `MasterSystemPrompt.md`, `docs/EXTRACTION_GUIDE.md`, `pipelines/README.md` |
| **Part II · Deterministic Salary Parser** | Replacement of 4-tier LLM ladder, input inversion, 100% provenance guarantee, confidence tiers A–D, deliver pipeline | `salary_parser/README.md`, `salary_parser/DELIVERY.md`, `salary_parser/PARSER_RULES.md`, `salary_parser/NEXT_STEPS.md`, `verification_pipeline/salary/SALARY_QA_MEMO.md` |
| **Part III · Non-Salary Correction & Verification** | Stage-0 ingest, 34 layers (G0→G33), verification protocol (propose→skeptical review→Opus arbiter), gates A1–A18, methodology lessons | `verification_pipeline/qa/PLAN.md`, `verification_pipeline/qa/conventions/general_conventions.md`, `verification_pipeline/qa/DATASETS.md`, `verification_pipeline/docs/DATA_LINEAGE.md`, `verification_pipeline/docs/DECISIONS.md`, `verification_pipeline/qa/full_audit/` |
| **Part IV · Derived Generosity & Wage Indices** | Two clocks, pooled-z formula, winsorisation, statutory floor, pension available-case rule, nominal wage index, monthly panel | `verification_pipeline/indices/METHODOLOGY.md`, `verification_pipeline/indices/INDICES_OVERVIEW.md`, `verification_pipeline/indices/INDICES_V2_PLAN.md`, `verification_pipeline/indices/ADVANCED_ANALYSIS.md`, `verification_pipeline/indices/NAMING.md` |
| **Part V · End-to-End Replication & Integrity** | Exact execution order, environment configuration, determinism guarantees, battery check | `rebuild.sh`, `salary_parser/deliver.py`, `verification_pipeline/indices/check_battery.py`, root `README.md` |
| **Appendices A–G** | Prompts, schemas, model tiering & parameters, error/truncation/layer inventories, worked CAO example, column dictionary | `schema/non_salary_schema.py`, `schema/salary_schema.py`, `verification_pipeline/qa/CORRECTIONS_SCHEMA.md`, `verification_pipeline/indices/DATA_GUIDE_FOR_ANALYSIS.md` |

---

## 2. Key Methodological Lessons to Feature

- **Snippet-vs-full-source verification**: Extract snippets caused ~97% false absence verdicts; full source text is necessary for verification.
- **Definition-less drift**: Judging values without providing the original Pydantic field definition produced ~22% error rate in proposed corrections.
- **Asymmetry in checker blind spots**: Dual checkers shared blind spots on missing provisions (restore direction 5/11 vs remove 0/14).
- **Pension available-case rule**: Mandate that pension blanks are strictly excluded from imputation to prevent synthetic generosity inflation.

