# Dutch Bargaining Agreements (CAOs) — Developer & Agent Guidelines

Unified data extraction, deterministic salary parsing, correction/QA, and derived indices pipeline for Dutch Collective Labour Agreements (Collectieve Arbeidsovereenkomsten / CAOs).

---

## 1. Directory Structure & Documentation Map

- `conf/config.yaml` — Upstream extraction pipeline configuration and credentials.
- `pipelines/` — Extraction pipeline stages (p1–p5). Pointers: [`pipelines/README.md`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/pipelines/README.md).
- `salary_parser/` — Deterministic salary parser replacing LLM salary extraction. See [`salary_parser/README.md`](file:///Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/salary_parser/README.md).
- `schema/` — Pydantic schemas for structured extraction (`non_salary_schema.py`, `salary_schema.py`).
- `verification_pipeline/` — QA correction layers, audit suites, and indices engine:
  - `verification_pipeline/qa/` — 34 audited correction layers (G0→G33).
    `qa/_archive/` holds retired scripts (never run them; see its README).
  - `verification_pipeline/indices/` — Derived indices, pooled-z scores, statutory floors, monthly panel.
  - `verification_pipeline/salary/` — Wage-scale anomaly checks and verification.
  - `verification_pipeline/repo_paths.py` — Centralized relative path definitions.
- `Reports/Pipeline/` — Comprehensive LaTeX replication guide and audit memos.
- `Reports/Analysis/` — CAO Descriptive Summary report (`scripts/run_all.sh` regenerates
  figures/tables from `verification_pipeline/qa` + `indices` + the salary parser output, then
  builds `main.pdf`). Moved here from `verification_pipeline/Reports/Analysis/` on 2026-09-16;
  its `scripts/common.py` roots at `verification_pipeline/` via `repo_paths.VERIFICATION_ROOT`,
  not by relative `parent.parent` math.
- `docs/EXTRACTION_GUIDE.md` — In-depth guide to extraction stages p1–p5.

---

## 2. Hard Rules (Do Not Violate)

1. **Protected Inputs & Reference Data**:
   - NEVER modify files in `inputs/`, `verification_pipeline/inputs/`, or `verification_pipeline/reference/qa_leave/`.
   - Never edit `corrections.csv` directly — always generate or update via the aggregator scripts.
   - Enforced automatically by `.claude/hooks/protect-paths.sh` and permission rules.

2. **Never Invent Values (The Grounding Doctrine)**:
   - Only write values that the source text states EXPLICITLY.
   - If the source is silent → leave the field empty.
   - If the source says "statutory" without a specific number → leave empty; do not convert, annualize, or infer numbers.
   - Record contextual facts in review notes, never by hallucinating cell values.

3. **Pension Imputation Hard Rule**:
   - The pension topic is **strictly available-case only**. Never impute blank pension cells with statutory baselines or zero-fills.

4. **Preservation-First Dataset Updates**:
   - `verification_pipeline/qa/corrected_dataset.csv` is CANONICAL. Never edit it in place. Always follow `apply → copy → verify → promote`.

5. **Source Language Discipline**:
   - Source text in `outputs/llm_extracted/new_flow/` and `verification_pipeline/inputs/by_topic/` is English-translated.
   - Keyword sets in `qa/shared/field_keywords.py` must remain ENGLISH-PRIMARY.
   - Retain Dutch terms ONLY when they survive translation verbatim (statutory acronyms like AOW, WAB, WWZ, ATW, WAZO; fund names like ABP, BPF, PFZW; legal concepts like ketenregeling).

---

## 3. Environment & Tooling

- **Python**: 3.13.0
- **Testing**: `pytest` for unit tests; `pytest qa -q` in `verification_pipeline/` (152 passing baseline).
  A separate 4-tier E2E acceptance suite audits `Reports/Pipeline/report/` against the canonical
  artifacts: `pytest tests_e2e -q` in `verification_pipeline/` (23 passing).
- **Core Libraries**: pandas, pydantic (v2), google-genai, pyyaml, openpyxl.
- **Paths**: Always use `repo_paths.py` in `verification_pipeline` to resolve cross-repository locations relative to the workspace root.
- **LaTeX**: `latexmk -pdf` via TeX Live for compiling replication documentation.

---

## 4. Subagent Policies & Context Budgets

- Subagents must be self-contained and run with clear, specific scopes (maximum parallel cap: 12).
- When assessing fields, subagents must ALWAYS receive the original field description and extraction definition from `schema/non_salary_schema.py` to prevent definition-less drift.
- Subagents must not spawn further subagents.
- Context budgets:
  - System prompt: $\le 3,500$ tokens.
  - Per-item context: soft target 2,000 tokens, hard cap 4,000 tokens (up to 6,000 for wage/bonus).
  - Total chunk context: $\le 50,000$ tokens (15–20 items per chunk).

