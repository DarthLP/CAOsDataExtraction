# Test Infrastructure Specification: CAO Technical Replication

**Date**: 2026-09-16  
**Author**: `test_writer_e2e`  
**Repository**: `/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction`  
**Status**: Operational & Validated  

---

## 1. Overview & Architecture

The CAO Technical Replication verification infrastructure implements a rigorous, opaque-box **4-Tier Verification Architecture** designed to guarantee end-to-end data integrity, architectural invariants, cross-pipeline execution stability, and publication accuracy across all pipeline stages:

```
+-----------------------------------------------------------------------------------+
|                        4-TIER VERIFICATION HARNESS                                |
+-----------------------------------------------------------------------------------+
|  Tier 1: Feature Coverage — Canonical Data Artifacts                             |
|  - Verifies presence, exact dimensions, delimiters, unique entities, and tiers    |
+-----------------------------------------------------------------------------------+
|  Tier 2: Boundary & Invariants — Codebase Invariants                              |
|  - Verifies script portability, LLM extraction hyperparameters, hard guards       |
+-----------------------------------------------------------------------------------+
|  Tier 3: Cross-Pipeline / Suite Integration                                       |
|  - Executes 152 automated QA regression tests and 6-stage indices battery         |
+-----------------------------------------------------------------------------------+
|  Tier 4: Real-World Acceptance — Publication Report Audit                         |
|  - Audits LaTeX report sections for exact numbers, path integrity, worked example |
+-----------------------------------------------------------------------------------+
```

---

## 2. Test Suite Components & Layout

| Component | Location | Framework / Tool | Scope |
|---|---|---|---|
| **E2E Acceptance Suite** | `verification_pipeline/qa/test_e2e_replication_report.py` | Pytest / Python CLI | 23 opaque-box tests spanning Tiers 1–4 |
| **Pytest Configuration** | `verification_pipeline/pytest.ini` | Pytest | Marker definitions (`tier1`, `tier2`, `tier3`, `tier4`, `e2e`) |
| **Stage 2 Regression Suite** | `verification_pipeline/qa/` (`full_audit/tests/`, `shared/tests/`) | Pytest | 152 unit and regression tests verifying layers G0–G33 |
| **Stage 3 Validation Battery** | `verification_pipeline/indices/check_battery.py` | Python script | 6-stage domain sanity, unit conversions, and coverage checks |
| **Master Rebuild Pipeline** | `verification_pipeline/indices/rebuild.sh` | Zsh shell script | Dynamic end-to-end regeneration of all derived indices and panel |

---

## 3. Tier Detailed Specifications

### Tier 1: Canonical Data Artifacts Verification
Verifies the existence, exact matrix shape, delimiter, and unique entity counts of all canonical frozen data artifacts:
- **Salary Parser Dataset (`outputs/parser_salary/extracted_data_salary_v2.csv`)**:
  - Delimiter: `;`
  - Rows: `359,474`
  - Columns: `749`
  - Confidence Tier Distribution:
    - Tier A: `254,334` (70.75%)
    - Tier B: `88,777` (24.70%)
    - Tier C: `13,693` (3.81%)
    - Tier D: `2,670` (0.74%)
    - Analytical Sample (Tiers A + B): `343,111` (95.45%)
- **Non-Salary Canonical Dataset (`verification_pipeline/qa/corrected_dataset.csv`)**:
  - Delimiter: `;`
  - Rows: `2,739`
  - Columns: `318`
  - Unique CAO Agreements: `242`
- **Composite Index (`verification_pipeline/indices/out/composite_index.csv`)**:
  - Delimiter: `;`
  - Rows: `2,698`
  - Columns: `112`
- **Monthly Wage Indices (`verification_pipeline/indices/out/mw_indices.csv`)**:
  - Delimiter: `;`
  - Rows: `1,970`
  - Columns: `22`
- **Scoring Parameter Registry (`verification_pipeline/indices/out/scoring_params.csv`)**:
  - Delimiter: `;`
  - Rows: `89` (registering 39 scored cardinal fields across topics)
  - Columns: `10`
- **Audit Suspect Queue (`verification_pipeline/indices/out/statutory_fingerprint_suspects.csv`)**:
  - Delimiter: `;`
  - Total Suspects: `1,244`
  - Unreviewed Suspects (`NEVER_CHECKED`): `1,150`
  - Columns: `6`

### Tier 2: Boundary & Codebase Invariants Verification
Ensures that key pipeline contracts, execution guards, and script configurations remain unbroken:
- **Portability of `rebuild.sh`**:
  - Zero hardcoded machine paths (`/Users/lorenzpiazolo`, `/Users/`, `/home/`).
  - Dynamic directory resolution: `cd "$(dirname "$0")"`.
  - Strict shell guards: `set -e` and `set -o pipefail`.
  - Executable permissions (`chmod +x`).
- **LLM Extraction Hyperparameters (`pipelines/p3_llmExtraction.py`)**:
  - Dataclass `ExtractionConfig` frozen values:
    - `model = 'gemini-2.5-flash'`
    - `temperature = 0.0`
    - `top_p = 0.1`
    - `top_k = 1`
    - `seed = 42`
    - `max_tokens = 65536`
- **Deterministic Salary Parser 100% Provenance Hard Guard (`salary_parser/deliver.py`)**:
  - Immediate execution abort via `SystemExit` if `'AMOUNT provenance: 100.00%'` is not achieved.
  - Active single entrypoint orchestrating stages 1 through 6.
- **Salary Parser Core Invariants (`salary_parser/salary_parser.py`)**:
  - Mangled decimal regex `MANGLED_DEC = re.compile(r'^-?[1-9]\d{0,2}\.\d{4,6}$')` (exact 2-cell rescue in CAO 592).
  - Header date grounding (strict header/title dates, no document-date fallback).

### Tier 3: Cross-Pipeline / Suite Integration
Validates end-to-end multi-suite execution across pipeline subsystems:
- **Stage 2 Regression Suite**:
  - Command: `python3 -m pytest qa -q --ignore=qa/test_e2e_replication_report.py`
  - Target: All 152 regression tests across 13 test modules must pass cleanly (exit code 0).
- **Stage 3 Validation Battery**:
  - Command: `python3 check_battery.py`
  - Target: All 6 stages must pass with 0 errors:
    1. Converter self-test (35/35 passed)
    2. Field sanity & semantics (39/39 OK, 39/39 consistent)
    3. Salary unit-class check (median converted EUR/month aligns across units)
    4. Percentile track sanity (10 topics in $[0, 1]$, positive rank correlation)
    5. Statutory fingerprint screen (1,244 suspects / 1,150 never agent-checked)
    6. Same-term coverage (1,254 diff-cell sides, 0 uncovered)

### Tier 4: Real-World Acceptance (LaTeX Report Audit)
Verifies that the compiled documentation (`Reports/Pipeline/report/sections/*.tex`) faithfully and accurately represents the live system:
- **Exact Numerical Consistency**:
  - `359,474` (salary rows) cited in `01_intro.tex` / `03_salary_parser.tex`.
  - `2,739` (corrected non-salary records) cited in `01_intro.tex` / `04_correction_verification.tex`.
  - `2,698` (composite index rows) cited in Table 1 (`01_intro.tex`) / `05_indices.tex`.
  - `1,970` (monthly wage index rows) cited in Table 1 (`01_intro.tex`) / `05_indices.tex`.
  - `89` (scoring parameters) cited in Table 1 (`01_intro.tex`) / `05_indices.tex`.
  - `0.64` (KMO factorability measure) cited in `05_indices.tex`.
  - `1,244` (total standing audit suspects) cited in `04_correction_verification.tex`.
- **CAO 1022 Worked Example Audit (`07_appendices.tex`, Appendix D)**:
  - Valid PDF paths cited (no reference to non-existent `inputs/pdfs/input_pdfs/1022/CAO MBO 2020-2021.pdf`).
  - Real collective agreement cited (e.g. `CAO_MBO_2018_2020_in_Word.pdf` / ID 1022011).
  - Accurate scale structure: 273 rows across 30 scales and 14 steps (not 48 steps across 14 scales).
  - Accurate QA layer citation: Layer 16 (`L16_dip_family_consistency`), not Layer 22.
- **Path Integrity Audit**:
  - Zero broken relative paths without prefixes (e.g. `docs/DATA_LINEAGE.md` must be `verification_pipeline/docs/DATA_LINEAGE.md`).
  - Zero un-prefixed suspect queues (`verification_pipeline/indices/out/statutory_fingerprint_suspects.csv`).
  - Zero un-prefixed wage verification paths (`verification_pipeline/salary/wage_check_full/`).
  - Zero presentation of dead script `apply_corrections.py` as an active regeneration tool.

---

## 4. Execution Commands

### Running via Pytest

```bash
# Run the entire E2E verification suite
cd verification_pipeline && python3 -m pytest qa/test_e2e_replication_report.py -v

# Run by tier
cd verification_pipeline && python3 -m pytest qa/test_e2e_replication_report.py -m tier1 -v
cd verification_pipeline && python3 -m pytest qa/test_e2e_replication_report.py -m tier2 -v
cd verification_pipeline && python3 -m pytest qa/test_e2e_replication_report.py -m tier3 -v
cd verification_pipeline && python3 -m pytest qa/test_e2e_replication_report.py -m tier4 -v

# Run the 152 historical QA regression tests
cd verification_pipeline && python3 -m pytest qa -q --ignore=qa/test_e2e_replication_report.py
```

### Running via Dedicated CLI Runner

```bash
# Run all tiers with formatted report
python3 verification_pipeline/qa/test_e2e_replication_report.py

# Run specific tier
python3 verification_pipeline/qa/test_e2e_replication_report.py --tier 1
python3 verification_pipeline/qa/test_e2e_replication_report.py --tier 2
python3 verification_pipeline/qa/test_e2e_replication_report.py --tier 3
python3 verification_pipeline/qa/test_e2e_replication_report.py --tier 4
```

### Running the Indices Battery

```bash
cd verification_pipeline/indices && python3 check_battery.py
```

---

## 5. Recursion & Isolation Guarantees

When Tier 3 executes the Stage 2 regression suite via pytest, it isolates execution using two independent safeguards:
1. **Subprocess Filter**: Tier 3 explicitly executes pytest with `--ignore=qa/test_e2e_replication_report.py`.
2. **Environment Guard**: Tier 3 injects `IN_E2E_SUBPROCESS=1` into the subprocess environment; `test_e2e_replication_report.py` inspects this variable on import and immediately marks all tests skipped if loaded within a nested subprocess.
