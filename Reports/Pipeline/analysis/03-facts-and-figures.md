# Facts and Figures (Live Verified Metrics)

All figures verified directly against the disk artifacts on 2026-09-15.

---

## 1. Salary Parser Metrics

- **Total Extracted Salary Rows**: 359,474
  - **Tier A (Highest Confidence)**: 254,334 (70.75%)
  - **Tier B (Medium Confidence)**: 88,777 (24.70%)
  - **Tier C (Low Confidence / Flagged)**: 13,693 (3.81%)
  - **Tier D (Reject / Garbage)**: 2,670 (0.74%)
- **Analytic Sample (Tiers A + B)**: 343,111 rows (**95.45%**)
- **Amount Provenance**: 100% exact substring match against upstream p3 extraction (with exactly 2 cells rescued for mangled decimals in CAO 592).
- **Source File**: `outputs/parser_salary/extracted_data_salary_v2.csv`

---

## 2. Non-Salary Dataset & Correction Pipeline

- **Total CSV Shape**: 2,739 rows $\times$ 318 columns.
- **Unique CAO Identifiers**: 242 unique CAO numbers.
- **Curated Top-100/95 Scope**: 1,505 records across 95 CAOs.
- **Correction Framework**:
  - Total audited correction layers: **34 layers** (G0 $\rightarrow$ G33).
  - Total corrected cell interventions: $\approx 12,000$ cells.
  - Open "Can't Tell" reviews: 30 rows in `verification_pipeline/indices/review/all_open_cant_tells.csv`.
- **Source File**: `verification_pipeline/qa/corrected_dataset.csv` (MD5: `6666621bf51e4121a47b57cbb24932f3`).

---

## 3. Derived Indices & Validation Metrics

- **Composite Generosity Index**: 2,739 records across 10 scored non-salary domains.
  - Source: `verification_pipeline/indices/out/composite_index.csv` (MD5: `8f1b389b08bd7c55d8f7298d094a70fa`).
- **Scoring Parameter Registry**:
  - Scored fields registered: 39 fields.
  - Source: `verification_pipeline/indices/out/scoring_params.csv` (MD5: `ab6720d29bf103bbf1556f29e839dec6`).
- **Minimum & Monthly Wage Indices**:
  - Wage ladder records: 2,739 records across 95 CAOs.
  - Source: `verification_pipeline/indices/out/mw_indices.csv` (MD5: `061ff3a2f6afb8085a089c2925e83510`).
- **Test Battery Results**:
  - QA unit test suite: 152 passed in 3.76s (`pytest qa -q`).
  - Unit converter tests: 35/35 passed.
  - Field sanity bounds: 39/39 fields OK (0 checks flagged).
  - Semantic role consistency: 39/39 fields verified.
  - Generosity direction sign tests: 39/39 fields verified.

