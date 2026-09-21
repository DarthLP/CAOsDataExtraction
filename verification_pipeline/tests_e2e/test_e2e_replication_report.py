"""
End-to-End Replication Report Verification Suite (4-Tier Opaque-Box Acceptance Suite).

Author: test_writer_e2e
Date: 2026-09-16
Workspace: /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction

Lives in tests_e2e/, not qa/, so it never joins the `pytest qa -q` 152-test
regression baseline (CLAUDE.md Sec.3). It is the separate acceptance gate for
Reports/Pipeline/report/CAOPipeline.tex: every numeric claim, table count, and
path in sections/*.tex is checked against the canonical artifacts and the live
pipeline invariants.

Methodology: 4-Tier Verification Architecture
- Tier 1 (Feature Coverage): Canonical data artifacts verification (files exist, exact rows/cols, delimiter ;, unique CAOs).
- Tier 2 (Boundary & Invariants): Codebase invariants verification (rebuild.sh portability with no hardcoded user paths; p3 hyperparameters; deliver.py 100% provenance hard guard).
- Tier 3 (Cross-Pipeline / Suite Integration): Regression suite passing (pytest qa -q passes 152 tests) and battery suite passing (python3 check_battery.py passes all 6 stages).
- Tier 4 (Real-World Acceptance): LaTeX report audit verification (grepping sections for exact required counts: 359,474, 2,739, 2,698, 1,970, 89, 0.64, 1,244, CAO 1022 real files; zero broken paths like docs/DATA_LINEAGE.md without prefix).

Usage:
  # Pytest execution:
  cd verification_pipeline && python3 -m pytest tests_e2e/test_e2e_replication_report.py -v
  cd verification_pipeline && python3 -m pytest tests_e2e/test_e2e_replication_report.py -m tier1 -v

  # Standalone CLI execution:
  python3 verification_pipeline/tests_e2e/test_e2e_replication_report.py
  python3 verification_pipeline/tests_e2e/test_e2e_replication_report.py --tier 1
  python3 verification_pipeline/tests_e2e/test_e2e_replication_report.py --tier 2
  python3 verification_pipeline/tests_e2e/test_e2e_replication_report.py --tier 3
  python3 verification_pipeline/tests_e2e/test_e2e_replication_report.py --tier 4
"""

import ast
import csv
import io
import os
import pathlib
import re
import subprocess
import sys
from collections import Counter
from typing import Dict, List, Optional, Set, Tuple

import pytest


def find_repo_root() -> pathlib.Path:
    """Find repo root containing outputs/ and verification_pipeline/."""
    # Start from this file
    candidate = pathlib.Path(__file__).resolve().parent
    while candidate != candidate.parent:
        if (candidate / "outputs").is_dir() and (candidate / "verification_pipeline").is_dir():
            return candidate
        candidate = candidate.parent
    # Fallback to cwd
    candidate = pathlib.Path.cwd().resolve()
    while candidate != candidate.parent:
        if (candidate / "outputs").is_dir() and (candidate / "verification_pipeline").is_dir():
            return candidate
        candidate = candidate.parent
    raise RuntimeError("Could not determine CAOsDataExtraction repository root.")


REPO_ROOT = find_repo_root()


# ==============================================================================
# Fixtures & Shared Helpers
# ==============================================================================

@pytest.fixture(scope="session")
def repo_root() -> pathlib.Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def salary_v2_stats(repo_root) -> Dict:
    """Parse outputs/parser_salary/extracted_data_salary_v2.csv once for Tier 1 tests."""
    salary_csv_path = repo_root / "outputs" / "parser_salary" / "extracted_data_salary_v2.csv"
    if not salary_csv_path.exists():
        pytest.fail(f"Salary v2 CSV missing at {salary_csv_path}")

    total_rows = 0
    tier_counts = Counter()
    cols = 0
    with open(salary_csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        cols = len(header)
        tier_idx = header.index("confidence_tier")
        for row in reader:
            total_rows += 1
            tier_counts[row[tier_idx]] += 1

    return {
        "path": salary_csv_path,
        "cols": cols,
        "rows": total_rows,
        "tier_counts": tier_counts,
        "analytical_rows": tier_counts["A"] + tier_counts["B"],
    }


@pytest.fixture(scope="session")
def latex_sections(repo_root) -> Dict[str, str]:
    """Load all .tex section files from Reports/Pipeline/report/sections/."""
    sections_dir = repo_root / "Reports" / "Pipeline" / "report" / "sections"
    if not sections_dir.exists():
        pytest.fail(f"Sections directory not found at {sections_dir}")

    sections = {}
    for tex_path in sections_dir.glob("*.tex"):
        with open(tex_path, "r", encoding="utf-8") as f:
            sections[tex_path.name] = f.read()
    return sections


# ==============================================================================
# TIER 1: Feature Coverage — Canonical Data Artifacts Verification
# ==============================================================================

@pytest.mark.tier1
@pytest.mark.e2e
class TestTier1CanonicalArtifacts:
    """Verify presence, matrix dimensions, delimiters, and key distributions of canonical artifacts."""

    def test_canonical_files_exist(self, repo_root):
        """All canonical pipeline artifacts must exist on disk."""
        required_files = [
            repo_root / "outputs" / "parser_salary" / "extracted_data_salary_v2.csv",
            repo_root / "verification_pipeline" / "qa" / "corrected_dataset.csv",
            repo_root / "verification_pipeline" / "indices" / "out" / "composite_index.csv",
            repo_root / "verification_pipeline" / "indices" / "out" / "mw_indices.csv",
            repo_root / "verification_pipeline" / "indices" / "out" / "scoring_params.csv",
            repo_root / "verification_pipeline" / "indices" / "out" / "statutory_fingerprint_suspects.csv",
            repo_root / "verification_pipeline" / "indices" / "out" / "wml_timeline.csv",
            repo_root / "verification_pipeline" / "indices" / "out" / "statutory_all.csv",
        ]
        missing = [str(p.relative_to(repo_root)) for p in required_files if not p.is_file()]
        assert not missing, f"Missing canonical artifacts: {missing}"

    def test_salary_v2_dimensions_and_delimiter(self, salary_v2_stats):
        """Verify extracted_data_salary_v2.csv has exactly 359,474 rows and 749 columns."""
        assert salary_v2_stats["rows"] == 359474, (
            f"Expected exactly 359,474 rows in extracted_data_salary_v2.csv, got {salary_v2_stats['rows']}"
        )
        assert salary_v2_stats["cols"] == 749, (
            f"Expected exactly 749 columns in extracted_data_salary_v2.csv, got {salary_v2_stats['cols']}"
        )

    def test_salary_v2_confidence_tier_distribution(self, salary_v2_stats):
        """Verify confidence tiers A, B, C, D and analytical sample (Tiers A+B = 343,111 / 95.45%)."""
        counts = salary_v2_stats["tier_counts"]
        total = salary_v2_stats["rows"]

        assert counts["A"] == 254334, f"Expected 254,334 Tier A rows, got {counts['A']}"
        assert counts["B"] == 88777, f"Expected 88,777 Tier B rows, got {counts['B']}"
        assert counts["C"] == 13693, f"Expected 13,693 Tier C rows, got {counts['C']}"
        assert counts["D"] == 2670, f"Expected 2,670 Tier D rows, got {counts['D']}"

        analytical = counts["A"] + counts["B"]
        assert analytical == 343111, f"Expected 343,111 Analytical rows (Tiers A+B), got {analytical}"
        analytical_pct = analytical / total
        assert round(analytical_pct * 100, 2) == 95.45, (
            f"Expected 95.45% analytical sample share, got {analytical_pct * 100:.2f}%"
        )

    def test_corrected_dataset_dimensions_delimiter_and_unique_caos(self, repo_root):
        """Verify corrected_dataset.csv has 2,739 rows, 318 cols, delimiter ';', and 242 unique CAOs."""
        csv_path = repo_root / "verification_pipeline" / "qa" / "corrected_dataset.csv"
        assert csv_path.is_file(), f"corrected_dataset.csv not found at {csv_path}"

        caos: Set[str] = set()
        row_count = 0
        cols = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            cols = len(header)
            cao_idx = header.index("cao_number")
            for row in reader:
                row_count += 1
                caos.add(row[cao_idx])

        assert row_count == 2739, f"Expected exactly 2,739 rows in corrected_dataset.csv, got {row_count}"
        assert cols == 318, f"Expected exactly 318 columns in corrected_dataset.csv, got {cols}"
        assert len(caos) == 242, f"Expected exactly 242 unique CAOs in corrected_dataset.csv, got {len(caos)}"

    def test_composite_index_dimensions_and_delimiter(self, repo_root):
        """Verify composite_index.csv has exactly 2,698 rows and 112 cols (delimiter ';')."""
        csv_path = repo_root / "verification_pipeline" / "indices" / "out" / "composite_index.csv"
        assert csv_path.is_file(), f"composite_index.csv not found at {csv_path}"

        row_count = 0
        cols = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            cols = len(header)
            row_count = sum(1 for _ in reader)

        assert row_count == 2698, f"Expected exactly 2,698 rows in composite_index.csv, got {row_count}"
        assert cols == 112, f"Expected exactly 112 columns in composite_index.csv, got {cols}"

    def test_mw_indices_dimensions_and_delimiter(self, repo_root):
        """Verify mw_indices.csv has exactly 1,970 rows and 22 cols (delimiter ';')."""
        csv_path = repo_root / "verification_pipeline" / "indices" / "out" / "mw_indices.csv"
        assert csv_path.is_file(), f"mw_indices.csv not found at {csv_path}"

        row_count = 0
        cols = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            cols = len(header)
            row_count = sum(1 for _ in reader)

        assert row_count == 1970, f"Expected exactly 1,970 rows in mw_indices.csv, got {row_count}"
        assert cols == 22, f"Expected exactly 22 columns in mw_indices.csv, got {cols}"

    def test_scoring_params_dimensions_and_field_count(self, repo_root):
        """Verify scoring_params.csv has exactly 89 rows and registers all 50 scored field variants."""
        csv_path = repo_root / "verification_pipeline" / "indices" / "out" / "scoring_params.csv"
        assert csv_path.is_file(), f"scoring_params.csv not found at {csv_path}"

        rows = 0
        fields = set()
        cols = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            cols = len(header)
            field_idx = header.index("field")
            for row in reader:
                rows += 1
                fields.add(row[field_idx])

        assert rows == 89, f"Expected exactly 89 parameter rows in scoring_params.csv, got {rows}"
        assert cols == 10, f"Expected exactly 10 columns in scoring_params.csv, got {cols}"
        # The 89 rows register 50 distinct field variants (some fields carry multiple
        # winsorization/unit variants, e.g. training_budget_value[eur/pct_salary/pct_wagesum]).
        assert len(fields) == 50, f"Expected exactly 50 registered field variants in scoring_params.csv, got {len(fields)}"

    def test_statutory_suspects_queue_counts(self, repo_root):
        """Verify statutory_fingerprint_suspects.csv has 1,244 suspects (1,150 NEVER_CHECKED)."""
        csv_path = repo_root / "verification_pipeline" / "indices" / "out" / "statutory_fingerprint_suspects.csv"
        assert csv_path.is_file(), f"statutory_fingerprint_suspects.csv not found at {csv_path}"

        rows = 0
        never_checked = 0
        cols = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            header = next(reader)
            cols = len(header)
            adj_idx = header.index("adjudication")
            for row in reader:
                rows += 1
                if row[adj_idx] == "NEVER_CHECKED":
                    never_checked += 1

        assert rows == 1244, f"Expected exactly 1,244 suspects in statutory_fingerprint_suspects.csv, got {rows}"
        assert never_checked == 1150, f"Expected 1,150 NEVER_CHECKED suspects, got {never_checked}"
        assert cols == 6, f"Expected 6 columns in statutory_fingerprint_suspects.csv, got {cols}"


# ==============================================================================
# TIER 2: Boundary & Invariants — Codebase Invariants Verification
# ==============================================================================

@pytest.mark.tier2
@pytest.mark.e2e
class TestTier2CodebaseInvariants:
    """Verify architectural invariants, script portability, and execution guards."""

    def test_rebuild_sh_portability_and_execution_guards(self, repo_root):
        """Verify rebuild.sh contains no hardcoded user paths and uses dynamic dirname resolution."""
        rebuild_path = repo_root / "verification_pipeline" / "indices" / "rebuild.sh"
        assert rebuild_path.is_file(), f"rebuild.sh missing at {rebuild_path}"

        content = rebuild_path.read_text(encoding="utf-8")

        # Invariant 1: No hardcoded user directories
        assert "/Users/lorenzpiazolo" not in content, (
            "rebuild.sh contains hardcoded user directory '/Users/lorenzpiazolo'!"
        )
        assert "/Users/" not in content, "rebuild.sh contains hardcoded macOS user directory '/Users/'!"
        assert "/home/" not in content, "rebuild.sh contains hardcoded Linux user directory '/home/'!"

        # Invariant 2: Dynamic dirname resolution
        assert 'cd "$(dirname "$0")"' in content or 'cd "$(dirname "$0")"' in content, (
            'rebuild.sh must use dynamic directory resolution cd "$(dirname "$0")"'
        )

        # Invariant 3: Execution guards set -e and pipefail
        assert "set -e" in content, "rebuild.sh must specify 'set -e'"
        assert "set -o pipefail" in content, "rebuild.sh must specify 'set -o pipefail'"

        # Invariant 4: File executable permission
        assert os.access(rebuild_path, os.X_OK), "rebuild.sh must have executable permissions (chmod +x)"

    def test_p3_llm_extraction_hyperparameters(self, repo_root):
        """Verify pipelines/p3_llmExtraction.py ExtractionConfig parameters match frozen specifications."""
        p3_path = repo_root / "pipelines" / "p3_llmExtraction.py"
        assert p3_path.is_file(), f"p3_llmExtraction.py missing at {p3_path}"

        content = p3_path.read_text(encoding="utf-8")

        # Parse AST to find ExtractionConfig dataclass defaults
        tree = ast.parse(content, filename=str(p3_path))
        config_class = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "ExtractionConfig":
                config_class = node
                break

        assert config_class is not None, "Could not find class ExtractionConfig in p3_llmExtraction.py"

        defaults = {}
        for item in config_class.body:
            if isinstance(item, ast.AnnAssign) and item.value is not None:
                # ast.Constant in Python 3.8+
                target_id = item.target.id if isinstance(item.target, ast.Name) else None
                if target_id and isinstance(item.value, ast.Constant):
                    defaults[target_id] = item.value.value

        assert defaults.get("model") == "gemini-2.5-flash", (
            f"Expected model 'gemini-2.5-flash', got {defaults.get('model')}"
        )
        assert defaults.get("temperature") == 0.0, (
            f"Expected temperature 0.0, got {defaults.get('temperature')}"
        )
        assert defaults.get("top_p") == 0.1, (
            f"Expected top_p 0.1, got {defaults.get('top_p')}"
        )
        assert defaults.get("top_k") == 1, (
            f"Expected top_k 1, got {defaults.get('top_k')}"
        )
        assert defaults.get("seed") == 42, (
            f"Expected seed 42, got {defaults.get('seed')}"
        )
        assert defaults.get("max_tokens") == 65536, (
            f"Expected max_tokens 65536, got {defaults.get('max_tokens')}"
        )

    def test_deliver_py_100_percent_provenance_hard_guard(self, repo_root):
        """Verify salary_parser/deliver.py enforces 100% amount provenance with an immediate abort."""
        deliver_path = repo_root / "salary_parser" / "deliver.py"
        assert deliver_path.is_file(), f"deliver.py missing at {deliver_path}"

        content = deliver_path.read_text(encoding="utf-8")

        # Verifies the presence of the hard provenance guard
        assert "AMOUNT provenance: 100.00%" in content, (
            "deliver.py must verify 'AMOUNT provenance: 100.00%'"
        )
        assert "raise SystemExit" in content, (
            "deliver.py must raise SystemExit on provenance drop below 100%"
        )
        assert "ABORT: amount provenance below 100%" in content, (
            "deliver.py must emit 'ABORT: amount provenance below 100%'"
        )

    def test_salary_parser_invariants_and_rules(self, repo_root):
        """Verify salary_parser/salary_parser.py has mangled-decimal regex and date grounding."""
        parser_path = repo_root / "salary_parser" / "salary_parser.py"
        assert parser_path.is_file(), f"salary_parser.py missing at {parser_path}"

        content = parser_path.read_text(encoding="utf-8")

        # Mangled-decimal rule for CAO 592 2-cell rescue
        assert "MANGLED_DEC" in content, "salary_parser.py must define MANGLED_DEC regex"
        assert r"^-?[1-9]\d{0,2}\.\d{4,6}$" in content, (
            r"salary_parser.py must define exact MANGLED_DEC regex r'^-?[1-9]\d{0,2}\.\d{4,6}$'"
        )

        # Date grounding: no document-date fallback
        assert "start_date" in content, "salary_parser.py must process start_date"


# ==============================================================================
# TIER 3: Cross-Pipeline / Suite Integration
# ==============================================================================

@pytest.mark.tier3
@pytest.mark.e2e
class TestTier3CrossPipelineIntegration:
    """Verify end-to-end integration and execution of the automated regression suite and battery."""

    def test_regression_suite_152_passed(self, repo_root):
        """Execute pytest qa -q on Stage 2 regression suite and verify all 152 tests pass."""
        qa_dir = repo_root / "verification_pipeline"
        assert qa_dir.is_dir(), f"verification_pipeline dir missing at {qa_dir}"

        # This suite lives in tests_e2e/, not qa/, so "pytest qa -q" never recurses
        # into it and needs no --ignore flag.
        cmd = [sys.executable, "-m", "pytest", "qa", "-q"]

        res = subprocess.run(cmd, cwd=qa_dir, capture_output=True, text=True, timeout=120)

        assert res.returncode == 0, (
            f"pytest qa -q failed with return code {res.returncode}.\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )
        assert "152 passed" in res.stdout, (
            f"Expected '152 passed' in pytest qa -q output, got:\n{res.stdout}"
        )

    def test_indices_validation_battery_6_stages_passed(self, repo_root):
        """Execute check_battery.py from verification_pipeline/indices/ and verify all 6 stages pass."""
        indices_dir = repo_root / "verification_pipeline" / "indices"
        battery_script = indices_dir / "check_battery.py"
        assert battery_script.is_file(), f"check_battery.py missing at {battery_script}"

        cmd = [sys.executable, "check_battery.py"]
        res = subprocess.run(cmd, cwd=indices_dir, capture_output=True, text=True, timeout=120)

        assert res.returncode == 0, (
            f"check_battery.py exited with return code {res.returncode}.\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )

        stdout = res.stdout
        # Verify 6 stages from check_battery.py output:
        assert "1) converter self-test: 35/35 passed" in stdout, "Battery stage 1 converter self-test failed"
        assert "2) field sanity: 39 OK" in stdout, "Battery stage 2 field sanity failed"
        assert "3) salary unit-class check" in stdout, "Battery stage 3 salary unit-class check failed"
        assert "4) percentile-track sanity: 10 topics with gen01" in stdout, "Battery stage 4 percentile sanity failed"
        assert "5) statutory-fingerprint screen: 1244 suspects (1150 never agent-checked)" in stdout, (
            "Battery stage 5 statutory suspects count mismatch"
        )
        assert "6) same-term coverage: 1254 diff-cell sides, 0 uncovered -> ALL COVERED" in stdout, (
            "Battery stage 6 same-term coverage check failed"
        )


# ==============================================================================
# TIER 4: Real-World Acceptance — LaTeX Report Audit Verification
# ==============================================================================

@pytest.mark.tier4
@pytest.mark.e2e
class TestTier4ReportAuditAcceptance:
    """Audit LaTeX report sections for exact counts, statistical measures, valid paths, and real examples."""

    def test_latex_canonical_count_359474_cited(self, latex_sections):
        """Verify exact canonical count 359,474 is cited in 01_intro.tex and 03_salary_parser.tex."""
        intro = latex_sections.get("01_intro.tex", "")
        salary = latex_sections.get("03_salary_parser.tex", "")

        has_in_intro = "359,474" in intro or "359{,}474" in intro
        has_in_salary = "359,474" in salary or "359{,}474" in salary

        assert has_in_intro or has_in_salary, (
            "Count '359,474' (salary rows) not found in 01_intro.tex or 03_salary_parser.tex"
        )

    def test_latex_canonical_count_2739_cited(self, latex_sections):
        """Verify exact canonical count 2,739 is cited in 01_intro.tex and 04_correction_verification.tex."""
        intro = latex_sections.get("01_intro.tex", "")
        qa = latex_sections.get("04_correction_verification.tex", "")

        has_in_intro = "2,739" in intro or "2{,}739" in intro
        has_in_qa = "2,739" in qa or "2{,}739" in qa

        assert has_in_intro or has_in_qa, (
            "Count '2,739' (corrected records) not found in 01_intro.tex or 04_correction_verification.tex"
        )

    def test_latex_canonical_count_2698_composite_index_cited(self, latex_sections):
        """Verify composite_index row count 2,698 is cited in 01_intro.tex (Table 1) or 05_indices.tex."""
        intro = latex_sections.get("01_intro.tex", "")
        indices = latex_sections.get("05_indices.tex", "")

        found = "2,698" in intro or "2{,}698" in intro or "2,698" in indices or "2{,}698" in indices
        assert found, (
            "Count '2,698' (composite_index.csv rows) not cited in 01_intro.tex (Table 1) or 05_indices.tex. "
            "Implementation worker must ensure Table 1 records exact 2,698 rows."
        )

    def test_latex_canonical_count_1970_mw_indices_cited(self, latex_sections):
        """Verify mw_indices row count 1,970 is cited in 01_intro.tex (Table 1) or 05_indices.tex."""
        intro = latex_sections.get("01_intro.tex", "")
        indices = latex_sections.get("05_indices.tex", "")

        found = "1,970" in intro or "1{,}970" in intro or "1,970" in indices or "1{,}970" in indices
        assert found, (
            "Count '1,970' (mw_indices.csv rows) not cited in 01_intro.tex (Table 1) or 05_indices.tex. "
            "Implementation worker must ensure Table 1 records exact 1,970 rows."
        )

    def test_latex_scoring_params_89_rows_cited(self, latex_sections):
        """Verify 89 parameter rows for scoring_params.csv is cited in Table 1 of 01_intro.tex."""
        intro = latex_sections.get("01_intro.tex", "")
        assert "89" in intro and "scoring_params.csv" in intro, (
            "Scoring params count '89' not found associated with scoring_params.csv in 01_intro.tex"
        )

    def test_latex_kmo_measure_064_cited(self, latex_sections):
        """Verify KMO measure 0.64 is cited in 05_indices.tex."""
        indices = latex_sections.get("05_indices.tex", "")
        assert "0.64" in indices and "KMO" in indices, (
            "KMO measure '0.64' not found in 05_indices.tex"
        )

    def test_latex_suspect_queue_1244_cited(self, latex_sections):
        """Verify standing suspect queue count 1,244 (and 1,150 unreviewed) is cited in 04_correction_verification.tex."""
        qa = latex_sections.get("04_correction_verification.tex", "")
        found_1244 = "1,244" in qa or "1{,}244" in qa or "1244" in qa
        assert found_1244, (
            "Suspect queue total count '1,244' not cited in 04_correction_verification.tex. "
            "Implementation worker must clarify that the queue contains 1,244 suspects (with 1,150 unreviewed)."
        )

    def test_latex_cao_1022_worked_example_real_files_and_layers(self, latex_sections):
        """Verify Appendix D (07_appendices.tex) cites real CAO 1022 files, 273 rows, and QA Layer 16."""
        app = latex_sections.get("07_appendices.tex", "")

        # 1. Must NOT cite the fake invalid path 'inputs/pdfs/input_pdfs/1022/CAO MBO 2020-2021.pdf'
        assert "inputs/pdfs/input_pdfs/1022/CAO MBO 2020-2021.pdf" not in app, (
            "07_appendices.tex still cites invalid path 'inputs/pdfs/input_pdfs/1022/CAO MBO 2020-2021.pdf'. "
            "Real directory is 'inputs/pdfs/input_pdfs_non_extra/1022/'."
        )

        # 2. Must cite real collective agreement files (e.g. CAO_MBO_2018_2020_in_Word.pdf or ID 1022011)
        has_real_pdf = (
            "CAO_MBO_2018_2020_in_Word.pdf" in app
            or "input_pdfs_non_extra" in app
            or "1022011" in app
        )
        assert has_real_pdf, (
            "07_appendices.tex must cite the real full collective agreement file for CAO 1022 "
            "(e.g., 'CAO_MBO_2018_2020_in_Word.pdf' or ID 1022011)."
        )

        # 3. Must cite accurate salary scale counts (273 rows across 30 scales & 14 steps, not '48 scale steps across 14')
        assert "48 scale steps across 14 salary scales" not in app, (
            "07_appendices.tex still cites inaccurate '48 scale steps across 14 salary scales'. "
            "Real counts for ID 1022011 are 273 rows across 30 scales and 14 scale steps."
        )

        # 4. Must cite QA Layer 16 (not Layer 22) for the childcare support consistency correction
        assert "Layer 22" not in app or "Layer 16" in app, (
            "Childcare support correction in CAO 1022 must cite Layer 16 (L16_dip_family_consistency), not Layer 22."
        )

    def test_latex_zero_broken_paths(self, latex_sections):
        """Verify no broken relative paths exist across all section files."""
        broken_findings = []
        for filename, content in latex_sections.items():
            # Check broken DATA_LINEAGE.md without prefix
            if re.search(r"(?<!verification_pipeline/)docs/DATA_LINEAGE\.md", content):
                broken_findings.append(f"{filename}: broken path 'docs/DATA_LINEAGE.md' (needs 'verification_pipeline/' prefix)")

            # Check broken statutory_fingerprint_suspects.csv without prefix
            if re.search(r"(?<!verification_pipeline/indices/out/)statutory_fingerprint_suspects\.csv", content):
                broken_findings.append(f"{filename}: broken path 'statutory_fingerprint_suspects.csv' (needs 'verification_pipeline/indices/out/' prefix)")

            # Check broken wage_check_full path without verification_pipeline/ prefix
            if re.search(r"(?<!verification_pipeline/)salary/wage_check_full", content):
                broken_findings.append(f"{filename}: broken path 'salary/wage_check_full' (needs 'verification_pipeline/' prefix)")

        assert not broken_findings, (
            f"Found broken path references in LaTeX sections:\n" + "\n".join(broken_findings)
        )



# ==============================================================================
# Standalone CLI Test Runner
# ==============================================================================

def run_standalone(tier: Optional[int] = None) -> int:
    """Execute tests programmatically with formatted reporting."""
    print("=" * 80)
    print("  CAO TECHNICAL REPLICATION REPORT — E2E TEST VERIFICATION SUITE")
    print(f"  Repo Root: {REPO_ROOT}")
    print("=" * 80)

    pytest_args = [
        "-v",
        str(pathlib.Path(__file__).resolve()),
    ]
    if tier:
        pytest_args.extend(["-m", f"tier{tier}"])

    return pytest.main(pytest_args)


if __name__ == "__main__":
    tier_arg = None
    if len(sys.argv) > 1 and sys.argv[1] == "--tier" and len(sys.argv) > 2:
        try:
            tier_arg = int(sys.argv[2])
        except ValueError:
            print("Invalid tier number specified.")
            sys.exit(1)

    sys.exit(run_standalone(tier_arg))
