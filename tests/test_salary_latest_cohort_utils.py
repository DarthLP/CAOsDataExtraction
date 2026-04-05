"""
Tests for Latest CAO salary cohort panels (calendar-year contract view and salary-year gap carry).

Exercises ``build_contract_year_latest_salary_calendar_panel`` and
``build_salary_year_latest_gap_panel`` on small synthetic frames so regressions are caught
without running the full descriptive plot pipeline.
"""

from __future__ import annotations

import unittest

import pandas as pd

from scripts.excel_analysis.salary_plot_cohort_utils import (
    build_contract_year_latest_salary_calendar_panel,
    build_salary_year_latest_gap_panel,
    snap_active_table_to_band_eligible_salary_files,
)


class TestSalaryLatestCohortUtils(unittest.TestCase):
    """Golden-style checks on toy CAO/file/slot timelines."""

    def test_contract_latest_includes_all_slots_on_active_file_without_date_filter(self) -> None:
        """
        Calendar-year latest merges every band-eligible long row on the active file for ``T``,
        including rows whose ``salary_start_date`` is after ``T``.
        """
        long = pd.DataFrame(
            {
                "cao_number": [10],
                "file_name": ["B.pdf"],
                "row_id": [2],
                "salary_index": [1],
                "salary_start_date": pd.to_datetime(["2010-01-01"]),
                "amount_monthly": [2500.0],
                "analysis_monthly_band_ok": [True],
            }
        )
        active = pd.DataFrame(
            {"cao_number": [10], "salary_year": [2008], "file_name": ["B.pdf"]}
        )
        out = build_contract_year_latest_salary_calendar_panel(long, active)
        self.assertEqual(len(out), 1)
        self.assertEqual(int(out["calendar_year"].iloc[0]), 2008)
        self.assertEqual(float(out["amount_monthly"].iloc[0]), 2500.0)

    def test_salary_latest_gap_carries_prior_effective_slots_on_new_file(self) -> None:
        """
        When the active file at ``T`` has no band-eligible slot with start ≤ end(``T``),
        the panel reuses the previous year's effective rows for that CAO.
        """
        dfw = pd.DataFrame(
            [
                {
                    "cao_number": 10,
                    "file_name": "A.pdf",
                    "id": 1,
                    "ingangsdatum": "2004-01-01",
                    "salary_1_start_date": "2004-06-01",
                    "salary_1_amount": 2000,
                    "salary_1_unit": "monthly",
                    "ft_hours": 40.0,
                },
                {
                    "cao_number": 10,
                    "file_name": "B.pdf",
                    "id": 2,
                    "ingangsdatum": "2006-01-01",
                    "salary_1_start_date": "2010-01-01",
                    "salary_1_amount": 2500,
                    "salary_1_unit": "monthly",
                    "ft_hours": 40.0,
                },
            ]
        )
        active_rows = []
        for y in range(2004, 2009):
            fn = "A.pdf" if y <= 2005 else "B.pdf"
            active_rows.append({"cao_number": 10, "salary_year": y, "file_name": fn})
        active = pd.DataFrame(active_rows)

        out = build_salary_year_latest_gap_panel(dfw, active)
        self.assertFalse(out.empty)
        y2006 = out[out["salary_year"] == 2006]
        self.assertGreaterEqual(len(y2006), 1)
        self.assertTrue((y2006["amount_monthly"] == 2000.0).any())

    def test_snap_carries_prior_file_when_nominal_has_no_eligible_slots(self) -> None:
        """
        Nominal forward-fill switches to B.pdf but df_slot only has rows on A.pdf; snapped table keeps A.pdf.
        """
        active = pd.DataFrame(
            [
                {"cao_number": 10, "salary_year": 2004, "file_name": "A.pdf"},
                {"cao_number": 10, "salary_year": 2005, "file_name": "A.pdf"},
                {"cao_number": 10, "salary_year": 2006, "file_name": "B.pdf"},
                {"cao_number": 10, "salary_year": 2007, "file_name": "B.pdf"},
            ]
        )
        df_slot = pd.DataFrame(
            {"cao_number": [10, 10], "file_name": ["A.pdf", "A.pdf"], "amount_monthly": [2000.0, 2100.0]}
        )
        out = snap_active_table_to_band_eligible_salary_files(active, df_slot)
        self.assertEqual(len(out), 4)
        self.assertTrue((out["file_name"] == "A.pdf").all())

    def test_snap_follows_nominal_when_new_file_has_eligible_rows(self) -> None:
        """Once B.pdf appears in df_slot for the CAO, snapped active file follows nominal B."""
        active = pd.DataFrame(
            [
                {"cao_number": 10, "salary_year": 2004, "file_name": "A.pdf"},
                {"cao_number": 10, "salary_year": 2005, "file_name": "B.pdf"},
            ]
        )
        df_slot = pd.DataFrame(
            {
                "cao_number": [10, 10],
                "file_name": ["A.pdf", "B.pdf"],
                "amount_monthly": [2000.0, 2500.0],
            }
        )
        out = snap_active_table_to_band_eligible_salary_files(active, df_slot)
        y4 = out.loc[out["salary_year"] == 2004, "file_name"].iloc[0]
        y5 = out.loc[out["salary_year"] == 2005, "file_name"].iloc[0]
        self.assertEqual(y4, "A.pdf")
        self.assertEqual(y5, "B.pdf")

    def test_snap_retains_cao_in_contract_calendar_panel_when_nominal_only_would_drop(self) -> None:
        """
        Inner merge on nominal (B) yields no rows in 2006; snapped (A) restores merge rows.
        """
        long_rows = pd.DataFrame(
            {
                "cao_number": [10],
                "file_name": ["A.pdf"],
                "row_id": [1],
                "salary_index": [1],
                "salary_start_date": pd.to_datetime(["2004-01-01"]),
                "amount_monthly": [2000.0],
                "analysis_monthly_band_ok": [True],
            }
        )
        nominal = pd.DataFrame(
            [
                {"cao_number": 10, "salary_year": 2005, "file_name": "A.pdf"},
                {"cao_number": 10, "salary_year": 2006, "file_name": "B.pdf"},
            ]
        )
        df_slot = pd.DataFrame({"cao_number": [10], "file_name": ["A.pdf"]})
        snapped = snap_active_table_to_band_eligible_salary_files(nominal, df_slot)
        out_nominal = build_contract_year_latest_salary_calendar_panel(long_rows, nominal)
        out_snapped = build_contract_year_latest_salary_calendar_panel(long_rows, snapped)
        self.assertEqual(len(out_nominal[out_nominal["calendar_year"] == 2006]), 0)
        self.assertEqual(len(out_snapped[out_snapped["calendar_year"] == 2006]), 1)


if __name__ == "__main__":
    unittest.main()
