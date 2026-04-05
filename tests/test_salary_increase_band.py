"""
Unit tests for NL statutory monthly floor lookup and salary increase band filtering.

Run: conda run -n caos-extract python -m unittest tests.test_salary_increase_band
"""

import os
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.excel_analysis.analysis_utils import SALARY_ANALYSIS_MONTHLY_CAP_EUR
from scripts.excel_analysis.nl_minimum_wage_monthly import (
    minimum_monthly_gross_eur,
    minimum_monthly_gross_eur_series,
)
from scripts.excel_analysis.salary_increase_derivation import (
    compute_analysis_monthly_floor_and_band_ok,
    derive_salary_increase_series,
)


class TestNLMinimumWageMonthly(unittest.TestCase):
    """Statutory floor lookup from conf CSV."""

    def test_lookup_jan_2020(self):
        """2020-06-15 uses the 2020-01-01 monthly amount."""
        v = minimum_monthly_gross_eur(pd.Timestamp("2020-06-15"))
        self.assertAlmostEqual(v, 1653.60, places=2)

    def test_before_1990_clamps_to_first_row(self):
        """Dates before first schedule row use the 1990 minimum."""
        v1985 = minimum_monthly_gross_eur(pd.Timestamp("1985-01-01"))
        v1990 = minimum_monthly_gross_eur(pd.Timestamp("1990-01-01"))
        self.assertAlmostEqual(v1985, v1990, places=2)

    def test_after_2025_uses_last_row(self):
        """Dates on or after last valid_from use the 2025 amount."""
        v = minimum_monthly_gross_eur(pd.Timestamp("2030-01-01"))
        self.assertAlmostEqual(v, 2312.87, places=2)

    def test_nat_returns_nan(self):
        self.assertTrue(np.isnan(minimum_monthly_gross_eur(pd.NaT)))

    def test_series_vectorized_matches_scalar(self):
        s = pd.Series([pd.Timestamp("2022-01-01"), pd.NaT])
        arr = minimum_monthly_gross_eur_series(s)
        self.assertAlmostEqual(arr[0], 1725.00, places=2)
        self.assertTrue(np.isnan(arr[1]))


class TestSalaryIncreaseBandDerivation(unittest.TestCase):
    """Band flags and diff masking in derive_salary_increase_series."""

    def test_below_floor_yields_nan_diff_and_band_summary(self):
        """Second step below statutory floor: diff NaN, merged NaN, summary counts below_floor."""
        df = pd.DataFrame(
            {
                "cao_number": [1],
                "file_name": ["f.pdf"],
                "salary_1_start_date": ["2020-01-01"],
                "salary_1_amount": [3000.0],
                "salary_1_unit": ["monthly"],
                "salary_2_start_date": ["2020-06-01"],
                "salary_2_amount": [500.0],
                "salary_2_unit": ["monthly"],
                "salary_2_increase_percent": [2.5],
            }
        )
        payload = derive_salary_increase_series(df)
        ev = payload["events"]
        self.assertIn("analysis_monthly_band_ok", ev.columns)
        self.assertIn("band_summary", payload)
        # First event in band, second below 2020 minimum (~1653.6)
        self.assertTrue(ev.iloc[0]["analysis_monthly_band_ok"])
        self.assertFalse(ev.iloc[1]["analysis_monthly_band_ok"])
        self.assertTrue(np.isnan(ev.iloc[1]["increase_diff_only"]))
        self.assertAlmostEqual(float(ev.iloc[1]["increase_merged_pref_csv"]), 2.5, places=5)
        bs = payload["band_summary"]
        self.assertGreaterEqual(bs["n_dropped_below_floor"], 1)
        self.assertEqual(bs["n_dropped_above_cap"], 0)

    def test_above_cap_nans_band(self):
        """Monthly amount above cap is excluded from band and merged series."""
        hi = SALARY_ANALYSIS_MONTHLY_CAP_EUR + 1000.0
        df = pd.DataFrame(
            {
                "cao_number": [1],
                "file_name": ["f.pdf"],
                "salary_1_start_date": ["2020-01-01"],
                "salary_1_amount": [hi],
                "salary_1_unit": ["monthly"],
                "salary_1_increase_percent": [1.25],
            }
        )
        payload = derive_salary_increase_series(df)
        ev = payload["events"]
        self.assertFalse(ev.iloc[0]["analysis_monthly_band_ok"])
        self.assertAlmostEqual(float(ev.iloc[0]["increase_merged_pref_csv"]), 1.25, places=5)
        self.assertGreaterEqual(payload["band_summary"]["n_dropped_above_cap"], 1)

    def test_compute_analysis_monthly_floor_and_band_ok_matches_derive(self):
        """Shared band helper agrees with derive_salary_increase_series on events."""
        df = pd.DataFrame(
            {
                "cao_number": [1],
                "file_name": ["f.pdf"],
                "salary_1_start_date": ["2020-01-01"],
                "salary_1_amount": [3000.0],
                "salary_1_unit": ["monthly"],
                "salary_2_start_date": ["2020-06-01"],
                "salary_2_amount": [500.0],
                "salary_2_unit": ["monthly"],
            }
        )
        ev = derive_salary_increase_series(df)["events"]
        floor_arr, band_ok = compute_analysis_monthly_floor_and_band_ok(
            ev["conversion_ok"].to_numpy(),
            ev["salary_start_date"],
            ev["amount_monthly"],
        )
        self.assertEqual(len(floor_arr), len(ev))
        np.testing.assert_allclose(
            floor_arr,
            ev["analysis_monthly_floor_eur"].to_numpy(),
            rtol=0,
            atol=0.01,
        )
        np.testing.assert_array_equal(band_ok, ev["analysis_monthly_band_ok"].to_numpy())


if __name__ == "__main__":
    unittest.main()
