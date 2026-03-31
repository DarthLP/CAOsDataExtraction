"""
Unit tests for salary analysis helpers: amount coercion and monthly normalization.

Covers compact pay-period codes (m, h, 4-w, w, d, a), daily wording, European decimal strings,
and full-time hours interpreted as annual vs weekly before hourly conversion.

Run: conda run -n caos-extract python -m unittest tests.test_salary_analysis_utils
"""

import os
import sys
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.excel_analysis.analysis_utils import (
    coerce_ft_hours_per_week_for_conversion,
    coerce_salary_amount_scalar,
    convert_salary_to_monthly,
    is_plausible_monthly_equivalent,
)


class TestSalaryAnalysisUtils(unittest.TestCase):
    """Tests for coerce_salary_amount_scalar, ft-hours handling, and convert_salary_to_monthly."""

    def test_coerce_salary_amount_european_decimals(self):
        """European-style decimals and thousands separators parse to floats."""
        self.assertEqual(coerce_salary_amount_scalar("1.234,56"), 1234.56)
        self.assertEqual(coerce_salary_amount_scalar("2500,50"), 2500.50)
        self.assertEqual(coerce_salary_amount_scalar(3000), 3000.0)
        self.assertIsNone(coerce_salary_amount_scalar(""))

    def test_coerce_salary_amount_us_thousands(self):
        """US-style comma thousands with dot decimal parse correctly (distinct from NL)."""
        self.assertEqual(coerce_salary_amount_scalar("2,230.91"), 2230.91)
        self.assertEqual(coerce_salary_amount_scalar("12,345.67"), 12345.67)

    def test_coerce_ft_hours_annual_to_weekly(self):
        """Values above annual threshold are divided by 52; weekly preserved."""
        self.assertAlmostEqual(
            coerce_ft_hours_per_week_for_conversion(1872.0), 1872.0 / 52.0, places=5
        )
        self.assertEqual(coerce_ft_hours_per_week_for_conversion(36.0), 36.0)

    def test_convert_compact_units_monthly_equivalent(self):
        """Compact schema units map to expected monthly scale (approx)."""
        hourly = 20.0
        hweek = 40.0
        month_from_h = convert_salary_to_monthly(hourly, "h", hweek)
        self.assertIsNotNone(month_from_h)
        self.assertAlmostEqual(month_from_h, hourly * hweek * 4.33, places=3)

        self.assertEqual(convert_salary_to_monthly(3000.0, "m", None), 3000.0)

        fourw = convert_salary_to_monthly(400.0, "4-w", None)
        self.assertIsNotNone(fourw)
        self.assertAlmostEqual(fourw, round(400.0 * (12.0 / 13.0), 2), places=5)

        week = convert_salary_to_monthly(500.0, "w", None)
        self.assertIsNotNone(week)
        self.assertAlmostEqual(week, 500.0 * 4.33, places=3)

        day_rate = 100.0
        month_from_d = convert_salary_to_monthly(day_rate, "d", None)
        self.assertIsNotNone(month_from_d)
        self.assertAlmostEqual(month_from_d, day_rate * 5.0 * 4.33, places=3)

        month_from_daily = convert_salary_to_monthly(day_rate, "daily", None)
        self.assertIsNotNone(month_from_daily)
        self.assertAlmostEqual(month_from_daily, month_from_d, places=5)

        self.assertEqual(
            convert_salary_to_monthly("2.230,91", "4-week", None),
            round(2230.91 * (12.0 / 13.0), 2),
        )

        annual = convert_salary_to_monthly(60000.0, "a", None)
        self.assertIsNotNone(annual)
        self.assertAlmostEqual(annual, 5000.0, places=3)

    def test_three_hour_activity_not_hourly(self):
        """Duration-style ``N-hour activity`` must not use hourly €/h × hours/month formula."""
        self.assertIsNone(convert_salary_to_monthly(80.0, "3-hour activity", 40.0))
        self.assertIsNone(convert_salary_to_monthly(80.0, "3-hour acti", 40.0))

    def test_offshore_day_like_daily(self):
        """Offshore day labels use the same working-day scaling as ``daily``."""
        rate = 100.0
        expected = round(rate * 5.0 * 4.33, 2)
        for unit in (
            "offshore day",
            "offshore da",
            "per offshore day",
            "per offshore da",
            "per offshore",
        ):
            got = convert_salary_to_monthly(rate, unit, None)
            self.assertIsNotNone(got, msg=unit)
            self.assertAlmostEqual(got, expected, places=2, msg=unit)

    def test_plausible_range_guard(self):
        """Plausible-range helper rejects None and extreme values."""
        self.assertTrue(is_plausible_monthly_equivalent(5000.0))
        self.assertFalse(is_plausible_monthly_equivalent(0.0))
        self.assertFalse(is_plausible_monthly_equivalent(200_000.0))


if __name__ == "__main__":
    unittest.main()
