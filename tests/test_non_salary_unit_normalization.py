"""
Unit tests for non-salary numeric unit normalization (plot canonical scales).

Run: conda run -n caos-extract python -m unittest tests.test_non_salary_unit_normalization
"""

import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.excel_analysis.non_salary_unit_normalization import normalize_for_plot


class TestNonSalaryUnitNormalization(unittest.TestCase):
    """Tests for normalize_for_plot per variable."""

    def test_contract_hours_per_week(self):
        self.assertEqual(
            normalize_for_plot(
                "contract_full_time_hours_value",
                38.0,
                "hours per week",
                default_ft_hours=38.0,
            ),
            38.0,
        )

    def test_contract_hours_annual_to_weekly(self):
        self.assertAlmostEqual(
            normalize_for_plot(
                "contract_full_time_hours_value",
                1976.0,
                "hours per calendar year",
                default_ft_hours=38.0,
            ),
            1976.0 / 52.0,
            places=4,
        )

    def test_contract_bare_percent_excluded(self):
        self.assertIsNone(
            normalize_for_plot(
                "contract_full_time_hours_value",
                100.0,
                "percent",
                default_ft_hours=38.0,
            )
        )

    def test_overtime_total_over_13_weeks(self):
        self.assertAlmostEqual(
            normalize_for_plot(
                "overtime_max_hours_per_week_value",
                52.0,
                "hours over 13 weeks",
                default_ft_hours=38.0,
            ),
            4.0,
            places=4,
        )

    def test_vacation_days_per_year_to_weekly_equiv(self):
        # 25 days/year * (38/5) hours/day / 52 weeks
        hpd = 38.0 / 5.0
        expected = (25.0 / 52.0) * hpd
        self.assertAlmostEqual(
            normalize_for_plot(
                "leave_vacation_time_value",
                25.0,
                "days per calendar year",
                default_ft_hours=38.0,
            ),
            expected,
            places=4,
        )

    def test_sickpay_duration_weeks(self):
        self.assertEqual(
            normalize_for_plot(
                "leave_sickpay_duration_value",
                104.0,
                "weeks at 100% pay",
                default_ft_hours=38.0,
            ),
            104.0,
        )

    def test_sickpay_duration_months(self):
        self.assertAlmostEqual(
            normalize_for_plot(
                "leave_sickpay_duration_value",
                6.0,
                "months",
                default_ft_hours=38.0,
            ),
            6.0 * 4.348,
            places=2,
        )

    def test_sickpay_continuation_percent_not_hours(self):
        self.assertEqual(
            normalize_for_plot(
                "leave_sickpay_continuation_value",
                70.0,
                "percent of gross wage for first 26 weeks",
                default_ft_hours=38.0,
            ),
            70.0,
        )

    def test_sickpay_continuation_tiered_prose_excluded(self):
        long_unit = (
            "percent of gross wage for the first 26 weeks; 90 percent for week 27 to 52; "
            "80 percent for week 53 to 78; 70 percent for week 79 to 104"
        )
        self.assertIsNone(
            normalize_for_plot(
                "leave_sickpay_continuation_value",
                100.0,
                long_unit,
                default_ft_hours=38.0,
            )
        )

    def test_pension_contrib_fraction_to_percent(self):
        self.assertAlmostEqual(
            normalize_for_plot(
                "pension_employee_contrib_value",
                0.25,
                "fraction of total premium",
                default_ft_hours=38.0,
            ),
            25.0,
            places=4,
        )

    def test_pension_contrib_eur_excluded(self):
        self.assertIsNone(
            normalize_for_plot(
                "pension_employee_contrib_value",
                120.0,
                "EUR",
                default_ft_hours=38.0,
            )
        )

    def test_retire_age_aow_unit(self):
        self.assertEqual(
            normalize_for_plot(
                "pension_retire_age_normal_value",
                67.0,
                "AOW-eligible age",
                default_ft_hours=38.0,
            ),
            67.0,
        )

    def test_retire_age_blank_unit_numeric(self):
        self.assertEqual(
            normalize_for_plot(
                "pension_retire_age_normal_value",
                66.0,
                None,
                default_ft_hours=38.0,
            ),
            66.0,
        )

    def test_training_hours_annually(self):
        self.assertAlmostEqual(
            normalize_for_plot(
                "training_time_yearly_value",
                52.0,
                "hours annually",
                default_ft_hours=38.0,
            ),
            1.0,
            places=4,
        )

    def test_blank_unit_excluded_for_vacation(self):
        self.assertIsNone(
            normalize_for_plot(
                "leave_vacation_time_value",
                20.0,
                "",
                default_ft_hours=38.0,
            )
        )

    def test_row_context_ft_for_percent_vacation(self):
        row = pd.Series(
            {
                "contract_full_time_hours_value": 36.0,
                "contract_full_time_hours_unit": "hours per week",
            }
        )
        self.assertAlmostEqual(
            normalize_for_plot(
                "leave_vacation_time_value",
                10.0,
                "percent of annual working hours",
                default_ft_hours=38.0,
                row=row,
            ),
            3.6,
            places=4,
        )


if __name__ == "__main__":
    unittest.main()
