"""
ABSENCE — vacation, sick pay and care leave (v2 Tier-1 addition, 2026-07-05).

The best-filled generosity fields in the dataset that no index consumed before:
vacation days (97%), holiday-allowance % (95%), sick-pay continuation % (94%) and
duration (94%), short-term care leave days/pay (52/58%), long-term care leave
weeks/pay (33/25%). All have statutory floors (era-aware, in statutory_all.csv:
20 days vacation, 8% vakantiegeld, 70% sick pay for 52/104 weeks, WAZO care
leave since 2001/2005) that are imputed into blanks in the full variant — the
same empty=statutory doctrine as parental leave.

Signs: all +1 (more days/weeks/percent = more worker-generous).
Sick-pay % stores the FIRST tier per convention C3 (year-1 rate).
Run: python3.13 indices/absence_index.py
"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = [
    ("leave_vacation_time_value",        "days_per_year", +1),
    ("leave_vacation_bonus_value",       "percent",       +1),
    ("leave_sickpay_continuation_value", "percent",       +1),
    ("leave_sickpay_duration_value",     "weeks",         +1),
    ("leave_short_term_care_value",      "days_per_year", +1),
    ("leave_short_term_care_pay_value",  "percent",       +1),
    ("leave_long_term_care_value",       "weeks",         +1),
    ("leave_long_term_care_pay_value",   "percent",       +1),
]
# absence coverage (moved from leave 2026-07-08): the sickness/care top-up presence booleans —
# they belong to absence's sickpay/care concepts. Makes absence a DUAL (numeric + coverage) topic.
BOOLEANS = ["leave_sick_topup_present", "leave_sickpay_extra_insurance_present",
            "leave_care_topup_present"]
SHORT = {
    "leave_vacation_time_value": "vacation_days_yr",
    "leave_vacation_bonus_value": "vacation_bonus_pct",
    "leave_sickpay_continuation_value": "sickpay_pct",
    "leave_sickpay_duration_value": "sickpay_wks",
    "leave_short_term_care_value": "stcare_days_yr",
    "leave_short_term_care_pay_value": "stcare_pay_pct",
    "leave_long_term_care_value": "ltcare_wks",
    "leave_long_term_care_pay_value": "ltcare_pay_pct",
}
CLAMP = {
    "leave_vacation_time_value": (5, 60),          # days/yr; mis-unit'd hours-as-days fall out
    "leave_vacation_bonus_value": (0, 20),         # %; absolute-EUR payouts are unit-dropped
    "leave_sickpay_continuation_value": (30, 100), # % year-1 rate
    "leave_sickpay_duration_value": (4, 260),      # weeks
    "leave_short_term_care_value": (0, 30),        # days/yr
    "leave_short_term_care_pay_value": (0, 100),
    "leave_long_term_care_value": (0, 52),         # weeks
    "leave_long_term_care_pay_value": (0, 100),
}

if __name__ == "__main__":
    il.build_simple_index("absence", FIELDS, BOOLEANS, SHORT, CLAMP, [],
                          os.path.join(il.OUT, "absence_index.csv"),
                          os.path.join(il.OUT, "absence_index_diagnostics.csv"))
