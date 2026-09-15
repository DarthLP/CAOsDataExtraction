"""BONUS — magnitude (13th-month % of annual, fixed annual lump €) + coverage.
13th-month 'months' units converted to % (1 month = 8.33% annual); fixed-lump %-bucket
-> descriptive. sign_on_bonus dropped (0% fill).
Run: python3.13 qa/indices/bonus_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = [("bonus_thirteenth_month_amt_value", "pct_annual", +1),
          ("bonus_fixed_annual_lump_value", "eur", +1)]
BOOLEANS = ["bonus_profit_sharing_present", "bonus_performance_bonus_present",
            "bonus_qual_bonus_present", "bonus_retire_gratuity_present",
            "bonus_seniority_loyalty_bonus", "bonus_job_allowances_present",
            "bonus_sign_on_bonus_present",
            # presence kept in coverage (2026-07-06, revised): bonus_thirteenth_month STAYS
            # in coverage even though the amount is zero-filled in magnitude — two-part model,
            # so the present-but-unquantified 13th-month CAOs (have it, no % stated) are still
            # counted. Earlier DISJOINT removal lost them; reverted.
            "bonus_thirteenth_month"]
SHORT = {"bonus_thirteenth_month_amt_value": "thirteenth_pct_annual",
         "bonus_fixed_annual_lump_value": "fixed_lump_eur"}
CLAMP = {"bonus_thirteenth_month_amt_value": (0, 30), "bonus_fixed_annual_lump_value": (0, 20000)}
DESC = [("bonus_fixed_lump_pct", "bonus_fixed_annual_lump_value",
         lambda v, u: v if (v is not None and il._PCT.search((u or "").lower())) else None)]

if __name__ == "__main__":
    # presence-gated zero-fill (2026-07-06): a missing 13th month = genuine absence = 0
    # when bonus_thirteenth_month is False (present in only 23% of CAOs, so discriminating)
    il.build_simple_index("bonus", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "bonus_index.csv"),
                          os.path.join(il.OUT, "bonus_index_diagnostics.csv"),
                          zerofill=[("bonus_thirteenth_month_amt_value", "bonus_thirteenth_month")])
