"""PENSION — magnitude (employee contribution share, accrual rate, franchise, early-
retirement age) + coverage (excedent top-up). THE HARD ONE.

CAVEAT (read before using): most CAOs defer pensions to a sector fund (bpfBOUW/ABP/
PFZW/PMT) and state no figures, so these fields are 14-38% filled and the blanks are
FUND-DEFERRAL, not statutory. We therefore do NOT impute statutory values into blanks
(unlike leave/term) — the index is strictly available-case and reflects only the
MINORITY of CAOs that state their own pension terms (a non-random subset, e.g. company
schemes). Plausibility clamps catch contamination (notably accrual=100%, the
Generatiepact '100% accrual maintained' misread). retire_age_normal is descriptive only
(mostly the statutory AOW/pensioenrichtleeftijd, not CAO generosity).
Signs: employee_contrib(-, lower share=more generous), accrual(+), franchise(-, lower=
more pensionable pay), early_retire_age(-, earlier=more generous).
Run: python3.13 qa/indices/pension_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = [("pension_employee_contrib_value", "percent", -1),
          ("pension_accrual_rate_value", "percent", +1),
          ("pension_franchise_value", "eur", -1),
          ("pension_retirement_age_early_value", "count", -1)]
BOOLEANS = ["pension_excedent_present",
            # Tier-2 2026-07-05: accrual continuation during statutory leaves / illness-yr-2
            # and a 50/50 premium split are pure bargaining (no statute). CAVEAT on
            # mandatory_participation: in verplichtgesteld-bpf sectors participation is
            # mandatory BY LAW (Wet Bpf 2000) even if the CAO is silent — coverage
            # understates the legal reality (statutory_all informational row).
            "pension_accrual_stat_leaves", "pension_accrual_illness_y2",
            "pension_premium_eq_split", "pension_mandatory_participation"]
SHORT = {"pension_employee_contrib_value": "employee_contrib_pct",
         "pension_accrual_rate_value": "accrual_rate_pct",
         "pension_franchise_value": "franchise_eur",
         "pension_retirement_age_early_value": "early_retire_age"}
CLAMP = {"pension_employee_contrib_value": (0, 100), "pension_accrual_rate_value": (0, 2.5),
         "pension_franchise_value": (5000, 50000), "pension_retirement_age_early_value": (50, 67)}
DESC = [("pension_retire_age_normal", "pension_retire_age_normal_value",
         lambda v, u: v if (v is not None and 50 <= v <= 75) else None)]

if __name__ == "__main__":
    il.build_simple_index("pension", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "pension_index.csv"),
                          os.path.join(il.OUT, "pension_index_diagnostics.csv"),
                          # presence of a deferred-retirement option (value 1.5% filled —
                          # too thin for a magnitude z, but the rare presence IS the signal):
                          derived_bools=[("pension_retire_age_deferred_present",
                                          "pension_retire_age_deferred_value", None)],
                          passthrough=["pension_pension_type"])  # DB/DC descriptive (no sign: Wtp era-dependent)
