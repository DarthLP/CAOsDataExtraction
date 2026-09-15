"""FRINGE — magnitude (commuting €/km, meal €, relocation €) + coverage (perks present).
commuting per-month bucket -> descriptive (per-km vs per-month are different things).
Run: python3.13 qa/indices/fringe_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = [("fringe_commuting_allowance_value", "eur_per_km", +1),
          ("fringe_meal_benefit_amt_value", "eur", +1),
          ("fringe_relocation_allowance_value", "eur", +1)]
BOOLEANS = ["fringe_bike_scheme_present", "fringe_internet_or_phone_reimbursement_present",
            "fringe_health_insurance_support_present",
            "fringe_insurance_or_savings_benefit_present",
            "fringe_mandatory_certifications_paid",
            # presence kept in coverage (2026-07-06, revised): meal/relocation/commuting
            # presence booleans STAY in coverage even though their amount is zero-filled in
            # magnitude. This is a two-part model: coverage tracks PRESENCE (incl. the
            # present-but-unquantified CAOs — 17%/24% for meal/reloc — that magnitude can't
            # score), magnitude tracks AMOUNT (absent -> 0). A benefit appears in both
            # dimensions (breadth AND level); that is not double-counting since the two
            # roll-ups are reported separately. The earlier DISJOINT removal lost the
            # present-but-unquantified group, so it was reverted.
            "fringe_meal_benefit_present", "fringe_relocation_allowance_present",
            "fringe_commuting_allowance_present"]
SHORT = {"fringe_commuting_allowance_value": "commuting_eur_km",
         "fringe_meal_benefit_amt_value": "meal_eur", "fringe_relocation_allowance_value": "relocation_eur"}
CLAMP = {"fringe_commuting_allowance_value": (0, 2), "fringe_meal_benefit_amt_value": (0, 50),
         "fringe_relocation_allowance_value": (0, 50000)}
DESC = [("fringe_commuting_permonth_eur", "fringe_commuting_allowance_value",
         lambda v, u: v if (v is not None and il._EUR.search((u or "").lower()) and "month" in (u or "").lower()) else None)]

if __name__ == "__main__":
    # presence-gated zero-fill (2026-07-06): missing meal/relocation allowance = genuine
    # absence = 0 when the matching present-boolean is False (43%/39% present = discriminating)
    il.build_simple_index("fringe", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "fringe_index.csv"),
                          os.path.join(il.OUT, "fringe_index_diagnostics.csv"),
                          zerofill=[("fringe_meal_benefit_amt_value", "fringe_meal_benefit_present"),
                                    ("fringe_relocation_allowance_value", "fringe_relocation_allowance_present"),
                                    # commuting €/km: gated by its specific present-boolean (81% present)
                                    ("fringe_commuting_allowance_value", "fringe_commuting_allowance_present")])
