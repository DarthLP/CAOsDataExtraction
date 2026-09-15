"""HOMEOFFICE — thin magnitude (stipend €/month, WFH days/week) + coverage.
Magnitude is sparse (stipend 7%, entitlement 1% filled) — coverage (right to WFH) is
the main signal. Stipend per-day normalised to €/month (x21.7).
Run: python3.13 qa/indices/homeoffice_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

# entitlement DROPPED from magnitude (2026-07-06): only 1.3% of docs state a WFH-days number,
# so it was a presence signal not a magnitude; its "can you WFH" info stays in coverage via
# homeoffice_has_homeoffice_rights.
FIELDS = [("homeoffice_stipend_value", "eur_per_month", +1)]
# stipend_present kept in coverage (2026-07-06, revised): two-part model — coverage tracks
# stipend PRESENCE (incl. the 12 present-but-unquantified CAOs that the zero-filled magnitude
# can't score), magnitude tracks the amount (absent -> 0). Earlier DISJOINT removal lost them.
BOOLEANS = ["homeoffice_has_homeoffice_rights", "homeoffice_costs_reimbursed",
            "homeoffice_stipend_present"]
SHORT = {"homeoffice_stipend_value": "stipend_eur_mo"}
CLAMP = {"homeoffice_stipend_value": (0, 2000)}
DESC = []

if __name__ == "__main__":
    il.build_simple_index("homeoffice", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "homeoffice_index.csv"),
                          os.path.join(il.OUT, "homeoffice_index_diagnostics.csv"),
                          # Tier-3 dummy: WFH at employee's request (vs employer-only/unspecified)
                          derived_bools=[("homeoffice_employee_discretion",
                                          "homeoffice_discretion", {"employee_request"})],
                          passthrough=["homeoffice_discretion"],
                          # presence-gated zero-fill (2026-07-06): missing stipend = genuine
                          # absence = 0 when homeoffice_stipend_present is False (stipend-SPECIFIC
                          # boolean). NOT entitlement: it has no specific boolean (only the broad
                          # has_homeoffice_rights) AND only ~1% of docs state a WFH-days number, so
                          # zero-filling it would fabricate a magnitude that just mirrors coverage.
                          zerofill=[("homeoffice_stipend_value", "homeoffice_stipend_present")])
