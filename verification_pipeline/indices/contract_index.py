"""
CONTRACT — security/flexibility index (v2: pooled z per file, file_date axis).
Magnitude fields (sign): ketenregeling_max_contracts(-), ketenregeling_max_duration(-),
full_time_hours(-), workhours_adjustment_tenure_requirement(-).
ketenregeling has a statutory DEFAULT (3 contracts; era duration 36->24->36) imputed
into blanks in the full variant. Coverage = conversion-right + workhours-adjustment.
Run:  python3.13 indices/contract_index.py
"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = [
    ("contract_ketenregeling_max_contracts_value",            "contracts",      -1),
    ("contract_ketenregeling_max_duration_value",             "months",         -1),
    ("contract_full_time_hours_value",                        "hours_per_week", -1),
    ("contract_workhours_adjustment_tenure_requirement_value","months",         -1),
]
BOOLEANS = ["contract_conversion_rights_temp_to_perm_present", "contract_workhours_adjustment_right_present",
            # Tier-2 2026-07-05. CAVEAT: the right to REQUEST hours adjustment is LAW since
            # 2000 (WAA -> Wet flexibel werken) — part_time_allowed partly restates it.
            # zero_hour_oncall_allowed / minmax_allowed excluded: ambiguous worker-generosity sign.
            "contract_part_time_allowed"]
SHORT = {
    "contract_ketenregeling_max_contracts_value": "keten_max_contracts",
    "contract_ketenregeling_max_duration_value": "keten_max_duration_mo",
    "contract_full_time_hours_value": "fulltime_hours_wk",
    "contract_workhours_adjustment_tenure_requirement_value": "wh_adjust_tenure_mo",
}
CLAMP = {
    "contract_ketenregeling_max_contracts_value": (1, 15),
    "contract_ketenregeling_max_duration_value": (1, 120),
    "contract_full_time_hours_value": (10, 60),
    "contract_workhours_adjustment_tenure_requirement_value": (0, 120),
}

if __name__ == "__main__":
    il.build_simple_index("contract", FIELDS, BOOLEANS, SHORT, CLAMP, [],
                          os.path.join(il.OUT, "contract_index.csv"),
                          os.path.join(il.OUT, "contract_index_diagnostics.csv"))
