"""SAFETY — COVERAGE index (12 boolean provisions; no numeric fields exist).
Share of safety/well-being provisions present (harassment & integrity protocols,
confidential counsellor, RI&E/PSA, arbodienst access, medical checkups, workload &
wellbeing, etc.). CAVEAT: several (RI&E, arbodienst, PSA prevention) are Arbowet-
mandated, so 'absent in text' != 'absent in fact' — coverage understates the legal floor.
Run: python3.13 qa/indices/safety_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = []
BOOLEANS = ["safety_harassment_protocol_present", "safety_integrity_protocol_present",
            "safety_confidential_counsellor_present", "safety_reporting_channel_external",
            "safety_safety_training_present", "safety_safety_committee_present",
            "safety_rie_psa_required", "safety_psa_prevention_measures_present",
            "safety_arbodienst_access_provided", "safety_preventive_medical_checkup_present",
            "safety_workload_monitoring_present", "safety_wellbeing_program_present"]
SHORT = {}; CLAMP = {}; DESC = []

if __name__ == "__main__":
    il.build_simple_index("safety", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "safety_index.csv"),
                          os.path.join(il.OUT, "safety_index_diagnostics.csv"))
