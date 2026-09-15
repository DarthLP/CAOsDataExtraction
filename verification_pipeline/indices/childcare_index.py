"""CHILDCARE — COVERAGE ONLY. Every numeric field is <2% filled and inhouse/discount/
priority booleans are ~0%, so a magnitude index is not meaningful. This reduces to a
presence signal: ~8% of CAOs mention any employer childcare support.
Run: python3.13 qa/indices/childcare_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = []                                  # too sparse for magnitude
BOOLEANS = ["childcare_childcare_support_present", "childcare_inhouse_present",
            "childcare_discount_present", "childcare_priority_access"]
SHORT = {}; CLAMP = {}; DESC = []

if __name__ == "__main__":
    il.build_simple_index("childcare", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "childcare_index.csv"),
                          os.path.join(il.OUT, "childcare_index_diagnostics.csv"))
