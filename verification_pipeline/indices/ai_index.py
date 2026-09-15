"""AI — COVERAGE index, NEAR-EMPTY. Only ~1% of CAOs have any AI provision yet
(policy 1%, governance 0%, training-rights 0%) — AI in CAOs is nascent (2024+).
Built as a presence flag; not a meaningful generosity index until the data fills in.
Run: python3.13 qa/indices/ai_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = []
BOOLEANS = ["ai_ai_policy_exists", "ai_ai_governance_body_present", "ai_ai_training_rights_present"]
SHORT = {}; CLAMP = {}; DESC = []

if __name__ == "__main__":
    il.build_simple_index("ai", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "ai_index.csv"),
                          os.path.join(il.OUT, "ai_index_diagnostics.csv"))
