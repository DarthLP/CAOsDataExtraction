"""Central path resolution for the salary_parser module.

Everything is resolved relative to THIS file, so the pipeline runs from any CWD.
Inputs are READ-ONLY; all output goes to the NEW outputs/parser_salary/ --
existing pipeline outputs are never touched.
"""
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# inputs (read-only)
EXT_ROOT = os.path.join(REPO_ROOT, 'outputs', 'llm_extracted', 'new_flow')
ANA_ROOT = os.path.join(REPO_ROOT, 'outputs', 'llm_analysis', 'salary')

# outputs (new, dedicated)
OUT_ROOT = os.path.join(REPO_ROOT, 'outputs', 'parser_salary')
ROLECHECK_DIR = os.path.join(OUT_ROOT, 'rolecheck')

os.makedirs(ROLECHECK_DIR, exist_ok=True)
