"""
One-command salary-parser pipeline (deterministic augmentation of p4's salary step).

  python3 salary_parser/run_parser_pipeline.py            # full corpus
  python3 salary_parser/run_parser_pipeline.py 51 1618    # role-check specific CAOs, verbose

Stages (all read-only on existing outputs; everything written to outputs/parser_salary/):
  1. run_all      -- parse every CAO, source-grounded audit (amount/date provenance, coverage)
  2. count_gate   -- per-file parser-vs-old-LLM amount counts (SOFT flag, Haiku adjudicates)
  3. role_check   -- label cross-validation vs old-LLM vocabularies + worker/inc_pct
                     enrichment -> outputs/parser_salary/rolecheck/<cao>.json

See PLAN_phase3_rolecheck.md (design) and the project memo for verdict semantics.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    args = sys.argv[1:]
    if args:                       # targeted verbose role-check only
        import role_check
        role_check.main(args, verbose=True)
        return
    import run_all
    print('===== 1/3 parse + source audit =====')
    run_all.main()
    print('\n===== 2/3 count gate (soft flags) =====')
    import count_gate
    count_gate.main()
    print('\n===== 3/4 role-check merge =====')
    import role_check
    role_check.main()
    print('\n===== 4/4 confidence roll-up =====')
    import confidence
    confidence.main()
    print('\nFirst-file pipeline done. For the FULL deliverable run:')
    print('  python3 all_files.py     # role-check every version file')
    print('  python3 flatten_csv.py   # -> outputs/parser_salary/extracted_data_salary_v2.csv')

if __name__ == '__main__':
    main()
