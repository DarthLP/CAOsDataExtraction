"""
ONE-COMMAND reproducible delivery of the parser salary dataset.

    python3 salary_parser/deliver.py            # full run (~20-30 min)
    python3 salary_parser/deliver.py --quick    # first-file-per-CAO only (metrics rerun)

Stages (all deterministic, no LLM/agents at run time):
  1. run_all          parse every CAO (first file) + provenance/coverage audit
  2. count_gate       parser-vs-old-LLM per-file amount comparison (soft flags)
  3. role_check       label role cross-validation + worker/inc_pct enrichment
  4. confidence       row tiers A-D + CAO buckets
  5. all_files        role-check EVERY version file (skipped with --quick)
  6. flatten_csv      -> outputs/parser_salary/extracted_data_salary_v2.csv

Every run writes a timestamped log + a run manifest (row counts, provenance, tier
distribution) to outputs/parser_salary/runs/ so results are comparable across runs.
HARD GUARD: aborts if amount provenance is not 100%.
"""
import io, json, os, sys, time, contextlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from paths import OUT_ROOT, REPO_ROOT


def reapply_merges():
    """Re-apply the guarded label/agent improvements onto the freshly-generated
    rolecheck_all. all_files (stage 5) regenerates rolecheck_all from the parser, which
    WIPES the relabel + agent-extraction merges -- so they MUST be re-applied here, before
    flatten. Order: relabel (per-row label fixes) first, then agent-extract (whole-file
    re-extraction) so the stronger fix wins on any shared file. Every merge has an orphan
    guard, so re-running against re-parsed docs is safe (skips rows no longer present)."""
    import glob as _glob
    import relabel_merge, agent_extract
    rel_root = os.path.join(OUT_ROOT, 'relabel_merged')
    ag_root = os.path.join(OUT_ROOT, 'agent_extracted')
    for d in sorted(_glob.glob(os.path.join(rel_root, '*'))):
        if os.path.isdir(d):
            print('--- relabel_merge %s ---' % os.path.basename(d))
            relabel_merge.main(d)
    for d in sorted(_glob.glob(os.path.join(ag_root, '*'))):
        if os.path.isdir(d):
            print('--- agent_extract merge %s ---' % os.path.basename(d))
            agent_extract.merge_dir(d)


def run_stage(name, fn, log):
    t0 = time.time()
    print('\n===== %s =====' % name); log.write('\n===== %s =====\n' % name)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        fn()
    out = buf.getvalue()
    print(out, end=''); log.write(out)
    log.write('[%s took %.1fs]\n' % (name, time.time() - t0))
    return out


def main():
    quick = '--quick' in sys.argv
    sys.argv = sys.argv[:1]        # stage scripts read sys.argv -- don't leak our flags
    os.chdir(REPO_ROOT)
    runs_dir = os.path.join(OUT_ROOT, 'runs'); os.makedirs(runs_dir, exist_ok=True)
    stamp = time.strftime('%Y%m%d_%H%M%S')
    log = open(os.path.join(runs_dir, 'run_%s.log' % stamp), 'w')

    import run_all, count_gate, role_check, confidence
    out1 = run_stage('1/6 parse + audit', run_all.main, log)
    # HARD GUARD: the core guarantee
    if 'AMOUNT provenance: 100.00%' not in out1:
        log.write('\nABORT: amount provenance below 100%!\n'); log.close()
        raise SystemExit('ABORT: amount provenance below 100% -- investigate before delivering.')
    run_stage('2/6 count gate', count_gate.main, log)
    run_stage('3/6 role-check merge', role_check.main, log)
    run_stage('4/6 confidence roll-up', confidence.main, log)
    if not quick:
        import all_files, flatten_csv
        run_stage('5/6 all version files', all_files.main, log)
        # re-apply guarded relabel + agent-extraction merges onto the fresh rolecheck_all
        # (all_files just regenerated it from the parser, wiping the prior merges).
        run_stage('5.5/6 re-apply relabel + agent merges', reapply_merges, log)
        out6 = run_stage('6/6 flatten CSV', flatten_csv.main, log)
    else:
        print('\n(--quick: skipped stages 5-6; CSV not refreshed)')
        log.write('\n--quick run: stages 5-6 skipped\n')

    manifest = {
        'stamp': stamp, 'quick': quick,
        'summary': json.load(open(os.path.join(OUT_ROOT, 'all_files_summary.json')))
                   if os.path.exists(os.path.join(OUT_ROOT, 'all_files_summary.json')) else None,
    }
    json.dump(manifest, open(os.path.join(runs_dir, 'run_%s_manifest.json' % stamp), 'w'), indent=1)
    log.close()
    print('\nDELIVERY COMPLETE. Log + manifest in %s' % runs_dir)
    print('Dataset: %s/extracted_data_salary_v2.csv' % OUT_ROOT)


if __name__ == '__main__':
    main()
