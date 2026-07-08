"""
Phase 6-lite: run the parser + role-check over ALL extract files (per-(CAO,file) output,
matching the deliverable CSV's shape) + duplicate-content diagnostic across a CAO's
version files. NO cross-version identity merging (Hanna 2026-07-03: renames/regrades make
versions non-comparable; canonical-version choice stays in the version-selection workstream).

Output: outputs/parser_salary/rolecheck_all/<cao>/<file>.json
        outputs/parser_salary/duplicate_files.csv   (exact + near-duplicate version files)
        outputs/parser_salary/all_files_summary.json
"""
import json, glob, os, re, sys, csv, hashlib, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import EXT_ROOT, ANA_ROOT, OUT_ROOT
import role_check as RC

ALL_DIR = os.path.join(OUT_ROOT, 'rolecheck_all')

def main():
    os.makedirs(ALL_DIR, exist_ok=True)
    caos = sorted([d for d in os.listdir(EXT_ROOT)
                   if re.fullmatch(r'\d+', d) and os.path.isdir(os.path.join(EXT_ROOT, d))], key=int)
    stats = collections.Counter(); tiers = collections.Counter()
    dup_rows = []
    for cao in caos:
        sigs = {}   # file -> (md5, amount-frozenset)
        cdir = os.path.join(ALL_DIR, cao); os.makedirs(cdir, exist_ok=True)
        for ep in sorted(glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json'))):
            base = os.path.basename(ep).replace('_extract.json', '')
            stats['files'] += 1
            try:
                ext = json.load(open(ep))
            except Exception:
                stats['unreadable'] += 1; continue
            wi = ext.get('wage_information', [])
            md5 = hashlib.md5(json.dumps(wi, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            ana = os.path.join(ANA_ROOT, cao, base + '_analysis.json')
            res = RC.rolecheck_file(ep, ana)
            if not res:
                stats['no_rows'] += 1
                sigs[base] = (md5, frozenset())
                # remove a STALE output from a previous run (this file no longer produces
                # rows, e.g. after a new exclusion) -- otherwise flatten picks it up again
                stale = os.path.join(cdir, base + '.json')
                if os.path.exists(stale):
                    os.remove(stale)
                continue
            stats['files_with_rows'] += 1
            amts = frozenset(round(p['amount'], 2) for r in res['rows'] for p in r['timeline'])
            sigs[base] = (md5, amts)
            for r in res['rows']:
                stats['rows'] += 1; tiers[r['role_verdict']] += 1
            json.dump({'cao': cao, 'file': base, 'rows': res['rows'],
                       'coverage': res.get('coverage', {}),
                       'table_verdicts': {str(k): v for k, v in res['verdicts'].items()}},
                      open(os.path.join(cdir, base + '.json'), 'w'), indent=1, ensure_ascii=False)
        # duplicate diagnostic within the CAO (flag, never merge)
        items = sorted(sigs.items())
        for i, (f1, (m1, a1)) in enumerate(items):
            for f2, (m2, a2) in items[i+1:]:
                if m1 == m2:
                    dup_rows.append((cao, f1, f2, 'exact', 1.0))
                elif a1 and a2:
                    j = len(a1 & a2) / len(a1 | a2)
                    if j >= 0.9:
                        dup_rows.append((cao, f1, f2, 'near', round(j, 3)))
    with open(os.path.join(OUT_ROOT, 'duplicate_files.csv'), 'w', newline='') as fh:
        w = csv.writer(fh); w.writerow(['cao', 'file_a', 'file_b', 'kind', 'jaccard'])
        w.writerows(dup_rows)
    summary = {'stats': dict(stats), 'row_verdicts': dict(tiers),
               'duplicate_pairs': len(dup_rows)}
    json.dump(summary, open(os.path.join(OUT_ROOT, 'all_files_summary.json'), 'w'), indent=1)
    print('===== PHASE 6-LITE (all files) =====')
    print(summary)

if __name__ == '__main__':
    main()
