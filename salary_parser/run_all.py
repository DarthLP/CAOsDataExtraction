"""
All-CAO test harness. One file per CAO; if the chosen file yields no salary rows,
try the next file of the SAME CAO (some files genuinely have no salary tables).
Reports per-CAO provenance + coverage and an aggregate, and flags problems.
Read-only. Usage: python3 run_all.py [N_CAOS]   (default: all)
"""
import json, glob, os, re, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import salary_parser as SP
from source_audit import raw_number_universe, header_date_universe

from paths import EXT_ROOT, OUT_ROOT

def has_any_pipe_table(ext):
    for blk in ext.get('wage_information', []):
        if SP.block_to_table(blk): return True
    return False

def audit_rows(ext, rows, diags):
    num_u = raw_number_universe(ext); date_u = header_date_universe(ext)
    # rescaled points (tagged mangled-decimal derivations, e.g. 2.18454->2184.54) are
    # counted SEPARATELY: they are documented digit-preserving derivations, deliberately
    # not present verbatim in source -- not provenance violations, but never hidden either.
    n_rescaled = sum(1 for r in rows for p in r['timeline']
                     if p.get('value_source') == 'rescaled')
    amts = [round(p['amount'],2) for r in rows for p in r['timeline']
            if p.get('value_source') != 'rescaled']
    dates = [p['start_date'] for r in rows for p in r['timeline']]
    amt_bad = [a for a in amts if a not in num_u]
    # None = honestly-undated point (no doc-date fallback, by design) -> reported as
    # date_null rate, NOT a provenance violation
    date_bad = [d for d in dates if d is not None and d not in date_u]
    date_null = sum(1 for d in dates if d is None)
    incl = [d for d in diags if d['included'] and d.get('seen_cells')]
    seen = sum(d['seen_cells'] for d in incl); emit = sum(d['emitted_points'] for d in incl)
    def benign(d):
        s,e=d.get('seen_cells',0),d.get('emitted_points',0)
        return e and any(abs(s-k*e)<=max(2,0.1*k*e) for k in (2,3,4,5))
    lowcov = [(d['desc'][:46], d['coverage']) for d in incl
              if d.get('coverage') is not None and d['coverage']<0.6 and not benign(d)]
    return {
        'points': len(amts) + n_rescaled,
        'rescaled': n_rescaled,
        'amt_prov': 1.0 if not amts else 1-len(amt_bad)/len(amts),
        'date_prov': 1.0 if not dates else 1-len(date_bad)/len(dates),
        'date_null': date_null,
        'seen': seen, 'emit': emit, 'amt_bad': amt_bad[:5], 'lowcov': lowcov[:4],
    }

def pick_and_parse(cao):
    files = sorted(glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json')))
    chosen = None; result = None; tried = 0
    # prefer first file that yields rows; remember first file-with-a-table as fallback
    fallback = None
    for f in files:
        ext = json.load(open(f)); tried += 1
        rows, diags, dd = SP.parse_extract(ext)
        if has_any_pipe_table(ext) and fallback is None:
            fallback = (f, ext, rows, diags, dd)
        if rows:
            chosen = (f, ext, rows, diags, dd); break
    if not chosen: chosen = fallback
    if not chosen:
        # no file in this CAO has any pipe table at all
        f0 = files[0] if files else None
        return {'cao':cao,'file':os.path.basename(f0) if f0 else None,'n_files':len(files),
                'tried':tried,'rows':0,'tableless':True}
    f, ext, rows, diags, dd = chosen
    a = audit_rows(ext, rows, diags)
    return {'cao':cao,'file':os.path.basename(f).replace('_extract.json',''),
            'n_files':len(files),'tried':tried,'rows':len(rows),'doc_date':dd,
            'tableless':not has_any_pipe_table(ext) and len(rows)==0, **a}

def main():
    caos = sorted([d for d in os.listdir(EXT_ROOT)
                   if re.fullmatch(r'\d+', d) and os.path.isdir(os.path.join(EXT_ROOT,d))],
                  key=int)
    if len(sys.argv)>1: caos = caos[:int(sys.argv[1])]
    reps = [pick_and_parse(c) for c in caos]
    json.dump(reps, open(os.path.join(OUT_ROOT,'run_all.json'),'w'),
              indent=1, ensure_ascii=False)

    nz = [r for r in reps if r['rows']>0]
    tableless = [r for r in reps if r.get('tableless')]
    zero_with_table = [r for r in reps if r['rows']==0 and not r.get('tableless')]
    tot_pts = sum(r['points'] for r in nz)
    amt_ok = sum(r['amt_prov']*r['points'] for r in nz)/max(tot_pts,1)
    date_ok = sum(r['date_prov']*r['points'] for r in nz)/max(tot_pts,1)
    seen = sum(r['seen'] for r in nz); emit = sum(r['emit'] for r in nz)
    prov_violations = [r for r in nz if r['amt_prov']<1.0]
    print('===== ALL-CAO RUN: %d CAOs (%d produced rows, %d table-less, %d zero-but-has-table) ====='%(
        len(reps), len(nz), len(tableless), len(zero_with_table)))
    print('  total points: %d'%tot_pts)
    print('  AMOUNT provenance: %.2f%%   (CAOs with any invented number: %d)'%(100*amt_ok, len(prov_violations)))
    n_resc = sum(r.get('rescaled',0) for r in nz)
    if n_resc:
        print('  rescued mangled-decimal points: %d (digit-preserving rescale, tagged value_source=rescaled, audited separately)'%n_resc)
    null_dates = sum(r.get('date_null',0) for r in nz)
    print('  DATE   provenance: %.2f%% of dated points   (undated-by-design: %d pts, %.1f%%)'%(
        100*date_ok, null_dates, 100*null_dates/max(tot_pts,1)))
    print('  COVERAGE: %.1f%% (%d/%d)'%(100*emit/max(seen,1), emit, seen))
    if prov_violations:
        print('  !! PROVENANCE VIOLATIONS (investigate):')
        for r in prov_violations[:10]:
            print('     CAO %s %s amt_prov=%.0f%% bad=%s'%(r['cao'],r['file'][:30],100*r['amt_prov'],r['amt_bad']))
    print('  CAOs flagged with genuine low-coverage tables:')
    lc = [(r['cao'],r['file'],r['lowcov']) for r in nz if r['lowcov']]
    for cao,f,lcv in lc[:15]:
        print('     CAO %-5s %-30s %s'%(cao, f[:30], [(t,round(c,2)) for t,c in lcv]))
    print('  zero-rows-but-has-a-pipe-table (possible misses to triage): %s'%(
        [r['cao'] for r in zero_with_table][:25]))
    print('  table-less CAOs (no pipe tables in tried files): %s'%([r['cao'] for r in tableless][:25]))

if __name__=='__main__':
    main()
