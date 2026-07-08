"""
SOURCE-GROUNDED audit — ground truth is the first-LLM extract (the table grid),
NOT the error-prone llm_analysis. Works for ANY CAO number.

Checks the parser invents nothing and dates are provenance-backed:
  - VALUE PROVENANCE: every parser amount must appear as a numeric token in the
    raw extract text of that file. (catches misparses / hallucination)
  - DATE PROVENANCE: every parser point's date must be derivable from its cell's
    column header, its table description, or the document effective date.
  - CELL COVERAGE: of the salary cells in INCLUDED tables (preferred-unit value
    columns, adult rows), what fraction did the parser emit? (catches drops)
Usage: python3 source_audit.py <cao_number> [<cao_number> ...]
"""
import json, glob, os, re, sys, collections
sys.path.insert(0, os.path.dirname(__file__))
import salary_parser as SP

from paths import EXT_ROOT

def raw_number_universe(extract):
    toks = set()
    for blk in extract.get('wage_information', []):
        if not isinstance(blk, list): continue
        for line in SP.flatten_lines(blk):
            if not isinstance(line, str): continue
            for cell in line.split('|'):
                v = SP.parse_num(cell.strip())
                if v is not None: toks.add(round(v, 2))
    return toks

def header_date_universe(extract):
    """all dates derivable from any column header, description, ROW-AXIS cell, + doc date.
    Row cells matter: some tables carry the effective date as a row key ('per 1-10-2011'),
    which parse_extract legitimately uses as start_date -- without scanning cells here,
    those dates were falsely flagged unprovenanced (CAO 156)."""
    ds = set()
    dd = SP.doc_effective_date(extract)
    if dd: ds.add(dd)
    for blk in extract.get('wage_information', []):
        t = SP.block_to_table(blk)
        if not t: continue
        dde = SP.date_from_desc(t['desc'])
        if dde: ds.add(dde)
        for c in t['cols']:
            # textual header dates ('1 October 2011', 'per July 1, 2019') need date_from_desc,
            # same as the parser's own parse_value_header does
            hd = SP.parse_header_date(c) or SP.date_from_desc(c)
            if hd: ds.add(hd)
        for r in t['rows']:
            for cell in r.split('|'):
                cell = cell.strip()
                if not cell: continue
                cd = SP.parse_header_date(cell) or SP.date_from_desc(cell)
                if cd: ds.add(cd)
    return ds

def audit_file(path):
    ext = json.load(open(path))
    rows, diags, dd = SP.parse_extract(ext)
    num_universe = raw_number_universe(ext)
    date_universe = header_date_universe(ext)

    # rescaled points are documented derivations (mangled-decimal rescue) -- audited
    # separately, never counted as source-verbatim
    n_rescaled = sum(1 for r in rows for p in r['timeline']
                     if p.get('value_source') == 'rescaled')
    p_amounts = [round(p['amount'],2) for r in rows for p in r['timeline']
                 if p.get('value_source') != 'rescaled']
    p_dates   = [p['start_date'] for r in rows for p in r['timeline']]

    amt_bad = [a for a in p_amounts if a not in num_universe]      # invented numbers
    date_bad = [d for d in p_dates if d not in date_universe]      # unprovenanced dates

    n_tables = len(diags); n_incl = sum(1 for d in diags if d['included'])
    incl = [d for d in diags if d['included'] and d.get('seen_cells')]
    seen = sum(d['seen_cells'] for d in incl)
    emit = sum(d['emitted_points'] for d in incl)
    def benign_multiunit(d):
        # seen ~= k*emit for small k => parser dropped non-preferred unit columns (correct)
        s,e = d.get('seen_cells',0), d.get('emitted_points',0)
        if not e: return False
        for k in (2,3,4,5):
            if abs(s - k*e) <= max(2, 0.1*k*e): return True
        return False
    low_cov = [(d['desc'][:50], d.get('coverage')) for d in incl
               if d.get('coverage') is not None and d['coverage'] < 0.6
               and not benign_multiunit(d)]
    return {
        'file': os.path.basename(path).replace('_extract.json',''),
        'rows': len(rows), 'points': len(p_amounts) + n_rescaled,
        'rescaled_points': n_rescaled,
        'tables': n_tables, 'included_tables': n_incl,
        'seen_cells': seen, 'emitted_points': emit,
        'amount_provenance': 1.0 if not p_amounts else 1-len(amt_bad)/len(p_amounts),
        'date_provenance':   1.0 if not p_dates   else 1-len(date_bad)/len(p_dates),
        'amt_violations': amt_bad[:8],
        'date_violations': list(set(date_bad))[:8],
        'low_coverage_tables': low_cov[:6],
    }

def main(caos):
    for cao in caos:
        files = sorted(glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json')))
        if not files:
            print('CAO %s: no files'%cao); continue
        rep = [audit_file(f) for f in files]
        tot_pts = sum(r['points'] for r in rep)
        amt_ok = sum(r['amount_provenance']*r['points'] for r in rep)
        dat_ok = sum(r['date_provenance']*r['points'] for r in rep)
        seen = sum(r['seen_cells'] for r in rep); emit = sum(r['emitted_points'] for r in rep)
        zero = [r['file'] for r in rep if r['rows']==0]
        print('=== CAO %s : %d files, %d parser rows, %d points ==='%(
            cao, len(files), sum(r['rows'] for r in rep), tot_pts))
        print('  AMOUNT provenance (no invented numbers): %.1f%%'%(100*amt_ok/max(tot_pts,1)))
        print('  DATE   provenance (date traceable)     : %.1f%%'%(100*dat_ok/max(tot_pts,1)))
        print('  COVERAGE (cells emitted / cells seen)  : %.1f%% (%d/%d)'%(100*emit/max(seen,1),emit,seen))
        print('  files with 0 parser rows: %d %s'%(len(zero), zero[:4]))
        lowcov = [(r['file'],t,c) for r in rep for (t,c) in r['low_coverage_tables']]
        for f,t,c in lowcov[:5]:
            print('   ! low-coverage table %.0f%% in %-30s : %s'%(100*c, f[:30], t))
        print()

if __name__=='__main__':
    main(sys.argv[1:] or ['10'])
