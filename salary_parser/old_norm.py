"""
Phase 2 — Old-LLM normalization layer.

Reads any `outputs/llm_analysis/salary/<cao>/<file>_analysis.json` and returns a
uniform structure regardless of which of the 3 schemas it uses:
  1. FULL-KEY   : row {jobgroup,step,worker,age_group,...,timeline:[{amount,start_date,...}]}
  2. SHORT+TL   : row {jg,st,wr,ag,...,tl:[{am,sd,ed,un,ip,nt}]}
  3. FLATTENED  : row {jg,st,wr,ag,am,sd,un,...}  (am/sd at row level, no timeline)
  + EMPTY       : salary_information missing / [] / {}

Public API:
  normalize_file(path)      -> list[Row]   Row = {jobgroup,step,worker,age,education,
                                                  permanency,is_entry,points:[Point]}
                                           Point = {amount,start_date,end_date,unit,
                                                    table_label,note,inc_pct}
  count_amounts(path)       -> int         number of salary amounts (points) in the file
  amount_index(path)        -> dict        round(amount,2) -> [labels]
  amount_date_index(path)   -> dict        (round(amount,2), start_date) -> [labels]

A "label" is {jobgroup,step,worker,age,unit,table_label} carried from the old LLM,
used later (Phase 3) to relabel parser amounts by amount / amount+date join.
"""
import json, os, glob

# ---- key aliases (short -> canonical) -------------------------------------
ROW_ALIAS = {
    'jobgroup': 'jobgroup', 'jg': 'jobgroup',
    'step': 'step', 'st': 'step',
    'worker': 'worker', 'wr': 'worker',
    'age_group': 'age', 'ag': 'age',
    'education': 'education', 'eu': 'education',
    'permanency': 'permanency', 'pe': 'permanency',
    'is_entry': 'is_entry', 'ie': 'is_entry',
    'hours_type': 'hours_type', 'ht': 'hours_type',
    'ft_hours': 'ft_hours', 'fh': 'ft_hours',
    'row_note': 'row_note', 'rn': 'row_note',
}
PT_ALIAS = {
    'amount': 'amount', 'am': 'amount',
    'start_date': 'start_date', 'sd': 'start_date',
    'end_date': 'end_date', 'ed': 'end_date',
    'unit': 'unit', 'un': 'unit',
    'table_label': 'table_label', 'tl_label': 'table_label',
    'note': 'note', 'nt': 'note',
    'inc_pct': 'inc_pct', 'ip': 'inc_pct',
    'holiday_incl': 'holiday_incl', 'hi': 'holiday_incl',
}
TIMELINE_KEYS = ('timeline', 'tl')


def _num(x):
    if isinstance(x, (int, float)):
        return round(float(x), 2)
    if isinstance(x, str):
        s = x.strip().replace('€', '').replace('$', '').replace('£', '').strip()
        s = s.replace(' ', '')
        if not s:
            return None
        # european: 2.107,50 -> 2107.50 ; 4,734 (ambiguous) handled by parser normally,
        # but here old LLM already emits clean numbers, so keep it simple.
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            # comma decimal if exactly 2 trailing digits, else thousands
            a, b = s.rsplit(',', 1)
            s = (a.replace(',', '') + '.' + b) if len(b) == 2 else s.replace(',', '')
        try:
            return round(float(s), 2)
        except ValueError:
            return None
    return None


def _row_get(row, canon):
    for k, c in ROW_ALIAS.items():
        if c == canon and k in row:
            v = row[k]
            if v not in (None, '', []):
                return v
    return None


def _pt_norm(pt):
    out = {'amount': None, 'start_date': None, 'end_date': None, 'unit': None,
           'table_label': None, 'note': None, 'inc_pct': None, 'holiday_incl': None}
    for k, v in pt.items():
        c = PT_ALIAS.get(k)
        if c:
            out[c] = v
    out['amount'] = _num(out['amount'])
    return out


def normalize_file(path):
    try:
        d = json.load(open(path))
    except Exception:
        return []
    si = d.get('salary_information') if isinstance(d, dict) else d
    if not si or not isinstance(si, list):
        return []
    rows = []
    for r in si:
        if not isinstance(r, dict):
            continue
        base = {
            'jobgroup': _row_get(r, 'jobgroup'),
            'step': _row_get(r, 'step'),
            'worker': _row_get(r, 'worker'),
            'age': _row_get(r, 'age'),
            'education': _row_get(r, 'education'),
            'permanency': _row_get(r, 'permanency'),
            'is_entry': _row_get(r, 'is_entry'),
            'ft_hours': _row_get(r, 'ft_hours'),
            'hours_type': _row_get(r, 'hours_type'),
        }
        # find timeline
        tl = None
        for tk in TIMELINE_KEYS:
            if tk in r and isinstance(r[tk], list):
                tl = r[tk]
                break
        pts = []
        if tl:
            for pt in tl:
                if isinstance(pt, dict):
                    p = _pt_norm(pt)
                    if p['amount'] is not None:
                        pts.append(p)
        else:
            # FLATTENED: amount/date live on the row itself
            p = _pt_norm(r)
            if p['amount'] is not None:
                pts.append(p)
        base['points'] = pts
        rows.append(base)
    return rows


def count_amounts(path):
    return sum(len(r['points']) for r in normalize_file(path))


def _label_of(row, pt):
    return {
        'jobgroup': row.get('jobgroup'), 'step': row.get('step'),
        'worker': row.get('worker'), 'age': row.get('age'),
        'unit': pt.get('unit'), 'table_label': pt.get('table_label'),
    }


def amount_index(path):
    idx = {}
    for row in normalize_file(path):
        for pt in row['points']:
            idx.setdefault(pt['amount'], []).append(_label_of(row, pt))
    return idx


def amount_date_index(path):
    idx = {}
    for row in normalize_file(path):
        for pt in row['points']:
            idx.setdefault((pt['amount'], pt.get('start_date')), []).append(_label_of(row, pt))
    return idx


def file_vocab(path):
    """Field VOCABULARIES of an old-LLM analysis file, for the role-check merge.
    Returns {'jobgroup': set, 'step': set, 'worker': set, 'age': set,
             'rows': [ {jobgroup, step, worker, age, amounts:set} ]}
    We consume ONLY these vocabularies (which fields the old LLM put which words in),
    never its per-amount label assignments -- those carry its misplacement bug."""
    out = {'jobgroup': set(), 'step': set(), 'worker': set(), 'age': set(),
           'education': set(), 'permanency': set(), 'ft_hours': set(), 'hours_type': set(),
           'rows': []}
    for r in normalize_file(path):
        amounts = {p['amount'] for p in r['points'] if p['amount'] is not None}
        rec = {'amounts': amounts}
        for f_norm, f_row in (('jobgroup','jobgroup'), ('step','step'),
                              ('worker','worker'), ('age','age'),
                              ('education','education'), ('permanency','permanency'),
                              ('ft_hours','ft_hours'), ('hours_type','hours_type')):
            v = r.get(f_row)
            if v not in (None, ''):
                s = str(v).strip()
                if s:
                    out[f_norm].add(s)
                    rec[f_norm] = s
        out['rows'].append(rec)
    return out


if __name__ == '__main__':
    import sys
    ROOT = 'outputs/llm_analysis/salary'
    caos = sys.argv[1:] or ['10']
    for cao in caos:
        files = sorted(glob.glob(os.path.join(ROOT, cao, '*_analysis.json')))
        print('=== CAO %s: %d analysis files ===' % (cao, len(files)))
        for f in files:
            rows = normalize_file(f)
            n = sum(len(r['points']) for r in rows)
            print('  %-52s rows=%-3d amounts=%-4d' % (os.path.basename(f)[:52], len(rows), n))
