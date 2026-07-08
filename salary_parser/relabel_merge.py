"""
Merge VALIDATED relabeled rows back into rolecheck_all/<cao>/<file>.json.

For each payload+fix pair in the given dir (validated by relabel_validate):
  - the ORIGINAL flagged rows that got accepted replacements are REMOVED,
  - the accepted replacement rows are APPENDED with role_verdict='RELABELED'
    and label_source='haiku_relabel' (flatten/confidence map that to tier B),
  - rows whose relabel was rejected/skipped stay exactly as they were (C/D).
Amounts/dates come only from the locked points -- guaranteed by the validator.

Usage: python3 relabel_merge.py <payload_dir>
"""
import json, os, sys, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import OUT_ROOT, EXT_ROOT
import relabel_validate as RV

ALL_DIR = os.path.join(OUT_ROOT, 'rolecheck_all')


def find_target(cao, fn):
    """rolecheck_all path for (cao, file) -- cao dir may differ from csv cao_number."""
    p = os.path.join(ALL_DIR, cao, fn + '.json')
    if os.path.exists(p):
        return p
    hits = glob.glob(os.path.join(ALL_DIR, '*', fn + '.json'))
    return hits[0] if len(hits) == 1 else None


def main(d):
    tot_repl = tot_removed = 0
    for pp in sorted(glob.glob(os.path.join(d, '*.json'))):
        base = os.path.basename(pp)
        if base.startswith('fix_'):
            continue
        cao = base[:-5]
        fp = os.path.join(d, 'fix_%s.json' % cao)
        if not os.path.exists(fp):
            continue
        pay = json.load(open(pp))
        res = RV.validate_pair(pp, fp)
        acc = res.get('accepted_rows') or []
        if not acc:
            print('%-6s nothing accepted' % cao); continue
        # FULL-COVERAGE GUARD: an original row may only be replaced when its accepted
        # replacements retain EVERY one of its locked points -- otherwise points would be
        # silently dropped at merge time. Partially-covered row_ids keep their original
        # (flagged) row untouched.
        locked_pts = {r['row_id']: {(p[0], round(float(p[1]), 2)) for p in r['points']}
                      for r in pay['locked_rows']}
        used_pts = {}
        for row in acc:
            used_pts.setdefault(row['row_id'], set()).update(
                (p[0], round(float(p[1]), 2)) for p in row['points'])
        full = {rid for rid, u in used_pts.items() if u >= locked_pts.get(rid, set())}
        dropped_partial = len(used_pts) - len(full)
        acc = [row for row in acc if row['row_id'] in full]
        if dropped_partial:
            print('%-6s coverage guard: %d row_ids kept as original (partial point coverage)'
                  % (cao, dropped_partial))
        if not acc:
            print('%-6s nothing fully covered' % cao); continue
        target = find_target(cao, pay['file'])
        if not target:
            print('%-6s TARGET NOT FOUND (%s)' % (cao, pay['file'])); continue
        doc = json.load(open(target))
        # identify original flagged rows by their (labels, points) signature captured in payload
        locked_by_id = {r['row_id']: r for r in pay['locked_rows']}
        accepted_ids = {r['row_id'] for r in acc}
        def sig(labels, points):
            return (tuple(sorted((k, str(v)) for k, v in labels.items() if v)),
                    frozenset((p[0], round(float(p[1]), 2)) for p in points))
        sig_to_rid = {sig(locked_by_id[i]['old_labels'], locked_by_id[i]['points']): i
                      for i in accepted_ids}
        remove_sigs = set(sig_to_rid)
        kept, removed = [], 0
        removed_rids = set()
        for r in doc['rows']:
            s = sig({k: r.get(k) for k in ('jobgroup', 'step', 'worker', 'age_group', 'unit')},
                    [(p['start_date'], p['amount']) for p in r['timeline']])
            if s in remove_sigs:
                removed += 1; remove_sigs.discard(s)   # remove each original once
                removed_rids.add(sig_to_rid[s])
            else:
                kept.append(r)
        # ORPHAN GUARD: append a replacement ONLY if its original was found and removed
        # in THIS doc -- a re-parsed doc may no longer contain the row (e.g. after a new
        # exclusion); appending then would duplicate/orphan data.
        orphaned = {row['row_id'] for row in acc} - removed_rids
        if orphaned:
            print('%-6s orphan guard: %d row_ids skipped (original no longer in doc)'
                  % (cao, len(orphaned)))
        acc = [row for row in acc if row['row_id'] in removed_rids]
        for row in acc:
            kept.append({
                'jobgroup': row.get('jobgroup'), 'step': row.get('step'),
                'worker': row.get('worker'), 'age_group': row.get('age_group'),
                'unit': row.get('unit'), 'is_entry': None, 'ft_hours': None,
                'timeline': [{'start_date': p[0], 'end_date': None, 'amount': float(p[1]),
                              'unit': row.get('unit'), 'holiday_incl': None,
                              'table_label': 'haiku_relabel'} for p in row['points']],
                'role_verdict': 'RELABELED', 'confidence': 'medium',
                'label_source': 'haiku_relabel', '_tables': [],
            })
        doc['rows'] = kept
        json.dump(doc, open(target, 'w'), indent=1, ensure_ascii=False)
        tot_repl += len(acc); tot_removed += removed
        print('%-6s removed %d originals, added %d relabeled rows -> %s'
              % (cao, removed, len(acc), os.path.basename(target)))
    print('\nTOTAL: +%d relabeled rows, -%d originals. Re-run flatten_csv.py to refresh the CSV.'
          % (tot_repl, tot_removed))


if __name__ == '__main__':
    main(sys.argv[1])
