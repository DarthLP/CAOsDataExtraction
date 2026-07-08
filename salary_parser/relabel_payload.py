"""
Build guarded-relabel payloads (tier C/D rows) WITH per-point cell anchors (guard G4).

Runs the parser+role-check FRESH for the requested (cao, file) -- the on-disk
rolecheck_all JSON may predate the anchor instrumentation. Payload format:

  { cao, file,
    source_tables : [[line, ...], ...],          # raw wage_information blocks (G2 corpus)
    locked_rows   : [{ row_id,
                       old_labels : {jobgroup, step, worker, age_group, unit},
                       flags      : [...],       # why this row is C/D
                       points     : [[date, amount, col_header, row_key], ...] }] }

The relabel agent must return fix_<cao>.json:
  { rows: [{ row_id, jobgroup, step, worker, age_group, unit,
             points: [[date, amount], ...] }, ...] }    (splits allowed; skip: true to skip)
Amounts/dates are LOCKED (G1); labels must exist in source (G2) and coordinate tokens
must come from each point's OWN anchors (G4) -- see relabel_validate.py.

Usage: python3 relabel_payload.py <out_dir> <cao> [<file_base>]
       (no file_base: the CAO's first file that has flagged rows)
"""
import json, glob, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import EXT_ROOT, ANA_ROOT
import role_check as RC
import confidence as CONF


def is_flagged(r):
    # heavy-pad rows are EXCLUDED: their cell anchors may themselves be misaligned
    # (the pad ambiguity), so anchored relabeling could confirm a wrong column.
    if (r.get('_ragged_pad') or 0) >= 0.5:
        return False
    return (str(r.get('role_verdict', '')).startswith('FLAG')
            or r.get('role_verdict') == 'NO_TABLE'
            or r.get('_dup_date') or r.get('_mono')
            # junk_label (a salary number fused into jobgroup/step/worker) is a
            # confidence-layer flag, not a role_verdict -- include it so the relabel wave
            # can strip the fused number and assign the real coordinate label.
            or CONF.junk_label(r))


def build(cao, file_base=None):
    """returns (payload dict or None, reason)."""
    eps = sorted(glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json')))
    if file_base:
        eps = [e for e in eps
               if os.path.basename(e) == file_base + '_extract.json']
    for ep in eps:
        base = os.path.basename(ep).replace('_extract.json', '')
        ana = os.path.join(ANA_ROOT, cao, base + '_analysis.json')
        res = RC.rolecheck_file(ep, ana)
        if not res:
            continue
        flagged = [r for r in res['rows'] if is_flagged(r)]
        if not flagged:
            continue
        ext = json.load(open(ep))
        tabs = []
        for blk in ext.get('wage_information', []):
            lines = blk if isinstance(blk, list) else [blk]
            out = []
            for l in lines:
                if isinstance(l, str):
                    out.extend(l.split('\n'))
            if out:
                tabs.append(out)
        locked = []
        for i, r in enumerate(flagged):
            flags = [k for k in ('_dup_date', '_mono', '_mixed_unit', '_rescaled') if r.get(k)]
            if str(r.get('role_verdict', '')).startswith(('FLAG', 'NO_TABLE')):
                flags.append(r['role_verdict'])
            locked.append({
                'row_id': i,
                'old_labels': {k: r.get(k) for k in
                               ('jobgroup', 'step', 'worker', 'age_group', 'unit')},
                'flags': flags,
                'points': [[p['start_date'], round(p['amount'], 2),
                            p.get('col_header'), p.get('row_key')]
                           for p in r['timeline']],
            })
        return ({'cao': cao, 'file': base, 'source_tables': tabs,
                 'locked_rows': locked}, 'ok')
    return (None, 'no flagged rows in any file' if not file_base else 'file has no flagged rows')


def main():
    out_dir, cao = sys.argv[1], sys.argv[2]
    fb = sys.argv[3] if len(sys.argv) > 3 else None
    os.makedirs(out_dir, exist_ok=True)
    pay, why = build(cao, fb)
    if not pay:
        print('%s: %s' % (cao, why)); return
    p = os.path.join(out_dir, '%s.json' % cao)
    json.dump(pay, open(p, 'w'), indent=1, ensure_ascii=False)
    kb = os.path.getsize(p) / 1024
    print('%s -> %s  (%d locked rows, %d tables, %.0f KB)'
          % (cao, p, len(pay['locked_rows']), len(pay['source_tables']), kb))


if __name__ == '__main__':
    main()
