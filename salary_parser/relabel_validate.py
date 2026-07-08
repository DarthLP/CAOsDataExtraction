"""
Deterministic validator for LLM-relabeled rows (guarded Haiku relabeling of tier C/D).

The LLM is only allowed to move LABELS; this validator enforces it:
  G1  point-lock : every output (date, amount) must exist among that locked row's points;
                   nothing added, values never altered (splits allowed, drops counted).
  G2  label-provenance : every jobgroup/step/worker/age token must appear in the source
                   tables' text (case-insensitive, word-level) -- no invented names.
  G3  sanity     : within an output row no duplicate (date, amount); unit from a fixed set.
  G4  per-point anchor provenance (709 lesson -- staircase off-by-one passed G1-G3):
                   payload points carry the exact CELL ANCHORS the parser read them from
                   ([date, amount, col_header, row_key]); every COORDINATE-LIKE token
                   ('0', '1', 'F', 'IV', '3a') of an assigned jobgroup/step/age must appear
                   in THAT row's own point anchors (or the row's original parser labels) --
                   not merely somewhere in the file. An off-by-one column<->step shift can
                   then never validate: '1' is not an anchor of the '0 functiejaren' cell.
                   Longer word tokens stay governed by G2 (file-level). Payloads without
                   anchors (pre-G4) are counted G4_skipped, not rejected.
Accepted rows -> tier B with label_source='haiku_relabel'. Anything failing stays C/D.

Usage: python3 relabel_validate.py <payload_dir>   (expects <cao>.json + fix_<cao>.json)
"""
import json, os, re, sys, glob, collections

UNITS = {None, '', 'monthly', 'hourly', 'weekly', '4-week', 'annual', 'period'}
UNIT_SYN = {'4-weekly': '4-week', '4 week': '4-week', 'per maand': 'monthly', 'maand': 'monthly',
            'month': 'monthly', 'per uur': 'hourly', 'uur': 'hourly', 'hour': 'hourly',
            'week': 'weekly', 'jaar': 'annual', 'year': 'annual', 'yearly': 'annual'}


def norm_unit(u):
    if u in (None, ''): return None
    u = str(u).strip().lower()
    return UNIT_SYN.get(u, u)


def norm_words(s):
    return set(re.sub(r'[^a-z0-9]+', ' ', str(s).lower()).split())


# coordinate-like token: an axis position (step number, scale letter, roman numeral,
# '3a' codes). These carry the off-by-one risk -> must be per-point anchored (G4).
COORD = re.compile(r'^(\d{1,3}[a-z]?|[a-z]|[ivxl]{1,4})$')


def validate_pair(payload_path, fix_path):
    pay = json.load(open(payload_path))
    try:
        fix = json.load(open(fix_path))
    except Exception as e:
        return {'error': 'unreadable fix: %s' % e}
    src_words = set()
    title_words = set()          # first line of each table = its title (G4 jobgroup source)
    for tab in pay['source_tables']:
        for i, line in enumerate(tab):
            if isinstance(line, str):
                src_words |= norm_words(line)
                if i == 0: title_words |= norm_words(line)
    locked = {r['row_id']: {(p[0], round(float(p[1]), 2)) for p in r['points']}
              for r in pay['locked_rows']}
    # G4 anchor maps: (date, amount) -> words of that point's own cell anchors;
    # plus the locked row's ORIGINAL parser labels (legitimate anchor material --
    # desc-derived ages/jobgroups live there, and the parser read them from real cells)
    anchors = {}; own_labels = {}; has_anchors = {}
    for r in pay['locked_rows']:
        rid = r['row_id']
        amap = collections.defaultdict(set)
        any_a = False
        for p in r['points']:
            k = (p[0], round(float(p[1]), 2))
            if len(p) >= 4:
                any_a = True
                if p[2]: amap[k] |= norm_words(p[2])   # col_header
                if p[3]: amap[k] |= norm_words(p[3])   # row_key
        anchors[rid] = amap; has_anchors[rid] = any_a
        own_labels[rid] = set()
        for v in (r.get('old_labels') or {}).values():
            if v: own_labels[rid] |= norm_words(v)
    res = collections.Counter(); rejects = []
    accepted = []
    for row in fix.get('rows', []):
        rid = row.get('row_id')
        if row.get('skip'):
            res['skipped'] += 1; continue
        if rid not in locked:
            res['bad_row_id'] += 1; continue
        # G1: point lock
        pts = {(p[0], round(float(p[1]), 2)) for p in row.get('points', [])}
        if not pts or not pts <= locked[rid]:
            res['G1_point_violation'] += 1
            rejects.append((rid, 'G1', sorted(pts - locked[rid])[:3])); continue
        # G2: label provenance (each token of each label must be a source word)
        ok = True
        for f in ('jobgroup', 'step', 'worker', 'age_group'):
            v = row.get(f)
            if v in (None, ''): continue
            toks = norm_words(v)
            if toks and not toks <= src_words:
                ok = False; rejects.append((rid, 'G2:%s' % f, sorted(toks - src_words)[:3])); break
        if not ok:
            res['G2_label_not_in_source'] += 1; continue
        # G4: per-point anchor provenance for coordinate-like tokens
        if has_anchors.get(rid):
            aw = set(own_labels.get(rid, ()))
            for k in pts:
                aw |= anchors[rid].get(k, set())
            for f in ('jobgroup', 'step', 'age_group'):
                v = row.get(f)
                if v in (None, ''): continue
                coord = {t for t in norm_words(v) if COORD.match(t)}
                # JOBGROUP may legitimately come from a table TITLE ('FWG 15' grids,
                # wave-2B autopsy: agents were right, G4 too strict) -- the parser's own
                # desc_jg does the same. STEP/AGE stay strictly per-cell-anchored: the
                # off-by-one risk (709) lives in the step/column mapping.
                allowed = aw | (title_words if f == 'jobgroup' else set())
                miss = coord - allowed
                if miss:
                    ok = False
                    rejects.append((rid, 'G4:%s' % f, sorted(miss)[:3])); break
            if not ok:
                res['G4_anchor_violation'] += 1; continue
        else:
            res['G4_skipped_no_anchors'] += 1
        # G3: sanity
        row['unit'] = norm_unit(row.get('unit'))
        if row.get('unit') not in UNITS:
            res['G3_bad_unit'] += 1; rejects.append((rid, 'G3_unit', row.get('unit'))); continue
        if len(pts) != len(row.get('points', [])):
            res['G3_dup_points'] += 1; rejects.append((rid, 'G3_dup', None)); continue
        res['accepted'] += 1
        accepted.append(row)
    # per-locked-row point retention across all accepted outputs
    used = collections.defaultdict(set)
    for row in accepted:
        used[row['row_id']] |= {(p[0], round(float(p[1]), 2)) for p in row['points']}
    retained = sum(len(used[rid] & locked[rid]) for rid in locked)
    total = sum(len(v) for v in locked.values())
    return {'counts': dict(res), 'rejects': rejects[:6],
            'point_retention': '%d/%d' % (retained, total), 'accepted_rows': accepted}


def main(d):
    agg = collections.Counter(); ret_n = ret_d = 0
    for pp in sorted(glob.glob(os.path.join(d, '*.json'))):
        base = os.path.basename(pp)
        if base.startswith('fix_'): continue
        cao = base[:-5]
        fp = os.path.join(d, 'fix_%s.json' % cao)
        if not os.path.exists(fp):
            print('%-6s NO FIX FILE' % cao); continue
        r = validate_pair(pp, fp)
        if 'error' in r:
            print('%-6s %s' % (cao, r['error'])); continue
        n, dd = map(int, r['point_retention'].split('/'))
        ret_n += n; ret_d += dd
        for k, v in r['counts'].items(): agg[k] += v
        print('%-6s %s retention=%s %s' % (cao, r['counts'], r['point_retention'],
              ('rejects: %s' % r['rejects']) if r['rejects'] else ''))
    print('\nTOTAL:', dict(agg), ' point retention: %d/%d (%.0f%%)'
          % (ret_n, ret_d, 100*ret_n/max(ret_d, 1)))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.')
