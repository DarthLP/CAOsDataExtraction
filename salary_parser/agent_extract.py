"""
GUARDED whole-file agent extraction for the HARD structural tables the deterministic
parser cannot lay out (2948 VVT section-tables, 759 SAG MIDDEN-ladders, 725 diagonals).

Flow (mirrors the relabel pipeline, but replaces the WHOLE file's rows, not just labels):
  build(cao, file)   -> payload {source_tables, num_universe, date_universe} for a Haiku agent
  validate(payload, agent_out)
     G1 PROVENANCE (hard): every amount must be a source number  -> else row rejected
     G2 date       : date must be in the file's header-date universe, or null
     G3 unit       : from the canonical set
     G4 label prov : coordinate-like jobgroup/step/age tokens must appear in source text
     -> returns accepted rows + reject stats
  merge(cao, file, accepted): REPLACE that file's rows in rolecheck_all with the accepted
     rows, tagged role_verdict='AGENT_EXTRACT' (-> tier B), label_source='agent_extract'.
     A .bak of the doc is kept. Only touches files we deliberately target.

The agent NEVER invents an amount that survives (G1 is exact-set membership on the source
number universe), so 100% amount provenance is preserved by construction.

Usage:
  python3 agent_extract.py build   <out_dir> <targets.json>   # targets=[{cao,file}]
  python3 agent_extract.py validate <out_dir>                 # expects <key>.json+fix_<key>.json
  python3 agent_extract.py merge    <out_dir>
"""
import json, os, sys, glob, re, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import EXT_ROOT, OUT_ROOT
import salary_parser as SP
from source_audit import raw_number_universe, header_date_universe

ALL_DIR = os.path.join(OUT_ROOT, 'rolecheck_all')
UNITS = {None, '', 'monthly', 'hourly', 'weekly', '4-week', 'annual', 'period', 'daily'}
UNIT_SYN = {'4-weekly': '4-week', '4 week': '4-week', 'month': 'monthly', 'maand': 'monthly',
            'hour': 'hourly', 'uur': 'hourly', 'week': 'weekly', 'jaar': 'annual',
            'year': 'annual', 'yearly': 'annual', 'per period': 'period', 'periode': 'period',
            'per maand': 'monthly', 'per month': 'monthly', 'maandsalaris': 'monthly',
            'maandloon': 'monthly', 'per uur': 'hourly', 'per hour': 'hourly',
            'uurloon': 'hourly', 'per week': 'weekly', 'weekloon': 'weekly',
            'per periode': 'period', 'periodesalaris': 'period', 'per 4 weken': '4-week',
            'per jaar': 'annual', 'per year': 'annual', 'jaarsalaris': 'annual',
            'basis van 12 maanden': 'annual', '4weekly': '4-week', '4-week': '4-week',
            'four-weekly': '4-week', 'vierwekelijks': '4-week', 'per 4 weken': '4-week',
            'daily': 'daily', 'per dag': 'daily', 'dag': 'daily'}


def _norm_unit(u):
    if u in ('', None):
        return None
    u = str(u).strip().lower()
    if u in UNIT_SYN:
        return UNIT_SYN[u]
    u2 = re.sub(r'^per\s+', '', u)          # 'per maand' -> 'maand'
    return UNIT_SYN.get(u2, u)
COORD = re.compile(r'^(\d{1,3}[a-z]?|[a-z]|[ivxl]{1,4})$')


_AGE_WORD = re.compile(r'\b(jaar|jaren|jr|years?|yrs?|leeftijd|age)\b', re.I)
# service/experience-year words: a low number here is a STEP, not a youth age -> never strip.
_SERVICE_WORD = re.compile(r'(ervaring|dienstjaar|dienstjaren|functiejaar|functiejaren|'
                           r'anci|senior|service|periodiek|trede|schaaljaar|step)', re.I)
def is_youth(age_group):
    """A row is YOUTH iff its age label denotes an actual age in the TEEN band 13-20
    (mirrors the parser's row-level youth rule: ages 13-20 are youth; 0-12 are steps/
    function-years, >=21 are adult). The parser EXCLUDES youth by design so the dataset
    stays a homogeneous adult-scale panel for the cross-file wage indices; agent rows must
    obey the same rule or youth wages leak into only the agent-extracted files and bias
    every pooled statistic. Amounts are provenanced either way — youth is a separate
    statutory schedule, derivable on its own.

    Guards: (1) require an age word or a bare number/range so a non-age label like
    'scale 7' can't trigger a drop; (2) never strip experience/service-year labels
    ('1 ervaringsjaar', '2 dienstjaren') — those are adult step rows the agent mislabeled
    into age_group; (3) only the 13-20 band counts as youth, so a bare '1 years' service
    year (<=12) is kept."""
    if not age_group:
        return False
    s = str(age_group).strip()
    if _SERVICE_WORD.search(s):
        return False
    looks_age = bool(_AGE_WORD.search(s)) or bool(re.fullmatch(r'[\d\s\-tot/m]+', s))
    if not looks_age:
        return False
    nums = [int(n) for n in re.findall(r'\d+', s)]
    if not nums:
        return False
    return 13 <= max(nums) <= 20


def strip_youth(accepted):
    """Drop youth point-rows from an accepted list. Returns (kept, n_dropped)."""
    kept = [r for r in accepted if not is_youth(r.get('age_group'))]
    return kept, len(accepted) - len(kept)


import confidence as _CONF
def _plausible(amount, unit):
    """True unless the amount exceeds the per-unit sanity ceiling (concatenation / unit
    mislabel). Used by the coverage gate so junk tokens don't count as coverage."""
    try:
        a = float(amount)
    except (TypeError, ValueError):
        return False
    ceil = _CONF._MAG_CEIL.get((unit or '').strip().lower())
    return ceil is None or a <= ceil


def _find_extract(cao, file_base):
    eps = glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json'))
    for e in eps:
        if os.path.basename(e) == file_base + '_extract.json':
            return e
    for e in eps:
        if os.path.basename(e).replace('_extract.json', '').strip() == file_base.strip():
            return e
    return None


def norm_words(s):
    return set(re.sub(r'[^a-z0-9]+', ' ', str(s).lower()).split())


def _tables_of(ext):
    tabs = []
    for blk in ext.get('wage_information', []):
        lines = blk if isinstance(blk, list) else [blk]
        out = []
        for l in lines:
            if isinstance(l, str):
                out.extend(l.split('\n'))
        if out:
            tabs.append(out)
    return tabs


def build(cao, file_base):
    ep = _find_extract(cao, file_base)
    if not ep:
        return None, 'extract not found'
    ext = json.load(open(ep))
    tabs = _tables_of(ext)
    if not tabs:
        return None, 'no wage tables'
    return ({'cao': cao, 'file': file_base, 'source_tables': tabs,
             'num_universe': sorted(raw_number_universe(ext)),
             'date_universe': sorted(header_date_universe(ext))}, 'ok')


_NUMTOK = re.compile(r'\d')
def _table_weight(tab):
    """Rough output cost of a table = number of numeric cells (each ~= one emitted row)."""
    w = 0
    for line in tab:
        for tok in re.split(r'[\s|,;]+', str(line)):
            if _NUMTOK.search(tok):
                w += 1
    return w


# a line that begins a fresh grid inside a flattened block (extractor sometimes dumps every
# sub-table of a file into ONE wage_information element -> one 900-line "table").
_SUBHDR = re.compile(r'^\s*(columns?\s*:|tabel\s|table\s|salaristabel|salary\s+scale|'
                     r'loontabel|loonschaal|schaal\s|scale\s|bijlage\s|appendix\s)', re.I)


def _n_numtok(line):
    return sum(1 for tok in re.split(r'[\s|,;]+', str(line)) if _NUMTOK.search(tok))


def _row_split(seg, max_weight):
    """Last-resort: split ONE grid whose weight still exceeds max_weight by its data rows,
    repeating the grid's header lines on every piece so the agent keeps column context. The
    header = the leading lines before the first line with >=2 numeric cells (title/Columns:).
    A uniform grid survives this cleanly; alignment is preserved because rows are never cut
    mid-line and the header travels with each piece."""
    hdr_end = 0
    for i, l in enumerate(seg):
        if _n_numtok(l) >= 2:
            hdr_end = i; break
    else:
        return [seg]                          # no data rows -> leave as-is
    header, body = seg[:hdr_end], seg[hdr_end:]
    if not body:
        return [seg]
    pieces, cur, curw = [], [], 0
    for row in body:
        rw = _n_numtok(row)
        if cur and curw + rw > max_weight:
            pieces.append(header + cur); cur, curw = [], 0
        cur.append(row); curw += rw
    if cur:
        pieces.append(header + cur)
    return pieces


def _split_giant_table(tab, max_weight=400):
    """Split one over-long flattened table into sub-tables at grid-header lines (Columns:/
    Table N/Salaristabel/...). The prose preamble before the first header rides with the
    first sub-table. Any resulting grid that is STILL over max_weight is row-split with
    header repetition. Returns a list of sub-tables (>=1). Never splits mid-row."""
    idxs = [i for i, l in enumerate(tab) if _SUBHDR.match(str(l))]
    if len(idxs) < 2:
        subs = [tab]
    else:
        cuts = idxs[:]
        if cuts[0] != 0:
            cuts = [0] + cuts
        subs = [tab[a:b] for a, b in zip(cuts, cuts[1:] + [len(tab)]) if tab[a:b]]
    out = []
    for seg in (subs or [tab]):
        if _table_weight(seg) > max_weight:
            out.extend(_row_split(seg, max_weight))
        else:
            out.append(seg)
    return out


def build_chunks(cao, file_base, max_weight=140):
    """Like build(), but splits a large file's source_tables into chunks whose combined
    numeric-cell weight stays under max_weight, so each chunk's agent pass stays well under
    the 32k output-token ceiling (632-class files exceed it as a single pass). A single
    oversized table becomes its own chunk (never split mid-table -- would break alignment).
    num_universe/date_universe stay GLOBAL (provenance whitelist is file-wide; every amount
    is still a real source number). Returns (list_of_payloads, msg). One payload if it fits."""
    ep = _find_extract(cao, file_base)
    if not ep:
        return None, 'extract not found'
    ext = json.load(open(ep))
    tabs = _tables_of(ext)
    if not tabs:
        return None, 'no wage tables'
    nu = sorted(raw_number_universe(ext))
    du = sorted(header_date_universe(ext))
    # expand any over-long single table into its constituent grids so it can be chunked
    units = []
    for tab in tabs:
        if _table_weight(tab) > max_weight:
            units.extend(_split_giant_table(tab, max_weight))
        else:
            units.append(tab)
    # greedy pack whole tables into weight-bounded chunks
    chunks, cur, curw = [], [], 0
    for tab in units:
        tw = _table_weight(tab)
        if cur and curw + tw > max_weight:
            chunks.append(cur); cur, curw = [], 0
        cur.append(tab); curw += tw
    if cur:
        chunks.append(cur)
    n = len(chunks)
    if n <= 1:
        return ([{'cao': cao, 'file': file_base, 'source_tables': tabs,
                  'num_universe': nu, 'date_universe': du,
                  'gkey': '%s_%s' % (cao, file_base), 'chunk': 0, 'nchunks': 1}], 'ok')
    pays = []
    for i, ct in enumerate(chunks):
        pays.append({'cao': cao, 'file': file_base, 'source_tables': ct,
                     'num_universe': nu, 'date_universe': du,
                     'gkey': '%s_%s' % (cao, file_base), 'chunk': i, 'nchunks': n})
    return pays, 'ok (%d chunks)' % n


def validate(payload, agent_out):
    nu = {round(float(x), 2) for x in payload['num_universe']}
    du = set(payload['date_universe'])
    src_words = set()
    for tab in payload['source_tables']:
        for line in tab:
            if isinstance(line, str):
                src_words |= norm_words(line)
    res = collections.Counter()
    accepted = []
    for row in agent_out.get('rows', []):
        amt = row.get('amount')
        try:
            amt = round(float(amt), 2)
        except (TypeError, ValueError):
            res['G1_bad_amount'] += 1; continue
        # G1 PROVENANCE (HARD): amount must be an actual source number -> else reject.
        if amt not in nu:
            res['G1_amount_not_in_source'] += 1; continue
        # G2/G3/G4 are DOWNGRADES, not rejections: the amount is already source-proven, so
        # keep the row but NULL any field we cannot back with source (a bad date/unit/label
        # is a metadata gap, not a reason to drop a verified wage amount).
        date = row.get('date') or None
        if date is not None and date not in du:
            res['G2_date_downgraded'] += 1; date = None
        u = _norm_unit(row.get('unit'))
        if u not in UNITS:
            res['G3_unit_downgraded'] += 1; u = None
        jg, st, ag = row.get('jobgroup') or None, row.get('step') or None, row.get('age_group') or None
        for f, val in (('jobgroup', jg), ('step', st), ('age_group', ag)):
            if not val:
                continue
            coord = {t for t in norm_words(val) if COORD.match(t)}
            if coord and not coord <= src_words:
                res['G4_label_downgraded'] += 1
                if f == 'jobgroup': jg = None
                elif f == 'step': st = None
                else: ag = None
        res['accepted'] += 1
        accepted.append({'jobgroup': jg, 'step': st, 'worker': row.get('worker') or None,
                         'age_group': ag, 'unit': u, 'amount': amt, 'date': date})
    return {'counts': dict(res), 'accepted': accepted}


def _find_target(cao, fn):
    p = os.path.join(ALL_DIR, cao, fn + '.json')
    if os.path.exists(p):
        return p
    hits = glob.glob(os.path.join(ALL_DIR, '*', fn + '.json'))
    return hits[0] if len(hits) == 1 else None


def merge(cao, file_base, accepted):
    """Replace ALL rows of the file's rolecheck doc with the accepted agent rows,
    grouped into timelines by (jobgroup, step, worker, age_group, unit)."""
    target = _find_target(cao, file_base)
    if not target:
        # NEVER-PARSED CAO: the parser produced no rolecheck doc for this file at all (its
        # tables defeated the parser entirely). Create a fresh minimal doc so the guarded
        # agent rows can be flattened into the dataset -- otherwise this CAO stays absent.
        target = os.path.join(ALL_DIR, cao, file_base + '.json')
        os.makedirs(os.path.dirname(target), exist_ok=True)
        doc = {'cao': cao, 'file': file_base, 'rows': [], 'table_verdicts': {},
               'coverage': {'missing_tables': False}}
    else:
        doc = json.load(open(target))
        if not os.path.exists(target + '.preagent.bak'):
            json.dump(doc, open(target + '.preagent.bak', 'w'), ensure_ascii=False)
    def _s(v):
        return None if v is None else str(v)
    groups = collections.OrderedDict()
    for r in accepted:
        k = (_s(r['jobgroup']), _s(r['step']), _s(r['worker']), _s(r['age_group']), r['unit'])
        groups.setdefault(k, []).append(r)
    rows = []
    for (jg, st, wk, ag, un), pts in groups.items():
        seen = set(); tl = []
        for p in sorted(pts, key=lambda x: (x['date'] or '9999-99-99', x['amount'])):
            key = (p['date'], p['amount'])
            if key in seen:
                continue
            seen.add(key)
            tl.append({'start_date': p['date'], 'end_date': None, 'amount': p['amount'],
                       'unit': un, 'holiday_incl': None, 'table_label': 'agent_extract'})
        rows.append({'jobgroup': jg, 'step': st, 'worker': wk, 'age_group': ag, 'unit': un,
                     'is_entry': None, 'ft_hours': None, 'timeline': tl,
                     'role_verdict': 'AGENT_EXTRACT', 'confidence': 'medium',
                     'label_source': 'agent_extract', '_tables': []})
    doc['rows'] = rows
    json.dump(doc, open(target, 'w'), indent=1, ensure_ascii=False)
    return 'ok', len(rows)


def main():
    cmd = sys.argv[1]
    if cmd in ('build', 'buildchunk'):
        out_dir, tpath = sys.argv[2], sys.argv[3]
        mw = int(sys.argv[4]) if len(sys.argv) > 4 else 140
        os.makedirs(out_dir, exist_ok=True)
        for t in json.load(open(tpath)):
            key = t.get('key', t['cao'])
            if cmd == 'buildchunk':
                pays, why = build_chunks(t['cao'], t['file'], max_weight=mw)
            else:
                p, why = build(t['cao'], t['file'])
                pays = [p] if p else None
            if not pays:
                print('%-6s SKIP %s' % (t['cao'], why)); continue
            n = len(pays)
            for pay in pays:
                fk = key if n == 1 else '%s__c%d' % (key, pay['chunk'])
                pay.setdefault('gkey', key)   # so validate regroups chunks -> one accepted
                json.dump(pay, open(os.path.join(out_dir, fk + '.json'), 'w'), indent=1, ensure_ascii=False)
            print('%-10s %-40s tabs=%d nums=%d chunks=%d' % (
                key, pays[0]['file'][:40], sum(len(p['source_tables']) for p in pays),
                len(pays[0]['num_universe']), n))
    elif cmd == 'validate':
        d = sys.argv[2]; agg = collections.Counter()
        groups = collections.OrderedDict()   # gkey -> {'cao','file','accepted':[...]}
        for pp in sorted(glob.glob(os.path.join(d, '*.json'))):
            b = os.path.basename(pp)
            if b.startswith(('fix_', 'accepted_')):
                continue
            key = b[:-5]
            fp = os.path.join(d, 'fix_%s.json' % key)
            if not os.path.exists(fp):
                print('%-10s NO FIX' % key); continue
            pay = json.load(open(pp))
            try:
                fix = json.load(open(fp))
            except Exception as e:
                print('%-10s unreadable: %s' % (key, e)); continue
            r = validate(pay, fix)
            gk = pay.get('gkey', key)
            g = groups.setdefault(gk, {'cao': pay['cao'], 'file': pay['file'], 'accepted': []})
            g['accepted'].extend(r['accepted'])
            for k, v in r['counts'].items():
                agg[k] += v
            print('%-10s %s' % (key, r['counts']))
        # write ONE accepted_<gkey>.json per file (chunks recombined)
        for gk, g in groups.items():
            json.dump(g, open(os.path.join(d, 'accepted_%s.json' % gk), 'w'), ensure_ascii=False)
        print('TOTAL:', dict(agg), '  files:', len(groups))
    elif cmd == 'merge':
        merge_dir(sys.argv[2])


def merge_dir(d):
    """Apply all accepted_*.json in dir d into rolecheck_all (youth-strip -> coverage gate
    -> replace rows). Callable from deliver.py so rebuilds re-apply agent extractions after
    all_files regenerates rolecheck_all. Returns (rows_merged, files_skipped, youth_stripped)."""
    tot = 0; skipped = 0; youth_tot = 0
    for ap in sorted(glob.glob(os.path.join(d, 'accepted_*.json'))):
        a = json.load(open(ap))
        # YOUTH STRIP: mirror the parser's adult-only design before the gate + merge,
        # so the coverage comparison is apples-to-apples (parser already excludes youth).
        a['accepted'], n_youth = strip_youth(a['accepted'])
        youth_tot += n_youth
        if n_youth:
            print('%-6s stripped %d youth rows' % (a['cao'], n_youth))
        if not a['accepted']:
            print('%-6s nothing accepted' % a['cao']); continue
        # COVERAGE GATE: only replace the parser's rows if the agent captures at least
        # as many DISTINCT source amounts (within 5%). Protects against files where the
        # agent drops whole tables (e.g. CAO 544 piece-rates) -> keep parser rows.
        # PLAUSIBLE-ONLY: count only amounts that pass the per-unit sanity ceiling, so a
        # parser file whose amounts are CONCATENATIONS (step-col fused into the wage, e.g.
        # 1574 GGZ 41507) can't inflate its coverage with junk tokens and thereby block a
        # clean agent extraction. (Concatenated tokens ARE in the source universe, so the
        # old raw count wrongly favoured the broken parser rows.)
        ep = _find_extract(a['cao'], a['file'])
        if ep:
            ext = json.load(open(ep))
            univ = raw_number_universe(ext)
            prows, _, _ = SP.parse_extract(ext)
            pcov = len({round(p['amount'], 2) for r in prows for p in r['timeline']
                        if _plausible(p['amount'], r.get('unit'))} & univ)
            acov = len({round(float(r['amount']), 2) for r in a['accepted']
                        if _plausible(r['amount'], r.get('unit'))} & univ)
            if acov < 0.95 * pcov:
                print('%-6s COVERAGE GATE: agent %d < parser %d plausible source-amts -> SKIP'
                      % (a['cao'], acov, pcov)); skipped += 1; continue
        msg, n = merge(a['cao'], a['file'], a['accepted'])
        print('%-6s %s -> %d rows' % (a['cao'], msg, n)); tot += n
    print('TOTAL agent rows merged:', tot, ' files skipped by coverage gate:', skipped,
          ' youth stripped:', youth_tot)
    return tot, skipped, youth_tot


if __name__ == '__main__':
    main()
