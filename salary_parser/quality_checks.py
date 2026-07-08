"""
Free (no-agent) quality checks that shrink the verification surface:

1. classified_coverage(ext, diags, old_amounts)
     Buckets every SOURCE money cell not emitted by the parser as:
       intentional_excluded  -- table the parser deliberately dropped (intern/youth/allowance)
       intentional_unit      -- non-preferred-unit column (hourly kept-monthly) => seen~=k*emit
       real_gap              -- INCLUDED table, cells seen but not emitted
       unrecognized          -- money in a pipe block block_to_table could not parse
     + old_only: amounts the OLD LLM has that the parser lacks (missed-table disambiguator).
     A file is flagged missing_tables only when real_gap/unrecognized money is BOTH
     substantial AND corroborated by old_only -- so intern tables and hour/month
     duplicates never trip it.

2. monotonicity_flags(rows)
     Within one (jobgroup, worker, age, date), amounts should rise with numeric step; within
     one (step, ...), rise with numeric jobgroup. Returns the row indices sitting in a group
     with >30% inverted adjacent pairs -- a strong deterministic misplacement signal.

Both consume the parser's existing diags -- no re-parsing, no tokens.
"""
import re, collections
import salary_parser as SP


def _money_cells_in_block(blk):
    """count money-looking cells in a raw wage_information block (recognized or not)."""
    n = 0
    for line in SP.flatten_lines(blk) if isinstance(blk, list) else []:
        if not isinstance(line, str):
            continue
        for cell in line.split('|'):
            c = cell.strip()
            v = SP.parse_num(c)
            if v is not None and SP._cell_is_money(c, v):
                n += 1
    return n


def _benign_multiunit(d):
    s, e = d.get('seen_cells', 0), d.get('emitted_points', 0)
    if not e:
        return False
    return any(abs(s - k*e) <= max(2, 0.1*k*e) for k in (2, 3, 4, 5))


def classified_coverage(ext, diags, old_amounts=None, parser_amounts=None):
    incl = [d for d in diags if d.get('included')]
    excl = [d for d in diags if not d.get('included')]
    seen = sum(d.get('seen_cells', 0) for d in incl)
    emit = sum(d.get('emitted_points', 0) for d in incl)
    # real gap = included tables with genuine low coverage (not the benign multi-unit shape)
    real_gap = sum(max(0, d.get('seen_cells', 0) - d.get('emitted_points', 0))
                   for d in incl
                   if d.get('coverage') is not None and d['coverage'] < 0.6 and not _benign_multiunit(d))
    intentional_unit = sum(max(0, d.get('seen_cells', 0) - d.get('emitted_points', 0))
                           for d in incl if _benign_multiunit(d))
    intentional_excluded = sum(_money_cells_in_block_desc(d) for d in excl)

    # unrecognized: money in pipe blocks that block_to_table could NOT parse into a table
    recognized_descs = {d['desc'] for d in diags}
    unrecognized = 0
    for blk in ext.get('wage_information', []):
        if not isinstance(blk, list):
            continue
        if not any('|' in l for l in SP.flatten_lines(blk) if isinstance(l, str)):
            continue
        t = SP.block_to_table(blk)
        if t is None:
            unrecognized += _money_cells_in_block(blk)

    old_only = 0
    if old_amounts is not None and parser_amounts is not None:
        old_only = len(set(old_amounts) - set(parser_amounts))

    # flag a real miss only when uncaptured salary-shaped money is substantial AND the old
    # LLM corroborates it (old_only high) -- intern/unit-dupes never satisfy both
    missing = (real_gap + unrecognized) >= 20 and old_only >= 20
    return {
        'seen': seen, 'emit': emit,
        'real_gap': real_gap, 'unrecognized': unrecognized,
        'intentional_excluded': intentional_excluded, 'intentional_unit': intentional_unit,
        'old_only': old_only, 'missing_tables': missing,
    }


def _money_cells_in_block_desc(d):
    # excluded diags don't retain raw cells; use n_data_rows as a rough proxy of dropped size
    return d.get('n_data_rows', 0)


def _step_num(s):
    if s is None:
        return None
    m = re.search(r'\d+', str(s))
    return int(m.group()) if m else None


def _jg_num(s):
    if s is None:
        return None
    m = re.search(r'\d+', str(s))
    return int(m.group()) if m else None


def _inversion_rate(entries):
    """entries = list of (order_key, amount, row_idx) with numeric order_key.
    Only CLEAN groups are scored: if any order_key carries >1 distinct amount, the group is
    ambiguous (min/max or multi-column-per-date -- already _dup_date-flagged) -> skip.
    Returns (rate, n) where rate = fraction of adjacent pairs decreasing as order_key rises."""
    per_key = collections.defaultdict(set)
    for k, a, _ in entries:
        if k is not None:
            per_key[k].add(round(a, 2))
    if any(len(v) > 1 for v in per_key.values()):
        return 0.0, 0                       # ambiguous group -> not scored
    seq = sorted((k, next(iter(v))) for k, v in per_key.items())
    if len(seq) < 3:
        return 0.0, 0
    # count only GROSS inversions: amount drops >15% as the order key rises. Small dips
    # (uitloop tails, rounding, a genuine 2% step-down) are NOT misplacement and must not
    # flag an otherwise-correct scale. Gross drops = column shift / misparse / wrong cell.
    inv = sum(1 for i in range(len(seq)-1)
              if seq[i][1] > 0 and seq[i+1][1] < 0.85 * seq[i][1])
    return inv / (len(seq)-1), len(seq)


def monotonicity_flags(rows, thresh=0.15):
    """return set of row indices that sit in a CLEAN group with >thresh inverted pairs.
    _dup_date rows are excluded up front (their ambiguity is already flagged elsewhere)."""
    bad = set()
    by_jg = collections.defaultdict(list)    # fixed jobgroup -> vary step
    by_step = collections.defaultdict(list)  # fixed step -> vary jobgroup
    for i, r in enumerate(rows):
        if r.get('_dup_date'):
            continue
        jgn, stn = _jg_num(r.get('jobgroup')), _step_num(r.get('step'))
        for p in r['timeline']:
            key_jg = (str(r.get('jobgroup')), r.get('worker'), r.get('age_group'), p['start_date'])
            by_jg[key_jg].append((stn, p['amount'], i))
            key_st = (str(r.get('step')), r.get('worker'), r.get('age_group'), p['start_date'])
            by_step[key_st].append((jgn, p['amount'], i))
    for group in (by_jg, by_step):
        for key, entries in group.items():
            rate, n = _inversion_rate(entries)
            if n >= 3 and rate > thresh:
                bad.update(i for _, _, i in entries)
    return bad


if __name__ == '__main__':
    import sys, json, glob, os, csv
    from paths import EXT_ROOT, ANA_ROOT, OUT_ROOT
    import old_norm as ON
    files = {r['cao']: r['file'] for r in csv.DictReader(open(os.path.join(OUT_ROOT, 'confidence_cao.csv')))}
    caos = sys.argv[1:] or ['234', '51', '4091', '827', '683', '2948', '475']
    for cao in caos:
        base = files.get(cao)
        ep = os.path.join(EXT_ROOT, cao, base + '_extract.json')
        ext = json.load(open(ep))
        rows, diags, _ = SP.parse_extract(ext)
        ap = os.path.join(ANA_ROOT, cao, base + '_analysis.json')
        oldamts = list(ON.amount_index(ap).keys()) if os.path.exists(ap) else []
        pamts = [round(p['amount'], 2) for r in rows for p in r['timeline']]
        cov = classified_coverage(ext, diags, oldamts, pamts)
        mono = monotonicity_flags(rows)
        print('CAO %-5s rows=%-4d  real_gap=%-4d unrec=%-4d old_only=%-4d MISSING=%-5s  mono_bad=%d/%d'
              % (cao, len(rows), cov['real_gap'], cov['unrecognized'], cov['old_only'],
                 cov['missing_tables'], len(mono), len(rows)))
