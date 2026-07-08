"""
Phase 5 -- confidence & flagging roll-up.

Combines, per row:  role-check verdict (+ table-score confidence), _dup_date,
min<=max sanity across (jobgroup,step,age,date) pairs;
per CAO:            tier shares, count-gate status (SOFT flag), low-coverage tables.

Tiers (row):  A = confirmed-high    B = confirmed-med / re-roled / no-old
              C = soft-flagged      D = hard-flagged or structurally inconsistent
Buckets (CAO), acceptance bar per Hanna (strict >=95%):
  auto_accept  : >=95% of rows in A+B and no D rows
  needs_review : 80-95% in A+B, or any D rows on an otherwise-good CAO
  llm_fallback : <80% in A+B (or no parser rows at all)

Reads  outputs/parser_salary/rolecheck/<cao>.json + count_gate.json
Writes outputs/parser_salary/confidence_rows.csv + confidence_cao.csv
"""
import json, glob, os, sys, csv, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import OUT_ROOT, ROLECHECK_DIR

VERDICT_TIER = {
    ('CONFIRMED', 'high'): 'A',
    ('CONFIRMED', 'medium'): 'B',
    ('RE_ROLED', None): 'B',
    ('RELABELED', None): 'B',   # guarded haiku relabel: points locked, labels source-proven
    ('AGENT_EXTRACT', None): 'B',  # guarded whole-file agent extraction: amounts source-proven
    ('UNVERIFIED_NO_OLD', None): 'B',   # no second opinion; parser guarantees still hold
    ('FLAG', None): 'C',                # soft by default; hardened below via reason
    ('FLAG_UNSAFE_REROLE', None): 'C',
    ('FLAG_MIXED', None): 'C',
    ('NO_TABLE', None): 'C',
}

def base_tier(r):
    v = r.get('role_verdict')
    return VERDICT_TIER.get((v, r.get('confidence'))) or VERDICT_TIER.get((v, None), 'C')


import re as _re
def junk_label(r):
    """Money-sized or clock-artifact tokens inside jobgroup/step/worker = junk labels
    (holistic audit 2026-07: CAO 2297 ragged rows fused salary amounts into the step
    label '2170 / 2280 / 24'; Excel '3:00 AM' artifacts). Numbers < 800 stay legit
    (ORBA ranges '150-170', ages, hours, youth staffels '40% / 16')."""
    for f in ('jobgroup', 'step', 'worker'):
        v = r.get(f)
        if not v: continue
        s = str(v)
        if _re.search(r'\d{1,2}:\d{2}\s*(AM|PM)', s, _re.I): return True
        for t in _re.split(r'[\s/|,;+]+', s):
            t = t.strip('€$.*()')
            if not _re.fullmatch(r'\d{3,5}(?:[.,]\d{1,2})?', t): continue
            try: x = float(t.replace(',', '.'))
            except ValueError: continue
            if x >= 800: return True
    return False


# Absolute per-unit sanity ceilings. A real Dutch CAO wage never exceeds these; an amount
# above its unit's ceiling is a concatenation (step-column fused into the wage, e.g. CAO 1574
# GGZ '4'+'1507' -> 41507) or a unit mislabel (a monthly value tagged 'hourly'). The
# provenance guard CANNOT catch a concatenation because the fused token '41507' is literally
# present in the flattened source text. Row-level demote-only; the amount is never altered.
# Ceilings sit well above the observed p99.9 (monthly 13,587; hourly p99 2,999 = mislabels).
_MAG_CEIL = {'monthly': 16000, '4-week': 16000, 'period': 16000, 'weekly': 6000,
             'hourly': 90, 'annual': 400000, 'daily': 2000}
def magnitude_implausible(r):
    """True if any timeline amount exceeds the absolute sanity ceiling for its unit.
    Blank/unknown unit -> skip (cannot judge). Catches the whole-file-concatenation case
    that magnitude_outlier (within-file relative) misses when MOST rows are concatenated."""
    u = (r.get('unit') or '').strip().lower()
    ceil = _MAG_CEIL.get(u)
    if not ceil:
        return False
    for p in r.get('timeline', ()):
        a = p.get('amount')
        if a and a > ceil:
            return True
    return False


def minmax_violations(rows):
    """indices of rows where a (jobgroup,step,age) min/max pair has min>max at a date."""
    pairs = collections.defaultdict(dict)
    for i, r in enumerate(rows):
        jg = str(r.get('jobgroup') or '')   # agent rows may carry a numeric jobgroup
        for tag in ('minimum', 'maximum'):
            suff = ' (%s)' % tag
            if jg.endswith(suff):
                key = (jg[:-len(suff)], r.get('step'), r.get('age_group'))
                pairs[key][tag] = i
    bad = set()
    for key, d in pairs.items():
        if 'minimum' in d and 'maximum' in d:
            lo = {p['start_date']: p['amount'] for p in rows[d['minimum']]['timeline']}
            hi = {p['start_date']: p['amount'] for p in rows[d['maximum']]['timeline']}
            for dt in set(lo) & set(hi):
                if lo[dt] > hi[dt]:
                    bad.add(d['minimum']); bad.add(d['maximum']); break
    return bad

def main():
    # count-gate status per (cao, file)
    gate = {}
    cg_path = os.path.join(OUT_ROOT, 'count_gate.json')
    if os.path.exists(cg_path):
        for cao, base, p, o in json.load(open(cg_path))['per_file']:
            gate[(cao, base)] = 'short' if (o > 0 and p < o) else 'ok'

    rows_csv = open(os.path.join(OUT_ROOT, 'confidence_rows.csv'), 'w', newline='')
    rw = csv.writer(rows_csv)
    rw.writerow(['cao', 'file', 'row_idx', 'jobgroup', 'step', 'worker', 'age_group', 'unit',
                 'n_points', 'role_verdict', 'tier', 'reasons'])
    cao_csv = open(os.path.join(OUT_ROOT, 'confidence_cao.csv'), 'w', newline='')
    cw = csv.writer(cao_csv)
    cw.writerow(['cao', 'file', 'n_rows', 'pct_A', 'pct_B', 'pct_C', 'pct_D',
                 'dup_date_rows', 'minmax_violations', 'mono_flags', 'missing_tables',
                 'count_gate', 'bucket'])

    buckets = collections.Counter(); tier_tot = collections.Counter()
    for f in sorted(glob.glob(os.path.join(ROLECHECK_DIR, '*.json')),
                    key=lambda p: int(os.path.basename(p)[:-5])):
        d = json.load(open(f)); cao, base, rows = d['cao'], d['file'], d['rows']
        # hard-flag reasons live on table verdicts
        hard_tables = {k for k, v in d['table_verdicts'].items()
                       if v.get('severity') == 'hard'}
        mmbad = minmax_violations(rows)
        # DOUBLE-REPRESENTATION guard (Haiku gate, CAO 4091): many (date,amount) pairs
        # shared by >=3 identities = heterogeneous tables merged (art-education vs main
        # scales in one mashed block) -> must not auto-accept.
        pair_count = collections.Counter((p['start_date'], round(p['amount'], 2))
                                         for r in rows for p in r['timeline'])
        n_shared3 = sum(1 for c in pair_count.values() if c >= 3)
        # MAGNITUDE sanity (Haiku gate, CAO 827): same-unit amounts in one file spanning
        # >40x (e.g. 1653.60 misparsed as 165360) -> flag the outlier rows as D.
        by_unit = collections.defaultdict(list)
        for i, r in enumerate(rows):
            for p in r['timeline']:
                if p['amount'] and p['amount'] > 0: by_unit[r.get('unit')].append((p['amount'], i))
        magbad = set()
        for u, vals in by_unit.items():
            amts = sorted(v for v, _ in vals)
            if len(amts) >= 8:
                med = amts[len(amts)//2]
                for v, i in vals:
                    if med > 0 and (v > 40*med or v < med/40): magbad.add(i)
        tiers = []
        for i, r in enumerate(rows):
            t = base_tier(r); reasons = []
            if r.get('role_verdict') == 'FLAG' and \
               any(str(tb) in hard_tables for tb in r.get('_tables', ())):
                t = 'D'; reasons.append('hard_flag_junk_labels')
            if i in mmbad:
                t = 'D'; reasons.append('min_gt_max')
            if i in magbad:
                t = 'D'; reasons.append('magnitude_outlier')
            if r.get('_dup_date'):
                t = {'A': 'B', 'B': 'C'}.get(t, t); reasons.append('dup_date')
            if r.get('_mono'):
                t = {'A': 'C', 'B': 'C'}.get(t, t); reasons.append('gross_step_inversion')
            if r.get('_rescaled'):   # mangled-decimal rescue: documented derivation, never tier A
                t = {'A': 'B'}.get(t, t); reasons.append('rescaled_decimal')
            if r.get('_unit_inferred'):   # magnitude-guessed unit: auditable, keep tier but flag
                reasons.append('unit_inferred_magnitude')
            rp = r.get('_ragged_pad')
            if rp:                   # right-padded source row: alignment not fully provable
                if rp >= 0.5:        # heavy pad (CAO 2165 class): true gaps may be LEADING
                    t = {'A': 'C', 'B': 'C'}.get(t, t); reasons.append('ragged_padded_heavy')
                else:
                    t = {'A': 'B'}.get(t, t); reasons.append('ragged_padded')
            # ordinal/classification rows (holistic audit, CAO 848 ORBA grid): every amount
            # a small whole number and not an hourly table -> counting sequence, not a wage.
            _amts = [p['amount'] for p in r.get('timeline', ())]
            if _amts and r.get('unit') != 'hourly' and \
               all(a == int(a) and a < 13 for a in _amts):
                t = 'D'; reasons.append('nonwage_smallint')
            if junk_label(r):
                t = {'A': 'C', 'B': 'C'}.get(t, t); reasons.append('junk_label')
            if magnitude_implausible(r):   # concatenation / unit-mislabel -> out of trusted set
                t = 'D'; reasons.append('magnitude_implausible')
            tiers.append(t); tier_tot[t] += 1
            rw.writerow([cao, base, i, r.get('jobgroup'), r.get('step'), r.get('worker'),
                         r.get('age_group'), r.get('unit'), len(r['timeline']),
                         r.get('role_verdict'), t, '|'.join(reasons)])
        n = len(tiers); c = collections.Counter(tiers)
        okshare = (c['A'] + c['B']) / n if n else 0.0
        missing = d.get('coverage', {}).get('missing_tables', False)
        if n and okshare >= 0.95 and c['D'] == 0 and n_shared3 < 10 and not missing:
            bucket = 'auto_accept'
        elif n and okshare >= 0.80: bucket = 'needs_review'
        else: bucket = 'llm_fallback'
        buckets[bucket] += 1
        cw.writerow([cao, base, n] +
                    ['%.2f' % (c[t] / n) for t in 'ABCD'] +
                    [sum(1 for r in rows if r.get('_dup_date')), len(mmbad),
                     sum(1 for r in rows if r.get('_mono')), int(bool(missing)),
                     gate.get((cao, base), 'n/a'), bucket])
    rows_csv.close(); cao_csv.close()
    print('===== CONFIDENCE ROLL-UP =====')
    print('row tiers :', dict(tier_tot))
    print('CAO buckets:', dict(buckets))
    print('-> %s + confidence_rows.csv / confidence_cao.csv' % OUT_ROOT)

if __name__ == '__main__':
    main()
