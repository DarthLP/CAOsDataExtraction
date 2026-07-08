"""
Phase 9 flatten: role-checked per-file salary rows -> the wide CSV schema
(extracted_data_salary.csv), one row per (file, jobgroup x step x worker x age), with up to
80 salary_N_* timeline blocks + our confidence columns appended.

Doc-level metadata (id, TTW, ingangsdatum, ...) is JOINED from the existing CSV by file_name
(those are document properties, not salary properties -- the parser doesn't produce them).
Amounts/dates are the parser's, untouched. NO agents.

Reads outputs/parser_salary/rolecheck_all/<cao>/<file>.json + confidence tier logic.
Writes outputs/parser_salary/extracted_data_salary_v2.csv
"""
import json, glob, os, sys, csv, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import OUT_ROOT
import confidence as CONF
from version_tag import build_version_map, VERSION_COLS

ALL_DIR = os.path.join(OUT_ROOT, 'rolecheck_all')
OLD_CSV = '/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/outputs/excel/new_results/extracted_data_salary.csv'
MAX_PTS = 80

# Plausible magnitude band per unit (full-time, generous — youth low / specialist high).
# A unit_inferred row whose amount falls OUTSIDE its band is the soft spot: a 150-row
# source-anchored audit (2026-07-08) put the unit-error rate at 7.8% for out-of-band vs 1.7%
# in-band (1.93% reweighted overall). We DON'T demote the tier (the amount is still 100%
# provenance-correct — only the unit is uncertain); we add the `unit_inferred_oob` sub-flag so
# an analyst can drop just the ~3,440 risky rows instead of all ~79,500 inferred ones.
_UNIT_BAND = {'monthly': (1500, 7000), '4-week': (1400, 6500), 'period': (1400, 6500),
              'weekly': (350, 1600), 'hourly': (8, 60), 'annual': (18000, 110000),
              'daily': (60, 400)}


def _unit_oob(r):
    lo_hi = _UNIT_BAND.get(r.get('unit'))
    if not lo_hi:
        return False
    lo, hi = lo_hi
    a = r['timeline'][0]['amount'] if r.get('timeline') else None
    return bool(a) and not (lo <= a <= hi)

ID_COLS = ['cao_number', 'id', 'TTW', 'ingangsdatum', 'expiratiedatum', 'datum_kennisgeving',
           'file_name', 'jobgroup', 'step_label', 'worker_type', 'is_entry', 'age_group',
           'education', 'ft_hours', 'permanency', 'hours_type', 'row_note']
PT_SUFFIX = ['start_date', 'end_date', 'amount', 'unit', 'table_label',
             'increase_percent', 'holiday_in_amount', 'note', 'hours_basis_ft_week']
CONF_COLS = ['confidence_tier', 'role_verdict', 'flag_reasons', 'label_source',
             'worker_source', 'source_file', 'coverage_missing_tables']


def doc_meta_by_file():
    """borrow document-level metadata (id/TTW/dates) from the existing CSV, keyed by file_name."""
    meta = {}
    if not os.path.exists(OLD_CSV):
        return meta
    for r in csv.DictReader(open(OLD_CSV), delimiter=';'):
        fn = r.get('file_name')
        if fn and fn not in meta:
            meta[fn] = {k: r.get(k, '') for k in
                        ('cao_number', 'id', 'TTW', 'ingangsdatum', 'expiratiedatum', 'datum_kennisgeving')}
    return meta


def tiers_for_file(rows):
    """replicate confidence.py per-row tiering (base verdict + all downgrades)."""
    mmbad = CONF.minmax_violations(rows)
    pair_count = collections.Counter((p['start_date'], round(p['amount'], 2))
                                     for r in rows for p in r['timeline'])
    by_unit = collections.defaultdict(list)
    for i, r in enumerate(rows):
        for p in r['timeline']:
            if p['amount'] and p['amount'] > 0:
                by_unit[r.get('unit')].append((p['amount'], i))
    magbad = set()
    for u, vals in by_unit.items():
        amts = sorted(v for v, _ in vals)
        if len(amts) >= 8:
            med = amts[len(amts)//2]
            for v, i in vals:
                if med > 0 and (v > 40*med or v < med/40):
                    magbad.add(i)
    out = []
    for i, r in enumerate(rows):
        t = CONF.base_tier(r); reasons = []
        if r.get('role_verdict') == 'FLAG':
            reasons.append('label_unconfirmed')
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
        rp = r.get('_ragged_pad')
        if rp:                   # right-padded source row: alignment not fully provable
            if rp >= 0.5:
                t = {'A': 'C', 'B': 'C'}.get(t, t); reasons.append('ragged_padded_heavy')
            else:
                t = {'A': 'B'}.get(t, t); reasons.append('ragged_padded')
        if r.get('_mixed_unit'):
            reasons.append('mixed_unit')
        if r.get('_unit_inferred'):
            reasons.append('unit_inferred_magnitude')
            if _unit_oob(r):
                reasons.append('unit_inferred_oob')
        # ordinal/classification rows (holistic audit, CAO 848 ORBA grid): every amount a
        # small whole number and not an hourly table -> counting sequence, not a wage.
        amts = [p['amount'] for p in r.get('timeline', ())]
        if amts and r.get('unit') != 'hourly' and \
           all(a == int(a) and a < 13 for a in amts):
            t = 'D'; reasons.append('nonwage_smallint')
        if CONF.junk_label(r):
            t = {'A': 'C', 'B': 'C'}.get(t, t); reasons.append('junk_label')
        if CONF.magnitude_implausible(r):   # concatenation / unit-mislabel -> out of trusted set
            t = 'D'; reasons.append('magnitude_implausible')
        out.append((t, reasons))
    return out


def load_workweek():
    """Full-time workweek maps used to BACKFILL ft_hours on hourly rows whose workweek is stated
    document-level (not per-table), so the dataset is self-contained. Workweeks change over time,
    so the PER-FILE map (file_workweek.csv) is preferred -- a 2010-edition 40h file and a 2020
    36h file of the same CAO get their own value; the CAO-level map (cao_workweek.csv) is the
    fallback for files with no confident per-file signal. Backfilled rows are tagged
    'ft_workweek_cao' so a document-level workweek is never mistaken for a per-table stated one.
    Returns (file_map keyed by (cao,file), cao_map keyed by cao)."""
    fm, cm = {}, {}
    pf = os.path.join(OUT_ROOT, 'file_workweek.csv')
    if os.path.exists(pf):
        for r in csv.DictReader(open(pf)):
            try: fm[(str(r['cao_number']), r['file_name'])] = str(int(float(r['ft_workweek'])))
            except (TypeError, ValueError): pass
    pc = os.path.join(OUT_ROOT, 'cao_workweek.csv')
    if os.path.exists(pc):
        for r in csv.DictReader(open(pc)):
            try: cm[str(r['cao_number'])] = str(int(float(r['ft_workweek'])))
            except (TypeError, ValueError): pass
    return fm, cm


def main():
    meta = doc_meta_by_file()
    file_ww, cao_ww = load_workweek()
    vmap = build_version_map(meta)   # file_name -> term_group / kennisgeving_rank / base_id / ...
    header = ID_COLS[:]
    for n in range(1, MAX_PTS+1):
        header += ['salary_%d_%s' % (n, s) for s in PT_SUFFIX]
    header += CONF_COLS
    header += VERSION_COLS
    out = open(os.path.join(OUT_ROOT, 'extracted_data_salary_v2.csv'), 'w', newline='')
    w = csv.writer(out, delimiter=';')
    w.writerow(header)

    n_rows = n_trunc = 0
    tier_tot = collections.Counter()
    files = sorted(glob.glob(os.path.join(ALL_DIR, '*', '*.json')))
    for f in files:
        d = json.load(open(f))
        rows = d['rows']
        if not rows:
            continue
        cov_missing = d.get('coverage', {}).get('missing_tables', False)
        tiers = tiers_for_file(rows)
        dm = meta.get(d['file'], {})
        for (t, reasons), r in zip(tiers, rows):
            tier_tot[t] += 1; n_rows += 1
            cao_number = dm.get('cao_number', d['cao'])
            # BACKFILL ft_hours: an hourly row whose workweek is stated document-level (not per
            # table) has no parser ft_hours; fill it from the PER-FILE map (this edition's own
            # workweek), else the CAO-level map. Keeps the row self-contained + time-accurate.
            eff_ft = r.get('ft_hours')
            if not eff_ft and any((p.get('unit') or '').lower() == 'hourly'
                                  for p in r.get('timeline', ())):
                ww = file_ww.get((str(cao_number), d['file'])) or cao_ww.get(str(cao_number))
                if ww:
                    eff_ft = ww
                    reasons = reasons + ['ft_workweek_cao']
            rec = {
                'cao_number': cao_number, 'id': dm.get('id', ''),
                'TTW': dm.get('TTW', ''), 'ingangsdatum': dm.get('ingangsdatum', ''),
                'expiratiedatum': dm.get('expiratiedatum', ''),
                'datum_kennisgeving': dm.get('datum_kennisgeving', ''),
                'file_name': d['file'], 'jobgroup': r.get('jobgroup'),
                'step_label': r.get('step'), 'worker_type': r.get('worker'),
                'is_entry': r.get('is_entry'), 'age_group': r.get('age_group'),
                'education': r.get('education'), 'ft_hours': eff_ft,
                'permanency': r.get('permanency'), 'hours_type': r.get('hours_type'),
                'row_note': r.get('_row_note') or r.get('source_table'),
            }
            row = [rec.get(c, '') for c in ID_COLS]
            tl = r['timeline'][:MAX_PTS]
            if len(r['timeline']) > MAX_PTS:
                n_trunc += 1
            for p in tl:
                note = p.get('note')
                if p.get('rescaled_from'):   # mangled-decimal rescue: visible in the CSV
                    note = 'rescaled from %s' % p['rescaled_from']
                row += [p.get('start_date'), p.get('end_date'), p.get('amount'), p.get('unit'),
                        p.get('table_label'), p.get('inc_pct'), p.get('holiday_incl'),
                        note, eff_ft]
            row += [''] * (9 * (MAX_PTS - len(tl)))
            row += [t, r.get('role_verdict'), '|'.join(reasons), r.get('label_source', 'parser'),
                    r.get('worker_source', ''), d['file'], int(bool(cov_missing))]
            v = vmap.get(d['file'], {})
            row += [v.get(c, '') for c in VERSION_COLS]
            w.writerow(row)
    out.close()
    print('===== FLATTEN -> extracted_data_salary_v2.csv =====')
    print('rows written: %d  (timelines truncated at %d pts: %d)' % (n_rows, MAX_PTS, n_trunc))
    print('row tiers:', dict(tier_tot))
    print('-> %s/extracted_data_salary_v2.csv' % OUT_ROOT)


if __name__ == '__main__':
    main()
