"""
Version-selection tagging (TAG, don't MERGE — per docs/CAO_VERSION_SELECTION_PLAN.md).

Most CAOs have several source files for the SAME agreement (the original plus mid-term
republications of the integral text). Downstream analysis (esp. the wage index) must not
double-count editions. Rather than merge wage rows across editions (fragile — labels don't
match), we TAG every row with which term it belongs to and which edition it is, and leave the
pick to analysis time.

Columns added (appended at the END of the row, existing column order untouched):
  term_group        cao_number + '|' + ingangsdatum  (constant across a term's editions)
  kennisgeving_rank 1 = earliest edition (base) ... N = latest, ordered by datum_kennisgeving
  base_id           id of the rank-1 (base) record in the term_group
  n_editions        number of source files in the term_group
  document_type     general_document_type (joined by file_name) — lets analysts keep full_cao_* only

Selection recipe for an analyst:
  - term-level snapshot  -> take kennisgeving_rank == 1 (base) or == n_editions (latest) per term_group
  - full wage timeline   -> pull ALL editions of the term_group (each holds only its wage-table window)
  - drop deltas          -> filter document_type LIKE 'full_cao_%'

Ordering rule (from the plan): rank by datum_kennisgeving (99% populated, = page-header edition
date). NEVER use `id` (does not track recency) or signing_date (26% populated) — id is a
stable tie-break only. Missing kennisgeving sorts LAST.

Both a library (build_version_map, used by flatten_csv.py so deliver.py rebuilds carry the tags)
and a one-shot CLI (retag an existing extracted_data_salary_v2.csv in place, with a .bak).
"""
import csv, os, re, sys, glob, collections
csv.field_size_limit(10**7)

# both date formats occur in the source CSVs (~60% DD/MM/YYYY, ~40% DD-MM-YYYY, per-CAO
# consistent). Parse both, and NORMALIZE the term_group string so a future format flip
# inside one CAO can never split a term.
_DMY = re.compile(r'^(\d{2})[-/](\d{2})[-/](\d{4})$')


def _dmy(s):
    """'DD/MM/YYYY' or 'DD-MM-YYYY' -> (dd, mm, yyyy) or None."""
    m = _DMY.match((s or '').strip())
    return m.groups() if m else None

NON_SALARY_CSV = ('/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/'
                  'outputs/excel/new_results/extracted_data_non_salary.csv')
VERSION_COLS = ['term_group', 'kennisgeving_rank', 'base_id', 'n_editions', 'document_type']


def _ken_sortkey(m):
    """date -> (0, YYYY, MM, DD) so earliest sorts first; unparseable -> (1,...) sorts last.
    id is the stable tie-break (NOT a recency signal, just determinism)."""
    idv = str(m.get('id') or '')
    idnum = int(idv) if idv.isdigit() else 10**9
    d = _dmy(m.get('datum_kennisgeving'))
    if d:
        dd, mm, yyyy = d
        return (0, yyyy, mm, dd, idnum)
    return (1, '', '', '', idnum)


def load_document_type(path=NON_SALARY_CSV):
    """(cao_number, file_name) -> general_document_type (deltas vs full CAO).
    Keyed by (cao, file): the same file_name exists under several CAOs — a filename-only
    join hands them all the first row's value. Empty map if the CSV is absent."""
    dt = {}
    if not os.path.exists(path):
        return dt
    for r in csv.DictReader(open(path), delimiter=';'):
        fn = r.get('file_name')
        key = (r.get('cao_number', ''), fn)
        if fn and key not in dt:
            dt[key] = r.get('general_document_type', '') or ''
    return dt


def build_version_map(meta_by_file, doc_type=None):
    """meta_by_file: (cao_number, file_name) -> {cao_number, ingangsdatum, datum_kennisgeving, id}.
    Returns (cao_number, file_name) -> {term_group, kennisgeving_rank, base_id, n_editions,
    document_type}. NB ranks are computed over ALL known editions of a term (incl. editions
    that produced no salary rows), so 'latest edition present in the salary CSV' is
    max(kennisgeving_rank) among its rows, not necessarily == n_editions."""
    if doc_type is None:
        doc_type = load_document_type()
    groups = collections.defaultdict(list)
    for key, m in meta_by_file.items():
        cao, fn = key
        ing = (m.get('ingangsdatum') or '').strip()
        d = _dmy(ing)
        if d:      # normalized DD/MM/YYYY so slash- and dash-format editions group together
            tg = f"{cao}|{d[0]}/{d[1]}/{d[2]}"
        elif ing:  # unparseable but non-empty: keep verbatim (still per-cao constant)
            tg = f"{cao}|{ing}"
        else:      # empty -> its own singleton group (never falsely merge unrelated editions)
            tg = f"{cao}|file:{fn}"
        groups[tg].append((key, m))
    out = {}
    for tg, files in groups.items():
        ordered = sorted(files, key=lambda it: _ken_sortkey(it[1]))
        base_id = str(ordered[0][1].get('id') or '')
        n = len(ordered)
        for rank, (key, m) in enumerate(ordered, 1):
            out[key] = {'term_group': tg, 'kennisgeving_rank': rank, 'base_id': base_id,
                        'n_editions': n, 'document_type': doc_type.get(key, '')}
    return out


def retag_csv(path):
    """One-shot: append the version columns to an existing flattened salary CSV (in place, .bak)."""
    doc_type = load_document_type()
    # pass 1: per-file metadata from the CSV's own columns (self-consistent with the file)
    meta = {}
    with open(path, newline='') as fh:
        rd = csv.DictReader(fh, delimiter=';')
        base_header = rd.fieldnames
        for row in rd:
            key = (row.get('cao_number', ''), row['file_name'])
            if key not in meta:
                meta[key] = {k: row.get(k, '') for k in
                             ('cao_number', 'ingangsdatum', 'datum_kennisgeving', 'id')}
    vmap = build_version_map(meta, doc_type)
    # strip any prior version cols so a re-run is idempotent
    keep = [c for c in base_header if c not in VERSION_COLS]
    new_header = keep + VERSION_COLS
    bak = path + '.bak'
    os.replace(path, bak)
    with open(bak, newline='') as fin, open(path, 'w', newline='') as fout:
        rd = csv.DictReader(fin, delimiter=';')
        w = csv.writer(fout, delimiter=';')
        w.writerow(new_header)
        for row in rd:
            v = vmap.get((row.get('cao_number', ''), row['file_name']), {})
            w.writerow([row.get(c, '') for c in keep] +
                       [v.get(c, '') for c in VERSION_COLS])
    # report
    ngroups = len(set(x['term_group'] for x in vmap.values()))
    multi = collections.Counter(x['term_group'] for x in vmap.values())
    nmulti = sum(1 for c in multi.values() if c > 1)
    print(f'retag {os.path.basename(path)}: {len(vmap)} files, {ngroups} term_groups '
          f'({nmulti} multi-edition). backup -> {os.path.basename(bak)}')
    dt = collections.Counter(x['document_type'] for x in vmap.values())
    print('  document_type:', dict(dt.most_common()))


if __name__ == '__main__':
    p = (sys.argv[1] if len(sys.argv) > 1 else
         '/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/'
         'outputs/parser_salary/extracted_data_salary_v2.csv')
    retag_csv(p)
