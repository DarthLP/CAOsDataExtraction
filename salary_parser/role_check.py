"""
Phase 3 -- ROLE-CHECK MERGE (design: Hanna 2026-07-03, PLAN_phase3_rolecheck.md).

The old LLM's per-cell label assignments carry its misplacement bug, but its field
VOCABULARIES ("in this file jobgroups look like 'Scale A'..'Scale D', steps look like
0..18") are reliable. So, per parser TABLE:
  1. match the table to old-LLM rows by AMOUNT-SET overlap    (diagnostic only)
  2. score parser-field token sets against old field vocabularies
  3. verdict: CONFIRMED / RE-ROLE (permute the parser's OWN fields) / NO_MAPPING (flag)
     / NO_OLD (unverified)
  4. apply per row via its contributing tables; enrich worker (table-constant only)
     and inc_pct (table-title regex; date-level metadata).
Amounts, dates and cell bindings are NEVER touched. No old-LLM string is written into
a row (worker constant-fill is the one table-level exception, tagged with its source).

Usage (from CAOsDataExtraction repo root):
  python3 role_check.py                # all CAOs, report + out/ROLECHECK/
  python3 role_check.py 51 1618 1630   # specific CAOs, verbose
"""
import json, glob, os, re, sys, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import salary_parser as SP
import old_norm as ON
import quality_checks as QC

from paths import EXT_ROOT, ANA_ROOT, ROLECHECK_DIR as OUT_DIR

# ---------------------------------------------------------------- token matching
_norm_re = re.compile(r'[^a-z0-9]+')

def norm_tok(s):
    return _norm_re.sub(' ', str(s).lower()).strip()

def tok_words(s):
    return set(norm_tok(s).split())

def _num_eq(a, b):
    try: return float(a) == float(b)
    except (TypeError, ValueError): return False

def tok_match(tok, vocab_entry):
    """fuzzy: equal after normalization; numeric-equal ('05'=='5'); or word-boundary
    containment in either direction ('5' in 'salarisschaal 5', 'scale a' contains 'a')."""
    a, b = norm_tok(tok), norm_tok(vocab_entry)
    if not a or not b: return False
    if a == b or _num_eq(a, b): return True
    wa, wb = tok_words(a), tok_words(b)
    return wa <= wb or wb <= wa

def split_composites(tokens):
    """parser tokens can be composites ('1 / 4', 'A (minimum)') -> compare their parts too."""
    out = set()
    for t in tokens:
        t = re.sub(r'\((?:minimum|maximum)\)', ' ', str(t))
        out.add(t.strip())
        for part in t.split(' / '):
            if part.strip(): out.add(part.strip())
    return {t for t in out if t}

def containment(parser_tokens, old_vocab, cap=40):
    """fraction of parser tokens matching ANY old vocab entry (None if either side empty)."""
    ptoks = sorted(split_composites(parser_tokens))[:cap]
    if not ptoks or not old_vocab: return None
    ov = list(old_vocab)[:200]
    hit = sum(1 for t in ptoks if any(tok_match(t, v) for v in ov))
    return hit / len(ptoks)

# ---------------------------------------------------------------- per-table verdict
PARSER_FIELDS = ('jg', 'step', 'age')          # parser axes we role-check
OLD_FIELDS = ('jobgroup', 'step', 'age', 'worker')
IDENTITY = {'jg': 'jobgroup', 'step': 'step', 'age': 'age'}
MIN_SCORE = 0.6      # a mapping must match >=60% of tokens to count
MARGIN = 0.25        # a re-role must beat identity by this much

def table_verdict(dg, old_vocab_sub, old_vocab_file):
    """score parser field token-sets vs old vocabularies; return verdict dict."""
    toks = {'jg': set(dg.get('jg_tokens', ())),
            'step': set(dg.get('step_tokens', ())),
            'age': set(dg.get('age_tokens', ()))}
    scores = {}          # (parser_field, old_field) -> score
    for pf in PARSER_FIELDS:
        if not toks[pf]: continue
        for of in OLD_FIELDS:
            vocab = old_vocab_sub.get(of) or old_vocab_file.get(of) or set()
            s = containment(toks[pf], vocab)
            if s is not None: scores[(pf, of)] = s
    if not scores:
        return {'verdict': 'NO_MAPPING', 'reason': 'no comparable tokens', 'scores': {}}
    # best old-field per parser-field (ties -> identity wins)
    mapping, detail = {}, {}
    for pf in PARSER_FIELDS:
        cand = {of: s for (p, of), s in scores.items() if p == pf}
        if not cand: continue
        ident_of = IDENTITY[pf]
        ident_s = cand.get(ident_of, 0.0)
        best_of = max(cand, key=lambda of: (cand[of], of == ident_of))
        best_s = cand[best_of]
        detail[pf] = {'identity': round(ident_s, 2),
                      'best': (best_of, round(best_s, 2)), 'all': {k: round(v, 2) for k, v in cand.items()}}
        if best_s < MIN_SCORE:
            # SPARSE-OLD AGREEMENT (CAO 750 '18122024' class): the old extraction found
            # only a fraction of the table (vocab {I,II} vs parser I..IX) so forward
            # containment is low -- but if (a) the old vocab is itself contained in the
            # parser's tokens for the IDENTITY field (the old LLM agrees everywhere it
            # has an opinion) and (b) there is no cross-field signal, identity holds.
            ov = old_vocab_sub.get(ident_of) or old_vocab_file.get(ident_of) or set()
            rev = containment(ov, toks[pf]) if ov and toks[pf] else None
            cross_max = max((s for of2, s in cand.items() if of2 != ident_of), default=0.0)
            if rev is not None and rev >= 0.8 and ident_s > 0 and cross_max < 0.1:
                mapping[pf] = ident_of
                detail[pf]['sparse_old'] = round(rev, 2)
            else:
                mapping[pf] = None                      # nothing matches well -> unknown
        elif best_of == ident_of or best_s < ident_s + MARGIN:
            mapping[pf] = ident_of                      # identity holds
        else:
            mapping[pf] = best_of                       # clear re-role winner
    known = {pf: of for pf, of in mapping.items() if of}
    # JUNK-LABEL GUARD: parser has jobgroup tokens, the old LLM has a jobgroup vocabulary,
    # yet the tokens match NOTHING in ANY old field -> labels are junk (e.g. amounts leaked
    # into jobgroup on a mangled table). Never CONFIRM such a table.
    jg_d = detail.get('jg')
    if toks['jg'] and (old_vocab_sub.get('jobgroup') or old_vocab_file.get('jobgroup')) \
       and jg_d and max(jg_d['all'].values(), default=0.0) < 0.3:
        # HARD junk only when the tokens actually LOOK like leaked VALUES (amounts /
        # dates) -- the guard's real target (mangled tables). Plausible scale codes the
        # old extraction simply missed (CAO 750: parser I..IX vs old vocab {I,II};
        # HISWA: named groups while step matches 1.0) fall through to normal scoring,
        # where solid other fields can still confirm the table.
        def _valueish(t):
            s = str(t)
            v = SP.parse_num(s)
            return (v is not None and (v >= 100 or '.' in s or ',' in s)) \
                or bool(SP.parse_header_date(s))
        n_val = sum(1 for t in toks['jg'] if _valueish(t))
        if n_val * 2 >= len(toks['jg']):
            return {'verdict': 'NO_MAPPING', 'reason': 'jobgroup tokens unrecognized (junk labels?)',
                    'scores': detail}
    if not known:
        return {'verdict': 'NO_MAPPING', 'reason': 'all fields below MIN_SCORE', 'scores': detail}
    # CONFLICT RESOLUTION: two parser fields claiming the same old field is contamination,
    # not a permutation -- but a WEAK claim must not nuke a table whose other fields are
    # solid. Keep only the strongest claim per contested target; drop the weaker ones
    # (they revert to unknown = keep-as-is). Flag only if the top claims are ~equal.
    by_target = collections.defaultdict(list)
    for pf, of in known.items():
        by_target[of].append((detail[pf]['best'][1], pf))
    for of, claims in by_target.items():
        if len(claims) > 1:
            claims.sort(reverse=True)
            if claims[0][0] - claims[1][0] < 0.1:
                # a TIE defaults to IDENTITY: identity is the null hypothesis; a cross-
                # claim must BEAT it, not merely tie (numeric step tokens routinely tie
                # into the old jobgroup vocab -- CAO 569 class). Flag only when the top
                # claims are ~equal and the identity owner is NOT among them.
                ident = next(((s, pf) for s, pf in claims if IDENTITY[pf] == of), None)
                if ident and claims[0][0] - ident[0] < 0.1:
                    for s, pf in claims:
                        if pf != ident[1]:
                            del known[pf]      # cross-claimants revert to keep-as-is
                else:
                    return {'verdict': 'NO_MAPPING', 'reason': 'conflicting role claims', 'scores': detail}
            else:
                for _, pf in claims[1:]:
                    del known[pf]
    if all(of == IDENTITY[pf] for pf, of in known.items()):
        conf = 'high' if any(d['identity'] >= 0.8 for d in detail.values()) else 'medium'
        return {'verdict': 'CONFIRMED', 'confidence': conf, 'scores': detail}
    return {'verdict': 'RE_ROLE', 'mapping': known, 'scores': detail}

# ---------------------------------------------------------------- inc_pct from titles
INC_RE = re.compile(r'(?:\+|including a|inclusief|incl\.?|met|verhoging van)\s*'
                    r'([\d]+(?:[.,]\d+)?)\s*(?:%|percent|procent)', re.I)

def inc_pct_from_desc(desc):
    m = INC_RE.search(desc or '')
    if not m: return None
    try: return float(m.group(1).replace(',', '.'))
    except ValueError: return None

# ---------------------------------------------------------------- per-file merge
def rolecheck_file(ext_path, ana_path):
    ext = json.load(open(ext_path))
    rows, diags, dd = SP.parse_extract(ext)
    if not rows: return None
    incl = {d['tidx']: d for d in diags if d.get('included') and d.get('amounts')}

    have_old = ana_path and os.path.exists(ana_path)
    vocab = ON.file_vocab(ana_path) if have_old else None
    old_has_rows = bool(vocab and vocab['rows'])
    vocab_amounts = {a for r in (vocab['rows'] if vocab else []) for a in r['amounts']}

    verdicts = {}
    for tidx, dg in incl.items():
        if not old_has_rows:
            verdicts[tidx] = {'verdict': 'NO_OLD'}
            continue
        # amount-set overlap -> matched old subset (DIAGNOSTIC + vocab scoping only)
        t_amts = set(dg['amounts'])
        matched = [r for r in vocab['rows'] if r['amounts'] & t_amts]
        overlap = len({a for r in matched for a in r['amounts']} & t_amts) / max(len(t_amts), 1)
        sub = {f: {r[f] for r in matched if f in r} for f in OLD_FIELDS} if len(matched) >= 3 else {}
        fullv = {f: vocab[f] for f in OLD_FIELDS}
        v = table_verdict(dg, sub, fullv)
        # SEVERITY (Haiku round-1, CAO 1471): 'conflicting role claims' and low-overlap
        # NO_MAPPINGs are mostly cosmetic vocabulary differences or missing second opinion
        # -> soft; unrecognized-jobgroup (junk labels) -> hard.
        if v['verdict'] == 'NO_MAPPING':
            v['severity'] = 'hard' if 'unrecognized' in v.get('reason', '') else 'soft'
        v['amount_overlap'] = round(overlap, 2)
        v['n_matched_old_rows'] = len(matched)
        # table-level CONSTANTS from the matched subset (>=90% same non-null value, n>=3):
        # worker + education/permanency/ft_hours/hours_type -- non-positional attributes,
        # safe to attach at table level (never per amount)
        for fld in ('worker', 'education', 'permanency', 'ft_hours', 'hours_type'):
            vals = [r[fld] for r in matched if r.get(fld) not in (None, '')]
            if vals:
                top, n = collections.Counter(map(str, vals)).most_common(1)[0]
                if n >= 0.9 * len(vals) and len(vals) >= 3:
                    v.setdefault('const', {})[fld] = top
        if v.get('const', {}).get('worker'): v['worker_const'] = v['const']['worker']
        v['inc_pct'] = inc_pct_from_desc(dg['desc'])
        verdicts[tidx] = v

    # ---- apply to rows via contributing tables ----
    out = []
    for r in rows:
        r = dict(r)
        vts = [verdicts.get(t) for t in r.get('_tables', ()) if verdicts.get(t)]
        kinds = {v['verdict'] for v in vts} if vts else set()
        if not vts:
            r['role_verdict'] = 'NO_TABLE'; r['confidence'] = 'low'
        elif kinds == {'CONFIRMED'}:
            r['role_verdict'] = 'CONFIRMED'
            r['confidence'] = min((v.get('confidence', 'medium') for v in vts),
                                  key=lambda c: {'high': 0, 'medium': 1}.get(c, 2))
        elif kinds == {'NO_OLD'}:
            r['role_verdict'] = 'UNVERIFIED_NO_OLD'; r['confidence'] = 'medium'
        elif kinds == {'RE_ROLE'}:
            maps = [tuple(sorted(v['mapping'].items())) for v in vts]
            if len(set(maps)) == 1:
                m = vts[0]['mapping']
                src = {'jg': r.get('jobgroup'), 'step': r.get('step'), 'age': r.get('age_group')}
                tgt_key = {'jobgroup': 'jobgroup', 'step': 'step', 'age': 'age_group', 'worker': 'worker'}
                # APPLY-SAFETY: a re-role may only write into fields that are either empty
                # or themselves re-mapped away -- never clobber an unmapped token.
                freed = {tgt_key[IDENTITY[pf]] for pf in m}
                safe = all(r.get(tgt_key[of]) is None or tgt_key[of] in freed
                           for of in m.values())
                if safe:
                    # clear re-mapped source fields, then place tokens at their voted role
                    for pf in m: r[tgt_key[IDENTITY[pf]]] = None
                    for pf, of in m.items():
                        if src[pf] is not None: r[tgt_key[of]] = src[pf]
                    r['role_verdict'] = 'RE_ROLED'; r['confidence'] = 'medium'
                    r['role_mapping'] = {pf: of for pf, of in m.items()}
                    # Haiku round-2 (317): a re-role that leaves jobgroup empty loses the
                    # table context -> carry the source-table desc as metadata (audit trail)
                    if not r.get('jobgroup'):
                        t0 = next((t for t in r.get('_tables', ()) if t in incl), None)
                        if t0 is not None: r['source_table'] = incl[t0]['desc'][:60]
                else:
                    r['role_verdict'] = 'FLAG_UNSAFE_REROLE'; r['confidence'] = 'low'
            else:
                r['role_verdict'] = 'FLAG_MIXED'; r['confidence'] = 'low'
        else:
            r['role_verdict'] = 'FLAG' if 'NO_MAPPING' in kinds else 'FLAG_MIXED'
            r['confidence'] = 'low'
        # table-level enrichment (source-tagged; never overwrites a parser value)
        for v in vts:
            for fld, val in (v.get('const') or {}).items():
                if not r.get(fld):
                    r[fld] = val; r.setdefault('_enriched', []).append(fld)
            if v.get('inc_pct') is not None:
                for p in r['timeline']:
                    p.setdefault('inc_pct', v['inc_pct'])
        if 'worker' in (r.get('_enriched') or []): r['worker_source'] = 'old_llm_table_constant'
        r['label_source'] = 'parser'
        out.append(r)
    # free deterministic quality checks (no tokens): gross-inversion soft flag + coverage
    mono = QC.monotonicity_flags(out)
    for i in mono: out[i]['_mono'] = True
    p_amts = [round(p['amount'], 2) for r in out for p in r['timeline']]
    o_amts = list(vocab_amounts) if have_old else None
    cov = QC.classified_coverage(ext, diags, o_amts, p_amts)
    return {'rows': out, 'verdicts': verdicts, 'n_tables': len(incl), 'coverage': cov}

# ---------------------------------------------------------------- corpus run
def pick_file(cao):
    for f in sorted(glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json'))):
        ext = json.load(open(f))
        rows, _, _ = SP.parse_extract(ext)
        if rows: return f
    return None

def main(caos=None, verbose=False):
    os.makedirs(OUT_DIR, exist_ok=True)
    all_caos = sorted([d for d in os.listdir(EXT_ROOT)
                       if re.fullmatch(r'\d+', d) and os.path.isdir(os.path.join(EXT_ROOT, d))], key=int)
    caos = caos or all_caos
    t_stats = collections.Counter(); r_stats = collections.Counter()
    worker_before = worker_after = inc_after = total_rows = 0
    for cao in caos:
        f = pick_file(cao)
        if not f: continue
        base = os.path.basename(f).replace('_extract.json', '')
        ana = os.path.join(ANA_ROOT, cao, base + '_analysis.json')
        res = rolecheck_file(f, ana)
        if not res: continue
        for v in res['verdicts'].values(): t_stats[v['verdict']] += 1
        for r in res['rows']:
            r_stats[r['role_verdict']] += 1
            total_rows += 1
            if r.get('worker_source'): worker_after += 1
            elif r.get('worker'): worker_before += 1; worker_after += 1
            if any('inc_pct' in p for p in r['timeline']): inc_after += 1
        json.dump({'cao': cao, 'file': base, 'rows': res['rows'],
                   'coverage': res.get('coverage', {}),
                   'table_verdicts': {str(k): v for k, v in res['verdicts'].items()}},
                  open(os.path.join(OUT_DIR, '%s.json' % cao), 'w'), indent=1, ensure_ascii=False)
        if verbose:
            print('=== CAO %s (%s): %d tables ===' % (cao, base[:40], res['n_tables']))
            for tidx, v in res['verdicts'].items():
                print('  t%-3d %-12s overlap=%-5s %s' % (tidx, v['verdict'],
                      v.get('amount_overlap', '-'), v.get('mapping', '') or v.get('reason', '')))
                for pf, d in (v.get('scores') or {}).items():
                    print('        %-5s identity=%.2f best=%s all=%s' % (pf, d['identity'], d['best'], d['all']))
            for r in res['rows'][:6]:
                print('   [%s/%s] jg=%r step=%r worker=%r' % (r['role_verdict'], r['confidence'],
                      r['jobgroup'], r['step'], r['worker']))
    print('\n===== ROLE-CHECK REPORT (%d CAOs) =====' % len(caos))
    print('TABLES:', dict(t_stats))
    print('ROWS  :', dict(r_stats))
    print('worker filled: %d of %d rows (%d via old-LLM table-constant)' % (
        worker_after, total_rows, worker_after - worker_before))
    print('inc_pct on >=1 point: %d rows' % inc_after)

if __name__ == '__main__':
    args = sys.argv[1:]
    main(args or None, verbose=bool(args))
