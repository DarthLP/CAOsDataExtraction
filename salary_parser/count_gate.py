"""
STRICT PER-FILE COUNT GATE (Hanna 2026-07-01):
  The parser must emit >= as many salary amounts as the old-LLM analysis output
  in EVERY file. Fewer than old LLM => FAIL (auto-flag). More is fine.

Matches each `<file>_extract.json` to its `<file>_analysis.json` sibling by base name.
Reports, per file: parser_amounts vs old_amounts, and a global pass/fail tally +
the worst shortfalls (candidate parser-missing tables to fix in Phase 1).

Run from CAOsDataExtraction repo root.
Usage: python3 <scratch>/count_gate.py [N_CAOS]
"""
import json, glob, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import salary_parser as SP
import old_norm as ON

from paths import EXT_ROOT, ANA_ROOT, OUT_ROOT


def parser_amounts(ext_path):
    try:
        ext = json.load(open(ext_path))
    except Exception:
        return None
    rows, diags, dd = SP.parse_extract(ext)
    return sum(len(r['timeline']) for r in rows)


def main():
    caos = sorted([d for d in os.listdir(EXT_ROOT)
                   if re.fullmatch(r'\d+', d) and os.path.isdir(os.path.join(EXT_ROOT, d))],
                  key=int)
    if len(sys.argv) > 1:
        caos = caos[:int(sys.argv[1])]

    per_file = []       # (cao, base, p_amt, o_amt)
    for cao in caos:
        exts = sorted(glob.glob(os.path.join(EXT_ROOT, cao, '*_extract.json')))
        for ep in exts:
            base = os.path.basename(ep).replace('_extract.json', '')
            ap = os.path.join(ANA_ROOT, cao, base + '_analysis.json')
            p = parser_amounts(ep)
            o = ON.count_amounts(ap) if os.path.exists(ap) else 0
            if p is None:
                continue
            per_file.append((cao, base, p, o))

    # A file is IN SCOPE for the gate only if old LLM produced amounts there
    # (if old LLM is empty, there is nothing to be "fewer than").
    scoped = [x for x in per_file if x[3] > 0]
    passed = [x for x in scoped if x[2] >= x[3]]
    failed = [x for x in scoped if x[2] < x[3]]
    exact = [x for x in scoped if x[2] == x[3]]
    more = [x for x in scoped if x[2] > x[3]]

    print('===== STRICT PER-FILE COUNT GATE (parser >= old-LLM amounts) =====')
    print('  files with a parser result           : %d' % len(per_file))
    print('  files in scope (old LLM has amounts) : %d' % len(scoped))
    print('  PASS  (parser >= old)                : %d  (%.1f%%)' % (len(passed), 100*len(passed)/max(len(scoped),1)))
    print('     of which exact-equal              : %d' % len(exact))
    print('     of which parser has MORE          : %d' % len(more))
    print('  FAIL  (parser < old, SHORTFALL)      : %d  (%.1f%%)' % (len(failed), 100*len(failed)/max(len(scoped),1)))

    # aggregate shortfall by CAO
    from collections import defaultdict
    cao_fail = defaultdict(list)
    for cao, base, p, o in failed:
        cao_fail[cao].append((base, p, o))
    print('\n  CAOs with >=1 failing file: %d' % len(cao_fail))
    # worst shortfalls first (by missing count)
    worst = sorted(failed, key=lambda x: (x[3]-x[2]), reverse=True)
    print('  --- worst per-file shortfalls (parser < old) ---')
    for cao, base, p, o in worst[:35]:
        print('    CAO %-5s parser=%-4d old=%-4d  miss=%-4d  %s' % (cao, p, o, o-p, base[:40]))

    json.dump({'per_file': per_file, 'failed': failed},
              open(os.path.join(OUT_ROOT, 'count_gate.json'), 'w'),
              indent=1, ensure_ascii=False)

    # zero-parser-but-old-has-amounts = total misses (most urgent)
    zero = [x for x in failed if x[2] == 0]
    print('\n  files where parser=0 but old LLM has amounts (total miss): %d' % len(zero))
    for cao, base, p, o in sorted(zero, key=lambda x: -x[3])[:20]:
        print('    CAO %-5s old=%-4d  %s' % (cao, o, base[:44]))


if __name__ == '__main__':
    main()
