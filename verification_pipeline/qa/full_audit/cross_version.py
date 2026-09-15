"""Check 2 — same-CAO cross-version consistency (consensus / odd-one-out).

Each cao_number is one agreement lineage with up to ~39 dated versions. Naively
flagging every cross-version *change* drowns in routine noise: booleans toggle as
the extractor opportunistically detects a feature, units get rephrased, fields
fill intermittently, and real renegotiations move values legitimately. Flagging
all of that produced ~24k flags — useless.

So we flag only the **odd-one-out against a strong lineage consensus** — the
same idea that tamed Check 1. Within a lineage, if a clear majority of versions
agree on a value and only ≤2 versions disagree, those few are the likely
extraction errors. A 50/50 split (genuine evolution / renegotiation) has no
consensus → nothing is surfaced. This is how we "label likely-error vs
renegotiation": routine renegotiations don't break a consensus, so they are
intentionally NOT flagged; only consensus-breaking minorities are.

Subchecks (all per field, within a cao lineage of ≥4 populated versions):
  - numeric_odd  : a value far (>50% / OOM) from the lineage median while a
                   ≥70% majority cluster near it. high if order-of-magnitude,
                   else medium.  [the core high-value cross-version signal]
  - unit_conflict: a unit whose SIGNATURE (dimension/time-base) conflicts with
                   the lineage consensus (dims differ, or both have a period and
                   the periods differ) — cosmetic "hours" vs "hours/week" is NOT
                   a conflict. medium.
  - dropout      : a SINGLE blank gap with IDENTICAL populated neighbours, in a
                   field populated in a ≥70% majority of versions — the strongest
                   missed-extraction signature. medium.
  - enum_odd     : an enum minority (≤2 versions) against a ≥6-version majority.
                   medium.
  - bool_odd     : a LONE boolean dissenter vs a ≥12-version consensus. LOW and
                   restricted — booleans toggle pervasively here (a field-level
                   reliability issue summarised in full_audit_summary.md), so we
                   surface only the strongest signature and keep it out of the
                   high+medium verification scope.

Surfacing only. Run:  python3.13 -m qa.full_audit.cross_version
"""
from __future__ import annotations

import json
import statistics as st
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

from qa.full_audit import common

OUT = Path(__file__).resolve().parent / "cross_version.csv"

MIN_CONS_VERSIONS = 4    # need ≥4 populated versions to assert a consensus
CONSENSUS_FRAC = 0.70    # majority must cover ≥70% of populated versions
MINORITY_MAX = 2         # flag only ≤2 odd-ones-out (truly idiosyncratic)
JUMP_REL = 0.5           # >50% from median = "far" (numeric)
OOM_RATIO = 10.0         # order-of-magnitude
ENUM_MIN_CONSENSUS = 6   # enum minority needs ≥6 agreeing versions
BOOL_MIN_CONSENSUS = 12  # boolean lone dissenter needs ≥12 agreeing versions


def _rel(a: float, b: float) -> float:
    m = max(abs(a), abs(b))
    return abs(a - b) / m if m > 0 else 0.0


def _is_oom(a: float, b: float) -> bool:
    a, b = abs(a), abs(b)
    if a == 0 or b == 0:
        return False
    return max(a, b) / min(a, b) >= OOM_RATIO


def _order(group: list[dict]) -> list[dict]:
    return sorted(
        group,
        key=lambda r: (common.parse_date(r.get("ingangsdatum", "")) or date.min,
                       common.norm(r.get("file_name", ""))),
    )


def _consensus(items: list[tuple[dict, str]]):
    """items = [(rec, value_key)]. Return (mode, mode_count, n, minority) when a
    strong consensus with a small dissenting minority exists, else None."""
    n = len(items)
    if n < MIN_CONS_VERSIONS:
        return None
    counts = Counter(v for _, v in items)
    mode, mc = counts.most_common(1)[0]
    if mc / n < CONSENSUS_FRAC:
        return None
    minority = [(rec, v) for rec, v in items if v != mode]
    if not (1 <= len(minority) <= MINORITY_MAX):
        return None
    return mode, mc, n, minority


def _sig_conflict(a: str, b: str) -> bool:
    """Two unit signatures conflict if dimensions differ, or both carry a
    period and the periods differ. Bare-vs-qualified (same dim) is cosmetic."""
    if a == b:
        return False
    da, _, pa = a.partition("/")
    db, _, pb = b.partition("/")
    if da != db:
        return True
    return bool(pa) and bool(pb) and pa != pb


def run() -> list[dict]:
    df = common.load_dataset()
    recs = df.to_dict("records")
    num_cols = common.numeric_columns()
    bool_cols = common.boolean_columns()
    enum_cols = common.enum_columns()
    unit_cols = common.unit_columns()

    by_cao: dict[str, list[dict]] = defaultdict(list)
    for r in recs:
        by_cao[common.norm(r.get("cao_number", ""))].append(r)

    flags: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    def emit(rec, field, subcheck, severity, reason, ctx):
        key = (common.norm(rec.get("id", "")), field, subcheck)
        if key in seen:
            return
        seen.add(key)
        flags.append(common.make_flag(rec, field, "cross_version", severity,
                                      reason, json.dumps(ctx, ensure_ascii=False),
                                      subcheck=subcheck))

    for cao, group in by_cao.items():
        if not cao or len(group) < MIN_CONS_VERSIONS:
            continue
        versions = _order(group)
        dates = [common.norm(r.get("ingangsdatum", "")) for r in versions]
        nver = len(versions)

        # ---- numeric: value far from a tight lineage consensus -------------
        for col in num_cols:
            seq = []
            for r in versions:
                raw = common.norm(r.get(col, ""))
                if common.is_blank(raw):
                    continue
                v = common.to_float(raw)
                if v is not None:
                    seq.append((r, v))
            if len(seq) < MIN_CONS_VERSIONS:
                continue
            med = st.median([v for _, v in seq])
            if med == 0:
                continue
            off = [(r, v) for r, v in seq if _rel(v, med) > JUMP_REL]
            near = len(seq) - len(off)
            if near / len(seq) >= CONSENSUS_FRAC and 1 <= len(off) <= MINORITY_MAX:
                for r, v in off:
                    oom = _is_oom(v, med)
                    emit(r, col, "numeric_odd", "high" if oom else "medium",
                         f"value {v:g} is far from the lineage median {med:g} "
                         f"({near}/{len(seq)} versions agree) "
                         f"— {'OOM, ' if oom else ''}likely a single-version error",
                         {"value": v, "median": med, "agree": near,
                          "n": len(seq), "oom": oom, "cao": cao})

        # ---- enum: minority (≤2) breaks a ≥6-version consensus --------------
        for col in enum_cols:
            items = [(r, common.norm(r.get(col, "")))
                     for r in versions if not common.is_blank(r.get(col, ""))]
            res = _consensus(items)
            if not res:
                continue
            mode, mc, n, minority = res
            if mc < ENUM_MIN_CONSENSUS:
                continue
            for r, v in minority:
                emit(r, col, "enum_odd", "medium",
                     f"enum {v!r} dissents from lineage consensus "
                     f"{mode!r} ({mc}/{n} versions) — likely extraction error",
                     {"value": v, "consensus": mode, "agree": mc, "n": n,
                      "cao": cao})

        # ---- boolean: lone dissenter vs a LARGE consensus (low, secondary) --
        # Booleans toggle pervasively across versions in this dataset (a
        # field-reliability issue, not per-record errors), so we surface only
        # the strongest signature — a single dissenter against ≥BOOL_MIN_CONSENSUS
        # agreeing versions — at LOW severity, and summarise the rest at the
        # field level in full_audit_summary.md rather than flooding here.
        for col in bool_cols:
            items = [(r, common.norm(r.get(col, "")))
                     for r in versions if not common.is_blank(r.get(col, ""))]
            res = _consensus(items)
            if not res:
                continue
            mode, mc, n, minority = res
            if len(minority) != 1 or mc < BOOL_MIN_CONSENSUS:
                continue
            r, v = minority[0]
            emit(r, col, "bool_odd", "low",
                 f"boolean {v!r} is the lone dissenter vs consensus "
                 f"{mode!r} ({mc}/{n} versions) — possible extraction miss",
                 {"value": v, "consensus": mode, "agree": mc, "n": n,
                  "cao": cao})

        # ---- unit: signature conflict with lineage consensus ----------------
        for col in unit_cols:
            items = [(r, common.norm(r.get(col, "")))
                     for r in versions if not common.is_blank(r.get(col, ""))]
            sig_items = [(r, common.unit_signature(u)) for r, u in items]
            res = _consensus(sig_items)
            if not res:
                continue
            mode_sig, mc, n, minority = res
            for r, sig in minority:
                if not _sig_conflict(sig, mode_sig):
                    continue
                emit(r, col, "unit_conflict", "medium",
                     f"unit signature {sig!r} conflicts with lineage consensus "
                     f"{mode_sig!r} ({mc}/{n} versions) — possible unit error",
                     {"unit": common.norm(r.get(col, "")), "sig": sig,
                      "consensus_sig": mode_sig, "agree": mc, "n": n, "cao": cao})

        # ---- dropout: blank gap in a normally-present field -----------------
        for col in num_cols + bool_cols + enum_cols:
            pop = [not common.is_blank(r.get(col, "")) for r in versions]
            npop = sum(pop)
            if npop < MIN_CONS_VERSIONS or npop / nver < CONSENSUS_FRAC:
                continue
            blanks = [i for i, p in enumerate(pop) if not p]
            if len(blanks) != 1:               # only a single isolated gap
                continue
            i = blanks[0]
            if not (0 < i < nver - 1 and pop[i - 1] and pop[i + 1]):
                continue
            pv = common.norm(versions[i - 1].get(col, ""))
            nv = common.norm(versions[i + 1].get(col, ""))
            if pv != nv:                        # identical neighbours ⇒ a miss
                continue
            emit(versions[i], col, "dropout", "medium",
                 f"blank at {dates[i]} but populated in {npop}/{nver} versions "
                 f"with identical neighbours ({dates[i-1]} & {dates[i+1]} = "
                 f"{pv!r}) — likely missed extraction",
                 {"prev": pv, "next": nv, "populated": npop, "n": nver,
                  "cao": cao})
    return flags


def main():
    flags = run()
    from qa.shared import resilient_csv
    resilient_csv.write_csv(flags, OUT, fieldnames=common.FLAG_COLUMNS)
    by_sub = Counter(f["subcheck"] for f in flags)
    print(f"[cross_version] {len(flags)} flags -> {OUT}")
    for k, v in by_sub.most_common():
        print(f"  {k:20s} {v}")


if __name__ == "__main__":
    main()
