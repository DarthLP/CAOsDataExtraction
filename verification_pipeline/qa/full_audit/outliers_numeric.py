"""Check 1 — statistical outliers on numeric fields.

Builds a robust distribution per (field × unit-signature) — the meaningful
"like-with-like" axis. Field maps 1:1 to topic here, so per-field and
per-(field×topic) collapse; the unit-SIGNATURE (coarse dimension + time base,
e.g. hours/week vs hours/year) is what actually separates comparable cohorts —
pooling 38 hrs/wk with 1900 hrs/yr was the dominant false-outlier source. Flags:

  - robust_fence     : BOTH MAD modified |z|>3.5 AND outside [Q1-3·IQR, Q3+3·IQR],
                       AND the value is rare in its group (a value many records
                       share is a 2nd mode, not an idiosyncratic data error)
  - scale_error      : value ≈ 100× / 1000× (or 1/100) the group median
                       — a decimal/unit slip signature
  - negative         : impossible negative in a non-negative field
  - zero_unexpected  : a rare 0 in a field that is normally strongly positive
                       (skips fields where 0 is valid: range_min, pay, contrib)
  - era_baseline     : statutory floor/cap breach (era_baselines.check_outside),
                       run across ALL records (not just the scoped ones)

Surfacing only. Run:  python3.13 -m qa.full_audit.outliers_numeric
"""
from __future__ import annotations

import json
import math
import statistics as st
from collections import Counter, defaultdict
from pathlib import Path

from qa.full_audit import common
from qa.shared import era_baselines

OUT = Path(__file__).resolve().parent / "outliers_numeric.csv"

MIN_N_FENCE = 20         # need this many to trust z/IQR fences
Z_THRESH = 3.5
IQR_K = 3.0
# Scale-error windows kept TIGHT (near-exact 100×/1000×) for precision — a true
# decimal/unit slip lands within ~25% of an exact power-of-ten multiple. Looser
# windows just re-flag ordinary high outliers (already caught by robust_fence).
SCALE_100_LO, SCALE_100_HI = 80.0, 125.0
SCALE_1000_LO, SCALE_1000_HI = 800.0, 1250.0
ZERO_RARE_FRAC = 0.05    # zeros must be <5% of the group to flag a 0
# Rare-value guard for robust_fence: a real data-entry error is idiosyncratic;
# a value SHARED by many records is a recognized minority category (a second
# mode), not an outlier. Skip the fence when the (rounded) value recurs at or
# above this frequency — kills the multimodal clusters (e.g. 191 records all
# with an OT trigger of 0/0.5/1h) while keeping one-off slips (1900 hrs/week).
FENCE_RARE_FRAC = 0.02
FENCE_RARE_MIN = 3


def _mad(values: list[float], med: float) -> float:
    return st.median([abs(v - med) for v in values]) if values else 0.0


def _meanad(values: list[float], med: float) -> float:
    return (sum(abs(v - med) for v in values) / len(values)) if values else 0.0


def _quartiles(values: list[float]) -> tuple[float, float]:
    s = sorted(values)
    n = len(s)
    if n < 4:
        return (s[0], s[-1])
    q1 = s[n // 4]
    q3 = s[(3 * n) // 4]
    return (q1, q3)


def _zero_is_meaningful(col: str, field_base: str) -> bool:
    """True when 0 is a legitimate value, so it must NOT be flagged.

    In this dataset 0 == "none / not applicable" is pervasive and valid:
      - *_range_min   : 0 is a natural lower bound of a range
      - *_pay*        : unpaid leave (0% / 0 pay) is a real, common option
      - *contrib*     : a 0 employee/employer contribution is real
      - *premium*     : a 0 premium share is real
    plus a small allowlist of fields where the schema documents 0 explicitly.
    """
    if col.endswith("_range_min"):
        return True
    if "_pay" in col or "contrib" in col or "premium" in col:
        return True
    return field_base in {
        "parental_min_tenure", "age_min", "min_tenure_months",
        "workhours_adjustment_tenure_requirement",
        "parental_min_contract_length",
    }


def run() -> list[dict]:
    df = common.load_dataset()
    info = common.classify_columns()
    num_cols = common.numeric_columns()
    recs = df.to_dict("records")

    # 1) collect populated numeric values per (field, unit_signature)
    #    keep parallel record refs for flagging + era handling.
    groups: dict[tuple[str, str], list[tuple[float, dict]]] = defaultdict(list)
    for rec in recs:
        for col in num_cols:
            raw = common.norm(rec.get(col, ""))
            if common.is_blank(raw):
                continue
            v = common.to_float(raw)
            if v is None:
                continue  # non-numeric handled by enum_format check
            usig = common.unit_signature(common.unit_for(rec, col))
            groups[(col, usig)].append((v, rec))

    flags: list[dict] = []
    seen: set[tuple[str, str, str]] = set()  # (record_id, field, subcheck) dedup

    def emit(rec, field, subcheck, severity, reason, stat_ctx):
        key = (common.norm(rec.get("id", "")), field, subcheck)
        if key in seen:
            return
        seen.add(key)
        flags.append(common.make_flag(rec, field, "numeric_outlier", severity,
                                      reason, json.dumps(stat_ctx,
                                      ensure_ascii=False), subcheck=subcheck))

    # 2) robust per-group stats + fences
    for (col, usig), pairs in groups.items():
        vals = [v for v, _ in pairs]
        n = len(vals)
        med = st.median(vals)
        q1, q3 = _quartiles(vals)
        iqr = q3 - q1
        mad = _mad(vals, med)
        scale = 1.4826 * mad if mad > 0 else 0.0
        base = info[col].base_field
        nzeros = sum(1 for v in vals if v == 0)
        # frequency of each (rounded) value — drives the rare-value guard below
        vcount = Counter(round(v, 2) for v in vals)
        rare_cut = max(FENCE_RARE_MIN, math.ceil(FENCE_RARE_FRAC * n))

        for v, rec in pairs:
            # negatives — impossible for every numeric field in this dataset
            if v < 0:
                emit(rec, col, "negative", "high",
                     f"negative value {v} in non-negative field (median {med})",
                     {"median": med, "n": n, "unit_sig": usig})
                continue

            # scale-error signature vs group median (decimal/unit slip).
            # TIGHT windows → high-precision subset; also require a meaningful
            # group (n≥MIN) so the median is trustworthy.
            if med > 0 and v > 0 and n >= MIN_N_FENCE:
                ratio = v / med
                if SCALE_100_LO <= ratio <= SCALE_100_HI:
                    emit(rec, col, "scale_error_100x", "high",
                         f"value {v} ≈ {ratio:.0f}× group median {med:g} "
                         f"({usig or 'no-unit'}) — likely decimal/unit slip",
                         {"median": med, "ratio": round(ratio, 1), "n": n,
                          "unit_sig": usig})
                    continue
                if SCALE_1000_LO <= ratio <= SCALE_1000_HI:
                    emit(rec, col, "scale_error_1000x", "high",
                         f"value {v} ≈ {ratio:.0f}× group median {med:g} "
                         f"({usig or 'no-unit'}) — likely 1000× scale slip",
                         {"median": med, "ratio": round(ratio, 1), "n": n,
                          "unit_sig": usig})
                    continue
                if (1 / SCALE_100_HI) <= ratio <= (1 / SCALE_100_LO):
                    emit(rec, col, "scale_error_div100", "medium",
                         f"value {v} ≈ 1/{med / v:.0f} of group median {med:g} "
                         f"({usig or 'no-unit'}) — possible extra decimal",
                         {"median": med, "inv_ratio": round(med / v, 1),
                          "n": n, "unit_sig": usig})
                    continue

            # robust double-criterion fence: BOTH MAD-z>3.5 AND outside the
            # 3·IQR fence. The intersection is the standard guard against a
            # false-positive flood on the skewed, multimodal CAO distributions.
            if n >= MIN_N_FENCE and scale > 0 and iqr > 0:
                z = 0.6745 * (v - med) / scale
                fence_lo, fence_hi = q1 - IQR_K * iqr, q3 + IQR_K * iqr
                out_iqr = v < fence_lo or v > fence_hi
                out_z = abs(z) > Z_THRESH
                # rare-value guard: a value many records share is a 2nd mode,
                # not an idiosyncratic data-entry error — don't fence it.
                rare = vcount[round(v, 2)] < rare_cut
                if out_iqr and out_z and rare:
                    direction = "high" if v > med else "low"
                    emit(rec, col, "robust_fence", "medium",
                         f"value {v} is a {direction} outlier "
                         f"(median {med:g}, IQR {iqr:g}, n={n}, z={z:.1f})",
                         {"median": med, "q1": q1, "q3": q3, "iqr": iqr,
                          "n": n, "z": round(z, 2), "direction": direction,
                          "unit_sig": usig})
                    continue

            # unexpected zero: rare 0 in a strongly-positive field
            if (v == 0 and med > 0 and not _zero_is_meaningful(col, base)
                    and n >= MIN_N_FENCE and nzeros / n < ZERO_RARE_FRAC):
                emit(rec, col, "zero_unexpected", "low",
                     f"zero in normally-positive field (median {med:g}, "
                     f"only {nzeros}/{n} zeros)",
                     {"median": med, "n": n, "zeros": nzeros, "unit_sig": usig})

    # 3) statutory floor/cap breaches across ALL records (era_baselines)
    checked_topics = ["leave", "term", "pension", "contract"]
    for topic in checked_topics:
        eb = era_baselines.EraBaseline(topic)
        checked_fields = sorted({s.field for s in eb._baselines
                                 if s.direction != "informational"})
        if not checked_fields:
            continue
        for rec in recs:
            ing = common.parse_date(rec.get("ingangsdatum", ""))
            if ing is None:
                continue
            for f in checked_fields:
                if f not in df.columns:
                    continue
                val = common.norm(rec.get(f, ""))
                if common.is_blank(val):
                    continue
                unit = common.unit_for(rec, f)
                outside, spec, reason = eb.check_outside(f, val, unit, ing)
                if outside:
                    rid = common.norm(rec.get("id", ""))
                    if (rid, f, "era_baseline") in seen:
                        continue
                    seen.add((rid, f, "era_baseline"))
                    fl = common.make_flag(
                        rec, f, "era_baseline", "high", reason,
                        json.dumps({"statutory_value": spec.value,
                                    "statutory_unit": spec.unit,
                                    "direction": spec.direction,
                                    "source": spec.statutory_source},
                                   ensure_ascii=False),
                        subcheck=spec.direction)
                    flags.append(fl)
    return flags


def main():
    flags = run()
    from qa.shared import resilient_csv
    resilient_csv.write_csv(flags, OUT, fieldnames=common.FLAG_COLUMNS)
    from collections import Counter
    by_sub = Counter(f["subcheck"] for f in flags)
    print(f"[outliers_numeric] {len(flags)} flags -> {OUT}")
    for k, v in by_sub.most_common():
        print(f"  {k:24s} {v}")


if __name__ == "__main__":
    main()
