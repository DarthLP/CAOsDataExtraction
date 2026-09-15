"""Agreement-consistency QA layer (SURFACE-ONLY — writes NO dataset).

Insight (Hanna, 2026-06-04): multiple full-CAO files sharing one `ingangsdatum`
are version-records of ONE multi-year agreement, not separate renewals. The right
grouping unit is the AGREEMENT = (cao_number x ingangsdatum), NOT cao_number alone
(which is what the old cross_version check used and why it was noisy). Within an
agreement the structured fields are ~83% identical; the rest splits into:

  * completeness GAPS  — one version blank, a sibling fills it  -> back-fill cand.
  * genuine CONFLICTS  — >=2 distinct filled values            -> verify (error or
                                                                   real mid-term change)

This module builds, against qa/corrected_dataset.csv (the promoted dataset):
  - agreement_index.csv          one row per agreement (records, TTW, doc_types)
  - backfill_candidates.csv      every blank cell a sibling can fill (~17k)
  - agreement_conflicts.csv      every within-agreement disagreement, triaged
  - agreement_consistency_summary.md   headline numbers + the regression metric

NOTHING is applied. Surface-only, per project convention. Stage 3 (definition-
anchored subagent verification) and Stage 4 (guarded apply) come later, on approval.

Run: python3.13 -m qa.full_audit.agreement_consistency [dataset_csv]
     (default dataset = qa/corrected_dataset.csv)
"""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from itertools import groupby
from pathlib import Path

import pandas as pd

from qa.full_audit import common

HERE = Path(__file__).resolve().parent
DEFAULT_DATASET = common.PROJECT_ROOT / "qa" / "corrected_dataset.csv"

IDX_OUT = HERE / "agreement_index.csv"
BACKFILL_OUT = HERE / "backfill_candidates.csv"
CONFLICT_OUT = HERE / "agreement_conflicts.csv"
SUMMARY_OUT = HERE / "agreement_consistency_summary.md"

# Version-ordering date (per-record filing date; ingangsdatum is shared so it
# cannot order siblings). Fall back through these, then record id.
ORDER_DATE_COLS = ["datum_kennisgeving", "general_signing_date",
                   "general_chg_eff_date", "expiratiedatum"]

STRUCTURED_KINDS = {"numeric", "unit", "enum", "boolean"}


def _structured_cols(info):
    excl = set(common.DATE_COLS) | {"general_document_type"}
    return [c for c, ci in info.items()
            if ci.topic in (set(common.TOPICS) | {"general"})
            and ci.kind in STRUCTURED_KINDS and c not in excl]


def _canon(kind, v):
    """Comparison key: float-normalize numerics (4 == 4.0), lower-strip else.
    None for blank."""
    if common.is_blank(v):
        return None
    if kind == "numeric":
        f = common.to_float(v)
        return None if f is None else f"{f:.6g}"
    return str(v).strip().lower()


def _order_key(rec):
    """Sort key to order an agreement's version-records in filing-time order."""
    for c in ORDER_DATE_COLS:
        d = common.parse_date(rec.get(c, ""))
        if d is not None:
            return (0, d.isoformat(), str(rec.get("id", "")))
    return (1, "", str(rec.get("id", "")))          # undated -> after dated, by id


def _record_fill(rec, structured):
    return sum(1 for c in structured if not common.is_blank(rec.get(c, "")))


def _triage(kind, seq_in_date_order, raw_units_filled, n_filled):
    """Route a conflict. seq_in_date_order = canon values (filled only) ordered by
    filing date; raw_units_filled = the raw unit strings (unit fields only).
    Returns (triage_label, priority, lone_minority_bool)."""
    # unit wording that collapses under period-normalization is not a real conflict
    if kind == "unit" and raw_units_filled:
        sigs = {common.unit_signature(u) for u in raw_units_filled}
        if len(sigs) == 1:
            return "UNIT_WORDING", "low", False

    counts = Counter(seq_in_date_order)
    lone = any(v == 1 for v in counts.values()) and len(seq_in_date_order) >= 3

    if kind == "boolean":
        return "BOOLEAN_DISAGREEMENT", "medium", lone

    if n_filled == 2:
        # a 1-vs-1 disagreement is genuinely ambiguous (error OR mid-term change);
        # only source/definition can tell -> highest review value.
        return "AMBIGUOUS_2REC", "high", False

    # n_filled >= 3: clean step (each value in one contiguous block) suggests a
    # real temporal change; recurrence (e.g. 2,5,2) is noise -> error.
    blocks = [k for k, _ in groupby(seq_in_date_order)]
    clean_step = len(blocks) == len(set(blocks))
    if clean_step:
        return "LIKELY_TEMPORAL_CHANGE", "low", lone
    return "LIKELY_EXTRACTION_ERROR", "high", lone


def main():
    ds_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATASET
    df = pd.read_csv(ds_path, sep=";", dtype=str, keep_default_na=False)
    info = common.classify_columns()
    structured = _structured_cols(info)
    inscope = common.inscope_caos()

    recs = df.to_dict("records")
    fill_of = {r["id"]: _record_fill(r, structured) for r in recs}

    # group into agreements
    agr = defaultdict(list)
    for r in recs:
        agr[(common.norm(r["cao_number"]), common.norm(r["ingangsdatum"]))].append(r)

    idx_rows, backfill_rows, conflict_rows = [], [], []
    # regression metric accumulators (structured cells in multi-record agreements)
    m_identical = m_gap = m_conflict = 0
    gapcells_by_topic = Counter()
    conflict_by_topic = Counter()
    conflict_by_triage = Counter()
    backfill_by_conf = Counter()

    for (cao, ing), group in agr.items():
        aid = f"{cao}@{ing}"
        in_scope = cao in inscope
        ttw = Counter(common.norm(r.get("TTW", "")).lower() for r in group)
        doctypes = sorted({common.norm(r.get("general_document_type", "")) for r in group} - {""})
        idx_rows.append({
            "agreement_id": aid, "cao_number": cao, "ingangsdatum": ing,
            "n_records": len(group),
            "record_ids": ",".join(common.norm(r["id"]) for r in group),
            "ttw_yes": ttw.get("yes", 0), "ttw_no": ttw.get("no", 0),
            "doc_types": ",".join(doctypes),
            "in_scope": in_scope,
        })
        if len(group) < 2:
            continue

        ordered = sorted(group, key=_order_key)
        for c in structured:
            ci = info[c]
            topic, kind = ci.topic, ci.kind
            ucol = ci.unit_partner
            vals = [(r, _canon(kind, r.get(c, ""))) for r in ordered]
            filled = [(r, cv) for r, cv in vals if cv is not None]
            n_filled = len(filled)
            if n_filled == 0:
                continue
            n_blank = len(vals) - n_filled
            distinct = {cv for _, cv in filled}

            if len(distinct) == 1 and n_blank == 0:
                m_identical += 1
                continue

            if len(distinct) == 1 and n_blank > 0:
                # ---- completeness GAP -> back-fill candidate ----
                m_gap += 1
                gapcells_by_topic[topic] += n_blank
                donor_recs = [r for r, _ in filled]
                blank_recs = [r for r, cv in vals if cv is None]
                # representative raw value/unit from the most complete donor
                donor = max(donor_recs, key=lambda r: fill_of[r["id"]])
                raw_val = common.norm(donor.get(c, ""))
                raw_unit = common.norm(donor.get(ucol, "")) if ucol else ""
                # confidence: thin re-filing inheriting a fuller original = high
                tgt_ttw_yes = any(common.norm(r.get("TTW", "")).lower() == "yes" for r in blank_recs)
                donor_ttw_no = any(common.norm(r.get("TTW", "")).lower() == "no" for r in donor_recs)
                donor_fuller = max(fill_of[r["id"]] for r in donor_recs) > \
                    max(fill_of[r["id"]] for r in blank_recs)
                conf = "high" if (tgt_ttw_yes and donor_ttw_no) or donor_fuller else "medium"
                backfill_by_conf[conf] += 1
                backfill_rows.append({
                    "agreement_id": aid, "cao_number": cao, "ingangsdatum": ing,
                    "in_scope": in_scope, "topic": topic, "field": c,
                    "base_field": ci.group_key, "kind": kind,
                    "agreed_value": raw_val, "unit": raw_unit,
                    "donor_record_ids": ",".join(common.norm(r["id"]) for r in donor_recs),
                    "blank_record_ids": ",".join(common.norm(r["id"]) for r in blank_recs),
                    "n_blank": n_blank, "donor_confidence": conf,
                })
                continue

            # ---- CONFLICT (>=2 distinct filled values) ----
            m_conflict += 1
            conflict_by_topic[topic] += 1
            seq = [cv for _, cv in filled]
            raw_units = ([common.norm(r.get(ucol, "")) for r, _ in filled]
                         if kind == "unit" else [])
            triage, prio, lone = _triage(kind, seq, raw_units, n_filled)
            conflict_by_triage[triage] += 1
            # value -> [id(date)] map and the date-ordered trajectory (raw values)
            vmap = defaultdict(list)
            for r, cv in filled:
                d = ""
                for dc in ORDER_DATE_COLS:
                    if not common.is_blank(r.get(dc, "")):
                        d = common.norm(r.get(dc, "")); break
                vmap[common.norm(r.get(c, ""))].append(f"{common.norm(r['id'])}({d})")
            modal_raw, modal_n = Counter(seq).most_common(1)[0]
            conflict_rows.append({
                "agreement_id": aid, "cao_number": cao, "ingangsdatum": ing,
                "in_scope": in_scope, "topic": topic, "field": c,
                "base_field": ci.group_key, "kind": kind,
                "n_filled": n_filled, "n_distinct": len(distinct),
                "value_map": "; ".join(f"{v}=[{','.join(ids)}]" for v, ids in vmap.items()),
                "trajectory": " -> ".join(common.norm(r.get(c, "")) for r, _ in filled),
                "modal_share": round(modal_n / n_filled, 2),
                "triage": triage, "priority": prio, "lone_minority": lone,
            })

    # ---- write artifacts ----
    pd.DataFrame(idx_rows).sort_values(["in_scope", "cao_number", "ingangsdatum"],
                                       ascending=[False, True, True]).to_csv(IDX_OUT, sep=";", index=False)
    pd.DataFrame(backfill_rows).sort_values(
        ["donor_confidence", "in_scope", "topic", "field"],
        ascending=[True, False, True, True]).to_csv(BACKFILL_OUT, sep=";", index=False)
    prio_rank = {"high": 0, "medium": 1, "low": 2}
    cdf = pd.DataFrame(conflict_rows)
    cdf["_p"] = cdf["priority"].map(prio_rank)
    cdf.sort_values(["_p", "in_scope", "topic", "field"],
                    ascending=[True, False, True, True]).drop(columns="_p").to_csv(
        CONFLICT_OUT, sep=";", index=False)

    multi = sum(1 for g in agr.values() if len(g) >= 2)
    tot = m_identical + m_gap + m_conflict
    gap_cells = int(sum(int(r["n_blank"]) for r in backfill_rows))
    hi_conf = sum(1 for r in conflict_rows if r["priority"] == "high")
    # regression metric: the one number to watch across future passes
    consistency = round(100 * (m_identical + m_gap) / tot, 2) if tot else 0.0
    identical_rate = round(100 * m_identical / tot, 2) if tot else 0.0

    lines = [
        f"# Agreement-consistency QA layer (surface-only)\n",
        f"Dataset: `{ds_path.name}` — {len(df)} records, {len(agr)} agreements "
        f"(cao x ingangsdatum), {multi} with >=2 records.\n",
        f"Structured fields measured: {len(structured)} (numeric/unit/enum/boolean; "
        f"freetext notes & dates excluded).\n",
        "## Regression metric (watch this across every future correction pass)\n",
        f"- **within-agreement consistency = {consistency}%** "
        f"(identical {identical_rate}% + back-fillable gap)",
        f"- a correct edit should move this UP; a pass that lowers it is suspect.\n",
        "## (agreement x structured-field) breakdown\n",
        f"- identical across all versions : {m_identical} ({round(100*m_identical/tot,1)}%)",
        f"- completeness GAP (back-fill)  : {m_gap} ({round(100*m_gap/tot,1)}%) "
        f"-> {gap_cells} blank cells fillable",
        f"- CONFLICT (>=2 distinct)       : {m_conflict} ({round(100*m_conflict/tot,1)}%) "
        f"-> {hi_conf} high-priority\n",
        "## Back-fill candidates by confidence\n",
        f"- high (thin re-filing inherits a fuller original): {backfill_by_conf.get('high',0)}",
        f"- medium: {backfill_by_conf.get('medium',0)}\n",
        "### back-fill blank cells by topic\n",
        "| topic | cells |", "|---|---:|",
        *[f"| {t} | {n} |" for t, n in gapcells_by_topic.most_common()],
        "\n## Conflicts by triage (routing for Stage 3)\n",
        "| triage | n | meaning |", "|---|---:|---|",
        f"| AMBIGUOUS_2REC | {conflict_by_triage.get('AMBIGUOUS_2REC',0)} | 1-vs-1, needs source/definition |",
        f"| LIKELY_EXTRACTION_ERROR | {conflict_by_triage.get('LIKELY_EXTRACTION_ERROR',0)} | >=3 recs, non-monotonic -> fix |",
        f"| LIKELY_TEMPORAL_CHANGE | {conflict_by_triage.get('LIKELY_TEMPORAL_CHANGE',0)} | clean step over time -> keep both |",
        f"| BOOLEAN_DISAGREEMENT | {conflict_by_triage.get('BOOLEAN_DISAGREEMENT',0)} | True vs False across versions |",
        f"| UNIT_WORDING | {conflict_by_triage.get('UNIT_WORDING',0)} | same period, different spelling -> not real |",
        "\n## Conflicts by topic\n",
        "| topic | conflicts |", "|---|---:|",
        *[f"| {t} | {n} |" for t, n in conflict_by_topic.most_common()],
        f"\nArtifacts: `{IDX_OUT.name}`, `{BACKFILL_OUT.name}`, `{CONFLICT_OUT.name}`. "
        "Nothing applied — surface-only.",
    ]
    SUMMARY_OUT.write_text("\n".join(lines), encoding="utf-8")

    print(f"[agreement_consistency] dataset={ds_path.name} records={len(df)} "
          f"agreements={len(agr)} multi={multi}")
    print(f"  regression metric: within-agreement consistency = {consistency}% "
          f"(identical {identical_rate}%)")
    print(f"  identical={m_identical} gap={m_gap}({gap_cells} cells) conflict={m_conflict}")
    print(f"  back-fill confidence: {dict(backfill_by_conf)}")
    print(f"  conflict triage: {dict(conflict_by_triage)}")
    print(f"  high-priority conflicts: {hi_conf}")
    print(f"  -> {IDX_OUT.name}, {BACKFILL_OUT.name}, {CONFLICT_OUT.name}, {SUMMARY_OUT.name}")


if __name__ == "__main__":
    main()
