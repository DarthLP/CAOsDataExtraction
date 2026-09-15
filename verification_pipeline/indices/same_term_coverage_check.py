"""Same-term coverage check (battery check 6).

Recomputes the campaign's event definition — same-term consecutive |Δ topic z| ≥ 0.5
with scoring-relevant cell diffs (normalized equality) — and verifies that BOTH sides
of every diff cell are in the adjudication ledger `indices/corrections/jump_campaign_adjudications.csv`
(plus the pre-campaign L14–L19 change-logs). The jump-verification campaign drove this
to a fixed point (0 uncovered, 2026-07-12); any future edit layer that reintroduces
uncovered sides shows up here and needs its own mini-ripple.

Run standalone: python3 same_term_coverage_check.py [--list]
"""
import os
import sys
import importlib
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il

TOPICS = ["leave", "absence", "term", "contract", "overtime", "training", "bonus",
          "fringe", "homeoffice", "pension", "safety_coverage", "childcare_coverage"]

LEAVE_NUMERIC = {
    "leave_paid_maternity_value": "weeks", "leave_partially_paid_maternity_value": "weeks",
    "leave_partially_paid_maternity_pay_value": "percent", "leave_unpaid_maternity_value": "weeks",
    "leave_paid_paternity_value": "weeks", "leave_partially_paid_paternity_value": "weeks",
    "leave_partially_paid_paternity_pay_value": "percent", "leave_unpaid_paternity_value": "weeks",
    "leave_adoption_value": "weeks", "leave_adoption_pay_value": "percent",
    "leave_parental_unpaid_value": "weeks", "leave_parental_topup_pay_value": "percent",
}

PRE_CAMPAIGN_LOGS = [
    ("dip_boolean_corrections_applied.csv", "record_id", "field"),
    ("dip_sweep_corrections_applied.csv", "record_id", "field"),
    ("dip_family2_corrections_applied.csv", "record_id", "field"),
    ("dip_3313_corrections_applied.csv", "record_id", "field"),
    ("topic_sweep_corrections_applied.csv", "record_id", "field"),
    ("unify_corrections_applied.csv", "record_id", "field"),
    ("gate_childcare_safety_apply.csv", "record_id", "field"),
    ("topicsweep_childcare.csv", "id", "field"),
    ("topicsweep_safety.csv", "id", "field"),
]

def _driver(t):
    name = {"safety_coverage": "safety_index", "childcare_coverage": "childcare_index",
            "leave": "parental_leave_index"}.get(t, f"{t}_index")
    return importlib.import_module(name)

def run(list_uncovered=False):
    numeric_map, bool_map = {}, {}
    for t in TOPICS:
        m = _driver(t)
        numeric_map[t] = dict(LEAVE_NUMERIC) if t == "leave" else {f[0]: f[1] for f in getattr(m, "FIELDS", [])}
        bool_map[t] = list(getattr(m, "BOOLEANS", []))

    adjudicated = set()
    led = il.read_csv_safe(il.locate("jump_campaign_adjudications.csv"))
    for _, r in led.iterrows():
        adjudicated.add((str(r["record_id"]).split(".")[0], str(r["field"])))
    for f, idc, fc in PRE_CAMPAIGN_LOGS:
        p = il.locate(f)
        if not os.path.exists(p):
            continue
        d = il.read_csv_safe(p)
        if idc in d.columns and fc in d.columns:
            for _, r in d.iterrows():
                adjudicated.add((str(r[idc]).split(".")[0], str(r[fc])))

    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    df = il.load_full_cao()
    df_idx = df.set_index(df["id"].astype(str))
    reg = il.read_csv_safe(il.locate("thin_docs_reviewed.csv"))
    thin_ids = set(reg.loc[reg["verdict"] == "THIN_CONFIRMED", "id"].astype(str))

    for t in TOPICS:
        comp[f"{t}_z"] = pd.to_numeric(comp[f"{t}_z"], errors="coerce")
    comp["_fd"] = comp["file_date"].map(il.parse_date)
    cs = comp.dropna(subset=["_fd"]).sort_values(["cao_number", "_fd", "id"])

    uncovered, sides = [], 0
    for (cao, ing), g in cs.groupby(["cao_number", comp["ingangsdatum"].astype(str)]):
        g = g.sort_values(["_fd", "id"]).reset_index(drop=True)
        if len(g) < 2:
            continue
        for t in TOPICS:
            z = g[f"{t}_z"]
            for i in range(1, len(g)):
                d1 = z[i] - z[i - 1]
                if pd.isna(d1) or abs(d1) < 0.5:
                    continue
                idp, idv = str(g.loc[i - 1, "id"]), str(g.loc[i, "id"])
                if idp in thin_ids or idv in thin_ids or idp not in df_idx.index or idv not in df_idx.index:
                    continue
                rp, rv = df_idx.loc[idp], df_idx.loc[idv]
                cells = []
                for vc, canon in numeric_map[t].items():
                    if vc not in df.columns:
                        continue
                    uc = il.unit_col(vc)
                    a = il.normalize(canon, il.parse_float(rp.get(vc)), rp.get(uc) if uc in df.columns else None)
                    b = il.normalize(canon, il.parse_float(rv.get(vc)), rv.get(uc) if uc in df.columns else None)
                    if (pd.isna(a) and pd.isna(b)) or (pd.notna(a) and pd.notna(b) and abs(a - b) < 1e-9):
                        continue
                    cells.append(vc)
                for bc in bool_map[t]:
                    if bc in df.columns and str(rp.get(bc)).strip().lower() != str(rv.get(bc)).strip().lower():
                        cells.append(bc)
                for c in cells:
                    for side in (idp, idv):
                        sides += 1
                        if (side, c) not in adjudicated:
                            uncovered.append((str(cao), side, c))
    if list_uncovered:
        for u in uncovered:
            print("    UNCOVERED:", u)
    return sides, uncovered

if __name__ == "__main__":
    sides, unc = run("--list" in sys.argv)
    print(f"same-term coverage: {sides} diff-cell sides | {len(unc)} uncovered"
          + (" -> ALL COVERED" if not unc else " (needs a ripple wave!)"))
