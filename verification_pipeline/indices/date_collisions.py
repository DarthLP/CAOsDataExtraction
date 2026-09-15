"""date_collisions.py — list CAOs where 2+ full-CAO documents share an identical
time-axis date (datum_kennisgeving, or ingangsdatum where that is the fallback).

These are the only cases where add_newest / term_dedup must break a tie; the tie-break
is (richer extraction _n_pop, then higher id). This report lets Hanna eyeball them.
Writes date_collisions.csv. Run: python3.13 indices/date_collisions.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il
import pandas as pd


def main():
    df = il.load_full_cao()
    df = il.add_newest(df)
    g = df.groupby(["cao_number", "file_date"])
    rows = []
    for (cao, fd), sub in g:
        if len(sub) < 2 or fd == "":
            continue
        picked = sub.sort_values(["_n_pop", "_idnum"]).tail(1)["id"].iloc[0]
        for _, r in sub.sort_values(["_n_pop", "_idnum"]).iterrows():
            rows.append({
                "cao_number": cao, "file_date": fd,
                "n_docs_same_date": len(sub),
                "id": r["id"], "ingangsdatum": r["ingangsdatum"],
                "document_type": r["document_type"],
                "date_is_fallback": r["file_date_fallback"],
                "extraction_cells_pop": r["_n_pop"],
                "picked_as_newest": r["id"] == picked,
            })
    out = pd.DataFrame(rows).sort_values(["cao_number", "file_date", "extraction_cells_pop"])
    path = os.path.join(il.OUT, "date_collisions.csv")
    out.to_csv(path, sep=";", index=False)
    ncol = out[["cao_number", "file_date"]].drop_duplicates().shape[0] if len(out) else 0
    print(f"wrote {path}: {ncol} collision groups, {len(out)} docs "
          f"({int(out['date_is_fallback'].sum()) if len(out) else 0} on fallback dates)")
    if len(out):
        print(out.to_string(index=False))


if __name__ == "__main__":
    main()
