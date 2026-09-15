"""
Surface disagreements between lifecycle heuristics and existing extract labels.

Reads ``outputs/qa/lifecycle/lifecycle_summary.csv`` plus signing/AVV fields from
the non-salary extract, and writes ``outputs/qa/lifecycle/lifecycle_mismatches.csv``
for rows matching any of
four mismatch rules (A–D). Prints counts per rule (zeros allowed; document in
NOTES if needed).

Usage::

    conda run -n caos-extract python scripts/qa/lifecycle/report_mismatches.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd


def _project_root() -> Path:
    """Return repository root path."""
    return _ROOT


def _to_bool_series(s: pd.Series) -> pd.Series:
    """Coerce CSV/string values to boolean for lifecycle flags."""
    def one(v: object) -> bool:
        if pd.isna(v):
            return False
        if isinstance(v, bool):
            return v
        t = str(v).strip().lower()
        return t in ("true", "1", "yes", "t")

    return s.map(one)


def _avv_extract_applies(val: object) -> bool:
    """True if the extract marks AVV as applying (accepts yes/true variants)."""
    if pd.isna(val):
        return False
    t = str(val).strip().lower()
    return t in ("yes", "true", "1", "y")


def _signing_date_present(val: object) -> bool:
    """True if general_signing_date is non-empty and parse-coercible as a date."""
    if pd.isna(val):
        return False
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return False
    parsed = pd.to_datetime(s, errors="coerce")
    return bool(pd.notna(parsed))


def main() -> None:
    """
    Build lifecycle_mismatches.csv from the joined non-salary extract.

    Side effects:
        Writes ``outputs/qa/lifecycle/lifecycle_mismatches.csv``; prints A–D counts.
    """
    root = _project_root()
    summary_path = root / "outputs" / "qa" / "lifecycle" / "lifecycle_summary.csv"
    non_salary_path = root / "outputs" / "excel" / "new_results" / "extracted_data_non_salary.csv"
    out = root / "outputs" / "qa" / "lifecycle" / "lifecycle_mismatches.csv"
    if not summary_path.is_file():
        print(f"ERROR: missing {summary_path} — run join_lifecycle.py first.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(summary_path, sep=";")
    df["cao_number"] = df["cao_number"].astype(str)
    df["file_name"] = df["file_name"].astype(str).str.strip()
    if non_salary_path.is_file():
        extra = pd.read_csv(
            non_salary_path,
            sep=";",
            usecols=["cao_number", "file_name", "general_signing_date", "general_avv_applies"],
        )
        extra["cao_number"] = extra["cao_number"].astype(str)
        extra["file_name"] = extra["file_name"].astype(str).str.strip()
        extra = extra.drop_duplicates(subset=["cao_number", "file_name"], keep="first")
        df = df.merge(extra, on=["cao_number", "file_name"], how="left", validate="one_to_one")
    else:
        df["general_signing_date"] = ""
        df["general_avv_applies"] = ""
    need = [
        "cao_number",
        "file_name",
        "general_document_type",
        "general_signing_date",
        "general_avv_applies",
        "lifecycle_stage",
        "is_signed",
        "is_filed_szw",
        "is_avv_declared",
        "lifecycle_evidence",
    ]
    missing = [c for c in need if c not in df.columns]
    if missing:
        print(f"ERROR: input CSV missing columns: {missing}", file=sys.stderr)
        sys.exit(1)

    df["is_signed"] = _to_bool_series(df["is_signed"])
    df["is_avv_declared"] = _to_bool_series(df["is_avv_declared"])

    gdt = df["general_document_type"].fillna("").astype(str).str.strip()
    stage = df["lifecycle_stage"].fillna("").astype(str).str.strip()
    src = df["lifecycle_source"].fillna("").astype(str).str.strip()

    # Rule A disabled: spot-checks showed negotiation cues on full CAOs are narrative
    # (in-intro onderhandelingsresultaat / cover principeakkoord on filed text), not
    # true negotiation-stage documents. Classifier treats filed + full CAO as definitive.
    a = pd.Series(False, index=df.index)
    b = df["general_signing_date"].map(_signing_date_present) & (~df["is_signed"])
    c = df["general_avv_applies"].map(_avv_extract_applies) & (~df["is_avv_declared"])
    d = (stage == "unspecified") & (src == "none") & (gdt == "full_cao_original")

    reasons: list[str] = []
    for i in range(len(df)):
        parts: list[str] = []
        if bool(a.iloc[i]):
            parts.append("A")
        if bool(b.iloc[i]):
            parts.append("B")
        if bool(c.iloc[i]):
            parts.append("C")
        if bool(d.iloc[i]):
            parts.append("D")
        reasons.append(",".join(parts) if parts else "")

    df = df.copy()
    df["mismatch_reasons"] = reasons
    mm = df[df["mismatch_reasons"] != ""].copy()

    cols = [
        "cao_number",
        "file_name",
        "general_document_type",
        "general_signing_date",
        "general_avv_applies",
        "lifecycle_stage",
        "is_signed",
        "is_filed_szw",
        "is_avv_declared",
        "lifecycle_evidence",
        "mismatch_reasons",
    ]
    mm = mm[[c for c in cols if c in mm.columns]]
    out.parent.mkdir(parents=True, exist_ok=True)
    mm.to_csv(out, sep=";", index=False)

    print("Mismatch counts (soft acceptance — zeros OK):")
    print(f"  A (full CAO vs draft/negotiation): {int(a.sum())}")
    print(f"  B (signing date but not signed): {int(b.sum())}")
    print(f"  C (AVV applies in extract, not in file): {int(c.sum())}")
    print(f"  D (full CAO original, no lifecycle signal): {int(d.sum())}")
    print(f"\nWrote {len(mm)} rows to {out}")


if __name__ == "__main__":
    main()
