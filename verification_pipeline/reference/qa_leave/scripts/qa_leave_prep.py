"""
qa_leave_prep.py — build per-record QA payloads.

For every CAO record in extracted_data_non_salary.csv:
  - try to find the matching block in leave_information.md (key = (cao_number, file_name))
  - extract the leave_* fields from the CSV row
  - emit one JSON object per record to leave_qa_payloads.jsonl

Records with no matching markdown block are still emitted but with
source_text = null and source_status = "source_unavailable" so they can be
filtered out cleanly during the LLM-judge phase.

Usage:
  python qa_leave_prep.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CSV_PATH = ROOT / "inputs" / "extracted_data_non_salary.csv"
MD_PATH = ROOT / "inputs" / "leave_information.md"
OUT_PATH = ROOT / "outputs" / "leave_qa_payloads.jsonl"
INDEX_PATH = ROOT / "outputs" / "leave_qa_payload_index.csv"

# ---------------------------------------------------------------------------
# Markdown parsing
# ---------------------------------------------------------------------------

# Each block in the markdown looks like:
#
# ## 12. CAO 1264 - VBE_cao_2012.pdf
#
# - cao_number: `1264`
# - source_file_name: `VBE_cao_2012.pdf`
# - ingangsdatum: `2012-01-01`
# - datum_kennisgeving: `2012-02-08`
#
# ```json
# [...]
# ```


HEADER_RE = re.compile(r"^##\s+(\d+)\.\s+CAO\s+(\d+)\s+-\s+(.+?)\s*$", re.MULTILINE)
META_RE = re.compile(r"^- (cao_number|source_file_name|ingangsdatum|datum_kennisgeving):\s*`([^`]*)`\s*$", re.MULTILINE)


def parse_markdown_blocks(md_text: str) -> dict[tuple[str, str], dict]:
    """
    Returns dict keyed by (cao_number_str, file_name_normalized) -> {
        'cao_number': str,
        'source_file_name': str,
        'ingangsdatum': str,
        'datum_kennisgeving': str,
        'source_text': str,   # the raw block text (markdown), used as judge ground truth
    }
    """
    blocks: dict[tuple[str, str], dict] = {}

    # Find all top-level "## N. CAO X - filename" headers
    matches = list(HEADER_RE.finditer(md_text))
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(md_text)
        chunk = md_text[start:end]

        meta = {k: v for k, v in META_RE.findall(chunk)}
        cao = meta.get("cao_number", "").strip()
        fn = meta.get("source_file_name", "").strip()
        if not cao or not fn:
            continue

        key = (cao, _normalize_filename(fn))
        blocks[key] = {
            "cao_number": cao,
            "source_file_name": fn,
            "ingangsdatum": meta.get("ingangsdatum", "").strip(),
            "datum_kennisgeving": meta.get("datum_kennisgeving", "").strip(),
            "source_text": chunk.strip(),
        }
    return blocks


def _normalize_filename(name: str) -> str:
    """
    Strip trailing .pdf only (the markdown export tacks on .pdf to all source files,
    sometimes adding it to filenames that already have .docx). The CSV keeps .docx
    but strips .pdf, so we mirror that: strip every trailing '.pdf' (with optional
    surrounding whitespace) but leave '.docx' / '.doc' intact.
    """
    s = name.strip()
    while True:
        new = re.sub(r"\s*\.pdf\s*$", "", s, flags=re.IGNORECASE)
        if new == s:
            break
        s = new
    s = re.sub(r"\s+", " ", s).strip()
    return s.lower()


# ---------------------------------------------------------------------------
# CSV row → leave fields
# ---------------------------------------------------------------------------


def extract_leave_fields(row: pd.Series) -> dict:
    """
    Pull all leave_* columns into a structured dict, organized by topic group.
    Empty / nan values are kept as null so the judge can see what's empty.
    """
    leave_cols = [c for c in row.index if c.startswith("leave_")]
    flat = {}
    for c in leave_cols:
        v = row[c]
        if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
            flat[c] = None
        else:
            flat[c] = v if not isinstance(v, str) else v.strip()

    grouped = {
        "general": _pick(flat, [
            "leave_has_leave_enhancements",
            "leave_hetero_present",
            "leave_note",
        ]),
        "maternity": _pick(flat, [
            "leave_has_above_statutory_maternity",
            "leave_paid_maternity_value", "leave_paid_maternity_unit",
            "leave_partially_paid_maternity_value", "leave_partially_paid_maternity_unit",
            "leave_partially_paid_maternity_pay_value", "leave_partially_paid_maternity_pay_unit",
            "leave_unpaid_maternity_value", "leave_unpaid_maternity_unit",
            "leave_maternity_note",
        ]),
        "paternity": _pick(flat, [
            "leave_paternity_explicitly_above_statutory",
            "leave_paid_paternity_value", "leave_paid_paternity_unit",
            "leave_partially_paid_paternity_value", "leave_partially_paid_paternity_unit",
            "leave_partially_paid_paternity_pay_value", "leave_partially_paid_paternity_pay_unit",
            "leave_unpaid_paternity_value", "leave_unpaid_paternity_unit",
        ]),
        "adoption": _pick(flat, [
            "leave_adoption_value", "leave_adoption_unit",
            "leave_adoption_pay_value", "leave_adoption_pay_unit",
        ]),
        "parental": _pick(flat, [
            "leave_parental_statutory_ref",
            "leave_parental_exceptions",
            "leave_parental_eligibility_present",
            "leave_parental_min_contract_length_value", "leave_parental_min_contract_length_unit",
            "leave_parental_min_tenure_value", "leave_parental_min_tenure_unit",
            "leave_parental_note",
            "leave_parental_topup_present",
            "leave_parental_topup_pay_value", "leave_parental_topup_pay_unit",
            "leave_parental_unpaid_value", "leave_parental_unpaid_unit",
            "leave_abortion_present",
        ]),
        "sick": _pick(flat, [
            "leave_sick_topup_present",
            "leave_sickpay_duration_value", "leave_sickpay_duration_unit",
            "leave_sickpay_continuation_value", "leave_sickpay_continuation_unit",
            "leave_sickpay_extra_insurance_present",
        ]),
        "care": _pick(flat, [
            "leave_care_statutory_ref",
            "leave_care_exceptions",
            "leave_care_topup_present",
            "leave_short_term_care_value", "leave_short_term_care_unit",
            "leave_short_term_care_pay_value", "leave_short_term_care_pay_unit",
            "leave_long_term_care_value", "leave_long_term_care_unit",
            "leave_long_term_care_pay_value", "leave_long_term_care_pay_unit",
        ]),
        "vacation_holidays": _pick(flat, [
            "leave_vacation_time_value", "leave_vacation_time_unit",
            "leave_vacation_bonus_value", "leave_vacation_bonus_unit",
            "leave_liberation_day_annual",
            "leave_liberation_day_lustrum",
            "leave_liberation_day_comp_note",
        ]),
        "seniority_special": _pick(flat, [
            "leave_extra_seniority_present",
            "leave_extra_seniority_schedule",
        ]),
    }
    return grouped


def _pick(d: dict, keys: list[str]) -> dict:
    return {k: d.get(k) for k in keys}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run() -> None:
    if not CSV_PATH.exists():
        sys.exit(f"CSV not found: {CSV_PATH}")
    if not MD_PATH.exists():
        sys.exit(f"Markdown not found: {MD_PATH}")

    md_text = MD_PATH.read_text(encoding="utf-8")
    blocks = parse_markdown_blocks(md_text)
    print(f"[qa_leave_prep] Parsed {len(blocks)} markdown blocks")

    df = pd.read_csv(CSV_PATH, sep=";", dtype=str, low_memory=False)
    print(f"[qa_leave_prep] Loaded {len(df)} CSV rows")

    matched = 0
    unmatched = 0
    n_records = 0
    index_rows: list[dict] = []

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as fout:
        for _, row in df.iterrows():
            cao = str(row.get("cao_number") or "").strip()
            fn = str(row.get("file_name") or "").strip()
            key = (cao, _normalize_filename(fn))
            blk = blocks.get(key)

            payload = {
                "record_id": row.get("id") or f"row_{_}",
                "cao_number": cao,
                "file_name": fn,
                "ingangsdatum": row.get("ingangsdatum"),
                "expiratiedatum": row.get("expiratiedatum"),
                "datum_kennisgeving": row.get("datum_kennisgeving"),
                "general_document_type": row.get("general_document_type"),
                "source_status": "matched" if blk else "source_unavailable",
                "source_text": blk["source_text"] if blk else None,
                "csv_leave_fields": extract_leave_fields(row),
            }
            fout.write(json.dumps(payload, ensure_ascii=False) + "\n")
            n_records += 1
            if blk:
                matched += 1
            else:
                unmatched += 1

            index_rows.append(
                {
                    "record_id": payload["record_id"],
                    "cao_number": cao,
                    "file_name": fn,
                    "ingangsdatum": payload["ingangsdatum"],
                    "general_document_type": payload["general_document_type"],
                    "source_status": payload["source_status"],
                    "source_text_chars": len(blk["source_text"]) if blk else 0,
                }
            )

    pd.DataFrame(index_rows).to_csv(INDEX_PATH, sep=";", index=False)
    print(f"[qa_leave_prep] Wrote {n_records} payloads to {OUT_PATH}")
    print(f"[qa_leave_prep]   matched (have markdown source) : {matched}")
    print(f"[qa_leave_prep]   unmatched (source_unavailable) : {unmatched}")
    print(f"[qa_leave_prep] Wrote index to {INDEX_PATH}")


if __name__ == "__main__":
    run()
