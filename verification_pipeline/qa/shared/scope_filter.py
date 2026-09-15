"""Filters the full extracted-data CSV down to the scope each topic operates on.

Scope (PLAN.md §0.6a / phase_0_5_findings.md):
  - 95 CAOs × ~16 versions = 1,505 records per topic — the curated subset
    that has source text in inputs/by_topic/*_information.md.
  - For each CAO, keep the record with the latest ingangsdatum where the
    topic's source section is non-empty.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from qa.shared import resilient_csv, source_text_loader


_INPUTS_ROOT_CACHE: Optional[Path] = None


def _inputs_root() -> Path:
    global _INPUTS_ROOT_CACHE
    if _INPUTS_ROOT_CACHE is None:
        here = Path(__file__).resolve()
        _INPUTS_ROOT_CACHE = here.parent.parent.parent / "inputs"
    return _INPUTS_ROOT_CACHE


def get_95_biggest_caos() -> set[str]:
    """Returns the set of cao_number values that appear in any by_topic file
    (the curated 95-CAO scope). Derived from leave_information.md since all
    13 by_topic files share the same record set.
    """
    blocks = source_text_loader._load_blocks("leave")
    return {cao for (cao, _fn) in blocks.keys()}


# Backward-compat alias for the original API name in PLAN.md
def get_100_biggest_caos() -> set[str]:
    """Alias for get_95_biggest_caos. Earlier PLAN drafts said '100 biggest';
    actual count verified in Phase 0.5 is 95."""
    return get_95_biggest_caos()


def most_recent_doc_per_cao(records, topic: str):
    """Filter records to the latest `ingangsdatum` per cao_number where the
    matching source section exists in inputs/by_topic/<filename>.

    Args:
        records: DataFrame from resilient_csv.read_csv with at least these
            columns: id, cao_number, file_name, ingangsdatum
        topic: short topic name; mapped via TOPIC_TO_FILENAME

    Returns:
        DataFrame, ≤ 95 rows (one per CAO that has a non-empty section).
    """
    import pandas as pd

    blocks = source_text_loader._load_blocks(topic)
    valid_keys = set(blocks.keys())

    # Build a filtered + key-resolved view
    rows = []
    for _, row in records.iterrows():
        rid = str(row.get("id", "")).strip()
        cao = str(row.get("cao_number", "")).strip()
        fn = str(row.get("file_name", "")).strip()
        key = (cao, source_text_loader._normalize_filename(fn))
        if key in valid_keys:
            # Confirm the source block isn't empty (e.g. `[]`)
            block_text = blocks[key]
            # A block is "empty" if its JSON content is just `[]` or whitespace
            if "```json\n[]\n```" in block_text:
                continue
            rows.append({
                "id": rid,
                "cao_number": cao,
                "file_name": fn,
                "ingangsdatum": str(row.get("ingangsdatum", "")).strip(),
                "_record": row,
            })
    if not rows:
        return records.iloc[0:0]  # empty DataFrame with same schema

    df = pd.DataFrame(rows)
    # Sort by cao_number + ingangsdatum descending, keep first per cao_number
    df = df.sort_values(["cao_number", "ingangsdatum"], ascending=[True, False])
    df = df.drop_duplicates(subset=["cao_number"], keep="first")
    # Reconstruct the original records subset
    keep_ids = set(df["id"].tolist())
    result = records[records["id"].astype(str).str.strip().isin(keep_ids)].copy()
    return result.reset_index(drop=True)


def load_full_csv():
    """Load the full extracted_data_non_salary.csv as a DataFrame.
    Convenience wrapper for downstream scripts."""
    return resilient_csv.read_csv(_inputs_root() / "extracted_data_non_salary.csv",
                                  delimiter=";")
