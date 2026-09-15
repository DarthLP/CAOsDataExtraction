"""Optional manual-override loader.

Per PLAN.md §4.1 and §10 Step 13: for leave, the qa_leave reference loads
fixes.csv, fixes_v2.csv, ..., fixes_v5.csv from
qa_leave/outputs/manual_review_followup/ in priority order (later versions
override earlier ones, but with verdict-priority shadowing — `clear` is
highest priority, `confirm` is lowest).

For non-leave topics, no overrides exist by default. If Hanna creates
qa/qa_<topic>/inputs/manual_review_followup/fixes.csv during NHR review,
that becomes the override source for re-runs.

Output dict keys: (record_id, field). Value: dict with keys
  verdict, csv_value_new, csv_unit_new, target_field, notes
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

from qa.shared import resilient_csv


# Verdict priority: lower number = stronger (wins on tie).
# Matches qa_leave_aggregate_corrections.py priority table.
_VERDICT_PRIORITY = {
    "clear":            0,
    "set_boolean":      1,
    "move":             2,
    "correct_in_place": 3,
    "confirm":          4,
    "unable_to_verify": 5,
}


def _override_dir_for_topic(topic: str) -> Path:
    """Returns the directory expected to hold fixes_*.csv for this topic.

    For leave: `qa_leave/outputs/manual_review_followup/`.
    For non-leave: `qa/qa_<topic>/inputs/manual_review_followup/`.
    """
    here = Path(__file__).resolve()
    root = here.parent.parent.parent  # project root
    if topic == "leave":
        return root / "reference" / "qa_leave" / "outputs" / "manual_review_followup"
    return root / "qa" / f"qa_{topic}" / "inputs" / "manual_review_followup"


def _fix_filename_priority() -> list[str]:
    """Filenames in load order. `fixes.csv` is implicit v1; v2..v5 follow."""
    return ["fixes.csv", "fixes_v2.csv", "fixes_v3.csv",
            "fixes_v4.csv", "fixes_v5.csv"]


def load_manual_overrides(topic: str) -> Dict[Tuple[str, str], dict]:
    """Load fixes_v1..v5.csv from the topic's override directory.

    Loading rule (matches qa_leave_aggregate_corrections.py lines 798-840):
      - Iterate files in v1 -> v5 order.
      - For each row, key = (record_id, field).
      - If key already loaded with a HIGHER-OR-EQUAL priority verdict
        (lower number), skip; else override.

    Returns:
        {(record_id, field): {verdict, csv_value_new, csv_unit_new,
                              target_field, notes, source_file}}
    """
    overrides: Dict[Tuple[str, str], dict] = {}
    od = _override_dir_for_topic(topic)
    if not od.exists():
        return overrides

    for fname in _fix_filename_priority():
        fpath = od / fname
        if not fpath.exists():
            continue
        try:
            df = resilient_csv.read_csv(fpath, delimiter=";")
        except Exception:
            # Try comma delimiter as fallback
            try:
                df = resilient_csv.read_csv(fpath, delimiter=",")
            except Exception:
                continue

        for _, row in df.iterrows():
            rid = str(row.get("record_id", "")).strip()
            field = str(row.get("field", "")).strip() or \
                    str(row.get("original_field", "")).strip()
            verdict = str(row.get("verdict", "")).strip().lower()
            if not rid or not field or not verdict:
                continue

            key = (rid, field)
            new_priority = _VERDICT_PRIORITY.get(verdict, 99)
            existing = overrides.get(key)
            if existing is not None:
                old_priority = _VERDICT_PRIORITY.get(
                    existing.get("verdict", ""), 99)
                if old_priority <= new_priority:
                    # Existing wins (lower priority number = stronger verdict)
                    continue

            overrides[key] = {
                "verdict": verdict,
                "csv_value_new": str(row.get("csv_value_new", "")).strip(),
                "csv_unit_new":  (str(row.get("csv_unit_new",
                                              row.get("unit_new", ""))).strip()),
                "target_field":  str(row.get("target_field", "")).strip(),
                "notes":         str(row.get("notes", "")).strip(),
                "source_file":   fname,
            }
    return overrides
