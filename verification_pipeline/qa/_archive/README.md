# `qa/_archive/` — retired scripts

Superseded scripts kept for reference. Nothing here is part of the active pipeline; nothing here
should be run. (Dated *dataset* snapshots live in `qa/_old/` and `qa/backups/` instead.)

## `apply_corrections.py`

Built **Layer 1** and nothing else, on 2026-05-27: it read `apply_list.csv` (540 candidate cells,
produced by `consolidate_review.py`) against the raw G0 extract and applied the 514 that passed its
four safety gates — the per-topic clean wins plus the 126 `MASTER_REVIEW_FOR_HANNA` edits, across 111
records. Changelog: `qa/apply_changelog.csv` (514 rows). Skips: `qa/apply_skipped.csv` (0 rows).
Its output is snapshotted at `qa/_old/corrected_dataset.bak.2026-06-04.csv`.

**Why it is retired.** It writes `qa/corrected_dataset.csv` unconditionally, starting from G0. That
file is now G33 — 34 correction layers, ~12,000 distinct corrected cells (~21,000 change events).
Running it would replace G33 with a G0+L1 state and discard Layers 2–34. The canonical dataset is
never rebuilt from scratch; it is advanced only by apply → copy → verify → promote
(`qa/DATASETS.md`, `docs/DATA_LINEAGE.md`).

Two things now stand in the way: its relative paths no longer resolve from `_archive/`, and its
`__main__` block aborts unless `CAO_ALLOW_L1_REPLAY=1` is set.

The four-gate pattern in `main()` — match on unique `id` only, require exactly one row, verify
`expected_current`, write one named cell — is still the house style, and the live apply scripts
(`qa/apply_collision_l11.py`, `qa/apply_pension_premium_l13.py`,
`salary/scripts/apply_salary_corrections.py`) mirror it.

> Not to be confused with `qa/full_audit/apply_corrections.py` (+ `_phaseB`, `_phaseB2`), which are
> different, Layer-2 files and remain in place.
