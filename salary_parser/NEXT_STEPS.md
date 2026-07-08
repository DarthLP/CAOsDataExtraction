# Salary parser — state & next steps (handoff 2026-07-08, rev 3)

## Current state (canonical)
- **`outputs/parser_salary/extracted_data_salary_v2.csv` — 359,474 rows, A+B = 95.4%**
  (A 254,334 · B 88,777 · C 13,693 · D 2,670), **100% amount+date provenance.**
- label_source: parser ~327k · haiku_relabel 9,321 · agent_extract ~23k.
- **Rebuild = one command: `python3 salary_parser/deliver.py`** (stage 5.5 auto-re-applies every
  relabel + agent-extract merge; the parser fixes + note-scans re-run on all files; HARD GUARD aborts
  if amount provenance < 100%). Every rebuild KEEPS the LLM fixes.
- Wage index: `indices/mw_indices.py` → 215 CAOs (version-aware two-stage dedup), median
  monthly-mean ~€2,530. Feeds composite + all_indices.xlsx. Per-file workweek maps:
  `file_workweek.csv` (preferred) + `cao_workweek.csv`.

## Session arc: A+B 83.2 → 91.9 → 93.4 → 94.5 → 95.2 → 95.4%. All C/D reachable by two proven,
repeatable mechanisms (cheap relabel for labels; chunked guarded re-extraction for structure).

## Next steps — prioritized

### A. Highest-value
1. **Version selection — DONE + verified (2026-07-08).** TAG-don't-MERGE per
   CAO_VERSION_SELECTION_PLAN.md. `salary_parser/version_tag.py` appends `term_group`
   (cao+ingangsdatum, date-normalized), `kennisgeving_rank` (1=earliest edition … N=latest),
   `base_id`, `n_editions`, `document_type` to every v2 row (wired into flatten_csv.py → carried
   by every deliver.py rebuild). 1,378 term_groups, 566 multi-edition (72% of rows). ALL joins
   keyed (cao_number, file_name) — filename-only collides (same name under several CAOs); doc
   metadata falls back to the NON-salary CSV for files the old salary CSV lacks (0 untagged rows).
   The wage index (`indices/mw_indices.py`) de-dups version-aware TWO-STAGE: per (cao, eff-date,
   jobgroup, step, age, worker, unit) key keep ALL rows of the winning file (max
   kennisgeving_rank) — never collapses within an edition — then drop exact-value repeats.
   Verified vs pre-version baseline: 215 CAOs (+563, mislabel fixed), median wage ±0.28%,
   observations +2.5% (collapses editions, preserves real splits). Analyst recipe: term snapshot
   = kennisgeving_rank==1 (base) or the MAX rank present in the salary CSV (ranks count all known
   editions incl. row-less ones, so ==n_editions may not exist here); full wage timeline = all
   editions of a term_group; drop deltas = document_type LIKE 'full_cao_%'.
2. **Commit + status docs.** The DBA repo (indices/, docs/, qa/) is on branch qa/setup; the parser
   repo has uncommitted salary_parser/ changes. Snapshot this session's work + refresh the status
   board so it's reproducible and reviewable. (NB: DBA .gitignore does NOT exclude raw inputs/ or
   the large regenerable indices xlsx/panels — decide commit scope before `git add -A`.)

### B. Quality polish (diminishing returns — each wave now buys a few hundred rows)
3. **Residual C/D ≈ 16.4k**: label tail beyond the ~140 files relabeled → more relabel batches;
   structural (magnitude/ragged) tail → more chunked re-extraction. Amounts are already ~99.7%
   correct (youth-aware audit), so this is label/unit/structure polish, not wrong-money fixing.
4. **563→157 cao_number mislabel — FIXED (2026-07-08)**: root cause was the filename-only
   metadata join (same file_name under several CAOs → first row's metadata won; hit 19 files:
   Zuivel 157/563, 'HB 5e editie 2024' under 822/824/826/827/2297, etc.). `doc_meta_by_file`
   now keys (cao_number, file_name); 563's 137 rows carry cao 563 + id 563010 and it enters
   the wage index as its own CAO (215 total).
5. **~697 hourly rows still without ft_hours**: piece-rate (544/3866 — correct) + ~6 source-silent
   CAOs (634/2451/4236/3221/2674/3041). An agent read of their full text MIGHT find a stray
   workweek; low odds (an earlier pass came back empty) — only if completeness is a hard requirement.

### C. Decisions for Hanna
6. **`unit_inferred_magnitude` — DECIDED: accept as-is (2026-07-08).** 150-row source-anchored Haiku
   audit → reweighted unit-error 1.93% (in-band 1.7%, out-of-band 7.8%). Amounts stay 100%
   provenance-correct; only the unit is uncertain. Added a free `unit_inferred_oob` flag (3,440
   rows) so the risky 4.3% is separately filterable. No tier change.
7. **Statutory workweek fallback = 36h — DECIDED: keep (2026-07-08).** NL has no statutory FT week;
   36h is the 2024 minimum-hourly-wage basis and is empirically confirmed by CAO 634 (implied 35.2h
   across 2010-2023). No older-era 40h applies (all silent files are 2010+). Stated 38/40h already
   used per-file. Tagged `ft_workweek_cao` / `statutory` (hours_source), so reversible.
8. **Confirm-to-promote verifier** (an agent confirms a file's FULL extraction is complete + correct,
   then promote its C/D→B): build it, or leave C/D as honest low-confidence tags? (Not built — cost.)

### Closed: the "21 CAOs with nothing parsed" (2026-07-08)
Of the 21 zero-row CAOs, 2 were fixable and are FIXED (2468 → 93 rows, 3221 → 120 rows, agent-
extracted). The other 19 are CORRECTLY empty — verified at BOTH levels: their extract-JSON
wage_information is 100% prose (definitions/rules; the only grid-shaped blocks are a %-increase
matrix (2527) and job-group descriptions (4171), not wage scales), AND their upstream parsed
PDFs contain no money-dense table lines (0-2 per CAO) — so nothing was dropped; the source CAO
texts publish no wage scales (630 footballers = individual salaries; 2857 Doorwerk = comparable-
employee rule; 3324 = statutory-minimum clause; 1294 Waterschappen = scales in a separate annex
not in our corpus). Do NOT re-investigate; extracting anything here would invent values.
19 = 630, 636, 1106, 1291, 1294, 1429, 2527, 2742, 2857, 3109, 3313, 3324, 3557, 3690, 3993,
4000, 4062, 4171, 4274.

### D. Downstream / consistency
9. Re-run the full topic-index battery + composite only if other-topic inputs changed (they didn't
   this session — only wage). Confirm all_indices.xlsx consumers are happy with the new units
   (period, daily) and the hours_source tagging.

## Watch-outs (durable)
- Agent campaigns: pass EXPLICIT payload paths (never "packet index N"); "process ONLY these, do NOT
  glob"; <=8 paths/packet to avoid prompt-too-long. Post-hoc per-payload provenance check (amount ∈
  own num_universe) catches mis-dispatch. Haiku default; Sonnet only for decimal-heavy redos.
- Note-scans are safe only for TIGHT regexes (ft_hours, holiday). unit/date note-scans over-fire
  (unit flips monthly→hourly; date grabs eligibility dates) — do NOT re-add.
- deliver.py rebuild is the ONLY correct path (re-applies merges). A bare parser run + flatten loses them.
