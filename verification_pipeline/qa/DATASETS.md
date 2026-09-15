# Datasets manifest (`qa/`)

One row per dataset file in this folder. Full lineage + ledger: [../docs/DATA_LINEAGE.md](../docs/DATA_LINEAGE.md).

| File | Role | Built by | = raw + | cells vs raw |
|---|---|---|---|---|
| `corrected_dataset.csv` | **CANONICAL** (indices read this) | G33, 2026-07-15 | L1…L34 | ~12k distinct cells (~21k change events) |
| `_old/corrected_dataset.bak.2026-07-08_pre-L13-pension.csv` | G11 snapshot | backup before L13 (pension fix) | L1…L12 | +3 (L12) |
| `_old/corrected_dataset.bak.2026-07-08_pre-L12-lowconf.csv` | G10 snapshot | backup before L12 | L1…L11 | +33 (L11) |
| `_old/corrected_dataset.bak.2026-07-08_pre-L11-collision.csv` | G9 snapshot | backup before L11 | L1…L10 | +111 (L10) |
| `qa/backups/corrected_dataset.csv.bak_numfix2` | G7 snapshot | backup before L9 | L1…L8 | +3,559 (L8) |
| `qa/backups/corrected_dataset.csv.bak_unify` | G8 snapshot | backup before L10 | L1…L9 | +61 (L9) |
| `_old/corrected_dataset.bak.2026-07-08_pre-L8.csv` | G6 snapshot | backup before L8 | L1…L7 | ~9,195 |
| `_old/corrected_dataset.bak.2026-07-05_pre-L7.csv` | G5 snapshot | backup before L7 promotion | L1…L6 | 9,155 |
| `_old/corrected_dataset.bak.2026-07-03_pre-L6.csv` | G4 snapshot | backup before L6 promotion | L1…L5 | ~9,022 |
| `_old/corrected_dataset.bak.2026-07-03_pre-wave2.csv` | G3 snapshot (pre-adjudication) | backup before L5 promotion | L1+L2+L3+L4 | ~6,917 |
| `_old/corrected_dataset.bak.2026-07-03.csv` | G2 snapshot (pre-removals) | backup before L4 promotion | L1+L2+L3 | 4,585 |
| `_old/corrected_dataset.bak.2026-07-01.csv` | G1 snapshot (pre-per-file) | backup before L3 promotion | L1+L2 | 1,295 |
| `_old/corrected_dataset.bak.2026-06-04.csv` | early snapshot (pre-full-audit) | `apply_corrections.py` | L1 | 514 |

> Build artifacts (`corrected_dataset.perfile_applied.csv`, `corrected_dataset.removals_applied.csv`) are **deleted after promotion** once verified byte-identical; regenerate via `full_audit/apply_perfile_fixes.py` / `full_audit/apply_removals.py`. Backups live in **`qa/_old/`**.

**Layers:** L1 = per-topic + master review · L2 = full-audit promotion · L3 = per-file `FIX_clear` ·
L4 = verified absence-removals (bool/enum, snippet-judged) · L5 = convention adjudication
(FIX_audit + surcharge + statutory_deferred, Haiku + Sonnet spot-verify) + Hanna's numeric rulings ·
L6 = CHECK-pattern resolutions (Hanna-accepted suggestions) · L7 = residue suggestions (Hanna-accepted) ·
L8 = source-verified boolean-flip + numeric-regression fixes (3,559, Sonnet-on-snippets across all 242 CAOs) ·
L9 = all-CAO numeric same-term extraction-error fixes (61, two-pass: Sonnet + independent 2nd-reader) ·
L10 = same-term consistency-unify + tie-break + full-passage recovery (111; incl. Hanna's tie decisions; 7 unit-blind unify clobbers reverted) ·
L11 = date-collision picked-record fixes (33 cells / 7 CAOs, two-pass Sonnet propose + independent 2nd-reader; index-relevant fields only; %-of-premium pension traps blanked, unit/max/contamination fixes). `qa/l11_collision_changelog.csv`; surfaced full set in `indices/review/collision_corrections_review.csv` ·
L12 = 2 Hanna-approved low-confidence collision fixes (3 cells: 125012 workhours-adjust-right→False, 429022 short-term-care "3 months"→blank). `qa/l12_collision_changelog.csv` ·
L13 = **systemic pension `employee_contrib` %-of-premium fix** (1,071 cells blanked; the field was frequently the employee's share OF THE PREMIUM, not % of salary → wrong quantity, blanked = available-case). 3 sub-passes: L13 explicit-premium unit (896, deterministic + Sonnet calibration 6/6), L13b same-CAO bare-unit siblings matching a premium value — defeats forward-fill (139), L13c source-verified plausibility ≥20% = premium-share (36, 8/8 confirmed). Result: index employee_contrib now 0–19.6% (was polluted by 45–100% splits), 0 residual ≥20. Changelogs `qa/l13{,b,c}_pension_*_changelog.csv`; apply `qa/apply_pension_premium_l13.py` + lists; backups `.bak.2026-07-08_pre-L13{,b,c}*.csv`.
**L14–L34** (jump campaign, tier A/B/C same-term campaigns, CANT_TELL closures, L31 restorative
re-verification, L32 family-consistency + L33 full-text closures, L34 follow-up flags): one tab per
layer in **`indices/corrections/corrections_log.xlsx`** (old→new + verbatim quote + provenance per
change) with the per-layer `*_applied.csv` logs in `indices/corrections/`; narrative per layer in
`docs/DATA_LINEAGE.md`. Their `.bak_*` snapshots live in **`qa/backups/`**.
**Full per-cell traceability:** `full_audit/PROVENANCE_all_layers.csv` (L1–L7: every change: layer, old→new, why, quote) + `indices/corrections/corrections_log.xlsx` (L14+).
**Never edit in place.** Apply → new copy → verify → promote over `corrected_dataset.csv` with a dated `.bak` (L1–L13 era: `qa/_old/`; L14+: `qa/backups/`). SINGLE-WRITER rule: one session at a time may apply to canonical, and layer numbers are reserved before applying (2026-07-15 incident).
Reversible changelogs: `qa/apply_changelog.csv` (L1), `qa/full_audit/apply_changelog.csv` (L2),
`qa/full_audit/perfile_apply_changelog.csv` (L3), `qa/full_audit/removals_apply_changelog.csv` (L4),
`qa/full_audit/wave2_apply_changelog.csv` (L5), `qa/full_audit/l6_apply_changelog.csv` (L6),
`qa/full_audit/l7_apply_changelog.csv` (L7 — includes why + quote per cell),
`qa/l11_collision_changelog.csv` (L11 — old→new + why + quote; applied by `qa/apply_collision_l11.py`).
