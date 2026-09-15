# qa_leave/ cleanup candidates — PROPOSAL (no deletions yet)

Per PLAN.md §0.6: deletion is gated on Hanna's per-file approval. This list documents the proposal; no files have been deleted.

Generated: 2026-05-17. Source: `ls -la qa_leave/scripts/` + correlation with the canonical-entrypoint finding from Phase 0.5 Explore.

## Files PROPOSED FOR DELETION

| File | Size | Reason |
|---|---|---|
| `qa_leave/scripts/_diag_unmatched.py` | 49 lines | Interim diagnostic (leading underscore convention = scratch) |
| `qa_leave/scripts/_pick_pilot.py` | 81 lines | Interim pilot-selection scratch |
| `qa_leave/scripts/_pilot_layer1_crosscheck.py` | 98 lines | Interim cross-check from pilot phase |
| `qa_leave/scripts/_pilot_verdicts.py` | 585 lines | Interim pilot verdict logic, superseded by canonical aggregate |
| `qa_leave/scripts/_deep_verdicts.py` | 209 lines | Interim, superseded by canonical |
| `qa_leave/scripts/qa_leave_aggregate_corrections.py.bak` | 53 KB | Stale .bak file from in-place edits |
| `qa_leave/scripts/__pycache__/` | (dir) | Python bytecode cache, regenerable |
| `qa_leave/outputs/corrections_manual_review.csv` | 11.7 KB | Interim NHR queue, superseded by `manual_review_followup/fixes_v1..v5.csv` |
| `qa_leave/outputs/corrections_manual_review_v2.csv` | 5.5 KB | Same — interim |
| `qa_leave/outputs/leave_consistency_scan_tightened.csv` | 20.4 KB | Superseded by `leave_consistency_scan.csv` |
| `qa_leave/inline_p3_processor.sh`, etc. | various | Interim shell scripts from manual iterations |

## Files CONFIRMED KEEP (canonical)

| File | Size | Reason |
|---|---|---|
| `qa_leave_aggregate.py` | 307 lines | **Canonical entrypoint** (primary pipeline runner) |
| `qa_leave_aggregate_corrections.py` | 1,146 lines | Post-processor for combining corrections (referenced by aggregate.py) |
| `qa_leave_consistency_scan.py` | 307 lines | Audit (port basis for `audit_lib.py`) |
| `qa_leave_presence.py` | 195 lines | L2 presence (port basis for `presence_scan.py`) |
| `qa_leave_rules.py` | 807 lines | Leave-specific rules (template for `qa_<topic>_rules.py`) |
| `qa_leave_patterns.py` | 592 lines | Pattern detectors (template for `qa_<topic>_patterns.py`) |
| `qa_leave_corrections_det.py` | 473 lines | Deterministic correction emitter (template) |
| `qa_leave_prep.py` | 268 lines | Source-text prep (port basis for `source_text_loader.py` + `scope_filter.py`) |
| `qa_leave_worksheets.py` | 442 lines | Worksheet builder (port basis for `worksheet_builder.py`) |
| `qa_leave_l1_followup_worksheets.py` | 250 lines | L1 follow-up (still useful as L1-replay reference) |
| `qa_leave_c2_followup_worksheets.py` | 230 lines | C2 follow-up reference |
| `qa_leave_chunk.py`, `qa_leave_worksheet_chunk.py` | 85, 54 lines | Chunking helpers (port → `worksheet_builder.build_chunk_items`) |
| `qa_leave_p3_review_worksheets.py` | 115 lines | P3 review reference |
| `qa_leave_progress.py` | 64 lines | Progress reporter (port → `logging_util` extension if needed) |
| `qa_leave_deep_sample.py` | 137 lines | Deep-sample reference |
| `qa_leave/outputs/corrections.csv` | 1.2 MB | **Final output, regression target** |
| `qa_leave/outputs/corrections_audit.csv` | 11.5 KB | Audit trail |
| `qa_leave/outputs/manual_review_followup/fixes_v1..v5.csv` | 71 KB total | Manual overrides (5 files in priority order) |
| `qa_leave/docs/*.md` | small | Reference prompts |

## Action

**Hanna**: review the "PROPOSED FOR DELETION" list. For each entry you confirm, run a manual `rm` on it (or grant Claude permission to do so in a follow-up). Claude has NOT deleted any files in this session.

After cleanup, write `qa_leave/README.md` per PLAN.md §0.6 — cleanup date, canonical entrypoints, pointer to `corrections.csv` as final output.
