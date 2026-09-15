# Leave — standardized-pipeline re-run vs. the original reference (comparison)

2026-05-27. `qa/qa_leave/` is a **fresh run through the standardized pipeline**, built to
compare against the original **reference** implementation at top-level `qa_leave/` (which
is frozen / "do not modify"). Reference left untouched.

## What was run
- Stages 0–3 (deterministic) fully: scope (95 records), det = **3,068 items** (3,021 L2 +
  43 R2 + 4 R1) → **165 chunks**.
- Stage 3 subagents: a **3-chunk calibration sample** (60 items) — a full 165-chunk run is
  infeasible in one rate-limit window.
- Stage 4 aggregate on the sample + **complete** post-hoc era-floor check over all 95 records.

## Findings
| Check | Result |
|---|---|
| Pipeline mechanically works on leave | ✅ valid 12-col output; 40 clean wins on the sample; era flag runs |
| Era floors (maternity 16w / paternity 1w / partial-pat 5w) | **0 below-statutory** across all 95 records ✅ |
| Field-level coverage (old-corrected fields flagged by new det) | **26/27** ✅ |
| Cell-level overlap (new clean-win cells also corrected by old) | **0% sample / 3% det** |

## Why the outputs are NOT similar (by design, not error)
- **Old reference** (bespoke prototype): **3,964 corrections**, mostly on *populated value*
  fields — an aggressive value-correction pass.
- **New standardized**: L1 consistency rules + an L2 false-negative scan on *empty* fields,
  then a guard chain that suppresses most to confirmations. The sample's clean wins are all
  `*_statutory_ref` flips and `has_leave_enhancements` — i.e. **boolean-consistency
  corrections the old run didn't target**.
- So they engage the **same fields** (26/27) but **different cells** → ~0% overlap. This
  reflects two different methodologies (early prototype vs refined pipeline), not a defect
  in either.

## Recommendation
Keep `qa_leave/` as the **frozen reference**. The comparison validates that the standardized
pipeline engages leave correctly (right fields, no below-floor values, valid output) — but it
will **not reproduce** the prototype's output cell-for-cell, and a full standardized re-run
would:
1. need leave **keywords seeded** in `field_keywords.py` first (leave has none → the L2
   stem-fallback over-fires to 3,021), and
2. run **165 chunks across several rate-limit windows**, and
3. produce a **smaller, more conservative** correction set than the old 3,964 (the suppressors
   collapse confirmations).

That's a deliberate project, not a quick validation. If Hanna wants leave folded into the
standardized corpus, it's worth doing properly (seed keywords → full run → it supersedes the
prototype). Otherwise the reference stands and the pipeline is validated against it at the
field/era level.
