# Per-file conflict verification — PILOT (8 units, 7 with source)

Method: for each within-agreement conflict, a subagent re-extracted the field from
**each version-record's own file source block INDEPENDENTLY** (blind to the dataset
value and to siblings), given the field's schema/pydantic definition + enum vocab as
context. We then compared the independent per-file extractions to the dataset.
SURFACE-ONLY — nothing applied.

Result: **7/7 conflicts actionable. 19 record-cells judged → 8 CONFIRM, 6 EMPTY,
3 FIX, 2 RECONCILE.** Four distinct outcome patterns:

## 1. Wrong-slot contamination across ALL versions → EMPTY (6 cells)
The "conflict" is a symptom: a value is misfiled into a field it doesn't belong to,
in every version. The files AGREE (all silent on the actual field).
- `157 bonus_fixed_annual_lump_value`: 3.5 vs 4.0 → both files state only a **year-end
  bonus as a % of annual income** (a percentage, which this EUR-lump field excludes).
  Both EMPTY. (The 3.5%→4.0% IS a real change — but of the *year-end-bonus* field, not this one.)
- `496 bonus_fixed_annual_lump_value`: 700 vs 2.01×3 → €700 is an explicitly **one-off**
  object allowance (not recurring annual); 2.01% is the year-end bonus %. All 4 EMPTY.

## 2. Single-record extraction error → FIX to the agreed value (3 cells)
The files actually agree; one record's recorded value is wrong.
- `50 overtime_allowance_range_max`: 50009=150 ✓, 50011=**175 → 150** (both files state 150% max).
- `2535 childcare_provider_scope`: 2535012=**sector_only → unspecified** (file doesn't restrict providers).
- `823 childcare_provider_scope`: 823005=**sector_only → unspecified** (file doesn't restrict providers).

## 3. Thin re-filing under-represents the agreement → RECONCILE (2 cells)
The files genuinely differ because a thin/empty re-filing omits a section the full
document carries. The agreement-level truth is the full doc's value. ("Thin docs are jumpy.")
- `163 ai_ai_governance_body_present`: full doc 163004 establishes a privacy committee = True;
  empty re-filing 163005 = False → RECONCILE to **True**.
- `35 overtime_has_overtime_rules`: 3 full originals = True; thin interim AVV amendment 35010 = False
  → RECONCILE to **True**.

## 4. Already correct → CONFIRM (8 cells)

## Answer to "how did we decide the 1,125 are temporal?" — the heuristic is LEAKY
Both heuristic-labelled `LIKELY_TEMPORAL_CHANGE` units in the pilot were **NOT** real
temporal changes when checked against source:
- `496` (700→2.01) = all-version contamination (pattern 1), not a raise.
- `823` (unspecified→sector_only) = single-record error (pattern 2), not a change.
The clean-step-over-dates rule cannot distinguish a real raise from a unit-confusion /
single-record error — **only the source text can.** The 1,125 must get the same per-file
check; do not trust the temporal label on its own.

## Implication
The agreement lens + per-file re-extraction is high-yield (every pilot conflict found a
real issue) and the verdicts are source-grounded and policy-clean (no-invent/no-statutory).
Ready to scale; RECONCILE (pattern 3) is a policy choice (inherit to agreement level vs.
keep per-version) to confirm before applying.
