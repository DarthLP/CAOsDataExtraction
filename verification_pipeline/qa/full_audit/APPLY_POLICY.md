# Apply policy — what gets written to the dataset, and what does not

The per-file verification produced six action types. This document records **exactly
which are applied** and why. Originally applied to a **copy** (`perfile_applied.csv`).

> **UPDATE (2026-07-01):** `FIX_clear` was subsequently **promoted** over `qa/corrected_dataset.csv`
> (now canonical; indices rebuilt on it). The untouched G1 original is preserved at
> `qa/_old/corrected_dataset.bak.2026-07-01.csv`; the `perfile_applied.csv` copy was deleted
> (byte-identical to canonical). Full lineage: `docs/DATA_LINEAGE.md`.

## Output
- **`qa/corrected_dataset.perfile_applied.csv`** — a copy of `corrected_dataset.csv`
  with ONLY the FIX_clear changes applied.
- **`qa/full_audit/perfile_apply_changelog.csv`** — every cell change (record, field,
  old value/unit → new value/unit, source quote) — fully reversible.

## What each action means, and whether it is applied

| action | n | applied? | why |
|---|---:|:--:|---|
| **FIX (clear)** | **2,949** | ✅ **YES** | the record's **own source** states a different value (verified: the new value = the record's own `source_value` in 100% of cases). High-confidence: source-numeric, same unit dimension, not a convention field, quote-backed. |
| FIX (audit) | 1,416 | ❌ no | own-source-stated, but the *value convention* needs a human call (statutory base-vs-total, surcharge increment-vs-total, tier-selection enums). → `FIX_audit.csv` |
| RECONCILE | 2,960 | ❌ no | removal based on **absence** (no version states it). Not applied — assumes source capture is complete. → review |
| EMPTY | 848 | ❌ no | removal based on absence. Not applied — same reason. → `EMPTY_candidates.csv` |
| KEEP | 2,681 | ❌ no | silent record, a sibling differs → **keep the record's OWN/old value** (never inherit from a sibling). No change. |
| REVIEW | 498 | ❌ no | undecidable → no change (old value kept). |
| CONFIRM | 13,434 | ❌ no | already correct → no change. |

Also **not applied** (kept separate, pending your decisions):
- `statutory_deferred.csv` (172) — deferred to a later deterministic statutory-baseline step.
- `surcharge_normalize.csv` (41) — mechanical normalization to the TOTAL convention.

## The rule in one line
**We only write a cell when the record's OWN source positively states a different value,
and the change is high-confidence (non-convention).** Nothing is inherited from a sibling;
nothing is removed on the basis of silence; nothing convention-dependent is auto-applied.

## Reversibility
Every applied cell is logged in `perfile_apply_changelog.csv` with its old value; the
original `corrected_dataset.csv` is untouched, so the copy can be regenerated or reverted
at any time.
