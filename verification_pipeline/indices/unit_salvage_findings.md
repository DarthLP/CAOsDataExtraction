# Unit-conversion salvage — findings (2026-07-07)

Four subagents thoroughly audited the top unit-drop offenders (`unit_loss_report.csv`) plus
a surface scan, cross-checking each dropped unit against the schema and the existing
`to_*()` converters. **Governing rule: never invent a conversion that needs data not in the
row** (salary, commute distance, wage sum, total premium, FX) — those stay available-case.

## The one conversion added
- **`homeoffice_stipend_value`**: `"eur over N (calendar) years/period"` → `value / (N·12)`
  eur/month. ~13 docs recovered. (One variant is a home-workplace **setup** budget — math is
  clean, semantics are a one-off, noted.)

## Why (almost) everything else is correctly left available-case
The drops are overwhelmingly **genuinely different quantities**, not missed conversions:
- **Absolute € in a %/months field** (severance €, overtime €/hour, reimbursement €, lump €):
  converting needs the base wage/salary → forbidden.
- **% of an external base** (training "% of wage sum", bonus "% of income", relocation "% of
  salary", "% of WML"): converting to € needs that base → forbidden.
- **Per-distance vs per-time** (commuting per-km canonical vs per-month/day amounts): needs the
  commute distance → forbidden.
- **Ambiguous "factor"** (overtime): premium (1.5→50%) vs multiplier (1.5→150%) is
  irreducible; all examples are 1.0 → no signal → correctly dropped.
- **pension "fraction/share/third of premium"**: this is a share of the *premium*, but the
  field's canonical is *% of salary* — a different base. **NOT** the ×100 case. Left dropped
  (also respects the pension available-case-only rule).

Most of these are already captured in **descriptive companion columns** (severance €/% buckets
via `split_severance`; `training_budget_pct`; `bonus_fixed_lump_pct`; `fringe_commuting_permonth_eur`),
so they are not truly "lost" — just not on the canonical magnitude axis. And with the zero-fill
bug fixed, they now stay available-case instead of being scored as 0.

## Flags for Hanna to review (extraction/schema, not index bugs)
1. **training_cost_reimbursement_value** holds bare € amounts (~185 docs, "eur", "eur per year"
   ~€1750) that are structurally **training budgets**, not reimbursement %s — likely mis-filed
   across `training_budget_value` ↔ `training_cost_reimbursement_value`. Worth an extraction
   spot-check. (A `training_cost_reimbursement_eur` companion could capture them if kept.)
2. **training_budget_pct** silently mixes two denominators — aggregate "% of wage sum" (sector-
   fund levy, ~0.4–2%) and per-employee "% of gross salary" — don't treat as one homogeneous base.
3. **term_severance "eur (gross) per calendar year"** is a *recurring* amount pooled into the
   one-off `eur` bucket by `split_severance` — a cadence mismatch to note.

## Existing converters spot-checked
The subagents reviewed the current `to_*()` rules (per-working-day ×21.7, per-week ×4.33,
per-year /12, weeks-of-salary → months ×12/52, etc.) and found them sound.
