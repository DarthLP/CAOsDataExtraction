# Per-Topic Failure Modes — Pension

Added 2026-05-27 from the pension run (95 records, 1042 L2 items, 0 clean wins, 0 NHR,
**5 era-baseline outliers** incl. 2 extraction errors). Highest-era-complexity topic with
term; run on Opus. Era baselines (Witteveen accrual cap, franchise floor) are post-hoc only.

## PENSION_FM_01 — Fund-deferred ⇒ leave empty (no fill-in)

**When it applies**: the CAO names a pension fund (bpfBOUW, ABP, PFZW, PMT, BPL, StiPP,
PGB, MITT, Bpf Detailhandel…) and defers premiums/accrual/franchise/ages to "the fund's
regulations" / a website / a separate pension-CAO, without stating its own figure.

**Subagent action**: do NOT supply a number from your knowledge of the fund or statute
(Witteveen 1.875% accrual, AOW age, the minimum franchise). Leave the field empty. This is
the dominant pattern — most CAOs are fund-deferred, so most fields are correctly empty
(696 confirm + 344 unable_to_verify, 0 corrections across 1042 items).

## PENSION_FM_02 — "100% accrual maintained" / Generatiepact ≠ a DB accrual rate (extraction-error pattern)

**When it applies**: a Generatiepact / 80-90-100 / part-time-pension clause says the
employee keeps "100% pension accrual" while working reduced hours; or a source states
"100% of (fictitious) pensionable salary".

**Subagent action**: this is accrual *continuity* (you accrue as if full-time), NOT the
annual DB accrual percentage. Do NOT put 100 (or 80/90/95) into `pension_accrual_rate_value`.
**The upstream extractor made exactly this error on 2 records (157017, 1287018 →
accrual_rate=100%), caught post-hoc by the Witteveen cap.** A genuine DB accrual rate is
~1.6–2.25%; anything near 100 is this misread.

## PENSION_FM_03 — Distinguish concepts; "excedentregeling" keyword is in the prompt header

**When it applies**: nearly every record's L2 flags `pension_excedent_present` because the
word "excedentregeling" appears in the schema *question label*, not the CAO content.

**Subagent action**: set `excedent_present=True` only for an actual above-cap excedent
scheme. The real content under that label is usually a Generatiepact / partial-pension /
survivor's pension / RVU — NOT an excedentregeling → keep False/empty (conservative). Also
keep distinct: employee contribution vs total premium vs cost-split *ratio* (1/3, 50/50);
VPL/FUR/levensloop/Suwas/WGA-gap premiums are NOT the pension contribution; franchise (AOW
offset, € amount) vs participation threshold; DC schemes (StiPP, beschikbare premie) have
no DB accrual rate (WTP-aware — a missing post-2023 accrual % is normal, not invented).

## PENSION_FM_04 — Single value vs range; value/range split across chunks (PIPELINE)

**When it applies**: a CAO states a single uniform contribution/premium/accrual (e.g.
employee 6%, total 27.71%) but the worksheet item is a `_range_min/max/unit` variant.

**Subagent action**: a single homogeneous value is NOT a cross-group range — leave the
`_range_*` fields empty (they're gated on `hetero_pension`). The scalar belongs in the
`_value` field, which is often in a *different* chunk (or already populated by the
extractor). **Pipeline note**: where the extractor already captured the scalar, L2 doesn't
re-flag it; the QA-visible items are the empty range variants. (Verified: the value-rich
records' scalars were already in the dataset — no uncaptured gap.) Era-flagging caveat: the
franchise floor compares EUR values without unit-normalizing **monthly** franchises to
annual — a "EUR per month" franchise will soft-flag below the annual floor (record 163011);
a human filters it.
