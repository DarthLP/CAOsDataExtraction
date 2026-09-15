# Termination (term) — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-24/25)
Source `termination_information.md` (5.7 MB), English-translated with retained Dutch
legal terms (BW 7:672/7:652/7:673, AOW, UWV/WERKbedrijf, transitievergoeding,
bouwplaats/uta-werknemers). The highest-era-complexity topic (WWZ/WAB transitions,
notice schedules, probation, severance). **~28 fields**: master boolean
`term_has_termination_rules`; employer/employee notice value + range; probation
fixedterm/indef; severance extra value/unit/formula + tenure note + WW supplement;
notice tenure present/rule; notice min floor; dismissal approval/protection;
`term_end_at_AOW_auto`; probation_allowed; shorten_notice_uwv; hetero.

**Era-baseline gate (Phase 4.5):** baselines approved by Hanna 2026-05-24 and encoded
**post-hoc only** — never injected into the subagent prompt (avoids statutory anchoring;
see `phase_4_5_era_baseline_review.md`). Probation cap (2 mo) and transitievergoeding
cap are flag-only; notice schedule / AOW table are informational.

## Stage 1 — Scope: 95 records / 95 CAOs.

## Stage 2 — Deterministic: **0 L1, 913 L2, 0 ENUM → 46 chunks** (20 items each).
913 L2 is high because termination concepts appear in almost every CAO and the
value+unit+range field families pair up; distribution healthy (max 67/95, no over-fire).

## Stage 3 — Subagent review: **Opus**, 46 chunks.
Calibrated the first 3 chunks (all clean — no statutory fill-in, conservative booleans)
before scaling. **Interrupted mid-run by the weekly rate limit; resumed after reset and
completed all 46.** Two cautions enforced via `TERM_RUN_NOTES.md`: (1) no statutory
fill-in; (2) conservative presence booleans. Both held across all 46 chunks.

## Stage 4 — Aggregate + audit + era flag (FINAL)
| Bucket | Count |
|---|---|
| Total worksheet items | 913 |
| **Clean wins** | **18** |
| NHR | 1 |
| **Era-baseline outliers** | **0** |
| Spelled-out / tenure-graded values surfaced for review | 11 |
| is_noop (incl. confirmations) | 894 |

Integrity: 913 rows = 913 det items, 0 duplicate keys, 0 non-`term_` fields, csv_recovery 0 unrecoverable.

### The 18 clean wins
Genuine **non-statutory** deviations where the CAO states a concrete digit:
- **Employer notice base value** extracted from explicit tenure schedules (1 month base): CAO 592, 405, 51, 433, 43.
- **CAO 496024**: 8-week notice for both employer & employee (verified verbatim: *"both parties have a notice period of 8 weeks"*).
- **Probation**: CAO 245 & 625 fixed-term probation = 2 months (explicit CAO maxima, not statutory citation).

### Why 894 no-ops (the extractor + "no fill-in" rule)
Term is overwhelmingly **statutory restatement**: most CAOs defer notice/probation/
severance to the BW, so those fields are correctly empty. The subagents left them empty
rather than copying statutory numbers — exactly the approved behaviour. **0 era outliers**
confirms no below-floor/above-cap values slipped in (probation extractions are ≤ the 2-mo
cap; severance fields all empty; notice is informational).

## Stage 5b — Items for Hanna
1. **TERM_FM_01..04** added to [term.md](../conventions/failure_modes/per_topic/term.md).
2. **18 clean wins, 1 NHR, 0 era outliers.**
3. **11 spelled-out / tenure-graded values surfaced** → `outputs/spelled_out_values_for_review.csv`. Across **5 CAOs (725023, 1618009 UMC, 727036, 609002, 163011)** the CAO states a genuine non-statutory notice/probation value, but either (a) spells the number out as words ("two months") so it failed the strict literal-digit evidence rule, or (b) is tenure-graded (1/2/3/4 months by service) with no single scalar. The subagents correctly routed these to `unable_to_verify` with an empty value (never fabricated a digit) — so they sit as no-ops, **not captured**. Each is trivially human-resolvable (confirm the intended digit, or populate the `notice_tenure_rule`/`_range` fields).
4. **Era baselines validated in production**: post-hoc flagging ran clean, 0 outliers — and crucially, the model was never shown a statutory figure.
