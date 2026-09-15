# Per-Topic Failure Modes — Wage / Salary-Scale Mechanics

Added 2026-05-23 from the wage run (95 scoped records, 163 worksheet items, 3 clean wins, 0 NHR). Wage shares `wage_information.md` with bonus. The dominant finding: scale movement in Dutch CAOs is almost always **automatic**, so the three discretionary provisions targeted here are rare and easy to over-claim.

## WAGE_FM_01 — Automatic periodieken / functiejaren are NOT performance-based step variation

**When it applies**: the CAO describes annual step increases by age, service years, functiejaren, normperiodiek, or "periodiek 0/1/2…" with no appraisal condition.

**Subagent action**: do NOT set `wage_perf_step_var_present=True` for automatic progression. Set True only when a step is explicitly **granted or withheld based on a performance appraisal/assessment** (e.g. "a periodic increase is granted with a positive appraisal"). An above-scale performance *bonus* is also NOT a step variation — that belongs to the bonus topic.

**CSV impact**: `wage_perf_step_var_present`, `wage_perf_step_var_rule`.

## WAGE_FM_02 — `inloopperiode`/`aanloopschaal` is a *lower* start; acting-up allowances are not scale-max supplements

**When it applies**: the source mentions an inloopperiode, aanloopschaal, wachtperiodieken (a *lower* starting step for employees who lack experience), OR a temporary waarneming/combifunctie/substitution allowance.

**Subagent action**:
- `wage_entry_step_exp_present=True` requires a *higher* entry step granted **in return for relevant prior experience** — the opposite of an aanloopschaal. Starting first-timers at the minimum, or a lower aanloopschaal, is `False`.
- `wage_pers_allow_max_scale=True` requires a personal allowance paid **at the scale maximum** OR **as a guarantee after reclassification/downgrade** (e.g. CAO 1471012, CAO 826's 55+ difference-allowance). Temporary acting-up (waarneming), combifunctie, or special-skills supplements are `False`.

**CSV impact**: `wage_entry_step_exp_present`, `wage_entry_step_exp_rule`, `wage_pers_allow_max_scale`, `wage_pers_allow_rule`.

## WAGE_FM_03 — L2 generates present/rule pairs independently → orphan rules and missed booleans (PIPELINE)

**When it applies**: each wage field is one half of a present-flag/rule-note pair. The L2 presence scan fires per-(record, field), so it can generate an item for `*_rule` but not the paired `*_present` (or vice versa).

**Observed**: CAO 1471012 got `wage_pers_allow_rule` populated but had no `wage_pers_allow_max_scale` item, leaving the boolean `False` against a populated rule (an R1-shaped inconsistency in the output). Conversely, CAOs 826/43/932 had genuine provisions for which **no item of either half** was generated, so QA never saw them.

**Pipeline action** (for a future re-run, not auto-applied this run per the surface-don't-fix rule): in `qa_wage_build_chunks.py`/det, when emitting a worksheet item for either half of a present/rule pair, **emit both halves together** so the subagent always sets the boolean and the rule consistently. Separately, broaden the wage keyword set (guarantee allowance, garantietoeslag, persoonlijke toeslag, beoordeling/appraisal-gated periodiek) to reduce never-scanned false-negatives.

**Note**: across 95 CAOs only 2 carried a genuine discretionary provision that reached a worksheet item (721017 perf_step_var, 1471012 pers_allow). The extractor's all-automatic reading was right for the rest.
