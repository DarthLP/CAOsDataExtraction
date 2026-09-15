# AI / Algorithmic Management — Topic QA Summary Memo

## Stage 0: Hazard check (2026-05-22)

### Language profile + source
Source `AI_information.md` (note the uppercase filename), English-translated with retained Dutch (AVG, OR/works council). **Extremely sparse**: only 20 of 1,506 blocks non-empty (token-median 207) — AI is the newest topic and barely appears in the CAO corpus yet.

### Schema shape
4 booleans (ai_policy_exists, ai_governance_body_present, ai_training_rights_present, and ai_automated_decisions — a soft enum never/with_human_review/unspecified/other) + 1 free-text note (ai_policy_note).

### Hazard list
| H# | Hazard | Field(s) | Status |
|---|---|---|---|
| H1 | Self-rostering / scheduling software framed as "AI" when it's just preference software | `ai_policy_exists`, `ai_automated_decisions` | **CONFIRMED** — several records mention automated self-rostering; only count as AI if the source frames it as algorithmic/AI decision-making |
| H2 | Vague "we will study technological change / AI" intent vs an actual AI policy | `ai_policy_exists` | RISK — an intent-to-study is not a policy |
| H3 | OR/works-council involvement in technology = governance body? | `ai_governance_body_present` | NOTED — needs a body actually overseeing AI |
| H4 | AVG/GDPR Article 22 automated-decision references | `ai_automated_decisions` | NOTED |
| H5 | `ai_policy_note` free-text field wrongly treated as an enum by the ENUM scan | (pipeline) | **BUG FOUND + FIXED** — see below |

### Pipeline bug found + fixed
The Stage-2 ENUM scan classified the free-text `ai_policy_note` as an enum (its schema description contains the quoted words 'annual' and 'none') and flagged 12 long note texts as "non-canonical enum values". **Fix:** `schema_lookup.get_topic_enum_fields` now excludes `_note`/`_text`/`_summary`/`_rule_text` free-text fields from enum detection. Verified legit enums (overtime compensation_mode/stacking_rule/selection_rule, homeoffice discretion, ai_automated_decisions) still detected; 66 tests pass.

### Recommendation: PROCEED. FM entries:
- **AI_FM_01** — Self-rostering/scheduling software is AI only if the source frames it as algorithmic/AI; otherwise don't set ai_policy_exists/automated_decisions True.
- **AI_FM_02** — Intent-to-study technological change is NOT an AI policy; set ai_policy_exists=True only for an actual policy/protocol.

**HARD STOP CHECK:** no. Tiny topic; schema fine.

## Stage 1 — Scope filter
- 2,739 → **16 scoped records, 16 unique CAOs** (only CAOs whose source mentions AI/algorithms).

## Stage 2 — Deterministic layer
- L1: 3 (R1 policy_exists=False+note). L2: 18. ENUM: 0 (after the fix). **21 items → 2 chunks.**

## Stage 3 — Subagent review
- 2/2 chunks, Sonnet, 1 batch.

## Stage 4 — Aggregate + audit (FINAL)
| Bucket | Count |
|---|---|
| Total worksheet items | 21 |
| **Clean wins** | **0** |
| NHR | 0 |
| Suppressed: noop-vs-CSV | 18 |

- **0 clean wins, 0 NHR.** Every subagent answer was `False` and matched the CSV's existing `False` — the extractor had already correctly identified that none of the 16 AI-mentioning CAOs contain a genuine AI policy, governance body, AI-specific training right, or algorithmic-decision provision. The "AI" mentions are self-rostering software, IT/data-analytics asides, and intent-to-study statements — none qualifying.
- This is the expected result for the newest, sparsest topic: there was nothing to correct, and QA confirmed it.

## Stage 5b — Items for Hanna
1. **AI_FM_01..02** added to [ai.md](../conventions/failure_modes/per_topic/ai.md).
2. **0 clean wins, 0 NHR.** AI is effectively absent from the current CAO corpus; the extractor's all-False AI flags are accurate.
3. Pipeline bug fixed (ENUM scan no longer treats free-text `_note` fields as enums) — applies to all topics.
