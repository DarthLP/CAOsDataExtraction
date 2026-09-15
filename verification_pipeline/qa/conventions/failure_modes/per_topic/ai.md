# Per-Topic Failure Modes — AI / Algorithmic Management

Added 2026-05-22 from ai run (16 scoped records, 0 clean wins, 0 NHR). The newest, sparsest topic — effectively absent from the current CAO corpus; the extractor's all-False AI flags were already accurate.

## AI_FM_01 — Self-rostering / scheduling software is not "AI" unless framed as such

**When it applies**: a CAO mentions automated self-rostering, preference-scheduling software, or a "calculation tool" — without framing it as AI or algorithmic decision-making.

**Subagent action**: do NOT set `ai_policy_exists` or `ai_automated_decisions` True for ordinary scheduling/preference software. Set True only if the source explicitly frames the system as AI / algorithmic management / automated decision-making about workers (cf. AVG/GDPR Art 22).

**CSV impact**: `ai_ai_policy_exists`, `ai_ai_automated_decisions`.

## AI_FM_02 — Intent-to-study is not an AI policy

**When it applies**: the CAO says parties "will study / monitor / launch a campaign on" technological change or AI, or mentions AI only in a sustainable-employability aside.

**Subagent action**: an intent or awareness campaign is NOT an AI policy. Set `ai_policy_exists=True` only for an actual policy/protocol governing AI use. Likewise `ai_governance_body_present=True` only for a body that actually oversees AI (not generic OR consultation).

**CSV impact**: `ai_ai_policy_exists`, `ai_ai_governance_body_present`, `ai_ai_training_rights_present`.

**Note**: across 16 AI-mentioning CAOs, zero genuine AI provisions were found — every QA answer confirmed the extractor's existing `False`. Expect this topic to grow in future CAO cycles; the FM entries will matter more then.
