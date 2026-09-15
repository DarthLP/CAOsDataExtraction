# Per-Topic Failure Modes — Safety & Integrity

Added 2026-05-22 from safety run (95 scoped records, 21 clean wins, 1 NHR). All fields are booleans + 2 notes; corrections are False→True flips where the extractor missed a provision.

## SAFETY_FM_01 — Statutory-vs-CAO booleans (RI&E / arbodienst)

**When it applies**: RI&E (`rie_psa_required`) and occupational-health access (`arbodienst_access_provided`) are partly statutory (Arbowet). A generic RI&E or arbodienst mention is common.

**Subagent action**: set `rie_psa_required=True` ONLY when the RI&E is stated to cover *psychosocial* risk (PSA / stress / burnout), not just generic physical Arbo. `arbodienst_access_provided=True` when the CAO references/guarantees occupational-health access. Don't infer beyond what's stated.

**CSV impact**: `safety_rie_psa_required`, `safety_arbodienst_access_provided`.

**Example**: CAO 884 industry RI&E explicitly covers PSA → rie_psa_required=True. CAO 725 generic RI&E with no PSA statement → False.

## SAFETY_FM_02 — Harassment vs integrity protocol

**When it applies**: a sexual-harassment / unwanted-behaviour (ongewenst gedrag) policy vs a general code of conduct / integrity / MVO policy.

**Subagent action**: sexual-harassment/PSA protocol → `harassment_protocol_present=True`; general code-of-conduct/integrity → `integrity_protocol_present=True`. Both can be true. A pure MVO/CSR endorsement is NOT a harassment protocol.

**CSV impact**: `safety_harassment_protocol_present`, `safety_integrity_protocol_present`.

## SAFETY_FM_03 — External reporting channel discipline

**When it applies**: a complaints/reporting procedure is mentioned.

**Subagent action**: `reporting_channel_external=True` ONLY for a guaranteed *external* channel (klokkenluidersregeling / external whistleblower hotline / external confidential body). An internal complaints procedure or internal confidential adviser does NOT set this True (that's `confidential_counsellor_present`).

**CSV impact**: `safety_reporting_channel_external`, `safety_confidential_counsellor_present`.

## SAFETY_FM_04 — `*_present` boolean discipline (genuine provision only)

**When it applies**: a `*_present` flag fires on a generic keyword. Arbodienst access is not a joint safety committee; Works-Council consultation is not a safety committee; awareness info is not funded training; "intends to investigate" is not a provision.

**Subagent action**: set a `*_present` flag True only for a genuine provision of that exact type. `safety_training_present` includes BHV/EHBO and funded mandatory safety/PSA training. `safety_committee_present` needs a joint safety/health committee, not generic OR consultation. Mirrors CONTRACT_FM_04 / BONUS_FM_04 / FRINGE_FM_02 boolean discipline.

**CSV impact**: all `safety_*_present` booleans.

**Note**: across 95 CAOs the extractor's safety booleans were already highly accurate — only 21 genuine missed provisions surfaced, and 368 subagent answers simply confirmed the existing (correct) `False` values.
