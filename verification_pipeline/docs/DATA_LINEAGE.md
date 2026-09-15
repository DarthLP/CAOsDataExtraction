# Data lineage — which dataset is real, and what's in it

**The one question this file answers:** if you open `qa/corrected_dataset.csv`, what is it,
how was it built, and what corrections are applied vs still pending?

Last updated: 2026-07-15 (G33).

---

## The canonical dataset

**`qa/corrected_dataset.csv` is canonical.** It is the raw upstream extract with **thirty-four
correction layers** applied, in order (~9,195 corrected cells through L7, plus L8's 3,559 source-verified
boolean/numeric flip fixes, L9's 61 two-pass-verified numeric same-term fixes, L10's 111
consistency-unify + tie-break + full-passage-recovery cells, L11+L12's 36 date-collision picked-record
fixes, L13's 1,071 systemic pension %-of-premium blanks, the jump campaign's L14 10 + L15 41 +
L16 77 + L17 14 + L18 400 + L19 14 = 556 source-verified fixes across both directions and all topics,
L20's 902-cell tier-A same-term campaign, L21's 106-cell CANT_TELL-resolution layer at the
full-text/re-OCR evidence tier, L22's 22 Hanna rulings, L23's 250-cell corrupt-parse re-extraction,
L24's 461-cell tier-B sandwich campaign, L25's 1,410-cell tier-C-suspicious + probation-family layer,
L26's 1,465-cell tier-C-persistent layer, L27's 511-cell final same-term ripple closure, and
L28's 213-cell ripple waves 2–6 to the fixed point, L29's 274-cell post-campaign extension (full-text CANT_TELL closure + part-time screen + CAO-1496 family sweep), and L32's 42-cell CANT_TELL family-consistency closure — every cross-edition index jump tier adjudicated propose→gate→ripple-closure, amendment-aware).
Shape 2,739 records × 317 cols (key column `id`), 242 CAOs. The 13 per-topic indices in
top-level `indices/` are built from it.

## The generations (all regenerable, all reversible)

| Gen | File | = parent + | cells vs raw | verified |
|---|---|---|---|---|
| G0 | `inputs/extracted_data_non_salary.csv` | (raw upstream copy) | — | — |
| — | `qa/_old/corrected_dataset.bak.2026-06-04.csv` | G0 + Layer 1 | 514 | per-topic apply |
| G1 | `qa/_old/corrected_dataset.bak.2026-07-01.csv` | + Layer 2 (full-audit promotion) | 1,295 | twice-verified |
| G2 | `qa/_old/corrected_dataset.bak.2026-07-03.csv` | + Layer 3 (per-file `FIX_clear`) | 4,585 / 1,366 recs | mechanical + spot-check |
| G3 | `qa/_old/corrected_dataset.bak.2026-07-03_pre-wave2.csv` | + Layer 4 (verified absence-removals) | ~6,917 | gate-sampled + snippet-judged |
| G4 | `qa/_old/corrected_dataset.bak.2026-07-03_pre-L6.csv` | + Layer 5 (convention adjudication + Hanna rulings) | ~9,022 | Haiku adjudicated + Sonnet spot-verified (57/60) |
| G5 | `qa/_old/corrected_dataset.bak.2026-07-05_pre-L7.csv` | + Layer 6 (CHECK-pattern resolutions, Hanna-accepted) | 9,155 | per-cell suggestions reviewed + accepted |
| G6 | `qa/_old/corrected_dataset.bak.2026-07-08_pre-L8.csv` | + Layer 7 (residue suggestions, Hanna-accepted) | ~9,195 | per-cell second-reader suggestions, quote-grounded |
| G7 | `qa/backups/corrected_dataset.csv.bak_numfix2` | + Layer 8 (source-verified boolean-flip + numeric-regression fixes) | 3,559 changed vs G6† | Sonnet-on-snippets, majority-vote across editions |
| G8 | `qa/backups/corrected_dataset.csv.bak_unify` | + Layer 9 (all-CAO numeric same-term extraction-error fixes) | 61 changed vs G7 | Sonnet verify + **independent 2nd-reader** confirm (50% of first-pass rejected) |
| G9 | `qa/_old/corrected_dataset.bak.2026-07-08_pre-L11-collision.csv` | + Layer 10 (same-term consistency-unify + tie-break + full-passage recovery of `cant_tell`/leave) | 111 changed vs G8 | 44 majority-unify (7 unit-blind clobbers reverted) + 32 tie-break (20 freq + 5 highest-rate + 6 Hanna per-case + 1 formatting) + 33 two-pass-verified (19 numeric + 14 blank-fills) + 2 paired-unit; ALL 30 ties resolved |
| G10 | `qa/_old/corrected_dataset.bak.2026-07-08_pre-L12-lowconf.csv` | + Layer 11 (date-collision picked-record fixes, index-relevant fields) | 33 changed vs G9 | two-pass (Sonnet propose w/ both docs' source + field defs → independent 2nd-reader; 39→29 confirmed, applied high+med) |
| G11 | `qa/_old/corrected_dataset.bak.2026-07-08_pre-L13-pension.csv` | + Layer 12 (2 Hanna-approved low-confidence collision fixes) | 3 changed vs G10 | Hanna per-case approval |
| G12 | `qa/backups/corrected_dataset.csv.bak_dipfix` | + Layer 13 (systemic pension `employee_contrib` %-of-premium blanks, 3 passes: explicit-unit 896 + same-CAO siblings 139 + ≥20% plausibility 36) | 1,071 changed vs G11 | deterministic unit rule + Sonnet source-calibration 6/6 + 8/8 high-value verification; intermediate baks `pre-L13b`/`pre-L13c` |
| G13 | `qa/backups/corrected_dataset.csv.bak_dipsweep` | + Layer 14 (downward-jump dip verification: 3 boolean restorations incl. 1 med-conf + 6 childcare over-extraction mislabels True→False [CAO 1029 ×4, 254001, 549004] + 310006's dropped probation unit) | 10 changed vs G12 | Sonnet two-pass (propose → independent 2nd reader; rejections at pass 2 incl. one caught via a source_lookup wrong-file bug, since fixed) + targeted rechecks; change-log `indices/corrections/dip_boolean_corrections_applied.csv`, provenance `L14_dip_boolean_verification` |
| G14 | `qa/backups/corrected_dataset.csv.bak_family2` | + Layer 15 (family + secondary-mover + next-side sweep of the dip events: 549 childcare ×7 over-coding, 2948 pension accrual ×12 systemic under-extraction, prev/next-side childcare/bonus/safety/homeoffice over-codings, 2 same-term/neighbour-evidenced restorations [3313 sick-topup, 488 psa+committee]) | 41 changed vs G13 | three-sided (prev/dip/next) Sonnet sweeps → independent final-gate adjudication (41 APPLY / 57 SKIP; killed a false restoration [125 pre-telework era], caught 2 stale next-side values); change-log `indices/corrections/dip_sweep_corrections_applied.csv`, provenance `L15_dip_family_and_secondary_sweep` |
| G15 | `qa/backups/corrected_dataset.csv.bak_3313` | + Layer 16 (family-CONSISTENCY completion: 2535 childcare ×14 + homeoffice ×17 ['thuiswerker' piece-rate ≠ telework], 791 childcare ×11 + profit-sharing ×10 + performance ×2 [advisory boilerplate/à-la-Carte swap], 1022 ×16 [phantom safety/childcare], 1264 hetero_pension ×7 [flat 40/60 split]) | 77 changed vs G14 | family sweep → independent final gate (77 APPLY / 19 CORRECT_AS_IS kept, 0 rejections; 1022's 7 real childcare-contribution editions preserved). ⚠ Gate finding: several of these phantom Trues were INTRODUCED by L8's flip campaign (evidence mis-cited) — L8's cells should be read with that caveat. Change-log `indices/corrections/dip_family2_corrections_applied.csv`, provenance `L16_dip_family_consistency` |
| G16 | `qa/backups/corrected_dataset.csv.bak_topicsweep` | + Layer 17 (CAO 3313 pension family: mandatory_participation ×5 + accrual_stat_leaves ×9 unsupported Trues → False; 3 explicit "participation is mandatory" Trues KEPT; 1 CANT_TELL held out) | 14 changed vs G15 | family sweep + independent second read (definitions verbatim; premium-payment ≠ accrual statement). Change-log `indices/corrections/dip_3313_corrections_applied.csv`, provenance `L17_3313_pension_family` |
| G17 | `qa/backups/corrected_dataset.csv.bak_unifypass` | + Layer 18 (TOPIC-LEVEL + UPWARD jump campaign: 162 families family-verified + upward risers + PDF-parked cases; incl. 76 childcare phantoms, 84 homeoffice fixes, 18+13 overtime modes → both/unspecified, bonus vakantietoeslag/EJU mapping, safety over/under-extraction, numeric fixes, and 310005's FABRICATED homeoffice block cleared after re-OCR of its rotated scanned PDF) | **400 changed vs G16** | 7 family sweeps + upward sweep → 4 independent final gates (16 rejections incl. gate-caught sweep errors) + original-PDF checks (2 verdicts overturned: 1060/50 overtime; 397003 contamination REFUTED; 152/563 modes settled). Resolver hardened twice (extension/prefix bug + underscore-collision bug, 9 collision groups). Change-log `indices/corrections/topic_sweep_corrections_applied.csv`, provenance `L18_topic_sweep` |
| G18 | `qa/backups/.bak_tierA_corrected_dataset.csv` | + Layer 19 (CONSISTENCY-UNIFY of ambiguous cells: 107 CANT_TELL cells / 82 families screened → 22 text-identity unify proposals → 14 applied [training-cost tier convention ×4, overtime mode siblings ×2, extraction-gap booleans ×6, phantom-pattern False ×2]; verified-real divergences and oscillating families explicitly protected) | **14 changed vs G17** | text-identity rule (same fact, same text → same value; NEVER against verified-real divergence; unit-checked) → independent gate (14/22; 8 rejected incl. anchors that were themselves suspect). 3 family-level flags parked for later review (1193 psa under-code, 2520 sibling over-code, M&T '55+ reclassification' false-positive trigger). Change-log `indices/corrections/unify_corrections_applied.csv`, provenance `L19_consistency_unify` |
| **G33** | **`qa/corrected_dataset.csv`** (canonical) | + Layer 34 (L32 FOLLOW-UP FLAGS closed, own-text verified per cell: 759 derived-42.5 quintet → blank [the literal "42,5" in 3 editions names a different fact — the Overlegregeling quarterly cap]; 3768 AOW-niche 6.0 trio + statutory-restatement 3.0 pair → blank; 243019 max-hours 47.5 filled [literal in own text — closed the last uncovered coverage side]; 1214 "one-off" flag DISSOLVED [own text: recurring structural December bonus — value kept, unit relabeled EUR per year]; 1029 maritime family re-routed [no rotating-shift scheme exists: shift_allowance_range cells blanked ×4 eds, unfavourable-hours base unified at the 60% weekday/Saturday tier incl. 1029008's Sunday-ceiling misread 100→60 and 1029006's port-watch misattribution 30→60]) | **37 changed vs G32** | provenance `L34_followup_flags`; log `indices/corrections/l34_corrections_applied.csv`; backup `qa/backups/.bak_L34…`. POST: coverage 0 uncovered, census unchanged (1 verified drop), battery green |
| G32 | `qa/backups/.bak_L34_corrected_dataset.csv` | + Layer 33 (FULL-TEXT CLOSURE of the 100 L31 restoration-doubt CANT_TELLs: 2 Sonnet readers at the parsed-markdown tier [71 FIX / 25 KEEP / 4 still-CT] → independent gate re-derived all 71 [71/71 APPLY — third read for these cells] + 249001 orphan-unit cleanup) | **90 changed vs G31** | provenance `L33_ct_fulltext`; log `indices/corrections/l33_corrections_applied.csv`. ⚠ CONCURRENCY INCIDENT, resolved: L32 (background session) and this layer were applied in parallel under the same "L32" label — the log file collided (this session's write clobbered the other's); the DATASET was unharmed (both applies verified intact cell-by-cell: 89/89 + 48/48), the lost log was reconstructed EXACTLY from the backup diff (.bak_L31b→.bak_L32) and layers renumbered. Overlap audit of the 35 cells both sessions adjudicated: 6 identical conclusions (guard no-ops), 34 family-consistency-CONFIRM vs full-text+gate FIX (full-text tier outranks; FIX stands), 1 substantive disagreement (249001 notice range_max — blank stands per the gated reason-specific ruling; flagged for Hanna in all_open_cant_tells.csv). LESSON: single-writer rule for the canonical dataset; reserve layer numbers before applying |
| G31 | `qa/backups/.bak_L33b_corrected_dataset.csv` | + Layer 32 (CANT_TELL FAMILY-CONSISTENCY CLOSURE, Hanna's rule 2026-07-15: all 173 open CANT_TELLs checked against same-cao+same-ingangsdatum siblings [and CAO-unanimous values for booleans] — 104 resolved [30 adopted the family value incl. 3313 sickpay year-1-rate + vacation-160 fixes, 74 confirmed-consistent], 69 stay open [52 no family signal, 12 family split, 5 WITHHELD where the sibling value matches an L31-gated offender pattern: 759 derived 42.5, 3768 AOW-niche 6.0, 1029 shift-routing, 1214 one-off EUR, 3855 junk units]) | **42 changed vs G30** | family-consistency rule + L31-precedent screen per cell; ledger `indices/corrections/l32_corrections_applied.csv` (reconstructed from the backup diff after the collision); resolved log `indices/review/ct_resolved_l32.csv`; follow-up flags `indices/review/l32_followup_flags.csv`; backup `qa/.bak_L32_corrected_dataset.csv` |
| G30 | `qa/.bak_L32_corrected_dataset.csv` | + Layer 31 (RESTORATIVE-DIRECTION RE-VERIFICATION, the audit's follow-up: all 1,628 still-standing False→True restorations and blank→value fills re-judged by 12 Sonnet readers with the FIELD-BOUNDARY question primary ["does this clause satisfy THIS field's definition, for the TYPICAL worker, or a sibling field's?"] — 1,102 KEEP / 412 FLIP / 114 CANT_TELL — then 5 EVIDENCE-TIER-AWARE gates re-derived all 426 disputed items [372 confirmed / 54 REJECTED — the rejects almost all being reader false-alarms on markdown-tier fixes whose quotes the gates verified verbatim in the parsed full text] + closing ripple over 13 exposed siblings) | **516 changed vs G29** (220 True→False de-restorations, 155 numeric/blank corrections, 141 companion units) | recurring confirmed mis-routings: ploegentoeslag ≠ job allowance (systemic, CAO 487/822 families), calamiteitenverlof ≠ care leave, verzuim registration ≠ workload monitoring, PMO/PAGO ≠ arbodienst access, niche groups (youth/apprentices/55+/AOW/single-occupation) ≠ typical worker, pension_premium_eq_split requires future-changes language not a base 50/50 split, "1 pay period" conversions killed. Post: flip-rate steady at 3.3%, census unchanged (1 verified drop), coverage 0 uncovered. Log `indices/corrections/l31_corrections_applied.csv`, provenance `L31_field_boundary`; backups `qa/backups/.bak_L31*` |
| G29 | `qa/backups/.bak_L31_corrected_dataset.csv` | + Layer 30 (INDEPENDENT END-TO-END AUDIT + Opus adjudication: 39-cell stratified random sample of applied fixes re-derived by 2 fresh Sonnet auditors [29 AGREE / 7 DISAGREE / 3 UNCLEAR] → all 10 disputes ruled by an Opus adjudicator [auditor right 6, fixer right 2 incl. refuting the auditor's sibling-import claim on 1646007, neither 2] → 8 repairs + 2 PersVeilig family cells) | **12 changed vs G28** | KEY FINDING: errors concentrate in the RESTORATIVE direction — sample error rates: remove-phantom 0/14, clear-to-blank 1/5, fill-blank 2/7, **restore False→True 5/11** (over-crediting related-but-wrong clauses: doctor-visit hours ≠ care leave, verzuim registration ≠ workload monitoring, ploegentoeslag ≠ job allowance, PersVeilig project ≠ safety training, 55+ clause ≠ general right). Follow-up recommended: targeted re-verification of the ~1,700 restorative cells with field-boundary discipline. Also: mechanical verification PASSED — 0 replay mismatches, 0 unlogged changes vs pre-L20 backup, churn 0.9%. Log `indices/corrections/l30_corrections_applied.csv`, provenance `L30_audit_adjudication`; backup `qa/backups/.bak_L30…` |
| G28 | `qa/backups/.bak_L30_corrected_dataset.csv` | + Layer 29 (POST-CAMPAIGN EXTENSIONS: [a] full-text CANT_TELL closure for the 435 tier-B/C/ripple CTs the L21 escalation never covered — 5 Sonnet readers on parsed markdown [132 FIX / 255 LEAVE / 47 still-CT] → independent skeptical gate [121/132 approved, 92%; caught a fabricated citation, an inferred lustrum rule, derived sums, field-routing errors]; [b] `contract_part_time_allowed` deterministic evidence screen over all 2,739 records → 186 suspects agent-read [54 FIX: 46 under-coded Falses with pro-rata content in other sections + 8 phantom Trues]; [c] ripple closure of the sides these applies exposed) | **274 changed vs G27** (168 full-text CT closure + 54 part-time screen + 35 ripple + 17 CAO-1496 family, incl. companion units) | provenances `L29_part_time_screen`/`L29_ct_fulltext`/`L29_ripple`; backups `qa/backups/.bak_L29*`; log `indices/corrections/l29_corrections_applied.csv`. NEW DURABLE QA INFRASTRUCTURE in `indices/`: adjudication ledger `corrections/jump_campaign_adjudications.csv`, `rebuild.sh`, `jump_census.py`, battery checks 5 (`statutory_fingerprints.py`, time-aware) + 6 (`same_term_coverage_check.py`); `boolean_flip_rate.py` monitor (flip rate mean 9.1%→3.2% pre→post campaign); upstream extractor prompt got a false-positive-traps block. Sickpay-continuation = year-1-rate convention gate-confirmed |
| G27 | `qa/backups/.bak_L29a_corrected_dataset.csv` | + Layer 28 (RIPPLE WAVES 2–6 to the FIXED POINT: each apply exposes the same phantom on one more previously-agreeing sibling; uncovered sides converged 662→168→44→22→11→7→**0** — waves adjudicated by Sonnet gate-standard readers [wave 2: 114 FIX/54 LEAVE; wave 3: 37/7; wave 4: 17/5; wave 5: 7/4; wave 6: 6 FIX/1 LEAVE — incl. 3690007 kept True because that edition embeds the full POB pension regulations its siblings lack]) | **202 changed vs G26** (incl. companion units) | same precedent catalogue; recurring new families: anti-avoidance replacement-hire clause ≠ conversion right, AOW-only ketenregeling extension ≠ general chain rule, boilerplate exclusion-list ≠ affirmative 13th-month grant ("HB 5e editie" template family 822/826/827), "decentralized former-CAO regulation" ≠ current benefit; 2 silent-document recoveries from the SAME document's other topic section (never sibling-import). Backup `qa/backups/.bak_L28…`; log `indices/corrections/l28_corrections_applied.csv`, provenance `L28_ripple_wave2`. POST: census 1 verified drop / 0 V-dips / 0 uncovered at each wave |
| G26 | `qa/backups/.bak_L28_corrected_dataset.csv` | + Layer 27 (FINAL SAME-TERM RIPPLE CLOSURE: the 662 UNCHECKED sibling sides newly exposed by the L24–L26 applies [fixing one edition surfaces the same phantom on a previously-agreeing third sibling], 8 Sonnet gate-standard readers on full extracts [674 verdicts: 422 FIX / 238 LEAVE / 14 CANT_TELL, 0 conflicts, 662/662 coverage] + the 24-cell wrap-up micro-check [637008 invented-sum cleared, 4255 voluntary-pension-continuation ×4 → False, tier-B cosmetic unit unifications]) | **511 changed vs G25** (494 ripple + 17 wrap-up, incl. companion units) | precedent catalogue enforced (statutory-restatement ≠ enhancement, ketenregeling ≠ conversion rights, generic RI&E ≠ PSA, vakantietoeslag ≠ 13th month / EJU ≈ 13th month, dual-rate training = general rate, work-away lodging ≠ relocation); genuine amendments deliberately preserved even against VERIFIED siblings when own-text supported. Backup `qa/backups/.bak_L27…`; log `indices/corrections/l27_corrections_applied.csv`, provenance `L27_final_ripple`/`L27_wrapup`. POST: overall census **1 drop (CAO 65 remplaçanten series switch, PDF-verified genuine), 0 V-dips, 0 Λ, 0 uncovered**; a wave-2 ripple over the 168 sides newly exposed by L27 itself was adjudicated to closure (see L28 row when applied) |
| G25 | `qa/backups/.bak_L27_corrected_dataset.csv` | + Layer 26 (TIER C PERSISTENT campaign: cross-term one-way jumps ≥0.5 that persist in later editions; 28 Sonnet readers, 3,261 verdicts [1,517 genuine / 771 extract-inconsistent / 726 phantom / 211 cant-tell / 36 cosmetic] → 14 independent gates: 1,194/1,419 confirmed [1,133 APPLY + 61 APPLY_MODIFIED / 225 SKIP, 84%] + gate-standard straggler sweep of 68 reader leftovers) | **1,465 changed vs G24** | dedup rule enforced vs tier B (a 0.5 jump that is the first leg of a V-shape belongs to tier B — tier-B core cells excluded); schema-dotted field names mapped via SECMAP; cross-contamination incident (16 stray CAO-3335 rows from a shared helper script) caught by aggregation key-validation → unique-helper-name rule. Backup `qa/backups/.bak_L26…`; log `indices/corrections/l26_corrections_applied.csv`, provenance `L26_tierC_per` |
| G24 | `qa/backups/.bak_L26_corrected_dataset.csv` | + Layer 25 (TIER C SUSPICIOUS campaign: cross-term one-way jumps ≥0.5 that revert later; 2,141 Sonnet reader verdicts [665 phantom / 663 extract-inconsistent / 585 genuine / 197 cant-tell / 31 cosmetic] → 13 independent gates: 1,088/1,270 confirmed [992 APPLY + 96 APPLY_MODIFIED / 182 SKIP, 86%]; PLUS L25a: the 2948/234 probation TEMPLATE-SHADOWING family resolution — full-text tiebreak REVERSED the extract-tier consensus: 2010-2012 editions have a governing body article "Artikel 8a Proeftijd … maximaal twee maanden" that the extractor SHADOWED with an unfilled model-contract appendix; 2948007/2948008 keep 2/2 months, all 2012+ template-only editions → blank [31 cells]) | **1,410 changed vs G23** (1,379 tier-C-sus + 31 probation family) | new failure mode catalogued: **template shadowing** (extractor grabs model-contract appendix, drops the governing body article — extract-silence ≠ document-silence); fix_ids 310005/3313004 excluded (L23 supersede). Backup `qa/backups/.bak_L25…`; log `indices/corrections/l25_corrections_applied.csv`, provenance `L25_tierC_sus`/`L25_probation_family` |
| G23 | `qa/backups/.bak_L25_corrected_dataset.csv` | + Layer 24 (TIER B cross-term SANDWICH campaign: 606 V/Λ reversal events / 939 middle-edition cells where both flanks agree, 8 Sonnet readers [455 FIX / 451 LEAVE / 25 CT / 8 COSMETIC] → quote-provenance check (27 hard fails re-derived) → 5 independent gates 417/452 confirmed) | **461 changed vs G22** | gates enforced never-import-into-silent-documents, strict-statement bar, C3 first-tier (rediscovered independently from qa/conventions), disability-insurance taxonomy; 310005 cells excluded (superseded by L23 re-extraction). Backup `qa/backups/.bak_L24…`; log `indices/corrections/l24_tierB_applied.csv`, provenance `L24_tierB_sandwich`. POST: overall census 2 drops + 2 V-dips, all campaign-verified, 0 uncovered — the 310 drop RESOLVED by L23 |
| G22 | `qa/backups/.bak_L24_corrected_dataset.csv` | + Layer 23 (CORRUPT-PARSE RE-EXTRACTION: full 310-column re-read of 3313004 + 286-column re-read of 310005 from re-OCR'd PDFs [both parses were fabrication-riddled: invented notice/probation schedules, phantom childcare/homeoffice blocks, wrong shift/overtime %s] + 12-cell ripple on 3313's 2011 editions + 2 gate-spot-check follow-ups) | **252 changed vs G21** | per-document evidence tier = re-OCR text; independent gates (91/113 and 145/162 approved; SAME-verdict spot-checks caught 2 extra errors); artifacts in `qa/reextraction/`; log `indices/corrections/l23_reextraction_applied.csv`. SIDE-FINDING: 310005's 572 upstream SALARY rows 100% fabricated — verified 252-row replacement + audit in `qa/reextraction/` (salary track to integrate); CAO 3313 has NO salary tables by design |
| G21 | `qa/backups/.bak_L23a_corrected_dataset.csv` | + Layer 22 (HANNA RULINGS on residual CANT_TELLs: C3 first-tier for 750/3798 pay tiers, probation=max stated tier, unfilled templates→blank, silent booleans→False, 4 precedent-analogy classifications) | 22 changed vs G20 | per-cell rulings logged with rationale; log `indices/corrections/l22_hanna_rulings_applied.csv`, provenance `L22_hanna_rulings` |
| G20 | `qa/backups/.bak_L22_corrected_dataset.csv` | + Layer 21 (CANT_TELL resolution at the next evidence tier: all 114 open CANT_TELL cells re-adjudicated against the PARSED FULL TEXT (markdown) instead of extracts [62 FIX/13 LEAVE/37 still-CT] + 3313004's corrupted parse RE-OCR'd from the PDF (43 pages; full CAO confirmed, its extraction was pervasively fabricated — 13 more fixes incl. invented €25 homeoffice stipend, wrong pension figures) + 15-cell normalizer knock-on closure incl. the CAO 187 safety drop [4 extraction gaps restored]) | **106 changed vs G19** (74 gated fixes + companion units) | evidence-tier-aware gate (each item verified against ITS source: markdown / re-OCR text / extract; 71 APPLY + 3 APPLY_MODIFIED / 15 SKIP incl. the gate DEFENDING current values via the Opus disability-insurance precedent). Backup `qa/backups/.bak_L21_corrected_dataset.csv`; change-log `indices/corrections/l21_corrections_applied.csv`, provenance `L21_<source>`. Residual: 28 documented CANT_TELLs (multi-tier no-selection-rule, silent text, borderline classification — see l21 report); re-OCR artifacts in `qa/reextraction/` |
| G19 | `qa/backups/.bak_L21_corrected_dataset.csv` | + Layer 20 (TIER-A SAME-TERM campaign, amendment-aware: all 645 same-term topic-jump events \|Δz\|≥0.5 / 1,144 scoring-relevant changed cells adjudicated by 11 Sonnet first readers on FULL extracts [verdicts: 456 extract-inconsistent, 396 phantom, 162 genuine-amendment LEAVE, 113 cant-tell, 19 cosmetic] → 764 deduped fix targets → 8 independent Sonnet gates [611/764 confirmed, 153 rejected: never-derive arithmetic, cross-edition imports, same-doc-silence] → ripple closure passes over the fixes' own knock-on diffs [2 Sonnet readers + Sonnet gate 54/57 + **Opus judge on 11 LEAVE-vs-gate tensions** (4 overrides, 3 L20 over-flip reverts: jubilee ≠ retire-gratuity, health-condition ≠ disability insurance) + 3 micro-closures], until 0 same-term cells lack two-side adjudication) | **902 changed vs G18** (809 main + 70 ripple + 8 judge + 8+5+2 closures; incl. ~200 companion-unit writes) | same-term = SELECTOR only, text decides; mechanical quote-provenance check (word-level containment; 1,600+ quotes, 15 hard fails → re-derived at gate, 3 were other-document bleed); guards: expected-current, value/unit split, conflict detection; backups `qa/backups/.bak_tierA*`; change-log `indices/corrections/tierA_corrections_applied.csv`, provenance `L20_tierA_*`. POST: overall census 3 drops + 1 V-dip (all verified real), same-term events 645→254, remaining 409 cells = adjudicated genuine amendments / cant-tells / cosmetics |

† G7's count is cells *changed relative to G6* (not net-new-vs-raw — many of these booleans were also touched by earlier layers). A round-trip side-effect additionally normalised 245 literal `"null"`/`"None"` strings to empty (benign; both already meant missing).

Backups are kept in **`qa/_old/`**. Build artifacts (`corrected_dataset.perfile_applied.csv`, `corrected_dataset.removals_applied.csv`) are deleted after promotion once byte-identical — regenerate via `qa/full_audit/apply_perfile_fixes.py` / `apply_removals.py` if needed.

**Upstream source** (external, not in this repo):
`/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction/outputs/excel/new_results/extracted_data_non_salary.csv`
(byte-identical to `inputs/`). Raw per-CAO JSONs: `…/outputs/llm_extracted/new_flow/<cao>/`.

## The applied layers

1. **Layer 1 — per-topic clean-wins + master review** (`qa/apply_corrections.py` ← `qa/apply_list.csv`):
   514 cells / 111 records. The deterministic+subagent corrections accepted across the 13 topics,
   plus the 126 human-reviewed `MASTER_REVIEW_FOR_HANNA` edits.
2. **Layer 2 — full-audit promotion** (`qa/full_audit/promote.py`, 2026-06-04): 781 cells / 522 records.
   Twice-verified (holistic + definition-anchored re-verify, which caught a ~22% over-correction rate).
3. **Layer 3 — per-file `FIX_clear`** (`qa/full_audit/apply_perfile_fixes.py`, promoted 2026-07-01):
   3,318 cells / 1,000 records / 110 fields. ONLY the subset where the new value == the record's **own
   source** (quote-backed, same unit dimension, non-convention). Reversible via
   `qa/full_audit/perfile_apply_changelog.csv`.
4. **Layer 4 — verified absence-removals** (`qa/full_audit/apply_removals.py`, promoted 2026-07-03):
   2,332 cells (1,813 boolean `True→False` + 519 enum clean-ups). The RECONCILE/EMPTY subset that passed
   a TWO-stage gate: (a) 100-cell stratified sample vs full source (found 15% false overall — numerics 83%!,
   so numerics were carved out entirely), then (b) per-cell **snippet-judging** of all 3,119 keyword-flagged
   bool/enum cells (calibrated: catches 4/5 known-bad, ~1.5% residual on applied). NOT applied: 929
   AFFIRMED/UNCLEAR cells → `removals_bool_enum_review.csv`; 547 numerics → `removals_numeric_ruling_sheet.csv`
   (convention rulings, Hanna). Reversible via `qa/full_audit/removals_apply_changelog.csv`.
5. **Layer 5 — convention adjudication + Hanna's numeric rulings** (`qa/full_audit/apply_wave2.py`,
   promoted 2026-07-03): 2,105 cells. (a) FIX_audit 1,416 + surcharge_normalize 41 + statutory_deferred 172
   adjudicated per cell under the ruled conventions (C1 base figure · C2 surcharge=increment · C3 first tier ·
   C4 representation-equality · C5 strict enums · C6 quote-evidence) by 12 Haiku agents → 1,354 changes
   (spot-verified by an independent Sonnet reader: 57/60 agree; the 3 disagreements excluded), 230 keeps,
   45 residue. (b) Hanna's ruling sheet: 22 DELETE patterns → 345 blanks; the notice-range conditional →
   13 cells set to 1 month. Reversible via `qa/full_audit/wave2_apply_changelog.csv`.
6. **Layer 6 — CHECK-pattern resolutions** (`qa/full_audit/apply_l6.py`, promoted 2026-07-03):
   151 cells from the 15 patterns Hanna reviewed via `removals_numeric_suggestions.md` — 81 value blanks
   (+ paired units) + the 683 bandwidth normalization (min/max → 75/125 % of contract hours); 19 keeps
   incl. all `overtime_min_rest` values (the 36 h rests turned out quote-backed, revising the draft
   suggestion). Reversible via `qa/full_audit/l6_apply_changelog.csv`.
7. **Layer 7 — residue suggestions** (`qa/full_audit/apply_l7.py`, promoted 2026-07-05): 40 cells —
   the Wave-2 residue (48 hard cells) got per-cell second-reader suggestions (quote-grounded, with
   confidence); Hanna accepted → 36 value changes + paired units applied (12 KEEP = no change).
   The L7 changelog carries **why + source quote per cell**. Reversible via
   `qa/full_audit/l7_apply_changelog.csv`.
8. **Layer 8 — source-verified boolean-flip + numeric-regression fixes** (`indices/apply_boolean_corrections.py`,
   applied 2026-07-08): **3,549 boolean + 10 numeric = 3,559 cells**. Cross-edition inconsistencies where a
   field flips between a CAO's editions. Every one re-checked against the raw `new_flow` extract via
   `indices/source_lookup.py` (all 242 CAOs, read-only) by **Sonnet-on-snippets** agents (Campaigns B + C,
   keyword-filtered passages, majority-vote across editions), gated to resolved verdicts with real evidence
   and confidence ≥ medium. This corrects *inconsistency* only — booleans wrong the **same** way across all
   editions are invisible to a flip method. Provenance in `indices/corrections/boolean_corrections_applied.csv` +
   `indices/corrections/numeric_corrections_applied.csv` (record · field · old → new · confidence · evidence quote),
   folded into `PROVENANCE_all_layers.csv` as `L8_flip_verification`. Reversible via
   `qa/_old/corrected_dataset.bak.2026-07-08_pre-L8.csv`. NB: applied directly to the canonical file (not via
   a promote script), so a 245-cell `"null"/"None"→empty` write-normalization rode along (benign).
9. **Layer 9 — all-CAO numeric same-term extraction-error fixes** (`indices/apply_numeric_corrections.py`,
   applied 2026-07-08): **61 numeric cells**. Every one of the 668 non-leave/non-overall
   SAME_TERM_republication numeric discrepancies (a field that shifts between editions of the *same* term =
   candidate extraction error) was source-verified by a **Sonnet-on-snippets** pass (38 batches → 125 proposed
   fixes), then each proposed fix was re-checked by an **independent second-reader** pass reading the source
   fresh (7 batches). Only the **61 confirmed by both** were applied — the 2nd reader rejected ~50% (computed
   values, 2-year-cumulative misreads, unit errors, truncated-passage over-reach), reproducing the known ~22%+
   numeric over-correction risk and stopping it before promotion. Provenance:
   `indices/corrections/numeric_corrections_v2_applied.csv` (+ `_confirmed.csv`; audit `numeric_verify_audit.csv`), folded
   into `PROVENANCE_all_layers.csv` as `L9_numeric_verification`. Reversible via
   `qa/backups/corrected_dataset.csv.bak_numfix2`. Leave (156) + overall (44) pairs were EXCLUDED (derived
   frequency columns / aggregate, not a single dataset cell) — they remain for manual review.
10. **Layer 10 — same-term consistency-unify + tie-break + full-passage recovery** (applied 2026-07-08):
    **111 cells** (incl. 2 paired-unit). Follow-ups on the L9 residue: (a) **44 consistency-unify** cells — for
    `ambiguous_multi` same-term pairs (editions picked different values from the same menu, e.g. 50%/100%
    training reimbursement), all editions of a term set to the **majority value**
    (`indices/corrections/unify_ambiguous_applied.csv`). **⚠️ Correction (verification pass):** the first cut was 51 cells,
    but a post-hoc audit found the unify compared **raw values ignoring unit families** and clobbered 7 cells
    where editions expressed the same fact in different units (e.g. `sickpay_duration` "2 years" = "104 weeks",
    `ketenregeling` weeks vs years, a pension % on two different denominators). Those 7 were **reverted to their
    pre-unify values** and the affected terms parked in `indices/review/unit_incompatible_review.csv`; one further cell
    (`1060009`) kept its correct value and got its unit fixed. 30 genuine ties
    (no majority) were then broken where possible by **cross-CAO frequency** (Hanna's rule: pick the value more
    common across other CAOs) — **20 cells across 18 terms** resolved (`indices/corrections/tie_break_applied.csv`); the remaining ties were then split further: 5 multi-rate cases unified to the **highest** value (Hanna rule), 1 formatting
    normalization, and 5 genuinely-conflicting values source-verified (2nd reader rejected all 5 — kept as legitimate/unresolvable
    in `review/ambiguous_ties_review.csv`). Net: all 30 ties resolved (26 by rule/frequency, 4 by Hanna's per-case decisions — training-days=general, full-time=base, commuting=lower, bonus=lowest-€ minimum). `review/ambiguous_ties_review.csv` now empty. (b) **33 recovered** cells — the 212 `cant_tell`
    pairs re-read with **full, untruncated passages** + the 5 leave value-disagreements, both run through the
    same 2-pass (propose → independent 2nd-reader confirm; ~65% rejected). Of the 33: **19 numeric
    corrections + 14 source-explicit blank-fills** (a figure the short snippets had cut off; the 2nd reader's
    blank-fill rule required the number to be literally present in that document, never statutory-inferred).
    Provenance: `indices/corrections/numeric_corrections_v3_applied.csv` + `corrections/unify_ambiguous_applied.csv`, folded into
    `PROVENANCE_all_layers.csv` as `L10_consistency_and_recovery`. Reversible via
    `qa/backups/corrected_dataset.csv.bak_unify`.

## Full traceability — the master provenance file

**`qa/full_audit/PROVENANCE_all_layers.csv`** consolidates every change event across L1–L10 into one
chain-ordered file: `record_id · field · layer · old → new · why · source quote`. Any cell in the
canonical dataset that differs from the original extraction can be traced there (coverage check in
`PROVENANCE_summary.md`). To reverse anything: the per-layer changelogs hold the old values, and
every generation has a dated backup in `qa/_old/`.

> **Counting note — provenance = change EVENTS, ledger/table = net cells.** A layer's provenance row
> count can exceed the per-layer cell counts in the table/ledger above, because provenance logs *every*
> change event including within-layer churn (a cell changed then reverted by that layer's re-verification
> pass logs 2 events but nets 0). Verified 2026-07-08: **L2** = 1,185 events (944 distinct cells; the ~126
> `reverify_revert` + 87 `reverify_refix` routes are the documented ~22% over-correction pass) → **781 net**
> cells (filled 208 + changed 527 + emptied 46). **L3** = 3,327 events = **3,318 distinct** cells + 9
> touched-twice. **L8–L10** reconcile to the cell against the raw extract (logged 3,731 + 245 benign
> `null`→empty normalizations = 3,976 total changed cells; 0 unexplained). So the numbers are consistent;
> they just answer "how many edits happened" vs "how many cells ended up different".

## Applied-vs-pending ledger

| Body of work | cells | status |
|---|---:|---|
| Layer 1 · per-topic + master review | 514 | ✅ applied (G1) |
| Layer 2 · full-audit promotion | 781 | ✅ applied (G1) |
| Layer 3 · per-file `FIX_clear` | 3,318 | ✅ applied (G2) |
| Layer 4 · verified absence-removals (bool/enum) | 2,332 | ✅ applied (G3) |
| Layer 5 · convention adjudication + Hanna rulings | 2,105 | ✅ applied (G4) — incl. FIX_audit, surcharge, statutory_deferred, DELETE patterns |
| Layer 6 · CHECK-pattern resolutions (Hanna-accepted) | 151 | ✅ applied (G5) |
| Layer 7 · residue suggestions (Hanna-accepted) | 40 | ✅ applied (G6) — residue queue CLOSED |
| Layer 8 · source-verified boolean-flip + numeric fixes | 3,559 | ✅ applied (G7) — cross-edition inconsistency only |
| Layer 9 · all-CAO numeric same-term fixes (two-pass verified) | 61 | ✅ applied (G8) — 668 candidates → 125 proposed → 61 confirmed; leave/overall parked |
| Layer 10 · consistency-unify + tie-break + full-passage recovery | 111 | ✅ applied (G9) — all 30 ambiguous ties resolved; 7 unit-blind unify clobbers caught + reverted (`review/unit_incompatible_review.csv`) |
| Layer 11 · date-collision picked-record fixes (two-pass) | 33 | ✅ applied (G10) — 39 proposed → 29 confirmed / 8 rejected / 2 uncertain; full review `indices/review/collision_corrections_review.csv` |
| Layer 12 · low-confidence collision fixes (Hanna-approved) | 3 | ✅ applied (G11) |
| Layer 32 · CANT_TELL family-consistency closure | 42 | ✅ applied (G31, canonical) — 104/173 CTs closed; 69 remain (`indices/review/all_open_cant_tells.csv`) |
| Layer 13 · pension `employee_contrib` %-of-premium blanks | 1,071 | ✅ applied (G12) — 3 passes (explicit unit / same-CAO siblings vs forward-fill masking / ≥20% plausibility); index employee_contrib now 0–19.6% |
| Removals bool/enum AFFIRMED/UNCLEAR | 929 | ✔ default = keep (in force); optional review `removals_bool_enum_review.csv` |
| Per-file `REVIEW` (undecidable) | 498 | parked by design |
| Legacy queue status | — | see `qa/full_audit/RETIRED_QUEUES.md` (agreement_conflicts + backfill retired; live residuals listed) |
| Holistic populated-value re-read | 12/13 topics | ⚠️ pension only; not rolled out |
| Salary track sign-offs + step-1 re-extraction rollout | 8 + 6 | 🔄 in progress (Hanna, 2026-07-01) — `salary/SALARY_QA_MEMO.md` |

**Pending, dataset-changing:** only the small tails — Wave-2 residue 48 (`wave2_residue.csv`),
per-file `REVIEW` 498 (parked by design), the optional bool/enum review 929 (default=keep already in
force), and the deferred holistic rollout (Wave 4). The former ~5,935-cell per-file backlog is fully
adjudicated and applied via Layers 4–6. **Known residual gap:** Layer 8 fixed only cross-edition boolean
*inconsistency*; booleans/numerics wrong the **same** way across all of a CAO's editions are not detectable
by a flip method — a verify-every-value pass (Wave 4 holistic, or a full source_lookup sweep) would be
needed to catch them. Master to-do: `../docs/ROADMAP.md`.

## Regeneration & caveats

- To rebuild from scratch: `apply_corrections.py` (→ G1 as `corrected_dataset.csv`) →
  `apply_perfile_fixes.py` (→ `perfile_applied.csv`, G2) → promote (copy G2 over `corrected_dataset.csv`).
  **Caveat:** re-running `apply_corrections.py` alone regresses the canonical file to G1 — re-apply
  Layer 3 and re-promote afterward, then rebuild indices.
- Indices live at top-level `indices/` (moved from `qa/indices/` on 2026-07-01); they read `qa/corrected_dataset.csv`.
  **Indices v2 (2026-07-05):** pooled z per file, `datum_kennisgeving` time axis, cao×month in-force panel,
  statutory pseudo-file, factor analysis — see `indices/INDICES_V2_PLAN.md`; v1 archived in `indices/_old_v1/`.
- **Wage (`mw`) indices read the deterministic-PARSER salary dataset**
  (`CAOsDataExtraction/outputs/parser_salary/extracted_data_salary_v2.csv`, 100% amount provenance,
  confidence tiers A+B only) — since v2, 2026-07-05 (previously the un-QA'd `salary_increase_events_derived.csv`).
  Non-salary corrections here don't affect it; salary-parser redeliveries do (rerun `mw_indices.py`).
  **CANONICAL salary dataset = this parser CSV.** The old second-LLM export
  (`CAOsDataExtraction/outputs/excel/new_results/extracted_data_salary.csv`) and its QA copy
  (`salary/outputs/corrected_salary.csv`, 8 unit cells fixed) are a static Feb/Jun-2025
  generation, **superseded and no longer maintained** — see the banner in `salary/SALARY_QA_MEMO.md`.
  Nothing is double-extracted: the old CSV was the *second* LLM's output; the parser reads the
  *first* LLM's wage grids directly. **v15 (2026-07-07):** 356,666 rows, A+B = 91.9%, after the
  holistic-audit parser fixes (`salary_parser/PARSER_RULES.md` + `DELIVERY.md`).
- iCloud/FUSE mount: file sizes/timestamps are unreliable — always verify by **content** (`diff`, pandas), not `ls`.
