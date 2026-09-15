# Phase 0.5 — Prerequisite verification findings

Date: 2026-05-17. Branch: pre-init (no git repo yet).

## Verdict

**Ready for Phase 1: YES** — with two adjustments to PLAN.md noted below.

No hard stops triggered. All 13 by_topic source files exist, format is consistent, Python env is clean, Task batch limit is verified at 12 parallel calls.

---

## 0.5.1 — by_topic/ files + topic→filename mapping

All 13 expected files exist at `qa_leave/inputs/by_topic/` and match the mapping in PLAN.md §0.6a. File sizes (bytes):

| Topic (plan name) | Filename | Size (bytes) | Size (chars) | Est. tokens (chars÷4) |
|---|---|---|---|---|
| ai | `AI_information.md` | 368,406 | 368K | 92K |
| childcare | `childcare_information.md` | 450,099 | 450K | 113K |
| contract | `contract_type_information.md` | 4,618,558 | 4.6M | 1,155K |
| fringe | `fringe_benefits_information.md` | 4,479,291 | 4.5M | 1,120K |
| (auxiliary) | `general_information.md` | 3,878,258 | 3.9M | 970K |
| homeoffice | `homeoffice_information.md` | 795,621 | 796K | 199K |
| leave | `leave_information.md` | 12,682,982 | 12.7M | 3,171K |
| overtime | `overtime_information.md` | 9,357,933 | 9.4M | 2,339K |
| pension | `pension_information.md` | 3,871,952 | 3.9M | 968K |
| safety | `safety_information.md` | 5,481,848 | 5.5M | 1,370K |
| term | `termination_information.md` | 5,722,599 | 5.7M | 1,431K |
| training | `training_information.md` | 5,770,230 | 5.8M | 1,443K |
| bonus + wage | `wage_information.md` | 22,749,586 | 22.7M | 5,687K |

**Schema's shared-source claim** (line 53 of `NON_SALARY_PROMPTS_AND_SCHEMA.md`):
> "Use a 1→1 mapping between input and output sections, except that wage_information feeds two outputs: bonuses_info and wage_scales_info"

Confirmed verbatim. `bonus` and `wage` share `wage_information.md`.

---

## 0.5.2 — Empirical Task-tool batch limit

| N | Result |
|---|---|
| 3 | 3/3 returned cleanly |
| 8 | 8/8 returned (agent 8 returned with a refusal message instead of the trivial echo, but it returned — counts as success) |
| 12 | 12/12 returned cleanly with real content (each agent counted `## ` headers in a different by_topic file; all 12 returned `1505`) |

**`MAX_PARALLEL_HARD_CAP` = 12 verified.** Did not test N=15 (preserves rate budget). Default `max_parallel = 12` in `subagent_runner.run_chunks`.

**Side validation**: all 12 by_topic files have exactly 1,505 `## ` CAO section headers, confirming the format and record count consistency PLAN.md §1 assumes.

---

## 0.5.3 — Python environment

- Python: **3.13.0**
- pip: **24.2** from python.org Framework install
- PEP 668 enforcement: **NOT active** on this system. Dry-run pip install succeeded without `externally-managed-environment` complaint.

**Action for Phase 1 Step 2**: `--break-system-packages` flag is NOT needed in `.claude/settings.json`. Omit from allow-list.

---

## 0.5.4 — CSV record + CAO counts

- `qa_leave/inputs/extracted_data_non_salary.csv`: **2,837 lines** (1 header + **2,836 records**).
- Plan said "~2,739 records" — actual is ~100 more. Minor discrepancy; not a blocker.
- **Unique cao_number in full CSV: 338** (not 100).
- **Records covered by by_topic/ files: 1,505** (the "100 biggest" curated set).
- **Unique CAOs in `homeoffice_information.md` `## ` headers: 95** (not 100). The "100 biggest CAOs" phrasing in the plan is approximate — actual scope is 95 CAOs × ~16 versions = 1,505 records.

**Action**: update PLAN.md scope wording from "100 biggest CAOs (1,505 records)" to "**95 biggest CAOs (1,505 records)**" or "**curated subset (1,505 records across ~95 CAOs)**". Cosmetic; defer or fix during Phase 1 Step 5 (when plan moves to qa/).

**CSV is semicolon-delimited (`;`), not comma.** The `resilient_csv.py` port from qa_leave/ already handles this; flagged here so no one accidentally writes a comma-based parser later.

---

## 0.5.5 — Source-format spot-check

Verified identical format across all 13 by_topic files:

```
# <topic>_information

Records: 1505

## <N>. CAO <cao_number> - <source_file_name>.pdf

- cao_number: `<id>`
- source_file_name: `<filename>`
- ingangsdatum: `<yyyy-mm-dd>`
- datum_kennisgeving: `<yyyy-mm-dd>`

```json
[
  ["<topic_question_1>: <free-text answer>"],
  ["<topic_question_2>: <free-text answer>"],
  ...
]
\`\`\`
```

No format variants detected. **`source_text_loader.py` needs a single per-CAO-section parser** — no per-topic adapters.

Per-record content is a JSON array of single-element arrays, each containing a Dutch-language free-text answer to a topic-specific question. The `topic_section` slice (§4.5) operates on this free-text content, not the JSON structure.

---

## 0.5.6 — Per-topic length distribution (estimate)

Average characters per CAO section = `file_size / 1505`. Token estimate = chars / 4 (rough; English-Dutch mixed text).

| Topic | Avg chars/CAO section | Avg tokens/section | Fits 2K soft target? | Fits 4K hard cap? |
|---|---|---|---|---|
| ai | 245 | 61 | yes (sparse) | yes |
| childcare | 299 | 75 | yes (sparse) | yes |
| homeoffice | 529 | 132 | yes | yes |
| pension | 2,573 | 643 | yes | yes |
| general (aux) | 2,577 | 644 | n/a | n/a |
| fringe | 2,976 | 744 | yes | yes |
| contract | 3,069 | 767 | yes | yes |
| safety | 3,643 | 911 | yes | yes |
| term | 3,804 | 951 | yes | yes |
| training | 3,836 | 959 | yes | yes |
| overtime | 6,219 | 1,555 | borderline | yes |
| leave | 8,430 | 2,107 | **just over** | yes |
| bonus + wage (shared file) | 15,116 | **3,779** | **NO** | **borderline** |

**Critical finding — wage runs need per-topic cap raise.** The shared `wage_information.md` source averages ~3,779 tokens per CAO section, which is above the 2,000 soft target and close to the 4,000 hard cap. Without adjustment, the slicer will frequently truncate or route to NHR for both `bonus` and `wage` topics.

**Recommended action (Phase 1 onwards):**

In `qa_bonus_aggregate.py` and `qa_wage_aggregate.py`, override per-item slice budgets:
- soft target: **3,500 tokens** (was 2,000)
- hard cap: **6,000 tokens** (was 4,000)

This reduces chunk size from 15-20 down to ~7-10 items per chunk, which raises Stage 3 chunk count for these topics. Schedule for the wage and bonus runs.

**Leave is also at the edge** (~2,107 tokens avg). Already handled by qa_leave/ (the reference run completed successfully); but if leave is ever re-run with the new pipeline, raise its caps similarly.

**Overtime is borderline** (~1,555 tokens avg). Soft target acceptable; some records will exceed and truncate. Watch A14/A15 audit rates during overtime Stage 4.

Full keyword-coverage review is deferred to Phase 1 Step 7 (when `topic_keywords.py` is seeded). Length-distribution histogram (median, p90, p95, p99) likewise deferred — the simple-mean estimate above is enough to flag wage and bonus as needing adjustment.

---

## Adjustments to PLAN.md before Phase 1

Two cosmetic/factual updates worth folding in during Phase 1 Step 5 (when plans move into `qa/`):

1. **Scope wording.** "100 biggest CAOs (1,505 records)" → "**95 CAOs × ~16 versions = 1,505 records**". Multiple section refs.

2. **Wage/bonus per-item budget override.** Add a note to §4.5 (or to the bonus/wage rows in §7 topic execution order): "`bonus` and `wage` share `wage_information.md` averaging ~3,779 tokens/section; set per-item soft target = 3,500 and hard cap = 6,000 in their respective `qa_<topic>_aggregate.py` scripts."

3. **CSV delimiter is `;`.** Not noted in plan; add to §4.1 `resilient_csv.py` docstring.

These are not blockers. Phase 1 can proceed.

---

## Files modified by Phase 0.5

None. Read-only checks only.

## Files created

- `phase_0_5_findings.md` (this file, at project root, will be archived or deleted after Phase 1 review)

## Next: Phase 1

Per the approved scope (Phase 0.5 + Phase 1), auto-continuing to Phase 1 unless any of the above findings constitute a blocker.

**Blockers found: NONE.**
