# Plan: Add `lifecycle_stage` + related flags to CAO extraction CSVs

> Self-contained handoff for an AI coding assistant (Cursor, Claude Code, etc.).
> Read this entire file before writing code. Do not skip the "Hard rules" or
> "Scope and non-scope" sections.

## 1. Context

Work happens in a single repo: the data extraction pipeline at
`/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction`.

Key paths inside that repo:
- Parsed CAO markdowns: `outputs/parsed_pdfs/parsed_pdfs_markdown/<cao_number>/<file_name>.md`
- Extracted CSVs: `outputs/excel/new_results/extracted_data_non_salary.csv` and `extracted_data_salary.csv` (delimiter `;`)
- Schema (Pydantic): `schema/non_salary_schema.py` (contains the existing `document_type` field, defined around lines 44–57)

The extracted CSVs already contain a column **`general_document_type`** (8-value taxonomy: `full_cao_original`, `full_cao_update`, `partial_amendment_of_original`, `partial_amendment_of_latest`, `annex`, `protocol`, `other_supplement`, `unspecified`). That column captures the **structural** kind of document. It does NOT capture **lifecycle/legal status** — i.e. whether a file is a negotiation result, a definitive signed text, a version filed with SZW (Ministerie van Sociale Zaken en Werkgelegenheid), or just an application/notification. A file can be `full_cao_original` AND still be only an aanmelding (filing) version rather than the signed final.

This plan adds the missing lifecycle dimension as new columns in both CSVs, derived by a deterministic heuristic pass over filename + first/last pages of the parsed markdown.

## 2. Goal (one paragraph)

Add a new set of per-file lifecycle/legal-status columns to both `extracted_data_non_salary.csv` and `extracted_data_salary.csv`, derived deterministically from filename tokens and the first/last ~2 pages of the parsed markdown. Surface — do **not** auto-fix — disagreements with existing `general_document_type` / `general_signing_date` / `general_avv_applies` labels in a separate mismatches CSV for human review.

## 3. Hard rules (do not violate)

- **NEVER** modify files under `outputs/parsed_pdfs/parsed_pdfs_markdown/` (read-only source).
- **NEVER** modify files under `inputs/`.
- **NEVER** rewrite or alter the existing `general_document_type` column. The new columns sit alongside it.
- **NEVER** call an LLM/API in this task. This is a deterministic heuristic pass only. (A future B2 pass may add an LLM fallback for ambiguous rows; that is out of scope.)
- **NEVER** rename or reorder existing columns in the two CSVs.
- If unsure between two outputs, pick `unspecified` for the categorical and `False` for booleans, and record the reason in `lifecycle_evidence`.
- All new code must run under Python 3.13 with `pandas` only (no extra third-party deps).
- CSV delimiter for both extracted files is `;` (semicolon). Preserve it.

## 4. Scope and non-scope

In scope:
1. New Python module that classifies one `(cao_number, file_name)` pair into the lifecycle columns.
2. Driver script that walks `parsed_pdfs_markdown/` and emits one lifecycle CSV.
3. Joiner script that merges the lifecycle CSV onto both extracted CSVs, writing new copies (do NOT overwrite the originals — write `*_with_lifecycle.csv` next to them).
4. Mismatch report (CSV) flagging rows where lifecycle vs. existing labels disagree.
5. Unit-style spot-check script: prints 20 random classified rows with the evidence snippet so a human can eyeball-verify.

Explicitly out of scope for this iteration: a `is_best_version_for_period` flag that picks the single most authoritative file within a `(cao_number, period)` group. That is a follow-up after the per-file labels are validated.

Out of scope (do not attempt):
- LLM-based re-classification.
- Re-running the upstream extractor.
- Updating `schema/non_salary_schema.py` (the new columns are post-hoc, not part of the LLM extraction schema).
- Modifying any file under `inputs/` or `parsed_pdfs/`.

## 5. New columns (added to both CSVs)

| Column | Type | Allowed values | Notes |
|---|---|---|---|
| `lifecycle_stage` | str (categorical) | `negotiation_result`, `draft`, `definitive_unsigned`, `definitive_signed`, `consolidated_amendments`, `unspecified` | Primary status. Single-pick. |
| `is_signed` | bool | `True` / `False` | Signatures explicitly present (filename or content). |
| `is_filed_szw` | bool | `True` / `False` | Filed with SZW for registration (aanmelding). |
| `is_avv_declared` | bool | `True` / `False` | AVV decision visible in this file (Staatscourant / "algemeen verbindend"). Note: separate from existing `general_avv_applies` which describes the CAO, not the file. |
| `lifecycle_evidence` | str | free text | Comma-separated list of cues that fired, e.g. `"filename:definitief, filename:ondertekend, content:Aldus overeengekomen"`. Required — empty only if `lifecycle_stage = unspecified` and no booleans fired. |
| `lifecycle_source` | str | `filename`, `content`, `both`, `none` | Where the evidence came from. |

Column placement: append to the end of each CSV, in the order listed above. Do not insert into the middle.

## 6. Detection logic

### 6.1 Filename tokens (case-insensitive, match on word boundaries)

Compile these into Python regex sets in a single module-level dict so they can be tuned later.

```python
# All matches case-insensitive. Use re.compile with re.IGNORECASE.
FILENAME_PATTERNS = {
    "definitive": [
        r"\bdefinitief\b", r"\bdef\b", r"\bDEF\b",
        r"\bdefinitive\b", r"\bfinal\b",
    ],
    "signed": [
        r"\bondertekend\b", r"\bgetekend\b", r"\bsigned\b",
        r"met[\s_-]+handtekening", r"completed and signed",
    ],
    "unsigned_negation": [   # downgrades is_signed to False even if "def" present
        r"zonder[\s_-]+namen", r"zonder[\s_-]+handtekeningen",
        r"niet[\s_-]+ondertekend",
    ],
    "filed_szw": [
        r"\baanmelding\b", r"\baangemeld\b", r"\bSZW\b",
        r"\bTTW\b",  # tussentijdse wijziging filed with SZW — overlaps; see §6.3
    ],
    "draft": [
        r"\bconcept\b", r"\bdraft\b", r"\bvoorlopig(e)?\b",
        r"\btussenstand\b",
    ],
    "negotiation_result": [
        r"onderhandelingsresultaat", r"principeakkoord",
        r"akkoord[\s_-]+op[\s_-]+hoofdlijnen", r"cao[\s_-]?akkoord",
    ],
    "consolidated_amendments": [
        r"wijzigingen[\s_-]+geaccepteerd",
        r"wijzigingen[\s_-]+doorgevoerd",
        r"\bintegraal\b", r"geconsolideerd",
    ],
    "avv_declared": [
        r"\bAVV\b", r"algemeen[\s_-]+verbindend",
    ],
}
```

### 6.2 Content cues (first ~80 lines + last ~40 lines of the parsed markdown)

Read the file as UTF-8. Skip the standard header (`# CAO Document - Extracted Content\n\n*Source: …\n\n---\n\n`). Then take:
- `head_lines = first 80 non-empty lines after the header`
- `tail_lines = last 40 non-empty lines`

Apply these regex on `head_text` and `tail_text` (combined into `body_sample` for the search). All case-insensitive.

```python
CONTENT_PATTERNS = {
    "szw_stamp": [
        r"ONTVANGEN[^\n]{0,80}\d{4}",          # ONTVANGEN <date>
        r"Postbus[\s]+Aanmelden",
        r"Aanmeldingsformulier",
    ],
    "signature_block": [
        r"Aldus[\s]+overeengekomen",
        r"Aldus[\s]+ondertekend",
        r"namens[\s]+(de[\s]+)?partij(en)?",
        r"Was getekend",
    ],
    "negotiation_header": [
        r"^onderhandelingsresultaat",
        r"^principeakkoord",
        r"^akkoord op hoofdlijnen",
    ],
    "definitive_header": [
        r"definitieve[\s]+tekst",
        r"vastgesteld[\s]+op",
    ],
    "draft_header": [
        r"^concept(versie)?\b",
        r"^voorlopige[\s]+tekst",
    ],
    "avv_decision": [
        r"algemeen[\s]+verbindend[\s]+verklaard",
        r"\bStaatscourant\b",
        r"Besluit[\s]+van[\s]+de[\s]+Minister[\s]+van[\s]+(SZW|Sociale[\s]+Zaken)",
    ],
    "consolidated_marker": [
        r"wijzigingen[\s]+geaccepteerd",
        r"geconsolideerde[\s]+versie",
    ],
}
```

The "negotiation_header" and "draft_header" patterns use `^` — apply them line-by-line on the first 20 lines of `head_lines` (those are likely cover-page lines).

### 6.3 Combining cues into `lifecycle_stage`

Compute the booleans first, then the categorical.

```python
# Booleans (any cue fires)
is_signed         = any_filename("signed")  or any_content("signature_block")
if any_filename("unsigned_negation"):
    is_signed = False
is_filed_szw      = any_filename("filed_szw") or any_content("szw_stamp")
is_avv_declared   = any_filename("avv_declared") or any_content("avv_decision")

# Categorical precedence (top to bottom; first match wins)
definitive_hit = any_filename("definitive") or any_content("definitive_header")
draft_hit      = any_filename("draft") or any_content("draft_header")
negotiation_hit = any_filename("negotiation_result") or any_content("negotiation_header")
consolidated_hit = any_filename("consolidated_amendments") or any_content("consolidated_marker")

if negotiation_hit and not definitive_hit:
    lifecycle_stage = "negotiation_result"
elif draft_hit and not definitive_hit:
    lifecycle_stage = "draft"
elif consolidated_hit:
    lifecycle_stage = "consolidated_amendments"
elif definitive_hit and is_signed:
    lifecycle_stage = "definitive_signed"
elif definitive_hit and not is_signed:
    lifecycle_stage = "definitive_unsigned"
elif is_signed and not definitive_hit:
    # Signed but nothing labels it definitive — treat as definitive_signed
    lifecycle_stage = "definitive_signed"
else:
    lifecycle_stage = "unspecified"
```

Note: `is_filed_szw` does NOT set `lifecycle_stage`. Aanmelding files in this corpus typically carry the full definitive text — the lifecycle stage should reflect the content, the filing flag is orthogonal.

### 6.4 Evidence column

Build a comma-separated string listing every fired cue, prefixed by source:

```
filename:definitief, filename:ondertekend, content:Aldus overeengekomen
```

For content cues, include up to ~60 chars of the matched snippet (truncated) so a reviewer can locate it. Keep this field under 500 chars total — truncate with `…` if longer.

Set `lifecycle_source`:
- `filename` if only filename cues fired
- `content` if only content cues fired
- `both` if both
- `none` if neither

## 7. File structure to create

Create under the **coding repo** (`/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction`):

```
scripts/qa/lifecycle/
├── __init__.py
├── patterns.py            # FILENAME_PATTERNS, CONTENT_PATTERNS dicts
├── classifier.py          # classify_file(cao_number, file_path) -> dict
├── derive_lifecycle.py    # walks parsed_pdfs_markdown/, writes lifecycle_results.csv
├── join_lifecycle.py      # joins onto both extracted CSVs, writes *_with_lifecycle.csv
├── report_mismatches.py   # writes lifecycle_mismatches.csv
└── spot_check.py          # prints 20 random rows + evidence for manual review

outputs/qa/lifecycle/
├── lifecycle_results.csv              # one row per (cao_number, file_name)
├── lifecycle_mismatches.csv           # rows where new ≠ existing labels
├── spot_check_sample.csv              # 20 random rows
├── extracted_data_non_salary_with_lifecycle.csv
└── extracted_data_salary_with_lifecycle.csv
```

Do not modify the original `extracted_data_non_salary.csv` or `extracted_data_salary.csv`. Write copies.

## 8. Implementation steps (ordered)

### Step 1 — `patterns.py`

Implement the two dicts from §6.1 and §6.2 verbatim, plus precompiled regex versions. Export `FILENAME_PATTERNS_RE` and `CONTENT_PATTERNS_RE` keyed identically but with `re.compile(p, re.IGNORECASE)` values.

### Step 2 — `classifier.py`

```python
def classify_file(cao_number: str, file_path: pathlib.Path) -> dict:
    """
    Read the parsed markdown at file_path and return a dict with keys:
      lifecycle_stage, is_signed, is_filed_szw, is_avv_declared,
      lifecycle_evidence, lifecycle_source
    Apply §6.3 logic.
    """
```

Read no more than the first 200 lines and last 80 lines of the file (cap memory). Strip the standard header. Build `head_text`, `tail_text`, `body_sample`. Run filename matching on `file_path.stem`. Apply combining rules. Return dict.

Edge cases:
- File missing or empty → return `{..., lifecycle_stage: "unspecified", lifecycle_evidence: "file_empty_or_missing", lifecycle_source: "none"}`.
- File can't be decoded as UTF-8 → fall back to `errors="replace"`; do not crash.

### Step 3 — `derive_lifecycle.py`

```python
def main():
    """
    Walk outputs/parsed_pdfs/parsed_pdfs_markdown/<cao_number>/*.md
    For each, call classifier.classify_file
    Write outputs/qa/lifecycle/lifecycle_results.csv with columns:
      cao_number, file_name, lifecycle_stage, is_signed, is_filed_szw,
      is_avv_declared, lifecycle_evidence, lifecycle_source
    Note: file_name in CSV must NOT include the ".md" extension —
    it should match the file_name column in the extracted CSVs (which
    references the original PDF/DOCX, without extension). Verify by
    sampling 5 rows from extracted_data_non_salary.csv first.
    """
```

Run on `python3` (3.13). Single-threaded is fine; the corpus is small (~5,000 files max).

Important: after writing, print summary counts of `lifecycle_stage` and the three booleans so you can sanity-check.

### Step 4 — `join_lifecycle.py`

```python
def main():
    """
    Load lifecycle_results.csv.
    Load extracted_data_non_salary.csv (sep=';').
    Left-merge on (cao_number, file_name).
    Report rows that failed to merge.
    Write outputs/qa/lifecycle/extracted_data_non_salary_with_lifecycle.csv (sep=';').
    Repeat for extracted_data_salary.csv.
    """
```

Coerce `cao_number` to string on both sides before merging. Print:
- count of rows in input CSV
- count merged successfully
- count of unmatched rows (and a sample of 5)

If more than 5% of rows fail to merge, halt with an error message — something is wrong with filename normalization.

### Step 5 — `report_mismatches.py`

```python
def main():
    """
    Load extracted_data_non_salary_with_lifecycle.csv.
    Emit a row to lifecycle_mismatches.csv if any of these are true:

    (A) general_document_type in {'full_cao_original','full_cao_update'}
        AND lifecycle_stage in {'draft','negotiation_result'}
        -> Existing label says full CAO but new pass says it's preliminary.

    (B) general_signing_date is non-empty AND non-NaN
        AND is_signed == False
        -> Extractor recorded a signing date but no signature evidence in the file.

    (C) general_avv_applies == 'yes' AND is_avv_declared == False
        -> CAO has AVV but this file shows no AVV evidence (often fine,
           but worth surfacing).

    (D) lifecycle_stage == 'unspecified' AND lifecycle_source == 'none'
        AND general_document_type == 'full_cao_original'
        -> No signal at all; verify manually.

    Columns to include:
      cao_number, file_name, general_document_type, general_signing_date,
      general_avv_applies, lifecycle_stage, is_signed, is_filed_szw,
      is_avv_declared, lifecycle_evidence, mismatch_reasons (A/B/C/D, comma-sep)
    """
```

### Step 6 — `spot_check.py`

Pick 20 random rows (use `random.seed(42)` for reproducibility) from `lifecycle_results.csv`, oversampling rare lifecycle_stage values so each stage is represented at least once if it has rows. Write `spot_check_sample.csv` with the columns from §5 plus the first 200 chars of the parsed markdown. Print to stdout in a human-readable format.

### Step 7 — Run end-to-end

```bash
cd /Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction
python3 scripts/qa/lifecycle/derive_lifecycle.py
python3 scripts/qa/lifecycle/join_lifecycle.py
python3 scripts/qa/lifecycle/report_mismatches.py
python3 scripts/qa/lifecycle/spot_check.py
```

Print summary at the end:
- Total files classified
- Lifecycle stage distribution
- Boolean counts (is_signed, is_filed_szw, is_avv_declared)
- Number of mismatches in each category (A/B/C/D)
- Path to the spot-check sample

## 9. Acceptance criteria

A reviewer must be able to confirm all of the following before merging:

1. **No upstream files modified.** New code lives only under `scripts/qa/lifecycle/`. New outputs land only under `outputs/qa/lifecycle/`. Original `outputs/excel/new_results/extracted_data_non_salary.csv` and `extracted_data_salary.csv` are byte-identical to their pre-run versions (verify with a checksum, e.g. `shasum`). Nothing under `inputs/` or `outputs/parsed_pdfs/` is touched.
2. **Coverage.** `lifecycle_results.csv` has one row for every `.md` under `parsed_pdfs_markdown/`. Row count printed by the driver matches a manual `find … -name "*.md" | wc -l`.
3. **Join rate ≥ 95%.** Both `extracted_data_*_with_lifecycle.csv` show < 5% unmatched rows after the merge. If higher, classifier exits with an error rather than producing an incomplete CSV.
4. **Schema preserved.** The two `*_with_lifecycle.csv` files have all original columns in original order, plus the 6 new columns appended at the end.
5. **Stage distribution is reasonable.** `definitive_signed + definitive_unsigned + consolidated_amendments` together cover ≥ 60% of full-CAO rows. If `unspecified` exceeds 25%, the patterns need tuning before sign-off.
6. **Spot-check sample is human-readable.** 20 rows in `spot_check_sample.csv`, each with a clear evidence trail.
7. **Mismatches surfaced, not auto-corrected.** No row in any existing CSV has been silently overwritten. `lifecycle_mismatches.csv` is generated with the full documented schema (all columns present, correct dtypes) even when zero rows match — a clean corpus is a valid outcome, not a failure. The driver prints to stdout the row count for each category (A/B/C/D), including zeros, so the reviewer can see at a glance what was checked. The script must complete successfully when any or all categories are empty. If total mismatches exceed 30% of full-CAO rows, print a warning (not an error) — that magnitude likely indicates a pattern-tuning issue worth investigating, but it doesn't block sign-off on its own.

## 10. Open issues to flag back (do not resolve unilaterally)

If any of these come up during implementation, write a note in `outputs/qa/lifecycle/NOTES.md` and continue with the documented default. Do not silently invent a solution.

1. **`file_name` column format mismatch.** If the extracted CSV's `file_name` includes path or extension while the `.md` files do not, normalize on the lifecycle side (strip `.md`) and document the rule used.
2. **Multiple periods per file.** Some files (e.g. `cao 1 januari 2022 - 31 december 2023 definitief 23012023.md`) cover overlapping periods. The `(cao_number, general_start_date, general_expiry_date)` grouping uses whatever the extractor recorded. If the same `(cao_number, period)` tuple has zero `full_cao_*` rows, no winner is picked for that group and all rows get `False`.
3. **`general_signing_date` formats.** If it's not consistently ISO `YYYY-MM-DD`, parse defensively with `pd.to_datetime(..., errors='coerce')`. Rows with un-parseable dates lose the tie-break and fall through to alphabetical.
4. **TTW token in `filed_szw`.** The `\bTTW\b` filename pattern was included because TTW files are typically filed with SZW. But TTW = "tussentijdse wijziging" is more about structural amendment than filing — it may move to its own boolean in a later iteration. Keep it under `filed_szw` for now and note it.
5. **Empty corpus folders.** Some `cao_number` folders may be empty. Log and skip; do not crash.

## 11. What "done" looks like (one-line)

`outputs/qa/lifecycle/extracted_data_non_salary_with_lifecycle.csv` and `extracted_data_salary_with_lifecycle.csv` exist, both contain the 6 new columns, the mismatches report has been generated, and the spot-check sample is ready for manual review. Original CSVs untouched.
