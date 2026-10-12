# p0 AVV selection (run of 2026-10-12)

Which Staatscourant AVV publication belongs to which CAO code of
`inputs/pdfs/input_pdfs_new/SectoralCAOs_Update.xlsx`. Rules and trade-offs: root `DECISIONS.md`.

## Flow (script: `pipelines/p0_fetch_avv_pdfs.py --work pipelines/p0_avv_selection <cmd>`)

| Step | Who | Input -> output |
|---|---|---|
| `catalog` | script | SRU API -> `catalog.json` (snapshot 2026-10-12, 5062 AVV decisions) |
| name variants | LLM (Haiku) | Overview names -> `llm/variants.json` |
| subject map | LLM (Sonnet), `subject_map_prompt.md` | distinct catalogue subjects -> `llm/subject_map/*.json` |
| `candidates` | script | catalogue + variants + map -> `candidates.json` |
| classify | LLM, `classify_prompt.md` | candidate batches -> `llm/classify/pass1_*.json` (Haiku), `pass2_*.json` (Sonnet: new candidates + pass-1 unsure) |
| `select` | script | classifications + `conflicts.json` (human review) -> `selection.json`, `unsure.json` |
| `download` | script | `selection.json` -> `input_pdfs_new/<code>/` |
| `verify-batches` + verify | script + LLM (Haiku), `verify_prompt.md` | page-1 text of each downloaded PDF -> `verification.json` (flags only); accepted rejections -> `conflicts.json` `remove_ids`, re-run `select` |
| verify stage 2 | `verify-batches --ids <flags+unsure> --pages 4` + LLM (Sonnet), `verify2_prompt.md`, `overview_names.txt` | every stage-1 flag and LLM-unsure candidate -> `verification2.json`; decisions -> `conflicts.json` `add_ids` / `remove_ids` |
| `merge` | script | amendments + the decision they amend -> `input_pdfs_new/<code>/merged/<base>_merged.pdf`, `merged.json`, `merge_unlinked.json` |
| `write-excel` | script | rows -> sheet `Information` (was `Lorenz`), Overview col J/K |

`conflicts.json` holds the human decisions: which code owns an AVV claimed by two codes (`pairs`, `by_date`),
subjects that belong to another Overview row (`remove`, `postfilter`, `remove_after`), `move`s, and single ids
rejected after verification (`remove_ids`).

## Quality (2026-10-12)
- Recall: 82/82 on Lorenz's labelled files (codes 932–2535), 91/91 on Sebastian's correct files (holdout).
- Precision: 40/40 and 43/45 in random samples; then cross-code conflicts resolved.
- Verification stage 1 (page 1, Haiku): 1046 ok, 22 wrong, 64 unsure of 1134.
- Verification stage 2 (pages 1-4, Sonnet) on 232 flagged/unsure: 92 include, 139 exclude, 0 unsure; 17 of 18 stage-1
  removals confirmed, 1 overturned. Final: 1117 selected (`selection.json`), 290 not selected with reason (`unsure.json`).
- Merge: 53 merged files covering 135 ids; 24 amendments without a selected base stay single.
- Re-running `candidates` + `select` from these files reproduces `selection.json` exactly.
