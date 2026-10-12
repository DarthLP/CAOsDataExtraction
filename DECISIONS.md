# Decisions

Append-only. Format: **claim** — reason — link.

## 2026-10-12 — Earlier CAO PDFs from officielebekendmakingen (`pipelines/p0_fetch_avv_pdfs.py`)

- **Only AVV decisions (Staatscourant) are downloaded** — zoek.officielebekendmakingen.nl holds AVVs, AVV requests and revocations, no CAO texts themselves; all 195 previously linked files (Lorenz + Sebastian) are AVVs — `inputs/pdfs/input_pdfs_new/SectoralCAOs_Update.xlsx`
- **Included: main-CAO AVVs, amendments, and same-sector side schemes (arbeidsongeschiktheid top-ups, bedrijfstakeigen regelingen, arbeid en gezondheid, functie-indeling, voorziening bij ongeval); excluded: funds (sociaal fonds, FCB, O&O), early retirement (VUT/vervroegd uittreden), pension** — side schemes carry working conditions; funds/VUT/pension do not (user decision) — this file
- **Side-scheme PDFs are not merged into main-CAO PDFs** — one Excel row per published file; the matching main edition is often ambiguous — proposed as a separate task
- **Window = publication date < Overview col I; empty col I = no upper bound** — deterministic before download (user decision)
- **Predecessor names of the same CAO count (e.g. verblijfsrecreatie → Recreatie, textielindustrie → Mode/interieur/tapijt/textiel); successor/merged broader CAOs and sectors with their own Overview code do not (e.g. VVT for 49/1045 after 2008-08-01; vleeswaren 709, reprografisch 85, metalektro hoger personeel 488, verzekering buitendienst 636, bouw UTA 517, suikerwerk/chocolade 197, kantoorvakhandel 1261)** — avoids one AVV under two codes
- **Shared AVVs go to the code whose name matches the title (21 over 20, 316 over 533, 182 over 1291, 633 over 1060)**; 20, 533, 1060, 1291 are not AVV'd under those names
- **Start = decree signing date stated in the title; blank when the title has none** — reproduces all 82 existing Lorenz Start values; older AVVs state no decree date on pages 1–3
- **Sebastian corrections go to sheet `sebastian_2` and folder `sebastian_2/<code>/`; originals untouched** — user decision
- **Predecessor CAOs that merged into today's CAO count only if they have no own row in the Overview (e.g. loodgieters → 2297, confectie → 2535 stay; zweminrichtingen → own row 3855 Zwembaden, excluded)** — user rule; downloaded files later deselected go to `<code>/_review/`, never deleted
- **Not-downloaded candidates (unsure/excluded) are listed in `inputs/pdfs/input_pdfs_new/review-not-downloaded-2026-10-12.csv`** — for manual review instead of silent drops
- **Verification agent reads page 1 of every downloaded PDF; 18 of 1134 rejected (12 fund/VUT schemes behind main-CAO titles, 3 AVV requests, 3 pension toeslagregelingen) → `conflicts.json` `remove_ids`, files to `<code>/_review/`; merger inputs it could not confirm are kept per the Overview-row rule** — titles alone miss scheme type — `pipelines/p0_avv_selection/verification.json`
- **Amendments ("tot wijziging van het besluit") are merged with the decision they amend into `<code>/merged/<base>_merged.pdf` (supersedes "not merged" for amendments only); single files and their Excel rows stay; Excel column J "Merged file"; side schemes stay unmerged (no reliable base link)** — user decision; base found via the "Stcrt. YYYY, nr. N" reference, chains followed to the original decision, ties left unlinked — `pipelines/p0_avv_selection/merged.json`
- **No document is dropped or left "unsure" on stage-1 evidence alone: every stage-1 flag and every LLM-unsure candidate (232) is re-judged by Sonnet on pages 1–4 (`verify2_prompt.md`); result 92 include / 139 exclude / 0 unsure; applied via `conflicts.json` `add_ids` / `remove_ids`** — user decision — `pipelines/p0_avv_selection/verification2.json`
- **SWA CAOs (Scholing, Werkgelegenheid en Arbeidsmarkt) are excluded everywhere, like O&O/training funds** — user decision; stage-2 agents disagreed between codes
- **The general pre-split Landbouw / Tuinbouw CAOs (1994–2000) are excluded, not assigned to Glastuinbouw / Open teelten / Dierhouderij** — user decision; too general to be the predecessor of one code
- **All files live in `input_pdfs_new/<code>/` (sebastian_2/ dissolved) and are listed in one sheet `Information` (old `Lorenz` + `sebastian_2` rows); `Comments` keeps "in Sebastian sheet" / "added 2026-10-12 (new)"** — user decision; supersedes the sebastian_2 layout above
