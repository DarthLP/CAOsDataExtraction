# Verification stage 2 (Sonnet): final verdict on flagged / unsure documents

Input: a batch from `verify-batches --ids <flags> --pages 4`: per code a header
`## CODE <code>: <Overview CAO name> | info | cutoff`, then `<id> :: <text of pages 1-4>`.
Also read `pipelines/p0_avv_selection/overview_names.txt` (every CAO with its own Overview row: `code | name | info`).

Decide for each document whether it belongs to THIS code. Read the whole text: decree header, the CAO name,
the parties (werkgevers-/werknemersorganisaties), the werkingssfeer (scope) article, and what the provisions are about.

`include` when it is an AVV decision (incl. amendments, rectifications/"verbetering" of such a decision, and
"verbindendverklaring van gewijzigde bepalingen") of:
- this code's CAO, or an older name of it; or
- a predecessor CAO that later merged into it AND has no own row in overview_names.txt; or
- a same-sector side scheme that is not a fund/early-retirement/pension scheme (e.g. arbeidsongeschiktheid
  top-ups, bedrijfstakeigen regelingen, arbeid en gezondheid, functie-indeling, voorziening bij ongeval,
  overgangsregeling, kinderopvang regeling).

`exclude` when it is: a fund (sociaal fonds, FCB, opleidings-/O&O-fonds, vakantie-/garantie-/risicofonds),
early retirement (VUT, vervroegd uittreden), pension or pension "toeslagregeling"; an AVV request ("verzoek")
or revocation ("intrekking") rather than a decision; another sector; or a CAO that has its own row in
overview_names.txt (name the other code).

`unsure` only if, after reading all 4 pages, the text genuinely does not allow a decision; say what is missing.

Write JSON to the given path: `{"<id>": {"code": <code>, "verdict": "include|exclude|unsure",
"reason": "<=20 words, cite the decisive phrase", "other_code": <code or null>}}` for EVERY document in the batch.
Copy ids exactly. Use only Read and Write. Reply "done" plus counts.
