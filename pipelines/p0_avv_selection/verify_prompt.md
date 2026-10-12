# Verification agent: are the downloaded AVV PDFs correctly assigned?

Input: one batch file from `p0_fetch_avv_pdfs.py verify-batches`. Per code a header
`## CODE <code>: <Overview CAO name> | info: ... | cutoff: <date or open end>`, then one line per document:
`<id> :: <first ~700 characters of page 1 of the downloaded PDF>`.

Judge each document from its page-1 text only (independent of how it was selected). Verdict:
- `ok`: an AVV decision (algemeen verbindendverklaring / verbindendverklaring CAO-bepalingen, incl. amendments
  "gewijzigde bepalingen" / "wijziging van het besluit") of THIS code's CAO, an older name of it, a predecessor
  CAO that merged into it, or a same-sector side scheme (arbeidsongeschiktheid, bedrijfstakeigen regelingen,
  arbeid en gezondheid, functie-indeling, voorziening bij ongeval, overgangsregeling).
- `wrong`: another sector, a fund (sociaal fonds, FCB, opleidings-/O&O-fonds), early retirement (VUT,
  vervroegd uittreden), pension, an AVV request ("verzoek") or revocation ("intrekking"), not an AVV at all,
  or "MISSING FILE".
- `unsure`: text unreadable/too short, or lineage cannot be judged from the text.

Write JSON to the given output path: `{"<id>": {"code": <code>, "verdict": "ok|wrong|unsure", "reason": "<=12 words"}}`
listing ONLY `wrong` and `unsure` documents (omit `ok`), plus a key `"_counts": {"ok": n, "wrong": n, "unsure": n}`.
Copy ids exactly. Reply only "done".
