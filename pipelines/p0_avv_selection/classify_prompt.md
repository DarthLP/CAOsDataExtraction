# Task
Decide which Staatscourant AVV publications belong to a given Dutch CAO (collective labour agreement) code.
Every candidate line is already an "algemeen verbindendverklaring" (AVV) decision, published before the cutoff.
Line format: id | publication date | flag | subject (title with boilerplate removed). flag W = amendment of an earlier AVV decision.

# INCLUDE (same CAO lineage as the code's CAO name)
- AVVs of the main CAO, and amendments (W) to them.
- Predecessor/older names of the SAME sector CAO (e.g. "verblijfsrecreatie" for Recreatie, "facilitaire callcenters" for Facilitaire contactcenters, "tijdschriftuitgeverijbedrijf" for Boeken- en tijdschriftuitgeverijbedrijf, "textielindustrie" for Mode-, interieur-, tapijt- en textielindustrie).
- Side schemes of that CAO that are NOT funds/early retirement/pension: e.g. "regeling aanvulling periodieke uitkeringen", "aanvullende voorzieningen bij arbeidsongeschiktheid", "functie-indeling/functieomschrijvingen", "winkelpersoneel en filiaalhouders" of the same sector.

# EXCLUDE
- Funds: "sociaal fonds", "fonds collectieve belangen"/"FCB", "opleidings-/O&O-fonds", "fonds overleg arbeidsvoorwaarden", any "... fonds ...".
- Early retirement: "vervroegd(e) uittreding/uittreden", "VUT", "brugpensioen"; pension ("pensioen"); "arbeid en gezondheid" schemes of OTHER sectors.
- Other sectors that merely share a word (e.g. "openbaar vervoer" for Open teelten; "banketbakkersbedrijf" for Bakkersbedrijf; "levensmiddelenbedrijf inzake kinderopvang" for Kinderopvang; "jachtbouw waterrecreatie" for Recreatie; "gezondheidscentra" for GGZ).
- Successor/merged CAOs with a broader scope that the code's info says it "falls under" (Vallen onder cao_nr. X), e.g. VVT ("verpleeg- verzorgingshuizen en thuiszorg") for Thuiszorg or for Verpleeg- en verzorgingshuizen. Before the merger, the narrower own-sector AVVs ARE included.
- Ids ending in "-n1"/"-n2" (correction re-publications) -> put them in "unsure" instead.
When genuinely unsure, put the id in "unsure" with a <=8-word reason; do not include it.

# Output
Write JSON to the given output path: {"<code>": {"include": [ids], "unsure": {"id": "reason"}}, ...}. Reply only "done".

# Learned from training review
- Pre-2005 "verblijfsrecreatie (en zweminrichtingen)" IS Recreatie; pre-2002 "textielindustrie" IS Mode-, interieur-, tapijt- en textielindustrie: a narrower/older name of the same sector is the same lineage -> include.
- "Stichting aanvullende verzekeringen bij arbeidsongeschiktheid" schemes of the same sector -> include (side scheme, not a fund).
- Different occupational group in a related sector (journalists vs publishers, retail vs wholesale, plumbers vs installation) -> unsure, not include.
- "-v1"/"-n1" duplicates of an included id -> unsure.
- Same-sector side schemes ARE included (not unsure): "arbeid en gezondheid voor <own sector>", "bedrijfstakeigen regelingen voor <own sector>", "regelingen medewerkers <own sector>", "overgangsregeling", "functiewaardering/-indeling", "arbeidsongeschiktheid" schemes. Only funds, early retirement and pension stay excluded.
- Retail (detailhandel) vs wholesale (groothandel) of the same goods, rail vs road transport, and sports employers vs sports retail are DIFFERENT CAOs -> exclude.
- Older names of the same industry ARE the same lineage: e.g. "suikerverwerkende industrie en suikerwerk- en chocoladeverwerkende industrie" -> Zoetwarenindustrie; "algemene bank cao" -> Banken.
