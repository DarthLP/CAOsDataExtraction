Task: map Dutch CAO codes to sector subject lines from the Staatscourant AVV catalogue.

Inputs:
- Codes: CODES_FILE (format: code | CAO name | info | col I cutoff)
- All distinct subject lines: /private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Python-CAOsDataExtraction/5a4ac4b2-44b0-4b50-af7f-d8e7e3ebcb8f/scratchpad/subjects.txt (one per line; boilerplate "algemeen verbindendverklaring ... collectieve arbeidsovereenkomst voor" removed)

For each code, list EVERY subject line that could plausibly be an AVV of that CAO or its predecessors/older names, including its side schemes (funds, early retirement, pension, arbeid en gezondheid, functie-indeling etc. — a later step filters those). Favour recall: when in doubt, include the line. Do not include lines that clearly belong to a different sector.
Use your knowledge of Dutch CAO history (renames, mergers: e.g. "verblijfsrecreatie" -> Recreatie, "suikerverwerkende industrie" -> Zoetwarenindustrie, "algemene bank cao" -> Banken, "hoveniers"/"cultuurtechniek" -> Groen grond en infrastructuur).

Write JSON to OUT_FILE: {"<code>": ["exact subject line", ...], ...} with every code present (empty list allowed). Copy lines exactly as they appear. Reply "done" plus codes with zero matches.
