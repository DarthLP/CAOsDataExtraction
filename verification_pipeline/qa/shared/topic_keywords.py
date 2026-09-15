"""Broad synonym lists per topic — used by source_text_loader.slice_for_item
to anchor passages. Generous, not narrow. False-positive anchors are cheap
(extra paragraph in slice); missing the only paragraph that mentions the
topic is expensive.

Seeded in Phase 1 from each topic's schema entry in
`inputs/NON_SALARY_PROMPTS_AND_SCHEMA.md` plus Dutch/English synonyms,
statutory acronyms, and related concepts.

Reviewed in Phase 0.5 Step 0.5.6 coverage sample; expanded via Stage 5a
feedback loop (PLAN.md §3.4).

HARD RULE: Never auto-edit this file. Human-merged convention only.
"""

from __future__ import annotations


TOPIC_KEYWORDS: dict[str, list[str]] = {
    # === BONUS (shares wage_information.md with wage) ===
    "bonus": [
        # Dutch
        "bonus", "bonussen", "premie", "premies", "toeslag", "toeslagen",
        "13e maand", "dertiende maand", "eindejaarsuitkering", "eindejaarspremie",
        "winstdeling", "winstuitkering", "prestatiebonus", "prestatietoeslag",
        "tekengeld", "instaptoeslag", "aanstellingsbonus",
        "kwalificatiebonus", "diplomatoeslag", "diplomapremie",
        "jubileumtoeslag", "jubileumuitkering", "ancienniteitstoeslag",
        "afscheidsuitkering", "afscheidspremie", "pensioneringspremie",
        "functietoeslag", "functietoeslagen", "vakcoderingstoeslag",
        "vast jaarlijks bedrag", "vaste jaaruitkering",
        # English
        "thirteenth month", "13th month", "year-end bonus",
        "profit sharing", "profit-sharing", "performance bonus",
        "sign-on bonus", "signing bonus", "qualification bonus",
        "seniority bonus", "loyalty bonus", "retirement gratuity",
        "job allowance", "job allowances",
    ],

    # === WAGE (shares wage_information.md with bonus) ===
    "wage": [
        # Dutch
        "loon", "salaris", "salarisschaal", "loonschaal", "schaal", "trede",
        "treden", "salaristabel", "loonsverhoging", "salarisverhoging",
        "instaptrede", "instapsalaris", "aanvangssalaris", "minimumloon",
        "persoonlijke toeslag", "persoonsgebonden toeslag",
        "prestatietrede", "prestatiestap", "beoordelingsstap",
        "schaalmaximum", "maximumschaal", "eindbedrag", "uurloon",
        # English
        "wage", "salary", "pay scale", "pay scales", "salary scale",
        "salary step", "salary grade", "minimum wage", "entry step",
        "personal allowance", "performance step",
    ],

    # === PENSION ===
    "pension": [
        # Dutch
        "pensioen", "pensioenen", "pensioenregeling", "pensioenfonds",
        "pensioenpremie", "pensioenopbouw", "pensioengevend salaris",
        "ABP", "PME", "PMT", "BPF", "PFZW", "Witteveen", "Witteveenkader",
        "AOW", "AOW-leeftijd", "ouderdomspensioen", "partnerpensioen",
        "nabestaandenpensioen", "wezenpensioen", "arbeidsongeschiktheidspensioen",
        "premievrije voortzetting", "premie-inhouding",
        "franchise", "dekkingsgraad", "indexatie", "toeslag op pensioen",
        "vervroegd uittreden", "RVU", "VPL",
        "opbouwpercentage", "middelloon", "eindloon", "premieovereenkomst",
        "uitkeringsovereenkomst",
        # English
        "pension", "pension scheme", "pension fund", "accrual rate",
        "premium", "contribution", "retirement age", "early retirement",
        "deferred retirement",
    ],

    # === TERM (termination) ===
    "term": [
        # Dutch
        "opzegtermijn", "opzegging", "ontslag", "ontslagvergoeding",
        "transitievergoeding", "beëindiging", "beeindiging",
        "einde dienstverband", "uitdiensttreding",
        "proeftijd", "proeftijdbeding", "ketenregeling", "ketenbepaling",
        "tijdelijk contract", "vast contract", "arbeidsovereenkomst",
        "UWV", "ontslagvergunning", "kantonrechter",
        "ww-suppletie", "ww-aanvulling", "wettelijke opzegtermijn",
        "non-concurrentiebeding", "concurrentiebeding", "geheimhoudingsbeding",
        "AOW-ontslag", "pensioenontslag",
        # WWZ / WAB era markers
        "WWZ", "WAB", "Wet werk en zekerheid", "Wet arbeidsmarkt in balans",
        # English
        "termination", "notice period", "severance", "dismissal",
        "probation", "probationary period", "chain rule", "fixed-term",
        "indefinite-term",
    ],

    # === OVERTIME ===
    "overtime": [
        # Dutch
        "overwerk", "overuren", "meeruren", "extra uren",
        "overwerktoeslag", "overwerkvergoeding",
        "tijd-voor-tijd", "tijd voor tijd", "TVT", "compensatieuren",
        "compensatieverlof", "compensatietijd",
        "onregelmatigheidstoeslag", "ORT", "ploegentoeslag",
        "weekendtoeslag", "feestdagentoeslag", "zaterdagtoeslag",
        "zondagtoeslag", "nachtdienst", "nachttoeslag",
        "wachtdienst", "consignatie", "consignatievergoeding",
        "minimumrust", "rusttijd",
        # English
        "overtime", "extra hours", "additional hours",
        "shift allowance", "shift premium", "unsocial hours",
        "weekend premium", "night shift", "on-call", "standby",
    ],

    # === TRAINING ===
    "training": [
        # Dutch
        "opleiding", "scholing", "bijscholing", "omscholing",
        "studiekosten", "studieregeling", "studieverlof", "studiebudget",
        "loopbaanbeleid", "loopbaanscan", "loopbaanadvies",
        "EVC", "BBL", "ROC", "vakopleiding",
        "opleidingsfonds", "O&O-fonds", "scholingsfonds",
        "leermeester", "leerwerkplek",
        "ontwikkelbudget", "individueel scholingsbudget",
        "terugbetalingsbeding", "studiekostenregeling",
        # English
        "training", "education", "study", "study leave", "study budget",
        "career scan", "skills development", "professional development",
    ],

    # === HOMEOFFICE ===
    "homeoffice": [
        # Dutch
        "thuiswerken", "thuiswerk", "telewerken", "telewerk",
        "hybride werken", "flexibel werken", "plaats van werk",
        "thuiswerkvergoeding", "thuiswerkbudget", "thuiswerkplek",
        "internetvergoeding", "telefoonvergoeding",
        "arbo thuiswerkplek", "RI&E thuiswerken",
        # English
        "home office", "home-office", "remote work", "remote working",
        "telework", "teleworking", "hybrid work", "work from home", "WFH",
        "home-working allowance", "internet allowance",
    ],

    # === CONTRACT (contract type) ===
    "contract": [
        # Dutch
        "arbeidsovereenkomst", "voltijd", "deeltijd",
        "fulltime", "parttime", "uren per week", "uren per maand",
        "min-max contract", "minmax", "nulurencontract", "oproepcontract",
        "ketenregeling", "ketenbepaling",
        "tijdelijk", "bepaalde tijd", "onbepaalde tijd",
        "verlenging", "verlenging arbeidsovereenkomst",
        "aanpassing arbeidsduur", "Wfa", "Wet flexibel werken",
        "omzetting tijdelijk naar vast",
        # English
        "contract", "full-time", "part-time", "fixed-term", "indefinite",
        "zero-hour", "on-call", "chain rule", "conversion to permanent",
    ],

    # === SAFETY ===
    "safety": [
        # Dutch
        "veiligheid", "arbo", "arbeidsomstandigheden",
        "intimidatie", "ongewenst gedrag", "pesten", "discriminatie",
        "integriteit", "vertrouwenspersoon", "klokkenluider",
        "BHV", "bedrijfshulpverlening", "EHBO", "Eerste Hulp",
        "PSA", "psychosociale arbeidsbelasting",
        "RI&E", "risico-inventarisatie", "preventiemedewerker",
        "arbodienst", "PMO", "preventief medisch onderzoek",
        "arbeidsongeval", "veiligheidstraining", "veiligheidscommissie",
        "werkdruk", "werkdrukmonitor", "vitaliteit",
        # English
        "safety", "occupational health", "harassment protocol",
        "integrity", "confidential counsellor", "whistleblower",
        "safety training", "safety committee", "wellbeing",
    ],

    # === CHILDCARE ===
    "childcare": [
        # Dutch
        "kinderopvang", "kinderdagverblijf", "BSO", "buitenschoolse opvang",
        "kindertoeslag", "kinderopvangtoeslag", "ouderbijdrage",
        "kindplaats", "leeftijd kind", "tegemoetkoming kinderopvang",
        "vergoeding kinderopvang", "korting kinderopvang",
        "bedrijfskinderopvang", "in-company kinderopvang",
        # English
        "childcare", "child care", "daycare", "after-school care",
        "childcare allowance", "childcare subsidy", "company daycare",
    ],

    # === AI ===
    "ai": [
        # Dutch
        "kunstmatige intelligentie", "AI", "algoritme", "algoritmes",
        "automatische besluitvorming", "geautomatiseerde besluitvorming",
        "AI-beleid", "AI-governance", "AI-toezicht",
        "AI-training", "machine learning", "monitoring",
        # English
        "artificial intelligence", "algorithm", "automated decision",
        "automated decision-making", "AI policy", "AI governance",
        "AI training", "AI monitoring",
    ],

    # === FRINGE (fringe benefits) ===
    "fringe": [
        # Dutch
        "secundaire arbeidsvoorwaarden",
        "reiskostenvergoeding", "woon-werkverkeer", "OV-vergoeding",
        "kilometervergoeding", "auto van de zaak", "leaseauto",
        "fietsplan", "fietsenregeling", "fietsvergoeding",
        "internetvergoeding", "telefoonvergoeding",
        "maaltijdvergoeding", "lunchvergoeding", "lunchregeling",
        "verhuiskostenvergoeding", "verhuisvergoeding",
        "zorgverzekering", "collectieve zorgverzekering",
        "spaarregeling", "verzekering", "extralegale voordelen",
        "bedrijfsfitness", "personeelsvereniging",
        "certificering", "diplomakosten",
        # English
        "fringe benefits", "commuting allowance", "travel allowance",
        "mileage", "company car", "bike scheme", "internet allowance",
        "meal allowance", "lunch", "relocation allowance",
        "health insurance support", "savings scheme",
    ],

    # === LEAVE (for reference; leave run is complete) ===
    "leave": [
        # Dutch
        "verlof", "vakantie", "vakantiedagen", "vakantiegeld", "vakantietoeslag",
        "zwangerschapsverlof", "bevallingsverlof",
        "kraamverlof", "geboorteverlof", "aanvullend geboorteverlof",
        "vaderschapsverlof", "ouderschapsverlof",
        "adoptieverlof", "pleegzorgverlof",
        "zorgverlof", "kortdurend zorgverlof", "langdurend zorgverlof",
        "calamiteitenverlof", "kort verzuim",
        "ziekteverlof", "ziekte", "arbeidsongeschiktheid",
        "loondoorbetaling bij ziekte", "WIA", "WGA",
        "studieverlof", "sabbatical",
        "feestdagen", "bijzonder verlof", "buitengewoon verlof",
        "WIEG", "UWV", "betaald verlof", "onbetaald verlof",
        # English
        "leave", "holiday", "vacation", "maternity", "paternity", "parental",
        "adoption", "sick leave", "sickness", "care leave", "emergency leave",
        "bereavement",
    ],
}


def _self_check():
    """Sanity check used by Phase 1 Step 7 spot-check verification."""
    assert len(TOPIC_KEYWORDS) == 13, f"expected 13 topics, got {len(TOPIC_KEYWORDS)}"
    for topic, kws in TOPIC_KEYWORDS.items():
        assert len(kws) >= 5, f"topic {topic} has only {len(kws)} keywords (cap 5)"
        # No duplicates within a topic
        assert len(kws) == len(set(kws)), f"topic {topic} has duplicate keywords"
    return True


if __name__ == "__main__":
    _self_check()
    print(f"OK — {len(TOPIC_KEYWORDS)} topics, "
          f"{sum(len(v) for v in TOPIC_KEYWORDS.values())} keywords total")
